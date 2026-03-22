"""Lightweight FastAPI for querying XRPL Indexer data"""
import base64
import json
import psycopg2
import psycopg2.extras
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config import Config

app = FastAPI(title="XRPL Indexer API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@contextmanager
def get_cursor() -> Generator[Any, None, None]:
    """Open a short-lived read-only database connection."""
    if Config.DATABASE_TYPE == "postgresql":
        conn = psycopg2.connect(Config.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            cursor.close()
            conn.close()
    else:
        db_path = Config.DATABASE_URL.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            cursor.close()
            conn.close()


def row_to_dict(row: Any) -> dict:
    """Convert a database row to a plain dict."""
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def rows_to_list(rows: list) -> list[dict]:
    return [row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Health / Status
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Simple liveness check."""
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")


@app.get("/status")
def status():
    """Return the last processed ledger index."""
    with get_cursor() as cur:
        cur.execute(
            "SELECT last_processed_ledger_index, updated_at "
            "FROM indexer_state ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        return {"last_processed_ledger_index": None, "updated_at": None}
    return row_to_dict(row)


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@app.get("/transactions")
def list_transactions(
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=50, ge=1, le=500, description="Results per page"),
    transaction_type: Optional[str] = Query(default=None, description="Filter by transaction type, e.g. Payment"),
    account: Optional[str] = Query(default=None, description="Filter by source account address"),
    destination: Optional[str] = Query(default=None, description="Filter by destination address"),
    source_tag: Optional[int] = Query(default=None, description="Filter by source tag"),
    destination_tag: Optional[int] = Query(default=None, description="Filter by destination tag"),
    ledger_min: Optional[int] = Query(default=None, description="Minimum ledger index"),
    ledger_max: Optional[int] = Query(default=None, description="Maximum ledger index"),
    from_date: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(default=None, description="End date (YYYY-MM-DD)"),
):
    """
    List transactions with optional filters and pagination.
    Returns transactions ordered by ledger index descending.
    """
    is_pg = Config.DATABASE_TYPE == "postgresql"
    ph = "%s" if is_pg else "?"

    conditions: list[str] = []
    params: list[Any] = []

    if transaction_type:
        conditions.append(f"transaction_type = {ph}")
        params.append(transaction_type)
    if account:
        conditions.append(f"account = {ph}")
        params.append(account)
    if destination:
        conditions.append(f"destination = {ph}")
        params.append(destination)
    if source_tag is not None:
        conditions.append(f"source_tag = {ph}")
        params.append(source_tag)
    if destination_tag is not None:
        conditions.append(f"destination_tag = {ph}")
        params.append(destination_tag)
    if ledger_min is not None:
        conditions.append(f"ledger_index >= {ph}")
        params.append(ledger_min)
    if ledger_max is not None:
        conditions.append(f"ledger_index <= {ph}")
        params.append(ledger_max)
    if from_date:
        conditions.append(f"created_at >= {ph}")
        params.append(from_date)
    if to_date:
        conditions.append(f"created_at <= {ph}")
        params.append(to_date + " 23:59:59")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * limit

    with get_cursor() as cur:
        cur.execute(f"SELECT COUNT(*) as count FROM transactions {where}", params)
        total = row_to_dict(cur.fetchone()).get("count", 0)

        cur.execute(
            f"SELECT id, ledger_index, transaction_hash, transaction_type, "
            f"account, destination, amount, fee, source_tag, destination_tag, created_at "
            f"FROM transactions {where} "
            f"ORDER BY ledger_index DESC "
            f"LIMIT {ph} OFFSET {ph}",
            params + [limit, offset],
        )
        rows = rows_to_list(cur.fetchall())

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": max(1, -(-int(total) // limit)),
        "data": rows,
    }


@app.get("/transactions/{tx_hash}")
def get_transaction(tx_hash: str):
    """Fetch a single transaction by its hash, including full raw data."""
    ph = "%s" if Config.DATABASE_TYPE == "postgresql" else "?"
    with get_cursor() as cur:
        cur.execute(
            f"SELECT * FROM transactions WHERE transaction_hash = {ph}",
            (tx_hash,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")
    result = row_to_dict(row)
    if isinstance(result.get("transaction_data"), str):
        try:
            result["transaction_data"] = json.loads(result["transaction_data"])
        except (ValueError, TypeError):
            pass
    return result


# ---------------------------------------------------------------------------
# Stats / Aggregations
# ---------------------------------------------------------------------------

@app.get("/stats")
def stats():
    """Return aggregated statistics about stored transactions."""
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM transactions")
        total = row_to_dict(cur.fetchone()).get("count", 0)

        cur.execute(
            "SELECT transaction_type, COUNT(*) as count "
            "FROM transactions "
            "GROUP BY transaction_type "
            "ORDER BY count DESC"
        )
        by_type = rows_to_list(cur.fetchall())

        cur.execute(
            "SELECT DATE(created_at) as date, COUNT(*) as count "
            "FROM transactions "
            "GROUP BY DATE(created_at) "
            "ORDER BY date DESC "
            "LIMIT 30"
        )
        by_day = rows_to_list(cur.fetchall())

        cur.execute(
            "SELECT MIN(ledger_index) as min_ledger, MAX(ledger_index) as max_ledger "
            "FROM transactions"
        )
        ledger_range = row_to_dict(cur.fetchone())

        cur.execute(
            "SELECT last_processed_ledger_index, updated_at "
            "FROM indexer_state ORDER BY id DESC LIMIT 1"
        )
        state_row = cur.fetchone()
        indexer_state = row_to_dict(state_row) if state_row else {}

    return {
        "total_transactions": total,
        "by_transaction_type": by_type,
        "by_day": by_day,
        "ledger_range": ledger_range,
        "indexer_state": indexer_state,
    }


@app.get("/stats/accounts")
def top_accounts(
    limit: int = Query(default=20, ge=1, le=100, description="Number of top accounts to return"),
):
    """Return the most active source accounts."""
    ph = "%s" if Config.DATABASE_TYPE == "postgresql" else "?"
    with get_cursor() as cur:
        cur.execute(
            f"SELECT account, COUNT(*) as count "
            f"FROM transactions "
            f"WHERE account IS NOT NULL "
            f"GROUP BY account "
            f"ORDER BY count DESC "
            f"LIMIT {ph}",
            (limit,),
        )
        rows = rows_to_list(cur.fetchall())
    return {"data": rows}


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def _encode_cursor(row_id: int) -> str:
    return base64.urlsafe_b64encode(str(row_id).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor value")


def _extract_tx_fields(row: dict, include_full: bool) -> dict:
    """Build the response dict for a single transaction row."""
    raw: Any = row.get("transaction_data")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            raw = {}
    if not isinstance(raw, dict):
        raw = {}

    # The indexer stores a _full_data sub-object with the canonical fields.
    # Fall back to the top-level dict for older records that don't have it.
    full = raw.get("_full_data") if isinstance(raw.get("_full_data"), dict) else raw
    meta = full.get("meta") if isinstance(full.get("meta"), dict) else None

    result: dict = {
        "id": row.get("id"),
        "ledger_index": row.get("ledger_index"),
        "transaction_hash": row.get("transaction_hash"),
        "transaction_type": row.get("transaction_type"),
        "account": row.get("account"),
        "destination": row.get("destination"),
        "fee": row.get("fee"),
        "source_tag": row.get("source_tag"),
        "destination_tag": row.get("destination_tag"),
        "created_at": str(row.get("created_at")) if row.get("created_at") else None,
        "close_time_iso": full.get("close_time_iso"),
        "tx_index": meta.get("TransactionIndex") if meta else None,
    }

    if include_full:
        result["tx_json"] = full.get("tx_json")
        result["meta"] = meta

    return result


@app.get("/sync/transactions")
def sync_transactions(
    after_ledger: Optional[int] = Query(default=None, description="Return transactions with ledger_index > this value"),
    cursor: Optional[str] = Query(default=None, description="Opaque cursor from a previous response for pagination"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max transactions to return"),
    include_full: bool = Query(default=False, description="Include full tx_json, meta, and close_time_iso"),
):
    """
    Stream transactions in ascending ledger order, suitable for syncing.

    - Start with `after_ledger` to begin from a known ledger.
    - Use the returned `next_cursor` on subsequent calls to page forward.
    - `include_full=true` adds tx_json, meta, and close_time_iso to each record.
    - Returns `has_more=false` when you have reached the latest data.
    """
    ph = "%s" if Config.DATABASE_TYPE == "postgresql" else "?"

    conditions: list[str] = []
    params: list[Any] = []

    if cursor:
        cursor_id = _decode_cursor(cursor)
        conditions.append(f"id > {ph}")
        params.append(cursor_id)
    elif after_ledger is not None:
        conditions.append(f"ledger_index > {ph}")
        params.append(after_ledger)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_cursor() as cur:
        cur.execute(
            f"SELECT id, ledger_index, transaction_hash, transaction_type, "
            f"account, destination, fee, source_tag, destination_tag, created_at, transaction_data "
            f"FROM transactions {where} "
            f"ORDER BY ledger_index ASC, id ASC "
            f"LIMIT {ph}",
            params + [limit],
        )
        rows = rows_to_list(cur.fetchall())

    data = [_extract_tx_fields(r, include_full) for r in rows]
    has_more = len(rows) == limit
    next_cursor = _encode_cursor(rows[-1]["id"]) if has_more and rows else None

    return {
        "has_more": has_more,
        "next_cursor": next_cursor,
        "count": len(data),
        "data": data,
    }
