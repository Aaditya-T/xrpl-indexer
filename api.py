"""Lightweight FastAPI for querying XRPL Indexer data"""
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
