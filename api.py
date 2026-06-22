"""Lightweight FastAPI for querying XRPL Indexer data"""
import base64
import json
import psycopg2
import psycopg2.extras
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator, Optional

from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import Config


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class _Base(BaseModel):
    """All response models inherit from this.
    model_config with arbitrary_types_allowed lets timestamp columns
    (datetime from PostgreSQL, str from SQLite) pass validation cleanly."""
    model_config = {"arbitrary_types_allowed": True}


class HealthResponse(_Base):
    status: str


class StatusResponse(_Base):
    last_processed_ledger_index: Optional[int] = None
    updated_at: Optional[Any] = None
    tracked_wallets: int


# --- Transactions ---

class TransactionSummary(_Base):
    id: Optional[int] = None
    ledger_index: Optional[int] = None
    transaction_hash: Optional[str] = None
    transaction_type: Optional[str] = None
    account: Optional[str] = None
    destination: Optional[str] = None
    amount: Optional[str] = None
    fee: Optional[str] = None
    source_tag: Optional[int] = None
    destination_tag: Optional[int] = None
    created_at: Optional[Any] = None


class TransactionListResponse(_Base):
    total: int
    page: int
    limit: int
    pages: int
    data: list[TransactionSummary]


class TransactionDetail(_Base):
    status: Optional[str] = None
    ledger_index: Optional[int] = None
    transaction_hash: Optional[str] = None
    transaction_type: Optional[str] = None
    close_time_iso: Optional[str] = None
    tx_json: Optional[dict[str, Any]] = None
    meta: Optional[dict[str, Any]] = None


# --- Stats ---

class TypeCount(_Base):
    transaction_type: Optional[str] = None
    count: int


class DayCount(_Base):
    date: Optional[Any] = None
    count: int


class LedgerRange(_Base):
    min_ledger: Optional[int] = None
    max_ledger: Optional[int] = None


class IndexerStateInfo(_Base):
    last_processed_ledger_index: Optional[int] = None
    updated_at: Optional[Any] = None


class StatsResponse(_Base):
    total_transactions: int
    by_transaction_type: list[TypeCount]
    by_day: list[DayCount]
    ledger_range: LedgerRange
    indexer_state: IndexerStateInfo
    tracked_wallets: int


class AccountEntry(_Base):
    account: Optional[str] = None
    count: int


class TopAccountsResponse(_Base):
    data: list[AccountEntry]


# --- Account state ---

class AccountState(_Base):
    address: str
    balance_drops: Optional[int] = None
    sequence: Optional[int] = None
    owner_count: Optional[int] = None
    flags: Optional[int] = None
    updated_at: Optional[Any] = None
    ledger_index: Optional[int] = None


# --- Balances ---

class Balance(_Base):
    currency: Optional[str] = None
    issuer: Optional[str] = None
    balance: Optional[str] = None
    limit_amount: Optional[str] = None
    limit_peer: Optional[str] = None
    authorized: Optional[bool] = None
    peer_authorized: Optional[bool] = None
    no_ripple: Optional[bool] = None
    no_ripple_peer: Optional[bool] = None
    freeze_flag: Optional[bool] = None
    peer_freeze_flag: Optional[bool] = None
    is_deleted: Optional[bool] = None


class BalancesResponse(_Base):
    address: str
    balances: list[Balance]


# --- Offers ---

class Offer(_Base):
    sequence: Optional[int] = None
    taker_gets_currency: Optional[str] = None
    taker_gets_issuer: Optional[str] = None
    taker_gets_value: Optional[str] = None
    taker_pays_currency: Optional[str] = None
    taker_pays_issuer: Optional[str] = None
    taker_pays_value: Optional[str] = None
    expiry_iso: Optional[str] = None
    flags: Optional[int] = None
    quality: Optional[str] = None
    ledger_index: Optional[int] = None


class OffersResponse(_Base):
    address: str
    offers: list[Offer]


# --- Token holders ---

class Holder(_Base):
    account: Optional[str] = None
    balance: Optional[str] = None
    limit_amount: Optional[str] = None
    authorized: Optional[bool] = None
    peer_authorized: Optional[bool] = None
    freeze_flag: Optional[bool] = None
    no_ripple: Optional[bool] = None


class HoldersResponse(_Base):
    issuer: str
    currency: str
    holder_count: int
    holders: list[Holder]


# --- Orderbook ---

class CurrencySpec(_Base):
    currency: str
    issuer: Optional[str] = None


class OrderbookOffer(_Base):
    account: Optional[str] = None
    sequence: Optional[int] = None
    taker_gets_value: Optional[str] = None
    taker_pays_value: Optional[str] = None
    expiry_iso: Optional[str] = None
    flags: Optional[int] = None
    quality: Optional[str] = None
    ledger_index: Optional[int] = None


class OrderbookResponse(_Base):
    taker_gets: CurrencySpec
    taker_pays: CurrencySpec
    offers: list[OrderbookOffer]


# --- Trades ---

class AmountInfo(_Base):
    currency: Optional[str] = None
    issuer: Optional[str] = None
    value: Optional[str] = None


class Fill(_Base):
    tx_hash: Optional[str] = None
    ledger_index: Optional[int] = None
    close_time_iso: Optional[str] = None
    maker_account: Optional[str] = None
    taker_account: Optional[str] = None
    filled_taker_gets: Optional[AmountInfo] = None
    filled_taker_pays: Optional[AmountInfo] = None
    fully_consumed: bool


class TradesResponse(_Base):
    count: int
    data: list[Fill]


# --- Ledger resolve ---

class LedgerResolveResponse(_Base):
    requested_timestamp: str
    ledger_index: Optional[int] = None
    ledger_close_time: Optional[str] = None


# --- Wallets ---

class TrackedWallet(_Base):
    address: str
    activation_tx_hash: Optional[str] = None
    activated_at: Optional[Any] = None


class WalletListResponse(_Base):
    total: int
    page: int
    limit: int
    data: list[TrackedWallet]


# --- Sync ---

class SyncTransaction(_Base):
    id: Optional[int] = None
    ledger_index: Optional[int] = None
    transaction_hash: Optional[str] = None
    transaction_type: Optional[str] = None
    account: Optional[str] = None
    destination: Optional[str] = None
    fee: Optional[str] = None
    source_tag: Optional[int] = None
    destination_tag: Optional[int] = None
    created_at: Optional[Any] = None
    close_time_iso: Optional[str] = None
    tx_index: Optional[int] = None
    tx_json: Optional[dict[str, Any]] = None
    meta: Optional[dict[str, Any]] = None


class SyncResponse(_Base):
    has_more: bool
    next_cursor: Optional[str] = None
    count: int
    data: list[SyncTransaction]


# ---------------------------------------------------------------------------

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
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def rows_to_list(rows: list) -> list[dict]:
    return [row_to_dict(r) for r in rows]


def _ph() -> str:
    return "%s" if Config.DATABASE_TYPE == "postgresql" else "?"


# ---------------------------------------------------------------------------
# Health / Status
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse)
def health():
    """Simple liveness check."""
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {e}")


@app.get("/status", response_model=StatusResponse)
def status():
    """Return the last processed ledger index and tracked wallet count."""
    with get_cursor() as cur:
        cur.execute(
            "SELECT last_processed_ledger_index, updated_at "
            "FROM indexer_state ORDER BY id DESC LIMIT 1"
        )
        state_row = cur.fetchone()
        cur.execute("SELECT COUNT(*) as count FROM tracked_wallets")
        tw_row = cur.fetchone()

    result = row_to_dict(state_row) if state_row else {"last_processed_ledger_index": None, "updated_at": None}
    result["tracked_wallets"] = row_to_dict(tw_row).get("count", 0) if tw_row else 0
    return result


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@app.get("/transactions", response_model=TransactionListResponse)
def list_transactions(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=500),
    transaction_type: Optional[str] = Query(default=None),
    account: Optional[str] = Query(default=None),
    destination: Optional[str] = Query(default=None),
    source_tag: Optional[int] = Query(default=None),
    destination_tag: Optional[int] = Query(default=None),
    ledger_min: Optional[int] = Query(default=None),
    ledger_max: Optional[int] = Query(default=None),
    from_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    to_date: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
):
    """List transactions with optional filters, paginated, newest first."""
    ph = _ph()
    conditions: list[str] = []
    params: list[Any] = []

    if transaction_type:
        conditions.append(f"transaction_type = {ph}"); params.append(transaction_type)
    if account:
        conditions.append(f"account = {ph}"); params.append(account)
    if destination:
        conditions.append(f"destination = {ph}"); params.append(destination)
    if source_tag is not None:
        conditions.append(f"source_tag = {ph}"); params.append(source_tag)
    if destination_tag is not None:
        conditions.append(f"destination_tag = {ph}"); params.append(destination_tag)
    if ledger_min is not None:
        conditions.append(f"ledger_index >= {ph}"); params.append(ledger_min)
    if ledger_max is not None:
        conditions.append(f"ledger_index <= {ph}"); params.append(ledger_max)
    if from_date:
        conditions.append(f"created_at >= {ph}"); params.append(from_date)
    if to_date:
        conditions.append(f"created_at <= {ph}"); params.append(to_date + " 23:59:59")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    offset = (page - 1) * limit

    with get_cursor() as cur:
        cur.execute(f"SELECT COUNT(*) as count FROM transactions {where}", params)
        total = row_to_dict(cur.fetchone()).get("count", 0)
        cur.execute(
            f"SELECT id, ledger_index, transaction_hash, transaction_type, "
            f"account, destination, amount, fee, source_tag, destination_tag, created_at "
            f"FROM transactions {where} ORDER BY ledger_index DESC LIMIT {ph} OFFSET {ph}",
            params + [limit, offset],
        )
        rows = rows_to_list(cur.fetchall())

    return {"total": total, "page": page, "limit": limit,
            "pages": max(1, -(-int(total) // limit)), "data": rows}


@app.get("/transactions/{tx_hash}", response_model=TransactionDetail)
def get_transaction(tx_hash: str):
    """Fetch a single transaction by hash with clean structured fields."""
    ph = _ph()
    with get_cursor() as cur:
        cur.execute(f"SELECT * FROM transactions WHERE transaction_hash = {ph}", (tx_hash,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Transaction not found")

    result = row_to_dict(row)
    raw = result.get("transaction_data")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            raw = {}

    full = raw.get("_full_data") if isinstance(raw, dict) and isinstance(raw.get("_full_data"), dict) else (raw or {})
    meta = full.get("meta") if isinstance(full.get("meta"), dict) else None

    return {
        "status": meta.get("TransactionResult") if meta else None,
        "ledger_index": result.get("ledger_index"),
        "transaction_hash": result.get("transaction_hash"),
        "transaction_type": result.get("transaction_type"),
        "close_time_iso": full.get("close_time_iso"),
        "tx_json": full.get("tx_json"),
        "meta": meta,
    }


# ---------------------------------------------------------------------------
# Stats / Aggregations
# ---------------------------------------------------------------------------

@app.get("/stats", response_model=StatsResponse)
def stats():
    """Aggregated statistics about stored transactions."""
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM transactions")
        total = row_to_dict(cur.fetchone()).get("count", 0)
        cur.execute(
            "SELECT transaction_type, COUNT(*) as count FROM transactions "
            "GROUP BY transaction_type ORDER BY count DESC"
        )
        by_type = rows_to_list(cur.fetchall())
        cur.execute(
            "SELECT DATE(created_at) as date, COUNT(*) as count FROM transactions "
            "GROUP BY DATE(created_at) ORDER BY date DESC LIMIT 30"
        )
        by_day = rows_to_list(cur.fetchall())
        cur.execute("SELECT MIN(ledger_index) as min_ledger, MAX(ledger_index) as max_ledger FROM transactions")
        ledger_range = row_to_dict(cur.fetchone())
        cur.execute("SELECT last_processed_ledger_index, updated_at FROM indexer_state ORDER BY id DESC LIMIT 1")
        state_row = cur.fetchone()
        cur.execute("SELECT COUNT(*) as count FROM tracked_wallets")
        tw_row = cur.fetchone()

    return {
        "total_transactions": total,
        "by_transaction_type": by_type,
        "by_day": by_day,
        "ledger_range": ledger_range,
        "indexer_state": row_to_dict(state_row) if state_row else {},
        "tracked_wallets": row_to_dict(tw_row).get("count", 0) if tw_row else 0,
    }


@app.get("/stats/accounts", response_model=TopAccountsResponse)
def top_accounts(limit: int = Query(default=20, ge=1, le=100)):
    """Most active source accounts."""
    ph = _ph()
    with get_cursor() as cur:
        cur.execute(
            f"SELECT account, COUNT(*) as count FROM transactions "
            f"WHERE account IS NOT NULL GROUP BY account ORDER BY count DESC LIMIT {ph}",
            (limit,),
        )
        rows = rows_to_list(cur.fetchall())
    return {"data": rows}


# ---------------------------------------------------------------------------
# Account state endpoints (hub-and-spoke)
# ---------------------------------------------------------------------------

@app.get("/accounts/{address}/info", response_model=AccountState)
def account_info(address: str):
    """Current account state: balance, sequence, flags."""
    ph = _ph()
    with get_cursor() as cur:
        cur.execute(f"SELECT * FROM account_states WHERE address = {ph}", (address,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Account not found or not tracked")
    return row_to_dict(row)


@app.get("/accounts/{address}/balances", response_model=BalancesResponse)
def account_balances(
    address: str,
    include_xrp: bool = Query(default=False, description="Include XRP balance as a row"),
    include_zero: bool = Query(default=False, description="Include trust lines with zero balance"),
):
    """
    Current trust-line balances for a tracked account.
    Excludes trust lines that have been removed (DeletedNode in metadata).
    """
    ph = _ph()
    with get_cursor() as cur:
        query = (
            f"SELECT issuer, currency, balance, limit_amount, limit_peer, "
            f"authorized, peer_authorized, no_ripple, no_ripple_peer, freeze_flag, peer_freeze_flag, is_deleted "
            f"FROM trustlines WHERE account = {ph} AND is_deleted = FALSE"
        )
        params: list[Any] = [address]
        if not include_zero:
            query += f" AND balance != {ph} AND balance != {ph}"
            params += ["0", "0.0"]
        cur.execute(query, params)
        trustlines = rows_to_list(cur.fetchall())

        xrp_row = None
        if include_xrp:
            cur.execute(f"SELECT balance_drops FROM account_states WHERE address = {ph}", (address,))
            r = cur.fetchone()
            if r:
                drops = row_to_dict(r).get("balance_drops")
                xrp_row = {"currency": "XRP", "issuer": None, "balance": str(drops) if drops is not None else None}

    result: list[dict] = []
    if xrp_row:
        result.append(xrp_row)
    result.extend(trustlines)
    return {"address": address, "balances": result}


@app.get("/accounts/{address}/offers", response_model=OffersResponse)
def account_offers(address: str):
    """All currently open offers placed by a tracked account."""
    ph = _ph()
    with get_cursor() as cur:
        cur.execute(
            f"SELECT sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value, "
            f"taker_pays_currency, taker_pays_issuer, taker_pays_value, "
            f"expiry_iso, flags, quality, ledger_index "
            f"FROM offers WHERE account = {ph} ORDER BY ledger_index ASC",
            (address,),
        )
        rows = rows_to_list(cur.fetchall())
    return {"address": address, "offers": rows}


# ---------------------------------------------------------------------------
# Token holders
# ---------------------------------------------------------------------------

@app.get("/tokens/{issuer}/{currency}/holders", response_model=HoldersResponse)
def token_holders(
    issuer: str,
    currency: str,
    exclude_addresses: Optional[str] = Query(default=None, description="Comma-separated addresses to exclude"),
):
    """All tracked accounts holding a balance of issuer/currency."""
    ph = _ph()
    exclude: list[str] = []
    if exclude_addresses:
        exclude = [a.strip() for a in exclude_addresses.split(",") if a.strip()]

    with get_cursor() as cur:
        query = (
            f"SELECT account, balance, limit_amount, authorized, peer_authorized, freeze_flag, no_ripple "
            f"FROM trustlines WHERE issuer = {ph} AND currency = {ph} AND is_deleted = FALSE "
            f"AND balance != {ph} AND balance != {ph}"
        )
        params: list[Any] = [issuer, currency, "0", "0.0"]
        if exclude:
            placeholders = ", ".join([ph] * len(exclude))
            query += f" AND account NOT IN ({placeholders})"
            params.extend(exclude)
        query += " ORDER BY CAST(balance AS FLOAT) DESC"
        cur.execute(query, params)
        rows = rows_to_list(cur.fetchall())

    return {"issuer": issuer, "currency": currency, "holder_count": len(rows), "holders": rows}


# ---------------------------------------------------------------------------
# Orderbook
# ---------------------------------------------------------------------------

@app.get("/orderbook", response_model=OrderbookResponse)
def orderbook(
    taker_gets_currency: str = Query(..., description="Currency the maker gives (e.g. USD)"),
    taker_gets_issuer: Optional[str] = Query(default=None, description="Issuer for taker_gets (omit for XRP)"),
    taker_pays_currency: str = Query(..., description="Currency the maker wants (e.g. XRP)"),
    taker_pays_issuer: Optional[str] = Query(default=None, description="Issuer for taker_pays (omit for XRP)"),
    limit: int = Query(default=50, ge=1, le=500),
):
    """Open offers matching a specific currency pair (from tracked wallets only)."""
    ph = _ph()
    conditions = [
        f"taker_gets_currency = {ph}",
        f"taker_pays_currency = {ph}",
    ]
    params: list[Any] = [taker_gets_currency, taker_pays_currency]

    if taker_gets_issuer:
        conditions.append(f"taker_gets_issuer = {ph}"); params.append(taker_gets_issuer)
    else:
        conditions.append("taker_gets_issuer IS NULL")

    if taker_pays_issuer:
        conditions.append(f"taker_pays_issuer = {ph}"); params.append(taker_pays_issuer)
    else:
        conditions.append("taker_pays_issuer IS NULL")

    where = "WHERE " + " AND ".join(conditions)

    with get_cursor() as cur:
        cur.execute(
            f"SELECT account, sequence, taker_gets_value, taker_pays_value, "
            f"expiry_iso, flags, quality, ledger_index "
            f"FROM offers {where} ORDER BY CAST(quality AS FLOAT) ASC LIMIT {ph}",
            params + [limit],
        )
        rows = rows_to_list(cur.fetchall())

    return {
        "taker_gets": {"currency": taker_gets_currency, "issuer": taker_gets_issuer},
        "taker_pays": {"currency": taker_pays_currency, "issuer": taker_pays_issuer},
        "offers": rows,
    }


# ---------------------------------------------------------------------------
# Trades (fill extraction from stored meta)
# ---------------------------------------------------------------------------

def _amount_to_info(amt: Any) -> Optional[dict]:
    """Normalise an XRPL amount (XRP string or IOU dict) to a plain dict."""
    if amt is None:
        return None
    if isinstance(amt, str):
        return {"currency": "XRP", "issuer": None, "value": amt}
    return {"currency": amt.get("currency"), "issuer": amt.get("issuer"), "value": amt.get("value")}


def _subtract_amounts(prev: Any, final: Any) -> Any:
    """
    Compute the actual fill size: prev_amount - final_amount.
    Works for both XRP drop strings and IOU dicts.
    """
    if isinstance(prev, str) and isinstance(final, str):
        try:
            return str(int(prev) - int(final))
        except (ValueError, TypeError):
            return "0"
    if isinstance(prev, dict) and isinstance(final, dict):
        try:
            delta = float(prev.get("value", 0)) - float(final.get("value", 0))
        except (ValueError, TypeError):
            delta = 0.0
        return {
            "currency": prev.get("currency") or final.get("currency"),
            "issuer": prev.get("issuer") or final.get("issuer"),
            "value": str(delta),
        }
    return None


def _extract_fills(tx_data_raw: Any, tx_hash: str, ledger_index: int, close_time_iso: Optional[str]) -> list[dict]:
    """
    Parse trade fills from a transaction's AffectedNodes.

    Fill sizes are reported as actual traded deltas:
    - DeletedNode (full fill): FinalFields amounts are what was wholly consumed.
    - ModifiedNode (partial fill): PreviousFields − FinalFields = the traded portion.
    """
    if isinstance(tx_data_raw, str):
        try:
            tx_data_raw = json.loads(tx_data_raw)
        except (ValueError, TypeError):
            return []
    if not isinstance(tx_data_raw, dict):
        return []

    full = tx_data_raw.get("_full_data") if isinstance(tx_data_raw.get("_full_data"), dict) else tx_data_raw
    meta = full.get("meta") if isinstance(full, dict) and isinstance(full.get("meta"), dict) else {}
    tx_json = full.get("tx_json") if isinstance(full, dict) else {}
    if not isinstance(tx_json, dict):
        tx_json = {}

    taker_account = tx_json.get("Account")
    fills: list[dict] = []

    for node_wrapper in meta.get("AffectedNodes", []):
        offer_account: Optional[str] = None
        filled_gets: Any = None
        filled_pays: Any = None
        is_deleted = False

        if "DeletedNode" in node_wrapper:
            node = node_wrapper["DeletedNode"]
            if node.get("LedgerEntryType") != "Offer":
                continue
            fields = node.get("FinalFields") or {}
            offer_account = fields.get("Account")
            # Full fill: the entire remaining amount was consumed
            filled_gets = fields.get("TakerGets")
            filled_pays = fields.get("TakerPays")
            is_deleted = True

        elif "ModifiedNode" in node_wrapper:
            node = node_wrapper["ModifiedNode"]
            if node.get("LedgerEntryType") != "Offer":
                continue
            prev  = node.get("PreviousFields") or {}
            final = node.get("FinalFields")    or {}
            # Only include if amounts actually changed (amounts are in PreviousFields)
            if "TakerGets" not in prev and "TakerPays" not in prev:
                continue
            offer_account = final.get("Account")
            # Partial fill: delta = what was there before minus what remains
            if "TakerGets" in prev:
                filled_gets = _subtract_amounts(prev["TakerGets"], final.get("TakerGets", prev["TakerGets"]))
            if "TakerPays" in prev:
                filled_pays = _subtract_amounts(prev["TakerPays"], final.get("TakerPays", prev["TakerPays"]))
        else:
            continue

        fills.append({
            "tx_hash": tx_hash,
            "ledger_index": ledger_index,
            "close_time_iso": close_time_iso,
            "maker_account": offer_account,
            "taker_account": taker_account,
            "filled_taker_gets": _amount_to_info(filled_gets),
            "filled_taker_pays": _amount_to_info(filled_pays),
            "fully_consumed": is_deleted,
        })

    return fills


@app.get("/trades", response_model=TradesResponse)
def trades(
    issuer: Optional[str] = Query(default=None, description="Filter by currency issuer"),
    currency: Optional[str] = Query(default=None, description="Filter by currency code"),
    account: Optional[str] = Query(default=None, description="Filter by maker or taker account"),
    from_ledger: Optional[int] = Query(default=None),
    to_ledger: Optional[int] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    order: str = Query(default="desc", pattern="^(asc|desc)$"),
):
    """
    Extract trade fills from stored transaction metadata.
    Returns fills from OfferCreate and Payment transactions.
    """
    ph = _ph()
    conditions = ["transaction_type IN ('OfferCreate', 'Payment')"]
    params: list[Any] = []

    if account:
        conditions.append(f"(account = {ph} OR destination = {ph})")
        params += [account, account]
    if from_ledger is not None:
        conditions.append(f"ledger_index >= {ph}"); params.append(from_ledger)
    if to_ledger is not None:
        conditions.append(f"ledger_index <= {ph}"); params.append(to_ledger)

    where = "WHERE " + " AND ".join(conditions)
    direction = "DESC" if order == "desc" else "ASC"

    with get_cursor() as cur:
        cur.execute(
            f"SELECT transaction_hash, ledger_index, transaction_data "
            f"FROM transactions {where} ORDER BY ledger_index {direction} LIMIT {ph}",
            params + [limit * 5],  # fetch more rows since not every tx produces fills
        )
        rows = cur.fetchall()

    all_fills: list[dict] = []
    for row in rows:
        r = row_to_dict(row)
        raw = r.get("transaction_data")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, TypeError):
                raw = {}
        full = raw.get("_full_data") if isinstance(raw, dict) and isinstance(raw.get("_full_data"), dict) else raw or {}
        close_time_iso = full.get("close_time_iso") if isinstance(full, dict) else None
        fills = _extract_fills(r.get("transaction_data"), r["transaction_hash"], r["ledger_index"], close_time_iso)
        # Filter by issuer/currency if requested
        for fill in fills:
            if issuer or currency:
                tg = fill.get("filled_taker_gets") or {}
                tp = fill.get("filled_taker_pays") or {}
                match = (
                    (not issuer or tg.get("issuer") == issuer or tp.get("issuer") == issuer)
                    and (not currency or tg.get("currency") == currency or tp.get("currency") == currency)
                )
                if not match:
                    continue
            all_fills.append(fill)
        if len(all_fills) >= limit:
            break

    return {"count": len(all_fills[:limit]), "data": all_fills[:limit]}


# ---------------------------------------------------------------------------
# Ledger resolution
# ---------------------------------------------------------------------------

@app.get("/ledgers/resolve", response_model=LedgerResolveResponse)
def resolve_ledger(
    timestamp: str = Query(..., description="ISO-8601 timestamp, e.g. 2024-01-15T12:00:00Z"),
):
    """
    Return the ledger_index whose close time is closest to the given timestamp.

    Uses the ledger_metadata table which is written for every processed ledger,
    regardless of whether any transactions from that ledger were stored.
    This means the result is never affected by transaction filters.
    """
    is_pg = Config.DATABASE_TYPE == "postgresql"
    ph = _ph()

    with get_cursor() as cur:
        if is_pg:
            cur.execute(
                f"SELECT ledger_index, close_time_iso "
                f"FROM ledger_metadata "
                f"ORDER BY ABS(EXTRACT(EPOCH FROM "
                f"  (close_time_iso::timestamptz - {ph}::timestamptz))) "
                f"LIMIT 1",
                (timestamp,),
            )
        else:
            cur.execute(
                "SELECT ledger_index, close_time_iso "
                "FROM ledger_metadata "
                "ORDER BY ABS(strftime('%s', close_time_iso) - strftime('%s', ?)) "
                "LIMIT 1",
                (timestamp,),
            )
        row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="No ledger metadata found. The indexer must process at least one ledger first.",
        )

    r = row_to_dict(row)
    return {
        "requested_timestamp": timestamp,
        "ledger_index": r.get("ledger_index"),
        "ledger_close_time": r.get("close_time_iso"),
    }


# ---------------------------------------------------------------------------
# Tracked wallets
# ---------------------------------------------------------------------------

@app.get("/wallets", response_model=WalletListResponse)
def list_tracked_wallets(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
):
    """List all wallets being tracked by the hub-and-spoke system."""
    ph = _ph()
    offset = (page - 1) * limit
    with get_cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM tracked_wallets")
        total = row_to_dict(cur.fetchone()).get("count", 0)
        cur.execute(
            f"SELECT address, activation_tx_hash, activated_at FROM tracked_wallets "
            f"ORDER BY activated_at DESC LIMIT {ph} OFFSET {ph}",
            (limit, offset),
        )
        rows = rows_to_list(cur.fetchall())
    return {"total": total, "page": page, "limit": limit, "data": rows}


# ---------------------------------------------------------------------------
# Sync (existing)
# ---------------------------------------------------------------------------

def _encode_cursor(row_id: int) -> str:
    return base64.urlsafe_b64encode(str(row_id).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid cursor value")


def _extract_tx_fields(row: dict, include_full: bool) -> dict:
    raw: Any = row.get("transaction_data")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            raw = {}
    if not isinstance(raw, dict):
        raw = {}

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


@app.get("/sync/transactions", response_model=SyncResponse)
def sync_transactions(
    after_ledger: Optional[int] = Query(default=None, description="Return transactions with ledger_index > this value"),
    cursor: Optional[str] = Query(default=None, description="Opaque cursor from a previous response"),
    limit: int = Query(default=100, ge=1, le=1000),
    include_full: bool = Query(default=False, description="Include tx_json, meta, and close_time_iso"),
):
    """
    Stream transactions in ascending ledger order for syncing.
    Use next_cursor for pagination. has_more=false means you're caught up.
    """
    ph = _ph()
    conditions: list[str] = []
    params: list[Any] = []

    if cursor:
        conditions.append(f"id > {ph}"); params.append(_decode_cursor(cursor))
    elif after_ledger is not None:
        conditions.append(f"ledger_index > {ph}"); params.append(after_ledger)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_cursor() as cur:
        cur.execute(
            f"SELECT id, ledger_index, transaction_hash, transaction_type, "
            f"account, destination, fee, source_tag, destination_tag, created_at, transaction_data "
            f"FROM transactions {where} ORDER BY ledger_index ASC, id ASC LIMIT {ph}",
            params + [limit],
        )
        rows = rows_to_list(cur.fetchall())

    data = [_extract_tx_fields(r, include_full) for r in rows]
    has_more = len(rows) == limit
    next_cursor = _encode_cursor(rows[-1]["id"]) if has_more and rows else None

    return {"has_more": has_more, "next_cursor": next_cursor, "count": len(data), "data": data}
