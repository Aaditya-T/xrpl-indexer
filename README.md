# XRPL Indexer

A Python-based XRPL blockchain indexer with a FastAPI read API. Monitors ledgers, stores transactions, and maintains live wallet state (balances, trust lines, open offers) for a hub-and-spoke wallet network.

---

## Features

- **Scheduled monitoring** — processes new ledgers on a configurable cron interval
- **Flexible transaction filtering** — filter by transaction type, address, or source tag
- **Hub-and-spoke state tracking** — auto-discovers wallets funded by a central wallet and maintains their account state, trust lines, and open offers in real time
- **Ledger metadata** — records the close time of every processed ledger for timestamp-based lookups
- **Dual database support** — PostgreSQL (production) and SQLite (testing/local)
- **FastAPI read API** — 15 endpoints with full OpenAPI/Swagger schema docs at `/docs`
- **Parallel processing** — optional concurrent ledger fetching for backlog catch-up

---

## Configuration

Copy `.env.example` to `.env`:

```bash
# XRPL network endpoint
XRPL_JSON_RPC_URL=https://s1.ripple.com:51234/

# Database — "postgresql" or "sqlite"
DATABASE_TYPE=postgresql
DATABASE_URL=postgresql://user:pass@host:5432/dbname

# How often to check for new ledgers (minutes)
CRON_INTERVAL_MINUTES=5

# ── Hub-and-spoke state tracking ──────────────────────────────────────────────
# The central ("hub") wallet. Any wallet this address activates is auto-tracked.
CENTRAL_WALLET_ADDRESS=rYourHubWalletAddress

# ── Transaction storage filters (comma-separated, all optional) ───────────────
# Leave a filter empty to match everything.
FILTER_TRANSACTION_TYPES=Payment,OfferCreate
FILTER_ADDRESSES=rN7n7otQDd6FczFgLdlqtyMVrn3eBsePke
FILTER_SOURCE_TAGS=123,456

# ── Parallel processing (disabled by default) ─────────────────────────────────
ENABLE_PARALLEL_PROCESSING=false
PARALLEL_WORKERS=5
```

### How filters interact with state tracking

There are two independent systems running on every ledger:

| System | Controlled by | What it does |
|---|---|---|
| **Transaction storage** | `FILTER_*` env vars | Writes matching txns to the `transactions` table |
| **State tracking** | `CENTRAL_WALLET_ADDRESS` | Updates `account_states`, `trustlines`, `offers` for all tracked wallets — runs on **every** transaction regardless of filters |

So with `FILTER_SOURCE_TAGS=608402356` and `CENTRAL_WALLET_ADDRESS=rHub...`, only source-tagged transactions are stored, but account state is maintained for the hub and all its child wallets across the entire ledger.

---

## Database Schema

### `indexer_state` (single row)
| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Always 1 — enforced by CHECK constraint |
| `last_processed_ledger_index` | BIGINT | Resume point on restart |
| `updated_at` | TIMESTAMP | Last update time |

### `transactions`
| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Primary key |
| `ledger_index` | BIGINT | Ledger containing this transaction |
| `transaction_hash` | VARCHAR | Unique hash |
| `transaction_type` | VARCHAR | Payment, OfferCreate, TrustSet, etc. |
| `account` | VARCHAR | Source account |
| `destination` | VARCHAR | Destination account (if any) |
| `amount` | TEXT | Amount (drops for XRP, JSON for IOU) |
| `fee` | VARCHAR | Transaction fee in drops |
| `source_tag` | BIGINT | Source tag (if present) |
| `destination_tag` | BIGINT | Destination tag (if present) |
| `transaction_data` | JSONB/JSON | Full raw transaction + metadata |
| `created_at` | TIMESTAMP | When stored |

### `ledger_metadata`
| Column | Type | Description |
|---|---|---|
| `ledger_index` | BIGINT | Primary key |
| `close_time_iso` | TEXT | Ledger close time (ISO-8601) |
| `stored_at` | TIMESTAMP | When recorded |

Written for **every processed ledger** regardless of filters. Powers `/ledgers/resolve`.

### `tracked_wallets`
| Column | Type | Description |
|---|---|---|
| `address` | VARCHAR | Wallet address |
| `activation_tx_hash` | VARCHAR | Hash of the funding transaction |
| `activated_at` | TIMESTAMP | When discovered |

### `account_states`
| Column | Type | Description |
|---|---|---|
| `address` | VARCHAR | Primary key |
| `balance_drops` | BIGINT | XRP balance in drops |
| `sequence` | BIGINT | Account sequence number |
| `owner_count` | INT | Number of owned objects |
| `flags` | BIGINT | Account flags bitmask |
| `ledger_index` | BIGINT | Ledger of last update |
| `updated_at` | TIMESTAMP | When last updated |

### `trustlines`
| Column | Type | Description |
|---|---|---|
| `account` | VARCHAR | Account holding the trust line |
| `issuer` | VARCHAR | Token issuer |
| `currency` | VARCHAR | Currency code |
| `balance` | TEXT | Current balance |
| `limit_amount` | TEXT | Trust limit set by account |
| `limit_peer` | TEXT | Trust limit set by issuer |
| `authorized` | BOOLEAN | Whether account is authorized |
| `peer_authorized` | BOOLEAN | Whether issuer is authorized |
| `no_ripple` | BOOLEAN | No-ripple flag |
| `no_ripple_peer` | BOOLEAN | Peer no-ripple flag |
| `freeze_flag` | BOOLEAN | Freeze flag |
| `peer_freeze_flag` | BOOLEAN | Peer freeze flag |
| `is_deleted` | BOOLEAN | True if removed via DeletedNode |

### `offers`
| Column | Type | Description |
|---|---|---|
| `account` | VARCHAR | Offer owner |
| `sequence` | BIGINT | Offer sequence (unique per account) |
| `taker_gets_currency` | VARCHAR | Currency the maker gives |
| `taker_gets_issuer` | VARCHAR | Issuer for taker_gets (NULL = XRP) |
| `taker_gets_value` | TEXT | Amount the maker gives |
| `taker_pays_currency` | VARCHAR | Currency the maker wants |
| `taker_pays_issuer` | VARCHAR | Issuer for taker_pays (NULL = XRP) |
| `taker_pays_value` | TEXT | Amount the maker wants |
| `quality` | TEXT | Exchange rate (taker_pays / taker_gets) |
| `flags` | BIGINT | Offer flags |
| `expiry_iso` | TEXT | Expiry time (ISO-8601, if set) |
| `ledger_index` | BIGINT | Ledger of last update |

---

## API Endpoints

Base URL: `http://your-server:8000` — interactive docs at `/docs`

### Health & Status

#### `GET /health`
Liveness check. Returns `{"status": "ok"}` or 503 if the database is unreachable.

#### `GET /status`
```json
{
  "last_processed_ledger_index": 95123456,
  "updated_at": "2026-05-14T12:00:00",
  "tracked_wallets": 42
}
```

### Transactions

#### `GET /transactions`
Paginated transaction list, newest first.

| Query param | Type | Description |
|---|---|---|
| `page` | int | Page number (default 1) |
| `limit` | int | Results per page (default 50, max 500) |
| `transaction_type` | string | Filter by type |
| `account` | string | Filter by source account |
| `destination` | string | Filter by destination |
| `source_tag` | int | Filter by source tag |
| `destination_tag` | int | Filter by destination tag |
| `ledger_min` / `ledger_max` | int | Ledger range |
| `from_date` / `to_date` | string | Date range (YYYY-MM-DD) |

```json
{
  "total": 1500,
  "page": 1,
  "limit": 50,
  "pages": 30,
  "data": [{ "ledger_index": 95123456, "transaction_hash": "...", ... }]
}
```

#### `GET /transactions/{tx_hash}`
Full transaction detail including raw `tx_json` and `meta`.

```json
{
  "status": "tesSUCCESS",
  "ledger_index": 95123456,
  "transaction_hash": "ABC123...",
  "transaction_type": "Payment",
  "close_time_iso": "2026-05-14T12:00:03Z",
  "tx_json": { ... },
  "meta": { ... }
}
```

### Stats

#### `GET /stats`
Aggregated overview: total transactions, breakdown by type, daily counts (last 30 days), ledger range, indexer state, tracked wallet count.

#### `GET /stats/accounts?limit=20`
Top source accounts by transaction count.

### Account State (hub-and-spoke)

#### `GET /accounts/{address}/info`
Current account state for a tracked wallet.

```json
{
  "address": "rABC...",
  "balance_drops": 25000000,
  "sequence": 12,
  "owner_count": 3,
  "flags": 0,
  "ledger_index": 95123456,
  "updated_at": "2026-05-14T12:00:03"
}
```

#### `GET /accounts/{address}/balances`
Token balances for a tracked wallet.

| Query param | Default | Description |
|---|---|---|
| `include_xrp` | false | Add XRP balance as first row |
| `include_zero` | false | Include zero-balance trust lines |

```json
{
  "address": "rABC...",
  "balances": [
    { "currency": "USD", "issuer": "rIssuer...", "balance": "100.5", ... }
  ]
}
```

#### `GET /accounts/{address}/offers`
All currently open offers for a tracked wallet.

```json
{
  "address": "rABC...",
  "offers": [
    {
      "sequence": 5,
      "taker_gets_currency": "USD", "taker_gets_issuer": "rIssuer...", "taker_gets_value": "100",
      "taker_pays_currency": "XRP", "taker_pays_issuer": null, "taker_pays_value": "50000000",
      "quality": "500000", "ledger_index": 95123456
    }
  ]
}
```

### Tokens

#### `GET /tokens/{issuer}/{currency}/holders`
All tracked accounts holding a non-zero balance of a token, sorted by balance descending.

| Query param | Description |
|---|---|
| `exclude_addresses` | Comma-separated addresses to exclude |

```json
{
  "issuer": "rIssuer...",
  "currency": "USD",
  "holder_count": 12,
  "holders": [{ "account": "rABC...", "balance": "500.0", ... }]
}
```

### Orderbook

#### `GET /orderbook`
Open offers from tracked wallets for a currency pair.

| Query param | Required | Description |
|---|---|---|
| `taker_gets_currency` | yes | Currency the maker gives (e.g. `USD`) |
| `taker_gets_issuer` | no | Issuer for taker_gets — omit for XRP |
| `taker_pays_currency` | yes | Currency the maker wants (e.g. `XRP`) |
| `taker_pays_issuer` | no | Issuer for taker_pays — omit for XRP |
| `limit` | no | Max results (default 50, max 500) |

```json
{
  "taker_gets": { "currency": "USD", "issuer": "rIssuer..." },
  "taker_pays": { "currency": "XRP", "issuer": null },
  "offers": [{ "account": "rABC...", "taker_gets_value": "100", "quality": "500000", ... }]
}
```

### Trades

#### `GET /trades`
Extract fill events from stored transaction metadata (OfferCreate and Payment types). Reports actual traded amounts — full fills from `DeletedNode`, partial fills as `PreviousFields − FinalFields`.

| Query param | Description |
|---|---|
| `issuer` | Filter by currency issuer |
| `currency` | Filter by currency code |
| `account` | Filter by maker or taker address |
| `from_ledger` / `to_ledger` | Ledger range |
| `limit` | Max results (default 50, max 500) |
| `order` | `asc` or `desc` (default `desc`) |

```json
{
  "count": 3,
  "data": [
    {
      "tx_hash": "ABC...",
      "ledger_index": 95123456,
      "maker_account": "rMaker...",
      "taker_account": "rTaker...",
      "filled_taker_gets": { "currency": "USD", "issuer": "rIssuer...", "value": "50.0" },
      "filled_taker_pays": { "currency": "XRP", "issuer": null, "value": "25000000" },
      "fully_consumed": true
    }
  ]
}
```

### Ledgers

#### `GET /ledgers/resolve?timestamp=2026-01-15T12:00:00Z`
Find the ledger closest to a given timestamp. Queries `ledger_metadata` which is written for every processed ledger — never affected by transaction filters.

```json
{
  "requested_timestamp": "2026-01-15T12:00:00Z",
  "ledger_index": 94100234,
  "ledger_close_time": "2026-01-15T12:00:02Z"
}
```

> Returns the **nearest** ledger (before or after). If you need the last ledger *before* a given time, filter your results client-side.

### Wallets

#### `GET /wallets`
All wallets tracked by the hub-and-spoke system, paginated.

```json
{
  "total": 42,
  "page": 1,
  "limit": 100,
  "data": [{ "address": "rABC...", "activation_tx_hash": "...", "activated_at": "..." }]
}
```

### Sync

#### `GET /sync/transactions`
Ordered stream of transactions for external sync clients. Use `next_cursor` to paginate forward efficiently.

| Query param | Description |
|---|---|
| `after_ledger` | Start after this ledger index |
| `cursor` | Opaque cursor from a previous response |
| `limit` | Max results (default 100, max 1000) |
| `include_full` | If true, includes `tx_json` and `meta` fields |

```json
{
  "has_more": true,
  "next_cursor": "MTIzNDU2",
  "count": 100,
  "data": [{ "id": 1, "ledger_index": 95000000, "transaction_hash": "...", ... }]
}
```

---

## Deployment (Ubuntu + PM2)

```bash
# Clone and set up
git clone https://github.com/your/xrpl-indexer.git
cd xrpl-indexer
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt   # or: uv sync

# Configure
cp .env.example .env
nano .env

# Start with PM2 (root PM2 instance — always prefix with sudo)
sudo pm2 start "python scheduler.py" --name xrpl-indexer-v1
sudo pm2 start "uvicorn api:app --host 0.0.0.0 --port 8000" --name xrpl-api
sudo pm2 save
sudo pm2 startup

# Deploy updates
git pull
sudo pm2 restart all

# View logs
sudo pm2 logs xrpl-api
sudo pm2 logs xrpl-indexer-v1
```

---

## Resetting the Indexer

Wipes all data and starts fresh from the current live ledger:

```bash
sudo pm2 stop xrpl-indexer-v1

psql $DATABASE_URL << 'SQL'
TRUNCATE TABLE transactions CASCADE;
TRUNCATE TABLE tracked_wallets CASCADE;
TRUNCATE TABLE account_states CASCADE;
TRUNCATE TABLE trustlines CASCADE;
TRUNCATE TABLE offers CASCADE;
TRUNCATE TABLE ledger_metadata CASCADE;
TRUNCATE TABLE indexer_state CASCADE;
SQL

sudo pm2 start xrpl-indexer-v1
```

With an empty `indexer_state`, the indexer automatically starts from the current validated ledger on next run.

---

## Running Tests

```bash
# Unit tests (~0.2s, no network)
python -m pytest tests/test_state_processor.py -v

# Integration tests against XRPL testnet (~40s, requires network)
pytest tests/test_integration_testnet.py -v -s
```

---

## Technical Stack

- **XRPL**: xrpl-py (JSON RPC)
- **Database**: PostgreSQL via psycopg2, SQLite for tests
- **Scheduling**: APScheduler with cron triggers
- **API**: FastAPI + Pydantic v2 + Uvicorn
- **Config**: python-dotenv
- **Package management**: uv (pyproject.toml)
