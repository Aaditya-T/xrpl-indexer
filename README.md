# XRPL Indexer

A Python-based XRPL blockchain indexer with a FastAPI read API. Monitors ledgers, stores transactions, and maintains live wallet state (balances, trust lines, open offers) for a hub-and-spoke wallet network.

---

## Features

- **Scheduled monitoring** — processes new ledgers on a configurable cron interval
- **Flexible transaction filtering** — filter by transaction type, address, or source tag
- **Wallet state tracking** — auto-discovers wallets activated by configured funding wallets or source-tagged payments, then maintains their account state, trust lines, and open offers
- **Ledger metadata** — records the close time of every processed ledger for timestamp-based lookups
- **Dual database support** — PostgreSQL (production) and SQLite (testing/local)
- **FastAPI read API** — documented endpoints with OpenAPI/Swagger schema docs at `/docs`
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

# ── Wallet discovery ──────────────────────────────────────────────────────────
# The existing single funding wallet setting remains supported.
CENTRAL_WALLET_ADDRESS=rYourHubWalletAddress
# Optional additional funding wallets, comma-separated.
PARENT_WALLET_ADDRESSES=rParent2,rParent3
# Optional SourceTags whose payments can activate and enroll new wallets.
TRACK_SOURCE_TAGS=123,456

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

Discovery, state updates, and transaction storage run on every processed ledger:

| System | Controlled by | What it does |
|---|---|---|
| **Transaction storage** | `FILTER_*` env vars, plus `TRACK_SOURCE_TAGS` | Writes transactions matching all configured `FILTER_*` rules, or any transaction with a configured tracking SourceTag |
| **Wallet discovery** | `CENTRAL_WALLET_ADDRESS`, `PARENT_WALLET_ADDRESSES`, `TRACK_SOURCE_TAGS` | Enrolls the destination of a Payment from any configured parent **or** carrying a configured SourceTag, only when the Payment creates that account |
| **State tracking** | Tracked wallets | Updates `account_states`, `trustlines`, `offers` for enrolled wallets on **every** transaction regardless of storage filters |

`FILTER_SOURCE_TAGS` still only filters ordinary transaction storage; it does not enroll wallets. To discover externally funded wallets, set `TRACK_SOURCE_TAGS`. A matching tagged transaction is stored even when an address or type filter would otherwise exclude it. Parent-funded activations still use the existing `FILTER_*` storage rules unless they also carry a tracking tag.

Discovery requires an `AccountRoot` creation for the Payment destination. A payment to an already active wallet does not enroll it. On restart, discovery also checks qualifying payments already in the `transactions` table; it cannot discover payments from ledgers that were never processed or stored. On a fresh database, indexing begins at the current ledger and proceeds forward.

**Ownership boundary:** An XRPL `SourceTag` is optional, sender-supplied transaction metadata. Anyone who knows a configured tag can use it on a payment, so tag-based discovery alone does not prove that a new wallet belongs to your platform. Use this mode only when that broader enrollment rule is acceptable, or verify ownership separately before treating an enrolled wallet as a platform wallet. See [XRPL source and destination tags](https://xrpl.org/docs/concepts/transactions/source-and-destination-tags).

### Deep-freeze migration and backfill

On startup, the database schema gains nullable `deep_freeze_flag` and
`peer_deep_freeze_flag` columns. New trustline updates populate both. Existing
rows stay `NULL` until backfilled, so an unknown historical value is never
reported as `false`.

Run the idempotent backfill against an XRPL node that retains the ledger in the
indexer's `last_processed_ledger_index`:

```bash
venv/bin/python -m ops.backfill_deep_freeze --rpc-url https://YOUR-ARCHIVAL-RPC/
```

The script reads each raw `RippleState` at that exact ledger and checks the
returned ledger index and accounts before updating the row. It also inspects
stored transaction metadata for comparison. Stored transactions alone cannot
prove complete coverage because historical `FILTER_*` settings may have
excluded later changes, even within the same ledger. If the node lacks the
required ledger, the command stops and leaves remaining fields `NULL`; rerun
with a node that has that history. Run this after deployment and check that
the command reports zero unknown rows. It does not rewrite balances, limits,
or the trustline's last-change ledger.

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
  "updated_at": "2026-05-14T12:00:03",
  "snapshot_ledger_index": 95123456,
  "indexed_at": "2026-05-14T12:00:03"
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
  "snapshot_ledger_index": 95123456,
  "indexed_at": "2026-05-14T12:00:03",
  "balances": [
    { "currency": "USD", "issuer": "rIssuer...", "balance": "100.5",
      "deep_freeze_flag": false, "peer_deep_freeze_flag": true, "freeze_flag": false }
  ]
}
```

`deep_freeze_flag` belongs to the requested account; `peer_deep_freeze_flag`
belongs to the other account on that line, matching the existing freeze fields.
Use `include_zero=true` to retrieve **all** active trust lines, including lines
with zero balance. A `null` deep-freeze value means a legacy row still needs
backfilling; it does not mean `false`.

#### `GET /accounts/{address}/snapshot`
Returns the tracked account's flags and **all** its active trust lines (including
zero balances) in one response. The top-level `snapshot_ledger_index` and
`indexed_at` identify the committed indexer state read by the response.

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
Tracked accounts with a trust line for a token, sorted by balance descending.
Zero-balance lines may appear, but this endpoint returns only selected fields;
use `/accounts/{address}/balances?include_zero=true` or a snapshot endpoint for
complete trustline data.

| Query param | Description |
|---|---|
| `exclude_addresses` | Comma-separated addresses to exclude |

```json
{
  "issuer": "rIssuer...",
  "currency": "USD",
  "snapshot_ledger_index": 95123456,
  "indexed_at": "2026-05-14T12:00:03",
  "holder_count": 12,
  "holders": [{ "account": "rABC...", "balance": "500.0", ... }]
}
```

#### `GET /issuers/{issuer}/snapshot`
Returns a **tracked** issuer's AccountRoot flags and every tracked peer's active
trustline to that issuer, including zero balances, from the same database read
snapshot. Returns 404 when the issuer account state is not tracked.

The indexer publishes each ledger's state changes and its watermark in one
database transaction. Compare both response-level markers when correlating
separate calls; use a snapshot endpoint when account flags and trustlines must
come from one response. `/status` alone does not identify the state read by a
different request.

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
| `issuer` | VARCHAR | Other account on the trust line (usually token issuer) |
| `currency` | VARCHAR | Currency code |
| `balance` | TEXT | Current balance |
| `limit_amount` | TEXT | Trust limit set by account |
| `limit_peer` | TEXT | Trust limit set by issuer |
| `authorized` | BOOLEAN | Whether account is authorized |
| `peer_authorized` | BOOLEAN | Whether issuer is authorized |
| `no_ripple` | BOOLEAN | No-ripple flag |
| `no_ripple_peer` | BOOLEAN | Peer no-ripple flag |
| `freeze_flag` | BOOLEAN | This account's freeze flag |
| `peer_freeze_flag` | BOOLEAN | Other account's freeze flag |
| `deep_freeze_flag` | BOOLEAN, nullable | This account's deep-freeze flag; NULL until a legacy row is backfilled |
| `peer_deep_freeze_flag` | BOOLEAN, nullable | Other account's deep-freeze flag; NULL until backfilled |
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

# Start the indexer, API, and host/domain monitor. The PM2 config includes
# automatic restarts, exponential backoff, and per-process memory limits.
sudo pm2 start ecosystem.config.cjs
sudo pm2 save
sudo pm2 startup

# Deploy updates
git pull
sudo pm2 startOrReload ecosystem.config.cjs --update-env
sudo pm2 save

# View logs
sudo pm2 logs xrpl-api
sudo pm2 logs xrpl-indexer-v1
sudo pm2 logs xrpl-monitor
```

New log lines have ISO-8601 timestamps. Indexer logs include structured
`indexing_cycle_started`, `indexing_cycle_finished`, `indexing_cycle_error`, and
`ledger_processed` events with durations, PID, and resident/peak memory. To keep
PM2 logs bounded, enable rotation once on the server:

```bash
sudo pm2 install pm2-logrotate
sudo pm2 set pm2-logrotate:max_size 20M
sudo pm2 set pm2-logrotate:retain 14
sudo pm2 set pm2-logrotate:compress true
sudo pm2 set pm2-logrotate:dateFormat 'YYYY-MM-DD_HH-mm-ss'
```

Inspect recent timestamped history without following the log indefinitely:

```bash
sudo pm2 logs xrpl-indexer-v1 --lines 200 --nostream
sudo pm2 logs xrpl-api --lines 200 --nostream
sudo pm2 logs xrpl-monitor --lines 200 --nostream
```

### EC2 monitoring and crash diagnosis

Set `MONITOR_PUBLIC_HEALTH_URL=https://your-domain.example/health` in `.env` so
the monitor checks both localhost and the public DNS/TLS/reverse-proxy path. Set
`MONITOR_WEBHOOK_URL` to receive an alert when memory, disk, or either endpoint
fails, and a recovery message when it returns. The monitor emits one JSON metric
record per minute to the `xrpl-monitor` PM2 log. It also alerts when the ledger
index exposed by `/status` has not advanced for 15 minutes (configurable with
`MONITOR_INDEXER_STALE_SECONDS`). Slack receives a warning on the transition from
healthy to warning and a recovery message when all checks clear, rather than a
message on every poll. It does not report individual transactions, SourceTags,
or PostgreSQL backup failures. A monitor on the same host cannot report a total
EC2 outage; use an external EC2 status-check alarm for that.

To collect the evidence that distinguishes OOM, disk/inode exhaustion, process
failure, and public routing failure, run this on the instance (pass the public
base URL without a trailing slash):

```bash
chmod +x ops/diagnose-ec2.sh
./ops/diagnose-ec2.sh https://your-domain.example
```

The previous-boot kernel section is especially important after an instance
restart: `Killed process` or `oom-kill` confirms RAM exhaustion. A full root
filesystem or zero free inodes confirms storage exhaustion. If localhost is
healthy while the public check fails, investigate DNS, TLS, Nginx/load balancer,
and the EC2 security group instead of the Python process.

For persistent AWS metrics and logs, attach an instance role with
`CloudWatchAgentServerPolicy`, install the Amazon CloudWatch Agent, then load the
checked-in config:

```bash
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 \
  -c file:ops/amazon-cloudwatch-agent.json -s
```

Create CloudWatch alarms for `mem_used_percent > 85`, `disk_used_percent > 85`,
and EC2 `StatusCheckFailed > 0`. Host-local monitoring cannot alert while the
whole instance is unreachable, so the EC2 status-check alarm is essential.

On a roughly 1 GiB instance, leave parallel processing disabled unless a load
test proves there is enough headroom. A small swap file can prevent abrupt OOM
kills during brief spikes, but it is a safety net rather than a substitute for
memory alarms or a larger instance.

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
