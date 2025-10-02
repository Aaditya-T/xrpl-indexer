# XRPL Indexer

A Python-based XRPL (XRP Ledger) indexer application that efficiently monitors and processes blockchain data using scheduled intervals.

## Features

- **Scheduled Monitoring**: Automatically checks for new ledgers every X minutes using cron jobs
- **Efficient Batch Processing**: Fetches all transactions between last processed and current ledger index
- **Optional Parallel Processing**: 3-5x faster synchronization for catch-up scenarios (disabled by default)
- **Local Database Storage**: Supports both PostgreSQL and SQLite
- **Flexible Filtering**: Filter transactions by type, address, or source tag
- **State Persistence**: Tracks last processed ledger index and resumes from correct position
- **Autonomous Operation**: Runs continuously with automatic error handling and recovery

## How It Works

1. **Initial Run**: On first execution, the indexer stores the current ledger index and waits for the next cycle
2. **Subsequent Runs**: Every X minutes, the indexer:
   - Fetches the current ledger index from XRPL
   - Compares with the last processed index
   - Processes all intermediate ledgers (e.g., if last was 100 and current is 110, processes 101-110)
   - Applies filters to transactions
   - Stores matching transactions in the database
   - Updates the last processed ledger index

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
# XRPL Network
XRPL_JSON_RPC_URL=https://s1.ripple.com:51234/

# Database (postgresql or sqlite)
DATABASE_TYPE=postgresql

# Schedule interval in minutes
CRON_INTERVAL_MINUTES=5

# Parallel Processing (Optional - DISABLED by default)
# WARNING: Only enable if you have no rate limits and need to sync faster
ENABLE_PARALLEL_PROCESSING=false  # Set to "true" to enable
PARALLEL_WORKERS=5                # Number of concurrent workers

# Optional Filters (comma-separated)
FILTER_TRANSACTION_TYPES=Payment,NFTokenMint
FILTER_ADDRESSES=rN7n7otQDd6FczFgLdlqtyMVrn3eBsePke
FILTER_SOURCE_TAGS=123,456
```

## Database Schema

### indexer_state
- `id`: Primary key
- `last_processed_ledger_index`: Last successfully processed ledger
- `updated_at`: Timestamp of last update

### transactions
- `id`: Primary key
- `ledger_index`: Ledger number containing this transaction
- `transaction_hash`: Unique transaction identifier
- `transaction_type`: Type of transaction (Payment, NFTokenMint, etc.)
- `account`: Source account address
- `destination`: Destination account address (if applicable)
- `amount`: Transaction amount (JSON format)
- `fee`: Transaction fee
- `source_tag`: Source tag (if present)
- `destination_tag`: Destination tag (if present)
- `transaction_data`: Full transaction data (JSONB/JSON)
- `created_at`: Timestamp when stored

## Running the Indexer

The indexer runs automatically via the configured workflow. It will:
- Run immediately on start
- Continue running every X minutes as configured
- Log all activity to the console
- Automatically handle errors and continue operation

## Filter Options

### Transaction Types
Filter by specific transaction types:
```
FILTER_TRANSACTION_TYPES=Payment,NFTokenMint,OfferCreate,TrustSet
```

### Addresses
Filter transactions involving specific addresses (as account or destination):
```
FILTER_ADDRESSES=rN7n7otQDd6FczFgLdlqtyMVrn3eBsePke,rPEPPER7kfTD9w2To4CQk6UCfuHM9c6GDY
```

### Source Tags
Filter by specific source tags:
```
FILTER_SOURCE_TAGS=123,456,789
```

Leave any filter empty to process all transactions.

## Parallel Processing (Performance Optimization)

The indexer supports optional parallel processing for faster synchronization when catching up on large backlogs.

### Default Mode: Sequential Processing
- Processes ledgers one at a time
- Includes 0.1s delay between ledgers to respect API rate limits
- **Best for**: Normal operations with public XRPL endpoints

### Parallel Mode: Concurrent Processing
- Processes multiple ledgers simultaneously (5 workers by default)
- **3-5x faster** for large backlogs
- No artificial delays between ledgers
- **Best for**: Private nodes with no rate limits, or catch-up scenarios

### When to Enable Parallel Processing

✅ **Enable when:**
- You have no XRPL API rate limits (private node)
- There's a significant backlog (hundreds of ledgers behind)
- You need faster synchronization for catch-up

❌ **Keep disabled when:**
- Using public XRPL endpoints (rate limits apply)
- Processing regular small batches
- Normal scheduled operations

### Configuration

```bash
# Enable parallel processing
ENABLE_PARALLEL_PROCESSING=true

# Adjust number of concurrent workers (default: 5)
PARALLEL_WORKERS=10  # Higher = faster, but more API calls
```

### Error Handling

Both sequential and parallel modes handle errors identically:
- Any failed ledger halts the entire cycle
- Last processed ledger index is preserved
- No silent failures or skipped ledgers
- Cycle retries on next scheduled run

## Database Queries

### View Recent Transactions
```sql
SELECT * FROM transactions ORDER BY ledger_index DESC LIMIT 10;
```

### Count by Transaction Type
```sql
SELECT transaction_type, COUNT(*) as count 
FROM transactions 
GROUP BY transaction_type 
ORDER BY count DESC;
```

### Check Indexer Status
```sql
SELECT * FROM indexer_state ORDER BY id DESC LIMIT 1;
```

## Technical Stack

- **XRPL Integration**: xrpl-py library for JSON RPC communication
- **Database**: PostgreSQL (via psycopg2) or SQLite
- **Scheduling**: APScheduler with cron triggers
- **Configuration**: python-dotenv for environment management
