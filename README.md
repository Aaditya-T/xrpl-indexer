# XRPL Indexer

A Python-based XRPL (XRP Ledger) indexer application that efficiently monitors and processes blockchain data using scheduled intervals.

## Features

- **Scheduled Monitoring**: Automatically checks for new ledgers every X minutes using cron jobs
- **Efficient Batch Processing**: Fetches all transactions between last processed and current ledger index
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
