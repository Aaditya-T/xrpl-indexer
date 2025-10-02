# XRPL Indexer

## Overview

This is a Python-based XRP Ledger (XRPL) blockchain indexer that continuously monitors and processes ledger data at scheduled intervals. The application fetches transactions from the XRPL network, applies configurable filters, and stores relevant data in a local database. It operates autonomously with automatic state tracking and error recovery, making it suitable for blockchain data aggregation and analysis.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Core Design Pattern

**Event-Driven Batch Processing with Scheduled Polling**

The system uses a scheduled polling approach rather than real-time streaming. A cron-based scheduler triggers batch processing at configured intervals (default: every 5 minutes), fetching all ledgers between the last processed index and current ledger index. This design choice balances resource efficiency with data freshness.

**Rationale**: Batch processing reduces API calls and network overhead compared to real-time streaming, while still maintaining reasonable data latency for most indexing use cases.

### Application Architecture

**Modular Component Design**

The application is structured into distinct, loosely-coupled modules:

1. **Configuration Layer** (`config.py`) - Centralized environment-based configuration with helper methods for parsing filter lists
2. **Data Access Layer** (`database.py`) - Database abstraction supporting both PostgreSQL and SQLite with unified interface
3. **External Client Layer** (`xrpl_client.py`) - XRPL network interaction wrapper using official xrpl-py SDK
4. **Business Logic Layer** (`indexer.py`) - Core transaction processing and filtering logic
5. **Scheduling Layer** (`scheduler.py`) - APScheduler-based job orchestration with graceful shutdown handling
6. **Entry Point** (`main.py`) - Application initialization

**Rationale**: This separation of concerns enables independent testing, easier maintenance, and flexible deployment options.

### Data Storage Strategy

**Dual Database Support with Abstraction**

The system supports both PostgreSQL (production) and SQLite (development/testing) through a unified database interface. The `Database` class handles connection management and provides database-agnostic methods.

**Schema Design**:
- `indexer_state` table: Tracks last processed ledger index for resumption after restarts
- `transactions` table: Stores filtered transaction data with indexes on ledger_index and transaction_hash

**Rationale**: SQLite enables local development and testing without infrastructure dependencies, while PostgreSQL provides production-grade reliability and scalability. The abstraction layer prevents vendor lock-in.

### Transaction Filtering System

**Configurable Multi-Criteria Filtering**

Three independent filter types can be applied:
1. Transaction Type (e.g., Payment, NFTokenMint)
2. Account/Destination Addresses
3. Source Tags

Filters are applied in the `should_include_transaction()` method using AND logic (all specified filters must match).

**Rationale**: This flexibility allows the indexer to be reused across different use cases without code changes—simply configure environment variables to index different transaction subsets.

### State Management

**Persistent Checkpoint System**

The application maintains state through the `indexer_state` table, storing the last successfully processed ledger index. On startup, it resumes from this checkpoint rather than reprocessing historical data.

**First Run Behavior**: On initial execution, the system stores the current ledger index and begins processing from the next cycle, avoiding historical data backfill unless explicitly configured.

**Rationale**: This checkpoint approach prevents data loss during restarts and enables efficient incremental processing without reindexing the entire blockchain.

### Error Handling and Resilience

**Graceful Degradation with Automatic Recovery**

- Signal handlers (SIGINT, SIGTERM) enable graceful shutdown
- Ledger processing failures halt the cycle and preserve the last processed index (same behavior in both sequential and parallel modes)
- Database connection failures are propagated for manual intervention
- APScheduler handles job scheduling resilience automatically

**Rationale**: The system prioritizes availability—temporary XRPL network issues shouldn't require manual restarts. The scheduler will retry on the next interval.

### Parallel Processing (Optional Performance Optimization)

**Configurable Concurrent Ledger Processing**

The indexer supports optional parallel processing for faster synchronization when there's a large backlog:

- **Default Mode**: Sequential processing (DISABLED) with 0.1s delay between ledgers
- **Parallel Mode**: Concurrent processing using ThreadPoolExecutor with configurable worker count
- **Configuration**: Controlled via `ENABLE_PARALLEL_PROCESSING` and `PARALLEL_WORKERS` environment variables
- **Error Handling**: Failed ledgers halt the cycle in both modes, preventing index advancement on errors

**When to Enable Parallel Processing**:
- You have no XRPL API rate limits
- There's a significant backlog to process (hundreds of ledgers)
- You need faster synchronization for catch-up scenarios

**When to Keep it Disabled** (default):
- Normal operations with regular 5-minute intervals
- Using public XRPL endpoints with rate limits
- Processing small batches of ledgers

**Rationale**: Parallel processing provides 3-5x speed improvement for large backlogs but increases API call rate, so it's disabled by default to work safely with public endpoints. The implementation maintains identical error handling behavior to sequential mode—any failed ledger halts the cycle to preserve data integrity.

## External Dependencies

### Blockchain Network

**XRPL JSON-RPC API** (https://s1.ripple.com:51234/)
- Primary data source for ledger and transaction information
- Uses official `xrpl-py` Python SDK for network communication
- Configurable endpoint via `XRPL_JSON_RPC_URL` environment variable
- Public endpoints available; can be pointed to private nodes if needed

### Database Systems

**PostgreSQL** (Production)
- Relational database for persistent storage
- Connection via `psycopg2` driver with `RealDictCursor` for dict-like result access
- Configured via `DATABASE_URL` environment variable
- Requires external PostgreSQL server

**SQLite** (Development/Testing)
- File-based embedded database
- No external server required
- Default fallback with local file storage (`xrpl_indexer.db`)

### Job Scheduling

**APScheduler**
- Python library for cron-like job scheduling
- Uses `BlockingScheduler` for foreground execution
- Handles interval-based job triggering (configurable in minutes)
- Built-in job persistence and misfire handling

### Python Package Dependencies

- `xrpl-py`: Official XRP Ledger Python SDK for blockchain interaction
- `psycopg2`: PostgreSQL database adapter
- `python-dotenv`: Environment variable management from .env files
- `apscheduler`: Advanced job scheduling library

All dependencies are standard Python packages installable via pip.