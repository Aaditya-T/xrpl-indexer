"""Database models and operations for XRPL Indexer"""
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor
import sqlite3
import json
import threading
from typing import Optional, List, Dict, Any, Union
from config import Config


DBConnectionErrors = (
    psycopg2.InterfaceError,
    psycopg2.OperationalError,
    sqlite3.InterfaceError,
    sqlite3.OperationalError,
    sqlite3.ProgrammingError,
)


class LockedCursor:
    """Cursor wrapper that holds the database lock until the cursor is closed."""

    def __init__(self, cursor, lock: threading.RLock):
        self._cursor = cursor
        self._lock = lock
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def close(self):
        if self._closed:
            return
        try:
            self._cursor.close()
        except Exception:
            pass
        finally:
            self._closed = True
            self._lock.release()


class Database:
    """Database handler supporting both PostgreSQL and SQLite"""

    def __init__(
        self,
        db_url: Optional[str] = None,
        db_type: Optional[str] = None,
    ):
        self.db_type = db_type or Config.DATABASE_TYPE
        self._db_url = db_url or Config.DATABASE_URL
        self.conn: Optional[Union[psycopg2.extensions.connection, sqlite3.Connection]] = None
        self._lock = threading.RLock()
        self._tracked_wallets_cache: set[str] = set()
        self.connect()
        self.create_tables()
        self._load_tracked_wallets_cache()

    def connect(self):
        """Establish database connection"""
        with self._lock:
            if self.db_type == "postgresql":
                self.conn = psycopg2.connect(self._db_url, cursor_factory=RealDictCursor)
            else:
                db_path = self._db_url.replace("sqlite:///", "")
                self.conn = sqlite3.connect(db_path, check_same_thread=False)
                self.conn.row_factory = sqlite3.Row

    def _connection_is_closed(self) -> bool:
        if self.conn is None:
            return True
        if self.db_type == "postgresql":
            return bool(self.conn.closed)
        try:
            cursor = self.conn.execute("SELECT 1")
            cursor.close()
            return False
        except DBConnectionErrors:
            return True

    def ensure_connection(self):
        """Reconnect if the database handle was closed by the server or app."""
        with self._lock:
            if self._connection_is_closed():
                print("[Database] Connection is closed; reconnecting...")
                self.connect()
            return self.conn

    def reconnect(self):
        """Force a fresh database connection after a connection-level failure."""
        with self._lock:
            if self.conn is not None:
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
            self.connect()

    def is_connection_error(self, error: Exception) -> bool:
        return isinstance(error, DBConnectionErrors)

    def _cursor(self):
        self._lock.acquire()
        try:
            conn = self.ensure_connection()
            return LockedCursor(conn.cursor(), self._lock)
        except Exception:
            self._lock.release()
            raise

    def _commit(self):
        try:
            self.ensure_connection().commit()
        except DBConnectionErrors:
            self.reconnect()
            raise

    def _rollback(self):
        try:
            if self.conn is not None and not self._connection_is_closed():
                self.conn.rollback()
        except DBConnectionErrors:
            self.reconnect()

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                self._create_tables_pg(cursor)
            else:
                self._create_tables_sqlite(cursor)

            self._commit()
        finally:
            cursor.close()

    def _create_tables_pg(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS indexer_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_processed_ledger_index BIGINT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (id = 1)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                ledger_index BIGINT NOT NULL,
                transaction_hash VARCHAR(255) UNIQUE NOT NULL,
                transaction_type VARCHAR(100),
                account VARCHAR(255),
                destination VARCHAR(255),
                amount TEXT,
                fee VARCHAR(50),
                source_tag BIGINT,
                destination_tag BIGINT,
                transaction_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_wallets (
                address VARCHAR(255) PRIMARY KEY,
                activation_tx_hash VARCHAR(255),
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS account_states (
                address VARCHAR(255) PRIMARY KEY,
                balance_drops BIGINT,
                sequence BIGINT,
                owner_count INTEGER,
                flags BIGINT,
                ledger_index BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trustlines (
                account VARCHAR(255) NOT NULL,
                issuer VARCHAR(255) NOT NULL,
                currency VARCHAR(40) NOT NULL,
                balance TEXT,
                limit_amount TEXT,
                limit_peer TEXT,
                authorized BOOLEAN DEFAULT FALSE,
                peer_authorized BOOLEAN DEFAULT FALSE,
                no_ripple BOOLEAN DEFAULT FALSE,
                no_ripple_peer BOOLEAN DEFAULT FALSE,
                freeze_flag BOOLEAN DEFAULT FALSE,
                peer_freeze_flag BOOLEAN DEFAULT FALSE,
                is_deleted BOOLEAN DEFAULT FALSE,
                ledger_index BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account, issuer, currency)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS offers (
                account VARCHAR(255) NOT NULL,
                sequence BIGINT NOT NULL,
                taker_gets_currency VARCHAR(40),
                taker_gets_issuer VARCHAR(255),
                taker_gets_value TEXT,
                taker_pays_currency VARCHAR(40),
                taker_pays_issuer VARCHAR(255),
                taker_pays_value TEXT,
                expiry_iso TEXT,
                flags BIGINT DEFAULT 0,
                quality TEXT,
                ledger_index BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account, sequence)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ledger_metadata (
                ledger_index BIGINT PRIMARY KEY,
                close_time_iso TEXT NOT NULL,
                stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_index ON transactions(ledger_index)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(transaction_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(transaction_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_account ON transactions(account)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_destination ON transactions(destination)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tl_issuer_currency ON trustlines(issuer, currency)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_account ON offers(account)")

    def _create_tables_sqlite(self, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS indexer_state (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_processed_ledger_index INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (id = 1)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ledger_index INTEGER NOT NULL,
                transaction_hash TEXT UNIQUE NOT NULL,
                transaction_type TEXT,
                account TEXT,
                destination TEXT,
                amount TEXT,
                fee TEXT,
                source_tag INTEGER,
                destination_tag INTEGER,
                transaction_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracked_wallets (
                address TEXT PRIMARY KEY,
                activation_tx_hash TEXT,
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS account_states (
                address TEXT PRIMARY KEY,
                balance_drops INTEGER,
                sequence INTEGER,
                owner_count INTEGER,
                flags INTEGER,
                ledger_index INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trustlines (
                account TEXT NOT NULL,
                issuer TEXT NOT NULL,
                currency TEXT NOT NULL,
                balance TEXT,
                limit_amount TEXT,
                limit_peer TEXT,
                authorized INTEGER DEFAULT 0,
                peer_authorized INTEGER DEFAULT 0,
                no_ripple INTEGER DEFAULT 0,
                no_ripple_peer INTEGER DEFAULT 0,
                freeze_flag INTEGER DEFAULT 0,
                peer_freeze_flag INTEGER DEFAULT 0,
                is_deleted INTEGER DEFAULT 0,
                ledger_index INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account, issuer, currency)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS offers (
                account TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                taker_gets_currency TEXT,
                taker_gets_issuer TEXT,
                taker_gets_value TEXT,
                taker_pays_currency TEXT,
                taker_pays_issuer TEXT,
                taker_pays_value TEXT,
                expiry_iso TEXT,
                flags INTEGER DEFAULT 0,
                quality TEXT,
                ledger_index INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account, sequence)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ledger_metadata (
                ledger_index INTEGER PRIMARY KEY,
                close_time_iso TEXT NOT NULL,
                stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_index ON transactions(ledger_index)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(transaction_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(transaction_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_account ON transactions(account)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_destination ON transactions(destination)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tl_issuer_currency ON trustlines(issuer, currency)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_offers_account ON offers(account)")

    # ------------------------------------------------------------------
    # Tracked wallets
    # ------------------------------------------------------------------

    def _load_tracked_wallets_cache(self):
        """Load all tracked wallet addresses into memory."""
        cursor = self._cursor()
        try:
            cursor.execute("SELECT address FROM tracked_wallets")
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if self.db_type == "postgresql":
            self._tracked_wallets_cache = {r["address"] for r in rows}
        else:
            self._tracked_wallets_cache = {r[0] for r in rows}

    def is_tracked_wallet(self, address: str) -> bool:
        """Return True if address is a tracked wallet (in-memory check)."""
        return address in self._tracked_wallets_cache

    def add_tracked_wallet(self, address: str, tx_hash: str) -> bool:
        """Add a wallet to tracking. Returns True if it was newly added."""
        if address in self._tracked_wallets_cache:
            return False
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "INSERT INTO tracked_wallets (address, activation_tx_hash) "
                    "VALUES (%s, %s) ON CONFLICT (address) DO NOTHING",
                    (address, tx_hash),
                )
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO tracked_wallets (address, activation_tx_hash) "
                    "VALUES (?, ?)",
                    (address, tx_hash),
                )
            self._commit()
            self._tracked_wallets_cache.add(address)
            return True
        except Exception as e:
            print(f"Error adding tracked wallet {address}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
            return False
        finally:
            cursor.close()

    def get_all_tracked_wallets(self) -> list[str]:
        """Return all tracked wallet addresses."""
        return list(self._tracked_wallets_cache)

    def get_central_wallet_payments_for_discovery(self, central_wallet: str) -> list[dict]:
        """
        Fetch Payment transactions from the central wallet, in ledger order.
        Returns address, tx_hash, and transaction_data so callers can inspect
        AffectedNodes for AccountRoot creation (the definitive activation signal).
        """
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "SELECT DISTINCT ON (destination) destination, transaction_hash, transaction_data "
                    "FROM transactions "
                    "WHERE account = %s AND transaction_type = 'Payment' AND destination IS NOT NULL "
                    "ORDER BY destination, ledger_index ASC",
                    (central_wallet,),
                )
            else:
                # SQLite: use a CTE to deterministically select the earliest tx per destination.
                # A bare GROUP BY would pick an arbitrary row for transaction_hash / transaction_data.
                cursor.execute(
                    "WITH earliest AS ("
                    "  SELECT destination, MIN(ledger_index) AS min_li"
                    "  FROM transactions"
                    "  WHERE account = ? AND transaction_type = 'Payment' AND destination IS NOT NULL"
                    "  GROUP BY destination"
                    ")"
                    "SELECT t.destination, t.transaction_hash, t.transaction_data"
                    " FROM transactions t"
                    " JOIN earliest ON t.destination = earliest.destination"
                    "              AND t.ledger_index = earliest.min_li"
                    " WHERE t.account = ? AND t.transaction_type = 'Payment'",
                    (central_wallet, central_wallet),
                )
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if self.db_type == "postgresql":
            return [
                {"address": r["destination"], "tx_hash": r["transaction_hash"], "transaction_data": r["transaction_data"]}
                for r in rows
            ]
        return [{"address": r[0], "tx_hash": r[1], "transaction_data": r[2]} for r in rows]

    # ------------------------------------------------------------------
    # Ledger metadata
    # ------------------------------------------------------------------

    def upsert_ledger_metadata(self, ledger_index: int, close_time_iso: str) -> None:
        """Store the close time for a ledger. Written for every processed ledger."""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "INSERT INTO ledger_metadata (ledger_index, close_time_iso) "
                    "VALUES (%s, %s) ON CONFLICT (ledger_index) DO NOTHING",
                    (ledger_index, close_time_iso),
                )
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO ledger_metadata (ledger_index, close_time_iso) "
                    "VALUES (?, ?)",
                    (ledger_index, close_time_iso),
                )
            self._commit()
        except Exception as e:
            print(f"Error storing ledger metadata for {ledger_index}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    # Account states
    # ------------------------------------------------------------------

    def upsert_account_state(
        self,
        address: str,
        balance_drops: Optional[int],
        sequence: Optional[int],
        owner_count: Optional[int],
        flags: Optional[int],
        ledger_index: int,
    ):
        """Upsert account state, only updating if ledger_index is newer."""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO account_states
                        (address, balance_drops, sequence, owner_count, flags, ledger_index, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (address) DO UPDATE SET
                        balance_drops = EXCLUDED.balance_drops,
                        sequence      = EXCLUDED.sequence,
                        owner_count   = EXCLUDED.owner_count,
                        flags         = EXCLUDED.flags,
                        ledger_index  = EXCLUDED.ledger_index,
                        updated_at    = NOW()
                    WHERE account_states.ledger_index IS NULL
                       OR account_states.ledger_index <= EXCLUDED.ledger_index
                    """,
                    (address, balance_drops, sequence, owner_count, flags, ledger_index),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO account_states
                        (address, balance_drops, sequence, owner_count, flags, ledger_index, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(address) DO UPDATE SET
                        balance_drops = excluded.balance_drops,
                        sequence      = excluded.sequence,
                        owner_count   = excluded.owner_count,
                        flags         = excluded.flags,
                        ledger_index  = excluded.ledger_index,
                        updated_at    = CURRENT_TIMESTAMP
                    WHERE account_states.ledger_index IS NULL
                       OR account_states.ledger_index <= excluded.ledger_index
                    """,
                    (address, balance_drops, sequence, owner_count, flags, ledger_index),
                )
            self._commit()
        except Exception as e:
            print(f"Error upserting account state for {address}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Trustlines
    # ------------------------------------------------------------------

    def upsert_trustline(
        self,
        account: str,
        issuer: str,
        currency: str,
        balance: str,
        limit_amount: str,
        limit_peer: str,
        authorized: bool,
        peer_authorized: bool,
        no_ripple: bool,
        no_ripple_peer: bool,
        freeze_flag: bool,
        peer_freeze_flag: bool,
        is_deleted: bool,
        ledger_index: int,
    ):
        """Upsert a trustline row, only updating if ledger_index is newer."""
        cursor = self._cursor()
        try:
            a = int(authorized)
            p_a = int(peer_authorized)
            nr = int(no_ripple)
            nr_p = int(no_ripple_peer)
            fr = int(freeze_flag)
            fr_p = int(peer_freeze_flag)
            d = int(is_deleted)

            if self.db_type == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO trustlines
                        (account, issuer, currency, balance, limit_amount, limit_peer,
                         authorized, peer_authorized, no_ripple, no_ripple_peer,
                         freeze_flag, peer_freeze_flag, is_deleted, ledger_index, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (account, issuer, currency) DO UPDATE SET
                        balance          = EXCLUDED.balance,
                        limit_amount     = EXCLUDED.limit_amount,
                        limit_peer       = EXCLUDED.limit_peer,
                        authorized       = EXCLUDED.authorized,
                        peer_authorized  = EXCLUDED.peer_authorized,
                        no_ripple        = EXCLUDED.no_ripple,
                        no_ripple_peer   = EXCLUDED.no_ripple_peer,
                        freeze_flag      = EXCLUDED.freeze_flag,
                        peer_freeze_flag = EXCLUDED.peer_freeze_flag,
                        is_deleted     = EXCLUDED.is_deleted,
                        ledger_index   = EXCLUDED.ledger_index,
                        updated_at     = NOW()
                    WHERE trustlines.ledger_index IS NULL
                       OR trustlines.ledger_index <= EXCLUDED.ledger_index
                    """,
                    (account, issuer, currency, balance, limit_amount, limit_peer,
                     bool(a), bool(p_a), bool(nr), bool(nr_p), bool(fr), bool(fr_p),
                     bool(d), ledger_index),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO trustlines
                        (account, issuer, currency, balance, limit_amount, limit_peer,
                         authorized, peer_authorized, no_ripple, no_ripple_peer,
                         freeze_flag, peer_freeze_flag, is_deleted, ledger_index, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                    ON CONFLICT(account, issuer, currency) DO UPDATE SET
                        balance          = excluded.balance,
                        limit_amount     = excluded.limit_amount,
                        limit_peer       = excluded.limit_peer,
                        authorized       = excluded.authorized,
                        peer_authorized  = excluded.peer_authorized,
                        no_ripple        = excluded.no_ripple,
                        no_ripple_peer   = excluded.no_ripple_peer,
                        freeze_flag      = excluded.freeze_flag,
                        peer_freeze_flag = excluded.peer_freeze_flag,
                        is_deleted     = excluded.is_deleted,
                        ledger_index   = excluded.ledger_index,
                        updated_at     = CURRENT_TIMESTAMP
                    WHERE trustlines.ledger_index IS NULL
                       OR trustlines.ledger_index <= excluded.ledger_index
                    """,
                    (account, issuer, currency, balance, limit_amount, limit_peer,
                     a, p_a, nr, nr_p, fr, fr_p, d, ledger_index),
                )
            self._commit()
        except Exception as e:
            print(f"Error upserting trustline {account}/{issuer}/{currency}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    def delete_trustline(self, account: str, issuer: str, currency: str, ledger_index: int) -> None:
        """Hard-delete a trust line row when the ledger removes it."""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "DELETE FROM trustlines "
                    "WHERE account = %s AND issuer = %s AND currency = %s "
                    "AND (ledger_index IS NULL OR ledger_index <= %s)",
                    (account, issuer, currency, ledger_index),
                )
            else:
                cursor.execute(
                    "DELETE FROM trustlines "
                    "WHERE account = ? AND issuer = ? AND currency = ? "
                    "AND (ledger_index IS NULL OR ledger_index <= ?)",
                    (account, issuer, currency, ledger_index),
                )
            self._commit()
        except Exception as e:
            print(f"Error deleting trustline {account}/{issuer}/{currency}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Offers
    # ------------------------------------------------------------------

    def upsert_offer(
        self,
        account: str,
        sequence: int,
        taker_gets_currency: Optional[str],
        taker_gets_issuer: Optional[str],
        taker_gets_value: Optional[str],
        taker_pays_currency: Optional[str],
        taker_pays_issuer: Optional[str],
        taker_pays_value: Optional[str],
        expiry_iso: Optional[str],
        flags: int,
        quality: Optional[str],
        ledger_index: int,
    ):
        """Upsert an open offer."""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO offers
                        (account, sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value,
                         taker_pays_currency, taker_pays_issuer, taker_pays_value,
                         expiry_iso, flags, quality, ledger_index, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (account, sequence) DO UPDATE SET
                        taker_gets_currency = EXCLUDED.taker_gets_currency,
                        taker_gets_issuer   = EXCLUDED.taker_gets_issuer,
                        taker_gets_value    = EXCLUDED.taker_gets_value,
                        taker_pays_currency = EXCLUDED.taker_pays_currency,
                        taker_pays_issuer   = EXCLUDED.taker_pays_issuer,
                        taker_pays_value    = EXCLUDED.taker_pays_value,
                        expiry_iso          = EXCLUDED.expiry_iso,
                        flags               = EXCLUDED.flags,
                        quality             = EXCLUDED.quality,
                        ledger_index        = EXCLUDED.ledger_index,
                        updated_at          = NOW()
                    WHERE offers.ledger_index IS NULL
                       OR offers.ledger_index <= EXCLUDED.ledger_index
                    """,
                    (account, sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value,
                     taker_pays_currency, taker_pays_issuer, taker_pays_value,
                     expiry_iso, flags, quality, ledger_index),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO offers
                        (account, sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value,
                         taker_pays_currency, taker_pays_issuer, taker_pays_value,
                         expiry_iso, flags, quality, ledger_index, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                    ON CONFLICT(account, sequence) DO UPDATE SET
                        taker_gets_currency = excluded.taker_gets_currency,
                        taker_gets_issuer   = excluded.taker_gets_issuer,
                        taker_gets_value    = excluded.taker_gets_value,
                        taker_pays_currency = excluded.taker_pays_currency,
                        taker_pays_issuer   = excluded.taker_pays_issuer,
                        taker_pays_value    = excluded.taker_pays_value,
                        expiry_iso          = excluded.expiry_iso,
                        flags               = excluded.flags,
                        quality             = excluded.quality,
                        ledger_index        = excluded.ledger_index,
                        updated_at          = CURRENT_TIMESTAMP
                    WHERE offers.ledger_index IS NULL
                       OR offers.ledger_index <= excluded.ledger_index
                    """,
                    (account, sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value,
                     taker_pays_currency, taker_pays_issuer, taker_pays_value,
                     expiry_iso, flags, quality, ledger_index),
                )
            self._commit()
        except Exception as e:
            print(f"Error upserting offer {account}/{sequence}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    def delete_offer(self, account: str, sequence: int, ledger_index: int):
        """Remove a fully-filled or cancelled offer."""
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "DELETE FROM offers "
                    "WHERE account = %s AND sequence = %s "
                    "AND (ledger_index IS NULL OR ledger_index <= %s)",
                    (account, sequence, ledger_index),
                )
            else:
                cursor.execute(
                    "DELETE FROM offers "
                    "WHERE account = ? AND sequence = ? "
                    "AND (ledger_index IS NULL OR ledger_index <= ?)",
                    (account, sequence, ledger_index),
                )
            self._commit()
        except Exception as e:
            print(f"Error deleting offer {account}/{sequence}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Original transaction methods (unchanged)
    # ------------------------------------------------------------------

    def get_last_processed_ledger_index(self) -> Optional[int]:
        cursor = self._cursor()
        try:
            cursor.execute("SELECT last_processed_ledger_index FROM indexer_state WHERE id = 1")
            result = cursor.fetchone()
        finally:
            cursor.close()
        if result:
            if self.db_type == "sqlite":
                return result[0]  # type: ignore
            else:
                return result["last_processed_ledger_index"]  # type: ignore
        return None

    def update_last_processed_ledger_index(self, ledger_index: int):
        cursor = self._cursor()
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    "INSERT INTO indexer_state (id, last_processed_ledger_index) VALUES (1, %s) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "last_processed_ledger_index = EXCLUDED.last_processed_ledger_index, "
                    "updated_at = CURRENT_TIMESTAMP",
                    (ledger_index,),
                )
            else:
                cursor.execute(
                    "INSERT INTO indexer_state (id, last_processed_ledger_index) VALUES (1, ?) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "last_processed_ledger_index = excluded.last_processed_ledger_index, "
                    "updated_at = CURRENT_TIMESTAMP",
                    (ledger_index,),
                )
            self._commit()
        finally:
            cursor.close()

    def insert_transaction(self, tx_data: Dict[str, Any]):
        cursor = self._cursor()
        ledger_index = tx_data.get("ledger_index")
        tx_hash = tx_data.get("hash")
        tx_type = tx_data.get("TransactionType")
        account = tx_data.get("Account")
        destination = tx_data.get("Destination")
        amount = json.dumps(tx_data.get("Amount")) if tx_data.get("Amount") else None
        fee = tx_data.get("Fee")
        source_tag = tx_data.get("SourceTag")
        destination_tag = tx_data.get("DestinationTag")
        try:
            if self.db_type == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO transactions
                        (ledger_index, transaction_hash, transaction_type, account, destination,
                         amount, fee, source_tag, destination_tag, transaction_data)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transaction_hash) DO NOTHING
                    """,
                    (ledger_index, tx_hash, tx_type, account, destination,
                     amount, fee, source_tag, destination_tag, json.dumps(tx_data)),
                )
            else:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO transactions
                        (ledger_index, transaction_hash, transaction_type, account, destination,
                         amount, fee, source_tag, destination_tag, transaction_data)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (ledger_index, tx_hash, tx_type, account, destination,
                     amount, fee, source_tag, destination_tag, json.dumps(tx_data)),
                )
            self._commit()
        except Exception as e:
            print(f"Error inserting transaction {tx_hash}: {e}")
            self._rollback()
            if self.is_connection_error(e):
                raise
        finally:
            cursor.close()

    def get_transaction_count(self) -> int:
        cursor = self._cursor()
        try:
            cursor.execute("SELECT COUNT(*) as count FROM transactions")
            result = cursor.fetchone()
        finally:
            cursor.close()
        if not result:
            return 0
        return result[0] if self.db_type == "sqlite" else result["count"]  # type: ignore

    def close(self):
        with self._lock:
            if self.conn:
                self.conn.close()
                self.conn = None
