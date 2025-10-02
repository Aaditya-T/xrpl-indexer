"""Database models and operations for XRPL Indexer"""
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor
import sqlite3
import json
from typing import Optional, List, Dict, Any, Union
from config import Config


class Database:
    """Database handler supporting both PostgreSQL and SQLite"""
    
    def __init__(self):
        self.db_type = Config.DATABASE_TYPE
        self.conn: Union[psycopg2.extensions.connection, sqlite3.Connection]
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Establish database connection"""
        if self.db_type == "postgresql":
            self.conn = psycopg2.connect(Config.DATABASE_URL, cursor_factory=RealDictCursor)
        else:
            # SQLite
            db_path = Config.DATABASE_URL.replace("sqlite:///", "")
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        cursor = self.conn.cursor()
        
        if self.db_type == "postgresql":
            # PostgreSQL schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indexer_state (
                    id SERIAL PRIMARY KEY,
                    last_processed_ledger_index BIGINT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            
            # Create indexes for PostgreSQL
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_index ON transactions(ledger_index)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(transaction_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(transaction_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_account ON transactions(account)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_destination ON transactions(destination)")
        else:
            # SQLite schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indexer_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_processed_ledger_index INTEGER NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ledger_index ON transactions(ledger_index)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_hash ON transactions(transaction_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(transaction_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_account ON transactions(account)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_destination ON transactions(destination)")
        
        self.conn.commit()
        cursor.close()
    
    def get_last_processed_ledger_index(self) -> Optional[int]:
        """Get the last processed ledger index"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT last_processed_ledger_index FROM indexer_state ORDER BY id DESC LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            if self.db_type == "sqlite":
                return result[0]  # type: ignore
            else:
                return result['last_processed_ledger_index']  # type: ignore
        return None
    
    def update_last_processed_ledger_index(self, ledger_index: int):
        """Update the last processed ledger index"""
        cursor = self.conn.cursor()
        
        if self.db_type == "postgresql":
            cursor.execute(
                "INSERT INTO indexer_state (last_processed_ledger_index) VALUES (%s)",
                (ledger_index,)
            )
        else:
            cursor.execute(
                "INSERT INTO indexer_state (last_processed_ledger_index) VALUES (?)",
                (ledger_index,)
            )
        
        self.conn.commit()
        cursor.close()
    
    def insert_transaction(self, tx_data: Dict[str, Any]):
        """Insert a transaction into the database"""
        cursor = self.conn.cursor()
        
        # Extract common fields
        ledger_index = tx_data.get('ledger_index')
        tx_hash = tx_data.get('hash')
        tx_type = tx_data.get('TransactionType')
        account = tx_data.get('Account')
        destination = tx_data.get('Destination')
        amount = json.dumps(tx_data.get('Amount')) if tx_data.get('Amount') else None
        fee = tx_data.get('Fee')
        source_tag = tx_data.get('SourceTag')
        destination_tag = tx_data.get('DestinationTag')
        
        try:
            if self.db_type == "postgresql":
                cursor.execute("""
                    INSERT INTO transactions 
                    (ledger_index, transaction_hash, transaction_type, account, destination, 
                     amount, fee, source_tag, destination_tag, transaction_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_hash) DO NOTHING
                """, (
                    ledger_index, tx_hash, tx_type, account, destination,
                    amount, fee, source_tag, destination_tag, json.dumps(tx_data)
                ))
            else:
                cursor.execute("""
                    INSERT OR IGNORE INTO transactions 
                    (ledger_index, transaction_hash, transaction_type, account, destination, 
                     amount, fee, source_tag, destination_tag, transaction_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ledger_index, tx_hash, tx_type, account, destination,
                    amount, fee, source_tag, destination_tag, json.dumps(tx_data)
                ))
            
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting transaction {tx_hash}: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
    
    def get_transaction_count(self) -> int:
        """Get total number of transactions stored"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else 0  # type: ignore
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
