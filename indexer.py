"""Core XRPL Indexer Logic"""
from typing import Optional
from database import Database
from xrpl_client import XRPLClient
from config import Config
import time


class XRPLIndexer:
    """Main indexer class for processing XRPL ledgers and transactions"""
    
    def __init__(self):
        self.db = Database()
        self.xrpl_client = XRPLClient()
        self.filter_tx_types = Config.get_filter_transaction_types()
        self.filter_addresses = Config.get_filter_addresses()
        self.filter_source_tags = Config.get_filter_source_tags()
    
    def should_include_transaction(self, tx_data: dict) -> bool:
        """Check if transaction matches filter criteria"""
        
        # Filter by transaction type
        if self.filter_tx_types:
            tx_type = tx_data.get('TransactionType')
            if tx_type not in self.filter_tx_types:
                return False
        
        # Filter by address (account or destination)
        if self.filter_addresses:
            account = tx_data.get('Account')
            destination = tx_data.get('Destination')
            
            if account not in self.filter_addresses and destination not in self.filter_addresses:
                return False
        
        # Filter by source tag
        if self.filter_source_tags:
            source_tag = tx_data.get('SourceTag')
            if source_tag not in self.filter_source_tags:
                return False
        
        return True
    
    def process_ledger(self, ledger_index: int) -> int:
        """Process a single ledger and return number of transactions stored"""
        print(f"Processing ledger {ledger_index}...")
        
        transactions = self.xrpl_client.get_ledger_with_transactions(ledger_index)
        stored_count = 0
        
        for tx in transactions:
            if isinstance(tx, dict):
                # Extract transaction data - check for tx_json first, then tx, then use root
                if 'tx_json' in tx:
                    tx_data = tx['tx_json'].copy()
                    # Add metadata from root level
                    tx_data['hash'] = tx.get('hash')
                    tx_data['ledger_index'] = tx.get('ledger_index', ledger_index)
                elif 'tx' in tx:
                    tx_data = tx['tx'].copy()
                    tx_data['hash'] = tx.get('hash')
                    tx_data['ledger_index'] = tx.get('ledger_index', ledger_index)
                else:
                    tx_data = tx.copy()
                    if 'ledger_index' not in tx_data:
                        tx_data['ledger_index'] = ledger_index
                
                # Apply filters
                if self.should_include_transaction(tx_data):
                    # Store full transaction with metadata for reference
                    tx_data['_full_data'] = tx
                    self.db.insert_transaction(tx_data)
                    stored_count += 1
        
        print(f"Ledger {ledger_index}: Processed {len(transactions)} transactions, stored {stored_count}")
        return stored_count
    
    def run_indexing_cycle(self):
        """Run a single indexing cycle"""
        try:
            # Get current ledger index from XRPL
            current_ledger_index = self.xrpl_client.get_current_ledger_index()
            print(f"\nCurrent ledger index: {current_ledger_index}")
            
            # Get last processed ledger index from database
            last_processed = self.db.get_last_processed_ledger_index()
            
            if last_processed is None:
                # First run - store current index and wait for next cycle
                print("First run detected. Storing current ledger index and waiting for next cycle...")
                self.db.update_last_processed_ledger_index(current_ledger_index)
                print(f"Stored ledger index: {current_ledger_index}")
                return
            
            print(f"Last processed ledger index: {last_processed}")
            
            # Calculate ledgers to process
            if current_ledger_index <= last_processed:
                print("No new ledgers to process.")
                return
            
            # Process ledgers from last_processed + 1 to current_ledger_index
            ledgers_to_process = list(range(last_processed + 1, current_ledger_index + 1))
            total_ledgers = len(ledgers_to_process)
            
            print(f"Processing {total_ledgers} ledgers ({last_processed + 1} to {current_ledger_index})...")
            
            total_stored = 0
            for i, ledger_index in enumerate(ledgers_to_process, 1):
                stored = self.process_ledger(ledger_index)
                total_stored += stored
                
                # Show progress
                if i % 10 == 0 or i == total_ledgers:
                    print(f"Progress: {i}/{total_ledgers} ledgers processed, {total_stored} transactions stored")
                
                # Small delay to avoid overwhelming the API
                if i < total_ledgers:
                    time.sleep(0.1)
            
            # Update last processed index
            self.db.update_last_processed_ledger_index(current_ledger_index)
            
            print(f"\nIndexing cycle complete!")
            print(f"Processed ledgers: {last_processed + 1} to {current_ledger_index}")
            print(f"Total transactions stored: {total_stored}")
            print(f"Total transactions in database: {self.db.get_transaction_count()}")
            
        except Exception as e:
            print(f"Error during indexing cycle: {e}")
            import traceback
            traceback.print_exc()
    
    def close(self):
        """Clean up resources"""
        self.db.close()
