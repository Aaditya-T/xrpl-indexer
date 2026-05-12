"""XRPL Indexer: fetches ledger transactions and maintains state tables."""
from typing import Optional
from database import Database
from xrpl_client import XRPLClient
from state_processor import StateProcessor
from config import Config
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


class XRPLIndexer:
    """Main indexer class for processing XRPL ledgers and transactions"""

    def __init__(self):
        self.db = Database()
        self.xrpl_client = XRPLClient()
        self.state_processor = StateProcessor(self.db)
        self.filter_tx_types = Config.get_filter_transaction_types()
        self.filter_addresses = Config.get_filter_addresses()
        self.filter_source_tags = Config.get_filter_source_tags()
        self.central_wallet = Config.CENTRAL_WALLET_ADDRESS.strip()

        # Retroactively discover wallets activated before this run
        if self.central_wallet:
            self._retroactive_wallet_scan()

    # ------------------------------------------------------------------
    # Wallet discovery
    # ------------------------------------------------------------------

    def _retroactive_wallet_scan(self):
        """
        Scan transactions already in the DB for Payments from the central
        wallet and register any untracked destinations so state tracking
        begins for them on the next indexing cycle.
        """
        rows = self.db.get_central_wallet_payment_destinations(self.central_wallet)
        newly_added = 0
        for row in rows:
            added = self.db.add_tracked_wallet(row["address"], row["tx_hash"])
            if added:
                newly_added += 1
        if newly_added:
            print(f"[WalletDiscovery] Retroactively registered {newly_added} wallet(s) from stored transactions.")

    def _check_wallet_discovery(self, tx_data: dict, tx_hash: str):
        """
        If this transaction is a Payment from the central wallet to a new
        address, register that address as a tracked wallet.
        """
        if not self.central_wallet:
            return
        if tx_data.get("TransactionType") != "Payment":
            return
        if tx_data.get("Account") != self.central_wallet:
            return
        destination = tx_data.get("Destination")
        if not destination:
            return
        added = self.db.add_tracked_wallet(destination, tx_hash)
        if added:
            print(f"[WalletDiscovery] New wallet activated: {destination} (tx: {tx_hash})")

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def should_include_transaction(self, tx_data: dict) -> bool:
        """Return True if the transaction passes all configured filters."""
        if self.filter_tx_types:
            if tx_data.get("TransactionType") not in self.filter_tx_types:
                return False

        if self.filter_addresses:
            account = tx_data.get("Account")
            destination = tx_data.get("Destination")
            if account not in self.filter_addresses and destination not in self.filter_addresses:
                return False

        if self.filter_source_tags:
            if tx_data.get("SourceTag") not in self.filter_source_tags:
                return False

        return True

    # ------------------------------------------------------------------
    # Ledger processing
    # ------------------------------------------------------------------

    def process_ledger(self, ledger_index: int) -> int:
        """Process a single ledger; return number of transactions stored."""
        print(f"Processing ledger {ledger_index}...")
        transactions = self.xrpl_client.get_ledger_with_transactions(ledger_index)
        stored_count = 0

        for tx in transactions:
            if not isinstance(tx, dict):
                continue

            if "tx_json" in tx:
                tx_data = tx["tx_json"].copy()
                tx_data["hash"] = tx.get("hash")
                tx_data["ledger_index"] = tx.get("ledger_index", ledger_index)
            elif "tx" in tx:
                tx_data = tx["tx"].copy()
                tx_data["hash"] = tx.get("hash")
                tx_data["ledger_index"] = tx.get("ledger_index", ledger_index)
            else:
                tx_data = tx.copy()
                if "ledger_index" not in tx_data:
                    tx_data["ledger_index"] = ledger_index

            tx_hash = tx_data.get("hash", "")

            # Wallet discovery runs on every transaction regardless of filters,
            # so we never miss a central-wallet activation.
            self._check_wallet_discovery(tx_data, tx_hash)

            if self.should_include_transaction(tx_data):
                tx_data["_full_data"] = tx
                self.db.insert_transaction(tx_data)
                stored_count += 1

                # Update live state tables from this transaction's metadata
                try:
                    self.state_processor.process_transaction(tx_data, ledger_index)
                except Exception as exc:
                    print(f"[StateProcessor] error on tx {tx_hash}: {exc}")

        print(f"Ledger {ledger_index}: Processed {len(transactions)} transactions, stored {stored_count}")
        return stored_count

    def process_ledgers_parallel(self, ledgers_to_process: list) -> int:
        """Process multiple ledgers in parallel using ThreadPoolExecutor."""
        total_stored = 0
        completed = 0
        total_ledgers = len(ledgers_to_process)
        failed_ledgers = []

        with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as executor:
            future_to_ledger = {
                executor.submit(self.process_ledger, ledger_index): ledger_index
                for ledger_index in ledgers_to_process
            }

            for future in as_completed(future_to_ledger):
                ledger_index = future_to_ledger[future]
                try:
                    stored = future.result()
                    total_stored += stored
                    completed += 1
                    if completed % 10 == 0 or completed == total_ledgers:
                        print(f"Progress: {completed}/{total_ledgers} ledgers processed, {total_stored} transactions stored")
                except Exception as e:
                    print(f"Error processing ledger {ledger_index}: {e}")
                    failed_ledgers.append((ledger_index, str(e)))

        if failed_ledgers:
            raise Exception(
                f"Failed to process {len(failed_ledgers)} ledger(s): "
                f"{', '.join(str(l[0]) for l in failed_ledgers)}"
            )
        return total_stored

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    def run_indexing_cycle(self):
        """Run a single indexing cycle."""
        try:
            current_ledger_index = self.xrpl_client.get_current_ledger_index()
            print(f"\nCurrent ledger index: {current_ledger_index}")

            last_processed = self.db.get_last_processed_ledger_index()

            if last_processed is None:
                print("First run detected. Storing current ledger index and waiting for next cycle...")
                self.db.update_last_processed_ledger_index(current_ledger_index)
                print(f"Stored ledger index: {current_ledger_index}")
                return

            print(f"Last processed ledger index: {last_processed}")

            if current_ledger_index <= last_processed:
                print("No new ledgers to process.")
                return

            ledgers_to_process = list(range(last_processed + 1, current_ledger_index + 1))
            total_ledgers = len(ledgers_to_process)
            print(f"Processing {total_ledgers} ledgers ({last_processed + 1} to {current_ledger_index})...")

            if Config.ENABLE_PARALLEL_PROCESSING:
                print(f"Using parallel processing with {Config.PARALLEL_WORKERS} workers")
                total_stored = self.process_ledgers_parallel(ledgers_to_process)
            else:
                total_stored = 0
                for i, ledger_index in enumerate(ledgers_to_process, 1):
                    stored = self.process_ledger(ledger_index)
                    total_stored += stored
                    if i % 10 == 0 or i == total_ledgers:
                        print(f"Progress: {i}/{total_ledgers} ledgers processed, {total_stored} transactions stored")
                    if i < total_ledgers:
                        time.sleep(0.1)

            self.db.update_last_processed_ledger_index(current_ledger_index)

            print("\nIndexing cycle complete!")
            print(f"Processed ledgers: {last_processed + 1} to {current_ledger_index}")
            print(f"Total transactions stored: {total_stored}")
            print(f"Total transactions in database: {self.db.get_transaction_count()}")

        except Exception as e:
            print(f"Error during indexing cycle: {e}")
            import traceback
            traceback.print_exc()

    def close(self):
        self.db.close()
