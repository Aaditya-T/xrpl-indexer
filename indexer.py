"""XRPL Indexer: fetches ledger transactions and maintains state tables."""
import json
from typing import Optional
from database import Database
from xrpl_client import XRPLClient
from state_processor import StateProcessor
from config import Config
from observability import emit
import time
from concurrent.futures import ThreadPoolExecutor


def _has_account_root_creation(meta: dict, destination: str) -> bool:
    """
    Return True if the transaction metadata contains a CreatedNode for an
    AccountRoot whose Account field matches `destination`.  This is the
    definitive signal that the account was newly funded/activated by this tx.
    """
    for node_wrapper in meta.get("AffectedNodes", []):
        if "CreatedNode" in node_wrapper:
            node = node_wrapper["CreatedNode"]
            if (node.get("LedgerEntryType") == "AccountRoot"
                    and (node.get("NewFields") or {}).get("Account") == destination):
                return True
    return False


class XRPLIndexer:
    """Main indexer class for processing XRPL ledgers and transactions"""

    def __init__(
        self,
        db: Optional["Database"] = None,
        xrpl_client: Optional["XRPLClient"] = None,
        central_wallet: Optional[str] = None,
        parent_wallets: Optional[list[str]] = None,
        track_source_tags: Optional[list[int]] = None,
    ):
        self.db = db if db is not None else Database()
        self.xrpl_client = xrpl_client if xrpl_client is not None else XRPLClient()
        self.state_processor = StateProcessor(self.db)
        self.filter_tx_types = Config.get_filter_transaction_types()
        self.filter_addresses = Config.get_filter_addresses()
        self.filter_source_tags = Config.get_filter_source_tags()
        self.central_wallet = (
            central_wallet if central_wallet is not None else Config.CENTRAL_WALLET_ADDRESS
        ).strip()
        if central_wallet is None and parent_wallets is None:
            self.parent_wallets = set(Config.get_parent_wallet_addresses())
        else:
            additional_parents = (
                parent_wallets if parent_wallets is not None
                else Config.PARENT_WALLET_ADDRESSES.split(",")
            )
            self.parent_wallets = {
                address.strip() for address in [self.central_wallet, *additional_parents]
                if address.strip()
            }
        self.track_source_tags = set(
            track_source_tags if track_source_tags is not None else Config.get_track_source_tags()
        )

        # Discover qualifying activations that were already stored.
        if self.parent_wallets or self.track_source_tags:
            self._retroactive_wallet_scan()

    # ------------------------------------------------------------------
    # Wallet discovery helpers
    # ------------------------------------------------------------------

    def _retroactive_wallet_scan(self):
        """
        Scan stored payments from a parent wallet or carrying a tracking tag.
        Only payments that created the destination account enroll a wallet.
        """
        newly_added = 0
        after_id = 0
        while rows := self.db.get_payments_for_discovery(
            self.parent_wallets, self.track_source_tags, after_id=after_id
        ):
            for row in rows:
                after_id = row["id"]
                destination = row["address"]
                if destination in self.parent_wallets:
                    continue
                tx_hash = row["tx_hash"]
                raw = row.get("transaction_data") or {}
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except (ValueError, TypeError):
                        raw = {}

                full = raw.get("_full_data") if isinstance(raw, dict) else {}
                meta = (full.get("meta") if isinstance(full, dict) else {}) or {}
                if not isinstance(meta, dict):
                    continue

                if (not self.db.is_tracked_wallet(destination)
                        and _has_account_root_creation(meta, destination)):
                    added = self.db.add_tracked_wallet(destination, tx_hash)
                    if added:
                        newly_added += 1

        if newly_added:
            print(f"[WalletDiscovery] Retroactively registered {newly_added} wallet(s) from stored transactions.")

    def _check_wallet_discovery(self, tx_data: dict, tx_hash: str):
        """
        Register the destination of a parent-funded or source-tagged Payment
        when its metadata shows that the destination account was created.
        """
        if tx_data.get("TransactionType") != "Payment":
            return
        if (tx_data.get("Account") not in self.parent_wallets
                and tx_data.get("SourceTag") not in self.track_source_tags):
            return
        destination = tx_data.get("Destination")
        if not destination or destination in self.parent_wallets:
            return

        # _full_data is set before this method is called in process_ledger
        full = tx_data.get("_full_data") or {}
        meta = (full.get("meta") if isinstance(full, dict) else {}) or {}
        if not isinstance(meta, dict):
            return

        if _has_account_root_creation(meta, destination):
            added = self.db.add_tracked_wallet(destination, tx_hash)
            if added:
                print(f"[WalletDiscovery] New wallet activated: {destination} (tx: {tx_hash})")

    # ------------------------------------------------------------------
    # Filtering (transactions table only)
    # ------------------------------------------------------------------

    def should_include_transaction(self, tx_data: dict) -> bool:
        """Return True if transaction should be stored in the transactions table."""
        # Explicit tracking tags retain the full transaction even when a
        # FILTER_ADDRESSES or FILTER_TRANSACTION_TYPES rule would exclude it.
        if tx_data.get("SourceTag") in self.track_source_tags:
            return True

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

    def process_ledger(
        self, ledger_index: int, ledger_data: Optional[tuple] = None
    ) -> int:
        """
        Fetch and process a single ledger.

        State tracking (wallet discovery + account/trustline/offer tables) runs
        on EVERY transaction regardless of the configured filters.  The filters
        only control what gets written to the `transactions` table.

        Returns the number of transactions stored in the transactions table.
        """
        started = time.monotonic()
        print(f"Processing ledger {ledger_index}...")
        if ledger_data is None:
            ledger_data = self.xrpl_client.get_ledger_with_transactions(ledger_index)
        if hasattr(self.db, "ledger_transaction"):
            with self.db.ledger_transaction(ledger_index):
                stored_count = self._apply_ledger_data(ledger_index, ledger_data)
        else:
            stored_count = self._apply_ledger_data(ledger_index, ledger_data)

        transactions, _ = ledger_data
        duration = round(time.monotonic() - started, 3)
        print(f"Ledger {ledger_index}: Processed {len(transactions)} transactions, stored {stored_count} in {duration}s")
        emit(
            "ledger_processed",
            ledger_index=ledger_index,
            transactions_seen=len(transactions),
            transactions_stored=stored_count,
            duration_seconds=duration,
        )
        return stored_count

    def _apply_ledger_data(self, ledger_index: int, ledger_data: tuple) -> int:
        transactions, close_time_iso = ledger_data
        stored_count = 0

        # Always store ledger close time — independent of any transaction filters
        if close_time_iso:
            self.db.upsert_ledger_metadata(ledger_index, close_time_iso)

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

            # Attach full raw response early; needed by discovery and state processor
            tx_data["_full_data"] = tx
            tx_hash = tx_data.get("hash", "")

            # --- State tracking: runs on ALL transactions, filter-independent ---

            # 1. Wallet discovery (requires AccountRoot CreatedNode in meta)
            self._check_wallet_discovery(tx_data, tx_hash)

            # 2. Update account_states / trustlines / offers from AffectedNodes
            try:
                self.state_processor.process_transaction(tx_data, ledger_index)
            except Exception as exc:
                print(f"[StateProcessor] error on tx {tx_hash}: {exc}")
                raise

            # --- Transaction storage: controlled by configured filters ---
            if self.should_include_transaction(tx_data):
                self.db.insert_transaction(tx_data)
                stored_count += 1

        return stored_count

    def process_ledgers_parallel(self, ledgers_to_process: list) -> int:
        """Fetch ledgers in parallel, then apply them in ledger order.

        Discovery in one ledger must happen before state changes in the next.
        Fixed-size batches keep memory bounded regardless of backlog size.
        """
        BATCH_SIZE = max(Config.PARALLEL_WORKERS * 4, 20)

        total_stored = 0
        completed = 0
        total_ledgers = len(ledgers_to_process)

        for batch_start in range(0, total_ledgers, BATCH_SIZE):
            batch = ledgers_to_process[batch_start:batch_start + BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=Config.PARALLEL_WORKERS) as executor:
                ledger_data_batch = list(
                    executor.map(self.xrpl_client.get_ledger_with_transactions, batch)
                )

            for ledger_index, ledger_data in zip(batch, ledger_data_batch):
                stored = self.process_ledger(ledger_index, ledger_data)
                total_stored += stored
                completed += 1
                if completed % 10 == 0 or completed == total_ledgers:
                    print(f"Progress: {completed}/{total_ledgers} ledgers processed, {total_stored} transactions stored")

        return total_stored

    # ------------------------------------------------------------------
    # Main cycle
    # ------------------------------------------------------------------

    def run_indexing_cycle(self) -> bool:
        """Run a single indexing cycle."""
        for attempt in range(2):
            try:
                self._run_indexing_cycle_once()
                return True
            except Exception as e:
                is_db_connection_error = (
                    hasattr(self.db, "is_connection_error")
                    and self.db.is_connection_error(e)
                )
                if is_db_connection_error and attempt == 0:
                    print(f"Database connection error during indexing cycle: {e}")
                    print("Reconnecting to database and retrying cycle once...")
                    self.db.reconnect()
                    continue

                print(f"Error during indexing cycle: {e}")
                emit(
                    "indexing_cycle_error",
                    level="error",
                    error_type=type(e).__name__,
                    error=str(e),
                    attempt=attempt + 1,
                )
                import traceback
                traceback.print_exc()
                return False

        return False

    def _run_indexing_cycle_once(self):
        """Run one indexing attempt. Exceptions are handled by run_indexing_cycle."""
        if hasattr(self.db, "ensure_connection"):
            self.db.ensure_connection()

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

        print("\nIndexing cycle complete!")
        print(f"Processed ledgers: {last_processed + 1} to {current_ledger_index}")
        print(f"Total transactions stored: {total_stored}")
        print(f"Total transactions in database: {self.db.get_transaction_count()}")

    def close(self):
        self.db.close()
