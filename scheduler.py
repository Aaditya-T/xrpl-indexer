from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from indexer import XRPLIndexer
from config import Config
from observability import emit, process_memory_mb
import signal
import sys
import time


class IndexerScheduler:
    """Scheduler for running XRPL indexer at configured intervals"""
    
    def __init__(self):
        self.scheduler = BlockingScheduler()
        self.indexer = XRPLIndexer()
        self.cycle_number = 0
        self.setup_signal_handlers()
    
    def setup_signal_handlers(self):
        """Setup graceful shutdown on SIGINT and SIGTERM"""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def shutdown(self, signum=None, frame=None):
        """Gracefully shutdown the scheduler"""
        emit("indexer_shutdown", signal=signum, **process_memory_mb())
        print("\nShutting down indexer...")
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        self.indexer.close()
        sys.exit(0)
    
    def run_indexer_job(self):
        """Job function to run indexing cycle"""
        self.cycle_number += 1
        started = time.monotonic()
        starting_memory = process_memory_mb()
        emit(
            "indexing_cycle_started",
            cycle=self.cycle_number,
            rss_mb=starting_memory["rss_mb"],
            peak_rss_mb=starting_memory["peak_rss_mb"],
        )
        print(f"\n{'='*60}")
        print("Running scheduled indexing job...")
        print(f"{'='*60}")
        success = False
        try:
            success = self.indexer.run_indexing_cycle()
        finally:
            emit(
                "indexing_cycle_finished",
                level="info" if success else "error",
                cycle=self.cycle_number,
                outcome="success" if success else "failed",
                duration_seconds=round(time.monotonic() - started, 3),
                **process_memory_mb(),
            )
            print(f"{'='*60}\n")
    
    def start(self):
        """Start the scheduler with configured interval"""
        interval_minutes = Config.CRON_INTERVAL_MINUTES
        emit(
            "indexer_process_started",
            interval_minutes=interval_minutes,
            database_type=Config.DATABASE_TYPE,
            parallel_processing=Config.ENABLE_PARALLEL_PROCESSING,
            parallel_workers=Config.PARALLEL_WORKERS if Config.ENABLE_PARALLEL_PROCESSING else 0,
            **process_memory_mb(),
        )
        
        print("XRPL Indexer Scheduler Started")
        print(f"{'='*60}")
        print("Configuration:")
        print(f"  - XRPL RPC URL: {Config.XRPL_JSON_RPC_URL}")
        print(f"  - Database Type: {Config.DATABASE_TYPE}")
        print(f"  - Cron Interval: Every {interval_minutes} minute(s)")
        
        if Config.ENABLE_PARALLEL_PROCESSING:
            print(f"  - Parallel Processing: ENABLED ({Config.PARALLEL_WORKERS} workers)")
        else:
            print("  - Parallel Processing: DISABLED (sequential mode)")
        
        if Config.get_filter_transaction_types():
            print(f"  - Transaction Type Filter: {', '.join(Config.get_filter_transaction_types())}")
        
        if Config.get_filter_addresses():
            print(f"  - Address Filter: {', '.join(Config.get_filter_addresses())}")
        
        if Config.get_filter_source_tags():
            print(f"  - Source Tag Filter: {', '.join(map(str, Config.get_filter_source_tags()))}")
        
        print(f"{'='*60}\n")
        
        print("Running initial indexing cycle...")
        self.run_indexer_job()
        
        self.scheduler.add_job(
            self.run_indexer_job,
            trigger=CronTrigger(minute=f'*/{interval_minutes}'),
            id='indexer_job',
            name='XRPL Indexer Job',
            replace_existing=True
        )
        
        print(f"Scheduled to run every {interval_minutes} minute(s)")
        print("Press Ctrl+C to stop\n")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.shutdown()
