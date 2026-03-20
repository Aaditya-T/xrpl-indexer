from scheduler import IndexerScheduler

def main():
    """Start the XRPL Indexer with scheduled cron jobs"""
    scheduler = IndexerScheduler()
    scheduler.start()

if __name__ == "__main__":
    main()
