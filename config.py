"""Configuration for XRPL Indexer"""
import os
from dotenv import load_dotenv

load_dotenv()


def _build_database_url() -> str:
    """Build DATABASE_URL from individual DB_* variables if DATABASE_URL is not set"""
    if os.getenv("DATABASE_URL"):
        return os.getenv("DATABASE_URL")  # type: ignore

    db_type = os.getenv("DATABASE_TYPE", "postgresql")
    if db_type == "sqlite":
        return "sqlite:///xrpl_indexer.db"

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    name = os.getenv("DB_NAME", "")

    if not all([user, password, name]):
        return "sqlite:///xrpl_indexer.db"

    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


class Config:
    XRPL_JSON_RPC_URL = os.getenv("XRPL_JSON_RPC_URL", "https://s1.ripple.com:51234/")

    DATABASE_TYPE = os.getenv("DATABASE_TYPE", "postgresql")
    DATABASE_URL = _build_database_url()
    
    CRON_INTERVAL_MINUTES = int(os.getenv("CRON_INTERVAL_MINUTES", "5"))
    
    ENABLE_PARALLEL_PROCESSING = os.getenv("ENABLE_PARALLEL_PROCESSING", "false").lower() == "true"
    PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", "5"))
    
    FILTER_TRANSACTION_TYPES = os.getenv("FILTER_TRANSACTION_TYPES", "")
    FILTER_ADDRESSES = os.getenv("FILTER_ADDRESSES", "")
    FILTER_SOURCE_TAGS = os.getenv("FILTER_SOURCE_TAGS", "")
    
    @staticmethod
    def get_filter_transaction_types():
        """Returns list of transaction types to filter, or empty list for all"""
        if Config.FILTER_TRANSACTION_TYPES:
            return [t.strip() for t in Config.FILTER_TRANSACTION_TYPES.split(",")]
        return []
    
    @staticmethod
    def get_filter_addresses():
        """Returns list of addresses to filter, or empty list for all"""
        if Config.FILTER_ADDRESSES:
            return [a.strip() for a in Config.FILTER_ADDRESSES.split(",")]
        return []
    
    @staticmethod
    def get_filter_source_tags():
        """Returns list of source tags to filter, or empty list for all"""
        if Config.FILTER_SOURCE_TAGS:
            return [int(t.strip()) for t in Config.FILTER_SOURCE_TAGS.split(",")]
        return []
