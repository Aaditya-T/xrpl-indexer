"""Configuration for XRPL Indexer"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    XRPL_JSON_RPC_URL = os.getenv("XRPL_JSON_RPC_URL", "https://s1.ripple.com:51234/")
    
    DATABASE_TYPE = os.getenv("DATABASE_TYPE", "postgresql")
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///xrpl_indexer.db")
    
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
