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


def _database_type_for_url(db_url: str) -> str:
    if db_url.startswith("sqlite:///"):
        return "sqlite"
    if db_url.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    configured_type = os.getenv("DATABASE_TYPE")
    if configured_type:
        return configured_type
    return "postgresql"


_DATABASE_URL = _build_database_url()


class Config:
    XRPL_JSON_RPC_URL = os.getenv("XRPL_JSON_RPC_URL", "https://s1.ripple.com:51234/")

    DATABASE_URL = _DATABASE_URL
    DATABASE_TYPE = _database_type_for_url(_DATABASE_URL)

    CRON_INTERVAL_MINUTES = int(os.getenv("CRON_INTERVAL_MINUTES", "5"))

    ENABLE_PARALLEL_PROCESSING = os.getenv("ENABLE_PARALLEL_PROCESSING", "false").lower() == "true"
    PARALLEL_WORKERS = int(os.getenv("PARALLEL_WORKERS", "5"))

    FILTER_TRANSACTION_TYPES = os.getenv("FILTER_TRANSACTION_TYPES", "")
    FILTER_ADDRESSES = os.getenv("FILTER_ADDRESSES", "")
    FILTER_SOURCE_TAGS = os.getenv("FILTER_SOURCE_TAGS", "")

    # Hub-and-spoke wallet tracking: address of the central wallet that funds user wallets
    CENTRAL_WALLET_ADDRESS = os.getenv("CENTRAL_WALLET_ADDRESS", "")
    # Additional funding wallets. The legacy central wallet remains supported.
    PARENT_WALLET_ADDRESSES = os.getenv("PARENT_WALLET_ADDRESSES", "")
    # Opt-in discovery rules, independent of the transaction storage filters.
    TRACK_SOURCE_TAGS = os.getenv("TRACK_SOURCE_TAGS", "")

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

    @staticmethod
    def get_parent_wallet_addresses():
        """Return the legacy central wallet and any additional parent wallets."""
        addresses = [Config.CENTRAL_WALLET_ADDRESS, *Config.PARENT_WALLET_ADDRESSES.split(",")]
        return list(dict.fromkeys(address.strip() for address in addresses if address.strip()))

    @staticmethod
    def get_track_source_tags():
        """Return the source tags that enroll wallets and retain matching txns."""
        if not Config.TRACK_SOURCE_TAGS:
            return []
        try:
            tags = [int(tag.strip()) for tag in Config.TRACK_SOURCE_TAGS.split(",")]
        except ValueError as exc:
            raise ValueError(
                "TRACK_SOURCE_TAGS must be comma-separated unsigned 32-bit integers"
            ) from exc
        if any(tag < 0 or tag > 0xFFFFFFFF for tag in tags):
            raise ValueError(
                "TRACK_SOURCE_TAGS must be comma-separated unsigned 32-bit integers"
            )
        return tags
