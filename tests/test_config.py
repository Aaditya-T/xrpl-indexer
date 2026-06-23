from config import _database_type_for_url


def test_database_type_follows_database_url_scheme():
    assert _database_type_for_url("sqlite:///xrpl_indexer.db") == "sqlite"
    assert _database_type_for_url("postgresql://user:pass@localhost/db") == "postgresql"
