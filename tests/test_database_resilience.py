import psycopg2
import pytest

from database import Database
from indexer import XRPLIndexer


def test_sqlite_database_reconnects_after_closed_handle(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")
    db.update_last_processed_ledger_index(123)

    db.conn.close()

    assert db.get_last_processed_ledger_index() == 123
    db.close()


def test_indexer_reconnects_and_retries_once_on_database_connection_error():
    class FlakyDB:
        def __init__(self):
            self.get_last_processed_calls = 0
            self.reconnect_calls = 0

        def ensure_connection(self):
            pass

        def get_last_processed_ledger_index(self):
            self.get_last_processed_calls += 1
            if self.get_last_processed_calls == 1:
                raise psycopg2.InterfaceError("connection already closed")
            return 10

        def is_connection_error(self, error):
            return isinstance(error, psycopg2.InterfaceError)

        def reconnect(self):
            self.reconnect_calls += 1

    class FakeXRPLClient:
        def get_current_ledger_index(self):
            return 10

    db = FlakyDB()
    indexer = XRPLIndexer(db=db, xrpl_client=FakeXRPLClient(), central_wallet="")

    indexer.run_indexing_cycle()

    assert db.reconnect_calls == 1
    assert db.get_last_processed_calls == 2


def test_parallel_processing_preserves_database_connection_errors():
    class FakeDB:
        def is_connection_error(self, error):
            return isinstance(error, psycopg2.InterfaceError)

    class FakeXRPLClient:
        pass

    indexer = XRPLIndexer(db=FakeDB(), xrpl_client=FakeXRPLClient(), central_wallet="")

    def fail_with_closed_connection(_ledger_index):
        raise psycopg2.InterfaceError("connection already closed")

    indexer.process_ledger = fail_with_closed_connection

    with pytest.raises(psycopg2.InterfaceError):
        indexer.process_ledgers_parallel([1])


def test_stale_trustline_delete_does_not_remove_newer_state(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")

    db.upsert_trustline(
        account="rAccount",
        issuer="rIssuer",
        currency="USD",
        balance="10",
        limit_amount="100",
        limit_peer="0",
        authorized=False,
        peer_authorized=False,
        no_ripple=False,
        no_ripple_peer=False,
        freeze_flag=False,
        peer_freeze_flag=False,
        is_deleted=False,
        ledger_index=20,
    )

    db.delete_trustline("rAccount", "rIssuer", "USD", ledger_index=10)

    cursor = db.conn.cursor()
    cursor.execute("SELECT balance FROM trustlines WHERE account = ? AND issuer = ? AND currency = ?", ("rAccount", "rIssuer", "USD"))
    assert cursor.fetchone()["balance"] == "10"
    cursor.close()

    db.delete_trustline("rAccount", "rIssuer", "USD", ledger_index=20)
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) AS count FROM trustlines")
    assert cursor.fetchone()["count"] == 0
    cursor.close()
    db.close()


def test_stale_offer_delete_does_not_remove_newer_state(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")

    db.upsert_offer(
        account="rAccount",
        sequence=1,
        taker_gets_currency="USD",
        taker_gets_issuer="rIssuer",
        taker_gets_value="10",
        taker_pays_currency="XRP",
        taker_pays_issuer=None,
        taker_pays_value="1000000",
        expiry_iso=None,
        flags=0,
        quality="100000",
        ledger_index=20,
    )

    db.delete_offer("rAccount", 1, ledger_index=10)

    cursor = db.conn.cursor()
    cursor.execute("SELECT taker_gets_value FROM offers WHERE account = ? AND sequence = ?", ("rAccount", 1))
    assert cursor.fetchone()["taker_gets_value"] == "10"
    cursor.close()

    db.delete_offer("rAccount", 1, ledger_index=21)
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) AS count FROM offers")
    assert cursor.fetchone()["count"] == 0
    cursor.close()
    db.close()
