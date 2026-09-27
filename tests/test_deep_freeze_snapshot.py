import sqlite3

import pytest

from database import Database
from ops.backfill_deep_freeze import backfill
from state_processor import StateProcessor


def _trustline(db, account="rLow", peer="rHigh", currency="USD", balance="0.00",
               deep=False, peer_deep=True, ledger=10):
    db.upsert_trustline(
        account=account, issuer=peer, currency=currency, balance=balance,
        limit_amount="100", limit_peer="0", authorized=False,
        peer_authorized=False, no_ripple=False, no_ripple_peer=False,
        freeze_flag=False, peer_freeze_flag=False,
        deep_freeze_flag=deep, peer_deep_freeze_flag=peer_deep,
        is_deleted=False, ledger_index=ledger,
    )


def test_deep_freeze_flags_follow_account_perspective():
    class CaptureDB:
        def __init__(self):
            self.rows = {}

        def is_tracked_wallet(self, _address):
            return True

        def upsert_trustline(self, **kwargs):
            self.rows[kwargs["account"]] = kwargs

    db = CaptureDB()
    fields = {
        "HighLimit": {"issuer": "rHigh", "currency": "USD", "value": "0"},
        "LowLimit": {"issuer": "rLow", "currency": "USD", "value": "100"},
        "Balance": {"value": "0"},
        "Flags": 0x02000000 | 0x00800000,
    }
    processor = StateProcessor(db)
    processor._ripple_state(fields, 10, deleted=False)
    assert db.rows["rLow"]["deep_freeze_flag"] is True
    assert db.rows["rLow"]["peer_deep_freeze_flag"] is False
    assert db.rows["rHigh"]["deep_freeze_flag"] is False
    assert db.rows["rHigh"]["peer_deep_freeze_flag"] is True
    assert db.rows["rLow"]["peer_freeze_flag"] is True

    fields["Flags"] = 0x04000000
    processor._ripple_state(fields, 11, deleted=False)
    assert db.rows["rLow"]["deep_freeze_flag"] is False
    assert db.rows["rLow"]["peer_deep_freeze_flag"] is True
    assert db.rows["rHigh"]["deep_freeze_flag"] is True
    assert db.rows["rHigh"]["peer_deep_freeze_flag"] is False


def test_legacy_schema_migrates_without_guessing_flags(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE trustlines (account TEXT, issuer TEXT, currency TEXT, "
                 "balance TEXT, is_deleted INTEGER, ledger_index INTEGER, "
                 "PRIMARY KEY (account, issuer, currency))")
    conn.execute("INSERT INTO trustlines VALUES ('rLow', 'rHigh', 'USD', '0', 0, 10)")
    conn.execute("INSERT INTO trustlines VALUES ('rOld', 'rPeer', 'EUR', '0', 1, 9)")
    conn.commit()
    conn.close()

    db = Database(db_url=f"sqlite:///{path}", db_type="sqlite")
    assert db.get_trustlines_missing_deep_freeze() == [
        {"account": "rLow", "issuer": "rHigh", "currency": "USD", "ledger_index": 10}
    ]
    db.close()


def test_backfill_uses_exact_ledger_and_updates_zero_balance_line(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")
    db.update_last_processed_ledger_index(20)
    _trustline(db, ledger=10)
    db.conn.execute("UPDATE trustlines SET deep_freeze_flag = NULL, peer_deep_freeze_flag = NULL")
    db.conn.commit()
    db.insert_transaction({
        "ledger_index": 10, "hash": "stored-trustset", "TransactionType": "TrustSet",
        "Account": "rLow", "_full_data": {"meta": {"AffectedNodes": [{
            "ModifiedNode": {"LedgerEntryType": "RippleState", "FinalFields": {
                "Flags": 0x04000000,
                "HighLimit": {"issuer": "rHigh", "currency": "USD"},
                "LowLimit": {"issuer": "rLow", "currency": "USD"},
            }}
        }]}}
    })
    assert db.get_stored_ripple_state("rLow", "rHigh", "USD", 10)["Flags"] == 0x04000000

    class Client:
        def get_ripple_state(self, account, peer, currency, ledger_index):
            assert (account, peer, currency, ledger_index) == ("rLow", "rHigh", "USD", 20)
            return {"Flags": 0x02000000, "HighLimit": {"issuer": "rHigh"},
                    "LowLimit": {"issuer": "rLow"}}

    assert backfill(db, Client()) == (1, 1)
    assert db.get_trustlines_missing_deep_freeze() == []
    row = db.conn.execute("SELECT deep_freeze_flag, peer_deep_freeze_flag, balance "
                          "FROM trustlines").fetchone()
    assert tuple(row) == (1, 0, "0.00")
    assert backfill(db, Client()) == (0, 0)
    db.close()


def test_backfill_does_not_guess_when_history_is_unavailable(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")
    db.update_last_processed_ledger_index(20)
    _trustline(db, ledger=10)
    db.conn.execute("UPDATE trustlines SET deep_freeze_flag = NULL, peer_deep_freeze_flag = NULL")
    db.conn.commit()

    class MissingHistory:
        def get_ripple_state(self, *_args):
            raise RuntimeError("lgrNotFound")

    with pytest.raises(RuntimeError, match="lgrNotFound"):
        backfill(db, MissingHistory())
    assert len(db.get_trustlines_missing_deep_freeze()) == 1
    db.close()


def test_ledger_transaction_publishes_state_and_marker_together(tmp_path, monkeypatch):
    import api

    url = f"sqlite:///{tmp_path / 'indexer.db'}"
    db = Database(db_url=url, db_type="sqlite")
    db.update_last_processed_ledger_index(9)
    db.upsert_account_state("rLow", 100, 1, 0, 1, 9)
    monkeypatch.setattr(api.Config, "DATABASE_TYPE", "sqlite")
    monkeypatch.setattr(api.Config, "DATABASE_URL", url)

    with pytest.raises(RuntimeError, match="interrupted"):
        with db.ledger_transaction(10):
            db.upsert_account_state("rLow", 200, 2, 1, 2, 10)
            _trustline(db)
            before_commit = api.account_snapshot("rLow")
            assert before_commit["account"]["flags"] == 1
            assert before_commit["trustlines"] == []
            assert before_commit["snapshot_ledger_index"] == 9
            raise RuntimeError("interrupted")

    assert api.account_snapshot("rLow")["snapshot_ledger_index"] == 9
    with db.ledger_transaction(10):
        db.upsert_account_state("rLow", 200, 2, 1, 2, 10)
        _trustline(db)
    snapshot = api.AccountSnapshotResponse.model_validate(api.account_snapshot("rLow"))
    assert snapshot.snapshot_ledger_index == 10
    assert snapshot.indexed_at is not None
    assert snapshot.account.flags == 2
    assert len(snapshot.trustlines) == 1
    assert snapshot.trustlines[0].balance == "0.00"
    assert snapshot.trustlines[0].peer_deep_freeze_flag is True
    assert api.account_balances("rLow", include_xrp=False, include_zero=False)["balances"] == []
    assert len(api.account_balances("rLow", include_xrp=False, include_zero=True)["balances"]) == 1
    assert api.account_info("rLow")["snapshot_ledger_index"] == 10
    db.close()


def test_issuer_snapshot_includes_zero_balance_trustlines(tmp_path, monkeypatch):
    import api

    url = f"sqlite:///{tmp_path / 'indexer.db'}"
    db = Database(db_url=url, db_type="sqlite")
    db.upsert_account_state("rIssuer", 100, 3, 2, 0x800000, 15)
    _trustline(db, account="rHolder", peer="rIssuer", ledger=15)
    db.update_last_processed_ledger_index(15)
    monkeypatch.setattr(api.Config, "DATABASE_TYPE", "sqlite")
    monkeypatch.setattr(api.Config, "DATABASE_URL", url)

    response = api.IssuerSnapshotResponse.model_validate(api.issuer_snapshot("rIssuer"))
    assert response.snapshot_ledger_index == 15
    assert response.indexed_at is not None
    assert response.account.flags == 0x800000
    assert len(response.trustlines) == 1
    assert response.trustlines[0].account == "rHolder"
    assert response.trustlines[0].balance == "0.00"
    assert response.trustlines[0].peer_deep_freeze_flag is True
    db.close()


def test_indexer_rolls_back_failed_ledger_and_preserves_watermark(tmp_path):
    from indexer import XRPLIndexer

    db = Database(db_url=f"sqlite:///{tmp_path / 'indexer.db'}", db_type="sqlite")
    db.add_tracked_wallet("rLow", "activation")
    db.update_last_processed_ledger_index(9)
    indexer = XRPLIndexer(
        db=db, xrpl_client=object(), central_wallet="rLow", parent_wallets=[],
        track_source_tags=[],
    )
    account_node = {"ModifiedNode": {
        "LedgerEntryType": "AccountRoot",
        "FinalFields": {"Account": "rLow", "Balance": "100", "Sequence": 2,
                        "OwnerCount": 0, "Flags": 1},
    }}
    activation_node = {"CreatedNode": {
        "LedgerEntryType": "AccountRoot",
        "NewFields": {"Account": "rNew", "Balance": "100", "Sequence": 1,
                      "OwnerCount": 0, "Flags": 0},
    }}
    invalid_line = {"ModifiedNode": {
        "LedgerEntryType": "RippleState",
        "FinalFields": {"Flags": "invalid"},
    }}
    transactions = [
        {"tx_json": {"TransactionType": "Payment", "Account": "rLow",
                     "Destination": "rNew"},
         "hash": "first", "meta": {"AffectedNodes": [account_node, activation_node]}},
        {"tx_json": {"TransactionType": "TrustSet", "Account": "rLow"},
         "hash": "second", "meta": {"AffectedNodes": [invalid_line]}},
    ]
    with pytest.raises(ValueError):
        indexer.process_ledger(10, (transactions, None))
    assert db.get_last_processed_ledger_index() == 9
    assert db.get_transaction_count() == 0
    assert db.conn.execute("SELECT COUNT(*) FROM account_states").fetchone()[0] == 0
    assert not db.is_tracked_wallet("rNew")
    db.close()
