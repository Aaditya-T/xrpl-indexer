"""Offline end-to-end coverage for parent and source-tag wallet discovery."""

import time

import pytest

from config import Config
from database import Database
from indexer import XRPLIndexer


HUB = "rHub"
PARENT = "rSecondParent"
EXTERNAL = "rExternalFunder"
CHILD_PARENT = "rParentChild"
CHILD_TAG = "rTaggedChild"
CHILD_OTHER = "rOtherChild"
CHILD_EXISTING = "rExistingChild"


def _payment(funder, destination, tx_hash, ledger_index, tag=None, created=True):
    tx_json = {
        "TransactionType": "Payment",
        "Account": funder,
        "Destination": destination,
        "Amount": "10000000",
        "Fee": "12",
    }
    if tag is not None:
        tx_json["SourceTag"] = tag
    account_fields = {
        "Account": destination,
        "Balance": "10000000",
        "Sequence": 1,
        "OwnerCount": 0,
        "Flags": 0,
    }
    node = (
        {"CreatedNode": {"LedgerEntryType": "AccountRoot", "NewFields": account_fields}}
        if created else
        {"ModifiedNode": {"LedgerEntryType": "AccountRoot", "FinalFields": account_fields}}
    )
    return {
        "tx_json": tx_json,
        "hash": tx_hash,
        "ledger_index": ledger_index,
        "meta": {"TransactionResult": "tesSUCCESS", "AffectedNodes": [node]},
    }


def _account_update(account, tx_hash, ledger_index):
    return {
        "tx_json": {"TransactionType": "AccountSet", "Account": account, "Fee": "12"},
        "hash": tx_hash,
        "ledger_index": ledger_index,
        "meta": {
            "TransactionResult": "tesSUCCESS",
            "AffectedNodes": [{"ModifiedNode": {
                "LedgerEntryType": "AccountRoot",
                "FinalFields": {
                    "Account": account,
                    "Balance": "9999988",
                    "Sequence": 2,
                    "OwnerCount": 0,
                    "Flags": 0,
                },
            }}],
        },
    }


def _store_raw(db, raw):
    db.insert_transaction({
        **raw["tx_json"],
        "hash": raw["hash"],
        "ledger_index": raw["ledger_index"],
        "_full_data": raw,
    })


class FakeXRPLClient:
    def __init__(self, ledgers, delayed_ledger=None):
        self.ledgers = ledgers
        self.delayed_ledger = delayed_ledger

    def get_ledger_with_transactions(self, ledger_index):
        if ledger_index == self.delayed_ledger:
            time.sleep(0.05)
        return self.ledgers[ledger_index], None


def test_config_combines_legacy_and_additional_parents(monkeypatch):
    monkeypatch.setattr(Config, "CENTRAL_WALLET_ADDRESS", HUB)
    monkeypatch.setattr(Config, "PARENT_WALLET_ADDRESSES", f" {PARENT}, {HUB}, ")
    monkeypatch.setattr(Config, "TRACK_SOURCE_TAGS", "0, 123")
    assert Config.get_parent_wallet_addresses() == [HUB, PARENT]
    assert Config.get_track_source_tags() == [0, 123]


@pytest.mark.parametrize("tags", ["-1", "4294967296", "123,", "abc"])
def test_invalid_tracking_tags_fail_at_startup(monkeypatch, tags):
    monkeypatch.setattr(Config, "TRACK_SOURCE_TAGS", tags)
    with pytest.raises(ValueError, match="TRACK_SOURCE_TAGS"):
        Config.get_track_source_tags()


def test_indexer_loads_discovery_rules_from_config(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "CENTRAL_WALLET_ADDRESS", HUB)
    monkeypatch.setattr(Config, "PARENT_WALLET_ADDRESSES", PARENT)
    monkeypatch.setattr(Config, "TRACK_SOURCE_TAGS", "0,123")
    db = Database(db_url=f"sqlite:///{tmp_path / 'config.db'}", db_type="sqlite")
    try:
        idx = XRPLIndexer(db=db, xrpl_client=FakeXRPLClient({}))
        assert idx.parent_wallets == {HUB, PARENT}
        assert idx.track_source_tags == {0, 123}
    finally:
        db.close()


def test_parent_or_configured_tag_enrolls_wallet_and_tagged_tx_is_stored(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'tracking.db'}", db_type="sqlite")
    try:
        client = FakeXRPLClient({1: [
            _payment(PARENT, CHILD_PARENT, "parent-activation", 1),
            _payment(EXTERNAL, CHILD_TAG, "tag-activation", 1, tag=123),
            _payment(EXTERNAL, CHILD_OTHER, "wrong-tag", 1, tag=456),
        ], 2: [
            _account_update(CHILD_PARENT, "parent-child-update", 2),
            _account_update(CHILD_TAG, "tag-child-update", 2),
        ]})
        idx = XRPLIndexer(
            db=db, xrpl_client=client, central_wallet=HUB,
            parent_wallets=[PARENT], track_source_tags=[123],
        )
        idx.filter_tx_types = ["OfferCreate"]
        idx.filter_addresses = [HUB]
        idx.filter_source_tags = [999]

        assert idx.process_ledger(1) == 1
        assert idx.process_ledger(2) == 0
        assert db.is_tracked_wallet(CHILD_PARENT)
        assert db.is_tracked_wallet(CHILD_TAG)
        assert not db.is_tracked_wallet(CHILD_OTHER)

        cur = db.conn.execute("SELECT transaction_hash FROM transactions")
        assert [row[0] for row in cur.fetchall()] == ["tag-activation"]
        cur.close()
        cur = db.conn.execute("SELECT sequence FROM account_states WHERE address = ?", (CHILD_TAG,))
        assert cur.fetchone()[0] == 2
        cur.close()
        cur = db.conn.execute("SELECT sequence FROM account_states WHERE address = ?", (CHILD_PARENT,))
        assert cur.fetchone()[0] == 2
        cur.close()
    finally:
        db.close()


def test_retroactive_discovery_uses_all_parents_and_configured_tags(tmp_path):
    db = Database(db_url=f"sqlite:///{tmp_path / 'retro.db'}", db_type="sqlite")
    try:
        _store_raw(db, _payment(PARENT, CHILD_PARENT, "stored-parent", 1))
        _store_raw(db, _payment(EXTERNAL, CHILD_TAG, "stored-tag", 2, tag=123))
        _store_raw(db, _payment(EXTERNAL, CHILD_OTHER, "stored-other", 3, tag=456))
        _store_raw(db, _payment(EXTERNAL, CHILD_EXISTING, "stored-existing", 4, tag=123, created=False))
        first_page = db.get_payments_for_discovery({PARENT}, {123}, limit=1)
        second_page = db.get_payments_for_discovery(
            {PARENT}, {123}, after_id=first_page[0]["id"], limit=1
        )
        assert first_page[0]["tx_hash"] == "stored-parent"
        assert second_page[0]["tx_hash"] == "stored-tag"
        idx = XRPLIndexer(
            db=db, xrpl_client=FakeXRPLClient({}), central_wallet=HUB,
            parent_wallets=[PARENT], track_source_tags=[123],
        )
        assert idx.db.is_tracked_wallet(CHILD_PARENT)
        assert idx.db.is_tracked_wallet(CHILD_TAG)
        assert not idx.db.is_tracked_wallet(CHILD_OTHER)
        assert not idx.db.is_tracked_wallet(CHILD_EXISTING)
    finally:
        db.close()


def test_parallel_fetch_applies_activation_before_later_state(tmp_path, monkeypatch):
    monkeypatch.setattr(Config, "PARALLEL_WORKERS", 2)
    db = Database(db_url=f"sqlite:///{tmp_path / 'ordered.db'}", db_type="sqlite")
    try:
        client = FakeXRPLClient({
            1: [_payment(EXTERNAL, CHILD_TAG, "activation", 1, tag=123)],
            2: [_account_update(CHILD_TAG, "later-update", 2)],
        }, delayed_ledger=1)
        idx = XRPLIndexer(
            db=db, xrpl_client=client, central_wallet="",
            parent_wallets=[], track_source_tags=[123],
        )
        idx.process_ledgers_parallel([1, 2])
        cur = db.conn.execute("SELECT sequence FROM account_states WHERE address = ?", (CHILD_TAG,))
        assert cur.fetchone()[0] == 2
        cur.close()
    finally:
        db.close()
