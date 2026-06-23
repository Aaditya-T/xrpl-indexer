import base64

from database import Database


def test_sync_cursor_supports_legacy_and_composite_formats():
    import api

    legacy = base64.urlsafe_b64encode(b"7").decode()
    assert api._decode_cursor(legacy) == (None, 7)

    composite = api._encode_cursor(ledger_index=10, row_id=7)
    assert api._decode_cursor(composite) == (10, 7)


def test_sync_transactions_uses_ledger_order_cursor(monkeypatch, tmp_path):
    import api

    db_path = tmp_path / "indexer.db"
    db_url = f"sqlite:///{db_path}"
    db = Database(db_url=db_url, db_type="sqlite")
    db.insert_transaction({
        "ledger_index": 20,
        "hash": "TX_LEDGER_20",
        "TransactionType": "Payment",
        "Account": "rSender",
    })
    db.insert_transaction({
        "ledger_index": 10,
        "hash": "TX_LEDGER_10",
        "TransactionType": "Payment",
        "Account": "rSender",
    })

    monkeypatch.setattr(api.Config, "DATABASE_TYPE", "sqlite")
    monkeypatch.setattr(api.Config, "DATABASE_URL", db_url)

    first_page = api.sync_transactions(after_ledger=None, cursor=None, limit=1, include_full=False)
    assert first_page["has_more"] is True
    assert first_page["data"][0]["ledger_index"] == 10

    second_page = api.sync_transactions(
        after_ledger=None,
        cursor=first_page["next_cursor"],
        limit=1,
        include_full=False,
    )
    assert second_page["has_more"] is False
    assert second_page["data"][0]["ledger_index"] == 20

    db.close()
