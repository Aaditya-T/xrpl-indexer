"""
Test suite for state_processor.py and hub-and-spoke wallet discovery.

Uses a MockDB and MockIndexer — no real database or XRPL node needed.
All AffectedNodes payloads are synthetic.
"""
from __future__ import annotations

import pytest
from typing import Optional
from state_processor import StateProcessor, RIPPLE_EPOCH_OFFSET
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Fixed test addresses
# ---------------------------------------------------------------------------
CENTRAL   = "rCENTRALfundingWallet111111111111111"
USER_A    = "rUSERaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
USER_B    = "rUSERbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
GATEWAY   = "rGATEWAYissuerXXXXXXXXXXXXXXXXXXXX"
UNTRACKED = "rUNTRACKEDwalletYYYYYYYYYYYYYYYYYY"


# ---------------------------------------------------------------------------
# Mock database
# ---------------------------------------------------------------------------
class MockDB:
    """In-memory stand-in for the real Database class."""

    def __init__(self, tracked: Optional[set[str]] = None):
        self._tracked: set[str] = set(tracked or [])
        self.account_states: dict[str, dict] = {}          # address → dict
        self.trustlines: dict[tuple, dict] = {}            # (account, issuer, currency) → dict
        self.offers: dict[tuple, dict] = {}                # (account, sequence) → dict
        self.deleted_offers: set[tuple] = set()            # (account, sequence)
        self.added_wallets: list[tuple] = []               # [(address, tx_hash)]

    # -- wallet tracking --------------------------------------------------

    def is_tracked_wallet(self, address: str) -> bool:
        return address in self._tracked

    def add_tracked_wallet(self, address: str, tx_hash: str) -> bool:
        if address in self._tracked:
            return False
        self._tracked.add(address)
        self.added_wallets.append((address, tx_hash))
        return True

    # -- account states ---------------------------------------------------

    def upsert_account_state(
        self, address, balance_drops, sequence, owner_count, flags, ledger_index
    ):
        existing = self.account_states.get(address)
        if existing and (existing.get("ledger_index") or 0) > ledger_index:
            return
        self.account_states[address] = {
            "address": address,
            "balance_drops": balance_drops,
            "sequence": sequence,
            "owner_count": owner_count,
            "flags": flags,
            "ledger_index": ledger_index,
        }

    # -- trustlines -------------------------------------------------------

    def upsert_trustline(
        self, account, issuer, currency, balance, limit_amount, limit_peer,
        authorized, peer_authorized, no_ripple, no_ripple_peer,
        freeze, peer_freeze, is_deleted, ledger_index,
    ):
        key = (account, issuer, currency)
        existing = self.trustlines.get(key)
        if existing and (existing.get("ledger_index") or 0) > ledger_index:
            return
        self.trustlines[key] = {
            "account": account, "issuer": issuer, "currency": currency,
            "balance": balance, "limit_amount": limit_amount, "limit_peer": limit_peer,
            "authorized": authorized, "peer_authorized": peer_authorized,
            "no_ripple": no_ripple, "no_ripple_peer": no_ripple_peer,
            "freeze": freeze, "peer_freeze": peer_freeze,
            "is_deleted": is_deleted, "ledger_index": ledger_index,
        }

    # -- offers -----------------------------------------------------------

    def upsert_offer(
        self, account, sequence, taker_gets_currency, taker_gets_issuer, taker_gets_value,
        taker_pays_currency, taker_pays_issuer, taker_pays_value,
        expiry_iso, flags, quality, ledger_index,
    ):
        key = (account, sequence)
        existing = self.offers.get(key)
        if existing and (existing.get("ledger_index") or 0) > ledger_index:
            return
        self.offers[key] = {
            "account": account, "sequence": sequence,
            "taker_gets_currency": taker_gets_currency,
            "taker_gets_issuer": taker_gets_issuer,
            "taker_gets_value": taker_gets_value,
            "taker_pays_currency": taker_pays_currency,
            "taker_pays_issuer": taker_pays_issuer,
            "taker_pays_value": taker_pays_value,
            "expiry_iso": expiry_iso, "flags": flags, "quality": quality,
            "ledger_index": ledger_index,
        }

    def delete_offer(self, account: str, sequence: int):
        key = (account, sequence)
        self.offers.pop(key, None)
        self.deleted_offers.add(key)


# ---------------------------------------------------------------------------
# Mock indexer (for wallet discovery tests)
# ---------------------------------------------------------------------------
class MockIndexer:
    """Minimal stand-in that exercises the wallet-discovery logic."""

    def __init__(self, db: MockDB, central_wallet: str = CENTRAL):
        self.db = db
        self.central_wallet = central_wallet

    def _check_wallet_discovery(self, tx_data: dict, tx_hash: str):
        if not self.central_wallet:
            return
        if tx_data.get("TransactionType") != "Payment":
            return
        if tx_data.get("Account") != self.central_wallet:
            return
        destination = tx_data.get("Destination")
        if not destination:
            return
        self.db.add_tracked_wallet(destination, tx_hash)


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _make_tx(affected_nodes: list, tx_json: Optional[dict] = None) -> dict:
    """Wrap AffectedNodes into the structure that StateProcessor expects."""
    return {
        "_full_data": {
            "meta": {"AffectedNodes": affected_nodes, "TransactionResult": "tesSUCCESS"},
            "tx_json": tx_json or {},
        }
    }


def _created(entry_type: str, new_fields: dict) -> dict:
    return {"CreatedNode": {"LedgerEntryType": entry_type, "NewFields": new_fields}}


def _modified(entry_type: str, final_fields: dict, previous_fields: Optional[dict] = None) -> dict:
    node: dict = {"LedgerEntryType": entry_type, "FinalFields": final_fields}
    if previous_fields:
        node["PreviousFields"] = previous_fields
    return {"ModifiedNode": node}


def _deleted(entry_type: str, final_fields: dict) -> dict:
    return {"DeletedNode": {"LedgerEntryType": entry_type, "FinalFields": final_fields}}


def _ripple_state_fields(
    high: str, low: str, currency: str, balance: str,
    high_limit: str = "0", low_limit: str = "1000",
    flags: int = 0,
) -> dict:
    return {
        "HighLimit": {"issuer": high, "currency": currency, "value": high_limit},
        "LowLimit":  {"issuer": low,  "currency": currency, "value": low_limit},
        "Balance":   {"currency": currency, "issuer": high, "value": balance},
        "Flags": flags,
    }


# ---------------------------------------------------------------------------
# Tests: wallet discovery
# ---------------------------------------------------------------------------

class TestWalletDiscovery:
    def test_payment_from_central_adds_destination(self):
        db = MockDB()
        idx = MockIndexer(db)
        idx._check_wallet_discovery(
            {"TransactionType": "Payment", "Account": CENTRAL, "Destination": USER_A},
            "TXHASH001",
        )
        assert USER_A in db._tracked
        assert ("USER_A" not in db._tracked)  # exact match only
        assert db.added_wallets == [(USER_A, "TXHASH001")]

    def test_non_payment_is_ignored(self):
        db = MockDB()
        idx = MockIndexer(db)
        idx._check_wallet_discovery(
            {"TransactionType": "OfferCreate", "Account": CENTRAL, "Destination": USER_A},
            "TXHASH002",
        )
        assert USER_A not in db._tracked

    def test_payment_from_non_central_ignored(self):
        db = MockDB()
        idx = MockIndexer(db)
        idx._check_wallet_discovery(
            {"TransactionType": "Payment", "Account": USER_B, "Destination": USER_A},
            "TXHASH003",
        )
        assert USER_A not in db._tracked

    def test_duplicate_payment_does_not_re_add(self):
        db = MockDB(tracked={USER_A})
        idx = MockIndexer(db)
        idx._check_wallet_discovery(
            {"TransactionType": "Payment", "Account": CENTRAL, "Destination": USER_A},
            "TXHASH004",
        )
        assert db.added_wallets == []  # add_tracked_wallet returned False → not recorded

    def test_empty_central_wallet_disables_discovery(self):
        db = MockDB()
        idx = MockIndexer(db, central_wallet="")
        idx._check_wallet_discovery(
            {"TransactionType": "Payment", "Account": "", "Destination": USER_A},
            "TXHASH005",
        )
        assert USER_A not in db._tracked


# ---------------------------------------------------------------------------
# Tests: AccountRoot → account_states
# ---------------------------------------------------------------------------

class TestAccountRoot:
    def test_initial_funding(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _created("AccountRoot", {
                "Account": USER_A,
                "Balance": "10000000",
                "Sequence": 1,
                "OwnerCount": 0,
                "Flags": 0,
            })
        ])
        sp.process_transaction(tx, ledger_index=1000)
        state = db.account_states[USER_A]
        assert state["balance_drops"] == 10_000_000
        assert state["sequence"] == 1
        assert state["owner_count"] == 0

    def test_balance_decrease(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "9000000", "Sequence": 2, "OwnerCount": 0, "Flags": 0},
                previous_fields={"Balance": "10000000", "Sequence": 1},
            )
        ])
        sp.process_transaction(tx, ledger_index=1001)
        assert db.account_states[USER_A]["balance_drops"] == 9_000_000
        assert db.account_states[USER_A]["sequence"] == 2

    def test_balance_increase(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        # First set initial state
        sp.process_transaction(_make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "9000000", "Sequence": 2, "OwnerCount": 0, "Flags": 0},
            )
        ]), ledger_index=1001)
        # Now increase
        sp.process_transaction(_make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "15000000", "Sequence": 2, "OwnerCount": 0, "Flags": 0},
            )
        ]), ledger_index=1002)
        assert db.account_states[USER_A]["balance_drops"] == 15_000_000

    def test_untracked_account_ignored(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _created("AccountRoot", {
                "Account": UNTRACKED,
                "Balance": "5000000",
                "Sequence": 1,
                "OwnerCount": 0,
                "Flags": 0,
            })
        ])
        sp.process_transaction(tx, ledger_index=1000)
        assert UNTRACKED not in db.account_states

    def test_stale_ledger_does_not_overwrite(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction(_make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "9000000", "Sequence": 5, "OwnerCount": 0, "Flags": 0},
            )
        ]), ledger_index=2000)
        # Older ledger arrives (e.g. parallel processing reorder)
        sp.process_transaction(_make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "8888888", "Sequence": 4, "OwnerCount": 0, "Flags": 0},
            )
        ]), ledger_index=1999)
        # Should keep the newer value
        assert db.account_states[USER_A]["balance_drops"] == 9_000_000


# ---------------------------------------------------------------------------
# Tests: RippleState → trustlines
# ---------------------------------------------------------------------------

class TestRippleState:
    # USER_A = low account, GATEWAY = high account in all fixtures
    # (arbitrary for testing; what matters is the flag parsing)

    def test_trustline_create(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _created("RippleState", _ripple_state_fields(
                high=GATEWAY, low=USER_A, currency="USD",
                balance="0", high_limit="0", low_limit="1000",
            ))
        ])
        sp.process_transaction(tx, ledger_index=2000)
        key = (USER_A, GATEWAY, "USD")
        assert key in db.trustlines
        tl = db.trustlines[key]
        assert tl["currency"] == "USD"
        assert tl["limit_amount"] == "1000"   # USER_A's limit (low)
        assert tl["limit_peer"] == "0"        # GATEWAY's limit (high)
        assert tl["is_deleted"] is False
        assert tl["balance"] == "0.0"

    def test_trustline_balance_change(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("RippleState",
                final_fields=_ripple_state_fields(
                    high=GATEWAY, low=USER_A, currency="USD",
                    balance="50.5", high_limit="0", low_limit="1000",
                )
            )
        ])
        sp.process_transaction(tx, ledger_index=2001)
        key = (USER_A, GATEWAY, "USD")
        # USER_A is low → positive balance means USER_A holds 50.5 USD
        assert db.trustlines[key]["balance"] == "50.5"

    def test_trustline_balance_negative_from_high_perspective(self):
        """When balance is positive, the HIGH account's perspective is negated."""
        db = MockDB(tracked={GATEWAY})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("RippleState",
                final_fields=_ripple_state_fields(
                    high=GATEWAY, low=USER_A, currency="USD",
                    balance="50.5",
                )
            )
        ])
        sp.process_transaction(tx, ledger_index=2001)
        key = (GATEWAY, USER_A, "USD")
        # GATEWAY is high → their view of balance = -(+50.5) = -50.5
        assert db.trustlines[key]["balance"] == "-50.5"

    def test_trustline_limit_change(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("RippleState",
                final_fields=_ripple_state_fields(
                    high=GATEWAY, low=USER_A, currency="USD",
                    balance="100", high_limit="0", low_limit="5000",
                )
            )
        ])
        sp.process_transaction(tx, ledger_index=2002)
        assert db.trustlines[(USER_A, GATEWAY, "USD")]["limit_amount"] == "5000"

    def test_trustline_freeze_set_by_high(self):
        # lsfHighFreeze = 0x00800000
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="EUR", balance="10")
        fields["Flags"] = 0x00800000  # lsfHighFreeze
        tx = _make_tx([_modified("RippleState", final_fields=fields)])
        sp.process_transaction(tx, ledger_index=2003)
        tl = db.trustlines[(USER_A, GATEWAY, "EUR")]
        # From USER_A (low) perspective: high has frozen → peer_freeze
        assert tl["peer_freeze"] is True
        assert tl["freeze"] is False

    def test_trustline_freeze_set_by_low(self):
        # lsfLowFreeze = 0x00400000
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="EUR", balance="10")
        fields["Flags"] = 0x00400000  # lsfLowFreeze
        tx = _make_tx([_modified("RippleState", final_fields=fields)])
        sp.process_transaction(tx, ledger_index=2004)
        tl = db.trustlines[(USER_A, GATEWAY, "EUR")]
        assert tl["freeze"] is True
        assert tl["peer_freeze"] is False

    def test_trustline_freeze_cleared(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields_frozen = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="EUR", balance="10")
        fields_frozen["Flags"] = 0x00400000
        sp.process_transaction(_make_tx([_modified("RippleState", final_fields=fields_frozen)]), ledger_index=2005)
        assert db.trustlines[(USER_A, GATEWAY, "EUR")]["freeze"] is True

        fields_clear = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="EUR", balance="10")
        fields_clear["Flags"] = 0
        sp.process_transaction(_make_tx([_modified("RippleState", final_fields=fields_clear)]), ledger_index=2006)
        assert db.trustlines[(USER_A, GATEWAY, "EUR")]["freeze"] is False

    def test_trustline_no_ripple_set(self):
        # lsfLowNoRipple = 0x00100000
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="USD", balance="0")
        fields["Flags"] = 0x00100000
        tx = _make_tx([_modified("RippleState", final_fields=fields)])
        sp.process_transaction(tx, ledger_index=2007)
        tl = db.trustlines[(USER_A, GATEWAY, "USD")]
        assert tl["no_ripple"] is True
        assert tl["no_ripple_peer"] is False

    def test_trustline_authorized_by_low(self):
        # lsfLowAuth = 0x00040000
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="USD", balance="0")
        fields["Flags"] = 0x00040000
        tx = _make_tx([_modified("RippleState", final_fields=fields)])
        sp.process_transaction(tx, ledger_index=2008)
        tl = db.trustlines[(USER_A, GATEWAY, "USD")]
        assert tl["authorized"] is True
        assert tl["peer_authorized"] is False

    def test_trustline_peer_authorized_by_high(self):
        # lsfHighAuth = 0x00080000
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        fields = _ripple_state_fields(high=GATEWAY, low=USER_A, currency="USD", balance="0")
        fields["Flags"] = 0x00080000
        tx = _make_tx([_modified("RippleState", final_fields=fields)])
        sp.process_transaction(tx, ledger_index=2009)
        tl = db.trustlines[(USER_A, GATEWAY, "USD")]
        assert tl["peer_authorized"] is True
        assert tl["authorized"] is False

    def test_trustline_deletion(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction(_make_tx([
            _created("RippleState", _ripple_state_fields(
                high=GATEWAY, low=USER_A, currency="USD",
                balance="0", high_limit="0", low_limit="1000",
            ))
        ]), ledger_index=2010)
        assert db.trustlines[(USER_A, GATEWAY, "USD")]["is_deleted"] is False

        sp.process_transaction(_make_tx([
            _deleted("RippleState", _ripple_state_fields(
                high=GATEWAY, low=USER_A, currency="USD",
                balance="0", high_limit="0", low_limit="0",
            ))
        ]), ledger_index=2011)
        assert db.trustlines[(USER_A, GATEWAY, "USD")]["is_deleted"] is True

    def test_both_parties_tracked_both_get_row(self):
        db = MockDB(tracked={USER_A, USER_B})
        sp = StateProcessor(db)
        tx = _make_tx([
            _created("RippleState", _ripple_state_fields(
                high=USER_B, low=USER_A, currency="XAU",
                balance="0", high_limit="0", low_limit="100",
            ))
        ])
        sp.process_transaction(tx, ledger_index=2012)
        assert (USER_A, USER_B, "XAU") in db.trustlines
        assert (USER_B, USER_A, "XAU") in db.trustlines

    def test_untracked_account_trustline_ignored(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("RippleState",
                final_fields=_ripple_state_fields(
                    high=GATEWAY, low=UNTRACKED, currency="USD", balance="100",
                )
            )
        ])
        sp.process_transaction(tx, ledger_index=2013)
        assert not db.trustlines  # nothing stored


# ---------------------------------------------------------------------------
# Tests: Offer → offers
# ---------------------------------------------------------------------------

class TestOffer:
    def _xrp_iou_offer(self, account: str, sequence: int, xrp_drops: str = "1000000",
                       iou_value: str = "10.0", currency: str = "USD", expiration: Optional[int] = None) -> dict:
        fields: dict = {
            "Account": account,
            "Sequence": sequence,
            "TakerGets": xrp_drops,                                      # XRP string
            "TakerPays": {"currency": currency, "issuer": GATEWAY, "value": iou_value},
            "Flags": 0,
            "Quality": "100000000000000",
        }
        if expiration is not None:
            fields["Expiration"] = expiration
        return fields

    def test_offer_create(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 42))])
        sp.process_transaction(tx, ledger_index=3000)
        key = (USER_A, 42)
        assert key in db.offers
        o = db.offers[key]
        assert o["taker_gets_currency"] == "XRP"
        assert o["taker_gets_issuer"] is None
        assert o["taker_gets_value"] == "1000000"
        assert o["taker_pays_currency"] == "USD"
        assert o["taker_pays_issuer"] == GATEWAY
        assert o["taker_pays_value"] == "10.0"
        assert o["expiry_iso"] is None

    def test_offer_partial_fill(self):
        """Modified Offer with FinalFields showing reduced amounts → upsert remaining."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        # Create
        sp.process_transaction(
            _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 43, xrp_drops="2000000", iou_value="20.0"))]),
            ledger_index=3001,
        )
        # Partial fill: half consumed
        sp.process_transaction(_make_tx([
            _modified("Offer",
                final_fields={
                    "Account": USER_A, "Sequence": 43,
                    "TakerGets": "1000000",
                    "TakerPays": {"currency": "USD", "issuer": GATEWAY, "value": "10.0"},
                    "Flags": 0, "Quality": "100000000000000",
                },
                previous_fields={
                    "TakerGets": "2000000",
                    "TakerPays": {"currency": "USD", "issuer": GATEWAY, "value": "20.0"},
                },
            )
        ]), ledger_index=3002)
        assert (USER_A, 43) in db.offers
        assert db.offers[(USER_A, 43)]["taker_gets_value"] == "1000000"
        assert db.offers[(USER_A, 43)]["taker_pays_value"] == "10.0"

    def test_offer_full_fill_deleted_node(self):
        """DeletedNode Offer → removed from open offers."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction(
            _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 44))]),
            ledger_index=3003,
        )
        assert (USER_A, 44) in db.offers

        sp.process_transaction(
            _make_tx([_deleted("Offer", self._xrp_iou_offer(USER_A, 44))]),
            ledger_index=3004,
        )
        assert (USER_A, 44) not in db.offers
        assert (USER_A, 44) in db.deleted_offers

    def test_offer_cancel(self):
        """OfferDelete transaction removes the offer via DeletedNode."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction(
            _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 45))]),
            ledger_index=3005,
        )
        # OfferDelete tx produces a DeletedNode
        sp.process_transaction(
            _make_tx([_deleted("Offer", {"Account": USER_A, "Sequence": 45,
                                          "TakerGets": "1000000",
                                          "TakerPays": {"currency": "USD", "issuer": GATEWAY, "value": "10.0"},
                                          "Flags": 0})]),
            ledger_index=3006,
        )
        assert (USER_A, 45) not in db.offers
        assert (USER_A, 45) in db.deleted_offers

    def test_offer_expiry_parsing(self):
        """Expiration field (Ripple epoch) should be converted to ISO-8601."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        # Ripple epoch 0 = 2000-01-01T00:00:00+00:00
        ripple_ts = 0
        tx = _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 46, expiration=ripple_ts))])
        sp.process_transaction(tx, ledger_index=3007)
        iso = db.offers[(USER_A, 46)]["expiry_iso"]
        assert iso is not None
        dt = datetime.fromisoformat(iso)
        assert dt.year == 2000
        assert dt.month == 1
        assert dt.day == 1

    def test_offer_expiry_known_timestamp(self):
        """Known Ripple epoch value maps to expected Unix time."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        # 1 Ripple second = 1 Unix second above the offset
        ripple_ts = 100
        tx = _make_tx([_created("Offer", self._xrp_iou_offer(USER_A, 47, expiration=ripple_ts))])
        sp.process_transaction(tx, ledger_index=3008)
        iso = db.offers[(USER_A, 47)]["expiry_iso"]
        dt = datetime.fromisoformat(iso)
        expected_unix = RIPPLE_EPOCH_OFFSET + 100
        assert int(dt.timestamp()) == expected_unix

    def test_offer_for_untracked_account_ignored(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([_created("Offer", self._xrp_iou_offer(UNTRACKED, 99))])
        sp.process_transaction(tx, ledger_index=3009)
        assert not db.offers


# ---------------------------------------------------------------------------
# Tests: mixed transaction / edge cases
# ---------------------------------------------------------------------------

class TestMixed:
    def test_mixed_transaction_updates_all_state_tables(self):
        """A single transaction can touch AccountRoot, RippleState, and Offer at once."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)

        affected_nodes = [
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "8000000", "Sequence": 10,
                               "OwnerCount": 2, "Flags": 0}),
            _created("RippleState", _ripple_state_fields(
                high=GATEWAY, low=USER_A, currency="USD",
                balance="0", high_limit="0", low_limit="500",
            )),
            _created("Offer", {
                "Account": USER_A, "Sequence": 55,
                "TakerGets": "500000",
                "TakerPays": {"currency": "USD", "issuer": GATEWAY, "value": "5.0"},
                "Flags": 0, "Quality": "10000000000",
            }),
        ]
        sp.process_transaction(_make_tx(affected_nodes), ledger_index=4000)

        assert db.account_states[USER_A]["balance_drops"] == 8_000_000
        assert (USER_A, GATEWAY, "USD") in db.trustlines
        assert (USER_A, 55) in db.offers

    def test_directory_nodes_skipped(self):
        """DirectoryNode entries should produce no state changes."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            {"ModifiedNode": {"LedgerEntryType": "DirectoryNode",
                               "FinalFields": {"Owner": USER_A, "RootIndex": "ABC"}}},
        ])
        sp.process_transaction(tx, ledger_index=4001)
        assert not db.account_states
        assert not db.trustlines
        assert not db.offers

    def test_missing_full_data_is_safe(self):
        """A tx without _full_data should not raise."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction({}, ledger_index=4002)  # no exception
        assert not db.account_states

    def test_empty_affected_nodes(self):
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        sp.process_transaction({"_full_data": {"meta": {"AffectedNodes": []}}}, ledger_index=4003)
        assert not db.account_states

    def test_mixed_tracked_and_untracked_only_tracked_updated(self):
        """When a tx affects both tracked and untracked accounts, only tracked ones are stored."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([
            _modified("AccountRoot",
                final_fields={"Account": USER_A, "Balance": "7000000", "Sequence": 3,
                               "OwnerCount": 0, "Flags": 0}),
            _modified("AccountRoot",
                final_fields={"Account": UNTRACKED, "Balance": "3000000", "Sequence": 1,
                               "OwnerCount": 0, "Flags": 0}),
        ])
        sp.process_transaction(tx, ledger_index=5000)
        assert USER_A in db.account_states
        assert UNTRACKED not in db.account_states

    def test_iou_to_iou_offer(self):
        """Offer where both sides are IOUs (no XRP)."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([_created("Offer", {
            "Account": USER_A,
            "Sequence": 77,
            "TakerGets": {"currency": "USD", "issuer": GATEWAY, "value": "100"},
            "TakerPays": {"currency": "EUR", "issuer": GATEWAY, "value": "90"},
            "Flags": 0,
        })])
        sp.process_transaction(tx, ledger_index=5001)
        o = db.offers[(USER_A, 77)]
        assert o["taker_gets_currency"] == "USD"
        assert o["taker_pays_currency"] == "EUR"
        assert o["taker_gets_issuer"] == GATEWAY

    def test_account_root_missing_balance_field(self):
        """AccountRoot node without a Balance field should still upsert (balance_drops=None)."""
        db = MockDB(tracked={USER_A})
        sp = StateProcessor(db)
        tx = _make_tx([_modified("AccountRoot",
            final_fields={"Account": USER_A, "Sequence": 7, "OwnerCount": 1, "Flags": 0}
        )])
        sp.process_transaction(tx, ledger_index=5002)
        assert db.account_states[USER_A]["balance_drops"] is None
        assert db.account_states[USER_A]["sequence"] == 7
