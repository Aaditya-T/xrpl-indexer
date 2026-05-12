"""
End-to-end integration tests against the XRPL testnet.

Strategy
--------
1. Fund a *hub* wallet, a *gateway* wallet, and a *spoke* wallet from the
   testnet faucet.
2. Submit real transactions through the sequence every hub-and-spoke system
   would see in production:
       hub  → spoke   : Payment that activates the spoke account
       spoke→ gateway : TrustSet  (trust line for USD, limit 1 000)
       gateway→ spoke : Payment   (issue 100 USD to spoke)
       spoke          : OfferCreate (sell 10 USD, want 200 XRP)
3. Spin up a fresh XRPLIndexer that talks to testnet via JSON-RPC and
   writes to an isolated SQLite file (never touches the real DB).
4. Process every ledger that contained one of those transactions.
5. Assert that every layer — wallet discovery, account states, trust lines,
   offers, and the transactions table — recorded the right data.

Running
-------
    # All integration tests (requires network):
    pytest tests/test_integration_testnet.py -v -s

    # Skip integration tests (e.g. in CI without testnet access):
    pytest -m "not integration"

The whole session uses module-scoped fixtures so the faucet is called only
once and the ledger-processing step runs once before all assertions.
"""
from __future__ import annotations

import os
import tempfile
import time
import pytest
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TESTNET_RPC = "https://s.altnet.rippletest.net:51234/"
USD_CURRENCY = "USD"
ACTIVATION_DROPS = "25000000"   # 25 XRP — enough to meet the reserve


# ---------------------------------------------------------------------------
# Testnet reachability guard
# ---------------------------------------------------------------------------

def _testnet_reachable() -> bool:
    try:
        from xrpl.clients import JsonRpcClient
        from xrpl.models.requests import Ledger as LedgerReq
        r = JsonRpcClient(TESTNET_RPC).request(LedgerReq(ledger_index="validated"))
        return r.is_successful()
    except Exception:
        return False


# Applied to every test in this module
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_testnet():
    """Skip the entire module if the testnet JSON-RPC is not reachable."""
    if not _testnet_reachable():
        pytest.skip("XRPL testnet not reachable — skipping integration suite")


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _ledger_index(response) -> int:
    """Extract the validated ledger_index from a submit_and_wait Response."""
    return int(response.result.get("ledger_index", 0))


def _sequence(response) -> int:
    """Extract the transaction Sequence number from a submit_and_wait Response."""
    # xrpl-py v4 puts the full tx fields at the top level of result
    seq = response.result.get("Sequence")
    if seq is None:
        seq = (response.result.get("tx_json") or {}).get("Sequence")
    return int(seq)


def _tx_hash(response) -> str:
    h = response.result.get("hash") or response.result.get("tx_json", {}).get("hash", "")
    return str(h)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def testnet_client():
    from xrpl.clients import JsonRpcClient
    return JsonRpcClient(TESTNET_RPC)


@pytest.fixture(scope="module")
def hub_wallet(testnet_client):
    """Central funding wallet — activated by the testnet faucet."""
    from xrpl.wallet import generate_faucet_wallet
    print("\n[Faucet] Funding hub wallet...")
    w = generate_faucet_wallet(testnet_client, debug=True)
    print(f"[Faucet] Hub: {w.classic_address}")
    return w


@pytest.fixture(scope="module")
def gateway_wallet(testnet_client):
    """Currency-issuing gateway wallet."""
    from xrpl.wallet import generate_faucet_wallet
    print("\n[Faucet] Funding gateway wallet...")
    w = generate_faucet_wallet(testnet_client, debug=True)
    print(f"[Faucet] Gateway: {w.classic_address}")
    return w


@pytest.fixture(scope="module")
def spoke_wallet(testnet_client):
    """
    Spoke wallet — starts with no XRP so we can observe hub activating it.

    We generate it with the faucet (to get a valid keypair) but then the
    indexer scenario expects hub's Payment to be what creates the AccountRoot.
    The faucet creates the account before our hub payment, so the hub payment
    will NOT carry a CreatedNode for the spoke account.

    Work-around: generate the keypair locally without calling the faucet, so
    the account genuinely does not exist on-chain until the hub funds it.
    """
    from xrpl.wallet import Wallet
    w = Wallet.create()
    print(f"\n[Wallet] Spoke (unfunded): {w.classic_address}")
    return w


@pytest.fixture(scope="module")
def test_db(tmp_path_factory):
    """Isolated SQLite database — never touches the real Postgres."""
    from database import Database
    db_file = tmp_path_factory.mktemp("integration_db") / "testnet.db"
    db = Database(db_url=f"sqlite:///{db_file}", db_type="sqlite")
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Main scenario fixture — runs once, all tests share the result
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def scenario(testnet_client, hub_wallet, gateway_wallet, spoke_wallet, test_db):
    """
    Submit the full hub-and-spoke transaction sequence to testnet, then
    run the indexer over every affected ledger.  Returns a dict with
    addresses, hashes, sequences, and ledger indexes for assertions.
    """
    from xrpl.transaction import submit_and_wait
    from xrpl.models.transactions import Payment, TrustSet, OfferCreate
    from xrpl.models.amounts import IssuedCurrencyAmount
    from xrpl_client import XRPLClient
    from indexer import XRPLIndexer

    info: dict = {
        "hub":     hub_wallet.classic_address,
        "gateway": gateway_wallet.classic_address,
        "spoke":   spoke_wallet.classic_address,
    }
    ledgers_touched: list[int] = []

    def submit(tx, wallet):
        print(f"\n[TX] Submitting {tx.__class__.__name__} from {wallet.classic_address[:12]}...")
        resp = submit_and_wait(tx, testnet_client, wallet)
        li = _ledger_index(resp)
        ledgers_touched.append(li)
        print(f"[TX] Landed in ledger {li}, hash={_tx_hash(resp)[:16]}...")
        return resp

    # ------------------------------------------------------------------
    # 1. Hub activates spoke (Payment that creates a new AccountRoot)
    # ------------------------------------------------------------------
    r1 = submit(
        Payment(
            account=hub_wallet.classic_address,
            destination=spoke_wallet.classic_address,
            amount=ACTIVATION_DROPS,
        ),
        hub_wallet,
    )
    info["activation_ledger"] = _ledger_index(r1)
    info["activation_hash"]   = _tx_hash(r1)

    # ------------------------------------------------------------------
    # 2. Spoke sets a trust line to gateway (USD, limit 1 000)
    # ------------------------------------------------------------------
    r2 = submit(
        TrustSet(
            account=spoke_wallet.classic_address,
            limit_amount=IssuedCurrencyAmount(
                currency=USD_CURRENCY,
                issuer=gateway_wallet.classic_address,
                value="1000",
            ),
        ),
        spoke_wallet,
    )
    info["trustset_ledger"] = _ledger_index(r2)

    # ------------------------------------------------------------------
    # 3. Gateway issues 100 USD to spoke
    # ------------------------------------------------------------------
    r3 = submit(
        Payment(
            account=gateway_wallet.classic_address,
            destination=spoke_wallet.classic_address,
            amount=IssuedCurrencyAmount(
                currency=USD_CURRENCY,
                issuer=gateway_wallet.classic_address,
                value="100",
            ),
        ),
        gateway_wallet,
    )
    info["payment_ledger"] = _ledger_index(r3)

    # ------------------------------------------------------------------
    # 4. Spoke creates an offer: give 10 USD, want 200 XRP
    # ------------------------------------------------------------------
    r4 = submit(
        OfferCreate(
            account=spoke_wallet.classic_address,
            taker_gets=IssuedCurrencyAmount(
                currency=USD_CURRENCY,
                issuer=gateway_wallet.classic_address,
                value="10",
            ),
            taker_pays="200000000",   # 200 XRP in drops
        ),
        spoke_wallet,
    )
    info["offer_ledger"]   = _ledger_index(r4)
    info["offer_sequence"] = _sequence(r4)

    # ------------------------------------------------------------------
    # Index every ledger that was touched
    # ------------------------------------------------------------------
    xrpl_c = XRPLClient(json_rpc_url=TESTNET_RPC)
    indexer = XRPLIndexer(
        db=test_db,
        xrpl_client=xrpl_c,
        central_wallet=hub_wallet.classic_address,
    )

    for li in sorted(set(ledgers_touched)):
        print(f"\n[Indexer] Processing ledger {li}...")
        indexer.process_ledger(li)

    print(f"\n[Scenario] Done. Info: {info}")
    return info


# ---------------------------------------------------------------------------
# Helper: raw sqlite query
# ---------------------------------------------------------------------------

def _query_one(db, sql: str, params: tuple = ()) -> Optional[dict]:
    cur = db.conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    if row is None:
        return None
    return dict(row)


def _query_all(db, sql: str, params: tuple = ()) -> list[dict]:
    cur = db.conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


# ===========================================================================
# Test classes
# ===========================================================================

class TestWalletDiscovery:
    """Hub-and-spoke: spoke activated by hub payment should be auto-discovered."""

    def test_spoke_is_tracked(self, scenario, test_db):
        """Spoke wallet appears in tracked_wallets after hub's activation payment."""
        assert test_db.is_tracked_wallet(scenario["spoke"]), (
            f"Expected {scenario['spoke']} to be in tracked_wallets"
        )

    def test_hub_is_not_tracked(self, scenario, test_db):
        """Hub itself is the central wallet, not a spoke — it should not be tracked."""
        assert not test_db.is_tracked_wallet(scenario["hub"])

    def test_gateway_is_not_tracked(self, scenario, test_db):
        """Gateway was funded by the faucet, not by the hub — should not be tracked."""
        assert not test_db.is_tracked_wallet(scenario["gateway"])

    def test_activation_tx_stored_in_tracked_wallets(self, scenario, test_db):
        """The tracked_wallets row should record the activating transaction hash."""
        row = _query_one(
            test_db,
            "SELECT activation_tx_hash FROM tracked_wallets WHERE address = ?",
            (scenario["spoke"],),
        )
        assert row is not None
        assert row["activation_tx_hash"] == scenario["activation_hash"], (
            f"Expected hash {scenario['activation_hash']!r}, "
            f"got {row['activation_tx_hash']!r}"
        )


class TestAccountState:
    """account_states should reflect the spoke's XRP balance after activation."""

    def test_spoke_account_state_exists(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT * FROM account_states WHERE address = ?",
            (scenario["spoke"],),
        )
        assert row is not None, "No account_states row for spoke"

    def test_spoke_has_positive_balance(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT balance_drops FROM account_states WHERE address = ?",
            (scenario["spoke"],),
        )
        assert row is not None
        balance = int(row["balance_drops"])
        # Spoke received 25 XRP (25_000_000 drops) and spent fees on 3 txns.
        # Reserve is 10 XRP.  Expect at least 10 XRP left.
        assert balance > 10_000_000, f"Expected > 10 XRP, got {balance} drops"

    def test_account_state_ledger_index_is_recent(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT ledger_index FROM account_states WHERE address = ?",
            (scenario["spoke"],),
        )
        assert row is not None
        # Should be at or after the activation ledger
        assert int(row["ledger_index"]) >= scenario["activation_ledger"]


class TestTrustline:
    """trustlines should capture the spoke↔gateway USD trust line."""

    def test_trustline_row_exists(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT * FROM trustlines WHERE account = ? AND issuer = ? AND currency = ?",
            (scenario["spoke"], scenario["gateway"], USD_CURRENCY),
        )
        assert row is not None, (
            f"No trustline row for spoke={scenario['spoke']}, "
            f"gateway={scenario['gateway']}, currency={USD_CURRENCY}"
        )

    def test_trustline_limit_reflects_trust_set(self, scenario, test_db):
        """The limit the spoke set (1 000 USD) should be stored."""
        row = _query_one(
            test_db,
            "SELECT limit_amount, limit_peer FROM trustlines "
            "WHERE account = ? AND issuer = ? AND currency = ?",
            (scenario["spoke"], scenario["gateway"], USD_CURRENCY),
        )
        assert row is not None
        # From spoke's perspective: limit_amount is what spoke trusts gateway for
        limit = float(row["limit_amount"] or 0)
        assert limit == pytest.approx(1000.0), f"Expected limit 1000, got {limit}"

    def test_trustline_balance_after_payment(self, scenario, test_db):
        """After gateway sends 100 USD, the balance should reflect that."""
        row = _query_one(
            test_db,
            "SELECT balance FROM trustlines "
            "WHERE account = ? AND issuer = ? AND currency = ?",
            (scenario["spoke"], scenario["gateway"], USD_CURRENCY),
        )
        assert row is not None
        balance = float(row["balance"] or 0)
        # 90 USD remains after spoke's offer deducted 10
        # (the offer may or may not have been filled on testnet DEX;
        # assert at least some positive balance exists)
        assert abs(balance) > 0, f"Expected non-zero balance, got {balance}"

    def test_trustline_is_not_deleted(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT is_deleted FROM trustlines "
            "WHERE account = ? AND issuer = ? AND currency = ?",
            (scenario["spoke"], scenario["gateway"], USD_CURRENCY),
        )
        assert row is not None
        assert not row["is_deleted"]


class TestOffer:
    """offers table should capture the spoke's open OfferCreate."""

    def test_offer_row_exists(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT * FROM offers WHERE account = ? AND sequence = ?",
            (scenario["spoke"], scenario["offer_sequence"]),
        )
        assert row is not None, (
            f"No offer row for spoke={scenario['spoke']}, "
            f"sequence={scenario['offer_sequence']}"
        )

    def test_offer_taker_gets_is_usd(self, scenario, test_db):
        """TakerGets was set to USD — the offer gives USD in exchange for XRP."""
        row = _query_one(
            test_db,
            "SELECT taker_gets_currency, taker_gets_issuer, taker_gets_value "
            "FROM offers WHERE account = ? AND sequence = ?",
            (scenario["spoke"], scenario["offer_sequence"]),
        )
        assert row is not None
        assert row["taker_gets_currency"] == USD_CURRENCY
        assert row["taker_gets_issuer"] == scenario["gateway"]
        assert float(row["taker_gets_value"]) == pytest.approx(10.0)

    def test_offer_taker_pays_is_xrp(self, scenario, test_db):
        """TakerPays was 200 XRP (200_000_000 drops)."""
        row = _query_one(
            test_db,
            "SELECT taker_pays_currency, taker_pays_issuer, taker_pays_value "
            "FROM offers WHERE account = ? AND sequence = ?",
            (scenario["spoke"], scenario["offer_sequence"]),
        )
        assert row is not None
        assert row["taker_pays_currency"] == "XRP"
        assert row["taker_pays_issuer"] is None
        assert int(float(row["taker_pays_value"])) == 200_000_000


class TestTransactionsTable:
    """The transactions table should store the activation payment from hub."""

    def test_activation_payment_stored(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT * FROM transactions WHERE transaction_hash = ?",
            (scenario["activation_hash"],),
        )
        assert row is not None, (
            f"Activation payment {scenario['activation_hash']} not in transactions table"
        )

    def test_activation_payment_fields(self, scenario, test_db):
        row = _query_one(
            test_db,
            "SELECT account, destination, transaction_type, ledger_index "
            "FROM transactions WHERE transaction_hash = ?",
            (scenario["activation_hash"],),
        )
        assert row is not None
        assert row["account"] == scenario["hub"]
        assert row["destination"] == scenario["spoke"]
        assert row["transaction_type"] == "Payment"
        assert int(row["ledger_index"]) == scenario["activation_ledger"]

    def test_spoke_txns_recorded(self, scenario, test_db):
        """All transactions originating from spoke should also be stored."""
        rows = _query_all(
            test_db,
            "SELECT transaction_type FROM transactions WHERE account = ? ORDER BY ledger_index",
            (scenario["spoke"],),
        )
        tx_types = [r["transaction_type"] for r in rows]
        assert "TrustSet" in tx_types,    f"TrustSet not found in spoke txns: {tx_types}"
        assert "OfferCreate" in tx_types, f"OfferCreate not found in spoke txns: {tx_types}"
