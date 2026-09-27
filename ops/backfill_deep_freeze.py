"""Backfill pre-existing trustline deep-freeze flags from an exact XRPL ledger.

Run after deploying the schema migration. The node must retain the indexer's
last published ledger; unknown values stay NULL if that ledger is unavailable.
"""
import argparse

from database import Database
from xrpl_client import XRPLClient

LOW_DEEP_FREEZE = 0x02000000
HIGH_DEEP_FREEZE = 0x04000000


def backfill(db: Database, client: XRPLClient) -> tuple[int, int]:
    ledger_index = db.get_last_processed_ledger_index()
    rows = db.get_trustlines_missing_deep_freeze()
    if not rows:
        return 0, 0
    if ledger_index is None:
        raise RuntimeError("No published indexer ledger; cannot backfill existing trustlines")

    updated = 0
    for row in rows:
        account, peer, currency = row["account"], row["issuer"], row["currency"]
        if row["ledger_index"] is not None and row["ledger_index"] > ledger_index:
            # A pre-upgrade run may have committed part of the next ledger.
            # Normal ledger replay will replace this row with complete flags.
            continue
        # Stored metadata is useful evidence, but historical filters can omit
        # a later transaction in the *same* ledger. The exact ledger entry is
        # the authority for a production backfill.
        stored = (db.get_stored_ripple_state(account, peer, currency, row["ledger_index"])
                  if row["ledger_index"] is not None else None)
        node = client.get_ripple_state(account, peer, currency, ledger_index)
        try:
            stored_flags = int(stored["Flags"]) if stored is not None else None
        except (KeyError, TypeError, ValueError):
            stored_flags = None
        if stored_flags is not None and stored_flags != int(node["Flags"]):
            print(f"Stored metadata differs from the ledger for {account}/{peer}/{currency}; "
                  "using the ledger value")
        high = (node.get("HighLimit") or {}).get("issuer")
        low = (node.get("LowLimit") or {}).get("issuer")
        if {high, low} != {account, peer} or high == low:
            raise RuntimeError(f"Mismatched RippleState accounts for {account}/{peer}/{currency}")
        flags = int(node["Flags"])
        is_high = account == high
        own_mask = HIGH_DEEP_FREEZE if is_high else LOW_DEEP_FREEZE
        peer_mask = LOW_DEEP_FREEZE if is_high else HIGH_DEEP_FREEZE
        if db.backfill_deep_freeze_flags(
            account, peer, currency, bool(flags & own_mask), bool(flags & peer_mask),
            ledger_index,
        ):
            updated += 1
    return updated, len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", help="Archival XRPL JSON-RPC URL (defaults to XRPL_JSON_RPC_URL)")
    args = parser.parse_args()
    db = Database()
    try:
        updated, examined = backfill(db, XRPLClient(json_rpc_url=args.rpc_url))
        remaining = len(db.get_trustlines_missing_deep_freeze())
        print(f"Backfilled {updated}/{examined} legacy trustlines; {remaining} remain unknown")
        if remaining:
            raise SystemExit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
