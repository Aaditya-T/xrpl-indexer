"""
State processor: applies AffectedNodes from transaction metadata to the
live state tables (account_states, trustlines, offers).

Supported node types:
  AccountRoot  → account_states  (balance, sequence, owner_count, flags)
  RippleState  → trustlines      (balance, limits, auth, freeze, no_ripple)
  Offer        → offers          (create / partial-fill / full-fill / cancel)

All other node types (DirectoryNode, FeeSettings, …) are silently skipped.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from database import Database

# Seconds between Unix epoch (1970-01-01) and Ripple epoch (2000-01-01)
RIPPLE_EPOCH_OFFSET: int = 946684800

# RippleState flag bitmasks
_LSF_LOW_RESERVE    = 0x00010000
_LSF_HIGH_RESERVE   = 0x00020000
_LSF_LOW_AUTH       = 0x00040000
_LSF_HIGH_AUTH      = 0x00080000
_LSF_LOW_NO_RIPPLE  = 0x00100000
_LSF_HIGH_NO_RIPPLE = 0x00200000
_LSF_LOW_FREEZE     = 0x00400000
_LSF_HIGH_FREEZE    = 0x00800000


def ripple_epoch_to_iso(ripple_ts: int) -> str:
    """Convert a Ripple-epoch timestamp (seconds since 2000-01-01) to ISO-8601."""
    unix_ts = ripple_ts + RIPPLE_EPOCH_OFFSET
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()


def decimal_to_text(value: Decimal) -> str:
    """Render Decimal values without binary-float artifacts or needless zeroes."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


class StateProcessor:
    """
    Processes a single stored transaction and updates state tables.

    Usage:
        processor = StateProcessor(db)
        processor.process_transaction(tx_data_dict, ledger_index)
    """

    def __init__(self, db: "Database") -> None:
        self.db = db

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def process_transaction(self, tx_data: dict, ledger_index: int) -> None:
        """
        Apply every AffectedNode from tx_data to the state tables.

        tx_data is the dict stored in the `transactions` table.  It must
        contain a `_full_data` sub-key that holds the raw XRPL response
        (including `meta.AffectedNodes`).
        """
        full = tx_data.get("_full_data")
        if not isinstance(full, dict):
            return
        meta = full.get("meta")
        if not isinstance(meta, dict):
            return

        for node_wrapper in meta.get("AffectedNodes", []):
            self._dispatch_node(node_wrapper, ledger_index)

    # ------------------------------------------------------------------
    # Node dispatch
    # ------------------------------------------------------------------

    def _dispatch_node(self, node_wrapper: dict, ledger_index: int) -> None:
        if "CreatedNode" in node_wrapper:
            node = node_wrapper["CreatedNode"]
            fields = node.get("NewFields") or {}
            self._apply(node.get("LedgerEntryType", ""), fields, ledger_index, deleted=False)

        elif "ModifiedNode" in node_wrapper:
            node = node_wrapper["ModifiedNode"]
            fields = node.get("FinalFields") or {}
            self._apply(node.get("LedgerEntryType", ""), fields, ledger_index, deleted=False)

        elif "DeletedNode" in node_wrapper:
            node = node_wrapper["DeletedNode"]
            # For deleted nodes we still want FinalFields (last known state) to
            # carry account / sequence / etc for look-up.
            fields = node.get("FinalFields") or {}
            self._apply(node.get("LedgerEntryType", ""), fields, ledger_index, deleted=True)

    def _apply(self, entry_type: str, fields: dict, ledger_index: int, deleted: bool) -> None:
        if entry_type == "AccountRoot":
            self._account_root(fields, ledger_index)
        elif entry_type == "RippleState":
            self._ripple_state(fields, ledger_index, deleted)
        elif entry_type == "Offer":
            self._offer(fields, ledger_index, deleted)
        # Everything else (DirectoryNode, LedgerHashes, FeeSettings …) → skip

    # ------------------------------------------------------------------
    # AccountRoot → account_states
    # ------------------------------------------------------------------

    def _account_root(self, fields: dict, ledger_index: int) -> None:
        address = fields.get("Account")
        if not address or not self.db.is_tracked_wallet(address):
            return

        balance_drops: Optional[int] = None
        raw_balance = fields.get("Balance")
        if raw_balance is not None:
            try:
                balance_drops = int(raw_balance)
            except (ValueError, TypeError):
                pass

        self.db.upsert_account_state(
            address=address,
            balance_drops=balance_drops,
            sequence=fields.get("Sequence"),
            owner_count=fields.get("OwnerCount"),
            flags=fields.get("Flags", 0),
            ledger_index=ledger_index,
        )

    # ------------------------------------------------------------------
    # RippleState → trustlines
    # ------------------------------------------------------------------

    def _ripple_state(self, fields: dict, ledger_index: int, deleted: bool) -> None:
        high_limit = fields.get("HighLimit") or {}
        low_limit  = fields.get("LowLimit")  or {}
        balance_field = fields.get("Balance") or {}
        flags = int(fields.get("Flags", 0))

        # In RippleState, HighLimit.issuer / LowLimit.issuer are the account
        # addresses (the naming is Ripple's own confusing convention).
        high_account = high_limit.get("issuer", "")
        low_account  = low_limit.get("issuer", "")
        currency     = high_limit.get("currency") or low_limit.get("currency", "")

        # Balance value: positive  → low account holds from high
        #                negative  → high account holds from low (rare)
        raw_bv = "0"
        if isinstance(balance_field, dict):
            raw_bv = balance_field.get("value", "0")
        elif isinstance(balance_field, (str, int, float)):
            raw_bv = str(balance_field)

        try:
            balance_decimal = Decimal(str(raw_bv))
        except (InvalidOperation, ValueError, TypeError):
            balance_decimal = Decimal("0")

        # Emit one row for each tracked account involved in this trust line.
        for account, is_high in ((high_account, True), (low_account, False)):
            if not account or not self.db.is_tracked_wallet(account):
                continue

            peer = low_account if is_high else high_account

            # Normalise balance to "I hold positive" from this account's view
            acc_balance = decimal_to_text(-balance_decimal if is_high else balance_decimal)

            if is_high:
                limit_amount    = high_limit.get("value", "0")
                limit_peer      = low_limit.get("value", "0")
                authorized      = bool(flags & _LSF_HIGH_AUTH)
                peer_authorized = bool(flags & _LSF_LOW_AUTH)
                no_ripple       = bool(flags & _LSF_HIGH_NO_RIPPLE)
                no_ripple_peer  = bool(flags & _LSF_LOW_NO_RIPPLE)
                freeze          = bool(flags & _LSF_HIGH_FREEZE)
                peer_freeze     = bool(flags & _LSF_LOW_FREEZE)
            else:
                limit_amount    = low_limit.get("value", "0")
                limit_peer      = high_limit.get("value", "0")
                authorized      = bool(flags & _LSF_LOW_AUTH)
                peer_authorized = bool(flags & _LSF_HIGH_AUTH)
                no_ripple       = bool(flags & _LSF_LOW_NO_RIPPLE)
                no_ripple_peer  = bool(flags & _LSF_HIGH_NO_RIPPLE)
                freeze          = bool(flags & _LSF_LOW_FREEZE)
                peer_freeze     = bool(flags & _LSF_HIGH_FREEZE)

            if deleted:
                self.db.delete_trustline(account, peer, currency, ledger_index)
            else:
                self.db.upsert_trustline(
                    account=account,
                    issuer=peer,
                    currency=currency,
                    balance=acc_balance,
                    limit_amount=limit_amount,
                    limit_peer=limit_peer,
                    authorized=authorized,
                    peer_authorized=peer_authorized,
                    no_ripple=no_ripple,
                    no_ripple_peer=no_ripple_peer,
                    freeze_flag=freeze,
                    peer_freeze_flag=peer_freeze,
                    is_deleted=False,
                    ledger_index=ledger_index,
                )

    # ------------------------------------------------------------------
    # Offer → offers
    # ------------------------------------------------------------------

    def _offer(self, fields: dict, ledger_index: int, deleted: bool) -> None:
        account  = fields.get("Account")
        sequence = fields.get("Sequence")
        if not account or sequence is None:
            return
        if not self.db.is_tracked_wallet(account):
            return

        # Deleted (full fill or cancel) → remove from open offers
        if deleted:
            self.db.delete_offer(account, int(sequence), ledger_index)
            return

        # Still open (created or partially filled) → upsert remaining amounts
        tg = fields.get("TakerGets") or {}
        tp = fields.get("TakerPays") or {}

        if isinstance(tg, str):          # XRP (drops string)
            tg_currency, tg_issuer, tg_value = "XRP", None, tg
        else:
            tg_currency = tg.get("currency")
            tg_issuer   = tg.get("issuer")
            tg_value    = tg.get("value")

        if isinstance(tp, str):          # XRP (drops string)
            tp_currency, tp_issuer, tp_value = "XRP", None, tp
        else:
            tp_currency = tp.get("currency")
            tp_issuer   = tp.get("issuer")
            tp_value    = tp.get("value")

        expiry_iso: Optional[str] = None
        raw_exp = fields.get("Expiration")
        if raw_exp is not None:
            try:
                expiry_iso = ripple_epoch_to_iso(int(raw_exp))
            except (ValueError, TypeError):
                pass

        quality = fields.get("Quality")

        self.db.upsert_offer(
            account=account,
            sequence=int(sequence),
            taker_gets_currency=tg_currency,
            taker_gets_issuer=tg_issuer,
            taker_gets_value=tg_value,
            taker_pays_currency=tp_currency,
            taker_pays_issuer=tp_issuer,
            taker_pays_value=tp_value,
            expiry_iso=expiry_iso,
            flags=int(fields.get("Flags", 0)),
            quality=str(quality) if quality is not None else None,
            ledger_index=ledger_index,
        )
