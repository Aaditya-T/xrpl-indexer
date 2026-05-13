from xrpl.clients import JsonRpcClient
from xrpl.models.requests import Ledger, Tx
from typing import Optional, List, Dict, Any
from config import Config


class XRPLClient:
    """Wrapper for XRPL JSON RPC client"""
    
    def __init__(self, json_rpc_url: str | None = None):
        self.json_rpc_url = json_rpc_url or Config.XRPL_JSON_RPC_URL
        self.client = JsonRpcClient(self.json_rpc_url)
    
    def get_current_ledger_index(self) -> int:
        """Get the current validated ledger index"""
        try:
            response = self.client.request(Ledger(ledger_index="validated"))
            if response.is_successful():
                return response.result['ledger_index']
            else:
                raise Exception(f"Failed to get current ledger: {response.result}")
        except Exception as e:
            print(f"Error getting current ledger index: {e}")
            raise
    
    def get_ledger_transactions(self, ledger_index: int) -> List[str]:
        """Get all transaction hashes from a specific ledger"""
        try:
            response = self.client.request(
                Ledger(
                    ledger_index=ledger_index,
                    transactions=True,
                    expand=False
                )
            )
            
            if response.is_successful():
                transactions = response.result.get('ledger', {}).get('transactions', [])
                return transactions if transactions else []
            else:
                print(f"Failed to get ledger {ledger_index}: {response.result}")
                return []
        except Exception as e:
            print(f"Error getting ledger {ledger_index} transactions: {e}")
            return []
    
    def get_transaction(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Get detailed transaction information"""
        try:
            response = self.client.request(Tx(transaction=tx_hash))
            
            if response.is_successful():
                tx_data = response.result
                # Add hash to the transaction data for convenience
                if 'hash' not in tx_data:
                    tx_data['hash'] = tx_hash
                return tx_data
            else:
                print(f"Failed to get transaction {tx_hash}: {response.result}")
                return None
        except Exception as e:
            print(f"Error getting transaction {tx_hash}: {e}")
            return None
    
    def get_ledger_with_transactions(
        self, ledger_index: int
    ) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Get all full transaction details from a specific ledger.
        Returns (transactions, close_time_iso) — close_time_iso may be None
        if the node does not return it.
        """
        try:
            response = self.client.request(
                Ledger(
                    ledger_index=ledger_index,
                    transactions=True,
                    expand=True
                )
            )

            if response.is_successful():
                ledger_data = response.result.get('ledger', {})
                close_time_iso: Optional[str] = ledger_data.get('close_time_iso')
                transactions = ledger_data.get('transactions', [])

                # Stamp every transaction with ledger_index and close_time_iso
                for tx in transactions:
                    if isinstance(tx, dict):
                        tx['ledger_index'] = ledger_index
                        if close_time_iso:
                            tx['close_time_iso'] = close_time_iso
                        if 'hash' not in tx and 'tx' in tx:
                            tx['hash'] = tx['tx'].get('hash')

                return transactions if transactions else [], close_time_iso
            else:
                print(f"Failed to get ledger {ledger_index} with transactions: {response.result}")
                return [], None
        except Exception as e:
            print(f"Error getting ledger {ledger_index} with transactions: {e}")
            return [], None
