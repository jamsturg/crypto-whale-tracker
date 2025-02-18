from typing import Dict, Optional, List
from abc import ABC, abstractmethod
from web3 import Web3
from datetime import datetime

class BlockchainConnector(ABC):
    """Base class for blockchain connections"""
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to blockchain node"""
        pass
    
    @abstractmethod
    async def get_latest_block(self) -> Dict:
        """Get latest block data"""
        pass
    
    @abstractmethod
    async def get_transaction(self, tx_hash: str) -> Optional[Dict]:
        """Get transaction details"""
        pass
    
    @abstractmethod
    async def get_balance(self, address: str) -> Dict:
        """Get address balance"""
        pass

class EthereumConnector(BlockchainConnector):
    """Ethereum blockchain connector implementation"""
    
    def __init__(self, node_url: str, api_key: Optional[str] = None):
        self.node_url = node_url
        self.api_key = api_key
        self.web3 = Web3(Web3.HTTPProvider(node_url))
    
    async def connect(self) -> bool:
        try:
            return self.web3.is_connected()
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    async def get_latest_block(self) -> Dict:
        block = self.web3.eth.get_block('latest', full_transactions=True)
        return {
            'number': block.number,
            'timestamp': datetime.fromtimestamp(block.timestamp),
            'transactions': [tx.hex() if isinstance(tx, bytes) else tx for tx in block.transactions],
            'hash': block.hash.hex()
        }
    
    async def get_transaction(self, tx_hash: str) -> Optional[Dict]:
        try:
            tx = self.web3.eth.get_transaction(tx_hash)
            if tx:
                return {
                    'hash': tx.hash.hex(),
                    'from': tx['from'],
                    'to': tx.to,
                    'value': self.web3.from_wei(tx.value, 'ether'),
                    'gas': tx.gas,
                    'gas_price': self.web3.from_wei(tx.gas_price, 'gwei'),
                    'nonce': tx.nonce
                }
            return None
        except Exception as e:
            print(f"Transaction retrieval error: {e}")
            return None
    
    async def get_balance(self, address: str) -> Dict:
        try:
            balance_wei = self.web3.eth.get_balance(address)
            return {
                'address': address,
                'balance_eth': self.web3.from_wei(balance_wei, 'ether'),
                'balance_wei': balance_wei
            }
        except Exception as e:
            print(f"Balance retrieval error: {e}")
            return {'address': address, 'balance_eth': 0, 'balance_wei': 0}