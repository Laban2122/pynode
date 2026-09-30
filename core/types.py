# Shared dataclasses (e.g. InternalMessage, BMResult)
from dataclasses import dataclass, field
from typing import Optional, List, Set, Dict, Any
from enum import Enum


@dataclass
class PeerEvent:
    """Event emitted by PeerLink to NetworkManager"""
    peer_id: int
    cmd: str
    payload: bytes


@dataclass
class BMRequest:
    """Request from BlockchainManager to NetworkManager"""
    type: str  # "need_headers", "need_blocks", etc.
    data: dict  # Request-specific data
    backfill_active: bool = False  # Whether backfill is currently active


@dataclass
class BMResult:
    """Result from BlockchainManager to NetworkManager"""
    type: str  # "valid_block", "invalid_block", etc.
    data: dict  # Result-specific data


# Phase 3: Peer state tracking structures

@dataclass
class InventoryVector:
    """Represents a single inventory item (tx or block)"""
    inv_type: int  # 1=MSG_TX, 2=MSG_BLOCK, etc.
    hash: bytes  # 32-byte hash
    
    def __hash__(self):
        return hash((self.inv_type, self.hash))
    
    def __eq__(self, other):
        if isinstance(other, InventoryVector):
            return self.inv_type == other.inv_type and self.hash == other.hash
        return False


@dataclass
class PeerState:
    """Tracks state for a single peer"""
    peer_id: int
    host: str
    port: int
    connected: bool = True
    
    # Inventory tracking
    known_inv: Set[InventoryVector] = field(default_factory=set)
    
    # Peer metadata
    services: int = 0
    version: int = 0
    user_agent: str = ""
    start_height: int = 0
    
    # Behavior tracking
    score: int = 0  # Can be used for misbehavior tracking
    last_send: float = 0.0
    last_recv: float = 0.0
    connection_time: float = 0.0
    
    # Address discovery tracking (Phase 5)
    addresses_received: int = 0  # Total addresses received from this peer
    address_limit_warned: bool = False  # Track if we've warned about exceeding limit


@dataclass
class DataMessage:
    """Data message to be forwarded to BlockchainManager"""
    peer_id: int
    msg_type: str  # "headers", "block", "tx"
    payload: bytes
    parsed_data: Optional[Any] = None  # Optional pre-parsed data


# ReorgDecision moved to blockchain/fork_handler.py (Phase 4.3)