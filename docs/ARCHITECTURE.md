# Pynode Architecture Overview

**Version:** 4.3.0  
**Purpose:** Bitcoin node implementation in Python with strict layer separation

---

## 🏗️ High-Level Component Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                         Main Program                             │
│  - Initializes queues & components                               │
│  - Runs 4 concurrent asyncio tasks                               │
│  - Version: 4.3.0 (Build: 2025-12-21)                            │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┬──────────────┐
        │                     │                     │              │
        ▼                     ▼                     ▼              ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐   ┌──────────────┐
│ BlockChain   │    │   Network    │    │     RPC      │   │    Block     │
│  Manager     │◄──►│   Manager    │◄──►│    Server    │   │ Backfiller   │
│     (BM)     │    │     (NM)     │    │              │   │              │
└──────────────┘    └──────────────┘    └──────────────┘   └──────────────┘
        ▲                   ▲                   ▲                  ▲
        │                   │                   │                  │
   ┌────┴────┐         ┌────┴────┐         ┌───┴────┐        ┌───┴────┐
   │Headers  │         │PeerLink │         │aiohttp │        │Headers │
   │ Chain   │         │ (PL) ×N │         │  web   │        │& Block │
   │& Block  │         └─────────┘         └────────┘        │  read  │
   │ Chain   │              │                                 │  only  │
   └─────────┘              ▼                                 └────────┘
                      ┌─────────┐
                      │  Peer   │
                      │ (TCP)   │
                      └─────────┘
```

---

## 📦 Core Components

### 1. **PeerLink (PM - Peer Manager)** 
📂 **Location:** `p2p/peer_link.py`  
🔧 **Main Class:** `PeerLink`

**Responsibilities:**
- ✅ Maintain TCP connection (asyncio streams)
- ✅ Perform Bitcoin handshake (version/verack)
- ✅ Wire-level framing (magic, cmd, length, checksum)
- ✅ Handle ping/pong internally
- ✅ Track connection metrics (latency, bytes sent/received)
- ✅ Emit standardized message events

**Key Methods:**
```python
async def run()                    # Main event loop
async def _do_handshake()          # Version/verack exchange
async def _send_message(cmd, payload)  # Frame & send
```

**Output Events:**
```python
{
  "peer_id": int,
  "cmd": str,              # "version", "inv", "headers", "__metadata__"
  "payload": bytes         # Raw or dict for internal cmds
}
```

**Forbidden:**
- ❌ Bitcoin logic interpretation (inv, headers, blocks)
- ❌ Peer selection
- ❌ Data validation

---

### 2. **NetworkManager (NM)**
📂 **Location:** `network/network_manager.py`  
🔧 **Main Class:** `NetworkManager`

**Responsibilities:**
- ✅ PeerLink lifecycle (connect, disconnect, monitor)
- ✅ Peer state & metadata tracking
- ✅ Message deserialization (inv, addr, addrv2)
- ✅ Route data messages to BM
- ✅ Schedule requests (getheaders, getdata)
- ✅ Peer selection for data requests
- ✅ Inbound connection listener
- ✅ Transaction broadcast coordination

**Key Data Structures:**
```python
self.peerlinks: Dict[(host,port), PeerLink]
self.peer_states: Dict[peer_id, PeerState]
self.known_addresses: Set[(ip, port)]
self.backfill_pending: Dict[hash, request_info]
self.pending_tx_broadcast: Dict[txid, tx_bytes]
```

**Key Methods:**
```python
async def run()                          # Main event loop
async def _handle_peer_event(event)     # Process PL messages
async def _process_bm_messages()        # Handle BM requests
async def _start_outbound(host, port)   # Create new peer
async def _broadcast_tx(txid, tx_bytes) # Send inv to all peers
```

**Communication Patterns:**
- 📥 From PeerLink: Bitcoin messages + metadata
- 📤 To PeerLink: getheaders, getdata, disconnect commands
- ➡️ To BM: headers, block, tx messages
- ⬅️ From BM: need_headers, need_blocks requests

**Forbidden:**
- ❌ Chain validation logic
- ❌ Block/header processing

---

### 3. **BlockchainManager (BM)**
📂 **Location:** `blockchain/blockchain_manager.py`  
🔧 **Main Class:** `BlockchainManager`

**Responsibilities:**
- ✅ Validate & manage block headers
- ✅ Validate & store blocks
- ✅ Transaction validation (via RPC to Bitcoin Core)
- ✅ Maintain chainstate
- ✅ Detect & handle reorgs (ForkHandler)
- ✅ Generate sync decisions (what to request)

**Core Components:**
```python
self.header_chain: HeaderChain      # Headers-first sync
self.block_chain: BlockChain        # Full block storage
self.fork_handler: ForkHandler      # Reorg detection
```

**Key Methods:**
```python
async def run()                      # Main event loop
async def _process_headers(msg)     # Validate & store headers
async def _process_block(msg)       # Validate & store blocks
async def _create_headers_response(req)  # Build headers message
```

**Decision Types:**
```python
{"type": "need_headers", "locator": [...]}
{"type": "need_blocks", "hashes": [...]}
{"type": "invalid_block", "reason": "..."}
```

**Forbidden:**
- ❌ Peer knowledge (peer-blind)
- ❌ Network message handling (inv, addr, ping)
- ❌ Socket operations

---

### 4. **RPCServer**
📂 **Location:** `rpc/rpc_server.py`  
🔧 **Main Class:** `RPCServer`  
🌐 **Endpoint:** `http://localhost:8888`

**Responsibilities:**
- ✅ HTTP JSON-RPC interface
- ✅ Route control commands to NM
- ✅ Direct read access to HeaderChain/BlockChain
- ✅ UTXO scanning for address queries (scantxoutset)
- ✅ Transaction broadcast (sendrawtransaction)

**RPC Methods:**
```python
addnode(node, cmd)           # Add/remove/onetry peer
getheader(height)            # Get header at height
getnetworkinfo()             # Network statistics
getblockcount()              # Current chain height
scantxoutset(address)        # Find UTXOs for address
sendrawtransaction(hex)      # Broadcast signed tx
```

**Communication:**
- 📤 To NM: addnode commands via `rpc_to_nm` queue
- 📖 Read-only: HeaderChain, BlockChain, UTXOScanner

---

### 5. **BlockBackfiller**
📂 **Location:** `blockchain/block_backfiller.py`  
🔧 **Main Class:** `BlockBackfiller`

**Responsibilities:**
- ✅ Detect missing blocks (headers exist but blocks don't)
- ✅ Request backfill via shared BM queue
- ✅ Periodic gap scanning (every 5 minutes)
- ✅ Resume from last position on restart

**Key Configuration:**
```python
DOWNLOAD_BLOCKS_FROM_HEIGHT = 900000  # Start point
poll_interval = 300  # 5 minutes between scans
```

**Operation Flow:**
1. Compare HeaderChain vs BlockChain heights
2. Find gaps (header exists, block missing)
3. Send `{"type": "need_blocks"}` to NM via shared queue
4. NM distributes requests across peers
5. Wait & re-scan periodically

---

## 🔄 Data Flow Diagrams

### Header Synchronization (Phase 4.1)
```
  Peer                PeerLink              NetworkManager         BlockchainManager
   │                     │                        │                       │
   │────headers(2000)───►│                        │                       │
   │                     │──PeerEvent────────────►│                       │
   │                     │  {cmd:"headers"}       │                       │
   │                     │                        │──DataMessage─────────►│
   │                     │                        │  {type:"headers"}     │
   │                     │                        │                       │
   │                     │                        │                       │─validate
   │                     │                        │                       │─append
   │                     │                        │                       │─update fork
   │                     │                        │                       │
   │                     │                        │◄──BMRequest───────────│
   │                     │                        │  {type:"need_headers",│
   │                     │◄──send_cmd─────────────│   locator:[...]}      │
   │                     │  "getheaders"          │                       │
   │◄───getheaders(loc)──│                        │                       │
```

### Block Download (Phase 4.2 - Backfiller)
```
  Backfiller          NetworkManager         PeerLink (×4)         Peer
     │                      │                      │                 │
     │──need_blocks────────►│                      │                 │
     │  [hash1..hash16]     │                      │                 │
     │                      │──distribute──────────►                 │
     │                      │  (4 blocks/peer)     │                 │
     │                      │                      │────getdata──────►
     │                      │                      │                 │
     │                      │                      │◄────block───────│
     │                      │◄─PeerEvent───────────│                 │
     │                      │  {cmd:"block"}       │                 │
     │                      │                      │                 │
     │                      │──validate & store──►BM                 │
     │                      │                      │                 │
   [5min]                   │                      │                 │
     │──scan gaps──────────►│                      │                 │
     │  (repeat)            │                      │                 │
```

### Transaction Broadcast (External RPC Client)
```
  RPC Client          RPCServer          NetworkManager         PeerLink (×N)
    │                     │                     │                      │
    │─sendrawtx(hex)─────►│                     │                      │
    │                     │──broadcast_tx──────►│                      │
    │                     │  (txid, bytes)      │                      │
    │                     │                     │──send_inv───────────►│×N
    │                     │                     │  MSG_TX              │
    │                     │◄─success────────────│                      │
    │◄──{result:txid}─────│                     │                      │
    │                     │                     │                      │
  [Peer requests tx]      │                     │                      │
    │                     │                     │◄─getdata─────────────│
    │                     │                     │──send_tx────────────►│
```

---

## 🔗 Queue-Based Communication

**All inter-layer communication uses asyncio queues (no direct calls):**

### PM → NM (per-peer queue)
```python
PeerEvent(peer_id, cmd, payload)
```
- Bitcoin messages: version, verack, inv, headers, block, tx, etc.
- Internal metadata: connection stats, errors

### NM → BM (shared queue: bm_in_q)
```python
DataMessage(peer_id, msg_type, payload, parsed_data)
```
- msg_type: "headers", "block", "tx", "getheaders"

### BM → NM (shared queue: bm_out_q)
```python
BMRequest(type, data, backfill_active)
```
- Types: "need_headers", "need_blocks", "send_headers"

### Backfiller → NM (shared bm_out_q)
```python
BMRequest(type="need_blocks", data={...})
```
- Shares same queue as BM
- NM routes based on request type

### RPC → NM (rpc_to_nm queue)
```python
{"command": "addnode", "args": {...}}
{"command": "broadcast_tx", "txid": ..., "tx_bytes": ...}
```

---

## 📁 Package Layout

```
Pynode/
├── pynode.py                  # Entry point, queue wiring
├── p2p/
│   ├── peer_link.py           # PeerLink class
│   └── wire.py                # Bitcoin wire protocol helpers
├── network/
│   └── network_manager.py     # NetworkManager class
├── blockchain/
│   ├── blockchain_manager.py  # BlockchainManager class
│   ├── headerchain.py         # HeaderChain (headers-first)
│   ├── blockchain.py          # BlockChain (block storage)
│   ├── fork_handler.py        # ForkHandler (reorg detection)
│   └── block_backfiller.py    # BlockBackfiller (maintenance)
├── rpc/
│   ├── rpc_server.py          # RPCServer (HTTP JSON-RPC)
│   └── utxo_scanner.py        # UTXOScanner (address queries)
└── core/
    ├── types.py               # Shared dataclasses
    ├── config.py              # Constants, RPC credentials
    ├── helpers.py             # Parsing utilities
    └── logging.py             # Global logging setup
```

---

## 🎯 Layer Responsibility Matrix

| Layer | ✅ Responsibilities | ❌ Forbidden |
|-------|---------------------|--------------|
| **PeerLink** | TCP, handshake, framing, ping/pong | Bitcoin logic, peer selection |
| **NetworkManager** | Peer lifecycle, routing, inventory, scheduling, hybrid coordination tasks (network + blockchain context) | Chain validation |
| **BlockchainManager** | Header/block/tx validation, chain state | Peer awareness, socket ops |
| **RPCServer** | HTTP interface, read-only queries | Direct layer calls (use queues) |
| **BlockBackfiller** | Gap detection, backfill requests | Block validation, peer selection |

---

## 🚀 Development Status

### ✅ Completed Phases
- **Phase 1-3:** PeerLink, NetworkManager, message routing
- **Phase 4.1:** Header synchronization (headers-first)
- **Phase 4.2:** Block backfilling with peer load balancing
- **Phase 4.3:** Fork handling and chain reorganization

### 🔮 Next Priority: Phase 5
**Transaction Relay & Mempool**
- Mempool storage & management
- Transaction validation
- Advanced relay (compact blocks)
- Fee estimation

---

## 🔐 Key Design Principles

1. **Layer Isolation:** No circular imports, strict separation of concerns
2. **Queue-Only Communication:** No direct cross-layer method calls
3. **Peer-Blind BM:** BlockchainManager never knows about peers
4. **Wire-Only PL:** PeerLink only handles TCP and framing
5. **Single Responsibility:** Each component has one clear purpose
6. **Testability:** Each layer can be tested in isolation
7. **Hybrid Task Rule:** Tasks requiring both peer/network context and blockchain intent belong in NetworkManager
8. **Scope Discipline:** End-user wallet functionality is out of scope for this repository

---

## 📊 Key Metrics & Configuration

### Performance
- Header sync: ~2000 headers per message
- Block batching: 16 blocks per peer during backfill
- Backfill scan: Every 5 minutes
- Fork depth limit: Configurable (`FORK_HANDLER_MAX_DEPTH`)

### Network
- Default outbound peers: `DESIRED_NOO_OUTBOUNDS` (config.py)
- Max total peers: `MAX_PEERS` (config.py)
- Inbound listening: `ENABLE_INBOUND`, port 8338
- RPC server: localhost:8888

### Storage
- Headers: `headers.dat` (80 bytes/header, sequential)
- Blocks: `blocks.dat` + `blocks.idx` (indexed by hash)

---

## 🛠️ Running the Node

```bash
# Standard mode (multiple peers)
python pynode.py

# Test mode (single peer)
python pynode.py test

# Check RPC availability
curl http://localhost:8888
```

---

## 📚 Reference Documents

- **Backfilling Guide:** `docs/BACKFILLING.md`
- **Development Charter:** `.github/copilot-instructions.md`
- **TODO Tracker:** `docs/TODO.md`

---

**Last Updated:** January 2026  
**Architecture Version:** 4.3.0
