# NetworkManager lives here
import asyncio
import time
from collections import deque
from datetime import datetime
from typing import Dict, Tuple, List, Any
from p2p.peer_link import PeerLink
from core.types import PeerState, InventoryVector, DataMessage, BMRequest
from core.helpers import parse_inv, parse_addr, parse_addrv2, parse_headers, inv_type_to_string
from core.config import MAGIC_MAINNET

try:
    from core.config import START_PEER_LIST, ENABLE_INBOUND, LISTEN_PORT, LISTEN_HOST, DESIRED_NOO_OUTBOUNDS, MAX_PEERS, PEER_CACHE_FILE, PEER_CACHE_MIN_COUNT
except ImportError:
    START_PEER_LIST = [ ("118.39.138.151", 8333),("158.220.103.193",8333), ("127.0.0.1", 8333),]
    ENABLE_INBOUND = False
    LISTEN_PORT = 8333
    LISTEN_HOST = "0.0.0.0"
    DESIRED_NOO_OUTBOUNDS = 8
    MAX_PEERS = 20
    PEER_CACHE_FILE = "peer_cache.json"
    PEER_CACHE_MIN_COUNT = 6

# After a peer connection drops, keep its address in the pool but don't re-dial it
# for this long. Addresses are never discarded on failure, so a transient outage
# (every dial failing) can't permanently drain the pool — it just parks addresses
# briefly and retries them once the cooldown expires.
ADDRESS_RETRY_COOLDOWN = 300  # seconds

# Disconnect reasons where the peer DID complete the handshake (proven good).
# These addresses are made immediately re-dialable rather than parked on the
# retry cooldown: a transient drop — especially a local network blip that
# severs many sockets at once — should recover fast, not bench known-good peers
# for 5 minutes. Only dial/handshake failures (connect_timeout, connect_failed,
# handshake_failed, error) keep the cooldown, since those addresses are unproven.
ESTABLISHED_DISCONNECT_REASONS = {
    "peer_eof", "peer_reset", "ping_timeout", "network_error", "clean", "user",
}

# Disconnect reasons where the TCP connection never even established — the
# address is unreachable, so it's removed from the pool entirely rather than
# parked for retry (no point re-dialing something that won't accept a socket).
# Safe to drop because the DNS low-water re-seed refills the pool, and the
# offline-pause prevents a real outage from draining it (we don't dial when
# the host has no internet, so we don't remove addresses during an outage).
DEAD_DIAL_REASONS = {"connect_timeout", "connect_failed"}

# When the eligible address pool falls below this, re-seed from DNS so dead
# addresses (connect_timeout/connect_failed) can't dominate and starve dialing.
ADDRESS_POOL_LOW_WATER = 25

# Connectivity-probe endpoints (public DNS resolvers on TCP :53 — highly
# available) and how often to re-check while the host's internet is down.
# When no probe succeeds we pause outbound dialing instead of thrashing the
# address pool with dials that all fail "network unreachable".
INTERNET_PROBE_ENDPOINTS = [("1.1.1.1", 53), ("8.8.8.8", 53), ("9.9.9.9", 53)]
INTERNET_PROBE_TIMEOUT = 3       # seconds per probe attempt
NETWORK_DOWN_RECHECK_INTERVAL = 20  # seconds between checks while offline
FAST_DIAL_INTERVAL = 3  # seconds between dials while below the desired peer minimum
HEADER_CATCHUP_INTERVAL = 5   # seconds between header-sync liveness checks while behind
HEADER_RESP_TIMEOUT = 10      # seconds to wait for a getheaders response before retrying
MAX_HEADER_SYNC_TIMEOUTS = 2  # consecutive getheaders timeouts before giving up on a peer
HEADER_APPLY_TIMEOUT = 5      # seconds to wait for BM to apply a header batch before next request
HEADER_SYNC_ROTATE_HEADERS = 100000  # rotate sync peer after this many headers in one stream
HEADER_SYNC_ROTATE_SECONDS = 60      # ...or after this many seconds, whichever comes first


def log_nm_msg(direction: str, peer_id: int, msg_type: str, details: str = ""):
    """
    Log NetworkManager P2P message flow
    direction: 'recv' (from PL), 'send' (to PL), 'fwd_bm' (to BM), 'from_bm' (from BM)
    """
    symbols = {
        'recv': '📥',
        'send': '📤', 
        'fwd_bm': '➡️',
        'from_bm': '⬅️'
    }
    symbol = symbols.get(direction, '❓')
    
    if direction == 'recv':
        print(f"[NM←PL{peer_id}] {symbol} {msg_type} {details}")
    elif direction == 'send':
        print(f"[NM→PL{peer_id}] {symbol} {msg_type} {details}")
    elif direction == 'fwd_bm':
        print(f"[NM→BM] {symbol} {msg_type} from PL{peer_id} {details}")
    elif direction == 'from_bm':
        print(f"[NM⬅BM] {symbol} {msg_type} {details}")


class NetworkManager:
    """Manages peer connections and routes messages between PeerLinks and BlockchainManager"""
    
    def __init__(self, bm_in_queue: asyncio.Queue, bm_out_queue: asyncio.Queue, mode: str = "test", start_height: int = 0, rpc_queue: asyncio.Queue | None = None, header_chain=None):
        self.bm_in_q = bm_in_queue
        self.bm_out_q = bm_out_queue
        self.mode = mode
        self.current_height = start_height
        self.rpc_queue = rpc_queue  # Queue for RPC commands
        self.header_chain = header_chain  # Read-only reference for building locators
        self.start_time = time.time()

        # Peer management
        self.peerlinks = {}  # (host, port) -> PeerLink
        self.peer_states: Dict[int, PeerState] = {}  # peer_id -> PeerState
        self.peer_event_q = asyncio.Queue()  # Queue for peer events
        self.next_peer_id = 0

        # Address management
        self.known_addresses = set()  # Set of (ip, port) tuples
        self.connected_addresses = set() # Set of (ip, port) tuples currently connected
        self.address_cooldown = {}  # (ip, port) -> earliest retry time (set on disconnect)
        self.priority_addresses = []  # (ip, port) to dial first (cached known-good peers on warm restart)
        self.addresses_received_since_last_report = 0  # Counter for maintenance reporting

        # Bandwidth accounting. Live peers carry their own counters; when a peer
        # disconnects we fold its final bytes into these accumulators so the
        # session total stays monotonic (and the per-interval rate stays correct
        # even when a busy peer leaves between reports).
        self.net_recv_closed = 0
        self.net_sent_closed = 0
        self._traffic_last_recv = 0
        self._traffic_last_sent = 0
        self._traffic_last_time = time.time()

        # Per-peer header sync queues: peer_id -> asyncio.Queue (active while _sync_headers_with_peer runs)
        self._peer_sync_queues: dict = {}
        
        # Backfill state management (Phase 4.2)
        self.backfill_active = False
        self.backfill_pending = {}  # block_hash -> {'peer_id': int, 'timestamp': float, 'retry_count': int}
        self.backfill_responsive_peers = set()  # peer_ids that actually delivered a block this session
        self.backfill_known_peers = set()  # peer_ids seen/asked this session (NOT proof of delivery)
        self.backfill_peer_load = {}  # peer_id -> pending_block_count
        self.backfill_blocks_received = 0  # Progress counter
        self.backfill_start_time = None
        self.backfill_monitor_task = None
        self.backfill_check_interval = 10  # Check every 10 seconds
        # Request standards (config): never flood a serving peer. Blocks are
        # queued and dispensed per peer up to an in-flight cap, topped up as
        # deliveries arrive, within a per-minute rate budget.
        from core.config import BACKFILL_INFLIGHT_PER_PEER, BACKFILL_BLOCKS_PER_MIN_PER_PEER
        self.backfill_inflight_per_peer = BACKFILL_INFLIGHT_PER_PEER
        self.backfill_rate_per_min = BACKFILL_BLOCKS_PER_MIN_PER_PEER
        self.backfill_queue = deque()  # block hashes awaiting assignment to a peer
        self.backfill_rate = {}  # peer_id -> [window_start_ts, blocks_requested_in_window]
        self.backfill_peer_bans = {}  # peer_id -> session attempts the ban still covers
        self.backfill_ban_attempts = 3  # zero-delivery peers sit out this many session restarts
        self.backfill_last_delivery_time = 0.0  # when the last backfill block arrived
        self.backfill_stall_threshold = 60  # no delivery for this long = session stalled
        self.backfill_recently_delivered = {}  # block_hash -> arrival time (delivered, possibly not yet applied)
        self.backfill_delivery_dedup_window = 300  # seconds a delivered block is excluded from re-request
        self.backfill_total_missing = 0  # chain-wide missing count as of the backfiller's last scan
        
        # Inbound connection handling
        self.server = None  # TCP server for inbound connections

        # Disconnect reason tally (peer-initiated vs self-initiated churn analysis).
        # Keyed by the reason PeerLink reports in the __disconnected__ event.
        self.disconnect_stats = {}

        # True while the host has no internet connectivity. While set, the
        # maintenance loop pauses outbound dialing and polls until it returns.
        self.network_down = False
        
        # Transaction broadcast (for wallet)
        self.pending_tx_broadcast = {}  # txid -> tx_bytes
        
        # Mempool tracking (Phase 5.1)
        self.seen_txids = set()  # Track requested/received transactions
        
        # Block announcement tracking (for duplicate suppression)
        self.currently_processing_block = None  # Block hash currently being processed
        self.duplicate_announcers = []  # Peers who announced the processing block

    def _is_invalid_address(self, ip: str) -> bool:
        """Check if IP address is invalid or non-routable (private/loopback/multicast)"""
        import ipaddress
        try:
            addr = ipaddress.ip_address(ip)
            
            # Filter out invalid addresses
            if addr.is_unspecified:  # 0.0.0.0 or ::
                return True
            if addr.is_loopback:  # 127.0.0.0/8 or ::1
                return True
            if addr.is_private:  # 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                return True
            if addr.is_multicast:  # 224.0.0.0/4
                return True
            if addr.is_link_local:  # 169.254.0.0/16 or fe80::/10
                return True
            if addr.is_reserved:  # Reserved ranges
                return True
                
            return False
        except ValueError:
            # Invalid IP format
            return True

    def _seed_addresses_from_dns(self):
        """Fetch peer addresses from DNS seeds and add to pool"""
        try:
            import dns.resolver
        except ImportError:
            print("[NM] ⚠️  dnspython not installed, skipping DNS seeding")
            return 0
        
        print("[NM] 🌐 Fetching peer addresses from DNS seeds...")

        seeds = [
            "seed.bitcoin.sipa.be",
            "dnsseed.bluematt.me",
            "dnsseed.bitcoin.dashjr.org",
            "seed.bitcoin.sprovoost.nl",
            "seed.bitcoin.wiz.biz",
        ]

        # Resolver strategy: try explicit public nameservers FIRST, then fall back
        # to the system default. On some hosts the system resolver times out on
        # these seed lookups (observed here: 0 addresses), which silently starved
        # the address pool. Per-nameserver timeout is short with a longer overall
        # lifetime so a slow nameserver rolls over to the next one.
        def _make_resolver(nameservers):
            r = dns.resolver.Resolver(configure=(nameservers is None))
            if nameservers:
                r.nameservers = nameservers
            r.timeout  = 2.0   # per-nameserver attempt
            r.lifetime = 8.0   # total budget across nameservers
            return r

        resolvers = [
            ("public", _make_resolver(["1.1.1.1", "8.8.8.8", "9.9.9.9"])),
            ("system", _make_resolver(None)),
        ]

        count_before = len(self.known_addresses)

        for seed in seeds:
            resolved_any = False
            for label, resolver in resolvers:
                for rtype in ("A", "AAAA"):
                    try:
                        for a in resolver.resolve(seed, rtype):
                            self.known_addresses.add((str(a), 8333))
                            resolved_any = True
                    except dns.resolver.NoAnswer:
                        pass  # seed has no record of this type — not an error
                    except Exception as e:
                        print(f"[NM] ⚠️  DNS {rtype} {seed} via {label}: {type(e).__name__}")
                if resolved_any:
                    break  # this resolver answered; don't retry with the backup
            if not resolved_any:
                print(f"[NM] ⚠️  No addresses from {seed} (all resolvers failed)")

        added = len(self.known_addresses) - count_before
        print(f"[NM] ✅ Added {added} addresses to pool from DNS")
        return added

    async def _connect_peer(self, host: str, port: int):
        """Connect to a peer given host and port"""
        if (host, port) in self.peerlinks:
            print(f"[NM] Already connected to {host}:{port}")
            return

        peer_id = self.next_peer_id
        self.next_peer_id += 1
        
        print(f"[NM] Creating PeerLink to {host}:{port} (peer_id={peer_id})")
        
        # Create peer state
        self.peer_states[peer_id] = PeerState(
            peer_id=peer_id,
            host=host,
            port=port,
            connected=True,
            connection_time=time.time()
        )
        
        # Create and start PeerLink (outbound)
        peer = PeerLink(host, port, peer_id=peer_id, nm_queue=self.peer_event_q, start_height=self.current_height)
        self.peerlinks[(host, port)] = peer
        self.connected_addresses.add((host, port))
        asyncio.create_task(peer.run())
        
        # Request peer addresses after handshake completes
        async def _request_addr_after_handshake():
            await asyncio.sleep(5)
            await self._send_getaddr(peer_id)
        asyncio.create_task(_request_addr_after_handshake())
    
    async def _handle_inbound_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle new inbound connection"""
        # Get peer address
        peername = writer.get_extra_info('peername')
        if peername:
            host, port = peername[0], peername[1]
        else:
            host, port = "unknown", 0
        
        # Check max peers
        if len(self.peerlinks) >= MAX_PEERS:
            print(f"[NM] ⛔ Max peers reached ({MAX_PEERS}), rejecting inbound from {host}:{port}")
            writer.close()
            await writer.wait_closed()
            return

        peer_id = self.next_peer_id
        self.next_peer_id += 1
        
        print(f"[NM] 📥 Accepting inbound connection from {host}:{port} (peer_id={peer_id})")
        
        # Create peer state
        self.peer_states[peer_id] = PeerState(
            peer_id=peer_id,
            host=host,
            port=port,
            connected=True,
            connection_time=time.time()
        )
        
        # Create PeerLink for inbound connection
        peer = PeerLink(
            host, port, 
            peer_id=peer_id, 
            nm_queue=self.peer_event_q, 
            start_height=self.current_height,
            reader=reader,
            writer=writer,
            is_inbound=True
        )
        self.peerlinks[(host, port)] = peer
        self.connected_addresses.add((host, port))
        asyncio.create_task(peer.run())
        
        # Request addresses after a delay to allow handshake to complete
        async def _request_addr_after_handshake():
            await asyncio.sleep(3)
            await self._send_getaddr(peer_id)
        asyncio.create_task(_request_addr_after_handshake())
    
    async def _start_listening(self):
        """Start TCP server to accept inbound connections"""
        if not ENABLE_INBOUND:
            print("[NM] 🔇 Inbound connections disabled")
            return
        
        try:
            self.server = await asyncio.start_server(
                self._handle_inbound_connection,
                LISTEN_HOST,
                LISTEN_PORT
            )
            
            addrs = ', '.join(str(sock.getsockname()) for sock in self.server.sockets)
            print(f"[NM] 🎧 Listening for inbound connections on {addrs}")
            
            async with self.server:
                await self.server.serve_forever()
                
        except OSError as e:
            print(f"[NM] ❌ Failed to start server: {e}")
            print(f"[NM]    (Port {LISTEN_PORT} may be in use or requires privileges)")
        except asyncio.CancelledError:
            print("[NM] 🔇 Listener cancelled")
            if self.server:
                self.server.close()
                await self.server.wait_closed()
            raise

    async def _startup(self):
        """Initialize connections: DNS → cache OR hardcoded (all up to DESIRED_NOO_OUTBOUNDS)"""

        # OFFLINE MODE - skip all peer connections
        if self.mode == "offline":
            print("[NM] 🔌 OFFLINE MODE - No peer connections will be made")
            print("[NM] 📡 RPC server still available on localhost:8888")
            return
        
        # Populate the address pool from every source. All dialing — initial fill
        # and ongoing top-up — is done by _fast_dial_loop, so there's a single
        # connect path. Cached known-good peers are queued to be dialed first for
        # a fast warm restart.
        dns_count = self._seed_addresses_from_dns()
        cached_peers = self._load_peer_cache()

        for peer in START_PEER_LIST:
            self.known_addresses.add(peer)

        if cached_peers:
            for peer in cached_peers:
                self.known_addresses.add(peer)
            self.priority_addresses = list(cached_peers[:DESIRED_NOO_OUTBOUNDS])
            print(f"[NM] 📂 {len(cached_peers)} cached peers queued first; "
                  f"pool seeded (DNS {dns_count}, hardcoded {len(START_PEER_LIST)})")
        else:
            print(f"[NM] 📋 Pool seeded (DNS {dns_count}, hardcoded {len(START_PEER_LIST)}); "
                  f"fast-dial loop will fill to {DESIRED_NOO_OUTBOUNDS}")

        if not self.known_addresses:
            print(f"[NM] ❌ No peers available (no cache, no hardcoded, DNS failed)")

    async def _handle_inv(self, peer_id: int, payload: bytes):
        """Process 'inv' message - request blocks and transactions we need"""
        try:
            from core.helpers import parse_inv_message
            
            items = parse_inv_message(payload)
            
            # Filter for blocks and transactions
            block_items = []
            tx_items = []
            
            for item in items:
                inv_type = item['type']
                if inv_type == 2 or inv_type == 0x40000002:  # MSG_BLOCK or MSG_WITNESS_BLOCK
                    block_items.append(item)
                elif inv_type == 1 or inv_type == 0x40000001:  # MSG_TX or MSG_WITNESS_TX
                    tx_items.append(item)
            
            if block_items:
                # Update known inventory for this peer
                from core.types import InventoryVector
                if peer_id in self.peer_states:
                    for item in block_items:
                        inv_vec = InventoryVector(inv_type=2, hash=item['hash'])
                        self.peer_states[peer_id].known_inv.add(inv_vec)
                
                # Check if this is a duplicate of the block currently being processed
                if len(block_items) == 1:
                    block_hash = block_items[0]['hash']
                    
                    if self.currently_processing_block == block_hash:
                        # Duplicate announcement during processing - track silently
                        self.duplicate_announcers.append(peer_id)
                    else:
                        # New block (or different block)
                        # Log new block announcement
                        hash_hex = block_hash[::-1].hex()
                        print("")
                        timestamp = datetime.now().strftime("%#d/%#m/%y - %H.%M")
                        print(f"************ New block! ************ [{timestamp}]")
                        print(f"[NM] 📦 New block inv from PL{peer_id}: {hash_hex[:6]}.....{hash_hex[-6:]}")
                        
                        # Mark as currently processing
                        self.currently_processing_block = block_hash
                        self.duplicate_announcers = []  # Reset for this new block
                        
                        # Request block via getdata
                        await self._send_getdata(peer_id, block_items)
                else:
                    # Multiple blocks in one inv (bulk sync)
                    log_nm_msg('recv', peer_id, 'inv', f"{len(block_items)} blocks, {len(tx_items)} txs")
                    for item in block_items[:3]:
                        hash_hex = item['hash'][::-1].hex()
                        print(f"     Block: {hash_hex[:6]}.....{hash_hex[-6:]}")
                    if len(block_items) > 3:
                        print(f"     ... and {len(block_items) - 3} more blocks")
                    
                    # Request all blocks via getdata
                    await self._send_getdata(peer_id, block_items)
            
            if tx_items:
                # Phase 5.1: TX deduplication (node3.py pattern)
                # Only request TXs we haven't seen yet (not in seen_txids)
                new_tx_items = []
                for item in tx_items:
                    # item['hash'] is already bytes in LE
                    txid = item['hash'][::-1].hex()
                    
                    if txid not in self.seen_txids:
                        new_tx_items.append(item)
                        self.seen_txids.add(txid)
                
                if new_tx_items:
                    # Request new transactions (silent deduplication - common during sync)
                    await self._send_getdata(peer_id, new_tx_items)
                # Silently ignore duplicate TXs (very common from multiple peers)
            
        except Exception as e:
            print(f"[NM] ❌ Error parsing inv from peer {peer_id}: {e}")
            import traceback
            traceback.print_exc()

    def _handle_addr(self, peer_id: int, payload: bytes):
        """Process 'addr' message - store addresses for peer discovery"""
        try:
            addresses = parse_addr(payload)
            
            # Spam protection: per-message limit (50 addresses)
            original_count = len(addresses)
            if original_count > 50:
                addresses = addresses[:50]
            
            # Spam protection: cumulative limit per peer (1000 addresses lifetime)
            if peer_id in self.peer_states:
                peer_state = self.peer_states[peer_id]
                if peer_state.addresses_received + len(addresses) > 1000:
                    if not peer_state.address_limit_warned:
                        print(f"[NM] ⚠️  Peer {peer_id} exceeded lifetime address limit (1000) - ignoring further addresses")
                        peer_state.address_limit_warned = True
                    return
                peer_state.addresses_received += len(addresses)
            
            # Add to known addresses (with filtering)
            new_count = 0
            for addr in addresses:
                ip = addr['ip']
                port = addr['port']
                
                # Filter invalid/private IPs
                if self._is_invalid_address(ip):
                    continue
                    
                if (ip, port) not in self.known_addresses and (ip, port) not in self.connected_addresses:
                    self.known_addresses.add((ip, port))
                    new_count += 1
            
            # Update counter for maintenance reporting
            self.addresses_received_since_last_report += new_count
                
        except Exception as e:
            print(f"[NM] ❌ Error parsing addr from peer {peer_id}: {e}")

    def _handle_addrv2(self, peer_id: int, payload: bytes):
        """Process 'addrv2' message - store addresses for peer discovery (BIP 155)"""
        try:
            addresses = parse_addrv2(payload)
            
            # Spam protection: per-message limit (50 addresses)
            original_count = len(addresses)
            if original_count > 50:
                addresses = addresses[:50]
            
            # Spam protection: cumulative limit per peer (1000 addresses lifetime)
            if peer_id in self.peer_states:
                peer_state = self.peer_states[peer_id]
                if peer_state.addresses_received + len(addresses) > 1000:
                    if not peer_state.address_limit_warned:
                        print(f"[NM] ⚠️  Peer {peer_id} exceeded lifetime address limit (1000) - ignoring further addresses")
                        peer_state.address_limit_warned = True
                    return
                peer_state.addresses_received += len(addresses)
                
            # Add to known addresses (with filtering)
            new_count = 0
            for addr in addresses:
                # Only support IPv4/IPv6 for now
                if addr['network_id'] in [1, 2]:
                    ip = addr['ip']
                    port = addr['port']
                    
                    # Filter invalid/private IPs
                    if self._is_invalid_address(ip):
                        continue
                        
                    if (ip, port) not in self.known_addresses and (ip, port) not in self.connected_addresses:
                        self.known_addresses.add((ip, port))
                        new_count += 1
            
            # Update counter for maintenance reporting
            self.addresses_received_since_last_report += new_count
                
        except Exception as e:
            print(f"[NM] ❌ Error parsing addrv2 from peer {peer_id}: {e}")

    async def _route_to_bm(self, peer_id: int, msg_type: str, payload: bytes, parsed_data=None):
        """Forward data message to BlockchainManager"""
        data_msg = DataMessage(
            peer_id=peer_id,
            msg_type=msg_type,
            payload=payload,
            parsed_data=parsed_data
        )
        
        await self.bm_in_q.put(data_msg)
        # log_nm_msg('fwd_bm', peer_id, msg_type, f"({len(payload)} bytes)")

    async def _handle_peer_events(self):
        """Process events from PeerLinks"""
        print("[NM] Starting peer event handler")
        
        while True:
            try:
                event = await self.peer_event_q.get()
                peer_id = event.get("peer_id")
                cmd = event.get("cmd")
                payload = event.get("payload", b"")
                
                # Update peer state timestamp
                if peer_id in self.peer_states:
                    self.peer_states[peer_id].last_recv = time.time()
                
                # Phase 3: Parse network-level messages
                if cmd == "inv":
                    await self._handle_inv(peer_id, payload)
                
                elif cmd == "addr":
                    self._handle_addr(peer_id, payload)
                
                elif cmd == "addrv2":
                    self._handle_addrv2(peer_id, payload)
                
                # Route data messages to BlockchainManager
                elif cmd == "headers":
                    if peer_id in self._peer_sync_queues:
                        # Active sync loop waiting — feed the queue
                        await self._peer_sync_queues[peer_id].put(payload)
                    else:
                        # Unsolicited push (sendheaders) — forward directly to BM
                        log_nm_msg('recv', peer_id, 'headers', f"({len(payload)} bytes) [push]")
                        try:
                            headers = parse_headers(payload)
                            await self._route_to_bm(peer_id, cmd, payload, headers)
                        except Exception as e:
                            print(f"[NM] ❌ Error parsing headers: {e}")
                
                elif cmd == "block":
                    # log_nm_msg('recv', peer_id, 'block', f"({len(payload)} bytes)")
                    
                    # Mark this peer as knowing this block
                    try:
                        # Extract block hash from payload (skip version, prev_hash, merkle_root)
                        # Hash is computed from the first 80 bytes (block header)
                        if len(payload) >= 80:
                            import hashlib
                            block_header = payload[:80]
                            block_hash = hashlib.sha256(hashlib.sha256(block_header).digest()).digest()
                            
                            # Track in backfill if active
                            if self.backfill_active and block_hash in self.backfill_pending:
                                # Block arrived - mark peer as responsive and remove from pending
                                peer_info = self.backfill_pending[block_hash]
                                peer_id_who_sent = peer_info['peer_id']
                                
                                self.backfill_responsive_peers.add(peer_id_who_sent)
                                del self.backfill_pending[block_hash]
                                self.backfill_last_delivery_time = time.time()
                                self.backfill_recently_delivered[block_hash] = self.backfill_last_delivery_time
                                
                                # Decrement peer load
                                if peer_id_who_sent in self.backfill_peer_load:
                                    self.backfill_peer_load[peer_id_who_sent] = max(0, self.backfill_peer_load[peer_id_who_sent] - 1)
                                
                                # Increment received counter
                                self.backfill_blocks_received += 1

                                # Log progress every 100 blocks
                                if self.backfill_blocks_received % 100 == 0:
                                    remaining = len(self.backfill_pending) + len(self.backfill_queue)
                                    peers = len(self.backfill_responsive_peers)
                                    print(f"[NM] 📊 Backfill: {self.backfill_blocks_received} received this session, {remaining} to go of ~{self.backfill_total_missing} missing chain-wide, {peers} peers")

                                # Windowed dispensing: top the peer back up once
                                # it has worked through half its in-flight cap.
                                if self.backfill_queue and self.backfill_peer_load.get(peer_id_who_sent, 0) <= self.backfill_inflight_per_peer // 2:
                                    await self._backfill_topup([peer_id_who_sent])
                            
                            if peer_id in self.peer_states:
                                from core.types import InventoryVector
                                inv_item = InventoryVector(inv_type=2, hash=block_hash)
                                self.peer_states[peer_id].known_inv.add(inv_item)
                    except Exception as e:
                        print(f"[NM] ⚠️  Could not extract block hash: {e}")
                    
                    await self._route_to_bm(peer_id, cmd, payload)
                
                elif cmd == "tx":
                    # Phase 5.1: Silent TX forwarding (very frequent, mempool logs show progress)
                    await self._route_to_bm(peer_id, cmd, payload)
                
                # Route getheaders to BlockchainManager for response
                elif cmd == "getheaders":
                    log_nm_msg('recv', peer_id, 'getheaders', "")
                    await self._route_to_bm(peer_id, cmd, payload)
                
                # Handle getdata - check for pending tx broadcasts, else forward to BM
                # NOTE: Current implementation is a SHORTCUT for wallet TX broadcast.
                # TX-only getdata is handled here (from pending_tx_broadcast dict),
                # and NOT forwarded to BM to avoid redundant notfound messages.
                # FUTURE: When implementing full TX relay (mempool), move TX handling
                # to BM and allow BM to query mempool/pending broadcasts.
                elif cmd == "getdata":
                    log_nm_msg('recv', peer_id, 'getdata', f"({len(payload)} bytes)")
                    # Check if any TX requests match our pending broadcasts
                    tx_only = await self._handle_getdata_tx(peer_id, payload)
                    # Only forward to BM if there are block requests (not TX-only)
                    if not tx_only:
                        await self._route_to_bm(peer_id, cmd, payload)
                
                # Handle notfound - peer doesn't have requested data
                elif cmd == "notfound":
                    log_nm_msg('recv', peer_id, 'notfound', f"({len(payload)} bytes)")
                    
                    if self.backfill_active:
                        try:
                            # Parse notfound items (same structure as inv)
                            items = parse_inv(payload)
                            
                            blocks_to_retry = []
                            for item in items:
                                inv_type = item['type']
                                block_hash = item['hash']
                                
                                # Check if it's a block that we're waiting for in backfill
                                if (inv_type == 2 or inv_type == 0x40000002) and block_hash in self.backfill_pending:
                                    peer_info = self.backfill_pending[block_hash]
                                    
                                    # Only handle if this is the peer we asked
                                    if peer_info['peer_id'] == peer_id:
                                        # Remove from pending immediately (don't wait for timeout)
                                        del self.backfill_pending[block_hash]
                                        
                                        # Decrement peer load
                                        if peer_id in self.backfill_peer_load:
                                            self.backfill_peer_load[peer_id] = max(0, self.backfill_peer_load[peer_id] - 1)
                                        
                                        # Mark for immediate retry with a different peer
                                        blocks_to_retry.append(block_hash)
                                        
                                        print(f"[NM] ⚠️  Peer {peer_id} doesn't have block - will retry with different peer")
                            
                            # Immediately redistribute these blocks to other peers
                            if blocks_to_retry:
                                await self._redistribute_blocks_immediately(blocks_to_retry)
                                
                        except Exception as e:
                            print(f"[NM] ❌ Error handling notfound: {e}")
                
                # Other protocol messages (log but don't process yet)
                elif cmd in ["sendheaders", "feefilter", "sendcmpct"]:
                    log_nm_msg('recv', peer_id, cmd, f"({len(payload)} bytes)")
                
                elif cmd == "__disconnected__":
                    # Get peer info
                    peer_info = ""
                    host, port = None, None
                    
                    if peer_id in self.peer_states:
                        state = self.peer_states[peer_id]
                        host, port = state.host, state.port
                        peer_info = f"({host}:{port})"

                    # Tally why the connection ended (peer-initiated vs ours).
                    reason = payload.decode(errors="replace") if payload else "unknown"
                    self.disconnect_stats[reason] = self.disconnect_stats.get(reason, 0) + 1

                    print(f"[NM] 🔌 Peer {peer_id} {peer_info} disconnected [{reason}]")
                    
                    # Always remove from backfill tracking on disconnect (before peer_states check)
                    if self.backfill_active:
                        # Remove peer from tracking
                        self.backfill_responsive_peers.discard(peer_id)
                        self.backfill_known_peers.discard(peer_id)
                        if peer_id in self.backfill_peer_load:
                            del self.backfill_peer_load[peer_id]
                        
                        self.backfill_rate.pop(peer_id, None)

                        # Return this peer's in-flight blocks to the queue so
                        # other peers pick them up immediately (within caps)
                        # instead of waiting for the backfiller's next scan.
                        requeued = 0
                        for block_hash, info in list(self.backfill_pending.items()):
                            if info['peer_id'] == peer_id:
                                del self.backfill_pending[block_hash]
                                self.backfill_queue.append(block_hash)
                                requeued += 1

                        if requeued > 0:
                            print(f"[NM] 🗑️  Peer {peer_id} disconnected - requeued {requeued} in-flight blocks for other peers")
                    
                    if peer_id in self.peer_states:
                        # Remove from peer_states
                        peer_state = self.peer_states.pop(peer_id)
                        # Remove from peerlinks (fold its byte counters into the
                        # closed-accumulators first, so the session total survives).
                        gone = self.peerlinks.pop((peer_state.host, peer_state.port), None)
                        if gone is not None:
                            self.net_recv_closed += getattr(gone, "bytes_recv", 0)
                            self.net_sent_closed += getattr(gone, "bytes_sent", 0)
                        # Remove from connected_addresses
                        if (peer_state.host, peer_state.port) in self.connected_addresses:
                            self.connected_addresses.remove((peer_state.host, peer_state.port))

                        if host and port:
                            if reason in DEAD_DIAL_REASONS:
                                # Unreachable address (TCP never established). Drop it from
                                # the pool entirely — retrying it just wastes dial slots.
                                self.known_addresses.discard((host, port))
                                self.address_cooldown.pop((host, port), None)
                                print(f"[NM] ℹ️  Peer {host}:{port} unreachable [{reason}] - removed from pool")
                            elif reason in ESTABLISHED_DISCONNECT_REASONS:
                                # Proven-good peer (handshake completed). Make it
                                # immediately re-dialable so a transient drop recovers
                                # fast instead of being benched for the cooldown.
                                self.address_cooldown.pop((host, port), None)
                                print(f"[NM] ℹ️  Peer {host}:{port} disconnected [{reason}] - re-dialable immediately (proven good)")
                            else:
                                # Reachable but handshake/unknown failure - unproven address.
                                # Park it on a retry cooldown so we don't hammer it, while
                                # keeping it in the pool for a later attempt.
                                self.address_cooldown[(host, port)] = time.time() + ADDRESS_RETRY_COOLDOWN
                                print(f"[NM] ℹ️  Peer {host}:{port} disconnected [{reason}] - cooldown {ADDRESS_RETRY_COOLDOWN}s")

                else:
                    # Unknown or unhandled message
                    log_nm_msg('recv', peer_id, cmd, f"({len(payload)} bytes) [unhandled]")
                
            except asyncio.CancelledError:
                print("[NM] Event handler cancelled")
                raise
            except Exception as e:
                print(f"[NM] ❌ Error handling event: {e}")
                import traceback
                traceback.print_exc()

    async def _relay_block(self, block_hash: bytes):
        """Relay block inventory to all peers that don't have it"""
        from core.types import InventoryVector
        from p2p.wire import make_message
        import struct
        
        # Create inventory vector for the block
        inv_item = InventoryVector(inv_type=2, hash=block_hash)  # MSG_BLOCK = 2
        
        # Create inv payload
        # count (varint) + type (4 bytes) + hash (32 bytes)
        payload = b'\x01' + struct.pack("<I", 2) + block_hash
        
        msg = make_message("inv", payload, MAGIC_MAINNET)
        
        count = 0
        relayed_to = []
        for (host, port), peer in self.peerlinks.items():
            peer_state = self.peer_states.get(peer.peer_id)
            
            # Skip if peer already knows this block
            if peer_state and inv_item in peer_state.known_inv:
                continue
                
            if peer.writer and not peer.writer.is_closing():
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                    
                    # Mark as known for this peer
                    if peer_state:
                        peer_state.known_inv.add(inv_item)
                    count += 1
                    relayed_to.append(f"PL{peer.peer_id}")
                except Exception as e:
                    print(f"[NM] ⚠️  Failed to relay block to PL{peer.peer_id}: {e}")
                    # We don't need to disconnect here, the PeerLink loop will likely catch it soon
        
        if count > 0:
            print(f"[NM] 📢 Block relayed to {count} peers")
            # Report duplicates that arrived during processing
            if self.duplicate_announcers:
                dup_count = len(self.duplicate_announcers)
                print(f"[NM] ℹ️  Also announced by {dup_count} more peer{'s' if dup_count > 1 else ''} during processing")
        else:
            print(f"[NM] 📢 No relay needed (all peers already know this block)")
        
        # Clear processing state
        self.currently_processing_block = None
        self.duplicate_announcers = []

    async def _broadcast_tx_inv(self, txid_hex: str):
        """Broadcast transaction inventory to all peers (for wallet tx broadcast)
        
        BUG FIX: Detects SegWit transactions and uses MSG_WITNESS_TX (0x40000001)
        instead of MSG_TX (1). Bitcoin Core tolerates incorrect inv types, but
        other nodes enforce strict protocol matching between advertised and actual
        transaction format.
        """
        from p2p.wire import make_message
        import struct
        
        # Convert txid to bytes (little-endian internal format)
        txid_bytes = bytes.fromhex(txid_hex)[::-1]
        
        # Detect if transaction is SegWit by checking for marker/flag bytes
        # SegWit format: version(4) + marker(0x00) + flag(0x01) + ...
        tx_bytes = self.pending_tx_broadcast.get(txid_hex)
        is_segwit = False
        if tx_bytes and len(tx_bytes) > 5:
            # Check for SegWit marker (0x00) and flag (0x01) after version
            is_segwit = tx_bytes[4] == 0x00 and tx_bytes[5] == 0x01
        
        # Use correct inventory type
        # MSG_TX = 1 (legacy, non-witness)
        # MSG_WITNESS_TX = 0x40000001 (witness transaction, BIP144)
        msg_type = 0x40000001 if is_segwit else 1
        
        # Create inv payload: count (varint) + type (4 bytes) + hash (32 bytes)
        payload = b'\x01' + struct.pack("<I", msg_type) + txid_bytes
        
        msg = make_message("inv", payload, MAGIC_MAINNET)
        
        count = 0
        for (host, port), peer in self.peerlinks.items():
            if peer.writer and not peer.writer.is_closing():
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                    count += 1
                except Exception as e:
                    print(f"[NM] ⚠️  Failed to send tx inv to PL{peer.peer_id}: {e}")
        
        tx_type = "witness" if is_segwit else "legacy"
        print(f"[NM] 📢 Broadcast {tx_type} tx inv {txid_hex[:16]}... to {count} peers")

    async def _handle_getdata_tx(self, peer_id: int, payload: bytes) -> bool:
        """Handle getdata request for transactions we're broadcasting
        
        ARCHITECTURAL NOTE:
        This is a minimal implementation for wallet transaction broadcast.
        It only handles TX requests for transactions in pending_tx_broadcast.
        
        FUTURE IMPLEMENTATION:
        When adding full mempool support, this logic should move to BM:
        - BM maintains mempool + pending broadcasts
        - BM handles all TX getdata requests
        - NM just routes getdata to BM like it does for blocks
        
        Current behavior prevents redundant notfound from BM for wallet TXs.
        
        Returns:
            True if all items were TX requests (don't forward to BM)
            False if there were block requests (forward to BM)
        """
        from io import BytesIO
        from core.helpers import read_varint
        from p2p.wire import make_message
        import struct
        
        stream = BytesIO(payload)
        count = read_varint(stream)
        
        peer = None
        for (host, port), p in self.peerlinks.items():
            if p.peer_id == peer_id:
                peer = p
                break
        
        if not peer or not peer.writer or peer.writer.is_closing():
            return False
        
        tx_count = 0
        handled_count = 0
        
        for i in range(count):
            inv_type = struct.unpack("<I", stream.read(4))[0]
            inv_hash = stream.read(32)
            
            # Check if TX request (MSG_TX=1 or MSG_WITNESS_TX=0x40000001)
            if inv_type == 1 or inv_type == 0x40000001:
                tx_count += 1
                # Convert to txid (big-endian for lookup)
                txid_hex = inv_hash[::-1].hex()
                
                if txid_hex in self.pending_tx_broadcast:
                    tx_bytes = self.pending_tx_broadcast[txid_hex]
                    
                    # Build tx message
                    msg = make_message("tx", tx_bytes, MAGIC_MAINNET)
                    
                    try:
                        peer.writer.write(msg)
                        await peer.writer.drain()
                        print(f"[NM] 📤 Sent tx {txid_hex[:16]}... to PL{peer_id}")
                        handled_count += 1
                    except Exception as e:
                        print(f"[NM] ⚠️  Failed to send tx to PL{peer_id}: {e}")
        
        # Return True if ALL items were TX requests (handled or not)
        # This prevents forwarding TX-only getdata to BM
        return tx_count == count

    async def _handle_bm_requests(self):
        """Handle requests from BlockchainManager"""
        print("[NM] Starting BM request handler")
        
        while True:
            try:
                request = await self.bm_out_q.get()
                
                if not isinstance(request, BMRequest):
                    print(f"[NM] ⚠️  Received non-BMRequest: {type(request)}")
                    continue
                
                # Handle different request types
                if request.type == "need_headers":
                    # Orphan-block trigger: fan-out getheaders to all connected peers
                    log_nm_msg('from_bm', -1, 'need_headers', "fan-out to all peers")
                    for peer in list(self.peerlinks.values()):
                        if peer.writer and not peer.writer.is_closing():
                            asyncio.create_task(self._sync_headers_with_peer(peer.peer_id))
                elif request.type == "send_headers":
                    # Send headers response to specific peer
                    peer_id = request.data["peer_id"]
                    payload = request.data["payload"]
                    log_nm_msg('from_bm', peer_id, 'send_headers', f"({len(payload)} bytes)")
                    await self._send_headers_to_peer(peer_id, payload)
                elif request.type == "relay_block":
                    # Relay block to other peers
                    block_hash = request.data["hash"]
                    log_nm_msg('from_bm', -1, 'relay_block', f"")
                    await self._relay_block(block_hash)
                elif request.type == "send_block":
                    # Send block to specific peer
                    peer_id = request.data["peer_id"]
                    block_payload = request.data["payload"]
                    log_nm_msg('from_bm', peer_id, 'send_block', f"({len(block_payload)} bytes)")
                    await self._send_block_to_peer(peer_id, block_payload)
                elif request.type == "send_notfound":
                    # Send notfound to specific peer
                    peer_id = request.data["peer_id"]
                    notfound_payload = request.data["payload"]
                    log_nm_msg('from_bm', peer_id, 'send_notfound', f"({len(notfound_payload)} bytes)")
                    await self._send_notfound_to_peer(peer_id, notfound_payload)
                elif request.type == "send_getdata":
                    # Send getdata request (from backfiller or BM)
                    items = request.data["items"]
                    is_backfill = request.data.get("is_backfill", False)
                    
                    if is_backfill:
                        # Route to backfill handler
                        log_nm_msg('from_backfiller', -1, 'send_getdata', f"{len(items)} blocks")
                        await self._handle_backfill_request(items)
                    else:
                        # Normal getdata - send to any available peer
                        peer_id = self._get_any_available_peer()
                        if peer_id is not None:
                            await self._send_getdata(peer_id, items)
                        else:
                            print(f"[NM] ⚠️  No available peers for getdata request")
                elif request.type == "tx_stored":
                    # Phase 5.1: BM notifying that TX was stored in mempool
                    # Already added to seen_txids by NM when inv was received
                    # This is just a confirmation
                    txid = request.data["txid"]
                    # Ensure txid is in seen_txids
                    if txid not in self.seen_txids:
                        self.seen_txids.add(txid)
                else:
                    print(f"[NM] ❓ Unknown request type: {request.type}")
                    
            except asyncio.CancelledError:
                print("[NM] BM request handler cancelled")
                raise
            except Exception as e:
                print(f"[NM] ❌ Error handling BM request: {e}")
                import traceback
                traceback.print_exc()
    
    async def _send_getaddr(self, peer_id: int):
        """Send getaddr message to request peer addresses"""
        # Find the peer
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                from p2p.wire import make_message
                
                # getaddr has no payload
                msg = make_message("getaddr", b"", MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                    print(f"[NM] 📬 Requested addresses from peer {peer_id} ({host}:{port})")
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    print(f"[NM] ⚠️  Failed to send getaddr to peer {peer_id}: {e}")
                return
    
    async def _send_headers_to_peer(self, peer_id: int, payload: bytes):
        """Send headers message to a specific peer"""
        # Find the peer
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                log_nm_msg('send', peer_id, 'headers', f"to {host}:{port} ({len(payload)} bytes)")
                
                from p2p.wire import make_message
                
                msg = make_message("headers", payload, MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    print(f"[NM] ⚠️  Failed to send headers to peer {peer_id}: {e}")
                return
        
        print(f"[NM] ⚠️  Peer {peer_id} not found or not connected")
    
    async def _send_block_to_peer(self, peer_id: int, block_payload: bytes):
        """Send block message to a specific peer"""
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                log_nm_msg('send', peer_id, 'block', f"to {host}:{port} ({len(block_payload)} bytes)")
                
                from p2p.wire import make_message
                msg = make_message("block", block_payload, MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    print(f"[NM] ⚠️  Failed to send block to peer {peer_id}: {e}")
                return
        
        print(f"[NM] ⚠️  Peer {peer_id} not found for block send")
    
    async def _send_notfound_to_peer(self, peer_id: int, notfound_payload: bytes):
        """Send notfound message to a specific peer"""
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                from io import BytesIO
                from core.helpers import read_varint
                
                stream = BytesIO(notfound_payload)
                count = read_varint(stream)
                
                log_nm_msg('send', peer_id, 'notfound', f"{count} items")
                
                from p2p.wire import make_message
                msg = make_message("notfound", notfound_payload, MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    print(f"[NM] ⚠️  Failed to send notfound to peer {peer_id}: {e}")
                return
        
        print(f"[NM] ⚠️  Peer {peer_id} not found for notfound")
    
    async def _respond_notfound(self, peer_id: int, getdata_payload: bytes):
        """Respond to getdata with notfound message (we don't serve data yet)"""
        # Find the peer
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                # Parse inventory items from getdata
                from io import BytesIO
                from core.helpers import read_varint
                
                stream = BytesIO(getdata_payload)
                count = read_varint(stream)
                
                # Just echo back the same inventory items as "not found"
                # This tells the peer we don't have what they requested
                log_nm_msg('send', peer_id, 'notfound', f"{count} items")
                
                from p2p.wire import make_message
                msg = make_message("notfound", getdata_payload, MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    # Peer connection lost - PeerLink will send __disconnected__ event shortly
                    print(f"[NM] ⚠️  Peer {peer_id} connection lost during notfound: {e}")
                return
        
        print(f"[NM] ⚠️  Peer {peer_id} not found for notfound response")
    
    def _get_any_available_peer(self):
        """Get first available connected peer ID, or None if no peers available"""
        for (host, port), peer in self.peerlinks.items():
            if peer.writer and not peer.writer.is_closing():
                return peer.peer_id
        return None
    
    async def _send_getdata(self, peer_id: int, items: List[Dict[str, Any]]):
        """Send getdata message to request blocks/txs"""
        from core.helpers import create_getdata_message
        from p2p.wire import make_message
        
        # Check if peer still exists (avoid spam when peer already disconnected)
        if peer_id not in self.peer_states:
            # Peer already disconnected, blocks will be retried via monitor
            return
        
        # Find the peer
        for (host, port), peer in self.peerlinks.items():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                payload = create_getdata_message(items)
                
                # log_nm_msg('send', peer_id, 'getdata', f"{len(items)} items")
                
                msg = make_message("getdata", payload, MAGIC_MAINNET)
                try:
                    peer.writer.write(msg)
                    await peer.writer.drain()
                except (ConnectionError, ConnectionResetError, OSError) as e:
                    # Peer connection lost - PeerLink will send __disconnected__ event shortly
                    print(f"[NM] ⚠️  Peer {peer_id} connection lost during getdata: {e}")
                return
        
        # Only warn if peer exists in peer_states but not in peerlinks (unusual state)
        print(f"[NM] ⚠️  Peer {peer_id} not found for getdata")
    
    async def _handle_rpc_commands(self):
        """Handle commands from RPC server"""
        if not self.rpc_queue:
            return  # RPC not enabled
        
        print("[NM] Starting RPC command handler")
        
        while True:
            try:
                cmd = await self.rpc_queue.get()
                cmd_type = cmd.get("type")
                
                if cmd_type == "rpc_addnode":
                    host = cmd.get("host")
                    port = cmd.get("port")
                    action = cmd.get("action")
                    
                    print(f"[NM] 📡 RPC addnode: {host}:{port} {action}")
                    
                    if action == "onetry":
                        # Immediately try to connect
                        if (host, port) in self.peerlinks:
                            print(f"[NM] ⚠️  Already connected to {host}:{port}")
                        else:
                            await self._connect_peer(host, port)
                    
                    elif action == "add":
                        # Add to known addresses pool (will connect when slot available)
                        if (host, port) not in self.known_addresses:
                            self.known_addresses.add((host, port))
                            print(f"[NM] ✅ Added {host}:{port} to address pool")
                        else:
                            print(f"[NM] ⚠️  {host}:{port} already in address pool")
                    
                    elif action == "remove":
                        # Remove from pool and disconnect if connected
                        if (host, port) in self.known_addresses:
                            self.known_addresses.remove((host, port))
                            print(f"[NM] 🗑️  Removed {host}:{port} from address pool")
                        
                        # Disconnect if connected
                        if (host, port) in self.peerlinks:
                            peer = self.peerlinks[(host, port)]
                            print(f"[NM] 🔌 Disconnecting peer {peer.peer_id} ({host}:{port})")
                            if peer.writer and not peer.writer.is_closing():
                                peer.writer.close()
                                try:
                                    await peer.writer.wait_closed()
                                except Exception:
                                    pass
                
                elif cmd_type == "rpc_sendrawtransaction":
                    # Broadcast raw transaction to network
                    tx_hex = cmd.get("tx_hex")
                    txid = cmd.get("txid")
                    
                    print(f"[NM] 📡 RPC sendrawtransaction: {txid[:16]}...")
                    
                    # Store transaction for getdata requests
                    tx_bytes = bytes.fromhex(tx_hex)
                    self.pending_tx_broadcast[txid] = tx_bytes
                    
                    # Send inv to all connected peers
                    await self._broadcast_tx_inv(txid)
                
                else:
                    print(f"[NM] ❓ Unknown RPC command type: {cmd_type}")
                    
            except asyncio.CancelledError:
                print("[NM] RPC command handler cancelled")
                raise
            except Exception as e:
                print(f"[NM] ❌ Error handling RPC command: {e}")
                import traceback
                traceback.print_exc()

    async def _get_available_candidates(self) -> list:
        """Addresses available to dial: known, not currently connected, and past cooldown."""
        now = time.time()
        return [
            addr for addr in (self.known_addresses - self.connected_addresses)
            if now >= self.address_cooldown.get(addr, 0)
        ]

    async def _has_internet(self) -> bool:
        """Quick connectivity probe: can the host reach the internet at all?

        Concurrently attempts a TCP connect to a few highly-available public
        endpoints (DNS resolvers on :53) and returns True as soon as one
        succeeds. Used to distinguish "host is offline" (pause and wait) from
        "just dialing dead peer addresses" (keep trying).
        """
        async def _probe(host, port):
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=INTERNET_PROBE_TIMEOUT
                )
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                return True
            except Exception:
                return False

        results = await asyncio.gather(*[_probe(h, p) for h, p in INTERNET_PROBE_ENDPOINTS])
        return any(results)

    async def _fast_dial_loop(self):
        """Keep outbound peers topped up to the desired minimum.

        Dials one new peer every FAST_DIAL_INTERVAL while below target. DESIRED
        is a floor, so a few over is fine. Dials are fire-and-forget — the
        connect/handshake (and getaddr) run in the background, and dead/failed
        addresses are pruned by the disconnect handler. Pauses while the host is
        offline (so a real outage can't thrash the pool) and re-seeds from DNS if
        the eligible pool runs thin.
        """
        import random
        print(f"[NM] Starting fast-dial loop (every {FAST_DIAL_INTERVAL}s while < {DESIRED_NOO_OUTBOUNDS} outbound)")
        last_inet_check = 0.0
        last_reseed = 0.0
        inet_ok = True

        while True:
            await asyncio.sleep(FAST_DIAL_INTERVAL)

            outbound = sum(1 for p in self.peerlinks.values() if not p.is_inbound)
            if outbound >= DESIRED_NOO_OUTBOUNDS:
                continue  # at/above the minimum — nothing to do

            now = time.time()

            # Throttled connectivity check: probe at most once per recheck window
            # even though we loop every few seconds. Offline -> pause dialing so
            # we don't fail-and-prune the whole pool during an outage.
            if now - last_inet_check >= NETWORK_DOWN_RECHECK_INTERVAL:
                inet_ok = await self._has_internet()
                last_inet_check = now
                if not inet_ok and not self.network_down:
                    self.network_down = True
                    print(f"[NM] 🌐❌ No internet connection — pausing outbound dialing until it returns")
                elif inet_ok and self.network_down:
                    self.network_down = False
                    print(f"[NM] 🌐✅ Internet connection restored — resuming outbound dialing")
            if not inet_ok:
                continue

            candidates = await self._get_available_candidates()
            if len(candidates) < ADDRESS_POOL_LOW_WATER and (now - last_reseed) >= 60:
                # Pool thin (dead addresses pruned / on cooldown) — top up from the
                # source of truth. DNS lookups block, so run them off the event loop.
                last_reseed = now
                print(f"[NM] 🌐 Eligible address pool low ({len(candidates)} < {ADDRESS_POOL_LOW_WATER}) - re-seeding from DNS")
                await asyncio.get_running_loop().run_in_executor(None, self._seed_addresses_from_dns)
                candidates = await self._get_available_candidates()
            if not candidates:
                continue

            # Prefer a queued cached/proven peer (warm restart) if it's still
            # eligible; otherwise dial a random pool address.
            eligible = set(candidates)
            choice = None
            while self.priority_addresses:
                p = self.priority_addresses.pop(0)
                if p in eligible:
                    choice = p
                    break
            if choice is None:
                choice = random.choice(candidates)

            host, port = choice
            await self._connect_peer(host, port)  # fire-and-forget; schedules getaddr after handshake
            # NOTE: we deliberately do NOT kick header sync per dial. Header
            # catch-up is a single sequential stream owned by _header_catchup_loop
            # — kicking a loop per peer made ~12 peers all fetch the same range
            # (BM discards the duplicates), crawling at ~40 headers/s instead of
            # the ~2000/s a single stream achieves.

    def _is_headers_current(self) -> bool:
        """True if the header tip is within ~2h of wall time (caught up to network)."""
        import struct
        hc = self.header_chain
        if not hc:
            return True  # nothing to sync against
        h = hc.get_height()
        if h < 0 or h >= len(hc.headers):
            return False
        tip = hc.headers[h]
        if len(tip) < 72:
            return False
        ts = struct.unpack('<I', tip[68:72])[0]
        return (time.time() - ts) < 7200

    async def _header_catchup_loop(self):
        """Sole driver of header catch-up: a single sequential sync stream.

        This is the only place header sync is started (we no longer kick a loop
        per peer dial — that made many peers fetch the same range redundantly).
        While behind and no sync loop is active, start ONE against a settled
        peer; the loop streams batches back-to-back at full speed (request next
        on receive). On failure it ends and we (re)start here, rotating peers.
        """
        import random
        while True:
            await asyncio.sleep(HEADER_CATCHUP_INTERVAL)
            if self._is_headers_current():
                continue
            if self._peer_sync_queues:
                continue  # the single sync stream is already running
            # Only pick peers connected long enough to have finished the
            # handshake, so getheaders isn't sent before verack.
            now = time.time()
            peers = []
            for p in self.peerlinks.values():
                if not (p.writer and not p.writer.is_closing()):
                    continue
                st = self.peer_states.get(p.peer_id)
                if st and (now - st.connection_time) >= 4:
                    peers.append(p)
            if peers:
                pid = random.choice(peers).peer_id
                print(f"[NM] 🔃 Header chain behind — (re)starting sync with PL{pid}")
                asyncio.create_task(self._sync_headers_with_peer(pid))

    def _traffic_totals(self):
        """Monotonic session totals (bytes): closed-peer accumulators plus the
        current counters of all live peers."""
        live_recv = sum(getattr(p, "bytes_recv", 0) for p in self.peerlinks.values())
        live_sent = sum(getattr(p, "bytes_sent", 0) for p in self.peerlinks.values())
        return self.net_recv_closed + live_recv, self.net_sent_closed + live_sent

    def _report_traffic(self):
        """Log ↓/↑ traffic since the last report, the implied GB/day rate, and
        the running session total. Called once per maintenance cycle."""
        MB = 1_000_000
        recv, sent = self._traffic_totals()
        now = time.time()
        elapsed = max(now - self._traffic_last_time, 1e-6)
        d_recv = recv - self._traffic_last_recv
        d_sent = sent - self._traffic_last_sent
        gb_day = ((d_recv + d_sent) / elapsed) * 86400 / 1_000_000_000

        print(f"[NM] 📊 Traffic: ↓{d_recv/MB:.1f} ↑{d_sent/MB:.1f} MB since last "
              f"({elapsed/60:.1f}m) → ~{gb_day:.2f} GB/day | session ↓{recv/MB:.1f} ↑{sent/MB:.1f} MB")

        self._traffic_last_recv = recv
        self._traffic_last_sent = sent
        self._traffic_last_time = now

    async def _maintain_connections(self):
        """Periodically check and maintain outbound connections"""
        print("[NM] Starting connection maintenance loop (2 min initial, then 10 min interval)")
        import random
        sleep_time = 120  # Initial 2 minute delay
        while True:
            try:
                await asyncio.sleep(sleep_time)
                uptime_s = int(time.time() - self.start_time)
                uptime = f"{uptime_s // 3600}h {(uptime_s % 3600) // 60}m {uptime_s % 60}s"
                now = datetime.now().strftime("%H:%M:%S")
                print(f"\n[NM] 🔄 Maintenance cycle starting (after {sleep_time}s delay) [{now}] (up {uptime})")
                sleep_time = 180  # Regular 3 minute interval after first run
                
                # Print connected peers list
                print(f"\n[NM] 📋 Connected Peers List:")
                print(f"{'ID':<5} {'Type':<10} {'Address':<30} {'Duration':<15}")
                print("-" * 65)
                current_time = time.time()
                
                # Snapshot dictionaries and sets to avoid concurrent modification issues
                peerlinks_snapshot = dict(self.peerlinks)
                peer_states_snapshot = dict(self.peer_states)
                known_addresses_snapshot = set(self.known_addresses)
                connected_addresses_snapshot = set(self.connected_addresses)
                
                # Filter for currently active peers
                active_peer_ids = [p.peer_id for p in peerlinks_snapshot.values()]
                
                for peer_id, state in peer_states_snapshot.items():
                    if peer_id in active_peer_ids:
                        peer_link = next((p for p in peerlinks_snapshot.values() if p.peer_id == peer_id), None)
                        if peer_link:
                            direction = "Inbound" if peer_link.is_inbound else "Outbound"
                            duration = int(current_time - state.connection_time)
                            hours = duration // 3600
                            minutes = (duration % 3600) // 60
                            duration_str = f"{hours}h {minutes}m"
                            
                            addr_str = f"{state.host}:{state.port}"
                            print(f"{peer_id:<5} {direction:<10} {addr_str:<30} {duration_str:<15}")
                print("-" * 65)
                
                print(f"[NM] 📊 Address pool: {len(known_addresses_snapshot)} total, {len(known_addresses_snapshot - connected_addresses_snapshot)} available")

                # Cumulative disconnect breakdown — peer-initiated (peer_eof,
                # peer_reset) vs ours (ping_timeout, connect_timeout, etc.).
                if self.disconnect_stats:
                    total_disc = sum(self.disconnect_stats.values())
                    breakdown = ", ".join(f"{r}={n}" for r, n in sorted(self.disconnect_stats.items(), key=lambda x: -x[1]))
                    print(f"[NM] 📊 Disconnects ({total_disc} total): {breakdown}")

                self._report_traffic()
                
                # Count outbound connections (use snapshot)
                outbound_count = sum(1 for p in peerlinks_snapshot.values() if not p.is_inbound)
                
                # Filling to the desired minimum is handled continuously by
                # _fast_dial_loop (one dial every few seconds while below target,
                # incl. offline-pause and DNS re-seed). Here we only report status.
                if outbound_count < DESIRED_NOO_OUTBOUNDS:
                    if self.network_down:
                        print(f"[NM] 📉 Outbound {outbound_count}/{DESIRED_NOO_OUTBOUNDS} — offline, dialing paused\n")
                    else:
                        print(f"[NM] 📉 Outbound {outbound_count}/{DESIRED_NOO_OUTBOUNDS} — fast-dial topping up\n")
                else:
                    print(f"[NM] ✅ Peer count OK: {outbound_count}/{DESIRED_NOO_OUTBOUNDS}\n")
                
                # Cache peers if we have enough connected (use snapshot)
                if len(peerlinks_snapshot) >= PEER_CACHE_MIN_COUNT:
                    self._save_peer_cache()
                
                # Report addresses received since last maintenance cycle (always print)
                print(f"[NM] 📬 {self.addresses_received_since_last_report} addresses received from peers since last update")
                self.addresses_received_since_last_report = 0  # Reset counter
                
                print()
                print(f"[NM] ✅ Maintenance cycle complete")
                        
            except asyncio.CancelledError:
                print("[NM] Connection maintenance loop cancelled")
                raise
            except Exception as e:
                print(f"[NM] ❌ Error in connection maintenance: {e}")
                import traceback
                traceback.print_exc()

    # ============================================================================
    # BACKFILL MANAGEMENT (Phase 4.2)
    # ============================================================================
    
    async def _handle_backfill_request(self, items: List[Dict[str, Any]]):
        """
        Handle backfill request from BlockBackfiller.

        The backfiller rescans every 60s and always sends the FULL missing list,
        because its scan only sees blocks persisted to disk. Blocks that are
        in flight, or delivered but still in BM's apply queue, look "missing"
        to it. Three cases:

        - Session active and delivering: ignore the request. Everything in it
          is already pending with a peer or queued for apply; re-requesting
          would download the same blocks again.
        - Session active but stalled (no delivery within stall threshold):
          ban zero-delivery peers, clear and restart fresh so stuck blocks
          rotate to other peers. This preserves the anti-deadlock behavior
          introduced in d523635 — stuck blocks are never merged-and-forgotten.
        - No session: start one, skipping blocks delivered within the dedup
          window (they are awaiting apply; the window expires so a block lost
          between delivery and apply can never be filtered out forever).

        Args:
            items: List of block items to request (format: [{'type': 2, 'hash': bytes}, ...])
        """
        block_hashes = [item['hash'] for item in items]
        now = time.time()
        newly_banned = set()

        # Every scan sends the full chain-wide missing list, so this is the
        # freshest total even when the request itself is ignored below.
        self.backfill_total_missing = len(block_hashes)

        if self.backfill_active:
            if (now - self.backfill_last_delivery_time) < self.backfill_stall_threshold:
                print(f"[NM] ℹ️  Backfill session delivering ({len(self.backfill_pending)} pending) - ignoring rescan of {len(block_hashes)} blocks")
                return

            # Stalled session: ban peers that were assigned blocks but delivered nothing
            assigned_peers = set(info['peer_id'] for info in self.backfill_pending.values())
            for peer_id in assigned_peers:
                if peer_id not in self.backfill_responsive_peers:
                    self.backfill_peer_bans[peer_id] = self.backfill_ban_attempts
                    newly_banned.add(peer_id)
                    print(f"[NM] 🚫 Peer {peer_id} banned from backfill for {self.backfill_ban_attempts} attempts (delivered 0 blocks)")

            # Clear previous session entirely - fresh restart
            remaining = len(self.backfill_pending)
            print(f"[NM] 🔄 Clearing stalled backfill session ({remaining} stale pending blocks)")
            await self._finish_backfill()

        # Bans persist for a few session attempts so a repeat zero-delivery
        # peer doesn't soak up assignments every single restart, but they
        # decay rather than last forever - a growing permanent ban set could
        # starve the backfill pool on a long-running node (the 66d25e5 fix).
        for peer_id in list(self.backfill_peer_bans):
            if peer_id in newly_banned:
                continue
            self.backfill_peer_bans[peer_id] -= 1
            if self.backfill_peer_bans[peer_id] <= 0:
                del self.backfill_peer_bans[peer_id]
                print(f"[NM] ♻️  Peer {peer_id} backfill ban expired - eligible again")

        # Drop blocks delivered within the dedup window: they are on their way
        # through BM's apply queue and just not persisted yet.
        self.backfill_recently_delivered = {
            h: t for h, t in self.backfill_recently_delivered.items()
            if (now - t) < self.backfill_delivery_dedup_window
        }
        fresh_hashes = [h for h in block_hashes if h not in self.backfill_recently_delivered]
        skipped = len(block_hashes) - len(fresh_hashes)
        if skipped > 0:
            print(f"[NM] ⏭️  Skipping {skipped} recently delivered blocks (awaiting apply)")
        if not fresh_hashes:
            print(f"[NM] ℹ️  All {len(block_hashes)} requested blocks recently delivered - nothing to fetch")
            return

        # Start new backfill session: queue everything, dispense within caps
        print(f"[NM] 🚀 Starting backfill: {len(fresh_hashes)} blocks queued (max {self.backfill_inflight_per_peer} in flight, {self.backfill_rate_per_min}/min per peer)")
        self.backfill_active = True
        self.backfill_start_time = now
        self.backfill_last_delivery_time = now  # grace period until first delivery
        self.backfill_pending = {}
        self.backfill_responsive_peers = set()
        self.backfill_known_peers = set()
        self.backfill_queue = deque(fresh_hashes)
        self.backfill_rate = {}

        dispatched = await self._backfill_topup(log_filter=True)
        print(f"[NM] 📦 Dispatched {dispatched} blocks to peers, {len(self.backfill_queue)} queued")
        
        # Start monitor task
        if self.backfill_monitor_task is None or self.backfill_monitor_task.done():
            self.backfill_monitor_task = asyncio.create_task(self._monitor_backfill_progress())
    
    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        """Format a duration as 45s / 12m 30s / 1h 24m."""
        seconds = int(seconds)
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds // 60}m {seconds % 60}s"
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"

    def _peer_alive(self, peer_id: int) -> bool:
        """True if the peer is still in peerlinks with an open writer."""
        for peer in self.peerlinks.values():
            if peer.peer_id == peer_id:
                return peer.writer is not None and not peer.writer.is_closing()
        return False

    def _backfill_eligible_peers(self, log_filter: bool = False) -> List[int]:
        """
        Peers eligible for backfill assignment: connected full nodes
        (NODE_NETWORK - pruned peers don't have historical blocks), not
        banned, and connected for at least 3 minutes.
        """
        MIN_PEER_AGE = 180  # 3 minutes
        NODE_NETWORK = 1
        now = time.time()
        eligible = []
        skipped_young = 0
        skipped_banned = 0
        skipped_pruned = 0
        for (host, port), peer in self.peerlinks.items():
            if peer.writer and not peer.writer.is_closing():
                if not (getattr(peer, 'remote_services', 0) & NODE_NETWORK):
                    skipped_pruned += 1
                    continue
                if peer.peer_id in self.backfill_peer_bans:
                    skipped_banned += 1
                    continue
                state = self.peer_states.get(peer.peer_id)
                if state and (now - state.connection_time) < MIN_PEER_AGE:
                    skipped_young += 1
                    continue
                eligible.append(peer.peer_id)

        if log_filter and (skipped_young or skipped_banned or skipped_pruned):
            print(f"[NM] 🔍 Backfill peer filter: {len(eligible)} eligible, {skipped_young} too young (<3min), {skipped_banned} banned, {skipped_pruned} pruned/no-NODE_NETWORK")
        return eligible

    def _backfill_rate_budget(self, peer_id: int, now: float) -> int:
        """Blocks this peer may still be asked for in the current 60s window."""
        window = self.backfill_rate.get(peer_id)
        if window is None or (now - window[0]) >= 60:
            self.backfill_rate[peer_id] = [now, 0]
            window = self.backfill_rate[peer_id]
        return max(0, self.backfill_rate_per_min - window[1])

    async def _backfill_topup(self, peer_ids: List[int] = None, log_filter: bool = False) -> int:
        """
        Feed eligible peers from the backfill queue, respecting the per-peer
        in-flight cap and per-minute rate budget. Called at session start, on
        block delivery, and every monitor cycle. Returns blocks dispatched.
        """
        if not self.backfill_active or not self.backfill_queue:
            return 0

        now = time.time()
        eligible = self._backfill_eligible_peers(log_filter=log_filter)
        if peer_ids is not None:
            wanted = set(peer_ids)
            eligible = [p for p in eligible if p in wanted]

        dispatched = 0
        for peer_id in eligible:
            if not self.backfill_queue:
                break
            load = self.backfill_peer_load.get(peer_id, 0)
            want = self.backfill_inflight_per_peer - load
            if want <= 0:
                continue
            want = min(want, self._backfill_rate_budget(peer_id, now), len(self.backfill_queue))
            if want <= 0:
                continue

            batch = [self.backfill_queue.popleft() for _ in range(want)]
            items = [{'type': 2, 'hash': h} for h in batch]
            await self._send_getdata(peer_id, items)

            for h in batch:
                self.backfill_pending[h] = {
                    'peer_id': peer_id,
                    'timestamp': now,
                    'retry_count': 0
                }
            self.backfill_peer_load[peer_id] = load + len(batch)
            self.backfill_rate[peer_id][1] += len(batch)
            self.backfill_known_peers.add(peer_id)
            dispatched += len(batch)

        return dispatched
    
    async def _monitor_backfill_progress(self):
        """
        Monitor backfill progress:
        - Check every 10 seconds
        - Discover new peers dynamically
        - Log progress
        - Detect completion
        
        Note: No retry logic here - backfiller handles all retries via 5-minute cycles
        """
        import time
        
        print(f"[NM] 👀 Backfill monitor started")
        
        try:
            while self.backfill_active:
                await asyncio.sleep(self.backfill_check_interval)
                
                now = time.time()
                
                # Safety check for start_time
                if self.backfill_start_time is None:
                    self.backfill_start_time = now
                
                elapsed = now - self.backfill_start_time
                
                # Check if complete (nothing in flight and nothing queued)
                if not self.backfill_pending and not self.backfill_queue:
                    print(f"[NM] ✅ Backfill session complete: all requested blocks delivered (apply may still be in progress)")
                    await self._finish_backfill()
                    return

                # Top up: newly eligible peers (aged past 3 min), refreshed
                # rate windows, and peers under their in-flight cap all get
                # fed from the queue here.
                await self._backfill_topup()

                # Log progress every cycle. In-flight is capped per peer; the
                # chain-wide missing count from the backfiller's last scan
                # gives the real remaining work.
                in_flight = len(self.backfill_pending)
                queued = len(self.backfill_queue)
                if in_flight > 0 or queued > 0:
                    print(f"[NM] 📊 Backfill session: {in_flight} in flight, {queued} queued, ~{self.backfill_total_missing} missing chain-wide, {len(self.backfill_responsive_peers)} responsive peers, {int(elapsed)}s elapsed")
        
        except asyncio.CancelledError:
            print(f"[NM] Backfill monitor cancelled")
            raise
        except Exception as e:
            print(f"[NM] ❌ Backfill monitor error: {e}")
            import traceback
            traceback.print_exc()
            await self._finish_backfill()
    
    async def _finish_backfill(self):
        """Clean up backfill state after completion"""
        import time
        
        if not self.backfill_active:
            return
        
        # Safety check for start_time
        elapsed = time.time() - (self.backfill_start_time or time.time())
        remaining = len(self.backfill_pending) + len(self.backfill_queue)
        received = self.backfill_blocks_received

        if remaining == 0:
            print(f"[NM] ✅ Backfill finished: {received} blocks received in {self._fmt_duration(elapsed)}")
        else:
            print(f"[NM] ⏹️  Backfill finished: {received} blocks received, {remaining} undelivered after {self._fmt_duration(elapsed)}")

        # Session summary with completion estimate from this session's rate.
        # backfill_total_missing is a snapshot from the scan that opened the
        # session, so subtract this session's deliveries to get what's left.
        still_missing = max(0, self.backfill_total_missing - received)
        if received > 0 and elapsed > 0 and still_missing > 0:
            eta = still_missing / (received / elapsed)
            print(f"[NM] ⏱️  {received} blocks received in {self._fmt_duration(elapsed)}. Estimated time to complete {still_missing} missing blocks: {self._fmt_duration(eta)}")
        else:
            print(f"[NM] ⏱️  {received} blocks received in {self._fmt_duration(elapsed)}")

        # Clear state
        self.backfill_active = False
        self.backfill_pending.clear()
        self.backfill_queue.clear()
        self.backfill_rate.clear()
        self.backfill_responsive_peers.clear()
        self.backfill_known_peers.clear()
        self.backfill_peer_load.clear()
        self.backfill_blocks_received = 0
        self.backfill_start_time = None
        
        # Cancel monitor task if running
        if self.backfill_monitor_task:
            self.backfill_monitor_task.cancel()
            self.backfill_monitor_task = None
    
    async def _redistribute_blocks_immediately(self, block_hashes: List[bytes]):
        """
        Return notfound blocks to the queue; the top-up dispenser reassigns
        them to other peers within the in-flight and rate caps.
        """
        if not self.backfill_active:
            return
        self.backfill_queue.extend(block_hashes)
        print(f"[NM] 🔄 Requeued {len(block_hashes)} notfound blocks for reassignment")
        await self._backfill_topup()
    
    def _load_peer_cache(self) -> List[Tuple[str, int]]:
        """Load cached peers from file. Returns list of (host, port) tuples or empty list."""
        import json
        import os
        
        if not os.path.exists(PEER_CACHE_FILE):
            return []
        
        try:
            with open(PEER_CACHE_FILE, 'r') as f:
                data = json.load(f)
            
            peers = data.get('peers', [])
            cached_count = len(peers)
            
            if cached_count > 0:
                # Convert to list of tuples
                peer_list = [(p['host'], p['port']) for p in peers]
                print(f"[NM] 💾 Loaded {cached_count} peers from cache (saved {data.get('saved_at', 'unknown')})")
                return peer_list
            
        except Exception as e:
            print(f"[NM] ⚠️  Failed to load peer cache: {e} - using hardcoded peers")
        
        return []
    
    def _save_peer_cache(self):
        """Save currently connected peers to cache file (if >= min count)"""
        import json
        from datetime import datetime
        
        if len(self.peerlinks) < PEER_CACHE_MIN_COUNT:
            return
        
        try:
            # Collect all connected peers
            peers = []
            for (host, port), pl in self.peerlinks.items():
                peers.append({
                    'host': host,
                    'port': port
                })
            
            cache_data = {
                'saved_at': datetime.now().isoformat(),
                'peer_count': len(peers),
                'peers': peers
            }
            
            with open(PEER_CACHE_FILE, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Only log on first save or periodically (don't spam)
            if not hasattr(self, '_last_cache_save'):
                print(f"[NM] 💾 Cached {len(peers)} peers to {PEER_CACHE_FILE}")
                self._last_cache_save = True
            
        except Exception as e:
            print(f"[NM] ⚠️  Failed to save peer cache: {e}")

    def _get_peerlink_by_id(self, peer_id: int):
        """Return PeerLink for peer_id if connected and writable, else None."""
        for peer in self.peerlinks.values():
            if peer.peer_id == peer_id and peer.writer and not peer.writer.is_closing():
                return peer
        return None

    def _build_getheaders_payload(self, locator) -> bytes:
        """Build getheaders wire payload from a list of locator hashes."""
        import struct
        from io import BytesIO
        from core.helpers import write_varint
        stream = BytesIO()
        stream.write(struct.pack("<i", 70015))
        write_varint(stream, len(locator))
        for h in locator:
            if isinstance(h, str):
                h = bytes.fromhex(h)
            stream.write(h)
        stream.write(b"\x00" * 32)  # stop hash = get up to 2000
        return stream.getvalue()

    async def _sync_headers_with_peer(self, peer_id: int, initial_delay: float = 0.0):
        """
        Self-contained getheaders loop for one peer.
        Runs until < 2000 headers returned, 5s timeout, or peer gone.
        Registered in _peer_sync_queues so _handle_peer_events feeds replies here.
        """
        from p2p.wire import make_message
        from io import BytesIO
        from core.helpers import read_varint, parse_headers

        if initial_delay:
            await asyncio.sleep(initial_delay)

        if self._peer_sync_queues:
            # Header catch-up is a single sequential stream — if any sync loop
            # is already running (this peer or another), don't start a second.
            return

        queue: asyncio.Queue = asyncio.Queue()
        self._peer_sync_queues[peer_id] = queue
        consecutive_timeouts = 0
        # Rotate off this peer after a bounded amount of work so a slow-but-
        # responding peer can't bottleneck the whole catch-up (single stream).
        # On break, the catch-up supervisor re-picks a (random) peer.
        stream_start_height = self.header_chain.get_height()
        stream_start_time = time.time()
        try:
            while True:
                if not self.header_chain:
                    break

                peer = self._get_peerlink_by_id(peer_id)
                if not peer:
                    break

                locator = self.header_chain.get_locator()
                payload = self._build_getheaders_payload(locator)

                tip_hex = locator[0][::-1].hex() if locator else "genesis"
                tip_height = self.header_chain.get_height()
                print(f"[NM] 📤 getheaders to PL{peer_id} from height {tip_height} ({tip_hex[:6]}.....{tip_hex[-6:]})")

                try:
                    peer.writer.write(make_message("getheaders", payload, MAGIC_MAINNET))
                    await peer.writer.drain()
                except (ConnectionError, OSError) as e:
                    print(f"[NM] ⚠️  getheaders send failed for PL{peer_id}: {e}")
                    break

                try:
                    raw_headers = await asyncio.wait_for(queue.get(), timeout=HEADER_RESP_TIMEOUT)
                except asyncio.TimeoutError:
                    # A transient timeout shouldn't kill the whole loop — retry the
                    # same peer from the current tip. Only give up (so the catch-up
                    # supervisor rotates to another peer) after several in a row.
                    consecutive_timeouts += 1
                    if consecutive_timeouts >= MAX_HEADER_SYNC_TIMEOUTS:
                        print(f"[NM] ⏰ getheaders timeout x{consecutive_timeouts} — giving up on PL{peer_id}")
                        break
                    continue
                consecutive_timeouts = 0

                try:
                    count = read_varint(BytesIO(raw_headers))
                except Exception:
                    break

                print(f"[NM] 📥 {count} headers from PL{peer_id}")
                height_before = self.header_chain.get_height()
                await self._route_to_bm(peer_id, "headers", raw_headers, parse_headers(raw_headers))

                if count < 2000:
                    break  # reached the peer's tip

                # Wait for BM to actually apply the batch before requesting the
                # next range. NM and BM share the HeaderChain object, so we just
                # watch the tip move. Without this NM outruns BM and rebuilds the
                # next locator from a stale tip — re-requesting the same headers
                # (the residual redundancy). This paces NM to BM, like the old
                # BM-driven sync, so every locator is fresh.
                applied = False
                deadline = time.time() + HEADER_APPLY_TIMEOUT
                while time.time() < deadline:
                    if self.header_chain.get_height() > height_before:
                        applied = True
                        break
                    await asyncio.sleep(0.02)
                if not applied:
                    # Tip didn't advance: caught up, duplicate, or non-connecting
                    # headers. Stop; the catch-up supervisor re-evaluates/rotates.
                    print(f"[NM] header batch from PL{peer_id} didn't advance tip — stopping sync")
                    break

                # Bounded work per peer: rotate so one slow peer can't dominate.
                gained = self.header_chain.get_height() - stream_start_height
                elapsed = time.time() - stream_start_time
                if gained >= HEADER_SYNC_ROTATE_HEADERS or elapsed >= HEADER_SYNC_ROTATE_SECONDS:
                    print(f"[NM] 🔄 Rotating sync peer (PL{peer_id}): +{gained} headers in {elapsed:.0f}s")
                    break
        finally:
            self._peer_sync_queues.pop(peer_id, None)

    async def run(self):
        """Main NetworkManager loop"""
        print("[NM] Starting NetworkManager (Phase 3)")

        # _startup runs as a task so _handle_peer_events is already running
        # when getheaders responses arrive — sync queues stay intact
        tasks = [
            asyncio.create_task(self._startup()),
            asyncio.create_task(self._handle_peer_events()),
            asyncio.create_task(self._handle_bm_requests()),
        ]

        # Peer-facing tasks: dialing, maintenance, header catch-up, inbound
        # listener. None of these belong in offline mode.
        if self.mode != "offline":
            tasks.extend([
                asyncio.create_task(self._maintain_connections()),
                asyncio.create_task(self._fast_dial_loop()),
                asyncio.create_task(self._header_catchup_loop()),
            ])
            if ENABLE_INBOUND:
                tasks.append(asyncio.create_task(self._start_listening()))
        
        # Add RPC handler if queue is provided
        if self.rpc_queue:
            tasks.append(asyncio.create_task(self._handle_rpc_commands()))
        
        # Run all tasks concurrently
        await asyncio.gather(*tasks)

