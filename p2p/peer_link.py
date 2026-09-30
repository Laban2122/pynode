# PeerLink / PM lives here
import asyncio
import socket
import struct
import time
import os
import hashlib

try:
    from .wire import dsha256, make_message, pack_net_addr, read_message
except ImportError:
    from wire import dsha256, make_message, pack_net_addr, read_message

# Bitcoin service flags
NODE_NETWORK = 1
NODE_NETWORK_LIMITED = (1 << 10)

# Protocol settings
try:
    from core.config import PEERLINK_VERSION, PEERLINK_USER_AGENT, MAGIC_MAINNET
    MY_VERSION = int(PEERLINK_VERSION)
    MY_USER_AGENT = PEERLINK_USER_AGENT.encode("ascii") if isinstance(PEERLINK_USER_AGENT, str) else bytes(PEERLINK_USER_AGENT)
    MY_MAGIC = bytes(MAGIC_MAINNET)
except Exception:
    # Fallback defaults for standalone runs where config import is unavailable.
    MY_VERSION = 70016
    MY_USER_AGENT = b"/pynode/"
    MY_MAGIC = bytes.fromhex("F9BEB4D9")

# Cap on the outbound TCP connect. A dead/unreachable host otherwise holds the slot
# for the OS TCP timeout (~21s on Windows, surfacing as WinError 121). Hardcoded: an
# implementation detail, not a user-facing knob.
CONNECT_TIMEOUT = 8  # seconds


class _CountingReader:
    """Wraps an asyncio StreamReader, tallying every byte read into the owning
    PeerLink's bytes_recv. Transparent: all other reader methods/attributes
    delegate to the wrapped reader, so existing read paths are unchanged."""
    def __init__(self, reader, owner):
        self._reader = reader
        self._owner = owner

    async def readexactly(self, n):
        data = await self._reader.readexactly(n)
        self._owner.bytes_recv += len(data)
        return data

    async def read(self, n=-1):
        data = await self._reader.read(n)
        self._owner.bytes_recv += len(data)
        return data

    def __getattr__(self, name):
        return getattr(self._reader, name)


class _CountingWriter:
    """Wraps an asyncio StreamWriter, tallying every byte written into the
    owning PeerLink's bytes_sent. All other methods (drain, close, get_extra_info,
    is_closing, ...) delegate, so NM/PL write sites need no changes."""
    def __init__(self, writer, owner):
        self._writer = writer
        self._owner = owner

    def write(self, data):
        self._owner.bytes_sent += len(data)
        return self._writer.write(data)

    def __getattr__(self, name):
        return getattr(self._writer, name)


class PeerLink:
    """Manages TCP connection and wire-level framing for a single peer"""
    
    def __init__(self, host: str, port: int, peer_id: int, nm_queue: asyncio.Queue, start_height: int = 0, 
                 reader: asyncio.StreamReader = None, writer: asyncio.StreamWriter = None, is_inbound: bool = False):
        self.host = host
        self.port = port
        self.peer_id = peer_id
        self.nm_queue = nm_queue
        self.start_height = start_height
        self.magic = MY_MAGIC  # chain identity from config (protocol.magic_mainnet)
        self.reader = reader  # Can be provided for inbound connections
        self.writer = writer  # Can be provided for inbound connections
        self.is_inbound = is_inbound  # True if connection was accepted (not initiated)
        self.running = False
        
        # Ping tracking
        self.ping_nonce = None
        self.ping_sent_time = None
        self.connection_start_time = None

        # Service bits the peer advertised in its version message (0 until
        # handshake). NM uses this to keep pruned peers (no NODE_NETWORK bit)
        # out of historical block backfill - they don't have old blocks.
        self.remote_services = 0

        # Bandwidth counters (wire bytes incl. 24-byte headers). Incremented by
        # the _CountingReader/_CountingWriter wrappers; NM aggregates these.
        self.bytes_recv = 0
        self.bytes_sent = 0

        # Why this connection ended. First-writer-wins: the path that first
        # detects the close records the true cause, so a later side-effect
        # (e.g. the ping loop tripping on an already-reset socket) can't
        # mislabel it. Reported to NM in the __disconnected__ event.
        #   peer_eof        - peer closed cleanly (sent EOF/FIN)
        #   peer_reset      - peer reset the socket (RST / abort)
        #   ping_timeout    - WE dropped it: no pong within PONG_TIMEOUT
        #   connect_timeout - WE gave up dialing
        #   network_error   - other OS/socket error
        #   handshake_failed- handshake did not complete
        #   error           - unexpected exception
        self.disconnect_reason = "unknown"

    def _set_disconnect_reason(self, reason: str):
        """Record the disconnect cause, first-writer-wins."""
        if self.disconnect_reason == "unknown":
            self.disconnect_reason = reason

    def _log_connection_lost(self, e: Exception):
        """Log a lost connection with its lifetime, for churn analysis."""
        duration = ""
        if self.connection_start_time:
            elapsed = time.time() - self.connection_start_time
            duration = f" (duration: {elapsed:.1f}s)"
        print(f"[PL{self.peer_id}] Connection lost: {type(e).__name__} [{self.disconnect_reason}]{duration}")

    def _make_version_payload(self) -> bytes:
        """Create version message payload"""
        version = MY_VERSION
        services = NODE_NETWORK_LIMITED  # Pruned node services (BIP 159)
        timestamp = int(time.time())
        
        # Peer address
        addr_recv = pack_net_addr(NODE_NETWORK, self.host, self.port)
        
        # Our address (dummy)
        if ':' in self.host:
            our_ip = "2001:db8::1"
        else:
            our_ip = "127.0.0.1"
        addr_from = pack_net_addr(0, our_ip, self.port)
        
        # Random nonce
        nonce = struct.unpack("<Q", os.urandom(8))[0]
        
        # User agent
        ua_len = bytes([len(MY_USER_AGENT)])
        
        # Starting height and relay flag
        start_height = self.start_height
        print (f"PeerLink {self.peer_id}: start_height={start_height}")
        relay = 1  # Enable relay to participate in the network
        
        # Pack the version payload
        payload = b""
        payload += struct.pack("<i", version)
        payload += struct.pack("<Q", services)
        payload += struct.pack("<q", timestamp)
        payload += addr_recv
        payload += addr_from
        payload += struct.pack("<Q", nonce)
        payload += ua_len + MY_USER_AGENT
        payload += struct.pack("<i", start_height)
        payload += struct.pack("<?", relay)
        
        return payload

    async def _do_handshake_inbound(self) -> bool:
        """Perform inbound handshake: wait for their version first, then send version + verack"""
        try:
            print(f"[PL{self.peer_id}] Starting inbound handshake with {self.host}:{self.port}")
            
            # 1. Receive peer version
            cmd, payload = await asyncio.wait_for(read_message(self.reader, self.magic), timeout=30.0)
            print(f"[PL{self.peer_id}] Received {cmd} ({len(payload)} bytes)")
            
            if cmd != "version":
                raise ConnectionError(f"Expected version, got {cmd}")
            self._parse_remote_version(payload)

            # 2. Send version
            version_payload = self._make_version_payload()
            version_msg = make_message("version", version_payload, self.magic)
            self.writer.write(version_msg)
            await self.writer.drain()
            print(f"[PL{self.peer_id}] Sent version")
            
            # 3. Send verack
            verack_msg = make_message("verack", b"", self.magic)
            self.writer.write(verack_msg)
            await self.writer.drain()
            print(f"[PL{self.peer_id}] Sent verack")
            
            # 4. Wait for verack (handle other protocol messages)
            got_verack = False
            max_attempts = 10
            
            for attempt in range(max_attempts):
                cmd, payload = await asyncio.wait_for(read_message(self.reader, self.magic), timeout=10.0)
                print(f"[PL{self.peer_id}] Received {cmd} ({len(payload)} bytes)")
                
                if cmd == "verack":
                    got_verack = True
                    break
                else:
                    # Ignore other protocol messages during handshake
                    continue
            
            if not got_verack:
                raise ConnectionError(f"Did not receive verack after {max_attempts} messages")
            
            print(f"[PL{self.peer_id}] ✅ Inbound handshake completed successfully")
            return True
            
        except asyncio.TimeoutError:
            print(f"[PL{self.peer_id}] ❌ Inbound handshake timeout")
            raise ConnectionError(f"Handshake timeout")
        except Exception as e:
            print(f"[PL{self.peer_id}] ❌ Inbound handshake failed: {e}")
            raise
    
    async def _do_handshake_outbound(self) -> bool:
        """Perform outbound handshake: send version first, then wait for version + verack"""
        try:
            print(f"[PL{self.peer_id}] Starting outbound handshake with {self.host}:{self.port}")
            
            # 1. Send version
            version_payload = self._make_version_payload()
            version_msg = make_message("version", version_payload, self.magic)
            self.writer.write(version_msg)
            await self.writer.drain()
            print(f"[PL{self.peer_id}] Sent version")
            
            # 2. Receive peer version
            cmd, payload = await asyncio.wait_for(read_message(self.reader, self.magic), timeout=30.0)
            print(f"[PL{self.peer_id}] Received {cmd} ({len(payload)} bytes)")
            
            if cmd != "version":
                raise ConnectionError(f"Expected version, got {cmd}")
            self._parse_remote_version(payload)

            # 3. Send verack
            verack_msg = make_message("verack", b"", self.magic)
            self.writer.write(verack_msg)
            await self.writer.drain()
            print(f"[PL{self.peer_id}] Sent verack")

            # 4. Wait for verack (handle other protocol messages)
            got_verack = False
            max_attempts = 10

            for attempt in range(max_attempts):
                cmd, payload = await asyncio.wait_for(read_message(self.reader, self.magic), timeout=10.0)
                print(f"[PL{self.peer_id}] Received {cmd} ({len(payload)} bytes)")

                if cmd == "verack":
                    got_verack = True
                    break
                else:
                    # Ignore other protocol messages during handshake
                    continue

            if not got_verack:
                raise ConnectionError(f"Did not receive verack after {max_attempts} messages")

            print(f"[PL{self.peer_id}] ✅ Outbound handshake completed successfully")
            return True
            
        except asyncio.TimeoutError:
            print(f"[PL{self.peer_id}] ❌ Outbound handshake timeout")
            raise ConnectionError(f"Handshake timeout")
        except Exception as e:
            print(f"[PL{self.peer_id}] ❌ Outbound handshake failed: {e}")
            raise
    
    def _parse_remote_version(self, payload: bytes):
        """Extract the peer's advertised service bits from its version payload."""
        if len(payload) >= 12:
            self.remote_services = struct.unpack("<Q", payload[4:12])[0]
            full = "full node" if self.remote_services & NODE_NETWORK else "pruned/limited"
            print(f"[PL{self.peer_id}] Peer services: 0x{self.remote_services:x} ({full})")

    async def _do_handshake(self) -> bool:
        """Perform handshake (routes to inbound or outbound version)"""
        if self.is_inbound:
            return await self._do_handshake_inbound()
        else:
            return await self._do_handshake_outbound()

    async def _handle_ping(self, payload: bytes):
        """Handle ping by sending pong"""
        if len(payload) == 8:
            nonce = int.from_bytes(payload, 'little')
            pong_msg = make_message("pong", payload, self.magic)
            self.writer.write(pong_msg)
            await self.writer.drain()
            print(f"[PL{self.peer_id}] ⏱️  Received ping (nonce={nonce}) → Sent pong")

    async def _handle_pong(self, payload: bytes):
        """Handle pong and calculate latency"""
        if len(payload) == 8:
            nonce = int.from_bytes(payload, 'little')
            
            # Check if this pong matches our ping
            if self.ping_nonce is not None and nonce == self.ping_nonce:
                latency_ms = int((time.time() - self.ping_sent_time) * 1000)
                print(f"[PL{self.peer_id}] 🏓 Received pong (nonce={nonce}) - Latency: {latency_ms}ms")
                
                # Clear ping tracking
                self.ping_nonce = None
                self.ping_sent_time = None
            else:
                print(f"[PL{self.peer_id}] 🏓 Received pong (nonce={nonce})")

    async def _send_ping(self):
        """Send ping to peer"""
        nonce = struct.unpack("<Q", os.urandom(8))[0]
        payload = struct.pack("<Q", nonce)
        
        ping_msg = make_message("ping", payload, self.magic)
        self.writer.write(ping_msg)
        await self.writer.drain()
        
        # Track ping for latency measurement
        self.ping_nonce = nonce
        self.ping_sent_time = time.time()
        
        print(f"[PL{self.peer_id}] 📤 Sent ping (nonce={nonce})")

    async def _ping_loop(self):
        """Send periodic pings and disconnect if no pong received"""
        PING_INTERVAL = 60  # Send ping every 60 seconds
        PONG_TIMEOUT = 60   # Disconnect if no pong in 60 seconds
        
        try:
            while self.running:
                await asyncio.sleep(PING_INTERVAL)
                if not self.running:
                    break
                    
                # Check if previous ping timed out (no pong received)
                if self.ping_nonce is not None:
                    elapsed = time.time() - self.ping_sent_time
                    if elapsed > PONG_TIMEOUT:
                        # WE are deciding to drop this peer (it stopped ponging).
                        self._set_disconnect_reason("ping_timeout")
                        print(f"[PL{self.peer_id}] ⏰ Ping timeout - no pong in {elapsed:.1f}s")
                        raise ConnectionError("Ping timeout - peer not responding")
                
                # Send new ping
                await self._send_ping()
                
        except asyncio.CancelledError:
            print(f"[PL{self.peer_id}] Ping loop cancelled")
            raise

    async def _message_loop(self):
        """Main message loop - read messages and emit events"""
        print(f"[PL{self.peer_id}] Entering message loop")
        
        try:
            while self.running:
                try:
                    cmd, payload = await read_message(self.reader, self.magic)
                    
                    # Handle ping/pong etiquette at wire level
                    if cmd == "ping":
                        await self._handle_ping(payload)
                        # Don't forward ping to NM, it's handled here
                        continue
                    
                    if cmd == "pong":
                        await self._handle_pong(payload)
                        # Don't forward pong to NM, it's handled here
                        continue
                    
                    # Emit standardized event to NetworkManager
                    event = {
                        "peer_id": self.peer_id,
                        "cmd": cmd,
                        "payload": payload
                    }
                    await self.nm_queue.put(event)
                    
                except asyncio.IncompleteReadError:
                    # Peer sent EOF/FIN — a clean close initiated by the peer.
                    self._set_disconnect_reason("peer_eof")
                    self.running = False  # let the ping loop exit without re-tripping
                    print(f"[PL{self.peer_id}] Peer closed connection cleanly")
                    break
                except (ConnectionResetError, ConnectionAbortedError) as e:
                    # Peer forcibly reset/aborted the socket (RST).
                    self._set_disconnect_reason("peer_reset")
                    self.running = False
                    self._log_connection_lost(e)
                    break
                except (ConnectionError, OSError) as e:
                    self._set_disconnect_reason("network_error")
                    self.running = False
                    self._log_connection_lost(e)
                    break
                    
        except asyncio.CancelledError:
            print(f"[PL{self.peer_id}] Message loop cancelled")
            raise
        except Exception as e:
            print(f"[PL{self.peer_id}] ❌ Unexpected error: {e}")
            raise

    async def disconnect(self):
        """Gracefully disconnect from peer"""
        print(f"[PL{self.peer_id}] Disconnecting...")
        self.running = False
        
        if self.writer and not self.writer.is_closing():
            try:
                self.writer.close()
                await self.writer.wait_closed()
                print(f"[PL{self.peer_id}] Disconnected")
            except Exception as e:
                print(f"[PL{self.peer_id}] Error during disconnect: {e}")

    async def _send_getaddr(self):
        """Send getaddr message to request peer addresses"""
        getaddr_msg = make_message("getaddr", b"", self.magic)
        self.writer.write(getaddr_msg)
        await self.writer.drain()
        print(f"[PL{self.peer_id}] 📤 Sent getaddr")

    async def _getaddr_loop(self):
        """Send getaddr once after 60 seconds"""
        try:
            await asyncio.sleep(60)
            if self.running:
                await self._send_getaddr()
        except asyncio.CancelledError:
            pass
        except (ConnectionError, OSError) as e:
            print(f"[PL{self.peer_id}] ⚠️  Connection lost during getaddr: {e}")
        except Exception as e:
            print(f"[PL{self.peer_id}] ❌ Error sending getaddr: {e}")

    async def run(self):
        """Main entry point - connect (if outbound), handshake, message loop"""
        # Phase flags so a failure can be attributed to dial / handshake /
        # post-handshake, keeping dial+handshake failures out of churn stats.
        connected = False
        handshake_done = False

        try:
            # Connect only if outbound (inbound already has reader/writer)
            if not self.is_inbound:
                print(f"[PL{self.peer_id}] Connecting to {self.host}:{self.port}...")
                self.reader, self.writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=CONNECT_TIMEOUT,
                )
                print(f"[PL{self.peer_id}] Connected")
            else:
                print(f"[PL{self.peer_id}] Accepted inbound connection from {self.host}:{self.port}")
            connected = True

            # Wrap streams so all reads/writes (handshake, ping/pong, and NM's
            # direct getheaders/getdata/tx writes) are byte-counted transparently.
            self.reader = _CountingReader(self.reader, self)
            self.writer = _CountingWriter(self.writer, self)

            # Configure TCP keepalive to prevent OS socket timeouts
            sock = self.writer.get_extra_info('socket')
            if sock:
                try:
                    # Basic keepalive - works on all platforms
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    print(f"[PL{self.peer_id}] ✓ TCP keepalive enabled")
                    
                    # Platform-specific tuning
                    import platform
                    if platform.system() == 'Windows':
                        try:
                            # Windows: (enable, idle_ms, interval_ms)
                            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 60000, 30000))
                            print(f"[PL{self.peer_id}] ✓ Windows keepalive tuned (60s idle, 30s interval)")
                        except (OSError, AttributeError):
                            print(f"[PL{self.peer_id}] ⚠ Windows keepalive tuning not available")
                    else:
                        # Linux/Unix
                        try:
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 10)
                            print(f"[PL{self.peer_id}] ✓ Linux/Unix keepalive tuned (60s idle, 30s interval)")
                        except (OSError, AttributeError):
                            print(f"[PL{self.peer_id}] ⚠ Linux/Unix keepalive tuning not available")
                            
                except Exception as e:
                    print(f"[PL{self.peer_id}] ⚠ Could not configure keepalive: {e}")
            
            self.connection_start_time = time.time()

            # Handshake
            await self._do_handshake()
            handshake_done = True

            # Start message loop and ping loop concurrently
            # Note: getaddr is NOT sent automatically - NetworkManager handles address requests
            self.running = True
            await asyncio.gather(
                self._message_loop(),
                self._ping_loop(),
                # self._getaddr_loop()  # Disabled - NM handles address requests on-demand
            )

            # Both loops returned without raising — a clean close that the
            # message loop already attributed (peer_eof). "clean" is only a
            # fallback if nothing more specific was recorded.
            self._set_disconnect_reason("clean")

        except KeyboardInterrupt:
            self._set_disconnect_reason("user")
            print(f"[PL{self.peer_id}] Interrupted by user")
        except asyncio.TimeoutError:
            self._set_disconnect_reason("connect_timeout")
            print(f"[PL{self.peer_id}] ⌛ Connect timed out after {CONNECT_TIMEOUT}s ({self.host}:{self.port})")
        except (ConnectionResetError, ConnectionAbortedError) as e:
            # Forcible reset. Attribute by phase: during handshake it's a
            # handshake failure; post-handshake the message loop already
            # tagged peer_reset (first-writer-wins keeps it).
            self._set_disconnect_reason("handshake_failed" if not handshake_done else "peer_reset")
            print(f"[PL{self.peer_id}] 🔌 Connection reset: {e} [{self.disconnect_reason}]")
        except OSError as e:
            # OSError covers dial failures (ConnectionRefused), WinError 64, and
            # (since ConnectionError subclasses OSError) the ping-loop timeout.
            if not connected:
                self._set_disconnect_reason("connect_failed")
            elif not handshake_done:
                self._set_disconnect_reason("handshake_failed")
            else:
                self._set_disconnect_reason("network_error")
            print(f"[PL{self.peer_id}] 🔌 Connection lost (OS error): {e} [{self.disconnect_reason}]")
        except Exception as e:
            self._set_disconnect_reason("error")
            print(f"[PL{self.peer_id}] ❌ Error: {e}")
        finally:
            await self.disconnect()
            # Notify NetworkManager about disconnection with reason
            event = {
                "peer_id": self.peer_id,
                "cmd": "__disconnected__",
                "payload": self.disconnect_reason.encode()  # Send reason as payload
            }
            await self.nm_queue.put(event)
