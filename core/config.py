# Configuration - config.json is the single source of truth.
#
# Every value the node consumes is defined in config.json and documented in
# config.info. There are deliberately no built-in fallback defaults: a missing
# file, invalid JSON, or a missing key is a fatal startup error, so config
# drift is caught immediately instead of the node silently running on stale
# built-in values.
import json
import sys
from pathlib import Path

# Deployed as a PyInstaller exe: config.json lives next to the exe, wherever
# the process was launched from. Running from source: cwd (the repo root).
if getattr(sys, "frozen", False):
    _CONFIG_PATH = Path(sys.executable).resolve().parent / "config.json"
else:
    _CONFIG_PATH = Path("config.json")

# section -> known keys. Anything in config.json not listed here triggers a
# warning: it is either a typo or a leftover from a removed feature.
_KNOWN_KEYS = {
    "rpc": {"host", "port"},
    "network": {
        "desired_outbound_peers", "max_peers", "enable_inbound",
        "listen_port", "listen_host", "start_peers",
        "peer_cache_file", "peer_cache_min_count",
    },
    "protocol": {"magic_mainnet", "version", "user_agent", "genesis_header"},
    "blockchain": {
        "download_blocks_from_height", "fork_handler_max_depth",
        "enable_tx_validation",
        "backfill_inflight_per_peer", "backfill_blocks_per_min_per_peer",
    },
}


def _fail(msg: str):
    print(f"[CONFIG] FATAL: {msg}")
    raise SystemExit(1)


def _load() -> dict:
    if not _CONFIG_PATH.exists():
        _fail(f"{_CONFIG_PATH} not found (must sit next to the exe, or in the working directory when running from source - see config.info)")
    try:
        with open(_CONFIG_PATH, "r") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        _fail(f"invalid JSON in {_CONFIG_PATH}: {e}")

    print(f"[CONFIG] Loaded configuration from {_CONFIG_PATH}")

    for section, values in cfg.items():
        if section not in _KNOWN_KEYS:
            print(f"[CONFIG] WARNING: unknown section '{section}' in config.json - ignored")
            continue
        if isinstance(values, dict):
            for key in values:
                if key not in _KNOWN_KEYS[section]:
                    print(f"[CONFIG] WARNING: unknown key '{section}.{key}' in config.json - ignored")
    return cfg


_cfg = _load()


def _require(section: str, key: str):
    try:
        return _cfg[section][key]
    except (KeyError, TypeError):
        _fail(f"missing required key '{section}.{key}' in config.json")


# ============================================================================
# Pynode RPC server (JSON-RPC for wallet/tools; localhost binding recommended)
# ============================================================================
RPC_HOST: str = _require("rpc", "host")
RPC_PORT: int = int(_require("rpc", "port"))

# ============================================================================
# P2P Network
# ============================================================================
DESIRED_NOO_OUTBOUNDS: int = _require("network", "desired_outbound_peers")
MAX_PEERS: int = _require("network", "max_peers")
ENABLE_INBOUND: bool = _require("network", "enable_inbound")
LISTEN_PORT: int = _require("network", "listen_port")
LISTEN_HOST: str = _require("network", "listen_host")

# JSON stores peers as [[host, port], ...]; the node uses (host, port) tuples
START_PEER_LIST = [tuple(peer) for peer in _require("network", "start_peers")]

PEER_CACHE_FILE: str = _require("network", "peer_cache_file")
PEER_CACHE_MIN_COUNT: int = _require("network", "peer_cache_min_count")

# ============================================================================
# Protocol
# ============================================================================
MAGIC_MAINNET: bytes = bytes.fromhex(_require("protocol", "magic_mainnet"))

# Genesis block header - defines chain identity alongside MAGIC_MAINNET.
# Single source of truth: HeaderChain and ForkHandler both derive from this.
GENESIS_HEADER: bytes = bytes.fromhex(_require("protocol", "genesis_header"))
if len(GENESIS_HEADER) != 80:
    _fail(f"protocol.genesis_header must be exactly 80 bytes (160 hex chars), got {len(GENESIS_HEADER)} bytes")

PROTOCOL_VERSION: int = _require("protocol", "version")
USER_AGENT: str = _require("protocol", "user_agent")
# PeerLink uses these when building the Bitcoin version payload during handshake.
# Keep USER_AGENT text here and convert to bytes at the wire layer.
PEERLINK_VERSION: int = PROTOCOL_VERSION
PEERLINK_USER_AGENT: str = USER_AGENT

# ============================================================================
# Blockchain
# ============================================================================
DOWNLOAD_BLOCKS_FROM_HEIGHT: int = _require("blockchain", "download_blocks_from_height")
FORK_HANDLER_MAX_DEPTH: int = _require("blockchain", "fork_handler_max_depth")
ENABLE_TX_VALIDATION: bool = _require("blockchain", "enable_tx_validation")

# Request standards for the block backfiller: never flood a serving peer.
# In-flight cap mirrors Bitcoin Core's 16-blocks-per-peer IBD limit; the
# per-minute budget caps sustained load (600 blocks/min of ~1.7 MB Runes-era
# blocks is roughly 17 MB/min = 2.3 Mbps per peer).
BACKFILL_INFLIGHT_PER_PEER: int = _require("blockchain", "backfill_inflight_per_peer")
BACKFILL_BLOCKS_PER_MIN_PER_PEER: int = _require("blockchain", "backfill_blocks_per_min_per_peer")
