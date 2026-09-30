import sys
import os
import re
import time
from datetime import datetime


class ConsoleFilter:
    """Console line filter.

    Collapses the verbose per-peer connection chatter down to just two lines —
    a connect and a disconnect — while the full text is still written to the log
    file (the TeeLogger logs unfiltered). The REAL peer_id is preserved in both
    lines (no renumbering), so the console index always matches the log file.
    """

    # Capture the address for a peer_id from the dial/accept lines (then suppress them)
    _RE_CREATE    = re.compile(r'Creating PeerLink to (\S+) \(peer_id=(\d+)\)')
    _RE_INBOUND   = re.compile(r'Accepting inbound connection from (\S+) \(peer_id=(\d+)\)')
    # Handshake completed (outbound or inbound) -> surface as a single connect line
    _RE_HANDSHAKE = re.compile(r'\[PL(\d+)\].*handshake completed successfully')
    # NM's main disconnect line (the 🔌 distinguishes it from the ℹ️ cooldown follow-ups)
    _RE_DISCONN   = re.compile(r'🔌 Peer (\d+).*disconnected \[([^\]]+)\]')

    # NM connection-mechanic lines suppressed from console (still in the log file).
    _NM_CONN_NOISE = (
        'Connecting to', 'Creating PeerLink', 'Accepting inbound', 'Accepted inbound',
        'Waiting for handshake', '⏳ Waiting', 'Connected to', 'Startup connections',
        'unreachable [', 'removed from pool', 're-dialable', 'cooldown',
        'Already connected', 'Max peers', 'rejecting inbound', 'No addresses in pool',
    )

    def __init__(self, suppress_keywords=None):
        self.suppress_keywords = suppress_keywords or []
        self.debug = False  # Set to True to see what's being filtered

        # Per-peer connection state, keyed by the REAL peer_id (never renumbered).
        self._peer_addr = {}   # peer_id -> "ip:port"
        self._peer_since = {}  # peer_id -> handshake-complete wall time
        self._handshaked = set()

    @staticmethod
    def _fmt_duration(secs: float) -> str:
        secs = int(secs)
        if secs < 120:
            return f"{secs}s"
        m, s = divmod(secs, 60)
        if m < 120:
            return f"{m}m {s}s"
        h, m = divmod(m, 60)
        return f"{h}h {m}m"

    def process_line(self, line: str) -> str | None:
        """Return the line to print on console, a rewritten line, or None to drop it."""
        # 1) Capture peer address from the dial/accept line, then suppress it.
        m = self._RE_CREATE.search(line) or self._RE_INBOUND.search(line)
        if m:
            self._peer_addr[int(m.group(2))] = m.group(1)
            return None

        # 2) Handshake complete -> one clean "connected" line.
        m = self._RE_HANDSHAKE.search(line)
        if m:
            pid = int(m.group(1))
            self._peer_since[pid] = time.time()
            self._handshaked.add(pid)
            return f"🟢 PL[{pid}] connected peer {self._peer_addr.get(pid, '?')}"

        # 3) Disconnect -> one clean line, but only for peers that actually connected.
        #    A failed dial (never handshaked) stays silent on the console.
        m = self._RE_DISCONN.search(line)
        if m:
            pid, reason = int(m.group(1)), m.group(2)
            addr = self._peer_addr.pop(pid, '?')
            since = self._peer_since.pop(pid, None)
            was_up = pid in self._handshaked
            self._handshaked.discard(pid)
            if was_up:
                dur = self._fmt_duration(time.time() - since) if since else '?'
                return f"🔴 PL[{pid}] disconnected after {dur}. Reason: {reason}  ({addr})"
            return None

        # 4) Drop all remaining per-peer wire chatter ([PLn] ...) and NM dial mechanics.
        if line.startswith('[PL'):
            return None
        for s in self._NM_CONN_NOISE:
            if s in line:
                return None

        # 5) User-specified keyword suppression (e.g. ping/pong).
        return self._filter_keywords(line)

    def _filter_keywords(self, line: str) -> str | None:
        line_lower = line.lower()
        for keyword in self.suppress_keywords:
            if keyword.lower() in line_lower:
                if self.debug:
                    print(f"[FILTER DEBUG] Suppressed (keyword='{keyword}'): {line[:60]}...", file=sys.__stdout__)
                return None
        return line


class TeeLogger:
    """
    A file-like object that writes to both stdout/stderr and a file.
    Used to capture all console output to a log file.
    """
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.filename = filename
        # Create log directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Rotate existing log file to .old
        if os.path.exists(filename):
            old_filename = filename + ".old"
            try:
                # Remove old backup if it exists
                if os.path.exists(old_filename):
                    os.remove(old_filename)
                # Rename current log to .old
                os.rename(filename, old_filename)
            except Exception as e:
                # If rotation fails, just continue (will overwrite)
                print(f"[LOG] Warning: Could not rotate log file: {e}", file=sys.__stdout__)
        
        # Open file in write mode (overwrite)
        self.log_file = open(filename, "w", encoding="utf-8")
        self.filter = None  # Optional console filter
        self.buffer = ""    # Line buffer for filtering

    def write(self, message):
        if self.filter:
            # Filtering enabled - buffer and process lines
            self.buffer += message
            while '\n' in self.buffer:
                line, self.buffer = self.buffer.split('\n', 1)
                result = self.filter.process_line(line)
                if result is not None:
                    # Print to console (filtered)
                    self.terminal.write(result + '\n')
                # Always write to log file (unfiltered)
                self.log_file.write(line + '\n')
                self.log_file.flush()
        else:
            # No filter - pass through
            self.terminal.write(message)
            self.log_file.write(message)
            self.log_file.flush()  # Ensure immediate write

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

def setup_global_logging(log_file_path="logs/pynode.log"):
    """
    Redirects sys.stdout and sys.stderr to write to both console and file.
    """
    # Create logs directory
    if os.path.dirname(log_file_path):
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        
    # Create the tee logger
    tee = TeeLogger(log_file_path)
    
    # Redirect stdout and stderr
    sys.stdout = tee
    sys.stderr = tee
    
    print(f"[LOG] Logging initialized. Writing to {log_file_path}")
    print(f"[LOG] Started at {datetime.now()}")


def enable_console_filter(suppress_keywords=None, debug=False):
    """
    Enable console filtering on the current TeeLogger.
    Lines containing any of the keywords will be suppressed from console (but still logged to file).
    
    Args:
        suppress_keywords: List of strings to suppress (e.g. ['ping', 'pong'])
        debug: Enable debug output to see filter decisions
    """
    if isinstance(sys.stdout, TeeLogger):
        sys.stdout.filter = ConsoleFilter(suppress_keywords=suppress_keywords or [])
        sys.stdout.filter.debug = debug
        keywords_str = ', '.join(f"'{k}'" for k in (suppress_keywords or []))
        print(f"[LOG] Console filtering enabled (suppress: {keywords_str}, debug={debug})")
    else:
        print("[LOG] Warning: stdout is not a TeeLogger, filtering not enabled")


def disable_console_filter():
    """Disable console filtering on the current TeeLogger."""
    if isinstance(sys.stdout, TeeLogger):
        sys.stdout.filter = None
        print("[LOG] Console filtering disabled")
    else:
        print("[LOG] Warning: stdout is not a TeeLogger")
