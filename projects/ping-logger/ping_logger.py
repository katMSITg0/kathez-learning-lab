#!/usr/bin/env python3
"""
Ping a list of hosts and append results to a CSV file.

Files:
- servers.txt (one host per line; blanks and #comments allowed)
- logs/ping_log.csv (created/appended)

Cross-platform:
- Windows uses: ping -n <count> -w <timeout_ms>
- Linux/macOS uses: ping -c <count> (with a subprocess timeout as a safety net)

Note: Ping output varies by OS/version; this parser aims to capture the common summary stats.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import platform
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


IS_WINDOWS = platform.system().lower().startswith("win")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def resolve_ip(host: str) -> str:
    """Best-effort DNS resolution; returns '' if it fails."""
    try:
        return socket.gethostbyname(host)
    except Exception:
        return ""


def load_hosts(path: Path) -> List[str]:
    hosts: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        hosts.append(line)
    return hosts


def build_ping_command(host: str, count: int, timeout_ms: int) -> List[str]:
    """
    Build OS-appropriate ping command.

    Windows: ping -n <count> -w <timeout_ms> <host>
    Linux/macOS: ping -c <count> <host>

    For Linux/macOS, we rely on subprocess timeout to avoid hanging.
    """
    if IS_WINDOWS:
        return ["ping", "-n", str(count), "-w", str(timeout_ms), host]
    return ["ping", "-c", str(count), host]


def run_ping(host: str, count: int, timeout_ms: int) -> Tuple[int, str, str]:
    """
    Execute ping and return (returncode, stdout, stderr).
    Uses a subprocess timeout as a guardrail.
    """
    cmd = build_ping_command(host, count=count, timeout_ms=timeout_ms)

    # Guard timeout: count * (timeout + a little overhead) + buffer
    timeout_s = max(3, int(count * (timeout_ms / 1000.0 + 0.5) + 2))

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as e:
        return 124, e.stdout or "", f"TimeoutExpired after {timeout_s}s"
    except FileNotFoundError:
        return 127, "", "ping command not found"
    except Exception as e:
        return 1, "", f"Exception: {e}"


def parse_ping_summary(stdout: str) -> Dict[str, Any]:
    """
    Extract common summary stats:
    - packets_sent, packets_received, packet_loss_pct, min_ms, avg_ms, max_ms

    The patterns mirror typical Windows and Linux/macOS summary formats.
    """
    data: Dict[str, Any] = {
        "packets_sent": "",
        "packets_received": "",
        "packet_loss_pct": "",
        "min_ms": "",
        "avg_ms": "",
        "max_ms": "",
    }
    text = stdout or ""

    if IS_WINDOWS:
        # Example: "Packets: Sent = 4, Received = 4, Lost = 0 (0% loss),"
        m = re.search(
            r"Sent\s*=\s*(\d+),\s*Received\s*=\s*(\d+),\s*Lost\s*=\s*(\d+)\s*\((\d+)%\s*loss\)",
            text,
            re.IGNORECASE,
        )
        if m:
            data["packets_sent"] = m.group(1)
            data["packets_received"] = m.group(2)
            data["packet_loss_pct"] = m.group(4)

        # Example: "Minimum = 2ms, Maximum = 2ms, Average = 2ms"
        m2 = re.search(
            r"Minimum\s*=\s*(\d+)ms,\s*Maximum\s*=\s*(\d+)ms,\s*Average\s*=\s*(\d+)ms",
            text,
            re.IGNORECASE,
        )
        if m2:
            data["min_ms"] = m2.group(1)
            data["max_ms"] = m2.group(2)
            data["avg_ms"] = m2.group(3)

    else:
        # Example: "4 packets transmitted, 4 received, 0% packet loss"
        m = re.search(
            r"(\d+)\s+packets\s+transmitted,\s+(\d+)\s+(?:packets\s+)?received.*?(\d+(?:\.\d+)?)%\s+packet\s+loss",
            text,
            re.IGNORECASE,
        )
        if m:
            data["packets_sent"] = m.group(1)
            data["packets_received"] = m.group(2)
            data["packet_loss_pct"] = m.group(3)

        # Example: "rtt min/avg/max/mdev = 9.220/9.813/10.234/0.414 ms"
        m2 = re.search(
            r"=\s*([\d\.]+)/([\d\.]+)/([\d\.]+)/([\d\.]+)\s*ms",
            text,
            re.IGNORECASE,
        )
        if m2:
            data["min_ms"] = m2.group(1)
            data["avg_ms"] = m2.group(2)
            data["max_ms"] = m2.group(3)

    return data


def ensure_csv_header(csv_path: Path, fieldnames: List[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def append_csv_row(csv_path: Path, fieldnames: List[str], row: Dict[str, Any]) -> None:
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ping hosts and log results to CSV.")
    parser.add_argument("--servers", default="servers.txt", help="Path to servers list file (default: servers.txt)")
    parser.add_argument("--out", default="logs/ping_log.csv", help="CSV output path (default: logs/ping_log.csv)")
    parser.add_argument("--count", type=int, default=4, help="Ping count per host (default: 4)")
    parser.add_argument("--timeout-ms", type=int, default=1000, help="Timeout per ping reply in ms (default: 1000)")
    parser.add_argument("--loop", type=int, default=0, help="Repeat every N seconds (0 = run once)")
    args = parser.parse_args()

    servers_path = Path(args.servers)
    out_path = Path(args.out)

    if not servers_path.exists():
        print(f"ERROR: servers file not found: {servers_path}", file=sys.stderr)
        return 2

    hosts = load_hosts(servers_path)
    if not hosts:
        print(f"ERROR: no hosts found in {servers_path}", file=sys.stderr)
        return 2

    fieldnames = [
        "timestamp_utc",
        "host",
        "resolved_ip",
        "status",  # UP / DOWN
        "packets_sent",
        "packets_received",
        "packet_loss_pct",
        "min_ms",
        "avg_ms",
        "max_ms",
        "returncode",
        "error",
    ]
    ensure_csv_header(out_path, fieldnames)

    def run_once() -> None:
        for host in hosts:
            ts = utc_now_iso()
            ip = resolve_ip(host)

            rc, stdout, stderr = run_ping(host, count=args.count, timeout_ms=args.timeout_ms)
            summary = parse_ping_summary(stdout)

            status = "UP" if rc == 0 else "DOWN"

            row = {
                "timestamp_utc": ts,
                "host": host,
                "resolved_ip": ip,
                "status": status,
                "packets_sent": summary["packets_sent"],
                "packets_received": summary["packets_received"],
                "packet_loss_pct": summary["packet_loss_pct"],
                "min_ms": summary["min_ms"],
                "avg_ms": summary["avg_ms"],
                "max_ms": summary["max_ms"],
                "returncode": rc,
                "error": (stderr or "").strip(),
            }
            append_csv_row(out_path, fieldnames, row)

            print(
                f"{ts}  {host:<30} {status:<4} "
                f"avg_ms={row['avg_ms'] or 'NA'} loss%={row['packet_loss_pct'] or 'NA'}"
            )

    if args.loop and args.loop > 0:
        print(f"Looping every {args.loop}s. Writing to: {out_path.resolve()}")
        while True:
            run_once()
            time.sleep(args.loop)
    else:
        run_once()
        print(f"Done. Results appended to: {out_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
