#!/usr/bin/env python3
"""CLI tool to launch all domain generator scripts concurrently."""

from __future__ import annotations

import argparse
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Tuple
import sys

from colorama import Fore, Style, init

ROOT = Path(__file__).resolve().parent
GEN_DIR = ROOT / "ingestion" / "rabbitmq_producers"
COLORS = [Fore.RED, Fore.GREEN, Fore.YELLOW, Fore.BLUE, Fore.MAGENTA, Fore.CYAN]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start domain-specific data generator scripts in parallel"
    )
    parser.add_argument(
        "--mode",
        choices=["live", "burst", "replay"],
        default="live",
        help="Generation mode",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Seconds between events (live mode)",
    )
    parser.add_argument(
        "--burst-count",
        type=int,
        default=100,
        help="Events to emit per script (burst mode)",
    )
    parser.add_argument(
        "--replay-path",
        type=Path,
        help="Path to CSV file for replay mode",
    )
    parser.add_argument(
        "--domains",
        default="all",
        help="Comma-separated list of domains to run (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing",
    )
    return parser.parse_args()


def find_scripts(domains: List[str]) -> Dict[str, List[Path]]:
    scripts_map: Dict[str, List[Path]] = {}
    for domain in domains:
        domain_dir = GEN_DIR / domain
        if not domain_dir.exists():
            print(f"[!] Domain not found: {domain}")
            continue
        scripts = sorted(p for p in domain_dir.glob("*.py"))
        if not scripts:
            print(f"[!] No scripts found for domain: {domain}")
            continue
        scripts_map[domain] = scripts
    return scripts_map


def build_command(script: Path, args: argparse.Namespace) -> List[str]:
    cmd = [sys.executable, str(script), "--mode", args.mode]
    if args.mode == "live":
        cmd += ["--interval", str(args.interval)]
    elif args.mode == "burst":
        cmd += ["--burst-count", str(args.burst_count)]
    elif args.mode == "replay":
        if not args.replay_path:
            raise ValueError("--replay-path is required for replay mode")
        cmd += ["--replay-path", str(args.replay_path)]
    return cmd


def stream_output(proc: subprocess.Popen, prefix: str, color: str) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        print(f"{color}[{prefix}] {line.rstrip()}{Style.RESET_ALL}")


def main() -> None:
    init(autoreset=True)
    args = parse_args()

    if args.mode == "replay" and not args.replay_path:
        print("[!] --replay-path is required for replay mode")
        return
    if args.mode == "replay" and args.replay_path and not args.replay_path.exists():
        print(f"[!] Replay file not found: {args.replay_path}")

    if args.domains == "all":
        domains = sorted(p.name for p in GEN_DIR.iterdir() if p.is_dir())
    else:
        domains = sorted(d.strip() for d in args.domains.split(",") if d.strip())

    scripts_map = find_scripts(domains)
    if not scripts_map:
        print("[!] No generator scripts found to run.")
        return

    processes: List[Tuple[subprocess.Popen, Path]] = []
    threads: List[threading.Thread] = []
    start_time = time.time()

    for idx, (domain, scripts) in enumerate(scripts_map.items()):
        color = COLORS[idx % len(COLORS)]
        for script in scripts:
            cmd = build_command(script, args)
            if args.dry_run:
                print(f"{color}[DRY-RUN][{domain}] {' '.join(cmd)}{Style.RESET_ALL}")
                continue
            print(f"{color}[+] Launching: {script}{Style.RESET_ALL}")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            processes.append((proc, script))
            thread = threading.Thread(
                target=stream_output, args=(proc, domain, color), daemon=False
            )
            thread.start()
            threads.append(thread)

    if args.dry_run:
        return

    try:
        for proc, _ in processes:
            proc.wait()
    except KeyboardInterrupt:
        print("\n[!] KeyboardInterrupt received, terminating generators...")
        for proc, _ in processes:
            proc.terminate()
        for proc, _ in processes:
            proc.wait()
    finally:
        for thread in threads:
            thread.join()
        for proc, script in processes:
            if proc.returncode:
                print(f"[!] {script} exited with code {proc.returncode}")
        elapsed = time.time() - start_time
        print(f"[+] Total run time: {elapsed:.2f} seconds")


if __name__ == "__main__":  # pragma: no cover
    main()
