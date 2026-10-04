#!/usr/bin/env python3
"""Repair plex's nginx DNS startup dependency and enable failure retries.

Preview: python3 scripts/fix-nginx-dns-restart.py --dry-run
Apply:   sudo python3 scripts/fix-nginx-dns-restart.py
Uses only the system Python standard library; does not use the pipeline venv.
"""

from __future__ import annotations

import argparse
import difflib
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

CONFIG = Path("/var/www/webpagetest/www/nginx.conf")
DROPIN = Path("/etc/systemd/system/nginx.service.d/90-dns-recovery.conf")
BACKUP_ROOT = Path("/var/backups")
SERVICE_CONFIG = b"""# Managed by fix-nginx-dns-restart.py
[Unit]
Wants=systemd-resolved.service
After=systemd-resolved.service
# Keep retrying through a resolver/network outage.
StartLimitIntervalSec=0

[Service]
Restart=on-failure
RestartSec=10s
"""


def patched_config(original: bytes) -> bytes:
    """Change only the known events block, retaining the file's line endings."""
    newline = b"\r\n" if b"\r\n" in original else b"\n"
    pattern = rb"location \^~ /events \{[^{}]*\}"
    matches = list(re.finditer(pattern, original))
    if len(matches) != 1:
        raise ValueError("Expected exactly one simple /events location; refusing to guess.")
    match = matches[0]
    block = match.group()
    old = b"        proxy_pass https://product.webpagetest.org;"
    new = newline.join([
        b"        # Resolve on requests, so DNS outages cannot prevent nginx startup.",
        b"        resolver 127.0.0.53 valid=60s;",
        b"        resolver_timeout 5s;",
        b"        set $wpt_events_origin product.webpagetest.org;",
        b"        proxy_pass https://$wpt_events_origin;",
    ])
    if new in block and old not in block:
        return original
    if block.count(old) != 1 or re.search(rb"\bresolver(?:_timeout)?\s|\$wpt_events_origin", original):
        raise ValueError("Unexpected proxy/resolver configuration; refusing to overwrite it.")
    # No URI suffix: preserve the original path and query string. Existing Host
    # and TLS SNI defaults continue to use the evaluated proxy hostname.
    return original[:match.start()] + block.replace(old, new) + original[match.end():]


def atomic_write(path: Path, content: bytes) -> None:
    """Keep existing ownership/mode, and never expose a partially written config."""
    metadata = path.stat() if path.exists() else None
    fd, temporary = tempfile.mkstemp(prefix=".nginx-recovery-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        if metadata:
            os.chown(temporary, metadata.st_uid, metadata.st_gid)
            os.chmod(temporary, metadata.st_mode & 0o7777)
        else:
            os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def run(*args: str) -> None:
    print("+ " + " ".join(args), flush=True)
    subprocess.run(args, check=True, timeout=90)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="show changes without writing or restarting")
    args = parser.parse_args()
    if not args.dry_run and os.geteuid() != 0:
        parser.error("Run with sudo, or use --dry-run to preview.")
    config = CONFIG.resolve(strict=True)
    if DROPIN.is_symlink():
        raise ValueError(f"Refusing to replace symlink: {DROPIN}")
    original = config.read_bytes()
    candidate = patched_config(original)
    previous_dropin = DROPIN.read_bytes() if DROPIN.exists() else None
    if previous_dropin is not None and previous_dropin != SERVICE_CONFIG:
        raise ValueError(f"Existing {DROPIN} has different contents; refusing to overwrite it.")
    for path, before, after in [(config, original, candidate), (DROPIN, previous_dropin or b"", SERVICE_CONFIG)]:
        print("".join(difflib.unified_diff(
            before.decode().splitlines(keepends=True), after.decode().splitlines(keepends=True),
            fromfile=str(path), tofile=str(path) + " (proposed)",
        )), end="", flush=True)
    if args.dry_run:
        print("Dry run only: no files or services changed.")
        return 0

    run("systemctl", "is-active", "--quiet", "systemd-resolved.service")
    BACKUP_ROOT.mkdir(exist_ok=True)
    backup = Path(tempfile.mkdtemp(prefix="nginx-dns-recovery-", dir=BACKUP_ROOT))
    shutil.copy2(config, backup / "webpagetest-nginx.conf")
    if previous_dropin is not None:
        shutil.copy2(DROPIN, backup / "90-dns-recovery.conf")
    print(f"Original configuration backed up to {backup}", flush=True)
    DROPIN.parent.mkdir(parents=True, exist_ok=True)
    try:
        atomic_write(config, candidate)
        atomic_write(DROPIN, SERVICE_CONFIG)
        # Check every production vhost before touching the running service.
        run("/usr/sbin/nginx", "-t")
        run("systemctl", "daemon-reload")
    except (Exception, KeyboardInterrupt):
        print("Validation/install failed; restoring original configuration.", file=sys.stderr)
        atomic_write(config, original)
        if previous_dropin is None:
            DROPIN.unlink(missing_ok=True)
        else:
            atomic_write(DROPIN, previous_dropin)
        subprocess.run(["systemctl", "daemon-reload"], check=False, timeout=90)
        raise

    # Keep validated recovery settings if service activation itself fails, so
    # systemd can retry. Never stop a healthy nginx just to apply this repair.
    run("systemctl", "reset-failed", "nginx.service")
    active = subprocess.run(["systemctl", "is-active", "--quiet", "nginx.service"], timeout=30).returncode == 0
    run("systemctl", "reload" if active else "start", "nginx.service")
    run("systemctl", "is-active", "nginx.service")
    run("systemctl", "show", "nginx.service", "-p", "Restart", "-p", "RestartUSec", "-p", "StartLimitIntervalUSec")
    run("curl", "--fail", "--silent", "--show-error", "--output", "/dev/null",
        "--max-time", "20", "--noproxy", "*", "--resolve", "news-tldr.com:443:127.0.0.1",
        "https://news-tldr.com/")
    print(f"Repair complete; origin HTTPS check passed. Backup: {backup}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Exception, KeyboardInterrupt) as error:
        print(f"Repair did not complete: {error}", file=sys.stderr)
        print("Inspect: systemctl status nginx --no-pager; journalctl -u nginx -n 30 --no-pager", file=sys.stderr)
        sys.exit(1)
