from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import sqlite3
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_ORIGIN = "https://news-tldr.com"


@contextmanager
def _sync_server(tmp_path: Path, **settings: str) -> Iterator[tuple[httpx.Client, Path]]:
    database_path = tmp_path / "sync.sqlite"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    environment = os.environ.copy()
    environment.update(
        {
            "SYNC_DB_PATH": str(database_path),
            "SYNC_ALLOWED_ORIGINS": ALLOWED_ORIGIN,
            "SYNC_MAX_READS": "10",
            "SYNC_MAX_ACTIVE_GROUPS": "10",
            "SYNC_MAX_DAILY_GROUP_CREATIONS": "10",
            **settings,
        }
    )
    process = subprocess.Popen(
        ["php", "-S", f"127.0.0.1:{port}", "server/sync/router.php"],
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=2)
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                response = client.get(
                    "/api/sync/v1/groups",
                    headers={"Origin": ALLOWED_ORIGIN},
                )
                if response.status_code == 405:
                    break
            except httpx.TransportError:
                pass
            if process.poll() is not None or time.monotonic() >= deadline:
                stderr = process.stderr.read() if process.stderr else ""
                raise RuntimeError(f"PHP sync test server did not start: {stderr}")
            time.sleep(0.05)
        yield client, database_path
    finally:
        client.close()
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
        if process.stderr:
            process.stderr.close()


def _create_group(client: httpx.Client, reads: dict[str, int] | None = None) -> httpx.Response:
    return client.post(
        "/api/sync/v1/groups",
        headers={"Origin": ALLOWED_ORIGIN},
        json={"reads": reads or {}},
    )


def _merge_group(client: httpx.Client, token: str, reads: dict[str, int]) -> httpx.Response:
    return client.post(
        "/api/sync/v1/merge",
        headers={"Origin": ALLOWED_ORIGIN, "Authorization": f"Bearer {token}"},
        json={"reads": reads},
    )


def test_sync_api_creates_merges_prunes_and_deletes_group(tmp_path: Path) -> None:
    with _sync_server(tmp_path) as (client, database_path):
        now = int(time.time() * 1000)
        create = _create_group(
            client,
            {
                "2026-09-01-current": now - 1_000,
                "2026-08-20-expired": now - 4 * 24 * 60 * 60 * 1000,
            },
        )

        assert create.status_code == 201
        assert create.headers["cache-control"] == "private, no-store, max-age=0"
        assert create.headers["cross-origin-resource-policy"] == "same-origin"
        created = create.json()
        token = created["token"]
        assert re.fullmatch(r"[A-Za-z0-9_-]{43}", token)
        assert created["reads"] == {"2026-09-01-current": now - 1_000}
        assert token.encode() not in database_path.read_bytes()

        merge = _merge_group(
            client,
            token,
            {
                "2026-09-01-current": now - 2_000,
                "2026-09-01-second": now,
            },
        )
        assert merge.status_code == 200
        merged = merge.json()
        assert merged["revision"] == 2
        assert merged["reads"]["2026-09-01-current"] == now - 1_000
        assert now - 1_000 <= merged["reads"]["2026-09-01-second"] <= now

        pull = _merge_group(client, token, {})
        assert pull.status_code == 200
        assert pull.json()["reads"] == merged["reads"]
        assert pull.json()["revision"] == 2

        delete = client.delete(
            "/api/sync/v1/group",
            headers={"Origin": ALLOWED_ORIGIN, "Authorization": f"Bearer {token}"},
        )
        assert delete.status_code == 204
        assert _merge_group(client, token, {}).status_code == 404


def test_sync_api_rejects_cross_origin_invalid_and_oversized_state(tmp_path: Path) -> None:
    with _sync_server(tmp_path, SYNC_MAX_READS="2") as (client, _):
        cross_origin = client.post(
            "/api/sync/v1/groups",
            headers={"Origin": "https://attacker.example"},
            json={"reads": {}},
        )
        assert cross_origin.status_code == 403
        assert cross_origin.json()["error"] == "origin_not_allowed"

        wrong_type = client.post(
            "/api/sync/v1/groups",
            headers={"Origin": ALLOWED_ORIGIN},
            content="reads=none",
        )
        assert wrong_type.status_code == 415

        invalid_id = _create_group(client, {"../escape": int(time.time() * 1000)})
        assert invalid_id.status_code == 400
        assert invalid_id.json()["error"] == "invalid_story_id"

        too_many = _create_group(
            client,
            {f"story-{index}": int(time.time() * 1000) for index in range(3)},
        )
        assert too_many.status_code == 413
        assert too_many.json()["error"] == "too_many_reads"

        invalid_order = client.post(
            "/api/sync/v1/groups",
            headers={"Origin": ALLOWED_ORIGIN},
            json={
                "reads": {"story-valid": int(time.time() * 1000)},
                "read_orders": {"story-valid": "not-an-order"},
            },
        )
        assert invalid_order.status_code == 400
        assert invalid_order.json()["error"] == "invalid_story_order"

        invalid_revision = client.post(
            "/api/sync/v1/merge",
            headers={"Origin": ALLOWED_ORIGIN, "Authorization": "Bearer " + "a" * 43},
            json={"reads": {}, "known_revision": 0},
        )
        assert invalid_revision.status_code == 400
        assert invalid_revision.json()["error"] == "invalid_revision"


def test_sync_api_compacts_read_prefix_and_skips_unchanged_state_download(tmp_path: Path) -> None:
    with _sync_server(tmp_path) as (client, database_path):
        now = int(time.time() * 1000)
        story_a = "2026-09-03-a"
        story_b = "2026-09-03-b"
        story_c = "2026-09-03-c"
        order_b = f"{now - 2_000:013d}:{story_b}"
        create = client.post(
            "/api/sync/v1/groups",
            headers={"Origin": ALLOWED_ORIGIN},
            json={
                "state_version": 2,
                "reads": {},
                "ordered_reads": [
                    [story_a, now, now - 3_000],
                    [story_b, now, now - 2_000],
                    [story_c, now, now - 1_000],
                ],
                "read_before": order_b,
            },
        )
        assert create.status_code == 201
        created = create.json()
        assert created["state_version"] == 2
        assert created["read_before"] == order_b
        accepted_now = created["reads"][story_c]
        assert now - 1_000 <= accepted_now <= now

        unchanged = client.post(
            "/api/sync/v1/merge",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Authorization": f"Bearer {created['token']}",
            },
            json={
                "state_version": 2,
                "reads": {},
                "ordered_reads": [[story_c, accepted_now, now - 1_000]],
                "read_before": order_b,
                "known_revision": 1,
            },
        )
        assert unchanged.status_code == 200
        assert unchanged.json()["unchanged"] is True
        assert unchanged.json()["revision"] == 1
        assert "reads" not in unchanged.json()

        story_d = "2026-09-03-d"
        changed = client.post(
            "/api/sync/v1/merge",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Authorization": f"Bearer {created['token']}",
            },
            json={
                "state_version": 2,
                "reads": {},
                "ordered_reads": [
                    [story_c, accepted_now, now - 1_000],
                    [story_d, accepted_now, now],
                ],
                "read_before": order_b,
                "known_revision": 1,
            },
        )
        assert changed.status_code == 200
        assert changed.json()["unchanged"] is False
        assert changed.json()["revision"] == 2
        assert "reads" not in changed.json()

        stale = client.post(
            "/api/sync/v1/merge",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Authorization": f"Bearer {created['token']}",
            },
            json={"reads": {}, "known_revision": 1},
        )
        assert stale.status_code == 200
        assert stale.json()["revision"] == 2
        assert stale.json()["read_before"] == order_b
        assert stale.json()["reads"] == {story_c: accepted_now, story_d: accepted_now}

    with sqlite3.connect(database_path) as database:
        stored = json.loads(database.execute("SELECT reads_json FROM sync_groups").fetchone()[0])
    assert stored["read_before"] == order_b
    assert stored["reads"] == {}
    assert {row[0] for row in stored["ordered_reads"]} == {story_c, story_d}


def test_sync_api_enforces_daily_creation_and_total_group_caps(tmp_path: Path) -> None:
    daily_dir = tmp_path / "daily"
    daily_dir.mkdir()
    with _sync_server(daily_dir, SYNC_MAX_DAILY_GROUP_CREATIONS="2") as (client, _):
        assert _create_group(client).status_code == 201
        assert _create_group(client).status_code == 201
        limited = _create_group(client)
        assert limited.status_code == 429
        assert limited.json()["error"] == "daily_group_limit_reached"

    capacity_dir = tmp_path / "capacity"
    capacity_dir.mkdir()
    with _sync_server(capacity_dir, SYNC_MAX_ACTIVE_GROUPS="1") as (client, _):
        assert _create_group(client).status_code == 201
        limited = _create_group(client)
        assert limited.status_code == 503
        assert limited.json()["error"] == "group_capacity_reached"


def test_sync_cleanup_removes_expired_groups_reads_and_counters(tmp_path: Path) -> None:
    with _sync_server(tmp_path) as (client, database_path):
        now = int(time.time() * 1000)
        first = _create_group(client, {"story-old": now})
        second = _create_group(client, {"story-current": now})
        assert first.status_code == second.status_code == 201

    with sqlite3.connect(database_path) as database:
        first_hash = hashlib.sha256(first.json()["token"].encode()).hexdigest()
        second_hash = hashlib.sha256(second.json()["token"].encode()).hexdigest()
        database.execute("UPDATE sync_groups SET expires_at = 0 WHERE token_hash = ?", (first_hash,))
        old_timestamp = int(time.time() * 1000) - 4 * 24 * 60 * 60 * 1000
        database.execute(
            "UPDATE sync_groups SET reads_json = ? WHERE token_hash = ?",
            (
                json.dumps({"story-expired": old_timestamp, "story-current": int(time.time() * 1000)}),
                second_hash,
            ),
        )
        database.execute(
            "INSERT INTO sync_daily_counters(day, groups_created, updated_at) VALUES ('2020-01-01', 1, 0)"
        )

    environment = os.environ.copy()
    environment["SYNC_DB_PATH"] = str(database_path)
    result = subprocess.run(
        ["php", "server/sync/cleanup.php", "--verbose"],
        cwd=PROJECT_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    stats = json.loads(result.stdout)
    assert stats == {
        "groups_deleted": 1,
        "groups_pruned": 1,
        "reads_deleted": 1,
        "counters_deleted": 1,
    }
    assert "Pruning expired sync groups" in result.stderr

    with sqlite3.connect(database_path) as database:
        rows = database.execute("SELECT token_hash, reads_json FROM sync_groups").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == second_hash
        stored = json.loads(rows[0][1])
        assert stored["state_version"] == 2
        assert set(stored["reads"]) == {"story-current"}


def test_sync_deployment_configuration_contains_origin_resources() -> None:
    nginx = (PROJECT_ROOT / "deploy/nginx/news-tldr.com").read_text(encoding="utf-8")
    pool = (PROJECT_ROOT / "deploy/php-fpm/news-tldr-sync.conf").read_text(encoding="utf-8")
    cron = (PROJECT_ROOT / "deploy/cron/news-tldr-sync").read_text(encoding="utf-8")
    installer = (PROJECT_ROOT / "scripts/install-sync-origin.sh").read_text(encoding="utf-8")

    assert "limit_req_zone $http_cf_connecting_ip zone=news_tldr_sync_client:10m rate=30r/m" in nginx
    assert "limit_req_zone $binary_remote_addr zone=news_tldr_sync_peer:10m rate=120r/m" in nginx
    assert "client_max_body_size 256k" in nginx
    assert "limit_conn news_tldr_sync_connections 4" in nginx
    assert "fastcgi_pass unix:/run/php/news-tldr-sync.sock" in nginx
    assert "fastcgi_param HTTP_ORIGIN $http_origin" in nginx
    assert "location /api/sync/" in nginx
    assert "pm.max_children = 3" in pool
    assert "memory_limit] = 32M" in pool
    assert "open_basedir] = /opt/news-tldr-sync:/var/lib/news-tldr-sync:/tmp" in pool
    assert "www-data" in cron
    assert "/opt/news-tldr-sync/cleanup.php" in cron
    assert 'nginx_identity="$(awk' in installer
    assert 'chown "$nginx_user:$nginx_group" "$socket_path"' in installer
    assert '[[ "$api_status" == "415" ]] && break' in installer
