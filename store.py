"""SQLite 持久层：名单与检测状态的唯一事实源。"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from checker import ProxyState, utcnow_iso

log = logging.getLogger("proxy-dashboard.store")

_COLS = (
    "url", "host", "port", "username", "password", "position",
    "status", "latency_ms", "exit_ip", "country", "last_checked_at",
    "total_checks", "ok_checks", "consecutive_failures",
    "first_seen_at", "updated_at",
)


def _row_to_state(row: tuple) -> ProxyState:
    d = dict(zip(_COLS, row))
    return ProxyState(
        id=0,  # id 由 load_all 按顺序分配
        url=d["url"],
        host=d["host"], port=d["port"],
        username=d["username"], password=d["password"],
        status=d["status"], latency_ms=d["latency_ms"],
        exit_ip=d["exit_ip"], country=d["country"],
        last_checked_at=d["last_checked_at"],
        total_checks=d["total_checks"], ok_checks=d["ok_checks"],
        consecutive_failures=d["consecutive_failures"],
    )


class ProxyStore:
    """所有调用经 asyncio.to_thread 进入；内部锁保证单写者。"""

    def __init__(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # isolation_level=None：显式管理事务
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS proxies (
                  url TEXT PRIMARY KEY,
                  host TEXT NOT NULL, port INTEGER NOT NULL,
                  username TEXT NOT NULL, password TEXT NOT NULL,
                  position INTEGER NOT NULL,
                  status TEXT NOT NULL DEFAULT 'pending',
                  latency_ms REAL, exit_ip TEXT, country TEXT, last_checked_at TEXT,
                  total_checks INTEGER NOT NULL DEFAULT 0,
                  ok_checks INTEGER NOT NULL DEFAULT 0,
                  consecutive_failures INTEGER NOT NULL DEFAULT 0,
                  first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL
                )
                """
            )

    def load_all(self) -> list[ProxyState]:
        """按 position 顺序载入全部代理（名单与顺序原样恢复）。"""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {', '.join(_COLS)} FROM proxies ORDER BY position"
            ).fetchall()
        states = [_row_to_state(r) for r in rows]
        for i, p in enumerate(states, 1):
            p.id = i
        return states

    def count(self) -> int:
        with self._lock:
            return self._conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]

    def apply_upload(self, parsed: list[ProxyState]) -> tuple[list[ProxyState], int, int]:
        """事务内替换名单：已有 URL 保留状态与计数，新 URL 插入，缺席删除。

        返回 (最终列表, kept, fresh)。
        """
        now = utcnow_iso()
        with self._lock:
            cur = self._conn.cursor()
            existing = {
                r[0] for r in cur.execute("SELECT url FROM proxies").fetchall()
            }
            kept = sum(1 for p in parsed if p.url in existing)
            fresh = len(parsed) - kept
            cur.execute("BEGIN")
            try:
                new_urls = {p.url for p in parsed}
                for url in existing:
                    if url not in new_urls:
                        cur.execute("DELETE FROM proxies WHERE url = ?", (url,))
                for pos, p in enumerate(parsed, 1):
                    if p.url in existing:
                        # 保留状态字段，仅刷新顺序与身份字段
                        cur.execute(
                            "UPDATE proxies SET position=?, host=?, port=?, "
                            "username=?, password=?, updated_at=? WHERE url=?",
                            (pos, p.host, p.port, p.username, p.password, now, p.url),
                        )
                    else:
                        cur.execute(
                            f"INSERT INTO proxies ({', '.join(_COLS)}) "
                            f"VALUES ({','.join('?' * len(_COLS))})",
                            (p.url, p.host, p.port, p.username, p.password, pos,
                             p.status, p.latency_ms, p.exit_ip, p.country,
                             p.last_checked_at, p.total_checks, p.ok_checks,
                             p.consecutive_failures, now, now),
                        )
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        return self.load_all(), kept, fresh

    def save_updates(self, states: list[ProxyState]) -> None:
        """批量写回检测状态字段。"""
        if not states:
            return
        now = utcnow_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                for p in states:
                    cur.execute(
                        "UPDATE proxies SET status=?, latency_ms=?, exit_ip=?, "
                        "country=?, last_checked_at=?, total_checks=?, ok_checks=?, "
                        "consecutive_failures=?, updated_at=? WHERE url=?",
                        (p.status, p.latency_ms, p.exit_ip, p.country,
                         p.last_checked_at, p.total_checks, p.ok_checks,
                         p.consecutive_failures, now, p.url),
                    )
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()
