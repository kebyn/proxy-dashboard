"""SQLite 持久层：多名单（lists）与检测状态的唯一事实源。"""
from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from checker import ProxyState, utcnow_iso

log = logging.getLogger("proxy-dashboard.store")

_COLS = (
    "list_id", "url", "host", "port", "username", "password", "position",
    "status", "latency_ms", "country", "asn", "last_checked_at",
    "total_checks", "ok_checks", "consecutive_failures",
    "first_seen_at", "updated_at",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS lists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proxies (
  list_id INTEGER NOT NULL REFERENCES lists(id) ON DELETE CASCADE,
  url TEXT NOT NULL,
  host TEXT NOT NULL, port INTEGER NOT NULL,
  username TEXT NOT NULL, password TEXT NOT NULL,
  position INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  latency_ms REAL, country TEXT, asn TEXT, last_checked_at TEXT,
  total_checks INTEGER NOT NULL DEFAULT 0,
  ok_checks INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY (list_id, url)
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def _row_to_state(row: tuple) -> ProxyState:
    d = dict(zip(_COLS, row))
    return ProxyState(
        id=0,  # id 由 load_all 按顺序分配
        list_id=d["list_id"],
        url=d["url"],
        host=d["host"], port=d["port"],
        username=d["username"], password=d["password"],
        status=d["status"], latency_ms=d["latency_ms"],
        country=d["country"], asn=d["asn"],
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
            self._migrate()

    # ---- schema -----------------------------------------------------------

    def _migrate(self) -> None:
        """建 v3 schema；从 v2（单名单、url 主键、含 exit_ip）无损迁移。"""
        tables = {
            r[0] for r in
            self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "proxies" in tables:
            cols = {r[1] for r in self._conn.execute("PRAGMA table_info(proxies)")}
            if "list_id" in cols:
                self._create_schema()  # 已是 v3（幂等）
                self._conn.execute("PRAGMA foreign_keys=ON")
                return
            # v2 → v3：旧表先改名，再建新表，数据并入「默认名单」（exit_ip 弃置）
            log.info("检测到 v2 库，迁移为多名单 schema")
            old_cols = ("url", "host", "port", "username", "password", "position",
                        "status", "latency_ms", "country", "last_checked_at",
                        "total_checks", "ok_checks", "consecutive_failures",
                        "first_seen_at", "updated_at")
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                cur.execute("ALTER TABLE proxies RENAME TO _v2_proxies")
                for stmt in _SCHEMA.split(";"):
                    if stmt.strip():
                        cur.execute(stmt)
                now = utcnow_iso()
                cur.execute("INSERT INTO lists (name, created_at) VALUES ('默认名单', ?)", (now,))
                list_id = cur.execute(
                    "SELECT id FROM lists WHERE name='默认名单'").fetchone()[0]
                cur.execute(
                    f"INSERT INTO proxies (list_id, {', '.join(old_cols)}) "
                    f"SELECT ?, {', '.join(old_cols)} FROM _v2_proxies",
                    (list_id,),
                )
                cur.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('active_list_id', ?)",
                    (str(list_id),),
                )
                cur.execute("DROP TABLE _v2_proxies")
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            migrated = self._conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
            log.info("迁移完成：旧名单已成为「默认名单」（%d 个代理）", migrated)
        else:
            self._create_schema()
        self._conn.execute("PRAGMA foreign_keys=ON")

    def _create_schema(self) -> None:
        for stmt in _SCHEMA.split(";"):
            if stmt.strip():
                self._conn.execute(stmt)

    # ---- 名单 -------------------------------------------------------------

    def lists(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT l.id, l.name, l.created_at, COUNT(p.url) "
                "FROM lists l LEFT JOIN proxies p ON p.list_id = l.id "
                "GROUP BY l.id ORDER BY l.id"
            ).fetchall()
        return [{"id": r[0], "name": r[1], "created_at": r[2], "count": r[3]} for r in rows]

    def get_active_id(self) -> int | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='active_list_id'"
            ).fetchone()
        return int(row[0]) if row else None

    def set_active(self, list_id: int) -> None:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('active_list_id', ?)",
                    (str(list_id),),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def delete_list(self, list_id: int) -> int | None:
        """删除名单（CASCADE 连带代理）。仅在删的是活跃名单时切换活跃指针。

        名单不存在时抛 LookupError；返回新的活跃名单 id（可能 None）。
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                row = cur.execute(
                    "SELECT value FROM meta WHERE key='active_list_id'"
                ).fetchone()
                current_active = int(row[0]) if row else None
                cur.execute("DELETE FROM lists WHERE id=?", (list_id,))
                if cur.rowcount == 0:
                    raise LookupError(f"名单 {list_id} 不存在")
                new_active = current_active
                if current_active == list_id:
                    # 删的是活跃名单：切到最近剩下的一个（无则清空）
                    last = cur.execute(
                        "SELECT id FROM lists ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                    new_active = last[0] if last else None
                if new_active is not None:
                    cur.execute(
                        "INSERT OR REPLACE INTO meta (key, value) VALUES ('active_list_id', ?)",
                        (str(new_active),),
                    )
                else:
                    cur.execute("DELETE FROM meta WHERE key='active_list_id'")
                cur.execute("COMMIT")
            except Exception:
                try:
                    cur.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass  # 事务已不在（如重复回滚）
                raise
            return new_active

    # ---- 代理 -------------------------------------------------------------

    def load_all(self, list_id: int) -> list[ProxyState]:
        """按 position 顺序载入指定名单的全部代理。"""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {', '.join(_COLS)} FROM proxies WHERE list_id=? ORDER BY position",
                (list_id,),
            ).fetchall()
        states = [_row_to_state(r) for r in rows]
        for i, p in enumerate(states, 1):
            p.id = i
        return states

    def apply_upload(self, name: str, parsed: list[ProxyState]) -> tuple[int, list[ProxyState], int, int]:
        """按名称 upsert 名单：新名称→新建；同名→替换成员，匹配 URL 保留统计。

        parsed 携带最新的 country/asn（geo 已填充），kept 行也刷新归属。
        返回 (list_id, 最终列表, kept, fresh)。
        """
        now = utcnow_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN")
            try:
                row = cur.execute("SELECT id FROM lists WHERE name=?", (name,)).fetchone()
                if row:
                    list_id = row[0]
                    existing = {
                        r[0] for r in cur.execute(
                            "SELECT url FROM proxies WHERE list_id=?", (list_id,)
                        ).fetchall()
                    }
                else:
                    cur.execute("INSERT INTO lists (name, created_at) VALUES (?, ?)", (name, now))
                    list_id = cur.lastrowid
                    existing = set()
                kept = sum(1 for p in parsed if p.url in existing)
                fresh = len(parsed) - kept

                new_urls = {p.url for p in parsed}
                for url in existing:
                    if url not in new_urls:
                        cur.execute("DELETE FROM proxies WHERE list_id=? AND url=?",
                                    (list_id, url))
                for pos, p in enumerate(parsed, 1):
                    if p.url in existing:
                        # 保留检测状态与计数，刷新顺序/身份/归属
                        cur.execute(
                            "UPDATE proxies SET position=?, host=?, port=?, username=?, "
                            "password=?, country=?, asn=?, updated_at=? "
                            "WHERE list_id=? AND url=?",
                            (pos, p.host, p.port, p.username, p.password,
                             p.country, p.asn, now, list_id, p.url),
                        )
                    else:
                        cur.execute(
                            f"INSERT INTO proxies ({', '.join(_COLS)}) "
                            f"VALUES ({','.join('?' * len(_COLS))})",
                            (list_id, p.url, p.host, p.port, p.username, p.password, pos,
                             p.status, p.latency_ms, p.country, p.asn, p.last_checked_at,
                             p.total_checks, p.ok_checks, p.consecutive_failures, now, now),
                        )
                cur.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('active_list_id', ?)",
                    (str(list_id),),
                )
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        return list_id, self.load_all(list_id), kept, fresh

    def save_updates(self, list_id: int, states: list[ProxyState]) -> None:
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
                        "UPDATE proxies SET status=?, latency_ms=?, last_checked_at=?, "
                        "total_checks=?, ok_checks=?, consecutive_failures=?, updated_at=? "
                        "WHERE list_id=? AND url=?",
                        (p.status, p.latency_ms, p.last_checked_at,
                         p.total_checks, p.ok_checks, p.consecutive_failures,
                         now, list_id, p.url),
                    )
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def refresh_geo(self, lookup_fn) -> int:
        """启动时全表刷新归属（mmdb 每次启动重新下载）。返回更新行数。"""
        changed = 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT list_id, url, host, country, asn FROM proxies"
            ).fetchall()
            updates = []
            for list_id, _url, host, country, asn in rows:
                new_country, new_asn = lookup_fn(host)
                if new_country != country or new_asn != asn:
                    updates.append((new_country, new_asn, utcnow_iso(), list_id, _url))
            if updates:
                cur = self._conn.cursor()
                cur.execute("BEGIN")
                try:
                    cur.executemany(
                        "UPDATE proxies SET country=?, asn=?, updated_at=? "
                        "WHERE list_id=? AND url=?",
                        updates,
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
                changed = len(updates)
        return changed

    def close(self) -> None:
        with self._lock:
            self._conn.close()
