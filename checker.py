"""名单解析 + 并发探活引擎。"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit

import aiohttp

log = logging.getLogger("proxy-dashboard.checker")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class ProxyState:
    """单个代理的全部状态（唯一事实源）。url 含凭据，永不进入公开载荷。"""

    id: int
    url: str
    host: str
    port: int
    username: str
    password: str
    status: str = "pending"  # pending | ok | proxy_error | dead
    latency_ms: float | None = None
    exit_ip: str | None = None
    country: str | None = None
    last_checked_at: str | None = None
    total_checks: int = 0
    ok_checks: int = 0
    consecutive_failures: int = 0

    def public(self) -> dict:
        """无凭据视图。"""
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "status": self.status,
            "latency_ms": self.latency_ms,
            "exit_ip": self.exit_ip,
            "country": self.country,
            "last_checked_at": self.last_checked_at,
            "total_checks": self.total_checks,
            "ok_checks": self.ok_checks,
            "consecutive_failures": self.consecutive_failures,
        }


def load_proxies(path: str) -> tuple[list[ProxyState], list[str]]:
    """解析名单文件。返回 (代理列表, 跳过原因列表)。

    行格式 http://user:pass@host:port；容忍 CRLF 行尾与空行；
    校验失败或重复的行跳过并记录原因。
    """
    proxies: list[ProxyState] = []
    skipped: list[str] = []
    seen: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip()  # 处理 CRLF 与杂散空白
            if not line:
                continue
            parts = urlsplit(line)
            try:
                port = parts.port
            except ValueError:
                port = None
            if (
                parts.scheme not in ("http", "https")
                or not parts.hostname
                or port is None
                or not parts.username
                or not parts.password
            ):
                skipped.append(f"第 {lineno} 行无法解析: {line[:60]}")
                continue
            if line in seen:
                skipped.append(f"第 {lineno} 行重复: {line[:60]}")
                continue
            seen.add(line)
            proxies.append(
                ProxyState(
                    id=len(proxies) + 1,
                    url=line,
                    host=parts.hostname,
                    port=port,
                    username=parts.username,
                    password=parts.password,
                )
            )
    if not proxies:
        raise SystemExit(f"名单 {path} 中没有可用的代理")
    return proxies, skipped


class CheckEngine:
    """持续扫描全部代理；手动重测与扫描共用同一在途去重表。"""

    def __init__(
        self,
        proxies: list[ProxyState],
        broker,
        *,
        check_url: str,
        timeout: float,
        concurrency: int,
        interval: float,
    ) -> None:
        self._proxies = proxies
        self._by_id = {p.id: p for p in proxies}
        self._broker = broker
        self._check_url = check_url
        self._timeout = timeout
        self._concurrency = concurrency
        self._interval = interval
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()  # 手动触发：跳过轮间等待
        self._inflight: set[int] = set()
        self.sweep_id = 0
        self._sweep_running = False
        self._sweep_checked = 0
        self._sweep_started_at: str | None = None
        self._last_finished_at: str | None = None
        self._last_duration_ms: float | None = None
        self._next_sweep_at: str | None = None

    # -- 生命周期 -----------------------------------------------------------

    async def start(self) -> None:
        # limit=0：并发由 semaphore 统一闸控，连接池默认 100 会暗中压低并发
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0),
            timeout=aiohttp.ClientTimeout(total=self._timeout),
            trust_env=False,  # 显式忽略环境变量里的代理设置
        )
        self._task = asyncio.create_task(self._sweep_loop(), name="sweep-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._session is not None:
            await self._session.close()
            self._session = None

    # -- 对外接口 -----------------------------------------------------------

    @property
    def proxies(self) -> list[ProxyState]:
        return self._proxies

    def get(self, pid: int) -> ProxyState | None:
        return self._by_id.get(pid)

    def is_inflight(self, pid: int) -> bool:
        return pid in self._inflight

    def request_sweep_now(self) -> str:
        """空闲→立即开扫（started）；扫描中→本轮结束后立即再扫（queued）。"""
        self._wake.set()
        self._next_sweep_at = None
        return "queued" if self._sweep_running else "started"

    def stats(self) -> dict:
        counts = self._status_counts()
        checked = counts["ok"] + counts["proxy_error"] + counts["dead"]
        latencies = [
            p.latency_ms for p in self._proxies
            if p.status == "ok" and p.latency_ms is not None
        ]
        return {
            "total": len(self._proxies),
            "checked": checked,
            **counts,
            "alive_rate": round(counts["ok"] / checked, 4) if checked else None,
            "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else None,
            "sweep": {
                "id": self.sweep_id,
                "running": self._sweep_running,
                "checked": self._sweep_checked,
                "total": len(self._proxies),
                "started_at": self._sweep_started_at,
                "last_finished_at": self._last_finished_at,
                "last_duration_ms": (
                    round(self._last_duration_ms) if self._last_duration_ms is not None else None
                ),
                "next_sweep_at": self._next_sweep_at,
            },
        }

    # -- 扫描循环 -----------------------------------------------------------

    async def _sweep_loop(self) -> None:
        while True:
            await self._run_sweep()
            self._next_sweep_at = (
                datetime.fromtimestamp(
                    time.time() + self._interval, tz=timezone.utc
                ).isoformat(timespec="seconds").replace("+00:00", "Z")
            )
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval)
            except TimeoutError:
                pass
            self._wake.clear()
            self._next_sweep_at = None

    async def _run_sweep(self) -> None:
        self.sweep_id += 1
        self._sweep_running = True
        self._sweep_checked = 0
        self._sweep_started_at = utcnow_iso()
        total = len(self._proxies)
        self._broker.publish(
            "sweep",
            {"phase": "start", "id": self.sweep_id, "total": total,
             "started_at": self._sweep_started_at},
        )
        t0 = time.monotonic()
        sem = asyncio.Semaphore(self._concurrency)

        async def guarded(p: ProxyState) -> None:
            async with sem:
                await self.check_one(p)
            self._sweep_checked += 1

        await asyncio.gather(*(guarded(p) for p in self._proxies))

        duration_ms = (time.monotonic() - t0) * 1000
        self._sweep_running = False
        self._last_finished_at = utcnow_iso()
        self._last_duration_ms = duration_ms
        self._broker.publish(
            "sweep",
            {"phase": "end", "id": self.sweep_id,
             "duration_ms": round(duration_ms),
             "finished_at": self._last_finished_at,
             **self._status_counts()},
        )

    # -- 单次检测 -----------------------------------------------------------

    async def check_one(self, p: ProxyState) -> dict | None:
        """检测单个代理并发布结果。已有同 id 检测在途则跳过（返回 None）。"""
        if p.id in self._inflight:
            return None
        self._inflight.add(p.id)
        try:
            t0 = time.monotonic()
            try:
                async with self._session.get(self._check_url, proxy=p.url) as resp:
                    body = await resp.text(errors="replace")
                    http_status = resp.status
                latency_ms = (time.monotonic() - t0) * 1000
                status, exit_ip, country = self._classify(http_status, body)
            except (aiohttp.ClientError, TimeoutError):
                # 超时 / 连接拒绝 / 重置 / 代理不可达
                status, latency_ms, exit_ip, country = "dead", None, None, None
            except Exception:
                log.exception("检测代理 %s 时出现未预期异常", p.host)
                status, latency_ms, exit_ip, country = "dead", None, None, None

            p.status = status
            p.last_checked_at = utcnow_iso()
            p.total_checks += 1
            if status == "ok":
                p.ok_checks += 1
                p.consecutive_failures = 0
                if exit_ip:
                    p.exit_ip = exit_ip
                if country:
                    p.country = country
            else:
                p.consecutive_failures += 1
                # 非 ok 保留上次 exit_ip / country（最近已知值）
            p.latency_ms = round(latency_ms, 1) if latency_ms is not None else None

            pub = p.public()
            self._broker.publish_update(pub)
            return pub
        finally:
            self._inflight.discard(p.id)

    @staticmethod
    def _classify(http_status: int, body: str) -> tuple[str, str | None, str | None]:
        """返回 (status, exit_ip, country)；dead 由调用方的异常路径判定。

        ok          2xx 且 body 为空或合法 JSON（代理可用）
        proxy_error 有 HTTP 响应但不可用（非 2xx，或 2xx 但 body 是被注入的垃圾）
        """
        if not 200 <= http_status < 300:
            return "proxy_error", None, None
        body = body.strip()
        if not body:
            return "ok", None, None  # 例如 generate_204 探活地址
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return "proxy_error", None, None
        if isinstance(data, dict):
            return "ok", data.get("query"), data.get("country")
        return "ok", None, None

    def _status_counts(self) -> dict[str, int]:
        counts = {"ok": 0, "proxy_error": 0, "dead": 0, "pending": 0}
        for p in self._proxies:
            counts[p.status] += 1
        return counts
