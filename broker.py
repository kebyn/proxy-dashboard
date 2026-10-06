"""SSE 事件代理：按客户端 fan-out，周期性把缓冲的代理结果合并推送。"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

log = logging.getLogger("proxy-dashboard.broker")


def sse_format(event: str, data) -> str:
    """格式化为一条 SSE 消息。"""
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n"


class Broker:
    """每个 SSE 客户端一个有界队列；卡死的客户端被丢弃，靠重连快照恢复。"""

    def __init__(self, *, flush_interval: float = 0.3) -> None:
        self._flush_interval = flush_interval
        self._stats_provider: Callable[[], dict] | None = None
        self._queues: set[asyncio.Queue[str]] = set()
        self._pending: list[dict] = []
        self._stats_dirty = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop(), name="broker-flush")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def set_stats_provider(self, provider: Callable[[], dict]) -> None:
        self._stats_provider = provider

    def subscribe(self) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
        self._queues.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[str]) -> None:
        self._queues.discard(q)

    def publish(self, event: str, data) -> None:
        """立即 fan-out 的事件（sweep 起止等）。"""
        self._fanout(sse_format(event, data))
        self._stats_dirty = True

    def publish_update(self, item: dict) -> None:
        """缓冲单个代理结果，flush 时合并成一个 proxy_update 事件。"""
        self._pending.append(item)

    def clear_pending(self) -> None:
        """丢弃滞留的待发更新（名单替换时用，防止旧 id 混入新快照之后）。"""
        self._pending.clear()

    def _fanout(self, message: str) -> None:
        for q in list(self._queues):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                # 消费停滞的客户端直接丢弃；其断线重连后靠 snapshot 恢复
                self._queues.discard(q)
                log.warning("SSE 客户端队列满，已丢弃（重连将自动恢复）")

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            if self._pending:
                items, self._pending = self._pending, []
                self._fanout(sse_format("proxy_update", {"items": items}))
                self._stats_dirty = True
            if self._stats_dirty and self._stats_provider is not None:
                self._stats_dirty = False
                self._fanout(sse_format("stats", self._stats_provider()))
