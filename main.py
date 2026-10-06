"""代理探活面板 — FastAPI 入口。

配置优先级：命令行参数 > 环境变量 > 默认值。
运行：uv run main.py [--host 0.0.0.0] [--port 8000] [--proxy-file ...]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from broker import Broker, sse_format
from checker import CheckEngine, load_proxies

log = logging.getLogger("proxy-dashboard")

STATIC_DIR = Path(__file__).parent / "static"
EXPORT_STATUSES = ("all", "ok", "proxy_error", "dead")


@dataclass
class Config:
    proxy_file: str
    check_url: str
    timeout: float
    concurrency: int
    interval: float
    host: str
    port: int


def parse_args() -> Config:
    ap = argparse.ArgumentParser(description="代理探活面板")
    ap.add_argument("--proxy-file", default=os.environ.get("PROXY_FILE", "/data/proxies_gn_001722.txt"),
                    help="代理名单文件路径")
    ap.add_argument("--check-url", default=os.environ.get(
        "CHECK_URL", "http://ip-api.com/json/?fields=status,query,country"),
        help="探活目标 URL")
    ap.add_argument("--timeout", type=float, default=float(os.environ.get("CHECK_TIMEOUT", "10")),
                    help="单次检测超时（秒）")
    ap.add_argument("--concurrency", type=int, default=int(os.environ.get("CONCURRENCY", "100")),
                    help="并发检测数")
    ap.add_argument("--interval", type=float, default=float(os.environ.get("SWEEP_INTERVAL", "120")),
                    help="两轮扫描之间的暂停（秒）")
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"), help="监听地址")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")), help="监听端口")
    return Config(**vars(ap.parse_args()))


def status_payload(app: FastAPI) -> dict:
    """/api/status 与 SSE snapshot 共用的全量快照（不含凭据）。"""
    cfg: Config = app.state.cfg
    engine: CheckEngine = app.state.engine
    return {
        "config": {
            "check_url": cfg.check_url,
            "timeout": cfg.timeout,
            "concurrency": cfg.concurrency,
            "sweep_interval": cfg.interval,
        },
        "stats": engine.stats(),
        "proxies": [p.public() for p in engine.proxies],
    }


def create_app(cfg: Config) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        proxies, skipped = load_proxies(cfg.proxy_file)
        for s in skipped:
            log.warning("跳过 %s", s)
        log.info("已加载 %d 个代理（跳过 %d 行），开始探活", len(proxies), len(skipped))
        app.state.engine = CheckEngine(
            proxies, app.state.broker,
            check_url=cfg.check_url, timeout=cfg.timeout,
            concurrency=cfg.concurrency, interval=cfg.interval,
        )
        app.state.broker.set_stats_provider(app.state.engine.stats)
        await app.state.broker.start()
        await app.state.engine.start()  # 首轮扫描立即开始
        yield
        await app.state.engine.stop()
        await app.state.broker.stop()

    app = FastAPI(lifespan=lifespan)
    app.state.cfg = cfg
    app.state.broker = Broker()

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/status")
    async def api_status(request: Request) -> dict:
        return status_payload(request.app)

    @app.get("/api/events")
    async def api_events(request: Request) -> StreamingResponse:
        broker: Broker = request.app.state.broker
        q = broker.subscribe()

        async def gen():
            try:
                # 连接（含重连）即发全量快照，天然消除事件/快照竞态
                q.put_nowait(sse_format("snapshot", status_payload(request.app)))
                while True:
                    try:
                        yield await asyncio.wait_for(q.get(), timeout=15)
                    except TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                broker.unsubscribe(q)

        return StreamingResponse(
            gen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/check/{pid}")
    async def api_check_one(pid: int, request: Request):
        engine: CheckEngine = request.app.state.engine
        p = engine.get(pid)
        if p is None:
            raise HTTPException(status_code=404, detail="未知的代理 id")
        if engine.is_inflight(pid):
            return JSONResponse(
                status_code=409,
                content={"detail": "该代理正在检测中", "proxy": p.public()},
            )
        result = await engine.check_one(p)
        if result is None:  # 极小概率：恰好在途
            return JSONResponse(
                status_code=409,
                content={"detail": "该代理正在检测中", "proxy": p.public()},
            )
        return result

    @app.post("/api/check-all")
    async def api_check_all(request: Request):
        result = request.app.state.engine.request_sweep_now()
        return JSONResponse(
            status_code=202 if result == "queued" else 200,
            content={"result": result},
        )

    @app.get("/api/proxies/{pid}/url")
    async def api_proxy_url(pid: int, request: Request) -> PlainTextResponse:
        p = request.app.state.engine.get(pid)
        if p is None:
            raise HTTPException(status_code=404, detail="未知的代理 id")
        return PlainTextResponse(p.url + "\n")

    @app.get("/api/export")
    async def api_export(request: Request, status: str = "ok") -> PlainTextResponse:
        engine: CheckEngine = request.app.state.engine
        if status not in EXPORT_STATUSES:
            raise HTTPException(status_code=422, detail="status 取值须为 all/ok/proxy_error/dead")
        urls = [p.url for p in engine.proxies if status == "all" or p.status == status]
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        body = "\n".join(urls) + ("\n" if urls else "")
        return PlainTextResponse(
            body,
            media_type="text/plain; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="proxies_{status}_{stamp}.txt"'
            },
        )

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    cfg = parse_args()
    app = create_app(cfg)
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_config=None)


if __name__ == "__main__":
    main()
