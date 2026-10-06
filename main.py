"""代理探活面板 — FastAPI 入口。

名单从前端上传（POST /api/upload），名单与检测状态持久化在 SQLite；
重启自动恢复。配置优先级：命令行参数 > 环境变量 > 默认值。
运行：uv run main.py [--host 0.0.0.0] [--port 8000] [--db data/proxies.db]
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
from checker import CheckEngine, ProxyState, load_proxies, parse_proxies
from store import ProxyStore

log = logging.getLogger("proxy-dashboard")

PROJECT_DIR = Path(__file__).parent
STATIC_DIR = PROJECT_DIR / "static"
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_DB = str(DATA_DIR / "proxies.db")
UPLOAD_MAX = 10 * 1024 * 1024  # 10MB
EXPORT_STATUSES = ("all", "ok", "proxy_error", "dead")


@dataclass
class Config:
    proxy_file: str | None
    db: str
    check_url: str
    timeout: float
    concurrency: int
    interval: float
    host: str
    port: int


def parse_args() -> Config:
    ap = argparse.ArgumentParser(description="代理探活面板")
    ap.add_argument("--proxy-file", default=os.environ.get("PROXY_FILE"),
                    help="可选：库为空时的首次种子名单路径（库非空则忽略）")
    ap.add_argument("--db", default=os.environ.get("DB_PATH", DEFAULT_DB), help="SQLite 库文件路径")
    ap.add_argument("--check-url", default=os.environ.get(
        "CHECK_URL", "http://www.gstatic.com/generate_204"), help="探活目标 URL")
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


async def persist_loop(engine: CheckEngine, store: ProxyStore) -> None:
    """每 300ms 把脏代理批量写入 SQLite（写库不阻塞事件循环）。"""
    while True:
        await asyncio.sleep(0.3)
        dirty = engine.take_dirty()
        if dirty:
            await asyncio.to_thread(store.save_updates, dirty)


def create_app(cfg: Config) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = ProxyStore(cfg.db)
        app.state.store = store
        if store.count() == 0 and cfg.proxy_file:
            seed, skipped = load_proxies(cfg.proxy_file)
            for s in skipped:
                log.warning("种子名单跳过 %s", s)
            log.info("从 %s 导入种子名单 %d 个", cfg.proxy_file, len(seed))
            proxies, _, _ = await asyncio.to_thread(store.apply_upload, seed)
        else:
            if cfg.proxy_file:
                log.info("数据库已有名单，忽略 --proxy-file")
            proxies = await asyncio.to_thread(store.load_all)
        log.info("已加载 %d 个代理，开始探活", len(proxies))

        app.state.engine = CheckEngine(
            proxies, app.state.broker,
            check_url=cfg.check_url, timeout=cfg.timeout,
            concurrency=cfg.concurrency, interval=cfg.interval,
        )
        app.state.broker.set_stats_provider(app.state.engine.stats)
        await app.state.broker.start()
        await app.state.engine.start()  # 有名单时首轮扫描立即开始
        app.state.persister = asyncio.create_task(
            persist_loop(app.state.engine, store), name="db-persister")
        yield
        app.state.persister.cancel()
        try:
            await app.state.persister
        except asyncio.CancelledError:
            pass
        await app.state.engine.stop()
        dirty = app.state.engine.take_dirty()  # 最终落盘
        if dirty:
            await asyncio.to_thread(store.save_updates, dirty)
        store.close()
        log.info("已保存状态并关闭")

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

    @app.post("/api/upload")
    async def api_upload(request: Request):
        """上传名单（text/plain，每行一个代理），覆盖旧名单。

        已在库中的 URL 保留检测状态与统计；原始文本归档到 data/uploads/。
        """
        raw = await request.body()
        if len(raw) > UPLOAD_MAX:
            return JSONResponse(status_code=413, content={"detail": "名单过大（上限 10MB）"})
        parsed, skipped = parse_proxies(raw.decode("utf-8", errors="replace"))
        if not parsed:
            return JSONResponse(
                status_code=400,
                content={"detail": "名单中没有可解析的代理", "skipped": skipped[:5]},
            )
        # 原始文本归档（含被跳过的行，供追溯）
        uploads_dir = DATA_DIR / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        (uploads_dir / f"{stamp}.txt").write_bytes(raw)

        store: ProxyStore = request.app.state.store
        engine: CheckEngine = request.app.state.engine
        merged, kept, fresh = await asyncio.to_thread(store.apply_upload, parsed)
        await engine.reload(merged)  # 新任务首轮立即开扫
        request.app.state.broker.clear_pending()
        request.app.state.broker.publish("snapshot", status_payload(request.app))
        log.info("上传名单：%d 个（保留 %d · 新增 %d · 跳过 %d 行）",
                 len(merged), kept, fresh, len(skipped))
        return {
            "loaded": len(merged),
            "kept": kept,
            "fresh": fresh,
            "skipped_count": len(skipped),
            "skipped": skipped[:5],
        }

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
        engine: CheckEngine = request.app.state.engine
        if not engine.proxies:
            raise HTTPException(status_code=400, detail="名单为空，请先上传")
        result = engine.request_sweep_now()
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
