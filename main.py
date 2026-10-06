"""代理探活面板 — FastAPI 入口。

名单从前端上传（POST /api/upload，多名单可切换），名单与检测状态持久化在
SQLite；IP 归属用本地 mmdb（启动时下载缓存）。重启自动恢复。
配置优先级：命令行参数 > 环境变量 > 默认值。
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
from geo import DEFAULT_MMDB_URL, GeoResolver, download_mmdb
from store import ProxyStore

log = logging.getLogger("proxy-dashboard")

PROJECT_DIR = Path(__file__).parent
STATIC_DIR = PROJECT_DIR / "static"
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_DB = str(DATA_DIR / "proxies.db")
DEFAULT_MMDB_PATH = str(DATA_DIR / "ip66.mmdb")
UPLOAD_MAX = 10 * 1024 * 1024  # 10MB
EXPORT_STATUSES = ("all", "ok", "proxy_error", "dead")
GRACEFUL_SHUTDOWN_SECONDS = 5  # SSE 长连接存在时的强制关闭时限


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
    mmdb_url: str
    mmdb_path: str


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
    ap.add_argument("--mmdb-url", default=os.environ.get("MMDB_URL", DEFAULT_MMDB_URL),
                    help="本地 IP 归属库下载地址")
    ap.add_argument("--mmdb-path", default=os.environ.get("MMDB_PATH", DEFAULT_MMDB_PATH),
                    help="mmdb 缓存文件路径")
    return Config(**vars(ap.parse_args()))


def status_payload(app: FastAPI) -> dict:
    """/api/status 与 SSE snapshot 共用的全量快照（不含凭据）。"""
    cfg: Config = app.state.cfg
    engine: CheckEngine = app.state.engine
    store: ProxyStore = app.state.store
    lists = store.lists()
    active = next((l for l in lists if l["id"] == engine.list_id), None)
    return {
        "config": {
            "check_url": cfg.check_url,
            "timeout": cfg.timeout,
            "concurrency": cfg.concurrency,
            "sweep_interval": cfg.interval,
        },
        "stats": engine.stats(),
        "lists": lists,
        "active_list": {"id": active["id"], "name": active["name"]} if active else None,
        "proxies": [p.public() for p in engine.proxies],
    }


async def persist_loop(engine: CheckEngine, store: ProxyStore) -> None:
    """每 300ms 把脏代理批量写入 SQLite（写库不阻塞事件循环）。"""
    while True:
        await asyncio.sleep(0.3)
        dirty = engine.take_dirty()
        if dirty:
            await asyncio.to_thread(store.save_updates, engine.list_id, dirty)


def create_app(cfg: Config) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = ProxyStore(cfg.db)
        app.state.store = store

        # mmdb：每次启动下载缓存；失败回退已缓存文件；再失败降级（探活不受影响）
        geo: GeoResolver | None = None
        if await download_mmdb(cfg.mmdb_url, cfg.mmdb_path):
            geo = GeoResolver(cfg.mmdb_path)
        elif Path(cfg.mmdb_path).exists():
            log.warning("mmdb 下载失败，使用已缓存文件 %s", cfg.mmdb_path)
            geo = GeoResolver(cfg.mmdb_path)
        else:
            log.warning("mmdb 不可用，IP 归属查询降级（国家列将为空）")
        app.state.geo = geo
        if geo is not None:
            changed = await asyncio.to_thread(store.refresh_geo, geo.lookup)
            log.info("已刷新 %d 个代理的 IP 归属", changed)

        # 载入名单：--proxy-file 仅在库为空时作种子
        active_id = store.get_active_id()
        if active_id is None and not store.lists() and cfg.proxy_file:
            seed, skipped = load_proxies(cfg.proxy_file)
            for s in skipped:
                log.warning("种子名单跳过 %s", s)
            log.info("从 %s 导入种子名单 %d 个", cfg.proxy_file, len(seed))
            if geo is not None:
                for p in seed:
                    p.country, p.asn = geo.lookup(p.host)
            active_id, proxies, _, _ = await asyncio.to_thread(
                store.apply_upload, "种子名单", seed)
        else:
            if cfg.proxy_file:
                log.info("数据库已有名单，忽略 --proxy-file")
            proxies = await asyncio.to_thread(store.load_all, active_id) if active_id else []
        log.info("已加载名单 %s：%d 个代理，开始探活", active_id, len(proxies))

        app.state.engine = CheckEngine(
            proxies, app.state.broker,
            check_url=cfg.check_url, timeout=cfg.timeout,
            concurrency=cfg.concurrency, interval=cfg.interval,
            list_id=active_id,
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
            await asyncio.to_thread(store.save_updates, app.state.engine.list_id, dirty)
        if app.state.geo is not None:
            app.state.geo.close()
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

    @app.get("/api/lists")
    async def api_lists(request: Request) -> dict:
        store: ProxyStore = request.app.state.store
        lists = await asyncio.to_thread(store.lists)
        active_id = request.app.state.engine.list_id
        return {"lists": lists, "active_list_id": active_id}

    @app.post("/api/lists/{list_id}/activate")
    async def api_activate_list(list_id: int, request: Request):
        store: ProxyStore = request.app.state.store
        engine: CheckEngine = request.app.state.engine
        lists = await asyncio.to_thread(store.lists)
        info = next((l for l in lists if l["id"] == list_id), None)
        if info is None:
            raise HTTPException(status_code=404, detail="未知的名单 id")
        states = await asyncio.to_thread(store.load_all, list_id)
        await asyncio.to_thread(store.set_active, list_id)
        await engine.reload(states, list_id)
        request.app.state.broker.clear_pending()
        request.app.state.broker.publish("snapshot", status_payload(request.app))
        return {"result": "activated", "list": info}

    @app.delete("/api/lists/{list_id}")
    async def api_delete_list(list_id: int, request: Request):
        store: ProxyStore = request.app.state.store
        engine: CheckEngine = request.app.state.engine
        try:
            new_active = await asyncio.to_thread(store.delete_list, list_id)
        except LookupError:
            raise HTTPException(status_code=404, detail="未知的名单 id")
        if new_active != engine.list_id:
            # 删的是活跃名单：切换到新的活跃名单（可能为空）
            states = (await asyncio.to_thread(store.load_all, new_active)
                      if new_active is not None else [])
            await engine.reload(states, new_active)
        request.app.state.broker.clear_pending()
        request.app.state.broker.publish("snapshot", status_payload(request.app))
        return {"result": "deleted", "active_list_id": new_active}

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
    async def api_upload(request: Request, name: str = ""):
        """上传名单（text/plain，每行一个代理）。按名称 upsert：新名新建并激活，
        同名替换成员（匹配 URL 保留统计）。原始文本归档到 data/uploads/。
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
        name = (name or "").strip() or datetime.now(timezone.utc).strftime("名单_%Y%m%d_%H%M")
        # 原始文本归档（含被跳过的行，供追溯）
        uploads_dir = DATA_DIR / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        (uploads_dir / f"{stamp}.txt").write_bytes(raw)

        geo: GeoResolver | None = request.app.state.geo
        if geo is not None:  # 本地 mmdb 归属填充（含待检测代理）
            for p in parsed:
                p.country, p.asn = geo.lookup(p.host)

        store: ProxyStore = request.app.state.store
        engine: CheckEngine = request.app.state.engine
        list_id, merged, kept, fresh = await asyncio.to_thread(
            store.apply_upload, name, parsed)
        await engine.reload(merged, list_id)  # 新任务首轮立即开扫
        request.app.state.broker.clear_pending()
        request.app.state.broker.publish("snapshot", status_payload(request.app))
        log.info("上传名单「%s」(id=%s)：%d 个（保留 %d · 新增 %d · 跳过 %d 行）",
                 name, list_id, len(merged), kept, fresh, len(skipped))
        return {
            "list_id": list_id,
            "name": name,
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
    # timeout_graceful_shutdown：SSE 长连接在停止时最多等 5 秒即强制关闭，
    # lifespan（最终落盘）随后照常执行——否则开着面板时服务无法停止
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_config=None,
                timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_SECONDS)


if __name__ == "__main__":
    main()
