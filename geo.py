"""本地 mmdb IP 归属查询：启动时下载缓存，查询代理主机 IP 的国家/ASN。"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import aiohttp
import maxminddb

log = logging.getLogger("proxy-dashboard.geo")

DEFAULT_MMDB_URL = "https://downloads.ip66.dev/db/ip66.mmdb"


async def download_mmdb(url: str, path: str, *, timeout: float = 60) -> bool:
    """下载 mmdb 到 path（临时文件 + 原子改名）。成功返回 True。"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp = path + ".tmp"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout), trust_env=False
        ) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                size = 0
                with open(tmp, "wb") as f:
                    async for chunk in resp.content.iter_chunked(1 << 16):
                        f.write(chunk)
                        size += len(chunk)
        os.replace(tmp, path)
        log.info("mmdb 已下载缓存: %s (%.1f MB)", path, size / 1e6)
        return True
    except Exception as e:
        log.warning("mmdb 下载失败（%s）：%s", url, e)
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False


class GeoResolver:
    """maxminddb 只读查询；查询失败不抛异常，返回 (None, None)。"""

    def __init__(self, path: str) -> None:
        self._reader = maxminddb.open_database(path)

    def lookup(self, ip: str) -> tuple[str | None, str | None]:
        """返回 (country, asn)。"""
        try:
            rec = self._reader.get(ip)
        except Exception:
            return None, None
        if not isinstance(rec, dict):
            return None, None
        country = None
        c = rec.get("country")
        if isinstance(c, dict):
            names = c.get("names")
            if isinstance(names, dict):
                country = names.get("en") or names.get("zh_cn") or names.get("zh")
            country = country or c.get("iso_code")
        asn = rec.get("autonomous_system_organization")
        return country, asn if isinstance(asn, str) else None

    def close(self) -> None:
        self._reader.close()
