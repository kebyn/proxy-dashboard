[English](README.md) | 简体中文

# 代理探活面板

实时探活 HTTP 代理并实时展示状态。名单从前端上传（**多名单**，可切换），
名单与检测状态持久化在 SQLite，重启自动恢复；IP 归属用本地 mmdb 查询。

## 运行

```bash
uv run main.py                # 监听 0.0.0.0:8000，打开 http://<主机>:8000/
```

首次打开上传名单（拖拽文件或粘贴文本，可命名）；上传后自动开始探活。
停止：`Ctrl+C` 或 `kill <pid>`——即使面板开着（SSE 长连接）也会在 5 秒内
优雅退出并落盘。

## 多名单

- 每次上传保存为独立名单（各自的状态/统计/顺序互不影响）
- 工具栏「名单 ▾」选择器：点击切换、✕ 删除（两步确认）
- 上传同名名单 = 覆盖该名单成员，**匹配 URL 的代理保留检测统计**
- 名称默认取文件名（去扩展名），粘贴上传且未填名称则按时间自动命名

## 持久化

- SQLite `data/proxies.db`（`--db` 可改）：`lists` + `proxies(复合主键)` +
  `meta(活跃名单)`；检测结果每 300ms 批量写库，退出时最终落盘
- **本地 mmdb IP 归属**：每次启动从 `--mmdb-url`（默认
  `https://downloads.ip66.dev/db/ip66.mmdb`，约 18MB）下载缓存到
  `data/ip66.mmdb`；查询代理主机 IP 的国家/ASN（上传即填充，待检测行也显示）；
  下载失败回退已缓存文件，仍不可用则降级（国家列"—"，探活不受影响）
- 每次上传的原始文本归档到 `data/uploads/<时间戳>.txt`（只增不删）
- 旧版单名单库启动时自动迁移为「默认名单」

## 配置

命令行参数优先于环境变量，环境变量优先于默认值。

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--proxy-file` | `PROXY_FILE` | 无 | 可选：库为空时的首次种子名单 |
| `--db` | `DB_PATH` | `data/proxies.db` | SQLite 库文件路径 |
| `--check-url` | `CHECK_URL` | `http://www.gstatic.com/generate_204` | 探活目标；2xx 即有效 |
| `--mmdb-url` | `MMDB_URL` | ip66 下载地址 | 本地 IP 归属库下载地址 |
| `--mmdb-path` | `MMDB_PATH` | `data/ip66.mmdb` | mmdb 缓存路径 |
| `--timeout` | `CHECK_TIMEOUT` | `10` | 单次检测超时（秒） |
| `--concurrency` | `CONCURRENCY` | `100` | 并发检测数 |
| `--interval` | `SWEEP_INTERVAL` | `120` | 两轮扫描之间的暂停（秒） |
| `--host` | `HOST` | `0.0.0.0` | 监听地址 |
| `--port` | `PORT` | `8000` | 监听端口 |

## 状态分类

| 状态（面板） | 导出菜单 | 判定 |
|---|---|---|
| 存活 | 有效 | 探活目标返回 2xx（如 gstatic 204） |
| 异常 | 失效 | 代理有响应但非 2xx（如 504） |
| 失败 | 无效 | 超时 / 连接拒绝 / 重置 |

国家 / ASN 来自本地 mmdb（代理主机 IP），与探活解耦。

## API

| 方法/路径 | 说明 |
|---|---|
| `GET /api/status` | 全量快照（无凭据，含名单列表） |
| `GET /api/events` | SSE：连接即发 `snapshot`，之后推 `proxy_update`/`stats`/`sweep` |
| `GET /api/lists` | 全部名单 |
| `POST /api/lists/{id}/activate` | 切换活跃名单 |
| `DELETE /api/lists/{id}` | 删除名单 |
| `POST /api/upload?name=...` | 上传名单（text/plain，每行一个代理） |
| `POST /api/check/{id}` | 重测单个代理 |
| `POST /api/check-all` | 立即开扫（空闲 200 / 扫描中 202 排队） |
| `GET /api/proxies/{id}/url` | 完整代理 URL（含凭据，复制按钮用） |
| `GET /api/export?status=` | 导出当前名单：`ok`/`proxy_error`/`dead`/`all` |

## 安全提示

面板、导出与上传接口暴露代理凭据，默认 `0.0.0.0` 仅适用于可信网络；
仅本机使用设 `HOST=127.0.0.1`，远程访问走 SSH 隧道。

## 文件

- `main.py` — FastAPI 入口（路由、SSE、上传/名单、持久化编排、优雅停止）
- `checker.py` — 名单解析 + 探活引擎
- `store.py` — SQLite 持久层（多名单 + 迁移）
- `geo.py` — mmdb 下载缓存 + 本地归属查询
- `broker.py` — SSE 事件代理（300ms 批量推送）
- `static/index.html` — 自包含前端（无外部依赖）
