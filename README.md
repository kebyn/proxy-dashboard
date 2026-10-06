# 代理探活面板

实时探活 HTTP 代理并实时展示状态。名单从前端上传，名单与检测状态持久化在
SQLite，重启自动恢复。

## 运行

```bash
uv run main.py                # 监听 0.0.0.0:8000，打开 http://<主机>:8000/
```

首次打开页面上传名单（拖拽文件或粘贴文本，每行一个
`http://user:pass@host:port`），上传后自动开始首轮探活。

## 名单与状态持久化

- SQLite 库 `data/proxies.db`（`--db` 可改）：名单、顺序、检测状态、延迟、
  出口 IP、成功计数全部落库，重启原样恢复
- 重新上传按 URL 匹配：**已在库中的代理保留检测统计**，新代理从零开始，
  不在新名单中的删除（完全替换）
- 每次上传的原始文本归档到 `data/uploads/<时间戳>.txt`（含被跳过的行，只增不删）
- 检测结果每 300ms 批量写库，关闭时最终落盘

## 配置

命令行参数优先于环境变量，环境变量优先于默认值。

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--proxy-file` | `PROXY_FILE` | 无 | 可选：**库为空时**的首次种子名单（库非空则忽略） |
| `--db` | `DB_PATH` | `data/proxies.db` | SQLite 库文件路径 |
| `--check-url` | `CHECK_URL` | `http://www.gstatic.com/generate_204` | 探活目标；2xx 即有效。换成 ip-api JSON 地址可显示出口 IP/国家 |
| `--timeout` | `CHECK_TIMEOUT` | `10` | 单次检测超时（秒） |
| `--concurrency` | `CONCURRENCY` | `100` | 并发检测数 |
| `--interval` | `SWEEP_INTERVAL` | `120` | 两轮扫描之间的暂停（秒） |
| `--host` | `HOST` | `0.0.0.0` | 监听地址 |
| `--port` | `PORT` | `8000` | 监听端口 |

## 状态分类

| 状态（面板） | 导出菜单 | 判定 |
|---|---|---|
| 存活 | 有效 | 2xx 且响应体为空或合法 JSON |
| 异常 | 失效 | 有 HTTP 响应但非 2xx，或 2xx 但响应体非法 |
| 失败 | 无效 | 超时 / 连接拒绝 / 重置 |

## API

| 方法/路径 | 说明 |
|---|---|
| `GET /api/status` | 全量快照（无凭据） |
| `GET /api/events` | SSE：连接即发 `snapshot`，之后推 `proxy_update`/`stats`/`sweep` |
| `POST /api/upload` | 上传名单（text/plain，每行一个代理），覆盖旧名单 |
| `POST /api/check/{id}` | 重测单个代理 |
| `POST /api/check-all` | 立即开扫（空闲 200 / 扫描中 202 排队） |
| `GET /api/proxies/{id}/url` | 完整代理 URL（含凭据，复制按钮用） |
| `GET /api/export?status=` | 导出名单：`ok`/`proxy_error`/`dead`/`all` |

## 安全提示

面板、导出与上传接口暴露代理凭据，默认 `0.0.0.0` 仅适用于可信网络；
仅本机使用设 `HOST=127.0.0.1`，远程访问走 SSH 隧道。

## 文件

- `main.py` — FastAPI 入口（路由、SSE、上传、持久化编排）
- `checker.py` — 名单解析 + 探活引擎
- `store.py` — SQLite 持久层
- `broker.py` — SSE 事件代理（300ms 批量推送）
- `static/index.html` — 自包含前端（无外部依赖）
