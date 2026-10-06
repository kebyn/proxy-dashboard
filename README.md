# 代理探活面板

实时探活 HTTP 代理名单并以 Web 面板展示状态的工具。

## 运行

```bash
uv run main.py                # 默认读取 /data/proxies_gn_001722.txt，监听 0.0.0.0:8000
```

浏览器打开 `http://<主机>:8000/`。

## 配置

命令行参数优先于环境变量，环境变量优先于默认值。

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--proxy-file` | `PROXY_FILE` | `/data/proxies_gn_001722.txt` | 代理名单，每行 `http://user:pass@host:port`（容忍 CRLF） |
| `--check-url` | `CHECK_URL` | `http://ip-api.com/json/?fields=status,query,country` | 探活目标；2xx 即存活，JSON 的 `query`/`country` 字段显示为出口 IP/国家 |
| `--timeout` | `CHECK_TIMEOUT` | `10` | 单次检测超时（秒） |
| `--concurrency` | `CONCURRENCY` | `100` | 并发检测数 |
| `--interval` | `SWEEP_INTERVAL` | `120` | 两轮扫描之间的暂停（秒） |
| `--host` | `HOST` | `0.0.0.0` | 监听地址 |
| `--port` | `PORT` | `8000` | 监听端口 |

启动即开始第一轮扫描，之后每轮结束暂停 `interval` 秒再扫下一轮；面板上的「全部重检」可跳过等待。

## 状态分类

| 状态 | 判定 | 含义 |
|---|---|---|
| 存活 | 2xx 且响应体为空或合法 JSON | 代理可用 |
| 异常 | 有 HTTP 响应但非 2xx，或 2xx 但响应体非法 | 代理可达但不可用（如 504、注入页） |
| 失败 | 超时 / 连接拒绝 / 重置 | 代理死亡 |

## API

| 方法/路径 | 说明 |
|---|---|
| `GET /api/status` | 全量快照（无凭据） |
| `GET /api/events` | SSE 实时流：连接即发 `snapshot`，之后推送 `proxy_update`/`stats`/`sweep` |
| `POST /api/check/{id}` | 重测单个代理 |
| `POST /api/check-all` | 立即开扫（空闲 200 / 扫描中 202 排队） |
| `GET /api/proxies/{id}/url` | 完整代理 URL（含凭据，复制按钮用） |
| `GET /api/export?status=ok` | 导出名单（`ok`/`proxy_error`/`dead`/`all`） |

## 安全提示

面板与导出接口会暴露代理凭据，默认 `0.0.0.0` 仅适用于可信网络；
仅本机使用请设 `HOST=127.0.0.1`，远程访问建议走 SSH 隧道。

## 文件

- `main.py` — FastAPI 入口（路由、SSE、配置）
- `checker.py` — 名单解析 + 探活引擎
- `broker.py` — SSE 事件代理（300ms 批量推送）
- `static/index.html` — 自包含前端（无外部依赖）
