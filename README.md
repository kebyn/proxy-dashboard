English | [简体中文](README.zh-CN.md)

# Proxy Health-Check Dashboard

A real-time dashboard that monitors HTTP proxies. Proxy lists are uploaded
from the browser (**multiple lists**, switchable), list and check state
persist in SQLite across restarts, and IP geolocation is resolved from a
local mmdb database.

## Run

```bash
uv run main.py                # listens on 0.0.0.0:8000, open http://<host>:8000/
```

On first launch, upload a list (drag & drop a file or paste text, optionally
named); checking starts immediately. Stop with `Ctrl+C` or `kill <pid>` —
even with the dashboard open (live SSE connections) the server exits
gracefully within 5 seconds and flushes state to disk.

## Multiple lists

- Each upload is saved as an independent list (state, stats and order are
  isolated per list)
- The `List ▾` selector in the toolbar: click to switch, ✕ to delete
  (two-step confirmation)
- Uploading under an existing name replaces that list's members while
  **keeping check stats for matching URLs**
- List names default to the uploaded filename (extension stripped);
  unnamed pastes get a timestamp-based name

## Persistence

- SQLite at `data/proxies.db` (`--db` to change): `lists` +
  `proxies` (composite key) + `meta` (active list); check results are
  batch-written every 300 ms with a final flush on exit
- **Local mmdb geolocation**: on every startup the database is downloaded
  from `--mmdb-url` (default `https://downloads.ip66.dev/db/ip66.mmdb`,
  ~18 MB) and cached at `data/ip66.mmdb`; country and ASN are resolved from
  each proxy's host IP (filled on upload, shown even for unchecked proxies).
  If the download fails the cached file is used; with no cache at all,
  geolocation degrades gracefully (country column shows "—", checking is
  unaffected)
- Every upload's raw text is archived to `data/uploads/<timestamp>.txt`
  (append-only)
- Legacy single-list databases are migrated automatically into a list named
  `默认名单` ("Default list") on startup

## Configuration

CLI flags override environment variables, which override defaults.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--proxy-file` | `PROXY_FILE` | none | Optional seed list, used only when the DB is empty |
| `--db` | `DB_PATH` | `data/proxies.db` | SQLite database path |
| `--check-url` | `CHECK_URL` | `http://www.gstatic.com/generate_204` | Health-check target; any 2xx counts as valid |
| `--mmdb-url` | `MMDB_URL` | ip66 download URL | Geolocation database download URL |
| `--mmdb-path` | `MMDB_PATH` | `data/ip66.mmdb` | mmdb cache path |
| `--timeout` | `CHECK_TIMEOUT` | `10` | Per-check timeout (seconds) |
| `--concurrency` | `CONCURRENCY` | `100` | Concurrent checks |
| `--interval` | `SWEEP_INTERVAL` | `120` | Pause between sweeps (seconds) |
| `--host` | `HOST` | `0.0.0.0` | Bind address |
| `--port` | `PORT` | `8000` | Bind port |

## Status classification

| Dashboard | Export menu | Verdict |
|---|---|---|
| 存活 (Alive) | 有效 (Valid) | Check target returned 2xx (e.g. gstatic 204) |
| 异常 (Degraded) | 失效 (Degraded) | Proxy responded but not 2xx (e.g. 504) |
| 失败 (Dead) | 无效 (Invalid) | Timeout / connection refused / reset |

Country / ASN come from the local mmdb (proxy host IP), decoupled from
health checks. The UI itself is Chinese.

## API

| Method / path | Description |
|---|---|
| `GET /api/status` | Full snapshot (no credentials; includes list index) |
| `GET /api/events` | SSE: `snapshot` on connect, then `proxy_update` / `stats` / `sweep` |
| `GET /api/lists` | All lists |
| `POST /api/lists/{id}/activate` | Switch active list |
| `DELETE /api/lists/{id}` | Delete a list |
| `POST /api/upload?name=...` | Upload a list (text/plain, one proxy per line) |
| `POST /api/check/{id}` | Re-check a single proxy |
| `POST /api/check-all` | Start a sweep now (200 idle / 202 queued) |
| `GET /api/proxies/{id}/url` | Full proxy URL (contains credentials; used by the copy button) |
| `GET /api/export?status=` | Export the active list: `ok` / `proxy_error` / `dead` / `all` |

## Security notes

The dashboard, export and upload endpoints expose proxy credentials.
The default `0.0.0.0` bind is meant for trusted networks only; for
local-only use set `HOST=127.0.0.1`, and use an SSH tunnel for remote
access.

## Project layout

- `main.py` — FastAPI entrypoint (routes, SSE, upload/lists, persistence
  wiring, graceful shutdown)
- `checker.py` — list parsing + health-check engine
- `store.py` — SQLite persistence (multi-list + migration)
- `geo.py` — mmdb download/cache + local geolocation lookups
- `broker.py` — SSE event broker (300 ms batched push)
- `static/index.html` — self-contained frontend (no external assets)
