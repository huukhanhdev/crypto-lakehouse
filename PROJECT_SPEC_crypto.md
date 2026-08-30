# Crypto Streaming Lakehouse — Build Spec

A spec for an agentic coding tool (Claude Code) to implement. A near-real-time
lakehouse over **Binance market data**: live trades and order-book depth streamed
in via websocket, captured through CDC, landed into Apache Iceberg Medallion
tiers, modeled with dbt, served through Trino to Superset.

This is a **portfolio project built and run by one person on a laptop.** Scope is
deliberately conservative for that constraint. Read §7 before adding anything.

The source is public Binance websocket data (no API key required, free, runs
24/7). This is the reason to choose crypto over forex/stocks: those have no free,
legal, always-on streaming source and no natural row-level-update stream, which
would gut the CDC and `MERGE INTO` parts of this project.

---

## 0. How to use this spec

- Build **phase by phase**. Each phase must run and be demonstrable before the
  next begins. Do not scaffold all services up front.
- After each phase, tick §9 and record any deviation in `docs/DECISIONS.md`.
- Read the existing repo (from the esports version, being repurposed):
  `docker-compose.yml`, `README.md`, `infra/`. The infra/CDC/Iceberg wiring
  carries over; only the **source layer** changes (websocket ingestor replaces
  the simulator, and the schema changes).
- **Simplification is allowed** where a component adds operational weight without
  teaching value — see §7. When you simplify, write why in `docs/DECISIONS.md`.
- Ask before: changing the schema after Phase 1 starts, swapping a core
  technology, or adding a service not in §2.

---

## 1. Goal and non-goals

**Goal.** A working lakehouse that demonstrates, end to end with evidence in the
repo: streaming ingestion of real market data, CDC into a lakehouse, row-level
upserts (`MERGE INTO`), a dimensional model with data-quality tests, and a live
dashboard — reproducible with `make up`.

**Non-goals.** Not a trading system. **No strategy, no backtesting, no signals,
no order execution.** This is a data-engineering project; the analytics are
descriptive (volume, spread, imbalance, OHLC), not predictive. Also: no HA, no
multi-node clusters, no Kubernetes, no cloud spend, no auth.

> Keeping trading logic out is deliberate — it keeps the project honestly a
> *data-engineering* portfolio piece and avoids the appearance of a
> get-rich bot. The DE skills are the point.

---

## 2. Target architecture

```
Binance WS ─▶ Ingestor ─▶ PostgreSQL ─(logical replication)─▶ Debezium ─▶ Redpanda
(trades +      (Python)     (OLTP)                                            │
 depth)                                                     Spark Structured Streaming
                                                                              │
                                          ┌───────────────────────────────────┤
                                          ▼                                   ▼
                                    Iceberg BRONZE                      Iceberg SILVER
                                 (raw CDC envelopes,                 (current state,
                                   append-only)                       MERGE INTO)
                                          │
                                          ▼
                                        dbt ─▶ Iceberg GOLD (star schema, tests)
                                                      │
                                      Nessie catalog ─┤─ MinIO (S3 storage)
                                                      ▼
                                                    Trino ─▶ Superset
                                                      ▲
                                    Dagster orchestrates Spark jobs + dbt
```

**Components and pinned choices**

| Concern | Choice | Notes |
|---|---|---|
| Market data | Binance websocket streams | `@trade` and `@depth` — public, keyless |
| Ingestor | Python (`websockets` + `psycopg2`) | replaces the esports generator |
| OLTP source | PostgreSQL 16 | CDC source of truth |
| CDC | Debezium 2.7 (pgoutput) | logical replication |
| Log/broker | Redpanda | Kafka-compatible, no ZK |
| Object store | MinIO | S3 API locally |
| Table format | Apache Iceberg | row-level updates are the reason |
| Catalog | Nessie | git-like branches for safe backfills |
| Stream compute | Spark Structured Streaming (PySpark) | Bronze + Silver |
| Transform | dbt-trino | Gold only |
| Query engine | Trino | serves Gold to BI |
| BI | Superset | dashboards |
| Orchestration | Dagster | assets for Spark + dbt |

**Allowed simplifications (see §7)** — you may drop Nessie, Dagster, or Spark for
lighter substitutes if RAM or wiring becomes the bottleneck. The non-negotiable
core is: **Binance WS → Postgres → Debezium → Redpanda → Iceberg (Bronze +
Silver with MERGE) → dbt Gold + tests → Trino → Superset.**

---

## 3. Source system — ingestor + schema (Phase 0, to build)

The esports generator is replaced by a **websocket ingestor**. It subscribes to
Binance combined streams for a small fixed symbol set and writes into Postgres,
which is the CDC source.

**Symbol set (fixed, small):** `BTCUSDT`, `ETHUSDT`, `SOLUSDT`. Three is enough
to show multi-symbol handling without flooding a laptop. Configurable via env.

**Binance streams to consume** (combined endpoint
`wss://stream.binance.com:9443/stream?streams=...`):
- `<symbol>@trade` — one message per trade. **Append-only, high volume.**
- `<symbol>@depth@100ms` — order-book diff updates (changed price levels).
  **This is the insert-then-update stream that forces `MERGE INTO`.**

> Note on order books: Binance sends *diffs*, not full snapshots. The ingestor
> keeps the mechanism honest — fetch an initial REST snapshot
> (`/api/v3/depth`), then apply diff updates, upserting each changed price level.
> A price level with quantity `0` means "remove this level" — treat as a delete.
> This is exactly the CDC pattern the lakehouse then mirrors downstream.

**Postgres schema (`infra/postgres/init/01_schema.sql`)**

```
-- Dimensions (slow-changing)
symbols(
  symbol_id PK, symbol, base_asset, quote_asset,
  status,              -- TRADING / BREAK / HALT  <- SCD2 driver
  tick_size, step_size,
  created_at, updated_at)          REPLICA IDENTITY FULL

exchanges(                         -- single row (Binance) for now; keeps the
  exchange_id PK, name, region,    -- dim modeling meaningful and extensible
  updated_at)                      REPLICA IDENTITY FULL

-- Facts
trades(                            -- APPEND-ONLY, high volume
  trade_id PK,                     -- Binance aggTrade/trade id
  symbol_id FK, price, quantity,
  quote_qty, is_buyer_maker,
  trade_time, ingested_at)         -- DEFAULT replica identity (WAL volume)

orderbook_levels(                  -- INSERT then repeated UPDATE  -> MERGE INTO
  symbol_id, side, price_level,    -- composite business key: (symbol, side, price)
  quantity,                        -- updated in place; 0 => level removed
  last_update_id, updated_at,
  PRIMARY KEY (symbol_id, side, price_level))
                                   REPLICA IDENTITY FULL
```

`REPLICA IDENTITY FULL` on dimensions and on `orderbook_levels` (need the full
before-image to detect which level/attribute changed); `trades` stays on default
to limit WAL. Publication `crypto_pub` covers all four tables.

**Ingestor (`ingestor/ingest.py`)** — async:
- One task per stream type. `@trade` → batch INSERT into `trades`.
- `@depth` → maintain a local book per symbol from the REST snapshot, apply
  diffs, and `INSERT ... ON CONFLICT (symbol_id, side, price_level) DO UPDATE`
  into `orderbook_levels`; delete levels that go to zero.
- Reconnect with backoff; on reconnect re-snapshot the book (Binance requires it).
- Seed `symbols` from `/api/v3/exchangeInfo` on startup; a periodic task flips a
  symbol's `status` occasionally if Binance reports it, giving the SCD2 dim real
  (if infrequent) movement.

> SCD2 caveat: crypto dimensions change less than an esports roster. `status`
> transitions are the honest SCD2 driver here. If they're too rare to demo,
> document a manual `UPDATE symbols SET status=...` step to show the SCD2 path —
> that's acceptable for a portfolio demo, just note it in the README.

---

## 4. Phase 1 — Storage, catalog, Bronze, Silver

**Deliverable.** CDC topics land into Iceberg. Bronze = append-only raw
envelopes; Silver = current state via `MERGE INTO`. Both queryable in Spark SQL.

**Services (compose profile `lake`):** `minio` (+ `mc` to create `warehouse`
bucket), `nessie` (or REST catalog — see §7), `spark` (single `spark-iceberg`
image preferred to save RAM).

**Bronze job (`spark/jobs/bronze_ingest.py`)**
- `readStream` each `crypto.public.*` topic from Redpanda.
- Parse Debezium JSON; keep `op`, `ts_ms`, `source.lsn`, Kafka
  `topic/partition/offset`, full `after` (+ `before` on updates).
- Append to `bronze.<table>` Iceberg tables, partitioned by ingestion date
  (and by symbol for the fact tables).
- Checkpoint to MinIO so restarts resume rather than replay.

**Silver job (`spark/jobs/silver_upsert.py`)**
- `trades` → light path: dedup on `trade_id`, append to `silver.trades`
  (append is correct here — trades are immutable). Still route through Silver for
  typing and schema stability.
- `orderbook_levels` → the core path: in `foreachBatch`, dedup to the latest per
  `(symbol, side, price_level)` by `last_update_id`, then
  `MERGE INTO silver.orderbook_levels` — update on match, insert on miss, and
  delete when quantity is 0. **This is the technical centrepiece; comment it
  well.** Silver holds the *current* book, one row per live price level.
- Dimensions (`symbols`, `exchanges`) → `MERGE INTO` current-state tables.

**Acceptance**
- `silver.orderbook_levels` count stays bounded (levels replaced, not appended)
  while `bronze.orderbook_levels` grows unbounded — the visible proof that MERGE
  works.
- A price level going to 0 removes exactly that row from Silver.
- Restarting the Spark job resumes from checkpoint with no Silver duplicates.

---

## 5. Phase 2 — Gold with dbt (star schema + tests)

**Deliverable.** `dbt-trino` project reading Silver, producing a dimensional
model in `gold` with tests passing.

**Models**
- Dimensions: `dim_symbol` (**SCD Type 2** on `status` via a dbt **snapshot**),
  `dim_exchange`, `dim_date`, `dim_time` (minute grain, for intraday charts).
- Facts:
  - `fct_trades` (grain: one trade) — price, qty, quote qty, maker/taker side.
  - `fct_ohlcv_1m` (grain: symbol × minute) — built from `fct_trades`: open,
    high, low, close, volume, trade count, VWAP. This is the analytically useful
    table and shows window/aggregate SQL.
  - `fct_book_snapshot` (grain: symbol × side × capture) — top-N levels, best
    bid/ask, spread, depth imbalance, derived from `silver.orderbook_levels`.

**Tests (required)**
- Generic: `unique` + `not_null` on keys; `relationships` facts→dims;
  `accepted_values` on `side`, `status`.
- Custom: OHLC sanity (`high >= low`, `high >= open`, `high >= close`, etc.);
  spread ≥ 0 in `fct_book_snapshot`; exactly one `is_current` per symbol in the
  SCD2 dim; no overlapping validity windows.

**Acceptance**
- `dbt build` clean; `dbt test` green.
- Flipping a symbol's `status` yields a second `dim_symbol` version with the old
  one closed.
- `dbt docs generate` produces a lineage graph.

---

## 6. Phase 3 — Serving, orchestration, CI

- **Trino** catalog on Nessie/Iceberg over MinIO; verify Gold is queryable.
- **Superset** (profile `bi`): one dashboard, ≥4 charts — 1m candlestick per
  symbol (from `fct_ohlcv_1m`), rolling volume, bid/ask spread over time,
  order-book depth imbalance. Export the dashboard to `superset/` so it's
  reproducible, not click-only.
- **Dagster** (profile `orchestrate`): Bronze/Silver Spark jobs and the dbt
  build as assets with dependencies; a sensor/schedule to run dbt as Silver
  advances. (Droppable — see §7.)
- **CI (`.github/workflows/ci.yml`)**: `ruff` + `black --check` (Python),
  `sqlfluff` (dbt), `dbt parse`/compile, `docker compose config` validation.
  Fast checks only, no full stack in CI.

**Acceptance**
- `make up` (all profiles) brings source → dashboard up with no manual steps
  beyond `make`.
- CI green on a clean checkout.

---

## 7. Scope guardrails & allowed simplifications

**Left out — do not add without asking:** trading strategy / backtesting /
signals, Kafka+ZK, Schema Registry/Avro, Kubernetes, Terraform, cloud, auth,
more than one Spark worker, more than ~3 symbols.

**You MAY simplify these if wiring or RAM hurts** (record the swap in
`docs/DECISIONS.md`):
- **Nessie → Iceberg REST catalog or Hadoop catalog.** Nessie's branching is
  nice-to-have; if version conflicts with the Spark/Iceberg jars eat time, a
  simpler catalog is acceptable. Mention it as a known trade-off.
- **Dagster → a Makefile target or a small Python scheduler.** Orchestration is
  the least load-bearing piece for demonstrating DE fundamentals here. Keep the
  asset-graph story if it's cheap; drop it if it's not.
- **Spark Structured Streaming → Spark micro-batch reading from Redpanda on a
  timer**, if continuous streaming proves fragile on the laptop. The `MERGE
  INTO` logic is identical either way — that's what matters.

**Do not simplify away:** CDC (Debezium), Iceberg with a real `MERGE INTO` on the
order book, the dbt Gold layer with tests, and Trino→Superset serving. Those four
are the portfolio's evidence. If the project must shrink, shrink breadth (fewer
symbols, drop `fct_book_snapshot`) not those pillars.

Order-book path (`MERGE INTO`) and the SCD2 `dim_symbol` are the two things that
must reach Gold. Everything else is negotiable.

RAM: keep each profile ~6 GB; only require all profiles up for the final demo.

---

## 8. Repo layout (target)

```
crypto-lakehouse/
├── docker-compose.yml       # profiles: (default) source+cdc, lake, bi, orchestrate
├── Makefile
├── README.md
├── PROJECT_SPEC.md          # this file
├── docs/{DECISIONS.md,architecture.md}
├── infra/
│   ├── postgres/init/       # schema + publication
│   ├── debezium/            # connector config + register.sh
│   ├── minio/  nessie/  trino/
├── ingestor/                # Binance websocket -> Postgres  (replaces generator)
│   ├── ingest.py  requirements.txt  Dockerfile
├── spark/
│   ├── conf/                # shared Iceberg/Nessie/S3A config
│   └── jobs/{bronze_ingest.py,silver_upsert.py}
├── dbt/{models/{staging,marts}/,snapshots/,tests/}
├── dagster/defs/
├── superset/
└── .github/workflows/ci.yml
```

## 9. Progress checklist

- [x] Phase 0 — Binance ingestor, Postgres schema, Debezium, Redpanda
- [ ] Phase 1 — MinIO, catalog, Spark, Bronze, Silver (`MERGE INTO` on the book)
- [ ] Phase 2 — dbt Gold, star schema, `fct_ohlcv_1m`, SCD2, tests
- [ ] Phase 3 — Trino, Superset, (Dagster), CI

## 10. Definition of done (portfolio bar)

- `git clone` → `make up` → live Binance data reaches a Superset candlestick
  chart on a fresh machine, following only the README.
- README has the architecture diagram, the "why crypto / why these choices"
  notes, and per-phase run instructions.
- `dbt test` green; the `MERGE INTO` and SCD2 behaviours are demonstrable with a
  documented step.
- CI badge green.
- `docs/DECISIONS.md` explains every deviation and every simplification taken.

---

## Appendix — Binance reference (no key required)

- Combined WS: `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth@100ms/...`
- Depth snapshot REST: `GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000`
- Exchange info: `GET https://api.binance.com/api/v3/exchangeInfo`
- Order-book maintenance rules (snapshot + diff buffering, `U`/`u` update-id
  bookkeeping): Binance "How to manage a local order book correctly" — the
  ingestor must follow it or the book drifts. Implement per those rules; don't
  improvise.
- Rate limits apply to REST snapshots, not the WS stream. Snapshot on start and
  on reconnect only.
