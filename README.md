# Crypto Streaming Lakehouse

A near-real-time lakehouse over **Binance market data**: live trades and
order-book depth streamed in via websocket, captured through CDC, landed into
Apache Iceberg Medallion tiers, modeled with dbt, served through Trino to
Superset.

Portfolio project, built to run on a single laptop. Full plan in
[`PROJECT_SPEC_crypto.md`](PROJECT_SPEC_crypto.md); every deviation is recorded
in [`docs/DECISIONS.md`](docs/DECISIONS.md).

> **Why crypto?** Binance's public websocket feed is free, keyless, always-on,
> and — crucially — its order-book `@depth` stream is a genuine row-level
> *update* stream. That's what makes the CDC and `MERGE INTO` parts of this
> project honest. Forex/stocks have no such free, legal, always-on source.

## Architecture

```
Binance WS ─▶ Ingestor ─▶ PostgreSQL ─(logical replication)─▶ Debezium ─▶ Redpanda
(trades +     (Python)      (OLTP)                                          │
 depth)                                                   Spark Structured Streaming
                                                                            │
                                        ┌───────────────────────────────────┤
                                        ▼                                    ▼
                                  Iceberg BRONZE                       Iceberg SILVER
                               (raw CDC, append)                   (current state, MERGE)
                                        │
                                        ▼
                                      dbt ─▶ Iceberg GOLD (star schema + tests)
                                                    │
                                                  Trino ─▶ Superset
```

## Status

- [x] **Phase 0** — Binance ingestor, Postgres schema, Debezium, Redpanda
- [x] **Phase 1** — MinIO, catalog, Spark, Bronze, Silver (`MERGE INTO`)
- [x] **Phase 2** — Trino + dbt Gold, star schema, `fct_ohlcv_1m`, SCD2, tests
- [ ] Phase 3 — Superset, (Dagster), CI

---

## Phase 0 — quickstart

Requires Docker + Docker Compose. From the repo root:

```bash
make up        # build + start postgres, redpanda, connect, ingestor, console
               # then auto-register the Debezium connector
```

Give it ~30–60s (Connect takes a moment to come up). Then verify the pipeline:

```bash
make status    # Debezium connector should report RUNNING
make psql      # then: SELECT count(*) FROM trades;  SELECT * FROM orderbook_levels LIMIT 10;
make topics    # lists crypto.public.* topics
make trades    # tail the latest trade CDC messages
make book      # tail the latest order-book CDC messages (insert/update/delete)
```

Or open **Redpanda Console** at <http://localhost:8080> to browse topics,
messages, and connector status in a UI.

### What "working" looks like

- `trades` grows continuously; `crypto.public.trades` fills with `op:"c"` events.
- `orderbook_levels` stays **bounded** (top-N per side per symbol) while the
  `crypto.public.orderbook_levels` topic shows a mix of `op:"c"` (insert),
  `op:"u"` (update), and `op:"d"` (delete) — the CDC pattern Silver will mirror
  with `MERGE INTO`.

### Demonstrating the SCD2 path

`symbols.status` drives the SCD2 dimension later. Real Binance status changes are
rare, so either:

- set `DEMO_FLIP_SECS` on the `ingestor` service (see `docker-compose.yml`) to
  toggle a symbol's status on a timer, **or**
- flip it manually and watch the CDC update:

  ```sql
  UPDATE symbols SET status = 'BREAK', updated_at = now() WHERE symbol = 'BTCUSDT';
  ```

### Config knobs (ingestor env)

| Env | Default | Meaning |
|---|---|---|
| `SYMBOLS` | `BTCUSDT,ETHUSDT,SOLUSDT` | symbol set (keep ≤3, see spec §7) |
| `TRACKED_DEPTH` | `20` | order-book levels persisted per side (see D0.2) |
| `SNAPSHOT_LIMIT` | `1000` | REST depth snapshot size |
| `DEMO_FLIP_SECS` | `0` (off) | synthetic SCD2 status-flip interval |

### Teardown

```bash
make down      # stop, keep data
make clean     # stop and wipe volumes (fresh start)
```

---

## Phase 1 — quickstart

Phase 1 lands the CDC stream into Apache Iceberg via Spark Structured Streaming.
It runs as the `lake` Compose profile (MinIO as the S3 object store, an Iceberg
REST catalog, and a single local-mode Spark driver hosting both streams).

With the Phase 0 stack already up (`make up`), start the lakehouse:

```bash
make lake        # start MinIO, the REST catalog, and Spark (Bronze + Silver)
make lake-logs   # follow the Spark driver as it builds tables and streams
```

Give Spark a minute on first run — it downloads the Kafka connector into the
`ivy` cache (cached across restarts). Streaming checkpoints live in the
`checkpoints` volume, so a restart resumes where it left off.

### What "working" looks like

- Spark logs print `>>> namespaces + tables ready` then `>>> streaming started`.
- **Bronze** (`lake.bronze.*`) grows continuously — one append-only table per
  source table, holding the verbatim Debezium envelopes.
- **Silver** (`lake.silver.*`) is the cleaned, typed, *current-state* tier built
  with `MERGE INTO`: `trades` is insert-only (dedup on `(symbol_id, trade_id)`),
  while `orderbook_levels` stays **bounded** — MERGE UPDATEs changed levels,
  INSERTs new ones, and DELETEs a level when it empties (`op='d'` or qty 0).
- Browse the data files in the **MinIO console** at <http://localhost:9001>
  (`minioadmin` / `minioadmin`), bucket `warehouse`.

### Teardown

```bash
make lake-down   # stop just the lake profile (keep Iceberg data in MinIO)
```

`make clean` still wipes **all** volumes — including the Iceberg warehouse and
Spark checkpoints — for a fully fresh start.

---

## Phase 2 — quickstart

Phase 2 builds the **Gold** star schema with **dbt-trino**. Trino queries the
*same* Iceberg REST catalog + MinIO that Phase 1 writes, so dbt reads the Silver
tables directly and materialises dimensional models into the `gold` namespace.
It runs as the `gold` Compose profile (MinIO, the REST catalog, and Trino).

Phase 2 only needs the Silver tables to exist in MinIO (from a `make lake` run);
the streaming stack does **not** have to be running.

```bash
make gold        # start Trino, then `dbt build` (snapshot + models + tests)
```

> **One-time repair.** Silver was first written with a timestamp-parsing bug (the
> Debezium `timestamptz` columns are ISO-8601 strings, not epoch millis — see
> DECISIONS D2.5). The Spark code is fixed; to backfill data already in MinIO,
> run `make gold-repair` once (rebuilds `silver.trades` from Bronze via Trino, no
> Spark needed) before `make gold`.

### The model

- **Dimensions** — `dim_symbol` (**SCD2** on `status` via a dbt snapshot,
  exposing `valid_from`/`valid_to`/`is_current`), `dim_exchange`, `dim_date`,
  `dim_time` (minute grain).
- **Facts** — `fct_trades` (one row per trade), `fct_ohlcv_1m` (symbol × minute
  OHLCV rolled up from trades), `fct_book_snapshot` (current top-of-book per
  symbol: best bid/ask + non-negative spread).
- **Tests** — spread ≥ 0; exactly one `is_current` per symbol; no overlapping
  SCD2 validity windows; unique `(symbol_id, trade_id)` and `(symbol, minute)`;
  plus the usual not-null/unique/accepted-values checks. `dbt build` is green.

### What "working" looks like

- `make gold` ends with `Done. PASS=30 ... ERROR=0`.
- Query Gold through Trino (host port **8085**), e.g. with any Trino client:

  ```sql
  SELECT symbol_id, best_bid, best_ask, spread FROM iceberg.gold.fct_book_snapshot;
  SELECT * FROM iceberg.gold.fct_ohlcv_1m WHERE symbol_id = 1 ORDER BY bar_minute DESC LIMIT 5;
  ```

### Demonstrating SCD2

With the full stack running, flip a symbol's status (see Phase 0), let Silver
apply it, then re-run the snapshot — `dim_symbol` gains a second version and the
old row's `is_current` flips to false:

```bash
make gold-build   # re-runs `dbt snapshot` as part of build
```

### Teardown

```bash
make gold-down    # stop Trino + catalog (keep Iceberg data in MinIO)
```

> **Note.** The demo REST catalog (SQLite) can occasionally wedge its WAL and
> return `SQLITE_BUSY_SNAPSHOT` on commit. If a build fails that way, restart it
> with `docker compose --profile gold restart rest` and re-run `make gold`.
