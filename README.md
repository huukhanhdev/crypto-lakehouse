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
- [ ] Phase 2 — dbt Gold, star schema, `fct_ohlcv_1m`, SCD2, tests
- [ ] Phase 3 — Trino, Superset, (Dagster), CI

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
