# Decisions & Deviations

Per PROJECT_SPEC §0/§7, every deviation and simplification is recorded here.

## Phase 0 — source system + CDC

### D0.1 — Built from scratch (no esports repo carried over)
The spec assumes the esports repo is present to repurpose its infra/CDC/Iceberg
wiring. That repo was not in the working directory, so Phase 0 was written fresh
following §3. Nothing was lost — the source layer changes anyway; only infra
boilerplate had to be re-authored rather than edited.

### D0.2 — Order book persists only the top-N levels per side (`TRACKED_DEPTH`)
Binance's `@depth@100ms` diff stream reports changes across the *entire* book,
which for BTCUSDT is hundreds of level updates per second. Persisting the full
book for 3 symbols would swamp a laptop's Postgres/WAL/CDC pipeline.

The ingestor keeps the **full book in memory** (needed for a correct best
bid/ask and to know which levels leave the top) but persists only the top-N
levels per side (`TRACKED_DEPTH`, default 20). This bounds `orderbook_levels`
row count and write rate while still exercising the full **insert / update /
delete** pattern that forces `MERGE INTO` downstream:
- a level's quantity changing → UPDATE,
- a new level entering the top-N → INSERT,
- a level going to qty 0 **or** falling out of the top-N → DELETE.

Trade-off: `fct_book_snapshot` downstream sees top-N depth, not the full book —
which is exactly what the spread / imbalance charts need anyway. Raise
`TRACKED_DEPTH` (env) if a deeper book is wanted and RAM allows.

### D0.3 — `trades` primary key is composite `(symbol_id, trade_id)`
Binance trade ids are unique per symbol, not globally. The spec lists
`trade_id PK`; using the composite key keeps ids correct across symbols. WAL
stays light because `trades` still uses the DEFAULT replica identity (PK only).

### D0.4 — Debezium `decimal.handling.mode=string`
Prices/quantities are Postgres `NUMERIC`. Default Debezium encoding is
base64 bytes, awkward to read and to parse in Spark. `string` mode emits exact
decimal strings — human-readable in Redpanda Console and lossless for Bronze.

### D0.5 — Redpanda Console added (demo aid)
Not in the spec's component list, but it is a zero-config read-only UI for
browsing topics/messages/connector status — it makes Phase 0 "demonstrable"
(§0) without adding pipeline weight. Droppable.

### D0.6 — SCD2 driver
`symbols.status` is the honest SCD2 driver. A periodic `exchangeInfo` refresh
(`REFRESH_SECS`) flows real status/tick/step changes through as UPDATEs. Because
real status changes are rare, an optional synthetic flipper (`DEMO_FLIP_SECS`,
default off) can toggle one symbol's status on a timer to make the SCD2 path
demonstrable on demand — as the spec's §3 caveat explicitly permits.

## Phase 1 — object store, catalog, Spark Bronze/Silver

### D1.1 — One SparkSession hosts both Bronze and Silver (local mode)
The spec allows separate jobs. Running both streams under a single local-mode
driver keeps the laptop to **one JVM** instead of two, which matters on a
memory-tight machine. `main.py` builds the session, creates the
namespaces/tables, starts both queries, and blocks on `awaitAnyTermination()`.

Trade-off: the two streams share the driver's resources and a failure in either
tears down the process (restarted by Compose). Acceptable at this scale; the
tiers stay logically independent and could be split into two containers later.

### D1.2 — Iceberg REST catalog backed by SQLite, not a JDBC/Hive metastore
The spec names an Iceberg catalog without mandating a backend. `tabulario/iceberg-rest`
over a local SQLite file is the lightest thing that still exercises a real REST
catalog. To avoid `SQLITE_BUSY` (HTTP 500) when the Bronze and Silver streams
commit concurrently, the JDBC URI sets `journal_mode=WAL&busy_timeout=30000` so
writers wait for the single-writer lock instead of erroring.

### D1.3 — MinIO as the S3 object store; `S3FileIO` (no Hadoop S3A)
MinIO gives a real S3 API on the laptop. Iceberg's native `S3FileIO` (bundled in
the `tabulario/spark-iceberg` image) is used instead of Hadoop's `s3a://` — fewer
jars, path-style access, and no Hadoop-AWS version juggling. A one-shot `mc`
service creates the `warehouse` bucket on startup.

### D1.4 — Bronze is at-least-once and append-only
Bronze writes via `foreachBatch` + `.append()`, so a rare batch retry can
duplicate raw rows. That is accepted for an immutable raw-landing tier: Bronze is
the replayable log, and **Silver** is the deduped/current tier (its keyed
`MERGE INTO` is idempotent, so restarts never double-apply). Empty per-table
slices are skipped to avoid wasteful Iceberg snapshots.

### D1.5 — after/before kept as raw JSON strings in Bronze
The Debezium envelope's `after`/`before` are stored verbatim as JSON strings in
Bronze and parsed per-table (with explicit schemas) only in Silver. This keeps
Bronze a uniform, source-agnostic envelope and defers typing to where it's used —
so a schema change in a source table can't break the Bronze landing.

### D1.6 — Laptop memory caps on Redpanda and Connect
Redpanda gets `--memory=1200M` / `mem_limit: 1600m` and Connect a
`KAFKA_HEAP_OPTS` cap, because Redpanda's default arena OOM-killed the VM before.
Not a spec requirement — a single-laptop survival tweak, recorded per §7.
