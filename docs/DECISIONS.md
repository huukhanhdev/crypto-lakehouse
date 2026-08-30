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
