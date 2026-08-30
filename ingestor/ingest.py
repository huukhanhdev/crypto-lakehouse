"""Binance websocket ingestor -> PostgreSQL (Phase 0 source system).

Subscribes to Binance combined streams for a small fixed symbol set and writes
into Postgres, which is the CDC source of truth for the lakehouse.

Two stream types, two write patterns (this asymmetry is the whole point):

  @trade          append-only, high volume  -> INSERT ... ON CONFLICT DO NOTHING
  @depth@100ms    order-book diffs           -> INSERT ... ON CONFLICT DO UPDATE
                                                (and DELETE when a level empties)

The depth path is the insert-then-update-then-delete stream that forces
`MERGE INTO` downstream in Silver. It follows Binance's "How to manage a local
order book correctly" algorithm: fetch a REST snapshot, then apply diffs with
strict update-id bookkeeping, re-snapshotting on any gap.

See PROJECT_SPEC §3 and the Binance appendix.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from decimal import Decimal

import httpx
import psycopg2
import psycopg2.extras
import websockets

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]

WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443")
REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com")

# How deep to persist the book. We keep the *full* book in memory (needed for a
# correct best bid/ask and to know which levels leave the top), but only persist
# the top-N levels per side to Postgres. This bounds row count and write rate on
# a laptop while still exercising insert/update/delete — see docs/DECISIONS.md.
TRACKED_DEPTH = int(os.getenv("TRACKED_DEPTH", "20"))
SNAPSHOT_LIMIT = int(os.getenv("SNAPSHOT_LIMIT", "1000"))  # REST snapshot depth

TRADE_BATCH_MAX = int(os.getenv("TRADE_BATCH_MAX", "200"))      # flush after N trades
TRADE_BATCH_SECS = float(os.getenv("TRADE_BATCH_SECS", "1.0"))  # or after this long

# Periodic refresh of symbol metadata from exchangeInfo — the honest SCD2 driver
# (status/tick/step changes flow through as UPDATEs). See PROJECT_SPEC §3 caveat.
REFRESH_SECS = int(os.getenv("REFRESH_SECS", "3600"))
# Optional synthetic status flip so the SCD2 path is demonstrable even when
# Binance reports no real status change. 0 = disabled (default). Documented as a
# demo aid in the README.
DEMO_FLIP_SECS = int(os.getenv("DEMO_FLIP_SECS", "0"))

PG = dict(
    host=os.getenv("PGHOST", "postgres"),
    port=int(os.getenv("PGPORT", "5432")),
    dbname=os.getenv("PGDATABASE", "crypto"),
    user=os.getenv("PGUSER", "crypto"),
    password=os.getenv("PGPASSWORD", "crypto"),
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("ingestor")


class Resync(Exception):
    """Raised when the depth diff stream has a gap and the book must re-snapshot."""


# ---------------------------------------------------------------------------
# DB helpers (psycopg2 is blocking -> run every call in a worker thread)
# ---------------------------------------------------------------------------
def connect():
    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    return conn


async def db(fn, *args):
    """Run a blocking DB function in a thread so it doesn't stall the event loop."""
    return await asyncio.to_thread(fn, *args)


def _seed_symbols(rows):
    """Upsert symbol metadata; return {symbol: symbol_id}."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO symbols (symbol, base_asset, quote_asset, status, tick_size, step_size, updated_at)
                    VALUES (%(symbol)s, %(base)s, %(quote)s, %(status)s, %(tick)s, %(step)s, now())
                    ON CONFLICT (symbol) DO UPDATE SET
                        base_asset  = EXCLUDED.base_asset,
                        quote_asset = EXCLUDED.quote_asset,
                        status      = EXCLUDED.status,
                        tick_size   = EXCLUDED.tick_size,
                        step_size   = EXCLUDED.step_size,
                        updated_at  = now()
                    WHERE  symbols.status    IS DISTINCT FROM EXCLUDED.status
                        OR symbols.tick_size IS DISTINCT FROM EXCLUDED.tick_size
                        OR symbols.step_size IS DISTINCT FROM EXCLUDED.step_size
                    """,
                    r,
                )
            cur.execute("SELECT symbol, symbol_id FROM symbols")
            mapping = {s: sid for s, sid in cur.fetchall()}
        conn.commit()
        return mapping
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Symbol seeding + periodic refresh (SCD2 driver)
# ---------------------------------------------------------------------------
async def fetch_exchange_info(client: httpx.AsyncClient):
    params = {"symbols": json.dumps(SYMBOLS, separators=(",", ":"))}
    resp = await client.get(f"{REST_BASE}/api/v3/exchangeInfo", params=params)
    resp.raise_for_status()
    rows = []
    for s in resp.json()["symbols"]:
        tick = step = None
        for f in s.get("filters", []):
            if f["filterType"] == "PRICE_FILTER":
                tick = f["tickSize"]
            elif f["filterType"] == "LOT_SIZE":
                step = f["stepSize"]
        rows.append(
            dict(
                symbol=s["symbol"],
                base=s["baseAsset"],
                quote=s["quoteAsset"],
                status=s["status"],
                tick=tick,
                step=step,
            )
        )
    return rows


async def refresh_task(client: httpx.AsyncClient):
    """Periodically re-seed from exchangeInfo so real status/tick/step changes
    flow through as UPDATEs (the SCD2 dim's real, if infrequent, movement)."""
    while True:
        await asyncio.sleep(REFRESH_SECS)
        try:
            rows = await fetch_exchange_info(client)
            await db(_seed_symbols, rows)
            log.info("refreshed symbol metadata from exchangeInfo")
        except Exception as e:  # noqa: BLE001 - keep the loop alive
            log.warning("refresh failed: %s", e)


def _flip_status(symbol):
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE symbols
                   SET status = CASE WHEN status = 'TRADING' THEN 'BREAK' ELSE 'TRADING' END,
                       updated_at = now()
                 WHERE symbol = %s
                RETURNING status
                """,
                (symbol,),
            )
            new = cur.fetchone()
        conn.commit()
        return new[0] if new else None
    finally:
        conn.close()


async def demo_flip_task():
    """Optional: toggle one symbol's status on a timer to guarantee SCD2 movement
    for a demo. Disabled unless DEMO_FLIP_SECS > 0."""
    if DEMO_FLIP_SECS <= 0:
        return
    target = SYMBOLS[0]
    while True:
        await asyncio.sleep(DEMO_FLIP_SECS)
        try:
            new = await db(_flip_status, target)
            log.info("DEMO status flip: %s -> %s", target, new)
        except Exception as e:  # noqa: BLE001
            log.warning("demo flip failed: %s", e)


# ---------------------------------------------------------------------------
# @trade stream -> trades (append-only)
# ---------------------------------------------------------------------------
def _insert_trades(batch):
    conn = connect()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO trades
                    (trade_id, symbol_id, price, quantity, quote_qty, is_buyer_maker, trade_time, ingested_at)
                VALUES %s
                ON CONFLICT (symbol_id, trade_id) DO NOTHING
                """,
                batch,
                template="(%s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000.0), now())",
            )
        conn.commit()
    finally:
        conn.close()


async def trade_stream(symbol_ids: dict):
    """One combined WS connection for all @trade streams; batch-insert trades."""
    streams = "/".join(f"{s.lower()}@trade" for s in SYMBOLS)
    url = f"{WS_BASE}/stream?streams={streams}"
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=180, max_queue=None) as ws:
                log.info("trade stream connected (%d symbols)", len(SYMBOLS))
                backoff = 1
                batch = []
                last_flush = asyncio.get_event_loop().time()
                async for msg in ws:
                    d = json.loads(msg)["data"]
                    sid = symbol_ids.get(d["s"])
                    if sid is None:
                        continue
                    price = Decimal(d["p"])
                    qty = Decimal(d["q"])
                    batch.append(
                        (d["t"], sid, price, qty, price * qty, d["m"], d["T"])
                    )
                    now = asyncio.get_event_loop().time()
                    if len(batch) >= TRADE_BATCH_MAX or (now - last_flush) >= TRADE_BATCH_SECS:
                        await db(_insert_trades, batch)
                        log.debug("flushed %d trades", len(batch))
                        batch = []
                        last_flush = now
        except Exception as e:  # noqa: BLE001
            log.warning("trade stream error (%s); reconnecting in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ---------------------------------------------------------------------------
# @depth stream -> orderbook_levels (insert / update / delete -> MERGE INTO)
# ---------------------------------------------------------------------------
class OrderBook:
    """In-memory book for one symbol. Full depth kept for correctness; only the
    top-N per side is persisted to Postgres (bounds write rate on a laptop)."""

    def __init__(self, symbol, symbol_id):
        self.symbol = symbol
        self.symbol_id = symbol_id
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        # what we currently have persisted: {(side, price): qty}
        self.persisted: dict[tuple[str, Decimal], Decimal] = {}

    def load_snapshot(self, snap):
        self.bids = {Decimal(p): Decimal(q) for p, q in snap["bids"] if Decimal(q) > 0}
        self.asks = {Decimal(p): Decimal(q) for p, q in snap["asks"] if Decimal(q) > 0}

    def apply_diff(self, ev):
        for p, q in ev["b"]:
            price, qty = Decimal(p), Decimal(q)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for p, q in ev["a"]:
            price, qty = Decimal(p), Decimal(q)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    def top_n(self):
        """Return the desired persisted set {(side, price): qty} for top-N."""
        top = {}
        for price in sorted(self.bids, reverse=True)[:TRACKED_DEPTH]:
            top[("bid", price)] = self.bids[price]
        for price in sorted(self.asks)[:TRACKED_DEPTH]:
            top[("ask", price)] = self.asks[price]
        return top

    def diff_persisted(self):
        """Compute (upserts, deletes) against what's currently in Postgres, then
        adopt the new set as persisted state."""
        target = self.top_n()
        upserts = [
            (side, price, qty)
            for (side, price), qty in target.items()
            if self.persisted.get((side, price)) != qty
        ]
        deletes = [key for key in self.persisted if key not in target]
        self.persisted = target
        return upserts, deletes


def _write_book(symbol_id, last_update_id, upserts, deletes):
    conn = connect()
    try:
        with conn.cursor() as cur:
            if upserts:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO orderbook_levels
                        (symbol_id, side, price_level, quantity, last_update_id, updated_at)
                    VALUES %s
                    ON CONFLICT (symbol_id, side, price_level) DO UPDATE SET
                        quantity       = EXCLUDED.quantity,
                        last_update_id = EXCLUDED.last_update_id,
                        updated_at     = EXCLUDED.updated_at
                    """,
                    [(symbol_id, side, price, qty, last_update_id) for side, price, qty in upserts],
                    template="(%s, %s, %s, %s, %s, now())",
                )
            if deletes:
                cur.executemany(
                    "DELETE FROM orderbook_levels WHERE symbol_id=%s AND side=%s AND price_level=%s",
                    [(symbol_id, side, price) for side, price in deletes],
                )
        conn.commit()
    finally:
        conn.close()


async def depth_stream(symbol, symbol_id, client: httpx.AsyncClient):
    """Maintain one symbol's local book per Binance's algorithm, persisting the
    top-N to Postgres. Re-snapshots on gaps or reconnects."""
    url = f"{WS_BASE}/ws/{symbol.lower()}@depth@100ms"
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=180, max_queue=None) as ws:
                log.info("[%s] depth stream connected", symbol)
                backoff = 1
                book = OrderBook(symbol, symbol_id)

                # 1) fetch REST snapshot (events arriving meanwhile are buffered
                #    by the websockets lib until we start reading).
                resp = await client.get(
                    f"{REST_BASE}/api/v3/depth", params={"symbol": symbol, "limit": SNAPSHOT_LIMIT}
                )
                resp.raise_for_status()
                snap = resp.json()
                last_update_id = snap["lastUpdateId"]
                book.load_snapshot(snap)

                # persist the snapshot's top-N as the initial book state
                up, dl = book.diff_persisted()
                await db(_write_book, symbol_id, last_update_id, up, dl)
                log.info("[%s] snapshot loaded (lastUpdateId=%s, %d/%d levels)",
                         symbol, last_update_id, len(book.bids), len(book.asks))

                prev_u = None
                async for msg in ws:
                    ev = json.loads(msg)
                    # 2) drop stale events fully covered by the snapshot
                    if ev["u"] <= last_update_id:
                        continue
                    # 3) first applied event must straddle lastUpdateId+1
                    if prev_u is None:
                        if not (ev["U"] <= last_update_id + 1 <= ev["u"]):
                            raise Resync(f"first event out of range U={ev['U']} u={ev['u']}")
                    # 4) subsequent events must be contiguous
                    elif ev["U"] != prev_u + 1:
                        raise Resync(f"gap: expected U={prev_u + 1} got U={ev['U']}")

                    book.apply_diff(ev)
                    up, dl = book.diff_persisted()
                    if up or dl:
                        await db(_write_book, symbol_id, ev["u"], up, dl)
                    prev_u = ev["u"]
        except Resync as e:
            log.info("[%s] resync: %s", symbol, e)
            continue  # reconnect immediately and re-snapshot
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] depth error (%s); reconnecting in %ds", symbol, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    log.info("ingestor starting for symbols: %s", ", ".join(SYMBOLS))

    async with httpx.AsyncClient(timeout=20.0) as client:
        # wait for Postgres, then seed symbols before streaming
        rows = None
        for attempt in range(60):
            try:
                rows = await fetch_exchange_info(client)
                break
            except Exception as e:  # noqa: BLE001
                log.info("waiting on exchangeInfo (%s)...", e)
                await asyncio.sleep(2)
        if rows is None:
            raise SystemExit("could not reach Binance exchangeInfo")

        symbol_ids = None
        for attempt in range(60):
            try:
                symbol_ids = await db(_seed_symbols, rows)
                break
            except Exception as e:  # noqa: BLE001
                log.info("waiting on postgres (%s)...", e)
                await asyncio.sleep(2)
        if symbol_ids is None:
            raise SystemExit("could not seed symbols into postgres")

        log.info("seeded symbols: %s", symbol_ids)

        tasks = [
            asyncio.create_task(trade_stream(symbol_ids)),
            asyncio.create_task(refresh_task(client)),
            asyncio.create_task(demo_flip_task()),
        ]
        for sym in SYMBOLS:
            tasks.append(asyncio.create_task(depth_stream(sym, symbol_ids[sym], client)))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop = loop.create_future()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: stop.cancel())
        except NotImplementedError:  # pragma: no cover - windows
            pass
    try:
        loop.run_until_complete(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("shutting down")
    finally:
        loop.close()
