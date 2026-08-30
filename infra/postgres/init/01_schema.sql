-- Crypto Streaming Lakehouse — OLTP source schema (Phase 0)
-- This is the CDC source of truth. Debezium (pgoutput) captures changes here
-- via logical replication and streams them to Redpanda.
--
-- REPLICA IDENTITY notes (see PROJECT_SPEC §3):
--   * Dimensions + orderbook_levels use FULL so the CDC "before" image carries
--     every column — needed to detect which level/attribute changed and to
--     reconstruct deletes downstream.
--   * trades stays on DEFAULT (PK only) to keep WAL volume down; trades are
--     append-only and immutable, so the before-image adds nothing.

-- ---------------------------------------------------------------------------
-- Dimensions (slow-changing)
-- ---------------------------------------------------------------------------

CREATE TABLE symbols (
    symbol_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol       TEXT        NOT NULL UNIQUE,       -- e.g. BTCUSDT
    base_asset   TEXT        NOT NULL,              -- e.g. BTC
    quote_asset  TEXT        NOT NULL,              -- e.g. USDT
    status       TEXT        NOT NULL DEFAULT 'TRADING',  -- TRADING / BREAK / HALT  <- SCD2 driver
    tick_size    NUMERIC(38, 18),                   -- min price increment
    step_size    NUMERIC(38, 18),                   -- min quantity increment
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE symbols REPLICA IDENTITY FULL;

CREATE TABLE exchanges (
    exchange_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         TEXT        NOT NULL UNIQUE,
    region       TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE exchanges REPLICA IDENTITY FULL;

-- Single row for now; keeps the dimensional model meaningful and extensible.
INSERT INTO exchanges (name, region) VALUES ('Binance', 'Global');

-- ---------------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------------

-- APPEND-ONLY, high volume. One row per trade off the @trade stream.
CREATE TABLE trades (
    trade_id        BIGINT      NOT NULL,           -- Binance trade id (unique per symbol)
    symbol_id       BIGINT      NOT NULL REFERENCES symbols (symbol_id),
    price           NUMERIC(38, 18) NOT NULL,
    quantity        NUMERIC(38, 18) NOT NULL,
    quote_qty       NUMERIC(38, 18) NOT NULL,       -- price * quantity
    is_buyer_maker  BOOLEAN     NOT NULL,           -- true => taker sold (aggressive sell)
    trade_time      TIMESTAMPTZ NOT NULL,           -- exchange event time
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol_id, trade_id)               -- trade_id only unique within a symbol
);
-- DEFAULT replica identity (PK only) — keep WAL light for the high-volume table.

-- INSERT then repeated UPDATE in place -> forces MERGE INTO downstream.
-- Composite business key: (symbol, side, price). quantity updated in place;
-- a level going to 0 is deleted here and must delete downstream too.
CREATE TABLE orderbook_levels (
    symbol_id       BIGINT      NOT NULL REFERENCES symbols (symbol_id),
    side            TEXT        NOT NULL CHECK (side IN ('bid', 'ask')),
    price_level     NUMERIC(38, 18) NOT NULL,
    quantity        NUMERIC(38, 18) NOT NULL,
    last_update_id  BIGINT      NOT NULL,           -- Binance depth update id (u)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol_id, side, price_level)
);
ALTER TABLE orderbook_levels REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- Logical replication publication for Debezium (pgoutput)
-- ---------------------------------------------------------------------------
CREATE PUBLICATION crypto_pub
    FOR TABLE symbols, exchanges, trades, orderbook_levels;
