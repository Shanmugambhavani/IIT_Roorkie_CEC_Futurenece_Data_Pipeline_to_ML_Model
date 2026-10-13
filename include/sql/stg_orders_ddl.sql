-- NammaMart ELT staging: orders table definition.
--
-- Declared explicitly rather than with CREATE TABLE AS SELECT, for two reasons.
-- First, order_id needs a PRIMARY KEY so the incremental load can be a genuine
-- upsert (INSERT OR REPLACE) rather than a delete-then-insert dance. Second, this
-- DDL doubles as the data dictionary for Part E2: the column, its type and its
-- meaning sit in one place that cannot drift from the table.
--
-- Facts are rejected, not repaired. Unlike the dimensions, an order with an
-- impossible amount or a store that does not exist has no defensible value, so it
-- is marked QUARANTINE and never reaches a mart.

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id         VARCHAR       PRIMARY KEY,  -- business key, one row per order
    order_ts         TIMESTAMP,                  -- when the order was placed
    order_date       DATE,                       -- derived, the grain of daily reporting
    customer_id      VARCHAR,                    -- FK to stg_customers
    store_id         VARCHAR,                    -- FK to stg_stores
    product_id       VARCHAR,                    -- FK to stg_products
    quantity         INTEGER,                    -- units ordered, must be > 0
    unit_price       DECIMAL(12,2),              -- price per unit at order time
    discount_pct     DECIMAL(6,2),               -- 0 to 100
    order_amount     DECIMAL(14,2),              -- quantity * unit_price * (1 - discount/100)
    payment_mode     VARCHAR,                    -- UPI | Card | Cash on Delivery | Wallet
    order_status     VARCHAR,                    -- DELIVERED | CANCELLED | RETURNED
    channel          VARCHAR,                    -- App | Web | Phone
    delivery_minutes INTEGER,                    -- door to door, 8 to 120 expected
    dq_status        VARCHAR,                    -- PASS | QUARANTINE
    failure_reason   VARCHAR,                    -- rule ids, semicolon separated
    _source_file     VARCHAR,                    -- which export this row came from
    _run_id          VARCHAR,                    -- which pipeline run wrote it
    _loaded_at       TIMESTAMP                   -- when it was written
);

CREATE TABLE IF NOT EXISTS stg_payments (
    payment_id     VARCHAR PRIMARY KEY,
    order_id       VARCHAR,
    payment_ts     TIMESTAMP,
    payment_mode   VARCHAR,
    amount_paid    DECIMAL(14,2),
    payment_status VARCHAR,                      -- SUCCESS | FAILED | REFUNDED
    gateway_ref    VARCHAR,
    dq_status      VARCHAR,
    failure_reason VARCHAR,
    _source_file   VARCHAR,
    _run_id        VARCHAR,
    _loaded_at     TIMESTAMP
);
