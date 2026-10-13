-- NammaMart ELT mart layer.
--
-- Business-ready tables, one per question Kavya and finance actually ask. Every
-- model reads only dq_status = 'PASS' rows, which is where the ELT pipeline does
-- what the ETL pipeline did back in Python: the bad rows are present in staging
-- for audit, and filtered out here.
--
-- Revenue counts DELIVERED orders only, per the client's rule. CANCELLED and
-- RETURNED rows stay in the fact so cancellation and return rates remain
-- answerable, but contribute zero revenue.

-- A single shared fact definition, so no two marts can disagree about what a
-- delivered order is worth.
CREATE OR REPLACE VIEW v_fact_orders AS
SELECT
    o.order_id,
    o.order_ts,
    o.order_date,
    o.customer_id,
    o.store_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.order_amount,
    o.payment_mode,
    o.order_status,
    o.channel,
    o.delivery_minutes,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.cost_price,
    p.mrp,
    p.cost_price_suspect,
    s.store_name,
    s.zone,
    s.area AS store_area,
    c.loyalty_tier,
    c.city,
    (o.order_status = 'DELIVERED')                              AS is_delivered,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN o.order_amount ELSE 0 END                         AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN round(o.quantity * p.cost_price, 2) ELSE 0 END    AS cogs,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN round(o.order_amount - o.quantity * p.cost_price, 2)
         ELSE 0 END                                             AS gross_margin,
    (o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15) AS within_15_min
FROM stg_orders o
LEFT JOIN stg_products  p ON o.product_id  = p.product_id
LEFT JOIN stg_stores    s ON o.store_id    = s.store_id
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS';

-- Q1: daily net revenue by zone and store, delivered orders only, after discount
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date,
    zone,
    store_id,
    store_name,
    count(DISTINCT order_id)                    AS orders,
    round(sum(net_revenue), 2)                  AS net_revenue,
    round(sum(gross_margin), 2)                 AS gross_margin,
    round(avg(delivery_minutes), 1)             AS avg_delivery_minutes,
    sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) AS orders_within_15_min,
    round(100.0 * sum(CASE WHEN within_15_min THEN 1 ELSE 0 END)
          / count(DISTINCT order_id), 1)        AS pct_within_15_min
FROM v_fact_orders
WHERE is_delivered
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3;

-- Q2: revenue and gross margin by product category
-- revenue_on_suspect_cost shows how much margin rests on a cost_price nobody
-- trusts, instead of quietly reporting a number built on bad data.
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    count(DISTINCT order_id)          AS orders,
    sum(quantity)                     AS units,
    round(sum(net_revenue), 2)        AS net_revenue,
    round(sum(cogs), 2)               AS cogs,
    round(sum(gross_margin), 2)       AS gross_margin,
    round(100.0 * sum(gross_margin) / nullif(sum(net_revenue), 0), 1) AS margin_pct,
    round(sum(CASE WHEN cost_price_suspect THEN net_revenue ELSE 0 END), 2)
                                      AS revenue_on_suspect_cost
FROM v_fact_orders
WHERE is_delivered
GROUP BY 1
ORDER BY net_revenue DESC;

-- Q3: average delivery time per store and the 15 minute promise
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    store_name,
    zone,
    count(*)                            AS delivered_orders,
    round(avg(delivery_minutes), 1)     AS avg_delivery_minutes,
    min(delivery_minutes)               AS fastest_minutes,
    max(delivery_minutes)               AS slowest_minutes,
    sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) AS orders_within_15_min,
    round(100.0 * sum(CASE WHEN within_15_min THEN 1 ELSE 0 END) / count(*), 1)
                                        AS pct_within_15_min
FROM v_fact_orders
WHERE is_delivered
GROUP BY 1, 2, 3
ORDER BY pct_within_15_min DESC;

-- Q4: revenue by payment mode
CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT
    payment_mode,
    count(DISTINCT order_id)   AS orders,
    round(sum(net_revenue), 2) AS net_revenue
FROM v_fact_orders
WHERE is_delivered
GROUP BY 1
ORDER BY net_revenue DESC;

-- Q4 continued: everything that does not tie out with the gateway, one row per
-- mismatch with a reason finance can act on.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH delivered AS (
    SELECT order_id, order_date, store_id, order_amount, payment_mode
    FROM v_fact_orders WHERE is_delivered
),
success AS (
    SELECT payment_id, order_id, amount_paid, payment_mode, payment_ts
    FROM stg_payments
    WHERE dq_status = 'PASS' AND payment_status = 'SUCCESS'
),
quarantined_orders AS (
    SELECT order_id FROM stg_orders WHERE dq_status = 'QUARANTINE'
),
joined AS (
    SELECT
        coalesce(d.order_id, s.order_id) AS order_id,
        s.payment_id,
        d.order_date,
        d.store_id,
        coalesce(d.order_amount, 0) AS order_amount,
        coalesce(s.amount_paid, 0)  AS amount_paid,
        d.payment_mode AS order_mode,
        s.payment_mode AS payment_mode,
        d.order_id IS NOT NULL AS has_order,
        s.payment_id IS NOT NULL AS has_payment
    FROM delivered d
    FULL OUTER JOIN success s ON d.order_id = s.order_id
)
SELECT
    order_id,
    payment_id,
    order_date,
    store_id,
    order_amount,
    amount_paid,
    round(amount_paid - order_amount, 2) AS difference,
    CASE
        WHEN has_order AND NOT has_payment THEN 'MISSING_PAYMENT'
        WHEN NOT has_order AND order_id IN (SELECT order_id FROM quarantined_orders)
                                       THEN 'PAYMENT_FOR_REJECTED_ORDER'
        WHEN NOT has_order             THEN 'PAYMENT_WITHOUT_ORDER'
        WHEN abs(amount_paid - order_amount) > 0.011 THEN 'AMOUNT_MISMATCH'
        ELSE 'MODE_MISMATCH'
    END AS reason
FROM joined
WHERE NOT has_order
   OR NOT has_payment
   OR abs(amount_paid - order_amount) > 0.011
   OR coalesce(order_mode, '') <> coalesce(payment_mode, '')
ORDER BY reason, order_id;

-- Q5: top 10 customers by spend. No raw email, phone or full name reaches here,
-- because stg_customers dropped those columns at the staging boundary.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    f.customer_id,
    c.customer_name_masked,
    c.email_masked,
    c.loyalty_tier,
    c.city,
    count(DISTINCT f.order_id)   AS orders,
    round(sum(f.net_revenue), 2) AS total_spend
FROM v_fact_orders f
LEFT JOIN stg_customers c ON f.customer_id = c.customer_id
WHERE f.is_delivered
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_spend DESC
LIMIT 10;

-- Q6: rows rejected this run, by file and by reason. unnest turns the semicolon
-- separated failure_reason into one row per rule, so a row that broke two rules is
-- counted under both.
CREATE OR REPLACE TABLE mart_rejected_summary AS
WITH rejected AS (
    SELECT _source_file, _run_id, failure_reason FROM stg_orders   WHERE dq_status = 'QUARANTINE'
    UNION ALL
    SELECT _source_file, _run_id, failure_reason FROM stg_payments WHERE dq_status = 'QUARANTINE'
)
SELECT
    _source_file AS source_file,
    _run_id      AS run_id,
    trim(reason) AS failure_rule,
    count(*)     AS rows_rejected
FROM rejected, unnest(str_split(failure_reason, ';')) AS t(reason)
WHERE trim(reason) <> ''
GROUP BY 1, 2, 3
ORDER BY rows_rejected DESC;
