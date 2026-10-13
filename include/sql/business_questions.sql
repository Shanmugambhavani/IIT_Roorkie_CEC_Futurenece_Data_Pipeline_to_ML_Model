-- NammaMart: the six business questions, one query each.
--
-- Run against include/warehouse_bhavani.duckdb after both DAGs have run.
-- Each question is answered twice, once from the ETL star schema and once from
-- the ELT mart, to show the two pipelines agree.
--
-- Answers below were captured with the increment file loaded, so they cover
-- 1 September to 1 October 2026.

-- ===================================================================
-- Q1. Daily net revenue by zone and by store, delivered orders only
-- ===================================================================

-- ELT
SELECT order_date, zone, store_name, orders, net_revenue
FROM mart_daily_store_kpis
ORDER BY net_revenue DESC
LIMIT 10;

-- ETL
SELECT order_date, zone, store_name,
       count(DISTINCT order_id) AS orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
WHERE is_delivered
GROUP BY 1, 2, 3
ORDER BY net_revenue DESC
LIMIT 10;

-- Rolled up by zone:
--   East      3,14,056.20
--   West      3,12,413.00
--   South     2,56,214.55
--   North     2,43,021.55
--   Unknown     85,382.00   <- S07 BTM Layout, the store with no zone in the master.
--                              Under a reject-the-dimension design this 85k would
--                              have vanished from the report entirely.

-- ===================================================================
-- Q2. Revenue and gross margin by product category
--     margin = revenue - quantity * cost_price
-- ===================================================================

SELECT category, net_revenue, cogs, gross_margin, margin_pct,
       revenue_on_suspect_cost
FROM mart_category_margin
ORDER BY net_revenue DESC;

-- Staples              2,87,601.45   62,178.09   21.6%
-- Personal Care        2,15,345.30   46,922.58   21.8%
-- Household            2,12,105.05   54,466.15   25.7%
-- Dairy & Bakery       1,79,568.95   47,633.00   26.5%
-- Snacks & Beverages   1,74,988.60   41,429.67   23.7%
-- Fruits & Vegetables    99,566.20   20,511.38   20.6%
-- Uncategorised          41,911.75    4,943.39   11.8%   <- 3 products with no category
--
-- Uncategorised margin of 11.8% is roughly half every other category. That is not a
-- pricing insight, it is a data gap: these products also carry suspect cost prices.
-- Worth asking merchandising about before anyone acts on it.

-- ===================================================================
-- Q3. Average delivery time per store, and % of delivered orders
--     within 15 minutes
-- ===================================================================

SELECT store_name, zone, delivered_orders, avg_delivery_minutes,
       orders_within_15_min, pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;

-- Overall: 2,228 delivered, average 21.3 minutes, 28.5% within 15 minutes.
-- Best:  Whitefield 35.4%, BTM Layout 32.7%, Hebbal 32.2%
-- Worst: Banashankari 21.6%, Basaveshwaranagar 22.9%
--
-- The headline for Kavya: a company promising 10 minute delivery is averaging 21.3
-- and hitting 15 minutes on fewer than three orders in ten. The spread between best
-- and worst store is 14 points, which is an operations problem, not a data one.

-- ===================================================================
-- Q4. Revenue by payment mode, and the total that does not reconcile
-- ===================================================================

SELECT payment_mode, orders, net_revenue
FROM mart_payment_mode_revenue
ORDER BY net_revenue DESC;

-- UPI               1,205 orders   6,38,752.15
-- Card                457          2,51,882.30
-- Cash on Delivery    320          1,82,334.40
-- Wallet              246          1,38,118.45

SELECT reason, count(*) AS rows_affected, round(sum(difference), 2) AS value
FROM mart_payment_reconciliation
GROUP BY 1
ORDER BY 2 DESC;

-- MISSING_PAYMENT             220   -1,22,095.75
-- PAYMENT_FOR_REJECTED_ORDER   69       76,405.73
-- AMOUNT_MISMATCH              13         -685.00
-- PAYMENT_WITHOUT_ORDER        10        4,193.70
--
-- MISSING_PAYMENT is 220 rather than 70 because the increment added 150 October
-- orders while payments.csv still only covers September. That is a data delivery
-- gap, not a collections problem, and the reconciliation table makes the difference
-- visible instead of letting it look like 1.2 lakh of unpaid revenue.
-- The September-only figure is 70 orders.

-- ===================================================================
-- Q5. Top 10 customers by spend, with loyalty tier, no raw PII
-- ===================================================================

SELECT customer_id, customer_name_masked, email_masked, loyalty_tier,
       orders, total_spend
FROM mart_top_customers;

-- C00136  Sanjay B.  sa***@example.com  Bronze   9   8,738.10
-- C00115  Sanjay K.  sa***@example.com  Bronze   8   8,037.70
-- C00283  Nikhil R.  ni***@example.com  Bronze   8   7,934.60
-- C00236  Sneha N.   sn***@example.com  Platinum 12  7,831.80
-- C00310  Kiran P.   ki***@example      Bronze   8   7,258.95
--
-- No raw email, phone or full name appears. ki***@example is a masked malformed
-- address, flagged customers_bad_email in dq_results.
--
-- Note for the loyalty team: four of the top five spenders are Bronze. Either the
-- tier rules are not based on spend, or they have not been refreshed. Worth asking.

-- ===================================================================
-- Q6. Rows rejected today, by file and by reason
-- ===================================================================

SELECT source_file, failure_rule, rows_rejected
FROM mart_rejected_summary
ORDER BY rows_rejected DESC;

-- orders.csv    orders_missing_customer_id        18
-- orders.csv    orders_amount_formula_mismatch    18
-- orders.csv    orders_quantity_not_positive      10
-- payments.csv  payments_non_positive_amount      10
-- orders.csv    orders_product_fk_missing          9
-- orders.csv    orders_delivered_without_minutes   8
-- payments.csv  payments_orphan_order_id           8
-- orders.csv    orders_future_order_ts             7
-- payments.csv  payments_future_payment_ts         7
-- orders.csv    orders_delivery_over_limit         6
-- orders.csv    orders_unparseable_order_ts        6
-- orders.csv    orders_store_fk_missing            5
-- orders.csv    orders_invalid_status              3
-- orders.csv    orders_discount_out_of_range       3

-- The same question answered from the quality scorecard, which also shows the
-- checks that passed. A rule that stops firing looks identical to a rule that
-- passes unless the zeroes are recorded too.
SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results
WHERE run_id = (SELECT max(run_id) FROM dq_results)
ORDER BY severity, rows_failed DESC;

-- ===================================================================
-- Do the two pipelines agree? (Part B4)
-- ===================================================================

SELECT 'ETL' AS pipeline, count(*) AS fact_rows,
       round(sum(net_revenue), 2) AS total_revenue,
       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                      THEN net_revenue ELSE 0 END), 2) AS september_revenue
FROM etl_fact_orders
UNION ALL
SELECT 'ELT', count(*), round(sum(net_revenue), 2),
       round(sum(CASE WHEN order_date < DATE '2026-10-01'
                      THEN net_revenue ELSE 0 END), 2)
FROM v_fact_orders;

-- ETL  2460  12,11,087.30  11,31,435.10
-- ELT  2460  12,11,087.30  11,31,435.10

-- Row by row, not just in total:
SELECT count(*) AS orders_that_differ
FROM etl_fact_orders e
FULL OUTER JOIN v_fact_orders l ON e.order_id = l.order_id
WHERE coalesce(e.net_revenue, -1) <> coalesce(l.net_revenue, -1);
-- 0
