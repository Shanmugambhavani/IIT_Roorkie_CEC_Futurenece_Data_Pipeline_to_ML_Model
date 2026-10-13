-- NammaMart ELT: the quality scorecard.
--
-- Reconstructed from the staging tables rather than recorded as the checks run,
-- which is the structural difference between the two pipelines. In ETL the result
-- of each check is captured in Python at the moment it fires. In ELT the evidence
-- is the failure_reason column itself, and this query unnests it back into one row
-- per check.
--
-- Both produce the same shape, and the check_name values are identical, because
-- the dimension and severity for each rule come from check_catalog, which is built
-- directly from include/dq_checks.py. One vocabulary, two pipelines.

INSERT INTO dq_results
WITH staged AS (
    SELECT 'orders'    AS dataset, dq_status, failure_reason FROM stg_orders
    UNION ALL
    SELECT 'payments'  AS dataset, dq_status, failure_reason FROM stg_payments
    UNION ALL
    SELECT 'customers' AS dataset, dq_status, failure_reason FROM stg_customers
    UNION ALL
    SELECT 'products'  AS dataset, dq_status, failure_reason FROM stg_products
    UNION ALL
    SELECT 'stores'    AS dataset, dq_status, failure_reason FROM stg_stores
),
dataset_sizes AS (
    SELECT dataset, count(*) AS rows_checked FROM staged GROUP BY 1
),
fired AS (
    SELECT dataset, trim(reason) AS check_name, count(*) AS rows_failed
    FROM staged, unnest(str_split(failure_reason, ';')) AS t(reason)
    WHERE trim(reason) <> ''
    GROUP BY 1, 2
),
-- Every registered check appears, including the ones that found nothing. A rule
-- that silently stops running looks identical to a rule that passes unless the
-- zero is recorded.
all_checks AS (
    SELECT c.check_name, c.dimension, c.severity, c.dataset
    FROM check_catalog c
)
SELECT
    getvariable('run_id')                       AS run_id,
    getvariable('dag_id')                       AS dag_id,
    a.check_name,
    a.dimension,
    a.severity,
    coalesce(d.rows_checked, 0)                 AS rows_checked,
    coalesce(f.rows_failed, 0)                  AS rows_failed,
    CASE
        WHEN coalesce(f.rows_failed, 0) = 0 THEN 'PASS'
        WHEN a.severity = 'Critical'        THEN 'FAIL'
        ELSE 'WARN'
    END                                         AS status,
    now()                                       AS checked_at,
    a.dataset                                   AS note
FROM all_checks a
LEFT JOIN fired f          ON f.check_name = a.check_name AND f.dataset = a.dataset
LEFT JOIN dataset_sizes d  ON d.dataset    = a.dataset
ORDER BY a.dataset, a.check_name;
