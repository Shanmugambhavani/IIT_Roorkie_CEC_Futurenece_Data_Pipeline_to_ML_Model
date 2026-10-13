-- NammaMart ELT staging: dimensions
--
-- Same policy as the ETL pipeline: dimension rows are repaired, not rejected.
-- Rejecting a store because its zone is blank would silently delete every order
-- placed there. Only a missing primary key is fatal.
--
-- Every row carries dq_status and failure_reason so the issue stays visible even
-- though the row loads. Rule names match include/dq_checks.py exactly, which is
-- what lets the two pipelines be compared check by check.

-- ---------------------------------------------------------------- stores
CREATE OR REPLACE TABLE stg_stores AS
WITH typed AS (
    SELECT
        trim(store_id)                               AS store_id,
        trim(store_name)                             AS store_name,
        nullif(trim(zone), '')                       AS zone_raw,
        trim(area)                                   AS area,
        trim(pincode)                                AS pincode,
        try_cast(open_date AS DATE)                  AS open_date,
        trim(manager_name)                           AS manager_name,
        try_cast(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        _row_id, _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(store_id) ORDER BY _row_id) AS _rn
    FROM raw_stores
)
SELECT
    store_id,
    store_name,
    coalesce(zone_raw, 'Unknown') AS zone,   -- repaired, not rejected
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
    CASE WHEN store_id IS NULL OR store_id = '' THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN store_id IS NULL OR store_id = ''      THEN 'stores_missing_id' END,
        CASE WHEN _rn > 1                                THEN 'stores_duplicate_id' END,
        CASE WHEN zone_raw IS NULL                       THEN 'stores_missing_zone' END,
        CASE WHEN NOT regexp_matches(pincode, '^\d{6}$') THEN 'stores_bad_pincode' END,
        CASE WHEN capacity_orders_per_day IS NULL
                  OR capacity_orders_per_day <= 0        THEN 'stores_non_positive_capacity' END
    ], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM typed
WHERE _rn = 1;

-- ---------------------------------------------------------------- products
CREATE OR REPLACE TABLE stg_products AS
WITH typed AS (
    SELECT
        trim(product_id)                      AS product_id,
        trim(product_name)                    AS product_name,
        nullif(trim(category), '')            AS category_raw,
        trim(sub_category)                    AS sub_category,
        trim(brand)                           AS brand,
        try_cast(mrp AS DECIMAL(12,2))        AS mrp,
        try_cast(cost_price AS DECIMAL(12,2)) AS cost_price,
        trim(unit)                            AS unit,
        upper(trim(is_perishable)) IN ('TRUE','1','YES') AS is_perishable,
        try_cast(launch_date AS DATE)         AS launch_date,
        _row_id, _source_file, _run_id, _loaded_at,
        -- The second copy of a duplicated product is the "(old)" row, which sits
        -- after the current one in the export. Keeping the first keeps the live price.
        row_number() OVER (PARTITION BY trim(product_id) ORDER BY _row_id) AS _rn
    FROM raw_products
)
SELECT
    product_id,
    product_name,
    array_to_string(list_transform(str_split(lower(trim(coalesce(category_raw, 'Uncategorised'))), ' '), w -> upper(substr(w,1,1)) || substr(w,2)), ' ') AS category,
    array_to_string(list_transform(str_split(lower(trim(sub_category)), ' '), w -> upper(substr(w,1,1)) || substr(w,2)), ' ') AS sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    -- Travels with the row so margin can be qualified rather than silently wrong.
    (cost_price > mrp OR mrp IS NULL OR mrp <= 0) AS cost_price_suspect,
    CASE WHEN product_id IS NULL OR product_id = '' THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN product_id IS NULL OR product_id = '' THEN 'products_missing_id' END,
        CASE WHEN _rn > 1                               THEN 'products_duplicate_id' END,
        CASE WHEN category_raw IS NULL                  THEN 'products_missing_category' END,
        CASE WHEN mrp IS NULL OR mrp <= 0               THEN 'products_non_positive_mrp' END,
        CASE WHEN cost_price > mrp                      THEN 'products_cost_above_mrp' END,
        CASE WHEN launch_date > CAST(getvariable('as_of') AS DATE)
                                                        THEN 'products_future_launch' END
    ], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM typed
WHERE _rn = 1;

-- ---------------------------------------------------------------- customers
-- PII masking happens here and the raw columns are dropped in the same step, so
-- no downstream model can carry an unmasked value forward. Mirrors
-- include/masking.py; change one and you must change the other.
CREATE OR REPLACE TABLE stg_customers AS
WITH typed AS (
    SELECT
        trim(customer_id)                  AS customer_id,
        trim(full_name)                    AS full_name,
        trim(email)                        AS email,
        regexp_replace(trim(phone), '[^0-9]', '', 'g') AS phone_digits,
        nullif(trim(city), '')             AS city_raw,
        trim(area)                         AS area,
        trim(pincode)                      AS pincode,
        try_cast(signup_date AS DATE)      AS signup_date,
        array_to_string(list_transform(str_split(lower(trim(loyalty_tier)), ' '), w -> upper(substr(w,1,1)) || substr(w,2)), ' ') AS loyalty_tier,
        upper(trim(is_active)) IN ('TRUE','1','YES') AS is_active,
        _row_id, _source_file, _run_id, _loaded_at,
        -- Conflicting duplicates: keep the last row seen. The export carries no
        -- updated_at, so this is an assumption, recorded in SUBMISSION.md.
        row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _row_id DESC) AS _rn
    FROM raw_customers
)
SELECT
    customer_id,
    CASE
        WHEN full_name = '' THEN ''
        WHEN len(str_split(full_name, ' ')) = 1 THEN full_name
        ELSE str_split(full_name, ' ')[1] || ' '
             || substr(str_split(full_name, ' ')[-1], 1, 1) || '.'
    END AS customer_name_masked,
    CASE
        WHEN email NOT LIKE '%@%' THEN ''
        ELSE substr(split_part(email, '@', 1), 1, 2) || '***@' || split_part(email, '@', 2)
    END AS email_masked,
    CASE
        WHEN length(phone_digits) < 4 THEN repeat('*', length(phone_digits))
        ELSE substr(phone_digits, 1, 2)
             || repeat('*', length(phone_digits) - 4)
             || substr(phone_digits, length(phone_digits) - 1, 2)
    END AS phone_masked,
    coalesce(city_raw, 'Bengaluru') AS city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    CASE WHEN customer_id IS NULL OR customer_id = '' THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status,
    array_to_string(list_filter([
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 'customers_missing_id' END,
        CASE WHEN _rn > 1                                 THEN 'customers_duplicate_id' END,
        CASE WHEN signup_date > CAST(getvariable('as_of') AS DATE)
                                                          THEN 'customers_future_signup' END,
        CASE WHEN NOT regexp_matches(email, '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
                                                          THEN 'customers_bad_email' END,
        CASE WHEN NOT regexp_matches(phone_digits, '^[6-9]\d{9}$')
                                                          THEN 'customers_bad_phone' END,
        CASE WHEN NOT regexp_matches(pincode, '^\d{6}$')  THEN 'customers_bad_pincode' END,
        CASE WHEN city_raw IS NULL                        THEN 'customers_missing_city' END,
        CASE WHEN loyalty_tier NOT IN ('Bronze','Silver','Gold','Platinum')
                                                          THEN 'customers_tier_casing' END
    ], x -> x IS NOT NULL), '; ') AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM typed
WHERE _rn = 1;
