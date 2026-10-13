-- NammaMart ELT staging: payments.
--
-- Same shape as stg_orders_upsert.sql: upsert on the payment_id primary key, so a
-- re-run cannot double count a payment. That matters more here than anywhere else,
-- because four payment_ids already arrive twice in the source export with the same
-- gateway_ref and amount. Appending blindly would inflate collections.

INSERT OR REPLACE INTO stg_payments
WITH typed AS (
    SELECT
        trim(payment_id)                       AS payment_id,
        trim(order_id)                         AS order_id,
        try_cast(payment_ts AS TIMESTAMP)      AS payment_ts,
        CASE lower(trim(payment_mode))
            WHEN 'upi'              THEN 'UPI'
            WHEN 'card'             THEN 'Card'
            WHEN 'cod'              THEN 'Cash on Delivery'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cashh'            THEN 'Cash on Delivery'
            WHEN 'wallet'           THEN 'Wallet'
            ELSE trim(payment_mode)
        END                                    AS payment_mode,
        trim(payment_mode)                     AS payment_mode_raw,
        try_cast(amount_paid AS DECIMAL(14,2)) AS amount_paid,
        upper(trim(payment_status))            AS payment_status,
        trim(gateway_ref)                      AS gateway_ref,
        _row_id, _source_file, _run_id, _loaded_at,
        row_number() OVER (PARTITION BY trim(payment_id) ORDER BY _row_id) AS _rn
    FROM raw_payments
),
checked AS (
    SELECT
        t.*,
        array_to_string(list_filter([
            CASE WHEN _rn > 1
                 THEN 'payments_duplicate_payment_id' END,
            CASE WHEN order_id NOT IN (SELECT order_id FROM stg_orders)
                 THEN 'payments_orphan_order_id' END,
            CASE WHEN payment_status NOT IN ('SUCCESS','FAILED','REFUNDED')
                 THEN 'payments_invalid_status' END,
            CASE WHEN payment_status = 'SUCCESS'
                      AND (amount_paid IS NULL OR amount_paid <= 0)
                 THEN 'payments_non_positive_amount' END,
            CASE WHEN payment_ts IS NULL
                 THEN 'payments_unparseable_payment_ts' END,
            CASE WHEN payment_ts > CAST(getvariable('as_of') AS TIMESTAMP)
                 THEN 'payments_future_payment_ts' END
        ], x -> x IS NOT NULL), '; ') AS critical_reason,
        array_to_string(list_filter([
            -- Scoped to non cash modes. All 301 blank gateway_refs in the export are
            -- Cash on Delivery, where no gateway reference exists. A blanket not-null
            -- rule here would raise 301 false alarms and teach people to ignore the
            -- quality report.
            CASE WHEN gateway_ref = '' AND payment_mode <> 'Cash on Delivery'
                 THEN 'payments_missing_gateway_ref' END,
            CASE WHEN payment_mode_raw NOT IN ('UPI','Card','Cash on Delivery','Wallet')
                 THEN 'payments_payment_mode_casing' END
        ], x -> x IS NOT NULL), '; ') AS warning_reason
    FROM typed t
)
SELECT
    payment_id, order_id, payment_ts, payment_mode, amount_paid,
    payment_status, gateway_ref,
    CASE WHEN critical_reason = '' THEN 'PASS' ELSE 'QUARANTINE' END AS dq_status,
    CASE WHEN critical_reason = '' AND warning_reason = '' THEN ''
         WHEN critical_reason = '' THEN warning_reason
         WHEN warning_reason  = '' THEN critical_reason
         ELSE critical_reason || '; ' || warning_reason END AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM checked;
