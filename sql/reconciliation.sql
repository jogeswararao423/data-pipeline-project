-- ==============================================================================
-- SQL Reconciliation & Integrity Verification Suite
-- Validates audit balances across raw, processed, current state, and aggregates
-- ==============================================================================

-- 1. High-Level Ingestion & Event Balance
-- Raw Ingested vs Processed + Rejected
SELECT 
    (SELECT COUNT(*) FROM raw_transaction_events) AS total_raw_events,
    (SELECT COUNT(*) FROM processed_transaction_events) AS total_processed_events,
    (SELECT COUNT(*) FROM rejected_records) AS total_rejected_records,
    ((SELECT COUNT(*) FROM processed_transaction_events) + (SELECT COUNT(*) FROM rejected_records)) AS total_accounted_events,
    ((SELECT COUNT(*) FROM raw_transaction_events) - 
     ((SELECT COUNT(*) FROM processed_transaction_events) + (SELECT COUNT(*) FROM rejected_records))) AS discrepancy;

-- 2. Event Uniqueness Verification
SELECT 
    event_id, 
    COUNT(*) AS occurrence_count
FROM processed_transaction_events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- 3. Current Order State Breakdown vs Event Ledger
SELECT 
    current_status,
    COUNT(*) AS total_orders,
    COALESCE(SUM(quantity), 0) AS total_units,
    COALESCE(SUM(total_amount), 0.00) AS total_amount
FROM current_order_state
GROUP BY current_status;

-- 4. Reversal and Cancellation Reconciliation
-- Verifies that orders marked CANCELLED or REVERSED match CANCEL/REVERSAL event counts
SELECT 
    'REVERSAL_EVENTS' AS metric_name,
    COUNT(*) AS event_count,
    COALESCE(SUM(total_amount), 0.00) AS event_volume
FROM processed_transaction_events
WHERE event_type = 'REVERSAL'
UNION ALL
SELECT 
    'REVERSED_ORDERS_CURRENT_STATE' AS metric_name,
    COUNT(*) AS order_count,
    COALESCE(SUM(total_amount), 0.00) AS order_volume
FROM current_order_state
WHERE current_status = 'REVERSED'
UNION ALL
SELECT 
    'CANCEL_EVENTS' AS metric_name,
    COUNT(*) AS event_count,
    COALESCE(SUM(total_amount), 0.00) AS event_volume
FROM processed_transaction_events
WHERE event_type = 'CANCEL'
UNION ALL
SELECT 
    'CANCELLED_ORDERS_CURRENT_STATE' AS metric_name,
    COUNT(*) AS order_count,
    COALESCE(SUM(total_amount), 0.00) AS order_volume
FROM current_order_state
WHERE current_status = 'CANCELLED';

-- 5. Daily Sales Aggregate vs Order State Reconciliation
-- Verifies that sum of daily net_sales matches current state active order amounts
SELECT 
    s.business_date,
    s.completed_orders AS daily_metrics_completed_orders,
    s.cancelled_orders AS daily_metrics_cancelled_orders,
    s.net_sales AS daily_metrics_net_sales,
    COALESCE(o.active_orders, 0) AS state_active_orders,
    COALESCE(o.cancelled_orders, 0) AS state_cancelled_orders,
    COALESCE(o.state_net_sales, 0.00) AS state_net_sales,
    (s.net_sales - COALESCE(o.state_net_sales, 0.00)) AS sales_discrepancy
FROM daily_sales_metrics s
LEFT JOIN (
    SELECT 
        business_date,
        COUNT(CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN 1 END) AS active_orders,
        COUNT(CASE WHEN current_status = 'CANCELLED' THEN 1 END) AS cancelled_orders,
        SUM(CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN total_amount ELSE 0.00 END) AS state_net_sales
    FROM current_order_state
    GROUP BY business_date
) o ON s.business_date = o.business_date
ORDER BY s.business_date;

-- 6. Product Metrics Reconciliation vs Daily Sales Net Sales
-- Verifies that the sum of product net_sales per business_date equals daily_sales net_sales
SELECT 
    dsm.business_date,
    dsm.net_sales AS total_daily_sales,
    COALESCE(SUM(dpm.net_sales), 0.00) AS product_sum_net_sales,
    (dsm.net_sales - COALESCE(SUM(dpm.net_sales), 0.00)) AS discrepancy
FROM daily_sales_metrics dsm
LEFT JOIN daily_product_metrics dpm ON dsm.business_date = dpm.business_date
GROUP BY dsm.business_date, dsm.net_sales
HAVING ABS(discrepancy) > 0.001;

-- 7. Customer Metrics Reconciliation vs Daily Sales Net Sales
-- Verifies that the sum of customer net_sales per business_date equals daily_sales net_sales
SELECT 
    dsm.business_date,
    dsm.net_sales AS total_daily_sales,
    COALESCE(SUM(dcm.net_sales), 0.00) AS customer_sum_net_sales,
    (dsm.net_sales - COALESCE(SUM(dcm.net_sales), 0.00)) AS discrepancy
FROM daily_sales_metrics dsm
LEFT JOIN daily_customer_metrics dcm ON dsm.business_date = dcm.business_date
GROUP BY dsm.business_date, dsm.net_sales
HAVING ABS(discrepancy) > 0.001;

-- 8. Referential Integrity Verification (No Orphan Records)
SELECT 
    COUNT(*) AS orphan_customer_events
FROM processed_transaction_events p
LEFT JOIN customers c ON p.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

SELECT 
    COUNT(*) AS orphan_product_events
FROM processed_transaction_events p
LEFT JOIN products pr ON p.product_id = pr.product_id
WHERE pr.product_id IS NULL;
