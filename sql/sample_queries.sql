-- ==============================================================================
-- Analytical & Business Intelligence Sample Queries
-- ==============================================================================

-- 1. Daily Revenue, Volume & Average Order Value Trend
SELECT 
    business_date,
    total_orders,
    completed_orders,
    cancelled_orders,
    gross_sales,
    net_sales,
    total_units_sold,
    distinct_customers,
    average_order_value
FROM daily_sales_metrics
ORDER BY business_date DESC;

-- 2. Top-Selling Products by Net Sales
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(dpm.units_sold) AS total_units_sold,
    SUM(dpm.gross_sales) AS lifetime_gross_sales,
    SUM(dpm.net_sales) AS lifetime_net_sales,
    SUM(dpm.order_count) AS lifetime_order_count
FROM daily_product_metrics dpm
JOIN products p ON dpm.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY lifetime_net_sales DESC
LIMIT 10;

-- 3. Top High-Value Customers by Cumulative Spend
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_email,
    SUM(dcm.order_count) AS total_orders,
    SUM(dcm.units_purchased) AS total_units_purchased,
    SUM(dcm.net_sales) AS total_net_spend
FROM daily_customer_metrics dcm
JOIN customers c ON dcm.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_email
ORDER BY total_net_spend DESC
LIMIT 10;

-- 4. Cancellation Rate Analysis by Business Date
SELECT 
    business_date,
    total_orders,
    completed_orders,
    cancelled_orders,
    ROUND((cancelled_orders * 100.0 / NULLIF(total_orders, 0)), 2) AS cancellation_rate_pct
FROM daily_sales_metrics
ORDER BY business_date ASC;

-- 5. Financial Reversals & Refunds Audit Log
SELECT 
    p.event_id,
    p.order_id,
    p.customer_id,
    p.product_id,
    p.total_amount AS reversed_amount,
    p.event_timestamp,
    p.business_date,
    p.source_file,
    p.run_id
FROM processed_transaction_events p
WHERE p.event_type = 'REVERSAL'
ORDER BY p.event_timestamp DESC;

-- 6. Current Order Status Distribution
SELECT 
    current_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM current_order_state), 2) AS pct_of_orders,
    SUM(total_amount) AS total_state_value
FROM current_order_state
GROUP BY current_status;

-- 7. High-Volume Audit: Top Rejection Reasons
SELECT 
    rejection_reason,
    COUNT(*) AS reject_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rejected_records), 2) AS pct_of_total_rejects
FROM rejected_records
GROUP BY rejection_reason
ORDER BY reject_count DESC;

-- 8. Pipeline Execution Health & Performance Summary
SELECT 
    run_id,
    pipeline_stage,
    status,
    records_read,
    records_valid,
    records_rejected,
    records_processed,
    metrics_recomputed,
    duration_seconds,
    start_time,
    end_time
FROM pipeline_run_log
ORDER BY start_time DESC;
