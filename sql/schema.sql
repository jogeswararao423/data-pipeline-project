-- ==============================================================================
-- YottaFlex.ai Data Engineering Code Challenge
-- Database Schema Definition (MySQL 8.x / 9.x Compatible)
-- Character Set: utf8mb4, Engine: InnoDB
-- ==============================================================================

-- 1. Reference Table: Customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    registration_date DATE NOT NULL,
    customer_status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id),
    INDEX idx_customers_status (customer_status),
    INDEX idx_customers_reg_date (registration_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Reference Table: Products
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    product_status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id),
    INDEX idx_products_category (category),
    INDEX idx_products_status (product_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Ingestion Staging Table: Raw Transaction Events
CREATE TABLE IF NOT EXISTS raw_transaction_events (
    staging_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64),
    order_id VARCHAR(64),
    customer_id VARCHAR(64),
    product_id VARCHAR(64),
    event_type VARCHAR(32),
    quantity INT,
    unit_price DECIMAL(12, 2),
    event_timestamp VARCHAR(64),
    business_date VARCHAR(32),
    source_file VARCHAR(255),
    source_version INT,
    run_id VARCHAR(64),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_raw_run_id (run_id),
    INDEX idx_raw_event_id (event_id),
    INDEX idx_raw_order_id (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. Immutable Processed Transaction Events Ledger
CREATE TABLE IF NOT EXISTS processed_transaction_events (
    event_id VARCHAR(64) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    total_amount DECIMAL(14, 2) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    business_date DATE NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    source_version INT NOT NULL DEFAULT 1,
    run_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id),
    INDEX idx_proc_order_id (order_id),
    INDEX idx_proc_business_date (business_date),
    INDEX idx_proc_cust_prod (customer_id, product_id),
    INDEX idx_proc_event_type (event_type),
    CONSTRAINT fk_proc_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    CONSTRAINT fk_proc_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. Current State Table: Current Order State
CREATE TABLE IF NOT EXISTS current_order_state (
    order_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    current_status VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    total_amount DECIMAL(14, 2) NOT NULL,
    last_event_id VARCHAR(64) NOT NULL,
    last_event_type VARCHAR(32) NOT NULL,
    last_event_timestamp TIMESTAMP NOT NULL,
    source_version INT NOT NULL DEFAULT 1,
    business_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id),
    INDEX idx_order_business_date (business_date),
    INDEX idx_order_status (current_status),
    INDEX idx_order_customer (customer_id),
    INDEX idx_order_product (product_id),
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    CONSTRAINT fk_order_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. Analytical Aggregate: Daily Sales Metrics
CREATE TABLE IF NOT EXISTS daily_sales_metrics (
    business_date DATE NOT NULL,
    total_orders INT NOT NULL DEFAULT 0,
    completed_orders INT NOT NULL DEFAULT 0,
    cancelled_orders INT NOT NULL DEFAULT 0,
    gross_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    net_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    total_units_sold INT NOT NULL DEFAULT 0,
    distinct_customers INT NOT NULL DEFAULT 0,
    average_order_value DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    run_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (business_date),
    INDEX idx_sales_date (business_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7. Analytical Aggregate: Daily Product Metrics
CREATE TABLE IF NOT EXISTS daily_product_metrics (
    business_date DATE NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    units_sold INT NOT NULL DEFAULT 0,
    gross_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    net_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    order_count INT NOT NULL DEFAULT 0,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    run_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (business_date, product_id),
    INDEX idx_prod_metrics_date (business_date),
    INDEX idx_prod_metrics_prod (product_id),
    CONSTRAINT fk_dpm_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8. Analytical Aggregate: Daily Customer Metrics
CREATE TABLE IF NOT EXISTS daily_customer_metrics (
    business_date DATE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    order_count INT NOT NULL DEFAULT 0,
    units_purchased INT NOT NULL DEFAULT 0,
    gross_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    net_sales DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    run_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (business_date, customer_id),
    INDEX idx_cust_metrics_date (business_date),
    INDEX idx_cust_metrics_cust (customer_id),
    CONSTRAINT fk_dcm_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. Dead Letter Queue / Quarantine: Rejected Records
CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    source_row_number INT NOT NULL,
    event_id VARCHAR(64),
    rejection_reason VARCHAR(500) NOT NULL,
    rejected_payload TEXT NOT NULL,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_id VARCHAR(64) NOT NULL,
    INDEX idx_reject_file (source_file),
    INDEX idx_reject_run (run_id),
    INDEX idx_reject_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 10. Audit & Observability: Pipeline Run Log
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id VARCHAR(64) NOT NULL,
    pipeline_stage VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    source_file VARCHAR(255),
    records_read INT NOT NULL DEFAULT 0,
    records_valid INT NOT NULL DEFAULT 0,
    records_rejected INT NOT NULL DEFAULT 0,
    records_processed INT NOT NULL DEFAULT 0,
    records_inserted INT NOT NULL DEFAULT 0,
    records_updated INT NOT NULL DEFAULT 0,
    metrics_recomputed INT NOT NULL DEFAULT 0,
    error_type VARCHAR(128),
    error_message TEXT,
    duration_seconds DECIMAL(10, 3),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    PRIMARY KEY (run_id, pipeline_stage),
    INDEX idx_run_status (status),
    INDEX idx_run_start (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 11. Idempotency Registry: Ingestion File Registry
CREATE TABLE IF NOT EXISTS ingestion_file_registry (
    file_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    file_path VARCHAR(500) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL UNIQUE,
    row_count INT NOT NULL DEFAULT 0,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    run_id VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_file_hash (file_hash),
    INDEX idx_file_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 12. Automated Quality Verification: Data Quality Results
CREATE TABLE IF NOT EXISTS data_quality_results (
    check_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(64) NOT NULL,
    check_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    records_checked INT NOT NULL DEFAULT 0,
    failed_records INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    details TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dq_run (run_id),
    INDEX idx_dq_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
