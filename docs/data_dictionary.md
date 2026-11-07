# Enterprise Data Dictionary

This document details the database schema, business grains, data types, constraints, and audit columns for all 12 tables in the data pipeline.

---

## 1. Reference Table: `customers`
- **Business Grain**: One record per registered customer.
- **Description**: Master customer reference dimension.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | VARCHAR(64) | NO | PRIMARY KEY | Unique business identifier for customer (e.g. `CUST-1001`). |
| `customer_name` | VARCHAR(255) | NO | | Full customer name. |
| `customer_email` | VARCHAR(255) | NO | | Customer email address. |
| `registration_date`| DATE | NO | INDEX | Date the customer registered. |
| `customer_status` | VARCHAR(32) | NO | INDEX, DEFAULT 'ACTIVE' | Account state (`ACTIVE`, `INACTIVE`, `SUSPENDED`). |
| `created_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit timestamp when row was created. |
| `updated_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit timestamp when row was last updated. |

---

## 2. Reference Table: `products`
- **Business Grain**: One record per catalog product.
- **Description**: Master product catalog reference dimension.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | VARCHAR(64) | NO | PRIMARY KEY | Unique business identifier for product (e.g. `PROD-2001`). |
| `product_name` | VARCHAR(255) | NO | | Display title of the product. |
| `category` | VARCHAR(100) | NO | INDEX | Product category (e.g. Electronics, Apparel). |
| `unit_price` | DECIMAL(12, 2) | NO | | Base catalog selling price in USD. |
| `product_status` | VARCHAR(32) | NO | INDEX, DEFAULT 'ACTIVE' | Lifecycle state (`ACTIVE`, `DISCONTINUED`). |
| `created_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit creation timestamp. |
| `updated_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit update timestamp. |

---

## 3. Staging Table: `raw_transaction_events`
- **Business Grain**: One record per raw line read from any input file.
- **Description**: Immutable landing area capturing input payloads prior to validation.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `staging_id` | BIGINT / INT | NO | PRIMARY KEY AUTO_INCREMENT | Auto-incrementing surrogate staging ID. |
| `event_id` | VARCHAR(64) | YES | INDEX | Raw event identifier from input. |
| `order_id` | VARCHAR(64) | YES | INDEX | Raw order identifier. |
| `customer_id` | VARCHAR(64) | YES | | Raw customer identifier. |
| `product_id` | VARCHAR(64) | YES | | Raw product identifier. |
| `event_type` | VARCHAR(32) | YES | | Raw event action string. |
| `quantity` | INT | YES | | Raw quantity field. |
| `unit_price` | DECIMAL(12, 2) | YES | | Raw price field. |
| `event_timestamp`| VARCHAR(64) | YES | | Raw timestamp string. |
| `business_date` | VARCHAR(32) | YES | | Raw business date string. |
| `source_file` | VARCHAR(255) | YES | | Originating filename. |
| `source_version` | INT | YES | | Version sequence number. |
| `run_id` | VARCHAR(64) | YES | INDEX | Pipeline execution run ID. |
| `ingested_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Timestamp row landed in staging. |

---

## 4. Ledger Table: `processed_transaction_events`
- **Business Grain**: One record per successfully validated transaction event.
- **Description**: Immutable, append-only historical audit ledger.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | VARCHAR(64) | NO | PRIMARY KEY | Unique event identifier. |
| `order_id` | VARCHAR(64) | NO | INDEX | Associated business order ID. |
| `customer_id` | VARCHAR(64) | NO | FK -> customers(customer_id) | Foreign key to customers dimension. |
| `product_id` | VARCHAR(64) | NO | FK -> products(product_id) | Foreign key to products dimension. |
| `event_type` | VARCHAR(32) | NO | INDEX (`CREATE`, `UPDATE`, `CANCEL`, `REVERSAL`) | Transaction lifecycle event type. |
| `quantity` | INT | NO | | Transacted quantity (> 0). |
| `unit_price` | DECIMAL(12, 2) | NO | | Transaction unit price. |
| `total_amount` | DECIMAL(14, 2) | NO | | Computed transaction total: `quantity * unit_price`. |
| `event_timestamp`| TIMESTAMP | NO | | Actual timestamp event occurred. |
| `business_date` | DATE | NO | INDEX | Business operational date. |
| `source_file` | VARCHAR(255) | NO | | Source CSV filename. |
| `source_version` | INT | NO | DEFAULT 1 | Version sequence for conflict resolution. |
| `run_id` | VARCHAR(64) | NO | | Pipeline run identifier that processed this event. |
| `processed_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Ledger insertion timestamp. |

---

## 5. Current State Table: `current_order_state`
- **Business Grain**: One record per business `order_id`.
- **Description**: Materialized latest operational state derived via deterministic event folding.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | VARCHAR(64) | NO | PRIMARY KEY | Unique business order ID. |
| `customer_id` | VARCHAR(64) | NO | FK -> customers(customer_id) | Ordering customer ID. |
| `product_id` | VARCHAR(64) | NO | FK -> products(product_id) | Ordered product ID. |
| `current_status` | VARCHAR(32) | NO | INDEX | `CREATED`, `UPDATED`, `CANCELLED`, `REVERSED`. |
| `quantity` | INT | NO | | Current active unit quantity. |
| `unit_price` | DECIMAL(12, 2) | NO | | Current agreed unit price. |
| `total_amount` | DECIMAL(14, 2) | NO | | Current recognized value (`0.00` if cancelled/reversed). |
| `last_event_id` | VARCHAR(64) | NO | | Most recent event applied to this order. |
| `last_event_type`| VARCHAR(32) | NO | | Most recent event type applied. |
| `last_event_timestamp`| TIMESTAMP| NO | | Timestamp of the latest applied event. |
| `source_version` | INT | NO | DEFAULT 1 | Highest version number applied. |
| `business_date` | DATE | NO | INDEX | Business date the order belongs to. |
| `created_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Creation timestamp of the order state. |
| `updated_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Last modification timestamp. |

---

## 6. Analytical Aggregate: `daily_sales_metrics`
- **Business Grain**: One record per `business_date`.
- **Description**: High-level financial and operational sales aggregate.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `business_date` | DATE | NO | PRIMARY KEY | Operational reporting date. |
| `total_orders` | INT | NO | DEFAULT 0 | Count of all orders initiated for this date. |
| `completed_orders`| INT | NO | DEFAULT 0 | Orders in active state (`CREATED`, `UPDATED`). |
| `cancelled_orders`| INT | NO | DEFAULT 0 | Orders marked `CANCELLED`. |
| `gross_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Total original sales before cancellations/reversals. |
| `net_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Recognized sales: `gross_sales` minus cancelled & reversed amounts. |
| `total_units_sold`| INT | NO | DEFAULT 0 | Total active units transacted. |
| `distinct_customers`| INT | NO | DEFAULT 0 | Count of unique customers with completed orders. |
| `average_order_value`| DECIMAL(14, 2)| NO| DEFAULT 0.00 | `net_sales / completed_orders` (or `0.00`). |
| `last_updated_at`| TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Timestamp when aggregate was computed. |
| `run_id` | VARCHAR(64) | NO | | Pipeline run that generated this partition. |

---

## 7. Analytical Aggregate: `daily_product_metrics`
- **Business Grain**: One record per `(business_date, product_id)`.
- **Description**: Daily product-level performance metrics.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `business_date` | DATE | NO | COMPOSITE PK (1/2) | Reporting date. |
| `product_id` | VARCHAR(64) | NO | COMPOSITE PK (2/2), FK | Product reference ID. |
| `product_name` | VARCHAR(255) | NO | | Joined product title. |
| `units_sold` | INT | NO | DEFAULT 0 | Total active units transacted for this product. |
| `gross_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Total gross sales volume. |
| `net_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Net recognized sales volume. |
| `order_count` | INT | NO | DEFAULT 0 | Count of distinct completed orders containing this product. |
| `last_updated_at`| TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit timestamp. |
| `run_id` | VARCHAR(64) | NO | | Pipeline run ID. |

---

## 8. Analytical Aggregate: `daily_customer_metrics`
- **Business Grain**: One record per `(business_date, customer_id)`.
- **Description**: Daily customer purchase behavior metrics.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `business_date` | DATE | NO | COMPOSITE PK (1/2) | Reporting date. |
| `customer_id` | VARCHAR(64) | NO | COMPOSITE PK (2/2), FK | Customer reference ID. |
| `order_count` | INT | NO | DEFAULT 0 | Count of completed orders placed by customer. |
| `units_purchased`| INT | NO | DEFAULT 0 | Units purchased. |
| `gross_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Gross spend. |
| `net_sales` | DECIMAL(14, 2) | NO | DEFAULT 0.00 | Net spend after reversals/cancellations. |
| `last_updated_at`| TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Audit timestamp. |
| `run_id` | VARCHAR(64) | NO | | Pipeline run ID. |

---

## 9. Dead-Letter Quarantine: `rejected_records`
- **Business Grain**: One record per rejected invalid input row.
- **Description**: Audit quarantine for malformed or rule-violating events.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `reject_id` | BIGINT / INT | NO | PRIMARY KEY AUTO_INCREMENT | Auto-increment surrogate key. |
| `source_file` | VARCHAR(255) | NO | INDEX | Originating CSV file name. |
| `source_row_number`| INT | NO | | 1-indexed row number from CSV file. |
| `event_id` | VARCHAR(64) | YES | INDEX | Event ID if parseable. |
| `rejection_reason` | VARCHAR(500) | NO | | Exact rule violated (e.g. `NEGATIVE_UNIT_PRICE`). |
| `rejected_payload` | TEXT | NO | | Exact raw row serialized as JSON. |
| `rejected_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Quarantine timestamp. |
| `run_id` | VARCHAR(64) | NO | INDEX | Pipeline run ID. |

---

## 10. Audit Log: `pipeline_run_log`
- **Business Grain**: One record per `(run_id, pipeline_stage)`.
- **Description**: Operational observability and execution metrics tracking.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR(64) | NO | COMPOSITE PK (1/2) | Unique run execution token. |
| `pipeline_stage` | VARCHAR(64) | NO | COMPOSITE PK (2/2) | Stage name (`INITIALIZE`, `BACKFILL`, `PIPELINE_TOTAL`). |
| `status` | VARCHAR(32) | NO | INDEX | `STARTED`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`. |
| `source_file` | VARCHAR(255) | YES | | Processed file path. |
| `records_read` | INT | NO | DEFAULT 0 | Total lines ingested. |
| `records_valid` | INT | NO | DEFAULT 0 | Valid lines passing data quality gates. |
| `records_rejected` | INT | NO | DEFAULT 0 | Lines sent to quarantine. |
| `records_processed`| INT | NO | DEFAULT 0 | Events written to ledger. |
| `records_inserted` | INT | NO | DEFAULT 0 | Orders inserted. |
| `records_updated` | INT | NO | DEFAULT 0 | Orders updated. |
| `metrics_recomputed`| INT | NO | DEFAULT 0 | Business date partitions recalculated. |
| `error_type` | VARCHAR(128) | YES | | Exception class name if failed. |
| `error_message` | TEXT | YES | | Stack trace or failure description. |
| `duration_seconds`| DECIMAL(10, 3)| YES | | Total execution time in seconds. |
| `start_time` | TIMESTAMP | NO | | Stage start time. |
| `end_time` | TIMESTAMP | YES | | Stage completion time. |

---

## 11. Registry: `ingestion_file_registry`
- **Business Grain**: One record per unique ingested file.
- **Description**: Prevents accidental re-ingestion and duplicate file processing.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `file_id` | BIGINT / INT | NO | PRIMARY KEY AUTO_INCREMENT | Auto-increment key. |
| `file_path` | VARCHAR(500) | NO | | Path of ingested file. |
| `file_name` | VARCHAR(255) | NO | | File basename. |
| `file_hash` | VARCHAR(64) | NO | UNIQUE INDEX | SHA-256 hexadecimal hash. |
| `row_count` | INT | NO | DEFAULT 0 | Row count in file. |
| `file_size_bytes` | BIGINT | NO | DEFAULT 0 | File size in bytes. |
| `status` | VARCHAR(32) | NO | INDEX | `IN_PROGRESS`, `SUCCESS`, `FAILED`. |
| `run_id` | VARCHAR(64) | NO | | Pipeline run ID that processed the file. |
| `ingested_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Registry timestamp. |

---

## 12. Verification: `data_quality_results`
- **Business Grain**: One record per automated check execution per run.
- **Description**: Automated health check and referential integrity audit results.

| Column Name | Data Type | Nullable | Constraints / Index | Description |
| :--- | :--- | :--- | :--- | :--- |
| `check_id` | BIGINT / INT | NO | PRIMARY KEY AUTO_INCREMENT | Auto-increment key. |
| `run_id` | VARCHAR(64) | NO | INDEX | Pipeline run ID. |
| `check_name` | VARCHAR(128) | NO | | Check code (e.g. `EVENT_ID_UNIQUENESS`). |
| `table_name` | VARCHAR(64) | NO | | Evaluated database table. |
| `records_checked`| INT | NO | DEFAULT 0 | Total records scanned. |
| `failed_records` | INT | NO | DEFAULT 0 | Number of non-conforming records detected. |
| `status` | VARCHAR(32) | NO | INDEX | `PASSED`, `FAILED`, `WARNING`. |
| `details` | TEXT | YES | | Diagnostic details message. |
| `executed_at` | TIMESTAMP | NO | DEFAULT CURRENT_TIMESTAMP | Check execution timestamp. |
