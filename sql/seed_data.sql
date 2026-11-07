-- ==============================================================================
-- Seed Data: Initial Reference Dimensions (Customers and Products)
-- ==============================================================================

-- Seed Customers
INSERT INTO customers (customer_id, customer_name, customer_email, registration_date, customer_status) VALUES
('CUST-1001', 'Alice Johnson', 'alice.j@example.com', '2025-01-15', 'ACTIVE'),
('CUST-1002', 'Bob Smith', 'bob.smith@example.com', '2025-02-10', 'ACTIVE'),
('CUST-1003', 'Carol Danvers', 'carol.d@example.com', '2025-03-05', 'ACTIVE'),
('CUST-1004', 'David Miller', 'david.m@example.com', '2025-04-12', 'ACTIVE'),
('CUST-1005', 'Eva Green', 'eva.green@example.com', '2025-05-20', 'ACTIVE'),
('CUST-1006', 'Frank Castle', 'frank.c@example.com', '2025-06-18', 'ACTIVE'),
('CUST-1007', 'Grace Hopper', 'grace.h@example.com', '2025-07-22', 'ACTIVE'),
('CUST-1008', 'Hank Pym', 'hank.pym@example.com', '2025-08-30', 'ACTIVE'),
('CUST-1009', 'Iris West', 'iris.west@example.com', '2025-09-14', 'ACTIVE'),
('CUST-1010', 'Jack Reacher', 'jack.r@example.com', '2025-10-01', 'ACTIVE')
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    customer_email = VALUES(customer_email),
    customer_status = VALUES(customer_status);

-- Seed Products
INSERT INTO products (product_id, product_name, category, unit_price, product_status) VALUES
('PROD-2001', 'Ultra Wireless Headphones', 'Electronics', 149.99, 'ACTIVE'),
('PROD-2002', 'Ergonomic Mechanical Keyboard', 'Electronics', 89.50, 'ACTIVE'),
('PROD-2003', 'Gaming Mouse with RGB', 'Electronics', 49.95, 'ACTIVE'),
('PROD-2004', '4K Ultra HD Monitor 27in', 'Electronics', 329.00, 'ACTIVE'),
('PROD-2005', 'USB-C Multiport Docking Station', 'Accessories', 59.99, 'ACTIVE'),
('PROD-2006', 'Organic Cotton T-Shirt', 'Apparel', 24.50, 'ACTIVE'),
('PROD-2007', 'Waterproof Trail Running Shoes', 'Apparel', 119.00, 'ACTIVE'),
('PROD-2008', 'Stainless Steel Insulated Bottle', 'Home & Kitchen', 29.99, 'ACTIVE'),
('PROD-2009', 'Ceramic Pour-Over Coffee Dripper', 'Home & Kitchen', 34.00, 'ACTIVE'),
('PROD-2010', 'Aromatherapy Essential Oil Diffuser', 'Home & Kitchen', 39.95, 'ACTIVE')
ON DUPLICATE KEY UPDATE
    product_name = VALUES(product_name),
    category = VALUES(category),
    unit_price = VALUES(unit_price),
    product_status = VALUES(product_status);
