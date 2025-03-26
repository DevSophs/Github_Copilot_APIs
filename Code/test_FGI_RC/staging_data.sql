-- Create sample data for STG_CUSTOMER
INSERT INTO STG_CUSTOMER (customer_id, name, surname, age) VALUES
(1, 'John', 'Smith', 35),
(2, 'Emma', 'Johnson', 28),
(3, 'Michael', 'Brown', 45),
(4, 'Sarah', 'Davis', 31),
(5, 'James', 'Wilson', 52);

-- Create sample data for STG_ACCOUNT
INSERT INTO STG_ACCOUNT (account_id, account_code, creation_date) VALUES
(101, 'ACC001', '2023-01-15'),
(102, 'ACC002', '2023-02-20'),
(103, 'ACC003', '2023-03-10'),
(104, 'ACC004', '2023-04-05'),
(105, 'ACC005', '2023-05-01');

-- Create sample data for STG_ACC_CUST
INSERT INTO STG_ACC_CUST (customer_id, account_id) VALUES
(1, 101),
(2, 102),
(3, 103),
(4, 104),
(5, 105),
(1, 102),
(2, 103);

-- Create sample data for STG_CARDS
INSERT INTO STG_CARDS (PAN, account_id, customer_id, expiration_date) VALUES
('4532789012345678', 101, 1, '2025-12-31'),
('4532789012345679', 102, 2, '2025-11-30'),
('4532789012345680', 103, 3, '2025-10-31'),
('4532789012345681', 104, 4, '2025-09-30'),
('4532789012345682', 105, 5, '2025-08-31');

-- Create sample data for STG_TRANSACTIONS
INSERT INTO STG_TRANSACTIONS (transaction_id, PAN, account_id, customer_id, amount) VALUES
(1001, '4532789012345678', 101, 1, 150.75),
(1002, '4532789012345679', 102, 2, 89.99),
(1003, '4532789012345680', 103, 3, 250.00),
(1004, '4532789012345681', 104, 4, 75.50),
(1005, '4532789012345682', 105, 5, 199.99),
(1006, '4532789012345678', 101, 1, 45.25),
(1007, '4532789012345679', 102, 2, 120.00),
(1008, '4532789012345680', 103, 3, 399.99),
(1009, '4532789012345681', 104, 4, 67.50),
(1010, '4532789012345682', 105, 5, 299.99);