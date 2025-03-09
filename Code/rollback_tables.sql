-- Revoke permissions first
REVOKE SELECT ON DIM_CUSTOMER FROM reporting_role;

-- Drop indexes
DROP INDEX IF EXISTS idx_customer_name;

-- Truncate tables before dropping (optional, for clean removal)
TRUNCATE TABLE DIM_CUSTOMER;

-- Drop the table
DROP TABLE IF EXISTS DIM_CUSTOMER;

-- Verify cleanup
SELECT table_name 
FROM information_schema.tables 
WHERE table_name = 'DIM_CUSTOMER';