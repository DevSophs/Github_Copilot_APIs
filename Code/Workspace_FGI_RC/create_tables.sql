-- Create Dimension Tables
CREATE TABLE DIM_CUSTOMER (
    id_customer NUMBER PRIMARY KEY,
    te_full_name VARCHAR(100),
    nu_age NUMBER,
    dt_start DATE,
    dt_end DATE,
    fl_active NUMBER(1)
);

-- Insert sample data
INSERT INTO DIM_CUSTOMER VALUES (1, 'John Doe', 30, CURRENT_DATE, '9999-12-31', 1);

-- Create index
CREATE INDEX idx_customer_name ON DIM_CUSTOMER(te_full_name);

-- Grant permissions
GRANT SELECT ON DIM_CUSTOMER TO reporting_role;