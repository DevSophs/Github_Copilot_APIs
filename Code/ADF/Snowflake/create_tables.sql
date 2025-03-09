-- Create dimension and fact tables in Snowflake
CREATE TABLE DIM_CUSTOMER (
    id_customer NUMBER,
    te_full_name VARCHAR(100),
    nu_age NUMBER,
    dt_start DATE,
    dt_end DATE,
    fl_active NUMBER(1)
);

CREATE TABLE DIM_ACCOUNT (
    id_account NUMBER,
    co_account VARCHAR(50),
    dt_creation DATE,
    fl_enterprise NUMBER(1),
    dt_start DATE,
    dt_end DATE,
    fl_active NUMBER(1)
);

CREATE TABLE DIM_CARDS (
    id_car VARCHAR(32),
    co_car VARCHAR(16),
    id_account NUMBER,
    id_customer NUMBER,
    dt_exp_date DATE,
    dt_start DATE,
    dt_end DATE,
    fl_active NUMBER(1)
);

CREATE TABLE FACT_TRANSACTIONS (
    id_trx NUMBER,
    id_car VARCHAR(32),
    id_account NUMBER,
    id_customer NUMBER,
    va_amount NUMBER(15,2),
    fl_reversal NUMBER(1)
);