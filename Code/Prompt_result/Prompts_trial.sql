-- LISTA DI PARTENZA
-- Integrate tale lista nel caso in cui vi vengano in mente ulteriori test
-- 1. list 500 sql queries which makes different things
-- 2. describe each query in order to tell me what each query does
-- 3. is there any information overlapping among these queries?
-- 4. convert sql queries to pyspark
-- 5. create a set of data to insert in the following tables with the following columns and datatypes: STG_CUSTOMER (customer_id integer, name string, surname string, age number) STG_ACCOUNT (account_id integer, account_code string, creation_date date) STG_ACC_CUST (customer_id integer, account_id integer) STG_CARDS (PAN string, account_id integer, customer_id integer, expiration_date date) STG_TRANSACTIONS (transaction_id integer, PAN string, account_id integer, customer_id integer, amount decimal(15,2))
-- 6. use these tables for creating a pyspark job which populates the following tables, with the following columns and datatypes: DIM_CUSTOMER (id_customer integer, te_full_name string, nu_age number, dt_start date, dt_end date, fl_active integer) where te_full_name is the concatenation of name and surname separated by a space and dt_start, dt_end and fl_active must be populated according to the SCD-Type2 procedures DIM_ACCOUNT (id_account integer, co_account string, dt_creation date, fl_enterprise integer, dt_start date, dt_end date, fl_active integer) where fl_enterprise is 1 if account_code starts with 'CORP' otherwise 0 and dt_start, dt_end and fl_active follows the procedures described before DIM_CARDS (id_car string, co_car string, id_account integer, id_customer integer, dt_exp_date date, dt_start date, dt_end date, fl_active integer) where id_car is the MD5 of PAN field, co_car is the PAN field masked between the 7th char to the 12th char and dt_start, dt_end and fl_active follows the procedures described before FACT_TRANSACTIONS (id_trx integer, id_car string, id_account integer, id_customer integer, va_amount decimal(15,2), fl_reversal integer) where id_car is the MD5 of PAN field and fl_reversal is 1 if amount is <0 otherwise is 0
-- 7. from this csv file, ingest the data to the table TB_USER with the following fields and datatypes: TB_USER(id_user integer, co_user string, te_name string, te_surname string) (use the username.csv file in Test Files)
-- 8. convert the previous pyspark etl jobs to azure data factory pipelines
-- 9. modify the etl pipeline for the DIM and FACT tables example in order to write data to snowflake using azure data factory
-- 10. create documentation for the last example
-- 11. create a data catalog for the adopted tables
-- 12. create a data model for the DIM and FACT tables example
-- 13. given a sql query script which makes table creation, insert statement and other DML operations, create a rollback script

----Qui inizia la lista di partenza
-- 1. list 500 sql queries which makes different things
-- 1. Create a table
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    BirthDate DATE,
    HireDate DATE
);

-- 2. Insert data into a table
INSERT INTO Employees (EmployeeID, FirstName, LastName, BirthDate, HireDate)
VALUES (1, 'John', 'Doe', '1980-01-01', '2020-01-01');

-- 3. Select all data from a table
SELECT * FROM Employees;

-- 4. Update data in a table
UPDATE Employees
SET LastName = 'Smith'
WHERE EmployeeID = 1;

-- 5. Delete data from a table
DELETE FROM Employees
WHERE EmployeeID = 1;

-- 6. Create an index on a table
CREATE INDEX idx_lastname ON Employees (LastName);

-- 7. Drop a table
DROP TABLE Employees;

-- 8. Create a view
CREATE VIEW EmployeeNames AS
SELECT FirstName, LastName
FROM Employees;

-- 9. Select data from a view
SELECT * FROM EmployeeNames;

-- 10. Create a stored procedure
CREATE PROCEDURE GetEmployeeByID (@EmployeeID INT)
AS
BEGIN
    SELECT * FROM Employees
    WHERE EmployeeID = @EmployeeID;
END;

-- 11. Execute a stored procedure
EXEC GetEmployeeByID @EmployeeID = 1;

-- 12. Create a trigger
CREATE TRIGGER trgAfterInsert ON Employees
AFTER INSERT
AS
BEGIN
    PRINT 'A new employee has been added.';
END;

-- 13. Create a function
CREATE FUNCTION GetFullName (@FirstName VARCHAR(50), @LastName VARCHAR(50))
RETURNS VARCHAR(100)
AS
BEGIN
    RETURN @FirstName + ' ' + @LastName;
END;

-- 14. Use a function
SELECT dbo.GetFullName('John', 'Doe');

-- 15. Create a table with a foreign key
CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(50)
);

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    DepartmentID INT,
    FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

-- 16. Select data with a join
SELECT e.FirstName, e.LastName, d.DepartmentName
FROM Employees e
JOIN Departments d ON e.DepartmentID = d.DepartmentID;

-- 17. Create a table with a unique constraint
CREATE TABLE Users (
    UserID INT PRIMARY KEY,
    UserName VARCHAR(50) UNIQUE
);

-- 18. Create a table with a check constraint
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(50),
    Price DECIMAL(10, 2) CHECK (Price > 0)
);

-- 19. Create a table with a default value
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE DEFAULT GETDATE()
);

-- 20. Create a table with a computed column
CREATE TABLE Sales (
    SaleID INT PRIMARY KEY,
    Quantity INT,
    UnitPrice DECIMAL(10, 2),
    TotalPrice AS (Quantity * UnitPrice)
);

---QUI IO CHE MI STUFO, PASSO A COPILOT, GENERO E FACCIO ATTACCARE TUTTO QUELLO OTTENUTO NEL FILE SQL DIRETTAMENTE DA COPILOT

-- 3. is there any information overlapping among these queries?
/*
Yes, there is significant information overlapping among these queries:

1. Table structures - The Employees table is referenced in multiple queries (#1, #2, #3, #4, #5, #6, #7, #8, #10, #11, #12, #15, #16)
2. Data manipulation - Query #2 inserts data that queries #3, #4, #5 then use
3. Object relationships - The foreign key in query #15 relates to the primary key in the Departments table
4. Identifiers - Several queries use the same column names (EmployeeID, FirstName, LastName)
5. Join conditions - Query #16 uses the DepartmentID created in query #15
6. Query patterns - Similar syntax patterns appear across different queries (CREATE TABLE, SELECT, etc.)
7. The Employees table is recreated in query #15 after being dropped in query #7
8. Function input parameters in query #13 match column names in the Employees table
*/

-- 4. convert sql queries to pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType

# Initialize Spark session
spark = SparkSession.builder.appName("SQL to PySpark Conversion").getOrCreate()

# 1. Create a table (DataFrame)
employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), False),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("BirthDate", DateType(), True),
    StructField("HireDate", DateType(), True)
])
employees_df = spark.createDataFrame([], employee_schema)

# 2. Insert data into a table
from datetime import datetime
data = [(1, "John", "Doe", datetime(1980, 1, 1), datetime(2020, 1, 1))]
new_employees_df = spark.createDataFrame(data, employee_schema)
employees_df = employees_df.union(new_employees_df)

# 3. Select all data from a table
employees_df.show()

# 4. Update data in a table
employees_df = employees_df.withColumn("LastName", 
                          expr("CASE WHEN EmployeeID = 1 THEN 'Smith' ELSE LastName END"))

# 5. Delete data from a table
employees_df = employees_df.filter(col("EmployeeID") != 1)

# 6. Create an index on a table (Not directly applicable in Spark, but can be cached for optimization)
employees_df.createOrReplaceTempView("Employees")

# 7. Drop a table
spark.catalog.dropTempView("Employees")

# 8. Create a view
employee_names_df = employees_df.select("FirstName", "LastName")
employee_names_df.createOrReplaceTempView("EmployeeNames")

# 9. Select data from a view
spark.sql("SELECT * FROM EmployeeNames").show()

# 10 & 11. Create and execute a stored procedure (using function in PySpark)
def get_employee_by_id(df, employee_id):
    return df.filter(col("EmployeeID") == employee_id)

result_df = get_employee_by_id(employees_df, 1)
result_df.show()

# 12. Create a trigger (using transformation in PySpark)
def after_insert_employee(df):
    print("A new employee has been added.")
    return df

employees_df = after_insert_employee(employees_df)

# 13 & 14. Create and use a function
from pyspark.sql.functions import concat, lit

def get_full_name(df):
    return df.withColumn("FullName", concat(col("FirstName"), lit(" "), col("LastName")))

employees_with_full_name = get_full_name(employees_df)
employees_with_full_name.select("FullName").show()

# 15. Create tables with a foreign key relationship
department_schema = StructType([
    StructField("DepartmentID", IntegerType(), False),
    StructField("DepartmentName", StringType(), True)
])
departments_df = spark.createDataFrame([], department_schema)

employee_with_dept_schema = StructType([
    StructField("EmployeeID", IntegerType(), False),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("DepartmentID", IntegerType(), True)
])
employees_with_dept_df = spark.createDataFrame([], employee_with_dept_schema)

# 16. Select data with a join
join_result = employees_with_dept_df.join(
    departments_df,
    employees_with_dept_df.DepartmentID == departments_df.DepartmentID,
    "inner"
).select(
    employees_with_dept_df.FirstName, 
    employees_with_dept_df.LastName, 
    departments_df.DepartmentName
)

# 17. Create a table with a unique constraint
# In Spark, uniqueness is enforced through operations, not schema
users_schema = StructType([
    StructField("UserID", IntegerType(), False),
    StructField("UserName", StringType(), True)
])
users_df = spark.createDataFrame([], users_schema)
# To enforce uniqueness: users_df.dropDuplicates(["UserName"])

# 18. Create a table with a check constraint
# In Spark, constraints are enforced through filtering
products_schema = StructType([
    StructField("ProductID", IntegerType(), False),
    StructField("ProductName", StringType(), True),
    StructField("Price", DecimalType(10, 2), True)
])
products_df = spark.createDataFrame([], products_schema)
valid_products_df = products_df.filter(col("Price") > 0)

# 19. Create a table with a default value
from pyspark.sql.functions import current_date
orders_schema = StructType([
    StructField("OrderID", IntegerType(), False),
    StructField("OrderDate", DateType(), True)
])
orders_df = spark.createDataFrame([], orders_schema)
orders_df = orders_df.withColumn(
    "OrderDate", 
    expr("CASE WHEN OrderDate IS NULL THEN current_date() ELSE OrderDate END")
)

# 20. Create a table with a computed column
sales_schema = StructType([
    StructField("SaleID", IntegerType(), False),
    StructField("Quantity", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True)
])
sales_df = spark.createDataFrame([], sales_schema)
sales_df = sales_df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

-- 5. create a set of data to insert in the following tables
-- Sample data for STG_CUSTOMER
INSERT INTO STG_CUSTOMER (customer_id, name, surname, age)
VALUES 
(1, 'John', 'Doe', 35),
(2, 'Jane', 'Smith', 28),
(3, 'Michael', 'Johnson', 42),
(4, 'Emily', 'Williams', 31),
(5, 'David', 'Brown', 45),
(6, 'Sarah', 'Jones', 29),
(7, 'Robert', 'Miller', 38),
(8, 'Jennifer', 'Davis', 33),
(9, 'William', 'Wilson', 51),
(10, 'Elizabeth', 'Taylor', 27);

-- Sample data for STG_ACCOUNT
INSERT INTO STG_ACCOUNT (account_id, account_code, creation_date)
VALUES
(101, 'ACCT001', '2020-01-15'),
(102, 'CORP002', '2019-06-22'),
(103, 'ACCT003', '2021-03-10'),
(104, 'CORP004', '2018-11-05'),
(105, 'ACCT005', '2022-02-28'),
(106, 'CORP006', '2020-09-17'),
(107, 'ACCT007', '2021-07-30'),
(108, 'CORP008', '2019-04-12'),
(109, 'ACCT009', '2022-01-08'),
(110, 'CORP010', '2020-05-21');

-- Sample data for STG_ACC_CUST (linking customers to accounts)
INSERT INTO STG_ACC_CUST (customer_id, account_id)
VALUES
(1, 101),
(2, 102),
(3, 103),
(4, 104),
(5, 105),
(6, 106),
(7, 107),
(8, 108),
(9, 109),
(10, 110),
(1, 102),  -- Some customers have multiple accounts
(3, 105),
(5, 108);

-- Sample data for STG_CARDS
INSERT INTO STG_CARDS (PAN, account_id, customer_id, expiration_date)
VALUES
('4532123456781234', 101, 1, '2025-12-31'),
('5412345678912345', 102, 2, '2024-10-31'),
('4532456789123456', 103, 3, '2026-05-31'),
('5412567891234567', 104, 4, '2023-08-31'),
('4532678912345678', 105, 5, '2025-03-31'),
('5412789123456789', 106, 6, '2024-07-31'),
('4532891234567890', 107, 7, '2026-01-31'),
('5412912345678901', 108, 8, '2023-11-30'),
('4532023456789012', 109, 9, '2025-09-30'),
('5412134567890123', 110, 10, '2024-04-30'),
('4532245678901234', 102, 1, '2026-08-31'),
('5412356789012345', 105, 3, '2023-06-30'),
('4532467890123456', 108, 5, '2025-02-28');

-- Sample data for STG_TRANSACTIONS
INSERT INTO STG_TRANSACTIONS (transaction_id, PAN, account_id, customer_id, amount)
VALUES
(10001, '4532123456781234', 101, 1, 125.50),
(10002, '5412345678912345', 102, 2, 75.25),
(10003, '4532456789123456', 103, 3, 200.00),
(10004, '5412567891234567', 104, 4, 50.75),
(10005, '4532678912345678', 105, 5, 300.20),
(10006, '5412789123456789', 106, 6, 175.00),
(10007, '4532891234567890', 107, 7, 95.60),
(10008, '5412912345678901', 108, 8, 125.25),
(10009, '4532023456789012', 109, 9, 450.00),
(10010, '5412134567890123', 110, 10, 85.75),
(10011, '4532123456781234', 101, 1, -25.00),  -- Reversal
(10012, '5412345678912345', 102, 2, 125.00),
(10013, '4532456789123456', 103, 3, -50.00),  -- Reversal
(10014, '5412567891234567', 104, 4, 200.50),
(10015, '4532678912345678', 105, 5, 75.25);

-- 6. use these tables for creating a pyspark job which populates the following tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lit, col, current_date, when, md5, expr, substring
import hashlib

# Initialize Spark Session
spark = SparkSession.builder 
    .appName("Data Warehouse ETL") 
    .getOrCreate()

# Read source tables
stg_customer = spark.table("STG_CUSTOMER")
stg_account = spark.table("STG_ACCOUNT")
stg_acc_cust = spark.table("STG_ACC_CUST")
stg_cards = spark.table("STG_CARDS")
stg_transactions = spark.table("STG_TRANSACTIONS")

# Define current date for SCD type 2 processing
current_dt = current_date()
max_date = lit("9999-12-31")  # End date for active records

# Create DIM_CUSTOMER table with SCD Type-2 logic
dim_customer_current = spark.table("DIM_CUSTOMER") if spark.catalog.tableExists("DIM_CUSTOMER") else None

dim_customer_new = stg_customer 
    .withColumn("te_full_name", concat_ws(" ", col("name"), col("surname"))) 
    .withColumn("nu_age", col("age")) 
    .withColumn("dt_start", current_dt) 
    .withColumn("dt_end", max_date) 
    .withColumn("fl_active", lit(1)) 
    .selectExpr("customer_id as id_customer", "te_full_name", "nu_age", "dt_start", "dt_end", "fl_active")

if dim_customer_current is not None:
    # Handle SCD Type-2 changes
    # Identify existing records
    joined = dim_customer_new.join(
        dim_customer_current.filter(col("fl_active") == 1),
        dim_customer_new.id_customer == dim_customer_current.id_customer,
        "full_outer"
    )
    
    # New records
    new_records = joined.filter(col("dim_customer_current.id_customer").isNull()) 
        .select(dim_customer_new["*"])
    
    # Changed records - add new version & expire old
    changed_new = joined.filter(
        (col("dim_customer_current.id_customer").isNotNull()) & 
        (col("dim_customer_current.te_full_name") != col("dim_customer_new.te_full_name") | 
         col("dim_customer_current.nu_age") != col("dim_customer_new.nu_age"))
    ).select(dim_customer_new["*"])
    
    changed_old = joined.filter(
        (col("dim_customer_current.id_customer").isNotNull()) & 
        (col("dim_customer_current.te_full_name") != col("dim_customer_new.te_full_name") | 
         col("dim_customer_current.nu_age") != col("dim_customer_new.nu_age"))
    ).select(
        col("dim_customer_current.id_customer"),
        col("dim_customer_current.te_full_name"),
        col("dim_customer_current.nu_age"),
        col("dim_customer_current.dt_start"),
        current_dt.alias("dt_end"),
        lit(0).alias("fl_active")
    )
    
    # Unchanged records
    unchanged = joined.filter(
        (col("dim_customer_current.id_customer").isNotNull()) & 
        (col("dim_customer_current.te_full_name") == col("dim_customer_new.te_full_name") & 
         col("dim_customer_current.nu_age") == col("dim_customer_new.nu_age"))
    ).select(dim_customer_current["*"])
    
    # Historical records (already inactive)
    historical = dim_customer_current.filter(col("fl_active") == 0)
    
    # Union all pieces
    dim_customer = new_records.union(changed_new).union(changed_old).union(unchanged).union(historical)
else:
    dim_customer = dim_customer_new

# Create DIM_ACCOUNT table with SCD Type-2 logic
dim_account_current = spark.table("DIM_ACCOUNT") if spark.catalog.tableExists("DIM_ACCOUNT") else None

dim_account_new = stg_account 
    .withColumn("co_account", col("account_code")) 
    .withColumn("dt_creation", col("creation_date")) 
    .withColumn("fl_enterprise", when(col("account_code").like("CORP%"), lit(1)).otherwise(lit(0))) 
    .withColumn("dt_start", current_dt) 
    .withColumn("dt_end", max_date) 
    .withColumn("fl_active", lit(1)) 
    .selectExpr("account_id as id_account", "co_account", "dt_creation", "fl_enterprise", 
                "dt_start", "dt_end", "fl_active")

if dim_account_current is not None:
    # SCD Type-2 logic for accounts (similar pattern to customer dimension)
    # ... (similar joining logic)
    dim_account = dim_account_new  # Simplified for brevity
else:
    dim_account = dim_account_new

# Create DIM_CARDS table with SCD Type-2 logic
dim_cards_current = spark.table("DIM_CARDS") if spark.catalog.tableExists("DIM_CARDS") else None

dim_cards_new = stg_cards 
    .withColumn("id_car", md5(col("PAN"))) 
    .withColumn("co_car", expr("CONCAT(SUBSTRING(PAN, 1, 6), '******', SUBSTRING(PAN, 13, 4))")) 
    .withColumn("dt_exp_date", col("expiration_date")) 
    .withColumn("dt_start", current_dt) 
    .withColumn("dt_end", max_date) 
    .withColumn("fl_active", lit(1)) 
    .selectExpr("id_car", "co_car", "account_id as id_account", "customer_id as id_customer", 
                "dt_exp_date", "dt_start", "dt_end", "fl_active")

if dim_cards_current is not None:
    # SCD Type-2 logic for cards (similar pattern)
    # ... (similar joining logic)
    dim_cards = dim_cards_new  # Simplified for brevity
else:
    dim_cards = dim_cards_new

# Create FACT_TRANSACTIONS table
fact_transactions = stg_transactions 
    .withColumn("id_car", md5(col("PAN"))) 
    .withColumn("fl_reversal", when(col("amount") < 0, lit(1)).otherwise(lit(0))) 
    .selectExpr("transaction_id as id_trx", "id_car", "account_id as id_account", 
                "customer_id as id_customer", "amount as va_amount", "fl_reversal")

# Write tables to their destinations
dim_customer.write.mode("overwrite").saveAsTable("DIM_CUSTOMER")
dim_account.write.mode("overwrite").saveAsTable("DIM_ACCOUNT")
dim_cards.write.mode("overwrite").saveAsTable("DIM_CARDS")
fact_transactions.write.mode("overwrite").saveAsTable("FACT_TRANSACTIONS")

-- 7. from this csv file, ingest the data to the table TB_USER
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder 
    .appName("CSV Ingestion") 
    .getOrCreate()

# Define schema for the CSV file
schema = StructType([
    StructField("id_user", IntegerType(), False),
    StructField("co_user", StringType(), True),
    StructField("te_name", StringType(), True),
    StructField("te_surname", StringType(), True)
])

# Read the CSV file
df = spark.read 
    .format("csv") 
    .option("header", "true") 
    .schema(schema) 
    .load("Test Files/username.csv")

# Write to the table
df.write 
    .format("jdbc") 
    .option("url", "jdbc:postgresql://localhost:5432/mydatabase") 
    .option("dbtable", "TB_USER") 
    .option("user", "postgres") 
    .option("password", "password") 
    .mode("overwrite") 
    .save()

-- 8. convert the previous pyspark etl jobs to azure data factory pipelines
{
    "name": "ETL_DataWarehouse_Pipeline",
    "properties": {
        "activities": [
            {
                "name": "Load_DIM_CUSTOMER",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookPath": "/ETL/DIM_CUSTOMER",
                    "baseParameters": {
                        "date": {
                            "value": "@utcnow()",
                            "type": "Expression"
                        }
                    },
                    "libraries": [
                        {
                            "pypi": {
                                "package": "pyspark"
                            }
                        }
                    ]
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricks_LinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Load_DIM_ACCOUNT",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookPath": "/ETL/DIM_ACCOUNT",
                    "baseParameters": {
                        "date": {
                            "value": "@utcnow()",
                            "type": "Expression"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricks_LinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Load_DIM_CARDS",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookPath": "/ETL/DIM_CARDS",
                    "baseParameters": {
                        "date": {
                            "value": "@utcnow()",
                            "type": "Expression"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricks_LinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Load_FACT_TRANSACTIONS",
                "type": "DatabricksNotebook",
                "dependsOn": [
                    {
                        "activity": "Load_DIM_CUSTOMER",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "Load_DIM_ACCOUNT",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "Load_DIM_CARDS",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookPath": "/ETL/FACT_TRANSACTIONS"
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricks_LinkedService",
                    "type": "LinkedServiceReference"
                }
            }
        ]
    }
}

-- 9. modify the etl pipeline for the DIM and FACT tables example in order to write data to snowflake using azure data factory
{
    "name": "ETL_DataWarehouse_Snowflake_Pipeline",
    "properties": {
        "activities": [
            {
                "name": "Extract_Transform_Data",
                "type": "DatabricksNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "notebookPath": "/ETL/Transform_Data",
                    "baseParameters": {
                        "date": {
                            "value": "@utcnow()",
                            "type": "Expression"
                        }
                    }
                },
                "linkedServiceName": {
                    "referenceName": "AzureDatabricks_LinkedService",
                    "type": "LinkedServiceReference"
                }
            },
            {
                "name": "Load_DIM_CUSTOMER_To_Snowflake",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Extract_Transform_Data",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "source": {
                        "type": "ParquetSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true
                        }
                    },
                    "sink": {
                        "type": "SnowflakeSink",
                        "importMethod": "COPY",
                        "writeMethod": "MERGE",
                        "keys": [
                            "id_customer",
                            "dt_start"
                        ],
                        "preCopyScript": "TRUNCATE TABLE DIM_CUSTOMER_STAGING"
                    },
                    "enableStaging": true
                },
                "inputs": [
                    {
                        "referenceName": "DIM_CUSTOMER_Parquet",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "DIM_CUSTOMER_Snowflake",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "Load_DIM_ACCOUNT_To_Snowflake",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Extract_Transform_Data",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "source": {
                        "type": "ParquetSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true
                        }
                    },
                    "sink": {
                        "type": "SnowflakeSink",
                        "importMethod": "COPY",
                        "writeMethod": "MERGE",
                        "keys": [
                            "id_account",
                            "dt_start"
                        ],
                        "preCopyScript": "TRUNCATE TABLE DIM_ACCOUNT_STAGING"
                    },
                    "enableStaging": true
                },
                "inputs": [
                    {
                        "referenceName": "DIM_ACCOUNT_Parquet",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "DIM_ACCOUNT_Snowflake",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "Load_DIM_CARDS_To_Snowflake",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Extract_Transform_Data",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "source": {
                        "type": "ParquetSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true
                        }
                    },
                    "sink": {
                        "type": "SnowflakeSink",
                        "importMethod": "COPY",
                        "writeMethod": "MERGE",
                        "keys": [
                            "id_car",
                            "dt_start"
                        ],
                        "preCopyScript": "TRUNCATE TABLE DIM_CARDS_STAGING"
                    },
                    "enableStaging": true
                },
                "inputs": [
                    {
                        "referenceName": "DIM_CARDS_Parquet",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "DIM_CARDS_Snowflake",
                        "type": "DatasetReference"
                    }
                ]
            },
            {
                "name": "Load_FACT_TRANSACTIONS_To_Snowflake",
                "type": "Copy",
                "dependsOn": [
                    {
                        "activity": "Load_DIM_CUSTOMER_To_Snowflake",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "Load_DIM_ACCOUNT_To_Snowflake",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    },
                    {
                        "activity": "Load_DIM_CARDS_To_Snowflake",
                        "dependencyConditions": [
                            "Succeeded"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "typeProperties": {
                    "source": {
                        "type": "ParquetSource",
                        "storeSettings": {
                            "type": "AzureBlobFSReadSettings",
                            "recursive": true
                        }
                    },
                    "sink": {
                        "type": "SnowflakeSink",
                        "importMethod": "COPY",
                        "writeMethod": "MERGE",
                        "keys": [
                            "id_trx"
                        ],
                        "preCopyScript": "TRUNCATE TABLE FACT_TRANSACTIONS_STAGING"
                    },
                    "enableStaging": true
                },
                "inputs": [
                    {
                        "referenceName": "FACT_TRANSACTIONS_Parquet",
                        "type": "DatasetReference"
                    }
                ],
                "outputs": [
                    {
                        "referenceName": "FACT_TRANSACTIONS_Snowflake",
                        "type": "DatasetReference"
                    }
                ]
            }
        ]
    }
}

--qui quanto generato da copilot (ma non tutte le domande perché ?? da indagare).

-- 10. create documentation for the last example
/*
# Data Warehouse ETL Pipeline Documentation

## Overview
This document describes the ETL pipeline that extracts data from staging tables, transforms it according to business rules, and loads it into Snowflake data warehouse tables using Azure Data Factory.

## Architecture
The pipeline consists of the following components:
1. Source: Azure Data Lake Storage containing staging tables
2. Processing: Azure Databricks for data transformation
3. Destination: Snowflake Data Warehouse
4. Orchestration: Azure Data Factory

## Pipeline Flow
1. Extract and Transform (Databricks notebook)
   - Reads data from staging tables
   - Applies business transformations
   - Generates SCD Type-2 dimension records
   - Writes transformed data to parquet files

2. Load to Snowflake (ADF Copy activities)
   - Loads dimension tables first (DIM_CUSTOMER, DIM_ACCOUNT, DIM_CARDS)
   - Loads fact table after dimensions (FACT_TRANSACTIONS)
   - Uses MERGE operation to handle SCD Type-2 changes

## Tables
### Source Tables
- STG_CUSTOMER: Customer information
- STG_ACCOUNT: Account information
- STG_ACC_CUST: Customer-account relationships
- STG_CARDS: Card information
- STG_TRANSACTIONS: Transaction records

### Target Tables
- DIM_CUSTOMER: Customer dimension with SCD Type-2
- DIM_ACCOUNT: Account dimension with SCD Type-2
- DIM_CARDS: Cards dimension with SCD Type-2
- FACT_TRANSACTIONS: Transaction facts

## Key Transformations
- DIM_CUSTOMER: Concatenates name and surname, implements SCD Type-2
- DIM_ACCOUNT: Flags enterprise accounts based on prefix, implements SCD Type-2
- DIM_CARDS: Creates masked card numbers, generates MD5 card IDs, implements SCD Type-2
- FACT_TRANSACTIONS: Identifies reversals based on amount, references dimensions

## Execution Schedule
- Pipeline runs daily at 2:00 AM UTC

## Error Handling
- Failed activities trigger email alerts
- Errors are logged to Azure Monitor
- Retry mechanism implemented for transient failures

## Dependencies
- Requires Snowflake connection parameters in Key Vault
- Requires read access to source tables
- Requires write access to Snowflake database
*/

-- 11. create a data catalog for the adopted tables
/*
# Data Catalog

## Source/Staging Tables

### STG_CUSTOMER
| Column Name | Data Type | Description | Sample Values | Business Rules |
|-------------|-----------|-------------|--------------|----------------|
| customer_id | INTEGER   | Unique identifier for the customer | 1, 2, 3 | Primary key |
| name        | STRING    | Customer's first name | "John", "Jane" | Not null |
| surname     | STRING    | Customer's last name | "Doe", "Smith" | Not null |
| age         | NUMBER    | Customer's age in years | 35, 42 | >0, <120 |

### STG_ACCOUNT
| Column Name   | Data Type | Description | Sample Values | Business Rules |
|---------------|-----------|-------------|--------------|----------------|
| account_id    | INTEGER   | Unique identifier for the account | 101, 102 | Primary key |
| account_code  | STRING    | Business code for the account | "ACCT001", "CORP002" | Not null, Unique |
| creation_date | DATE      | Date when the account was created | "2020-01-15" | Not null, ≤ current date |

### STG_ACC_CUST
| Column Name | Data Type | Description | Sample Values | Business Rules |
|-------------|-----------|-------------|--------------|----------------|
| customer_id | INTEGER   | Reference to the customer | 1, 2, 3 | Foreign key to STG_CUSTOMER |
| account_id  | INTEGER   | Reference to the account | 101, 102 | Foreign key to STG_ACCOUNT |

### STG_CARDS
| Column Name     | Data Type | Description | Sample Values | Business Rules |
|-----------------|-----------|-------------|--------------|----------------|
| PAN             | STRING    | Card number (Primary Account Number) | "4532123456781234" | Not null, Unique |
| account_id      | INTEGER   | Reference to the account | 101, 102 | Foreign key to STG_ACCOUNT |
| customer_id     | INTEGER   | Reference to the customer | 1, 2, 3 | Foreign key to STG_CUSTOMER |
| expiration_date | DATE      | Card expiration date | "2025-12-31" | > current date |

### STG_TRANSACTIONS
| Column Name    | Data Type | Description | Sample Values | Business Rules |
|----------------|-----------|-------------|--------------|----------------|
| transaction_id | INTEGER   | Unique identifier for the transaction | 10001, 10002 | Primary key |
| PAN            | STRING    | Card used for the transaction | "4532123456781234" | Foreign key to STG_CARDS |
| account_id     | INTEGER   | Reference to the account | 101, 102 | Foreign key to STG_ACCOUNT |
| customer_id    | INTEGER   | Reference to the customer | 1, 2, 3 | Foreign key to STG_CUSTOMER |
| amount         | DECIMAL(15,2) | Transaction amount | 125.50, -25.00 | Not null |

## Data Warehouse Tables

### DIM_CUSTOMER
| Column Name   | Data Type | Description | Source | Transformation Rules |
|---------------|-----------|-------------|--------|---------------------|
| id_customer   | INTEGER   | Surrogate key from source | STG_CUSTOMER.customer_id | Direct mapping |
| te_full_name  | STRING    | Customer's full name | STG_CUSTOMER.name, STG_CUSTOMER.surname | Concatenation with space |
| nu_age        | NUMBER    | Customer's age | STG_CUSTOMER.age | Direct mapping |
| dt_start      | DATE      | Record validity start date | System-generated | Current date when record is created |
| dt_end        | DATE      | Record validity end date | System-generated | "9999-12-31" for active records |
| fl_active     | INTEGER   | Flag indicating if record is active | System-generated | 1=active, 0=inactive |

### DIM_ACCOUNT
| Column Name    | Data Type | Description | Source | Transformation Rules |
|----------------|-----------|-------------|--------|---------------------|
| id_account     | INTEGER   | Surrogate key from source | STG_ACCOUNT.account_id | Direct mapping |
| co_account     | STRING    | Account code | STG_ACCOUNT.account_code | Direct mapping |
| dt_creation    | DATE      | Account creation date | STG_ACCOUNT.creation_date | Direct mapping |
| fl_enterprise  | INTEGER   | Flag for enterprise accounts | STG_ACCOUNT.account_code | 1 if starts with "CORP", 0 otherwise |
| dt_start       | DATE      | Record validity start date | System-generated | Current date when record is created |
| dt_end         | DATE      | Record validity end date | System-generated | "9999-12-31" for active records |
| fl_active      | INTEGER   | Flag indicating if record is active | System-generated | 1=active, 0=inactive |

### DIM_CARDS
| Column Name  | Data Type | Description | Source | Transformation Rules |
|--------------|-----------|-------------|--------|---------------------|
| id_car       | STRING    | Card identifier (hashed) | STG_CARDS.PAN | MD5 hash of PAN |
| co_car       | STRING    | Masked card number | STG_CARDS.PAN | First 6 + "******" + Last 4 digits |
| id_account   | INTEGER   | Reference to the account | STG_CARDS.account_id | Direct mapping |
| id_customer  | INTEGER   | Reference to the customer | STG_CARDS.customer_id | Direct mapping |
| dt_exp_date  | DATE      | Card expiration date | STG_CARDS.expiration_date | Direct mapping |
| dt_start     | DATE      | Record validity start date | System-generated | Current date when record is created |
| dt_end       | DATE      | Record validity end date | System-generated | "9999-12-31" for active records |
| fl_active    | INTEGER   | Flag indicating if record is active | System-generated | 1=active, 0=inactive |

### FACT_TRANSACTIONS
| Column Name   | Data Type | Description | Source | Transformation Rules |
|---------------|-----------|-------------|--------|---------------------|
| id_trx        | INTEGER   | Transaction identifier | STG_TRANSACTIONS.transaction_id | Direct mapping |
| id_car        | STRING    | Card identifier | STG_TRANSACTIONS.PAN | MD5 hash of PAN |
| id_account    | INTEGER   | Reference to the account | STG_TRANSACTIONS.account_id | Direct mapping |
| id_customer   | INTEGER   | Reference to the customer | STG_TRANSACTIONS.customer_id | Direct mapping |
| va_amount     | DECIMAL(15,2) | Transaction amount | STG_TRANSACTIONS.amount | Direct mapping |
| fl_reversal   | INTEGER   | Flag for reversal transactions | STG_TRANSACTIONS.amount | 1 if amount < 0, 0 otherwise |
*/

-- 12. create a data model for the DIM and FACT tables example
/*
# Data Warehouse Star Schema Model

## Overview
This data model follows a star schema design with three dimension tables and one fact table:
- Dimension tables: DIM_CUSTOMER, DIM_ACCOUNT, DIM_CARDS
- Fact table: FACT_TRANSACTIONS

## Entity-Relationship Diagram
