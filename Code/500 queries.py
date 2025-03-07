from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, year, sum, col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SQL Queries to PySpark") \
    .getOrCreate()

# Basic SELECT Queries
# 1. Select all employees
employees_df = spark.table("Employees")

# 2. Select specific columns
customers_names = spark.table("Customers").select("FirstName", "LastName")

# 3. Select distinct values
distinct_countries = spark.table("Suppliers").select("Country").distinct()

# 4. Count records
product_count = spark.table("Products").count()

# 5. Average price
avg_price = spark.table("Products").select(avg("Price").alias("AveragePrice"))

# Filtering Queries
# 6. Date filtering
recent_orders = spark.table("Orders").filter(col("OrderDate") >= "2024-01-01")

# 7. Price filtering
premium_products = spark.table("Products").filter(col("UnitPrice") > 50).select("ProductName")

# 8. Department filtering
sales_employees = spark.table("Employees").filter(col("Department") == "Sales")

# 9. Multiple country filtering
selected_customers = spark.table("Customers").filter(col("Country").isin("USA", "UK", "Canada"))

# 10. Not equal filtering
active_orders = spark.table("Orders").filter(col("OrderStatus") != "Cancelled")

# JOIN Queries
# 11. Inner join
order_customer = spark.table("Orders").alias("o") \
    .join(spark.table("Customers").alias("c"), 
          col("o.CustomerID") == col("c.CustomerID"), 
          "inner") \
    .select("o.OrderID", "c.CustomerName")

# 12. Left join
products_categories = spark.table("Products").alias("p") \
    .join(spark.table("Categories").alias("c"), 
          col("p.CategoryID") == col("c.CategoryID"), 
          "left") \
    .select("p.ProductName", "c.CategoryName")

# Aggregation Queries
# 13. Count by group
customers_by_country = spark.table("Customers") \
    .groupBy("Country") \
    .agg(count("*").alias("CustomerCount"))

# 14. Sum by year
yearly_orders = spark.table("Orders") \
    .groupBy(year("OrderDate").alias("Year")) \
    .agg(sum("OrderTotal").alias("TotalOrderValue"))