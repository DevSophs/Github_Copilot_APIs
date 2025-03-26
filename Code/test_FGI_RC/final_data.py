from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    concat_ws, lit, when, current_date, md5, 
    regexp_replace, col, substring, lead, year
)
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Data Warehouse Loading") \
    .getOrCreate()

# Read staging tables
stg_customer = spark.table("STG_CUSTOMER")
stg_account = spark.table("STG_ACCOUNT")
stg_acc_cust = spark.table("STG_ACC_CUST")
stg_cards = spark.table("STG_CARDS")
stg_transactions = spark.table("STG_TRANSACTIONS")

# DIM_CUSTOMER transformation
dim_customer = stg_customer \
    .withColumn("te_full_name", concat_ws(" ", col("name"), col("surname"))) \
    .withColumn("dt_start", current_date()) \
    .withColumn("dt_end", lit("9999-12-31")) \
    .withColumn("fl_active", lit(1)) \
    .select(
        col("customer_id").alias("id_customer"),
        "te_full_name",
        col("age").alias("nu_age"),
        "dt_start",
        "dt_end",
        "fl_active"
    )

# DIM_ACCOUNT transformation
dim_account = stg_account \
    .withColumn("fl_enterprise", when(col("account_code").startswith("CORP"), 1).otherwise(0)) \
    .withColumn("dt_start", current_date()) \
    .withColumn("dt_end", lit("9999-12-31")) \
    .withColumn("fl_active", lit(1)) \
    .select(
        col("account_id").alias("id_account"),
        col("account_code").alias("co_account"),
        "creation_date",
        "fl_enterprise",
        "dt_start",
        "dt_end",
        "fl_active"
    )

# DIM_CARDS transformation
dim_cards = stg_cards \
    .withColumn("id_car", md5(col("PAN"))) \
    .withColumn("co_car", 
        concat(
            substring(col("PAN"), 1, 6),
            lit("XXXXXX"),
            substring(col("PAN"), 13, 4)
        )
    ) \
    .withColumn("dt_start", current_date()) \
    .withColumn("dt_end", lit("9999-12-31")) \
    .withColumn("fl_active", lit(1)) \
    .select(
        "id_car",
        "co_car",
        col("account_id").alias("id_account"),
        col("customer_id").alias("id_customer"),
        col("expiration_date").alias("dt_exp_date"),
        "dt_start",
        "dt_end",
        "fl_active"
    )

# FACT_TRANSACTIONS transformation
fact_transactions = stg_transactions \
    .withColumn("id_car", md5(col("PAN"))) \
    .withColumn("fl_reversal", when(col("amount") < 0, 1).otherwise(0)) \
    .select(
        col("transaction_id").alias("id_trx"),
        "id_car",
        col("account_id").alias("id_account"),
        col("customer_id").alias("id_customer"),
        col("amount").alias("va_amount"),
        "fl_reversal"
    )

# Write to target tables
dim_customer.write.mode("overwrite").saveAsTable("DIM_CUSTOMER")
dim_account.write.mode("overwrite").saveAsTable("DIM_ACCOUNT")
dim_cards.write.mode("overwrite").saveAsTable("DIM_CARDS")
fact_transactions.write.mode("overwrite").saveAsTable("FACT_TRANSACTIONS")