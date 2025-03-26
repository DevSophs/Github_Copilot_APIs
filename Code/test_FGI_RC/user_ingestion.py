from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace, monotonically_increasing_id

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("User Data Ingestion") \
    .getOrCreate()

# Define schema for the CSV file
schema = StructType([
    StructField("Username", StringType(), True),
    StructField("Identifier", StringType(), True),
    StructField("First_name", StringType(), True),
    StructField("Last_name", StringType(), True)
])

# Read CSV file
df_users = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .schema(schema) \
    .csv("c:/Users/UD436NS/OneDrive - EY/Desktop/Documents/Data Lab GenAI/GithHUB Copilot/repo/Github_Copilot_APIs/Data/username.csv")

# Transform data to match target table schema
tb_user = df_users \
    .select(
        col("Identifier").cast(IntegerType()).alias("id_user"),
        col("Username").alias("co_user"),
        col("First_name").alias("te_name"),
        col("Last_name").alias("te_surname")
    )

# Write to target table
tb_user.write.mode("overwrite").saveAsTable("TB_USER")

# Optional: Show the results
tb_user.show()