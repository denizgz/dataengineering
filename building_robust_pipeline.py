
# Clean the directory
dbutils.fs.rm("dbfs:/data/processed/gold_customer_transactions", True)

# Explicitly drop the table first to ensure no metadata exists in the catalog
spark.sql("DROP TABLE IF EXISTS gold_data.customer_transactions")

# Recreate the table
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold_data.customer_transactions
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    AS SELECT * FROM (SELECT NULL AS placeholder) WHERE 1=0
""")

# Read and transform data from the silver table
silver_df = spark.read.format("delta").table("silver_data.customer_transactions")

# Apply necessary transformations (e.g., filtering, deduplication, or aggregation)
silver_transformed_df = silver_df.dropDuplicates().filter("transaction_amount > 0")

# Read from the silver table
silver_df = spark.read.format("delta").table("silver_data.customer_transactions")

# Write the transformed data to the gold table
silver_transformed_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_data.customer_transactions")

# Write to the gold table
silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_data.customer_transactions")

print("Gold table created successfully.")