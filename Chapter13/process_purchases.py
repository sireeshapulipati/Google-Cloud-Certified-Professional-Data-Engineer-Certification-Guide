from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .getOrCreate()

# Load the data
input_path = "gs://ecommerce-data-bucket-<unique-suffix>/customer_purchases.csv"
purchases_df = spark.read.csv(input_path, header=True, inferSchema=True)

# Data transformations
top_products = purchases_df.groupBy("product_id") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "total_sales") \
    .orderBy("total_sales", ascending=False)

avg_spending = purchases_df.groupBy("category") \
    .avg("amount") \
    .withColumnRenamed("avg(amount)", "average_spending")

# Write results to BigQuery
top_products.write.format("bigquery") \
    .option("table", "ecommerce_analysis.top_products") \
    .mode("overwrite") \
    .save()

avg_spending.write.format("bigquery") \
    .option("table", "ecommerce_analysis.avg_spending") \
    .mode("overwrite") \
    .save()

spark.stop()
