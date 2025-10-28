from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when, to_date, sum as _sum, avg, countDistinct, to_timestamp, isnan, when, count
import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
spark = SparkSession.builder \
    .appName("RetailLakehouseLocalETL") \
    .master("local[*]") \
    .getOrCreate()


order_items_cleaned = order_items \
    .withColumn("id", col("id").cast(IntegerType())) \
    .withColumn("order_id", col("order_id").cast(IntegerType())) \
    .withColumn("user_id", col("user_id").cast(IntegerType())) \
    .withColumn("days_since_prior_order", 
                when(col("days_since_prior_order").isNull(), 0)
                .otherwise(col("days_since_prior_order").cast(IntegerType()))) \
    .withColumn("product_id", col("product_id").cast(IntegerType())) \
    .withColumn("add_to_cart_order", col("add_to_cart_order").cast(IntegerType())) \
    .withColumn("reordered", col("reordered").cast(BooleanType())) \
    .withColumn("order_timestamp", 
                when(col("order_timestamp").isNotNull(), 
                     to_timestamp(col("order_timestamp")))
                .otherwise(lit(None).cast(TimestampType()))) \
    .withColumn("date", 
                when(col("date").isNotNull(), to_date(col("date")))
                .otherwise(to_date(col("order_timestamp")))) \
    .withColumn("processed_at", lit(current_timestamp())) \
    .withColumn("data_quality_flag", 
                when((col("id").isNull()) | (col("order_id").isNull()) | (col("product_id").isNull()), 
                     lit("INVALID"))
                .otherwise(lit("VALID")))

order_items_deduped = order_items_cleaned \
    .dropDuplicates(["id", "order_id"])

order_items_final = order_items_deduped.filter(col("data_quality_flag") == "VALID")

print(f"Cleaned order_items count: {order_items_final.count()}")
print(f"Duplicates removed: {order_items_deduped.count() - order_items_final.count()}")



orders_cleaned = orders \
    .withColumn("order_num", col("order_num").cast(IntegerType())) \
    .withColumn("order_id", col("order_id").cast(IntegerType())) \
    .withColumn("user_id", col("user_id").cast(IntegerType())) \
    .withColumn("order_timestamp", 
                when(col("order_timestamp").isNotNull(), 
                     to_timestamp(col("order_timestamp")))
                .otherwise(lit(None).cast(TimestampType()))) \
    .withColumn("total_amount", 
                when(col("total_amount").isNull(), 0.0)
                .otherwise(col("total_amount").cast(DoubleType()))) \
    .withColumn("date", 
                when(col("date").isNotNull(), to_date(col("date")))
                .otherwise(to_date(col("order_timestamp")))) \
    .withColumn("data_quality_flag", 
                when((col("order_id").isNull()) | (col("user_id").isNull()), 
                     lit("INVALID"))
                .when(col("total_amount") < 0, lit("INVALID_AMOUNT"))
                .otherwise(lit("VALID")))

orders_deduped = orders_deduped \
    .dropDuplicates(["order_id"])

orders_final = orders_deduped.filter(col("data_quality_flag") == "VALID")

print(f"Cleaned orders count: {orders_final.count()}")
print(f"Records filtered: {orders.count() - orders_final.count()}")




products_cleaned = products \
    .withColumn("product_id", col("product_id").cast(IntegerType())) \
    .withColumn("department_id", col("department_id").cast(IntegerType())) \
    .withColumn("shelf_id", col("shelf_id").cast(IntegerType())) \
    .withColumn("department", 
                when(col("department").isNull(), lit("Unknown"))
                .otherwise(trim(col("department")))) \
    .withColumn("product_name", 
                when(col("product_name").isNull(), lit("Unknown Product"))
                .otherwise(trim(col("product_name")))) \
    .withColumn("product_name_clean", 
                regexp_replace(lower(col("product_name")), "[^a-z0-9\\s]", "")) \
    .withColumn("data_quality_flag", 
                when(col("product_id").isNull(), lit("INVALID"))
                .otherwise(lit("VALID")))

products_deduped = products_cleaned \
    .dropDuplicates(["product_id"])

products_final = products_deduped.filter(col("data_quality_flag") == "VALID")

print(f"Cleaned products count: {products_final.count()}")
print(f"Duplicates removed: {products.count() - products_final.count()}")
