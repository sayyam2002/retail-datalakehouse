"""
Retail Lakehouse - Local Testing Script
========================================================
Pipeline: Raw -> Bronze -> Silver -> Gold -> Processed
- Raw: Original CSV files
- Bronze: Raw data loaded as-is with metadata
- Silver: Cleaned and validated data
- Gold: Business-ready aggregated data
- Processed: Archive for processed raw files

Requirements:
    pip install pyspark delta-spark
"""

import os
import sys
import shutil
import logging
from datetime import datetime

# Check required packages
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (col, to_date, trim, when, count, lit, 
                                       current_timestamp, input_file_name, sum as _sum, 
                                       avg, max as _max, min as _min, countDistinct)
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
    from delta import configure_spark_with_delta_pip, DeltaTable
except ImportError as e:
    print("\n" + "="*60)
    print("ERROR: Missing required packages!")
    print("="*60)
    print("\nPlease install required packages:")
    print("  pip install pyspark delta-spark")
    print("\nMissing package:", str(e))
    print("="*60 + "\n")
    sys.exit(1)

# ==================== CONFIGURATION ====================
# Local folder paths - UPDATE THESE PATHS
LOCAL_RAW_FOLDER = 'C:/path/to/your/local/folder'  # ← Your CSV files location
LOCAL_BRONZE_FOLDER = './lakehouse/bronze'          # Raw data with metadata
LOCAL_SILVER_FOLDER = './lakehouse/silver'          # Cleaned data
LOCAL_GOLD_FOLDER = './lakehouse/gold'              # Business aggregations
LOCAL_PROCESSED_FOLDER = './lakehouse/processed'    # Archive for processed files

# Create all folders
for folder in [LOCAL_RAW_FOLDER, LOCAL_BRONZE_FOLDER, LOCAL_SILVER_FOLDER, 
               LOCAL_GOLD_FOLDER, LOCAL_PROCESSED_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# File mappings (same structure as your S3 upload)
files_to_process = {
    'order_items_2024-06-06.csv': 'transactional/order_items/',
    'order_items_2024-06-07.csv': 'transactional/order_items/',
    'orders_2024-06-06.csv': 'transactional/orders/',
    'orders_2024-06-07.csv': 'transactional/orders/',
    'orders_2024-06-06-Updated.csv': 'transactional/orders/',
    'products.csv': 'product/'
}

# Table configurations
TABLE_CONFIG = {
    'orders': {
        'primary_key': ['order_id'],
        'partition_column': 'order_date',
        'files': [f for f in files_to_process.keys() if 'orders' in f and 'order_items' not in f]
    },
    'order_items': {
        'primary_key': ['order_id', 'id'],
        'partition_column': 'order_date',
        'files': [f for f in files_to_process.keys() if 'order_items' in f]
    },
    'products': {
        'primary_key': ['product_id'],
        'partition_column': None,
        'files': [f for f in files_to_process.keys() if 'products' in f]
    }
}

# ==================== LOGGING SETUP ====================
import sys

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('retail_lakehouse_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ==================== SPARK SESSION ====================
def create_spark_session():
    """Initialize Spark session with Delta Lake support"""
    try:
        # Set Java/Python paths for Windows
        import sys
        if sys.platform == 'win32':
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        builder = SparkSession.builder \
            .appName("RetailLakehousePipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.host", "localhost") \
            .master("local[*]")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")  # Reduce Spark logging
        logger.info("✓ Spark session created with Delta Lake support")
        return spark
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {e}")
        raise


# ==================== SCHEMA DEFINITIONS ====================
def get_bronze_schema(table_name):
    """Return schema for Bronze layer (raw data + metadata)"""
    base_schemas = {
        'orders': StructType([
            StructField("order_num", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("date", StringType(), True)
        ]),
        'order_items': StructType([
            StructField("id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("days_since_prior_order", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("add_to_cart_order", StringType(), True),
            StructField("reordered", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("date", StringType(), True)
        ]),
        'products': StructType([
            StructField("product_id", StringType(), True),
            StructField("department_id", StringType(), True),
            StructField("shelf_id", StringType(), True),
            StructField("department", StringType(), True),
            StructField("product_name", StringType(), True)
        ])
    }
    return base_schemas.get(table_name)


# ==================== BRONZE LAYER ====================
def load_to_bronze(spark, table_name, file_list):
    """
    BRONZE LAYER: Load raw CSV files as-is with metadata
    - Read CSV files without transformation
    - Add metadata columns (load_timestamp, source_file)
    - Store in Delta format
    """
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"BRONZE LAYER - Loading {table_name}")
        logger.info(f"{'='*60}")
        
        bronze_path = os.path.join(LOCAL_BRONZE_FOLDER, table_name)
        schema = get_bronze_schema(table_name)
        all_dfs = []
        
        for file_name in file_list:
            file_path = os.path.join(LOCAL_RAW_FOLDER, file_name)
            
            if not os.path.exists(file_path):
                logger.warning(f"✗ File not found: {file_path}")
                continue
            
            logger.info(f"Reading: {file_name}")
            
            # Read CSV as-is (all strings)
            df = spark.read \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .schema(schema) \
                .csv(file_path)
            
            # Add metadata columns
            df = df.withColumn("load_timestamp", current_timestamp()) \
                   .withColumn("source_file", lit(file_name))
            
            record_count = df.count()
            logger.info(f"  Loaded {record_count} records")
            all_dfs.append(df)
        
        if not all_dfs:
            raise ValueError(f"No valid files found for {table_name}")
        
        # Combine all dataframes
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.union(df)
        
        # Write to Bronze Delta table
        combined_df.write.format("delta").mode("overwrite").save(bronze_path)
        
        total_count = combined_df.count()
        logger.info(f"✓ Bronze layer created: {total_count} records")
        logger.info(f"  Location: {bronze_path}")
        
        return bronze_path
        
    except Exception as e:
        logger.error(f"✗ Bronze layer failed for {table_name}: {e}")
        raise


# ==================== SILVER LAYER ====================
def transform_to_silver(spark, table_name, bronze_path, primary_keys):
    """
    SILVER LAYER: Clean and validate data
    - Cast to proper data types
    - Remove nulls in primary keys
    - Handle nulls in other columns
    - Remove duplicates
    - Trim whitespace
    """
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"SILVER LAYER - Cleaning {table_name}")
        logger.info(f"{'='*60}")
        
        silver_path = os.path.join(LOCAL_SILVER_FOLDER, table_name)
        
        # Read from Bronze
        df = spark.read.format("delta").load(bronze_path)
        initial_count = df.count()
        logger.info(f"Initial records from Bronze: {initial_count}")
        
        # Data type transformations based on table
        if table_name == 'orders':
            df = df.withColumn("order_num", col("order_num").cast(IntegerType())) \
                   .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2))) \
                   .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType())) \
                   .withColumn("date", to_date(col("date"))) \
                   .withColumn("order_date", to_date(col("date")))
        
        elif table_name == 'order_items':
            df = df.withColumn("days_since_prior_order", col("days_since_prior_order").cast(IntegerType())) \
                   .withColumn("add_to_cart_order", col("add_to_cart_order").cast(IntegerType())) \
                   .withColumn("reordered", col("reordered").cast(IntegerType())) \
                   .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType())) \
                   .withColumn("date", to_date(col("date"))) \
                   .withColumn("order_date", to_date(col("date")))
        
        # Trim all string columns
        string_cols = [field.name for field in df.schema.fields 
                      if isinstance(field.dataType, StringType)]
        for col_name in string_cols:
            df = df.withColumn(col_name, trim(col(col_name)))
        
        # Check null values
        logger.info("Checking null values:")
        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        null_dict = null_counts.collect()[0].asDict()
        
        for col_name, null_count in null_dict.items():
            if null_count > 0:
                logger.warning(f"  {col_name}: {null_count} nulls")
        
        # Remove rows with null primary keys
        for pk in primary_keys:
            before = df.count()
            df = df.filter(col(pk).isNotNull())
            after = df.count()
            if before != after:
                logger.warning(f"Removed {before - after} rows with null {pk}")
        
        # Fill nulls in non-PK columns
        for field in df.schema.fields:
            col_name = field.name
            if col_name not in primary_keys and col_name not in ['load_timestamp', 'source_file']:
                if isinstance(field.dataType, (IntegerType, DecimalType)):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
                elif isinstance(field.dataType, StringType):
                    df = df.withColumn(col_name, when(col(col_name).isNull(), lit("UNKNOWN")).otherwise(col(col_name)))
        
        # Remove duplicates
        before_dedup = df.count()
        df = df.dropDuplicates(primary_keys)
        after_dedup = df.count()
        
        if before_dedup != after_dedup:
            logger.warning(f"Removed {before_dedup - after_dedup} duplicates")
        
        # Write to Silver Delta table
        writer = df.write.format("delta").mode("overwrite")
        if table_name in ['orders', 'order_items']:
            writer = writer.partitionBy("order_date")
        writer.save(silver_path)
        
        logger.info(f"✓ Silver layer created: {after_dedup} clean records")
        logger.info(f"  Location: {silver_path}")
        
        return silver_path
        
    except Exception as e:
        logger.error(f"✗ Silver layer failed for {table_name}: {e}")
        raise


# ==================== GOLD LAYER ====================
def create_gold_layer(spark):
    """
    GOLD LAYER: Business-ready aggregations and analytics
    - Daily sales summary
    - Product performance metrics
    - Customer insights
    """
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"GOLD LAYER - Creating Business Aggregations")
        logger.info(f"{'='*60}")
        
        # Read Silver tables
        orders_path = os.path.join(LOCAL_SILVER_FOLDER, "orders")
        items_path = os.path.join(LOCAL_SILVER_FOLDER, "order_items")
        products_path = os.path.join(LOCAL_SILVER_FOLDER, "products")
        
        orders_df = spark.read.format("delta").load(orders_path)
        items_df = spark.read.format("delta").load(items_path)
        products_df = spark.read.format("delta").load(products_path)
        
        # Gold Table 1: Daily Sales Summary
        logger.info("Creating gold_daily_sales...")
        daily_sales = orders_df.groupBy("order_date").agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("user_id").alias("unique_customers")
        ).orderBy("order_date")
        
        gold_daily_path = os.path.join(LOCAL_GOLD_FOLDER, "daily_sales")
        daily_sales.write.format("delta").mode("overwrite").save(gold_daily_path)
        logger.info(f"✓ gold_daily_sales created: {daily_sales.count()} records")
        
        # Gold Table 2: Product Performance
        logger.info("Creating gold_product_performance...")
        product_perf = items_df.groupBy("product_id").agg(
            count("order_id").alias("times_ordered"),
            _sum("reordered").alias("times_reordered"),
            countDistinct("user_id").alias("unique_buyers")
        ).join(products_df, "product_id", "left") \
         .select("product_id", "product_name", "department", 
                 "times_ordered", "times_reordered", "unique_buyers") \
         .orderBy(col("times_ordered").desc())
        
        gold_product_path = os.path.join(LOCAL_GOLD_FOLDER, "product_performance")
        product_perf.write.format("delta").mode("overwrite").save(gold_product_path)
        logger.info(f"✓ gold_product_performance created: {product_perf.count()} records")
        
        # Gold Table 3: Customer Order Frequency
        logger.info("Creating gold_customer_metrics...")
        customer_metrics = orders_df.groupBy("user_id").agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("lifetime_value"),
            avg("total_amount").alias("avg_order_value"),
            _max("order_date").alias("last_order_date"),
            _min("order_date").alias("first_order_date")
        ).orderBy(col("lifetime_value").desc())
        
        gold_customer_path = os.path.join(LOCAL_GOLD_FOLDER, "customer_metrics")
        customer_metrics.write.format("delta").mode("overwrite").save(gold_customer_path)
        logger.info(f"✓ gold_customer_metrics created: {customer_metrics.count()} records")
        
        logger.info("✓ Gold layer completed successfully")
        
    except Exception as e:
        logger.error(f"✗ Gold layer creation failed: {e}")
        raise


# ==================== MOVE TO PROCESSED ====================
def move_to_processed(file_list):
    """Move processed files from Raw to Processed folder"""
    try:
        logger.info(f"\n{'='*60}")
        logger.info("Moving files to Processed archive")
        logger.info(f"{'='*60}")
        
        for file_name in file_list:
            source_path = os.path.join(LOCAL_RAW_FOLDER, file_name)
            dest_path = os.path.join(LOCAL_PROCESSED_FOLDER, file_name)
            
            if not os.path.exists(source_path):
                logger.warning(f"✗ File not found: {file_name}")
                continue
            
            # Create destination directory
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Move file
            shutil.move(source_path, dest_path)
            logger.info(f"✓ Moved: {file_name}")
        
        logger.info("✓ All files archived to Processed folder")
        
    except Exception as e:
        logger.error(f"✗ Error moving files: {e}")
        raise


# ==================== VALIDATION QUERIES ====================
def run_validation_queries(spark):
    """Run validation queries on all layers"""
    try:
        logger.info(f"\n{'='*60}")
        logger.info("VALIDATION QUERIES")
        logger.info(f"{'='*60}\n")
        
        # Bronze validation
        logger.info("BRONZE LAYER:")
        for table in ['orders', 'order_items', 'products']:
            path = os.path.join(LOCAL_BRONZE_FOLDER, table)
            if os.path.exists(path):
                df = spark.read.format("delta").load(path)
                logger.info(f"  {table}: {df.count()} raw records")
        
        # Silver validation
        logger.info("\nSILVER LAYER:")
        for table in ['orders', 'order_items', 'products']:
            path = os.path.join(LOCAL_SILVER_FOLDER, table)
            if os.path.exists(path):
                df = spark.read.format("delta").load(path)
                logger.info(f"  {table}: {df.count()} clean records")
        
        # Gold validation
        logger.info("\nGOLD LAYER:")
        gold_tables = ['daily_sales', 'product_performance', 'customer_metrics']
        for table in gold_tables:
            path = os.path.join(LOCAL_GOLD_FOLDER, table)
            if os.path.exists(path):
                df = spark.read.format("delta").load(path)
                logger.info(f"  {table}: {df.count()} aggregated records")
                df.show(5, truncate=False)
        
        logger.info("✓ Validation completed\n")
        
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")


# ==================== MAIN PIPELINE ====================
def run_pipeline():
    """Execute complete Lakehouse pipeline"""
    spark = None
    try:
        logger.info("\n" + "="*60)
        logger.info("RETAIL LAKEHOUSE PIPELINE - LOCAL TESTING")
        logger.info("Pipeline: Raw -> Bronze -> Silver -> Gold -> Processed")
        logger.info("="*60 + "\n")
        
        # Initialize Spark
        spark = create_spark_session()
        
        # Process each table through Bronze and Silver
        for table_name, config in TABLE_CONFIG.items():
            # Bronze Layer
            bronze_path = load_to_bronze(spark, table_name, config['files'])
            
            # Silver Layer
            transform_to_silver(spark, table_name, bronze_path, config['primary_key'])
        
        # Gold Layer (aggregations across tables)
        create_gold_layer(spark)
        
        # Move raw files to processed
        all_files = [f for files in [cfg['files'] for cfg in TABLE_CONFIG.values()] for f in files]
        move_to_processed(all_files)
        
        # Run validation
        run_validation_queries(spark)
        
        logger.info("="*60)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info(f"\nBronze Layer: {LOCAL_BRONZE_FOLDER}")
        logger.info(f"Silver Layer: {LOCAL_SILVER_FOLDER}")
        logger.info(f"Gold Layer: {LOCAL_GOLD_FOLDER}")
        logger.info(f"Processed Archive: {LOCAL_PROCESSED_FOLDER}")
        logger.info(f"Log File: retail_lakehouse_pipeline.log\n")
        
    except Exception as e:
        logger.error(f"\n{'='*60}")
        logger.error(f"✗ PIPELINE FAILED: {e}")
        logger.error(f"{'='*60}\n")
        raise
        
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


# ==================== EXECUTION ====================
if __name__ == "__main__":
    run_pipeline()






# """
# Retail Lakehouse - Local Testing Script
# ========================================================
# Pipeline: Raw → Bronze → Silver → Gold → Processed
# - Raw: Original CSV files
# - Bronze: Raw data loaded as-is with metadata
# - Silver: Cleaned and validated data
# - Gold: Business-ready aggregated data
# - Processed: Archive for processed raw files
# """

# import os
# import shutil
# import logging
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (col, to_date, trim, when, count, lit, 
#                                    current_timestamp, input_file_name, sum as _sum, 
#                                    avg, max as _max, min as _min, countDistinct)
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType
# from delta import configure_spark_with_delta_pip, DeltaTable

# # ==================== CONFIGURATION ====================
# # Local folder paths - UPDATE THESE PATHS
# LOCAL_RAW_FOLDER = './dataset'  # ← Your CSV files location
# LOCAL_BRONZE_FOLDER = './lakehouse/bronze'          # Raw data with metadata
# LOCAL_SILVER_FOLDER = './lakehouse/silver'          # Cleaned data
# LOCAL_GOLD_FOLDER = './lakehouse/gold'              # Business aggregations
# LOCAL_PROCESSED_FOLDER = './lakehouse/processed'    # Archive for processed files

# # Create all folders
# for folder in [LOCAL_RAW_FOLDER, LOCAL_BRONZE_FOLDER, LOCAL_SILVER_FOLDER, 
#                LOCAL_GOLD_FOLDER, LOCAL_PROCESSED_FOLDER]:
#     os.makedirs(folder, exist_ok=True)

# # File mappings (same structure as your S3 upload)
# files_to_process = {
#     'order_items_2024-06-06.csv': 'transactional/order_items/',
#     'order_items_2024-06-07.csv': 'transactional/order_items/',
#     'orders_2024-06-06.csv': 'transactional/orders/',
#     'orders_2024-06-07.csv': 'transactional/orders/',
#     'orders_2024-06-06-Updated.csv': 'transactional/orders/',
#     'products.csv': 'product/'
# }

# # Table configurations
# TABLE_CONFIG = {
#     'orders': {
#         'primary_key': ['order_id'],
#         'partition_column': 'order_date',
#         'files': [f for f in files_to_process.keys() if 'orders' in f and 'order_items' not in f]
#     },
#     'order_items': {
#         'primary_key': ['order_id', 'id'],
#         'partition_column': 'order_date',
#         'files': [f for f in files_to_process.keys() if 'order_items' in f]
#     },
#     'products': {
#         'primary_key': ['product_id'],
#         'partition_column': None,
#         'files': [f for f in files_to_process.keys() if 'products' in f]
#     }
# }

# # ==================== LOGGING SETUP ====================
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('retail_lakehouse_pipeline.log'),
#         logging.StreamHandler()
#     ]
# )
# logger = logging.getLogger(__name__)


# # ==================== SPARK SESSION ====================
# def create_spark_session():
#     """Initialize Spark session with Delta Lake support"""
#     try:
#         builder = SparkSession.builder \
#             .appName("RetailLakehousePipeline") \
#             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#             .config("spark.driver.memory", "4g") \
#             .config("spark.sql.shuffle.partitions", "2") \
#             .master("local[*]")
        
#         spark = configure_spark_with_delta_pip(builder).getOrCreate()
#         logger.info("✓ Spark session created with Delta Lake support")
#         return spark
#     except Exception as e:
#         logger.error(f"✗ Failed to create Spark session: {e}")
#         raise


# # ==================== SCHEMA DEFINITIONS ====================
# def get_bronze_schema(table_name):
#     """Return schema for Bronze layer (raw data + metadata)"""
#     base_schemas = {
#         'orders': StructType([
#             StructField("order_num", StringType(), True),
#             StructField("order_id", StringType(), True),
#             StructField("user_id", StringType(), True),
#             StructField("order_timestamp", StringType(), True),
#             StructField("total_amount", StringType(), True),
#             StructField("date", StringType(), True)
#         ]),
#         'order_items': StructType([
#             StructField("id", StringType(), True),
#             StructField("order_id", StringType(), True),
#             StructField("user_id", StringType(), True),
#             StructField("days_since_prior_order", StringType(), True),
#             StructField("product_id", StringType(), True),
#             StructField("add_to_cart_order", StringType(), True),
#             StructField("reordered", StringType(), True),
#             StructField("order_timestamp", StringType(), True),
#             StructField("date", StringType(), True)
#         ]),
#         'products': StructType([
#             StructField("product_id", StringType(), True),
#             StructField("department_id", StringType(), True),
#             StructField("shelf_id", StringType(), True),
#             StructField("department", StringType(), True),
#             StructField("product_name", StringType(), True)
#         ])
#     }
#     return base_schemas.get(table_name)


# # ==================== BRONZE LAYER ====================
# def load_to_bronze(spark, table_name, file_list):
#     """
#     BRONZE LAYER: Load raw CSV files as-is with metadata
#     - Read CSV files without transformation
#     - Add metadata columns (load_timestamp, source_file)
#     - Store in Delta format
#     """
#     try:
#         logger.info(f"\n{'='*60}")
#         logger.info(f"BRONZE LAYER - Loading {table_name}")
#         logger.info(f"{'='*60}")
        
#         bronze_path = os.path.join(LOCAL_BRONZE_FOLDER, table_name)
#         schema = get_bronze_schema(table_name)
#         all_dfs = []
        
#         for file_name in file_list:
#             file_path = os.path.join(LOCAL_RAW_FOLDER, file_name)
            
#             if not os.path.exists(file_path):
#                 logger.warning(f"✗ File not found: {file_path}")
#                 continue
            
#             logger.info(f"Reading: {file_name}")
            
#             # Read CSV as-is (all strings)
#             df = spark.read \
#                 .option("header", "true") \
#                 .option("mode", "PERMISSIVE") \
#                 .schema(schema) \
#                 .csv(file_path)
            
#             # Add metadata columns
#             df = df.withColumn("load_timestamp", current_timestamp()) \
#                    .withColumn("source_file", lit(file_name))
            
#             record_count = df.count()
#             logger.info(f"  Loaded {record_count} records")
#             all_dfs.append(df)
        
#         if not all_dfs:
#             raise ValueError(f"No valid files found for {table_name}")
        
#         # Combine all dataframes
#         combined_df = all_dfs[0]
#         for df in all_dfs[1:]:
#             combined_df = combined_df.union(df)
        
#         # Write to Bronze Delta table
#         combined_df.write.format("delta").mode("overwrite").save(bronze_path)
        
#         total_count = combined_df.count()
#         logger.info(f"✓ Bronze layer created: {total_count} records")
#         logger.info(f"  Location: {bronze_path}")
        
#         return bronze_path
        
#     except Exception as e:
#         logger.error(f"✗ Bronze layer failed for {table_name}: {e}")
#         raise


# # ==================== SILVER LAYER ====================
# def transform_to_silver(spark, table_name, bronze_path, primary_keys):
#     """
#     SILVER LAYER: Clean and validate data
#     - Cast to proper data types
#     - Remove nulls in primary keys
#     - Handle nulls in other columns
#     - Remove duplicates
#     - Trim whitespace
#     """
#     try:
#         logger.info(f"\n{'='*60}")
#         logger.info(f"SILVER LAYER - Cleaning {table_name}")
#         logger.info(f"{'='*60}")
        
#         silver_path = os.path.join(LOCAL_SILVER_FOLDER, table_name)
        
#         # Read from Bronze
#         df = spark.read.format("delta").load(bronze_path)
#         initial_count = df.count()
#         logger.info(f"Initial records from Bronze: {initial_count}")
        
#         # Data type transformations based on table
#         if table_name == 'orders':
#             df = df.withColumn("order_num", col("order_num").cast(IntegerType())) \
#                    .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2))) \
#                    .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType())) \
#                    .withColumn("date", to_date(col("date"))) \
#                    .withColumn("order_date", to_date(col("date")))
        
#         elif table_name == 'order_items':
#             df = df.withColumn("days_since_prior_order", col("days_since_prior_order").cast(IntegerType())) \
#                    .withColumn("add_to_cart_order", col("add_to_cart_order").cast(IntegerType())) \
#                    .withColumn("reordered", col("reordered").cast(IntegerType())) \
#                    .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType())) \
#                    .withColumn("date", to_date(col("date"))) \
#                    .withColumn("order_date", to_date(col("date")))
        
#         # Trim all string columns
#         string_cols = [field.name for field in df.schema.fields 
#                       if isinstance(field.dataType, StringType)]
#         for col_name in string_cols:
#             df = df.withColumn(col_name, trim(col(col_name)))
        
#         # Check null values
#         logger.info("Checking null values:")
#         null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
#         null_dict = null_counts.collect()[0].asDict()
        
#         for col_name, null_count in null_dict.items():
#             if null_count > 0:
#                 logger.warning(f"  {col_name}: {null_count} nulls")
        
#         # Remove rows with null primary keys
#         for pk in primary_keys:
#             before = df.count()
#             df = df.filter(col(pk).isNotNull())
#             after = df.count()
#             if before != after:
#                 logger.warning(f"Removed {before - after} rows with null {pk}")
        
#         # Fill nulls in non-PK columns
#         for field in df.schema.fields:
#             col_name = field.name
#             if col_name not in primary_keys and col_name not in ['load_timestamp', 'source_file']:
#                 if isinstance(field.dataType, (IntegerType, DecimalType)):
#                     df = df.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
#                 elif isinstance(field.dataType, StringType):
#                     df = df.withColumn(col_name, when(col(col_name).isNull(), lit("UNKNOWN")).otherwise(col(col_name)))
        
#         # Remove duplicates
#         before_dedup = df.count()
#         df = df.dropDuplicates(primary_keys)
#         after_dedup = df.count()
        
#         if before_dedup != after_dedup:
#             logger.warning(f"Removed {before_dedup - after_dedup} duplicates")
        
#         # Write to Silver Delta table
#         writer = df.write.format("delta").mode("overwrite")
#         if table_name in ['orders', 'order_items']:
#             writer = writer.partitionBy("order_date")
#         writer.save(silver_path)
        
#         logger.info(f"✓ Silver layer created: {after_dedup} clean records")
#         logger.info(f"  Location: {silver_path}")
        
#         return silver_path
        
#     except Exception as e:
#         logger.error(f"✗ Silver layer failed for {table_name}: {e}")
#         raise


# # ==================== GOLD LAYER ====================
# def create_gold_layer(spark):
#     """
#     GOLD LAYER: Business-ready aggregations and analytics
#     - Daily sales summary
#     - Product performance metrics
#     - Customer insights
#     """
#     try:
#         logger.info(f"\n{'='*60}")
#         logger.info(f"GOLD LAYER - Creating Business Aggregations")
#         logger.info(f"{'='*60}")
        
#         # Read Silver tables
#         orders_path = os.path.join(LOCAL_SILVER_FOLDER, "orders")
#         items_path = os.path.join(LOCAL_SILVER_FOLDER, "order_items")
#         products_path = os.path.join(LOCAL_SILVER_FOLDER, "products")
        
#         orders_df = spark.read.format("delta").load(orders_path)
#         items_df = spark.read.format("delta").load(items_path)
#         products_df = spark.read.format("delta").load(products_path)
        
#         # Gold Table 1: Daily Sales Summary
#         logger.info("Creating gold_daily_sales...")
#         daily_sales = orders_df.groupBy("order_date").agg(
#             count("order_id").alias("total_orders"),
#             _sum("total_amount").alias("total_revenue"),
#             avg("total_amount").alias("avg_order_value"),
#             countDistinct("user_id").alias("unique_customers")
#         ).orderBy("order_date")
        
#         gold_daily_path = os.path.join(LOCAL_GOLD_FOLDER, "daily_sales")
#         daily_sales.write.format("delta").mode("overwrite").save(gold_daily_path)
#         logger.info(f"✓ gold_daily_sales created: {daily_sales.count()} records")
        
#         # Gold Table 2: Product Performance
#         logger.info("Creating gold_product_performance...")
#         product_perf = items_df.groupBy("product_id").agg(
#             count("order_id").alias("times_ordered"),
#             _sum("reordered").alias("times_reordered"),
#             countDistinct("user_id").alias("unique_buyers")
#         ).join(products_df, "product_id", "left") \
#          .select("product_id", "product_name", "department", 
#                  "times_ordered", "times_reordered", "unique_buyers") \
#          .orderBy(col("times_ordered").desc())
        
#         gold_product_path = os.path.join(LOCAL_GOLD_FOLDER, "product_performance")
#         product_perf.write.format("delta").mode("overwrite").save(gold_product_path)
#         logger.info(f"✓ gold_product_performance created: {product_perf.count()} records")
        
#         # Gold Table 3: Customer Order Frequency
#         logger.info("Creating gold_customer_metrics...")
#         customer_metrics = orders_df.groupBy("user_id").agg(
#             count("order_id").alias("total_orders"),
#             _sum("total_amount").alias("lifetime_value"),
#             avg("total_amount").alias("avg_order_value"),
#             _max("order_date").alias("last_order_date"),
#             _min("order_date").alias("first_order_date")
#         ).orderBy(col("lifetime_value").desc())
        
#         gold_customer_path = os.path.join(LOCAL_GOLD_FOLDER, "customer_metrics")
#         customer_metrics.write.format("delta").mode("overwrite").save(gold_customer_path)
#         logger.info(f"✓ gold_customer_metrics created: {customer_metrics.count()} records")
        
#         logger.info("✓ Gold layer completed successfully")
        
#     except Exception as e:
#         logger.error(f"✗ Gold layer creation failed: {e}")
#         raise


# # ==================== MOVE TO PROCESSED ====================
# def move_to_processed(file_list):
#     """Move processed files from Raw to Processed folder"""
#     try:
#         logger.info(f"\n{'='*60}")
#         logger.info("Moving files to Processed archive")
#         logger.info(f"{'='*60}")
        
#         for file_name in file_list:
#             source_path = os.path.join(LOCAL_RAW_FOLDER, file_name)
#             dest_path = os.path.join(LOCAL_PROCESSED_FOLDER, file_name)
            
#             if not os.path.exists(source_path):
#                 logger.warning(f"✗ File not found: {file_name}")
#                 continue
            
#             # Create destination directory
#             os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
#             # Move file
#             shutil.move(source_path, dest_path)
#             logger.info(f"✓ Moved: {file_name}")
        
#         logger.info("✓ All files archived to Processed folder")
        
#     except Exception as e:
#         logger.error(f"✗ Error moving files: {e}")
#         raise


# # ==================== VALIDATION QUERIES ====================
# def run_validation_queries(spark):
#     """Run validation queries on all layers"""
#     try:
#         logger.info(f"\n{'='*60}")
#         logger.info("VALIDATION QUERIES")
#         logger.info(f"{'='*60}\n")
        
#         # Bronze validation
#         logger.info("BRONZE LAYER:")
#         for table in ['orders', 'order_items', 'products']:
#             path = os.path.join(LOCAL_BRONZE_FOLDER, table)
#             if os.path.exists(path):
#                 df = spark.read.format("delta").load(path)
#                 logger.info(f"  {table}: {df.count()} raw records")
        
#         # Silver validation
#         logger.info("\nSILVER LAYER:")
#         for table in ['orders', 'order_items', 'products']:
#             path = os.path.join(LOCAL_SILVER_FOLDER, table)
#             if os.path.exists(path):
#                 df = spark.read.format("delta").load(path)
#                 logger.info(f"  {table}: {df.count()} clean records")
        
#         # Gold validation
#         logger.info("\nGOLD LAYER:")
#         gold_tables = ['daily_sales', 'product_performance', 'customer_metrics']
#         for table in gold_tables:
#             path = os.path.join(LOCAL_GOLD_FOLDER, table)
#             if os.path.exists(path):
#                 df = spark.read.format("delta").load(path)
#                 logger.info(f"  {table}: {df.count()} aggregated records")
#                 df.show(5, truncate=False)
        
#         logger.info("✓ Validation completed\n")
        
#     except Exception as e:
#         logger.error(f"✗ Validation failed: {e}")


# # ==================== MAIN PIPELINE ====================
# def run_pipeline():
#     """Execute complete Lakehouse pipeline"""
#     spark = None
#     try:
#         logger.info("\n" + "="*60)
#         logger.info("RETAIL LAKEHOUSE PIPELINE - LOCAL TESTING")
#         logger.info("Pipeline: Raw → Bronze → Silver → Gold → Processed")
#         logger.info("="*60 + "\n")
        
#         # Initialize Spark
#         spark = create_spark_session()
        
#         # Process each table through Bronze and Silver
#         for table_name, config in TABLE_CONFIG.items():
#             # Bronze Layer
#             bronze_path = load_to_bronze(spark, table_name, config['files'])
            
#             # Silver Layer
#             transform_to_silver(spark, table_name, bronze_path, config['primary_key'])
        
#         # Gold Layer (aggregations across tables)
#         create_gold_layer(spark)
        
#         # Move raw files to processed
#         all_files = [f for files in [cfg['files'] for cfg in TABLE_CONFIG.values()] for f in files]
#         move_to_processed(all_files)
        
#         # Run validation
#         run_validation_queries(spark)
        
#         logger.info("="*60)
#         logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
#         logger.info("="*60)
#         logger.info(f"\nBronze Layer: {LOCAL_BRONZE_FOLDER}")
#         logger.info(f"Silver Layer: {LOCAL_SILVER_FOLDER}")
#         logger.info(f"Gold Layer: {LOCAL_GOLD_FOLDER}")
#         logger.info(f"Processed Archive: {LOCAL_PROCESSED_FOLDER}")
#         logger.info(f"Log File: retail_lakehouse_pipeline.log\n")
        
#     except Exception as e:
#         logger.error(f"\n{'='*60}")
#         logger.error(f"✗ PIPELINE FAILED: {e}")
#         logger.error(f"{'='*60}\n")
#         raise
        
#     finally:
#         if spark:
#             spark.stop()
#             logger.info("Spark session stopped")


# # ==================== EXECUTION ====================
# if __name__ == "__main__":
#     run_pipeline()
































# # import boto3
# # import json
# # from botocore.exceptions import ClientError

# # # Initialize IAM client
# # iam = boto3.client('iam')

# # # Define your bucket
# # bucket_name = "s3-raw-zone-retail"

# # # Define the policy JSON
# # policy_name = "S3-bucket-custom-policy"
# # policy_document = {
# #     "Version": "2012-10-17",
# #     "Statement": [
# #         {
# #             "Effect": "Allow",
# #             "Action": [
# #                 "s3:ListBucket"
# #             ],
# #             "Resource": f"arn:aws:s3:::{bucket_name}"
# #         },
# #         {
# #             "Effect": "Allow",
# #             "Action": [
# #                 "s3:GetObject",
# #                 "s3:PutObject",
# #                 "s3:DeleteObject"
# #             ],
# #             "Resource": f"arn:aws:s3:::{bucket_name}/*"
# #         }
# #     ]
# # }

# # try:
# #     response = iam.create_policy(
# #         PolicyName=policy_name,
# #         PolicyDocument=json.dumps(policy_document),
# #         Description=f"Allows read/write access to the {bucket_name} bucket"
# #     )
# #     print(f" Policy created successfully: {response['Policy']['Arn']}")
# # except ClientError as e:
# #     if e.response['Error']['Code'] == 'EntityAlreadyExists':
# #         print(f" Policy '{policy_name}' already exists.")
# #     else:
# #         print(f" Error creating policy: {e}")






