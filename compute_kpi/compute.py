import os
import sys
import json
import logging
import datetime
from decimal import Decimal
from typing import Dict, Any
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum as _sum,
    countDistinct,
    count,
    when,
    round,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def load_environment_variables() -> Dict[str, Any]:
    """Load and validate required environment variables."""
    load_dotenv()

    return {
        "access_key_id": os.getenv("Access_key_ID"),
        "secret_access_key": os.getenv("Secret_access_key"),
        "input_bucket": os.getenv("BUCKET_NAME"),
        "output_bucket": os.getenv("BUCKET_NAME"),
        "region_name": os.getenv("REGION_NAME"),
        "orders_prefix": os.getenv("ORDERS_PREFIX"),
        "order_items_prefix": os.getenv("ORDER_ITEMS_PREFIX"),
        "products_file": os.getenv("PRODUCTS_FILE"),
        "output_prefix": os.getenv("OUTPUT_PREFIX"),
    }


def write_category_kpis_to_dynamodb(spark_df, table_name, session):
    """
    Write Category KPIs to DynamoDB using put_item for reliability

    :param spark_df: Spark DataFrame containing category KPIs
    :param table_name: DynamoDB table name
    :param session: AWS boto3 session
    """
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table(table_name)
    logger.info(f"Starting write of category KPIs to DynamoDB table: {table_name}")

    # Coalesce to fewer partitions to improve performance
    num_partitions = 5
    logger.info(f"Coalescing DataFrame to {num_partitions} partition(s)")
    spark_df = spark_df.coalesce(num_partitions)

    def process_partition(iterator):
        # Create boto3 resource per partition for efficiency
        dynamodb_partition = session.resource("dynamodb")
        table_partition = dynamodb_partition.Table(table_name)

        # Custom JSON encoder to handle date objects
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return super().default(obj)

        items_processed = 0
        successful_items = 0

        for row in iterator:
            items_processed += 1
            try:
                # Convert row to dictionary
                item_dict_raw = row.asDict(recursive=True)

                # Ensure both category and order_date exist
                if "category" not in item_dict_raw or "order_date" not in item_dict_raw:
                    logger.warning(f"Skipping row missing required fields: {row}")
                    continue

                # Create a unique ID for each category+date combination
                category = item_dict_raw["category"]
                order_date = item_dict_raw["order_date"]

                # Use the custom encoder to handle date objects
                item_json = json.dumps(item_dict_raw, cls=DateEncoder)
                item_dict = json.loads(item_json, parse_float=Decimal)

                # Use put_item instead of update_item for more reliability
                table_partition.put_item(Item=item_dict)
                successful_items += 1

                # Log progress periodically
                if successful_items % 10 == 0:
                    logger.info(f"Successfully processed {successful_items} items")

            except Exception as e:
                logger.error(
                    f"Error processing row for {table_name}, category: {item_dict_raw.get('category', 'unknown')}, "
                    f"date: {item_dict_raw.get('order_date', 'unknown')}: {e}"
                )

        logger.info(
            f"Partition complete: Processed {items_processed} items, successfully wrote {successful_items} items"
        )

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Finished writing to DynamoDB table: {table_name}")
    except Exception as write_err:
        logger.error(
            f"Error during foreachPartition write to {table_name}: {write_err}",
            exc_info=True,
        )
        raise


def write_order_kpis_to_dynamodb(spark_df, table_name, session):
    """
    Write Order KPIs to DynamoDB using put_item for reliability

    :param spark_df: Spark DataFrame containing order KPIs
    :param table_name: DynamoDB table name
    :param session: AWS boto3 session
    """
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table(table_name)
    logger.info(f"Starting write of order KPIs to DynamoDB table: {table_name}")

    # Coalesce to fewer partitions to improve performance
    num_partitions = 5
    logger.info(f"Coalescing DataFrame to {num_partitions} partition(s)")
    spark_df = spark_df.coalesce(num_partitions)

    def process_partition(iterator):
        # Create boto3 resource per partition for efficiency
        dynamodb_partition = session.resource("dynamodb")
        table_partition = dynamodb_partition.Table(table_name)

        # Custom JSON encoder to handle date objects
        class DateEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (datetime.date, datetime.datetime)):
                    return obj.isoformat()
                return super().default(obj)

        items_processed = 0
        successful_items = 0

        for row in iterator:
            items_processed += 1
            try:
                # Convert row to dictionary
                item_dict_raw = row.asDict(recursive=True)

                # Ensure order_date exists
                if "order_date" not in item_dict_raw:
                    logger.warning(f"Skipping row missing order_date: {row}")
                    continue

                # Use the custom encoder to handle date objects
                item_json = json.dumps(item_dict_raw, cls=DateEncoder)
                item_dict = json.loads(item_json, parse_float=Decimal)

                # Use put_item instead of update_item for more reliability
                table_partition.put_item(Item=item_dict)
                successful_items += 1

                # Log progress periodically
                if successful_items % 10 == 0:
                    logger.info(f"Successfully processed {successful_items} items")

            except Exception as e:
                logger.error(
                    f"Error processing row for {table_name}, date: {item_dict_raw.get('order_date', 'unknown')}: {e}"
                )

        logger.info(
            f"Partition complete: Processed {items_processed} items, successfully wrote {successful_items} items"
        )

    try:
        spark_df.rdd.foreachPartition(process_partition)
        logger.info(f"Finished writing to DynamoDB table: {table_name}")
    except Exception as write_err:
        logger.error(
            f"Error during foreachPartition write to {table_name}: {write_err}",
            exc_info=True,
        )
        raise


def main():
    spark = None  # Initialize spark variable

    try:
        # Load environment variables
        env_vars = load_environment_variables()

        # Initialize AWS session
        session = boto3.Session(
            aws_access_key_id=env_vars["access_key_id"],
            aws_secret_access_key=env_vars["secret_access_key"],
            region_name=env_vars["region_name"],
        )

        # Initialize Spark session with S3A configuration for IAM role auth
        spark = (
            SparkSession.builder.appName("DataCleaningECS")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.access.key", env_vars["access_key_id"])
            .config("spark.hadoop.fs.s3a.secret.key", env_vars["secret_access_key"])
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.region", env_vars["region_name"])
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .getOrCreate()
        )
        logger.info("Spark session started with S3A IAM role configuration.")
    except Exception as e:
        logger.exception("Error during initialization: %s", e)
        return

    try:
        # --- Read input data from Cleaned S3 Bucket ---
        orders_path = f"s3a://{env_vars['output_bucket']}/{env_vars['output_prefix']}clean_orders/"
        order_items_path = f"s3a://{env_vars['output_bucket']}/{env_vars['output_prefix']}clean_order_items/"
        products_path = f"s3a://{env_vars['output_bucket']}/{env_vars['output_prefix']}clean_products/"

        logger.info(f"Reading orders data from S3 directory: {orders_path}")
        orders_df = spark.read.parquet(orders_path).cache()

        logger.info(f"Reading order items data from S3 directory: {order_items_path}")
        order_items_df = spark.read.parquet(order_items_path).cache()

        logger.info(f"Reading products data from S3 directory: {products_path}")
        products_df = spark.read.parquet(products_path).cache()

        logger.info("Cleaned Parquet files loaded successfully from S3.")

    except Exception as e:
        logger.exception(
            f"Error loading cleaned Parquet files from S3: {e}", exc_info=True
        )
        if spark:
            spark.stop()
        return

    try:
        # Preprocess orders and order_items
        orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
        # Cast might still be needed depending on Parquet source types
        order_items_df = order_items_df.withColumn(
            "sale_price", col("sale_price").cast("float")
        )
        logger.info("Preprocessing completed.")
    except Exception as e:
        logger.exception("Error during preprocessing: %s", e)
        if spark:
            spark.stop()
        return

    category_kpis_df = None
    order_kpis_df = None
    try:
        # Join data for Category-Level KPIs
        joined_category_df = (
            order_items_df.join(
                orders_df.select("order_id", "order_date"), on="order_id"
            )
            .join(
                products_df.select(col("id").alias("product_id"), "category"),
                on="product_id",
            )
            .withColumn(
                "is_returned", when(col("status") == "returned", 1).otherwise(0)
            )
        )
        logger.info("Data joined for category-level KPIs.")

        # Compute Category-Level KPIs
        category_kpis_df = (
            joined_category_df.groupBy("category", "order_date")
            .agg(
                round(_sum("sale_price"), 2).alias("daily_revenue"),
                round(_sum("sale_price") / countDistinct("order_id"), 2).alias(
                    "avg_order_value"
                ),
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias(
                    "avg_return_rate"
                ),
            )
            .cache()
        )
        logger.info("Category-level KPIs computed successfully.")
        category_kpis_df.show(5, truncate=False)  # Display sample

    except Exception as e:
        logger.exception("Error computing category-level KPIs: %s", e)

    try:
        joined_order_df = (
            order_items_df.alias("oi")
            .join(orders_df.alias("o"), col("oi.order_id") == col("o.order_id"))
            .select(
                col("o.order_date"),
                col("o.order_id"),
                col("o.user_id"),
                col("oi.id"),  # item id
                col("oi.sale_price"),
                when(col("o.status") == "returned", 1)
                .otherwise(0)
                .alias("is_returned"),
            )
        )
        logger.info("Data joined for order-level KPIs.")

        # Compute Order-Level KPIs
        order_kpis_df = (
            joined_order_df.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                round(_sum("sale_price"), 2).alias("total_revenue"),
                count("id").alias("total_items_sold"),
                round(_sum("is_returned") / countDistinct("order_id"), 4).alias(
                    "return_rate"
                ),
                countDistinct("user_id").alias("unique_customers"),
            )
            .cache()
        )
        logger.info("Order-level KPIs computed successfully.")
        order_kpis_df.show(5, truncate=False)

    except Exception as e:
        logger.exception("Error computing order-level KPIs: %s", e)

    # --- Write KPI Results to DynamoDB ---
    try:
        if category_kpis_df is not None:
            write_category_kpis_to_dynamodb(
                category_kpis_df, table_name="category_kpi_table", session=session
            )
        else:
            logger.warning("Category KPIs DataFrame is None, skipping DynamoDB write.")

        if order_kpis_df is not None:
            write_order_kpis_to_dynamodb(
                order_kpis_df, table_name="order_kpi_table", session=session
            )
        else:
            logger.warning("Order KPIs DataFrame is None, skipping DynamoDB write.")
    except Exception as e:
        logger.exception("Error writing KPIs to DynamoDB: %s", e)

    # Clear DataFrames from cache to free up memory
    if order_kpis_df is not None:
        order_kpis_df.unpersist()
    if category_kpis_df is not None:
        category_kpis_df.unpersist()

    # Stop Spark session
    if spark:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
