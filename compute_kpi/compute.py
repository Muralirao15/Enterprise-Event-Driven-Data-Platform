import datetime
import logging
import os
import sys
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    round,
    sum as _sum,
    to_date,
    when,
)


# ============================================================================
# Logging Configuration
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

def load_environment_variables() -> dict[str, Any]:
    """
    Load and validate application configuration from environment variables.

    AWS credentials are intentionally not loaded here.
    boto3 will use the standard AWS credential provider chain:
        - IAM role
        - AWS CLI profile
        - environment variables
        - ECS/EC2/Lambda credentials
    """

    load_dotenv()

    required_vars = {
        "OUTPUT_BUCKET": os.getenv("OUTPUT_BUCKET"),
        "OUTPUT_PREFIX": os.getenv("OUTPUT_PREFIX", ""),
        "AWS_REGION": os.getenv("AWS_REGION"),
    }

    missing_vars = [
        name
        for name, value in required_vars.items()
        if not value
    ]

    if missing_vars:
        raise EnvironmentError(
            "Missing required environment variables: "
            + ", ".join(missing_vars)
        )

    return {
        "output_bucket": required_vars["OUTPUT_BUCKET"],
        "output_prefix": required_vars["OUTPUT_PREFIX"],
        "region_name": required_vars["AWS_REGION"],
        "dynamodb_partitions": int(
            os.getenv("DYNAMODB_PARTITIONS", "5")
        ),
    }


# ============================================================================
# AWS Session
# ============================================================================

def initialize_aws_session(region: str) -> boto3.Session:
    """
    Initialize a boto3 AWS session.

    Credentials are resolved automatically by boto3.
    """

    try:
        session = boto3.Session(region_name=region)

        # Validate that a credential provider is available.
        credentials = session.get_credentials()

        if credentials is None:
            raise RuntimeError(
                "AWS credentials could not be resolved. "
                "Configure an IAM role, AWS CLI profile, "
                "or AWS credential environment variables."
            )

        logger.info(
            "AWS session initialized successfully for region: %s",
            region,
        )

        return session

    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            "Failed to initialize AWS session."
        ) from exc


# ============================================================================
# DynamoDB Value Conversion
# ============================================================================

def convert_to_dynamodb_value(value: Any) -> Any:
    """
    Convert Spark/Python values into DynamoDB-compatible values.

    DynamoDB does not support Python float values directly,
    so floats are converted to Decimal.
    """

    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            key: convert_to_dynamodb_value(val)
            for key, val in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [
            convert_to_dynamodb_value(item)
            for item in value
        ]

    return value


# ============================================================================
# DynamoDB Writer
# ============================================================================

def write_dataframe_to_dynamodb(
    spark_df: DataFrame,
    table_name: str,
    region: str,
    num_partitions: int = 5,
) -> None:
    """
    Write a Spark DataFrame to DynamoDB.

    Each Spark partition creates its own boto3 DynamoDB resource.
    This avoids sharing a boto3 client/resource between executors.
    """

    if spark_df is None:
        logger.warning(
            "DataFrame is None. Skipping DynamoDB write for %s.",
            table_name,
        )
        return

    logger.info(
        "Starting DynamoDB write: table=%s, partitions=%s",
        table_name,
        num_partitions,
    )

    dataframe = spark_df.coalesce(num_partitions)

    def process_partition(iterator):
        """
        Process one Spark partition and write its records to DynamoDB.
        """

        dynamodb = boto3.resource(
            "dynamodb",
            region_name=region,
        )

        table = dynamodb.Table(table_name)

        processed = 0
        successful = 0
        failed = 0

        for row in iterator:
            processed += 1

            try:
                raw_item = row.asDict(recursive=True)

                if not raw_item:
                    logger.warning(
                        "Skipping empty row for table %s.",
                        table_name,
                    )
                    continue

                item = {
                    key: convert_to_dynamodb_value(value)
                    for key, value in raw_item.items()
                }

                table.put_item(Item=item)

                successful += 1

            except (BotoCoreError, ClientError, TypeError, ValueError) as exc:
                failed += 1

                logger.error(
                    "Failed to write row to DynamoDB table %s: %s",
                    table_name,
                    exc,
                )

        logger.info(
            "Partition completed for %s | processed=%s | "
            "successful=%s | failed=%s",
            table_name,
            processed,
            successful,
            failed,
        )

    try:
        dataframe.rdd.foreachPartition(process_partition)

        logger.info(
            "Successfully completed DynamoDB write for table: %s",
            table_name,
        )

    except Exception as exc:
        logger.exception(
            "DynamoDB write failed for table %s: %s",
            table_name,
            exc,
        )
        raise


# ============================================================================
# Spark Session
# ============================================================================

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and configure the Spark session for S3 access.
    """

    try:
        spark = (
            SparkSession.builder
            .appName(app_name)
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                "s3.amazonaws.com",
            )
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .getOrCreate()
        )

        logger.info(
            "Spark session created successfully."
        )

        return spark

    except Exception as exc:
        logger.exception(
            "Failed to create Spark session: %s",
            exc,
        )
        raise


# ============================================================================
# Load Data
# ============================================================================

def load_cleaned_data(
    spark: SparkSession,
    output_bucket: str,
    output_prefix: str,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Load cleaned Parquet datasets from S3.
    """

    prefix = output_prefix.rstrip("/")

    if prefix:
        prefix = f"{prefix}/"

    orders_path = (
        f"s3a://{output_bucket}/"
        f"{prefix}clean_orders/"
    )

    order_items_path = (
        f"s3a://{output_bucket}/"
        f"{prefix}clean_order_items/"
    )

    products_path = (
        f"s3a://{output_bucket}/"
        f"{prefix}clean_products/"
    )

    logger.info(
        "Reading orders from: %s",
        orders_path,
    )

    orders_df = spark.read.parquet(orders_path).cache()

    logger.info(
        "Reading order items from: %s",
        order_items_path,
    )

    order_items_df = (
        spark.read
        .parquet(order_items_path)
        .cache()
    )

    logger.info(
        "Reading products from: %s",
        products_path,
    )

    products_df = (
        spark.read
        .parquet(products_path)
        .cache()
    )

    logger.info(
        "Cleaned Parquet datasets loaded successfully."
    )

    return (
        orders_df,
        order_items_df,
        products_df,
    )


# ============================================================================
# Preprocessing
# ============================================================================

def preprocess_data(
    orders_df: DataFrame,
    order_items_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Prepare orders and order_items DataFrames for KPI calculations.
    """

    orders_df = orders_df.withColumn(
        "order_date",
        to_date(col("created_at")),
    )

    order_items_df = order_items_df.withColumn(
        "sale_price",
        col("sale_price").cast("double"),
    )

    logger.info(
        "Data preprocessing completed successfully."
    )

    return orders_df, order_items_df


# ============================================================================
# Category KPI Calculation
# ============================================================================

def calculate_category_kpis(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """
    Calculate category-level daily KPIs.

    Metrics:
        - Daily revenue
        - Average order value
        - Average return rate
    """

    joined_category_df = (
        order_items_df
        .join(
            orders_df.select(
                "order_id",
                "order_date",
            ),
            on="order_id",
            how="inner",
        )
        .join(
            products_df.select(
                col("id").alias("product_id"),
                "category",
            ),
            on="product_id",
            how="inner",
        )
        .withColumn(
            "is_returned",
            when(
                col("status") == "returned",
                1,
            ).otherwise(0),
        )
    )

    logger.info(
        "Data joined successfully for category KPIs."
    )

    category_kpis_df = (
        joined_category_df
        .groupBy(
            "category",
            "order_date",
        )
        .agg(
            round(
                _sum("sale_price"),
                2,
            ).alias("daily_revenue"),

            round(
                _sum("sale_price")
                / countDistinct("order_id"),
                2,
            ).alias("avg_order_value"),

            round(
                _sum("is_returned")
                / countDistinct("order_id"),
                4,
            ).alias("avg_return_rate"),
        )
        .cache()
    )

    logger.info(
        "Category-level KPIs calculated successfully."
    )

    category_kpis_df.show(
        5,
        truncate=False,
    )

    return category_kpis_df


# ============================================================================
# Order KPI Calculation
# ============================================================================

def calculate_order_kpis(
    orders_df: DataFrame,
    order_items_df: DataFrame,
) -> DataFrame:
    """
    Calculate daily order-level KPIs.

    Metrics:
        - Total orders
        - Total revenue
        - Total items sold
        - Return rate
        - Unique customers
    """

    joined_order_df = (
        order_items_df.alias("oi")
        .join(
            orders_df.alias("o"),
            col("oi.order_id")
            == col("o.order_id"),
            how="inner",
        )
        .select(
            col("o.order_date"),
            col("o.order_id"),
            col("o.user_id"),
            col("oi.id").alias("item_id"),
            col("oi.sale_price"),
            when(
                col("o.status") == "returned",
                1,
            )
            .otherwise(0)
            .alias("is_returned"),
        )
    )

    logger.info(
        "Data joined successfully for order KPIs."
    )

    order_kpis_df = (
        joined_order_df
        .groupBy("order_date")
        .agg(
            countDistinct(
                "order_id"
            ).alias("total_orders"),

            round(
                _sum("sale_price"),
                2,
            ).alias("total_revenue"),

            count(
                "item_id"
            ).alias("total_items_sold"),

            round(
                _sum("is_returned")
                / countDistinct("order_id"),
                4,
            ).alias("return_rate"),

            countDistinct(
                "user_id"
            ).alias("unique_customers"),
        )
        .cache()
    )

    logger.info(
        "Order-level KPIs calculated successfully."
    )

    order_kpis_df.show(
        5,
        truncate=False,
    )

    return order_kpis_df


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """
    Main application workflow.

    Pipeline:

        Cleaned S3 Parquet
                ↓
        Spark preprocessing
                ↓
        Category KPI calculation
                ↓
        Order KPI calculation
                ↓
        DynamoDB
    """

    spark = None
    orders_df = None
    order_items_df = None
    products_df = None
    category_kpis_df = None
    order_kpis_df = None

    try:
        # ------------------------------------------------------------------
        # Load configuration
        # ------------------------------------------------------------------

        config = load_environment_variables()

        logger.info(
            "Configuration loaded successfully."
        )

        # ------------------------------------------------------------------
        # Initialize AWS session
        # ------------------------------------------------------------------

        initialize_aws_session(
            config["region_name"]
        )

        # ------------------------------------------------------------------
        # Initialize Spark
        # ------------------------------------------------------------------

        spark = create_spark_session(
            "Enterprise-KPI-Processing"
        )

        # ------------------------------------------------------------------
        # Load cleaned data
        # ------------------------------------------------------------------

        (
            orders_df,
            order_items_df,
            products_df,
        ) = load_cleaned_data(
            spark=spark,
            output_bucket=config["output_bucket"],
            output_prefix=config["output_prefix"],
        )

        # ------------------------------------------------------------------
        # Preprocess data
        # ------------------------------------------------------------------

        (
            orders_df,
            order_items_df,
        ) = preprocess_data(
            orders_df,
            order_items_df,
        )

        # ------------------------------------------------------------------
        # Calculate Category KPIs
        # ------------------------------------------------------------------

        category_kpis_df = calculate_category_kpis(
            orders_df,
            order_items_df,
            products_df,
        )

        # ------------------------------------------------------------------
        # Calculate Order KPIs
        # ------------------------------------------------------------------

        order_kpis_df = calculate_order_kpis(
            orders_df,
            order_items_df,
        )

        # ------------------------------------------------------------------
        # Write Category KPIs to DynamoDB
        # ------------------------------------------------------------------

        write_dataframe_to_dynamodb(
            spark_df=category_kpis_df,
            table_name="category_kpi_table",
            region=config["region_name"],
            num_partitions=config["dynamodb_partitions"],
        )

        # ------------------------------------------------------------------
        # Write Order KPIs to DynamoDB
        # ------------------------------------------------------------------

        write_dataframe_to_dynamodb(
            spark_df=order_kpis_df,
            table_name="order_kpi_table",
            region=config["region_name"],
            num_partitions=config["dynamodb_partitions"],
        )

        logger.info(
            "KPI processing pipeline completed successfully."
        )

    except (
        EnvironmentError,
        RuntimeError,
        FileNotFoundError,
        BotoCoreError,
        ClientError,
    ) as exc:

        logger.error(
            "Pipeline failed: %s",
            exc,
        )

        sys.exit(1)

    except Exception as exc:

        logger.exception(
            "Unexpected error occurred: %s",
            exc,
        )

        sys.exit(1)

    finally:

        # ------------------------------------------------------------------
        # Release cached DataFrames
        # ------------------------------------------------------------------

        for dataframe in (
            orders_df,
            order_items_df,
            products_df,
            category_kpis_df,
            order_kpis_df,
        ):
            if dataframe is not None:
                try:
                    dataframe.unpersist()
                except Exception:
                    logger.warning(
                        "Unable to unpersist DataFrame.",
                        exc_info=True,
                    )

        # ------------------------------------------------------------------
        # Stop Spark
        # ------------------------------------------------------------------

        if spark is not None:
            try:
                spark.stop()
                logger.info(
                    "Spark session stopped successfully."
                )
            except Exception:
                logger.warning(
                    "Unable to stop Spark session cleanly.",
                    exc_info=True,
                )


# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == "__main__":
    main()
