import os
import sys
from typing import Dict, Any
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
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

def initialize_s3_client(access_key: str, secret_key: str, region: str) -> boto3.client:
    """Initialize and return an S3 client."""
    try:
        return boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to initialize S3 client: {str(e)}") from e

def validate_s3_path(s3_client: boto3.client, bucket: str, path: str, is_file: bool = False) -> bool:
    """Validate that files exist in the specified S3 path."""
    try:
        if is_file:
            # For single file, use head_object
            s3_client.head_object(Bucket=bucket, Key=path)
            return True
        else:
            # For directory prefix, use list_objects
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=path,
                MaxKeys=1
            )
            return 'Contents' in response
    except ClientError as e:
        logger.error(f"Error validating S3 path s3://{bucket}/{path}: {e}")
        return False

def clean_orders(orders_df):
    """Clean and validate orders data."""
    logger.info("Starting cleaning for orders data.")

    impute_values = {
        "order_id": "UNKNOWN_ORDER",
        "user_id": "UNKNOWN_USER",
        "created_at": "1970-01-01",
        "status": "UNKNOWN_STATUS"
    }

    orders_df = orders_df.fillna(impute_values)
    orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
    orders_df = orders_df.filter(col("order_date").isNotNull())
    orders_df = orders_df.dropDuplicates(["order_id"])

    logger.info(f"Orders cleaned. Remaining records: {orders_df.count()}")
    return orders_df

def clean_order_items(order_items_df):
    """Clean and validate order_items data."""
    logger.info("Starting cleaning for order_items data.")

    impute_values = {
        "id": "UNKNOWN_ID",
        "order_id": "UNKNOWN_ORDER",
        "product_id": "UNKNOWN_PRODUCT",
        "sale_price": 0.0
    }

    order_items_df = order_items_df.fillna(impute_values)
    order_items_df = order_items_df.withColumn("sale_price", col("sale_price").cast("float"))
    order_items_df = order_items_df.filter(col("sale_price").isNotNull())
    order_items_df = order_items_df.dropDuplicates(["id"])

    logger.info(f"Order items cleaned. Remaining records: {order_items_df.count()}")
    return order_items_df

def clean_products(products_df):
    """Clean and validate products data."""
    logger.info("Starting cleaning for products data.")

    impute_values = {
        "id": "UNKNOWN_ID",
        "sku": "UNKNOWN_SKU",
        "cost": 0.0,
        "category": "UNKNOWN_CATEGORY",
        "retail_price": 0.0
    }

    products_df = products_df.fillna(impute_values)
    products_df = products_df.withColumn("cost", col("cost").cast("float"))
    products_df = products_df.withColumn("retail_price", col("retail_price").cast("float"))
    products_df = products_df.filter(col("cost").isNotNull() & col("retail_price").isNotNull())
    products_df = products_df.dropDuplicates(["id"])

    logger.info(f"Products cleaned. Remaining records: {products_df.count()}")
    return products_df

def process_data_from_s3(config: Dict[str, Any]) -> None:
    """Process data from S3 and upload transformed results."""
    spark = None
    s3_client = initialize_s3_client(
        config["access_key_id"],
        config["secret_access_key"],
        config["region_name"]
    )

    try:
        # Validate input paths exist in S3
        if not validate_s3_path(s3_client, config["input_bucket"], config["orders_prefix"]):
            raise FileNotFoundError(f"No files found at s3://{config['input_bucket']}/{config['orders_prefix']}")
        
        if not validate_s3_path(s3_client, config["input_bucket"], config["order_items_prefix"]):
            raise FileNotFoundError(f"No files found at s3://{config['input_bucket']}/{config['order_items_prefix']}")
        
        if not validate_s3_path(s3_client, config["input_bucket"], config["products_file"], is_file=True):
            raise FileNotFoundError(f"File not found at s3://{config['input_bucket']}/{config['products_file']}")

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("S3DataTransformation") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", config["access_key_id"]) \
            .config("spark.hadoop.fs.s3a.secret.key", config["secret_access_key"]) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.region", config["region_name"]) \
            .getOrCreate()

        # Read data from S3
        orders_df = spark.read.csv(
            f"s3a://{config['input_bucket']}/{config['orders_prefix']}*.csv",
            header=True,
            inferSchema=True
        )
        order_items_df = spark.read.csv(
            f"s3a://{config['input_bucket']}/{config['order_items_prefix']}*.csv",
            header=True,
            inferSchema=True
        )
        products_df = spark.read.csv(
            f"s3a://{config['input_bucket']}/{config['products_file']}",
            header=True,
            inferSchema=True
        )

        # Clean data
        clean_orders_df = clean_orders(orders_df)
        clean_order_items_df = clean_order_items(order_items_df)
        clean_products_df = clean_products(products_df)

        # Write to S3
        output_paths = {
            "orders": f"s3a://{config['output_bucket']}/{config['output_prefix']}clean_orders",
            "order_items": f"s3a://{config['output_bucket']}/{config['output_prefix']}clean_order_items",
            "products": f"s3a://{config['output_bucket']}/{config['output_prefix']}clean_products"
        }

        for df, path in [
            (clean_orders_df, output_paths["orders"]),
            (clean_order_items_df, output_paths["order_items"]),
            (clean_products_df, output_paths["products"])
        ]:
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Data written to {path}")

        # Validate outputs
        for file_type in ["clean_orders", "clean_order_items", "clean_products"]:
            prefix = f"{config['output_prefix']}{file_type}/"
            if not validate_s3_path(s3_client, config["output_bucket"], prefix):
                raise RuntimeError(f"Output validation failed for {prefix}")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

def main() -> None:
    """Main execution function."""
    try:
        config = load_environment_variables()
        process_data_from_s3(config)
        logger.info("Data processing completed successfully")
    except (EnvironmentError, RuntimeError, FileNotFoundError) as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()