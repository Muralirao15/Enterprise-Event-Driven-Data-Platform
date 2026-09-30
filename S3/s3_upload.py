import logging
import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_environment_variables() -> dict[str, str]:
    """Load and validate required application configuration."""

    load_dotenv()

    required_vars = {
        "BUCKET_NAME": os.getenv("BUCKET_NAME"),
        "ORDER_DATA_PATH": os.getenv("ORDER_DATA_PATH"),
        "ORDER_ITEMS_DATA_PATH": os.getenv("ORDER_ITEMS_DATA_PATH"),
        "PRODUCT_DATA_PATH": os.getenv("PRODUCT_DATA_PATH"),
        "AWS_REGION": os.getenv("AWS_REGION"),
    }

    missing_vars = [
        name for name, value in required_vars.items()
        if not value
    ]

    if missing_vars:
        raise EnvironmentError(
            "Missing required environment variables: "
            + ", ".join(missing_vars)
        )

    return {
        name.lower(): value
        for name, value in required_vars.items()
        if value is not None
    }


# ---------------------------------------------------------------------------
# AWS S3
# ---------------------------------------------------------------------------

def initialize_s3_client(region: str) -> Any:
    """Create and return an AWS S3 client.

    AWS credentials are resolved automatically by boto3 using
    its standard credential provider chain.
    """

    try:
        return boto3.client(
            "s3",
            region_name=region,
        )

    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            "Failed to initialize the S3 client."
        ) from exc


# ---------------------------------------------------------------------------
# File Upload
# ---------------------------------------------------------------------------

def upload_file_to_s3(
    s3_client: Any,
    bucket_name: str,
    local_path: str,
    s3_key: str,
) -> None:
    """Upload a single file to an S3 bucket."""

    file_path = Path(local_path)

    if not file_path.is_file():
        raise FileNotFoundError(
            f"File does not exist: {file_path}"
        )

    try:
        logger.info(
            "Uploading %s -> s3://%s/%s",
            file_path,
            bucket_name,
            s3_key,
        )

        s3_client.upload_file(
            str(file_path),
            bucket_name,
            s3_key,
        )

        logger.info(
            "Successfully uploaded %s",
            file_path,
        )

    except (BotoCoreError, ClientError, OSError) as exc:
        raise RuntimeError(
            f"Failed to upload {file_path}"
        ) from exc


def upload_directory_to_s3(
    s3_client: Any,
    bucket_name: str,
    local_path: str,
    s3_prefix: str,
) -> None:
    """Upload all files from a directory to an S3 prefix."""

    directory = Path(local_path)

    if not directory.is_dir():
        raise FileNotFoundError(
            f"Directory does not exist: {directory}"
        )

    files = [
        file_path
        for file_path in directory.iterdir()
        if file_path.is_file()
    ]

    if not files:
        logger.warning(
            "No files found in %s",
            directory,
        )
        return

    for file_path in files:
        s3_key = f"{s3_prefix}/{file_path.name}"

        upload_file_to_s3(
            s3_client,
            bucket_name,
            str(file_path),
            s3_key,
        )


# ---------------------------------------------------------------------------
# Main Application
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the S3 data upload pipeline."""

    try:
        config = load_environment_variables()

        s3_client = initialize_s3_client(
            config["aws_region"]
        )

        bucket_name = config["bucket_name"]

        # Upload orders
        upload_directory_to_s3(
            s3_client,
            bucket_name,
            config["order_data_path"],
            "orders",
        )

        # Upload order items
        upload_directory_to_s3(
            s3_client,
            bucket_name,
            config["order_items_data_path"],
            "order_items",
        )

        # Upload products
        product_path = Path(config["product_data_path"])

        upload_file_to_s3(
            s3_client,
            bucket_name,
            str(product_path),
            f"products/{product_path.name}",
        )

        logger.info(
            "All S3 upload operations completed successfully."
        )

    except (
        EnvironmentError,
        RuntimeError,
        FileNotFoundError,
    ) as exc:

        logger.error("Upload process failed: %s", exc)
        sys.exit(1)

    except Exception:
        logger.exception(
            "Unexpected error occurred during S3 upload."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
