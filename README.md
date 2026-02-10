# E-Commerce Event-Driven Data Pipeline

A real-time, event-driven data pipeline that supports operational analytics for an e-commerce platform.

## Project Overview

This project implements a data pipeline that processes e-commerce data through multiple stages:
1. Data ingestion from S3
2. Data validation and cleaning
3. Computing key performance indicators (KPIs)
4. Storing results in DynamoDB

## Architecture Components

### S3 Module
- Handles initial data upload and storage
- Located in `/S3` directory
- Manages raw data files for orders, order items, and products

### Validation Module
- Performs data cleaning and validation
- Located in `/validate` directory
- Validates input data structure
- Cleans and transforms data
- Outputs cleaned Parquet files back to S3

### Compute KPI Module
- Calculates business metrics
- Located in `/compute_kpi` directory
- Computes two types of KPIs:
  - Category-level metrics (revenue, order value, return rate)
  - Order-level metrics (total orders, revenue, items sold, return rate, unique customers)
- Stores results in DynamoDB

## Technology Stack

- Python 3.9
- Apache Spark
- AWS Services:
  - S3 for storage
  - DynamoDB for KPI storage
- Docker for containerization

## Setup and Installation

Each module contains its own Dockerfile and can be built independently:

```bash
# Build S3 module
cd S3
docker build -t upload_s3 .

# Build validation module
cd validate
docker build -t validate .

# Build compute KPI module
cd compute_kpi
docker build -t compute .
```

## Environment Variables Required

Create a `.env` file with the following variables:
```
Access_key_ID=<your-aws-access-key>
Secret_access_key=<your-aws-secret-key>
BUCKET_NAME=<your-s3-bucket>
REGION_NAME=<aws-region>
ORDERS_PREFIX=<s3-prefix-for-orders>
ORDER_ITEMS_PREFIX=<s3-prefix-for-order-items>
PRODUCTS_FILE=<s3-path-to-products-file>
OUTPUT_PREFIX=<output-directory-prefix>
```

## Development

Each module includes a Makefile with common commands:
```bash
make install    # Install dependencies
make format    # Format code using black
make lint      # Run pylint
make refactor  # Run both format and lint
make all       # Run install and refactor
```

## License

This project is licensed under the GNU General Public License v3.0 - see the LICENSE file for details.
