# FoodPanda ETL

A Rust-based ETL (Extract, Transform, Load) application that extracts vendor data from FoodPanda's API, transforms it into Parquet format, and loads it into MinIO (S3-compatible storage) with partitioned structure.

## Features

- Extracts vendor data from FoodPanda's API for specified cities
- Handles rate limiting and retries with exponential backoff
- Converts JSON data to Parquet format for efficient storage and querying
- Uploads data to MinIO with partitioned structure (city/year/month/day)
- Supports concurrent processing with connection pooling
- Comprehensive logging with both file and console output
- Docker support for containerized deployment

## Prerequisites

- Rust 1.85+ (for standalone setup)
- Docker and Docker Compose (for containerized setup)
- MinIO instance (included in Docker setup)

## Configuration

The application uses a YAML configuration file (`config/default.yaml`) for settings:

```yaml
cities:
  - "69036"  # Add your city IDs here

minio:
  endpoint: "http://minio:9000"
  access_key: "access_key"
  secret_key: "secret_key"
  bucket: "food-panda-vendors"
  region: "us-east-1"
```

## Running with Docker

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd foodpanda-etl
   ```

2. Build and run using Docker Compose:
   ```bash
   docker compose up --build
   ```

This will:
- Start a MinIO instance
- Create required buckets
- Run the ETL process
- Store data in the configured MinIO bucket

### Accessing MinIO Console
- URL: http://localhost:9001
- Access Key: access_key
- Secret Key: secret_key

## Standalone Setup

1. Install Rust (1.85 or later):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. Install system dependencies (Debian/Ubuntu):
   ```bash
   sudo apt-get update && sudo apt-get install -y \
       build-essential \
       cmake \
       perl \
       pkg-config \
       libclang-dev
   ```

3. Clone the repository:
   ```bash
   git clone <repository-url>
   cd foodpanda-etl
   ```

4. Create required directories:
   ```bash
   mkdir -p data logs
   ```

5. Update configuration:
   - Edit `config/default.yaml` with your MinIO credentials and endpoint
   - Set environment variables:
     ```bash
     export USER_LOGIN=your_username
     export OUTPUT_DIR=./data
     export MINIO_ENDPOINT=http://localhost:9000
     export MINIO_ACCESS_KEY=access_key
     export MINIO_SECRET_KEY=secret_key
     ```

6. Build and run:
   ```bash
   cargo build --release
   ./target/release/foodpanda_etl
   ```

## Output

The application generates:
- JSON files in the `data` directory (temporary)
- Parquet files uploaded to MinIO with the structure:
  ```
  food-panda-vendors/
  └── city_id=<city_id>/
      └── year=<year>/
          └── month=<month>/
              └── day=<day>/
                  └── vendors_<timestamp>.parquet
  ```
- Detailed logs in the `logs` directory

## Environment Variables

- `USER_LOGIN`: Username for logging (default: "default_user")
- `OUTPUT_DIR`: Directory for temporary JSON files (default: "data")
- `MINIO_ENDPOINT`: MinIO endpoint URL
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key

## Logging

Logs are written to both:
- Console (JSON format)
- File (`logs/foodpanda_etl_<timestamp>_<user>.log`)