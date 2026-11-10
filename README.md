# Data Ingestion Pipeline

## What does it do?

Collects raw data from three different sources (PostgreSQL, a REST API, and a CSV file) and stores it in the Bronze layer without applying any transformations.

The Bronze layer is currently stored locally in a `bronze/` folder. The collected data will be used by the next stages for data quality checks and transformations.

## Data Sources

- **PostgreSQL:** Tables `customers`, `products`, and `orders`.
- **REST API:** DummyJSON Products API (paginated requests).
- **CSV File:** `data/sample/shipments.csv`.

## Bronze Output Structure

```
bronze/
├── postgres/
│   ├── customers/<date>/customers_<run_id>.csv
│   ├── products/<date>/products_<run_id>.csv
│   └── orders/<date>/orders_<run_id>.csv
├── rest_api/
│   └── products/<date>/products_<run_id>.json
└── csv_files/
    └── shipments/<date>/shipments_<run_id>.csv
```

Each file includes a unique `run_id` in its name to identify the ingestion run.

The folders `bronze/`, `logs/`, and `state/` are created when the script runs. They are not stored in this repository (see `.gitignore`).

## Features

- **Incremental Load (PostgreSQL only):** PostgreSQL tables use the `updated_at` column as a watermark to retrieve records newer than the last successful ingestion point. The REST API and the CSV file are read in full on every run.
- **API Pagination:** Retrieves products from the DummyJSON API using paginated requests (`limit=30`), continuing until all available pages are fetched.
- **Retry Mechanism:** Retries failed database and API operations up to 3 attempts.
- **Logging:** Writes logs to both the console and `logs/ingestion.log`.
- **Run History:** Records the status of each source in `logs/run_history.jsonl`, with one JSON line per source per run.
- **Error Handling:** If one source fails, the other sources still run. At the end, the script returns exit code `1` if any source failed, allowing workflow schedulers such as Apache Airflow to detect failures.
- **Raw Data Preservation:** Stores data in its original CSV or JSON format without applying transformations.
- **Secure Credentials:** Does not store database passwords in the source code. The password is read from the `PG_PASSWORD` environment variable or requested interactively.

## Configuration

The database connection is configured with environment variables. If a variable is not set, the default value is used.

| Variable | Default |
|---|---|
| `PG_HOST` | `localhost` |
| `PG_PORT` | `5432` |
| `PG_DB` | `postgres` |
| `PG_USER` | `postgres` |
| `PG_PASSWORD` | asked when you run the script |
| `BRONZE_DIR` | `bronze` |

## How to Run

### 1. Requirements

- Python 3.10+
- PostgreSQL
- Required Python packages:

```
pip install psycopg2-binary requests faker
```

### 2. Create the Demo Source Tables

```
python setup_source.py
```

**Warning:** This script drops and recreates the `customers`, `products`, and `orders` tables. Running it again will delete their existing data and generate new demo data containing intentional data quality issues.

### 3. Run the Ingestion Script

```
python ingestion.py
```

### 4. Test Incremental Loading

First, add 50 new orders:

```
python ingestion.py add
```

Then run ingestion again:

```
python ingestion.py
```

The ingestion process should retrieve the newly added orders based on the `updated_at` watermark.

## Next Steps

- Perform data quality checks on the Bronze files.
- Transform and clean data in the Silver layer using Apache Spark.
- Build an Apache Airflow DAG to schedule and monitor the ingestion process.
- Move Bronze storage from the local filesystem to object storage (e.g., MinIO). The `save_bronze()` function provides a central place for this change.
- Set up Docker Compose for the complete project environment.

## Author

Aesha Allam