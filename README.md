# Premier League Data Pipeline — End-to-End AWS Data Engineering Project

A **production-style data pipeline** that ingests live Premier League standings from a public API, transforms the data with PySpark on **AWS EMR Serverless**, stores results in **S3**, exposes them for SQL querying via **Athena**, and orchestrates the whole flow with **Apache Airflow**.

Built as a portfolio project to demonstrate real data engineering skills: API ingestion, distributed data processing, cloud infrastructure, workflow orchestration, and — critically — **debugging actual production failures**, not just happy-path tutorial code.

---

## Architecture

```
football-data.org API
        │
        ▼
ingest.py  ──────────────►  S3 (landing/date=YYYY-MM-DD/standings.csv)
        │                              │
        │                              ▼
        │                    EMR Serverless (PySpark)
        │                              │
        │                              ▼
        │                   S3 (processed/date=YYYY-MM-DD/*.parquet)
        │                              │
        │                              ▼
        │                       Athena (SQL queries)
        │
        └────────── orchestrated by ──────────┐
                                               ▼
                                    Apache Airflow DAG
                          (ingest_data >> run_emr_job)
```

---

## Features

- **API Ingestion**: Authenticated requests to football-data.org, with nested JSON flattening (`pd.json_normalize`) and date-partitioned raw storage
- **Distributed Transform**: PySpark job on EMR Serverless computing week-over-week point and position deltas via DataFrame joins
- **Cloud Storage**: S3 with a `landing/` (raw) and `processed/` (transformed) prefix structure within a single bucket
- **SQL Analytics**: Athena external table over partitioned Parquet output, with partition pruning via `date=` Hive-style folder naming
- **Workflow Orchestration**: Airflow DAG enforcing that ingestion must succeed before the EMR transform job can run
- **Resilient to missing history**: Gracefully handles the start of a season (no prior week's data to compare against) without crashing or producing an inconsistent schema
- **Secrets management**: API keys and AWS identifiers loaded from `.env`, never hardcoded or committed

---

## Tech Stack

**Data Processing**

- Python 3.12
- pandas (lightweight ingestion-time transforms)
- PySpark (distributed transform, joins, window-over-time comparison)
- Apache Airflow 2.9 (orchestration)

**Cloud / Infrastructure**

- AWS S3 (data lake: raw + processed layers)
- AWS EMR Serverless (managed Spark execution)
- AWS Athena (serverless SQL over Parquet)
- AWS IAM (execution roles for EMR)
- boto3 (S3 interaction, existence checks)

**Tooling**

- python-dotenv (secrets/config)
- AWS CLI (deployment, job submission)

---

## Requirements

- Python 3.12+
- AWS account with S3, EMR Serverless, and Athena access configured
- AWS CLI configured locally (`aws configure`)
- Free API key from [football-data.org](https://www.football-data.org/)

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/pl_data_analysis.git
cd pl_data_analysis
```

### 2. Set up the ingestion/Spark environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Set up the Airflow environment

```bash
python3 -m venv airflow-venv
source airflow-venv/bin/activate
pip install -r requirements-airflow.txt
export AIRFLOW_HOME=~/airflow
airflow db init
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
API_KEY=your_football_data_api_key
APPLICATION_ID=your_emr_serverless_application_id
EXECUTION_ROLE_ARN=arn:aws:iam::your_account_id:role/EMRServerlessRole
REGION=eu-west-2
BUCKET_NAME=your-s3-bucket-name
```

---

## Usage

### Run ingestion manually

```bash
source .venv/bin/activate
python ingest.py
```

Fetches current standings and writes a date-partitioned CSV to `data/date=YYYY-MM-DD/standings.csv`, then uploads it to `s3://your-bucket/landing/date=YYYY-MM-DD/standings.csv`.

### Run the Spark transform on EMR Serverless

```bash
aws s3 cp spark_pipeline.py s3://your-bucket/scripts/
./aws_emr.bash
```

Triggers an EMR Serverless job that reads the latest landing data, joins it against the prior week's snapshot (if available), and writes partitioned Parquet to `processed/`.

### Run the full pipeline via Airflow

```bash
source airflow-venv/bin/activate
airflow webserver --port 8080 &
airflow scheduler &
```

Visit `http://localhost:8080`, trigger the `pl_analysis` DAG. Ingestion and the EMR job run in the correct dependency order automatically.

### Query results in Athena

```sql
CREATE EXTERNAL TABLE standings (
    position int,
    name string,
    points int,
    points_gained int,
    positions_gained int
)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://your-bucket/processed/';

MSCK REPAIR TABLE standings;

SELECT * FROM standings WHERE date = '2026-06-22';
```

---

## Project Structure

```
pl_data_analysis/
│
├── ingest.py                   # API ingestion → flatten → CSV → S3 upload
├── spark_pipeline.py           # PySpark transform: join, deltas, fallback branch
├── aws_emr.bash                # Triggers EMR Serverless job with dynamic date injection
├── requirements.txt            # Dependencies for ingestion/Spark venv
├── requirements-airflow.txt    # Dependencies for Airflow venv
├── .env                        # API keys, AWS identifiers (gitignored)
├── .gitignore
│
├── dags/
│   └── pl_analysis_pipeline.py # Airflow DAG: ingest_data >> run_emr_job
│
└── data/
    └── sample/
        └── standings.csv       # Example raw output, committed for reviewers
```

---

## Roadmap

- [ ] Migrate local Airflow to AWS MWAA (managed orchestration)
- [ ] Add dbt for SQL-based transformations downstream of Athena
- [ ] Extend join logic to track multi-week form trends, not just week-over-week deltas