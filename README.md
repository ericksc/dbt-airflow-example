# my_dbt_project: Ready-to-Run dbt + BigQuery Example

This project scaffolds a complete dbt workflow for BigQuery, including example tables, models, and tests.

---

## 1. Environment Setup

Install dbt for BigQuery:

```sh
# Create virtualenv
python3 -m venv dbt-env
source dbt-env/bin/activate

# Install dbt for BigQuery
pip install dbt-bigquery

# Check installation
dbt --version
```

You should see `dbt-core` and `dbt-bigquery` listed.

---

## 2. Google Cloud Setup

- **Enable APIs**:  
  - BigQuery API  
  - IAM API

- **Create Service Account**:  
  - Roles:  
    - BigQuery Data Viewer  
    - BigQuery Data Editor  
  - Download key as JSON:  
    - Save as: `~/.gcp/dbt-service-key.json`

- **Set environment variable**:
  ```sh
  export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/dbt-service-key.json
  ```

---

## 3. Create BigQuery Datasets

```sql
CREATE SCHEMA IF NOT EXISTS `my_project.raw`;
CREATE SCHEMA IF NOT EXISTS `my_project.analytics`;
```

---

## 4. Initialize dbt Project

```sh
dbt init my_dbt_project
```

Adapter: bigquery  
Project ID: `my_project`  
Dataset: `analytics`  
Keyfile: `/home/erick/.gcp/dbt-service-key.json`

---

## 5. Configure dbt Profile

Edit `profiles.yml` (or `~/.dbt/profiles.yml`):

```yaml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: my_project
      dataset: analytics
      keyfile: /home/erick/.gcp/dbt-service-key.json
      threads: 4
      timeout_seconds: 300
      location: US
```

---

## 6. Define Sources (raw tables)

`models/staging/src.yml`:

```yaml
version: 2

sources:
  - name: raw
    database: my_project
    schema: raw
    tables:
      - name: sales
        description: "Raw sales transactions table"
```

---

## 7. Create Staging Model

`models/staging/stg_sales.sql`:

```sql
{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    product_id,
    amount,
    order_date
FROM {{ source('raw', 'sales') }}
```

---

## 8. Create Intermediate Model

`models/intermediate/int_sales_summary.sql`:

```sql
{{ config(materialized='table') }}

SELECT
    customer_id,
    COUNT(order_id) AS num_orders,
    SUM(amount) AS total_amount,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
FROM {{ ref('stg_sales') }}
GROUP BY customer_id
```

---

## 9. Add Schema Tests + Docs

`models/schema.yml`:

```yaml
version: 2

models:
  - name: stg_sales
    description: "Staging model for sales raw data"
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
  - name: int_sales_summary
    description: "Aggregated sales summary per customer"
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
```

---

## 10. BigQuery Example Tables

Run in BigQuery to seed test data:

```sql
CREATE SCHEMA IF NOT EXISTS `my_project.raw`;
CREATE SCHEMA IF NOT EXISTS `my_project.analytics`;

CREATE OR REPLACE TABLE `my_project.raw.sales` AS
SELECT
  GENERATE_UUID() AS order_id,
  CAST(FLOOR(RAND()*1000) AS INT64) AS customer_id,
  CAST(FLOOR(RAND()*100) AS INT64) AS product_id,
  CAST(ROUND(RAND()*100,2) AS NUMERIC) AS amount,
  DATE_SUB(CURRENT_DATE(), INTERVAL CAST(FLOOR(RAND()*365) AS INT64) DAY) AS order_date
FROM UNNEST(GENERATE_ARRAY(1, 500)) AS x;
```

---

## 11. Run dbt

```sh
# Test connection
dbt debug

# Run models
dbt run

# Test constraints
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

---

## ✅ Results

- `stg_sales` → view in `analytics`
- `int_sales_summary` → table in `analytics` (grouped by customer)

---

## Project Structure

```
my_dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── src.yml
│   │   └── stg_sales.sql
│   ├── intermediate/
│   │   └── int_sales_summary.sql
│   └── schema.yml
└── profiles.yml
```

---

You now have a ready-to-run dbt project for BigQuery, including example tables

# Airflow + Astronomer + GCP: Sync & Run dbt Projects from GCS

This guide shows how to orchestrate dbt runs in Airflow (Astronomer) by syncing your dbt project from Google Cloud Storage (GCS) to your Airflow worker at DAG runtime.

---

## 1. Store dbt Project in GCS

Organize your bucket as follows:

```
gs://my-dbt-projects/my_dbt_project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   ├── intermediate/
│   └── mart/
└── profiles.yml
```

GCS stores objects with `/` in their names, so folders are just prefixes.

---

## 2. Airflow DAG: Recursive Sync from GCS

Astronomer includes the GCS provider.  
You can use `GCSSynchronizeBucketsOperator` (provider >= 10.x) or fallback to `gsutil rsync` via `BashOperator`.

### Option A: GCSSynchronizeBucketsOperator

```python
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
import subprocess
from pendulum import datetime

DBT_LOCAL_DIR = "/usr/local/airflow/dbt/my_dbt_project"
GCS_BUCKET = "my-dbt-projects"
GCS_PREFIX = "my_dbt_project"

@dag(schedule="@daily", start_date=datetime(2025,1,1), catchup=False, tags=["dbt","gcs"])
def dbt_bq_pipeline():
    # 1. Sync dbt project from GCS → local
    sync_dbt = GCSSynchronizeBucketsOperator(
        task_id="sync_dbt_project",
        source_bucket=GCS_BUCKET,
        source_object=GCS_PREFIX,
        destination_bucket=None,          # not syncing bucket → bucket
        destination_object=DBT_LOCAL_DIR, # local path
        recursive=True,
        delete_extra_files=True
    )

    @task
    def run_dbt(stage: str):
        subprocess.run(
            ["dbt", "run", "-m", stage],
            cwd=DBT_LOCAL_DIR,
            check=True
        )

    @task
    def run_tests():
        subprocess.run(
            ["dbt", "test"],
            cwd=DBT_LOCAL_DIR,
            check=True
        )

    s = run_dbt("staging")
    i = run_dbt("intermediate")
    m = run_dbt("mart")
    t = run_tests()

    sync_dbt >> s >> i >> m >> t

dbt_bq_pipeline()
```

---

## 3. Best Practices

- **GCS Layout**: Store your dbt project under a clear prefix (e.g., `my_dbt_project/`).
- **Atomic Updates**: Upload your dbt project via CI/CD, then sync in Airflow DAG.
- **Version Locking**: Tag dbt project in GCS (e.g., `my_dbt_project/v1.0.0/`) and pin Airflow DAG to a version.
- **Credentials**:
  - Airflow workers need `roles/storage.objectViewer` on the bucket.
  - Use GCP Secret Manager for service account JSON.
- **profiles.yml**: Template with environment variables for flexibility.

---

With this setup, your Airflow DAG will always run the latest (or pinned) dbt project from GCS,
