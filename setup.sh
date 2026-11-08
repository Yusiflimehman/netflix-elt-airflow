#!/usr/bin/env bash
# Creates the Netflix ELT project files. Run once: bash setup.sh
set -e

cat > 'README.md' <<'__END_OF_FILE__'
# Netflix Subscriptions – ELT pipeline (Inbank Data Engineer task)

An Airflow DAG loads `Netflix_dataset.csv` into SQLite using **ELT**. Python only moves the file into a raw table and orchestrates the steps. All transformations, quality checks and report queries are **SQL** files.

```
netflix_elt/
├── dags/
│   ├── netflix_elt_dag.py          # Airflow DAG (TaskFlow API)
│   ├── netflix_pipeline/tasks.py   # task functions (no Airflow imports → also runnable locally)
│   └── sql/
│       ├── 01_create_schema.sql    # raw, staging, normalized model, DQ tables
│       ├── 02_transform_staging.sql# typing, trimming, date parsing
│       ├── 03_quality_checks.sql   # CRITICAL / WARNING / INFO checks
│       ├── 04_load_model.sql       # idempotent upserts into the model
│       └── reports/                # answers to question 4 a–d
├── data/
│   ├── Netflix_dataset.csv         # source file (drop new files here)
│   ├── netflix.db                  # SQLite database (created by the pipeline)
│   └── reports/*.csv               # exported results of the 4 queries
├── run_local.py                    # same steps without Airflow
└── docker-compose.yaml             # one-container Airflow
```

## How to run

**With Airflow (Docker)**
```bash
echo "AIRFLOW_UID=$(id -u)" > .env        # Linux only: lets the container write to ./data
docker compose up -d
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```
Open http://localhost:8080 and log in as `admin` with that password. The DAG `netflix_subscriptions_elt` is unpaused and runs automatically. You can also trigger it with ▶. The results are written to `data/netflix.db` and `data/reports/`.

**Without Airflow**
```bash
python run_local.py
```

## DAG

```
wait_for_source_file → create_schema → extract_load_raw → transform_staging
   → data_quality_checks → load_model → report_a / report_b / report_c / report_d
```

| Step | What happens | Why |
|---|---|---|
| `wait_for_source_file` | Sensor waits until the file exists and its header matches. | The run does not fail just because the file arrives late. A changed file layout is caught before anything is loaded. |
| `extract_load_raw` | The CSV is copied 1:1 as TEXT into `raw_netflix_subscriptions`, with `batch_id`, `loaded_at`, file name and row number. | This is ELT: nothing is lost or changed on the way in. The raw layer keeps history, so any batch can be audited or reprocessed. |
| `transform_staging` | SQL trims values, casts types, converts `dd.mm.yyyy` to ISO dates and parses `"1 Month"` → `1`. | A value is cast only if it really is numeric. SQLite would otherwise silently turn `'abc'` into `0`. Bad values become NULL, so the checks can catch them. |
| `data_quality_checks` | 17 SQL checks are run and stored in `dq_check_results`. Any failed CRITICAL check fails the task, with no retry. | A bad file must never reach the model. Results are kept per batch, so trends can be monitored. |
| `load_model` | `INSERT … ON CONFLICT DO UPDATE` into the normalized tables. | The load is idempotent: re-running a batch updates rows instead of duplicating them. |
| `report_*` | Each query in `sql/reports/` is materialized as an `rpt_*` table and exported to CSV. | BI tools can read the tables. The CSVs are easy to share. |

## 1. Normalized data model (3NF)

```mermaid
erDiagram
    dim_country ||--o{ customer : "lives in"
    customer ||--|| subscription : "holds"
    dim_subscription_plan ||--o{ subscription : "is on"
    subscription ||--|| viewing_activity : "has usage"
    dim_device ||--o{ viewing_activity : "watched on"

    dim_country { int country_id PK  text country_name UK }
    dim_subscription_plan { int plan_id PK  text plan_name UK  int tier_order }
    dim_device { int device_id PK  text device_name UK }
    customer { int customer_id PK  int age  text gender  int country_id FK }
    subscription { int subscription_id PK  int customer_id FK  int plan_id FK  numeric monthly_revenue  date join_date  date last_payment_date  int plan_duration_months  int active_profiles  int household_profile_ind }
    viewing_activity { int subscription_id PK  int device_id FK  int movies_watched  int series_watched }
```

Why the model looks like this:
- **Repeating text goes into lookup tables.** Country, plan and device each get their own table. A value is stored once, a typo cannot create a new "country", and foreign keys enforce valid values.
- **Person, contract and usage are separate tables.** `customer` holds who the person is, `subscription` holds what they bought and pay, and `viewing_activity` holds how they watch. These change for different reasons and at different times. For example, a customer can change plan without their age or country changing. The source has one row per User ID, so the relationships are 1:1 today. The split lets a customer have several contracts later without redesigning the model.
- **`monthly_revenue` stays on `subscription`, not on the plan.** The data shows the same plan sold at 6 different prices (see below), so price is not a property of the plan.
- **`dim_subscription_plan` is seeded reference data with `tier_order`.** The list of allowed plans is a business decision. A new plan arriving in a file is flagged instead of being accepted silently.
- **Invalid ages are stored as NULL** and protected by a CHECK constraint. The original value stays in the raw layer for audit.

## 2. ELT load & repeatability

- **Python + SQL.** Python reads the file and bulk-inserts raw text. SQL does every transformation inside the database.
- **Scheduled.** The DAG runs `@daily`. The sensor waits for the file. Use `max_active_runs=1`, retries with a delay, and no retry for DQ failures.
- **Idempotent.** The raw layer is delete-then-insert per `batch_id` (the Airflow `run_id`). The model uses upserts. All DDL uses `IF NOT EXISTS`. Retrying or re-triggering any run gives the same result. I tested this by loading the same batch twice and then a second batch: the model still had 2,500 customers.
- **Auditable.** Every model row has `last_batch_id`, and the raw layer keeps every file ever loaded.
- **Production next steps:** move to Postgres (or another warehouse), land files in object storage, and skip files that were already loaded using a file hash. Add SCD2 history on `subscription` to track plan and price changes over time. Alert on DQ warnings (Slack/e-mail) and move the checks into dbt tests or Great Expectations.

## 3. Data-quality controls & odd findings

Controls in `03_quality_checks.sql`:

| Severity | Checks |
|---|---|
| CRITICAL (stop the load) | non-empty batch, raw = staging row count, unique User ID, mandatory fields present and parseable, plan in reference list, household ind ∈ {1,2}, revenue > 0, last payment ≥ join date, no future dates |
| WARNING (load, flag) | age 18–100, gender domain, one price per plan, Premium not cheaper than Basic, ≤ 5 profiles, single-profile account not "multi-household" |
| INFO | constant columns, customers with no recent payment |

There are also controls outside this file: the header is validated, the field count is checked per row, and the database itself enforces FK and CHECK constraints.

**Odd things found in the dataset:**
1. **Price does not depend on the plan.** Basic, Standard and Premium each have every price from 10 to 15. 602 Premium subscribers pay less than some Basic subscribers. Only the first 73 rows look consistent (Basic 10, Standard 12, Premium 15).
2. **Impossible ages:** 904, 107, 2, 15 and 17 (5 rows, User IDs 1693, 1705, 1869, 1922, 2166). The ages 2, 15 and 17 are minors, who cannot be contract holders.
3. **Active Profiles goes up to 8** (869 accounts have more than 5), but Netflix allows at most 5 profiles.
4. **150 accounts have 1 profile but are flagged "profiles from different households"**, which is logically impossible.
5. **`Plan Duration` is "1 Month" for every row**, so the column carries no information.
6. **The data looks synthetic.** Eight countries have exactly 183 customers each. Plan mix per country is extreme: UK is 98% Standard, Mexico 98% Standard, Italy 96% Basic. Movies and series counts are evenly spread between 1 and 10.
7. **No churn is visible.** Every last payment falls between 10.06.2023 and 15.07.2023, so the file is a snapshot of active customers only.
8. **Formatting issues:** some column names end with a space, the delimiter is `;`, and dates are in `dd.mm.yyyy`.

## 4. Queries and results

| | Query | Answer (from `data/reports/`) |
|---|---|---|
| a | `a_most_profitable_country.sql` | **United States**: 5,664/month MRR, 63,218 estimated lifetime revenue. Spain is a very close 2nd (5,662). There is no cost data, so revenue is used as the measure of profit. Lifetime revenue = monthly fee × the number of monthly bills from join month to last-payment month. |
| b | `b_most_popular_package_per_country.sql` | Basic: US, Canada, Brazil, Germany, Italy. Premium: Spain, France, Australia. Standard: UK, Mexico. Ties are kept by using `RANK()`. |
| c | `c_household_sharing_potential.sql` | **United States**: 210 sharing accounts, +627.90/month at a 2.99 fee. Italy has the highest *relative* uplift (+12.0%). |
| d | `d_content_popularity_by_segment.sql` | Country × age group × gender × device, with totals, averages, movies share and preferred content. Totals reconcile with the model (13,916 movies, 6,195 series). Invalid ages are shown as "Unknown" instead of being dropped. |

**Assumptions**
- `Household Profile Ind`: 1 = all profiles are in one household, 2 = profiles are in more than one household. Only accounts with 2 **and** more than 1 profile count as sharing.
- The extra fee in (c) is a flat amount per sharing account per month. It is set in one place (`params` CTE) so other prices can be simulated. With a flat fee, the ranking does not depend on the fee value.
- Age groups in (d): 18–29, 30–39, 40–49, 50+.
__END_OF_FILE__

cat > 'docker-compose.yaml' <<'__END_OF_FILE__'
# Single-container Airflow for running the task locally.
#   docker compose up -d
#   UI: http://localhost:8080  user: admin
#   password: docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
services:
  airflow:
    image: apache/airflow:2.10.5-python3.11
    command: standalone
    user: "${AIRFLOW_UID:-50000}:0"
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
      NETFLIX_DATA_DIR: /opt/airflow/data
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
__END_OF_FILE__

cat > 'run_local.py' <<'__END_OF_FILE__'
"""
Run the same pipeline steps as the Airflow DAG, without Airflow.
Useful for a quick check:  python run_local.py
"""
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "dags"))
from netflix_pipeline import tasks as t  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

if __name__ == "__main__":
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "manual__" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    assert t.source_file_ready(), f"Source file not found: {t.SOURCE_FILE}"
    t.create_schema()
    t.extract_load_raw(batch_id)
    t.transform_staging(batch_id)
    t.run_quality_checks(batch_id)
    t.load_model(batch_id)
    for name in t.REPORTS:
        t.build_report(name)
    print(f"\nDone. Batch {batch_id} -> {t.DB_PATH}, reports in {t.REPORT_DIR}")
__END_OF_FILE__

mkdir -p dags
cat > 'dags/.airflowignore' <<'__END_OF_FILE__'
netflix_pipeline/
sql/
__END_OF_FILE__

mkdir -p dags
cat > 'dags/netflix_elt_dag.py' <<'__END_OF_FILE__'
"""
### Netflix subscriptions ELT

`wait_for_source_file` → `create_schema` → `extract_load_raw` → `transform_staging`
→ `data_quality_checks` → `load_model` → 4 report tasks (run in parallel)

* **E/L** – the CSV is copied 1:1 (all text) into `raw_netflix_subscriptions`, tagged with the run's `batch_id`.
* **T** – SQL files in `dags/sql/` type & clean the batch, check it, and upsert it into the normalized model.
* A failing **CRITICAL** quality check stops the run before the model is touched.
* Every step is idempotent (delete-by-batch / upsert), so any run can be retried or re-triggered safely.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.sensors.python import PythonSensor

from netflix_pipeline import tasks as t


def _batch_id() -> str:
    # run_id is unique per DAG run (e.g. scheduled__2026-11-08T00:00:00+00:00)
    return get_current_context()["run_id"]


@dag(
    dag_id="netflix_subscriptions_elt",
    description="Load Netflix subscription CSV into a normalized SQLite model and build reports",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # SQLite has a single writer; also keeps batches ordered
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["netflix", "elt", "inbank"],
    doc_md=__doc__,
)
def netflix_subscriptions_elt():
    wait_for_source_file = PythonSensor(
        task_id="wait_for_source_file",
        python_callable=t.source_file_ready,
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    @task
    def create_schema():
        t.create_schema()

    @task
    def extract_load_raw() -> int:
        return t.extract_load_raw(_batch_id())

    @task
    def transform_staging():
        t.transform_staging(_batch_id())

    @task(retries=0)  # a data problem will not fix itself on retry
    def data_quality_checks():
        return t.run_quality_checks(_batch_id())

    @task
    def load_model():
        t.load_model(_batch_id())

    model_loaded = load_model()
    (
        wait_for_source_file
        >> create_schema()
        >> extract_load_raw()
        >> transform_staging()
        >> data_quality_checks()
        >> model_loaded
    )

    @task
    def build_report(name: str) -> str:
        return str(t.build_report(name))

    for report_name in t.REPORTS:
        model_loaded >> build_report.override(task_id=f"report_{report_name}")(report_name)


netflix_subscriptions_elt()
__END_OF_FILE__

mkdir -p dags/netflix_pipeline
cat > 'dags/netflix_pipeline/__init__.py' <<'__END_OF_FILE__'

__END_OF_FILE__

mkdir -p dags/netflix_pipeline
cat > 'dags/netflix_pipeline/tasks.py' <<'__END_OF_FILE__'
"""
Task functions for the Netflix ELT pipeline.

Python is used only for what SQL cannot do well: reading the file,
moving bytes into the database, orchestrating SQL files and failing the
run when quality checks fail. Every transformation, check and report is
plain SQL in dags/sql/.

The functions have no Airflow imports, so the same code runs in the
Airflow DAG (dags/netflix_elt_dag.py) and locally (run_local.py).
"""
from __future__ import annotations

import csv
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DAGS_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = DAGS_DIR / "sql"
REPORT_SQL_DIR = SQL_DIR / "reports"

DATA_DIR = Path(os.environ.get("NETFLIX_DATA_DIR", DAGS_DIR.parent / "data"))
SOURCE_FILE = Path(os.environ.get("NETFLIX_SOURCE_FILE", DATA_DIR / "Netflix_dataset.csv"))
DB_PATH = Path(os.environ.get("NETFLIX_DB_PATH", DATA_DIR / "netflix.db"))
REPORT_DIR = Path(os.environ.get("NETFLIX_REPORT_DIR", DATA_DIR / "reports"))

# Source header (after trimming the trailing spaces some columns have) -> raw column
COLUMN_MAP = {
    "User ID": "user_id",
    "Subscription Type": "subscription_type",
    "Monthly Revenue": "monthly_revenue",
    "Join Date": "join_date",
    "Last Payment Date": "last_payment_date",
    "Country": "country",
    "Age": "age",
    "Gender": "gender",
    "Device": "device",
    "Plan Duration": "plan_duration",
    "Active Profiles": "active_profiles",
    "Household Profile Ind": "household_profile_ind",
    "Movies Watched": "movies_watched",
    "Series Watched": "series_watched",
}

REPORTS = {
    "a_most_profitable_country": "rpt_a_most_profitable_country",
    "b_most_popular_package_per_country": "rpt_b_most_popular_package_per_country",
    "c_household_sharing_potential": "rpt_c_household_sharing_potential",
    "d_content_popularity_by_segment": "rpt_d_content_popularity_by_segment",
}


class DataQualityError(Exception):
    """Raised when a CRITICAL data-quality check fails."""


# ---------------------------------------------------------------- helpers
@contextmanager
def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _set_batch(conn: sqlite3.Connection, batch_id: str) -> None:
    conn.execute("DELETE FROM etl_run_context")
    conn.execute("INSERT INTO etl_run_context (batch_id) VALUES (?)", (batch_id,))


def _run_sql_file(conn: sqlite3.Connection, name: str) -> None:
    path = SQL_DIR / name
    log.info("Running %s", path)
    # executescript commits implicitly, so wrap the whole file in one transaction
    conn.commit()
    conn.executescript("BEGIN;\n" + path.read_text(encoding="utf-8") + "\nCOMMIT;")


def _read_header(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return [h.strip() for h in next(csv.reader(f, delimiter=";"))]


# ------------------------------------------------------------------ tasks
def source_file_ready() -> bool:
    """Sensor callable: file exists, is not empty and has the expected header."""
    if not SOURCE_FILE.exists() or SOURCE_FILE.stat().st_size == 0:
        log.info("Waiting for %s", SOURCE_FILE)
        return False
    header = _read_header(SOURCE_FILE)
    if header != list(COLUMN_MAP):
        raise ValueError(f"Unexpected header in {SOURCE_FILE}: {header}")
    return True


def create_schema() -> None:
    with connect() as conn:
        _run_sql_file(conn, "01_create_schema.sql")


def extract_load_raw(batch_id: str) -> int:
    """E + L: copy the file 1:1 (as text) into the raw table. Idempotent per batch."""
    loaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with SOURCE_FILE.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = [h.strip() for h in next(reader)]
        if header != list(COLUMN_MAP):
            raise ValueError(f"Unexpected header: {header}")
        rows = [
            (batch_id, loaded_at, SOURCE_FILE.name, line_no, *values)
            for line_no, values in enumerate(reader, start=2)
            if any(v.strip() for v in values)          # skip blank lines
        ]
    bad = [r[3] for r in rows if len(r) != 4 + len(COLUMN_MAP)]
    if bad:
        raise ValueError(f"Rows with wrong number of fields at lines: {bad[:20]}")

    columns = ["batch_id", "loaded_at", "source_file", "source_row_number", *COLUMN_MAP.values()]
    with connect() as conn:
        conn.execute("DELETE FROM raw_netflix_subscriptions WHERE batch_id = ?", (batch_id,))
        conn.executemany(
            f"INSERT INTO raw_netflix_subscriptions ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' * len(columns))})",
            rows,
        )
    log.info("Loaded %s raw rows for batch %s", len(rows), batch_id)
    return len(rows)


def transform_staging(batch_id: str) -> None:
    with connect() as conn:
        _set_batch(conn, batch_id)
        _run_sql_file(conn, "02_transform_staging.sql")


def run_quality_checks(batch_id: str) -> list[dict]:
    with connect() as conn:
        _set_batch(conn, batch_id)
        _run_sql_file(conn, "03_quality_checks.sql")
        conn.row_factory = sqlite3.Row
        results = [
            dict(r)
            for r in conn.execute(
                "SELECT check_name, severity, failed_rows, description FROM dq_check_results "
                "WHERE batch_id = ? ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 "
                "WHEN 'WARNING' THEN 2 ELSE 3 END, check_name",
                (batch_id,),
            )
        ]
    for r in results:
        status = "OK  " if r["failed_rows"] == 0 else "FAIL"
        log.info("[%s] %-8s %-42s %5s rows  %s", status, r["severity"], r["check_name"],
                 r["failed_rows"], r["description"])
    critical = [r for r in results if r["severity"] == "CRITICAL" and r["failed_rows"] > 0]
    if critical:
        raise DataQualityError(
            "Critical data-quality checks failed: "
            + ", ".join(f"{r['check_name']} ({r['failed_rows']} rows)" for r in critical)
        )
    return results


def load_model(batch_id: str) -> None:
    with connect() as conn:
        _set_batch(conn, batch_id)
        _run_sql_file(conn, "04_load_model.sql")


def build_report(report_name: str) -> Path:
    """Materialize one report query as rpt_* table and export it to CSV."""
    table = REPORTS[report_name]
    query = (REPORT_SQL_DIR / f"{report_name}.sql").read_text(encoding="utf-8").strip().rstrip(";")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"{report_name}.csv"
    with connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS\n{query}")
        cur = conn.execute(f"SELECT * FROM {table}")
        with out.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([c[0] for c in cur.description])
            w.writerows(cur)
    log.info("Report %s -> table %s, file %s", report_name, table, out)
    return out
__END_OF_FILE__

mkdir -p dags/sql
cat > 'dags/sql/01_create_schema.sql' <<'__END_OF_FILE__'
-- =====================================================================
-- 01_create_schema.sql
-- Idempotent DDL for all layers (safe to run on every DAG run).
--   raw_*   : source data exactly as received (all TEXT), one batch per run
--   stg_*   : typed, trimmed copy of the current batch
--   dim_* / customer / subscription / viewing_activity : normalized model (3NF)
--   dq_*    : data-quality results per batch
-- =====================================================================

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------
-- Run context: the Python task writes the current batch_id here so that
-- every SQL file can stay parameter-free.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_context (
    batch_id TEXT NOT NULL
);

-- ---------------------------------------------------------------------
-- RAW layer (E + L of ELT): no types, no cleaning, full history kept.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_netflix_subscriptions (
    batch_id              TEXT    NOT NULL,
    loaded_at             TEXT    NOT NULL,
    source_file           TEXT    NOT NULL,
    source_row_number     INTEGER NOT NULL,
    user_id               TEXT,
    subscription_type     TEXT,
    monthly_revenue       TEXT,
    join_date             TEXT,
    last_payment_date     TEXT,
    country               TEXT,
    age                   TEXT,
    gender                TEXT,
    device                TEXT,
    plan_duration         TEXT,
    active_profiles       TEXT,
    household_profile_ind TEXT,
    movies_watched        TEXT,
    series_watched        TEXT,
    PRIMARY KEY (batch_id, source_row_number)
);

CREATE VIEW IF NOT EXISTS v_raw_current_batch AS
SELECT r.*
FROM raw_netflix_subscriptions r
WHERE r.batch_id = (SELECT batch_id FROM etl_run_context);

-- ---------------------------------------------------------------------
-- STAGING layer (T of ELT, step 1): typed + standardized current batch.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_netflix_subscriptions (
    batch_id              TEXT    NOT NULL,
    user_id               INTEGER,
    subscription_type     TEXT,
    monthly_revenue       NUMERIC,
    join_date             TEXT,      -- ISO yyyy-mm-dd, NULL if unparseable
    last_payment_date     TEXT,      -- ISO yyyy-mm-dd, NULL if unparseable
    country               TEXT,
    age                   INTEGER,
    gender                TEXT,
    device                TEXT,
    plan_duration_months  INTEGER,
    active_profiles       INTEGER,
    household_profile_ind INTEGER,
    movies_watched        INTEGER,
    series_watched        INTEGER
);

-- ---------------------------------------------------------------------
-- NORMALIZED MODEL (3NF)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_subscription_plan (
    plan_id    INTEGER PRIMARY KEY,
    plan_name  TEXT    NOT NULL UNIQUE,
    tier_order INTEGER NOT NULL UNIQUE      -- 1 = cheapest tier
);

-- Reference data: the allowed plans are a business decision, not data.
INSERT OR IGNORE INTO dim_subscription_plan (plan_id, plan_name, tier_order) VALUES
    (1, 'Basic',    1),
    (2, 'Standard', 2),
    (3, 'Premium',  3);

CREATE TABLE IF NOT EXISTS dim_device (
    device_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    device_name TEXT NOT NULL UNIQUE
);

-- Person-level attributes (who the customer is).
CREATE TABLE IF NOT EXISTS customer (
    customer_id      INTEGER PRIMARY KEY,                -- = source "User ID"
    age              INTEGER CHECK (age IS NULL OR age BETWEEN 18 AND 100),
    gender           TEXT,
    country_id       INTEGER NOT NULL REFERENCES dim_country (country_id),
    last_batch_id    TEXT    NOT NULL
);

-- Contract-level attributes (what the customer bought and pays).
CREATE TABLE IF NOT EXISTS subscription (
    subscription_id       INTEGER PRIMARY KEY,           -- one contract per User ID in the source
    customer_id           INTEGER NOT NULL UNIQUE REFERENCES customer (customer_id),
    plan_id               INTEGER NOT NULL REFERENCES dim_subscription_plan (plan_id),
    monthly_revenue       NUMERIC NOT NULL CHECK (monthly_revenue > 0),
    join_date             TEXT    NOT NULL,
    last_payment_date     TEXT    NOT NULL,
    plan_duration_months  INTEGER NOT NULL CHECK (plan_duration_months > 0),
    active_profiles       INTEGER NOT NULL CHECK (active_profiles >= 1),
    household_profile_ind INTEGER NOT NULL CHECK (household_profile_ind IN (1, 2)),
    last_batch_id         TEXT    NOT NULL,
    CHECK (last_payment_date >= join_date)
);

-- Usage facts (how the subscription is consumed).
CREATE TABLE IF NOT EXISTS viewing_activity (
    subscription_id INTEGER PRIMARY KEY REFERENCES subscription (subscription_id),
    device_id       INTEGER NOT NULL REFERENCES dim_device (device_id),
    movies_watched  INTEGER NOT NULL CHECK (movies_watched >= 0),
    series_watched  INTEGER NOT NULL CHECK (series_watched >= 0),
    last_batch_id   TEXT    NOT NULL
);

-- ---------------------------------------------------------------------
-- DATA QUALITY results (one row per check per batch)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_check_results (
    batch_id    TEXT    NOT NULL,
    check_name  TEXT    NOT NULL,
    severity    TEXT    NOT NULL CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),
    failed_rows INTEGER NOT NULL,
    description TEXT    NOT NULL,
    checked_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (batch_id, check_name)
);
__END_OF_FILE__

mkdir -p dags/sql
cat > 'dags/sql/02_transform_staging.sql' <<'__END_OF_FILE__'
-- =====================================================================
-- 02_transform_staging.sql
-- Rebuild staging from the CURRENT raw batch only.
--   * trim whitespace, standardize casing of coded values
--   * cast numbers ONLY when the text is really numeric (SQLite would
--     otherwise silently turn 'abc' into 0) -> bad values become NULL
--     and are caught by the quality checks
--   * convert dd.mm.yyyy to ISO yyyy-mm-dd and reject impossible dates
-- =====================================================================

DELETE FROM stg_netflix_subscriptions;

INSERT INTO stg_netflix_subscriptions (
    batch_id, user_id, subscription_type, monthly_revenue,
    join_date, last_payment_date, country, age, gender, device,
    plan_duration_months, active_profiles, household_profile_ind,
    movies_watched, series_watched
)
WITH cleaned AS (
    SELECT
        batch_id,
        TRIM(user_id)               AS user_id,
        TRIM(subscription_type)     AS subscription_type,
        TRIM(monthly_revenue)       AS monthly_revenue,
        TRIM(join_date)             AS join_date,
        TRIM(last_payment_date)     AS last_payment_date,
        TRIM(country)               AS country,
        TRIM(age)                   AS age,
        TRIM(gender)                AS gender,
        TRIM(device)                AS device,
        LOWER(TRIM(plan_duration))  AS plan_duration,
        TRIM(active_profiles)       AS active_profiles,
        TRIM(household_profile_ind) AS household_profile_ind,
        TRIM(movies_watched)        AS movies_watched,
        TRIM(series_watched)        AS series_watched
    FROM v_raw_current_batch
),
iso AS (
    SELECT
        c.*,
        SUBSTR(join_date, 7, 4) || '-' || SUBSTR(join_date, 4, 2) || '-' || SUBSTR(join_date, 1, 2)
            AS join_iso,
        SUBSTR(last_payment_date, 7, 4) || '-' || SUBSTR(last_payment_date, 4, 2) || '-' || SUBSTR(last_payment_date, 1, 2)
            AS last_payment_iso,
        TRIM(REPLACE(REPLACE(plan_duration, 'months', ''), 'month', '')) AS plan_duration_num
    FROM cleaned c
)
SELECT
    batch_id,
    CASE WHEN user_id <> '' AND user_id NOT GLOB '*[^0-9]*' THEN CAST(user_id AS INTEGER) END,
    CASE WHEN subscription_type <> ''
         THEN UPPER(SUBSTR(subscription_type, 1, 1)) || LOWER(SUBSTR(subscription_type, 2)) END,
    CASE WHEN monthly_revenue <> '' AND monthly_revenue NOT GLOB '*[^0-9.]*'
         THEN CAST(monthly_revenue AS NUMERIC) END,
    CASE WHEN join_date GLOB '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
              AND DATE(join_iso) = join_iso THEN join_iso END,
    CASE WHEN last_payment_date GLOB '[0-9][0-9].[0-9][0-9].[0-9][0-9][0-9][0-9]'
              AND DATE(last_payment_iso) = last_payment_iso THEN last_payment_iso END,
    NULLIF(country, ''),
    CASE WHEN age <> '' AND age NOT GLOB '*[^0-9]*' THEN CAST(age AS INTEGER) END,
    CASE WHEN gender <> '' THEN UPPER(SUBSTR(gender, 1, 1)) || LOWER(SUBSTR(gender, 2)) END,
    NULLIF(device, ''),
    CASE WHEN plan_duration_num <> '' AND plan_duration_num NOT GLOB '*[^0-9]*'
         THEN CAST(plan_duration_num AS INTEGER) END,
    CASE WHEN active_profiles <> '' AND active_profiles NOT GLOB '*[^0-9]*'
         THEN CAST(active_profiles AS INTEGER) END,
    CASE WHEN household_profile_ind <> '' AND household_profile_ind NOT GLOB '*[^0-9]*'
         THEN CAST(household_profile_ind AS INTEGER) END,
    CASE WHEN movies_watched <> '' AND movies_watched NOT GLOB '*[^0-9]*'
         THEN CAST(movies_watched AS INTEGER) END,
    CASE WHEN series_watched <> '' AND series_watched NOT GLOB '*[^0-9]*'
         THEN CAST(series_watched AS INTEGER) END
FROM iso;
__END_OF_FILE__

mkdir -p dags/sql
cat > 'dags/sql/03_quality_checks.sql' <<'__END_OF_FILE__'
-- =====================================================================
-- 03_quality_checks.sql
-- Every check returns the number of offending rows for the current batch.
--   CRITICAL -> the Airflow task fails, the model is NOT updated
--   WARNING  -> loaded, but flagged/handled (e.g. invalid age -> NULL)
--   INFO     -> observation about the data, no action
-- Results are stored in dq_check_results for auditing and trend monitoring.
-- =====================================================================

DELETE FROM dq_check_results
WHERE batch_id = (SELECT batch_id FROM etl_run_context);

INSERT INTO dq_check_results (batch_id, check_name, severity, failed_rows, description)
SELECT (SELECT batch_id FROM etl_run_context), check_name, severity, failed_rows, description
FROM (
    -- ------------------------------ CRITICAL ------------------------------
    SELECT 'batch_not_empty' AS check_name, 'CRITICAL' AS severity,
           CASE WHEN (SELECT COUNT(*) FROM v_raw_current_batch) = 0 THEN 1 ELSE 0 END AS failed_rows,
           'The source file produced at least one row' AS description

    UNION ALL
    SELECT 'row_count_raw_equals_staging', 'CRITICAL',
           ABS((SELECT COUNT(*) FROM v_raw_current_batch) - (SELECT COUNT(*) FROM stg_netflix_subscriptions)),
           'No rows lost between raw and staging'

    UNION ALL
    SELECT 'unique_user_id', 'CRITICAL',
           (SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM stg_netflix_subscriptions),
           'User ID is the business key and must be unique'

    UNION ALL
    SELECT 'mandatory_fields_not_null_or_unparseable', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE user_id IS NULL OR subscription_type IS NULL OR monthly_revenue IS NULL
                OR join_date IS NULL OR last_payment_date IS NULL OR country IS NULL
                OR device IS NULL OR plan_duration_months IS NULL OR active_profiles IS NULL
                OR household_profile_ind IS NULL OR movies_watched IS NULL OR series_watched IS NULL),
           'Required values present, numeric fields numeric, dates valid dd.mm.yyyy'

    UNION ALL
    SELECT 'subscription_type_in_reference_list', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions s
             WHERE s.subscription_type NOT IN (SELECT plan_name FROM dim_subscription_plan)),
           'Plan must be Basic / Standard / Premium'

    UNION ALL
    SELECT 'household_profile_ind_domain', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE household_profile_ind NOT IN (1, 2)),
           'Household Profile Ind must be 1 or 2'

    UNION ALL
    SELECT 'revenue_positive', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE monthly_revenue <= 0),
           'Monthly revenue must be > 0'

    UNION ALL
    SELECT 'last_payment_not_before_join', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE last_payment_date < join_date),
           'A payment cannot happen before sign-up'

    UNION ALL
    SELECT 'dates_not_in_future', 'CRITICAL',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE join_date > DATE('now') OR last_payment_date > DATE('now')),
           'Join / payment dates cannot be after the load date'

    -- ------------------------------ WARNING -------------------------------
    UNION ALL
    SELECT 'age_plausible_18_100', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE age IS NULL OR age NOT BETWEEN 18 AND 100),
           'Contract holder must be an adult with a realistic age; invalid ages are loaded as NULL'

    UNION ALL
    SELECT 'gender_in_expected_values', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE gender IS NULL OR gender NOT IN ('Male', 'Female')),
           'Gender outside the expected values'

    UNION ALL
    SELECT 'single_price_per_plan', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE subscription_type IN (SELECT subscription_type
                                           FROM stg_netflix_subscriptions
                                          GROUP BY subscription_type
                                         HAVING COUNT(DISTINCT monthly_revenue) > 1)),
           'Rows belonging to a plan that has more than one monthly price'

    UNION ALL
    SELECT 'premium_not_cheaper_than_basic', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions p
             WHERE p.subscription_type = 'Premium'
               AND p.monthly_revenue < (SELECT MAX(monthly_revenue)
                                          FROM stg_netflix_subscriptions
                                         WHERE subscription_type = 'Basic')),
           'Premium subscribers paying less than some Basic subscribers (price tier inversion)'

    UNION ALL
    SELECT 'active_profiles_within_limit_5', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions WHERE active_profiles > 5),
           'Netflix allows at most 5 profiles per account'

    UNION ALL
    SELECT 'single_profile_not_multi_household', 'WARNING',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE active_profiles = 1 AND household_profile_ind = 2),
           'An account with one profile cannot have profiles in more than one household'

    -- ------------------------------ INFO ----------------------------------
    UNION ALL
    SELECT 'plan_duration_has_variation', 'INFO',
           CASE WHEN (SELECT COUNT(DISTINCT plan_duration_months) FROM stg_netflix_subscriptions) = 1
                THEN (SELECT COUNT(*) FROM stg_netflix_subscriptions) ELSE 0 END,
           'Plan Duration has a single value for every row (column carries no information)'

    UNION ALL
    SELECT 'customers_without_recent_payment', 'INFO',
           (SELECT COUNT(*) FROM stg_netflix_subscriptions
             WHERE last_payment_date < DATE((SELECT MAX(last_payment_date) FROM stg_netflix_subscriptions), '-1 month')),
           'Customers whose last payment is >1 month older than the newest payment (possible churn)'
);
__END_OF_FILE__

mkdir -p dags/sql
cat > 'dags/sql/04_load_model.sql' <<'__END_OF_FILE__'
-- =====================================================================
-- 04_load_model.sql
-- Upsert the validated staging batch into the normalized model.
-- INSERT ... ON CONFLICT makes the load idempotent: re-running the same
-- batch (or a later file with the same users) updates rows instead of
-- duplicating them.
-- ("WHERE true" is required by SQLite's parser for INSERT-SELECT upserts.)
-- =====================================================================

PRAGMA foreign_keys = ON;

-- Dimensions --------------------------------------------------------------
INSERT INTO dim_country (country_name)
SELECT DISTINCT country FROM stg_netflix_subscriptions WHERE true
ON CONFLICT (country_name) DO NOTHING;

INSERT INTO dim_device (device_name)
SELECT DISTINCT device FROM stg_netflix_subscriptions WHERE true
ON CONFLICT (device_name) DO NOTHING;

-- Customer (person) -------------------------------------------------------
INSERT INTO customer (customer_id, age, gender, country_id, last_batch_id)
SELECT s.user_id,
       CASE WHEN s.age BETWEEN 18 AND 100 THEN s.age END,   -- implausible ages -> NULL (see DQ)
       s.gender,
       c.country_id,
       s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_country c ON c.country_name = s.country
WHERE true
ON CONFLICT (customer_id) DO UPDATE SET
    age           = excluded.age,
    gender        = excluded.gender,
    country_id    = excluded.country_id,
    last_batch_id = excluded.last_batch_id;

-- Subscription (contract) -------------------------------------------------
INSERT INTO subscription (
    subscription_id, customer_id, plan_id, monthly_revenue, join_date,
    last_payment_date, plan_duration_months, active_profiles,
    household_profile_ind, last_batch_id
)
SELECT s.user_id,
       s.user_id,
       p.plan_id,
       s.monthly_revenue,
       s.join_date,
       s.last_payment_date,
       s.plan_duration_months,
       s.active_profiles,
       s.household_profile_ind,
       s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_subscription_plan p ON p.plan_name = s.subscription_type
WHERE true
ON CONFLICT (subscription_id) DO UPDATE SET
    plan_id               = excluded.plan_id,
    monthly_revenue       = excluded.monthly_revenue,
    join_date             = excluded.join_date,
    last_payment_date     = excluded.last_payment_date,
    plan_duration_months  = excluded.plan_duration_months,
    active_profiles       = excluded.active_profiles,
    household_profile_ind = excluded.household_profile_ind,
    last_batch_id         = excluded.last_batch_id;

-- Viewing activity (usage) ------------------------------------------------
INSERT INTO viewing_activity (subscription_id, device_id, movies_watched, series_watched, last_batch_id)
SELECT s.user_id, d.device_id, s.movies_watched, s.series_watched, s.batch_id
FROM stg_netflix_subscriptions s
JOIN dim_device d ON d.device_name = s.device
WHERE true
ON CONFLICT (subscription_id) DO UPDATE SET
    device_id      = excluded.device_id,
    movies_watched = excluded.movies_watched,
    series_watched = excluded.series_watched,
    last_batch_id  = excluded.last_batch_id;
__END_OF_FILE__

mkdir -p dags/sql/reports
cat > 'dags/sql/reports/a_most_profitable_country.sql' <<'__END_OF_FILE__'
-- a. The most profitable country for Netflix.
-- The dataset has no cost data, so "profit" is measured by revenue:
--   * monthly_recurring_revenue : what the country brings in every month now
--   * estimated_lifetime_revenue: monthly fee x number of monthly bills from
--     join month to last-payment month (plan is billed monthly, 1 Month)
-- Both rankings are returned; rank 1 by MRR is the answer.
WITH subscription_revenue AS (
    SELECT
        c.country_name,
        s.monthly_revenue,
        (CAST(STRFTIME('%Y', s.last_payment_date) AS INTEGER) * 12 + CAST(STRFTIME('%m', s.last_payment_date) AS INTEGER))
      - (CAST(STRFTIME('%Y', s.join_date)         AS INTEGER) * 12 + CAST(STRFTIME('%m', s.join_date)         AS INTEGER))
      + 1 AS months_billed
    FROM subscription s
    JOIN customer    cu ON cu.customer_id = s.customer_id
    JOIN dim_country c  ON c.country_id   = cu.country_id
)
SELECT
    country_name,
    COUNT(*)                                          AS subscribers,
    SUM(monthly_revenue)                              AS monthly_recurring_revenue,
    ROUND(AVG(monthly_revenue), 2)                    AS avg_revenue_per_user,
    SUM(monthly_revenue * months_billed)              AS estimated_lifetime_revenue,
    RANK() OVER (ORDER BY SUM(monthly_revenue) DESC)                 AS rank_by_mrr,
    RANK() OVER (ORDER BY SUM(monthly_revenue * months_billed) DESC) AS rank_by_lifetime_revenue
FROM subscription_revenue
GROUP BY country_name
ORDER BY monthly_recurring_revenue DESC
__END_OF_FILE__

mkdir -p dags/sql/reports
cat > 'dags/sql/reports/b_most_popular_package_per_country.sql' <<'__END_OF_FILE__'
-- b. The most popular package (subscription plan) per country.
-- Popularity = number of subscribers. RANK() keeps ties, so a country
-- with two equally popular plans returns both.
WITH plan_counts AS (
    SELECT
        c.country_name,
        p.plan_name,
        COUNT(*)             AS subscribers,
        SUM(s.monthly_revenue) AS monthly_revenue
    FROM subscription s
    JOIN customer              cu ON cu.customer_id = s.customer_id
    JOIN dim_country           c  ON c.country_id   = cu.country_id
    JOIN dim_subscription_plan p  ON p.plan_id      = s.plan_id
    GROUP BY c.country_name, p.plan_name
),
ranked AS (
    SELECT
        country_name,
        plan_name,
        subscribers,
        monthly_revenue,
        ROUND(100.0 * subscribers / SUM(subscribers) OVER (PARTITION BY country_name), 1) AS share_of_country_pct,
        RANK() OVER (PARTITION BY country_name ORDER BY subscribers DESC)               AS popularity_rank
    FROM plan_counts
)
SELECT country_name, plan_name AS most_popular_plan, subscribers, share_of_country_pct, monthly_revenue
FROM ranked
WHERE popularity_rank = 1
ORDER BY country_name
__END_OF_FILE__

mkdir -p dags/sql/reports
cat > 'dags/sql/reports/c_household_sharing_potential.sql' <<'__END_OF_FILE__'
-- c. Which country could earn the most from an extra fee for sharing
--    Netflix outside the subscriber's own household?
-- Assumptions (documented in README):
--   * household_profile_ind = 2 means "profiles are NOT all from one household"
--   * an account can only share if it has more than 1 active profile
--     (single-profile accounts flagged 2 are a data error and are excluded)
--   * extra fee is a flat amount per sharing account per month (parameter below)
-- Ranking metric: additional monthly revenue. uplift_pct shows how big it
-- is relative to the country's current revenue.
WITH params AS (
    SELECT 2.99 AS extra_household_fee        -- change here to re-simulate
),
accounts AS (
    SELECT
        c.country_name,
        s.monthly_revenue,
        CASE WHEN s.household_profile_ind = 2 AND s.active_profiles > 1 THEN 1 ELSE 0 END AS is_sharing
    FROM subscription s
    JOIN customer    cu ON cu.customer_id = s.customer_id
    JOIN dim_country c  ON c.country_id   = cu.country_id
)
SELECT
    a.country_name,
    COUNT(*)                                              AS total_accounts,
    SUM(a.is_sharing)                                     AS sharing_accounts,
    ROUND(100.0 * SUM(a.is_sharing) / COUNT(*), 1)        AS sharing_accounts_pct,
    SUM(a.monthly_revenue)                                AS current_monthly_revenue,
    ROUND(SUM(a.is_sharing) * p.extra_household_fee, 2)   AS potential_extra_monthly_revenue,
    ROUND(100.0 * SUM(a.is_sharing) * p.extra_household_fee / SUM(a.monthly_revenue), 1) AS revenue_uplift_pct,
    RANK() OVER (ORDER BY SUM(a.is_sharing) * p.extra_household_fee DESC)               AS potential_rank
FROM accounts a
CROSS JOIN params p
GROUP BY a.country_name, p.extra_household_fee
ORDER BY potential_extra_monthly_revenue DESC
__END_OF_FILE__

mkdir -p dags/sql/reports
cat > 'dags/sql/reports/d_content_popularity_by_segment.sql' <<'__END_OF_FILE__'
-- d. Popularity of Movies vs Series by customer segment and device,
--    across all markets (countries).
-- Segment = age group x gender. Invalid ages are NULL in the model and are
-- shown as 'Unknown' instead of being dropped, so totals still reconcile.
WITH base AS (
    SELECT
        c.country_name,
        CASE
            WHEN cu.age IS NULL THEN 'Unknown'
            WHEN cu.age < 30    THEN '18-29'
            WHEN cu.age < 40    THEN '30-39'
            WHEN cu.age < 50    THEN '40-49'
            ELSE '50+'
        END                     AS age_group,
        COALESCE(cu.gender, 'Unknown') AS gender,
        d.device_name,
        v.movies_watched,
        v.series_watched
    FROM viewing_activity v
    JOIN subscription s  ON s.subscription_id = v.subscription_id
    JOIN customer     cu ON cu.customer_id    = s.customer_id
    JOIN dim_country  c  ON c.country_id      = cu.country_id
    JOIN dim_device   d  ON d.device_id       = v.device_id
)
SELECT
    country_name,
    age_group,
    gender,
    device_name,
    COUNT(*)                                   AS subscribers,
    SUM(movies_watched)                        AS total_movies_watched,
    SUM(series_watched)                        AS total_series_watched,
    ROUND(AVG(movies_watched), 2)              AS avg_movies_per_subscriber,
    ROUND(AVG(series_watched), 2)              AS avg_series_per_subscriber,
    ROUND(100.0 * SUM(movies_watched) / NULLIF(SUM(movies_watched) + SUM(series_watched), 0), 1) AS movies_share_pct,
    CASE
        WHEN SUM(movies_watched) > SUM(series_watched) THEN 'Movies'
        WHEN SUM(movies_watched) < SUM(series_watched) THEN 'Series'
        ELSE 'Equal'
    END                                        AS preferred_content
FROM base
GROUP BY country_name, age_group, gender, device_name
ORDER BY country_name, age_group, gender, device_name
__END_OF_FILE__

mkdir -p data
if [ -f Netflix_dataset.csv ]; then git mv -f Netflix_dataset.csv data/Netflix_dataset.csv 2>/dev/null || mv Netflix_dataset.csv data/Netflix_dataset.csv; fi
[ -f data/Netflix_dataset.csv ] || echo 'WARNING: upload Netflix_dataset.csv to the repo first'
echo "AIRFLOW_UID=$(id -u)" > .env
printf '.env\n__pycache__/\n' > .gitignore
git add -A && git commit -qm 'Add Netflix ELT Airflow pipeline' && git push -q || true
echo 'Done. Next: docker compose up -d'
