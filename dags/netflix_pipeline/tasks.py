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
