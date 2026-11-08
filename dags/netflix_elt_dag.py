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
