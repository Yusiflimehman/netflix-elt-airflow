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
