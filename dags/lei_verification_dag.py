"""
Airflow DAG: lei_verification_pipeline

Orchestrates the full ETL pipeline:
  Task 1 (ingest)      → read upload file, write to DuckDB staging
  Task 2 (fetch_gleif) → async-batch fetch GLEIF records
  Task 3 (validate)    → cross-verify client vs GLEIF fields
  Task 4 (resolve)     → RAG-based entity resolution for unmatched records
  Task 5 (quality)     → compute quality score + write quality_history
  Task 6 (store)       → upsert verified records into curated DuckDB store
  Task 7 (export)      → write Parquet snapshot for downstream consumers

Schedule: daily at 02:00 UTC
Retry: 2 retries, 5-min delay — handles transient GLEIF API failures
"""

from datetime import datetime, timedelta
import json
import sys
import os

# Make project root importable inside Airflow worker
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

DEFAULT_ARGS = {
    "owner": "shikhar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Task functions ─────────────────────────────────────────────────────────

def task_ingest(**context):
    from services.ingestion import ingest_client_upload
    file_path = context["dag_run"].conf.get("file_path", "data/sample_upload.csv")
    entities = ingest_client_upload(file_path)
    # Push serializable payload to XCom
    context["ti"].xcom_push(
        key="client_entities",
        value=[e.model_dump() for e in entities]
    )
    log.info(f"[ingest] loaded {len(entities)} client entities")
    return len(entities)


def task_fetch_gleif(**context):
    from services.gleif_api import get_legal_entities_by_lei
    from models.entity import ClientEntity

    raw = context["ti"].xcom_pull(key="client_entities", task_ids="ingest")
    clients = [ClientEntity(**r) for r in raw]
    leis = [c.entity_lei for c in clients if c.entity_lei]

    gleif_entities = get_legal_entities_by_lei(leis)
    context["ti"].xcom_push(
        key="gleif_entities",
        value=[e.model_dump() for e in gleif_entities]
    )
    log.info(f"[fetch_gleif] fetched {len(gleif_entities)} GLEIF records for {len(leis)} LEIs")
    return len(gleif_entities)


def task_validate(**context):
    from services.validation import validate_entities_batch
    from models.entity import ClientEntity, LegalEntity

    raw_clients  = context["ti"].xcom_pull(key="client_entities",  task_ids="ingest")
    raw_gleif    = context["ti"].xcom_pull(key="gleif_entities",   task_ids="fetch_gleif")

    clients = [ClientEntity(**r) for r in raw_clients]
    gleif   = [LegalEntity(**r)  for r in raw_gleif]

    results = validate_entities_batch(clients, gleif)
    context["ti"].xcom_push(
        key="validation_results",
        value=[r.model_dump() for r in results]
    )
    verified = sum(1 for r in results if r.verification_status.value == "VERIFIED")
    log.info(f"[validate] {verified}/{len(results)} records verified")
    return len(results)


def task_resolve(**context):
    """
    Run RAG-based resolution only on MISMATCH / MISSING records.
    Avoids re-processing already-verified entities.
    """
    from services.resolution import resolve_entities, build_index
    from models.entity import ClientEntity, LegalEntity, VerificationStatus

    raw_results = context["ti"].xcom_pull(key="validation_results", task_ids="validate")
    raw_clients = context["ti"].xcom_pull(key="client_entities",    task_ids="ingest")
    raw_gleif   = context["ti"].xcom_pull(key="gleif_entities",     task_ids="fetch_gleif")

    gleif = [LegalEntity(**r) for r in raw_gleif]

    # Only resolve unmatched / mismatched clients
    unmatched_ids = {
        r["client_id"]
        for r in raw_results
        if r["verification_status"] in ("MISMATCH", "MISSING", "UNVERIFIED")
    }
    unmatched_clients = [
        ClientEntity(**c) for c in raw_clients
        if c["client_id"] in unmatched_ids
    ]

    if unmatched_clients:
        build_index(gleif)
        resolution_results = resolve_entities(unmatched_clients, gleif)
        resolved = sum(1 for r in resolution_results if r.match_score >= 0.80)
        log.info(f"[resolve] resolved {resolved}/{len(unmatched_clients)} previously unmatched entities")
    else:
        log.info("[resolve] all entities already verified — skipping resolution")


def task_quality(**context):
    from services.quality import compute_quality_score
    from models.entity import ValidationResult

    raw = context["ti"].xcom_pull(key="validation_results", task_ids="validate")
    results = [ValidationResult(**r) for r in raw]

    run_id = context["run_id"]
    report = compute_quality_score(results)

    context["ti"].xcom_push(key="quality_report", value=report.model_dump())
    log.info(
        f"[quality] score={report.overall_quality_score:.1%} | "
        f"verified={report.verified}/{report.total_records}"
    )
    return report.overall_quality_score


def task_store(**context):
    from storage.curated_store import upsert_results
    from models.entity import ValidationResult

    raw = context["ti"].xcom_pull(key="validation_results", task_ids="validate")
    results = [ValidationResult(**r) for r in raw]
    upsert_results(results)
    log.info(f"[store] upserted {len(results)} records to curated store")
    return len(results)


def task_export(**context):
    from storage.curated_store import export_to_parquet
    path = export_to_parquet()
    log.info(f"[export] Parquet snapshot written to {path}")
    return path


# ── DAG definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="lei_verification_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily LEI cross-verification and entity data quality pipeline",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lei", "gleif", "data-quality", "etl"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
    )

    fetch_gleif = PythonOperator(
        task_id="fetch_gleif",
        python_callable=task_fetch_gleif,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
    )

    resolve = PythonOperator(
        task_id="resolve",
        python_callable=task_resolve,
    )

    quality = PythonOperator(
        task_id="quality",
        python_callable=task_quality,
    )

    store = PythonOperator(
        task_id="store",
        python_callable=task_store,
    )

    export = PythonOperator(
        task_id="export",
        python_callable=task_export,
    )

    # Pipeline dependency chain
    ingest >> fetch_gleif >> validate >> resolve >> quality >> store >> export
