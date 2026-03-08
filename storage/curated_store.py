"""
Curated entity storage layer using DuckDB.

Design decisions:
- DuckDB for analytics (sub-second aggregations on 50K+ rows via columnar storage)
- Parquet export for archival and interop with Spark/dbt
- All writes go through upsert logic — re-running pipeline is idempotent
"""

import duckdb
import logging
from pathlib import Path
from typing import List, Optional
from models.entity import ValidationResult, VerificationStatus, DataQualityReport

logger = logging.getLogger(__name__)

CURATED_DB = "data/curated.duckdb"
PARQUET_PATH = "data/verified_entities.parquet"

DDL = """
CREATE TABLE IF NOT EXISTS verified_entities (
    client_id          VARCHAR PRIMARY KEY,
    entity_lei         VARCHAR,
    entity_name        VARCHAR,
    gleif_name         VARCHAR,
    gleif_country      VARCHAR,
    lei_status         VARCHAR,
    verification_status VARCHAR,
    name_match         BOOLEAN,
    country_match      BOOLEAN,
    legal_form_match   BOOLEAN,
    quality_score      DOUBLE,
    issues             VARCHAR,        -- JSON array stored as string
    processed_at       TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS quality_history (
    run_id             VARCHAR,
    run_at             TIMESTAMP DEFAULT now(),
    total_records      INTEGER,
    verified           INTEGER,
    mismatched         INTEGER,
    inactive           INTEGER,
    missing            INTEGER,
    overall_score      DOUBLE
);
"""


def _get_connection() -> duckdb.DuckDBPyConnection:
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect(CURATED_DB)
    con.execute(DDL)
    return con


def upsert_results(results: List[ValidationResult]) -> None:
    """
    Write validation results to the curated store.
    Uses INSERT OR REPLACE for idempotency — safe to re-run.
    """
    import json
    con = _get_connection()

    rows = [
        (
            r.client_id,
            r.entity_lei,
            r.entity_name,
            r.gleif_name,
            r.gleif_country,
            r.lei_status,
            r.verification_status.value,
            r.name_match,
            r.country_match,
            r.legal_form_match,
            r.quality_score,
            json.dumps(r.issues),
        )
        for r in results
    ]

    con.executemany("""
        INSERT OR REPLACE INTO verified_entities (
            client_id, entity_lei, entity_name, gleif_name, gleif_country,
            lei_status, verification_status, name_match, country_match,
            legal_form_match, quality_score, issues
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    con.close()
    logger.info(f"Upserted {len(results)} records into curated store")


def compute_quality_report(run_id: str = "latest") -> DataQualityReport:
    """
    Compute the data quality report from the curated store.
    DuckDB columnar scan on 50K rows completes in <100ms.
    """
    con = _get_connection()

    summary = con.execute("""
    SELECT
        COUNT(*) AS total,

        SUM(CASE WHEN verification_status = 'VERIFIED' THEN 1 ELSE 0 END) AS verified,

        SUM(CASE WHEN verification_status = 'MISMATCH' THEN 1 ELSE 0 END) AS mismatched,

        SUM(CASE WHEN verification_status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive,

        SUM(CASE WHEN verification_status IN ('MISSING','UNVERIFIED') THEN 1 ELSE 0 END) AS missing,

        ROUND(AVG(quality_score), 4) AS avg_score

    FROM verified_entities
""").fetchone()

    total, verified, mismatched, inactive, missing, avg_score = summary

    completeness = con.execute("""
    SELECT
        ROUND(1.0 - SUM(CASE WHEN entity_lei IS NULL THEN 1 ELSE 0 END) / COUNT(*), 3) AS lei_completeness,

        ROUND(1.0 - SUM(CASE WHEN gleif_name IS NULL THEN 1 ELSE 0 END) / COUNT(*), 3) AS name_completeness,

        ROUND(1.0 - SUM(CASE WHEN gleif_country IS NULL THEN 1 ELSE 0 END) / COUNT(*), 3) AS country_completeness

    FROM verified_entities
""").fetchone()
    # Persist to quality history
    con.execute("""
        INSERT INTO quality_history (run_id, total_records, verified, mismatched, inactive, missing, overall_score)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (run_id, total, verified, mismatched, inactive, missing, avg_score or 0.0))

    con.close()

    return DataQualityReport(
        total_records=total or 0,
        verified=verified or 0,
        mismatched=mismatched or 0,
        inactive=inactive or 0,
        missing=missing or 0,
        overall_quality_score=float(avg_score or 0.0),
        field_completeness={
            "lei": float(completeness[0] or 0),
            "name": float(completeness[1] or 0),
            "country": float(completeness[2] or 0),
        },
    )


def export_to_parquet() -> str:
    """Export curated store to Parquet for downstream dbt / Spark pipelines."""
    con = _get_connection()
    con.execute(f"COPY verified_entities TO '{PARQUET_PATH}' (FORMAT PARQUET)")
    con.close()
    logger.info(f"Exported curated entities to {PARQUET_PATH}")
    return PARQUET_PATH


def get_verified_entities(limit: int = 1000, status: Optional[str] = None) -> list:
    """Query curated store — sub-second on 50K rows due to DuckDB columnar storage."""
    con = _get_connection()
    where = f"WHERE verification_status = '{status}'" if status else ""
    df = con.execute(
        f"SELECT * FROM verified_entities {where} ORDER BY quality_score DESC LIMIT {limit}"
    ).df()
    con.close()
    return df.to_dict("records")
