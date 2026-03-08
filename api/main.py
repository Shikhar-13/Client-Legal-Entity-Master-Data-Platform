"""
FastAPI application — async throughout for 100+ concurrent requests under 200ms.
"""

import asyncio
import logging
import time
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
import shutil
import tempfile
import os

from models.entity import (
    ClientEntity, ValidationResult, DataQualityReport, VerificationStatus
)
from services.gleif_api import get_legal_entities_async, get_single_entity
from services.ingestion import ingest_client_upload
from services.validation import validate_entities_batch
from services.quality import compute_quality_score
from services.resolution import resolve_entities, build_index
from storage.curated_store import (
    upsert_results, compute_quality_report, get_verified_entities, export_to_parquet
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="LEI Cross-Verification Platform",
    description="Automated LEI verification and entity data governance using GLEIF",
    version="1.0.0",
)

@app.get("/")
async def root():
    return {"status": "ok", "service": "lei-verification-platform"}
@app.get("/health")
async def health():
    return {"status": "ok", "service": "lei-verification-platform"}


@app.get("/lei/{lei}", response_model=dict)
async def get_lei_record(lei: str):
    """
    Real-time single LEI lookup from GLEIF.
    Async — non-blocking, returns in <200ms for single records.
    """
    entity = await get_single_entity(lei)
    if not entity:
        raise HTTPException(status_code=404, detail=f"LEI {lei} not found in GLEIF")
    return entity.model_dump()


@app.post("/verify/batch", response_model=List[dict])
async def verify_batch(leis: List[str]):
    """
    Verify a batch of LEIs directly — no file upload needed.
    Async concurrent fetching supports 100+ LEIs efficiently.
    """
    start = time.perf_counter()
    gleif_entities = await get_legal_entities_async(leis)

    # Build dummy client entities for verification
    clients = [
        ClientEntity(client_id=lei, entity_name="", entity_lei=lei)
        for lei in leis
    ]
    results = validate_entities_batch(clients, gleif_entities)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(f"Batch verify {len(leis)} LEIs completed in {elapsed_ms:.1f}ms")

    return [r.model_dump() for r in results]


@app.post("/upload/verify", response_model=dict)
async def upload_and_verify(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """
    Upload CSV/Excel of client entities → trigger full verification pipeline.
    Heavy processing runs in background; returns job acknowledgement immediately.
    """
    if not file.filename.endswith((".csv", ".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only CSV and Excel files supported")

    # Save to temp file
    suffix = os.path.splitext(file.filename)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    background_tasks.add_task(_run_full_pipeline, tmp_path)

    return {
        "status": "accepted",
        "message": f"Processing {file.filename} in background",
        "file": file.filename,
    }


async def _run_full_pipeline(file_path: str):
    """Background task — full pipeline: ingest → GLEIF → validate → store."""
    try:
        clients = ingest_client_upload(file_path)
        leis = [c.entity_lei for c in clients if c.entity_lei]
        gleif_entities = await get_legal_entities_async(leis)
        results = validate_entities_batch(clients, gleif_entities)
        upsert_results(results)
        report = compute_quality_score(results)
        logger.info(
            f"Pipeline complete | records={report.total_records} | "
            f"quality={report.overall_quality_score:.1%}"
        )
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
    finally:
        os.unlink(file_path)


@app.get("/quality/report", response_model=dict)
async def quality_report():
    """Return the latest data quality report from the curated store."""
    report = compute_quality_report()
    return report.model_dump()


@app.get("/entities", response_model=List[dict])
async def list_entities(
    status: Optional[str] = Query(None, description="Filter by verification status"),
    limit: int = Query(100, le=1000),
):
    """Query verified entities from the curated DuckDB store."""
    entities = get_verified_entities(limit=limit, status=status)
    return entities


@app.post("/export/parquet")
async def export_parquet():
    """Export curated store to Parquet for downstream dbt/Spark pipelines."""
    path = export_to_parquet()
    return {"status": "exported", "path": path}
