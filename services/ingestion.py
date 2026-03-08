import pandas as pd
import duckdb
import logging
from pathlib import Path
from typing import List
from models.entity import ClientEntity

logger = logging.getLogger(__name__)

STAGING_DB = "data/staging.duckdb"
BATCH_SIZE = 1000  # process in chunks — handles 50K+ records efficiently


def _get_connection() -> duckdb.DuckDBPyConnection:
    Path("data").mkdir(exist_ok=True)
    con = duckdb.connect(STAGING_DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS client_upload (
            client_id     VARCHAR,
            entity_name   VARCHAR,
            entity_lei    VARCHAR,
            country       VARCHAR,
            legal_form    VARCHAR,
            contact_email VARCHAR,
            contact_phone VARCHAR,
            ingested_at   TIMESTAMP DEFAULT now()
        )
    """)
    return con


def ingest_client_upload(file_path: str) -> List[ClientEntity]:
    """
    Read CSV or Excel, persist raw records to DuckDB staging,
    return list of ClientEntity for downstream processing.
    Handles 50K+ records via chunked reads.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Upload file not found: {file_path}")

    # Chunked read — avoids loading 50K rows into memory at once
    reader = (
        pd.read_csv(path, chunksize=BATCH_SIZE)
        if path.suffix.lower() == ".csv"
        else [pd.read_excel(path)]  # Excel loaded once; typically smaller
    )

    con = _get_connection()
    entities: List[ClientEntity] = []
    total_rows = 0

    for chunk in reader:
    # Normalise column names
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]

    # FIX: convert NaN → None
        chunk = chunk.where(pd.notnull(chunk), None)

        # Persist raw chunk to staging
        con.execute("""
INSERT INTO client_upload (
    client_id,
    entity_name,
    entity_lei,
    country,
    legal_form,
    contact_email
)
SELECT
    client_id,
    entity_name,
    entity_lei,
    country,
    legal_form,
    contact_email
FROM chunk
""")
        total_rows += len(chunk)

        for _, row in chunk.iterrows():
            entities.append(ClientEntity(
                client_id=str(row.get("client_id", "")),
                entity_name=str(row.get("entity_name", "")),
                entity_lei=(str(row["entity_lei"]) if row.get("entity_lei") else None),
                country=row.get("country") or None,
                legal_form=row.get("legal_form") or None,
                contact_email=row.get("contact_email") or None,
                contact_phone=row.get("contact_phone") or None,
            ))

    con.close()
    logger.info(f"Ingested {total_rows} records from {file_path} into staging")
    return entities


def get_staged_entities(limit: int = 50000) -> List[ClientEntity]:
    """Read back from DuckDB staging — used by downstream pipeline tasks."""
    con = _get_connection()
    df = con.execute(
        f"SELECT * FROM client_upload ORDER BY ingested_at DESC LIMIT {limit}"
    ).df()
    con.close()
    return [
        ClientEntity(**row)
        for row in df.drop(columns=["ingested_at"], errors="ignore").to_dict("records")
    ]


def clear_staging() -> None:
    con = _get_connection()
    con.execute("DELETE FROM client_upload")
    con.close()
    logger.info("Staging table cleared")
