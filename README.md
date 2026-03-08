# LEI Cross-Verification & Entity Data Governance Platform

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow)](https://duckdb.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

> Automated LEI verification and entity data quality pipeline — cross-checks client entity records against the **GLEIF** authoritative registry, resolves mismatches using RAG-based LLM entity resolution, and stores verified records in a curated DuckDB data store.

---

## The Problem

Financial institutions and enterprises managing thousands of counterparty entities face a recurring challenge — client entity data collected from internal systems is frequently **inconsistent, incomplete, or outdated**:

- Entity names abbreviated differently across systems ("Goldman Sachs Group" vs "The Goldman Sachs Group, Inc.")
- Wrong country codes entered ("UK" instead of GLEIF-standard "GB")
- Inactive or lapsed LEIs remaining undetected in active counterparty records
- No single trusted source of truth for legal entity data

Manual verification is slow, error-prone, and doesn't scale beyond a few hundred records.

---

## Solution

This platform provides a fully automated LEI verification pipeline that:

1. **Ingests** client entity uploads (CSV / Excel) into a raw staging layer
2. **Fetches** authoritative records from GLEIF via async batch API calls
3. **Validates** each entity field-by-field against the official registry
4. **Resolves** unmatched entities using RAG (embeddings + LLM re-ranking)
5. **Scores** data quality across 4 weighted dimensions
6. **Stores** verified records in a curated DuckDB store with Parquet export

---

## Architecture

```
CSV / Excel Upload
       │
       ▼
┌─────────────────┐
│  Staging Layer  │  ← DuckDB (raw, immutable)
│  ingestion.py   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GLEIF API     │  ← async aiohttp, semaphore-controlled
│  gleif_api.py   │    batch fetching (200 LEIs/request)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Validation     │  ← name / country / legal form / status
│  validation.py  │    cross-check, per-field quality score
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
VERIFIED   MISMATCH / MISSING
    │         │
    │         ▼
    │  ┌─────────────────┐
    │  │ RAG Resolution  │  ← sentence-transformers + FAISS
    │  │ resolution.py   │    + optional LLM re-ranking
    │  └────────┬────────┘
    │           │
    └─────┬─────┘
          │
          ▼
┌─────────────────┐
│ Curated Store   │  ← DuckDB (verified, queryable)
│ curated_store.py│    + Parquet export for dbt / Spark
└─────────────────┘
          │
          ▼
     FastAPI REST
     Airflow DAG
```

---

## Key Features

| Feature | Detail |
|---|---|
| **Async GLEIF fetching** | `aiohttp` + `asyncio.Semaphore` — 100+ LEIs concurrently, <200ms per batch |
| **Field-level validation** | Name, country, legal form, LEI status — each flagged independently |
| **RAG entity resolution** | `all-MiniLM-L6-v2` embeddings → FAISS ANN search → optional LLM re-ranking |
| **Quality scoring** | Weighted model: name 40%, country 20%, legal form 20%, active status 20% |
| **Idempotent pipeline** | `INSERT OR REPLACE` — safe to re-run without duplicates |
| **Airflow orchestration** | 7-task DAG, daily schedule, 2 retries, XCom-based task handoff |
| **Sub-second DuckDB queries** | Columnar storage — aggregations on 50K+ rows in <100ms |
| **Parquet export** | One-command export for downstream dbt / Spark / Snowflake pipelines |

---

## Project Structure

```
lei-platform/
├── api/
│   └── main.py              # FastAPI app — async REST endpoints
├── dags/
│   └── lei_verification_dag.py   # Airflow DAG — 7-task ETL pipeline
├── models/
│   └── entity.py            # Pydantic models — ClientEntity, LegalEntity, ValidationResult
├── services/
│   ├── gleif_api.py         # Async GLEIF API client
│   ├── ingestion.py         # CSV/Excel ingestion + DuckDB staging
│   ├── validation.py        # Field-by-field cross-verification
│   ├── resolution.py        # RAG entity resolution (embeddings + LLM)
│   ├── quality.py           # Quality score computation
│   └── analytics.py        # LLM-powered entity querying
├── storage/
│   └── curated_store.py     # DuckDB curated store + Parquet export
├── tests/
│   └── test_core.py         # 35 unit tests across 5 classes
├── data/                    # Auto-created — staging.duckdb, curated.duckdb
├── sample_upload.csv        # 25-record test dataset (all validation scenarios)
├── conftest.py
└── requirements.txt
```

---

## Quick Start

### 1. Clone and set up environment

```bash
git clone https://github.com/your-username/lei-platform.git
cd Client-Legal-Entity-Master-Data-Platform

python -m venv .venv

# Mac/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> **Note:** `faiss-cpu` requires NumPy < 2. If you have NumPy 2.x installed:
> ```bash
> pip install "numpy<2"
> pip install faiss-cpu
> ```

### 3. Start the API server

```bash
uvicorn api.main:app --reload --port 8000
```

Open **http://localhost:8000/docs** for the interactive Swagger UI.

### 4. Run the test suite

```bash
pytest tests/ -v
```

Expected: **35 tests passing**

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Server health check |
| `GET` | `/lei/{lei}` | Real-time single LEI lookup from GLEIF |
| `POST` | `/verify/batch` | Batch verify a list of LEI codes |
| `POST` | `/upload/verify` | Upload CSV/Excel → trigger full pipeline |
| `GET` | `/quality/report` | Data quality report from curated store |
| `GET` | `/entities` | Query verified entities (filter by status) |
| `POST` | `/export/parquet` | Export curated store to Parquet |

### Example — Single LEI lookup

```bash
curl http://localhost:8000/lei/5493001KJTIIGC8Y1R12
```

```json
{
  "lei": "5493001KJTIIGC8Y1R12",
  "name": "Apple Inc.",
  "country": "US",
  "legal_form": "Corporation",
  "status": "ACTIVE"
}
```

### Example — Batch verification

```bash
curl -X POST http://localhost:8000/verify/batch \
  -H "Content-Type: application/json" \
  -d '["5493001KJTIIGC8Y1R12", "7LTWFZYICNSX8D621K86", "FAKELEIFAKE123456789"]'
```

### Example — Upload and verify

```bash
curl -X POST http://localhost:8000/upload/verify \
  -F 'file=@sample_upload.csv'
```

---

## Airflow Pipeline

The DAG `lei_verification_pipeline` runs daily at **02:00 UTC** and orchestrates 7 sequential tasks:

```
ingest → fetch_gleif → validate → resolve → quality → store → export
```

### Trigger manually

```bash
# Start Airflow
airflow scheduler &
airflow webserver --port 8080

# Trigger via CLI
airflow dags trigger lei_verification_pipeline \
  --conf '{"file_path": "/path/to/upload.csv"}'
```

Or use the Airflow UI at **http://localhost:8080**.

---

## Validation Logic

Each entity is cross-checked against GLEIF on 4 fields:

```
Quality Score = name_match × 0.4
              + country_match × 0.2
              + legal_form_match × 0.2
              + lei_active × 0.2
```

| Status | Meaning |
|---|---|
| `VERIFIED` | All fields match authoritative GLEIF record |
| `MISMATCH` | One or more fields differ from official record |
| `INACTIVE` | LEI exists but status is INACTIVE / lapsed |
| `MISSING` | No LEI provided, or LEI not found in GLEIF |

---

## RAG Entity Resolution

For unmatched entities, the platform runs a 3-stage resolution pipeline:

```
1. Embed entity name → 384-dim vector (all-MiniLM-L6-v2)
2. FAISS ANN search → top-5 candidates from GLEIF index
3. LLM re-ranking → GPT-4o-mini selects best match + confidence score
```

Fallback chain: `RAG (embeddings + LLM)` → `embedding only` → `fuzzywuzzy` — the platform never crashes due to missing optional dependencies.

---

## Test Dataset

`sample_upload.csv` contains 25 records covering every validation path:

| Records | Scenario |
|---|---|
| C001–C005 | Clean exact matches → `VERIFIED` |
| C006–C009 | Name variations (abbreviated, missing suffix) → `MISMATCH` |
| C010–C011 | Wrong country code → `MISMATCH` |
| C012–C013 | Wrong legal form (LLC vs PLC, GmbH vs AG) → `MISMATCH` |
| C014 | Name + country + legal form all wrong → `MISMATCH` |
| C015–C017 | No LEI provided → `MISSING` |
| C018–C019 | Fake/invalid LEI codes → `MISSING` |
| C020 | Archegos Capital (dissolved entity) → `INACTIVE` |
| C021–C022 | Partial data, country/legal form missing → partial score |
| C023 | Duplicate LEI from different system → tests idempotency |
| C024–C025 | Ambiguous names, no LEI → tests RAG resolution |

---

## Tech Stack

| Component | Technology |
|---|---|
| API framework | FastAPI + Uvicorn (ASGI) |
| Async HTTP | aiohttp |
| Data models | Pydantic v2 |
| Analytics DB | DuckDB |
| Data processing | pandas |
| File format | Parquet (via PyArrow) |
| Orchestration | Apache Airflow 2.9 |
| Embeddings | sentence-transformers (`all-MiniLM-L6-v2`) |
| Vector search | FAISS |
| LLM re-ranking | OpenAI GPT-4o-mini (optional) |
| Fuzzy matching | fuzzywuzzy (fallback) |
| Testing | pytest |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | Optional | Enables LLM re-ranking in entity resolution. Without it, falls back to embedding-only matching. |

---

## Use Cases

- **Investment banks** — verify counterparty LEIs before trade execution
- **Compliance teams** — ensure entity records meet KYC/AML reporting standards
- **FinTech platforms** — validate client company data at onboarding
- **Risk management** — detect inactive or dissolved counterparty entities

---

## Future Improvements

- [ ] Real-time streaming verification via Kafka
- [ ] Dashboard UI for discrepancy monitoring and manual review queue
- [ ] Automated alerts for newly inactive LEIs
- [ ] Integration with Bloomberg / Refinitiv for enrichment
- [ ] Entity relationship graph analysis (subsidiary → parent mapping)
- [ ] dbt models on top of Parquet export layer

---
