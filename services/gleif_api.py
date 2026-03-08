import asyncio
import aiohttp
import logging
from typing import List, Optional
from models.entity import LegalEntity
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

GLEIF_BASE = os.getenv("GLEIF_BASE")
BATCH_SIZE = 200          # GLEIF API max per request
MAX_CONCURRENT = 10       # concurrent requests
TIMEOUT = aiohttp.ClientTimeout(total=30)


async def _fetch_batch(
    session: aiohttp.ClientSession,
    leis: List[str],
    semaphore: asyncio.Semaphore
) -> List[LegalEntity]:
    """Fetch one batch of LEIs with semaphore-controlled concurrency."""
    async with semaphore:
        lei_param = ",".join(leis)
        url = f"{GLEIF_BASE}?filter[lei]={lei_param}&page[size]={BATCH_SIZE}"
        try:
            async with session.get(url, timeout=TIMEOUT) as resp:
                resp.raise_for_status()
                data = (await resp.json()).get("data", [])
        except aiohttp.ClientError as e:
            logger.error(f"GLEIF API error for batch {leis[:3]}...: {e}")
            return []

    entities = []
    for item in data:
        attr = item.get("attributes", {})
        entity = attr.get("entity", {})
        legal_address = entity.get("legalAddress", {})
        reg_auth = attr.get("registrationAuthority", {}).get("id")
        entities.append(LegalEntity(
            lei=item.get("id"),
            name=entity.get("legalName", {}).get("name"),
            country=legal_address.get("country"),
            legal_form=entity.get("legalForm", {}).get("name"),
            status=attr.get("registration", {}).get("status"),
            registration_authority=reg_auth,
        ))
    return entities


async def get_legal_entities_async(leis: List[str]) -> List[LegalEntity]:
    """
    Fetch LEI records from GLEIF in parallel batches.
    Supports 100+ concurrent LEIs with controlled semaphore concurrency.
    """
    if not leis:
        return []

    # Deduplicate
    unique_leis = list(dict.fromkeys(leis))

    # Split into batches of BATCH_SIZE
    batches = [
        unique_leis[i:i + BATCH_SIZE]
        for i in range(0, len(unique_leis), BATCH_SIZE)
    ]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results: List[LegalEntity] = []

    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_batch(session, batch, semaphore) for batch in batches]
        batch_results = await asyncio.gather(*tasks)
        for br in batch_results:
            results.extend(br)

    logger.info(f"Fetched {len(results)} entities from GLEIF for {len(unique_leis)} LEIs")
    return results


def get_legal_entities_by_lei(leis: List[str]) -> List[LegalEntity]:
    """Sync wrapper — used by Airflow tasks and non-async callers."""
    return asyncio.run(get_legal_entities_async(leis))


async def get_single_entity(lei: str) -> Optional[LegalEntity]:
    """Fetch a single LEI record — used by real-time API endpoint."""
    results = await get_legal_entities_async([lei])
    return results[0] if results else None
