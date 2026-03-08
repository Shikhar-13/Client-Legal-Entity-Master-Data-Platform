import logging
from typing import List, Dict
from models.entity import (
    ClientEntity, LegalEntity, ValidationResult, VerificationStatus
)

logger = logging.getLogger(__name__)

NAME_SIMILARITY_THRESHOLD = 85  # fuzz score for name match fallback


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _name_matches(client_name: str, gleif_name: str | None) -> bool:
    if not gleif_name:
        return False
    cn = _normalize(client_name)
    gn = _normalize(gleif_name)
    # Exact match
    if cn == gn:
        return True
    # Substring match (handles "Ltd" vs "Limited" etc.)
    if cn in gn or gn in cn:
        return True
    return False


def validate_entity(
    client: ClientEntity,
    gleif_map: Dict[str, LegalEntity]
) -> ValidationResult:
    """
    Cross-verify a single ClientEntity against its GLEIF record.
    Returns a ValidationResult with per-field match flags and a quality score.
    """
    issues = []
    result = ValidationResult(
        client_id=client.client_id,
        entity_lei=client.entity_lei,
        entity_name=client.entity_name,
        verification_status=VerificationStatus.UNVERIFIED,
    )

    # --- Missing LEI ---
    if not client.entity_lei:
        result.verification_status = VerificationStatus.MISSING
        result.issues = ["entity_lei is missing"]
        result.quality_score = 0.0
        return result

    gleif = gleif_map.get(client.entity_lei)

    # --- LEI not found in GLEIF ---
    if not gleif:
        result.verification_status = VerificationStatus.MISSING
        result.issues = [f"LEI {client.entity_lei} not found in GLEIF"]
        result.quality_score = 0.1
        return result

    result.gleif_name = gleif.name
    result.gleif_country = gleif.country
    result.lei_status = gleif.status

    # --- Inactive LEI ---
    if gleif.status and gleif.status.upper() != "ACTIVE":
        result.verification_status = VerificationStatus.INACTIVE
        result.issues.append(f"LEI status is {gleif.status}")

    # --- Field matching ---
    result.name_match = _name_matches(client.entity_name, gleif.name)
    result.country_match = (
        _normalize(client.country) == _normalize(gleif.country)
        if client.country and gleif.country else False
    )
    result.legal_form_match = (
        _normalize(client.legal_form) == _normalize(gleif.legal_form)
        if client.legal_form and gleif.legal_form else False
    )

    if not result.name_match:
        issues.append(f"Name mismatch: '{client.entity_name}' vs GLEIF '{gleif.name}'")
    if client.country and not result.country_match:
        issues.append(f"Country mismatch: '{client.country}' vs GLEIF '{gleif.country}'")
    if client.legal_form and not result.legal_form_match:
        issues.append(f"Legal form mismatch: '{client.legal_form}' vs GLEIF '{gleif.legal_form}'")

    result.issues = issues

    # --- Verification status ---
    if result.verification_status != VerificationStatus.INACTIVE:
        if issues:
            result.verification_status = VerificationStatus.MISMATCH
        else:
            result.verification_status = VerificationStatus.VERIFIED

    # --- Quality score (0.0 – 1.0) ---
    score = 0.0
    score += 0.4 if result.name_match else 0.0
    score += 0.2 if result.country_match else 0.0
    score += 0.2 if result.legal_form_match else 0.0
    score += 0.2 if gleif.status and gleif.status.upper() == "ACTIVE" else 0.0
    result.quality_score = round(score, 2)

    return result


def validate_entities_batch(
    clients: List[ClientEntity],
    gleif_entities: List[LegalEntity]
) -> List[ValidationResult]:
    """
    Validate a full batch.
    Builds a LEI → LegalEntity lookup map for O(1) access.
    """
    gleif_map: Dict[str, LegalEntity] = {e.lei: e for e in gleif_entities}
    results = [validate_entity(c, gleif_map) for c in clients]
    logger.info(
        f"Validated {len(results)} entities | "
        f"verified={sum(1 for r in results if r.verification_status == VerificationStatus.VERIFIED)} | "
        f"mismatched={sum(1 for r in results if r.verification_status == VerificationStatus.MISMATCH)} | "
        f"inactive={sum(1 for r in results if r.verification_status == VerificationStatus.INACTIVE)}"
    )
    return results
