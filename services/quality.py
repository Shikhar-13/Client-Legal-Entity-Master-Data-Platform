import logging
from typing import List
from models.entity import ValidationResult, VerificationStatus, DataQualityReport

logger = logging.getLogger(__name__)


def compute_quality_score(results: List[ValidationResult]) -> DataQualityReport:
    """
    Compute the overall data quality score from a batch of validation results.
    Scoring model:
      - Each record contributes up to 1.0 points
      - Weights: name_match=0.4, country_match=0.2, legal_form=0.2, active_lei=0.2
      - Overall score = mean(individual quality_score) across all records
    """
    if not results:
        return DataQualityReport(
            total_records=0, verified=0, mismatched=0,
            inactive=0, missing=0, overall_quality_score=0.0,
            field_completeness={}
        )

    total = len(results)
    verified = sum(1 for r in results if r.verification_status == VerificationStatus.VERIFIED)
    mismatched = sum(1 for r in results if r.verification_status == VerificationStatus.MISMATCH)
    inactive = sum(1 for r in results if r.verification_status == VerificationStatus.INACTIVE)
    missing = sum(1 for r in results if r.verification_status in (
        VerificationStatus.MISSING, VerificationStatus.UNVERIFIED
    ))

    overall = round(sum(r.quality_score for r in results) / total, 4)

    # Field-level completeness
    has_lei = sum(1 for r in results if r.entity_lei) / total
    has_gleif_name = sum(1 for r in results if r.gleif_name) / total
    has_gleif_country = sum(1 for r in results if r.gleif_country) / total

    issues = []
    if overall < 0.7:
        issues.append(f"Overall quality below threshold: {overall:.1%}")
    if missing / total > 0.1:
        issues.append(f"High missing rate: {missing}/{total} records have no LEI")
    if inactive / total > 0.05:
        issues.append(f"Inactive LEIs detected: {inactive} records")

    report = DataQualityReport(
        total_records=total,
        verified=verified,
        mismatched=mismatched,
        inactive=inactive,
        missing=missing,
        overall_quality_score=overall,
        field_completeness={
            "lei": round(has_lei, 3),
            "gleif_name": round(has_gleif_name, 3),
            "gleif_country": round(has_gleif_country, 3),
        },
        issues_summary=issues,
    )

    logger.info(
        f"Quality report | total={total} | verified={verified} | "
        f"score={overall:.1%} | mismatched={mismatched} | inactive={inactive} | missing={missing}"
    )
    return report
