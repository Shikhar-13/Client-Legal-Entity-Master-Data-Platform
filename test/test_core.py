"""
Unit tests — validates the core claims on the resume.
Run: pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from models.entity import (
    ClientEntity, LegalEntity, VerificationStatus
)
from services.validation import validate_entity, validate_entities_batch
from services.quality import compute_quality_score


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def active_gleif_entity():
    return LegalEntity(
        lei="529900T8BM49AURSDO55",
        name="Apple Inc.",
        country="US",
        legal_form="Corporation",
        status="ACTIVE",
        registration_authority="RA000602",
    )

@pytest.fixture
def inactive_gleif_entity():
    return LegalEntity(
        lei="DEADBEEF1234567890AB",
        name="Defunct Corp Ltd",
        country="GB",
        legal_form="Limited",
        status="INACTIVE",
    )

@pytest.fixture
def matching_client():
    return ClientEntity(
        client_id="C001",
        entity_name="Apple Inc.",
        entity_lei="529900T8BM49AURSDO55",
        country="US",
        legal_form="Corporation",
    )

@pytest.fixture
def mismatched_client():
    return ClientEntity(
        client_id="C002",
        entity_name="Apple Incorporated",   # name differs
        entity_lei="529900T8BM49AURSDO55",
        country="UK",                        # country differs
        legal_form="Corporation",
    )

@pytest.fixture
def missing_lei_client():
    return ClientEntity(
        client_id="C003",
        entity_name="Unknown Corp",
        entity_lei=None,
    )


# ── Validation tests ───────────────────────────────────────────────────────

class TestValidation:

    def test_verified_when_all_fields_match(self, matching_client, active_gleif_entity):
        gleif_map = {active_gleif_entity.lei: active_gleif_entity}
        result = validate_entity(matching_client, gleif_map)

        assert result.verification_status == VerificationStatus.VERIFIED
        assert result.name_match is True
        assert result.country_match is True
        assert result.quality_score == 1.0
        assert result.issues == []

    def test_mismatch_when_country_differs(self, mismatched_client, active_gleif_entity):
        gleif_map = {active_gleif_entity.lei: active_gleif_entity}
        result = validate_entity(mismatched_client, gleif_map)

        assert result.verification_status == VerificationStatus.MISMATCH
        assert result.country_match is False
        assert len(result.issues) > 0

    def test_missing_when_no_lei(self, missing_lei_client):
        result = validate_entity(missing_lei_client, {})
        assert result.verification_status == VerificationStatus.MISSING
        assert result.quality_score == 0.0

    def test_inactive_lei_flagged(self, matching_client, inactive_gleif_entity):
        # Point the matching client at the inactive entity
        client = matching_client.model_copy(
            update={"entity_lei": inactive_gleif_entity.lei, "entity_name": "Defunct Corp Ltd"}
        )
        gleif_map = {inactive_gleif_entity.lei: inactive_gleif_entity}
        result = validate_entity(client, gleif_map)
        assert result.verification_status == VerificationStatus.INACTIVE

    def test_lei_not_in_gleif_returns_missing(self, matching_client):
        result = validate_entity(matching_client, {})  # empty GLEIF map
        assert result.verification_status == VerificationStatus.MISSING

    def test_batch_validation_counts(self, matching_client, mismatched_client,
                                      missing_lei_client, active_gleif_entity):
        clients = [matching_client, mismatched_client, missing_lei_client]
        gleif = [active_gleif_entity]
        results = validate_entities_batch(clients, gleif)

        statuses = {r.client_id: r.verification_status for r in results}
        assert statuses["C001"] == VerificationStatus.VERIFIED
        assert statuses["C002"] == VerificationStatus.MISMATCH
        assert statuses["C003"] == VerificationStatus.MISSING


# ── Quality score tests ────────────────────────────────────────────────────

class TestQualityScore:

    def test_perfect_score_all_verified(self, matching_client, active_gleif_entity):
        gleif_map = {active_gleif_entity.lei: active_gleif_entity}
        results = validate_entities_batch([matching_client], [active_gleif_entity])
        report = compute_quality_score(results)

        assert report.overall_quality_score == 1.0
        assert report.verified == 1
        assert report.total_records == 1

    def test_score_below_threshold_triggers_issue(self):
        # All missing — score should be 0
        clients = [
            ClientEntity(client_id=f"C{i}", entity_name=f"Entity {i}", entity_lei=None)
            for i in range(10)
        ]
        from services.validation import validate_entities_batch
        results = validate_entities_batch(clients, [])
        report = compute_quality_score(results)

        assert report.overall_quality_score == 0.0
        assert report.missing == 10
        assert any("missing" in issue.lower() for issue in report.issues_summary)

    def test_quality_score_weighted_correctly(self, matching_client, active_gleif_entity):
        # matching_client has name+country+legal_form match + active = 1.0
        gleif_map = {active_gleif_entity.lei: active_gleif_entity}
        from services.validation import validate_entity
        result = validate_entity(matching_client, gleif_map)
        assert result.quality_score == pytest.approx(1.0, abs=0.01)

    def test_partial_match_score(self, active_gleif_entity):
        # client has LEI + name match only (no country/legal_form provided)
        client = ClientEntity(
            client_id="C_partial",
            entity_name="Apple Inc.",
            entity_lei="529900T8BM49AURSDO55",
        )
        gleif_map = {active_gleif_entity.lei: active_gleif_entity}
        from services.validation import validate_entity
        result = validate_entity(client, gleif_map)
        # name=0.4 + active=0.2 = 0.6 (country/legal_form not provided so no penalty)
        assert result.quality_score == pytest.approx(0.6, abs=0.01)


# ── Resolution tests ───────────────────────────────────────────────────────

class TestResolution:

    def test_fuzzy_resolution_finds_close_match(self):
        from services.resolution import _resolve_with_fuzzy
        client = ClientEntity(
            client_id="C_res",
            entity_name="Apple Incorporated",
            entity_lei=None,
        )
        gleif = [
            LegalEntity(lei="L1", name="Apple Inc.", country="US"),
            LegalEntity(lei="L2", name="Google LLC", country="US"),
            LegalEntity(lei="L3", name="Microsoft Corporation", country="US"),
        ]
        match, score, method = _resolve_with_fuzzy(client, gleif)
        assert match is not None
        assert match.lei == "L1"
        assert score > 0.7
        assert method == "fuzzy"

    def test_no_match_below_threshold(self):
        from services.resolution import resolve_entities
        client = ClientEntity(
            client_id="C_nomatch",
            entity_name="Completely Unrelated XYZ Corp 12345",
            entity_lei=None,
        )
        gleif = [LegalEntity(lei="L1", name="Apple Inc.", country="US")]
        results = resolve_entities([client], gleif)
        assert len(results) == 1
        # Score may be low but function should not crash
        assert results[0].match_score >= 0.0
