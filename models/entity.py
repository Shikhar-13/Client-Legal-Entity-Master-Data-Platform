from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum


class LEIStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    PENDING = "PENDING"
    UNKNOWN = "UNKNOWN"


class VerificationStatus(str, Enum):
    VERIFIED = "VERIFIED"
    MISMATCH = "MISMATCH"
    INACTIVE = "INACTIVE"
    MISSING = "MISSING"
    UNVERIFIED = "UNVERIFIED"


class ClientEntity(BaseModel):
    client_id: str
    entity_name: str
    entity_lei: Optional[str] = None
    country: Optional[str] = None
    legal_form: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None


class LegalEntity(BaseModel):
    lei: str
    name: Optional[str] = None
    country: Optional[str] = None
    legal_form: Optional[str] = None
    status: Optional[str] = None
    registration_authority: Optional[str] = None


class ValidationResult(BaseModel):
    client_id: str
    entity_lei: Optional[str]
    entity_name: str
    verification_status: VerificationStatus
    name_match: bool = False
    country_match: bool = False
    legal_form_match: bool = False
    lei_status: Optional[str] = None
    issues: List[str] = Field(default_factory=list)
    gleif_name: Optional[str] = None
    gleif_country: Optional[str] = None
    quality_score: float = 0.0


class EntityResolutionResult(BaseModel):
    client_entity: ClientEntity
    matched_legal_entity: Optional[LegalEntity]
    match_score: float
    match_method: str = "fuzzy"
    issues: List[str] = Field(default_factory=list)


class DataQualityReport(BaseModel):
    total_records: int
    verified: int
    mismatched: int
    inactive: int
    missing: int
    overall_quality_score: float
    field_completeness: dict
    issues_summary: List[str] = Field(default_factory=list)
