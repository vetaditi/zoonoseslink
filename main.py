"""
ZoonosesLink API v3.1 — India Zoonoses Cross-Reporting System
================================================================
Built on v3.0. Additions in this release:

  • woah_code in INDIA_PRIORITY_ZOONOSES (all 21 diseases)
    WOAH Terrestrial Animal Health Code, Chapter 3 (32nd ed., 2023)
  • woah_code auto-populated on ingest; stored in DBReport + Annexure-XI
  • wahisTrigger flag computed and stored per report
  • WAHIS notification in dispatch_notifications (DAHD channel, logs intent)
  • GET /api/v3/zoonoses/export/wahis/csv — proper StreamingResponse (text/csv)
    not a JSON wrapper — designed for manual WAHIS portal upload by DAHD
  • POST /api/v3/zoonoses/nadrs/ingest — dedicated NADRS endpoint
    locks source="NADRS"; accepts nadrsReportNo; reuses _process_report()
  • nadrsReportNo field on CrossReportRequest and DBReport
  • wahisOnly filter on dashboard endpoint
  • woah_code visible in dashboard, nearby, and cluster responses

WOAH code verification:
  Source: WOAH Terrestrial Animal Health Code, 32nd edition (2023)
  URL: https://www.woah.org/en/what-we-do/standards/codes-and-manuals/
  Re-verify before each production deployment (list updated annually).

Author: Dr. Aditi Sharma
        Council for Environment & Sustainable Development, Dehradun
        MSc One Health, University of Edinburgh (s2260265)
Version: 3.1.0 | May 2026
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Query
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator
from typing import List, Literal, Optional, Dict
from datetime import datetime, date
from enum import Enum
import uuid, os, hashlib, csv
from io import StringIO
from sqlalchemy import (
    create_engine, Column, Integer, String, Float,
    DateTime, JSON, Text, Boolean, text, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from dotenv import load_dotenv

load_dotenv()

# Wildlife enrichment module (GBIF / iNaturalist / IUCN / Movebank / NASA)
from wildlife_enrichment import enrich_report

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:password@localhost:5432/zoonoses_db"
)
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─────────────────────────────────────────────
# INDIA PRIORITY ZOONOSES MASTER LIST
# woah_code: official name per WOAH Terrestrial Code Chapter 3, 32nd ed.
# None = not on WOAH notifiable list.
# ─────────────────────────────────────────────
INDIA_PRIORITY_ZOONOSES: Dict[str, dict] = {
    # TIER 1 — Immediate, IHR notifiable
    "RABIES":           {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Wildlife/domestic",      "vectors":["Dog","Jackal","Fox","Bat"],               "woah_code":"Infection with rabies virus"},
    "ANTHRAX":          {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Environment/livestock",  "vectors":["Cattle","Buffalo","Soil"],                 "woah_code":"Anthrax"},
    "PLAGUE":           {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Wildlife/peri-domestic", "vectors":["Rat flea","Rodent"],                       "woah_code":"Plague"},
    "AVIAN_INFLUENZA":  {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Wildlife/poultry",       "vectors":["Poultry","Migratory bird"],                "woah_code":"High pathogenicity avian influenza"},
    "NIPAH":            {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Wildlife",               "vectors":["Fruit bat","Pig"],                         "woah_code":"Nipah virus encephalitis"},
    "CRIMEAN_CONGO_HF": {"tier":1,"ihr":True, "schedule":"SCHEDULE_I",  "reservoir":"Livestock/wildlife",     "vectors":["Hyalomma tick"],                           "woah_code":"Crimean-Congo haemorrhagic fever"},
    # TIER 2 — Urgent, IDSP Integrated Alert (48h)
    "BRUCELLOSIS":      {"tier":2,"ihr":False,"schedule":"SCHEDULE_II", "reservoir":"Livestock",              "vectors":["Cattle","Buffalo","Goat","Dog"],            "woah_code":"Brucellosis (Brucella abortus/melitensis/suis)"},
    "LEPTOSPIROSIS":    {"tier":2,"ihr":False,"schedule":"SCHEDULE_II", "reservoir":"Wildlife/peri-domestic", "vectors":["Rat","Cattle","Dog"],                       "woah_code":None},
    "JEV":              {"tier":2,"ihr":False,"schedule":"SCHEDULE_II", "reservoir":"Wildlife/livestock",     "vectors":["Culex mosquito","Pig","Wading bird"],       "woah_code":"Japanese encephalitis"},
    "SCRUB_TYPHUS":     {"tier":2,"ihr":False,"schedule":"SCHEDULE_II", "reservoir":"Wildlife/peri-domestic", "vectors":["Trombiculid mite","Rodent"],                "woah_code":None},
    "KYASANUR_FOREST":  {"tier":2,"ihr":False,"schedule":"SCHEDULE_II", "reservoir":"Wildlife",               "vectors":["Haemaphysalis tick","Monkey"],              "woah_code":None},
    "MPOX":             {"tier":2,"ihr":True, "schedule":"SCHEDULE_II", "reservoir":"Wildlife",               "vectors":["Rodent","NHP"],                             "woah_code":"Mpox"},
    # TIER 3 — Routine monthly surveillance
    "SALMONELLOSIS":    {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock/environment",  "vectors":["Poultry","Cattle","Reptile"],               "woah_code":None},
    "CAMPYLOBACTERIOSIS":{"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock",             "vectors":["Poultry","Cattle"],                         "woah_code":None},
    "CRYPTOSPORIDIOSIS":{"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock/environment",  "vectors":["Cattle","Water"],                           "woah_code":None},
    "Q_FEVER":          {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock",              "vectors":["Cattle","Goat","Sheep","Tick"],             "woah_code":"Q fever"},
    "TOXOPLASMOSIS":    {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Domestic/wildlife",      "vectors":["Cat","Pig","Sheep"],                        "woah_code":None},
    "ZOONOTIC_TB":      {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock/wildlife",     "vectors":["Cattle","Buffalo","Elephant","NHP"],        "woah_code":"Bovine tuberculosis"},
    "LEISHMANIASIS":    {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Wildlife/domestic",      "vectors":["Phlebotomus sandfly","Dog"],                "woah_code":None},
    "FASCIOLOSIS":      {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Livestock/environment",  "vectors":["Cattle","Buffalo","Water snail"],           "woah_code":None},
    "MELIOIDOSIS":      {"tier":3,"ihr":False,"schedule":"SCHEDULE_III","reservoir":"Environment",            "vectors":["Soil","Water"],                             "woah_code":None},
}


# ─────────────────────────────────────────────
# STATE ROUTING MAP
# slzc_constituted per NCDC NOHP-PCZ data — verify at:
# ncdc.mohfw.gov.in/inter-sectoral-coordination-for-prevention-and-control-of-zoonotic-diseases/
# ─────────────────────────────────────────────
STATE_ROUTING_MAP: Dict[str, dict] = {
    "RJ": {"name":"Rajasthan",        "slzc_email":"slzc@rajasthan.gov.in",   "dlzc_template":"dlzc-{d}@rajasthan.gov.in",   "idsp_node":"RJ-IDSP","nadrs_node":"RJ-NADRS","slzc_constituted":True},
    "UK": {"name":"Uttarakhand",      "slzc_email":"slzc@uk.gov.in",          "dlzc_template":"dlzc-{d}@uk.gov.in",          "idsp_node":"UK-IDSP","nadrs_node":"UK-NADRS","slzc_constituted":True},
    "UP": {"name":"Uttar Pradesh",    "slzc_email":"slzc@up.gov.in",          "dlzc_template":"dlzc-{d}@up.gov.in",          "idsp_node":"UP-IDSP","nadrs_node":"UP-NADRS","slzc_constituted":True},
    "KL": {"name":"Kerala",           "slzc_email":"slzc@kerala.gov.in",      "dlzc_template":"dlzc-{d}@kerala.gov.in",      "idsp_node":"KL-IDSP","nadrs_node":"KL-NADRS","slzc_constituted":True},
    "KA": {"name":"Karnataka",        "slzc_email":"slzc@karnataka.gov.in",   "dlzc_template":"dlzc-{d}@karnataka.gov.in",   "idsp_node":"KA-IDSP","nadrs_node":"KA-NADRS","slzc_constituted":True},
    "MH": {"name":"Maharashtra",      "slzc_email":"slzc@maharashtra.gov.in", "dlzc_template":"dlzc-{d}@maharashtra.gov.in", "idsp_node":"MH-IDSP","nadrs_node":"MH-NADRS","slzc_constituted":True},
    "GA": {"name":"Goa",              "slzc_email":"slzc@goa.gov.in",         "dlzc_template":"dlzc-{d}@goa.gov.in",         "idsp_node":"GA-IDSP","nadrs_node":"GA-NADRS","slzc_constituted":False},
    "AS": {"name":"Assam",            "slzc_email":"slzc@assam.gov.in",       "dlzc_template":"dlzc-{d}@assam.gov.in",       "idsp_node":"AS-IDSP","nadrs_node":"AS-NADRS","slzc_constituted":True},
    "WB": {"name":"West Bengal",      "slzc_email":"slzc@wb.gov.in",          "dlzc_template":"dlzc-{d}@wb.gov.in",          "idsp_node":"WB-IDSP","nadrs_node":"WB-NADRS","slzc_constituted":True},
    "HP": {"name":"Himachal Pradesh", "slzc_email":"slzc@hp.gov.in",          "dlzc_template":"dlzc-{d}@hp.gov.in",          "idsp_node":"HP-IDSP","nadrs_node":"HP-NADRS","slzc_constituted":False},
    "MN": {"name":"Manipur",          "slzc_email":"slzc@manipur.gov.in",     "dlzc_template":"dlzc-{d}@manipur.gov.in",     "idsp_node":"MN-IDSP","nadrs_node":"MN-NADRS","slzc_constituted":False},
    "PB": {"name":"Punjab",           "slzc_email":"slzc@punjab.gov.in",      "dlzc_template":"dlzc-{d}@punjab.gov.in",      "idsp_node":"PB-IDSP","nadrs_node":"PB-NADRS","slzc_constituted":False},
    "GJ": {"name":"Gujarat",          "slzc_email":"slzc@gujarat.gov.in",     "dlzc_template":"dlzc-{d}@gujarat.gov.in",     "idsp_node":"GJ-IDSP","nadrs_node":"GJ-NADRS","slzc_constituted":True},
    "TN": {"name":"Tamil Nadu",       "slzc_email":"slzc@tn.gov.in",          "dlzc_template":"dlzc-{d}@tn.gov.in",          "idsp_node":"TN-IDSP","nadrs_node":"TN-NADRS","slzc_constituted":True},
}

SOURCE_API_KEYS: Dict[str, str] = {
    os.getenv("KEY_IDSP",   "ihip-integration-key"):   "IDSP_IHIP",
    os.getenv("KEY_NADRS",  "nadrs-integration-key"):  "NADRS",
    os.getenv("KEY_NADRES", "nadres-integration-key"):  "NADRES_v2",
    os.getenv("KEY_NDLM",   "ndlm-integration-key"):   "NDLM",
    os.getenv("KEY_SSSZ",   "sssz-integration-key"):   "SSSZ",
    os.getenv("KEY_ADMIN",  "zoonoseslink-admin-2026"): "ADMIN",
}


# ─────────────────────────────────────────────
# DB MODELS
# ─────────────────────────────────────────────
class DBReport(Base):
    __tablename__ = "zoonoses_reports"
    id               = Column(Integer, primary_key=True, index=True)
    reportId         = Column(String, unique=True, index=True, default=lambda: str(uuid.uuid4()))
    dedupeKey        = Column(String, index=True)
    source           = Column(String, index=True)
    eventType        = Column(String, index=True)
    disease_code     = Column(String, index=True)
    disease_name     = Column(String)
    zoonoticPriority = Column(String)
    woah_code        = Column(String, nullable=True)
    nadrsReportNo    = Column(String, nullable=True)
    riskScore        = Column(Integer, default=0)
    riskTier         = Column(String, default="LOW")
    riskFlags        = Column(JSON)
    actionsInitiated = Column(JSON)
    reportingUnit    = Column(String)
    district         = Column(String, index=True)
    block            = Column(String)
    stateCode        = Column(String, default="UK", index=True)
    lat              = Column(Float)
    lon              = Column(Float)
    timestamp        = Column(DateTime, default=datetime.utcnow)
    dateOnset        = Column(String)
    humanCases       = Column(Integer, default=0)
    humanDeaths      = Column(Integer, default=0)
    animalCases      = Column(Integer, default=0)
    animalDeaths     = Column(Integer, default=0)
    species          = Column(JSON)
    labResults       = Column(JSON)
    symptoms         = Column(Text)
    sourceSuspected  = Column(Text)
    exposureRoute    = Column(String)
    nohpczEventId    = Column(String, nullable=True)
    ssszCode         = Column(String, nullable=True)
    slzcActive       = Column(Boolean, nullable=True)
    ihrTrigger       = Column(Boolean, default=False)
    wahisTrigger     = Column(Boolean, default=False)
    crossReportForm  = Column(JSON)
    formStatus       = Column(String, default="PENDING_SLZC_REVIEW")
    enrichment       = Column(JSON, nullable=True)   # v3.2: GBIF/iNat/IUCN/Movebank/NASA context
    geom             = Column(Geometry("POINT", srid=4326, spatial_index=True))


class DBAuditLog(Base):
    __tablename__ = "audit_log"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    reportId  = Column(String, index=True)
    action    = Column(String)
    actor     = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    detail    = Column(Text)


class DBNotification(Base):
    __tablename__ = "notifications"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    reportId  = Column(String, index=True)
    channel   = Column(String)
    recipient = Column(String)
    priority  = Column(String)
    sentAt    = Column(DateTime, default=datetime.utcnow)
    status    = Column(String, default="DISPATCHED")


def create_tables():
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        conn.commit()
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────
# RISK SCORING ENGINE
# ─────────────────────────────────────────────
def calculate_risk(disease_code, event_type, human_cases, human_deaths, source, risk_flags):
    score = 0
    d = INDIA_PRIORITY_ZOONOSES.get(disease_code, {"tier": 3})
    score += {1: 40, 2: 25, 3: 10}.get(d["tier"], 5)
    score += {"CONFIRMED": 25, "OUTBREAK": 20, "SUSPECTED": 10, "EARLY_WARNING": 5}.get(event_type, 5)
    if human_cases >= 10:    score += 20
    elif human_cases >= 5:   score += 15
    elif human_cases >= 1:   score += 10
    if human_deaths >= 3:    score += 15
    elif human_deaths >= 1:  score += 10
    score += {"NADRS": 5, "NADRES_v2": 5, "IDSP_IHIP": 5, "SSSZ": 5, "NDLM": 3}.get(source, 0)
    if risk_flags.get("wildlife_involvement"): score += 3
    if risk_flags.get("transboundary_risk"):   score += 4
    score = min(score, 100)
    tier = "CRITICAL" if score >= 70 else "HIGH" if score >= 50 else "MEDIUM" if score >= 30 else "LOW"
    return score, tier


# ─────────────────────────────────────────────
# DEDUPLICATION KEY
# ─────────────────────────────────────────────
def build_dedupe_key(disease_code, district, onset, species):
    d_str = str(onset)[:7] if onset else datetime.utcnow().strftime("%Y-%m")
    sp_str = sorted(species)[0] if species else "unknown"
    return hashlib.md5(f"{disease_code}|{district.lower()}|{d_str}|{sp_str}".encode()).hexdigest()


# ─────────────────────────────────────────────
# ANNEXURE-XI FORM GENERATOR
# ─────────────────────────────────────────────
def generate_annexure_xi(report_id, payload, risk_score, risk_tier, disease_meta, state_info):
    now = datetime.utcnow().isoformat() + "Z"
    ihr = disease_meta.get("ihr", False) and payload.affected.humanCases > 0
    wahis = disease_meta.get("woah_code") is not None
    wahis_required = wahis and (ihr or risk_tier in ["CRITICAL", "HIGH"])
    cfr = round(payload.affected.humanDeaths / payload.affected.humanCases * 100, 1) if payload.affected.humanCases > 0 else 0
    action = {
        "CRITICAL": "IMMEDIATE — Deploy Joint RRT within 6h. Notify NCDC/IDSP Central Unit. IHR focal point assessment required.",
        "HIGH":     "URGENT — State RRT to deploy within 24h. SLZC emergency meeting within 48h.",
        "MEDIUM":   "ALERT — DLZC to investigate within 72h. Enhanced active surveillance in adjacent blocks.",
        "LOW":      "MONITOR — Passive surveillance. Include in monthly SLZC report.",
    }[risk_tier]
    containment = []
    if disease_meta.get("tier", 3) == 1:
        containment += ["Isolate affected animals immediately", "Demarcate quarantine zone (≥5 km radius)", "Restrict livestock movement"]
    if payload.affected.humanCases > 0:
        containment += ["Activate district epidemic response protocol", "Coordinate with CMO/CMHO for case management"]
    if disease_meta.get("tier", 3) <= 2:
        containment += ["Active surveillance in adjacent blocks", "Dispatch samples to State/NCDC reference laboratory"]
    slzc_email = state_info["slzc_email"] if state_info.get("slzc_constituted") else "SLZC not constituted — escalate to NCDC"
    dlzc_email = state_info["dlzc_template"].format(d=payload.location.district.lower().replace(" ", "-"))
    return {
        "formReference":  f"AXI-{report_id[:8].upper()}",
        "formVersion":    "Annexure-XI v3.0 (NCDC 2024)",
        "generatedAt":    now,
        "nohpczLinked":   payload.nohpczEventId or "Not yet linked",
        "sectionA_Administrative": {
            "reportId": report_id, "source": payload.source,
            "nadrsReportNo": getattr(payload, "nadrsReportNo", None),
            "ssszCode": payload.ssszCode, "nohpczEventId": payload.nohpczEventId,
            "reportingUnit": payload.reportingUnit or "Not specified",
            "reportingOfficer": payload.reporterName or "System-generated",
            "designation": payload.reporterDesignation or "Automated",
            "dateReportGenerated": now[:10],
            "dateOnset": str(payload.dateOnset) if payload.dateOnset else "Unknown",
        },
        "sectionB_DiseaseClassification": {
            "diseaseCode": payload.disease.code, "diseaseName": payload.disease.name,
            "priorityTier": f"Tier {disease_meta['tier']}", "schedule": disease_meta.get("schedule"),
            "ihrNotifiable": disease_meta.get("ihr", False),
            "ihrTimeline": "24h (IHR 2005 Art. 6)" if disease_meta.get("ihr") else "Not required",
            "woahListed": wahis,
            "woahCode": disease_meta.get("woah_code") or "Not on WOAH notifiable list",
            "wahisRequired": "IMMEDIATE WAHIS notification required" if wahis_required else "Not required",
            "reservoir": disease_meta.get("reservoir"), "knownVectors": disease_meta.get("vectors", []),
            "confirmedBy": payload.disease.confirmedBy,
        },
        "sectionC_Geographic": {
            "state": f"{state_info['name']} ({payload.location.stateCode})",
            "district": payload.location.district,
            "block": getattr(payload.location, "block", None) or "Not specified",
            "coordinates": f"{payload.location.lat}, {payload.location.lon}" if payload.location.lat else "Not recorded",
            "slzcConstituted": state_info.get("slzc_constituted", False),
            "slzcContact": slzc_email, "dlzcContact": dlzc_email,
            "idspNode": state_info.get("idsp_node"), "nadrsNode": state_info.get("nadrs_node"),
        },
        "sectionD_Epidemiology": {
            "humanCases": payload.affected.humanCases, "humanDeaths": payload.affected.humanDeaths,
            "humanCFR_pct": cfr, "animalCases": payload.affected.animalCases,
            "animalDeaths": payload.affected.animalDeaths, "speciesAffected": payload.affected.speciesAffected,
            "suspectedSource": payload.evidence.suspectedSource or "Under investigation",
            "exposureRoute": payload.evidence.exposureRoute,
        },
        "sectionE_Laboratory": {
            "labConfirmed": payload.evidence.labConfirmed, "labName": payload.evidence.labName or "Pending",
            "sampleTypes": payload.evidence.sampleTypes, "labResults": payload.evidence.labResults,
        },
        "sectionF_RiskAndAction": {
            "compositeRiskScore": risk_score, "riskTier": risk_tier,
            "riskFlags": payload.riskFlags.dict(), "recommendedAction": action,
            "containmentMeasures": containment, "actionsInitiated": payload.actionsInitiated.dict(),
            "ihrFocalPointAlert": "REQUIRED within 24h" if ihr else "Not triggered",
            "woahWAHIS": "IMMEDIATE WAHIS notification required" if wahis_required else "Not required",
            "idspCentralUnit": "Notify immediately" if payload.affected.humanCases >= 5 else "State-level",
            "ncdc_nohpcz_ref": "ncdc.mohfw.gov.in/inter-sectoral-coordination-for-prevention-and-control-of-zoonotic-diseases/",
        },
        "sectionG_StatusTracking": {
            "formStatus": "PENDING_SLZC_REVIEW",
            "ackDue": "Within 6h" if risk_tier == "CRITICAL" else "Within 48h",
            "nextUpdateDue": "24h" if risk_tier in ["CRITICAL", "HIGH"] else "7 days",
            "closureConditions": ["No new cases for 21 days", "Lab confirmation/exclusion complete", "Joint RRT report submitted"],
        },
    }


# ─────────────────────────────────────────────
# PYDANTIC SCHEMAS
# ─────────────────────────────────────────────
class SourceEnum(str, Enum):
    IDSP_IHIP = "IDSP_IHIP"
    NADRS     = "NADRS"
    NADRES_v2 = "NADRES_v2"
    NDLM      = "NDLM"
    SSSZ      = "SSSZ"


class EventTypeEnum(str, Enum):
    OUTBREAK      = "OUTBREAK"
    SUSPECTED     = "SUSPECTED"
    CONFIRMED     = "CONFIRMED"
    EARLY_WARNING = "EARLY_WARNING"


class DiseasePayload(BaseModel):
    code: str = Field(..., example="RABIES")
    name: str = Field(..., example="Rabies")
    confirmedBy: Optional[Literal["Clinical", "Laboratory", "Epidemiological", "Presumptive"]] = "Presumptive"
    woah_code: Optional[str] = Field(None, description="Read-only — auto-populated from master list on ingest.")

    @validator("code")
    def validate_code(cls, v):
        v = v.upper()
        if v not in INDIA_PRIORITY_ZOONOSES:
            raise ValueError(f"'{v}' not in India Priority Zoonoses list. Valid: {sorted(INDIA_PRIORITY_ZOONOSES)}")
        return v


class LocationPayload(BaseModel):
    stateCode: str  = Field(..., example="UK")
    district:  str  = Field(..., example="Dehradun")
    block:     Optional[str]   = None
    village:   Optional[str]   = None
    lat:       Optional[float] = None
    lon:       Optional[float] = None

    @validator("stateCode")
    def validate_state(cls, v):
        if v.upper() not in STATE_ROUTING_MAP:
            raise ValueError(f"State '{v}' not in routing map. Valid: {sorted(STATE_ROUTING_MAP)}")
        return v.upper()


class AffectedPayload(BaseModel):
    humanCases:      int = Field(0, ge=0)
    humanDeaths:     int = Field(0, ge=0)
    animalCases:     int = Field(0, ge=0)
    animalDeaths:    int = Field(0, ge=0)
    speciesAffected: List[str] = Field(default_factory=list)


class EvidencePayload(BaseModel):
    labConfirmed:    bool = False
    labResults:      List[str] = Field(default_factory=list)
    sampleTypes:     List[str] = Field(default_factory=list)
    labName:         Optional[str] = None
    symptoms:        Optional[str] = None
    suspectedSource: Optional[str] = None
    exposureRoute:   Optional[Literal["Contact", "Ingestion", "Inhalation", "Vector", "Unknown"]] = "Unknown"


class RiskFlags(BaseModel):
    human_exposure:       bool = False
    wildlife_involvement: bool = False
    foodborne_risk:       bool = False
    transboundary_risk:   bool = False


class ActionsInitiated(BaseModel):
    vaccination:     bool = False
    quarantine:      bool = False
    rrt_deployed:    bool = False
    sample_dispatch: bool = False
    public_advisory: bool = False


class CrossReportRequest(BaseModel):
    source:    SourceEnum
    eventType: EventTypeEnum
    disease:   DiseasePayload
    location:  LocationPayload
    dateOnset: Optional[date] = None
    affected:  AffectedPayload
    evidence:  EvidencePayload     = Field(default_factory=EvidencePayload)
    riskFlags: RiskFlags           = Field(default_factory=RiskFlags)
    actionsInitiated: ActionsInitiated = Field(default_factory=ActionsInitiated)
    reportingUnit: Optional[Literal["Facility", "Veterinary_Institution", "Laboratory", "Field_Team", "SSSZ_Site"]] = None
    reporterName:        Optional[str] = None
    reporterDesignation: Optional[str] = None
    nohpczEventId: Optional[str] = Field(None, description="IHIP-NOHP-PCZ portal event ID")
    ssszCode:      Optional[str] = Field(None, description="SSSZ site code (NOHP-PCZ network)")
    slzcActive:    Optional[bool] = Field(None, description="Override SLZC status; None → resolved from STATE_ROUTING_MAP")
    nadrsReportNo: Optional[str] = Field(None, description="NADRS report number — include when source=NADRS")


class NADRSIngestionPayload(CrossReportRequest):
    """Dedicated NADRS schema — locks source to NADRS."""
    source: Literal["NADRS"] = "NADRS"


# ─────────────────────────────────────────────
# AUTH + AUDIT
# ─────────────────────────────────────────────
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

def verify_api_key(x_api_key: Optional[str] = Depends(_api_key_header)):
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-API-Key header is missing")
    actor = SOURCE_API_KEYS.get(x_api_key)
    if not actor:
        raise HTTPException(status_code=403, detail="Invalid API key")
    return actor


def audit(db, report_id, action, actor, detail=None):
    db.add(DBAuditLog(reportId=report_id, action=action, actor=actor, detail=detail))
    db.commit()


# ─────────────────────────────────────────────
# NOTIFICATION DISPATCHER (v3.1 — WAHIS channel)
# ─────────────────────────────────────────────
async def dispatch_notifications(report_id, risk_tier, state_info, ihr_trigger, human_cases, disease_meta, db):
    recipients = []
    slzc_ok = state_info.get("slzc_constituted", False)
    if slzc_ok:
        recipients.append({"channel": "EMAIL", "recipient": state_info["slzc_email"], "priority": risk_tier})
    else:
        recipients.append({"channel": "EMAIL", "recipient": "surveillance@ncdc.gov.in", "priority": "HIGH"})
    if risk_tier in ["CRITICAL", "HIGH"]:
        recipients.append({"channel": "EMAIL", "recipient": "surveillance@ncdc.gov.in", "priority": "HIGH"})
        recipients.append({"channel": "EMAIL", "recipient": "idsp@mohfw.gov.in",         "priority": "HIGH"})
    if ihr_trigger:
        recipients.append({"channel": "EMAIL", "recipient": "ihr-focal-point@mohfw.gov.in", "priority": "CRITICAL"})
    if human_cases >= 5 and disease_meta.get("tier", 3) <= 2:
        recipients.append({"channel": "EMAIL", "recipient": "dadf-zoonosis@gov.in", "priority": "HIGH"})

    # WAHIS channel — intent logging only.
    # Production: DAHD national delegate submits via https://wahis.woah.org
    # Use GET /api/v3/zoonoses/export/wahis/csv to generate the submission file.
    wahis_trigger = disease_meta.get("woah_code") is not None and (ihr_trigger or risk_tier in ["CRITICAL", "HIGH"])
    if wahis_trigger:
        recipients.append({"channel": "WAHIS", "recipient": "DAHD-WOAH-Delegate (WAHIS portal)", "priority": "IMMEDIATE"})
        print(f"[ZoonosesLink v3.1] WAHIS trigger — {disease_meta['woah_code']} | {report_id[:8]}")

    for r in recipients:
        db.add(DBNotification(reportId=report_id, channel=r["channel"], recipient=r["recipient"], priority=r["priority"]))
    db.commit()
    print(f"[ZoonosesLink v3.1] {len(recipients)} notifications | {report_id[:8]} | {risk_tier}")
    return wahis_trigger


# ─────────────────────────────────────────────
# CORE PROCESSING (shared by both endpoints)
# ─────────────────────────────────────────────
async def _process_report(payload: CrossReportRequest, background_tasks: BackgroundTasks, db: Session, actor: str) -> dict:
    d_meta     = INDIA_PRIORITY_ZOONOSES[payload.disease.code]
    state_info = STATE_ROUTING_MAP[payload.location.stateCode]

    # Auto-populate WOAH code (read from master list, not from submitter)
    payload.disease.woah_code = d_meta.get("woah_code")

    slzc_active = payload.slzcActive if payload.slzcActive is not None else state_info["slzc_constituted"]
    dedupe_key  = build_dedupe_key(payload.disease.code, payload.location.district, payload.dateOnset, payload.affected.speciesAffected)

    existing = db.query(DBReport).filter(DBReport.dedupeKey == dedupe_key).first()
    if existing:
        raise HTTPException(status_code=409, detail={
            "error": "Duplicate event detected", "dedupeKey": dedupe_key,
            "existingId": existing.reportId,
            "resolution": "PATCH /api/v3/zoonoses/report/{id} to update existing report",
        })

    risk_score, risk_tier = calculate_risk(
        payload.disease.code, payload.eventType,
        payload.affected.humanCases, payload.affected.humanDeaths,
        payload.source, payload.riskFlags.dict(),
    )
    # IHR flag split — three semantically distinct concepts
    # annex2AssessmentRelevant: this event warrants Annex 2 assessment by national IHR focal point
    annex2_assessment_relevant = d_meta.get("ihr", False) and payload.affected.humanCases > 0
    # alwaysNotifiableToWHO: only 4 diseases qualify (smallpox, wild polio, novel influenza A, SARS)
    # none of the 21 India priority zoonoses in this list qualify — always False here
    always_notifiable_to_who = False
    # nationalImmediateEscalation: Tier 1 disease, any event type, with or without human cases
    national_immediate_escalation = d_meta.get("tier", 3) == 1
    # retain ihr_trigger as the stored boolean (annex2AssessmentRelevant)
    ihr_trigger = annex2_assessment_relevant
    wahis_trigger = d_meta.get("woah_code") is not None and (ihr_trigger or risk_tier in ["CRITICAL", "HIGH"])
    report_id     = str(uuid.uuid4())
    form          = generate_annexure_xi(report_id, payload, risk_score, risk_tier, d_meta, state_info)
    geom          = from_shape(Point(payload.location.lon, payload.location.lat), srid=4326) \
                    if payload.location.lat and payload.location.lon else None

    db.add(DBReport(
        reportId=report_id, dedupeKey=dedupe_key, source=payload.source, eventType=payload.eventType,
        disease_code=payload.disease.code, disease_name=payload.disease.name,
        zoonoticPriority=d_meta["schedule"], woah_code=payload.disease.woah_code,
        nadrsReportNo=getattr(payload, "nadrsReportNo", None),
        riskScore=risk_score, riskTier=risk_tier,
        riskFlags=payload.riskFlags.dict(), actionsInitiated=payload.actionsInitiated.dict(),
        reportingUnit=payload.reportingUnit,
        district=payload.location.district, block=payload.location.block, stateCode=payload.location.stateCode,
        lat=payload.location.lat, lon=payload.location.lon,
        dateOnset=str(payload.dateOnset) if payload.dateOnset else None,
        humanCases=payload.affected.humanCases, humanDeaths=payload.affected.humanDeaths,
        animalCases=payload.affected.animalCases, animalDeaths=payload.affected.animalDeaths,
        species=payload.affected.speciesAffected,
        labResults=payload.evidence.labResults, symptoms=payload.evidence.symptoms,
        sourceSuspected=payload.evidence.suspectedSource, exposureRoute=payload.evidence.exposureRoute,
        nohpczEventId=payload.nohpczEventId, ssszCode=payload.ssszCode,
        slzcActive=slzc_active, ihrTrigger=ihr_trigger, wahisTrigger=wahis_trigger,
        crossReportForm=form, geom=geom,
    ))
    db.commit()
    audit(db, report_id, "REPORT_CREATED", actor,
          f"source={payload.source} disease={payload.disease.code} state={payload.location.stateCode} "
          f"risk={risk_tier} woah={payload.disease.woah_code or 'None'}")

    background_tasks.add_task(
        dispatch_notifications, report_id, risk_tier, state_info, ihr_trigger,
        payload.affected.humanCases, d_meta, db
    )
    # Wildlife context enrichment (GBIF / iNaturalist / IUCN / Movebank / NASA)
    background_tasks.add_task(
        enrich_report, report_id, payload.disease.code,
        payload.affected.speciesAffected, payload.location.lat, payload.location.lon, db
    )
    return {
        "reportId": report_id, "status": "ACCEPTED",
        "riskScore": risk_score, "riskTier": risk_tier,
        "annex2AssessmentRelevant":    annex2_assessment_relevant,
        "alwaysNotifiableToWHO":       always_notifiable_to_who,
        "nationalImmediateEscalation": national_immediate_escalation,
        "wahisTrigger":                wahis_trigger,
        # ihrTrigger kept for backward compatibility (equals annex2AssessmentRelevant)
        "ihrTrigger":                  ihr_trigger,
        "woahCode": payload.disease.woah_code, "dedupeKey": dedupe_key,
        "formRef": form["formReference"],
        "slzcRouted": state_info["slzc_email"] if slzc_active else "NCDC (SLZC not constituted)",
        "links": {
            "form":       f"/api/v3/zoonoses/form/{report_id}",
            "audit":      f"/api/v3/zoonoses/audit/{report_id}",
            "wahis_csv":  f"/api/v3/zoonoses/export/wahis/csv?report_id={report_id}",
            "enrichment": f"/api/v3/zoonoses/enrichment/{report_id}",
        },
    }


# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────
app = FastAPI(
    title="ZoonosesLink API v3.1",
    description="India Zoonoses Cross-Reporting — NOHP-PCZ aligned, Annexure-XI, WAHIS export",
    version="3.1.0", docs_url="/docs",
)
_cors_raw = os.getenv("CORS_ORIGINS", "*")
_cors_origins = [o.strip() for o in _cors_raw.split(",")] if _cors_raw != "*" else ["*"]
app.add_middleware(CORSMiddleware, allow_origins=_cors_origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def startup():
    create_tables()
    print("[ZoonosesLink v3.1] Ready — WOAH codes, NADRS endpoint, WAHIS StreamingResponse CSV active.")


# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────
@app.post("/api/v3/zoonoses/report", status_code=201, tags=["Cross-Reporting"])
async def submit_report(payload: CrossReportRequest, bt: BackgroundTasks, db: Session = Depends(get_db), actor: str = Depends(verify_api_key)):
    """Submit a zoonoses event. Sources: IDSP_IHIP | NADRS | NADRES_v2 | NDLM | SSSZ. For NADRS, prefer /nadrs/ingest."""
    return await _process_report(payload, bt, db, actor)


@app.post("/api/v3/zoonoses/nadrs/ingest", status_code=201, tags=["NADRS"])
async def nadrs_ingest(payload: NADRSIngestionPayload, bt: BackgroundTasks, db: Session = Depends(get_db), actor: str = Depends(verify_api_key)):
    """
    Dedicated NADRS ingestion — source locked to 'NADRS'.
    Include nadrsReportNo for traceability to NADRS bulletin.
    Full pipeline: dedup → risk → Annexure-XI → SLZC routing → WAHIS trigger.
    """
    return await _process_report(payload, bt, db, actor)


@app.get("/api/v3/zoonoses/form/{report_id}", tags=["Cross-Reporting"])
async def get_form(report_id: str, db: Session = Depends(get_db)):
    r = db.query(DBReport).filter(DBReport.reportId == report_id).first()
    if not r: raise HTTPException(status_code=404, detail="Report not found")
    return r.crossReportForm


@app.get("/api/v3/zoonoses/export/wahis/csv", tags=["Export"])
async def export_wahis_csv(
    report_id:  Optional[str] = Query(None, description="Single report UUID — omit for full export"),
    risk_tier:  Optional[str] = Query(None, description="Filter: CRITICAL | HIGH | MEDIUM | LOW"),
    state_code: Optional[str] = Query(None, description="Filter by state code, e.g. UK"),
    db: Session = Depends(get_db),
):
    """
    WAHIS-compatible CSV export (text/csv StreamingResponse — direct file download).

    Filters to WOAH-listed diseases only. Designed for manual upload to
    https://wahis.woah.org by the India WOAH National Delegate (DAHD).
    This endpoint does NOT make direct calls to WAHIS.
    """
    q = db.query(DBReport).filter(DBReport.woah_code != None)
    if report_id:  q = q.filter(DBReport.reportId == report_id)
    if risk_tier:  q = q.filter(DBReport.riskTier == risk_tier.upper())
    if state_code: q = q.filter(DBReport.stateCode == state_code.upper())
    reports = q.order_by(DBReport.timestamp.desc()).all()

    HEADERS = [
        "reportId","woah_code","disease_code","eventType","stateCode","district",
        "riskTier","riskScore","humanCases","humanDeaths","animalCases","animalDeaths",
        "species","ihrTrigger","wahisTrigger","nadrsReportNo","nohpczEventId",
        "ssszCode","source","dateOnset","timestamp",
    ]

    def generate_csv():
        buf = StringIO(); csv.writer(buf).writerow(HEADERS); yield buf.getvalue()
        for r in reports:
            buf = StringIO()
            csv.writer(buf).writerow([
                r.reportId, r.woah_code or "N/A", r.disease_code, r.eventType,
                r.stateCode, r.district, r.riskTier, r.riskScore,
                r.humanCases, r.humanDeaths, r.animalCases, r.animalDeaths,
                ";".join(r.species or []), r.ihrTrigger, r.wahisTrigger,
                r.nadrsReportNo or "", r.nohpczEventId or "", r.ssszCode or "",
                r.source, r.dateOnset or "",
                r.timestamp.isoformat() if r.timestamp else "",
            ])
            yield buf.getvalue()

    filename = f"wahis_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(generate_csv(), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/api/v3/zoonoses/audit/{report_id}", tags=["Audit"])
async def get_audit(report_id: str, db: Session = Depends(get_db)):
    rows = db.query(DBAuditLog).filter(DBAuditLog.reportId == report_id).order_by(DBAuditLog.id).all()
    return {"reportId": report_id, "trail": [{"action":r.action,"actor":r.actor,"timestamp":str(r.timestamp),"detail":r.detail} for r in rows]}


@app.get("/api/v3/zoonoses/dashboard", tags=["Dashboard"])
async def dashboard(
    stateCode:   Optional[str] = None, riskTier:    Optional[str] = None,
    diseaseCode: Optional[str] = None, wahisOnly:   bool = False,
    limit: int = 100, offset: int = 0, db: Session = Depends(get_db),
):
    # Global summary — always unfiltered, for headline metrics
    summary = {
        "totalReports":    db.query(DBReport).count(),
        "criticalEvents":  db.query(DBReport).filter(DBReport.riskTier == "CRITICAL").count(),
        "annex2Flags":     db.query(DBReport).filter(DBReport.ihrTrigger == True).count(),
        "statesActive":    db.query(func.count(func.distinct(DBReport.stateCode))).scalar() or 0,
        "wahisTriggered":  db.query(DBReport).filter(DBReport.wahisTrigger == True).count(),
    }
    # Filtered result set for table display
    q = db.query(DBReport)
    if stateCode:   q = q.filter(DBReport.stateCode    == stateCode.upper())
    if riskTier:    q = q.filter(DBReport.riskTier     == riskTier.upper())
    if diseaseCode: q = q.filter(DBReport.disease_code == diseaseCode.upper())
    if wahisOnly:   q = q.filter(DBReport.wahisTrigger == True)
    total = q.count()
    rows  = q.order_by(DBReport.riskScore.desc(), DBReport.timestamp.desc()).offset(offset).limit(limit).all()
    return {"summary": summary, "total": total, "offset": offset, "reports": [
        {"reportId":r.reportId,"source":r.source,"eventType":r.eventType,"disease_code":r.disease_code,
         "woah_code":r.woah_code,"district":r.district,"stateCode":r.stateCode,"riskScore":r.riskScore,
         "riskTier":r.riskTier,"ihrTrigger":r.ihrTrigger,"wahisTrigger":r.wahisTrigger,
         "annex2AssessmentRelevant":r.ihrTrigger,"nationalImmediateEscalation":r.disease_code in [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v["tier"]==1],
         "humanCases":r.humanCases,"humanDeaths":r.humanDeaths,"formStatus":r.formStatus,
         "slzcActive":r.slzcActive,"nohpczEventId":r.nohpczEventId,"ssszCode":r.ssszCode,
         "nadrsReportNo":r.nadrsReportNo,"timestamp":str(r.timestamp)} for r in rows]}


@app.get("/api/v3/zoonoses/nearby", tags=["Spatial"])
async def nearby(
    lat: float = Query(...), lon: float = Query(...), radius_km: float = Query(50.0),
    disease_code: Optional[str] = None, db: Session = Depends(get_db),
):
    radius_m = radius_km * 1000
    q = db.query(DBReport).filter(text(f"ST_DWithin(geom::geography, ST_MakePoint({lon},{lat})::geography, {radius_m})"))
    if disease_code: q = q.filter(DBReport.disease_code == disease_code.upper())
    rows = q.order_by(DBReport.timestamp.desc()).limit(100).all()
    return {"centre":{"lat":lat,"lon":lon},"radius_km":radius_km,"count":len(rows),
            "reports":[{"reportId":r.reportId,"disease":r.disease_code,"woah_code":r.woah_code,
                        "district":r.district,"riskTier":r.riskTier,"lat":r.lat,"lon":r.lon} for r in rows]}


@app.get("/api/v3/zoonoses/clusters", tags=["Spatial"])
async def clusters(
    stateCode: Optional[str] = Query(None), eps_km: float = Query(10.0), min_points: int = Query(3),
    db: Session = Depends(get_db),
):
    eps_m = eps_km * 1000
    where = f'WHERE "stateCode" = \'{stateCode.upper()}\'' if stateCode else ""
    rows = db.execute(text(f"""
        SELECT "reportId", disease_code, "woah_code", district, lat, lon,
               "nohpczEventId", "ssszCode", "riskTier",
               ST_ClusterDBSCAN(geom::geometry, {eps_m}, {min_points}) OVER() AS cluster_id
        FROM zoonoses_reports {where} ORDER BY cluster_id, timestamp DESC
    """)).fetchall()
    out: Dict[int, list] = {}
    for r in rows:
        cid = r.cluster_id if r.cluster_id is not None else -1
        out.setdefault(cid, []).append({"reportId":r.reportId,"disease":r.disease_code,"woahCode":r.woah_code,
                                        "district":r.district,"riskTier":r.riskTier,"lat":r.lat,"lon":r.lon,
                                        "nohpczEventId":r.nohpczEventId,"ssszCode":r.ssszCode})
    return {"clusterCount":len([c for c in out if c!=-1]),"noiseCount":len(out.get(-1,[])),"stateFilter":stateCode or "All","eps_km":eps_km,"clusters":out}


@app.get("/api/v3/zoonoses/disease-list", tags=["Reference"])
async def disease_list():
    return {"totalDiseases":len(INDIA_PRIORITY_ZOONOSES),
            "woahListed":    [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v.get("woah_code")],
            "ihrNotifiable": [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v.get("ihr")],
            "tier1":         [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v["tier"]==1],
            "tier2":         [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v["tier"]==2],
            "tier3":         [k for k,v in INDIA_PRIORITY_ZOONOSES.items() if v["tier"]==3],
            "diseases":      INDIA_PRIORITY_ZOONOSES}


@app.get("/api/v3/zoonoses/states", tags=["Reference"])
async def state_routing():
    return {"states": STATE_ROUTING_MAP}


@app.get("/api/v3/zoonoses/enrichment/{report_id}", tags=["Wildlife Context"])
async def get_enrichment(report_id: str, db: Session = Depends(get_db)):
    """
    Wildlife context enrichment for a report.
    Populated asynchronously after ingest — allow 10-30 seconds for full results.

    Sections returned:
      gbif        — Species occurrence records near outbreak (GBIF)
      inaturalist — Community wildlife observations near outbreak (iNaturalist)
      iucn        — IUCN Red List conservation status of implicated species
      movebank    — Animal tracking studies for implicated species (Movebank)
      nasa        — Environmental/habitat datasets for the location (NASA Earthdata CMR)

    Authentication: IUCN and Movebank sections require credentials in .env
    (IUCN_API_TOKEN, MOVEBANK_USERNAME, MOVEBANK_PASSWORD).
    GBIF, iNaturalist, and NASA CMR collection search require no credentials.
    """
    r = db.query(DBReport).filter(DBReport.reportId == report_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Report not found")
    if not r.enrichment:
        return {
            "reportId": report_id,
            "status": "PENDING",
            "message": "Enrichment is running in the background — retry in 15-30 seconds",
        }
    return r.enrichment


@app.get("/health", tags=["System"])
async def health(db: Session = Depends(get_db)):
    count       = db.query(DBReport).count()
    wahis_count = db.query(DBReport).filter(DBReport.wahisTrigger == True).count()
    postgis_ver = db.execute(text("SELECT PostGIS_Version();")).scalar()
    return {"status":"healthy","version":"3.1.0","database":"PostgreSQL + PostGIS",
            "postgis_version":postgis_ver,"total_reports":count,"wahis_triggered":wahis_count,
            "woah_codes_loaded":len([v for v in INDIA_PRIORITY_ZOONOSES.values() if v.get("woah_code")])}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
