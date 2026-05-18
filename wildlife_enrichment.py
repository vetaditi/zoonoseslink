"""
wildlife_enrichment.py — ZoonosesLink Wildlife Context Enrichment Module
=========================================================================
Integrates five external APIs from the Wildlife Health Monitoring API collection
(github.com/vetaditi/supreme-octo-train) to enrich each zoonotic cross-report
with ecological and environmental context.

APIs integrated:
  1. GBIF     — Species occurrence data near outbreak location (no auth)
  2. iNaturalist — Community wildlife observations near location (no auth)
  3. IUCN Red List — Conservation status of implicated species (API token)
  4. Movebank — Animal tracking studies in India (Basic Auth)
  5. NASA Earthdata (CMR) — Environmental/habitat datasets for location (no auth for search)

Design:
  • All calls are async and best-effort — failures are logged, never block ingest
  • Results stored as JSON in DBReport.enrichment column
  • Triggered as a BackgroundTask after successful report creation
  • Each section is independently nullable (API down → section is None, rest still stored)

Common names → GBIF scientific name mapping covers the 21 India priority zoonoses
reservoir species. Extend SPECIES_MAP for additional taxa.

Author: Dr. Aditi Sharma, CESD Dehradun / MSc One Health, University of Edinburgh
Version: 1.0.0 | May 2026
"""

import httpx
import os
import base64
from typing import Optional, Dict, List, Any
from datetime import datetime, date

# ─────────────────────────────────────────────
# CREDENTIALS (set in .env)
# IUCN: register at https://apiv3.iucnredlist.org/api/v3/token
# Movebank: register at https://www.movebank.org
# NASA Earthdata: register at https://urs.earthdata.nasa.gov
# ─────────────────────────────────────────────
IUCN_TOKEN       = os.getenv("IUCN_API_TOKEN", "")
MOVEBANK_USER    = os.getenv("MOVEBANK_USERNAME", "")
MOVEBANK_PASS    = os.getenv("MOVEBANK_PASSWORD", "")

# Timeout for all external API calls (seconds)
API_TIMEOUT = 10.0

# ─────────────────────────────────────────────
# SPECIES MAP — common/field names → GBIF scientific names
# Covers reservoir and vector species in INDIA_PRIORITY_ZOONOSES
# ─────────────────────────────────────────────
SPECIES_MAP: Dict[str, str] = {
    # Domestic/peri-domestic
    "Dog":              "Canis lupus familiaris",
    "Cattle":           "Bos taurus",
    "Buffalo":          "Bubalus bubalis",
    "Goat":             "Capra hircus",
    "Sheep":            "Ovis aries",
    "Pig":              "Sus scrofa",
    "Cat":              "Felis catus",
    "Horse":            "Equus caballus",
    "Donkey":           "Equus africanus asinus",
    "Poultry":          "Gallus gallus domesticus",
    # Wildlife
    "Rat":              "Rattus rattus",
    "Rodent":           "Rattus norvegicus",
    "Bat":              "Pteropus giganteus",       # Indian flying fox (Nipah/Rabies)
    "Fruit bat":        "Pteropus giganteus",
    "Jackal":           "Canis aureus",
    "Fox":              "Vulpes bengalensis",
    "Monkey":           "Macaca mulatta",           # Rhesus macaque (KFD)
    "NHP":              "Macaca mulatta",
    "Elephant":         "Elephas maximus",
    "Wading bird":      "Ardea cinerea",
    "Migratory bird":   "Anser anser",
    # Vectors/invertebrates (GBIF has records but low utility — still included)
    "Culex mosquito":   "Culex quinquefasciatus",
    "Haemaphysalis tick": "Haemaphysalis spinigera",  # KFD tick
    "Hyalomma tick":    "Hyalomma marginatum",
}

# IUCN uses common names or scientific names — mapping for key species
IUCN_SPECIES_MAP: Dict[str, str] = {
    "Bat":           "Pteropus giganteus",
    "Fruit bat":     "Pteropus giganteus",
    "Elephant":      "Elephas maximus",
    "Monkey":        "Macaca mulatta",
    "NHP":           "Macaca mulatta",
    "Jackal":        "Canis aureus",
    "Tiger":         "Panthera tigris",
    "Leopard":       "Panthera pardus",
}


# ─────────────────────────────────────────────
# 1. GBIF — Species occurrence search
# Endpoint: GET https://api.gbif.org/v1/occurrence/search
# Auth: None
# Docs: github.com/vetaditi/supreme-octo-train → src/gbif.md
# ─────────────────────────────────────────────
async def fetch_gbif_occurrences(
    species_list: List[str],
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float = 50.0,
) -> Optional[Dict]:
    """
    Query GBIF for recent occurrence records of implicated species
    within radius_km of the outbreak coordinates.
    Returns top 5 occurrences per species (max 3 species queried).
    """
    results = {}
    wildlife_species = [s for s in species_list if s in SPECIES_MAP and s != "Human"][:3]
    if not wildlife_species:
        return {"note": "No mappable wildlife species in this report", "occurrences": []}

    async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
        for common_name in wildlife_species:
            sci_name = SPECIES_MAP[common_name]
            params = {"scientificName": sci_name, "limit": 5, "hasCoordinate": "true"}
            if lat and lon:
                # GBIF uses decimalLatitude/decimalLongitude bounding box
                delta = radius_km / 111.0  # ~1 degree = 111 km
                params.update({
                    "decimalLatitude":  f"{lat - delta},{lat + delta}",
                    "decimalLongitude": f"{lon - delta},{lon + delta}",
                })
            try:
                r = await client.get("https://api.gbif.org/v1/occurrence/search", params=params)
                if r.status_code == 200:
                    data = r.json()
                    results[common_name] = {
                        "scientificName": sci_name,
                        "totalRecords":   data.get("count", 0),
                        "recentRecords":  [
                            {
                                "occurrenceId": o.get("key"),
                                "locality":     o.get("locality") or o.get("stateProvince") or "India",
                                "eventDate":    o.get("eventDate"),
                                "datasetName":  o.get("datasetName"),
                                "lat":          o.get("decimalLatitude"),
                                "lon":          o.get("decimalLongitude"),
                                "gbifLink":     f"https://www.gbif.org/occurrence/{o.get('key')}",
                            }
                            for o in data.get("results", [])[:5]
                        ],
                    }
            except Exception as e:
                results[common_name] = {"error": str(e)}

    return {"queriedAt": datetime.utcnow().isoformat() + "Z", "species": results}


# ─────────────────────────────────────────────
# 2. iNaturalist — Wildlife observations
# Endpoint: GET https://api.inaturalist.org/v1/observations
# Auth: None (read operations)
# Docs: github.com/vetaditi/supreme-octo-train → src/inaturalist.md
# ─────────────────────────────────────────────
async def fetch_inaturalist_observations(
    species_list: List[str],
    lat: Optional[float],
    lon: Optional[float],
    radius_km: float = 30.0,
) -> Optional[Dict]:
    """
    Query iNaturalist for research-grade wildlife observations near outbreak location.
    Returns recent observations of implicated species.
    """
    results = []
    wildlife_species = [s for s in species_list if s in SPECIES_MAP and s != "Human"][:3]

    async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
        for common_name in wildlife_species:
            sci_name = SPECIES_MAP[common_name]
            params = {
                "taxon_name":  sci_name,
                "per_page":    5,
                "quality_grade": "research",
                "order":       "desc",
                "order_by":    "observed_on",
            }
            if lat and lon:
                params.update({"lat": lat, "lng": lon, "radius": radius_km})
            try:
                r = await client.get("https://api.inaturalist.org/v1/observations", params=params)
                if r.status_code == 200:
                    data = r.json()
                    for obs in data.get("results", [])[:5]:
                        results.append({
                            "species":        common_name,
                            "scientificName": sci_name,
                            "observedOn":     obs.get("observed_on"),
                            "place":          obs.get("place_guess"),
                            "quality":        obs.get("quality_grade"),
                            "inatLink":       f"https://www.inaturalist.org/observations/{obs.get('id')}",
                        })
            except Exception as e:
                results.append({"species": common_name, "error": str(e)})

    return {
        "queriedAt":    datetime.utcnow().isoformat() + "Z",
        "totalResults": len(results),
        "observations": results,
    }


# ─────────────────────────────────────────────
# 3. IUCN Red List — Conservation status
# Endpoint: GET https://apiv3.iucnredlist.org/api/v3/species/{name}
# Auth: API token as query param `token`
# Docs: github.com/vetaditi/supreme-octo-train → src/iucn.md
# ─────────────────────────────────────────────
async def fetch_iucn_status(species_list: List[str]) -> Optional[Dict]:
    """
    Retrieve IUCN Red List conservation status for wildlife species in the report.
    Flags threatened/endangered species involvement — adds conservation urgency.
    Requires IUCN_API_TOKEN in .env.
    """
    if not IUCN_TOKEN:
        return {"note": "IUCN_API_TOKEN not configured — set in .env to enable", "statuses": []}

    statuses = []
    wildlife_species = [s for s in species_list if s in IUCN_SPECIES_MAP][:4]

    async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
        for common_name in wildlife_species:
            sci_name = IUCN_SPECIES_MAP[common_name]
            try:
                r = await client.get(
                    f"https://apiv3.iucnredlist.org/api/v3/species/{sci_name}",
                    params={"token": IUCN_TOKEN},
                )
                if r.status_code == 200:
                    data = r.json()
                    result = data.get("result", [{}])[0] if data.get("result") else {}
                    statuses.append({
                        "commonName":     common_name,
                        "scientificName": sci_name,
                        "category":       result.get("category", "Unknown"),  # CR/EN/VU/NT/LC/DD
                        "populationTrend": result.get("population_trend"),
                        "assessmentYear": result.get("assessment_date", "")[:4] if result.get("assessment_date") else None,
                        "iucnLink":       f"https://www.iucnredlist.org/search?query={sci_name.replace(' ', '+')}",
                        "conservationUrgency": "HIGH" if result.get("category") in ["CR","EN","EW","EX"] else
                                               "MODERATE" if result.get("category") in ["VU","NT"] else "LOW",
                    })
                else:
                    statuses.append({"commonName": common_name, "error": f"HTTP {r.status_code}"})
            except Exception as e:
                statuses.append({"commonName": common_name, "error": str(e)})

    return {
        "queriedAt": datetime.utcnow().isoformat() + "Z",
        "statuses":  statuses,
        "threatenedSpeciesInvolved": any(
            s.get("category") in ["CR","EN","VU"] for s in statuses
        ),
    }


# ─────────────────────────────────────────────
# 4. Movebank — Animal tracking studies
# Endpoint: GET https://www.movebank.org/movebank/service/direct-read
# Auth: Basic Auth (username / password)
# Docs: github.com/vetaditi/supreme-octo-train → src/movebank.md
# ─────────────────────────────────────────────
async def fetch_movebank_studies(
    species_list: List[str],
    country: str = "India",
) -> Optional[Dict]:
    """
    Search Movebank for animal tracking studies involving implicated species in India.
    Identifies whether movement corridor data exists for reservoir species.
    Requires MOVEBANK_USERNAME and MOVEBANK_PASSWORD in .env.
    """
    if not MOVEBANK_USER or not MOVEBANK_PASS:
        return {"note": "MOVEBANK_USERNAME/PASSWORD not configured — set in .env to enable", "studies": []}

    auth_str = base64.b64encode(f"{MOVEBANK_USER}:{MOVEBANK_PASS}".encode()).decode()
    headers  = {"Authorization": f"Basic {auth_str}"}

    try:
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            r = await client.get(
                "https://www.movebank.org/movebank/service/direct-read",
                params={"entity_type": "study", "study_type": "research"},
                headers=headers,
            )
            if r.status_code != 200:
                return {"note": f"Movebank returned HTTP {r.status_code}", "studies": []}

            # Parse CSV response from Movebank
            lines = r.text.strip().split("\n")
            if len(lines) < 2:
                return {"studies": [], "total": 0}

            header = [h.strip('"') for h in lines[0].split(",")]
            studies = []
            for line in lines[1:21]:   # cap at 20 rows
                row = dict(zip(header, [v.strip('"') for v in line.split(",")]))
                name = row.get("name", "").lower()
                # Filter for India-related or species-related studies
                if "india" in name or any(s.lower() in name for s in species_list if s != "Human"):
                    studies.append({
                        "studyId":    row.get("id"),
                        "studyName":  row.get("name"),
                        "taxon":      row.get("main_location_long"),
                        "movebankLink": f"https://www.movebank.org/cms/webapp?gwt_fragment=page%3Dsearch_map_linked%2CstudyId%3D{row.get('id')}",
                    })

            return {
                "queriedAt":    datetime.utcnow().isoformat() + "Z",
                "studiesFound": len(studies),
                "studies":      studies[:5],
            }
    except Exception as e:
        return {"note": f"Movebank query failed: {str(e)}", "studies": []}


# ─────────────────────────────────────────────
# 5. NASA Earthdata (CMR) — Environmental context
# Endpoint: GET https://cmr.earthdata.nasa.gov/search/collections.json
# Auth: None for collection search; Earthdata credentials for granule download
# Docs: github.com/vetaditi/supreme-octo-train → src/nasa-earthdata.md
# ─────────────────────────────────────────────
async def fetch_nasa_environmental(
    lat: Optional[float],
    lon: Optional[float],
    disease_code: str = "",
) -> Optional[Dict]:
    """
    Query NASA CMR for environmental datasets relevant to the outbreak location.
    Three targeted queries:
      - Land cover / habitat (MOD12Q1 — MODIS Land Cover)
      - Vegetation index / habitat quality (MOD13A1 — MODIS NDVI)
      - Surface temperature (MOD11A2 — MODIS LST, relevant for vector activity)
    """
    datasets = [
        {"short_name": "MOD12Q1",  "label": "Land cover / habitat type (MODIS)",     "keyword": "land cover"},
        {"short_name": "MOD13A1",  "label": "Vegetation index / NDVI (MODIS)",        "keyword": "vegetation index NDVI"},
        {"short_name": "MOD11A2",  "label": "Land surface temperature (MODIS LST)",   "keyword": "land surface temperature"},
    ]

    results = []
    async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
        for ds in datasets:
            try:
                params = {"short_name": ds["short_name"], "page_size": 1}
                r = await client.get(
                    "https://cmr.earthdata.nasa.gov/search/collections.json",
                    params=params,
                )
                if r.status_code == 200:
                    items = r.json().get("feed", {}).get("entry", [])
                    if items:
                        item = items[0]
                        results.append({
                            "dataset":      ds["label"],
                            "shortName":    ds["short_name"],
                            "collectionId": item.get("id"),
                            "version":      item.get("version_id"),
                            "granuleSearch": (
                                f"https://cmr.earthdata.nasa.gov/search/granules.json"
                                f"?short_name={ds['short_name']}&page_size=5"
                                + (f"&bounding_box={lon-0.5},{lat-0.5},{lon+0.5},{lat+0.5}" if lat and lon else "")
                            ),
                            "earthdataLink": f"https://search.earthdata.nasa.gov/search?q={ds['short_name']}",
                        })
            except Exception as e:
                results.append({"dataset": ds["label"], "error": str(e)})

    return {
        "queriedAt":   datetime.utcnow().isoformat() + "Z",
        "coordinates": {"lat": lat, "lon": lon} if lat and lon else "Not provided",
        "note":        "Collection search only — granule download requires NASA Earthdata credentials (urs.earthdata.nasa.gov)",
        "datasets":    results,
    }


# ─────────────────────────────────────────────
# MASTER ENRICHMENT FUNCTION
# Called as a BackgroundTask after _process_report()
# Stores result in DBReport.enrichment column
# ─────────────────────────────────────────────
async def enrich_report(
    report_id: str,
    disease_code: str,
    species_affected: List[str],
    lat: Optional[float],
    lon: Optional[float],
    db,  # SQLAlchemy Session — imported from main
):
    """
    Run all five external API enrichments for a report and store results.
    Fully best-effort: any individual failure is captured, never raises.
    """
    from sqlalchemy.orm import Session

    print(f"[Enrichment] Starting for {report_id[:8]} — disease={disease_code} species={species_affected}")

    enrichment = {
        "enrichedAt":  datetime.utcnow().isoformat() + "Z",
        "reportId":    report_id,
        "diseaseCode": disease_code,
        "gbif":        None,
        "inaturalist": None,
        "iucn":        None,
        "movebank":    None,
        "nasa":        None,
    }

    # Run enrichments independently so one failure doesn't stop others
    try:
        enrichment["gbif"] = await fetch_gbif_occurrences(species_affected, lat, lon)
    except Exception as e:
        enrichment["gbif"] = {"error": str(e)}

    try:
        enrichment["inaturalist"] = await fetch_inaturalist_observations(species_affected, lat, lon)
    except Exception as e:
        enrichment["inaturalist"] = {"error": str(e)}

    try:
        enrichment["iucn"] = await fetch_iucn_status(species_affected)
    except Exception as e:
        enrichment["iucn"] = {"error": str(e)}

    try:
        enrichment["movebank"] = await fetch_movebank_studies(species_affected)
    except Exception as e:
        enrichment["movebank"] = {"error": str(e)}

    try:
        enrichment["nasa"] = await fetch_nasa_environmental(lat, lon, disease_code)
    except Exception as e:
        enrichment["nasa"] = {"error": str(e)}

    # Write to DB
    try:
        from main import DBReport
        report = db.query(DBReport).filter(DBReport.reportId == report_id).first()
        if report:
            report.enrichment = enrichment
            db.commit()
            print(f"[Enrichment] Complete for {report_id[:8]}")
    except Exception as e:
        print(f"[Enrichment] DB write failed for {report_id[:8]}: {e}")
