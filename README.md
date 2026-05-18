# ZoonosesLink API v3.0
**India Zoonoses Cross-Reporting System — NOHP-PCZ Aligned**

Integrates IDSP/IHIP ↔ NADRS/NADRES v2/NDLM/SSSZ → State Zoonoses and One Health Committees.  
Generates Annexure-XI digital cross-reporting forms with risk scoring, IHR triggers, and PostGIS spatial queries.

---

## Quick start (Docker — recommended)

```bash
git clone <repo>
cd zoonoseslink-v3
cp .env.example .env          # edit if needed
docker-compose up --build
```

API:       http://localhost:8000  
Swagger:   http://localhost:8000/docs  
Dashboard: open `dashboard.html` in any browser

---

## Quick start (local, no Docker)

```bash
pip install -r requirements.txt
cp .env.example .env          # update DATABASE_URL
alembic upgrade head
uvicorn main:app --reload --port 8000
```

Requires: PostgreSQL 14+ with PostGIS 3.x extension enabled.

---

## Project structure

```
zoonoseslink-v3/
├── main.py                 Full v3.0 API (PostGIS + Alembic-ready)
├── requirements.txt
├── .env.example            Copy to .env before running
├── alembic.ini
├── migrations/
│   ├── env.py              GeoAlchemy2-aware Alembic config
│   └── script.py.mako
├── docker-compose.yml      PostGIS + API + auto-migration
├── Dockerfile
├── dashboard.html          Interactive dashboard (all v3 endpoints)
└── README.md
```

---

## Key API endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v3/zoonoses/report` | Submit cross-report → Annexure-XI form + risk score |
| GET  | `/api/v3/zoonoses/form/{id}` | Retrieve generated Annexure-XI form |
| GET  | `/api/v3/zoonoses/dashboard` | Filterable dashboard (state, tier, disease) |
| GET  | `/api/v3/zoonoses/nearby` | ST_DWithin spatial query |
| GET  | `/api/v3/zoonoses/clusters` | ST_ClusterDBSCAN outbreak detection |
| GET  | `/api/v3/zoonoses/audit/{id}` | Full audit trail for a report |
| GET  | `/api/v3/zoonoses/disease-list` | India Priority Zoonoses master list |
| GET  | `/api/v3/zoonoses/states` | State routing map (SLZC/DLZC contacts) |
| GET  | `/health` | System health + PostGIS version |

---

## Source systems supported

| Source | Description |
|--------|-------------|
| `IDSP_IHIP` | Integrated Disease Surveillance Programme / IHIP portal |
| `NADRS` | National Animal Disease Reporting System |
| `NADRES_v2` | NADRES version 2 (ICAR-NIVEDI) |
| `NDLM` | National Digital Livestock Mission |
| `SSSZ` | Sentinel Surveillance Sites for Zoonoses (NOHP-PCZ network) |

---

## NOHP-PCZ integration fields

Three fields align every report with the NCDC NOHP-PCZ programme:

- `nohpczEventId` — links to the IHIP-NOHP-PCZ portal event record  
- `ssszCode` — identifies the reporting Sentinel Surveillance Site  
- `slzcActive` — drives SLZC vs NCDC escalation routing  

NOHP-PCZ portal: https://ncdc.mohfw.gov.in/inter-sectoral-coordination-for-prevention-and-control-of-zoonotic-diseases/  
IHIP module: https://ihip.mohfw.gov.in/nohpcz/

---

## Risk scoring

Composite 0–100 score from five components:

| Component | Max pts |
|-----------|---------|
| Disease priority tier (T1=40, T2=25, T3=10) | 40 |
| Event type (Confirmed=25, Outbreak=20, Suspected=10) | 25 |
| Human case count | 20 |
| Human deaths | 15 |
| Source credibility + risk flags | 9 |

**CRITICAL ≥70 · HIGH 50–69 · MEDIUM 30–49 · LOW <30**

---

## Alembic migrations

```bash
# Generate a new migration after model changes
alembic revision --autogenerate -m "describe change"

# Apply all pending migrations
alembic upgrade head

# Roll back one migration
alembic downgrade -1
```

---

## Production deployment notes

- Replace `allow_origins=["*"]` with `["*.rajasthan.gov.in", "*.ncdc.mohfw.gov.in"]`
- Replace API key auth with OAuth2 + mTLS (NIC PKI or RajSSO)
- Connect notification dispatcher to RajSSO / NIC eOffice / WhatsApp Business API
- Enable PostGIS spatial indexing on `geom` column (handled automatically by GeoAlchemy2)
- Run behind Nginx reverse proxy with TLS

---

## Author

Dr. Aditi Sharma  
Wildlife Veterinarian  
founder & Director, Council for Environment & Sustainable Development, Dehradun  
