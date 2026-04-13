# RiskLens — Implementation Plan
**FRTB Data Catalog & AI Lineage Explorer**

**Date:** 2026-04-13  
**Target:** 2–3 weeks, publicly shippable  
**Design spec:** `2026-04-13-risklens-design.md`

---

## Repository Structure

```
risklens/
├── ingestion/
│   ├── jobs/
│   │   ├── bronze_trades.py        ← DTCC SDR → BigQuery bronze
│   │   ├── bronze_rates.py         ← FRED API → BigQuery bronze
│   │   ├── bronze_prices.py        ← Yahoo Finance → BigQuery bronze
│   │   ├── bronze_synthetic.py     ← Generate synthetic data → bronze
│   │   ├── silver_transform.py     ← Bronze → Silver (clean + validate)
│   │   └── gold_aggregate.py       ← Silver → Gold (business-ready)
│   ├── dags/
│   │   └── risklens_pipeline.py    ← Cloud Composer DAG
│   └── synthetic/
│       └── generate.py             ← Synthetic VaR/ES/P&L/ownership generator
├── indexing/
│   ├── chunker.py                  ← LangChain TextSplitter
│   ├── embedder.py                 ← LangChain Embeddings → BigQuery
│   └── bm25_index.py               ← BM25 keyword index builder
├── api/
│   ├── main.py                     ← FastAPI app
│   ├── routers/
│   │   ├── catalog.py
│   │   ├── lineage.py
│   │   ├── governance.py
│   │   ├── assets.py
│   │   ├── search.py
│   │   └── chat.py                 ← Streaming RAG endpoint
│   ├── rag/
│   │   ├── pipeline.py             ← LangChain LCEL RAG chain
│   │   ├── agent.py                ← LangGraph agent
│   │   └── retriever.py            ← EnsembleRetriever (BM25 + vector)
│   └── db/
│       └── bigquery.py             ← BigQuery client + queries
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── CatalogView.jsx
│   │   │   ├── LineageGraph.jsx
│   │   │   ├── GovernanceView.jsx
│   │   │   ├── AssetsView.jsx
│   │   │   └── ChatPanel.jsx       ← Persistent chat, always visible
│   │   └── App.jsx
│   └── Dockerfile
├── infra/
│   ├── k8s/
│   │   ├── api-deployment.yaml
│   │   ├── frontend-deployment.yaml
│   │   └── services.yaml
│   └── terraform/                  ← GCP infra (optional)
├── api/Dockerfile
└── docker-compose.yml              ← Local dev
```

---

## Phase 1 — Infrastructure & Data Foundation (Days 1–3)

### Step 1.1 — GCP Project Setup
- Create GCP project `risklens`
- Enable APIs: BigQuery, Dataproc, Cloud Composer, GCS, GKE
- Create GCS bucket: `gs://risklens-raw/`
- Create BigQuery datasets: `risklens_bronze`, `risklens_silver`, `risklens_gold`, `risklens_catalog`, `risklens_lineage`, `risklens_embeddings`
- Create service account with roles: BigQuery Admin, Dataproc Editor, Storage Admin

### Step 1.2 — BigQuery Schema Setup
Create tables:

**`risklens_catalog`**
```sql
assets(asset_id STRING, name STRING, type STRING, domain STRING, 
       layer STRING, description STRING, tags ARRAY<STRING>, created_at TIMESTAMP)

ownership(asset_id STRING, owner_name STRING, team STRING, 
          steward STRING, email STRING, assigned_date DATE)

quality_scores(asset_id STRING, null_rate FLOAT64, schema_drift BOOL,
               freshness_status STRING, last_checked TIMESTAMP)

sla_status(asset_id STRING, expected_refresh TIMESTAMP, 
           actual_refresh TIMESTAMP, breach_flag BOOL)

schema_registry(asset_id STRING, column_name STRING, data_type STRING,
                nullable BOOL, sample_value STRING)
```

**`risklens_lineage`**
```sql
nodes(node_id STRING, name STRING, type STRING, metadata JSON)
edges(from_node_id STRING, to_node_id STRING, relationship STRING, pipeline_job STRING)
```

**`risklens_embeddings`**
```sql
chunks(chunk_id STRING, asset_id STRING, text STRING, source_type STRING, created_at TIMESTAMP)
vectors(chunk_id STRING, embedding ARRAY<FLOAT64>)
```

### Step 1.3 — Synthetic Data Generator
Build `ingestion/synthetic/generate.py`:
- Generate 5 trading desks: Rates, FX, Credit, Equities, Commodities
- 10,000 trades per desk per day (instrument, notional, book, counterparty)
- Daily VaR and ES per desk (normally distributed, seeded from real FRED rates)
- P&L vectors (100 scenarios per trade)
- Ownership registry (5 data owners, 3 stewards)
- Pipeline job logs (status, rows, duration, errors)
- Data quality results (null rates, schema checks, freshness)

---

## Phase 2 — Ingestion Pipeline (Days 3–6)

### Step 2.1 — Bronze Jobs (Spark on Dataproc)

**`bronze_trades.py`** — DTCC SDR
- Download daily SDR file from DTCC public FTP
- Read with Spark, infer schema
- Write as-is to `risklens_bronze.trades` partitioned by date

**`bronze_rates.py`** — FRED API
- Fetch series: DFF (Fed Funds), DGS10 (10Y Treasury), BAMLH0A0HYM2 (HY Spread), DEXUSEU (EUR/USD)
- Write to `risklens_bronze.rates` partitioned by date

**`bronze_prices.py`** — Yahoo Finance
- Fetch daily OHLCV for basket of instruments matching DTCC trades
- Write to `risklens_bronze.prices` partitioned by date

**`bronze_synthetic.py`**
- Run synthetic generator
- Write all synthetic assets to `risklens_bronze.risk_outputs`, `risklens_bronze.pipeline_logs`, `risklens_bronze.ownership`

### Step 2.2 — Silver Job (Spark on Dataproc)

**`silver_transform.py`**
- Null checks — quarantine records with >10% nulls to `risklens_bronze.quarantine`
- Schema normalization — enforce standard column names across all sources
- Type casting — ensure correct data types
- Deduplication — remove duplicate records by primary key + date
- Write clean data to `risklens_silver.*`
- Write quality scores to `risklens_catalog.quality_scores`

### Step 2.3 — Gold Job (Spark on Dataproc)

**`gold_aggregate.py`**
- Join trades + prices + rates → enriched trade positions
- Calculate synthetic VaR/ES from enriched positions
- Build P&L attribution by desk
- Write to `risklens_gold.*`
- Update `risklens_catalog.assets` with schema + freshness metadata
- Write lineage edges to `risklens_lineage.edges`

### Step 2.4 — Cloud Composer DAG

**`risklens_pipeline.py`**
```
bronze_trades >> bronze_rates >> bronze_prices >> bronze_synthetic
    >> silver_transform >> gold_aggregate >> trigger_indexing
```
Schedule: `0 6 * * *` (daily at 6am)

---

## Phase 3 — AI / RAG Layer (Days 6–10)

### Step 3.1 — Document Preparation
Build `indexing/chunker.py`:
- Load asset descriptions from `risklens_catalog.assets`
- Load schema docs from `risklens_catalog.schema_registry`
- Load FRED series descriptions from FRED API metadata
- Load synthetic pipeline documentation (markdown files)
- Use `RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)`
- Tag each chunk with `asset_id`, `source_type`, `domain`

### Step 3.2 — Embeddings
Build `indexing/embedder.py`:
- Use `CohereEmbeddings(model="embed-english-v3.0")` — cloud-portable
- Embed all chunks
- Write to `risklens_embeddings.chunks` + `risklens_embeddings.vectors`
- Create BigQuery vector index: `CREATE VECTOR INDEX ON risklens_embeddings.vectors (embedding) OPTIONS (index_type='IVF')`

### Step 3.3 — BM25 Index
Build `indexing/bm25_index.py`:
- Build BM25 index over asset names, table names, column names, descriptions
- Serialize and store index file in GCS: `gs://risklens-raw/indexes/bm25.pkl`

### Step 3.4 — RAG Pipeline
Build `api/rag/retriever.py`:
- `BigQueryVectorStore` retriever — top-5 semantic results
- `BM25Retriever` — top-5 keyword results
- `EnsembleRetriever([bm25, vector], weights=[0.4, 0.6])` — RRF fusion

Build `api/rag/pipeline.py` (LangChain LCEL):
```python
chain = retriever | format_docs | prompt | claude | StrOutputParser()
```

### Step 3.5 — LangGraph Agent
Build `api/rag/agent.py`:

```
Node 1: router      → classify query (lineage / catalog / governance / general)
Node 2: retriever   → EnsembleRetriever hybrid search
Node 3: augmenter   → fetch metadata from BigQuery based on retrieved assets
Node 4: synthesizer → Claude generates answer with context
Node 5: streamer    → yield tokens to FastAPI streaming response
```

LangSmith tracing enabled via `LANGCHAIN_TRACING_V2=true` env var.

---

## Phase 4 — Backend API (Days 10–13)

Build `api/main.py` with FastAPI:

### Endpoints

**`GET /api/catalog`**
- Query `risklens_catalog.assets` + `ownership` + `sla_status`
- Support filters: domain, layer, owner, freshness
- Return paginated asset list

**`GET /api/asset/{id}`**
- Join `assets` + `schema_registry` + `quality_scores` + `sla_status`
- Return full asset detail

**`GET /api/lineage/{id}`**
- Traverse `risklens_lineage.edges` from given node
- Return nodes + edges JSON for graph rendering

**`GET /api/governance`**
- Aggregate: total assets, stale count, ownership gaps, SLA breach count
- Return KPI metrics

**`GET /api/search`**
- Hybrid search via `EnsembleRetriever`
- Return ranked asset list with relevance score

**`POST /api/chat`**
- Accept `{ "query": "..." }`
- Run LangGraph agent
- Return `StreamingResponse` — tokens streamed as Server-Sent Events

### Dockerfile (API)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Phase 5 — Frontend (Days 13–17)

Build React app with 4 views + persistent chat panel.

### Components

**`App.jsx`**
- Tab navigation: Catalog | Lineage | Governance | Assets
- Chat panel always rendered on right side (fixed width: 380px)

**`CatalogView.jsx`**
- Search bar (calls `/api/search`)
- Filter dropdowns: domain, layer, owner, freshness
- Asset grid/table with freshness badge (green/amber/red)
- Click row → navigate to asset detail

**`LineageGraph.jsx`**
- Use `react-flow` library for interactive graph
- Fetch `/api/lineage/{id}` on asset selection
- Nodes colored by type: source (blue), pipeline (orange), table (green), report (purple)
- Click any node to re-center lineage on that node

**`GovernanceView.jsx`**
- KPI cards: Total Assets, Stale Datasets, Missing Owners, SLA Breaches
- Table: SLA breach list with asset name, expected vs actual refresh
- Table: Ownership gaps — assets with no owner assigned

**`AssetsView.jsx`**
- Schema table: column name, type, nullable, sample value
- Quality metrics: null rate, schema drift flag, last checked
- Pipeline history: last 7 job runs with status badges

**`ChatPanel.jsx`**
- Fixed right panel, always visible
- Input box at bottom
- Streaming response — tokens appear as they arrive (SSE)
- Each answer cites source assets as clickable links

### Dockerfile (Frontend)
```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json .
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
```

---

## Phase 6 — GKE Deployment (Days 17–19)

### Kubernetes Manifests

**`api-deployment.yaml`**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: risklens-api
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: api
        image: gcr.io/risklens/api:latest
        ports:
        - containerPort: 8000
        env:
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: risklens-secrets
              key: anthropic-api-key
```

**`frontend-deployment.yaml`**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: risklens-frontend
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: frontend
        image: gcr.io/risklens/frontend:latest
        ports:
        - containerPort: 80
```

**`services.yaml`**
- LoadBalancer service for frontend (public IP)
- ClusterIP service for API (internal)
- Ingress routing `/api/*` → API, `/*` → frontend

### CI/CD
- GitHub Actions workflow:
  - On push to `main`: build Docker images → push to GCR → deploy to GKE

---

## Phase 7 — Polish & Public Launch (Days 19–21)

### README
- Project overview with architecture diagram
- Live demo link (GKE LoadBalancer IP)
- Local setup instructions (`docker-compose up`)
- Tech stack badges

### Demo Data
- Pre-load 30 days of synthetic data so the catalog has enough assets to look real
- At least 50 assets across 5 domains, 3 teams, multiple SLA states

### Local Dev
`docker-compose.yml` runs API + frontend locally against BigQuery (using service account key).

---

## Milestone Summary

| Phase | Days | Deliverable |
|---|---|---|
| 1 — Infrastructure | 1–3 | GCP setup, BigQuery schema, synthetic generator |
| 2 — Ingestion | 3–6 | Spark jobs, medallion pipeline, Composer DAG |
| 3 — AI / RAG | 6–10 | LangChain pipeline, LangGraph agent, embeddings |
| 4 — Backend API | 10–13 | FastAPI, all endpoints, Docker |
| 5 — Frontend | 13–17 | React app, 4 views + chat panel, Docker |
| 6 — GKE Deploy | 17–19 | Kubernetes manifests, CI/CD, live URL |
| 7 — Polish | 19–21 | README, demo data, local dev setup |

---

## Analytics & Usage Tracking

### Layer 1 — Google Analytics (who, where, when)

Add to `frontend/index.html`:
```html
<!-- Google Analytics -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-XXXXXXXXXX"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-XXXXXXXXXX');
</script>
```

Tracks automatically (zero backend changes):
- Country, city, browser, OS, device type
- Pages visited, session duration, bounce rate
- How they found the URL (direct, Google, LinkedIn, etc.)
- Number of unique vs returning visitors

Setup: Create free Google Analytics 4 property → copy Measurement ID → paste above.

---

### Layer 2 — BigQuery Access Log (what they did)

Add table to `risklens_catalog`:

```sql
access_log(
  event_id     STRING,
  page         STRING,      -- catalog / lineage / governance / assets / chat
  action       STRING,      -- page_view / asset_click / chat_query / search
  detail       STRING,      -- asset_id, query text, search term
  ip_address   STRING,
  country      STRING,
  city         STRING,
  browser      STRING,
  os           STRING,
  referrer     STRING,      -- how they found the URL
  session_id   STRING,
  timestamp    TIMESTAMP
)
```

Add FastAPI middleware in `api/main.py` — logs every request automatically:

```python
@app.middleware("http")
async def log_requests(request: Request, call_next):
    response = await call_next(request)
    await bigquery_client.log_access(
        page=request.url.path,
        ip_address=request.client.host,
        referrer=request.headers.get("referer"),
        user_agent=request.headers.get("user-agent"),
        timestamp=datetime.utcnow()
    )
    return response
```

No frontend changes needed. Every API call — catalog browse, chat query, lineage view — is logged.

---

### What You Can See

| Question | Source |
|---|---|
| How many people visited today? | Google Analytics |
| Which country are they from? | Google Analytics |
| What browser/OS? | Google Analytics |
| How long did they stay? | Google Analytics |
| Which assets did they view? | BigQuery access_log |
| What did they ask the chat? | BigQuery access_log |
| Which view is most used? | BigQuery access_log |
| How many Claude API calls today? | BigQuery access_log |

---

## Security

### GCS Buckets — No Public Access
- All buckets created with `--no-public-access` (uniform bucket-level access enforced)
- No `allUsers` or `allAuthenticatedUsers` IAM bindings ever
- Bucket access only via service account with `roles/storage.objectViewer` or `roles/storage.objectAdmin`
- Enable audit logging on all buckets

### BigQuery — Private Datasets
- All datasets private by default — no public sharing
- Access only via service account with `roles/bigquery.dataViewer` (API) and `roles/bigquery.dataEditor` (Spark jobs)
- Never expose BigQuery credentials in code — use Workload Identity on GKE

### Secrets Management — GCP Secret Manager
All sensitive values stored in **GCP Secret Manager**, never in code or environment files:

| Secret | Key |
|---|---|
| Anthropic API key | `risklens/anthropic-api-key` |
| Cohere API key | `risklens/cohere-api-key` |
| FRED API key | `risklens/fred-api-key` |
| GCP service account key | `risklens/gcp-sa-key` |

Kubernetes pods access secrets via GCP Workload Identity — no static keys mounted in pods.

### GKE — Least Privilege
- Private GKE cluster — nodes have no public IPs
- Workload Identity enabled — pods authenticate to GCP APIs without service account key files
- API service: `ClusterIP` only (not exposed directly) — traffic goes through Ingress
- Frontend: `LoadBalancer` with HTTPS only (SSL cert via GCP-managed certificate)
- Network policy: API pod only accepts traffic from frontend pod

### FastAPI — API Security
- CORS restricted to frontend domain only (no wildcard `*`)
- Rate limiting on `/api/chat` endpoint (prevent abuse of Claude API)
- Input validation on all endpoints via Pydantic models
- No stack traces exposed in error responses (production mode)

### Docker Images — No Secrets Baked In
- `.dockerignore` excludes: `.env`, `*.json` key files, `secrets/`
- No `ENV` instructions for secrets in Dockerfiles
- Images scanned for secrets before push (add `gitleaks` to CI/CD)

### Repository — No Secrets in Git
- `.gitignore` includes: `.env`, `*.json` (service account keys), `secrets/`
- Add `gitleaks` pre-commit hook to block accidental secret commits

---

## Key Dependencies

```
# Python (API + ingestion)
langchain
langchain-anthropic
langchain-community
langchain-google-bigquery
langgraph
langsmith
fastapi
uvicorn
pyspark
google-cloud-bigquery
google-cloud-storage
cohere
rank-bm25
apache-airflow

# JavaScript (frontend)
react
react-flow
axios
tailwindcss
```
