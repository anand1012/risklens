# RiskLens — Design Specification
**FRTB Data Catalog & AI Lineage Explorer**

**Date:** 2026-04-13  
**Author:** Anand Swaroop  
**Status:** Approved

---

## 1. Problem Statement

Financial firms running FRTB-IMA programs have hundreds of data assets scattered across teams — trade book tables, market data feeds, risk model outputs, and regulatory reports. Nobody has a single view of:
- What data exists and where it lives
- Whether it is fresh, accurate, and trustworthy
- What feeds what — if an upstream source breaks, what downstream reports break?
- Who owns each dataset and who is accountable

Today this knowledge lives in people's heads and scattered wikis. When a regulator asks "show me the lineage of your VaR number" — it takes weeks to answer.

**RiskLens solves this** — a GCP-native data catalog for FRTB regulatory data with AI-powered lineage tracing and natural language search.

---

## 2. Target Users

| Persona | Primary need |
|---|---|
| **Data Analyst / Quant** | Find datasets fast, understand schema and freshness |
| **Data Engineer** | Trace lineage, diagnose pipeline failures, monitor quality |
| **CDO / Head of Data** | Governance dashboard, audit trail, SLA compliance |

---

## 3. Project Name

**RiskLens** — Intelligent Data Catalog for Capital Markets

---

## 4. Architecture Overview

RiskLens has 5 layers:

```
LAYER 1 — DATA SOURCES
  External: DTCC SDR (trades), FRED API (rates/macro), Yahoo Finance (prices)
  Synthetic: VaR/ES/P&L outputs, pipeline logs, ownership registry, quality scores

LAYER 2 — INGESTION
  Spark on Dataproc → Medallion architecture (Bronze → Silver → Gold)
  Scheduled via Cloud Composer (Airflow)
  Raw files land in GCS before BigQuery

LAYER 3 — STORAGE
  GCS: raw landing zone
  BigQuery: medallion datasets + catalog metadata + lineage graph + embeddings

LAYER 4 — AI LAYER
  LangChain (document loading, chunking, embedding, RAG) + LangGraph (agents) + LangSmith (observability)
  Claude (Anthropic) as LLM — cloud-portable, not locked to GCP

LAYER 5 — APPLICATION
  FastAPI backend + React frontend
  Both containerized (Docker), deployed on GKE
```

---

## 5. Data Sources

### 5.1 External Data (real, public)

| Source | Data | Cadence | Why ingested |
|---|---|---|---|
| **DTCC SDR** | OTC derivatives trade data | Daily | Real trade book — instruments, notionals, desks |
| **FRED API** | Interest rates, FX, CPI, credit spreads | Daily | Risk models consume macro market data for VaR/ES |
| **Yahoo Finance** | Prices, volatility surfaces | Daily | Market data for risk output calculations |

### 5.2 Internal Data (synthetic, generated)

| Asset | Description |
|---|---|
| Trade book tables | Normalized from DTCC SDR — desk, instrument, notional, book |
| Market data tables | Prices and vol surfaces derived from Yahoo Finance |
| Risk output tables | VaR, Expected Shortfall, P&L vectors per desk (calculated synthetically from real inputs) |
| Reference data | Instrument master, counterparty master, currency master |
| Spark pipeline logs | Job run history — status, rows processed, duration, errors |
| Data quality results | Null rates, schema drift, freshness SLA pass/fail per table |
| Dataset ownership registry | Owner, team, steward, SLA, classification per asset |
| dbt model definitions | SQL transformation definitions from raw → clean risk tables |

---

## 6. Ingestion Layer — Spark on Dataproc

### 6.1 Medallion Architecture

```
SOURCE → GCS (raw) → Bronze (BigQuery) → Silver (BigQuery) → Gold (BigQuery)
```

| Layer | Spark Job | Responsibility |
|---|---|---|
| **Bronze** | Job 1 | Land raw data as-is. No transforms. Immutable. Schema auto-detected. |
| **Silver** | Job 2 | Clean nulls, normalize schemas, validate types, deduplicate, quarantine bad records |
| **Gold** | Job 3 | Aggregate, enrich, join across sources. Business-ready tables for catalog + AI layer |

### 6.2 BigQuery Datasets

```
risklens_bronze      ← raw landing zone
risklens_silver      ← cleaned, validated
risklens_gold        ← business-ready assets
risklens_catalog     ← metadata, ownership, SLA, quality scores
risklens_lineage     ← lineage graph (nodes + edges)
risklens_embeddings  ← chunked text + vector embeddings for RAG
```

### 6.3 GCS Structure

```
gs://risklens-raw/
  ├── dtcc/YYYY-MM-DD/
  ├── fred/YYYY-MM-DD/
  ├── yahoo/YYYY-MM-DD/
  └── synthetic/YYYY-MM-DD/
```

### 6.4 Scheduling

Cloud Composer (managed Airflow) triggers jobs sequentially:
`Bronze → Silver → Gold → Index update`

Each source runs daily. Synthetic risk outputs trigger after trades + market data land.

---

## 7. Storage Layer — BigQuery

### 7.1 Catalog Metadata (`risklens_catalog`)

| Table | Key Columns | Powers |
|---|---|---|
| `assets` | asset_id, name, type, domain, layer, description, tags | Catalog browser |
| `ownership` | asset_id, owner_name, team, steward, email | Governance view |
| `quality_scores` | asset_id, null_rate, schema_drift, freshness_status, last_checked | Governance view |
| `sla_status` | asset_id, expected_refresh, actual_refresh, breach_flag | Governance dashboard |
| `schema_registry` | asset_id, column_name, data_type, nullable, sample_value | Asset detail page |

### 7.2 Lineage Graph (`risklens_lineage`)

| Table | Key Columns | Powers |
|---|---|---|
| `nodes` | node_id, name, type (source/pipeline/table/report) | Lineage graph nodes |
| `edges` | from_node_id, to_node_id, relationship, pipeline_job | Lineage graph edges |

Example lineage chain:
```
DTCC SDR → bronze_trades → silver_trades → gold_risk_assets → VaR_output → Regulatory Report
```

### 7.3 Embeddings (`risklens_embeddings`)

| Table | Key Columns | Powers |
|---|---|---|
| `chunks` | chunk_id, asset_id, text, source_type, created_at | RAG retrieval |
| `vectors` | chunk_id, embedding ARRAY<FLOAT64> | BigQuery ANN vector search |

Text chunks sourced from: asset descriptions, schema docs, FRED series descriptions, SEC filing sections, synthetic pipeline documentation.

---

## 8. AI / RAG Layer

### 8.1 Design Principle

**All heavy work happens at index time, not query time.**

| Index Time (offline, post-ingestion) | Query Time (real-time) |
|---|---|
| Chunk documents | Embed query |
| Build hierarchical index | Hybrid search (BM25 + semantic) |
| Pre-compute embeddings → BigQuery | Rerank top-k |
| Build BM25 keyword index | LangGraph agent synthesizes answer |
| | Stream tokens to UI |

### 8.2 Tool Roles

| Tool | Role |
|---|---|
| **LangChain DocumentLoaders** | Load documents — schema docs, FRED descriptions, pipeline docs |
| **LangChain TextSplitter** | Chunk text — `RecursiveCharacterTextSplitter` for hierarchical chunking |
| **LangChain Embeddings** | Convert chunks to vectors (Cohere or OpenAI embeddings — cloud-portable) |
| **LangChain BigQueryVectorStore** | Store + retrieve embeddings natively from BigQuery |
| **LangChain EnsembleRetriever** | Hybrid search — BM25 (keyword) + semantic (vector) with RRF fusion |
| **LangChain LCEL** | Compose RAG pipeline — retrieval → prompt → Claude |
| **LangGraph** | Multi-step agentic queries requiring multiple retrieval steps |
| **LangSmith** | Trace every query — latency per step, retrieval quality, token usage |
| **Claude (Anthropic)** | Final answer synthesis with streaming response |

### 8.3 LangGraph Agent Flow

```
User query
    │
    ▼
Step 1: Router       → classify query type (lineage / catalog / governance / general)
Step 2: Retriever    → hybrid search (BM25 + BigQuery Vector ANN)
Step 3: Augmenter    → fetch supporting metadata from BigQuery
Step 4: Synthesizer  → Claude assembles final answer
Step 5: Streamer     → tokens stream to React UI in real time
```

### 8.5 Query Latency Targets

| Scenario | Target |
|---|---|
| Simple asset lookup | < 500ms |
| Multi-step lineage query | < 2s |
| Complex cross-asset analysis | < 4s |

---

## 9. Backend API — FastAPI on GKE

| Endpoint | Description |
|---|---|
| `GET /api/catalog` | List/search all assets |
| `GET /api/asset/{id}` | Asset detail — schema, owner, quality, pipeline history |
| `GET /api/lineage/{id}` | Lineage graph nodes + edges for an asset |
| `GET /api/governance` | Dashboard metrics — stale assets, SLA breaches, ownership gaps |
| `POST /api/chat` | Streaming RAG endpoint (LangGraph → Claude) |
| `GET /api/search` | Hybrid search across all assets |

All endpoints containerized in Docker, deployed as GKE services.

---

## 10. Frontend — React on GKE

### 10.1 Layout

```
┌─────────────────────────────────────────────┬──────────────┐
│  [Catalog] [Lineage] [Governance] [Assets]  │              │
├─────────────────────────────────────────────│   AI Chat    │
│                                             │   Panel      │
│   Main view renders here                   │   (always    │
│   based on selected tab                    │   visible)   │
│                                             │              │
│                                             │   Powered by │
│                                             │   Claude     │
└─────────────────────────────────────────────┴──────────────┘
```

### 10.2 Views

| View | What user sees |
|---|---|
| **Catalog** | Searchable grid of all assets — name, domain, layer, owner, freshness badge |
| **Lineage** | Interactive graph — click any node to trace upstream/downstream |
| **Governance** | KPI cards: total assets, stale count, ownership gaps, SLA breach list |
| **Assets** | Drill-down detail — schema, quality scores, sample data, pipeline history |
| **Chat (persistent)** | Ask anything — answers stream in real time, cites source assets |

---

## 11. End-to-End Data Flow

```
1. INGEST (daily, Cloud Composer)
   DTCC + FRED + Yahoo + Synthetic
   → GCS raw zone
   → Spark: Bronze → Silver → Gold (BigQuery)
   → Catalog metadata updated
   → Lineage edges updated
   → Quality scores computed

2. INDEX (triggered post-ingestion)
   Gold tables + schema docs + text
   → LangChain DocumentLoaders + TextSplitter chunks documents
   → LangChain Embeddings → stored in BigQuery (risklens_embeddings)
   → BM25 index built over asset metadata

3. QUERY (real-time)
   User types in chat panel
   → FastAPI /api/chat
   → LangGraph: classify → retrieve → augment → synthesize
   → Claude streams answer
   → LangSmith traces call
```

---

## 12. Full Technology Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Spark on Dataproc |
| Orchestration | Cloud Composer (Airflow) |
| Raw Storage | GCS |
| Analytical Storage | BigQuery (medallion architecture) |
| Vector Search | BigQuery native ANN (IVF) |
| RAG Orchestration | LangChain (loaders, splitters, embeddings, retrievers, LCEL) |
| Agentic Workflows | LangGraph |
| Observability | LangSmith |
| LLM | Claude (Anthropic) |
| Backend API | FastAPI |
| Frontend | React |
| Containerization | Docker |
| Container Orchestration | Kubernetes (GKE) |
| External Data | DTCC SDR, FRED API, Yahoo Finance |

---

## 13. Interview Positioning

> "I spent 3 years building FRTB data pipelines at Citi. The governance gap was real — lineage was always manual, audit prep was painful. RiskLens is what I'd show a Head of Risk at a bank to explain why they need Atlan. I built the problem that Atlan solves."

This project demonstrates:
- Deep capital markets domain expertise (FRTB, VaR, ES, trade lineage)
- Full GCP data engineering stack (Dataproc, BigQuery, GCS, GKE, Composer)
- Modern AI engineering (RAG, agents, LangChain, LangGraph, LangSmith)
- Cloud-portable AI layer (Anthropic Claude — not locked to GCP)
- End-to-end product thinking (catalog + lineage + governance + AI in one app)
