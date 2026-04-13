# RiskLens
### FRTB Data Catalog & AI Lineage Explorer

> An intelligent data catalog for capital markets — built to solve the exact governance problem that Atlan solves in regulated financial institutions.

[![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataproc%20%7C%20GKE-4285F4?logo=googlecloud)](https://cloud.google.com)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)](https://spark.apache.org)
[![LangChain](https://img.shields.io/badge/LangChain%20%7C%20LangGraph%20%7C%20LangSmith-1C3C3C)](https://langchain.com)
[![Claude](https://img.shields.io/badge/LLM-Claude%20(Anthropic)-8B5CF6)](https://anthropic.com)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://python.org)

---

## The Problem

Financial firms running FRTB-IMA programs have hundreds of data assets scattered across teams — trade book tables, market data feeds, risk model outputs, and regulatory reports. Nobody has a single view of:

- **What data exists** and where it lives
- **Whether it's fresh and trustworthy** — or stale and broken
- **What feeds what** — if an upstream source breaks, which downstream reports fail?
- **Who owns each dataset** — and who is accountable to the regulator

When a regulator asks *"show me the full lineage of your VaR number"* — it takes weeks to answer manually. RiskLens makes it instant.

---

## What RiskLens Does

| Feature | Description |
|---|---|
| **Data Catalog** | Discover any dataset across your FRTB data estate — search by name, domain, owner, or freshness |
| **Lineage Graph** | Interactive visual trace from raw source → pipeline → table → regulatory report |
| **Governance Dashboard** | SLA breach alerts, ownership gaps, data quality scores across all assets |
| **AI Chat (Claude)** | Ask anything in natural language — *"Which tables feed the ES calculation for the Rates desk?"* |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                   │
│  DTCC SDR (trades) │ FRED API (rates) │ Yahoo Finance (prices) │
│  Synthetic: VaR/ES/P&L, pipeline logs, ownership, quality      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  INGESTION — Apache Spark on GCP Dataproc                      │
│  Bronze (raw) → Silver (cleaned) → Gold (business-ready)       │
│  Orchestrated via Cloud Composer (Airflow) for initial load     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  STORAGE — Google BigQuery (Medallion Architecture)            │
│  risklens_bronze │ risklens_silver │ risklens_gold              │
│  risklens_catalog │ risklens_lineage │ risklens_embeddings      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  AI LAYER — LangChain + LangGraph + LangSmith + Claude         │
│  RAG pipeline │ Hybrid search (BM25 + Vector) │ Agents         │
│  Cloud-portable — not locked to GCP                            │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  APPLICATION — Docker + Kubernetes (GKE)                       │
│  FastAPI backend │ React frontend │ Persistent AI chat panel   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Spark 3.5 on GCP Dataproc |
| Orchestration | Cloud Composer (Airflow) for initial load, Cloud Scheduler for refresh |
| Raw Storage | Google Cloud Storage (GCS) |
| Analytical Storage | BigQuery — Medallion architecture (Bronze/Silver/Gold) |
| Vector Search | BigQuery native ANN (IVF index) |
| RAG Orchestration | LangChain (LCEL, EnsembleRetriever, BM25 + semantic) |
| Agentic Workflows | LangGraph — multi-step query routing |
| LLM Observability | LangSmith |
| LLM | Claude (Anthropic) — cloud-portable |
| Backend API | FastAPI |
| Frontend | React |
| Containerization | Docker |
| Container Orchestration | Kubernetes (GKE) |
| Secrets | GCP Secret Manager |
| Analytics | Google Analytics 4 + BigQuery access log |

---

## Data Sources

### External (real, public)

| Source | Data | Why |
|---|---|---|
| [DTCC SDR](https://pddata.dtcc.com/gtr/cftc/) | OTC derivatives trade data (daily) | Real trade book — instruments, notionals, desks |
| [FRED API](https://fred.stlouisfed.org/) | Interest rates, FX, CPI, credit spreads (daily) | Risk models consume macro market data for VaR/ES |
| [Yahoo Finance](https://finance.yahoo.com/) | Equity prices, commodity futures, FX spot (daily) | Market data for risk output calculations |

### Internal (synthetic, FRTB-realistic)

| Asset | Description |
|---|---|
| Trade book | 5 desks (Rates, FX, Credit, Equities, Commodities), 10K trades/day |
| Risk outputs | VaR (99%), Expected Shortfall (97.5%), P&L vectors (100 scenarios) |
| Reference data | Instrument master, counterparty master, currency reference |
| Pipeline logs | Spark job run history — status, rows, duration, errors |
| Quality scores | Null rates, schema drift, freshness SLA per dataset |
| Ownership registry | Owner, team, steward, SLA per asset |
| Lineage graph | Full node/edge graph from source → report |

---

## Project Structure

```
risklens/
├── ingestion/
│   ├── jobs/
│   │   ├── bronze_trades.py       ← DTCC SDR → BigQuery bronze
│   │   ├── bronze_rates.py        ← FRED API → BigQuery bronze
│   │   ├── bronze_prices.py       ← Yahoo Finance → BigQuery bronze
│   │   ├── bronze_synthetic.py    ← Synthetic data → BigQuery bronze
│   │   ├── silver_transform.py    ← Bronze → Silver (clean + validate)
│   │   └── gold_aggregate.py      ← Silver → Gold (business-ready)
│   ├── dags/
│   │   └── risklens_pipeline.py   ← Cloud Composer DAG
│   └── synthetic/
│       └── generate.py            ← Synthetic FRTB data generator
├── indexing/
│   ├── chunker.py                 ← LangChain document chunking
│   ├── embedder.py                ← LangChain embeddings → BigQuery
│   └── bm25_index.py              ← BM25 keyword index
├── api/
│   ├── main.py                    ← FastAPI application
│   ├── routers/                   ← catalog, lineage, governance, chat
│   ├── rag/                       ← LangChain pipeline + LangGraph agent
│   └── db/                        ← BigQuery client
├── frontend/
│   └── src/components/            ← React views + persistent chat panel
├── infra/
│   └── k8s/                       ← Kubernetes manifests
├── scripts/
│   ├── setup_gcp.sh               ← One-time GCP infrastructure setup
│   ├── setup_bigquery.py          ← BigQuery schema creation
│   ├── setup_composer.sh          ← Initial data load via Composer
│   └── refresh_data.sh            ← Manual data refresh via Dataproc
└── docs/
    ├── design.md                  ← Architecture design spec
    └── implementation-plan.md     ← Phase-by-phase build plan
```

---

## Getting Started

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI authenticated
- Python 3.11+
- Node 20+
- Docker

### 1. GCP Infrastructure Setup
```bash
chmod +x scripts/setup_gcp.sh
./scripts/setup_gcp.sh YOUR_GCP_PROJECT_ID
```

### 2. Add API Keys to Secret Manager
```bash
echo -n 'YOUR_ANTHROPIC_KEY' | gcloud secrets versions add risklens-anthropic-api-key --data-file=-
echo -n 'YOUR_COHERE_KEY'    | gcloud secrets versions add risklens-cohere-api-key --data-file=-
echo -n 'YOUR_FRED_KEY'      | gcloud secrets versions add risklens-fred-api-key --data-file=-
```

### 3. Create BigQuery Schema
```bash
pip install -r requirements.txt
python scripts/setup_bigquery.py --project YOUR_GCP_PROJECT_ID
```

### 4. Initial Data Load (Composer auto-deletes after run)
```bash
chmod +x scripts/setup_composer.sh
./scripts/setup_composer.sh YOUR_GCP_PROJECT_ID
```

### 5. Future Data Refreshes
```bash
./scripts/refresh_data.sh YOUR_GCP_PROJECT_ID --days 1
```

### Local Development
```bash
docker-compose up   # API + frontend against BigQuery
```

---

## Cost Profile

| Service | Usage | Cost |
|---|---|---|
| BigQuery | ~500MB storage, ~50GB queries/month | **~$0** (free tier) |
| GCS | ~1GB raw files | **~$0** (free tier) |
| GKE | 1 node, e2-standard-2 | **~$50/month** |
| Dataproc | Ephemeral — spun up per refresh, torn down after | **~$0.50/refresh** |
| Cloud Composer | Initial load only — deleted after | **~$5-10 one-time** |
| Secret Manager | 4 secrets | **~$0** (free tier) |

> GKE is the primary ongoing cost. Shut down the cluster when not demoing to save credits.
> ```bash
> gcloud container clusters resize risklens-cluster --num-nodes=0 --region=us-central1
> ```

---

## Background

Built as a portfolio project to demonstrate the intersection of:
- **15+ years of capital markets data engineering** (FRTB-IMA at Citi, data platforms at BofA/JPMorgan)
- **Modern AI engineering** — RAG, agents, LangGraph, LangSmith
- **Full GCP stack** — certified GCP Professional Data Engineer

> *"I spent 3 years building FRTB data pipelines at Citi. The governance gap was real — lineage was always manual, audit prep was painful. RiskLens is what I'd show a Head of Risk at a bank to explain why they need a data catalog."*

---

## License

MIT
