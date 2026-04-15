# RiskLens
### FRTB Data Catalog & AI Lineage Explorer

> An intelligent data catalog for capital markets — built to solve the exact governance problem that Atlan solves in regulated financial institutions.

**Live:** http://34.102.203.211 · [https://risklens.preciseguess.com](https://risklens.preciseguess.com) *(pending DNS)*

[![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20GKE-4285F4?logo=googlecloud)](https://cloud.google.com)
[![LangChain](https://img.shields.io/badge/LangChain%20%7C%20LangGraph%20%7C%20LangSmith-1C3C3C)](https://langchain.com)
[![Claude](https://img.shields.io/badge/LLM-Claude%20(Anthropic)-8B5CF6)](https://anthropic.com)
[![React](https://img.shields.io/badge/Frontend-React%20%7C%20Vite%20%7C%20Tailwind-61DAFB?logo=react)](https://react.dev)
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
| **Data Catalog** | Discover all FRTB assets across the data estate — search, filter by domain/layer/type, click any asset to see schema, quality, SLA, and ownership |
| **Lineage Graph** | Interactive DAG showing the full pipeline from raw source to regulatory report — dagre auto-layout (LR flow). Click any arrow for a business-language transformation story. |
| **Governance Dashboard** | SLA breach alerts, data quality scores (freshness, null rate, schema drift), and ownership registry across all assets |
| **FRTB IMA Dashboard** | ES 97.5%, back-testing traffic lights (GREEN/AMBER/RED), PLAT (UPL, Spearman, KS), RFET eligibility, capital charge — all BCBS 457 metrics live |
| **Gold Assets View** | Card-based view of all business-ready (gold layer) tables with key metrics |
| **AI Chat (Claude)** | Ask anything in natural language — *"Which tables feed the ES calculation?"* — powered by hybrid BM25 + vector RAG with source citations, rendered with Markdown |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                   │
│  DTCC SDR (trades) │ FRED API (rates) │ Yahoo Finance (prices) │
│  Synthetic: VaR/ES/P&L, ownership, quality, SLA                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  STORAGE — Google BigQuery (Medallion Architecture)            │
│  risklens_bronze │ risklens_silver │ risklens_gold              │
│  risklens_catalog │ risklens_lineage │ risklens_embeddings      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  AI LAYER — LangChain + LangSmith + Claude (Anthropic)         │
│  Hybrid RAG (BM25 + Vertex AI text-embedding-004)              │
│  109 chunks indexed: asset, schema, pipeline, UI/workflow docs │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│  APPLICATION — Docker + Kubernetes (GKE Standard)              │
│  FastAPI (Python) │ React + Vite + Tailwind │ Nginx             │
│  GKE single-zone us-central1-a, e2-medium, Recreate strategy  │
│  Cloudflare CDN + HTTPS │ GitHub Actions CI/CD                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Analytical Storage | BigQuery — Medallion architecture (Bronze/Silver/Gold) |
| Vector Embeddings | Vertex AI `text-embedding-004` |
| RAG Orchestration | LangChain 0.3 (EnsembleRetriever: BM25 + semantic hybrid) |
| Agentic Workflows | LangGraph — multi-step query routing |
| LLM Observability | LangSmith |
| LLM | Claude Sonnet 4.6 + Claude Haiku 4.5 (Anthropic) |
| Backend API | FastAPI + Uvicorn |
| Frontend | React 18, Vite, Tailwind CSS, TanStack Query |
| Lineage Graph | ReactFlow (`@xyflow/react`) + Dagre auto-layout |
| Containerization | Docker (multi-stage builds) |
| Container Orchestration | GKE Standard, single-zone (`us-central1-a`), e2-medium |
| Secrets | GCP Secret Manager + Workload Identity |
| CI/CD | GitHub Actions — build, push to Artifact Registry, rollout |
| CDN / HTTPS | Cloudflare (auto-purge on deploy) |

---

## Key UI Features

### Data Catalog
- 31 FRTB assets with domain, layer, owner, freshness, SLA, and row count
- Click any row to open a detail drawer with schema, quality metrics, and ownership
- Filter by domain (risk / market_data / reference / regulatory) and layer (bronze / silver / gold)

### Lineage Graph
- **Dagre LR auto-layout** — nodes ranked left-to-right by pipeline stage, no edge crossings
- **Custom ReactFlow nodes** with left/right handles for clean horizontal arrows
- **Clickable edges** — click any arrow to see a business-language transformation story:
  - What the transformation does
  - Business impact (FRTB capital implications)
  - Frequency (when it runs)
  - Owner (which team is responsible)
- Adjustable hop radius (1–4) to explore upstream/downstream dependencies

### AI Chat
- Streaming responses via SSE with 3-dot animation while waiting
- Hybrid retrieval: BM25 keyword + Vertex AI vector search over 109 indexed chunks
- Source cards show which assets each answer was drawn from
- Suggestion buttons auto-send on click (no need to press Enter)

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
| Trade book | 5 desks (Rates, FX, Credit, Equities, Commodities) |
| Risk outputs | VaR (99%), Expected Shortfall (97.5%), P&L vectors (100 scenarios) |
| Reference data | Instrument master, counterparty master, currency reference |
| Quality scores | Null rates, schema drift, freshness SLA per dataset |
| Ownership registry | Owner, team, steward, SLA per asset |
| Lineage graph | Full node/edge DAG from source API → regulatory report |

---

## Project Structure

```
risklens/
├── ingestion/
│   ├── jobs/
│   │   ├── bronze_trades.py       ← DTCC SDR → BigQuery bronze
│   │   ├── bronze_rates.py        ← FRED API → BigQuery bronze
│   │   ├── bronze_prices.py       ← Yahoo Finance → BigQuery bronze
│   │   ├── bronze_synthetic.py    ← Synthetic FRTB data → bronze
│   │   ├── silver_transform.py    ← Bronze → Silver (clean + validate)
│   │   ├── silver_enrich.py       ← Silver → Silver (join + enrich positions)
│   │   └── gold_aggregate.py      ← Silver → Gold (FRTB IMA metrics)
│   └── synthetic/
│       └── generate.py            ← Synthetic FRTB data generator
├── indexing/
│   ├── chunker.py                 ← LangChain document chunking
│   ├── embedder.py                ← Vertex AI text-embedding-004 → BigQuery
│   ├── bm25_index.py              ← BM25 keyword index builder
│   └── run_indexing.py            ← Build RAG index (BM25 + embeddings)
├── api/
│   ├── main.py                    ← FastAPI app + router registration
│   ├── routers/
│   │   ├── catalog.py             ← Asset list + detail + schema endpoints
│   │   ├── lineage.py             ← DAG graph + transformation stories
│   │   ├── governance.py          ← SLA, quality, ownership endpoints
│   │   ├── search.py              ← Hybrid semantic search
│   │   └── chat.py                ← SSE streaming chat endpoint
│   ├── rag/                       ← LangChain RAG + LangGraph agent
│   └── db/bigquery.py             ← Shared BigQuery client
├── frontend/
│   └── src/
│       ├── views/
│       │   ├── Catalog.tsx        ← Asset list + detail drawer
│       │   ├── Lineage.tsx        ← ReactFlow + dagre + edge stories
│       │   ├── Governance.tsx     ← SLA / Quality / Ownership tabs
│       │   └── Assets.tsx         ← Gold layer card grid
│       ├── components/
│       │   ├── Layout.tsx         ← Sidebar + persistent chat button
│       │   ├── ChatPanel.tsx      ← Streaming AI chat panel
│       │   └── Badges.tsx         ← Layer / domain / freshness badges
│       └── api.ts                 ← All API calls + SSE stream handler
├── infra/
│   ├── k8s/                       ← Kubernetes manifests
│   │   ├── api-deployment.yaml
│   │   ├── frontend-deployment.yaml
│   │   ├── ingress.yaml
│   │   └── namespace.yaml
│   └── setup_gke.sh               ← GKE cluster setup script
├── .github/workflows/ci.yml       ← GitHub Actions: build → push → deploy
└── scripts/
    ├── setup_gcp.sh               ← One-time GCP infra setup
    ├── setup_bigquery.py          ← BigQuery schema creation
    ├── refresh_data.sh            ← End-to-end pipeline refresh
    └── dump_table_stats.py        ← Snapshot BQ table stats → docs/table_stats.json
```

---

## Cost Profile

| Service | Usage | Cost |
|---|---|---|
| BigQuery | ~500MB storage, ~50GB queries/month | **~$0** (free tier) |
| GCS | ~1GB raw files | **~$0** (free tier) |
| GKE | 1 node, e2-medium, single-zone | **~$25/month** |
| Vertex AI | Embedding calls during indexing only | **~$0.01/re-index** |
| Secret Manager | 4 secrets | **~$0** (free tier) |
| Cloudflare | Free tier (CDN + HTTPS) | **$0** |

> GKE is the primary ongoing cost. Scale to zero when not demoing:
> ```bash
> gcloud container clusters resize risklens-cluster --num-nodes=0 --zone=us-central1-a
> ```

---

## Local Development

```bash
# Backend
pip install -r requirements.txt
uvicorn api.main:app --reload

# Frontend
cd frontend
npm install
npm run dev
```

Or run both with Docker:
```bash
docker-compose up
```

---

## Background

Built as a portfolio project to demonstrate the intersection of:
- **Capital markets domain knowledge** — FRTB-IMA data pipelines, risk data governance, BCBS 239
- **Modern AI engineering** — RAG, hybrid retrieval, LangGraph, LangSmith observability
- **Full GCP production stack** — GKE, BigQuery, Vertex AI, Secret Manager, Workload Identity
- **Product thinking** — lineage graphs that tell a business story, not just SQL

> *"I spent years building FRTB data pipelines. The governance gap was real — lineage was always manual, audit prep was painful. RiskLens is what I'd show a Head of Risk at a bank to explain why they need a data catalog."*

---

## License

MIT
