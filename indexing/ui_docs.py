"""
RiskLens — UI Documentation Chunks
Describes every screen, component, control, and interaction in the RiskLens
application so the AI assistant can answer user questions about the UI.

Each chunk covers one topic with full context: what it is, where it lives,
what it reveals, what to look for, and how it connects to FRTB compliance.
These are fed into the BM25 + vector index at indexing time (source_type = "ui_doc").
"""

from indexing.chunker import ChunkDoc, _chunk_id

_DOMAIN = "ui"
_SOURCE = "ui_doc"


def _doc(topic: str, text: str) -> ChunkDoc:
    return ChunkDoc(
        chunk_id=_chunk_id(text),
        asset_id=f"ui_{topic}",
        text=text,
        source_type=_SOURCE,
        domain=_DOMAIN,
        metadata={"topic": topic},
    )


UI_CHUNKS: list[ChunkDoc] = [

    # ── Overall application ───────────────────────────────────────────────────

    _doc("overview", """
RiskLens Application — What It Is and Why It Exists

RiskLens is a browser-based FRTB (Fundamental Review of the Trading Book) Data Catalog
and AI Lineage Explorer built for quantitative risk analysts, data engineers, and
compliance officers at trading firms.

The core problem it solves: under BCBS 457 (FRTB), regulators require firms to prove
their internal models use only eligible, observable market data (RFET), that those
models accurately predict P&L (PLAT), and that capital charges are correctly computed
(ES 97.5%, back-testing VaR 99%). RiskLens makes all of that auditable and visible
in one place — every data asset, every transformation step, every compliance metric.

The application has five main sections in the left sidebar:
  1. Catalog      — search and browse all 20+ data assets (tables, feeds, models)
  2. Governance   — SLA health, data quality scores, team ownership
  3. Assets       — card grid of gold-layer (business-ready) regulatory outputs
  4. Lineage      — interactive graph showing how data flows through the pipeline
  5. FRTB Risk    — live regulatory dashboard: ES, capital charge, back-testing, PLAT, RFET

The data pipeline has three medallion layers:
  Bronze — raw, immutable landing zone (FRED rates, synthetic risk outputs, trade feeds)
  Silver — cleaned, validated, enriched data (joins, quality checks, transformations)
  Gold   — aggregated regulatory outputs submitted to compliance (ES, capital, backtesting)

At the bottom of the sidebar is the "✦ Ask RiskLens AI" button which opens the AI
assistant — it has full knowledge of every asset, schema, lineage edge, and FRTB rule.

The sidebar can be collapsed to icon-only mode by clicking the ← → arrow at the top.
Hover any collapsed icon to see its label.
"""),

    _doc("chat_panel", """
AI Chat Panel — The AI Assistant Inside RiskLens

Location: Click "✦ Ask RiskLens AI" at the bottom of the left sidebar.
The panel slides in from the right (480px wide) and stays open as you navigate.

What the AI knows:
  • Every data asset in the catalog — schema, description, owner, quality metrics
  • The full data lineage — every pipeline edge from raw sources to gold outputs
  • Live BigQuery data — it can query actual values from gold tables (desks in red
    zone, current capital charges, which risk factors failed RFET, etc.)
  • FRTB regulations — BCBS 457 rules for ES, VaR, PLAT, RFET, traffic-light zones
  • The RiskLens UI — every tab, control, badge, and workflow

How to use it:
  • Type your question in the text box at the bottom and press Enter or click ↑
  • Shift+Enter adds a line break without sending
  • The AI streams its answer in real time, token by token
  • Below each answer, coloured source cards show which catalog/schema/lineage
    chunks were retrieved — you can see exactly what data the AI used
  • Four suggested starter questions appear when the chat is empty — click any to send

What to expect from answers:
  • Schema questions → immediate answers from the catalog index
  • Live data questions (e.g. "which desks are in red zone?") → the AI runs a
    BigQuery SQL query and shows you the actual results
  • Lineage questions → traces paths through the pipeline graph
  • UI questions → explains where controls are and what they reveal

Conversation management:
  • Chat history is saved in sessionStorage — survives tab navigation, cleared on
    browser close
  • "Clear" button (top of panel) wipes the conversation
  • ✕ closes the panel but keeps history

Best practices:
  • Be specific: "ES 97.5% for the Rates desk as of the latest calc date" works
    better than "show me ES"
  • For lineage questions, name the asset: "trace gold.capital_charge upstream"
  • For SLA issues, include the asset name: "why might gold.backtesting be breached?"
"""),

    # ── Catalog tab ───────────────────────────────────────────────────────────

    _doc("catalog_tab", """
Catalog Tab — Searching and Browsing All Data Assets

Location: The first item in the left sidebar; also the home page (route "/").

What it shows: Every data asset registered in the RiskLens catalog — 20+ tables
spanning all three medallion layers (bronze, silver, gold) and all domains
(risk, market_data, reference, regulatory). This is the starting point for
understanding what data exists, its quality, and who owns it.

Table columns and what they tell you:
  Name        — asset name (e.g. silver.risk_enriched) plus a short description.
                The name follows the pattern: layer.asset_name
  Domain      — colour-coded badge showing the business domain:
                  risk (red) = VaR, ES, P&L, back-testing assets
                  market_data (blue) = rates, prices, reference prices
                  reference (purple) = counterparty, instrument master, currency lookups
                  regulatory (amber) = outputs submitted to regulators
  Layer       — bronze (amber) / silver (blue) / gold (yellow) — indicates where
                in the medallion pipeline this asset lives
  Owner       — the person accountable for this asset's quality and freshness
  Freshness   — FRESH (green) / STALE (yellow) / CRITICAL (red) — how recently the
                data was refreshed relative to its expected schedule
  SLA         — "On time" (green) or "SLA breach" (red) — whether the latest refresh
                met its contractual deadline
  Rows        — current row count, formatted with thousand separators

Filters (above the table — all instant, client-side):
  Search box       — type any text to filter by name or description instantly
  Domain dropdown  — filter to risk / market_data / reference / regulatory
  Layer dropdown   — filter to bronze / silver / gold
  Type dropdown    — filter to table / feed / model / report

What to look for:
  • Red SLA badges or CRITICAL freshness on gold-layer assets are urgent — those
    are the tables that feed regulatory submissions
  • STALE silver assets often explain why downstream gold tables are breached
  • High null_rate or schema drift (visible in the drawer) can indicate upstream
    pipeline failures

How to use:
  Click any row → opens the Asset Detail Drawer on the right (384px wide)
  Click the same row again → closes the drawer
  The selected row is highlighted in dark blue

Example: to audit all gold-layer risk outputs, set Layer = gold and Domain = risk.
You will see: gold.es_outputs, gold.capital_charge, gold.backtesting, gold.risk_summary
"""),

    _doc("asset_drawer", """
Asset Detail Drawer — Everything About a Single Data Asset

Location: Opens on the right side (384px) when you click any row in the Catalog table.

What it shows: The complete profile of one data asset — quality, ownership,
schema, and SLA status — without leaving the catalog view.

Sections inside the drawer:

Overview section:
  • Full asset name in monospace font (e.g. silver.risk_enriched)
  • Type (table / feed / model / report) and Layer badge
  • Domain badge with colour coding
  • Full description explaining what this asset contains and its purpose
  • Tags — keywords like "frtb", "risk", "backtesting", "regulatory"

Ownership section:
  • Owner name (person accountable for data quality)
  • Team (e.g. "Quant Risk", "Market Data", "Data Engineering")
  • Data steward name and email (clickable mailto: link to contact directly)

Quality section — what to look for here:
  • Freshness badge: FRESH = data refreshed on schedule; STALE = late but not critical;
    CRITICAL = severely overdue, likely impacting compliance reporting
  • Null rate % — what % of cells are null. High null rates (> 5%) on silver/gold
    tables suggest upstream data feed issues.
  • Duplicate rate % — what % of rows are duplicates. Any duplicates in gold tables
    are a serious data quality issue for FRTB.
  • Schema drift — ✓ stable (schema is as expected) or ⚠ drifted (a column was
    added, removed, or changed type). Schema drift on silver/gold tables can break
    downstream regulatory calculations.

SLA section:
  • SLA breach badge — red if the latest refresh missed its deadline
  • Breach duration in minutes — how long the data has been overdue
  • If breach duration is > 60 min on gold.backtesting or gold.capital_charge,
    this may need to be escalated to compliance immediately

Schema section:
  • Full column list: column name, data type, nullable (✓ if nullable, — if required)
  • Scroll down to see all columns if the table has many
  • Gold tables typically have 10-15 columns; bronze tables may have 20-40+

"View Lineage →" button at the bottom:
  Navigates directly to the Lineage tab with this asset as the root node.
  Use this to trace where the data came from (upstream) and where it feeds into (downstream).

Example: click gold.backtesting →
  Overview: gold layer, risk domain, regulatory tag
  Owner: Compliance Analytics team
  Quality: FRESH, null_rate 0.0%, no schema drift
  Schema columns: desk, calc_date, var_99_1d, hyp_pnl, act_pnl, hyp_exception,
    act_exception, exceptions_250d, traffic_light_zone, multiplier, processed_at
  Click "View Lineage →" to see the full pipeline that feeds this table
"""),

    # ── Lineage tab ───────────────────────────────────────────────────────────

    _doc("lineage_tab", """
Lineage Tab — The Data Flow Graph

Location: Fourth item in the left sidebar (route "/lineage/:assetId").

What it shows: A directed acyclic graph (DAG) of the entire RiskLens FRTB data
pipeline — how raw market data and risk computations flow through bronze → silver → gold
layers to produce the regulatory outputs. Every node is a data asset; every arrow is
a data transformation or dependency.

The full pipeline visible in the lineage graph:
  External sources → Bronze layer:
    FRED API            → bronze.fred_rates       (daily interest rate feeds)
    Synthetic generator → bronze.risk_outputs     (simulated VaR/ES calculations)
    Trade system        → bronze.trade_feed        (raw trade records)

  Bronze → Silver transformations:
    bronze.fred_rates     → silver.rates           (cleaned, normalised rates)
    bronze.risk_outputs   → silver.risk_outputs    (validated risk metrics)
    bronze.trade_feed     → silver.trades          (enriched with ref data)
    silver.risk_outputs
    + silver.rates        → silver.risk_enriched   (risk joined with market context)
    ref.counterparty      → silver.trades          (counterparty enrichment)
    ref.instrument_master → silver.trades          (instrument enrichment)
    ref.currency          → gold.trade_positions   (currency normalisation)

  Silver → Gold regulatory outputs:
    silver.risk_enriched → gold.es_outputs        (Expected Shortfall 97.5%)
    silver.risk_enriched → gold.backtesting       (VaR back-testing results)
    gold.es_outputs
    + gold.backtesting   → gold.capital_charge    (total regulatory capital)
    gold.capital_charge  → gold.risk_summary      (firm-level summary)

Node colours by layer:
  Amber/orange border — bronze (raw data)
  Blue border         — silver (transformed)
  Yellow border       — gold (regulatory outputs)
  Purple border       — reference/lookup data
  Green border        — external source (API feeds)
  Slate/grey border   — pipeline jobs (Spark transforms)

Layout: left-to-right. Upstream (raw sources) on the left, downstream (gold outputs)
on the right. Arrows show the direction data flows.

Canvas controls:
  Pan     — click and drag the background to move around
  Zoom    — scroll wheel or pinch; + / − buttons bottom-left
  Fit     — ⊡ button bottom-left resets zoom to show the full graph
  MiniMap — bottom-right; the small rectangle represents your current viewport;
            drag it to pan quickly across a large graph

How to open the lineage graph:
  • From the Catalog tab: click a row → Asset Drawer → "View Lineage →"
  • From the Assets tab: hover a card → "View lineage →"
  • Direct URL: /lineage/gold_capital_charge (any asset ID works)
"""),

    _doc("lineage_hops", """
Lineage Hops Control — How Far to Expand the Pipeline Graph

Location: Top-right corner of the Lineage tab header — four buttons labelled 1, 2, 3, 4.
The currently selected hop count is highlighted with the brand colour (teal/indigo).
Default on page load: Hop 2.

What "hops" means in the context of this pipeline:
  A hop is one step of traversal away from the root (currently selected) asset —
  one upstream parent OR one downstream child. The hops setting controls how many
  levels in each direction are displayed simultaneously.

What each hop level reveals:

  Hop 1 — Immediate relationships only
    Shows only the direct parents and direct children of the root asset.
    Use this when you just need to know "what feeds directly into X" or "what does X feed into".
    Example (root = silver.risk_enriched, Hop 1):
      You see: silver.risk_outputs → silver.risk_enriched → gold.es_outputs
               silver.rates → silver.risk_enriched → gold.backtesting

  Hop 2 — Two levels each way (default)
    Shows the root, its parents and their parents, plus its children and their children.
    Use this for most investigations — enough context to understand the pipeline stage.
    Example (root = gold.capital_charge, Hop 2):
      Upstream: silver.risk_enriched, gold.es_outputs, gold.backtesting
      Downstream: gold.risk_summary
      (You can see the immediate silver inputs but NOT the bronze raw sources yet)

  Hop 3 — Three levels — sees into the silver transformation layer
    Example (root = gold.capital_charge, Hop 3):
      Upstream: bronze.fred_rates, bronze.risk_outputs → silver layer → gold layer
      You can now see where the market data originates (FRED API feed)

  Hop 4 — Full pipeline — maximum visibility, all the way to raw sources
    Shows the complete FRTB data lineage: FRED API → bronze → silver → gold → summary
    Use Hop 4 when investigating root causes, e.g. if gold.backtesting has an SLA breach,
    Hop 4 will reveal whether the problem started at a bronze source (data feed down)
    or at a silver transformation step (job failure).
    Also shows all reference table inputs: ref.counterparty, ref.instrument_master, ref.currency

What to look for at each hop level:
  • Stale or red nodes upstream at Hop 4 → the problem is at the source
  • All sources fresh but gold is stale → a silver transformation job likely failed
  • Missing reference nodes at Hop 3 → ref data enrichment may have gaps
  • The graph stats panel (top-left of the canvas) always shows current hop radius

Changing hops: click any of the four buttons — the graph re-fetches immediately
from the API with no page reload. The URL updates to reflect the new hop count.

Common use case: investigating an SLA breach
  1. Open lineage for the breached asset (e.g. gold.backtesting)
  2. Start at Hop 2 — check if gold.es_outputs or silver.risk_enriched look stale
  3. Increase to Hop 4 — if bronze nodes look stale, a raw data feed is likely down
  4. Click the stale node to see its metadata and owner contact in the Node Panel
"""),

    _doc("lineage_search", """
Lineage Node Search — Jumping to Any Asset in the Graph

Location: Monospace search input (w-56) in the Lineage tab header, left of the Hops buttons.

What it does: lets you navigate directly to any data asset as the new root node,
without going back to the Catalog tab. Useful when you want to explore a specific
part of the pipeline — e.g. jump straight to silver.risk_enriched without navigating
through the full graph from a gold asset.

How to use:
  1. Click the search input and start typing any part of an asset ID or display name
     (e.g. type "enrich" to find silver.risk_enriched, or "rate" to find silver.rates
     or bronze.fred_rates)
  2. A dropdown appears with up to 8 matching suggestions, showing node IDs
  3. Click a suggestion — OR press Enter to select the first match
  4. The graph reloads with that asset as the new root, at the current hops setting
  5. Press Escape to close the dropdown without navigating

All searchable asset IDs in the RiskLens pipeline:
  bronze: fred_rates, risk_outputs, trade_feed, prices
  silver: rates, risk_outputs, risk_enriched, trades, prices
  gold: es_outputs, capital_charge, backtesting, risk_summary, trade_positions
  reference: ref_counterparty, ref_instrument_master, ref_currency
  sources: fred_api_source, synthetic_generator

Example: type "silver" → suggestions show all five silver-layer nodes.
Click "silver_risk_enriched" → graph reloads centred on that node.
From there, set Hops = 1 to see exactly what feeds into the risk enrichment step.
"""),

    _doc("lineage_node_click", """
Lineage Node Panel — What You See When You Click a Node

Location: Appears in the top-right of the canvas area when you click any node (box)
in the lineage graph.

What it shows: The metadata profile of the selected asset — essentially a compact
version of the Asset Detail Drawer, shown inline without leaving the graph view.

Panel contents:
  • Node ID      — full asset identifier in monospace (e.g. silver_risk_enriched)
  • Type         — table / source / pipeline (pipeline nodes represent Spark jobs)
  • Domain       — risk / market_data / reference / regulatory
  • Layer        — bronze / silver / gold
  • Description  — short plain-English explanation of what this asset contains
  • Metadata     — any additional key-value pairs (e.g. owner, row count, SLA status,
                   freshness, null_rate, schema_drift)

"View lineage from here →" button:
  Clicking this makes the selected node the new root of the graph and re-fetches
  the lineage at the current hops setting. Use this to drill into a sub-graph —
  for example, click silver.risk_enriched → "View lineage from here" to centre
  the view on all inputs and outputs of the enrichment step.

How to dismiss: click anywhere on the canvas background (outside the panel).

What to look for in the node panel:
  • freshness = CRITICAL or STALE → this node's data is overdue
  • sla_breach = true → this asset missed its refresh deadline
  • schema_drift = true → a column changed, which may break downstream calculations
  • null_rate > 0.05 (5%) → data quality issue, especially concerning for silver/gold
  • owner and team → who to contact if there's a problem with this asset

Example: click the gold.capital_charge node →
  Type: table, Domain: regulatory, Layer: gold
  Description: "Aggregated regulatory capital charge per desk under FRTB IMA"
  Metadata: owner=Compliance Analytics, team=Quant Risk, rows=250,
             freshness=FRESH, sla_breach=false
"""),

    _doc("lineage_edge_click", """
Lineage Edge Stories — What You Learn When You Click an Arrow

Location: A panel appears at the bottom-centre of the canvas when you click any
arrow (edge) connecting two nodes in the lineage graph.

What it shows: Not just "A feeds B" — but the business story of WHY and HOW that
data transformation happens, what it means for FRTB compliance, and who owns it.

Edge Story panel contents:
  • Title          — plain-English name of the pipeline step
                     e.g. "Risk enrichment with market context"
  • What           — technical description of the transformation:
                     what join, aggregation, or calculation happens
  • Business impact — why this step is critical for FRTB compliance
  • Frequency      — how often data flows across this edge (e.g. "daily", "real-time")
  • Owner          — team responsible for this transformation step

Key edges and what they reveal:

  bronze.fred_rates → silver.rates
    "Daily rate normalisation" — FRED API data is cleaned, validated, and converted
    to the 30+ rate curves needed for GIRR (interest rate risk) calculations.
    Business impact: if this fails, all interest rate risk metrics are stale.

  silver.risk_outputs → silver.risk_enriched
    "Risk enrichment with market context" — Joins VaR/ES calculations with current
    SOFR rates, VIX levels, and HY credit spreads to produce market-conditioned metrics.
    Business impact: this is the primary input to ES and back-testing gold tables.

  silver.risk_enriched → gold.es_outputs
    "Expected Shortfall aggregation" — Computes ES 97.5% per desk per risk class
    with FRTB liquidity horizon adjustments (10/20/40/60 days by risk class).
    Business impact: feeds directly into the capital charge calculation.

  gold.es_outputs + gold.backtesting → gold.capital_charge
    "Capital charge consolidation" — Applies the back-testing multiplier (3.0–4.0×)
    to ES scaled by liquidity horizon. The final number reported to regulators.

How to dismiss: click anywhere on the canvas background.
"""),

    # ── Governance tab ────────────────────────────────────────────────────────

    _doc("governance_tab", """
Governance Tab — SLA Health, Data Quality, and Ownership

Location: Second item in the left sidebar (route "/governance").

What it shows: The operational health of the entire data pipeline — which assets are
breaching their SLA deadlines, which have data quality issues, and who owns what.
This is the first place to look when something is wrong with regulatory reporting.

The tab has three sub-tabs at the top:

━━━ SLA Status sub-tab ━━━

Summary cards at the top (4 columns):
  Total assets  — total count of registered assets
  SLA breaches  — count of assets missing their SLA deadline (red card if > 0)
  On time       — count of assets meeting SLA (green)
  Critical      — count of assets with CRITICAL freshness (urgent, red card)

"Breaches only" checkbox:
  Filters the table to show ONLY assets with active SLA breaches.
  Use this immediately when something in the risk dashboard looks wrong — it quickly
  surfaces which upstream asset caused the issue.

Table columns: Asset name | Domain | Layer | Status badge | Breach duration (min) | Last checked
  • Breach duration = how many minutes the data has been overdue
  • If gold.backtesting or gold.capital_charge are breached by > 30 min, escalate to compliance

What to look for:
  • Bronze asset breached → a raw data feed (e.g. FRED API) is likely down
  • Silver asset breached → a Spark transformation job likely failed
  • Gold asset breached but silver is fresh → the aggregation job failed
  • Multiple assets breached with similar timestamps → a systemic pipeline failure

━━━ Data Quality sub-tab ━━━

Bar chart: freshness distribution across all assets (FRESH = green, STALE = yellow,
CRITICAL = red). The chart gives an at-a-glance view of overall pipeline health.

Table columns: Asset | Domain | Freshness badge | Null rate % | Dup rate % | Schema drift

Schema drift ✓ = stable; ⚠ = a column was added, removed, or its type changed.
Schema drift on gold tables (e.g. gold.backtesting gaining a new column) can break
regulatory reporting templates.

Null rate thresholds to watch:
  > 0%  on gold assets — investigate immediately, nulls in regulatory outputs are not acceptable
  > 5%  on silver assets — upstream data feed likely has gaps
  > 20% on bronze assets — feed may be partially failing

━━━ Ownership sub-tab ━━━

Team filter dropdown — select a team or "All teams" to filter the ownership table.
Table columns: Asset | Domain | Layer | Owner | Team | Steward

What to use this for:
  • Finding the right person to contact about a data issue
  • Understanding which team owns which part of the FRTB pipeline
  • Auditing ownership coverage — every gold asset should have a steward

Interactions across all sub-tabs:
  • Click sub-tab → instant switch, no reload
  • "Breaches only" checkbox → instant re-filter
  • Team dropdown → instant re-filter
  • Hover chart bar → tooltip with exact count
"""),

    # ── Assets tab ────────────────────────────────────────────────────────────

    _doc("assets_tab", """
Assets Tab — The Gold Layer at a Glance

Location: Third item in the left sidebar (route "/assets").

What it shows: ONLY the gold-layer assets — the business-ready, regulatory-grade
outputs at the top of the medallion pipeline. This is the "executive view" of the
data catalog, showing only the tables that matter for FRTB compliance reporting.

Gold assets visible in this tab:
  gold.es_outputs        — Expected Shortfall 97.5% per desk and risk class
  gold.capital_charge    — Regulatory capital charge per desk (ES × multiplier)
  gold.backtesting       — VaR 99% back-testing results and traffic-light zones
  gold.risk_summary      — Firm-level consolidated risk summary
  gold.trade_positions   — Aggregated trading positions with currency normalisation

Summary header (4 KPI cards at the top):
  Total assets    — count of gold assets (typically 5-6)
  Total rows      — sum of all row counts across gold tables
  SLA breaches    — count of gold assets missing SLA (orange/red if > 0)
                    ANY breach here is urgent — these are the regulatory tables
  Stale/critical  — count of gold assets with freshness issues (orange if > 0)

Domain filter dropdown: filter cards by domain (risk / regulatory / market_data / all)

What each card shows:
  • Asset name in monospace font
  • Asset type (table / model / report)
  • Freshness badge (FRESH / STALE / CRITICAL)
  • 2-line description (truncated — hover for full text)
  • Domain badge with colour
  • Row count
  • Owner name

Hover a card to reveal the "View lineage →" button in the bottom-right corner.

Interactions:
  • Click a card → navigates to the Catalog tab with that asset's drawer open
  • Hover card → reveals "View lineage →" button
  • Click "View lineage →" → opens Lineage tab for that asset
  • Domain dropdown → reloads cards filtered by domain

What to look for:
  • Red SLA badge on any gold card → escalate to compliance
  • CRITICAL freshness → the last refresh is severely overdue
  • All gold cards green but risk dashboard shows stale numbers → check silver layer
    in the Catalog tab (silver.risk_enriched is the most common bottleneck)
"""),

    # ── FRTB Risk tab ─────────────────────────────────────────────────────────

    _doc("risk_tab_overview", """
FRTB Risk Tab — The Live Regulatory Dashboard

Location: Fifth item in the left sidebar (route "/risk").

What it shows: The live FRTB compliance dashboard — every metric that risk managers
and compliance officers need to monitor daily under BCBS 457. Data is pulled in
real time from the gold-layer BigQuery tables.

Five sub-tabs:
  Summary      — firm-wide overview: total ES, total capital charge, traffic-light zones
  Capital      — per-desk capital breakdown by risk class and liquidity horizon
  Back-testing — VaR 99% back-testing results, exception counts, traffic-light zones
  PLAT         — P&L Attribution Test: UPL ratio, Spearman correlation, KS statistic
  RFET         — Risk Factor Eligibility Test: which market data factors pass/fail

Traffic-light zones (BCBS 457 §351-368) — used across all sub-tabs:
  GREEN — 0 to 4 back-testing exceptions in 250 trading days
          Multiplier = 3.0× (minimum regulatory capital)
          No regulatory action required
  AMBER — 5 to 9 exceptions in 250 trading days
          Multiplier = 3.0× to 3.99× (sliding scale)
          Desk is "under watch" — model review recommended
  RED   — 10+ exceptions in 250 trading days
          Multiplier = 4.0× (maximum surcharge)
          Regulator notification required; IMA approval may be revoked

What to look for across all sub-tabs:
  • Any desk in RED zone → immediate attention required, regulator notification
  • PLAT FAIL on any desk → model accuracy problem, capital surcharge risk
  • RFET FAIL on any risk factor → that factor cannot be used in IMA, must switch to SA
  • High multipliers (> 3.5×) even in AMBER → capital is being inflated significantly

Shared interactions:
  • Click any sub-tab → instant switch, no reload
  • Hover any table row → darker highlight for readability
  • Hover any chart area → tooltip with exact values
  • Click any chart point → (where applicable) drill-down detail
"""),

    _doc("risk_summary_subtab", """
FRTB Risk — Summary Sub-tab: The Firm-Wide View

Location: First sub-tab in the FRTB Risk tab.

What it shows: The single most important screen for a compliance officer —
the firm-level regulatory picture in one view. Total ES, total capital charge,
overall traffic-light status, and which desks are failing PLAT.

KPI cards (4 columns at the top):
  Firm ES 97.5%
    Total Expected Shortfall across all desks, all risk classes, in USD millions.
    ES is the primary risk measure under FRTB IMA (replacing VaR as the headline number).
    Formula: ES = weighted average of worst tail losses at 97.5th percentile.
    What to watch: spikes in ES usually reflect market stress (VIX up, credit spreads wide).

  Firm Capital Charge
    Total regulatory capital the firm must hold, in USD millions. Highlighted in brand colour.
    Formula: Capital = ES × liquidity-horizon scalar × back-testing multiplier.
    Multiplier ranges from 3.0× (GREEN) to 4.0× (RED).
    What to watch: any jump > 10% day-over-day warrants investigation.

  Traffic Light
    Firm-level back-testing zone (GREEN / AMBER / RED), coloured accordingly.
    Shows the WORST zone across all desks (one RED desk makes the firm RED).
    What to watch: RED is a regulatory trigger — requires escalation to senior risk.

  PLAT Failing Desks
    Count of desks failing the P&L Attribution Test. If > 0, those desks cannot
    use IMA for that risk class and must use the more expensive SA capital approach.

Trend chart (below KPI cards):
  Area chart with two overlapping series:
    Capital Charge (darker fill) — total regulatory capital over time
    ES 97.5% Scaled (lighter fill) — scaled ES underlying the capital charge
  X-axis = calculation date (daily bars)
  Y-axis = USD millions
  Hover the chart → tooltip shows exact values on any given date
  What to look for: divergence between ES and Capital (means multiplier changed);
  sharp spikes in either series (market stress event or model change)

Desk-level detail table (below the chart):
  Desk | Risk Class | ES 97.5% 1d | ES Scaled | Capital Charge | Traffic Light | PLAT | Exceptions (250d)

  • Exceptions (250d) highlighted in amber if >= 5 (approaching AMBER zone)
  • PLAT column: PASS (green badge) or FAIL (red badge)
  • Traffic Light badge coloured GREEN / AMBER / RED
  • Clicking a row does not navigate (it's a summary view — use Capital or Back-testing
    sub-tabs for the drill-down per desk)

Example reading:
  FX desk: ES 97.5% 1d = $12.3M, ES Scaled = $17.4M (FX LH = 10 days),
  Capital = $52.2M (multiplier 3.0×, GREEN zone, 2 exceptions in 250 days), PLAT PASS
"""),

    _doc("risk_capital_subtab", """
FRTB Risk — Capital Sub-tab: Per-Desk Capital Breakdown

Location: Second sub-tab in the FRTB Risk tab.

What it shows: The detailed regulatory capital charge for every desk, broken down
by risk class and showing the exact liquidity-horizon scaling and multiplier applied.
This is the calculation detail behind the "Firm Capital Charge" headline number.

KPI cards (3 columns):
  Total Capital Charge — firm total in USD millions (same as Summary tab)
  Desks               — count of desks with capital data
  Max Multiplier      — the highest multiplier currently applied across all desks
                        3.0 = all desks GREEN; any value > 3.0 means at least one desk
                        is in AMBER or RED zone and paying a capital surcharge

Detail table columns:
  Desk         — trading desk name (e.g. Rates, FX, Equities, Credit, Commodities)
  Risk Class   — FRTB risk class category:
                  GIRR = General Interest Rate Risk
                  FX   = Foreign Exchange
                  CSR_NS = Credit Spread Risk (non-securitisation)
                  EQ   = Equity
                  COMM = Commodities
  LH           — Liquidity Horizon in days (mandated by BCBS 457 §189):
                  GIRR = 10 days, FX = 10, CSR_NS = 20, EQ = 20, COMM = 20
  ES 1d        — 1-day Expected Shortfall (base ES before LH scaling), USD M
  ES Scaled    — ES adjusted for liquidity horizon: ES_scaled = ES_1d × sqrt(LH/10)
                  Example: COMM LH=20 → ES_scaled = ES_1d × sqrt(2) ≈ 1.41 × ES_1d
  Reg Floor    — regulatory floor (minimum capital from SA approach), USD M
  Capital Charge — max(ES Scaled × Multiplier, Reg Floor), USD M
  Multiplier   — back-testing multiplier: 3.0 (GREEN) to 4.0 (RED)
  Zone         — traffic-light zone badge

What to look for:
  • High ES Scaled on COMM or CSR_NS (longer LH → bigger scaling factor)
  • Multiplier > 3.0 → that desk is in AMBER or RED zone
  • Capital Charge hitting the Reg Floor → desk would pay less under IMA than SA floor
    (indicates the internal model may be too conservative for that risk class)
  • Large differences between desks in the same risk class → check model assumptions

Example:
  Rates desk | GIRR | LH=10 | ES_1d=$5.2M | ES_scaled=$5.2M | Floor=$14.1M |
  Capital=$16.9M | Multiplier=3.25 | AMBER

  This means: Rates desk has 6 back-testing exceptions (AMBER), paying 3.25×
  instead of minimum 3.0×. Extra capital = ($3.25 - $3.0) × $5.2M = $1.3M per day.
"""),

    _doc("risk_backtesting_subtab", """
FRTB Risk — Back-Testing Sub-tab: VaR Exception Tracking

Location: Third sub-tab in the FRTB Risk tab.

What it shows: VaR 99% 1-day back-testing results — whether each desk's risk model
correctly predicted P&L losses. Under FRTB IMA, every firm must run back-testing daily
and track exceptions over a rolling 250-day window. Exceptions determine the traffic-light
zone and therefore the capital multiplier.

Controls:
  Calc Date dropdown (top-right corner) — select which trading day to view.
  Options come from available dates in gold.backtesting. Default = most recent date.
  Use this to investigate specific historical dates (e.g. a market stress event).

KPI cards (3 columns):
  RED zone desks   — count of desks with 10+ exceptions (red card, urgent)
  AMBER zone desks — count of desks with 5-9 exceptions (amber card, monitor)
  GREEN zone desks — count of desks with 0-4 exceptions (green card, compliant)

The traffic-light exception thresholds (BCBS 457 §351-368):
  GREEN: 0-4 exceptions in 250 days → multiplier 3.0× → minimum capital
  AMBER: 5 exceptions = 3.40×; 6 = 3.50×; 7 = 3.65×; 8 = 3.75×; 9 = 3.85×
  RED:   10+ exceptions → multiplier 4.0× → maximum capital (33% surcharge over GREEN)

Detail table columns:
  Desk             — trading desk
  VaR 99% 1d      — model-predicted maximum loss at 99% confidence for 1 day, USD M
                     (this is the threshold — actual losses beyond this are exceptions)
  Hyp PnL          — hypothetical P&L: P&L using static positions (model's own prediction)
  Actual PnL       — actual P&L including all intraday activity
  Hyp Exception    — "Yes" (red) if Hyp PnL < –VaR (model failed on hypothetical terms)
  Act Exception    — "Yes" (red) if Actual PnL < –VaR (model failed on actual terms)
  Exceptions (250d) — rolling 250-day exception count; amber highlight if >= 5
  Zone             — GREEN / AMBER / RED badge
  Multiplier       — current capital multiplier for this desk

What to look for:
  • Hyp Exception YES but Act Exception NO → model is overly conservative
  • Act Exception YES but Hyp Exception NO → intraday desk activity causing losses
    the model doesn't capture (potential model gap)
  • Rising exception count trending toward 5 → warn the desk; approaching AMBER
  • Any desk already in RED → requires senior risk manager and compliance sign-off

Example: Equities desk on 2026-01-15:
  VaR=$8.4M, Hyp PnL=$11.2M, Act PnL=–$9.1M, Hyp=No, Act=Yes
  This exception is recorded. Exceptions (250d) = 7 → AMBER zone, multiplier 3.65×
"""),

    _doc("risk_plat_subtab", """
FRTB Risk — PLAT Sub-tab: P&L Attribution Test

Location: Fourth sub-tab in the FRTB Risk tab.

What it shows: Whether each desk's risk model can explain its daily P&L — the P&L
Attribution Test (PLAT). Under FRTB IMA (BCBS 457 §329-345), every desk using IMA
must demonstrate that its risk model's P&L predictions are statistically consistent
with actual P&L. Desks that fail PLAT cannot use IMA and must use the standardised
approach (SA), which typically charges 2-3× more capital.

KPI cards (3 columns):
  Failing desks   — count of desks failing PLAT (red alert if > 0)
                    Even one failing desk represents significant extra capital
  Passing desks   — count passing all three statistical tests
  Avg UPL ratio   — firm-wide average unexplained P&L ratio (lower is better)

The three PLAT statistical tests (ALL three must pass for PLAT PASS):

  1. UPL (Unexplained P&L) Ratio
     Formula: |Hyp PnL – Actual PnL| / std(PnL)
     Pass threshold: < 0.95 (less than 95% of standard deviation unexplained)
     What it measures: how much of the actual P&L the risk model fails to explain
     High UPL → model is missing risk factors or positions

  2. Spearman Correlation
     Rank correlation between hypothetical P&L and actual P&L over the test period
     Pass threshold: > 0.40 (must be positively correlated)
     What it measures: whether good model days are also good actual days
     Low Spearman → model and actual P&L moving in opposite directions (bad sign)

  3. KS Statistic (Kolmogorov-Smirnov)
     Tests whether hypothetical P&L and actual P&L come from the same distribution
     Pass threshold: < 0.20
     What it measures: distributional similarity between model and actual P&L
     High KS → the two distributions are very different (model mis-specified)

Detail table columns:
  Desk | PLAT badge | UPL Ratio | UPL Pass | Spearman | Spearman Pass |
  KS Stat | KS Pass | Obs Count | Notes

What to look for:
  • PLAT FAIL → identify which of the three sub-tests failed
  • Spearman < 0 → serious model problem; model is anti-correlated with reality
  • UPL > 1.5 → desk P&L is largely unexplained; likely missing positions or risk factors
  • KS > 0.30 → distributions are very different; consider model recalibration
  • Notes column often has the quant team's explanation for any failures

Example:
  FX desk: UPL=0.82 ✓, Spearman=0.71 ✓, KS=0.14 ✓ → PLAT PASS, uses IMA
  Credit desk: UPL=0.91 ✓, Spearman=0.31 ✗ → PLAT FAIL → must use SA capital
  Extra capital for Credit on SA vs IMA: typically 2-3× the IMA charge
"""),

    _doc("risk_rfet_subtab", """
FRTB Risk — RFET Sub-tab: Risk Factor Eligibility Test

Location: Fifth sub-tab in the FRTB Risk tab.

What it shows: Which market data risk factors are eligible for use in IMA (Internal
Models Approach) capital calculations under FRTB. The Risk Factor Eligibility Test (RFET)
requires that risk factors used in IMA models have sufficient real market price observations.
Factors that fail RFET cannot be used in IMA — they must be capitalised under the more
expensive Stress Scenario Risk Measure (SSRM) or standardised approach.

Controls:
  Risk Class dropdown (top of table) — filter by GIRR / FX / CSR_NS / EQ / COMM / All

KPI cards (3 columns):
  Failing risk factors — count of factors currently failing RFET (red alert if > 0)
  Eligible for IMA     — count of factors passing RFET and approved for IMA use
  Total risk factors   — total count of factors being monitored

RFET pass rules (BCBS 457 §76-80) — a factor passes if EITHER condition is met:
  Condition 1: ≥ 75 real price observations in the past 12 months (approx 1 per 3 trading days)
  Condition 2: ≥ 25 real price observations in the past 90 days (approx 1 per 3 trading days)
  Either condition alone is sufficient — a factor only fails if BOTH are unmet.

Detail table columns:
  Risk Factor ID    — unique identifier in monospace (e.g. USD_SOFR_3M, NGAS_HENRY_HUB)
  Risk Class        — GIRR / FX / CSR_NS / EQ / COMM
  Obs 12m           — count of real price observations in past 12 months
  12m Pass          — ✓ if Obs 12m >= 75
  Obs 90d           — count of real price observations in past 90 days
  90d Pass          — ✓ if Obs 90d >= 25
  RFET badge        — PASS (green) / FAIL (red)
  IMA Eligible      — whether this factor can be used in IMA models (= RFET PASS)
  Staleness (days)  — days since the last real price observation (high = illiquid)
  Failure Reason    — explanation of why the factor failed (e.g. "insufficient 90d observations")

What to look for:
  • RFET FAIL + IMA Eligible FAIL → this factor is excluded from IMA; using it in
    a model without SSRM treatment is a regulatory violation
  • High staleness (> 30 days) but still passing → the factor is barely passing,
    watch it closely; a few more gaps will push it to FAIL
  • Multiple COMM (commodities) failures → commodity markets may be illiquid or
    the data feed for that factor class is missing observations
  • GIRR failures (rate factors) → unusual and urgent; rate factors rarely fail RFET
    since rates markets are among the most liquid

What happens when a factor fails RFET:
  • The risk factor is marked non-IMA-eligible
  • The quant team must capitalise it under SSRM (a conservative stress-based method)
  • SSRM capital is typically 2-5× higher than IMA capital for the same position
  • The risk committee must approve any proxy or substitute factor

Example:
  USD_SOFR_3M: Obs 12m=240 ✓, Obs 90d=55 ✓ → RFET PASS → IMA Eligible ✓
  NGAS_HENRY_HUB: Obs 12m=18 ✗, Obs 90d=6 ✗ → RFET FAIL → IMA Eligible ✗
    Failure Reason: "Insufficient real price observations — illiquid commodity factor"
    Action required: capitalise under SSRM until observation count recovers to threshold
"""),

    # ── Badges ───────────────────────────────────────────────────────────────

    _doc("badges_and_indicators", """
Badges and Colour Indicators in RiskLens — Complete Reference

Badges appear throughout every tab and convey status at a glance.
This is the complete reference for every badge colour and what it means.

━━━ Layer badges (Catalog, Lineage, Governance, Assets tabs) ━━━
  bronze  — amber/orange background — raw, immutable landing data (from feeds, APIs)
  silver  — blue background         — cleaned, validated, transformed data
  gold    — yellow/golden background — aggregated, business-ready, regulatory-grade data

  Reading the layer tells you WHERE in the FRTB pipeline this asset lives.
  Gold = what regulators care about; Bronze = raw sources; Silver = in-between.

━━━ Domain badges ━━━
  risk         — red/rose      — VaR, ES, P&L, back-testing, capital charge data
  market_data  — blue          — market rates, prices, FRED feeds, synthetic prices
  reference    — purple/violet — master data: counterparty, instrument, currency tables
  regulatory   — amber/orange  — compliance outputs submitted to regulators
  lineage      — slate/grey    — pipeline metadata, job logs, transformation records

━━━ Freshness badges (Catalog, Governance, Assets tabs) ━━━
  FRESH    — green  — data was refreshed within its expected schedule window
                      This asset is current and reliable
  STALE    — yellow — data is late but not yet at a critical threshold
                      Investigate if this is a silver or gold asset
  CRITICAL — red    — data is severely overdue — the refresh is missing by a large margin
                      Any CRITICAL badge on a gold asset requires immediate attention;
                      it may be affecting live regulatory calculations

━━━ SLA badges ━━━
  On time    — green — data was delivered within its SLA deadline
  SLA breach — red   — data missed its contractual delivery deadline
                      Breach duration (minutes overdue) is shown next to the badge.
                      Even small breaches on gold.capital_charge must be documented.

━━━ Traffic-light zone badges (FRTB Risk tab, Back-testing) ━━━
  GREEN — green  — 0-4 exceptions in 250 trading days
                   Multiplier = 3.0× (minimum regulatory capital)
                   Compliant; no regulatory action needed
  AMBER — amber  — 5-9 exceptions in 250 trading days
                   Multiplier = 3.0× to 3.99× (increases per exception)
                   "Under watch" — desk is at risk of moving to RED
  RED   — red    — 10+ exceptions in 250 trading days
                   Multiplier = 4.0× (maximum; 33% capital surcharge vs GREEN)
                   Regulators must be notified; IMA approval at risk

━━━ PASS / FAIL badges (PLAT and RFET sub-tabs) ━━━
  PASS — emerald/green — all required tests passed; IMA eligible; compliant
  FAIL — red           — one or more tests failed; requires remediation action

━━━ Exception highlighting in tables ━━━
  Amber row highlight — Exceptions (250d) >= 5 (approaching AMBER zone)
  Red text in cells   — individual "Yes" exception on a specific trading day

━━━ Node colours in Lineage graph ━━━
  Amber/orange border — bronze data assets (raw)
  Blue border         — silver data assets (transformed)
  Yellow border       — gold data assets (regulatory outputs)
  Purple border       — reference/lookup tables
  Green border        — external source nodes (FRED API, trade system)
  Slate/grey border   — pipeline job nodes (Spark jobs)
"""),

    # ── Common workflows ─────────────────────────────────────────────────────

    _doc("workflow_investigate_breach", """
Workflow: Investigating an SLA Breach Step by Step

This workflow explains exactly how to use RiskLens to find, understand, and
escalate an SLA breach in the FRTB data pipeline.

When to use: You notice a warning in the risk dashboard, or you receive an alert
that a gold asset (e.g. gold.backtesting) has not refreshed on time.

Step-by-step:

1. Open the Governance tab → SLA Status sub-tab
   Click "Governance" in the left sidebar. The SLA Status sub-tab shows automatically.

2. Check the summary cards
   Look at "SLA breaches" card — if it's red and shows a count > 0, something is wrong.
   Check "Critical" card too — CRITICAL freshness means the data is very overdue.

3. Enable "Breaches only" checkbox
   Tick the "Breaches only" checkbox to hide all healthy assets.
   Now you only see what's broken. Note the Breach Duration column — higher numbers
   mean the problem started earlier and is more urgent.

4. Identify the layer
   Is the breached asset bronze, silver, or gold?
   • Bronze breach → a raw data feed (FRED API, trade system) is likely down.
                     Contact the Market Data team or check feed provider status.
   • Silver breach → a Spark transformation job likely failed.
                     Contact the Data Engineering team.
   • Gold breach   → the aggregation pipeline failed.
                     Also check if upstream silver assets are also breached.

5. Open the Catalog tab and search for the breached asset
   Type the asset name in the search box (e.g. "backtesting").
   Click the row to open the Asset Drawer.

6. Check the Quality section in the Asset Drawer
   Look at: null_rate, duplicate_rate, schema_drift.
   • High null_rate → upstream feed has gaps, data arrived incomplete
   • Schema drift ⚠ → a column changed; the pipeline job may have failed because
                       the schema changed unexpectedly
   • These clues often explain WHY the breach happened

7. Click "View Lineage →" in the Asset Drawer
   This opens the Lineage tab with the breached asset as root.

8. Set Hops to 4 (top-right of Lineage tab)
   Expanding to full depth reveals the complete upstream chain.
   Look for any node with a stale or critical colour — that's where the problem started.

9. Click suspicious upstream nodes
   Click any amber or stale-looking node to open its Node Panel (top-right of canvas).
   Check the metadata: freshness, sla_breach, owner.

10. Click the pipeline job edges
    Click the arrows between a stale source and a healthy downstream node.
    The Edge Story (bottom of canvas) tells you which team owns that pipeline step.

11. Escalate with context
    You now know: which asset is breached, how overdue it is, which upstream node
    caused it, and which team owns that step. Contact the steward directly.

12. Ask the AI for additional context
    Open the AI Chat panel and ask: "Why might gold.backtesting have an SLA breach today?"
    The AI can cross-reference the lineage, schema docs, and pipeline descriptions.
"""),

    _doc("workflow_trace_lineage", """
Workflow: Tracing the Full Data Lineage of a Regulatory Output

This workflow shows how to trace where any gold-layer regulatory table gets its data —
from the first raw API feed through every transformation to the final gold output.
Essential for FRTB model validation, data lineage audits, and regulatory inquiries.

Example: trace gold.capital_charge back to its raw sources.

Step 1: Start from the Catalog tab
  Click "Catalog" in the sidebar. You see all 20+ assets.
  Use the Layer filter (gold) to narrow to just gold assets.
  Click on "gold.capital_charge" row to open the Asset Drawer.

Step 2: Read the Asset Drawer context
  Note: gold layer, regulatory domain, owner = Compliance Analytics.
  Schema columns: desk, risk_class, lh_days, es_1d, es_scaled, reg_floor,
                  capital_charge, multiplier, zone, processed_at.
  This table aggregates per-desk capital charges applying back-testing multipliers.

Step 3: Click "View Lineage →"
  The Lineage tab opens. Root = gold.capital_charge. Default Hops = 2.
  At Hop 2, you can see:
    gold.es_outputs → gold.capital_charge
    gold.backtesting → gold.capital_charge
    gold.capital_charge → gold.risk_summary

Step 4: Expand to Hops = 4
  Click "4" in the top-right hops control.
  Now the full pipeline is visible:
    FRED API source → bronze.fred_rates
    Synthetic generator → bronze.risk_outputs
    bronze.fred_rates → silver.rates
    bronze.risk_outputs → silver.risk_outputs
    silver.rates + silver.risk_outputs → silver.risk_enriched
    silver.risk_enriched → gold.es_outputs
    silver.risk_enriched → gold.backtesting
    gold.es_outputs + gold.backtesting → gold.capital_charge
    gold.capital_charge → gold.risk_summary
  Reference tables also visible: ref.counterparty, ref.instrument_master, ref.currency

Step 5: Explore specific nodes
  Click "bronze.fred_rates" node → Node Panel shows:
    Type: source feed, Domain: market_data, Layer: bronze
    Description: "Daily interest rate data from FRED API — SOFR, Treasury, swap rates"
  Click "silver.risk_enriched" node → shows this is the critical junction:
    "Enriched risk metrics: VaR/ES joined with current market context (SOFR, VIX, HY spreads)"

Step 6: Understand each transformation
  Click the arrow from silver.risk_enriched → gold.es_outputs.
  Edge Story: "Expected Shortfall aggregation — applies FRTB liquidity horizon scaling
  (sqrt(LH/10)) per risk class to compute ES 97.5%."
  Click the arrow from gold.es_outputs → gold.capital_charge.
  Edge Story: "Capital consolidation — applies back-testing multiplier (3.0-4.0×)
  to produce the final BCBS 457 regulatory capital charge."

Step 7: Use the search to jump between nodes
  Type "enriched" in the lineage search box → select silver_risk_enriched.
  Graph recentres. Set Hops = 1 to see ONLY its direct inputs and outputs.

Step 8: Ask the AI to summarise
  "What is the full data lineage of gold.capital_charge, step by step?"
  The AI traces from FRED API through every transformation to the final capital number.
"""),

    _doc("workflow_check_rfet_failures", """
Workflow: Auditing RFET Eligibility — Finding Risk Factors That Cannot Use IMA

This workflow explains how to use the RFET sub-tab to identify which market data
risk factors have failed the eligibility test and understand the capital implications.

When to use: Regular daily monitoring of RFET status; model validation audits;
responding to a regulatory inquiry about which factors are in IMA vs SA capital.

Step 1: Open the FRTB Risk tab → RFET sub-tab
  Click "FRTB Risk" in the sidebar (last icon).
  Click the "RFET" sub-tab (rightmost of the five sub-tabs).

Step 2: Read the KPI cards
  "Failing risk factors" card shows the count in red. If > 0, there are IMA exclusions.
  "Eligible for IMA" shows how many factors are approved.
  Compare these numbers: if Eligible / Total < 80%, the firm has significant SSRM exposure.

Step 3: Filter by Risk Class
  Use the Risk Class dropdown to focus on one category.
  Start with COMM (commodities) — these are most often illiquid and prone to RFET failure.
  Then check CSR_NS (credit) — credit factors can also have observation gaps.
  GIRR and FX failures are unusual and warrant immediate attention.

Step 4: Identify failing factors in the table
  Sort or scan for rows with RFET = FAIL (red badge).
  For each failure, check:
    Obs 12m — how far below 75 is the count? (e.g. 18 obs = 57 short of threshold)
    Obs 90d — how far below 25? (e.g. 6 obs = 19 short)
    Staleness (days) — how many days since last real price? (high = very illiquid)
    Failure Reason — the quant team's explanation

Step 5: Understand the capital impact
  Any RFET FAIL factor that is currently used in an IMA model must be:
  a) Removed from the IMA calculation, OR
  b) Capitalised under the Stress Scenario Risk Measure (SSRM)
  SSRM capital = stressed ES using a proxy/scenario, typically 2-5× the IMA capital.
  Contact the Quant Risk team immediately with the list of failing factors.

Step 6: Cross-reference with the capital sub-tab
  Go to Capital sub-tab. Look at Commodities (COMM) and Credit (CSR_NS) desks.
  If RFET factors are failing in those classes, the desk may be understating capital
  if SSRM has not been applied. This is a regulatory violation risk.

Step 7: Ask the AI
  "Which risk factors currently fail the RFET eligibility test and what is the impact?"
  "Which COMM risk factors are closest to failing RFET?"
  "How many observations does NGAS_HENRY_HUB have and why did it fail RFET?"
"""),

    _doc("workflow_ask_ai", """
Workflow: Getting the Most from the RiskLens AI Chat

The AI assistant is powered by Claude (Anthropic) with full access to the RiskLens
catalog, schema definitions, data lineage graph, live BigQuery data, FRTB regulation
knowledge, and complete UI documentation. It can answer virtually any question about
the data, the pipeline, the compliance metrics, or how to use the application.

How to open: Click "✦ Ask RiskLens AI" at the bottom of the left sidebar.
The panel slides in from the right and stays open as you navigate between tabs.

━━━ Types of questions and example prompts ━━━

Schema and catalog questions (answered from the index, instant):
  "What columns does gold.backtesting contain?"
  "What is the difference between gold.risk_summary and gold.es_outputs?"
  "What does the silver.risk_enriched table store and what domain is it in?"
  "Who owns the gold.capital_charge table and what team are they in?"
  "Which assets have schema drift right now?"

Lineage and pipeline questions (answered from the lineage graph):
  "What is the complete data lineage of gold.capital_charge?"
  "What feeds into silver.risk_enriched and what does it feed into?"
  "Which pipeline jobs transform data from bronze to silver?"
  "What external data sources does RiskLens depend on?"
  "If bronze.fred_rates was stale, which gold tables would be affected?"

Live data questions (AI runs BigQuery and returns actual results):
  "Which desks are currently in the RED traffic-light zone?"
  "What is the current total capital charge for the firm?"
  "Which risk factors failed the RFET eligibility test?"
  "Show me the back-testing exception counts for all desks."
  "What are the latest ES 97.5% values per desk?"
  "Are there any SLA breaches on gold-layer assets right now?"

FRTB regulation questions (deep regulatory knowledge built-in):
  "What is RFET and how many real price observations are required to pass?"
  "Explain the difference between VaR 99% and ES 97.5% under FRTB."
  "What are the traffic-light zone thresholds for back-testing exceptions?"
  "What is PLAT and what three statistical tests does it use?"
  "What happens if a desk fails PLAT? Can it still use IMA?"
  "What is the liquidity horizon for CSR_NS under BCBS 457?"
  "How is the back-testing multiplier calculated for an AMBER-zone desk?"

UI and navigation questions (the AI knows the full application):
  "What does the Hops control in the top right of the Lineage tab do?"
  "How do I filter the Governance tab to see only breaches?"
  "How do I trace the lineage of a gold table back to its raw sources?"
  "What does a red badge on an asset in the Catalog mean?"
  "How do I find which desks are failing PLAT in the UI?"
  "What is the difference between the PLAT and RFET sub-tabs?"
  "How do I jump to a specific node in the lineage graph?"

━━━ Tips for better answers ━━━
  • Name the specific asset: "gold.backtesting" not just "the backtesting table"
  • Specify the desk for live data: "Equities desk ES" not just "show ES"
  • For lineage, say direction: "What feeds INTO silver.risk_enriched" or
    "What does gold.capital_charge feed INTO"
  • The AI shows source cards below each answer — these tell you which catalog
    chunks, schema docs, or lineage edges it retrieved to generate the answer
  • If the answer seems incomplete, ask a follow-up — the AI maintains context
    across the conversation

━━━ What the AI will decline to answer ━━━
  • Anything completely unrelated to RiskLens, FRTB, or financial data
  • Requests to modify data (it is read-only)
  • Personally identifiable information beyond what is in the catalog
"""),

]


def build_ui_docs() -> list[ChunkDoc]:
    """Return all UI documentation chunks for indexing."""
    return UI_CHUNKS
