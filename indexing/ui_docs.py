"""
RiskLens — UI Documentation Chunks
Describes every screen, component, control, and interaction in the RiskLens
application so the AI assistant can answer user questions about the UI.

Each ChunkDoc covers one topic: a tab, a control, a badge, a workflow, or
a concrete usage example.  These are fed into the BM25 + vector index at
indexing time (source_type = "ui_doc").
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
RiskLens Application Overview
RiskLens is a browser-based FRTB Data Catalog and AI Lineage Explorer.

Navigation: a collapsible sidebar on the left contains five main tabs:
  1. Catalog      — browse and search all data assets
  2. Governance   — SLA health, data quality scores, and team ownership
  3. Assets       — card grid of gold-layer (business-ready) tables
  4. Lineage      — interactive data-flow graph between assets
  5. FRTB Risk    — live risk dashboard (ES, capital charge, back-testing, PLAT, RFET)

The sidebar collapses to icon-only mode on small screens. Click the → / ←
arrow button to toggle it. Each nav item has an icon; hover shows the label
when collapsed.

At the bottom of the sidebar is the "Ask RiskLens AI" button (✦).
Clicking it opens the AI Chat panel on the right side of the screen.
"""),

    _doc("chat_panel", """
AI Chat Panel — How to Use
The AI Chat panel opens when you click the "✦ Ask RiskLens AI" button in the
bottom of the sidebar.  It slides in from the right as a 480px-wide panel.

Features:
  • Type any question in the text box at the bottom and press Enter (or click ↑).
  • Shift+Enter inserts a line break without sending.
  • The assistant streams its reply token-by-token in real time.
  • After each answer, coloured source cards show which catalog chunks
    were retrieved: asset descriptions, schema docs, and lineage nodes.
  • Chat history is preserved in sessionStorage — it survives page navigation
    but is cleared when you close the browser tab.
  • Click "Clear" (top of panel) to wipe the conversation.
  • Click ✕ to close the panel (history is kept until you clear or close the tab).

Example questions you can ask:
  - "What columns does gold.backtesting contain?"
  - "Which desks are in the red traffic-light zone?"
  - "What is the data lineage of gold.capital_charge?"
  - "Explain RFET and how many observations are needed."
  - "What does the Hops control do on the Lineage tab?"
  - "Show me the latest ES values by desk."

The panel also shows four suggested starter questions when the conversation
is empty — click any to send instantly.
"""),

    # ── Catalog tab ───────────────────────────────────────────────────────────

    _doc("catalog_tab", """
Catalog Tab — Browsing Data Assets
The Catalog tab is the home screen (route "/").  It shows every data asset
(table, feed, model, or report) registered in the RiskLens catalog.

Layout: a filterable table on the left; a detail drawer slides in on the right
when you click a row.

Table columns:
  Name        — asset name + short description (truncated)
  Domain      — colour-coded domain badge (risk, market_data, reference, regulatory)
  Layer       — bronze / silver / gold badge
  Owner       — person responsible for the asset
  Freshness   — FRESH / STALE / CRITICAL badge
  SLA         — "On time" (green) or "SLA breach" (red)
  Rows        — row count formatted with thousand separators

Filters (above the table):
  • Search box  — type any text to filter by name or description (client-side,
    instant, no network round-trip)
  • Domain dropdown — filter by risk / market_data / reference / regulatory
  • Layer dropdown  — filter by bronze / silver / gold
  • Type dropdown   — filter by table / feed / model / report

Example: to find all gold-layer risk tables, set Layer = gold and Domain = risk.
The count updates automatically.

Interactions:
  • Click any row → opens the Asset Detail Drawer on the right
  • Click the same row again → closes the drawer
  • The selected row is highlighted in dark blue
"""),

    _doc("asset_drawer", """
Asset Detail Drawer — Deep-dive on a Single Asset
Clicking a row in the Catalog table opens a 384px drawer on the right.

Sections inside the drawer:

Overview
  • Full asset name (monospace font)
  • Type, Domain badge, Layer badge
  • Full description
  • Tags (e.g. frtb, risk, backtesting)

Ownership
  • Owner name and team
  • Data steward and email (clickable mailto link)

Quality
  • Freshness status badge (FRESH / STALE / CRITICAL)
  • Null rate % — percentage of cells that are null
  • Duplicate rate % — percentage of duplicate rows
  • Schema drift indicator — ✓ stable or ⚠ drifted

SLA
  • SLA breach badge
  • Breach duration in minutes (if breached)

Schema
  • Table of all columns: column name, data type, nullable (✓ / —)
  • Scroll if the table has many columns

"View Lineage →" button at the bottom navigates to the Lineage tab
pre-loaded with this asset as the root node.

Example: click on "gold.backtesting" → drawer shows it has columns
desk, calc_date, var_99_1d, hyp_pnl, act_pnl, hyp_exception, act_exception,
exceptions_250d, traffic_light_zone, multiplier, processed_at.
"""),

    # ── Lineage tab ───────────────────────────────────────────────────────────

    _doc("lineage_tab", """
Lineage Tab — Interactive Data-Flow Graph
The Lineage tab (route "/lineage/:assetId") renders a directed acyclic graph
showing how data flows between assets in the RiskLens pipeline.

Opening the graph:
  • From the Catalog drawer, click "View Lineage →"
  • From the Assets tab, hover a card and click "View lineage →"
  • Navigate directly: /lineage/gold_capital_charge

What you see:
  • Nodes — each box is a data asset (table, source, pipeline job)
  • Edges — arrows show the direction of data flow (left → right)
  • Colour coding by layer:
      Bronze nodes — amber/orange border — raw ingested data
      Silver nodes — blue border         — cleaned/transformed data
      Gold nodes   — yellow border       — aggregated business-ready data
      Reference    — purple border       — lookup/master data
      Source       — green border        — external API feeds
      Pipeline     — slate border        — Spark jobs
  • Layout is left-to-right (upstream on left, downstream on right)

Canvas controls:
  • Pan — click and drag the background
  • Zoom — scroll wheel or pinch; zoom buttons (+ / −) bottom-left
  • Fit — the "fit" button (⊡) bottom-left resets to show the full graph
  • MiniMap — bottom-right corner; drag the viewport rectangle to pan quickly
"""),

    _doc("lineage_hops", """
Lineage Hops Control — Adjusting Graph Depth
The Hops control is a row of four buttons (1 · 2 · 3 · 4) in the top-right
corner of the Lineage tab header.

What "hops" means:
  A hop is one step of traversal in the lineage graph — one upstream parent
  or one downstream child from the currently selected (root) asset.

  Hop 1 → show only the direct parents and direct children of the root node.
  Hop 2 → show parents of parents + children of children (2 levels each way).
  Hop 3 → 3 levels upstream and downstream.
  Hop 4 → full pipeline ancestry — shows all the way back to raw sources and
           forward to final reports.

The active hop count is highlighted with the brand colour.
Default on page load is Hop 2.

Example:
  Root = gold.capital_charge, Hops = 1:
    shows gold.es_outputs → gold.capital_charge → gold.risk_summary

  Root = gold.capital_charge, Hops = 3:
    shows bronze.fred_rates → silver.rates → silver.risk_enriched →
          gold.es_outputs → gold.capital_charge → gold.risk_summary
    (and all the other parallel paths at each level)

Changing hops re-fetches from the API immediately — no page reload needed.
The stats panel (top-left of the canvas) always shows the current hop radius.
"""),

    _doc("lineage_search", """
Lineage Node Search — Jump to Any Asset
The search input (monospace, w-56) in the Lineage tab header lets you jump
directly to any node in the current graph.

How it works:
  1. Type any part of a node ID (e.g. "capital") or display name ("Capital")
  2. A dropdown appears with up to 8 matching suggestions
  3. Click a suggestion OR press Enter to navigate to that node as the new root
  4. Press Escape to close the dropdown without navigating
  5. The graph re-fetches with the new root and current hops setting

Example: type "silver" → suggestions show all silver-layer nodes:
  silver_rates, silver_prices, silver_risk_outputs, silver_risk_enriched, silver_trades
Click "silver_risk_enriched" → graph reloads with that node as centre.
"""),

    _doc("lineage_node_click", """
Lineage Node Panel — Clicking a Node
Clicking any node in the lineage graph opens the Node Detail Panel in the
top-right of the canvas area.

The panel shows:
  • Node ID  (monospace, e.g. gold_capital_charge)
  • Type     (table / source / pipeline)
  • Domain   (risk / market_data / reference / regulatory)
  • Layer    (bronze / silver / gold)
  • Metadata — any extra key-value pairs stored for that node

Button: "View lineage from here →"
  Clicking this re-centres the graph on the selected node, making it the
  new root.  Useful for drilling into a specific part of the pipeline.

Click anywhere outside the panel (on the canvas background) to dismiss it.
"""),

    _doc("lineage_edge_click", """
Lineage Edge Stories — Clicking an Edge
Clicking an arrow (edge) between two nodes opens the Edge Story Panel at the
bottom-centre of the canvas.

The Edge Story explains the business context of the data flow:
  • Title          — e.g. "Bronze rates feed into Silver rates"
  • What           — plain-English description of the transformation
  • Business impact — why this step matters for FRTB compliance
  • Frequency      — how often the data flows (e.g. "daily")
  • Owner          — team responsible for this pipeline step

Example: click the edge from silver_risk_outputs → silver_risk_enriched:
  Title: "Risk enrichment with market context"
  What:  "Joins cleaned risk outputs with SOFR, VIX, and HY spread from
          silver_rates to produce market-conditioned risk metrics."
  Impact: "Enriched data is the primary input to ES and back-testing gold tables."

Click anywhere on the canvas background to dismiss the panel.
"""),

    # ── Governance tab ────────────────────────────────────────────────────────

    _doc("governance_tab", """
Governance Tab — SLA, Quality, and Ownership
The Governance tab (route "/governance") has three sub-tabs at the top:
  SLA Status | Data Quality | Ownership

SLA Status tab:
  • Summary cards: Total assets, SLA breaches (red if > 0), On time
  • "Breaches only" checkbox — hides all assets that are not breached
  • Table columns: Asset, Domain, Layer, Status (badge), Breach duration (mins), Last checked
  • Example: if gold.backtesting shows "SLA breach" with 45 mins, the data is
    45 minutes overdue compared to its expected refresh time.

Data Quality tab:
  • Bar chart showing freshness distribution across all assets
    (FRESH = green, STALE = yellow, CRITICAL = red)
  • Table columns: Asset, Domain, Freshness badge, Null rate %, Dup rate %, Schema drift
  • Schema drift ✓ = stable schema; ⚠ = column added/removed/type changed
  • Example: silver.risk_enriched shows null_rate = 0.0% and no schema drift.

Ownership tab:
  • Team filter dropdown — select a specific team or "All teams"
  • Table columns: Asset, Domain, Layer, Owner, Team, Steward
  • Example: filter to "Quant Risk" team to see all tables owned by that team.

Interactions:
  • Click sub-tab button → switches view instantly (no reload)
  • Checkbox (SLA) → refetches with breachOnly filter
  • Team dropdown (Ownership) → refetches filtered data
  • Hover chart bar (Quality) → tooltip with exact count
"""),

    # ── Assets tab ────────────────────────────────────────────────────────────

    _doc("assets_tab", """
Assets Tab — Gold Layer Card Grid
The Assets tab (route "/assets") shows only gold-layer (business-ready) assets
in a responsive card grid (1 / 2 / 3 columns depending on screen width).

Header summary cards (4 columns):
  Total assets     — count of gold assets
  Total rows       — sum of all row counts across gold tables
  SLA breaches     — count (orange/red card if > 0)
  Stale/critical   — count of assets with freshness issues (orange if > 0)

Domain filter dropdown — filter cards by domain (risk / regulatory / market_data / all)

Each card shows:
  • Asset name (monospace)
  • Asset type
  • Freshness badge
  • Description (2-line truncated)
  • Domain badge
  • Row count
  • Owner name

Hover a card to reveal the "View lineage →" button in the bottom-right corner.

Interactions:
  • Click card → navigates to Catalog home with this asset highlighted (/?asset=...)
  • Click "View lineage →" (hover reveal) → opens Lineage tab for this asset
  • Click "View lineage →" does NOT also navigate to the catalog (event stops propagation)
  • Select domain filter → reloads cards for that domain
"""),

    # ── FRTB Risk tab ─────────────────────────────────────────────────────────

    _doc("risk_tab_overview", """
FRTB Risk Tab — Overview
The FRTB Risk tab (route "/risk") is the live regulatory dashboard.
It has five sub-tabs: Summary | Capital | Back-testing | PLAT | RFET

Each sub-tab has:
  • KPI cards at the top (key metrics at a glance)
  • A table or chart below with full detail

Traffic-light zone colours used across the tab:
  GREEN  — fewer than 5 back-testing exceptions; multiplier = 3.0×
  AMBER  — 5–9 exceptions; multiplier = 3.0–3.99×
  RED    — 10+ exceptions; multiplier = 4.0× (worst-case capital surcharge)

Interactions shared across tabs:
  • Click sub-tab → instant switch
  • Hover any table row → highlights with darker background
  • Hover chart area → shows tooltip with exact values
"""),

    _doc("risk_summary_subtab", """
FRTB Risk — Summary Sub-tab
The Summary sub-tab provides the firm-wide regulatory overview.

KPI cards (4 columns):
  Firm ES 97.5%       — total Expected Shortfall across all desks (USD M)
  Firm Capital Charge — total regulatory capital required (USD M), highlighted
  Traffic Light       — firm-level zone (GREEN / AMBER / RED), zone-coloured
  PLAT Failing Desks  — count of desks failing the P&L Attribution Test

Trend chart:
  Area chart showing Total Capital Charge and ES 97.5% Scaled over time.
  X-axis = calculation date; Y-axis = USD millions.
  Two overlapping area fills: one for Capital Charge, one for ES.
  Hover anywhere on the chart for a tooltip with exact values on that date.

Table (desk-level breakdown):
  Desk | Risk Class | ES 97.5% 1d | ES Scaled | Capital Charge | Traffic Light | PLAT | Exceptions (250d)
  Exceptions >= 5 are amber-highlighted.
  PLAT column shows PASS (green) or FAIL (red) badge.

Example: FX desk in GREEN zone with 2 exceptions over 250 days, ES = $12.3M,
capital charge = $36.9M (3.0× multiplier).
"""),

    _doc("risk_capital_subtab", """
FRTB Risk — Capital Sub-tab
Shows the regulatory capital charge breakdown per desk and risk class.

KPI cards (3 columns):
  Total Capital Charge  — firm total (USD M), highlighted in brand colour
  Desks                 — count of desks with capital data
  Max Multiplier        — highest multiplier across all desks (3.0 = GREEN, 4.0 = RED)

Table columns:
  Desk | Risk Class | LH (liquidity horizon, days) | ES 1d | ES Scaled | Reg Floor | Capital Charge | Multiplier | Zone

Liquidity horizon (LH) values by risk class under FRTB IMA:
  GIRR (interest rate) = 10 days
  FX                   = 10 days
  CSR_NS (credit)      = 20 days
  EQ (equity)          = 20 days
  COMM (commodities)   = 20 days

Capital Charge = ES Scaled × Multiplier (minimum 3.0, max 4.0).

Example: Rates desk, GIRR risk class, LH=10, ES_1d=$5.2M, ES_scaled=$16.4M,
multiplier=3.25 (AMBER), capital=$53.3M.
"""),

    _doc("risk_backtesting_subtab", """
FRTB Risk — Back-Testing Sub-tab
Shows VaR back-testing results per desk under BCBS 457.

Controls:
  Calc Date dropdown (top-right) — select which calculation date to view.
  Populated from available dates in the gold.backtesting table.
  Default = most recent date.

KPI cards (3 columns):
  RED zone desks   — alert (red card) if any desks have 10+ exceptions
  AMBER zone desks — alert (amber card) if any desks are in amber
  GREEN zone desks — green card

Table columns:
  Desk | VaR 99% 1d | Hyp PnL | Actual PnL | Hyp Exception | Act Exception |
  Exceptions (250d) | Zone | Multiplier

  Hyp Exception = "Yes" (red) when hypothetical P&L < –VaR
  Act Exception = "Yes" (red) when actual P&L < –VaR
  Exceptions (250d) = rolling count; amber if >= 5

Traffic-light zones (BCBS 457 §351-368):
  GREEN  = 0–4 exceptions in 250 trading days   → multiplier 3.0
  AMBER  = 5–9 exceptions                        → multiplier 3.0–3.99
  RED    = 10+ exceptions                        → multiplier 4.0 (maximum)

Example: Equities desk has 7 exceptions in 250 days → AMBER zone → multiplier 3.5.
"""),

    _doc("risk_plat_subtab", """
FRTB Risk — PLAT Sub-tab (P&L Attribution Test)
Shows whether each desk's risk model can explain its daily P&L.

KPI cards (3 columns):
  Failing desks — alert (red) if > 0 desks fail PLAT
  Passing desks — green
  Avg UPL ratio — firm average unexplained P&L ratio (lower is better, < 0.95)

Table columns:
  Desk | PLAT (PASS/FAIL) | UPL Ratio | UPL Pass | Spearman | Spearman Pass |
  KS Stat | KS Pass | Obs Count | Notes

Three tests (all must pass for PLAT PASS, per BCBS 457 §329–345):
  1. UPL Ratio     — |actual - hypothetical| / std < 0.95
  2. Spearman      — rank correlation between actual & hypothetical P&L > 0.40
  3. KS Stat       — Kolmogorov-Smirnov test statistic < 0.20

Desks failing PLAT face additional capital surcharges and may lose IMA approval.

Example: FX desk UPL ratio = 0.82 ✓, Spearman = 0.71 ✓, KS = 0.14 ✓ → PASS.
         Credit desk Spearman = 0.31 ✗ → FAIL → requires model review.
"""),

    _doc("risk_rfet_subtab", """
FRTB Risk — RFET Sub-tab (Risk Factor Eligibility Test)
Shows which risk factors are eligible to be used in IMA capital models.

Controls:
  Risk Class filter dropdown — filter by GIRR / FX / CSR_NS / EQ / COMM / All

KPI cards (3 columns):
  Failing risk factors — alert (red) if any fail
  Eligible for IMA     — count passing RFET
  Total risk factors   — total count

Table columns:
  Risk Factor ID (mono) | Risk Class | Obs 12m | 12m Pass | Obs 90d | 90d Pass |
  RFET (PASS/FAIL) | IMA Eligible | Staleness (days) | Failure Reason

RFET rules (BCBS 457 §76–80):
  A risk factor passes RFET if EITHER:
    • ≥ 75 real price observations in the past 12 months, OR
    • ≥ 25 real price observations in the past 90 days
  Failed factors cannot be used in IMA models — they must be capitalised
  under the Stress Scenario Risk Measure (SSRM) or a proxy approved by
  the Risk Committee.

Example: USD_LIBOR_3M has 240 obs in 12m → 12m PASS → RFET PASS → IMA Eligible.
         NGAS_HENRY_HUB has only 18 obs in 12m and 6 in 90d → RFET FAIL →
         must use SA capital for this factor.
"""),

    # ── Badges ───────────────────────────────────────────────────────────────

    _doc("badges_and_indicators", """
Badges and Colour Indicators in RiskLens
Various coloured badges appear throughout the application:

Layer badges:
  bronze — amber/orange   — raw ingested data (immutable landing zone)
  silver — blue           — cleaned and validated data
  gold   — yellow/golden  — aggregated, business-ready data

Domain badges:
  risk         — red/rose         — VaR, ES, P&L, back-testing data
  market_data  — blue             — rates, prices, trade inputs
  reference    — purple/violet    — master/lookup tables
  regulatory   — amber/orange     — compliance outputs submitted to regulators
  lineage      — slate/grey       — pipeline metadata

Freshness badges:
  FRESH    — green  — data refreshed within its expected window
  STALE    — yellow — data is late but not critically so
  CRITICAL — red    — data is severely overdue; may affect compliance

SLA badges:
  On time    — green — data delivered within SLA
  SLA breach — red   — data missed its SLA deadline

Traffic-light zone badges (back-testing / capital):
  GREEN — green  — 0–4 exceptions; compliant; multiplier 3.0×
  AMBER — amber  — 5–9 exceptions; under watch; multiplier up to 3.99×
  RED   — red    — 10+ exceptions; non-compliant; multiplier 4.0×

PASS / FAIL badges (PLAT, RFET):
  PASS — emerald/green
  FAIL — red

Breach flag on asset cards / tables:
  "SLA breach" red badge — appears on any asset that missed its refresh SLA.
"""),

    # ── Common workflows ─────────────────────────────────────────────────────

    _doc("workflow_investigate_breach", """
Workflow: Investigating an SLA Breach
Step-by-step guide to finding and understanding a data breach in RiskLens:

1. Open the Governance tab → SLA Status sub-tab.
2. Check the "Breaches only" checkbox to hide on-time assets.
3. Find the breached asset (e.g. gold.backtesting, 45 min overdue).
4. Note the Domain and Layer to understand which pipeline stage is affected.
5. Click the Catalog tab, search for the asset name.
6. Click the row → Asset Detail Drawer opens.
7. Check the Quality section for null_rate or schema_drift anomalies.
8. Click "View Lineage →" to see the upstream pipeline.
9. Set Hops = 3 to see the full upstream chain.
10. Look for upstream silver/bronze nodes — if a source node is stale,
    the downstream gold tables will also be affected.
11. Ask the AI: "Why might gold.backtesting have an SLA breach today?"
"""),

    _doc("workflow_trace_lineage", """
Workflow: Tracing Data Lineage End-to-End
How to trace a gold table all the way back to its raw source:

1. Open the Catalog tab.
2. Click on a gold-layer asset (e.g. gold.capital_charge).
3. In the Asset Drawer, click "View Lineage →".
4. The Lineage tab opens with gold.capital_charge as root, Hops = 2.
5. You will see: gold.es_outputs and gold.backtesting feeding into capital_charge.
6. Click Hops = 4 to expand the full ancestry.
7. Now you see the complete chain:
     FRED API → bronze.fred_rates → silver.rates → silver.risk_enriched
     synthetic → bronze.risk_outputs → silver.risk_outputs → silver.risk_enriched
     silver.risk_enriched → gold.es_outputs → gold.capital_charge
8. Click any node to see its metadata in the Node Panel.
9. Click any arrow to read the Edge Story explaining that transformation step.
10. Use the search input to jump to a specific node (e.g. type "enriched").
11. Ask the AI: "What is the full lineage of gold.capital_charge?"
"""),

    _doc("workflow_check_rfet_failures", """
Workflow: Checking RFET Eligibility Failures
How to find which risk factors cannot be used in IMA models:

1. Open the FRTB Risk tab.
2. Click the RFET sub-tab.
3. The "Failing risk factors" KPI card (top-left) shows the count in red.
4. Use the Risk Class dropdown to filter (e.g. select COMM for commodities).
5. In the table, look for rows with RFET = FAIL (red badge).
6. Check the "Failure Reason" column for explanation.
7. Check "Obs 12m" and "Obs 90d" to see how far below the 75/25 thresholds
   the factor is.
8. Factors with RFET FAIL and IMA Eligible = FAIL must be capitalised under SSRM.
9. Ask the AI: "Which risk factors failed the RFET eligibility test?"
"""),

    _doc("workflow_ask_ai", """
Workflow: Getting the Most from the AI Chat
The RiskLens AI assistant (powered by Claude) can answer questions about
data assets, lineage, FRTB regulations, live risk metrics, and how to use
the application.

Opening the chat: click "✦ Ask RiskLens AI" in the sidebar bottom.

Types of questions it handles well:

Schema questions:
  "What columns does gold.backtesting contain?"
  "What is the difference between gold.risk_summary and gold.es_outputs?"
  "What does the silver.risk_enriched table store?"

Lineage questions:
  "What is the data lineage of gold.capital_charge?"
  "Which pipeline jobs transform data from bronze to silver?"
  "What external data sources feed into the RiskLens pipeline?"

Live data questions (triggers a BigQuery query):
  "Which desks are currently in the red traffic-light zone?"
  "How many rows are in gold.backtesting right now?"
  "Which risk factors failed RFET eligibility?"
  "What is the current capital charge for each desk?"

FRTB regulatory questions:
  "What is RFET and how many observations are required?"
  "Explain the difference between VaR 99% and ES 97.5% under FRTB."
  "What are the traffic-light zones for back-testing?"
  "What is PLAT and what statistical tests does it use?"

UI / navigation questions:
  "What does the Hops control do?"
  "How do I filter the Governance tab by team?"
  "What does a red badge on an asset mean?"
  "How do I trace the lineage of a gold table?"

Tips:
  • Be specific — "ES for the Rates desk today" works better than "show ES"
  • The AI retrieves relevant catalog context before answering, so it knows
    your actual schema and pipeline structure
  • Sources shown below each answer link the retrieved catalog chunks used
"""),

]


def build_ui_docs() -> list[ChunkDoc]:
    """Return all UI documentation chunks for indexing."""
    return UI_CHUNKS
