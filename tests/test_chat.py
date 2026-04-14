"""
RiskLens — AI Chat Integration Tests
Tests the /api/chat endpoint against the live GKE deployment.

Usage:
    # Against live GKE
    python tests/test_chat.py

    # Against local API (docker or uvicorn)
    API_BASE=http://localhost:8000 python tests/test_chat.py

    # Save results to file
    python tests/test_chat.py --output test_results.json

Each test case is a dict with:
    prompt   : str   — the chat query
    category : str   — frtb | table | breach_sla | lineage | offtopic
    expect   : str   — "answer" | "decline"  (what we expect back)
    keywords : list  — strings that should appear in a passing answer (optional)
"""

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx

# ── Config ────────────────────────────────────────────────────────────────────

API_BASE = os.environ.get("API_BASE", "http://34.102.203.211")
CHAT_URL  = f"{API_BASE}/api/chat"
TIMEOUT   = 60  # seconds per prompt

# ── Test cases ────────────────────────────────────────────────────────────────

TEST_CASES = [
    # ── FRTB / regulatory questions ──────────────────────────────────────────
    {
        "id": "frtb_01",
        "category": "frtb",
        "expect": "answer",
        "prompt": "What is the FRTB back-testing requirement and what are the traffic light zones?",
        "keywords": ["backtesting", "exceptions", "green", "amber", "red"],
    },
    {
        "id": "frtb_02",
        "category": "frtb",
        "expect": "answer",
        "prompt": "Explain the difference between VaR 99% 1-day and Expected Shortfall 97.5%. Which one does FRTB IMA use?",
        "keywords": ["expected shortfall", "IMA", "99", "97.5"],
    },
    {
        "id": "frtb_03",
        "category": "frtb",
        "expect": "answer",
        "prompt": "What is the capital charge multiplier and how does the traffic light zone affect it?",
        "keywords": ["capital", "multiplier", "zone"],
    },
    {
        "id": "frtb_04",
        "category": "frtb",
        "expect": "answer",
        "prompt": "What is the P&L Attribution Test (PLAT) and what metrics does it measure?",
        "keywords": ["plat", "attribution", "spearman", "upl"],
    },
    {
        "id": "frtb_05",
        "category": "frtb",
        "expect": "answer",
        "prompt": "What is RFET (Risk Factor Eligibility Test) and how many observations are needed?",
        "keywords": ["rfet", "observations", "eligible"],
    },

    # ── Table-level questions ─────────────────────────────────────────────────
    {
        "id": "table_01",
        "category": "table",
        "expect": "answer",
        "prompt": "What columns does the gold.backtesting table contain?",
        "keywords": ["desk", "calc_date", "var"],
    },
    {
        "id": "table_02",
        "category": "table",
        "expect": "answer",
        "prompt": "What is the purpose of silver.risk_enriched and what market context does it add?",
        "keywords": ["sofr", "vix", "hy_spread", "risk_enriched"],
    },
    {
        "id": "table_03",
        "category": "table",
        "expect": "answer",
        "prompt": "What tables are in the gold layer and what does each one represent?",
        "keywords": ["gold", "backtesting", "capital_charge"],
    },

    # ── Failures, breaches, and SLA ───────────────────────────────────────────
    {
        "id": "breach_01",
        "category": "breach_sla",
        "expect": "answer",
        "prompt": "Which desks are currently in the red traffic light zone for back-testing exceptions?",
        "keywords": [],  # answer depends on live data; just check it responds
    },
    {
        "id": "breach_02",
        "category": "breach_sla",
        "expect": "answer",
        "prompt": "Which risk factors failed the RFET eligibility test?",
        "keywords": ["rfet", "fail"],
    },
    {
        "id": "breach_03",
        "category": "breach_sla",
        "expect": "answer",
        "prompt": "Are there any SLA breaches or data quality issues in the current pipeline run?",
        "keywords": [],  # answer depends on live data
    },

    # ── Lineage questions ─────────────────────────────────────────────────────
    {
        "id": "lineage_01",
        "category": "lineage",
        "expect": "answer",
        "prompt": "What is the data lineage of gold.capital_charge? What tables feed into it?",
        "keywords": ["silver", "risk_enriched", "gold"],
    },
    {
        "id": "lineage_02",
        "category": "lineage",
        "expect": "answer",
        "prompt": "Trace the lineage from DTCC raw trade data to gold.trade_positions.",
        "keywords": ["bronze", "silver", "trades"],
    },

    # ── Off-topic queries (should be declined) ────────────────────────────────
    {
        "id": "offtopic_01",
        "category": "offtopic",
        "expect": "decline",
        "prompt": "What is the capital of France?",
        "keywords": ["outside", "specialized", "frtb"],  # decline message keywords
    },
    {
        "id": "offtopic_02",
        "category": "offtopic",
        "expect": "decline",
        "prompt": "Give me a recipe for chocolate cake.",
        "keywords": [],
    },
    {
        "id": "offtopic_03",
        "category": "offtopic",
        "expect": "decline",
        "prompt": "Who won the FIFA World Cup in 2022?",
        "keywords": [],
    },
    {
        "id": "offtopic_04",
        "category": "offtopic",
        "expect": "decline",
        "prompt": "Write a Python function to sort a list of numbers.",
        "keywords": [],
    },
]

# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class TestResult:
    test_id: str
    category: str
    prompt: str
    expect: str
    passed: bool
    answer: str
    sources_count: int
    latency_s: float
    failure_reason: Optional[str] = None
    keywords_matched: list = field(default_factory=list)
    keywords_missing: list = field(default_factory=list)


# ── HTTP helpers ──────────────────────────────────────────────────────────────

async def call_chat(prompt: str) -> tuple[str, int, float]:
    """
    Call /api/chat and collect the full streamed response.
    Returns (full_answer_text, sources_count, latency_seconds).
    """
    t0 = time.monotonic()
    full_text = ""
    sources_count = 0

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        async with client.stream(
            "POST",
            CHAT_URL,
            json={"query": prompt, "top_k": 8},
            headers={"Accept": "text/event-stream"},
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                if payload == "__done__":
                    break
                if payload.startswith("__sources__"):
                    try:
                        sources = json.loads(payload[len("__sources__"):])
                        sources_count = len(sources)
                    except json.JSONDecodeError:
                        pass
                else:
                    try:
                        full_text += json.loads(payload)
                    except json.JSONDecodeError:
                        full_text += payload

    latency = time.monotonic() - t0
    return full_text.strip(), sources_count, latency


# ── Test runner ───────────────────────────────────────────────────────────────

DECLINE_MARKERS = [
    "outside my area",
    "i'm risklens ai",
    "specialized in frtb",
    "please ask a question",
    "area of expertise",
]


def _is_decline(text: str) -> bool:
    t = text.lower()
    return any(m in t for m in DECLINE_MARKERS)


async def run_test(tc: dict) -> TestResult:
    try:
        answer, sources_count, latency = await call_chat(tc["prompt"])
    except Exception as e:
        return TestResult(
            test_id=tc["id"],
            category=tc["category"],
            prompt=tc["prompt"],
            expect=tc["expect"],
            passed=False,
            answer="",
            sources_count=0,
            latency_s=0.0,
            failure_reason=f"HTTP error: {e}",
        )

    declined = _is_decline(answer)
    keywords = tc.get("keywords", [])
    matched = [k for k in keywords if k.lower() in answer.lower()]
    missing = [k for k in keywords if k.lower() not in answer.lower()]

    if tc["expect"] == "decline":
        passed = declined
        failure_reason = None if passed else "Expected decline but got an answer"
    else:
        passed = not declined and (not keywords or len(matched) > 0)
        failure_reason = None if passed else (
            "Got decline instead of answer" if declined
            else f"Missing keywords: {missing}"
        )

    return TestResult(
        test_id=tc["id"],
        category=tc["category"],
        prompt=tc["prompt"],
        expect=tc["expect"],
        passed=passed,
        answer=answer[:300],
        sources_count=sources_count,
        latency_s=round(latency, 2),
        failure_reason=failure_reason,
        keywords_matched=matched,
        keywords_missing=missing,
    )


async def run_all(output_path: Optional[str] = None) -> int:
    """Run all test cases. Returns exit code (0=all pass, 1=failures)."""
    print(f"\nRiskLens Chat Tests  |  {CHAT_URL}")
    print("=" * 70)

    results: list[TestResult] = []
    categories = {}

    for tc in TEST_CASES:
        print(f"  [{tc['id']}] {tc['prompt'][:60]}... ", end="", flush=True)
        result = await run_test(tc)
        results.append(result)

        status = "PASS" if result.passed else "FAIL"
        print(f"{status}  ({result.latency_s}s, {result.sources_count} sources)")
        if not result.passed:
            print(f"         Reason: {result.failure_reason}")
        if result.passed and result.keywords_missing:
            print(f"         Matched: {result.keywords_matched}")

        cat = tc["category"]
        categories.setdefault(cat, {"pass": 0, "fail": 0})
        if result.passed:
            categories[cat]["pass"] += 1
        else:
            categories[cat]["fail"] += 1

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    print("\n" + "=" * 70)
    print(f"Results: {passed}/{total} passed\n")
    for cat, counts in categories.items():
        t = counts["pass"] + counts["fail"]
        print(f"  {cat:15s}  {counts['pass']}/{t}")

    if output_path:
        data = [
            {
                "test_id": r.test_id,
                "category": r.category,
                "prompt": r.prompt,
                "expect": r.expect,
                "passed": r.passed,
                "answer_preview": r.answer,
                "sources_count": r.sources_count,
                "latency_s": r.latency_s,
                "failure_reason": r.failure_reason,
                "keywords_matched": r.keywords_matched,
                "keywords_missing": r.keywords_missing,
            }
            for r in results
        ]
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\nResults saved → {output_path}")

    return 0 if passed == total else 1


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RiskLens chat integration tests")
    parser.add_argument("--output", help="Save results as JSON to this path")
    args = parser.parse_args()
    sys.exit(asyncio.run(run_all(output_path=args.output)))


if __name__ == "__main__":
    main()
