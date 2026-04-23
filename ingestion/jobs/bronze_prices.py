"""
RiskLens — Bronze Layer: Yahoo Finance Price Ingestion
Fetches daily OHLCV prices for instruments matching the DTCC trade universe.
Lands raw data as-is into BigQuery bronze layer. No transforms.

Rate-limit mitigation:
  - Single batched yf.download() call for all tickers (much more polite than
    one HTTP request per ticker).
  - 1-second sleep between per-ticker fallback calls if the batch returns empty.
  - 3-attempt exponential backoff (5s → 10s → 20s) on any Exception.
  - Zero-row guard: raises on weekdays with no data so the pipeline fails loudly.

Instruments fetched:
  Equities  : JPM, GS, BAC, C, MS, BARC.L, DB
  ETFs/Index: SPY, EFA, HYG, LQD
  FX        : EURUSD=X, GBPUSD=X, JPYUSD=X
  Commodities: GC=F (Gold), CL=F (WTI), BZ=F (Brent), NG=F (Nat Gas)
  Vol       : ^VIX

Usage (local):
    python ingestion/jobs/bronze_prices.py \
        --project risklens-frtb-2026 \
        --bucket risklens-raw-risklens-frtb-2026 \
        --days 30

Usage (Dataproc):
    Submitted via refresh_data.sh
"""

import argparse
import logging
import time
from datetime import datetime, timedelta

import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, LongType, StringType,
                                StructField, StructType, TimestampType)

try:
    import google.cloud.logging as _cloud_logging
    _cloud_logging.Client().setup_logging(
        log_level=logging.INFO,
        labels={"app": "risklens", "service": "ingestion", "layer": "bronze", "job": "bronze_prices"},
    )
except Exception:
    logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bronze_prices")

INSTRUMENTS = {
    # Equities
    "JPM":    {"name": "JPMorgan Chase",      "asset_class": "equity",     "currency": "USD"},
    "GS":     {"name": "Goldman Sachs",        "asset_class": "equity",     "currency": "USD"},
    "BAC":    {"name": "Bank of America",      "asset_class": "equity",     "currency": "USD"},
    "C":      {"name": "Citigroup",            "asset_class": "equity",     "currency": "USD"},
    "MS":     {"name": "Morgan Stanley",       "asset_class": "equity",     "currency": "USD"},
    "BARC.L": {"name": "Barclays",             "asset_class": "equity",     "currency": "GBP"},
    "DB":     {"name": "Deutsche Bank",        "asset_class": "equity",     "currency": "EUR"},
    # ETFs / Credit proxies
    "SPY":    {"name": "S&P 500 ETF",          "asset_class": "etf",        "currency": "USD"},
    "HYG":    {"name": "HY Corporate Bond ETF","asset_class": "etf",        "currency": "USD"},
    "LQD":    {"name": "IG Corporate Bond ETF","asset_class": "etf",        "currency": "USD"},
    # FX
    "EURUSD=X":{"name": "EUR/USD",             "asset_class": "fx",         "currency": "USD"},
    "GBPUSD=X":{"name": "GBP/USD",             "asset_class": "fx",         "currency": "USD"},
    "JPYUSD=X":{"name": "JPY/USD",             "asset_class": "fx",         "currency": "USD"},
    # Commodities
    "GC=F":   {"name": "Gold Futures",         "asset_class": "commodity",  "currency": "USD"},
    "CL=F":   {"name": "WTI Crude Oil",        "asset_class": "commodity",  "currency": "USD"},
    "BZ=F":   {"name": "Brent Crude Oil",      "asset_class": "commodity",  "currency": "USD"},
    "NG=F":   {"name": "Natural Gas Futures",  "asset_class": "commodity",  "currency": "USD"},
    # Volatility
    "^VIX":   {"name": "CBOE VIX",             "asset_class": "volatility", "currency": "USD"},
}

BRONZE_SCHEMA = StructType([
    StructField("ticker",       StringType(),   True),
    StructField("name",         StringType(),   True),
    StructField("asset_class",  StringType(),   True),
    StructField("currency",     StringType(),   True),
    StructField("date",         DateType(),     True),
    StructField("open",         DoubleType(),   True),
    StructField("high",         DoubleType(),   True),
    StructField("low",          DoubleType(),   True),
    StructField("close",        DoubleType(),   True),
    StructField("adj_close",    DoubleType(),   True),
    StructField("volume",       LongType(),     True),
    StructField("ingested_at",  TimestampType(),True),
    StructField("trade_date",   StringType(),   True),
])


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if f != f else f   # NaN check
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        f = float(val)
        return None if f != f else int(f)
    except (TypeError, ValueError):
        return None


def _rows_from_hist(ticker: str, hist, end_date: datetime) -> list[dict]:
    """Convert a yfinance history DataFrame to a list of row dicts."""
    if hist is None or hist.empty:
        return []
    meta = INSTRUMENTS[ticker]
    now  = datetime.utcnow()
    rows = []
    for date, row in hist.iterrows():
        # yfinance ≥0.2 uses auto_adjust=True so "Adj Close" may not exist
        try:
            adj_close = _safe_float(row.get("Adj Close", row.get("Close")))
        except Exception:
            adj_close = _safe_float(row.get("Close"))
        rows.append({
            "ticker":      ticker,
            "name":        meta["name"],
            "asset_class": meta["asset_class"],
            "currency":    meta["currency"],
            "date":        date.date(),
            "open":        _safe_float(row.get("Open")),
            "high":        _safe_float(row.get("High")),
            "low":         _safe_float(row.get("Low")),
            "close":       _safe_float(row.get("Close")),
            "adj_close":   adj_close,
            "volume":      _safe_int(row.get("Volume")),
            "ingested_at": now,
            "trade_date":  end_date.strftime("%Y-%m-%d"),
        })
    return rows


def fetch_all_tickers_batched(tickers: list[str], start: datetime, end: datetime,
                               max_attempts: int = 3) -> list[dict]:
    """
    Download all tickers in a single batched yf.download() call.
    Falls back to per-ticker calls (with 1s sleep) if batch returns empty.
    Retries up to max_attempts with exponential backoff (5s → 10s → 20s).
    """
    start_str = start.strftime("%Y-%m-%d")
    end_str   = end.strftime("%Y-%m-%d")

    # ── Attempt batched download ──────────────────────────────────────────────
    backoff = 5
    for attempt in range(1, max_attempts + 1):
        try:
            log.info(f"Batched yf.download attempt {attempt}/{max_attempts}: "
                     f"{len(tickers)} tickers from {start_str} to {end_str}")
            raw = yf.download(
                tickers,
                start=start_str,
                end=end_str,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=False,   # single-threaded: gentler on rate limits
            )
            break
        except Exception as e:
            log.warning(f"  Batch download attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                log.info(f"  Sleeping {backoff}s before retry…")
                time.sleep(backoff)
                backoff *= 2
            else:
                log.error("  All batch download attempts exhausted — falling back to per-ticker.")
                raw = None

    all_rows: list[dict] = []

    if raw is not None and not raw.empty:
        # Multi-ticker download: columns are (field, ticker) MultiIndex
        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    hist = raw
                else:
                    hist = raw[ticker] if ticker in raw.columns.get_level_values(1) else None
                rows = _rows_from_hist(ticker, hist, end)
                if rows:
                    log.info(f"  {ticker}: {len(rows)} rows (batched)")
                    all_rows.extend(rows)
                else:
                    log.warning(f"  {ticker}: no data in batch result")
            except Exception as e:
                log.warning(f"  {ticker}: error extracting from batch result: {e}")
    else:
        log.warning("  Batch download returned empty — falling back to per-ticker calls.")

    # ── Per-ticker fallback for any tickers still missing ────────────────────
    fetched_tickers = {r["ticker"] for r in all_rows}
    missing = [t for t in tickers if t not in fetched_tickers]

    if missing:
        log.info(f"  Falling back to per-ticker for {len(missing)} tickers: {missing}")
        for ticker in missing:
            fallback_backoff = 5
            for attempt in range(1, max_attempts + 1):
                try:
                    time.sleep(1)   # be polite between individual requests
                    hist = yf.download(
                        ticker,
                        start=start_str,
                        end=end_str,
                        auto_adjust=True,
                        progress=False,
                    )
                    rows = _rows_from_hist(ticker, hist, end)
                    if rows:
                        log.info(f"  {ticker}: {len(rows)} rows (per-ticker fallback, attempt {attempt})")
                        all_rows.extend(rows)
                    else:
                        log.warning(f"  {ticker}: still no data on attempt {attempt}")
                    break
                except Exception as e:
                    log.warning(f"  {ticker} attempt {attempt} failed: {e}")
                    if attempt < max_attempts:
                        log.info(f"  Sleeping {fallback_backoff}s before retry…")
                        time.sleep(fallback_backoff)
                        fallback_backoff *= 2
                    else:
                        log.error(f"  {ticker}: all {max_attempts} attempts failed — skipping.")

    return all_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--days",    type=int, default=1)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("RiskLens-Bronze-Prices")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=args.days + 5)  # buffer for non-trading days

    log.info(f"Fetching prices for {len(INSTRUMENTS)} instruments "
             f"from {start_date.date()} to {end_date.date()}")

    tickers  = list(INSTRUMENTS.keys())
    all_rows = fetch_all_tickers_batched(tickers, start_date, end_date)

    if not all_rows:
        # Zero-row guard: fail loudly on weekdays
        today = datetime.utcnow()
        if today.weekday() < 5:
            raise RuntimeError(
                f"bronze_prices: Zero rows fetched for weekday {today.strftime('%Y-%m-%d')}. "
                "Yahoo Finance may be rate-limiting or down. "
                "Investigate — pipeline must not silently write nothing."
            )
        log.warning("No price data fetched (weekend/holiday — acceptable).")
        spark.stop()
        return

    df = spark.createDataFrame(all_rows, schema=BRONZE_SCHEMA)
    row_count = df.count()
    log.info(f"Writing {row_count:,} rows to BigQuery risklens_bronze.prices_r")

    (
        df.write
        .format("bigquery")
        .option("project",          args.project)
        .option("dataset",          "risklens_bronze")
        .option("table",            "prices_r")
        .option("writeMethod",      "indirect")
        .option("temporaryGcsBucket", args.bucket)
        .option("partitionField",   "date")
        .option("partitionType",    "DAY")
        .option("clusteredFields",  "ticker,asset_class,currency")
        .mode("append")
        .save()
    )

    log.info(f"Done: {row_count:,} rows written to risklens_bronze.prices_r")
    spark.stop()


if __name__ == "__main__":
    main()
