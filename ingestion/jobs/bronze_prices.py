"""
RiskLens — Bronze Layer: Yahoo Finance Price Ingestion
Fetches daily OHLCV prices for instruments matching the DTCC trade universe.
Lands raw data as-is into BigQuery bronze layer. No transforms.

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
from datetime import datetime, timedelta

import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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


def fetch_ticker(ticker: str, start: datetime, end: datetime) -> list[dict]:
    """Fetch OHLCV for a single ticker via yfinance."""
    try:
        meta = INSTRUMENTS[ticker]
        hist = yf.download(
            ticker,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=False,
        )

        if hist.empty:
            log.warning(f"  No data for {ticker}")
            return []

        rows = []
        for date, row in hist.iterrows():
            rows.append({
                "ticker":      ticker,
                "name":        meta["name"],
                "asset_class": meta["asset_class"],
                "currency":    meta["currency"],
                "date":        date.date(),
                "open":        float(row["Open"])      if row["Open"]      == row["Open"] else None,
                "high":        float(row["High"])      if row["High"]      == row["High"] else None,
                "low":         float(row["Low"])       if row["Low"]       == row["Low"]  else None,
                "close":       float(row["Close"])     if row["Close"]     == row["Close"]else None,
                "adj_close":   float(row["Adj Close"]) if row["Adj Close"] == row["Adj Close"] else None,
                "volume":      int(row["Volume"])      if row["Volume"]    == row["Volume"]else None,
                "ingested_at": datetime.utcnow(),
                "trade_date":  end.strftime("%Y-%m-%d"),
            })

        log.info(f"  {ticker}: {len(rows)} trading days")
        return rows

    except Exception as e:
        log.warning(f"  Failed to fetch {ticker}: {e}")
        return []


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

    log.info(f"Fetching prices for {len(INSTRUMENTS)} instruments from {start_date.date()} to {end_date.date()}")

    all_rows = []
    for ticker in INSTRUMENTS:
        rows = fetch_ticker(ticker, start_date, end_date)
        all_rows.extend(rows)

    if not all_rows:
        log.warning("No price data fetched.")
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
