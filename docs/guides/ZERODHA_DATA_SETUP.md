# Zerodha Data Setup Guide

**Data Source**: Zerodha historical equity data (2015-2025)
**Repository**: https://github.com/bh1rg1v/algorithmic-trading
**Jio Cloud Link**: https://www.jioaicloud.com/l/?u=nJeSTwHnU5GtuaLD7aYu97WZUO0E-HJCtLqWE-q4gD3VbsX1gBXZVMyTO5OGzLd-hkW

---

## Quick Start

### Automated Download (Recommended)

```bash
# Download Zerodha data from Jio Cloud
doppler run -- uv run python scripts/download_zerodha_data.py
```

**What it does**:
1. Opens browser to Jio Cloud page
2. Attempts to find and click download button
3. Downloads to `data/vendor/zerodha/`
4. Supports daily data (0.2 GB) or full dataset

---

## Manual Download (If Automation Fails)

### Step 1: Visit Jio Cloud Link

Open: https://www.jioaicloud.com/l/?u=nJeSTwHnU5GtuaLD7aYu97WZUO0E-HJCtLqWE-q4gD3VbsX1gBXZVMyTO5OGzLd-hkW

### Step 2: Download Data

Look for:
- **Daily data**: `zerodha_daily.zip` (~0.2 GB) - **START WITH THIS**
- **Minute data**: `zerodha_minute.zip` (larger)

### Step 3: Save to Project

Save downloaded ZIP file to:
```
data/vendor/zerodha/
```

---

## Ingest Zerodha Data

### Extract Files

```bash
cd data/vendor/zerodha
unzip zerodha_daily.zip
```

### Ingest Daily Candles

```bash
# Ingest all daily data
doppler run -- uv run python scripts/ingest_vendor_candles.py data/vendor/zerodha/day --timeframe day --vendor zerodha
```

### Ingest Minute Candles (Optional)

```bash
# Ingest and aggregate minute data to daily
doppler run -- uv run python scripts/ingest_vendor_candles.py data/vendor/zerodha/minute --timeframe minute --vendor zerodha
```

---

## Data Format

### Expected Structure

```
data/vendor/zerodha/
├── day/                  # Daily OHLCV
│   ├── INFY.csv
│   ├── RELIANCE.csv
│   ├── TCS.csv
│   └── ...
└── minute/               # Minute OHLCV (optional)
    ├── INFY.csv
    ├── RELIANCE.csv
    └── ...
```

### CSV Format

Each CSV file should have columns:
- `Date` or `Datetime`
- `Open`
- `High`
- `Low`
- `Close`
- `Volume`

Example:
```csv
Date,Open,High,Low,Close,Volume
2015-01-01,1000.0,1010.0,995.0,1005.0,1000000
2015-01-02,1005.0,1020.0,1000.0,1015.0,1200000
```

---

## Troubleshooting

### Download Fails Automatically

1. Run the script again with browser visible (`headless=False` in script)
2. Manually download when browser opens
3. Save to `data/vendor/zerodha/`

### Ingestion Fails

1. Check CSV format matches expected columns
2. Ensure files are extracted from ZIP
3. Run with `--dry-run` flag to test parsing:
   ```bash
   doppler run -- uv run python scripts/ingest_vendor_candles.py data/vendor/zerodha/day --timeframe day --dry-run
   ```

---

## Data Coverage

- **Period**: 2015 to 2025
- **Symbols**: All NSE equities
- **Data Type**: OHLCV (daily and minute)
- **Source**: Zerodha (reliable broker data)

---

## Next Steps After Ingestion

Once data is ingested:

```bash
# 1. Run daily pipeline for any date
doppler run -- uv run python scripts/run_daily_pipeline.py 2024-02-07

# 2. View in dashboard
doppler run -- uv run nseml-dashboard

# 3. Run scans
doppler run -- uv run python -m nse_momentum_lab.services.scan.worker 2024-02-07
```

---

**Questions?**

- Daily data is sufficient for Phase 1 (momentum, scans, paper ledger)
- Minute data is optional but useful for liquidity/slippage modeling
- The ingest script supports both daily and intraday data
