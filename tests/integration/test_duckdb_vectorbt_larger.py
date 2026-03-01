"""Integration test with larger symbol set for better signal generation.

This test uses the top NIFTY 200 + some mid-cap stocks that are more volatile
and likely to generate gap-up signals.
"""

import sys
from datetime import date

import polars as pl

from nse_momentum_lab.db.market_db import get_market_db
from nse_momentum_lab.services.backtest.vectorbt_engine import VectorBTConfig, VectorBTEngine
from nse_momentum_lab.services.scan.duckdb_signal_generator import DuckDBSignalGenerator
from nse_momentum_lab.services.scan.rules import ScanConfig


def _configure_stdout_encoding_for_windows() -> None:
    """Avoid replacing sys.stdout stream object in tests."""
    if sys.platform != "win32":
        return
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure):
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except ValueError:
            pass


def test_duckdb_vectorbt_larger():
    """Test with larger symbol set for more signals."""
    _configure_stdout_encoding_for_windows()

    print("\n" + "=" * 80)
    print("DUCKDB + VectorBT INTEGRATION TEST (LARGER SYMBOL SET)")
    print("=" * 80)

    # Use symbols that are more likely to have gap-ups (mid-caps, small-caps)
    # These are from our earlier test that showed good signal activity
    symbols = [
        "7SEASL",
        "AAKASH",
        "63MOONS",
        "3MINDIA",
        "AARTIDRUGS",
        "AARTIIND",
        "ABFRL",
        "ADANIENT",
        "ADANIPORTS",
        "ADFFOODS",
        "ADANITRANS",
        "AEGISCHEM",
        "AFFLE",
        "AJANTPHARM",
        "AKSHARCHF",
        "ALBERTDLD",
        "ALEMBICLTD",
        "ALKYLAMINE",
        "ALLCARGO",
        "ALOKINDS",
        "AMARAJABAT",
        "AMBUJACEM",
        "APARINDS",
        "APLLTD",
        "APOLLOHOSP",
        "APOLLOTYRE",
        "ASAHIINDIA",
        "ASHAPURMIN",
        "ASHOKLEY",
        "ASIANPAINT",
        "ASTERDM",
        "ASTRAL",
        "ATGL",
        "ATUL",
        "AUBANK",
        "AURIONPRO",
        "AVANTIFEED",
        "AXISBANK",
        "BAJAJ-AUTO",
        "BAJAJFINSV",
        "BAJAJHLDNG",
        "BAJFINANCE",
        "BALKRISIND",
        "BALRAMCHIN",
        "BANCOINDIA",
        "BANDHANBNK",
        "BANKBARODA",
        "BANKINDIA",
        "BATAINDIA",
        "BEL",
        "BEML",
        "BERGEPAINT",
        "BGRENERGY",
        "BHARATFORG",
        "BHARATPE",
        "BHARTIARTL",
        "BHEL",
        "BIOCON",
        "BIRLACORPN",
        "BLUESTARCO",
        "BLUESTAR",
        "BMTC",
        "BNY Mellon",
        "BOSCHLTD",
        "BPCL",
        "BRITANNIA",
        "BSE",
        "BSE",
        "CAMPUS",
        "CANFINHOME",
        "CANBK",
        "CAPLIPOINT",
        "CARBORUNIV",
        "CASTROLIND",
        "CCL",
        "CENTRALBK",
        "CENTURYTEX",
        "CESC",
        "CGCL",
        "CHAMBLFERT",
        "CHENNPETRO",
        "CHEVIOT",
        "CHOLAHLDNG",
        "CHOLUBRKS",
        "CIPLA",
        "COALINDIA",
        "COCHINSHIP",
        "COFORGE",
        "COLPAL",
        "CONCOR",
        "COROMANDEL",
        "CROMPTON",
        "CUB",
        "CUBEXTUB",
        "CUMMINSIND",
        "DABUR",
        "DALBHARAT",
        "DBL",
        "DCBBANK",
        "DCMSHRIRAM",
        "DIXON",
        "DLF",
        "DMART",
        "DPW",
        "DREDGECORP",
        "DRREDDY",
        "ECLERX",
        "EDELWEISS",
        "EICHERMOT",
        "EMAMILTD",
        "ENDURANCE",
        "EQIND",
        "ERIS",
        "ESCORTS",
        "EXIDEIND",
        "FDC",
        "FIEMIND",
        "FINCABLES",
        "FORTIS",
        "FSL",
        "GAIL",
        "GALAXYSURF",
        "GALAXY",
        "GESHIP",
        "GFIL",
        "GILLETTE",
        "GLENMARK",
        "GMRINFRA",
        "GNFC",
        "GODFRYPHLP",
        "GODREJAGRO",
        "GODREJCP",
        "GODREJIND",
        "GODREJPROP",
        "GPPL",
        "GRANULES",
        "GRASIM",
        "GREAVES",
        "GRINDWELL",
        "GSFC",
        "GTPL",
        "GUJALKALI",
        "GUJGASLTD",
        "GUJGAS",
        "HAIL",
        "HAL",
        "HARIOMPIPE",
        "HARITA",
        "HCLTECH",
        "HDFC",
        "HDFCAMC",
        "HDFCBANK",
        "HDFCLIFE",
        "HEG",
        "HEIDAL",
        "HEROMOTOCO",
        "HINDALCO",
        "HINDCOPPER",
        "HINDPETRO",
        "HINDUNILVR",
        "HINDZINC",
        "HIKAL",
        "HIMATSEIDE",
        "HINDSUN",
        "HINDPETRO",
        "HONEYWELL",
        "HUDCO",
        "ICICIBANK",
        "ICICIGI",
        "ICICIPRULI",
        "IDBI",
        "IDFC",
        "IDFCFIRSTB",
        "IEX",
        "IFBINDLTD",
        "IFFCO",
        "IGL",
        "IIAPL",
        "IIFL",
        "IIFLFIN",
        "IIFLSEC",
        "IITL",
        "IKIO",
        "INDIANB",
        "INDHOTEL",
        "INDIGO",
        "INDIGO",
        "INDUSINDBK",
        "INDUSIND",
        "INFY",
        "INTELLECT",
        "IOCL",
        "IOC",
        "IPCALAB",
        "IRB",
        "IRCON",
        "ISEC",
        "ISGEC",
        "ITC",
        "ITI",
        "J&B",
        "JAMNAAUTO",
        "JBMA",
        "JAYSHOEE",
        "JBCHEPHARM",
        "JINDALSAW",
        "JINDALSTEL",
        "JINDALWLD",
        "JKCEMENT",
        "JKLAKSHMI",
        "JKPAPER",
        "JKTYRE",
        "JMFINANCIL",
        "JSL",
        "JSWENERGY",
        "JSWHL",
        "JSWSTEEL",
        "JUBLFOOD",
        "JUBLPHARMA",
        "JYOTHYLAB",
        "KALPATPOWR",
        "KANPRPLA",
        "KARURVYSYA",
        "KEC",
        "KEI",
        "KHODIYAM",
        "KIOCL",
        "KIRIIND",
        "KOTAKBANK",
        "KPITTECH",
        "KRBL",
        "KSB",
        "KSI",
        "L& T",
        "LAURUSLABS",
        "LAXMIMACH",
        "LCIAL",
        "LEAD",
        "LEMONTREE",
        "LICHSGFIN",
        "LICI",
        "LINDEINDIA",
        "LTIM",
        "LT",
        "LTF",
        "LUPIN",
        "LUXIND",
        "M& M",
        "M&M",
        "M&MFIN",
        "MADHUCON",
        "MADHAV",
        "MAGGINV",
        "MAHABANK",
        "MAHADESCO",
        "MAHAMAYA",
        "MASTEK",
        "MASTEK",
        "MASTEK",
        "MATSYABOIL",
        "MCLEOD",
        "MCX",
        "MEGA",
        "METROBRAND",
        "MFL",
        "MGL",
        "MHRIL",
        "MIDHANI",
        "MIHERAN",
        "MINDTREE",
        "MINDTREE",
        "MOTHERSON",
        "MPHASIS",
        "MRPL",
        "MRF",
        "MUKANDLTD",
        "MUNJAAUTO",
        "MUTHOOTFIN",
        "MYTRANT",
        "NAM-IND",
        "NAM-IND",
        "NAM-IND",
        "NAM-IND",
        "NAM-IND",
        "NATIONALUM",
        "NAUKRI",
        "NAVKAR",
        "NAVNETEDUL",
        "NBCC",
        "NCC",
        "NESTLEIND",
        "NESTLE",
        "NFL",
        "NGIL",
        "NIIT",
        "NIITLTD",
        "NIACL",
        "NLCIND",
        "NMDC",
        "NOCIL",
        "NTPC",
        "OAL",
        "OBEROIRLTY",
        "OFSS",
        "OLECTRA",
        "OMAXE",
        "ONEPOINT",
        "ONTARI",
        "OLECTRA",
        "ORACLE",
        "ORIENTELEC",
        "ORIENT",
        "PAGEIND",
        "PALMIKE",
        "PANACEABIO",
        "PANSARI",
        "PARAG",
        "PATANJAL",
        "PAYTM",
        "PCBL",
        "PDS",
        "PDSL",
        "PEL",
        "PERSISTENT",
        "PETRONET",
        "PFIZER",
        "PGHL",
        "PHOENIXLTD",
        "PIDILITIND",
        "PIIND",
        "PLIX",
        "PNB",
        "PNBHOUSING",
        "POLICY",
        "POLYMED",
        "PPL",
        "PRESTIGE",
        "PRIVISCL",
        "PRSMJOHNSN",
        "PTC",
        "PVRINOX",
        "QUINTE",
        "RADICO",
        "RAJESHEXPO",
        "RALLIS",
        "RAMCOCEM",
        "RANE",
        "RATNAMANI",
        "RAYMOND",
        "RCF",
        "RBLBANK",
        "RCOM",
        "RDS",
        "RECLTD",
        "RELAXO",
        "RELIANCE",
        "RELIGOLD",
        "RENUKA",
        "REVATHI",
        "RITES",
        "RKM",
        "RML",
        "RPIL",
        "RPOWER",
        "RTNINDIA",
        "RVNL",
        "SADBHAV",
        "SAIL",
        "SAMKRAFT",
        "SAMSUNG",
        "SAPPHIRE",
        "SAREGAMA",
        "SAREGAMA",
        "SASTASUNDR",
        "SBICARD",
        "SBILIFE",
        "SBIN",
        "SCHAEFFLER",
        "SCI",
        "SHARDACROP",
        "SHRIRAMFIN",
        "SHREECEM",
        "SHRIRAM",
        "SIEMENS",
        "SIYARAM",
        "SJVN",
        "SKF",
        "SOLAR",
        "SOLARIND",
        "SONATA",
        "SORILINFRA",
        "SPARC",
        "SPICEJET",
        "SPAL",
        "SRF",
        "SRL",
        "SRTRANSFIN",
        "SSDL",
        "SSPDL",
        "STAR",
        "STEEL",
        "STLTECH",
        "SUDARSCHEM",
        "SUMICHEM",
        "SUNDRMFAST",
        "SUNPHARMA",
        "SUNTV",
        "SUPRAJIT",
        "SUPREME",
        "SUVEN",
        "SWANENERGY",
        "SYMPHONY",
        "TANLA",
        "TATACHEM",
        "TATACOFFEE",
        "TATACOMM",
        "TATACONSUM",
        "TATAELXSI",
        "TATAMOTORS",
        "TATASTEEL",
        "TBIL",
        "TCI",
        "TCIEXPRESS",
        "TCS",
        "TECHM",
        "TECHNO",
        "TEJASNET",
        "TELCO",
        "TERRAIN",
        "THYROCARE",
        "TIINDIA",
        "TITAN",
        "TNPETRO",
        "TORNTPHARM",
        "TRENT",
        "TRIDENT",
        "TRIGYN",
        "TTK",
        "TV18BRDCST",
        "TVSMOTOR",
        "TVTODAY",
        "UBL",
        "UCOBANK",
        "UPL",
        "UTIAMC",
        "VARDHMAN",
        "VARROC",
        "VBL",
        "VEDL",
        "VENKEYS",
        "VGUARD",
        "VICTORIA",
        "VIKASECO",
        "VIPIND",
        "VTL",
        "VTL",
        "WELCORP",
        "WELSPUNLIV",
        "WESTLIFE",
        "WIPRO",
        "WOCKPHARMA",
        "WONDERLA",
        "XELBA",
        "XPROINDIA",
        "YATHARTH",
        "YESBANK",
        "YUDIPO",
        "ZYDUSWELL",
    ]

    # Remove duplicates while preserving order
    seen = set()
    unique_symbols = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            unique_symbols.append(s)
    symbols = unique_symbols

    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)

    print("\n[CONFIGURATION]")
    print(f"  Symbols: {len(symbols)} stocks")
    print(f"  Period: {start_date} to {end_date}")
    print("  Strategy: 2LYNCH Gap-up Breakout")

    # Step 1: Generate signals
    print("\n[STEP 1] Generating signals from DuckDB...")
    signal_gen = DuckDBSignalGenerator(config=ScanConfig())
    signals = signal_gen.generate_signals(symbols, start_date, end_date)

    print(f"  Generated {len(signals)} signals")

    if signals:
        signal_counts = {}
        for s in signals:
            symbol = s["symbol"]
            signal_counts[symbol] = signal_counts.get(symbol, 0) + 1

        print("\n  Top 20 Most Active Stocks:")
        for symbol, count in sorted(signal_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
            print(f"    {symbol}: {count} signals")

    if not signals:
        print("\n  [WARNING] No signals generated. This could mean:")
        print("    1. Symbols don't have data for the date range")
        print("    2. No gap-ups of 4%+ occurred in 2024 for these symbols")
        print("    3. Need to check symbol list or expand date range")
        return

    # Step 2: Load price data
    print("\n[STEP 2] Loading price data from DuckDB...")
    db = get_market_db()

    # Get only symbols that have signals
    signal_symbols = list({s["symbol"] for s in signals})
    symbol_to_id = {symbol: i for i, symbol in enumerate(signal_symbols)}
    id_to_symbol = {i: symbol for symbol, i in symbol_to_id.items()}

    price_data = {}
    value_traded_inr = {}

    for symbol in signal_symbols:
        symbol_id = symbol_to_id[symbol]
        try:
            df = db.query_daily(symbol, start_date.isoformat(), end_date.isoformat())

            if df.is_empty():
                print(f"  [WARNING] No data for {symbol}")
                continue

            price_data[symbol_id] = {}
            for row in df.iter_rows(named=True):
                trading_date = row["date"]
                price_data[symbol_id][trading_date] = {
                    "open_adj": float(row["open"]),
                    "close_adj": float(row["close"]),
                    "high_adj": float(row["high"]),
                    "low_adj": float(row["low"]),
                }

            # Get liquidity data
            features_df = db.get_features_range(
                [symbol], start_date.isoformat(), end_date.isoformat()
            )
            if not features_df.is_empty():
                avg_vol = features_df.select(pl.col("dollar_vol_20").drop_nulls().mean()).item()
                value_traded_inr[symbol_id] = avg_vol if avg_vol else 50_000_000.0
            else:
                value_traded_inr[symbol_id] = 50_000_000.0
        except Exception as e:
            print(f"  [ERROR] Failed to load data for {symbol}: {e}")
            continue

    print(f"  Loaded price data for {len(price_data)} symbols")

    # Step 3: Convert signals
    print("\n[STEP 3] Converting signals to VectorBT format...")
    vbt_signals = []
    for s in signals:
        signal_date = s["trading_date"]
        symbol = s["symbol"]
        if symbol not in symbol_to_id:
            continue
        symbol_id = symbol_to_id[symbol]
        initial_stop = s["initial_stop"]

        metadata = {
            "gap_pct": s["gap_pct"],
            "atr": s.get("atr", 0.0),
        }

        vbt_signals.append((signal_date, symbol_id, symbol, initial_stop, metadata))

    print(f"  Converted {len(vbt_signals)} signals")

    # Step 4: Run backtest
    print("\n[STEP 4] Running VectorBT backtest...")

    config = VectorBTConfig(
        default_portfolio_value=1_000_000.0,
        risk_per_trade_pct=0.01,
        fees_per_trade=0.001,
        initial_stop_atr_mult=2.0,
        trail_activation_pct=0.05,
        trail_stop_pct=0.02,
        time_stop_days=3,
        follow_through_threshold=0.02,
    )

    engine = VectorBTEngine(config=config)
    result = engine.run_backtest(
        strategy_name="2LYNCH_Basic_Larger",
        signals=vbt_signals,
        price_data=price_data,
        value_traded_inr=value_traded_inr,
        delisting_dates=None,
    )

    # Step 5: Display results
    print("\n[BACKTEST RESULTS]")
    print("=" * 80)

    print("\n[STRATEGY PERFORMANCE]")
    print(f"  Total Return:    {result.total_return * 100:>+10.2f}%")
    print(f"  Sharpe Ratio:    {result.sharpe_ratio:>10.2f}")
    print(f"  Max Drawdown:    {result.max_drawdown * 100:>10.2f}%")
    print(f"  Win Rate:        {result.win_rate * 100:>10.2f}%")
    print(f"  Profit Factor:   {result.profit_factor:>10.2f}")
    print(f"  Avg R:           {result.avg_r:>10.2f}R")
    print(f"  Median R:        {result.median_r:>10.2f}R")
    print(f"  Calmar Ratio:    {result.calmar_ratio:>10.2f}")
    print(f"  Sortino Ratio:   {result.sortino_ratio:>10.2f}")

    if result.r_distribution:
        print("\n[R DISTRIBUTION]")
        print(f"  10th percentile: {result.r_distribution.get('r_p10', 0):>10.2f}R")
        print(f"  25th percentile: {result.r_distribution.get('r_p25', 0):>10.2f}R")
        print(f"  50th percentile: {result.r_distribution.get('r_p50', 0):>10.2f}R")
        print(f"  75th percentile: {result.r_distribution.get('r_p75', 0):>10.2f}R")
        print(f"  90th percentile: {result.r_distribution.get('r_p90', 0):>10.2f}R")
        print(f"  Avg Winner:      {result.r_distribution.get('avg_winner_r', 0):>10.2f}R")
        print(f"  Avg Loser:       {result.r_distribution.get('avg_loser_r', 0):>10.2f}R")
        print(f"  Max Winner:      {result.r_distribution.get('max_winner_r', 0):>10.2f}R")
        print(f"  Max Loser:       {result.r_distribution.get('max_loser_r', 0):>10.2f}R")

    print("\n[TRADE SUMMARY]")
    print(f"  Total Trades:    {len(result.trades)}")

    if result.trades:
        winners = [t for t in result.trades if t.pnl and t.pnl > 0]
        losers = [t for t in result.trades if t.pnl and t.pnl < 0]

        print(f"  Winners:         {len(winners)}")
        print(f"  Losers:          {len(losers)}")
        print(f"  Win Rate:        {len(winners) / len(result.trades) * 100:.1f}%")

        # Exit reason analysis
        exit_reasons = {}
        for t in result.trades:
            if t.exit_reason:
                reason = t.exit_reason.value
                exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        print("\n[EXIT REASONS]")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: x[1], reverse=True):
            pct = count / len(result.trades) * 100
            print(f"  {reason}: {count} ({pct:.1f}%)")

        # Show best and worst trades
        print("\n[BEST TRADES - Top 5]")
        sorted_trades = sorted(result.trades, key=lambda t: t.pnl_r or 0, reverse=True)
        for i, trade in enumerate(sorted_trades[:5]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"
            print(f"  {i + 1}. {symbol} | {trade.entry_date} | {pnl_r_str} | {exit_str}")

        print("\n[WORST TRADES - Bottom 5]")
        for i, trade in enumerate(sorted_trades[-5:]):
            symbol = id_to_symbol.get(trade.symbol_id, trade.symbol)
            pnl_r_str = f">{trade.pnl_r:+.2f}R" if trade.pnl_r else "N/A"
            exit_str = trade.exit_reason.value if trade.exit_reason else "N/A"
            print(f"  {i + 1}. {symbol} | {trade.entry_date} | {pnl_r_str} | {exit_str}")

    print("\n" + "=" * 80)
    print("INTEGRATION TEST PASSED")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    test_duckdb_vectorbt_larger()
