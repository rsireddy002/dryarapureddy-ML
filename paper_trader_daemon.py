"""
paper_trader_daemon.py - Standalone, browser-independent paper trading.

WHY THIS EXISTS: the interactive app's paper trading only updates while
someone has a browser tab open with auto-refresh on -- that refresh timer
runs in the BROWSER, not the server, so closing your laptop or phone
stops it completely even though the Streamlit process on AWS is still
technically alive. This script has no such dependency: it's a plain
Python loop that runs on the server itself, checking/opening paper
trades on its own schedule regardless of whether anyone is watching.

Shares the SAME sahi_zones_cache.json and paper_trades.json as the
interactive app (dryarapureddy-ML) -- run this from that same folder.
Whatever this daemon does shows up in the app's "Paper Trading" tab
whenever you do open it.

SCHEDULE:
  - Once per day, before market open (or on first run if the cache is
    missing/stale): runs a full Precompute.
  - Every ZONE_REFRESH_INTERVAL_SECONDS (~5 min) during market hours:
    refreshes intraday zones.
  - Every QUOTE_SCAN_INTERVAL_SECONDS (~60s) during market hours: fetches
    quotes, checks exits on open paper trades, opens any new qualifying
    candidates.
  - Outside market hours: sleeps, waking up periodically to check if the
    next session has started.

USAGE:
    $env:UPSTOX_ACCESS_TOKEN = "your_token_here"   (or set as a systemd
    Environment= var, same as the interactive app's service)
    python3 paper_trader_daemon.py
"""
import os
import re
import json
import time
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone, time as dtime, date

import requests
import pandas as pd

from sahi_style_key_levels import sahi_style_key_levels
from zone_validation import cross_validated_zones
from ml_predict import predict_break_probability
from candles_with_levels import compute_cumulative_volume_delta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("paper_trader_daemon.log"), logging.StreamHandler()],
)
log = logging.getLogger("paper_trader_daemon")

IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST)

# ---------------- Config (matches app.py exactly) ----------------
INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"
QUOTES_URL = "https://api.upstox.com/v2/market-quote/quotes"
CACHE_PATH = "sahi_zones_cache.json"
PAPER_TRADE_LOG_PATH = "paper_trades.json"
HEARTBEAT_PATH = "daemon_heartbeat.json"

DAILY_LOOKBACK_DAYS = 30
COMPOSITE_LOOKBACK_DAYS = 18
RVOL_BASELINE_DAYS = 20
COMPOSITE_N_BINS = 50
INTRADAY_N_BINS = 45
MIN_PROMINENCE_PCT = 0.08
MIN_BIN_DISTANCE = 2
MAX_ZONES = 6
MIN_DISPLAY_PCT = 2.0

MARKET_OPEN_TIME = dtime(9, 15)
MARKET_CLOSE_TIME = dtime(15, 30)

PAPER_TRADE_SIZE_RUPEES = 25000
PAPER_TRADE_ML_RISK_THRESHOLD = 15.0
PAPER_TRADE_UNIVERSE_TOP_N = 10  # only the top-N Wide Range stocks (widest support-resistance gap) are eligible for entries

ZONE_REFRESH_INTERVAL_SECONDS = 300   # ~5 min
CANDLE_INTERVAL_MINUTES = 5
CANDLE_CLOSE_BUFFER_SECONDS = 5   # wait this long past the exact boundary so Upstox's quote has settled to reflect the just-closed candle
IDLE_CHECK_INTERVAL_SECONDS = 120     # how often to check "has the market opened yet" outside hours

EQUITY_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "SBIN", "AXISBANK",
    "KOTAKBANK", "BAJFINANCE", "BHARTIARTL", "ITC", "LT", "HINDUNILVR",
    "MARUTI", "TMPV", "TATASTEEL", "SUNPHARMA", "TITAN", "ULTRACEMCO",
    "ASIANPAINT", "WIPRO", "NTPC", "POWERGRID", "M&M", "ADANIENT",
    "ADANIPORTS", "BAJAJFINSV", "HCLTECH", "JSWSTEEL", "ONGC", "COALINDIA",
    "TECHM", "GRASIM", "DIVISLAB", "DRREDDY", "CIPLA", "EICHERMOT",
    "HEROMOTOCO", "HINDALCO", "BPCL", "BRITANNIA", "APOLLOHOSP", "SBILIFE",
    "HDFCLIFE", "INDUSINDBK", "BAJAJ-AUTO", "TATACONSUM", "UPL", "SHREECEM",
    "NESTLEIND", "VEDANTA", "GAIL", "PIDILITIND", "DLF", "GODREJCP",
    "SIEMENS", "AMBUJACEM", "BANDHANBNK", "BANKBARODA", "PNB", "CANBK",
    "IDFCFIRSTB", "FEDERALBNK", "AUROPHARMA", "BEL", "BIOCON", "CHOLAFIN",
    "COLPAL", "CONCOR", "CUMMINSIND", "DABUR", "DEEPAKNTR", "ESCORTS",
    "EXIDEIND", "GODREJPROP", "HAVELLS", "HDFCAMC", "ICICIGI", "ICICIPRULI",
    "IEX", "INDIGO", "INDUSTOWER", "IOC", "IRCTC", "JINDALSTEL", "JUBLFOOD",
    "LICHSGFIN", "LTIM", "LUPIN", "MANAPPURAM", "MARICO", "MCDOWELL-N",
    "MFSL", "MOTHERSON", "MPHASIS", "MRF", "MUTHOOTFIN", "NAUKRI",
    "NMDC", "OBEROIRLTY", "OFSS", "PAGEIND", "PEL", "PERSISTENT",
    "PETRONET", "PFC", "PIIND", "POLYCAB", "RECLTD", "SAIL", "SBICARD",
    "SRF", "SYNGENE", "TATACOMM", "TATAPOWER", "TORNTPHARM", "TRENT",
    "TVSMOTOR", "UBL", "VOLTAS", "ZEEL", "ZYDUSLIFE", "CDSL", "IRFC",
    "IDEA", "YESBANK", "SUZLON", "ETERNAL", "DMART", "JIOFIN", "PAYTM",
    "NYKAA", "POLICYBZR", "DELHIVERY", "LODHA", "PATANJALI", "ABCAPITAL",
    "ALKEM", "APLAPOLLO", "ASHOKLEY", "ASTRAL", "ATUL", "BALKRISNIND",
    "BATAINDIA", "BHARATFORG", "BHEL", "BSOFT", "CANFINHOME", "CROMPTON",
    "CUB", "DALBHARAT", "GLENMARK", "GMRINFRA", "GNFC", "GRANULES",
    "GUJGASLTD", "HAL", "HINDCOPPER", "HINDPETRO", "IBULHSGFIN", "IGL",
    "INDHOTEL", "INDIAMART", "IPCALAB", "JKCEMENT", "L&TFH", "LALPATHLAB",
    "LAURUSLABS", "M&MFIN", "METROPOLIS", "NATIONALUM", "NAVINFLUOR",
    "OIL", "PVRINOX", "RAIN", "RBLBANK", "SUNTV", "TATACHEM",
    "TATAELXSI", "TORNTPOWER", "UNIONBANK", "VBL", "WHIRLPOOL",
    "AARTIIND", "ABFRL", "ANGELONE", "APOLLOTYRE", "AUBANK", "BANKINDIA",
    "BSE", "CGPOWER", "CHAMBLFERT", "COFORGE", "COROMANDEL", "DIXON",
    "FORTIS", "GICRE", "GODFRYPHLP", "GRAPHITE", "GSPL", "HFCL",
    "HUDCO", "IIFL", "INDIACEM", "IRB", "ITI", "KALYANKJIL",
    "KEI", "LTF", "MANKIND", "MAXHEALTH", "MGL", "MOTILALOFS",
    "NBCC", "NCC", "NHPC", "PFIZER", "PGEL", "POWERINDIA",
    "PRESTIGE", "RVNL", "SJVN", "SOLARINDS", "SONACOMS", "STARHEALTH",
    "SUPREMEIND", "TIINDIA", "TITAGARH", "VEDL", "ZFCVINDIA",
    "SHRIRAMFIN",
]
FUTURES_SYMBOLS = ["NIFTY", "BANKNIFTY"]


def get_token():
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN not set.")
    return token.strip()


def resolve_equity_instrument_key(symbol, token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"query": symbol, "exchanges": "NSE", "segments": "EQ",
              "instrument_types": "EQ", "page_number": 1, "records": 10}
    resp = requests.get(INSTRUMENT_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    candidates = [inst for inst in resp.json().get("data", [])
                  if inst.get("trading_symbol", "").upper() == symbol.upper()]
    return candidates[0]["instrument_key"] if candidates else None


def resolve_futures_instrument_key(name, token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"query": name, "exchanges": "NSE", "segments": "FO",
              "instrument_types": "FUT", "page_number": 1, "records": 30}
    resp = requests.get(INSTRUMENT_SEARCH_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    candidates = [inst for inst in resp.json().get("data", [])
                  if inst.get("instrument_type") == "FUT"
                  and inst.get("underlying_symbol", "").upper() == name.upper()]
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["expiry"])
    return candidates[0]["instrument_key"]


def fetch_candles(instrument_key, token, unit, interval, lookback_days):
    to_date = now_ist().strftime("%Y-%m-%d")
    from_date = (now_ist() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    return df


def fetch_intraday_candles(instrument_key, token, unit="minutes", interval="5"):
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/{unit}/{interval}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    candles = resp.json().get("data", {}).get("candles", [])
    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"] = df["timestamp"].dt.date
    return df


def fetch_today_candles(instrument_key, token):
    df = fetch_intraday_candles(instrument_key, token, "minutes", "5")
    if not df.empty:
        return df
    df = fetch_candles(instrument_key, token, "minutes", "5", lookback_days=1)
    if df.empty:
        return df
    latest = df["date"].max()
    return df[df["date"] == latest]


def compute_composite_zones(intraday_df):
    if intraday_df.empty:
        return []
    try:
        _, shown = sahi_style_key_levels(
            intraday_df, n_bins=COMPOSITE_N_BINS, max_zones=MAX_ZONES,
            min_display_pct=MIN_DISPLAY_PCT, min_prominence_pct=MIN_PROMINENCE_PCT,
            min_bin_distance=MIN_BIN_DISTANCE,
        )
        return [asdict(z) for z in shown]
    except Exception:
        return []


def compute_intraday_zones(today_only_df):
    if today_only_df.empty:
        return []
    today = today_only_df["date"].max()
    today_df = today_only_df[today_only_df["date"] == today]
    try:
        _, shown = sahi_style_key_levels(
            today_df, n_bins=INTRADAY_N_BINS, max_zones=MAX_ZONES,
            min_display_pct=MIN_DISPLAY_PCT, min_prominence_pct=MIN_PROMINENCE_PCT,
            min_bin_distance=MIN_BIN_DISTANCE,
        )
        return [asdict(z) for z in shown]
    except Exception:
        return []


def fetch_batch_quotes(instrument_keys, token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    params = {"instrument_key": ",".join(instrument_keys)}
    resp = requests.get(QUOTES_URL, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("data", {})


def nearest_zones(ltp, validated_zones):
    support, support_dist = None, None
    resistance, resistance_dist = None, None
    for z in validated_zones:
        if ltp is None:
            break
        if z["price_mode"] <= ltp:
            dist = abs(ltp - z["price_high"]) / ltp * 100
            if support_dist is None or dist < support_dist:
                support, support_dist = z, dist
        else:
            dist = abs(z["price_low"] - ltp) / ltp * 100
            if resistance_dist is None or dist < resistance_dist:
                resistance, resistance_dist = z, dist
    return support, support_dist, resistance, resistance_dist


def get_top_wide_range_symbols(cache, price_lookup, top_n=10):
    """Ranks symbols by the gap % between nearest validated support and
    resistance (genuine room to move), returns the top_n symbol names.
    Mirrors the interactive app's Wide Range tab logic exactly. Uses
    zones already in cache and prices already fetched this cycle, so
    this costs no extra API calls."""
    rows = []
    for symbol, c in cache.items():
        ltp = price_lookup.get(symbol)
        if ltp is None:
            continue
        val_comp, _, _ = cross_validated_zones(
            c.get("composite_zones", []), c.get("intraday_zones", [])
        )
        support, _, resistance, _ = nearest_zones(ltp, val_comp)
        if support is None or resistance is None:
            continue
        gap_price = resistance["price_mode"] - support["price_mode"]
        if gap_price <= 0:
            continue
        gap_pct = gap_price / ltp * 100
        rows.append((symbol, gap_pct))
    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:top_n]]


def crossed_zones(prev_ltp, ltp, validated_zones):
    breakdowns, reclaims = [], []
    if prev_ltp is None or ltp is None:
        return breakdowns, reclaims
    for z in validated_zones:
        level = z["price_mode"]
        if prev_ltp >= level > ltp:
            breakdowns.append(z)
        elif prev_ltp < level <= ltp:
            reclaims.append(z)
    return breakdowns, reclaims


def _pct_from_label_safe(label):
    m = re.search(r"[\d.]+", str(label))
    return float(m.group()) if m else 0.0


# ---------------- Paper trade log (identical logic to app.py) ----------------

def load_paper_trades():
    if os.path.exists(PAPER_TRADE_LOG_PATH):
        with open(PAPER_TRADE_LOG_PATH, "r") as f:
            return json.load(f)
    return {"trades": []}


def save_paper_trades(log_data):
    with open(PAPER_TRADE_LOG_PATH, "w") as f:
        json.dump(log_data, f, indent=2)


def write_heartbeat(last_successful_scan, last_error, consecutive_errors):
    """Written every loop iteration -- proves the daemon PROCESS is alive
    and looping (last_loop_time), separately from whether its most
    recent SCAN actually succeeded (last_successful_scan / last_error).
    A process can be alive but stuck failing every cycle (e.g. an
    expired token) -- both pieces of info matter for spotting that."""
    heartbeat = {
        "last_loop_time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "last_successful_scan": last_successful_scan,
        "last_error": last_error,
        "consecutive_errors": consecutive_errors,
    }
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            json.dump(heartbeat, f, indent=2)
    except Exception as e:
        log.warning(f"Could not write heartbeat file: {e}")


def has_open_paper_trade(paper_log, symbol):
    return any(t["symbol"] == symbol and t["status"] == "open" for t in paper_log["trades"])


def open_paper_trade(paper_log, candidate):
    qty = int(PAPER_TRADE_SIZE_RUPEES // candidate["entry_price"])
    if qty < 1:
        return
    paper_log["trades"].append({
        "symbol": candidate["symbol"], "direction": candidate["direction"],
        "entry_price": candidate["entry_price"],
        "entry_time": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "stop_loss": candidate["stop_loss"], "target": candidate["target"], "qty": qty,
        "ml_risk_pct": candidate["ml_risk_pct"], "zone_pct": candidate["zone_pct"],
        "status": "open", "exit_price": None, "exit_time": None,
        "exit_reason": None, "pnl": None,
    })
    log.info(f"OPENED paper trade: {candidate['symbol']} {candidate['direction']} "
             f"@ {candidate['entry_price']} qty={qty} (ML risk {candidate['ml_risk_pct']}%)")


def check_paper_trade_exits(paper_log, price_lookup, force_eod=False):
    now_str = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    for t in paper_log["trades"]:
        if t["status"] != "open":
            continue
        ltp = price_lookup.get(t["symbol"])
        if ltp is None:
            continue
        exit_reason = None
        if t["direction"] == "long":
            if ltp <= t["stop_loss"]:
                exit_reason = "stop_loss"
            elif t["target"] is not None and ltp >= t["target"]:
                exit_reason = "target"
        else:
            if ltp >= t["stop_loss"]:
                exit_reason = "stop_loss"
            elif t["target"] is not None and ltp <= t["target"]:
                exit_reason = "target"
        if exit_reason is None and force_eod:
            exit_reason = "end_of_day"
        if exit_reason:
            t["status"] = "closed"
            t["exit_price"] = ltp
            t["exit_time"] = now_str
            t["exit_reason"] = exit_reason
            if t["direction"] == "long":
                t["pnl"] = round((ltp - t["entry_price"]) * t["qty"], 2)
            else:
                t["pnl"] = round((t["entry_price"] - ltp) * t["qty"], 2)
            log.info(f"CLOSED paper trade: {t['symbol']} {t['direction']} "
                     f"exit={ltp} reason={exit_reason} pnl={t['pnl']}")
    return paper_log


# ---------------- Precompute / zone refresh ----------------

def run_precompute(token):
    log.info("Starting Precompute (this takes a while)...")
    cache = {}
    all_symbols = [(s, "equity") for s in EQUITY_SYMBOLS] + [(s, "futures") for s in FUTURES_SYMBOLS]
    for i, (symbol, kind) in enumerate(all_symbols):
        try:
            key = (resolve_equity_instrument_key(symbol, token) if kind == "equity"
                   else resolve_futures_instrument_key(symbol, token))
            if key is None:
                continue
            daily_df = fetch_candles(key, token, "days", "1", DAILY_LOOKBACK_DAYS)
            intraday_df = fetch_candles(key, token, "minutes", "5", COMPOSITE_LOOKBACK_DAYS)
            prev_close = float(daily_df["close"].iloc[-1]) if not daily_df.empty else None
            avg_daily_volume = (float(daily_df["volume"].tail(RVOL_BASELINE_DAYS).mean())
                                 if len(daily_df) >= RVOL_BASELINE_DAYS else None)
            composite_zones = compute_composite_zones(intraday_df)
            intraday_zones = compute_intraday_zones(intraday_df)
            cache[symbol] = {
                "instrument_key": key, "prev_close": prev_close,
                "avg_daily_volume": avg_daily_volume,
                "composite_zones": composite_zones, "intraday_zones": intraday_zones,
                "zones_updated_at": now_ist().strftime("%Y-%m-%d %H:%M:%S"),
                "prev_ltp": None, "prev_vwap_above": None,
            }
        except Exception as e:
            log.warning(f"{symbol}: precompute failed ({e}), skipping.")
        if (i + 1) % 25 == 0:
            log.info(f"Precompute progress: {i+1}/{len(all_symbols)}")
        time.sleep(0.15)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)
    log.info(f"Precompute done. {len(cache)} symbols cached.")
    return cache


def run_zone_refresh(cache, token):
    for symbol in list(cache.keys()):
        try:
            key = cache[symbol]["instrument_key"]
            today_df = fetch_today_candles(key, token)
            cache[symbol]["intraday_zones"] = compute_intraday_zones(today_df)
            cache[symbol]["zones_updated_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            log.warning(f"{symbol}: zone refresh failed ({e}), keeping previous zones.")
        time.sleep(0.1)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)
    return cache


def cache_is_stale(cache):
    """True if the cache is missing, empty, or has no zone data from
    today. Checks zones_updated_at's date rather than a dedicated
    "precompute_date" field, since this cache file is SHARED with the
    interactive app.py, which only ever sets zones_updated_at (not a
    separate precompute-specific marker) -- checking that field keeps
    this daemon from treating a cache the interactive app already
    refreshed today as stale, and vice versa."""
    if not cache:
        return True
    today_str = now_ist().strftime("%Y-%m-%d")
    sample = next(iter(cache.values()))
    updated_at = sample.get("zones_updated_at", "")
    return not updated_at.startswith(today_str)


def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r") as f:
            return json.load(f)
    return {}


# ---------------- The scan cycle: quotes + entry/exit detection ----------------

def run_scan_cycle(cache, token, paper_log):
    """One quote-scan cycle: fetch prices, detect entry candidates
    (level-cross + same-tick VWAP-cross + low ML risk + CVD confirming
    the trade direction), restricted to the top Wide Range stocks
    (genuine room to move) instead of the full 220+ universe -- check
    exits on open positions, open any new qualifying ones. Mutates
    cache and paper_log in place; caller is responsible for saving
    both."""
    symbols = list(cache.keys())
    instrument_keys = [cache[s]["instrument_key"] for s in symbols]
    key_to_symbol = {cache[s]["instrument_key"]: s for s in symbols}
    quotes = fetch_batch_quotes(instrument_keys, token)

    # Pass 1: extract every price from this cycle's quotes FIRST -- the
    # wide-range ranking needs ALL prices known before it can pick a
    # meaningful top-N, so this can't be built incrementally in the
    # same loop that generates candidates.
    price_lookup = {}
    for q in quotes.values():
        sym = key_to_symbol.get(q.get("instrument_token"))
        if sym and q.get("last_price") is not None:
            price_lookup[sym] = q["last_price"]

    top_wide_range_symbols = set(get_top_wide_range_symbols(
        cache, price_lookup, top_n=PAPER_TRADE_UNIVERSE_TOP_N
    ))

    # Pass 2: VWAP-cross/level-cross state updates (every symbol, same
    # as before) and candidate generation (only for the restricted
    # universe, with the added CVD gate).
    candidates = []

    for quote_key, q in quotes.items():
        instrument_key = q.get("instrument_token")
        symbol = key_to_symbol.get(instrument_key)
        if not symbol:
            continue
        c = cache[symbol]
        ltp = q.get("last_price")
        vwap = q.get("average_price")
        prev_close = c.get("prev_close")

        if ltp is None:
            continue

        val_comp, _, _ = cross_validated_zones(
            c.get("composite_zones", []), c.get("intraday_zones", [])
        )

        crossed_up, crossed_down = False, False
        if vwap is not None:
            vwap_above_now = ltp > vwap
            prev_vwap_above = c.get("prev_vwap_above")
            crossed_up = prev_vwap_above is False and vwap_above_now
            crossed_down = prev_vwap_above is True and not vwap_above_now
            cache[symbol]["prev_vwap_above"] = vwap_above_now

        prev_ltp = c.get("prev_ltp")
        level_breakdowns, level_reclaims = crossed_zones(prev_ltp, ltp, val_comp)
        cache[symbol]["prev_ltp"] = ltp

        day_open = (q.get("ohlc") or {}).get("open") or prev_close
        if day_open is not None and vwap is not None and symbol in top_wide_range_symbols:
            # CVD needs actual candle data (not in the quotes response)
            # -- fetched lazily, only for restricted-universe symbols
            # that already have a level-cross this tick, to keep this
            # cheap despite the extra per-symbol API call it requires.
            latest_cvd = None
            if level_reclaims or level_breakdowns:
                cvd_candles = fetch_today_candles(c["instrument_key"], token)
                if not cvd_candles.empty:
                    latest_cvd = compute_cumulative_volume_delta(cvd_candles).iloc[-1]

            for z in level_reclaims:
                if not crossed_up:
                    continue
                if latest_cvd is None or latest_cvd <= 0:
                    continue  # need net BUYING pressure to confirm a long
                risk = predict_break_probability(
                    z, ltp=ltp, vwap=vwap, day_open=day_open,
                    session_start_time=MARKET_OPEN_TIME, session_end_time=MARKET_CLOSE_TIME,
                    now_time=now_ist().time(), is_intraday_validated=True,
                )
                if risk is None or risk * 100 >= PAPER_TRADE_ML_RISK_THRESHOLD:
                    continue
                _, _, next_resistance, _ = nearest_zones(ltp, val_comp)
                if next_resistance is None:
                    continue
                candidates.append({
                    "symbol": symbol, "direction": "long", "entry_price": ltp,
                    "stop_loss": z["price_mode"], "target": next_resistance["price_mode"],
                    "ml_risk_pct": round(risk * 100, 1),
                    "zone_pct": _pct_from_label_safe(z["label"]),
                })
            for z in level_breakdowns:
                if not crossed_down:
                    continue
                if latest_cvd is None or latest_cvd >= 0:
                    continue  # need net SELLING pressure to confirm a short
                risk = predict_break_probability(
                    z, ltp=ltp, vwap=vwap, day_open=day_open,
                    session_start_time=MARKET_OPEN_TIME, session_end_time=MARKET_CLOSE_TIME,
                    now_time=now_ist().time(), is_intraday_validated=True,
                )
                if risk is None or risk * 100 >= PAPER_TRADE_ML_RISK_THRESHOLD:
                    continue
                next_support, _, _, _ = nearest_zones(ltp, val_comp)
                if next_support is None:
                    continue
                candidates.append({
                    "symbol": symbol, "direction": "short", "entry_price": ltp,
                    "stop_loss": z["price_mode"], "target": next_support["price_mode"],
                    "ml_risk_pct": round(risk * 100, 1),
                    "zone_pct": _pct_from_label_safe(z["label"]),
                })

    # exits BEFORE opens -- same critical ordering fix as the interactive app
    market_closing_now = now_ist().time() >= MARKET_CLOSE_TIME
    paper_log = check_paper_trade_exits(paper_log, price_lookup, force_eod=market_closing_now)
    for candidate in candidates:
        if not has_open_paper_trade(paper_log, candidate["symbol"]):
            open_paper_trade(paper_log, candidate)

    return cache, paper_log


def seconds_until_next_candle_close(now_dt):
    """Seconds until the next 5-min-aligned boundary (:00, :05, :10, ...),
    plus a small buffer so Upstox's quote has settled to reflect the
    just-closed candle rather than catching it mid-update. This is what
    makes the scan align to actual candle closes instead of firing on
    an arbitrary rolling timer untethered to candle boundaries."""
    minute = now_dt.minute
    next_boundary_minute = ((minute // CANDLE_INTERVAL_MINUTES) + 1) * CANDLE_INTERVAL_MINUTES
    next_boundary = now_dt.replace(second=0, microsecond=0) + timedelta(minutes=next_boundary_minute - minute)
    wait_seconds = (next_boundary - now_dt).total_seconds() + CANDLE_CLOSE_BUFFER_SECONDS
    return max(wait_seconds, 1)


def seconds_until_next_check(now_dt):
    """During market hours: wait until the next 5-min candle boundary,
    so the scan cycle evaluates the CONFIRMED close of each candle,
    not noisy intra-candle ticks on an arbitrary timer. Outside market
    hours: a longer, simple fixed wait is fine since nothing time-
    sensitive is happening."""
    if MARKET_OPEN_TIME <= now_dt.time() < MARKET_CLOSE_TIME:
        return seconds_until_next_candle_close(now_dt)
    return IDLE_CHECK_INTERVAL_SECONDS


def main():
    token = get_token()
    cache = load_cache()
    last_zone_refresh = 0.0
    last_successful_scan = None
    consecutive_errors = 0

    log.info("paper_trader_daemon starting.")

    while True:
        now = now_ist()
        last_error = None

        try:
            if cache_is_stale(cache):
                cache = run_precompute(token)
                last_zone_refresh = time.time()

            market_open_now = MARKET_OPEN_TIME <= now.time() < MARKET_CLOSE_TIME

            if market_open_now:
                if time.time() - last_zone_refresh >= ZONE_REFRESH_INTERVAL_SECONDS:
                    log.info("Refreshing intraday zones...")
                    cache = run_zone_refresh(cache, token)
                    last_zone_refresh = time.time()

                paper_log = load_paper_trades()
                cache, paper_log = run_scan_cycle(cache, token, paper_log)
                save_paper_trades(paper_log)
                with open(CACHE_PATH, "w") as f:
                    json.dump(cache, f)
                last_successful_scan = now_ist().strftime("%Y-%m-%d %H:%M:%S")
            consecutive_errors = 0
        except Exception as e:
            last_error = str(e)
            consecutive_errors += 1
            log.error(f"Cycle failed ({consecutive_errors} in a row): {e}")

        write_heartbeat(last_successful_scan, last_error, consecutive_errors)
        # Re-read the clock HERE, right before scheduling sleep -- using
        # the stale `now` captured at the top of this iteration would
        # understate how much time has actually passed if Precompute (or
        # any slow cycle) ran in between, causing the next wake-up to
        # drift earlier than the true next candle boundary.
        time.sleep(seconds_until_next_check(now_ist()))


if __name__ == "__main__":
    main()
