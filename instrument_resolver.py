"""
instrument_resolver.py - Resolves Upstox instrument keys for the full F&O
universe (231 equities + NIFTY/BANKNIFTY futures), same symbol list and
resolution logic as fno-liquid-scanner-live's app.py, so the WebSocket
feed subscribes to exactly the same universe the Streamlit app scans.

Results are cached to instrument_keys_cache.json so you don't re-hit the
instrument search API on every restart (futures keys DO need refreshing
around expiry -- see resolve_futures_instrument_key's no-expiry-filter
fix, same fix already applied in hvn-lvn-scanner and sahi-key-levels-live
for the same reason).
"""
import json
import os
import time

import requests

INSTRUMENT_SEARCH_URL = "https://api.upstox.com/v2/instruments/search"
CACHE_PATH = "instrument_keys_cache.json"

# Same 231-symbol F&O universe as fno-liquid-scanner-live/app.py.
# Keep these two lists in sync if you change one -- they're duplicated
# here (rather than imported) so this repo has no dependency on the
# Streamlit app's file, per "separate repo, don't disturb existing".
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
    if token:
        return token.strip()
    if os.path.exists("upstox_token.txt"):
        with open("upstox_token.txt", "r") as f:
            t = f.read().strip()
        if t and t != "PASTE_YOUR_TOKEN_HERE":
            return t
    raise RuntimeError("No token found. Set $env:UPSTOX_ACCESS_TOKEN or create upstox_token.txt.")


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
    """No expiry filter -- sorts client-side by expiry (same fix already
    applied across your other repos: 'current_month' keyword returns zero
    results once that month's contract expires but the calendar hasn't
    rolled over yet)."""
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


def resolve_all(token, force_refresh=False):
    """Returns {symbol: instrument_key} for the full universe. Uses the
    disk cache unless force_refresh=True or the cache is missing/empty --
    call with force_refresh=True at least once around monthly expiry
    rollover so futures keys stay current."""
    if not force_refresh and os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r") as f:
            cached = json.load(f)
        if cached:
            return cached

    resolved = {}
    all_symbols = [(s, "equity") for s in EQUITY_SYMBOLS] + [(s, "futures") for s in FUTURES_SYMBOLS]
    for i, (symbol, kind) in enumerate(all_symbols):
        try:
            key = (resolve_futures_instrument_key(symbol, token) if kind == "futures"
                   else resolve_equity_instrument_key(symbol, token))
            if key:
                resolved[symbol] = key
            else:
                print(f"  {symbol}: no instrument key found, skipping")
        except Exception as e:
            print(f"  {symbol}: resolution failed ({e}), skipping")
        if (i + 1) % 20 == 0:
            print(f"  resolved {i + 1}/{len(all_symbols)}...")
        time.sleep(0.1)

    with open(CACHE_PATH, "w") as f:
        json.dump(resolved, f, indent=2)
    print(f"Resolved {len(resolved)}/{len(all_symbols)} instrument keys -> {CACHE_PATH}")
    return resolved


if __name__ == "__main__":
    token = get_token()
    resolve_all(token, force_refresh=True)
