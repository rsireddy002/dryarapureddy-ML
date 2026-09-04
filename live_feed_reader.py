"""
live_feed_reader.py - Reads fno_live_candles.json, written by the
fno-websocket-feed repo's feed_listener.py, and converts a symbol's
candles into the same DataFrame shape fetch_today_candles_cached()
already returns (columns: timestamp, open, high, low, close, volume).

This repo has NO dependency on fno-websocket-feed's code -- it only
reads a JSON file that repo happens to produce. If that file is missing,
stale, or doesn't have data for a requested symbol yet, every function
here returns None so the caller can fall back to the existing REST fetch
without any special-casing.

Point FNO_LIVE_CANDLES_PATH at wherever feed_listener.py is writing to,
e.g. if you set $env:FNO_LIVE_CANDLES_OUTPUT to write directly into this
repo's folder when running feed_listener.py, the default here ("./fno_live_candles.json")
already matches with no extra config needed.
"""
import json
import os
import time

import pandas as pd

FNO_LIVE_CANDLES_PATH = os.environ.get("FNO_LIVE_CANDLES_PATH", "fno_live_candles.json")

# If the file hasn't been touched in this long, treat it as stale (the
# listener probably isn't running) and fall back to REST rather than
# serving frozen/outdated candles without any warning.
MAX_STALENESS_SECONDS = 30


def is_feed_available():
    """Cheap check: does the file exist and was it written recently?
    Doesn't parse the file -- just os.path calls, safe to call on every
    Streamlit rerun without meaningful overhead."""
    if not os.path.exists(FNO_LIVE_CANDLES_PATH):
        return False
    age = time.time() - os.path.getmtime(FNO_LIVE_CANDLES_PATH)
    return age <= MAX_STALENESS_SECONDS


def get_live_candles(symbol):
    """
    Returns a DataFrame with columns [timestamp, open, high, low, close,
    volume] for this symbol from the live feed, or None if the feed isn't
    available, the file can't be parsed, or this symbol has no data in it
    yet (e.g. it hasn't ticked since the listener started).
    """
    if not is_feed_available():
        return None

    try:
        with open(FNO_LIVE_CANDLES_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        # File mid-write or corrupted -- feed_listener.py writes via
        # temp-file-then-rename specifically to avoid this, but a defensive
        # catch here costs nothing and a REST fallback is always safe.
        return None

    bars = data.get(symbol)
    if not bars:
        return None

    df = pd.DataFrame(bars)
    if df.empty:
        return None

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df[["timestamp", "open", "high", "low", "close", "volume"]]
