"""
feed_listener.py - Connects to Upstox's V3 market-data WebSocket feed,
subscribes to the full F&O universe in tiers (same tiered pattern as
upstox-feed-listener: Tier 1 gets full_d30 depth, Tier 2 gets full),
feeds every tick into CandleAggregator, and periodically dumps all
symbols' rolling 5-min candles to a shared JSON file that
fno-liquid-scanner-live's Streamlit app reads instead of hitting the
REST historical-candle endpoint ~230 times per refresh.

*** IMPORTANT -- READ BEFORE RUNNING ***
Upstox's WebSocket feed sends protobuf-encoded binary frames. You
ALREADY have a working, tested decoder for this in upstox-feed-listener
(including the fix for int64 fields being serialized as strings). Rather
than re-guess that .proto schema here and risk silently wrong data, this
script imports a `decode_feed_message` function from proto_decoder.py --
a file YOU need to create in this repo by copying the relevant decode
logic over from upstox-feed-listener. See proto_decoder.py's docstring
for the exact function signature expected.

SETUP:
    pip install -r requirements.txt
    $env:UPSTOX_ACCESS_TOKEN = "your_token_here"
    python instrument_resolver.py     # one-time: builds instrument_keys_cache.json
    # create proto_decoder.py (see its docstring) before running this
    python feed_listener.py

OUTPUT:
    fno_live_candles.json -- updated every DUMP_INTERVAL_SECONDS, holding
    {symbol: [{"timestamp": iso, "open":, "high":, "low":, "close":, "volume":}, ...]}
    for every symbol currently subscribed.
"""
import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta

import requests
import websocket  # pip install websocket-client

from candle_aggregator import CandleAggregator
from instrument_resolver import resolve_all, get_token

try:
    from proto_decoder import decode_feed_message
except ImportError:
    raise SystemExit(
        "\nproto_decoder.py not found. This repo needs YOUR existing protobuf "
        "decode logic from upstox-feed-listener -- see feed_listener.py's "
        "docstring and proto_decoder.py's docstring for what to copy over.\n"
    )

IST = timezone(timedelta(hours=5, minutes=30))
AUTHORIZE_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
OUTPUT_PATH = "fno_live_candles.json"
DUMP_INTERVAL_SECONDS = 5

# Same tiering idea as upstox-feed-listener: a small "Tier 1" set gets
# richer depth (full_d30), everything else gets the lighter "full" mode.
# Adjust TIER1_SYMBOLS to whatever you actively trade/watch most closely.
TIER1_SYMBOLS = ["NIFTY", "BANKNIFTY"]
TIER1_MODE = "full_d30"
TIER2_MODE = "full"

aggregator = CandleAggregator()
symbol_by_key = {}  # instrument_key -> symbol, built from instrument_resolver's output
_stop_event = threading.Event()


def get_authorized_ws_url(token):
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    resp = requests.get(AUTHORIZE_URL, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.json()["data"]["authorized_redirect_uri"]


def build_subscribe_message(instrument_keys, mode):
    return json.dumps({
        "guid": f"sub-{mode}-{int(time.time())}",
        "method": "sub",
        "data": {"mode": mode, "instrumentKeys": instrument_keys},
    })


def dump_loop():
    """Runs in its own thread: every DUMP_INTERVAL_SECONDS, writes the
    aggregator's current state to OUTPUT_PATH. Writes to a temp file then
    renames, so the Streamlit app never reads a half-written file."""
    tmp_path = OUTPUT_PATH + ".tmp"
    while not _stop_event.is_set():
        snapshot = aggregator.snapshot()
        serializable = {
            sym: [
                {**bar, "timestamp": bar["timestamp"].isoformat()}
                for bar in bars
            ]
            for sym, bars in snapshot.items()
        }
        with open(tmp_path, "w") as f:
            json.dump(serializable, f)
        os.replace(tmp_path, OUTPUT_PATH)
        _stop_event.wait(DUMP_INTERVAL_SECONDS)


def on_message(ws, message):
    """message is raw bytes (protobuf-encoded). decode_feed_message must
    return a dict like:
        {instrument_key: {"ltp": float, "ltt": int_ms_epoch_or_None, "volume": float_or_None}, ...}
    for whichever instruments had an update in this message -- see
    proto_decoder.py's docstring for the exact contract."""
    try:
        updates = decode_feed_message(message)
    except Exception as e:
        print(f"decode error: {e}")
        return

    for instrument_key, tick in updates.items():
        symbol = symbol_by_key.get(instrument_key)
        if not symbol:
            continue
        ltp = tick.get("ltp")
        if ltp is None:
            continue
        ltt_ms = tick.get("ltt")
        ts = datetime.fromtimestamp(ltt_ms / 1000, tz=IST) if ltt_ms else datetime.now(IST)
        aggregator.on_tick(symbol, ltp, tick.get("volume"), timestamp=ts)


def on_error(ws, error):
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} {close_msg}")


def on_open(ws, tier1_keys, tier2_keys):
    print(f"Connected. Subscribing {len(tier1_keys)} Tier-1 ({TIER1_MODE}) "
          f"and {len(tier2_keys)} Tier-2 ({TIER2_MODE}) symbols...")
    if tier1_keys:
        ws.send(build_subscribe_message(tier1_keys, TIER1_MODE))
    if tier2_keys:
        # Upstox recommends chunking large subscribe lists rather than
        # one giant message -- 100 per message is a safe conservative size.
        chunk_size = 100
        for i in range(0, len(tier2_keys), chunk_size):
            ws.send(build_subscribe_message(tier2_keys[i:i + chunk_size], TIER2_MODE))
            time.sleep(0.2)
    print("Subscription messages sent.")


def run():
    global symbol_by_key
    token = get_token()

    print("Resolving instrument keys (cached after first run)...")
    key_by_symbol = resolve_all(token)
    symbol_by_key = {v: k for k, v in key_by_symbol.items()}

    tier1_keys = [key_by_symbol[s] for s in TIER1_SYMBOLS if s in key_by_symbol]
    tier2_keys = [key_by_symbol[s] for s in key_by_symbol if s not in TIER1_SYMBOLS]

    dump_thread = threading.Thread(target=dump_loop, daemon=True)
    dump_thread.start()

    while not _stop_event.is_set():
        try:
            ws_url = get_authorized_ws_url(token)
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.on_open = lambda ws_: on_open(ws_, tier1_keys, tier2_keys)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"Connection failed ({e}), retrying in 5s...")

        if not _stop_event.is_set():
            print("Reconnecting in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\nStopping...")
        _stop_event.set()
