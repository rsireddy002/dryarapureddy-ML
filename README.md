# fno-websocket-feed

A standalone WebSocket listener that replaces ~230 REST calls per refresh
(one per symbol, hitting Upstox's historical-candle endpoint) with a
single persistent WebSocket connection streaming live ticks for the
entire F&O universe at once, aggregated locally into 5-minute OHLCV
candles.

## Why this exists

`fno-liquid-scanner-live`'s Sectors tab (scroll-through-all-sectors view)
fetches candle data for ~230 symbols via REST on demand. That's slow, is
easy to rate-limit, and re-downloads a whole day's candles every time.
This repo instead:

1. Opens ONE WebSocket connection to Upstox's V3 market-data feed.
2. Subscribes to all ~230 symbols in tiers (NIFTY/BANKNIFTY get richer
   `full_d30` depth; everything else gets lighter `full` mode) -- same
   tiering pattern as `upstox-feed-listener`.
3. Feeds every tick into an in-memory 5-min candle aggregator.
4. Dumps the current state of every symbol's candles to
   `fno_live_candles.json` every 5 seconds.

`fno-liquid-scanner-live` can then read that JSON file directly instead
of hitting the REST API at all for chart data -- a local file read
instead of 230 HTTP round-trips.

## Setup

```
pip install -r requirements.txt
$env:UPSTOX_ACCESS_TOKEN = "your_token_here"
python instrument_resolver.py     # one-time: resolves & caches instrument keys
```

**Before running feed_listener.py**, you MUST create `proto_decoder.py`
with your real protobuf decode logic, copied over from
`upstox-feed-listener` (which already has this working, including the
int64-as-string fix). See `proto_decoder.py`'s docstring for the exact
function signature required. This repo intentionally does NOT ship a
guessed/reimplemented protobuf schema -- that risk isn't worth it for
data a live trading tool depends on.

Once `proto_decoder.py` is filled in:

```
python feed_listener.py
```

Leave it running in its own terminal/window during market hours. It
reconnects automatically on disconnect.

## Files

- `instrument_resolver.py` -- resolves & caches Upstox instrument keys
  for the full 231-symbol F&O universe (same list as
  `fno-liquid-scanner-live`, duplicated here so this repo has no
  dependency on that one).
- `candle_aggregator.py` -- pure logic, turns a tick stream into rolling
  5-min OHLCV bars per symbol. No network code, easy to unit test.
- `feed_listener.py` -- WebSocket connection, subscription, reconnect
  logic, and the periodic JSON dump. Imports `decode_feed_message` from
  `proto_decoder.py`.
- `proto_decoder.py` -- **you fill this in** using your existing working
  decoder from `upstox-feed-listener`.

## Output format

`fno_live_candles.json`:

```json
{
  "RELIANCE": [
    {"timestamp": "2026-09-05T09:15:00+05:30", "open": 1330.0, "high": 1332.5, "low": 1329.0, "close": 1331.2, "volume": 45210},
    ...
  ],
  "NIFTY": [...],
  ...
}
```

## Not yet done

Wiring `fno-liquid-scanner-live` to actually READ this file instead of
calling REST is a separate change, made in that repo, not this one --
keeping this build isolated as requested. Once `proto_decoder.py` is
filled in and this listener is confirmed working (check
`fno_live_candles.json` is updating every few seconds during market
hours), that integration is a relatively small change: swap
`fetch_today_candles_cached()` calls in the Sectors/Chart tabs for a
function that reads and parses this JSON file instead.
