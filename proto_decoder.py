"""
proto_decoder.py - YOU NEED TO FILL THIS IN using your existing, working
protobuf decode logic from upstox-feed-listener.

Why this file is empty: Upstox's WebSocket feed sends binary protobuf
messages (schema: MarketDataFeedV3.proto, compiled to a *_pb2.py module
with protoc). You already solved this in upstox-feed-listener -- including
the specific fix for int64 fields being serialized as strings in the
protobuf output, which needs explicit int() casting (noted in your own
build history). Re-deriving that schema from scratch here risks producing
a decoder that "works" but silently returns wrong values -- not
acceptable for a feed a live trading tool depends on.

WHAT TO DO:
1. Copy your compiled protobuf module (e.g. MarketDataFeedV3_pb2.py, or
   whatever it's named in upstox-feed-listener) into this repo folder.
2. Copy/adapt whatever function in upstox-feed-listener currently parses
   an incoming WebSocket message into per-instrument LTP/volume/timestamp
   values. Wire it into the function below.

REQUIRED FUNCTION SIGNATURE:

    def decode_feed_message(raw_bytes: bytes) -> dict:
        '''
        Returns {instrument_key: {"ltp": float, "ltt": int_or_None, "volume": float_or_None}, ...}
        for every instrument that had data in this message. Include only
        instruments actually present in the message -- feed_listener.py
        already handles partial updates fine.

        Field notes based on Upstox's V3 feed:
        - ltp: last traded price (float)
        - ltt: last traded time, epoch milliseconds (int) -- used to
          bucket the tick into the correct 5-min bar. If unavailable,
          return None and feed_listener.py will use receive-time instead
          (slightly less accurate bucketing, but functional).
        - volume: CUMULATIVE volume traded so far today for this
          instrument (not a per-tick delta -- candle_aggregator.py
          computes the delta itself from consecutive cumulative values).
          Remember your own int64-as-string fix here if applicable.
        '''
        ...

Below is a minimal skeleton to adapt -- replace the body with your real
decode logic once the pb2 module is in place.
"""

# Example of what this typically looks like once wired up (uncomment and
# adapt once you've copied your pb2 module over):
#
# import MarketDataFeedV3_pb2 as pb
#
# def decode_feed_message(raw_bytes: bytes) -> dict:
#     feed_response = pb.FeedResponse()
#     feed_response.ParseFromString(raw_bytes)
#     updates = {}
#     for instrument_key, feed in feed_response.feeds.items():
#         full_feed = feed.fullFeed
#         if full_feed.HasField("marketFF"):
#             ltpc = full_feed.marketFF.ltpc
#         elif full_feed.HasField("indexFF"):
#             ltpc = full_feed.indexFF.ltpc
#         else:
#             continue
#         updates[instrument_key] = {
#             "ltp": ltpc.ltp,
#             "ltt": int(ltpc.ltt) if ltpc.ltt else None,  # int64-as-string fix
#             "volume": None,  # pull from marketOHLC or wherever your
#                               # existing code sources cumulative volume
#         }
#     return updates


def decode_feed_message(raw_bytes: bytes) -> dict:
    raise NotImplementedError(
        "Fill this in using your working protobuf decode logic from "
        "upstox-feed-listener -- see this file's module docstring."
    )
