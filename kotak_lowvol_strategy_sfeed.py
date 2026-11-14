"""
Kotak Neo - Lowest Volume Candle + Confirmation Strategy (Alert Only, TRUE REALTIME)
-------------------------------------------------------------------------------------
Built on the CURRENT, actively-maintained SDK: `kotakneoapi` (PyPI), i.e.
github.com/Kotak-Neo/kotak-neo-python - NOT the retired kotak-neo-api-v2.

Install:
    pip install kotakneoapi

Uses the async/await SFeed WebSocket client to get true tick-by-tick data for
a watchlist of stocks, builds live candles, and runs the same "Daily Lowest
Volume Candle + Confirmation" logic ported from the original Pine Script.

NO ORDERS ARE PLACED. Alerts are printed to console + appended to alerts.log.

NOTE ON TICK FIELDS: SFeedScrip is a typed Pydantic message with fields like
`trading_symbol`, `instrument_token`, `last_traded_price`. Its exact field
name for traded quantity isn't confirmed here, so this script tries several
likely attribute names and - importantly - PRINTS THE RAW FIRST MESSAGE per
symbol so you can see the real field names. Paste me one of those printouts
and I'll lock the extraction to the exact field in one edit.
"""

import asyncio
from datetime import datetime

from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedScrip

# ─────────────────────────────────────────────
# CONFIG - edit these
# ─────────────────────────────────────────────
CONSUMER_KEY = "dfgd-9dfgdfg"  # from NEO app -> Invest -> Trade API -> Your Applications
MOBILE_NUMBER = ""
UCC = "fgdfg"

# Your 10 stocks: (trading_symbol as in scrip master, exchange_segment)
SYMBOLS = [
    ("SUPREMEIND-EQ", "nse_cm"),
    ("GODFRYPHLP-EQ", "nse_cm"),
    ("LODHA-EQ", "nse_cm"),
    ("ZYDUSLIFE-EQ", "nse_cm"),
    ("ATHERENERG-EQ", "nse_cm"),
    ("ABB-EQ", "nse_cm"),
    ("BHARATFORG-EQ", "nse_cm"),
    ("ADANIGREEN-EQ", "nse_cm"),
    ("PIDILITIND-EQ", "nse_cm"),
    ("SIEMENS-EQ", "nse_cm"),
    ("ADANIENSOL-EQ", "nse_cm"),
    ("SONACOMS-EQ", "nse_cm"),
]

CANDLE_INTERVAL_MINUTES = 5
VOL_MULTIPLIER = 3.5
SIGNAL_MODE = "Both"  # "Bullish Only" | "Bearish Only" | "Both"
ALERT_LOG_FILE = "./alerts.log"

# When False, only real confirmation signals (🔔 ALERT lines) are printed.
# When True, also prints every time a candle becomes the day's new
# lowest-volume reference candle - useful for debugging, noisy otherwise.
VERBOSE = False

# ─────────────────────────────────────────────
# Alert helper
# ─────────────────────────────────────────────
def log_alert(text: str):
    line = f"[{datetime.now().isoformat()}] {text}"
    print(f"\033[93m🔔 {line}\033[0m")
    with open(ALERT_LOG_FILE, "a") as f:
        f.write(line + "\n")


# ─────────────────────────────────────────────
# Candle builder (per symbol, real wall-clock buckets)
# ─────────────────────────────────────────────
class CandleBuilder:
    def __init__(self, interval_minutes):
        self.interval_s = interval_minutes * 60
        self.current = None  # dict: start, open, high, low, close, cum_start, cum_end

    def _bucket_start(self, ts):
        return int(ts // self.interval_s) * self.interval_s

    def update(self, ts, ltp, cum_volume):
        """cum_volume = volume_traded_today (a running total, not a per-tick delta)."""
        bucket = self._bucket_start(ts)
        closed = None
        if self.current is None or self.current["start"] != bucket:
            if self.current is not None:
                closed = {
                    "start": self.current["start"],
                    "open": self.current["open"],
                    "high": self.current["high"],
                    "low": self.current["low"],
                    "close": self.current["close"],
                    "volume": self.current["cum_end"] - self.current["cum_start"],
                }
            self.current = {
                "start": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "cum_start": cum_volume, "cum_end": cum_volume,
            }
        else:
            c = self.current
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp
            c["cum_end"] = cum_volume
        return closed


# ─────────────────────────────────────────────
# Strategy state (direct port of the Pine Script logic)
# ─────────────────────────────────────────────
class LowVolStrategy:
    def __init__(self, symbol):
        self.symbol = symbol
        self.current_day = None
        self.lowest_day_volume = None
        self.identified_red = None
        self.confirmation_bull = False
        self.identified_green = None
        self.confirmation_bear = False
        self.prev_new_lowest_red = False
        self.prev_new_lowest_green = False

    def _reset(self):
        self.lowest_day_volume = None
        self.identified_red = None
        self.confirmation_bull = False
        self.identified_green = None
        self.confirmation_bear = False
        self.prev_new_lowest_red = False
        self.prev_new_lowest_green = False

    def on_candle_close(self, candle):
        day = datetime.fromtimestamp(candle["start"]).date()
        if self.current_day != day:
            self.current_day = day
            self._reset()

        o, h, l, c, v = candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"]
        is_red = c < o
        is_green = c > o

        is_new_lowest_red = is_red and (self.lowest_day_volume is None or v < self.lowest_day_volume)
        is_new_lowest_green = is_green and (self.lowest_day_volume is None or v < self.lowest_day_volume)

        if is_new_lowest_red:
            self.identified_red = {"volume": v, "high": h, "open": o}
        if is_new_lowest_green:
            self.identified_green = {"volume": v, "low": l, "open": o}

        if self.lowest_day_volume is None or v < self.lowest_day_volume:
            self.lowest_day_volume = v

        if (self.prev_new_lowest_red and is_green and self.identified_red
                and v > self.identified_red["volume"] and v < self.identified_red["volume"] * VOL_MULTIPLIER
                and c > self.identified_red["high"] and not self.confirmation_bull):
            self.confirmation_bull = True
            if SIGNAL_MODE != "Bearish Only":
                log_alert(f"BULLISH CONFIRMATION on {self.symbol} | close={c} > red-high={self.identified_red['high']} | vol={v}")

        if (self.prev_new_lowest_green and is_red and self.identified_green
                and v > self.identified_green["volume"] and v < self.identified_green["volume"] * VOL_MULTIPLIER
                and c < self.identified_green["low"] and not self.confirmation_bear):
            self.confirmation_bear = True
            if SIGNAL_MODE != "Bullish Only":
                log_alert(f"BEARISH CONFIRMATION on {self.symbol} | close={c} < green-low={self.identified_green['low']} | vol={v}")

        if VERBOSE and is_new_lowest_red:
            print(f"[{self.symbol}] new lowest-volume RED candle (vol={v})")
        if VERBOSE and is_new_lowest_green:
            print(f"[{self.symbol}] new lowest-volume GREEN candle (vol={v})")

        self.prev_new_lowest_red = is_new_lowest_red
        self.prev_new_lowest_green = is_new_lowest_green


# ─────────────────────────────────────────────
# Tick field extraction (SFeedScrip - see note at top)
# ─────────────────────────────────────────────
# Confirmed field name from a real tick: 'volume_traded_today' (cumulative
# running total for the day). No guessing needed anymore.


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    client = NeoAPI(consumer_key=CONSUMER_KEY, environment="prod")

    totp = input("Enter current 6-digit TOTP: ").strip()
    client.totp_login(mobile_number=MOBILE_NUMBER, ucc=UCC, totp=totp)
    print("✅ TOTP accepted.")

    mpin = input("Enter your 6-digit MPIN: ").strip()
    client.totp_validate(mpin=mpin)
    print("✅ Logged in.")

    # Resolve each trading symbol to its instrument token
    ws_tokens = []
    symbol_by_token = {}
    for symbol, seg in SYMBOLS:
        result = client.search_scrip(exchange_segment=seg, symbol=symbol.replace("-EQ", ""))
        rows = result if isinstance(result, list) else result.get("data", []) if isinstance(result, dict) else []
        match = next((r for r in rows if r.get("pTrdSymbol") == symbol), rows[0] if rows else None)

        if not match:
            print(f"⚠️  Could not resolve token for {symbol} - skipping.")
            continue

        token = str(match.get("pSymbol") or match.get("instrument_token"))
        symbol_by_token[token] = symbol
        ws_tokens.append(WsToken(seg, token))
        print(f"Resolved {symbol} -> token {token}")

    builders = {sym: CandleBuilder(CANDLE_INTERVAL_MINUTES) for sym in symbol_by_token.values()}
    strategies = {sym: LowVolStrategy(sym) for sym in symbol_by_token.values()}
    first_tick_seen = set()

    print(f"Subscribing to {len(ws_tokens)} symbols. Streaming live ticks, building {CANDLE_INTERVAL_MINUTES}-minute candles...")

    async with client.create_websocket() as ws:
        await ws.subscribe_scrips(ws_tokens)

        async for message in ws:
            if not isinstance(message, SFeedScrip):
                continue

            symbol = message.trading_symbol or symbol_by_token.get(str(message.instrument_token))
            if symbol not in strategies:
                continue

            if symbol not in first_tick_seen:
                first_tick_seen.add(symbol)
                try:
                    raw = message.model_dump()
                except AttributeError:
                    raw = str(message)
                print(f"[{symbol}] RAW first tick (for field verification): {raw}")

            ltp = float(message.last_traded_price)
            cum_vol = float(message.volume_traded_today or 0)

            closed = builders[symbol].update(datetime.now().timestamp(), ltp, cum_vol)
            if closed:
                strategies[symbol].on_candle_close(closed)


if __name__ == "__main__":
    asyncio.run(main())