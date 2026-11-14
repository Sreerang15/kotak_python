"""
Kotak Neo - First-3-Candle VWAP Reversal Strategy (Alert Only, TRUE REALTIME)
------------------------------------------------------------------------------
Built on the CURRENT, actively-maintained SDK: `kotakneoapi` (PyPI), i.e.
github.com/Kotak-Neo/kotak-neo-python.

Install:
    pip install kotakneoapi python-dotenv

Create a .env file next to this script:
    CONSUMER_KEY=your-consumer-key-token
    MOBILE_NUMBER=+919876543210
    UCC=YOUR_UCC

Uses the async/await SFeed WebSocket client to get true tick-by-tick data for
a watchlist of stocks (loaded from symbols.txt), builds live 5-minute candles,
computes a running intraday VWAP from ticks, and checks the following pattern
on the FIRST THREE 5-minute candles of the day only:

    Candle 1 (e.g. 9:15-9:20): BULLISH  (close > open)  AND close > VWAP
    Candle 2 (e.g. 9:20-9:25): body < upper wick  AND  body < lower wick
    Candle 3 (e.g. 9:25-9:30): BEARISH  (close < open)  AND close < VWAP

If, and only if, candle 1 qualifies, then candle 2 qualifies, then candle 3
qualifies (all three, in that exact sequence) -> fire ONE alert for that
symbol for the day. After the 3rd candle closes, that symbol is done for the
day regardless of outcome (only the first 3 candles are ever evaluated).

You are expected to start this script manually at/just before 9:15 AM so the
first tick received begins the first candle. No scheduling is built in.

NO ORDERS ARE PLACED. Only ALERTS are printed to the console (plus the TOTP/MPIN
prompts at login). Alerts are also appended to alerts.log; everything else
(candle pass/fail, raw first tick, warnings) goes quietly to debug.log.

NOTE ON TICK FIELDS: confirmed field names from a real tick (same as the
reference script) - `trading_symbol`, `instrument_token`, `last_traded_price`,
`volume_traded_today` (cumulative running total for the day). The raw first
tick per symbol is written to debug.log so you can re-verify the schema.
"""

import ast
import asyncio
import os
from datetime import datetime, time as dtime

from dotenv import load_dotenv
from neo_api_client import NeoAPI
from neo_api_client.websocket.feed import WsToken, SFeedScrip

# ─────────────────────────────────────────────
# CONFIG - edit these
# ─────────────────────────────────────────────
load_dotenv()  # reads .env from the current directory

def _require_env(name):
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing {name} in .env file")
    return value

CONSUMER_KEY = _require_env("CONSUMER_KEY")    # NEO app -> Invest -> Trade API -> Your Applications
MOBILE_NUMBER = _require_env("MOBILE_NUMBER")  # with country code, e.g. +919876543210
UCC = _require_env("UCC")

SYMBOLS_FILE = "./symbols.txt"          # separate file, holds SYMBOLS = [(symbol, segment), ...]
CANDLE_INTERVAL_MINUTES = 5
FIRST_CANDLE_START = dtime(9, 15)       # candles before this wall-clock time are ignored
ALERT_LOG_FILE = "./alerts.log"
DEBUG_LOG_FILE = "./debug.log"   # candle pass/fail details, warnings, raw first ticks (never printed)
VERBOSE = True                   # write per-candle pass/fail details to debug.log


# ─────────────────────────────────────────────
# Load SYMBOLS from the external symbols.txt file
# ─────────────────────────────────────────────
def load_symbols(path):
    with open(path, "r") as f:
        source = f.read()
    module_ast = ast.parse(source, filename=path)
    for node in module_ast.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "SYMBOLS" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise ValueError(f"Could not find a SYMBOLS = [...] assignment in {path}")


# ─────────────────────────────────────────────
# Alert helper
# ─────────────────────────────────────────────
def log_alert(text: str):
    line = f"[{datetime.now().isoformat()}] {text}"
    print(f"\033[93m🔔 {line}\033[0m")
    with open(ALERT_LOG_FILE, "a") as f:
        f.write(line + "\n")


def log_debug(text: str):
    """Quiet diagnostics: written to debug.log only, never to the console."""
    with open(DEBUG_LOG_FILE, "a") as f:
        f.write(f"[{datetime.now().isoformat()}] {text}\n")


def log_verbose(text: str):
    if VERBOSE:
        log_debug(text)


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
# Running intraday VWAP, computed from ticks (typical price approximated
# by LTP, weighted by the per-tick traded-quantity delta)
# ─────────────────────────────────────────────
class VWAPCalculator:
    def __init__(self):
        self.day = None
        self.cum_pv = 0.0
        self.cum_vol = 0.0
        self.last_seen_volume = 0.0
        self.vwap = None

    def update_tick(self, ts, ltp, cum_volume_today):
        day = datetime.fromtimestamp(ts).date()
        if self.day != day:
            self.day = day
            self.cum_pv = 0.0
            self.cum_vol = 0.0
            self.last_seen_volume = 0.0

        delta = cum_volume_today - self.last_seen_volume
        if delta < 0:
            delta = 0  # guard against any feed resets/glitches
        self.last_seen_volume = cum_volume_today

        if delta > 0:
            self.cum_pv += ltp * delta
            self.cum_vol += delta

        self.vwap = (self.cum_pv / self.cum_vol) if self.cum_vol > 0 else ltp
        return self.vwap


# ─────────────────────────────────────────────
# Strategy state - evaluates ONLY the first 3 candles of each day
# ─────────────────────────────────────────────
class ThreeCandleVwapStrategy:
    def __init__(self, symbol):
        self.symbol = symbol
        self.current_day = None
        self.slot = 0           # how many of the first-3 candles we've evaluated today
        self.done_today = False
        self.candle1_ok = False
        self.candle2_ok = False

    def _reset_for_new_day(self, day):
        self.current_day = day
        self.slot = 0
        self.done_today = False
        self.candle1_ok = False
        self.candle2_ok = False

    def on_candle_close(self, candle, vwap_at_close):
        day = datetime.fromtimestamp(candle["start"]).date()
        if self.current_day != day:
            self._reset_for_new_day(day)

        if self.done_today:
            return  # already used up (or failed/succeeded) the first-3-candle window

        candle_time = datetime.fromtimestamp(candle["start"]).time()
        if candle_time < FIRST_CANDLE_START:
            return  # ignore any candle before the configured start time

        self.slot += 1
        o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
        body = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l

        if self.slot == 1:
            is_bullish = c > o
            above_vwap = vwap_at_close is not None and c > vwap_at_close
            self.candle1_ok = is_bullish and above_vwap
            log_verbose(
                f"[{self.symbol}] Candle 1 @ {candle_time}: O={o} C={c} VWAP={vwap_at_close:.2f} "
                f"bullish={is_bullish} above_vwap={above_vwap} -> {'PASS' if self.candle1_ok else 'FAIL'}"
            )
            if not self.candle1_ok:
                self.done_today = True  # sequence already broken, no point waiting for candles 2 & 3

        elif self.slot == 2:
            body_lt_upper = body < upper_wick
            body_lt_lower = body < lower_wick
            self.candle2_ok = self.candle1_ok and body_lt_upper and body_lt_lower
            log_verbose(
                f"[{self.symbol}] Candle 2 @ {candle_time}: body={body:.2f} upper_wick={upper_wick:.2f} "
                f"lower_wick={lower_wick:.2f} body<upper={body_lt_upper} body<lower={body_lt_lower} "
                f"-> {'PASS' if self.candle2_ok else 'FAIL'}"
            )
            if not self.candle2_ok:
                self.done_today = True

        elif self.slot == 3:
            is_bearish = c < o
            below_vwap = vwap_at_close is not None and c < vwap_at_close
            candle3_ok = self.candle2_ok and is_bearish and below_vwap
            log_verbose(
                f"[{self.symbol}] Candle 3 @ {candle_time}: O={o} C={c} VWAP={vwap_at_close:.2f} "
                f"bearish={is_bearish} below_vwap={below_vwap} -> {'PASS' if candle3_ok else 'FAIL'}"
            )
            if candle3_ok:
                log_alert(
                    f"3-CANDLE VWAP REVERSAL on {self.symbol} | "
                    f"C1(bullish>{'VWAP'})  C2(wick>body)  C3(bearish<VWAP, close={c}, vwap={vwap_at_close:.2f})"
                )
            self.done_today = True  # first-3-candle window is over either way


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    symbols = load_symbols(SYMBOLS_FILE)
    log_debug(f"Loaded {len(symbols)} symbols from {SYMBOLS_FILE}")

    client = NeoAPI(consumer_key=CONSUMER_KEY, environment="prod")

    totp = input("Enter current 6-digit TOTP: ").strip()
    client.totp_login(mobile_number=MOBILE_NUMBER, ucc=UCC, totp=totp)
    log_debug("TOTP accepted.")

    mpin = input("Enter your 6-digit MPIN: ").strip()
    client.totp_validate(mpin=mpin)
    print("✅ Logged in.")

    # Resolve each trading symbol to its instrument token
    ws_tokens = []
    symbol_by_token = {}
    for symbol, seg in symbols:
        result = client.search_scrip(exchange_segment=seg, symbol=symbol.replace("-EQ", ""))
        rows = result if isinstance(result, list) else result.get("data", []) if isinstance(result, dict) else []
        match = next((r for r in rows if r.get("pTrdSymbol") == symbol), rows[0] if rows else None)

        if not match:
            log_debug(f"Could not resolve token for {symbol} - skipping.")
            continue

        token = str(match.get("pSymbol") or match.get("instrument_token"))
        symbol_by_token[token] = symbol
        ws_tokens.append(WsToken(seg, token))
        log_debug(f"Resolved {symbol} -> token {token}")

    builders = {sym: CandleBuilder(CANDLE_INTERVAL_MINUTES) for sym in symbol_by_token.values()}
    vwaps = {sym: VWAPCalculator() for sym in symbol_by_token.values()}
    strategies = {sym: ThreeCandleVwapStrategy(sym) for sym in symbol_by_token.values()}
    last_vwap_snapshot = {sym: None for sym in symbol_by_token.values()}
    first_tick_seen = set()

    log_debug(
        f"Subscribing to {len(ws_tokens)} symbols. Streaming live ticks, building "
        f"{CANDLE_INTERVAL_MINUTES}-minute candles, evaluating first 3 candles from "
        f"{FIRST_CANDLE_START.strftime('%H:%M')}..."
    )

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
                log_debug(f"[{symbol}] RAW first tick (for field verification): {raw}")

            ltp = float(message.last_traded_price)
            cum_vol = float(message.volume_traded_today or 0)
            ts = datetime.now().timestamp()

            # Snapshot VWAP BEFORE this tick is folded in - that snapshot is what was
            # "true" as of the end of the previous candle, i.e. the candle this tick
            # might be closing.
            vwap_snapshot = last_vwap_snapshot[symbol]

            closed = builders[symbol].update(ts, ltp, cum_vol)

            new_vwap = vwaps[symbol].update_tick(ts, ltp, cum_vol)
            last_vwap_snapshot[symbol] = new_vwap

            if closed:
                strategies[symbol].on_candle_close(closed, vwap_snapshot if vwap_snapshot is not None else new_vwap)


if __name__ == "__main__":
    asyncio.run(main())