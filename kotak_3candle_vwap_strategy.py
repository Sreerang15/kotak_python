"""
Kotak Neo - First-3-Candle VWAP Reversal Strategy, Bullish + Bearish (Alert
Only, TRUE REALTIME)
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
computes a running intraday VWAP from ticks, and checks TWO mirror-image
patterns on the FIRST THREE 5-minute candles of the day only:

BEARISH (reversal down through VWAP):
    Candle 1: close > VWAP (bullish or bearish body, doesn't matter)
    Candle 2: body < upper wick
              AND candle 2 high > candle 1 high
    Candle 3: BEARISH (close < open)  AND close < VWAP
              AND candle 3 volume > candle 2 volume

BULLISH (mirror image, reversal up through VWAP):
    Candle 1: close < VWAP (bullish or bearish body, doesn't matter)
    Candle 2: body < lower wick
              AND candle 2 low < candle 1 low
    Candle 3: BULLISH (close > open)  AND close > VWAP
              AND candle 3 volume > candle 2 volume

A symbol can only complete one of the two patterns on a given day, since
candle 1 can't be both above and below VWAP at once. If, and only if, candle 1
qualifies, then candle 2 qualifies, then candle 3 qualifies (all three, in
that exact sequence, for the same direction) -> that symbol is added to the
day's Bullish or Bearish list. After the 3rd candle closes, that symbol is
done for the day regardless of outcome (only the first 3 candles are ever
evaluated).

PRE-FILTER (9:28 AM): candle 3 is still forming at 9:28, so at PRE_FILTER_TIME
the script takes every symbol that has already passed candles 1 and 2 and
checks the candle-3 rules against the in-progress candle (open, latest price,
volume so far, latest VWAP). Matches are printed once under a "Pre-filter"
heading, in the same Bullish / Bearish layout, and appended to pre_filter.log.
Because candle 3 isn't finished, this is an early heads-up: a stock can pass at
9:28 and fail at 9:30 (or the reverse); the final 9:31 summary is authoritative.

Once candle 3's window has closed for the day (a short grace period after
FIRST_CANDLE_START + 3 candle intervals), the results are printed to the
console ONCE as two grouped lines, e.g.:

    Bullish
    LTF , TCS

    Bearish
    VEDL , KEI

(-EQ is stripped from every symbol.)

You are expected to start this script manually at/just before 9:15 AM so the
first tick received begins the first candle. No scheduling is built in.

NO ORDERS ARE PLACED. Only the final Bullish/Bearish summary is printed to the
console (plus the TOTP/MPIN prompts at login). The same summary is appended to
alerts.log; everything else (candle pass/fail, raw first tick, warnings) goes
quietly to debug.log.

NOTE ON TICK FIELDS: confirmed field names from a real tick (same as the
reference script) - `trading_symbol`, `instrument_token`, `last_traded_price`,
`volume_traded_today` (cumulative running total for the day). The raw first
tick per symbol is written to debug.log so you can re-verify the schema.
"""

import ast
import asyncio
import os
from datetime import datetime, time as dtime, timedelta

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
FLUSH_GRACE_MINUTES = 1                 # wait this long after candle 3's window ends before printing
PRE_FILTER_TIME = dtime(9, 28)          # early check on the still-forming 3rd candle
PRE_FILTER_LOG_FILE = "./pre_filter.log"
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


def clean_symbol(symbol: str) -> str:
    """LTF-EQ -> LTF, MAHABANK-EQ -> MAHABANK"""
    return symbol.replace("-EQ", "")


def format_groups(bullish, bearish):
    return "\n".join(["Bullish", " , ".join(bullish), "", "Bearish", " , ".join(bearish)])


# ─────────────────────────────────────────────
# Results collector - groups qualifying symbols by direction and prints them
# together, once, after candle 3's window has closed for the day.
# ─────────────────────────────────────────────
class ResultsCollector:
    def __init__(self, flush_time_of_day: dtime):
        self.flush_time_of_day = flush_time_of_day
        self.day = None
        self.bullish = []
        self.bearish = []
        self.flushed = False

    def _reset_for_new_day(self, day):
        self.day = day
        self.bullish = []
        self.bearish = []
        self.flushed = False

    def add(self, now: datetime, direction: str, symbol: str):
        if self.day != now.date():
            self._reset_for_new_day(now.date())
        name = clean_symbol(symbol)
        target = self.bullish if direction == "bullish" else self.bearish
        if name not in target:
            target.append(name)

    def maybe_flush(self, now: datetime):
        if self.day != now.date():
            self._reset_for_new_day(now.date())
        if not self.flushed and now.time() >= self.flush_time_of_day:
            self._print_and_log()
            self.flushed = True

    def _print_and_log(self):
        block = format_groups(self.bullish, self.bearish)
        print(f"\033[93m{block}\033[0m")
        with open(ALERT_LOG_FILE, "a") as f:
            f.write(f"[{datetime.now().isoformat()}]\n{block}\n\n")


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
# Strategy state - evaluates ONLY the first 3 candles of each day.
# direction="bearish": close>VWAP, high breakout, then bearish close<VWAP.
# direction="bullish" : mirror image - close<VWAP, low breakdown, then
#                        bullish close>VWAP.
# BOTH directions also require candle 3 volume > candle 2 volume.
# ─────────────────────────────────────────────
class ThreeCandleVwapStrategy:
    def __init__(self, symbol, direction, results: ResultsCollector):
        assert direction in ("bullish", "bearish")
        self.symbol = symbol
        self.direction = direction
        self.results = results
        self.current_day = None
        self.slot = 0           # how many of the first-3 candles we've evaluated today
        self.done_today = False
        self.candle1_ok = False
        self.candle1_extreme = None   # high (bearish setup) or low (bullish setup)
        self.candle2_ok = False
        self.candle2_volume = None
        self.last_start = None  # start timestamp of the last candle this strategy evaluated

    def _reset_for_new_day(self, day):
        self.current_day = day
        self.slot = 0
        self.done_today = False
        self.candle1_ok = False
        self.candle1_extreme = None
        self.candle2_ok = False
        self.candle2_volume = None
        self.last_start = None

    def pre_filter_passes(self, forming, vwap_now):
        """Check candle-3 rules against the still-forming candle. Read-only: does
        not change any strategy state. `forming` is CandleBuilder.current."""
        if (self.done_today or self.slot != 2 or not self.candle2_ok
                or forming is None or vwap_now is None
                or self.last_start is None or forming["start"] <= self.last_start):
            return False
        o, c = forming["open"], forming["close"]
        v = forming["cum_end"] - forming["cum_start"]
        if self.candle2_volume is None or v <= self.candle2_volume:
            return False
        if self.direction == "bearish":
            return c < o and c < vwap_now
        return c > o and c > vwap_now

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
        self.last_start = candle["start"]
        o, h, l, c, v = candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"]
        body = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        tag = f"[{self.symbol}/{self.direction}]"

        if self.slot == 1:
            if self.direction == "bearish":
                self.candle1_ok = vwap_at_close is not None and c > vwap_at_close
                self.candle1_extreme = h
            else:
                self.candle1_ok = vwap_at_close is not None and c < vwap_at_close
                self.candle1_extreme = l
            log_verbose(
                f"{tag} Candle 1 @ {candle_time}: O={o} C={c} H={h} L={l} VWAP={vwap_at_close:.2f} "
                f"-> {'PASS' if self.candle1_ok else 'FAIL'}"
            )
            if not self.candle1_ok:
                self.done_today = True  # sequence already broken, no point waiting for candles 2 & 3

        elif self.slot == 2:
            if self.direction == "bearish":
                wick_ok = body < upper_wick   # bearish: body must be smaller than the UPPER wick
                extreme_ok = self.candle1_extreme is not None and h > self.candle1_extreme
            else:
                wick_ok = body < lower_wick   # bullish: body must be smaller than the LOWER wick
                extreme_ok = self.candle1_extreme is not None and l < self.candle1_extreme
            self.candle2_ok = self.candle1_ok and wick_ok and extreme_ok
            self.candle2_volume = v
            log_verbose(
                f"{tag} Candle 2 @ {candle_time}: body={body:.2f} upper_wick={upper_wick:.2f} "
                f"lower_wick={lower_wick:.2f} H={h} L={l} C1_extreme={self.candle1_extreme} "
                f"wick_ok={wick_ok} extreme_ok={extreme_ok} -> "
                f"{'PASS' if self.candle2_ok else 'FAIL'}"
            )
            if not self.candle2_ok:
                self.done_today = True

        elif self.slot == 3:
            if self.direction == "bearish":
                body_ok = c < o
                vwap_ok = vwap_at_close is not None and c < vwap_at_close
                volume_confirm = self.candle2_volume is not None and v > self.candle2_volume
            else:
                body_ok = c > o
                vwap_ok = vwap_at_close is not None and c > vwap_at_close
                volume_confirm = self.candle2_volume is not None and v > self.candle2_volume
            candle3_ok = self.candle2_ok and body_ok and vwap_ok and volume_confirm
            log_verbose(
                f"{tag} Candle 3 @ {candle_time}: O={o} C={c} V={v} C2_V={self.candle2_volume} "
                f"VWAP={vwap_at_close:.2f} body_ok={body_ok} vwap_ok={vwap_ok} "
                f"volume_confirm={volume_confirm} -> {'PASS' if candle3_ok else 'FAIL'}"
            )
            if candle3_ok:
                self.results.add(datetime.now(), self.direction, self.symbol)
            self.done_today = True  # first-3-candle window is over either way


# ─────────────────────────────────────────────
# Pre-filter: one early look at the still-forming 3rd candle
# ─────────────────────────────────────────────
class PreFilter:
    def __init__(self, run_time: dtime):
        self.run_time = run_time
        self.done_day = None

    def maybe_run(self, now: datetime, strategies, builders, vwaps):
        if self.done_day == now.date() or now.time() < self.run_time:
            return
        self.done_day = now.date()
        bullish, bearish = [], []
        for sym, strats in strategies.items():
            for st in strats:
                if st.pre_filter_passes(builders[sym].current, vwaps[sym].vwap):
                    (bullish if st.direction == "bullish" else bearish).append(clean_symbol(sym))
        block = "Pre-filter\n" + format_groups(bullish, bearish)
        #print(f"\033[96m{block}\033[0m")
        with open(PRE_FILTER_LOG_FILE, "a") as f:
            f.write(f"[{now.isoformat()}]\n{block}\n\n")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    symbols = load_symbols(SYMBOLS_FILE)
    #log_debug(f"Loaded {len(symbols)} symbols from {SYMBOLS_FILE}")

    client = NeoAPI(consumer_key=CONSUMER_KEY, environment="prod")

    totp = input("Enter current 6-digit TOTP: ").strip()
    client.totp_login(mobile_number=MOBILE_NUMBER, ucc=UCC, totp=totp)
    print("TOTP accepted.")

    mpin = input("Enter your 6-digit MPIN: ").strip()
    client.totp_validate(mpin=mpin)
    print("Logged in.")

    # Resolve each trading symbol to its instrument token
    ws_tokens = []
    symbol_by_token = {}
    for symbol, seg in symbols:
        result = client.search_scrip(exchange_segment=seg, symbol=symbol.replace("-EQ", ""))
        rows = result if isinstance(result, list) else result.get("data", []) if isinstance(result, dict) else []
        match = next((r for r in rows if r.get("pTrdSymbol") == symbol), rows[0] if rows else None)

        if not match:
            #log_debug(f"Could not resolve token for {symbol} - skipping.")
            continue

        token = str(match.get("pSymbol") or match.get("instrument_token"))
        symbol_by_token[token] = symbol
        ws_tokens.append(WsToken(seg, token))
        #log_debug(f"Resolved {symbol} -> token {token}")

    # Candle 3's window ends at FIRST_CANDLE_START + 3 intervals; flush the
    # grouped Bullish/Bearish summary shortly after that.
    flush_dt = datetime.combine(datetime.min, FIRST_CANDLE_START) + timedelta(
        minutes=3 * CANDLE_INTERVAL_MINUTES + FLUSH_GRACE_MINUTES
    )
    results = ResultsCollector(flush_time_of_day=flush_dt.time())
    pre_filter = PreFilter(PRE_FILTER_TIME)

    builders = {sym: CandleBuilder(CANDLE_INTERVAL_MINUTES) for sym in symbol_by_token.values()}
    vwaps = {sym: VWAPCalculator() for sym in symbol_by_token.values()}
    # Each symbol runs both the bullish and bearish pattern in parallel;
    # only one of the two can actually complete, since candle 1 can't be
    # both above and below VWAP at once.
    strategies = {
        sym: [
            ThreeCandleVwapStrategy(sym, "bullish", results),
            ThreeCandleVwapStrategy(sym, "bearish", results),
        ]
        for sym in symbol_by_token.values()
    }
    last_vwap_snapshot = {sym: None for sym in symbol_by_token.values()}
    first_tick_seen = set()

    # log_debug(
    #     f"Subscribing to {len(ws_tokens)} symbols. Streaming live ticks, building "
    #     f"{CANDLE_INTERVAL_MINUTES}-minute candles, evaluating first 3 candles from "
    #     f"{FIRST_CANDLE_START.strftime('%H:%M')}..."
    # )

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
                #log_debug(f"[{symbol}] RAW first tick (for field verification): {raw}")

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
                for strat in strategies[symbol]:
                    strat.on_candle_close(closed, vwap_snapshot if vwap_snapshot is not None else new_vwap)

            now = datetime.now()
            pre_filter.maybe_run(now, strategies, builders, vwaps)
            results.maybe_flush(now)


if __name__ == "__main__":
    asyncio.run(main())

#      python kotak_3candle_vwap_strategy.py