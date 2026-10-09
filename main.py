"""
Multi-Chain Telegram Alert Bot  (Solana | Ethereum | BNB Chain | Base | Robinhood Chain)
-----------------------------------------------------------------------------------------
Module 1: Zipcoin Re-Run Scanner  - DexScreener REST, polled every 60s, all chains
Module 2: Whale Tracker webhooks  - POST /webhook      (Helius, Solana)
                                    POST /webhook/evm  (Alchemy Address Activity, EVM chains)

Run locally:  uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
from collections import Counter
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# ----------------------------------------------------------------------------
# CONFIG (environment variables)
# ----------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
RAW_TOKEN_LIST = os.getenv("TOKEN_LIST", "")          # "solana:MINT,base:0x...,bsc:0x..."
RAW_PEAK_SEEDS = os.getenv("PEAK_SEEDS", "{}") or "{}"  # {"base:0xabc": 45000000}
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")       # Helius authHeader value
ALCHEMY_SIGNING_KEY = os.getenv("ALCHEMY_SIGNING_KEY", "")  # Alchemy webhook signing key
EVM_WHALES = {w.strip().lower() for w in os.getenv("EVM_WHALES", "").split(",") if w.strip()}
STATE_FILE = os.getenv("STATE_FILE", "state.json")

# Module 1 thresholds (applied identically on every chain)
POLL_SECONDS = 60
MIN_PEAK_MC = float(os.getenv("MIN_PEAK_MC", "10000000"))
RETRACE_MIN, RETRACE_MAX = 60.0, 75.0
MIN_LIQ_TO_MC = 0.06
MIN_VOL_TO_MC = 0.50
MIN_TRADERS_24H = 1_000
MIN_PAIR_AGE_HOURS = 24
COOLDOWN_SECONDS = 12 * 3600
MIN_LIVE_LIQUIDITY_USD = 1_000  # pairs below this are treated as dead/drained

# Auto-discovery (the bot finds coins by itself; TOKEN_LIST becomes optional)
AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "true").lower() != "false"
DISCOVERY_SECONDS = int(os.getenv("DISCOVERY_SECONDS", "300"))
MAX_DISCOVERED = int(os.getenv("MAX_DISCOVERED", "400"))
DISMISS_SECONDS = 24 * 3600          # coins judged hopeless are ignored for 24h
MAX_BACKFILL_PER_CYCLE = 8           # peak look-ups per minute (free API allows ~30/min)
MIN_MC_FRACTION = 0.25               # a coin can't be 60-75% down from a peak >= MIN_PEAK_MC
                                     # unless its current MC is >= 25% of MIN_PEAK_MC
DISCOVERY_FEEDS = [
    "https://api.dexscreener.com/token-profiles/latest/v1",
    "https://api.dexscreener.com/token-boosts/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1",
]
GECKO_NETWORK = {"solana": "solana", "ethereum": "eth", "bsc": "bsc",
                 "base": "base", "robinhood": "robinhood"}

# Module 2 threshold
MIN_WHALE_TRADE_USD = 5_000

# --- Learning system ---------------------------------------------------------
WIN_MULTIPLE = float(os.getenv("WIN_MULTIPLE", "2.0"))     # win = reaches 2x the call market cap...
STOP_MULTIPLE = float(os.getenv("STOP_MULTIPLE", "0.5"))   # ...before falling to 0.5x of it
TRACK_HOURS = float(os.getenv("TRACK_HOURS", "72"))        # how long each call is followed
AUTO_TUNE = os.getenv("AUTO_TUNE", "false").lower() == "true"  # let the bot adjust its own filters
MIN_CALLS_TUNE = int(os.getenv("MIN_CALLS_TUNE", "10"))    # finished calls needed before self-tuning
MIN_SHADOWS_TUNE = 8                                       # finished near-misses needed per experiment
REPORT_EVERY_CALLS = int(os.getenv("REPORT_EVERY_CALLS", "3"))
MAX_SHADOWS_OPEN = 60
SHADOW_COOLDOWN = 24 * 3600
TELEGRAM_BACKUP = os.getenv("TELEGRAM_BACKUP", "true").lower() != "false"
BACKUP_TAG = "BOT_STATE_BACKUP v1"
BACKUP_MIN_GAP = 300
MAX_SHADOWS_OPEN = 150

# --- Smart wallets, themes, narrative radar -----------------------------------
SMART_CLUSTER_MIN = int(os.getenv("SMART_CLUSTER_MIN", "2"))   # wallets buying the same coin...
CLUSTER_WINDOW = 1800                                          # ...within 30 minutes = cluster
ZONE_MIN, ZONE_MAX = 50.0, 85.0   # a whale buy inside this retrace zone gets the "re-run" tag
NARRATIVE_ALERTS = os.getenv("NARRATIVE_ALERTS", "true").lower() != "false"
NARR_MIN_MC = float(os.getenv("NARR_MIN_MC", "250000"))
NARR_MAX_MC = float(os.getenv("NARR_MAX_MC", "25000000"))
NARR_MIN_LIQ = float(os.getenv("NARR_MIN_LIQ", "40000"))
NARR_MIN_VOL_RATIO = float(os.getenv("NARR_MIN_VOL_RATIO", "1.0"))  # 24h volume / market cap
NARR_MIN_TRADERS = int(os.getenv("NARR_MIN_TRADERS", "500"))
NARR_MAX_H24 = float(os.getenv("NARR_MAX_H24", "150"))   # % - skip coins that already ran
NARR_MIN_AGE_H = float(os.getenv("NARR_MIN_AGE_H", "3"))
NARR_MAX_PER_DAY = int(os.getenv("NARR_MAX_PER_DAY", "5"))

DEFAULT_THEMES = {
    "stocks": "stonk,stonks,stock,stocks,nasdaq,nyse,nvda,nvidia,tsla,tesla,mstr,spy,aapl,msft,amzn,"
              "ipo,earnings,wallstreet,wsb,sp500,etf",
    "ai": "ai,agent,agents,gpt,llm,openai,grok,agi,accelerationism,eacc,neural",
    "politics": "trump,maga,biden,vance,harris,election,potus,congress",
    "celebrity": "elon,musk,kanye,snoop,taylor",
    "animals": "dog,cat,frog,pepe,inu,ape,monkey,pig,hamster,penguin,shiba",
    "space": "mars,moon,rocket,spacex,nasa",
}


def load_themes() -> dict[str, set[str]]:
    themes = {k: {w.strip() for w in v.split(",") if w.strip()} for k, v in DEFAULT_THEMES.items()}
    try:  # THEME_KEYWORDS='{"stocks": ["abc"], "newtheme": ["word1","word2"]}' adds to / extends the list
        for k, words in json.loads(os.getenv("THEME_KEYWORDS", "{}") or "{}").items():
            themes.setdefault(k, set()).update(str(w).strip().lower() for w in words)
    except (json.JSONDecodeError, AttributeError, TypeError):
        logging.getLogger("bot").error("THEME_KEYWORDS is not valid JSON - ignored")
    return themes


THEMES = load_themes()
WORD = re.compile(r"[a-z0-9]+")

# Filters the bot is allowed to tune (everything else stays fixed)
T = {"retrace_min": RETRACE_MIN, "retrace_max": RETRACE_MAX, "liq": MIN_LIQ_TO_MC,
     "vol": MIN_VOL_TO_MC, "traders": float(MIN_TRADERS_24H)}
BOUNDS = {"retrace_min": (50.0, 70.0), "retrace_max": (70.0, 85.0), "liq": (0.03, 0.20),
          "vol": (0.25, 1.5), "traders": (300.0, 5000.0)}
PARAM_LABEL = {"retrace_min": "minimum retrace %", "retrace_max": "maximum retrace %",
               "liq": "minimum liquidity/market-cap", "vol": "minimum volume/market-cap",
               "traders": "minimum 24h transactions"}
CODE_LABEL = {"retrace_lo": "retrace too small", "retrace_hi": "retrace too deep",
              "liq": "liquidity", "vol": "volume", "traders": "transactions"}

DEX_API = "https://api.dexscreener.com/tokens/v1"

# chain id (DexScreener) -> explorer + display info
CHAINS = {
    "solana":    {"name": "Solana",         "explorer": "https://solscan.io",
                  "token": "/token/{}", "addr": "/account/{}", "tx": "/tx/{}"},
    "ethereum":  {"name": "Ethereum",       "explorer": "https://etherscan.io",
                  "token": "/token/{}", "addr": "/address/{}", "tx": "/tx/{}"},
    "bsc":       {"name": "BNB Chain",      "explorer": "https://bscscan.com",
                  "token": "/token/{}", "addr": "/address/{}", "tx": "/tx/{}"},
    "base":      {"name": "Base",           "explorer": "https://basescan.org",
                  "token": "/token/{}", "addr": "/address/{}", "tx": "/tx/{}"},
    "robinhood": {"name": "Robinhood Chain", "explorer": "https://robinhoodchain.blockscout.com",
                  "token": "/token/{}", "addr": "/address/{}", "tx": "/tx/{}"},
}
ALIASES = {
    "sol": "solana", "eth": "ethereum", "bnb": "bsc", "binance": "bsc", "bnbchain": "bsc",
    "hood": "robinhood", "rh": "robinhood",
}

# Known quote assets used to value EVM swaps (addresses lowercase)
WRAPPED_NATIVE = {
    "ethereum": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "base":     "0x4200000000000000000000000000000000000006",
    "bsc":      "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
}
EVM_STABLES = {
    "ethereum": {"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   # USDC
                 "0xdac17f958d2ee523a2206206994597c13d831ec7",   # USDT
                 "0x6b175474e89094c44da98b954eedeac495271d0f"},  # DAI
    "base":     {"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"},  # USDC
    "bsc":      {"0x55d398326f99059ff775485246999027b3197955",   # USDT
                 "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",   # USDC
                 "0xe9e7cea3dedca5984780bafc599bd69add087d56"},  # BUSD
    "robinhood": set(),  # unknown stables fall back to DexScreener pricing (USDG ~ $1)
}
NATIVE_PRICE_SOURCE = {  # chain -> (dexscreener chain, wrapped native address) for native coin price
    "ethereum": ("ethereum", WRAPPED_NATIVE["ethereum"]),
    "base": ("ethereum", WRAPPED_NATIVE["ethereum"]),
    "robinhood": ("ethereum", WRAPPED_NATIVE["ethereum"]),  # native gas token is ETH
    "bsc": ("bsc", WRAPPED_NATIVE["bsc"]),
}

# Solana quote mints
WSOL = "So11111111111111111111111111111111111111112"
USDC_SOL = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_SOL = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
SOL_STABLES = {USDC_SOL, USDT_SOL}
SOL_QUOTES = SOL_STABLES | {WSOL}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")
http = requests.Session()
http.headers.update({"User-Agent": "multichain-alert-bot/2.0"})


# ----------------------------------------------------------------------------
# CHAIN / TOKEN HELPERS
# ----------------------------------------------------------------------------
def norm_addr(chain: str, addr: str) -> str:
    return addr if chain == "solana" else addr.lower()


def norm_chain(c: str) -> str | None:
    c = c.strip().lower()
    c = ALIASES.get(c, c)
    return c if c in CHAINS else None


def parse_token_ref(ref: str):
    """'base:0xabc' -> ('base','0xabc'). Bare non-0x address is assumed Solana."""
    ref = ref.strip()
    if not ref:
        return None
    if ":" in ref:
        c, a = ref.split(":", 1)
        chain = norm_chain(c)
        if not chain:
            log.warning("Unknown chain '%s' in '%s' (valid: %s)", c, ref, ", ".join(CHAINS))
            return None
        return chain, norm_addr(chain, a.strip())
    if ref.startswith("0x"):
        log.warning("'%s' is an EVM address with no chain prefix - use e.g. base:%s", ref, ref)
        return None
    return "solana", ref


def parse_token_list(raw: str) -> list[tuple[str, str]]:
    seen, out = set(), []
    for item in raw.split(","):
        t = parse_token_ref(item)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


TOKENS = parse_token_list(RAW_TOKEN_LIST)


def sk(chain: str, addr: str) -> str:
    """State key."""
    return f"{chain}:{norm_addr(chain, addr)}"


def link(chain: str, kind: str, value: str) -> str:
    c = CHAINS[chain]
    return c["explorer"] + c[kind].format(value)


def short(addr: str) -> str:
    return f"{addr[:4]}...{addr[-4:]}" if len(addr) > 12 else addr


# ----------------------------------------------------------------------------
# STATE (peaks + cooldowns), JSON-persisted
# ----------------------------------------------------------------------------
_state_lock = threading.Lock()
state = {"peaks": {}, "cooldowns": {}, "discovered": {}, "dismissed": {}, "backfilled": {},
         "bf_fails": {}, "shadow_cd": {}, "meta": {}, "whale_buys": {},
         "records": [], "tuning": {}, "backup": {}, "wallets": {}}
DICT_BUCKETS = ("peaks", "cooldowns", "discovered", "dismissed", "backfilled", "bf_fails",
                "shadow_cd", "meta", "whale_buys")


def merge_state(loaded: dict):
    for bucket in DICT_BUCKETS:
        for k, v in (loaded.get(bucket) or {}).items():
            state[bucket][k if ":" in k else f"solana:{k}"] = v  # migrate v1 keys
    state["wallets"].update(loaded.get("wallets") or {})
    state["records"].extend(loaded.get("records") or [])
    state["tuning"].update(loaded.get("tuning") or {})
    state["backup"].update(loaded.get("backup") or {})


def load_state():
    loaded = None
    try:
        with open(STATE_FILE) as f:
            loaded = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    if loaded is not None:
        merge_state(loaded)
    elif TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:  # Render's free disk is wiped on restart -> recover memory from Telegram
            if restore_from_telegram():
                log.info("Restored memory from Telegram backup (%d tracked calls)", len(state["records"]))
            else:
                log.info("No Telegram backup found - starting with a fresh memory")
        except Exception as e:
            log.warning("Could not restore from Telegram backup: %s", scrub(e))
    try:
        for ref, peak in json.loads(RAW_PEAK_SEEDS).items():
            t = parse_token_ref(ref)
            if t:
                key = sk(*t)
                state["peaks"][key] = max(state["peaks"].get(key, 0), float(peak))
    except (json.JSONDecodeError, ValueError, AttributeError):
        log.error("PEAK_SEEDS is not valid JSON - ignored")
    if AUTO_TUNE:
        for param, val in (state["tuning"].get("T") or {}).items():
            if param in T:
                T[param] = float(val)
        log.info("Self-tuning ON. Current filters: %s", T)


def save_state():
    with _state_lock:
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, STATE_FILE)
        except (OSError, RuntimeError, TypeError, ValueError) as e:
            log.warning("Could not persist state: %s", e)


# ----------------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------------
def md_escape(text: str) -> str:
    for ch in ("_", "*", "`", "["):
        text = text.replace(ch, "\\" + ch)
    return text


def scrub(e) -> str:
    """Hide the bot token if it appears in an error message (so logs are safe to share)."""
    t = str(e)
    return t.replace(TELEGRAM_BOT_TOKEN, "***") if TELEGRAM_BOT_TOKEN else t


def send_telegram(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "Markdown", "disable_web_page_preview": True}
    try:
        r = http.post(url, json=payload, timeout=15)
        if r.status_code == 400:  # markdown parse problem -> resend plain
            payload.pop("parse_mode")
            r = http.post(url, json=payload, timeout=15)
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        log.error("Telegram send failed: %s", scrub(e))
        return False


# ----------------------------------------------------------------------------
# DEXSCREENER HELPERS
# ----------------------------------------------------------------------------
def get_with_retry(url: str, tries: int = 4, **kw):
    """GET that backs off and retries when the API says 'Too Many Requests' (429) or errors (5xx).
    Render's free servers share an IP address, so 429s are common."""
    kw.setdefault("timeout", 15)
    for attempt in range(1, tries + 1):
        r = http.get(url, **kw)
        if r.status_code in (429, 500, 502, 503, 504) and attempt < tries:
            try:
                wait = float(r.headers.get("Retry-After", ""))
            except (TypeError, ValueError):
                wait = 4.0 * attempt
            time.sleep(min(wait, 20.0))
            continue
        r.raise_for_status()
        return r
    return r


def fetch_pairs(chain: str, addrs: list[str]) -> list[dict]:
    """Up to 30 comma-separated token addresses per call, one chain per call."""
    r = get_with_retry(f"{DEX_API}/{chain}/{','.join(addrs)}")
    data = r.json()
    return data if isinstance(data, list) else (data.get("pairs") or [])


def num(d, *path, default=0.0) -> float:
    for key in path:
        if not isinstance(d, dict):
            return default
        d = d.get(key)
    try:
        return float(d) if d is not None else default
    except (TypeError, ValueError):
        return default


def best_live_pair(pairs: list[dict]):
    """
    Dead/migrated-rug filter: drop drained pairs, ignore the stale pump.fun
    bonding-curve pair when a real DEX pair exists, take the deepest liquidity.
    """
    live = [p for p in pairs if num(p, "liquidity", "usd") >= MIN_LIVE_LIQUIDITY_USD]
    if not live:
        return None
    non_curve = [p for p in live if p.get("dexId") != "pumpfun"]
    return max(non_curve or live, key=lambda p: num(p, "liquidity", "usd"))


_price_cache: dict[str, tuple[float, float, str]] = {}


def dex_price(chain: str, addr: str):
    """(price_usd, symbol) from the deepest pair; cached 60s."""
    key = sk(chain, addr)
    hit = _price_cache.get(key)
    if hit and time.time() - hit[0] < 60:
        return hit[1], hit[2]
    try:
        want = norm_addr(chain, addr)
        pairs = [p for p in fetch_pairs(chain, [addr])
                 if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == want]
        if pairs:
            p = max(pairs, key=lambda x: num(x, "liquidity", "usd"))
            price, sym = num(p, "priceUsd"), p["baseToken"].get("symbol", "?")
            _price_cache[key] = (time.time(), price, sym)
            return price, sym
    except requests.RequestException as e:
        log.warning("price lookup failed for %s: %s", key, e)
    return 0.0, "?"


# ----------------------------------------------------------------------------
# MODULE 1: RE-RUN SCANNER
# ----------------------------------------------------------------------------
def fmt_usd(v: float) -> str:
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.2f}M"
    if v >= 1e3:
        return f"${v/1e3:.1f}K"
    return f"${v:,.2f}"


def compute_metrics(key: str, pair: dict):
    """All numbers the filters need (also updates the locally observed peak). None if no market cap."""
    mc = num(pair, "marketCap") or num(pair, "fdv")
    if mc <= 0:
        return None
    peak = max(state["peaks"].get(key, 0.0), mc)
    state["peaks"][key] = peak
    liq = num(pair, "liquidity", "usd")
    vol24 = num(pair, "volume", "h24")
    traders = int(num(pair, "txns", "h24", "buys") + num(pair, "txns", "h24", "sells"))
    created_ms = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created_ms) / 3_600_000 if created_ms else 0.0
    return dict(mc=mc, peak=peak, retrace=(peak - mc) / peak * 100 if peak else 0.0, liq=liq,
                liq_ratio=liq / mc, vol24=vol24, vol_ratio=vol24 / mc, traders=traders, age_h=age_h)


def evaluate(key: str, pair: dict):
    """Returns (passed, metrics, fails) where fails = [(code, text), ...]."""
    m = compute_metrics(key, pair)
    if m is None:
        return False, {}, [("nomc", "no market cap")]
    mc, peak, retrace = m["mc"], m["peak"], m["retrace"]
    fails = []
    if peak < MIN_PEAK_MC:
        fails.append(("peak", f"peak {fmt_usd(peak)} < {fmt_usd(MIN_PEAK_MC)}"))
    if retrace < T["retrace_min"]:
        fails.append(("retrace_lo", f"retrace {retrace:.1f}% < {T['retrace_min']:.0f}%"))
    elif retrace > T["retrace_max"]:
        fails.append(("retrace_hi", f"retrace {retrace:.1f}% > {T['retrace_max']:.0f}%"))
    if m["liq_ratio"] < T["liq"]:
        fails.append(("liq", f"liq/mc {m['liq_ratio']:.3f} < {T['liq']}"))
    if m["vol24"] < mc * T["vol"]:
        fails.append(("vol", f"vol24 {fmt_usd(m['vol24'])} < {T['vol']*100:.0f}% of MC"))
    if m["traders"] < T["traders"]:
        fails.append(("traders", f"traders {m['traders']} < {T['traders']:.0f}"))
    if m["age_h"] < MIN_PAIR_AGE_HOURS:
        fails.append(("age", f"age {m['age_h']:.1f}h < {MIN_PAIR_AGE_HOURS}h"))
    return not fails, m, fails


def is_near(code: str, m: dict) -> bool:
    """Is a single failed filter only a narrow miss? (candidate for a 'shadow' experiment)"""
    if code == "retrace_lo":
        return m["retrace"] >= T["retrace_min"] - 12
    if code == "retrace_hi":
        return m["retrace"] <= T["retrace_max"] + 12
    if code == "liq":
        return m["liq_ratio"] >= T["liq"] * 0.5
    if code == "vol":
        return m["vol_ratio"] >= T["vol"] * 0.5
    if code == "traders":
        return m["traders"] >= T["traders"] * 0.5
    return False


def build_rerun_message(chain: str, pair: dict, m: dict, extra: str = "") -> str:
    base = pair.get("baseToken", {})
    addr = base.get("address", "")
    symbol = md_escape(base.get("symbol", "?"))
    pair_addr = pair.get("pairAddress", "")
    return (
        f"🔁 *ZIPCOIN RE-RUN SETUP: ${symbol}*  |  {CHAINS[chain]['name']}\n\n"
        f"📈 Peak MC: *{fmt_usd(m['peak'])}*\n"
        f"💰 Current MC: *{fmt_usd(m['mc'])}*\n"
        f"📉 Retrace: *-{m['retrace']:.1f}%*\n"
        f"💧 Liquidity: {fmt_usd(m['liq'])} ({m['liq_ratio']*100:.1f}% of MC)\n"
        f"📊 24h Volume: {fmt_usd(m['vol24'])} ({m['vol24']/m['mc']*100:.0f}% of MC)\n"
        f"👥 24h Txns (buys+sells): {m['traders']:,}\n"
        f"⏱ Pair age: {m['age_h']:.1f}h\n"
        f"📌 Tracking this call for {TRACK_HOURS:.0f}h (win = {WIN_MULTIPLE:g}x)\n"
        + extra + "\n"
        f"`{addr}`\n\n"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair_addr}) | "
        f"[Explorer]({link(chain, 'token', addr)})"
    )


def watch_list() -> list[tuple[str, str]]:
    """Manual TOKEN_LIST plus everything auto-discovered."""
    seen, out = set(), []
    tracked = {r["key"] for r in state["records"] if r["status"] == "open"}
    for chain, addr in (list(TOKENS) + [tuple(k.split(":", 1)) for k in state["discovered"]]
                        + [tuple(k.split(":", 1)) for k in tracked]):
        if (chain, addr) not in seen:
            seen.add((chain, addr))
            out.append((chain, addr))
    return out


def dismiss(key: str):
    state["discovered"].pop(key, None)
    state["meta"].pop(key, None)
    state["dismissed"][key] = time.time()


def discover_tokens() -> int:
    """Pull DexScreener's free profile/boost feeds and add coins on our chains."""
    now = time.time()
    manual = set(TOKENS)
    for k in [k for k, t in state["dismissed"].items() if now - t > DISMISS_SECONDS]:
        state["dismissed"].pop(k, None)
    added = failures = 0
    for n, url in enumerate(DISCOVERY_FEEDS):
        if n:
            time.sleep(3)  # space the three feed calls out
        try:
            items = get_with_retry(url).json()
        except (requests.RequestException, ValueError) as e:
            failures += 1
            status = getattr(getattr(e, "response", None), "status_code", "error")
            log.warning("discovery feed busy (%s) on %s", status, url.rsplit("/", 2)[-2])
            continue
        if not isinstance(items, list):
            continue
        for it in items:
            chain = norm_chain(str(it.get("chainId", "")))
            addr = it.get("tokenAddress")
            if not chain or not addr:
                continue
            addr = norm_addr(chain, addr)
            key = sk(chain, addr)
            if (chain, addr) in manual or key in state["discovered"] or key in state["dismissed"]:
                continue
            state["discovered"][key] = now
            desc = (it.get("description") or "").strip()[:200]
            if desc:
                state["meta"][key] = {"d": desc}  # used for theme detection
            added += 1
    if failures == len(DISCOVERY_FEEDS):
        return -1  # every feed failed - caller retries soon
    overflow = len(state["discovered"]) - MAX_DISCOVERED
    if overflow > 0:  # drop the oldest
        for k in sorted(state["discovered"], key=state["discovered"].get)[:overflow]:
            state["discovered"].pop(k, None)
    return added


def passes_cheap_checks(pair: dict, relax: float = 1.0) -> bool:
    """Everything except peak/retrace - decides if a peak look-up is worth it.
    relax=0.5 also admits near-misses so they can be followed as experiments."""
    mc = num(pair, "marketCap") or num(pair, "fdv")
    if mc < MIN_PEAK_MC * MIN_MC_FRACTION:
        return False
    created = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3_600_000 if created else 0.0
    traders = num(pair, "txns", "h24", "buys") + num(pair, "txns", "h24", "sells")
    return (age_h >= MIN_PAIR_AGE_HOURS
            and num(pair, "liquidity", "usd") / mc >= T["liq"] * relax
            and num(pair, "volume", "h24") >= mc * T["vol"] * relax
            and traders >= T["traders"] * relax)


def fetch_peak_mc(chain: str, pair: dict):
    """
    Estimate all-time-high market cap from GeckoTerminal daily candles:
    highest daily high x (current market cap / current price).
    """
    net, pool = GECKO_NETWORK.get(chain), pair.get("pairAddress")
    price = num(pair, "priceUsd")
    mc = num(pair, "marketCap") or num(pair, "fdv")
    if not (net and pool and price > 0 and mc > 0):
        return None
    r = http.get(
        f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}/ohlcv/day",
        params={"aggregate": 1, "limit": 1000, "currency": "usd"},
        headers={"Accept": "application/json;version=20230302"}, timeout=20)
    r.raise_for_status()
    candles = r.json()["data"]["attributes"]["ohlcv_list"]
    highs = [float(c[2]) for c in candles if len(c) >= 3]
    return max(highs) * (mc / price) if highs else None


def maybe_backfill_peak(key: str, chain: str, pair: dict, budget: list):
    if key in state["backfilled"] or budget[0] <= 0 or not passes_cheap_checks(pair, relax=0.5):
        return
    budget[0] -= 1
    time.sleep(2.2)  # stay under GeckoTerminal's free rate limit
    try:
        peak = fetch_peak_mc(chain, pair)
    except (requests.RequestException, KeyError, ValueError, TypeError) as e:
        peak = None
        log.warning("peak look-up failed for %s: %s", key, e)
    if peak:
        state["peaks"][key] = max(state["peaks"].get(key, 0.0), peak)
        state["backfilled"][key] = time.time()
        log.info("peak for %s estimated at %s", key, fmt_usd(state["peaks"][key]))
    else:
        state["bf_fails"][key] = state["bf_fails"].get(key, 0) + 1
        if state["bf_fails"][key] >= 3:  # give up, rely on locally observed peak
            state["backfilled"][key] = time.time()


def scan_once():
    watch = watch_list()
    if not watch:
        log.info("Nothing to scan yet - waiting for discovery to find coins")
        return
    now = time.time()
    manual = set(TOKENS)
    budget = [MAX_BACKFILL_PER_CYCLE]
    checked = alerts = 0

    by_chain: dict[str, list[str]] = {}
    for chain, addr in watch:
        by_chain.setdefault(chain, []).append(addr)

    for chain, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            batch = addrs[i:i + 30]
            time.sleep(1.0)  # space requests out - Render's shared IP gets rate-limited easily
            try:
                pairs = fetch_pairs(chain, batch)
            except requests.RequestException as e:
                status = getattr(getattr(e, "response", None), "status_code", "error")
                log.warning("[%s] DexScreener busy (%s) - will retry next minute", chain, status)
                continue

            grouped: dict[str, list[dict]] = {}
            for p in pairs:
                a = norm_addr(chain, p.get("baseToken", {}).get("address", ""))
                grouped.setdefault(a, []).append(p)

            for addr in batch:
                key = sk(chain, addr)
                is_manual = (chain, addr) in manual
                pair = best_live_pair(grouped.get(norm_addr(chain, addr), []))
                if not pair:  # dead / migrated / drained
                    if open_records(key):
                        track(key, 0.0, now)  # liquidity gone -> counts as a stop-out
                    elif not is_manual:
                        dismiss(key)
                    continue
                mc = num(pair, "marketCap") or num(pair, "fdv")
                sym = pair["baseToken"].get("symbol", "?")
                track(key, mc, now)  # update outcomes of earlier calls on this coin

                if not is_manual and mc < MIN_PEAK_MC * MIN_MC_FRACTION and not open_records(key):
                    # too small for a re-run setup; keep it only if it matches a theme (narrative radar)
                    if NARRATIVE_ALERTS and mc >= NARR_MIN_MC and themes_for(key, pair):
                        maybe_narrative(key, chain, pair, now)
                    else:
                        dismiss(key)
                    continue

                maybe_backfill_peak(key, chain, pair, budget)
                checked += 1
                passed, m, fails = evaluate(key, pair)
                if not passed:
                    codes = {c for c, _ in fails}
                    if len(fails) <= 1:  # near miss - worth seeing in the logs
                        log.info("[%s] %s NEAR MISS: %s", chain, sym, "; ".join(t for _, t in fails))
                    if len(fails) == 1 and is_near(fails[0][0], m):
                        maybe_add_shadow(key, chain, pair, m, fails[0][0], now)
                    elif len(fails) >= 2 and not (codes & {"peak", "age", "nomc"}) and 45 <= m["retrace"] <= 92:
                        # in the retrace zone but blocked by several rules: follow it to see if we MISSED a runner
                        maybe_add_shadow(key, chain, pair, m, "+".join(sorted(codes)), now)
                    if m:
                        maybe_narrative(key, chain, pair, now, m)
                    continue
                if now - state["cooldowns"].get(key, 0) < COOLDOWN_SECONDS:
                    log.info("%s passed filters but is on cooldown", key)
                    continue
                th, sw = themes_for(key, pair), len(recent_wallets(key))
                extra = ((f"🏷 Themes: {', '.join(th)}\n" if th else "")
                         + (f"👛 Tracked wallets that bought in the last 24h: {sw}\n" if sw else ""))
                if send_telegram(build_rerun_message(chain, pair, m, extra)):
                    state["cooldowns"][key] = now
                    add_record("call", key, chain, pair, m, "", now)
                    alerts += 1
                    log.info("ALERT sent for %s", key)

    try:
        learning_cycle()
    except Exception:
        log.exception("learning cycle error")
    save_state()
    try:
        backup_state_to_telegram()
    except Exception as e:
        log.warning("Telegram backup failed: %s", scrub(e))
    log.info("Scan complete: %d coins checked (%d auto-discovered pool, %d manual), %d alerts, "
             "%d calls + %d experiments being tracked", checked, len(state["discovered"]), len(manual),
             alerts, len(open_records(kind="call")), len(open_records(kind="shadow")))


async def scanner_loop():
    last_discovery, wait_for = 0.0, 0
    while True:
        if AUTO_DISCOVER and time.time() - last_discovery >= wait_for:
            wait_for = DISCOVERY_SECONDS
            try:
                added = await asyncio.to_thread(discover_tokens)
                if added < 0:
                    wait_for = 90  # all feeds failed (rate limited) - try again in 90s
                    log.warning("Discovery failed this round, retrying in %ds", wait_for)
                else:
                    log.info("Discovery: +%d new coins (pool now %d)", added, len(state["discovered"]))
            except Exception:
                log.exception("discovery error")
            last_discovery = time.time()
        try:
            await asyncio.to_thread(scan_once)
        except Exception:
            log.exception("scanner loop error")
        await asyncio.sleep(POLL_SECONDS)


# ----------------------------------------------------------------------------
# LEARNING: follow every call, run experiments, score wallets and themes, report, optionally self-tune
# ----------------------------------------------------------------------------
ALERT_KINDS = ("call", "narrative")


def open_records(key: str | None = None, kind: str | None = None) -> list[dict]:
    return [r for r in state["records"] if r["status"] == "open"
            and (key is None or r["key"] == key) and (kind is None or r["kind"] == kind)]


def closed_records(kind: str) -> list[dict]:
    return [r for r in state["records"] if r["kind"] == kind and r["status"] != "open"]


def recent_wallets(key: str, window: float = 86400) -> set[str]:
    """Distinct tracked wallets that bought this coin recently."""
    now = time.time()
    return {w for t, w, _ in state["whale_buys"].get(key, []) if now - t <= window}


def themes_for(key: str, pair: dict) -> list[str]:
    """Which themes (stocks, ai, politics...) does this coin's name / symbol / description match?"""
    base = pair.get("baseToken", {})
    head = f"{base.get('name', '')} {base.get('symbol', '')}".lower()
    words = set(WORD.findall(head)) | set(WORD.findall((state["meta"].get(key) or {}).get("d", "").lower()))
    squashed = head.replace(" ", "")
    return [theme for theme, kws in THEMES.items()
            if any(kw in words or (len(kw) >= 4 and kw in squashed) for kw in kws)]


def add_record(kind: str, key: str, chain: str, pair: dict, m: dict, failed: str, now: float,
               extra: dict | None = None):
    for r in open_records(key):  # already following this coin (for whales: this wallet's buy)
        if r["kind"] == kind and (kind != "whale" or r.get("wallet") == (extra or {}).get("wallet")):
            return
    rec = {
        "key": key, "chain": chain, "sym": pair.get("baseToken", {}).get("symbol", "?"),
        "kind": kind, "failed": failed, "t0": now, "mc0": m["mc"], "max_mc": m["mc"],
        "min_mc": m["mc"], "best": 1.0, "final": 1.0, "status": "open", "t1": None,
        "themes": themes_for(key, pair), "sw": len(recent_wallets(key)),
        "m": {k: round(float(m[k]), 4) for k in ("retrace", "liq_ratio", "vol_ratio", "traders", "age_h", "peak")},
    }
    if extra:
        rec.update(extra)
    state["records"].append(rec)


def maybe_add_shadow(key: str, chain: str, pair: dict, m: dict, code: str, now: float):
    """Follow a coin that was blocked by one (or several) rules, to learn whether those rules are right."""
    if open_records(key) or now - state["shadow_cd"].get(key, 0) < SHADOW_COOLDOWN:
        return
    if len(open_records(kind="shadow")) >= MAX_SHADOWS_OPEN:
        return
    state["shadow_cd"][key] = now
    add_record("shadow", key, chain, pair, m, code, now)


def track(key: str, mc: float, now: float):
    """Update every open record on this coin; close it on win / stop / expiry. mc=0 means rugged."""
    for r in open_records(key):
        if mc > 0:
            r["max_mc"] = max(r["max_mc"], mc)
            r["min_mc"] = min(r["min_mc"], mc)
        r["best"] = round(r["max_mc"] / r["mc0"], 3)
        r["final"] = round(mc / r["mc0"], 3)
        if mc <= STOP_MULTIPLE * r["mc0"]:
            close_record(r, "stop", mc, now)
        elif r["max_mc"] >= WIN_MULTIPLE * r["mc0"]:
            close_record(r, "win", mc, now)
        elif now - r["t0"] >= TRACK_HOURS * 3600:
            close_record(r, "expired", mc, now)


def close_record(r: dict, outcome: str, mc: float, now: float):
    r["status"], r["t1"] = outcome, now
    if r["kind"] == "whale":  # score the wallet that made this buy
        w = state["wallets"].setdefault(r["wallet"], {"w": 0, "s": 0, "e": 0, "chain": r["chain"]})
        w[{"win": "w", "stop": "s"}.get(outcome, "e")] += 1
        return
    if r["kind"] not in ALERT_KINDS:
        return  # experiments close silently
    hrs = (now - r["t0"]) / 3600
    tag = "NARRATIVE " if r["kind"] == "narrative" else ""
    head = {"win": f"🎯 *{tag}CALL HIT*", "stop": f"🛑 *{tag}CALL STOPPED OUT*",
            "expired": f"⏱ *{tag}CALL EXPIRED*"}[outcome]
    send_telegram(
        f"{head}: ${md_escape(r['sym'])} | {CHAINS[r['chain']]['name']}\n"
        f"Called at {fmt_usd(r['mc0'])} → now {fmt_usd(mc)} (*{r['final']:.2f}x*, best {r['best']:.2f}x) "
        f"after {hrs:.1f}h\nThe bot is learning from this result.")


def summarize(rs: list[dict]) -> dict:
    n = len(rs)
    w = sum(r["status"] == "win" for r in rs)
    st = sum(r["status"] == "stop" for r in rs)
    return {"n": n, "w": w, "s": st, "e": n - w - st, "wr": (w / n) if n else 0.0,
            "avg_best": (sum(r["best"] for r in rs) / n) if n else 0.0}


def fmt_num(v: float) -> str:
    return f"{v:,.0f}" if abs(v) >= 100 else f"{v:.3g}"


def median(xs: list[float]) -> float:
    xs = sorted(xs)
    if not xs:
        return 0.0
    mid = len(xs) // 2
    return xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) / 2


def bounded(param: str, val: float) -> float:
    lo, hi = BOUNDS[param]
    v = min(max(val, lo), hi)
    return float(round(v)) if param == "traders" else round(v, 3 if param in ("liq", "vol") else 1)


def suggestions() -> list[tuple]:
    """Evidence-based filter changes: [(param, new_value, reason, direction, old_value)]."""
    calls, shadows = closed_records("call"), closed_records("shadow")
    cs = summarize(calls)
    out: dict[str, tuple] = {}
    conflicts: set[str] = set()

    def add(param, new, reason, direction):
        new = bounded(param, new)
        if new == T[param]:
            return
        if param in out and out[param][2] != direction:
            conflicts.add(param)
        out.setdefault(param, (new, reason, direction, T[param]))

    if cs["n"] >= 5:  # loosen a filter if the coins it blocked did as well as our real calls
        for code, param in (("liq", "liq"), ("vol", "vol"), ("traders", "traders"),
                            ("retrace_lo", "retrace_min"), ("retrace_hi", "retrace_max")):
            ss = summarize([r for r in shadows if r["failed"] == code])  # single-rule near-misses only
            if ss["n"] >= MIN_SHADOWS_TUNE and ss["wr"] > 0 and ss["wr"] >= cs["wr"]:
                new = (T[param] * 0.9 if param in ("liq", "vol", "traders")
                       else T[param] - 3 if param == "retrace_min" else T[param] + 3)
                add(param, new, f"{ss['n']} near-misses blocked only by {CODE_LABEL[code]} won "
                                f"{ss['wr']:.0%} vs {cs['wr']:.0%} for real calls", "loosen")

    wins = [r for r in calls if r["status"] == "win"]
    losers = [r for r in calls if r["status"] != "win"]
    if cs["n"] >= MIN_CALLS_TUNE and len(wins) >= 3 and len(losers) >= 3:  # tighten if winners look better
        for param, mk in (("liq", "liq_ratio"), ("vol", "vol_ratio"), ("traders", "traders")):
            mw, ml = median([r["m"][mk] for r in wins]), median([r["m"][mk] for r in losers])
            if ml > 0 and mw > ml * 1.2:
                add(param, T[param] * 1.1, f"winners had a median of {fmt_num(mw)} vs {fmt_num(ml)} for losers",
                    "tighten")
    return [(p,) + out[p] for p in out if p not in conflicts]


def apply_tuning(sugg: list[tuple]) -> list[tuple[str, str]]:
    applied = []
    if not AUTO_TUNE or len(closed_records("call")) < MIN_CALLS_TUNE:
        return applied
    for param, new, reason, direction, old in sugg[:2]:  # at most two small changes per report
        T[param] = new
        state["tuning"].setdefault("T", {})[param] = new
        applied.append((param, f"{PARAM_LABEL[param]}: {fmt_num(old)} → {fmt_num(new)}  ({reason})"))
    return applied


def build_report(sugg: list[tuple], applied: list[tuple[str, str]]) -> str:
    calls, narrs = closed_records("call"), closed_records("narrative")
    shadows = closed_records("shadow")
    c, nr = summarize(calls), summarize(narrs)
    L = ["📊 *BOT REPORT*", ""]
    if c["n"]:
        L.append(f"Real re-run calls finished: *{c['n']}*  ✅ {c['w']} wins | 🛑 {c['s']} stopped | ⏱ {c['e']} flat")
        L.append(f"Win rate: *{c['wr']:.0%}* | average best move: *{c['avg_best']:.2f}x*")
        by_chain: dict[str, list[dict]] = {}
        for r in calls:
            by_chain.setdefault(r["chain"], []).append(r)
        L.append("By chain: " + ", ".join(f"{CHAINS[ch]['name']} {summarize(rs)['w']}/{len(rs)}"
                                           for ch, rs in by_chain.items()))
        wins = [r for r in calls if r["status"] == "win"]
        losers = [r for r in calls if r["status"] != "win"]
        if len(wins) >= 3 and len(losers) >= 3:
            L.append("Winners vs losers (median): " + "; ".join(
                f"{lab} {fmt_num(median([r['m'][k] for r in wins]))} vs {fmt_num(median([r['m'][k] for r in losers]))}"
                for lab, k in (("liquidity/MC", "liq_ratio"), ("volume/MC", "vol_ratio"), ("txns", "traders"))))
    else:
        L.append("No real re-run calls have finished yet.")
    L.append(f"(Win = reaches {WIN_MULTIPLE:g}x before falling to {STOP_MULTIPLE:g}x, within {TRACK_HOURS:.0f}h)")

    if nr["n"]:
        L += ["", f"📰 Narrative alerts finished: *{nr['n']}*  ✅ {nr['w']} wins | 🛑 {nr['s']} stopped "
                  f"→ win rate *{nr['wr']:.0%}*"]

    alerts = calls + narrs
    with_w = [r for r in alerts if r.get("sw", 0) >= 1]
    without = [r for r in alerts if r.get("sw", 0) == 0]
    if len(with_w) >= 3 and len(without) >= 3:
        a, b = summarize(with_w), summarize(without)
        L.append(f"👛 With tracked wallets buying: {a['w']}/{a['n']} won vs {b['w']}/{b['n']} without")

    themed = [r for r in alerts + shadows if r.get("themes")]
    if themed:
        by_theme: dict[str, list[dict]] = {}
        for r in themed:
            for t in r["themes"]:
                by_theme.setdefault(t, []).append(r)
        rows = sorted(((t, summarize(rs)) for t, rs in by_theme.items() if len(rs) >= 2),
                      key=lambda x: (-x[1]["wr"], -x[1]["n"]))
        if rows:
            L.append("🏷 Themes (won/followed): " + ", ".join(f"{t} {x['w']}/{x['n']}" for t, x in rows[:6]))

    ws = [(w, d, d["w"] + d["s"] + d["e"]) for w, d in state["wallets"].items()]
    ws = [x for x in ws if x[2] >= 2]
    if ws:
        best = [x for x in sorted(ws, key=lambda x: (-(x[1]["w"] / x[2]), -x[2])) if x[1]["w"] >= 1][:5]
        if best:
            L.append("🏆 Best tracked wallets (wins/buys followed): "
                     + ", ".join(f"{short(w)} {d['w']}/{n}" for w, d, n in best))
        weak = [x for x in ws if x[2] >= 3 and x[1]["w"] == 0]
        if weak:
            L.append("🗑 No wins yet (consider removing): " + ", ".join(short(w) for w, _, _ in weak[:3]))

    if shadows:
        sa = summarize(shadows)
        L += ["", f"🔎 Coins in the retrace zone we did NOT call: {sa['w']}/{sa['n']} ran {WIN_MULTIPLE:g}x"]
        ran = [r for r in shadows if r["status"] == "win"]
        if ran:
            cnt = Counter(code for r in ran for code in r["failed"].split("+") if code)
            L.append("Why those runners were missed (rule that blocked them): "
                     + ", ".join(f"{CODE_LABEL.get(k, k)} ×{v}" for k, v in cnt.most_common()))
        exp = []
        for code, lab in CODE_LABEL.items():
            ss = summarize([r for r in shadows if r["failed"] == code])
            if ss["n"]:
                exp.append(f"{lab} {ss['w']}/{ss['n']}")
        if exp:
            L.append("Near-misses by the single rule that blocked them (won/followed): " + " | ".join(exp))
    L += ["", f"Following now: {len(open_records(kind='call'))} calls, {len(open_records(kind='narrative'))} "
              f"narrative, {len(open_records(kind='shadow'))} experiments, {len(open_records(kind='whale'))} wallet buys"]

    done = {p for p, _ in applied}
    pending = [x for x in sugg if x[0] not in done]
    if pending:
        L += ["", "💡 *Suggestions*"] + [
            f"• {PARAM_LABEL[p]}: {fmt_num(old)} → {fmt_num(new)} ({why})" for p, new, why, _, old in pending]
        if not AUTO_TUNE:
            L.append("(Self-tuning is OFF, so nothing was changed.)")
    if applied:
        L += ["", "🔧 *Applied automatically*"] + [f"• {txt}" for _, txt in applied]
    if c["n"] < MIN_CALLS_TUNE:
        L += ["", f"⚠️ Only {c['n']} finished calls so far - too few to trust. Treat all of this as early signals."]
    return "\n".join(L)[:3900]


def learning_cycle():
    now = time.time()
    tun = state["tuning"]
    closed_alerts = len(closed_records("call")) + len(closed_records("narrative"))
    due_calls = closed_alerts - tun.get("reported_calls", 0) >= REPORT_EVERY_CALLS
    total_closed = sum(1 for r in state["records"] if r["status"] != "open")
    due_week = total_closed >= 5 and now - tun.get("last_report", 0) >= 7 * 86400
    if due_calls or due_week:
        sugg = suggestions()
        applied = apply_tuning(sugg)
        if send_telegram(build_report(sugg, applied)):
            tun["reported_calls"], tun["last_report"] = closed_alerts, now
    # housekeeping: forget old finished records, stale wallet buys and unused descriptions
    cutoff = now - 45 * 86400
    state["records"] = [r for r in state["records"] if r["status"] == "open" or (r["t1"] or 0) > cutoff][-1500:]
    for k in [k for k, t in state["shadow_cd"].items() if now - t > SHADOW_COOLDOWN]:
        state["shadow_cd"].pop(k, None)
    for k in list(state["whale_buys"]):
        state["whale_buys"][k] = [x for x in state["whale_buys"][k] if now - x[0] <= 86400]
        if not state["whale_buys"][k]:
            del state["whale_buys"][k]
    keep = set(state["discovered"]) | {r["key"] for r in state["records"] if r["status"] == "open"}
    state["meta"] = {k: v for k, v in state["meta"].items() if k in keep}


# ----------------------------------------------------------------------------
# NARRATIVE RADAR: themed coins (stocks, AI, politics...) waking up BEFORE a big run
# ----------------------------------------------------------------------------
def maybe_narrative(key: str, chain: str, pair: dict, now: float, m: dict | None = None) -> bool:
    if not NARRATIVE_ALERTS:
        return False
    th = themes_for(key, pair)
    if not th:
        return False
    m = m or compute_metrics(key, pair)
    if not m or not (NARR_MIN_MC <= m["mc"] <= NARR_MAX_MC):
        return False
    h24 = num(pair, "priceChange", "h24")
    if (m["liq"] < NARR_MIN_LIQ or m["liq_ratio"] < 0.04 or m["vol_ratio"] < NARR_MIN_VOL_RATIO
            or m["traders"] < NARR_MIN_TRADERS or m["age_h"] < NARR_MIN_AGE_H or h24 > NARR_MAX_H24):
        return False
    ckey = "narr:" + key
    if now - state["cooldowns"].get(ckey, 0) < 24 * 3600:
        return False
    stamps = [t for t in state["tuning"].get("narr_ts", []) if now - t < 86400]
    state["tuning"]["narr_ts"] = stamps
    if len(stamps) >= NARR_MAX_PER_DAY:
        return False
    sw = len(recent_wallets(key))
    base = pair.get("baseToken", {})
    addr = base.get("address", "")
    text = (
        f"📰 *NARRATIVE WATCH: ${md_escape(base.get('symbol', '?'))}*  |  {CHAINS[chain]['name']}\n\n"
        f"🏷 Themes: {', '.join(th)}\n"
        f"💰 Market cap: *{fmt_usd(m['mc'])}*  |  💧 Liquidity: {fmt_usd(m['liq'])}\n"
        f"📊 24h volume: {fmt_usd(m['vol24'])} (*{m['vol_ratio']:.1f}x* its market cap)\n"
        f"👥 24h txns: {m['traders']:,}  |  📈 24h price: {h24:+.0f}%\n"
        + (f"👛 Tracked wallets that bought in the last 24h: {sw}\n" if sw else "")
        + f"\n⚠️ Early-stage theme play, NOT a re-run setup - higher risk. Tracked for {TRACK_HOURS:.0f}h.\n\n"
        f"`{addr}`\n\n"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
        f"[Explorer]({link(chain, 'token', addr)})")
    if not send_telegram(text):
        return False
    state["cooldowns"][ckey] = now
    stamps.append(now)
    add_record("narrative", key, chain, pair, m, "", now)
    log.info("NARRATIVE alert for %s (%s)", key, ",".join(th))
    return True


# ----------------------------------------------------------------------------
# SMART WALLETS: whale buys -> context, clusters, wallet scoring
# ----------------------------------------------------------------------------
def token_context(chain: str, token: str):
    """Current pair + metrics (with an estimated all-time-high) for a coin a whale just bought."""
    want = norm_addr(chain, token)
    try:
        pairs = [p for p in fetch_pairs(chain, [token])
                 if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == want]
    except requests.RequestException:
        return None
    pair = best_live_pair(pairs)
    if not pair:
        return None
    key = sk(chain, token)
    if key not in state["backfilled"]:
        try:
            time.sleep(2.2)
            peak = fetch_peak_mc(chain, pair)
            if peak:
                state["peaks"][key] = max(state["peaks"].get(key, 0.0), peak)
                state["backfilled"][key] = time.time()
        except (requests.RequestException, KeyError, ValueError, TypeError):
            pass
    m = compute_metrics(key, pair)
    return {"pair": pair, "m": m, "key": key} if m else None


def handle_whale_swap(swap: dict) -> bool:
    """Send the whale alert, tag re-run accumulation, detect clusters, and start scoring this wallet."""
    now = time.time()
    chain, token, wallet = swap["chain"], swap["token"], swap["wallet"]
    key = sk(chain, token)
    extra, ctx, cluster = "", None, set()
    if swap["side"] == "BUY":
        ctx = token_context(chain, token)
        buys = state["whale_buys"].setdefault(key, [])
        buys.append([now, wallet, swap["usd"]])
        buys[:] = [b for b in buys if now - b[0] <= 86400]
        cluster = {w for t, w, _ in buys if now - t <= CLUSTER_WINDOW}
        if (key not in state["discovered"] and key not in state["dismissed"]
                and (chain, norm_addr(chain, token)) not in set(TOKENS)):
            state["discovered"][key] = now  # let the scanner follow this coin from now on
        if ctx:
            m = ctx["m"]
            zone = m["peak"] >= MIN_PEAK_MC * 0.8 and ZONE_MIN <= m["retrace"] <= ZONE_MAX
            extra = (f"📉 {m['retrace']:.0f}% below its peak of {fmt_usd(m['peak'])} "
                     f"(market cap now {fmt_usd(m['mc'])})\n")
            if zone:
                extra = "🔥 *SMART MONEY RE-RUN ACCUMULATION*\n" + extra
            add_record("whale", key, chain, ctx["pair"], m, "", now, extra={"wallet": wallet, "zone": zone})
    ok = send_telegram(build_whale_message(swap, extra))
    if len(cluster) >= SMART_CLUSTER_MIN and now - state["cooldowns"].get("cluster:" + key, 0) > 7200:
        state["cooldowns"]["cluster:" + key] = now
        total = sum(u for t, w, u in state["whale_buys"][key] if now - t <= CLUSTER_WINDOW)
        send_telegram(
            f"👥 *WALLET CLUSTER*: {len(cluster)} tracked wallets bought ${md_escape(swap['symbol'])} "
            f"within 30 min (total {fmt_usd(total)})  |  {CHAINS[chain]['name']}\n" + extra
            + f"[DexScreener](https://dexscreener.com/{chain}/{token}) | [Token]({link(chain, 'token', token)})")
    return ok


# ----------------------------------------------------------------------------
# MEMORY BACKUP (Render's free disk is wiped on restart) -> pinned file in your Telegram chat
# ----------------------------------------------------------------------------
def tg_api(method: str, **kw):
    r = http.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", timeout=30, **kw)
    r.raise_for_status()
    return r.json().get("result")


def core_hash() -> str:
    core = json.dumps([state["records"], state["cooldowns"], state["tuning"]], sort_keys=True)
    return hashlib.md5(core.encode()).hexdigest()


def backup_state_to_telegram():
    if not (TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    now, b = time.time(), state["backup"]
    h = core_hash()
    unchanged = h == b.get("hash") and now - b.get("ts", 0) < 12 * 3600
    if unchanged or now - b.get("ts", 0) < BACKUP_MIN_GAP:
        return
    data = json.dumps(state).encode()
    msg = tg_api("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": BACKUP_TAG,
                                       "disable_notification": "true"},
                 files={"document": ("bot_memory.json", data)})
    old = b.get("msg_id")
    tg_api("pinChatMessage", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": msg["message_id"],
                                   "disable_notification": True})
    b.update(msg_id=msg["message_id"], ts=now, hash=h)
    if old:
        try:
            tg_api("deleteMessage", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": old})
        except requests.RequestException:
            pass  # old backup can't be deleted (e.g. older than 48h) - harmless


def restore_from_telegram() -> bool:
    chat = tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}
    pm = chat.get("pinned_message") or {}
    doc = pm.get("document")
    if not doc or pm.get("caption") != BACKUP_TAG:
        return False
    info = tg_api("getFile", json={"file_id": doc["file_id"]})
    r = http.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=30)
    r.raise_for_status()
    merge_state(r.json())
    state["backup"].update(msg_id=pm["message_id"], ts=time.time(), hash=core_hash())
    return True


# ----------------------------------------------------------------------------
# MODULE 2 (shared): whale message + dedupe
# ----------------------------------------------------------------------------
def build_whale_message(s: dict, extra: str = "") -> str:
    chain, w = s["chain"], s["wallet"]
    emoji = "🟢" if s["side"] == "BUY" else "🔴"
    return (
        f"🐋 *WHALE {s['side']}* {emoji}  |  {CHAINS[chain]['name']}\n\n"
        f"🪙 Token: *${md_escape(s['symbol'])}*\n"
        f"💵 Value: *{fmt_usd(s['usd'])}*\n"
        f"👛 Wallet: `{short(w)}`\n"
        + extra + "\n"
        f"[Wallet]({link(chain, 'addr', w)}) | "
        f"[Tx]({link(chain, 'tx', s['signature'])}) | "
        f"[DexScreener](https://dexscreener.com/{chain}/{s['token']}) | "
        f"[Token]({link(chain, 'token', s['token'])})"
    )


_seen_sigs: dict[str, float] = {}


def _is_duplicate(key: str) -> bool:
    now = time.time()
    for k in [k for k, t in _seen_sigs.items() if now - t > 3600]:
        _seen_sigs.pop(k, None)
    if key in _seen_sigs:
        return True
    _seen_sigs[key] = now
    return False


# ----------------------------------------------------------------------------
# MODULE 2a: SOLANA (Helius enhanced webhook)
# ----------------------------------------------------------------------------
def sol_quote_usd(mint: str, amount: float) -> float:
    if mint in SOL_STABLES:
        return amount
    if mint == WSOL:
        return amount * dex_price("solana", WSOL)[0]
    return 0.0


def _helius_legs(swap: dict, side: str):
    legs = []
    native = swap.get(f"native{side}")
    if native and native.get("amount"):
        legs.append((WSOL, int(native["amount"]) / 1e9))
    for t in swap.get(f"token{side}s") or []:
        raw = t.get("rawTokenAmount") or {}
        try:
            legs.append((t["mint"], int(raw["tokenAmount"]) / 10 ** int(raw.get("decimals", 0))))
        except (KeyError, ValueError, TypeError):
            continue
    return legs


def parse_helius_swap(tx: dict):
    swap = (tx.get("events") or {}).get("swap")
    if tx.get("type") != "SWAP" or not swap:
        return None
    ins, outs = _helius_legs(swap, "Input"), _helius_legs(swap, "Output")
    out_tokens = [l for l in outs if l[0] not in SOL_QUOTES]
    in_tokens = [l for l in ins if l[0] not in SOL_QUOTES]
    if out_tokens:
        side, (mint, amt), paid = "BUY", out_tokens[0], ins
    elif in_tokens:
        side, (mint, amt), paid = "SELL", in_tokens[0], outs
    else:
        return None
    usd = sum(sol_quote_usd(m, a) for m, a in paid if m in SOL_QUOTES)
    if usd <= 0:
        usd = dex_price("solana", mint)[0] * amt
    return {"chain": "solana", "wallet": tx.get("feePayer", "unknown"), "side": side,
            "token": mint, "symbol": dex_price("solana", mint)[1], "usd": usd,
            "signature": tx.get("signature", "")}


# ----------------------------------------------------------------------------
# MODULE 2b: EVM (Alchemy Address Activity webhook)
# ----------------------------------------------------------------------------
def network_to_chain(network: str) -> str | None:
    n = (network or "").upper()
    if "ROBINHOOD" in n:
        return "robinhood"
    if n.startswith("BNB") or "BSC" in n:
        return "bsc"
    if n.startswith("BASE"):
        return "base"
    if n.startswith("ETH"):
        return "ethereum"
    return None


def native_price(chain: str) -> float:
    return dex_price(*NATIVE_PRICE_SOURCE[chain])[0]


def evm_is_quote(chain: str, addr: str | None) -> bool:
    return addr is None or addr == WRAPPED_NATIVE.get(chain) or addr in EVM_STABLES.get(chain, set())


def evm_quote_usd(chain: str, addr: str | None, amount: float) -> float:
    if addr is None or addr == WRAPPED_NATIVE.get(chain):
        return amount * native_price(chain)
    return amount if addr in EVM_STABLES.get(chain, set()) else 0.0


def parse_evm_payload(payload: dict) -> list[dict]:
    event = payload.get("event") or {}
    chain = network_to_chain(event.get("network", ""))
    if not chain:
        log.warning("Unrecognised Alchemy network: %s", event.get("network"))
        return []

    by_hash: dict[str, list[dict]] = {}
    for a in event.get("activity") or []:
        if a.get("hash"):
            by_hash.setdefault(a["hash"], []).append(a)

    results = []
    for tx_hash, acts in by_hash.items():
        for whale in EVM_WHALES:
            outs, ins = [], []  # legs as (token_addr_or_None, symbol, amount)
            for a in acts:
                amt = num(a, "value")
                if amt <= 0:
                    continue
                raw_addr = (a.get("rawContract") or {}).get("address")
                is_native = a.get("category") in ("external", "internal") or not raw_addr
                leg = (None if is_native else raw_addr.lower(), a.get("asset") or "?", amt)
                if (a.get("fromAddress") or "").lower() == whale:
                    outs.append(leg)
                if (a.get("toAddress") or "").lower() == whale:
                    ins.append(leg)
            if not ins or not outs:
                continue  # plain transfer, not a swap

            in_tok = [l for l in ins if not evm_is_quote(chain, l[0])]
            out_tok = [l for l in outs if not evm_is_quote(chain, l[0])]
            if in_tok:
                side, (token, sym, amt), paid = "BUY", max(in_tok, key=lambda l: l[2]), outs
            elif out_tok:
                side, (token, sym, amt), paid = "SELL", max(out_tok, key=lambda l: l[2]), ins
            else:
                continue  # ETH <-> stable swap, ignore

            usd = sum(evm_quote_usd(chain, l[0], l[2]) for l in paid if evm_is_quote(chain, l[0]))
            if usd <= 0:  # token-to-token: price the token itself
                usd = dex_price(chain, token)[0] * amt
            results.append({"chain": chain, "wallet": whale, "side": side, "token": token,
                            "symbol": dex_price(chain, token)[1] if sym == "?" else sym,
                            "usd": usd, "signature": tx_hash})
    return results


def process_evm(payload: dict) -> int:
    sent = 0
    for swap in parse_evm_payload(payload):
        try:
            if swap["usd"] <= MIN_WHALE_TRADE_USD:
                continue
            if _is_duplicate(f"{swap['signature']}:{swap['wallet']}"):
                continue
            if handle_whale_swap(swap):
                sent += 1
        except Exception:
            log.exception("failed to process EVM swap")
    return sent


# ----------------------------------------------------------------------------
# FASTAPI APP
# ----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(load_state)
    log.info("Tracking %d tokens across %s", len(TOKENS), sorted({c for c, _ in TOKENS}))
    log.info("EVM whale wallets configured: %d", len(EVM_WHALES))
    await asyncio.to_thread(
        send_telegram,
        "✅ Bot is online. "
        + ("Auto-discovery is ON - it finds coins by itself. " if AUTO_DISCOVER else "")
        + f"Manually watching {len(TOKENS)} coin(s). "
        f"Learning is ON: every call is followed for {TRACK_HOURS:.0f}h and scored "
        f"(self-tuning {'ON' if AUTO_TUNE else 'OFF'}). "
        f"Narrative radar {'ON' if NARRATIVE_ALERTS else 'OFF'}. "
        "You'll get a message here when a coin matches your filters.",
    )
    task = asyncio.create_task(scanner_loop())
    yield
    task.cancel()
    save_state()


app = FastAPI(title="Multi-Chain Alert Bot", lifespan=lifespan)

_bg_tasks: set = set()


def run_in_background(fn, *args):
    """Reply to the webhook sender right away; do the (slower) work afterwards."""
    task = asyncio.create_task(asyncio.to_thread(fn, *args))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


@app.get("/")
@app.get("/health")
def health():
    return {"status": "ok", "manual_tokens": len(TOKENS), "discovered_tokens": len(state["discovered"]), "chains": sorted(CHAINS)}


def process_helius(txs: list) -> int:
    sent = 0
    for tx in txs:
        try:
            swap = parse_helius_swap(tx)
            if not swap or swap["usd"] <= MIN_WHALE_TRADE_USD or _is_duplicate(swap["signature"]):
                continue
            if handle_whale_swap(swap):
                sent += 1
        except Exception:
            log.exception("failed to process Solana tx")
    return sent


@app.get("/stats")
def stats_page():
    """Open https://<your-app>.onrender.com/stats in a browser for the learning report."""
    return PlainTextResponse(build_report(suggestions(), []).replace("*", ""))


@app.post("/webhook")
async def webhook_solana(request: Request, authorization: str | None = Header(default=None)):
    """Helius enhanced webhook (Solana)."""
    if WEBHOOK_SECRET and authorization != WEBHOOK_SECRET:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    txs = payload if isinstance(payload, list) else [payload]
    run_in_background(process_helius, txs)
    return {"received": len(txs), "processing": True}


@app.post("/webhook/evm")
async def webhook_evm(request: Request, x_alchemy_signature: str | None = Header(default=None)):
    """Alchemy Address Activity webhook (Ethereum, BNB, Base, Robinhood Chain)."""
    raw = await request.body()
    if ALCHEMY_SIGNING_KEY:
        expected = hmac.new(ALCHEMY_SIGNING_KEY.encode(), raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, x_alchemy_signature or ""):
            return JSONResponse({"error": "bad signature"}, status_code=401)
    if not EVM_WHALES:
        log.warning("EVM_WHALES is empty - cannot attribute swaps to a wallet")
        return {"alerts_sent": 0}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    run_in_background(process_evm, payload)
    return {"processing": True}
