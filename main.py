"""
Multi-Chain Telegram Alert Bot  (Solana | Ethereum | BNB Chain | Base | Robinhood Chain)
-----------------------------------------------------------------------------------------
Module 1: Zipcoin Re-Run Scanner  - DexScreener REST, polled every 60s, all chains
Module 2: Whale Tracker webhooks  - POST /webhook      (Helius, Solana)
                                    POST /webhook/evm  (Alchemy Address Activity, EVM chains)
Module 3: Bottomed coins          - Sparsity Research's public "bottoming" page + who is buying them (smart money)

Run locally:  uvicorn main:app --host 0.0.0.0 --port 8000
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime

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

def env_float(name: str, default: float) -> float:
    """A number from the environment; a missing or mistyped value quietly falls back to the default."""
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return float(default)


# Module 1 thresholds (applied identically on every chain)
POLL_SECONDS = 60
MIN_PEAK_MC = env_float("MIN_PEAK_MC", 5_000_000)   # the coin must once have been worth at least this much
RETRACE_MIN = env_float("RETRACE_MIN", 50.0)        # "dipped" = down at least this many % from its peak...
RETRACE_MAX = env_float("RETRACE_MAX", 90.0)        # ...and at most this many %
if not (0 <= RETRACE_MIN < RETRACE_MAX <= 99):      # nonsense in the settings -> use the normal band
    RETRACE_MIN, RETRACE_MAX = 50.0, 90.0
MAX_COIN_AGE_DAYS = env_float("MAX_COIN_AGE_DAYS", 365)   # coins whose pool is older than this are skipped (0 = no limit)
MIN_LIQ_TO_MC = 0.06
MIN_VOL_TO_MC = 0.50
MIN_TRADERS_24H = 1_000
MIN_PAIR_AGE_HOURS = 24
COOLDOWN_SECONDS = 12 * 3600
MIN_LIVE_LIQUIDITY_USD = 1_000  # pairs below this are treated as dead/drained

# Auto-discovery (the bot finds coins by itself; TOKEN_LIST becomes optional)
AUTO_DISCOVER = os.getenv("AUTO_DISCOVER", "true").lower() != "false"
DISCOVERY_SECONDS = int(os.getenv("DISCOVERY_SECONDS", "600"))
MAX_DISCOVERED = int(os.getenv("MAX_DISCOVERED", "400"))
DISMISS_SECONDS = 24 * 3600          # coins judged hopeless are ignored for 24h
MAX_BACKFILL_PER_CYCLE = 3           # past-high look-ups per minute (GeckoTerminal's free limit is ~10 requests a minute in total)
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
SHADOW_COOLDOWN = 24 * 3600
TELEGRAM_BACKUP = os.getenv("TELEGRAM_BACKUP", "true").lower() != "false"
BACKUP_TAG = "BOT_STATE_BACKUP v1"
BACKUP_MIN_GAP = 300                  # never re-save the memory more often than this...
BACKUP_REFRESH_SECONDS = 6 * 3600     # ...and re-save at least this often even if only prices moved
MAX_SHADOWS_OPEN = 150
GECKO_POOL_PAGES = int(os.getenv("GECKO_POOL_PAGES", "2"))   # pages (20 pools each) per chain, every round; 0 = off
GECKO_DEEP_PAGES = min(10, int(os.getenv("GECKO_DEEP_PAGES", "8")))  # one chain per round is searched this deep
MAX_COIN_MC = 1_000_000_000                                  # coins above $1B never do a deep-dip re-run setup

# --- Staying awake, status check-ins, double-copy detection --------------------
BOT_VERSION = "2.8"
# Render's free plan puts a service to sleep after 15 minutes without incoming web traffic, so the bot
# visits its own web address every few minutes. Render fills in RENDER_EXTERNAL_URL by itself.
PUBLIC_URL = (os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")
KEEPALIVE_SECONDS = max(30, int(os.getenv("KEEPALIVE_SECONDS", "240")))
KEEPALIVE_START_DELAY = 30                                # seconds to let the server finish starting first
WALLET_LOOP_START_DELAY = 45                              # ...and before the first look inside the watched wallets
COMMAND_LOOP_START_DELAY = 8                              # ...and before the bot starts listening for chat commands
CHECKIN_HOURS = float(os.getenv("CHECKIN_HOURS", "6"))   # status message (closest coins etc.); 0 = off
FIRST_CHECKIN_MINUTES = 12                                # first check-in this long after start-up
HEARTBEAT_SECONDS = 300                                   # how often copies of the bot look for each other
INSTANCE_ID = secrets.token_hex(3)                        # identifies THIS running copy of the bot
STARTED_AT = time.time()

# --- Smart wallets, themes, narrative radar -----------------------------------
SMART_CLUSTER_MIN = int(os.getenv("SMART_CLUSTER_MIN", "2"))   # wallets buying the same coin...
CLUSTER_WINDOW = 1800                                          # ...within 30 minutes = cluster
NARRATIVE_ALERTS = os.getenv("NARRATIVE_ALERTS", "true").lower() != "false"
NARR_MIN_MC = float(os.getenv("NARR_MIN_MC", "250000"))
NARR_MAX_MC = float(os.getenv("NARR_MAX_MC", "25000000"))
NARR_MIN_LIQ = float(os.getenv("NARR_MIN_LIQ", "40000"))
NARR_MIN_VOL_RATIO = float(os.getenv("NARR_MIN_VOL_RATIO", "1.0"))  # 24h volume / market cap
NARR_MIN_TRADERS = int(os.getenv("NARR_MIN_TRADERS", "500"))
NARR_MAX_H24 = float(os.getenv("NARR_MAX_H24", "150"))   # % - skip coins that already ran
NARR_MIN_AGE_H = float(os.getenv("NARR_MIN_AGE_H", "3"))
NARR_MAX_PER_DAY = int(os.getenv("NARR_MAX_PER_DAY", "5"))
NARR_MIN_H24 = float(os.getenv("NARR_MIN_H24", "-35"))              # % - skip coins that are already dumping
NARR_MIN_LIQ_RATIO = float(os.getenv("NARR_MIN_LIQ_RATIO", "0.05"))  # liquidity / market cap
NARR_MAX_VOL_LIQ = float(os.getenv("NARR_MAX_VOL_LIQ", "80"))       # 24h volume / liquidity; far above = bots trading with themselves

# --- Protection against one-off bad readings (they happen, above all on brand-new chains) ---
CONFIRM_MIN_GAP = 20      # seconds: an alert needs two separate scans to agree...
CONFIRM_MAX_GAP = 300     # ...and they must be no more than this far apart
RUG_CONFIRM_SECONDS = 240  # a coin with no live pool at all must stay that way this long before it counts as a rug

# --- Your own watched wallets ("are my smart wallets in this coin?") and the conviction signals ---------
WATCH_WALLETS_SEED = os.getenv("WATCH_WALLETS", "")   # optional: "name:address,address" - normally you add wallets in Telegram with /watch
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
EVM_RPC = {"ethereum": os.getenv("RPC_ETHEREUM", "https://ethereum-rpc.publicnode.com").strip(),
           "base": os.getenv("RPC_BASE", "https://mainnet.base.org").strip(),
           "bsc": os.getenv("RPC_BSC", "https://bsc-dataseed.binance.org").strip(),
           "robinhood": os.getenv("RPC_ROBINHOOD", "https://rpc.mainnet.chain.robinhood.com").strip()}   # Robinhood Chain's own free public node (chain id 4663)
WATCH_MIN_HOLD_USD = env_float("WATCH_MIN_HOLD_USD", 100)    # a wallet counts as "in" a coin when its position is worth at least this
WATCH_MIN_MOVE_USD = env_float("WATCH_MIN_MOVE_USD", 500)    # tell me when a watched wallet picks up / dumps at least this much of a coin
WATCH_SWEEP_SECONDS = max(120, int(env_float("WATCH_SWEEP_SECONDS", 600)))   # how often watched Solana wallets are looked at
WATCH_DIP_ALERTS = os.getenv("WATCH_DIP_ALERTS", "true").lower() != "false"  # alert on dipped coins your wallets hold, even if volume etc. is short
WATCH_MIN_HOLDERS = max(1, int(env_float("WATCH_MIN_HOLDERS", 1)))           # watched wallets that must hold the coin for that alert
WATCH_DIP_MAX_PER_DAY = int(env_float("WATCH_DIP_MAX_PER_DAY", 6))
COMMANDS = os.getenv("TELEGRAM_COMMANDS", "true").lower() != "false"         # chat commands (/watch, /check ...)
YOUNG_DAYS = 30                                                              # "young coin" signal

# --- v2.8: bottomed coins (Sparsity) and finding smart money by results ------------------------------------------------
SPARSITY_FEED = os.getenv("SPARSITY_FEED", "true").lower() != "false"           # read Sparsity Research's public "bottoming" page
SPARSITY_URL = os.getenv("SPARSITY_URL", "https://sparsitydata.com/bottoming").strip()
SPARSITY_REFRESH_HOURS = max(1.0, env_float("SPARSITY_REFRESH_HOURS", 4))      # the page itself is only updated once a day
BOTTOM_ALERTS = os.getenv("BOTTOM_ALERTS", "true").lower() != "false"
BOTTOM_MIN_SIGNS = int(min(3, max(1, env_float("BOTTOM_MIN_SIGNS", 3))))       # signs (out of 3) a coin needs for an alert
BOTTOM_FOLLOW_SIGNS = 2                                                         # coins with this many signs are followed quietly
BOTTOM_MAX_PER_DAY = int(env_float("BOTTOM_MAX_PER_DAY", 10))
BOTTOM_COOLDOWN_DAYS = env_float("BOTTOM_COOLDOWN_DAYS", 7)
BOTTOM_TRACK_HOURS = env_float("BOTTOM_TRACK_HOURS", 168)                      # a bottom needs longer to play out than a re-run
BOTTOM_MIN_LIQ_USD = env_float("BOTTOM_MIN_LIQ_USD", 20_000)
MAX_BOTTOM_SHADOWS = 60
BUYER_CAPTURE = os.getenv("BUYER_CAPTURE", "true").lower() != "false"          # look at who is buying each bottomed coin
BUYER_PER_SCAN = max(1, int(env_float("BUYER_PER_SCAN", 2)))                    # coins looked at per scan (GeckoTerminal's free limit)
BUYER_RECAPTURE_HOURS = env_float("BUYER_RECAPTURE_HOURS", 12)
BUYER_TRADE_FLOOR_USD = env_float("BUYER_TRADE_FLOOR_USD", 50)                 # ignore dust trades when listing the latest trades
BUYER_MIN_USD = env_float("BUYER_MIN_USD", 500)                                # "accumulating" = bought at least this (more for big coins)
BUYER_MC_FRACTION = 0.0003
BUYER_MAX_USD_THRESHOLD = 5_000
BUYER_MAX_TRADES = 25                                                           # more trades than this in the window = a bot, not a person
BUYER_MAX_SELL_SHARE = 0.4                                                      # accumulating = sold less than 40% of what it bought
BUYER_RECORDS_PER_COIN = 5
SMART_AUTO = os.getenv("SMART_AUTO", "true").lower() != "false"                # put wallets with a good record on the watch list by themselves
SMART_MIN_BUYS = int(env_float("SMART_MIN_BUYS", 3))
SMART_MIN_GOOD = int(env_float("SMART_MIN_GOOD", 2))
SMART_MIN_RATE = env_float("SMART_MIN_RATE", 0.5)
SMART_GOOD_MULT = env_float("SMART_GOOD_MULT", 1.5)                            # a "good buy": the coin reached this multiple afterwards
SMART_MAX_AUTO = int(env_float("SMART_MAX_AUTO", 15))
GECKO_MIN_GAP = max(2.0, env_float("GECKO_MIN_GAP_SECONDS", 6.5))              # GeckoTerminal: ~10 requests a minute for ALL of the bot

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
BOUNDS = {"retrace_min": (40.0, 70.0), "retrace_max": (70.0, 95.0), "liq": (0.03, 0.20),
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
    "robinhood": {"name": "Robinhood Chain", "explorer": "https://robin.etherscan.io",
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
SPL_TOKEN_PROGRAMS = ("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",     # the two token programs a wallet's coins live under
                      "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")
http = requests.Session()
http.headers.update({"User-Agent": f"multichain-alert-bot/{BOT_VERSION}"})


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


def manual_coin(chain: str, addr: str) -> bool:
    """Is this coin in the TOKEN_LIST the owner typed in by hand? Those are never skipped for size or age."""
    return (chain, norm_addr(chain, addr)) in set(TOKENS)


def mc_floor() -> float:
    """The smallest market cap a coin can have today and still be a re-run setup: its past high must have
    been at least MIN_PEAK_MC, and it may be down at most RETRACE_MAX % from there."""
    return MIN_PEAK_MC * max(0.05, 1 - T["retrace_max"] / 100)


def age_days_of(pair: dict) -> float:
    created = num(pair, "pairCreatedAt")
    return (time.time() * 1000 - created) / 86_400_000 if created > 0 else 0.0


def too_old(pair: dict) -> bool:
    """Older than the age limit (a year, unless the owner changed MAX_COIN_AGE_DAYS)."""
    return MAX_COIN_AGE_DAYS > 0 and age_days_of(pair) > MAX_COIN_AGE_DAYS


def fmt_age(hours: float) -> str:
    if hours <= 0:
        return "?"
    if hours < 48:
        return f"{hours:.0f}h"
    days = hours / 24
    return f"{days:.0f}d" if days < 60 else f"{days / 30.4:.0f}mo"


_odd_mc: Counter = Counter()   # coin key -> readings in a row whose market cap did not fit its usual supply


def pair_mc(key: str, pair: dict) -> float:
    """A coin's market cap, always counted the same way.
    DexScreener usually gives 'marketCap' (circulating supply x price) next to 'fdv' (total supply x price), but
    some answers leave 'marketCap' out or give a very different number. Falling back to the (often 2-7x bigger)
    fdv made coins look like they jumped or crashed within minutes, and it fed fake 'past highs' - so fake dips -
    into the memory. So the supply seen with a normal 'marketCap' is remembered, and an odd reading is valued at
    price x that supply instead. (Three odd readings in a row are accepted as a real change of supply.)"""
    price, mc, fdv = num(pair, "priceUsd"), num(pair, "marketCap"), num(pair, "fdv")
    if price <= 0:
        return mc or fdv
    sup = state["supply"].get(key)
    if mc > 0:
        implied = mc / price
        if sup is None:
            if fdv > 1.25 * mc:
                # first time this coin is seen with both numbers and they differ: a past high saved earlier may
                # have been read in fdv terms, so forget it and let it be looked up again
                for bucket in ("peaks", "backfilled", "bf_fails"):
                    state[bucket].pop(key, None)
            state["supply"][key] = implied
            return mc
        if 0.8 <= implied / sup <= 1.25:
            state["supply"][key] = implied      # the same way of counting: follow slow changes of supply
            _odd_mc.pop(key, None)
            return mc
        _odd_mc[key] += 1
        if _odd_mc[key] >= 3:
            _odd_mc.pop(key, None)
            state["supply"][key] = implied      # it lasted: the supply really did change
            return mc
        return sup * price
    return sup * price if sup else fdv


# ----------------------------------------------------------------------------
# STATE (peaks + cooldowns), JSON-persisted
# ----------------------------------------------------------------------------
_state_lock = threading.Lock()
state = {"peaks": {}, "cooldowns": {}, "discovered": {}, "dismissed": {}, "backfilled": {},
         "bf_fails": {}, "shadow_cd": {}, "meta": {}, "whale_buys": {},
         "records": [], "tuning": {}, "backup": {}, "wallets": {}, "watch": {}, "supply": {}, "bottoms": {}}
DICT_BUCKETS = ("peaks", "cooldowns", "discovered", "dismissed", "backfilled", "bf_fails",
                "shadow_cd", "meta", "whale_buys", "supply", "bottoms")
OLD_RESULTS_SET_ASIDE = 0   # how many results from older versions were set aside when the memory was loaded


def quarantine_old_results() -> int:
    """Versions before 2.5 followed a coin by its market-cap field alone. That number can jump between
    'marketCap' and the (often much bigger) fully-diluted value, so they recorded 'wins' that were really data
    glitches (a coin that 'doubled' within two minutes). Those results stay in the memory as history but are
    never counted as evidence, and the ones still open are closed quietly."""
    global OLD_RESULTS_SET_ASIDE
    n, now = 0, time.time()
    for r in state["records"]:
        if "pa" in r or r.get("void"):   # records from 2.5 on remember which pool they were called on
            continue
        r["void"] = True
        if r["status"] == "open":
            r["status"], r["t1"] = "expired", now
        n += 1
    OLD_RESULTS_SET_ASIDE += n
    return n


def merge_state(loaded: dict):
    for bucket in DICT_BUCKETS:
        for k, v in (loaded.get(bucket) or {}).items():
            state[bucket][k if ":" in k else f"solana:{k}"] = v  # migrate v1 keys
    state["wallets"].update(loaded.get("wallets") or {})
    state["watch"].update(loaded.get("watch") or {})
    state["records"].extend(loaded.get("records") or [])
    quarantine_old_results()
    state["tuning"].update(loaded.get("tuning") or {})
    state["backup"].update(loaded.get("backup") or {})
    # A wider dip range lets smaller coins qualify. Coins thrown out earlier for being "too small" must get
    # another look, so those throw-outs are forgotten whenever the smallest allowed size has gone down.
    before = (loaded.get("tuning") or {}).get("floor")
    try:
        lowered = before is None or mc_floor() < float(before) * 0.99
    except (TypeError, ValueError):
        lowered = True
    if lowered:
        state["dismissed"].clear()
    state["tuning"]["floor"] = mc_floor()


restore_pending = False   # True if a memory backup exists but could not be read yet (never overwrite it!)


def load_state():
    global restore_pending
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
            restore_pending = True  # Telegram hiccup: retry later and don't overwrite the saved memory meanwhile
            log.warning("Could not restore from Telegram backup (will retry): %s", scrub(e))
    try:
        for ref, peak in json.loads(RAW_PEAK_SEEDS).items():
            t = parse_token_ref(ref)
            if t:
                key = sk(*t)
                state["peaks"][key] = max(state["peaks"].get(key, 0), float(peak))
    except (json.JSONDecodeError, ValueError, AttributeError):
        log.error("PEAK_SEEDS is not valid JSON - ignored")
    if AUTO_TUNE:
        tuned = state["tuning"].get("T") or {}
        if state["tuning"].get("band") != [RETRACE_MIN, RETRACE_MAX]:
            # The dip range was set (or changed) after the bot last tuned it: the range the owner asked for wins,
            # an old tuned range must not quietly stay in force. (Other tuned filters are kept.)
            tuned.pop("retrace_min", None)
            tuned.pop("retrace_max", None)
        for param, val in tuned.items():
            if param in T:
                T[param] = float(val)
        log.info("Self-tuning ON. Current filters: %s", T)
    state["tuning"]["band"] = [RETRACE_MIN, RETRACE_MAX]
    state["tuning"].setdefault("floor", mc_floor())
    try:
        seed_watch_wallets()
    except Exception:
        log.exception("WATCH_WALLETS could not be read - ignored")


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
    """Hide the bot token and any private node links (they can contain an API key) if they appear in an error
    message, so logs are safe to share."""
    t = str(e)
    if TELEGRAM_BOT_TOKEN:
        t = t.replace(TELEGRAM_BOT_TOKEN, "***")
    for url in (SOLANA_RPC_URL, *EVM_RPC.values()):
        if url:
            t = t.replace(url, "<node link>")
    return re.sub(r"(?i)\b(api[-_]?key|key|token)=[^&\s'\")]+", r"\1=***", t)


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
    mc = pair_mc(key, pair)
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
                liq_ratio=liq / mc, vol24=vol24, vol_ratio=vol24 / mc, traders=traders, age_h=age_h,
                # shorter-term activity: is the coin waking up? (used for the signals)
                vol_h6=num(pair, "volume", "h6"), buys_h6=num(pair, "txns", "h6", "buys"),
                sells_h6=num(pair, "txns", "h6", "sells"), chg_h1=num(pair, "priceChange", "h1"),
                chg_h6=num(pair, "priceChange", "h6"))


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
    if MAX_COIN_AGE_DAYS > 0 and m["age_h"] > MAX_COIN_AGE_DAYS * 24 and not manual_coin(*key.split(":", 1)):
        fails.append(("old", f"older than {MAX_COIN_AGE_DAYS:g} days"))
    return not fails, m, fails


def is_near(code: str, m: dict) -> bool:
    """Is a single failed filter only a narrow miss? (candidate for a 'shadow' experiment)"""
    if code == "retrace_lo":
        return m["retrace"] >= T["retrace_min"] - 8
    if code == "retrace_hi":
        return m["retrace"] <= T["retrace_max"] + 6
    if code == "liq":
        return m["liq_ratio"] >= T["liq"] * 0.5
    if code == "vol":
        return m["vol_ratio"] >= T["vol"] * 0.5
    if code == "traders":
        return m["traders"] >= T["traders"] * 0.5
    return False


SIGNAL_LABELS = {
    "heat": "🔥 volume picking up",
    "buyers": "🟢 more buyers than sellers",
    "bounce": "↗️ price turning up",
    "young": "🆕 young coin",
    "wallets": "👛 your wallets are in",
}


def signals_for(m: dict, wallets_in: int = 0) -> list[str]:
    """Quick yes/no clues that a dipped coin is waking up. They are experimental: every alert is saved together
    with the clues it had, so the report can show which ones really went with wins."""
    sig = []
    if m.get("vol24", 0) > 0 and m.get("vol_h6", 0) >= 0.40 * m["vol24"]:
        sig.append("heat")        # a flat day would put only ~25% of the day's volume into the last 6 hours
    t6 = m.get("buys_h6", 0) + m.get("sells_h6", 0)
    if t6 >= 50 and m["buys_h6"] / t6 >= 0.55:
        sig.append("buyers")
    if m.get("chg_h6", 0) >= 3 and m.get("chg_h1", 0) >= -2:
        sig.append("bounce")
    if 0 < m.get("age_h", 0) <= YOUNG_DAYS * 24:
        sig.append("young")
    if wallets_in > 0:
        sig.append("wallets")
    return sig


def signals_text(sig: list[str]) -> str:
    return (f"🎯 Signals: {len(sig)}/{len(SIGNAL_LABELS)}" + (" - " + ", ".join(SIGNAL_LABELS[s] for s in sig) if sig else "")
            + "\n")


def build_rerun_message(chain: str, pair: dict, m: dict, extra: str = "", title: str = "🔁 ZIPCOIN RE-RUN SETUP") -> str:
    base = pair.get("baseToken", {})
    addr = base.get("address", "")
    symbol = md_escape(base.get("symbol", "?"))
    pair_addr = pair.get("pairAddress", "")
    return (
        f"*{title}: ${symbol}*  |  {CHAINS[chain]['name']}\n\n"
        f"📈 Peak MC: *{fmt_usd(m['peak'])}*\n"
        f"💰 Current MC: *{fmt_usd(m['mc'])}*\n"
        f"📉 Retrace: *-{m['retrace']:.1f}%*\n"
        f"💧 Liquidity: {fmt_usd(m['liq'])} ({m['liq_ratio']*100:.1f}% of MC)\n"
        f"📊 24h Volume: {fmt_usd(m['vol24'])} ({m['vol24']/m['mc']*100:.0f}% of MC)\n"
        f"👥 24h Txns (buys+sells): {m['traders']:,}\n"
        f"⏱ Pair age: {fmt_age(m['age_h'])}\n"
        f"📌 Tracking this call for {TRACK_HOURS:.0f}h (win = {WIN_MULTIPLE:g}x)\n"
        + extra + "\n"
        f"`{addr}`\n\n"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair_addr}) | "
        f"[Explorer]({link(chain, 'token', addr)})"
    )


def watch_list() -> list[tuple[str, str]]:
    """Manual TOKEN_LIST plus everything auto-discovered."""
    seen, out = set(), []
    tracked = list(dict.fromkeys(r["key"] for r in state["records"] if r["status"] == "open"))
    bottomed = [k for k, b in list(state["bottoms"].items()) if bottom_active(b)]
    for chain, addr in (list(TOKENS) + [tuple(k.split(":", 1)) for k in list(state["discovered"])]
                        + [tuple(k.split(":", 1)) for k in tracked] + [tuple(k.split(":", 1)) for k in bottomed]):
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
    for k in [k for k, t in list(state["dismissed"].items()) if now - t > DISMISS_SECONDS]:
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
    try:
        added += discover_from_volume_leaders(now)
    except Exception:
        log.exception("volume-leader discovery error")
    if failures == len(DISCOVERY_FEEDS) and not added:
        return -1  # every feed failed - caller retries soon
    overflow = len(state["discovered"]) - MAX_DISCOVERED
    if overflow > 0:  # drop the oldest
        for k in sorted(state["discovered"], key=state["discovered"].get)[:overflow]:
            state["discovered"].pop(k, None)
    return added


def is_quote_token(chain: str, addr: str) -> bool:
    """SOL / ETH / BNB / stablecoins - never a coin we want to scan."""
    if chain == "solana":
        return addr in SOL_QUOTES
    return addr == WRAPPED_NATIVE.get(chain) or addr in EVM_STABLES.get(chain, set())


_gecko_round = 0
_gecko_skip: set = set()   # chains GeckoTerminal says it doesn't know (nothing to look up there)


def discover_from_volume_leaders(now: float) -> int:
    """
    Add the highest-24h-volume pools on each chain (GeckoTerminal, free). Coins that already
    have real volume and a real market cap are exactly the ones that can do a re-run.
    Every round looks at the top GECKO_POOL_PAGES pages of each chain, and one chain per round (taking turns)
    is searched much deeper, because mid-size coins - the usual re-run candidates - sit further down the list.
    """
    global _gecko_round
    if GECKO_POOL_PAGES <= 0:
        return 0
    manual, added = set(TOKENS), 0
    live = [c for c in GECKO_NETWORK if c not in _gecko_skip]
    deep_chain = live[_gecko_round % len(live)] if live else None
    _gecko_round += 1
    for chain in live:
        net = GECKO_NETWORK[chain]
        last_page = max(GECKO_POOL_PAGES, GECKO_DEEP_PAGES) if chain == deep_chain else GECKO_POOL_PAGES
        for page in range(1, last_page + 1):
            try:  # (gecko_get spaces every GeckoTerminal request out: the free limit is ~10 a minute for the whole bot)
                r = gecko_get(
                    f"https://api.geckoterminal.com/api/v2/networks/{net}/pools", tries=2,
                    params={"page": page, "sort": "h24_volume_usd_desc"}, timeout=20)
                pools = r.json().get("data") or []
            except (requests.RequestException, ValueError) as e:
                status = getattr(getattr(e, "response", None), "status_code", "error")
                if status == 404 and page == 1:
                    _gecko_skip.add(chain)
                    log.warning("GeckoTerminal does not list %s - leaving it out of the volume-leader search", chain)
                else:
                    log.warning("volume-leader feed busy (%s) for %s", status, chain)
                    bot_stats["gecko_busy"] += 1
                break
            if not pools:
                break
            for pool in pools:
                try:
                    attrs = pool.get("attributes") or {}
                    base_id = ((pool.get("relationships") or {}).get("base_token") or {}).get("data", {}).get("id", "")
                    if "_" not in base_id:
                        continue
                    addr = norm_addr(chain, base_id.split("_", 1)[1])
                    key = sk(chain, addr)
                    if (is_quote_token(chain, addr) or (chain, addr) in manual
                            or key in state["discovered"] or key in state["dismissed"]):
                        continue
                    mc = float(attrs.get("market_cap_usd") or attrs.get("fdv_usd") or 0)
                    if not (mc_floor() <= mc <= MAX_COIN_MC):
                        continue
                    created = datetime.fromisoformat(str(attrs.get("pool_created_at", "")).replace("Z", "+00:00"))
                    pool_age = now - created.timestamp()
                    if pool_age < MIN_PAIR_AGE_HOURS * 3600 or (MAX_COIN_AGE_DAYS > 0 and pool_age > MAX_COIN_AGE_DAYS * 86400):
                        continue
                    state["discovered"][key] = now
                    added += 1
                except (ValueError, TypeError, AttributeError):
                    continue
    return added


def passes_cheap_checks(pair: dict, relax: float = 1.0, key: str | None = None) -> bool:
    """Everything except peak/retrace - decides if a peak look-up is worth it.
    relax=0.5 also admits near-misses so they can be followed as experiments."""
    mc = pair_mc(key, pair) if key else (num(pair, "marketCap") or num(pair, "fdv"))
    if mc < mc_floor():
        return False
    created = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3_600_000 if created else 0.0
    traders = num(pair, "txns", "h24", "buys") + num(pair, "txns", "h24", "sells")
    return (age_h >= MIN_PAIR_AGE_HOURS
            and num(pair, "liquidity", "usd") / mc >= T["liq"] * relax
            and num(pair, "volume", "h24") >= mc * T["vol"] * relax
            and traders >= T["traders"] * relax)


def fetch_peak_mc(chain: str, pair: dict, key: str | None = None):
    """
    Estimate all-time-high market cap from GeckoTerminal daily candles:
    highest daily high x (current market cap / current price).
    """
    net, pool = GECKO_NETWORK.get(chain), pair.get("pairAddress")
    price = num(pair, "priceUsd")
    mc = pair_mc(key, pair) if key else (num(pair, "marketCap") or num(pair, "fdv"))
    if not (net and pool and price > 0 and mc > 0):
        return None
    r = gecko_get(
        f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}/ohlcv/day",
        params={"aggregate": 1, "limit": 1000, "currency": "usd"}, timeout=20)
    candles = r.json()["data"]["attributes"]["ohlcv_list"]
    highs = [float(c[2]) for c in candles if len(c) >= 3]
    return max(highs) * (mc / price) if highs else None


def maybe_backfill_peak(key: str, chain: str, pair: dict, budget: list, force: bool = False):
    """Look up the coin's past high once. force=True skips the 'is it active enough to bother' test."""
    if key in state["backfilled"] or budget[0] <= 0 or not (force or passes_cheap_checks(pair, relax=0.5, key=key)):
        return
    budget[0] -= 1
    try:
        peak = fetch_peak_mc(chain, pair, key)
    except requests.RequestException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status in (None, 429, 500, 502, 503, 504):
            # busy / timed out: that says nothing about this coin, so don't count it as a failure
            log.warning("peak look-up busy (%s) for %s - will retry", status or "timeout", key)
            bot_stats["gecko_busy"] += 1
            budget[0] = 0  # the shared connection is being throttled - stop look-ups for this minute
            return
        peak = None
        log.warning("peak look-up failed for %s: %s", key, scrub(e))
    except (KeyError, ValueError, TypeError) as e:
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


last_scan: dict = {}        # what the latest scan saw (feeds the status check-in and /status)
bot_stats: Counter = Counter()   # alerts sent since this copy of the bot started

_pending: dict[str, tuple[float, float]] = {}   # alert key -> (when first seen, market cap then)
_missing: Counter = Counter()                    # coin key -> scans in a row with no live pool


def confirmed_twice(tag: str, key: str, mc: float, now: float) -> bool:
    """A new alert is only sent when the coin passed on two separate scans with a similar market cap.
    One odd price reading (bad data, a one-second spike) therefore can't trigger an alert on its own."""
    for old in [k for k, (t, _) in _pending.items() if now - t > CONFIRM_MAX_GAP * 2]:
        _pending.pop(old, None)
    k = f"{tag}:{key}"
    prev = _pending.get(k)
    if (prev and prev[1] > 0 and CONFIRM_MIN_GAP <= now - prev[0] <= CONFIRM_MAX_GAP
            and 0.7 <= mc / prev[1] <= 1.43):
        _pending.pop(k, None)
        return True
    if not prev or now - prev[0] > CONFIRM_MIN_GAP:  # start (or restart) the wait with this reading
        _pending[k] = (now, mc)
    return False


def chain_note(chain: str) -> str:
    """Honest warning for the newest chain, where DexScreener's numbers have been seen to jump around."""
    return ("⚠️ Robinhood Chain is brand new: DexScreener's numbers for it can jump around. "
            "Check the chart yourself first.\n" if chain == "robinhood" else "")


def scan_once():
    global last_scan
    watch = watch_list()
    if not watch:
        log.info("Nothing to scan yet - waiting for discovery to find coins")
        return
    now = time.time()
    manual = set(TOKENS)
    budget = [MAX_BACKFILL_PER_CYCLE]
    caps = [BUYER_PER_SCAN]        # coins whose buyers may be looked at during this scan
    checked = alerts = errors = 0
    rows: list[dict] = []
    fail_counts: Counter = Counter()

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
                errors += 1
                continue
            if len(batch) >= 3 and not pairs:
                # several coins and not one answer: a hiccup on their side, not 3+ dead coins at once
                log.warning("[%s] DexScreener sent an empty answer for %d coins - ignoring it", chain, len(batch))
                errors += 1
                continue

            grouped: dict[str, list[dict]] = {}
            for p in pairs:
                a = norm_addr(chain, p.get("baseToken", {}).get("address", ""))
                grouped.setdefault(a, []).append(p)

            for addr in batch:
                key = sk(chain, addr)
                is_manual = (chain, addr) in manual
                pairs_here = grouped.get(norm_addr(chain, addr), [])
                pair = best_live_pair(pairs_here)
                if not pair:  # dead / migrated / drained
                    if open_records(key):
                        track(key, 0.0, now)  # liquidity gone -> counts as a rug once it has lasted a few minutes
                    elif not is_manual and not bottom_entry(key):
                        _missing[key] += 1
                        if _missing[key] >= 2:  # gone on two scans in a row: not just a one-off hiccup
                            _missing.pop(key, None)
                            dismiss(key)
                    continue
                _missing.pop(key, None)
                mc = pair_mc(key, pair)
                sym = pair["baseToken"].get("symbol", "?")
                track(key, mc, now, pair, pairs_here)  # update outcomes of earlier calls on this coin

                bt = bottom_entry(key)   # Sparsity lists this coin (2+ bottom signs) right now: never thrown out for size or age
                if not is_manual and not bt and mc > MAX_COIN_MC and not open_records(key):
                    dismiss(key)
                    continue
                if not is_manual and not bt and too_old(pair) and not open_records(key):
                    bot_stats["old"] += 1
                    dismiss(key)  # older than the age limit (a year) - and it can only get older
                    continue
                if not is_manual and not bt and mc < mc_floor() and not open_records(key):
                    # too small for a re-run setup; keep it only if it matches a theme (narrative radar)
                    if NARRATIVE_ALERTS and mc >= NARR_MIN_MC and themes_for(key, pair):
                        maybe_narrative(key, chain, pair, now)
                    else:
                        dismiss(key)
                    continue

                if bt:
                    try:
                        maybe_bottom(key, chain, pair, bt, now, caps)
                    except Exception:
                        log.exception("bottom check error")

                maybe_backfill_peak(key, chain, pair, budget)
                checked += 1
                passed, m, fails = evaluate(key, pair)
                if m:  # remember how close every coin is, for the status check-in
                    codes_now = [c for c, _ in fails]
                    if "peak" in codes_now and key not in state["backfilled"]:
                        # its past high hasn't been looked up, so "peak" and "retrace" aren't meaningful yet
                        codes_now = ["peak_unknown" if c == "peak" else c for c in codes_now if c != "retrace_lo"]
                    rows.append({"key": key, "chain": chain, "sym": sym, "pair": pair.get("pairAddress", ""),
                                 "mc": m["mc"], "peak": m["peak"], "retrace": m["retrace"], "fails": codes_now,
                                 "age_h": m["age_h"], "price": num(pair, "priceUsd"), "sig": signals_for(m)})
                    fail_counts.update(set(codes_now))
                if not passed:
                    codes = {c for c, _ in fails}
                    if len(fails) <= 1:  # near miss - worth seeing in the logs
                        log.info("[%s] %s NEAR MISS: %s", chain, sym, "; ".join(t for _, t in fails))
                    if len(fails) == 1 and is_near(fails[0][0], m):
                        maybe_add_shadow(key, chain, pair, m, fails[0][0], now)
                    elif (len(fails) >= 2 and not (codes & {"peak", "age", "nomc", "old"})
                          and T["retrace_min"] - 5 <= m["retrace"] <= T["retrace_max"] + 5):
                        # in the retrace zone but blocked by several rules: follow it to see if we MISSED a runner
                        maybe_add_shadow(key, chain, pair, m, "+".join(sorted(codes)), now)
                    if m:
                        maybe_narrative(key, chain, pair, now, m)
                        if WATCH_DIP_ALERTS and state["watch"]:
                            try:  # a dipped coin that YOUR watched wallets are holding
                                maybe_watch_dip(key, chain, pair, m, fails, now, budget)
                            except Exception:
                                log.exception("watched-wallet check error")
                    continue
                if now - state["cooldowns"].get(key, 0) < COOLDOWN_SECONDS:
                    log.info("%s passed filters but is on cooldown", key)
                    continue
                if not confirmed_twice("rerun", key, m["mc"], now):
                    log.info("%s passes every filter - the alert goes out if the next scan agrees", key)
                    continue
                th, sw = themes_for(key, pair), len(recent_wallets(key))
                held = watch_holders(chain, addr, num(pair, "priceUsd"))
                sig = signals_for(m, len(held["in"]))
                extra = ((f"🏷 Themes: {', '.join(th)}\n" if th else "")
                         + (f"👛 Tracked wallets that bought in the last 24h: {sw}\n" if sw else "")
                         + wallets_line(held) + signals_text(sig)
                         + chain_note(chain))
                if send_telegram(build_rerun_message(chain, pair, m, extra)):
                    state["cooldowns"][key] = now
                    add_record("call", key, chain, pair, m, "", now, extra={"sig": sig})
                    alerts += 1
                    bot_stats["rerun"] += 1
                    log.info("ALERT sent for %s", key)

    last_scan = {"t": time.time(), "checked": checked, "pool": len(watch), "rows": rows,
                 "fails": dict(fail_counts), "errors": errors}
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
        round_started = time.time()
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
        if SPARSITY_FEED:
            try:
                await asyncio.to_thread(refresh_sparsity)
            except Exception:
                log.exception("Sparsity refresh error")
        try:
            await asyncio.to_thread(scan_once)
        except Exception:
            log.exception("scanner loop error")
        try:
            await asyncio.to_thread(housekeeping)
        except Exception:
            log.exception("housekeeping error")
        # one round every POLL_SECONDS, however long the work took (but always a short breather)
        await asyncio.sleep(max(min(5.0, POLL_SECONDS), POLL_SECONDS - (time.time() - round_started)))


# ----------------------------------------------------------------------------
# LEARNING: follow every call, run experiments, score wallets and themes, report, optionally self-tune
# ----------------------------------------------------------------------------
ALERT_KINDS = ("call", "narrative", "wallet", "bottom")   # records that were sent to you as alerts (the rest are silent experiments)


def open_records(key: str | None = None, kind: str | None = None) -> list[dict]:
    return [r for r in state["records"] if r["status"] == "open"
            and (key is None or r["key"] == key) and (kind is None or r["kind"] == kind)]


def closed_records(kind: str) -> list[dict]:
    """Finished results that count as evidence (results set aside as untrustworthy are left out)."""
    return [r for r in state["records"] if r["kind"] == kind and r["status"] != "open" and not r.get("void")]


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
        # which pool + price the call was made on, so later readings are measured the same way
        "pa": pair.get("pairAddress", ""), "p0": num(pair, "priceUsd"), "last": m["mc"],
        "themes": themes_for(key, pair), "sw": len(recent_wallets(key)),
        "m": {k: round(float(m[k]), 4) for k in ("retrace", "liq_ratio", "vol_ratio", "traders", "age_h", "peak")},
        "sig": signals_for(m),   # the clues this coin had when it was called / followed (alerts add the wallet clue)
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


def read_mc(r: dict, mc: float, pair: dict | None, pairs: list[dict] | None) -> float | None:
    """Where is this call's coin right now, in the same market-cap terms as when the call was made?
    It is measured by the PRICE on the pool the call was made on (falling back to the best live pool if
    that one was drained), because DexScreener's market-cap field can jump between 'marketCap' and 'fdv'
    or between pools. 0.0 = no live pool left; None = no usable reading this time."""
    if pair is None:
        return 0.0
    p0 = float(r.get("p0") or 0.0)
    if p0 <= 0:  # record made by an older version of the bot: plain market-cap reading
        return mc if mc > 0 else None
    # stay on the original pool while it still holds a real share of the liquidity (a leftover dust pool
    # after a migration can show a stale price, so it must not be followed forever)
    floor = max(MIN_LIVE_LIQUIDITY_USD, 0.2 * num(pair, "liquidity", "usd"))
    same = next((p for p in (pairs or []) if p.get("pairAddress") == r.get("pa")
                 and num(p, "liquidity", "usd") >= floor), None)
    px = num(same or pair, "priceUsd")
    return r["mc0"] * px / p0 if px > 0 else None


def track(key: str, mc: float, now: float, pair: dict | None = None, pairs: list[dict] | None = None):
    """Update every open record on this coin; close it on win / stop / expiry. pair=None means no live pool.
    A stop or a win only counts when TWO readings in a row agree, so one odd reading can't end a call."""
    for r in open_records(key):
        cur = read_mc(r, mc, pair, pairs)
        hours = float(r.get("th") or TRACK_HOURS)      # bottom calls are followed longer than re-runs
        if cur is None:  # nothing usable this time - only the clock can end the call
            if now - r["t0"] >= hours * 3600:
                close_record(r, "expired", r.get("last", r["mc0"]), now)
            continue
        prev = r.get("last", cur)
        r["last"] = cur
        if cur > 0:
            r.pop("z", None)
        else:
            r.setdefault("z", now)               # since when has this coin had no live pool at all?
        hi, lo = max(cur, prev), min(cur, prev)
        r["max_mc"] = max(r["max_mc"], lo)       # a new high counts once two readings in a row reach it
        if hi > 0:
            r["min_mc"] = min(r["min_mc"], hi)   # ...and a new low once two in a row are at or under it
        r["best"] = round(r["max_mc"] / r["mc0"], 3)
        r["final"] = round(cur / r["mc0"], 3)
        # "no live pool" must last a few minutes before it counts as a rug (a data outage looks the same)
        if hi <= STOP_MULTIPLE * r["mc0"] and (cur > 0 or now - r["z"] >= RUG_CONFIRM_SECONDS):
            log.info("STOP %s: reading %s (%.2fx) twice in a row; pool %s", key, fmt_usd(cur),
                     cur / r["mc0"], r.get("pa") or "?")
            close_record(r, "stop", cur, now)
        elif r["max_mc"] >= WIN_MULTIPLE * r["mc0"]:
            close_record(r, "win", cur, now)
        elif now - r["t0"] >= hours * 3600:
            close_record(r, "expired", cur, now)
        elif cur <= STOP_MULTIPLE * r["mc0"]:
            log.info("%s: one reading of %s (%.2fx of the call) - waiting for a second one to agree",
                     key, fmt_usd(cur), cur / r["mc0"])


def close_record(r: dict, outcome: str, mc: float, now: float):
    r["status"], r["t1"] = outcome, now
    if r.get("void"):
        return  # a result set aside as untrustworthy: closes quietly, no message, no scoring
    if r["kind"] == "whale":  # score the wallet that made this buy
        w = state["wallets"].setdefault(r["wallet"], {"w": 0, "s": 0, "e": 0, "chain": r["chain"]})
        w[{"win": "w", "stop": "s"}.get(outcome, "e")] += 1
        return
    if r["kind"] not in ALERT_KINDS:
        return  # experiments close silently
    tag = {"narrative": "NARRATIVE ", "wallet": "WALLET-BACKED ", "bottom": "BOTTOM "}.get(r["kind"], "")
    head = {"win": f"🎯 *{tag}CALL HIT*", "stop": f"🛑 *{tag}CALL STOPPED OUT*",
            "expired": f"⏱ *{tag}CALL EXPIRED*"}[outcome]
    fast = ("\n⚡ That was very fast - look at the chart: it is either a rug or bad price data."
            if outcome == "stop" and now - r["t0"] < 900 else "")
    send_telegram(
        f"{head}: ${md_escape(r['sym'])} | {CHAINS[r['chain']]['name']}\n"
        f"Called at {fmt_usd(r['mc0'])} → now {fmt_usd(mc)} (*{r['final']:.2f}x*, best {r['best']:.2f}x) "
        f"after {ago(now - r['t0'])}{fast}\n`{r['key'].split(':', 1)[1]}`\nThe bot is learning from this result.")


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


def retrace_bucket(x: float) -> str:
    for hi, lab in ((50, "under 50%"), (60, "50-60%"), (75, "60-75%"), (90, "75-90%")):
        if x < hi:
            return lab
    return "over 90%"


RETRACE_ORDER = ("under 50%", "50-60%", "60-75%", "75-90%", "over 90%")
AGE_BUCKETS = (("under 30d", 0, 720), ("30-90d", 720, 2160), ("90d+", 2160, 10 ** 9))   # hours


def breakdown_lines(real: list[dict], shadows: list[dict]) -> list[str]:
    """What seems to matter? Won/followed for the alerts, split by how deep the dip was, how old the coin was
    and how many clues it had. (Experiments = coins in the zone that we did not call.)"""
    out = []
    by_dip: dict[str, list[dict]] = {}
    for r in real:
        by_dip.setdefault(retrace_bucket(r["m"]["retrace"]), []).append(r)
    miss: dict[str, list[dict]] = {}
    for r in shadows:
        miss.setdefault(retrace_bucket(r["m"]["retrace"]), []).append(r)
    parts = []
    for lab in RETRACE_ORDER:
        a, b = summarize(by_dip.get(lab, [])), summarize(miss.get(lab, []))
        if a["n"] or b["n"]:
            parts.append(f"{lab} {a['w']}/{a['n']}" + (f" [not called: {b['w']}/{b['n']}]" if b["n"] else ""))
    if parts:
        out.append("📉 By how deep the dip was (won/followed): " + " | ".join(parts))
    parts = []
    for lab, lo, hi in AGE_BUCKETS:
        rs = [r for r in real if lo <= r["m"].get("age_h", 0) < hi]
        if rs:
            s = summarize(rs)
            parts.append(f"{lab} {s['w']}/{s['n']}")
    if parts:
        out.append("🆕 By coin age (won/followed): " + " | ".join(parts))
    withsig = [r for r in real if "sig" in r]
    parts = []
    for lab, lo, hi in (("0-1 clues", 0, 1), ("2 clues", 2, 2), ("3+ clues", 3, 99)):
        rs = [r for r in withsig if lo <= len(r["sig"]) <= hi]
        if rs:
            s = summarize(rs)
            parts.append(f"{lab} {s['w']}/{s['n']}")
    if len(parts) >= 2:
        out.append("🎯 By number of clues (won/followed): " + " | ".join(parts))
    parts = []
    for sig, lab in SIGNAL_LABELS.items():
        rs = [r for r in withsig if sig in r["sig"]]
        if len(rs) >= 2:
            s = summarize(rs)
            parts.append(f"{lab} {s['w']}/{s['n']}")
    if parts:
        out.append("🔬 When this clue was present: " + "; ".join(parts))
    return out


def build_report(sugg: list[tuple], applied: list[tuple[str, str]]) -> str:
    calls, narrs, wdips = closed_records("call"), closed_records("narrative"), closed_records("wallet")
    shadows = closed_records("shadow")
    c, nr, wd = summarize(calls), summarize(narrs), summarize(wdips)
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
        young = [r for r in narrs if 0 < r["m"].get("age_h", 0) < 72]     # brand-new launches vs coins with some history
        older = [r for r in narrs if r["m"].get("age_h", 0) >= 72]
        if young and older:
            ys, old_s = summarize(young), summarize(older)
            L.append(f"By coin age: under 3 days {ys['w']}/{ys['n']} won | older {old_s['w']}/{old_s['n']} won")
    if wd["n"]:
        L += ["", f"👛 Wallet-backed alerts finished: *{wd['n']}*  ✅ {wd['w']} wins | 🛑 {wd['s']} stopped "
                  f"→ win rate *{wd['wr']:.0%}*"]
    L += breakdown_lines(calls + wdips, shadows) if len(calls + wdips) >= 3 else []
    L += bottom_report_lines()

    alerts = calls + narrs + wdips
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
    set_aside = sum(1 for r in state["records"] if r.get("void"))
    if set_aside:
        L += ["", f"🧹 {set_aside} older result(s) from before the price-glitch fixes were set aside - not counted."]
    L += ["", f"Following now: {len(open_records(kind='call'))} calls, {len(open_records(kind='narrative'))} "
              f"narrative, {len(open_records(kind='wallet'))} wallet-backed, {len(open_records(kind='bottom'))} bottom, "
              f"{len(open_records(kind='shadow')) + len(open_records(kind='bshadow'))} "
              f"experiments, {len(open_records(kind='whale'))} wallet buys"]

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
    closed_alerts = sum(len(closed_records(k)) for k in ALERT_KINDS)
    due_calls = closed_alerts - tun.get("reported_calls", 0) >= REPORT_EVERY_CALLS
    total_closed = sum(1 for r in state["records"] if r["status"] != "open" and not r.get("void"))
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
    for k in [k for k, b in state["bottoms"].items() if now - b.get("last", 0) > 21 * 86400 and not open_records(k)]:
        state["bottoms"].pop(k, None)     # Sparsity has not listed it for three weeks
    try:
        promote_smart_wallets()
    except Exception:
        log.exception("smart-wallet promotion error")


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
    if (m["liq"] < NARR_MIN_LIQ or m["liq_ratio"] < NARR_MIN_LIQ_RATIO or m["vol_ratio"] < NARR_MIN_VOL_RATIO
            or m["traders"] < NARR_MIN_TRADERS or m["age_h"] < NARR_MIN_AGE_H
            or h24 > NARR_MAX_H24 or h24 < NARR_MIN_H24            # already ran, or already dumping
            or m["vol24"] > m["liq"] * NARR_MAX_VOL_LIQ):          # volume wildly out of line with the liquidity
        return False
    ckey = "narr:" + key
    if now - state["cooldowns"].get(ckey, 0) < 24 * 3600:
        return False
    stamps = [t for t in state["tuning"].get("narr_ts", []) if now - t < 86400]
    state["tuning"]["narr_ts"] = stamps
    if len(stamps) >= NARR_MAX_PER_DAY:
        return False
    if not confirmed_twice("narr", key, m["mc"], now):
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
        + f"\n⚠️ Early-stage theme play, NOT a re-run setup - higher risk. Tracked for {TRACK_HOURS:.0f}h.\n"
        + chain_note(chain) + "\n"
        f"`{addr}`\n\n"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
        f"[Explorer]({link(chain, 'token', addr)})")
    if not send_telegram(text):
        return False
    state["cooldowns"][ckey] = now
    stamps.append(now)
    bot_stats["narrative"] += 1
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
            peak = fetch_peak_mc(chain, pair, key)
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
            zone = m["peak"] >= MIN_PEAK_MC * 0.8 and T["retrace_min"] <= m["retrace"] <= T["retrace_max"]
            extra = (f"📉 {m['retrace']:.0f}% below its peak of {fmt_usd(m['peak'])} "
                     f"(market cap now {fmt_usd(m['mc'])})\n")
            if zone:
                extra = "🔥 *SMART MONEY RE-RUN ACCUMULATION*\n" + extra
            add_record("whale", key, chain, ctx["pair"], m, "", now, extra={"wallet": wallet, "zone": zone})
    ok = send_telegram(build_whale_message(swap, extra))
    if ok:
        bot_stats["whale"] += 1
    if len(cluster) >= SMART_CLUSTER_MIN and now - state["cooldowns"].get("cluster:" + key, 0) > 7200:
        state["cooldowns"]["cluster:" + key] = now
        total = sum(u for t, w, u in state["whale_buys"][key] if now - t <= CLUSTER_WINDOW)
        send_telegram(
            f"👥 *WALLET CLUSTER*: {len(cluster)} tracked wallets bought ${md_escape(swap['symbol'])} "
            f"within 30 min (total {fmt_usd(total)})  |  {CHAINS[chain]['name']}\n" + extra
            + f"`{token}`\n"
            + f"[DexScreener](https://dexscreener.com/{chain}/{token}) | [Token]({link(chain, 'token', token)})")
    return ok


# ----------------------------------------------------------------------------
# YOUR WATCHED WALLETS: "did my smart wallets buy this coin - and are they still in?"
# Add wallets in the Telegram chat with /watch. The bot then (1) says which of them hold each coin it flags,
# (2) adds the coins they hold to its watch list, (3) tells you when one of them buys or dumps a coin, and
# (4) sends a "wallet-backed dip" alert when they hold a coin that is in the dip range.
# ----------------------------------------------------------------------------
SOL_ADDR = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
EVM_ADDR = re.compile(r"^0x[0-9a-fA-F]{40}$")


def wallet_kind(text: str) -> str | None:
    if EVM_ADDR.match(text):
        return "evm"       # the same address works on Ethereum, Base, BNB Chain and Robinhood Chain
    return "solana" if SOL_ADDR.match(text) else None


def wallet_label(addr: str) -> str:
    return (state["watch"].get(addr) or {}).get("name") or short(addr)


def add_watch(addr: str, name: str = "") -> str | None:
    """Start watching a wallet. Returns its stored key, or None if the text is not a wallet address."""
    kind = wallet_kind(addr)
    if not kind:
        return None
    key = addr.lower() if kind == "evm" else addr
    info = state["watch"].setdefault(key, {"kind": kind, "t": time.time()})
    if name:
        info["name"] = name[:30]
    gone = state["tuning"].get("watch_removed") or []
    if key in gone:
        state["tuning"]["watch_removed"] = [g for g in gone if g != key]
    return key


def find_watch(text: str) -> str | None:
    """A watched wallet by full address, short form (ab12...cd34), the first letters of its address, or its name."""
    t = text.strip().lower()
    if not t:
        return None
    for w, d in list(state["watch"].items()):
        if (t == w.lower() or t == short(w).lower() or (len(t) >= 6 and w.lower().startswith(t))
                or t == (d.get("name") or "").lower()):
            return w
    return None


def remove_watch(key: str):
    state["watch"].pop(key, None)
    _snaps.pop(key, None)
    for k in [k for k in list(_hold_cache) if k[0] == key]:
        _hold_cache.pop(k, None)
    gone = state["tuning"].setdefault("watch_removed", [])
    if key not in gone:
        gone.append(key)    # so that WATCH_WALLETS does not bring it back after a restart


def seed_watch_wallets():
    """WATCH_WALLETS="Whale1:address,address2" - an optional extra on top of adding wallets in the chat."""
    gone = set(state["tuning"].get("watch_removed") or [])
    for item in WATCH_WALLETS_SEED.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, addr = item.rpartition(":")
        addr, name = addr.strip(), name.strip()
        kind = wallet_kind(addr)
        if not kind:
            log.warning("WATCH_WALLETS: '%s' is not a wallet address - ignored", short(item))
            continue
        key = addr.lower() if kind == "evm" else addr
        if key not in gone and key not in state["watch"]:
            add_watch(addr, name)


# --- looking inside a wallet (read-only: only public blockchain data, nothing is ever signed or sent) ---
_rpc_fail: dict[str, tuple[int, float]] = {}     # link -> (failures in a row, when the last one happened)
_hold_cache: dict[tuple[str, str], tuple[float, float]] = {}   # (wallet, coin) -> (when looked, amount held)
_snaps: dict[str, tuple[float, dict[str, float]]] = {}         # Solana wallet -> (when, {coin: amount}) from the last sweep
_priced: dict[str, tuple[float, dict | None]] = {}             # Solana coin -> (when priced, best live pool or None)
_decimals: dict[tuple[str, str], int] = {}
_sweep = {"now": False, "last": 0.0}
HOLD_CACHE_SECONDS = 300


def rpc_url_for(chain: str) -> str:
    return SOLANA_RPC_URL if chain == "solana" else EVM_RPC.get(chain, "")


def rpc_call(url: str, method: str, params: list, timeout: float = 12.0):
    """One JSON-RPC request to a blockchain node. After three failures in a row that link is left alone for 2 minutes."""
    n, t = _rpc_fail.get(url, (0, 0.0))
    if n >= 3 and time.time() - t < 120:
        raise RuntimeError("node link is busy - skipped for a moment")
    try:
        r = http.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=timeout)
        r.raise_for_status()
        d = r.json()
        if not isinstance(d, dict) or d.get("error") or "result" not in d:
            raise ValueError("node answered with an error: " + str(d.get("error") if isinstance(d, dict) else d)[:100])
    except (requests.RequestException, ValueError):
        _rpc_fail[url] = (n + 1, time.time())
        raise
    _rpc_fail.pop(url, None)
    return d["result"]


def sol_token_amounts(owner: str, flt: dict) -> dict[str, float]:
    """{coin: amount} for a Solana wallet's token accounts. flt = {"mint": coin} or {"programId": token program}."""
    res = rpc_call(SOLANA_RPC_URL, "getTokenAccountsByOwner", [owner, flt, {"encoding": "jsonParsed"}], timeout=20)
    out: dict[str, float] = {}
    for acc in (res or {}).get("value") or []:
        info = ((((acc or {}).get("account") or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        ta = info.get("tokenAmount") or {}
        try:
            amt = float(ta.get("uiAmountString") or ta.get("uiAmount") or 0)
        except (TypeError, ValueError):
            continue
        if info.get("mint") and amt > 0:
            out[info["mint"]] = out.get(info["mint"], 0.0) + amt
    return out


def sol_snapshot(owner: str) -> dict[str, float]:
    """Everything a Solana wallet holds. Both token programs must answer, or the whole look counts as failed
    (half a picture would make coins look 'sold')."""
    out: dict[str, float] = {}
    for prog in SPL_TOKEN_PROGRAMS:
        for mint, amt in sol_token_amounts(owner, {"programId": prog}).items():
            out[mint] = out.get(mint, 0.0) + amt
    return out


def evm_balance(chain: str, wallet: str, token: str) -> float:
    url = EVM_RPC.get(chain, "")
    if not url:
        raise RuntimeError("no node link for " + chain)
    dec = _decimals.get((chain, token))
    if dec is None:
        dec = int(rpc_call(url, "eth_call", [{"to": token, "data": "0x313ce567"}, "latest"]), 16)   # decimals()
        _decimals[(chain, token)] = dec
    data = "0x70a08231" + wallet[2:].lower().rjust(64, "0")                                          # balanceOf(wallet)
    return int(rpc_call(url, "eth_call", [{"to": token, "data": data}, "latest"]), 16) / 10 ** dec


def watch_holders(chain: str, token: str, price: float, use_rpc: bool = True, max_age: float = HOLD_CACHE_SECONDS) -> dict:
    """Which of the owner's watched wallets are holding this coin right now? Never raises.
    {"in": [(name, usd, amount)], "out": [names], "unknown": [names], "total": how many wallets could hold it}
    An earlier answer younger than max_age seconds is reused instead of asking the blockchain again."""
    res = {"in": [], "out": [], "unknown": [], "total": 0}
    try:
        kind = "solana" if chain == "solana" else "evm"
        if not rpc_url_for(chain):
            return res   # no node link for this chain (Robinhood Chain, unless the owner pasted one): nothing to say
        wallets = [w for w, d in list(state["watch"].items()) if d.get("kind") == kind]
        res["total"] = len(wallets)
        key, now = sk(chain, token), time.time()
        for w in wallets:
            amt = None
            snap = _snaps.get(w)
            if kind == "solana" and snap and now - snap[0] < WATCH_SWEEP_SECONDS * 2:
                amt = snap[1].get(token, 0.0)             # from the latest sweep: no extra request needed
            else:
                hit = _hold_cache.get((w, key))
                if hit and now - hit[0] < max_age:
                    amt = hit[1]
                elif use_rpc and rpc_url_for(chain):
                    try:
                        amt = (sol_token_amounts(w, {"mint": token}).get(token, 0.0) if kind == "solana"
                               else evm_balance(chain, w, norm_addr(chain, token)))
                        _hold_cache[(w, key)] = (now, amt)
                    except Exception as e:
                        log.warning("could not check %s for %s: %s", short(w), key, scrub(e))
            label = wallet_label(w)
            if amt is None:
                res["unknown"].append(label)
            elif amt > 0 and (price <= 0 or amt * price >= WATCH_MIN_HOLD_USD):
                res["in"].append((label, amt * price, amt))
            else:
                res["out"].append(label)
    except Exception:
        log.exception("watched-wallet lookup error")
    return res


def wallets_line(res: dict) -> str:
    """One line for alerts and check-ins: which of the owner's watched wallets are still in this coin."""
    if not res["total"]:
        return ""
    if res["in"]:
        who = ", ".join(f"*{md_escape(n)}*" + (f" ({fmt_usd(u)})" if u > 0 else "")
                        for n, u, _ in sorted(res["in"], key=lambda x: -x[1]))
        more = (f" · {len(res['out'])} not holding" if res["out"] else "") + \
               (f" · couldn't check {len(res['unknown'])}" if res["unknown"] else "")
        return f"👛 Your wallets still in: {who}{more}\n"
    if not res["out"]:
        return f"👛 Couldn't check your {res['total']} watched wallet(s) just now.\n"
    return (f"👛 None of your {res['total']} watched wallet(s) are in it"
            + (f" (couldn't check {len(res['unknown'])})" if res["unknown"] else "") + ".\n")


# --- sweeps: look inside the watched Solana wallets every few minutes ---
def price_coins(mints: list[str]) -> dict[str, dict]:
    """The best live DexScreener pool for each Solana coin, 30 per request. Answers are remembered for a while
    (a wallet's junk airdrops have no pool at all and are not asked about again for hours)."""
    now, out, todo = time.time(), {}, []
    for mint in mints:
        hit = _priced.get(mint)
        if hit and now - hit[0] < (900 if hit[1] else 6 * 3600):
            if hit[1]:
                out[mint] = hit[1]
        else:
            todo.append(mint)
    for i in range(0, len(todo), 30):
        batch = todo[i:i + 30]
        time.sleep(1.0)
        try:
            pairs = fetch_pairs("solana", batch)
        except requests.RequestException as e:
            log.warning("coin prices for a wallet check are busy (%s)", getattr(getattr(e, "response", None), "status_code", "error"))
            continue
        if len(batch) >= 3 and not pairs:
            continue  # an empty answer for a whole batch is a hiccup - don't remember it as 'no pool'
        grouped: dict[str, list[dict]] = {}
        for p in pairs:
            grouped.setdefault(p.get("baseToken", {}).get("address", ""), []).append(p)
        for mint in batch:
            pair = best_live_pair(grouped.get(mint, []))
            _priced[mint] = (now, pair)
            if pair:
                out[mint] = pair
    return out


def handle_watch_move(wallet: str, mint: str, side: str, usd: float, pair: dict) -> bool:
    """A watched wallet picked up or dumped a coin: tell the owner (it also feeds clusters + wallet scoring)."""
    now, key = time.time(), sk("solana", mint)
    if side == "BUY" and any(w == wallet and now - t < 3600 for t, w, _ in state["whale_buys"].get(key, [])):
        return False  # the Helius webhook already announced this buy
    swap = {"chain": "solana", "wallet": wallet, "side": side, "token": mint,
            "symbol": pair.get("baseToken", {}).get("symbol", "?"), "usd": usd,
            "signature": f"sweep:{wallet}:{mint}:{int(now // 600)}", "src": "watch"}
    if _is_duplicate(swap["signature"]):
        return False
    return handle_whale_swap(swap)


def wallet_sweep() -> int:
    """Look inside every watched Solana wallet. Coins they hold join the list the scanner follows; a coin that is
    new in a wallet (or that it mostly dumped) is announced. The first look at a wallet only sets the baseline."""
    now, sent = time.time(), 0
    for w in [w for w, d in list(state["watch"].items()) if d.get("kind") == "solana"]:
        try:
            snap = sol_snapshot(w)
        except Exception as e:
            log.warning("could not look inside wallet %s: %s", short(w), scrub(e))
            continue
        old = _snaps.get(w)
        _snaps[w] = (now, snap)
        first, prev = old is None, (old[1] if old else {})
        need = [m_ for m_, a in snap.items() if first or a >= 1.5 * prev.get(m_, 0.0)]        # new coins, or positions that grew 50%+
        need += [m_ for m_, a in prev.items() if snap.get(m_, 0.0) < 0.5 * a and m_ not in need]   # coins it mostly sold
        need = [m_ for m_ in need if not is_quote_token("solana", m_)]
        priced = price_coins(need)
        for mint in need:
            pair = priced.get(mint)
            if not pair:
                continue
            px, liq = num(pair, "priceUsd"), num(pair, "liquidity", "usd")
            mc = num(pair, "marketCap") or num(pair, "fdv")
            cur, was, key = snap.get(mint, 0.0), prev.get(mint, 0.0), sk("solana", mint)
            held = cur * px
            if (held >= WATCH_MIN_HOLD_USD and mc_floor() <= mc <= MAX_COIN_MC and not too_old(pair)
                    and key not in state["discovered"] and key not in state["dismissed"]
                    and not manual_coin("solana", mint)):
                state["discovered"][key] = now   # the scanner will follow it from the next scan on
            if first or liq < 10_000 or px <= 0:
                continue
            try:
                if cur >= 1.5 * was and (cur - was) * px >= WATCH_MIN_MOVE_USD:   # a new position, or a big add to one
                    sent += bool(handle_watch_move(w, mint, "BUY", held, pair))
                elif was > 0 and cur < 0.5 * was and (was - cur) * px >= WATCH_MIN_MOVE_USD:
                    sent += bool(handle_watch_move(w, mint, "SELL", (was - cur) * px, pair))
            except Exception:
                log.exception("wallet move could not be reported")
    return sent


DIP_HARD = {"peak", "retrace_lo", "retrace_hi", "age", "nomc", "old"}   # problems a wallet-backed alert cannot excuse


def maybe_watch_dip(key: str, chain: str, pair: dict, m: dict, fails: list, now: float, budget: list) -> bool:
    """A coin in the dip range that one of YOUR watched wallets is still holding. It may be a little short on
    volume or trades for a normal re-run alert, but smart money staying in is a signal of its own."""
    codes = {c for c, _ in fails}
    known = key in state["backfilled"]
    if not codes or codes & {"age", "nomc", "old"} or (known and codes & DIP_HARD):
        return False   # it must really be a dipped coin of the right size and age
    if (m["liq"] < 10_000 or m["liq_ratio"] < T["liq"] * 0.5 or m["vol_ratio"] < T["vol"] * 0.3
            or m["traders"] < T["traders"] * 0.2):
        return False   # still has to be a live, tradable coin
    ckey = "wal:" + key
    if now - state["cooldowns"].get(ckey, 0) < COOLDOWN_SECONDS:
        return False
    stamps = [t for t in state["tuning"].get("wal_ts", []) if now - t < 86400]
    state["tuning"]["wal_ts"] = stamps
    if len(stamps) >= WATCH_DIP_MAX_PER_DAY:
        return False
    held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
    if len(held["in"]) < WATCH_MIN_HOLDERS:
        return False
    if not known:  # a quiet coin is not worth a past-high look-up - unless your wallets hold it
        maybe_backfill_peak(key, chain, pair, budget, force=True)
        if key not in state["backfilled"]:
            return False   # busy or failed: try again on the next scan
        _, m, fails = evaluate(key, pair)
        if not m or {c for c, _ in fails} & DIP_HARD:
            return False
        codes = {c for c, _ in fails}
        if not codes:
            return False   # it passes everything after all: the normal alert path will pick it up
    if not confirmed_twice("wal", key, m["mc"], now):
        return False
    sig = signals_for(m, len(held["in"]))
    extra = (wallets_line(held) + signals_text(sig)
             + f"ℹ️ Not a full re-run setup ({'; '.join(fail_label(c) for c in sorted(codes))}), "
               "but wallets you trust are still holding it.\n" + chain_note(chain))
    if not send_telegram(build_rerun_message(chain, pair, m, extra, title="👛 WALLET-BACKED DIP")):
        return False
    state["cooldowns"][ckey] = now
    stamps.append(now)
    bot_stats["walletdip"] += 1
    add_record("wallet", key, chain, pair, m, "", now, extra={"sig": sig})
    log.info("WALLET-BACKED DIP alert for %s", key)
    return True


async def wallet_loop():
    """Every few minutes: look inside the watched Solana wallets. Does nothing until you add one with /watch."""
    await asyncio.sleep(WALLET_LOOP_START_DELAY)
    while True:
        try:
            if any(d.get("kind") == "solana" for d in list(state["watch"].values())) and (
                    _sweep["now"] or time.time() - _sweep["last"] >= WATCH_SWEEP_SECONDS):
                _sweep["now"], _sweep["last"] = False, time.time()
                await asyncio.to_thread(wallet_sweep)
        except Exception:
            log.exception("wallet sweep error")
        await asyncio.sleep(10)


# ----------------------------------------------------------------------------
# GECKOTERMINAL: one shared, polite connection. Its free plan allows roughly 10 requests a minute IN TOTAL, so
# every part of the bot that needs it (past highs, coin lists, recent trades) queues here instead of bursting.
# ----------------------------------------------------------------------------
GECKO_HEADERS = {"Accept": "application/json;version=20230302"}
_gecko_lock = threading.Lock()
_gecko_next = 0.0


def gecko_wait():
    global _gecko_next
    with _gecko_lock:
        now = time.time()
        slot = max(now, _gecko_next)
        _gecko_next = slot + GECKO_MIN_GAP
    if slot > now:
        time.sleep(slot - now)


def gecko_pause(seconds: float):
    """GeckoTerminal said 'too many requests': leave it alone for a while."""
    global _gecko_next
    with _gecko_lock:
        _gecko_next = max(_gecko_next, time.time() + seconds)


def gecko_get(url: str, tries: int = 3, **kw):
    gecko_wait()
    kw.setdefault("headers", GECKO_HEADERS)
    try:
        return get_with_retry(url, tries=tries, **kw)
    except requests.RequestException as e:
        if getattr(getattr(e, "response", None), "status_code", None) == 429:
            gecko_pause(60)
        raise


# ----------------------------------------------------------------------------
# BOTTOMED COINS: Sparsity Research's public "bottoming" page.
# It reads three on-chain signs after a coin has fallen, in order: 1 sellers capitulated, 2 the low is holding,
# 3 demand returning. The bot reads that page a few times a day, follows the coins it lists, alerts on 3 of 3, and
# for every such coin looks at WHO has been buying it. Wallets that keep buying coins shortly before they rise are
# added to your watch list automatically (smart money found by results, not by guesswork).
# ----------------------------------------------------------------------------
SPARSITY_STEPS = ("sellers capitulated", "the low is holding", "demand returning")
_UNITS = {"": 1.0, "K": 1e3, "M": 1e6, "B": 1e9}
_sp_unresolved: dict[str, float] = {}     # coin address -> when no chain could be found for it (asked again later)
_sp = {"last_try": 0.0, "last_ok": 0.0, "fails": 0, "warned": 0.0, "updated": "", "err": "", "n": 0, "n3": 0,
       "followed": 0, "partial": False}


def _money(text: str, unit: str) -> float:
    try:
        return float(text.replace(",", "")) * _UNITS.get((unit or "").upper(), 1.0)
    except (ValueError, AttributeError):
        return 0.0


def read_caption(cap: str) -> dict:
    """'Market cap now $4.01M, 35% above the low of $2.97M on Oct 8 · 30-day high $25.62M' -> the numbers in it."""
    out: dict = {}
    m = re.search(r"Market cap now \$([\d.,]+)([KMB]?)", cap)
    if m:
        out["mc"] = _money(m.group(1), m.group(2))
    m = re.search(r"(\d+(?:\.\d+)?)% above the low of \$([\d.,]+)([KMB]?)(?: on ([A-Za-z]{3} \d{1,2}))?", cap)
    if m:
        out["above_low"], out["low"], out["low_day"] = float(m.group(1)), _money(m.group(2), m.group(3)), m.group(4) or ""
    elif "at its low" in cap:
        out["above_low"] = 0.0
    m = re.search(r"30-day high \$([\d.,]+)([KMB]?)", cap)
    if m:
        out["hi"] = _money(m.group(1), m.group(2))
    return out


def slim_sparsity_coin(c) -> dict | None:
    """One coin from Sparsity's list, reduced to what the bot needs. None if it is not a usable coin."""
    if not isinstance(c, dict):
        return None
    ca, name = str(c.get("ca") or "").strip(), str(c.get("name") or "").strip()
    if not name or not (EVM_ADDR.match(ca) or SOL_ADDR.match(ca)):
        return None
    signs = c.get("signs")
    on_map = str(c.get("place") or "ok") == "ok"
    signs = int(signs) if (on_map and isinstance(signs, (int, float)) and not isinstance(signs, bool)
                           and 0 <= signs <= 3) else None
    price = c.get("price") if isinstance(c.get("price"), dict) else {}
    cap = str(price.get("caption") or "")[:220]
    try:
        fall = float(c.get("fall") or 0.0)
    except (TypeError, ValueError):
        fall = 0.0
    out = {"name": name[:24], "ca": ca, "signs": signs, "fall": fall, "cap": cap,
           "through": str(c.get("through") or "")[:30]}
    out.update(read_caption(cap))
    return out


def parse_sparsity(html: str):
    """The coin list inside Sparsity's page -> (coins, 'Oct 11, 21:30 UTC' or ''). (None, '') if the page is not in
    a layout I recognise. The list is embedded in the page's own data (Next.js 'self.__next_f.push' pieces)."""
    dec = json.JSONDecoder()
    parts, pos, marker = [], 0, "self.__next_f.push([1,"
    while True:
        i = html.find(marker, pos)
        if i < 0:
            break
        pos = i + len(marker)
        while pos < len(html) and html[pos] in " \t\r\n":
            pos += 1
        try:
            piece, end = dec.raw_decode(html, pos)
        except ValueError:
            continue
        if isinstance(piece, str):
            parts.append(piece)
        pos = end
    text = "".join(parts)
    raw = None
    for blob in (text, html):                      # the page's data first, then the raw page as a second chance
        k = blob.find('"coins":[')
        if k >= 0:
            try:
                raw, _ = dec.raw_decode(blob, k + len('"coins":'))
                break
            except ValueError:
                raw = None
    if not isinstance(raw, list):
        return None, ""
    coins = [c for c in (slim_sparsity_coin(x) for x in raw) if c]
    um = (re.search(r'"Updated ","([^"]{4,40})"', text)
          or re.search(r"Updated\s+([A-Z][a-z]{2} \d{1,2}, \d{1,2}:\d{2} UTC)", html))
    return (coins or None), (um.group(1) if um else "")


def lookup_chain(addr: str):
    """(chain, answered): which of our EVM chains does this coin trade on (the deepest pool wins)?
    answered=False means DexScreener could not be reached, so 'not found' must not be remembered."""
    try:
        data = get_with_retry(f"https://api.dexscreener.com/latest/dex/tokens/{addr}", tries=3).json()
    except (requests.RequestException, ValueError):
        return None, False
    best, best_liq = None, 0.0
    for p in (data.get("pairs") or []) if isinstance(data, dict) else []:
        ch = norm_chain(str(p.get("chainId", "")))
        if ch and ch != "solana" and norm_addr(ch, p.get("baseToken", {}).get("address", "")) == addr.lower():
            liq = num(p, "liquidity", "usd")
            if liq > best_liq:
                best, best_liq = ch, liq
    return best, True


def bottom_active(b: dict) -> bool:
    """Is this coin on Sparsity's list right now with enough signs to follow? (A page that has not been readable
    for a long time stops counting, so a dead feed cannot keep old coins alive.)"""
    s = b.get("signs")
    return (isinstance(s, int) and s >= BOTTOM_FOLLOW_SIGNS
            and time.time() - b.get("last", 0) <= SPARSITY_REFRESH_HOURS * 7200 + 6 * 3600)


def bottom_entry(key: str) -> dict | None:
    b = state["bottoms"].get(key)
    return b if b and bottom_active(b) else None


def ingest_sparsity(coins: list[dict], updated: str) -> dict:
    """Update the memory of listed coins. EVM coins need one DexScreener question to learn their chain."""
    now = time.time()
    book = state["bottoms"]
    by_ca = {str(b.get("ca", "")).lower(): k for k, b in book.items()}
    errors, partial = 0, False
    for c in coins:
        ca, signs = c["ca"], c["signs"]
        key = by_ca.get(ca.lower())
        if key is None:
            if signs is None or signs < BOTTOM_FOLLOW_SIGNS:
                continue
            tried = _sp_unresolved.get(ca.lower())
            if tried and now - tried < 6 * 3600:
                continue
            if errors >= 2:             # DexScreener is not answering: do the rest on the next try
                partial = True
                continue
            if EVM_ADDR.match(ca):
                time.sleep(0.3)
                chain, answered = lookup_chain(ca)
                if not answered:
                    errors += 1
                    partial = True
                    continue
            else:
                chain = "solana"
            if not chain:
                _sp_unresolved[ca.lower()] = now
                continue
            key = sk(chain, ca)
            book[key] = {"ca": norm_addr(chain, ca), "chain": chain, "first": now}
            by_ca[ca.lower()] = key
        b = book[key]
        b["prev"] = b.get("signs")
        b.update(name=c["name"], signs=signs, fall=c["fall"], caption=c["cap"], through=c["through"], last=now,
                 upd=updated)
        for k in ("mc", "above_low", "low", "low_day", "hi"):
            b[k] = c.get(k)
    _sp.update(updated=updated, n=len(coins), n3=sum(1 for c in coins if c["signs"] == 3),
               followed=sum(1 for b in book.values() if bottom_active(b)), partial=partial)
    return {"partial": partial}


def refresh_sparsity(force: bool = False) -> int:
    """Read Sparsity's page (at most every SPARSITY_REFRESH_HOURS - the page itself changes once a day).
    Returns how many coins were read (0 = nothing done or it failed). Never raises for a page problem."""
    if not SPARSITY_FEED:
        return 0
    now = time.time()
    gap = SPARSITY_REFRESH_HOURS * 3600
    if _sp["partial"]:
        gap = min(gap, 1800)
    elif _sp["fails"]:
        gap = min(gap, 1800 * _sp["fails"])
    if not force and now - _sp["last_try"] < gap:
        return 0
    _sp["last_try"] = now
    coins, updated, err = None, "", ""
    try:
        r = http.get(SPARSITY_URL, timeout=30, headers={
            "Accept": "text/html", "Accept-Language": "en",
            "User-Agent": f"Mozilla/5.0 (compatible; multichain-alert-bot/{BOT_VERSION})"})
        r.raise_for_status()
        page = r.content.decode("utf-8", errors="replace")
        coins, updated = parse_sparsity(page)
        if not coins:
            if any(x in page for x in ("Just a moment", "cf-browser-verification", "challenge-platform", "Attention Required")):
                err = "the site is showing a 'prove you are human' check to my server"
            else:
                err = "the page no longer has the coin list I know how to read"
    except requests.RequestException as e:
        err = "could not open the page (%s)" % (getattr(getattr(e, "response", None), "status_code", None) or "no answer")
    if not coins:
        _sp["fails"] += 1
        _sp["err"] = err
        log.warning("Sparsity feed: %s (try %d)", err, _sp["fails"])
        if _sp["fails"] >= 3 and now - _sp["warned"] > 24 * 3600:
            _sp["warned"] = now
            send_telegram(f"⚠️ I can't read the Sparsity bottom list right now: {err}. Everything else keeps running; "
                          "I'll keep trying.")
        return 0
    _sp["fails"], _sp["err"], _sp["last_ok"] = 0, "", now
    ingest_sparsity(coins, updated)
    log.info("Sparsity: %d coins read (%d with all 3 signs), %d followed, page says '%s'",
             _sp["n"], _sp["n3"], _sp["followed"], updated or "?")
    if not state["tuning"].get("sp_intro"):
        state["tuning"]["sp_intro"] = True
        ready = [b for b in list(state["bottoms"].values()) if bottom_active(b) and b["signs"] >= BOTTOM_MIN_SIGNS]
        if BOTTOM_ALERTS and len(ready) >= 3:
            send_telegram(
                f"🧭 *Bottomed-coin list is live.* Sparsity currently shows {len(ready)} coins with "
                f"{BOTTOM_MIN_SIGNS} of 3 bottom signs: "
                + ", ".join("$" + md_escape(b["name"]) for b in sorted(ready, key=lambda x: x["name"])[:15])
                + ".\nI'll check who is buying each one and send them over the next few minutes. "
                  "Send /bottoms any time to see the list.")
    return len(coins)


# --- who is buying a coin? (GeckoTerminal's latest trades on its main pool) ---
def gecko_trades(chain: str, pool: str) -> list:
    net = GECKO_NETWORK.get(chain)
    if not (net and pool):
        raise ValueError("no pool to look at")
    url = f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}/trades"
    try:
        r = gecko_get(url, params={"trade_volume_in_usd_greater_than": BUYER_TRADE_FLOOR_USD}, timeout=20)
    except requests.HTTPError as e:
        if getattr(e.response, "status_code", None) not in (400, 422):
            raise
        r = gecko_get(url, timeout=20)           # the size filter was refused: ask for all recent trades instead
    data = r.json().get("data")
    return data if isinstance(data, list) else []


def is_bundler(chain: str, wallet: str) -> bool:
    """0x4337... addresses are account-abstraction relayers: one address sends other people's trades, so it says
    nothing about a single trader (very common on the newest chain)."""
    return chain != "solana" and wallet.lower().startswith("0x4337")


def analyze_trades(chain: str, trades: list, pool: str, token: str, mc: float) -> dict:
    """Group a coin's latest trades by wallet, set bots and routers aside, and pick out the wallets that are
    ACCUMULATING (bought a meaningful amount and sold little)."""
    per: dict[str, dict] = {}
    kinds: dict[tuple, set] = {}
    first = last = 0.0
    skip = {norm_addr(chain, pool), norm_addr(chain, token)}
    n = 0
    for t in trades or []:
        a = t.get("attributes") if isinstance(t, dict) else None
        if not isinstance(a, dict):
            continue
        w, kind = str(a.get("tx_from_address") or ""), a.get("kind")
        if not w or kind not in ("buy", "sell"):
            continue
        try:
            usd = float(a.get("volume_in_usd") or 0)
        except (TypeError, ValueError):
            continue
        if not (0 < usd < 1e12):
            continue
        w = norm_addr(chain, w)
        n += 1
        ts = 0.0
        try:
            ts = datetime.fromisoformat(str(a.get("block_timestamp", "")).replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
        if ts:
            first, last = (min(first, ts) if first else ts), max(last, ts)
        d = per.setdefault(w, {"w": w, "buy": 0.0, "sell": 0.0, "n": 0, "pxw": 0.0})
        d["n"] += 1
        if kind == "buy":
            d["buy"] += usd
            try:
                d["pxw"] += float(a.get("price_to_in_usd") or 0) * usd
            except (TypeError, ValueError):
                pass
        else:
            d["sell"] += usd
        kinds.setdefault((w, str(a.get("tx_hash") or "")), set()).add(kind)
    for (w, _), ks in kinds.items():
        if len(ks) > 1:
            per[w]["arb"] = True          # bought AND sold in one transaction: a router / arbitrage bot
    for d in per.values():
        d["bot"] = bool(d["n"] > BUYER_MAX_TRADES or d.get("arb") or d["w"] in skip or is_bundler(chain, d["w"]))
        d["px"] = d["pxw"] / d["buy"] if d["buy"] > 0 else 0.0
    people = [d for d in per.values() if not d["bot"]]
    thr = min(max(BUYER_MIN_USD, mc * BUYER_MC_FRACTION), BUYER_MAX_USD_THRESHOLD)
    acc = sorted((d for d in people if d["buy"] >= thr and d["sell"] <= BUYER_MAX_SELL_SHARE * d["buy"]),
                 key=lambda d: d["buy"] - d["sell"], reverse=True)
    top = sorted((d for d in people if d["buy"] > d["sell"]), key=lambda d: d["buy"] - d["sell"], reverse=True)
    return {"n": n, "span": (last - first) / 3600 if first and last > first else 0.0,
            "buy": sum(d["buy"] for d in people), "sell": sum(d["sell"] for d in people),
            "nb": sum(1 for d in people if d["buy"] > d["sell"]), "ns": sum(1 for d in people if d["sell"] > d["buy"]),
            "bots": sum(1 for d in per.values() if d["bot"]), "thr": thr, "acc": acc, "top": top[:10]}


def fmt_span(hours: float) -> str:
    return "less than an hour" if hours < 1 else f"~{hours:.0f}h" if hours < 48 else f"~{hours / 24:.0f}d"


def capture_buyers(key: str, chain: str, pair: dict, m: dict, b: dict, now: float) -> bool:
    """Look at who has been buying this coin and remember it. The wallets that are accumulating are followed as
    'wallet buys': if the coin then rises, they earn credit, and wallets with a good record are promoted."""
    b["cap_try"] = now
    try:
        trades = gecko_trades(chain, pair.get("pairAddress", ""))
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        b["cap_tries"] = b.get("cap_tries", 0) + 1
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status in (None, 429, 500, 502, 503, 504):
            bot_stats["gecko_busy"] += 1
        log.warning("could not look at the buyers of %s (%s)", key, status or "no answer")
        return False
    info = analyze_trades(chain, trades, pair.get("pairAddress", ""), key.split(":", 1)[1], m["mc"])
    b["cap"] = {"t": now, "n": info["n"], "span": round(info["span"], 1), "buy": round(info["buy"]),
                "sell": round(info["sell"]), "nb": info["nb"], "ns": info["ns"], "bots": info["bots"],
                "acc": [[d["w"], round(d["buy"]), round(d["sell"]), d["n"], d["px"]] for d in info["acc"][:6]]}
    b["cap_tries"] = 0
    for d in info["acc"][:BUYER_RECORDS_PER_COIN]:
        add_record("whale", key, chain, pair, m, "", now,
                   extra={"wallet": d["w"], "zone": True, "src": "bottom", "usd": round(d["buy"])})
        if d["w"] in state["watch"]:      # a wallet that is already on your list: that counts as a tracked buy
            buys = state["whale_buys"].setdefault(key, [])
            if not any(x[1] == d["w"] and now - x[0] < 3600 for x in buys):
                buys.append([now, d["w"], d["buy"]])
    bot_stats["captured"] += 1
    return True


def smart_in_cap(b: dict) -> list[str]:
    """Wallets on your watch list (yours, or ones I promoted) that are among this coin's latest accumulators."""
    return [r[0] for r in (b.get("cap") or {}).get("acc", []) if r and r[0] in state["watch"]]


def also_buying(b: dict, wallet: str) -> list[str]:
    """Names of the OTHER bottomed coins this wallet is accumulating too (from the latest look at each coin's buyers)."""
    out = []
    for x in list(state["bottoms"].values()):
        if x is not b and bottom_active(x) and any(r and r[0] == wallet for r in (x.get("cap") or {}).get("acc", [])):
            out.append(str(x.get("name", "?")))
    return out


def buyers_lines(b: dict) -> str:
    cap = b.get("cap")
    if not cap:
        return ""
    L = [f"🧠 Who's buying (latest {cap.get('n', 0)} trades, spanning {fmt_span(cap.get('span', 0))}): "
         f"*{cap.get('nb', 0)}* wallets net-buying vs {cap.get('ns', 0)} net-selling - bought "
         f"{fmt_usd(cap.get('buy', 0))}, sold {fmt_usd(cap.get('sell', 0))}"]
    for w, buy, sell, _, _ in (cap.get("acc") or [])[:3]:
        star = " ⭐" if w in state["watch"] else ""
        more = also_buying(b, w)
        L.append(f"   • `{w}` bought {fmt_usd(buy)}" + (f", sold {fmt_usd(sell)}" if sell else "") + star
                 + (" · also accumulating " + ", ".join("$" + md_escape(n) for n in more[:3]) if more else ""))
    if cap.get("acc"):
        L.append("   (/watch <address> <name> follows one of them)")
    else:
        L.append("   No single wallet stands out as accumulating.")
    return "\n".join(L) + "\n"


def bottom_metrics(key: str, pair: dict, b: dict):
    """The usual numbers, with Sparsity's 30-day high as the 'peak' when it is higher than what I have seen."""
    m = compute_metrics(key, pair)
    if not m:
        return None
    hi = float(b.get("hi") or 0)
    if hi > m["peak"]:
        m["peak"] = hi
        m["retrace"] = (hi - m["mc"]) / hi * 100
    return m


def build_bottom_message(chain: str, pair: dict, m: dict, b: dict, held: dict, smart: list[str], title: str) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(base.get("symbol", "?"))
    signs = int(b.get("signs") or 0)
    marks = " · ".join(f"{step} {'✅' if i < signs else '▫️'}" for i, step in enumerate(SPARSITY_STEPS))
    L = [f"*{title}: ${sym}*  |  {CHAINS[chain]['name']}", "",
         f"🧭 Sparsity: *{signs} of 3* bottom signs\n   {marks}",
         f"📉 Down *{m['retrace']:.0f}%* from its 30-day high of {fmt_usd(m['peak'])}"
         + (f" (its low was {b['fall'] * 100:.0f}% under the high)" if b.get("fall") else ""),
         f"💰 Market cap: *{fmt_usd(m['mc'])}*"
         + (f" · {b['above_low']:.0f}% above its low of {fmt_usd(b['low'])}" + (f" ({b['low_day']})" if b.get("low_day") else "")
            if b.get("low") and b.get("above_low") is not None else " · right at its low" if b.get("above_low") == 0 else ""),
         f"💧 Liquidity: {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC) | 📊 24h volume: {fmt_usd(m['vol24'])} "
         f"({m['vol_ratio'] * 100:.0f}% of MC)",
         f"👥 24h txns: {m['traders']:,} | ⏱ Pair age: {fmt_age(m['age_h'])}"]
    body = "\n".join(L) + "\n" + buyers_lines(b)
    if smart:
        body += "⭐ Wallets on your list are accumulating it: " + ", ".join(
            "*" + md_escape(wallet_label(w)) + "*" for w in smart) + "\n"
    body += wallets_line(held)
    if chain == "robinhood":
        body += ("⚠️ Robinhood Chain is brand new: many of its wallets are airdrop-farming bots, so wallet signals are "
                 "weaker here, and DexScreener's numbers can jump around.\n")
    body += (f"📌 Tracking this for {BOTTOM_TRACK_HOURS / 24:g} days (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). "
             "A base forming is a shortlist, not a buy signal on its own.\n"
             f"ℹ️ Sparsity data through {md_escape(b.get('through') or b.get('upd') or '?')}.\n\n"
             f"`{addr}`\n\n"
             f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', addr)}) | [Sparsity]({SPARSITY_URL})")
    return body


def following_coin(key: str) -> bool:
    """Is a call or an experiment (not just a wallet buy) already following this coin?"""
    return any(r["status"] == "open" and r["key"] == key and r["kind"] in ("bottom", "bshadow", "shadow", "call", "narrative", "wallet")
               for r in state["records"])


def drop_quiet_follow(key: str, now: float):
    """The coin just got a real alert: its quiet follow-up is closed and not counted, so it is not counted twice."""
    for r in open_records(key):
        if r["kind"] == "bshadow":
            r["status"], r["t1"], r["void"] = "expired", now, True


def maybe_add_bottom_shadow(key: str, chain: str, pair: dict, m: dict, b: dict, now: float):
    """Follow a coin quietly (2 of 3 signs, or an alert that was held back) so the report can compare it with the alerts."""
    if following_coin(key) or now - state["shadow_cd"].get("b:" + key, 0) < SHADOW_COOLDOWN:
        return
    if len(open_records(kind="bshadow")) >= MAX_BOTTOM_SHADOWS:
        return
    state["shadow_cd"]["b:" + key] = now
    add_record("bshadow", key, chain, pair, m, "", now, extra={"signs": b.get("signs"), "th": BOTTOM_TRACK_HOURS})


def capture_due(b: dict, now: float) -> bool:
    """Is it time to look at who is buying this coin (never looked at, or the last look is old), and has the
    last attempt not just failed twice?"""
    if not BUYER_CAPTURE or now - (b.get("cap") or {}).get("t", 0) <= BUYER_RECAPTURE_HOURS * 3600:
        return False
    return b.get("cap_tries", 0) < 2 or now - b.get("cap_try", 0) > 6 * 3600


def maybe_bottom(key: str, chain: str, pair: dict, b: dict, now: float, caps: list) -> bool:
    """Runs on every scan for a coin Sparsity currently lists with 2+ signs: looks at who is buying it (a couple
    of coins per scan), sends the alert at 3 of 3 (or 2 of 3 when a wallet on your list is accumulating it), and
    otherwise just follows the coin quietly."""
    signs = int(b.get("signs") or 0)
    if signs < BOTTOM_FOLLOW_SIGNS or num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD:
        return False
    m = bottom_metrics(key, pair, b)
    if not m:
        return False
    cooling = now - b.get("alerted", 0) < BOTTOM_COOLDOWN_DAYS * 86400
    if b.get("cap_tries", 0) >= 2 and now - b.get("cap_try", 0) > 6 * 3600:
        b["cap_tries"] = 0                        # it has been a while since it last failed: try again
    if capture_due(b, now):
        # coins with all the signs go first: a coin with 2 of 3 waits while one with 3 of 3 is still waiting
        queue_ahead = signs < BOTTOM_MIN_SIGNS and any(
            x is not b and bottom_active(x) and int(x.get("signs") or 0) >= BOTTOM_MIN_SIGNS and capture_due(x, now)
            for x in state["bottoms"].values())
        if caps[0] > 0 and not queue_ahead:
            caps[0] -= 1
            capture_buyers(key, chain, pair, m, b, now)
        elif BOTTOM_ALERTS and not cooling and signs >= BOTTOM_MIN_SIGNS:
            return False                          # this coin's turn comes on a later scan
    smart = smart_in_cap(b)
    stamps = [t for t in state["tuning"].get("bottom_ts", []) if now - t < 86400]
    state["tuning"]["bottom_ts"] = stamps
    if (BOTTOM_ALERTS and not cooling and (signs >= BOTTOM_MIN_SIGNS or smart) and len(stamps) < BOTTOM_MAX_PER_DAY):
        if not confirmed_twice("bottom", key, m["mc"], now):
            return False
        held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
        title = "🧭 BOTTOM SIGNAL" if signs >= BOTTOM_MIN_SIGNS else "🧭 BOTTOM + WALLET YOU TRUST"
        if not send_telegram(build_bottom_message(chain, pair, m, b, held, smart, title)):
            return False
        b["alerted"] = now
        stamps.append(now)
        drop_quiet_follow(key, now)
        sig = signals_for(m, len(held["in"]))
        add_record("bottom", key, chain, pair, m, "", now,
                   extra={"signs": signs, "th": BOTTOM_TRACK_HOURS, "sig": sig, "smart": len(smart)})
        bot_stats["bottom"] += 1
        log.info("BOTTOM alert for %s (%d of 3 signs)", key, signs)
        return True
    maybe_add_bottom_shadow(key, chain, pair, m, b, now)
    return False


# --- smart money: which wallets have a good record? ---
def wallet_scores() -> dict[str, dict]:
    """Per wallet, from the finished 'wallet buy' records: how many of its followed buys were GOOD (the coin
    reached SMART_GOOD_MULT x, or hit the win mark). One count per coin."""
    out: dict[str, dict] = {}
    seen = set()
    for r in state["records"]:
        w = r.get("wallet")
        if r["kind"] != "whale" or r["status"] == "open" or r.get("void") or not w or (w, r["key"]) in seen:
            continue
        seen.add((w, r["key"]))
        d = out.setdefault(w, {"n": 0, "good": 0, "coins": [], "chain": r["chain"]})
        good = r["status"] == "win" or r.get("best", 0) >= SMART_GOOD_MULT
        d["n"] += 1
        d["good"] += bool(good)
        d["coins"].append((r["sym"], r["chain"], bool(good)))
    return out


def promote_smart_wallets():
    """Wallets that keep buying coins shortly before they rise join the watch list on their own."""
    if not SMART_AUTO:
        return
    removed = set(state["tuning"].get("watch_removed") or [])
    auto_n = sum(1 for d in state["watch"].values() if d.get("auto"))
    for w, d in sorted(wallet_scores().items(), key=lambda kv: (-kv[1]["good"], kv[1]["n"])):
        if (auto_n >= SMART_MAX_AUTO or d["n"] < SMART_MIN_BUYS or d["good"] < SMART_MIN_GOOD
                or d["good"] / d["n"] < SMART_MIN_RATE):
            continue
        kind = wallet_kind(w)
        key = w.lower() if kind == "evm" else w
        if not kind or key in state["watch"] or key in removed or key in EVM_WHALES:
            continue
        info = state["watch"].setdefault(key, {"kind": kind, "t": time.time()})
        info.update(name=f"smart-{short(key)[:4]}{short(key)[-4:]}", auto=True, good=d["good"], n=d["n"])
        if kind == "solana":
            _sweep["now"] = True
        auto_n += 1
        bot_stats["smart"] += 1
        names = ", ".join(("✅" if g else "▫️") + "$" + md_escape(s) for s, _, g in d["coins"][:6])
        send_telegram(
            f"🧠 *NEW SMART WALLET FOUND*\n\n`{key}`\n"
            f"{d['good']} of its {d['n']} followed buys were good (the coin rose {SMART_GOOD_MULT:g}x or more afterwards): {names}\n"
            f"I'm now watching it like the wallets you add yourself: you'll see it on alerts when it holds a coin, "
            f"and an alert can fire when it accumulates a bottomed coin. Remove it any time with /unwatch.\n"
            f"[Wallet]({link(d['chain'] if d['chain'] in CHAINS else ('solana' if kind == 'solana' else 'ethereum'), 'addr', key)})")
        log.info("smart wallet promoted: %s (%d/%d)", short(key), d["good"], d["n"])


# --- chat commands for all of this ---
def resolve_coin_ref(ref: str):
    """'<address>' or 'base:0x...' -> (chain, address, None), or (None, None, a message explaining the problem)."""
    ref = ref.strip()
    if ":" in ref:
        t = parse_token_ref(ref)
        chain, addr = t if t else (None, ref)
    elif EVM_ADDR.match(ref):
        chain, addr = find_chain_for(ref), ref.lower()
    elif SOL_ADDR.match(ref):
        chain, addr = "solana", ref
    else:
        return None, None, "That doesn't look like a coin address. Example: /check So11111111111111111111111111111111111111112"
    if not chain:
        return None, None, ("I couldn't find that address on Ethereum, BNB Chain, Base or Robinhood Chain. "
                            "If you know the chain, write it like this: /check base:0x...")
    return chain, addr, None


def cmd_bottoms() -> str:
    if not SPARSITY_FEED:
        return "The bottomed-coin list is switched off (SPARSITY_FEED=false)."
    now = time.time()
    items = [(k, b) for k, b in list(state["bottoms"].items()) if bottom_active(b)]
    if not items:
        if _sp["fails"]:
            return f"I can't read Sparsity's bottom list right now: {_sp['err']}. I keep trying."
        return "No coin on Sparsity's list shows 2 or 3 bottom signs yet (or I haven't read the page yet)."
    items.sort(key=lambda kb: (-int(kb[1].get("signs") or 0), kb[1].get("name", "")))
    three = [x for x in items if x[1]["signs"] >= 3]
    two = [x for x in items if x[1]["signs"] < 3]
    L = [f"🧭 *Coins showing bottom signs*  (Sparsity: {md_escape(_sp['updated'] or '?')}, "
         f"read {ago(now - _sp['last_ok'])} ago)" if _sp["last_ok"] else "🧭 *Coins showing bottom signs*"]
    if three:
        L.append("\n*All 3 signs* (sellers capitulated, the low is holding, demand returning):")
    for i, (k, b) in enumerate(three[:12], 1):
        chain, ca = b["chain"], b["ca"]
        bits = [f"${md_escape(b.get('name', '?'))}", CHAINS[chain]["name"]]
        if b.get("mc"):
            bits.append("MC " + fmt_usd(b["mc"]))
        if b.get("above_low") is not None and b.get("low"):
            bits.append(f"{b['above_low']:.0f}% above its low")
        mark = " ✅alerted" if b.get("alerted") else ""
        L.append(f"{i}. " + " · ".join(bits) + mark + f"\n    `{ca}`  [chart](https://dexscreener.com/{chain}/{ca})")
    if two:
        L.append("\n*2 of 3* (followed quietly): " + ", ".join("$" + md_escape(b.get("name", "?")) for _, b in two[:25]))
    L.append("\nA base forming is a shortlist, not a buy signal. /buyers <address> shows who is buying a coin.")
    return "\n".join(L)


def cmd_smart() -> str:
    scores = wallet_scores()
    auto = [(w, d) for w, d in state["watch"].items() if d.get("auto")]
    open_n = len(open_records(kind="whale"))
    L = ["🧠 *Smart-money tracker*", "",
         f"I look at who is buying every coin on the bottom list, follow the biggest accumulators for {TRACK_HOURS:.0f}h, "
         f"and promote wallets with at least {SMART_MIN_GOOD} good buys out of {SMART_MIN_BUYS}+ "
         f"(good = the coin rose {SMART_GOOD_MULT:g}x or more).",
         f"Following {open_n} wallet buys right now · {len(scores)} wallets have finished results · "
         f"{len(auto)} promoted so far."]
    if auto:
        L.append("\n*Found by results (now on your watch list):*")
        for w, d in auto[:10]:
            L.append(f"• *{md_escape(wallet_label(w))}* {d.get('good', '?')}/{d.get('n', '?')} good\n   `{w}`")
    cand = sorted(((w, d) for w, d in scores.items() if d["good"] >= 1 and w not in state["watch"]),
                  key=lambda kv: (-kv[1]["good"], kv[1]["n"]))[:6]
    if cand:
        L.append("\n*Candidates* (a good buy or more, not enough history yet):")
        for w, d in cand:
            L.append(f"• {d['good']}/{d['n']} good: " + ", ".join("$" + md_escape(s) for s, _, g in d["coins"][:4] if g)
                     + f"\n   `{w}`")
    if not auto and not cand:
        L.append("\nNo wallet has earned a place yet. Results come in a few days after the first bottom alerts.")
    multi: dict[str, list] = {}
    for x in list(state["bottoms"].values()):
        if bottom_active(x):
            for r in (x.get("cap") or {}).get("acc", []):
                if r:
                    multi.setdefault(r[0], []).append((str(x.get("name", "?")), r[1]))
    multi = {w: v for w, v in multi.items() if len(v) >= 2}
    if multi:
        L.append("\n*Buying more than one bottomed coin right now* (worth a look, not proof):")
        for w, v in sorted(multi.items(), key=lambda kv: -sum(a for _, a in kv[1]))[:5]:
            L.append(f"• `{w}`{' ⭐' if w in state['watch'] else ''}\n   " + ", ".join(f"${md_escape(n)} ({fmt_usd(a)})" for n, a in v[:4]))
    L.append("\nWallets you add yourself: /watch <address> <name>. Who is buying a coin right now: /buyers <address>.")
    return "\n".join(L)


def cmd_buyers(ref: str) -> str:
    chain, addr, err = resolve_coin_ref(ref)
    if err:
        return err.replace("/check", "/buyers")
    want = norm_addr(chain, addr)
    try:
        pairs = [p for p in fetch_pairs(chain, [addr]) if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == want]
    except requests.RequestException:
        return "DexScreener is busy right now - try again in a minute."
    pair = best_live_pair(pairs)
    if not pair:
        return f"I couldn't find a live trading pool for that address on {CHAINS[chain]['name']}."
    key = sk(chain, addr)
    mc = pair_mc(key, pair)
    try:
        trades = gecko_trades(chain, pair.get("pairAddress", ""))
    except (requests.RequestException, ValueError) as e:
        return ("GeckoTerminal (where I read recent trades) is busy or doesn't have this pool right now - "
                f"try again in a minute. ({getattr(getattr(e, 'response', None), 'status_code', None) or 'no answer'})")
    info = analyze_trades(chain, trades, pair.get("pairAddress", ""), addr, mc)
    sym = md_escape(pair.get("baseToken", {}).get("symbol", "?"))
    if not info["n"]:
        return f"🔎 *${sym}* | {CHAINS[chain]['name']}\nNo recent trades above ${BUYER_TRADE_FLOOR_USD:g} came back."
    L = [f"🔎 *Who is buying ${sym}*  |  {CHAINS[chain]['name']}",
         f"Latest {info['n']} trades (above ${BUYER_TRADE_FLOOR_USD:g}), spanning {fmt_span(info['span'])}: "
         f"*{info['nb']}* wallets net-buying vs {info['ns']} net-selling - bought {fmt_usd(info['buy'])}, "
         f"sold {fmt_usd(info['sell'])} (net {'+' if info['buy'] >= info['sell'] else '-'}{fmt_usd(abs(info['buy'] - info['sell']))}).",
         f"Set aside as bots/routers: {info['bots']} address(es). Accumulating = bought at least {fmt_usd(info['thr'])} and sold under "
         f"{BUYER_MAX_SELL_SHARE * 100:.0f}% of it.", ""]
    if not info["top"]:
        L.append("No wallet is net-buying in this window.")
    for i, d in enumerate(info["top"][:8], 1):
        star = " ⭐" if d["w"] in state["watch"] else ""
        acc = " 🟢accumulating" if d in info["acc"] else ""
        L.append(f"{i}. `{d['w']}`{star}{acc}\n    bought {fmt_usd(d['buy'])} in {d['n']} trade{'s' if d['n'] != 1 else ''}"
                 + (f", sold {fmt_usd(d['sell'])}" if d["sell"] else "")
                 + (f", avg price ${d['px']:.6g}" if d["px"] else "")
                 + f"  [wallet]({link(chain, 'addr', d['w'])})")
    L.append("\nThese are wallets that bought recently - not proof they are smart. /watch <address> <name> follows one; "
             "the bot scores wallets it follows by what happens next.")
    return "\n".join(L)


def sparsity_status_line() -> str:
    if _sp["last_ok"]:
        s = (f"🧭 Bottom list (Sparsity, data from {md_escape(_sp['updated'] or '?')}): {_sp['n']} coins read, "
             f"{_sp['n3']} with all 3 signs, I follow {_sp['followed']}; alerts since this copy started: {bot_stats['bottom']}.")
        return s + (f" ⚠️ The last try failed ({_sp['err']})." if _sp["fails"] else "")
    if _sp["fails"]:
        return f"🧭 Bottom list (Sparsity): ⚠️ not readable yet ({_sp['err']}); I keep trying."
    return "🧭 Bottom list (Sparsity): not read yet."


def bottom_report_lines() -> list[str]:
    """The bottom-signal part of the learning report: do Sparsity's bottoms (and the wallets behind them) pay off?"""
    bt, bsh = closed_records("bottom"), closed_records("bshadow")
    out: list[str] = []
    if bt:
        s = summarize(bt)
        out += ["", f"🧭 Bottom alerts finished: *{s['n']}*  ✅ {s['w']} wins | 🛑 {s['s']} stopped | ⏱ {s['e']} flat "
                    f"→ win rate *{s['wr']:.0%}*, average best move *{s['avg_best']:.2f}x* "
                    f"(followed {BOTTOM_TRACK_HOURS / 24:g} days each)"]
        by_chain: dict[str, list[dict]] = {}
        for r in bt:
            by_chain.setdefault(r["chain"], []).append(r)
        if len(by_chain) > 1:
            out.append("   By chain: " + ", ".join(f"{CHAINS[ch]['name']} {summarize(rs)['w']}/{len(rs)}"
                                                    for ch, rs in by_chain.items()))
        backed = [r for r in bt if r.get("smart")]
        if backed and len(backed) < len(bt):
            a, b2 = summarize(backed), summarize([r for r in bt if not r.get("smart")])
            out.append(f"   With a trusted wallet accumulating: {a['w']}/{a['n']} won | without: {b2['w']}/{b2['n']}")
    if bsh:
        s2 = summarize(bsh)
        out.append(("" if bt else "\n") + f"   Bottom coins followed quietly (2 of 3 signs): {s2['w']}/{s2['n']} won")
    auto = sum(1 for d in state["watch"].values() if d.get("auto"))
    if auto:
        out.append(f"🧠 Smart wallets found by results and now watched: {auto}")
    return out


# ----------------------------------------------------------------------------
# CHAT COMMANDS (only from your own chat): /watch /unwatch /wallets /check /status /report /help
# ----------------------------------------------------------------------------
HELP_TEXT = (
    "🤖 *What you can tell me*\n\n"
    "/watch `<wallet address>` `[name]` - add a wallet you trust. I'll say which of your wallets are still in each "
    "coin I flag, add the coins they hold to my watch list, and tell you when one buys or dumps a coin.\n"
    "/unwatch `<name or address>` - stop watching it\n"
    "/wallets - your watched wallets (the ones marked 🧠 were found by the bot from results)\n"
    "/bottoms - coins Sparsity shows with bottom signs (I alert on all 3, and look at who is buying them)\n"
    "/buyers `<coin address>` - who has been buying a coin recently (accumulating wallets, bots set aside)\n"
    "/smart - the smart-money tracker: wallets that earned a place by buying before coins rose\n"
    "/check `<coin address>` - look at any coin right now (for an Ethereum-style address I find the chain; "
    "or write it like base:0x...)\n"
    "/status - the status check-in, right now\n"
    "/report - the learning report, right now\n\n"
    "You can also just paste a coin's contract address and I'll check it.")


def find_chain_for(addr: str) -> str | None:
    """Which of our EVM chains does this coin trade on? (the deepest pool wins)"""
    return lookup_chain(addr)[0]


def cmd_check(ref: str) -> str:
    chain, addr, err = resolve_coin_ref(ref)
    if err:
        return err
    ctx = token_context(chain, addr)
    if not ctx:
        return (f"I couldn't find a live trading pool for that address on {CHAINS[chain]['name']}. "
                "If it is a wallet, add it with /watch instead.")
    pair, m, key = ctx["pair"], ctx["m"], ctx["key"]
    passed, m, fails = evaluate(key, pair)
    held = watch_holders(chain, addr, num(pair, "priceUsd"))
    sig = signals_for(m, len(held["in"]))
    sym = md_escape(pair.get("baseToken", {}).get("symbol", "?"))
    peak_note = "estimated from its price history" if key in state["backfilled"] else "not looked up - treat with care"
    verdict = ("✅ It passes every rule right now - this is what a re-run setup looks like." if passed
               else "✗ Not a re-run setup right now: " + "; ".join(fail_label(c) for c, _ in fails) + ".")
    return (
        f"🔍 *${sym}*  |  {CHAINS[chain]['name']}\n`{addr}`\n\n"
        f"💰 Market cap: *{fmt_usd(m['mc'])}*  |  📈 Past high: *{fmt_usd(m['peak'])}* ({peak_note})\n"
        f"📉 Down *{m['retrace']:.0f}%* from the high (my dip range is {T['retrace_min']:.0f}-{T['retrace_max']:.0f}%)\n"
        f"💧 Liquidity: {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC)  |  "
        f"📊 24h volume: {fmt_usd(m['vol24'])} ({m['vol_ratio'] * 100:.0f}% of MC)\n"
        f"👥 24h txns: {m['traders']:,}  |  ⏱ Pair age: {fmt_age(m['age_h'])}\n"
        f"{verdict}\n{signals_text(sig)}{wallets_line(held)}"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
        f"[Explorer]({link(chain, 'token', pair.get('baseToken', {}).get('address', addr))})")


def cmd_watch(args: list[str]) -> str:
    addr = next((a for a in args if wallet_kind(a)), None)
    if not addr:
        return "Send it like this: /watch <wallet address> <a name you choose>\nExample: /watch 7xKX...AsU Whale1"
    name = " ".join(a for a in args if a != addr).strip()
    new = (addr.lower() if wallet_kind(addr) == "evm" else addr) not in state["watch"]
    key = add_watch(addr, name)
    kind = state["watch"][key]["kind"]
    if kind == "solana":
        _sweep["now"] = True
        how = ("I'll look inside it in the next minute or so, then every ~10 minutes. Coins it holds join my watch list, "
               f"and I'll message you when it buys or dumps a position worth ${WATCH_MIN_MOVE_USD:,.0f} or more. "
               "Alerts and check-ins will show whether it's still in each coin.")
    else:
        how = ("This is an Ethereum-style wallet (the same address works on Ethereum, Base and BNB Chain). "
               "Alerts and check-ins will show whether it holds each coin I flag. I can't list everything it holds "
               "for free, so live buy/sell messages for it only come if you have also added it to your Alchemy "
               "webhook (or the `EVM_WHALES` setting).")
    return (f"✅ {'Now watching' if new else 'Already watching'} *{md_escape(wallet_label(key))}* "
            f"(`{short(key)}`, {'Solana' if kind == 'solana' else 'Ethereum-style'} wallet).\n{how}")


def cmd_unwatch(args: list[str]) -> str:
    key = find_watch(" ".join(args))
    if not key:
        return "I don't have a wallet like that. Send /wallets to see the list, then /unwatch <name or address>."
    label = wallet_label(key)
    remove_watch(key)
    return f"🗑 Stopped watching *{md_escape(label)}*."


def cmd_wallets() -> str:
    ws = list(state["watch"].items())
    if not ws:
        return ("You're not watching any wallets yet. Add one you trust:\n/watch <wallet address> <a name you choose>")
    L = [f"👛 *Your watched wallets ({len(ws)})*"]
    for i, (w, d) in enumerate(ws, 1):
        snap = _snaps.get(w)
        if snap:
            info = f"holds {len(snap[1])} coin{'' if len(snap[1]) == 1 else 's'} (looked {ago(time.time() - snap[0])} ago)"
        elif d.get("kind") != "solana":
            info = "checked coin by coin"
        elif _rpc_fail.get(SOLANA_RPC_URL):
            info = "couldn't look inside it yet - the Solana node link isn't answering (I keep trying)"
        else:
            info = "not looked at yet"
        chain = "solana" if d.get("kind") == "solana" else "ethereum"
        L.append(f"{i}. *{md_escape(wallet_label(w))}* · {'Solana' if chain == 'solana' else 'Ethereum-style'} · "
                 f"[{short(w)}]({link(chain, 'addr', w)}) · {info}" + (" · 🧠 found by results" if d.get("auto") else ""))
    return "\n".join(L)


def handle_command(text: str) -> str | None:
    """The reply for one chat message (None = nothing to say)."""
    parts = text.strip().split()
    if not parts:
        return None
    if not parts[0].startswith("/"):
        one = len(parts) == 1 and (wallet_kind(parts[0]) or (":" in parts[0] and parse_token_ref(parts[0])))
        return cmd_check(parts[0]) if one else None   # a pasted coin address is treated like /check
    cmd, args = parts[0][1:].split("@", 1)[0].lower(), parts[1:]
    if cmd in ("start", "help"):
        return HELP_TEXT
    if cmd == "status":
        return build_checkin()
    if cmd == "report":
        return build_report(suggestions(), [])
    if cmd == "watch":
        return cmd_watch(args)
    if cmd == "unwatch":
        return cmd_unwatch(args)
    if cmd == "wallets":
        return cmd_wallets()
    if cmd == "bottoms":
        return cmd_bottoms()
    if cmd == "smart":
        return cmd_smart()
    if cmd == "buyers":
        return cmd_buyers(args[0]) if args else "Send it like this: /buyers <coin contract address>"
    if cmd == "check":
        return cmd_check(args[0]) if args else "Send it like this: /check <coin contract address>"
    return "I don't know that command. Send /help to see what I understand."


def handle_update(u: dict):
    msg = u.get("message") or {}
    if str((msg.get("chat") or {}).get("id")) != str(TELEGRAM_CHAT_ID):
        return    # only the owner can give commands
    text = (msg.get("text") or "").strip()
    if not text:
        return
    try:
        reply = handle_command(text)
    except Exception:
        log.exception("chat command failed: %s", text.split()[0][:20])
        reply = "Sorry - that hit a snag on my side. Try again in a minute."
    if reply:
        send_telegram(reply[:3900])


def fetch_updates(offset: int | None, wait: int) -> list[dict]:
    kw = {"timeout": wait, "allowed_updates": ["message"]}
    if offset is not None:
        kw["offset"] = offset
    return tg_api("getUpdates", json=kw) or []


async def command_loop():
    """Listen for the owner's chat commands. (Telegram holds each request open up to 20 s until something arrives.)"""
    if not (COMMANDS and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    await asyncio.sleep(COMMAND_LOOP_START_DELAY)
    offset, first, fails = None, True, 0
    while True:
        started = time.time()
        try:
            ups = await asyncio.to_thread(fetch_updates, offset, 0 if first else 20)
            fails = 0
        except Exception as e:
            fails += 1
            status = getattr(getattr(e, "response", None), "status_code", None)
            if fails in (1, 10, 100):
                log.warning("Chat-command check failed (%s): %s", status or "error", scrub(e))
            await asyncio.sleep(15 if status == 409 else min(60, 5 * fails))   # 409 = another copy of the bot is listening
            continue
        for u in ups:
            offset = max(offset or 0, int(u.get("update_id", 0)) + 1)
            if first and time.time() - float((u.get("message") or {}).get("date") or 0) > 900:
                continue   # a message from before this start-up: not answered after the fact
            try:
                await asyncio.to_thread(handle_update, u)
            except Exception:
                log.exception("chat command error")
        first = False
        if not ups and time.time() - started < 2:
            await asyncio.sleep(2)   # never spin


# ----------------------------------------------------------------------------
# MEMORY BACKUP (Render's free disk is wiped on restart) -> pinned file in your Telegram chat
# ----------------------------------------------------------------------------
def tg_api(method: str, **kw):
    r = http.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}", timeout=30, **kw)
    r.raise_for_status()
    return r.json().get("result")


def core_hash() -> str:
    """Fingerprint of the things that matter for the memory file: which calls are open/closed, cooldowns,
    tuning, tracked wallets. Price ticks don't change it, so the memory isn't re-saved every few minutes."""
    core = [[(r["key"], r["kind"], r["status"], round(r["t0"])) for r in state["records"]],
            sorted(state["cooldowns"].items()), state["tuning"], sorted(state["wallets"]),
            sorted((w, d.get("name", "")) for w, d in state["watch"].items()),
            sorted((k, round(b.get("alerted", 0))) for k, b in state["bottoms"].items())]
    return hashlib.md5(json.dumps(core, sort_keys=True, default=str).encode()).hexdigest()


# --- Heartbeats: every running copy of the bot notes "I'm alive" in the pinned file's caption, so a
# --- second copy (e.g. two Render services using the same bot) can be spotted and reported.
HB_RE = re.compile(r"([0-9a-f]{6}),([\w.\-]*),(\d{9,11})")
_hb_seen: dict[str, tuple[str, float]] = {}   # copy id -> (host, last heartbeat) as last read from Telegram


def my_host() -> str:
    return PUBLIC_URL.split("://", 1)[-1] if PUBLIC_URL else "local"


def parse_heartbeats(caption: str) -> dict[str, tuple[str, float]]:
    return {i: (h, float(t)) for i, h, t in HB_RE.findall(caption or "")}


def backup_caption(others: dict | None = None) -> str:
    now = time.time()
    beats = {i: v for i, v in (others or {}).items() if i != INSTANCE_ID and now - v[1] < 3600}
    beats[INSTANCE_ID] = (my_host(), now)
    newest = sorted(beats.items(), key=lambda kv: -kv[1][1])[:6]
    return BACKUP_TAG + " | live: " + "; ".join(f"{i},{h},{int(t)}" for i, (h, t) in newest)


def backup_state_to_telegram(force: bool = False):
    """Keep ONE pinned file in the chat up to date. It is edited in place, so the chat doesn't fill up with
    'pinned a file' / 'pinned Deleted message' notices (a new file is only sent if the old one is gone)."""
    if not (TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or restore_pending:
        return
    now, b = time.time(), state["backup"]
    h, since = core_hash(), now - b.get("ts", 0)
    changed = h != b.get("hash")
    if not force and not ((changed and since >= BACKUP_MIN_GAP) or since >= BACKUP_REFRESH_SECONDS):
        return
    data, caption, old = json.dumps(state).encode(), backup_caption(_hb_seen), b.get("msg_id")
    if old:
        try:
            tg_api("editMessageMedia",
                   data={"chat_id": TELEGRAM_CHAT_ID, "message_id": old,
                         "media": json.dumps({"type": "document", "media": "attach://memory", "caption": caption})},
                   files={"memory": ("bot_memory.json", data)})
            b.update(ts=now, hash=h)
            return
        except requests.RequestException as e:
            body = getattr(getattr(e, "response", None), "text", "") or ""
            if "not modified" in body:  # identical content - nothing to do
                b.update(ts=now, hash=h)
                return
            log.info("Could not update the pinned memory file in place (%s) - sending a fresh one", scrub(body or e))
    msg = tg_api("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption,
                                       "disable_notification": "true"},
                 files={"document": ("bot_memory.json", data)})
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
    if not doc or not (pm.get("caption") or "").startswith(BACKUP_TAG):
        return False
    info = tg_api("getFile", json={"file_id": doc["file_id"]})
    r = http.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=30)
    r.raise_for_status()
    merge_state(r.json())
    state["backup"].update(msg_id=pm["message_id"], ts=time.time(), hash=core_hash())
    return True


def retry_restore():
    """If the memory couldn't be read at start-up (Telegram hiccup), keep trying - and don't overwrite it."""
    global restore_pending
    if not restore_pending:
        return
    try:
        if restore_from_telegram():
            log.info("Restored memory from Telegram backup on retry (%d tracked calls)", len(state["records"]))
        restore_pending = False
    except Exception as e:
        log.warning("Memory restore still failing: %s", scrub(e))


def heartbeat_check():
    """Note on the pinned file that this copy is alive, and warn if ANOTHER copy is alive too."""
    global _hb_seen
    if not (TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or restore_pending:
        return
    pm = (tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}).get("pinned_message") or {}
    caption = pm.get("caption") or ""
    if not pm.get("document") or not caption.startswith(BACKUP_TAG):
        return  # no memory file pinned yet (the first one appears after the first scan)
    now, b = time.time(), state["backup"]
    _hb_seen = parse_heartbeats(caption)
    b["msg_id"] = pm["message_id"]  # follow the file that is pinned right now
    others = {i: v for i, v in _hb_seen.items() if i != INSTANCE_ID and now - v[1] < 600}
    # a copy that is being replaced during an update disappears within a minute or two, so only
    # complain when another copy is still writing heartbeats well after this one has started
    if others and now - STARTED_AT > 900 and now - b.get("dup_warned", 0) > 6 * 3600:
        names = ", ".join(sorted({h or "?" for h, _ in others.values()}))
        if send_telegram(
                "⚠️ *TWO COPIES OF THE BOT ARE RUNNING*\n\n"
                f"This copy: {md_escape(my_host())}\nThe other copy: {md_escape(names)}\n\n"
                "Both use the same Telegram bot, so every alert arrives twice and they overwrite each other's "
                "memory. Fix: open dashboard.render.com, find the extra service (the one you are NOT keeping) "
                "and delete it. Two always-on free services also use up Render's free monthly hours twice as "
                "fast, and then Render switches them all off until next month."):
            b["dup_warned"] = now
    try:
        tg_api("editMessageCaption", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": pm["message_id"],
                                           "caption": backup_caption(_hb_seen)})
    except requests.RequestException as e:
        if "not modified" not in (getattr(getattr(e, "response", None), "text", "") or ""):
            raise


# ----------------------------------------------------------------------------
# MODULE 2 (shared): whale message + dedupe
# ----------------------------------------------------------------------------
def build_whale_message(s: dict, extra: str = "") -> str:
    chain, w = s["chain"], s["wallet"]
    emoji = "🟢" if s["side"] == "BUY" else "🔴"
    if s.get("src") == "watch":  # spotted by looking inside one of YOUR watched wallets
        head = (f"👛 *{md_escape(wallet_label(w))} " + ("BOUGHT" if s["side"] == "BUY" else "SOLD / MOVED")
                + f"* {emoji}  |  {CHAINS[chain]['name']}\n\n")
        value = "💵 Position now worth: " if s["side"] == "BUY" else "💵 Sold / moved about: "
    else:
        head, value = f"🐋 *WHALE {s['side']}* {emoji}  |  {CHAINS[chain]['name']}\n\n", "💵 Value: "
    # a finding from looking inside a wallet has no single transaction to link to, so no Tx link for those
    tx_link = "" if s.get("src") == "watch" else f"[Tx]({link(chain, 'tx', s['signature'])}) | "
    return (
        head
        + f"🪙 Token: *${md_escape(s['symbol'])}*\n"
        f"{value}*{fmt_usd(s['usd'])}*\n"
        f"👛 Wallet: `{short(w)}`\n"
        + extra + f"`{s['token']}`\n\n"
        f"[Wallet]({link(chain, 'addr', w)}) | "
        + tx_link
        + f"[DexScreener](https://dexscreener.com/{chain}/{s['token']}) | "
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
# STATUS CHECK-IN: proof of life + the coins that came closest to qualifying
# ----------------------------------------------------------------------------
def fail_label(code: str) -> str:
    return {
        "peak": f"peak never reached {fmt_usd(MIN_PEAK_MC)}",
        "peak_unknown": "past high not checked yet",
        "retrace_lo": f"not down {T['retrace_min']:.0f}% from its peak yet",
        "retrace_hi": f"down more than {T['retrace_max']:.0f}% from its peak",
        "liq": f"liquidity under {T['liq'] * 100:.0f}% of market cap",
        "vol": f"24h volume under {T['vol'] * 100:.0f}% of market cap",
        "traders": f"under {T['traders']:.0f} trades in 24h",
        "age": f"pair younger than {MIN_PAIR_AGE_HOURS}h",
        "old": f"older than {MAX_COIN_AGE_DAYS:g} days",
        "nomc": "no market-cap data",
    }.get(code, code)


def ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{seconds / 3600:.1f} h"


def closest_coins(rows: list[dict], n: int = 5) -> list[dict]:
    """The coins that came nearest to passing every filter: fewest broken rules first, then the ones that
    did reach the minimum peak; among equals the NEWER coin comes first, then the ones nearest the dip range."""
    def off_band(r: dict) -> float:
        lo, hi = T["retrace_min"], T["retrace_max"]
        return 0.0 if lo <= r["retrace"] <= hi else min(abs(r["retrace"] - lo), abs(r["retrace"] - hi))
    def peak_problem(r: dict) -> bool:
        return "peak" in r["fails"] or "peak_unknown" in r["fails"]
    def age_rank(r: dict) -> int:      # 0 = under 2 weeks old ... 3 = over 6 months old
        d = r.get("age_h", 0) / 24
        return 0 if d < 14 else 1 if d < 60 else 2 if d < 180 else 3
    return sorted(rows, key=lambda r: (len(r["fails"]), peak_problem(r), age_rank(r), off_band(r), -r["peak"]))[:n]


def build_checkin(with_wallets: bool = True) -> str:
    """with_wallets=False (the web page) uses only what is already known about the watched wallets - no lookups."""
    now, s = time.time(), last_scan
    L = [f"🩺 *BOT CHECK-IN*  (v{BOT_VERSION})", ""]
    if not s:
        L.append("✅ I'm running, but my first scan hasn't finished yet.")
    else:
        rows = s["rows"]
        per_chain = Counter(r["chain"] for r in rows)
        chains = ", ".join(f"{CHAINS[c]['name']} {n}" for c, n in per_chain.most_common())
        L.append(f"✅ Running. My last scan was {ago(now - s['t'])} ago.")
        L.append(f"🔎 Pool: {s['pool']} coins found by auto-discovery; {len(rows)} were large enough to "
                 "examine closely" + (f" ({chains})." if chains else "."))
        L.append(f"📚 Past highs looked up so far: {len(state['backfilled'])} coins.")
        if bot_stats["old"]:
            L.append(f"📅 Since I started I skipped {bot_stats['old']} coin(s) older than {MAX_COIN_AGE_DAYS:g} days.")
        if s.get("errors"):
            L.append(f"⚠️ {s['errors']} price request(s) were rate-limited in that scan (normal on a free server; "
                     "I retry every minute).")
        if bot_stats["gecko_busy"]:
            L.append(f"⚠️ The coin-list / price-history service (GeckoTerminal) was busy {bot_stats['gecko_busy']} "
                     "time(s) since I started. That only slows down finding coins and their past highs; I keep retrying.")
        best = closest_coins(rows)
        if best:
            L += ["", "🎯 *Closest to qualifying right now* (newer coins first when two are equally close):"]
            for i, r in enumerate(best, 1):
                why = "✅ passes every filter" if not r["fails"] else "✗ " + "; ".join(fail_label(c) for c in r["fails"])
                chart = f"  [chart](https://dexscreener.com/{r['chain']}/{r['pair']})" if r["pair"] else ""
                addr = r["key"].split(":", 1)[1]
                age = f", {fmt_age(r['age_h'])} old" if r.get("age_h") else ""
                clues = " ".join(SIGNAL_LABELS[x].split()[0] for x in r.get("sig", []))
                L.append(f"{i}. ${md_escape(r['sym'])} ({CHAINS[r['chain']]['name']}{age}): market cap {fmt_usd(r['mc'])}, "
                         f"peak {fmt_usd(r['peak'])}, down {r['retrace']:.0f}%" + (f"  {clues}" if clues else ""))
                L.append(f"    {why}{chart}")
                if state["watch"]:
                    wl = wallets_line(watch_holders(r["chain"], addr, r.get("price", 0.0), use_rpc=with_wallets)).strip()
                    if wl:
                        L.append("    " + wl)
                L.append(f"    `{addr}`")
            if any(r.get("sig") for r in best):
                L.append("Clues: 🔥 volume picking up · 🟢 more buyers than sellers · ↗️ price turning up · 🆕 young coin")
        elif s["pool"]:
            L += ["", "No coin in the pool is large enough to examine closely yet - discovery keeps adding more."]
        if s["fails"]:
            top = sorted(s["fails"].items(), key=lambda kv: -kv[1])[:5]
            L += ["", "📋 *Why coins were skipped (how many broke each rule):*",
                  "; ".join(f"{fail_label(c)} ×{n}" for c, n in top)]
    if SPARSITY_FEED:
        L += ["", sparsity_status_line()]
    sent = ", ".join(f"{n} {k}" for k, n in (("re-run", bot_stats["rerun"]), ("narrative", bot_stats["narrative"]),
                                              ("wallet-backed", bot_stats["walletdip"]), ("bottom", bot_stats["bottom"]),
                                              ("whale", bot_stats["whale"])))
    L += ["", f"⚙️ Rules in use: past high of at least {fmt_usd(MIN_PEAK_MC)}, now down {T['retrace_min']:.0f}-"
              f"{T['retrace_max']:.0f}% from it, liquidity at least {T['liq'] * 100:.0f}% and 24h volume at least "
              f"{T['vol'] * 100:.0f}% of market cap, {T['traders']:.0f}+ trades in 24h, pair at least "
              f"{MIN_PAIR_AGE_HOURS}h old" + (f" and at most {MAX_COIN_AGE_DAYS:g} days old." if MAX_COIN_AGE_DAYS > 0 else "."),
          f"📣 Alerts sent since this copy started: {sent}.",
          f"👀 Following now: {len(open_records(kind='call'))} calls, {len(open_records(kind='narrative'))} narrative, "
          f"{len(open_records(kind='wallet'))} wallet-backed, {len(open_records(kind='bottom'))} bottom, "
          f"{len(open_records(kind='shadow')) + len(open_records(kind='bshadow'))} experiments.",
          (f"👛 Watching {len(state['watch'])} wallet(s) - /wallets to see them." if state["watch"]
           else "👛 Tip: add wallets you trust with /watch <wallet address> and I'll show whether they're still in each coin."),
          "ℹ️ No alert just means nothing passed ALL of your filters at once - that is by design.",
          "", f"copy {INSTANCE_ID} · {md_escape(my_host())} · up {ago(now - STARTED_AT)}"]
    return "\n".join(L)[:3900]


_next_checkin = STARTED_AT + FIRST_CHECKIN_MINUTES * 60


def maybe_checkin():
    """Send the status message when it's due: first a few minutes after start-up, then every CHECKIN_HOURS."""
    global _next_checkin
    if CHECKIN_HOURS <= 0 or time.time() < _next_checkin or not last_scan:
        return
    ok = send_telegram(build_checkin())
    _next_checkin = time.time() + (CHECKIN_HOURS * 3600 if ok else 600)


_last_beat = 0.0


def housekeeping():
    """Runs after every scan: memory-restore retry, double-copy check, status check-in."""
    global _last_beat
    if time.time() - _last_beat >= HEARTBEAT_SECONDS:
        _last_beat = time.time()
        retry_restore()
        try:
            heartbeat_check()
        except Exception as e:
            log.warning("heartbeat check failed: %s", scrub(e))
    maybe_checkin()


# ----------------------------------------------------------------------------
# STAYING AWAKE: Render's free plan sleeps after 15 minutes without incoming web traffic
# ----------------------------------------------------------------------------
keepalive = {"pings": 0, "ok": 0, "last_ok": 0.0}


def ping_self() -> bool:
    """Visit our own public web address - to Render that counts as incoming traffic, so it never goes to sleep."""
    keepalive["pings"] += 1
    try:
        r = http.get(PUBLIC_URL + "/health", timeout=20)
        if r.status_code < 500:
            keepalive["ok"] += 1
            keepalive["last_ok"] = time.time()
            return True
        log.warning("stay-awake ping got HTTP %s", r.status_code)
    except requests.RequestException as e:
        log.warning("stay-awake ping failed: %s", scrub(e))
    return False


async def keepalive_loop():
    if not PUBLIC_URL:
        log.info("No public web address found (RENDER_EXTERNAL_URL / PUBLIC_URL) - stay-awake pings are off")
        return
    await asyncio.sleep(KEEPALIVE_START_DELAY)
    while True:
        ok = await asyncio.to_thread(ping_self)
        if keepalive["pings"] % 15 == 1 or not ok:  # roughly once an hour when all is well
            log.info("Stay-awake ping %s (%d sent) -> %s", "OK" if ok else "FAILED", keepalive["pings"], PUBLIC_URL)
        await asyncio.sleep(KEEPALIVE_SECONDS)


class _HideHealthVisits(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # keep Render's log readable
        return "/health" not in record.getMessage()


logging.getLogger("uvicorn.access").addFilter(_HideHealthVisits())


# ----------------------------------------------------------------------------
# FASTAPI APP
# ----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(load_state)
    log.info("Bot v%s copy %s starting at %s", BOT_VERSION, INSTANCE_ID, my_host())
    log.info("Tracking %d tokens across %s", len(TOKENS), sorted({c for c, _ in TOKENS}))
    log.info("EVM whale wallets configured: %d", len(EVM_WHALES))
    await asyncio.to_thread(
        send_telegram,
        f"✅ Bot is online (v{BOT_VERSION}, copy {INSTANCE_ID}). "
        + ("Auto-discovery ON. " if AUTO_DISCOVER else "")
        + (f"Manually watching {len(TOKENS)} coin(s). " if TOKENS else "")
        + f"Learning ON (self-tuning {'ON' if AUTO_TUNE else 'OFF'}), "
        f"narrative radar {'ON' if NARRATIVE_ALERTS else 'OFF'}.\n"
        + (f"😴 Stay-awake ON: it visits {md_escape(my_host())} every {KEEPALIVE_SECONDS // 60} min.\n"
           if PUBLIC_URL else
           "😴 Stay-awake OFF (no web address found) - on Render's free plan it will sleep when idle.\n")
        + (f"📋 First status check-in in about {FIRST_CHECKIN_MINUTES} min, then every {CHECKIN_HOURS:g}h.\n"
           if CHECKIN_HOURS > 0 else "")
        + f"📉 Looking for coins {T['retrace_min']:.0f}-{T['retrace_max']:.0f}% below a past high of {fmt_usd(MIN_PEAK_MC)}+"
        + (f", at most {MAX_COIN_AGE_DAYS:g} days old" if MAX_COIN_AGE_DAYS > 0 else "") + ".\n"
        + (f"🧭 Bottomed-coin list ON (Sparsity, checked every {SPARSITY_REFRESH_HOURS:g}h): alerts at {BOTTOM_MIN_SIGNS} of 3 signs, "
           "and I look at who is buying each coin to find smart wallets.\n" if SPARSITY_FEED else "")
        + (f"🧹 Set aside {OLD_RESULTS_SET_ASIDE} old result(s) from earlier versions (they could be fooled by price glitches).\n"
           if OLD_RESULTS_SET_ASIDE else "")
        + (f"👛 Watching {len(state['watch'])} wallet(s). " if state["watch"] else "")
        + ("Send /help to see the chat commands (e.g. /watch to add a wallet, /check to look at a coin).\n"
           if COMMANDS else "")
        + "You get a coin alert only when it passes ALL your filters.",
    )
    tasks = [asyncio.create_task(scanner_loop()), asyncio.create_task(keepalive_loop()),
             asyncio.create_task(wallet_loop()), asyncio.create_task(command_loop())]
    yield
    for t in tasks:
        t.cancel()
    save_state()
    try:  # last chance to save the memory before this copy stops (an update or a restart)
        await asyncio.wait_for(asyncio.to_thread(backup_state_to_telegram, True), timeout=10)
    except Exception:
        pass


app = FastAPI(title="Multi-Chain Alert Bot", lifespan=lifespan)

_bg_tasks: set = set()


def run_in_background(fn, *args):
    """Reply to the webhook sender right away; do the (slower) work afterwards."""
    task = asyncio.create_task(asyncio.to_thread(fn, *args))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
@app.get("/health", include_in_schema=False)
@app.head("/health", include_in_schema=False)
def health():
    now, s = time.time(), last_scan
    return {
        "status": "ok", "version": BOT_VERSION, "copy": INSTANCE_ID, "uptime_minutes": round((now - STARTED_AT) / 60),
        "manual_tokens": len(TOKENS), "discovered_tokens": len(state["discovered"]), "chains": sorted(CHAINS),
        "watched_wallets": len(state["watch"]),
        "bottoms_followed": _sp["followed"], "sparsity_read": bool(_sp["last_ok"]),
        "last_scan_seconds_ago": round(now - s["t"]) if s else None,
        "coins_examined_last_scan": len(s["rows"]) if s else None,
        "stay_awake": {"on": bool(PUBLIC_URL), "pings_sent": keepalive["pings"], "pings_ok": keepalive["ok"],
                       "last_ok_seconds_ago": round(now - keepalive["last_ok"]) if keepalive["last_ok"] else None},
    }


@app.get("/status")
def status_page():
    """Open https://<your-app>.onrender.com/status in a browser for the same summary as the Telegram check-in."""
    return PlainTextResponse(build_checkin(with_wallets=False).replace("*", "").replace("\\", "").replace("`", ""))


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
