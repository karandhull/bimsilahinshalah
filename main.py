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
import math
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
MAX_DISCOVERED = int(os.getenv("MAX_DISCOVERED", "500"))
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
TRACK_HOURS = float(os.getenv("TRACK_HOURS", "168"))       # how long each call is followed (v3.4: 7 days - some coins take longer than 3 days to cook)
WALLET_TRACK_HOURS = env_float("WALLET_TRACK_HOURS", 336)  # v3.4: a wallet's buy (and a coin your wallets hold in a dip) is followed this long: 14 days
AUTO_TUNE = os.getenv("AUTO_TUNE", "false").lower() == "true"  # let the bot adjust its own filters
MIN_CALLS_TUNE = int(os.getenv("MIN_CALLS_TUNE", "10"))    # finished calls needed before self-tuning
MIN_SHADOWS_TUNE = 8                                       # finished near-misses needed per experiment
REPORT_EVERY_CALLS = int(os.getenv("REPORT_EVERY_CALLS", "3"))
SHADOW_COOLDOWN = 24 * 3600
TELEGRAM_BACKUP = os.getenv("TELEGRAM_BACKUP", "true").lower() != "false"
BACKUP_TAG = "BOT_STATE_BACKUP v1"
BACKUP_MIN_GAP = 300                  # never re-save the memory more often than this...
BACKUP_REFRESH_SECONDS = 6 * 3600     # ...and re-save at least this often even if only prices moved
SNAPSHOT_DAYS = env_float("SNAPSHOT_DAYS", 7)   # v3.4: a dated safety copy of the memory is left in the chat this often, and one before every update (0 = none)
MAX_MEMORY_FILE_MB = 19               # Telegram lets a bot read files up to 20 MB
MAX_SHADOWS_OPEN = 150
GECKO_POOL_PAGES = int(os.getenv("GECKO_POOL_PAGES", "2"))   # pages (20 pools each) per chain, every round; 0 = off
GECKO_DEEP_PAGES = min(10, int(os.getenv("GECKO_DEEP_PAGES", "8")))  # one chain per round is searched this deep
MAX_COIN_MC = 1_000_000_000                                  # coins above $1B never do a deep-dip re-run setup

# --- Staying awake, status check-ins, double-copy detection --------------------
BOT_VERSION = "3.8.1"
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
NARRATIVE_ALERTS = os.getenv("NARRATIVE_ALERTS", "false").lower() == "true"      # v3.0: off unless switched on (/alerts narrative on)
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
# --- v3.2: a ready-made wallet list, and a limit on the "<wallet> BOUGHT / SOLD" messages ------------------------------------
WATCH_MOVES_MAX_PER_DAY = int(env_float("WATCH_MOVES_MAX_PER_DAY", 12))      # such messages per 24h (0 = no limit); past it the moves are still followed and scored, just not announced one by one
WATCH_MOVES_PER_WALLET = int(env_float("WATCH_MOVES_PER_WALLET", 3))         # v3.3: ...and at most this many per wallet per 24h (0 = no limit), so one very busy wallet cannot use up the whole day's messages
# --- v3.4: "several of my wallets keep adding to the same coin" - small top-ups over days that never trigger a 'bought' message ----
ACCUM_ON = os.getenv("ACCUM_ALERTS", "true").lower() != "false"              # (it also needs the 'smart' alerts to be on: /alerts smart on)
ACCUM_DAYS = env_float("ACCUM_DAYS", 7)                                      # adds are added up over this many days
ACCUM_STEP_MIN = 0.02                                                        # a change of less than 2% of what the wallet holds is rounding / dust, not a trade
ACCUM_NOTE_MIN_USD = env_float("ACCUM_NOTE_MIN_USD", 25)                     # ...and a change worth less than this many dollars is not written down either
ACCUM_MIN_WALLET_USD = env_float("ACCUM_MIN_WALLET_USD", 500)                # a wallet counts as "adding" when its net adds are worth this much...
ACCUM_MIN_GROWTH = env_float("ACCUM_MIN_GROWTH", 0.10)                       # ...and its position grew by at least this share (a big holder adding 0.3% is nothing)
ACCUM_MIN_WALLETS = max(1, int(env_float("ACCUM_MIN_WALLETS", 2)))           # this many of your wallets adding the same coin (one of them in 2+ steps) = a signal...
ACCUM_SOLO_USD = env_float("ACCUM_SOLO_USD", 2000)                           # ...or ONE wallet adding at least this much...
ACCUM_SOLO_STEPS = 3                                                         # ...in at least this many separate top-ups (one big buy is already announced)
ACCUM_MAX_PER_DAY = int(env_float("ACCUM_MAX_PER_DAY", 3))                   # such messages per 24h
ACCUM_COOLDOWN_DAYS = env_float("ACCUM_COOLDOWN_DAYS", 3)                    # the same coin is not announced again for this long
ACCUM_SHOW_USD = 250                                                         # the scorecard lists a wallet's adds / sells of a coin above this much
ACCUM_MAX_ROWS = 60                                                          # changes remembered per wallet and coin
STARTER_WALLETS_ON = os.getenv("STARTER_WALLETS", "true").lower() != "false" # "false" = do not add the built-in list below
MAX_PASTED_COMMANDS = 12                                                     # several /watch lines in ONE chat message: this many at most
# Found on 15-16 Oct 2026 from DOPE and baton (coins that dipped hard on Solana and then ran): wallets that bought near the bottom
# AND have a good 30-day record on Defined.fi (older wallets, no bots, profitable on other coins too). A clue, not a guarantee -
# the wallet book keeps scoring everything they buy. "dip" = bought at the low; "base" = bought in the base just before the run.
STARTER_WALLETS = (
    ("DOPE dip 6F5x", "6F5x9fKqXCfPf8Rufs6ZvLjzPfCicytUgWexzx6LxoDi"),
    ("DOPE dip A8pZ", "A8pZuUXnN4bAC66DMfJkKYSgNGDvWGz7KCVFdYnfJynr"),
    ("DOPE dip BQKq", "BQKqM6L1QTbmqZ7CvkiYuCGRNUVxmS5EXeQYa9zBoYJ3"),
    ("DOPE dip 9aU4", "9aU4GWeG6F6dc94ZJTRXzC8xuSkHyiNidWYcfyhcTqB3"),
    ("DOPE dip DKaw", "DKawRGCQaB7gSyjwczSNqjbbWYywVQCdvrC7JnKbFefk"),
    ("DOPE early bcAT", "bcATZhi2mDrJ1JdP4Hvn99ksmaYE2fppS9igY8jAVSz"),
    ("baton dip ESVP", "ESVPiw89EPDawTyqXWmot6nJ8D92MMNHH5fmDmficY32"),
    ("baton dip DZZV", "DZZVfvX7qyW468KgLa1mS5bdDunAoMq2JXPqHi2HjAvR"),
    ("baton dip CM9c", "CM9cJ6EK65YreZ4tJC8pW4MQjQ3JX9MZDstmwPBN2CtZ"),
    ("baton dip AEKU", "AEKU5rbkAXUmMK4cWsvJbPoEHTruGzV6P32rRC6TaL22"),
    ("baton dip AgDt", "AgDt6WfypoeY5upuoLnVZCRCNHHvXborLvSeRXP9uUuY"),
    ("baton base 6Cpi", "6CpipySaXTAiUKDyyhWeCfYSMQLCiHrbCN1WvRv5s8kF"),
    ("baton base Cr1n", "Cr1n5ZTc1W42zxHQ2LEAHUyvKPrm3ABAHKmFABMh9bKT"),
    ("baton base ethu", "ethuWdnPxqmwFskE4EpN18qdjzfCM79kALftiAGDw8Z"),
)
STARTER_ADDED: list[str] = []       # names of the built-in wallets added during this start-up (the start-up message mentions them once)
COMMANDS = os.getenv("TELEGRAM_COMMANDS", "true").lower() != "false"         # chat commands (/watch, /check ...)
YOUNG_DAYS = 30                                                              # "young coin" signal

# --- v3.5: named wallets (VIPs), loud low-cap pings, market caps on every coin, the dip-watch for coins a KOL buys or shills -----------
# Why: in the owner's data (12-25 Oct) coins under $1M that several saved wallets bought early doubled about 4 times in 10, and the
# one big recent hit (MOTION) was exactly that: two wallets the bot had promoted + three more bought it at ~$340K market cap.
LOWCAP_MC = env_float("LOWCAP_MC", 1_000_000)               # "low cap" = a market cap at or under this (change it in the chat: /lowcap 500k)
VIP_ON = os.getenv("VIP_ALERTS", "true").lower() != "false"                  # named wallets + loud low-cap pings
VIP_SWEEP_SECONDS = max(45, int(env_float("VIP_SWEEP_SECONDS", 90)))        # how often the VIP wallets are looked at
VIP_LOOP_START_DELAY = 40                                                    # seconds after start-up before the first VIP look
VIP_MIN_BUY_USD = env_float("VIP_MIN_BUY_USD", 150)         # a VIP buy smaller than this is not worth a ping (change it: /vip min 300)
VIP_MIN_LIQ_USD = env_float("VIP_MIN_LIQ_USD", 5_000)       # a coin with less liquidity than this cannot really be bought
VIP_REMINDERS = int(min(4, max(0, env_float("VIP_REMINDERS", 2))))          # a loud alert is repeated this many times (change it: /vip ping 3)
VIP_REMIND_AFTER = (75, 240, 600, 1200)                      # ...this many seconds after the alert
VIP_LOUD_MAX_PER_DAY = int(env_float("VIP_LOUD_MAX_PER_DAY", 20))           # past this, a low-cap buy is still announced - once, without repeats
VIP_PLAIN_MAX_PER_DAY = int(env_float("VIP_PLAIN_MAX_PER_DAY", 12))         # buys of bigger coins (not low cap): one short message each, this many a day
VIP_REBUY_HOURS = 6                                          # the same wallet adding to the same coin within this: no second alert
VIP_FIRST_LOOKBACK_MIN = 15                                  # first look at an Ethereum-style VIP wallet: its last 15 minutes are read
VIP_MAX_CATCHUP_BLOCKS = 250_000                             # after a long pause at most this many blocks are read back (~7 hours on Robinhood Chain)
VIP_BLOCKS_PER_SECOND = 9.5                                  # Robinhood Chain
VIP_LOG_WINDOW = 25_000                                      # the free node allows at most 30,000 blocks per log request
HUNTER_ON = os.getenv("HUNTERS", "true").lower() != "false"  # wallets that keep buying low-cap coins that double join the VIP list by themselves
HUNTER_MAX = int(env_float("HUNTER_MAX", 10))
HUNTER_MIN_WINS = int(env_float("HUNTER_MIN_WINS", 2))       # low-cap buys that doubled
HUNTER_MIN_RATE = env_float("HUNTER_MIN_RATE", 0.5)          # ...and at least this share of its decided low-cap buys
LOWCAP_ALERTS = os.getenv("LOWCAP_ALERTS", "true").lower() != "false"       # "several saved wallets bought a low-cap coin"
LOWCAP_MIN_WALLETS = max(2, int(env_float("LOWCAP_MIN_WALLETS", 3)))
LOWCAP_MIN_USD = env_float("LOWCAP_MIN_USD", 2_000)          # together they bought at least this much
LOWCAP_MIN_LIQ_USD = env_float("LOWCAP_MIN_LIQ_USD", 15_000)
LOWCAP_MAX_RUN = env_float("LOWCAP_MAX_RUN", 0.6)            # already up more than this (60%) on what they paid: too late for this alert
LOWCAP_MAX_PER_DAY = int(env_float("LOWCAP_MAX_PER_DAY", 6))
LOWCAP_HOURS = env_float("LOWCAP_HOURS", 36)                 # their buys count when they are younger than this
KOL_ALERTS = os.getenv("KOL_ALERTS", "true").lower() != "false"             # dip-watch on coins a KOL (unipcs) bought or shilled
KOL_DIP_MIN = env_float("KOL_DIP_MIN", 60)                   # alert when a watched coin is down at least this many % from its high...
KOL_DIP_MAX = env_float("KOL_DIP_MAX", 80)                   # ...and at most this many (past that it is left alone)
if not (0 <= KOL_DIP_MIN < KOL_DIP_MAX <= 99):
    KOL_DIP_MIN, KOL_DIP_MAX = 60.0, 80.0
KOL_SECONDS = max(60, int(env_float("KOL_SECONDS", 150)))   # how often the watched coins are priced
KOL_MAX_COINS = int(env_float("KOL_MAX_COINS", 150))
KOL_KEEP_DAYS = env_float("KOL_KEEP_DAYS", 30)
KOL_MIN_BUY_USD = env_float("KOL_MIN_BUY_USD", 30)           # a buy smaller than this does not put a coin on the dip-watch
KOL_MAX_PER_DAY = int(env_float("KOL_MAX_PER_DAY", 8))
KOL_COOLDOWN_DAYS = env_float("KOL_COOLDOWN_DAYS", 7)
KOL_TRACK_HOURS = env_float("KOL_TRACK_HOURS", 168)
KOL_MIN_LIQ_USD = env_float("KOL_MIN_LIQ_USD", 3_000)
# Named wallets. how sure: "certain" = the address matched the trader's Fomo profile exactly (same coins, same amounts, same times);
# "likely" = a strong match but not perfect. kol = coins this wallet buys go on the dip-watch (the 'bonk guy').
VIP_WALLETS = (
    # (name, address, chain, how sure, kol, note[, style])   (style = the wallet's default style until you set one with /vip style)
    ("cryptoklotz", "0x5a08728a6704d99468d8919ba0adc2e1d2733083", "robinhood", "certain", False, "Fomo trader, bought MOTION early"),
    ("0xdetweiler", "0x838a336d19ae121a0ca754ebbdd57282de656dfa", "robinhood", "certain", False, "Fomo trader, traded CONDUCTOR"),
    ("unipcs", "2heJbC32Tpfcb3nbUb5ER61K11FGZVfVGtVnDm6LDogF", "solana", "likely", True, "Fomo trader (the 'bonk guy'), traded PANGEA"),
    # v3.7: 'Wood' (Fomo @notanicecat69, a Top 100 trader). His address was matched on-chain to the ABYSS buys shown on his Fomo profile. Picky by default
    # (he trades few coins and with size), so a buy of his gets the loud ping on coins up to PICKY_MAX_MC.
    ("notanicecat69", "0xb02208d1b27811480cf6171bccc2b7af9513cddb", "robinhood", "certain", False,
     "Fomo trader 'Wood' (@notanicecat69, Top 100), bought ABYSS early", "selective"),
    # v3.7: 'angery' (Fomo, 335 followers). His address was matched on-chain to the holdings on his Fomo profile: 12.8M CLAUS (Ethereum) and on
    # Robinhood Chain 3.45M Artificial Inu and 511.3K Ramses (the amounts agree to 3 digits; the CLAUS buy cost about $3.9K at the launch).
    # No default style: the bot judges him itself after a few days. A VIP wallet is only looked at on Robinhood Chain for now.
    ("angery", "0x340e669fc0ccfa36b29a6cbdfcc919570643b7fc", "robinhood", "certain", False,
     "Fomo trader 'angery', holds Artificial Inu and Ramses on Robinhood Chain, bought CLAUS at its launch"),
)
VIP_SWEEPS_ON = os.getenv("VIP_SWEEPS", "true").lower() != "false"          # look inside the VIP wallets (the nodes' free plans are used politely)
HUNTER_SWEEP_SECONDS = max(120, int(env_float("HUNTER_SWEEP_SECONDS", 300)))  # bot-found wallets on Solana are looked at less often than the named ones
VIP_STALE_MIN = env_float("VIP_STALE_MIN", 90)               # a buy found later than this many minutes after it happened is announced quietly (no repeats)
VIP_RETRY_SECONDS = 45                                       # a coin that could not be priced yet (new, or DexScreener busy) is tried again this often...
VIP_RETRY_TRIES = 8                                          # ...this many times
LOWCAP_RECAPTURE_HOURS = env_float("LOWCAP_RECAPTURE_HOURS", 3)  # who is buying a low-cap coin that saved wallets are in: looked at this often (other coins: once a day)
# --- v3.6: how picky a wallet is (its "style") ---------------------------------------------------------------------------------
# Why: the owner noticed that big wallets that rarely trade win more often than micro-sprayers that buy everything. The bot's own wallet
# book agrees (580 wallets, 12-27 Oct): wallets that bought 1-3 different coins were right ~88% of the time, 4-9 coins ~35%, 10+ coins ~17%.
# So a VIP wallet is counted by HOW MANY DIFFERENT COINS it buys: picky wallets get a better ping, sprayers a quiet one.
STYLE_ON = os.getenv("WALLET_STYLES", "true").lower() != "false"
STYLE_MIN_BUY_USD = env_float("STYLE_MIN_BUY_USD", 150)        # a buy counts toward "how many coins" when it is at least this big
STYLE_SPRAY_7D = int(env_float("STYLE_SPRAY_7D", 14))          # 14+ different coins in 7 days = a sprayer...
STYLE_SPRAY_24H = int(env_float("STYLE_SPRAY_24H", 5))         # ...or 5+ in one day
STYLE_PICKY_7D = int(env_float("STYLE_PICKY_7D", 5))           # picky = at most 5 coins in 7 days...
STYLE_PICKY_24H = int(env_float("STYLE_PICKY_24H", 2))         # ...and at most 2 in one day...
STYLE_PICKY_MIN_DAYS = env_float("STYLE_PICKY_MIN_DAYS", 3)    # ...and watched for at least this many days (a wallet seen for an hour looks picky by accident)
PICKY_MAX_MC = env_float("PICKY_MAX_MC", 50_000_000)           # a picky wallet gets the loud ping on any coin up to this market cap (others: only under the low-cap line)
PICKY_LOUD_MAX_PER_DAY = int(env_float("PICKY_LOUD_MAX_PER_DAY", 10))    # (its own limit: picky wallets do not use up the 20 loud pings a day of the others)
SPRAY_MAX_PER_DAY = int(env_float("SPRAY_MAX_PER_DAY", 4))     # a sprayer's low-cap buys: silent messages, this many a day
STYLE_KEEP_DAYS = 10                                           # buys are remembered this long (the style looks at the last 7 days)
STYLE_MAX_ROWS = 80                                            # ...and at most this many per wallet
STYLE_WORDS = {"selective": "selective", "picky": "selective", "whale": "selective", "sniper": "selective",
               "spray": "spray", "sprayer": "spray", "micro": "spray", "microspray": "spray", "sprayed": "spray",
               "normal": "normal", "auto": "auto", "default": "auto", "reset": "auto"}
STYLE_LABEL = {"selective": "🎯 picky", "spray": "🔕 sprayer", "normal": "▫️ normal"}
# --- v3.6: the accumulation radar (Robinhood Chain) ----------------------------------------------------------------------------
# Finds coins that many wallets are buying AND KEEPING (not flipping) over the last day, by reading the coin's own transfer log: every
# transfer is grouped into 'pool/router -> wallet' (a buy), 'wallet -> pool/router' (a sell) and 'wallet <-> wallet'. A wallet that bought
# at least RADAR_MIN_BUY_USD, sold a quarter or less of it and still holds most of it is an accumulator. Pools and routers are found by
# themselves (contracts that move the coin to/from 10+ different addresses), so nothing here depends on a list of known pools.
RADAR_ON = os.getenv("RADAR", "true").lower() != "false"
RADAR_HOURS = env_float("RADAR_HOURS", 24)                      # look-back window
RADAR_BLOCKS_PER_SECOND = 9.8                                   # Robinhood Chain
RADAR_LOOP_START_DELAY = 150                                    # seconds after start-up (the other loops go first)
RADAR_ROUND_SECONDS = max(60, int(env_float("RADAR_ROUND_SECONDS", 120)))   # one round (a few coins) this often
RADAR_COINS_PER_ROUND = max(1, int(env_float("RADAR_COINS_PER_ROUND", 3)))
RADAR_READ_EVERY_MIN = env_float("RADAR_READ_EVERY_MIN", 60)    # each coin is read again after this long (coins that qualify: half as long)
RADAR_LIST_HOURS = env_float("RADAR_LIST_HOURS", 3)             # the list of coins to read is rebuilt this often (GeckoTerminal pool pages)
RADAR_LIST_PAGES = max(1, min(5, int(env_float("RADAR_LIST_PAGES", 3))))
RADAR_MAX_COINS = int(env_float("RADAR_MAX_COINS", 70))
RADAR_MIN_LIQ_USD = env_float("RADAR_MIN_LIQ_USD", 80_000)      # a coin with less liquidity than this cannot be bought in any size
RADAR_MIN_MC = env_float("RADAR_MIN_MC", 150_000)
RADAR_MAX_MC = env_float("RADAR_MAX_MC", 500_000_000)
RADAR_MIN_AGE_H = env_float("RADAR_MIN_AGE_H", 48)              # launch-day coins are pumps and wash trades, not accumulation
RADAR_MAX_CALLS = int(env_float("RADAR_MAX_CALLS", 14))         # node requests for one coin; a busier coin is skipped (and says so)
RADAR_MIN_BUY_USD = env_float("RADAR_MIN_BUY_USD", 300)         # a wallet counts as an accumulator from this much bought...
RADAR_MAX_SOLD = 0.25                                           # ...selling at most this share of it...
RADAR_MIN_HELD = 0.6                                            # ...and still holding at least this share of what it bought...
RADAR_MAX_BUYS = 40                                             # ...in at most this many buys (a bot that trades all day is not accumulating)
RADAR_HUB_FANOUT = 10                                           # an address that moves the coin to/from this many different addresses is a pool / router / exchange
RADAR_MIN_ACC = int(env_float("RADAR_MIN_ACC", 25))             # alert: at least this many accumulators...
RADAR_MIN_NET_USD = env_float("RADAR_MIN_NET_USD", 20_000)      # ...who together bought (net of sells) at least this much...
RADAR_MIN_SHARE_MC = 0.0015                                     # ...which is at least 0.15% of the market cap (or of the liquidity x 2, when the market cap is not known)
RADAR_MAX_TOP1 = env_float("RADAR_MAX_TOP1", 0.35)              # ...with no single wallet more than 35% of it (one big wallet is a different story)
RADAR_MIN_RECENT = int(env_float("RADAR_MIN_RECENT", 8))        # ...at least this many of them still buying in the last 6 hours
RADAR_SMALL_MIN_ACC = int(env_float("RADAR_SMALL_MIN_ACC", 10))        # (the /radar list also shows 'smaller but steady' coins: at least this many wallets...
RADAR_SMALL_MIN_SHARE = env_float("RADAR_SMALL_MIN_SHARE", 0.005)      # ...who bought at least 0.5% of the coin's market cap, no single wallet over the 35%...
RADAR_SMALL_MIN_RECENT = int(env_float("RADAR_SMALL_MIN_RECENT", 3))   # ...and at least this many still buying in the last 6 hours - listed, never alerted)
RADAR_MAX_RUN = env_float("RADAR_MAX_RUN", 25)                  # ...and the price not up more than 25% in 24h (this is for buying BEFORE a run)...
RADAR_MIN_DROP = env_float("RADAR_MIN_DROP", -40)               # ...nor down more than 40% (a falling knife is not accumulation)
RADAR_MAX_PER_DAY = int(env_float("RADAR_MAX_PER_DAY", 3))      # alerts per 24h
RADAR_COOLDOWN_DAYS = env_float("RADAR_COOLDOWN_DAYS", 3)       # the same coin is not announced again for this long
RADAR_TRACK_HOURS = env_float("RADAR_TRACK_HOURS", 168)         # a radar alert is followed this long...
RADAR_WIN_X, RADAR_STOP_X = 1.5, 0.7                            # ...win = +50% before -30% (these coins are bigger than the low caps, where 2x before 0.5x is the yardstick)
RADAR_KEEP_HOURS = 30                                           # a reading older than this is not shown any more
RADAR_BASKET_COINS = 6                                          # a wallet accumulating this many coins at once is a basket buyer (counted at half weight)
RADAR_TOP_KEEP = 15                                             # the biggest accumulators of each coin are remembered (for the basket check and the 'known wallets' line)
QUOTE_SYMBOLS ={"USDC", "USDT", "USDG", "DAI", "WETH", "ETH", "WBTC", "CBBTC", "USDE", "USDS", "PYUSD", "FDUSD", "TUSD", "WSOL", "SOL", "WBNB", "BNB"}

# --- v2.8: bottomed coins (Sparsity) and finding smart money by results ------------------------------------------------
SPARSITY_FEED = os.getenv("SPARSITY_FEED", "true").lower() != "false"           # read Sparsity Research's public "bottoming" page
SPARSITY_URL = os.getenv("SPARSITY_URL", "https://sparsitydata.com/bottoming").strip()
SPARSITY_REFRESH_HOURS = max(1.0, env_float("SPARSITY_REFRESH_HOURS", 4))      # the page itself is only updated once a day
BOTTOM_ALERTS = os.getenv("BOTTOM_ALERTS", "false").lower() == "true"      # v3.5: off unless switched on (/alerts bottom on): 2 of 42 finished bottom alerts doubled before halving; they are still followed quietly
BOTTOM_MIN_SIGNS = int(min(3, max(1, env_float("BOTTOM_MIN_SIGNS", 3))))       # signs (out of 3) a coin needs for an alert
BOTTOM_FOLLOW_SIGNS = int(min(3, max(0, env_float("BOTTOM_FOLLOW_SIGNS", 0))))  # coins on Sparsity's map with at least this many signs are followed (0 = all of them, quietly unless there is more to go on)
BOTTOM_MAX_PER_DAY = int(env_float("BOTTOM_MAX_PER_DAY", 10))
BOTTOM_COOLDOWN_DAYS = env_float("BOTTOM_COOLDOWN_DAYS", 7)
BOTTOM_TRACK_HOURS = env_float("BOTTOM_TRACK_HOURS", 168)                      # a bottom needs longer to play out than a re-run
BOTTOM_MIN_LIQ_USD = env_float("BOTTOM_MIN_LIQ_USD", 20_000)
MAX_BOTTOM_SHADOWS = 100
BUYER_CAPTURE = os.getenv("BUYER_CAPTURE", "true").lower() != "false"          # look at who is buying each bottomed coin
BUYER_PER_SCAN = max(1, int(env_float("BUYER_PER_SCAN", 2)))                    # coins looked at per scan (GeckoTerminal's free limit)
BUYER_RECAPTURE_HOURS = env_float("BUYER_RECAPTURE_HOURS", 12)
BUYER_TRADE_FLOOR_USD = env_float("BUYER_TRADE_FLOOR_USD", 50)                 # ignore dust trades when listing the latest trades
BUYER_MIN_USD = env_float("BUYER_MIN_USD", 500)                                # "accumulating" = bought at least this (more for big coins)
BUYER_MC_FRACTION = 0.0003
BUYER_MAX_USD_THRESHOLD = env_float("BUYER_MAX_USD_THRESHOLD", 2_500)           # "accumulating" never needs more than this (about the $1K buys that matter on a mid-size coin)
BUYER_MAX_TRADES = 25                                                           # more trades than this in the window AND selling a fair share of them = a bot, not a person
BUYER_HARD_MAX_TRADES = 120                                                     # ...and this many is a bot even if it only ever buys
BUYER_MAX_SELL_SHARE = 0.4                                                      # accumulating = sold less than 40% of what it bought
BUYER_RECORDS_PER_COIN = 5
SMART_AUTO = os.getenv("SMART_AUTO", "true").lower() != "false"                # put wallets with a good record on the watch list by themselves
SMART_MIN_BUYS = int(env_float("SMART_MIN_BUYS", 3))
SMART_MIN_GOOD = int(env_float("SMART_MIN_GOOD", 2))
SMART_MIN_RATE = env_float("SMART_MIN_RATE", 0.5)
SMART_GOOD_MULT = env_float("SMART_GOOD_MULT", 1.5)                            # a "good buy": the coin reached this multiple afterwards
SMART_MAX_AUTO = int(env_float("SMART_MAX_AUTO", 15))
GECKO_MIN_GAP = max(2.0, env_float("GECKO_MIN_GAP_SECONDS", 6.5))              # GeckoTerminal: ~10 requests a minute for ALL of the bot

# --- v2.9: chart shapes (cup & handle, bottom base, double bottom), the bot's own bottom signs, the wallet book ---------
CHARTS = os.getenv("CHARTS", "true").lower() != "false"                          # read the price chart of every scanned coin
CHART_ALERTS = os.getenv("CHART_ALERTS", "false").lower() == "true"       # v3.5: off unless switched on (/alerts chart on): 2 of 41 finished chart alerts doubled before halving; they are still followed quietly
CHART_MIN_MC = env_float("CHART_MIN_MC", 250_000)                               # smallest coin whose chart is read
CHART_MIN_LIQ_USD = env_float("CHART_MIN_LIQ_USD", 15_000)
CHART_MIN_AGE_HOURS = env_float("CHART_MIN_AGE_HOURS", 72)                      # a younger pool has too little history to read a shape from
CHART_REFRESH_HOURS = max(1.0, env_float("CHART_REFRESH_HOURS", 6))
CHART_PER_SCAN = max(0, int(env_float("CHART_PER_SCAN", 1)))                    # chart look-ups per scan (GeckoTerminal's free limit is shared)
CHART_MIN_SCORE = env_float("CHART_MIN_SCORE", 60)                              # shape score (0-100) a chart needs to count as a setup
CHART_ALERT_RANK = env_float("CHART_ALERT_RANK", 70)                            # shape score + support (signs, wallets) needed for an alert
CHART_MAX_PER_DAY = int(env_float("CHART_MAX_PER_DAY", 6))
CHART_COOLDOWN_DAYS = env_float("CHART_COOLDOWN_DAYS", 5)
CHART_TRACK_HOURS = env_float("CHART_TRACK_HOURS", 168)
MAX_CHART_SHADOWS = 80
CHART_KEEP_HOURS = 72                                                           # a chart reading older than this is forgotten
GEN_CAPTURE_HOURS = env_float("GEN_CAPTURE_HOURS", 24)                          # who is buying an ordinary scanned coin: looked at about this often
SETUP_CAPTURE_HOURS = env_float("SETUP_CAPTURE_HOURS", 8)                       # ...coins with a chart setup or bottom signs more often
BOOK_MAX = int(env_float("BOOK_MAX", 600))                                      # wallets kept in the wallet book
BOOK_PICKS = 12                                                                 # coin buys remembered per wallet
BOOK_TRACK_HOURS = env_float("BOOK_TRACK_HOURS", 168)                           # how long a wallet's buy is followed
BOOK_KEEP_DAYS = 21                                                             # a wallet seen once and never again is forgotten after this
FLOW_KEEP_DAYS = 7                                                              # buys seen over several looks add up for this long
SMART_BUY_MAX_PER_DAY = int(env_float("SMART_BUY_MAX_PER_DAY", 8))
SMART_BUY_ALERTS = os.getenv("SMART_BUY_ALERTS", "true").lower() != "false"

# --- v3.0: which alerts reach your chat (everything is still followed and scored; "off" only stops the message) ------------
RERUN_ALERTS = os.getenv("RERUN_ALERTS", "false").lower() == "true"     # the "ZIPCOIN RE-RUN SETUP" message: off since v3.0 (such setups are still followed in silence)
ALERT_DEFAULT = {"rerun": RERUN_ALERTS, "narrative": NARRATIVE_ALERTS, "walletdip": WATCH_DIP_ALERTS,
                 "bottom": BOTTOM_ALERTS, "chart": CHART_ALERTS, "smartbuy": SMART_BUY_ALERTS,
                 "vip": VIP_ON, "lowcap": LOWCAP_ALERTS, "kol": KOL_ALERTS, "radar": RADAR_ON}
ALERT_MENU = (    # what /alerts lists, in this order: (word you type, internal name, what it is)
    ("vip", "vip", "🚨 VIP low-cap ping - a named wallet (cryptoklotz, 0xdetweiler, unipcs, notanicecat69, angery, wallets the bot found) buys a low-cap coin: loud, with repeats. A picky wallet (few coins, big buys) is loud on bigger coins too; a sprayer gets a silent message (/vip style)"),
    ("radar", "radar", "📡 Accumulation radar - a coin on Robinhood Chain that many wallets keep buying and holding (/radar lists them)"),
    ("lowcap", "lowcap", "🎯 Low-cap crowd - 3+ saved wallets bought a coin under your low-cap line (/lowcap changes it)"),
    ("kol", "kol", "📉 Dip-watch - a coin unipcs bought or shilled is down 60-80% from its high"),
    ("smart", "smartbuy", "🧠 Smart-wallet buying - wallets with a good record (2+ stars) or wallets you watch are accumulating a coin, or several of them keep adding to one over days"),
    ("wallet", "walletdip", "👛 Wallet-backed dip - a dipped coin that wallets YOU watch are holding"),
    ("bottom", "bottom", "🧭 Bottom signal - coins on Sparsity's bottom map"),
    ("chart", "chart", "📈 Chart setup - cup & handle, base, double bottom"),
    ("rerun", "rerun", "🔁 Zipcoin re-run setup - a dipped coin that passes all the volume rules"),
    ("narrative", "narrative", "📰 Narrative watch - themed coins (AI, stocks, animals...) waking up"),
)
ALERT_WORDS = {"vip": "vip", "vips": "vip", "ping": "vip", "loud": "vip",
               "radar": "radar", "accum": "radar", "accumulation": "radar", "accumulating": "radar", "accumulate": "radar",
               "lowcap": "lowcap", "low-cap": "lowcap", "low": "lowcap", "crowd": "lowcap",
               "kol": "kol", "dipwatch": "kol", "dip-watch": "kol", "shill": "kol", "unipcs": "kol",
               "smart": "smartbuy", "smartbuy": "smartbuy", "smartwallet": "smartbuy", "smart-wallet": "smartbuy",
               "wallet": "walletdip", "walletdip": "walletdip", "wallet-backed": "walletdip", "dip": "walletdip",
               "bottom": "bottom", "bottoms": "bottom", "sparsity": "bottom",
               "chart": "chart", "charts": "chart", "pattern": "chart", "patterns": "chart",
               "rerun": "rerun", "re-run": "rerun", "zipcoin": "rerun", "zip": "rerun", "zipper": "rerun", "ziper": "rerun",
               "narrative": "narrative", "narr": "narrative", "theme": "narrative", "themes": "narrative"}
CHECK_MAX_PER_MESSAGE = 3                                                       # coin addresses scored per message
CHECK_TRACK_HOURS = env_float("CHECK_TRACK_HOURS", 168)                         # a coin you ask about is followed this long (silently)

# --- v3.1: honest numbers -------------------------------------------------------------------------------------------------
# 1) Coins that barely move (stock tokens, gold, wrapped coins, stable coins) cannot "dip and re-run": the dips and wins the bot
#    saw on them were bad price data. A coin whose whole daily history stayed inside this band is a "steady asset" and is left out.
STEADY_BAND = max(1.1, env_float("STEADY_BAND", 1.5))                           # highest / lowest daily price over its whole life
STEADY_MIN_DAYS = max(3, int(env_float("STEADY_MIN_DAYS", 4)))                  # days of history needed before that can be said
STEADY_RECHECK_DAYS = 10                                                        # a steady coin gets another look after this long (it may wake up)
STEADY_PROBES_PER_SCAN = 1                                                      # coins looked up for this, per scan, in the background
# 2) A crowd is one piece of evidence, not many: when lots of saved wallets bought the same coin they all "win" or all "lose"
#    together. A coin that this many saved wallets (or fewer) bought counts fully for each of them; a bigger crowd counts less.
CROWD_FULL_WEIGHT = max(1, int(env_float("CROWD_FULL_WEIGHT", 3)))
PROMISING_MIN_WEIGHT = env_float("PROMISING_MIN_WEIGHT", 0.6)                   # "promising" = at least this much good evidence (one good buy on a coin few of the saved wallets bought)

# --- v3.7: a 1-3 star rating for every wallet, from how its finished buys ENDED --------------------------------------------
# A followed buy ends one of three ways: the coin doubled before it halved (a hit), it halved before it doubled (a miss), or it did
# neither in the 7 days (flat). A hit only has to beat a miss 1 time in 3 to break even (win +100%, miss -50%), so the lines sit
# on that: of the buys that ended CLEARLY (hit or miss), the share that were hits, pulled towards an "average wallet" (STAR_PRIOR,
# counting as STAR_PRIOR_WEIGHT buys) so a few lucky buys cannot earn many stars. For a wallet with ~20 clear results this reads as
# about 70% hits = 3 stars, 60% = 2.5, 50% = 2, 40% = 1.5, 25% = 1, less = half a star; with fewer results it asks for more.
STAR_PRIOR = env_float("STAR_PRIOR", 0.22)
STAR_PRIOR_WEIGHT = env_float("STAR_PRIOR_WEIGHT", 8)
STAR_STEPS = ((0.55, 3.0), (0.47, 2.5), (0.40, 2.0), (0.33, 1.5), (0.22, 1.0))   # (pulled-in hit share needed, stars); below the last line = 0.5
STAR_MIN_BUYS = 3                                                               # fewer finished buys than this: "not rated yet"
STAR_MIN_CLEAR = 2                                                              # ...or fewer than this many that ended in a hit or a miss
STAR_CAPS = ((5, 2.0), (10, 2.5))                                               # under 5 finished buys: 2 stars at most; under 10: 2.5 at most
STAR_GOOD = env_float("STAR_GOOD", 2.0)                                         # "a good record" = this many stars or more (also what 'proven' needs)
STAR_COOL_BUYS = int(env_float("STAR_COOL_BUYS", 8))                            # a wallet the bot put on your list by itself cools off after this many finished buys...
STAR_COOL_CLEAR = int(env_float("STAR_COOL_CLEAR", 6))                          # ...at least this many of them clear (a hit or a miss)...
STAR_COOL_BELOW = env_float("STAR_COOL_BELOW", 1.5)                             # ...with fewer stars than this (wallets you add yourself are never cooled)

# --- v3.8: only FRESH buys, patient follow-ups, the coins you send, wallets that keep losing -----------------------------------
# Why (the owner, 27 Oct): four "low-cap crowd" pings were about OLD coins (one 10 months old) whose wallets had bought long ago, while the coin was
# low. The bot stamped a wallet's buy with the time it LOOKED, not with the time the wallet really bought, so an old buy looked new. And a week
# is too short for some coins (bigger ones especially). In the owner's own data (844 followed calls) wallet-driven calls on coins under 14 days old
# doubled about 3 times in 10, on older coins fewer than 1 in 10.
LOWCAP_FRESH_HOURS = env_float("LOWCAP_FRESH_HOURS", 24)          # a wallet counts in a crowd only if its LATEST buy of the coin is younger than this
LOWCAP_MAX_AGE_DAYS = env_float("LOWCAP_MAX_AGE_DAYS", 14)        # the crowd alert skips coins whose pool is older than this (0 = no limit; /lowcap age 30 changes it)
LOWCAP_MAX_DOWN = env_float("LOWCAP_MAX_DOWN", 0.25)              # ...and coins where the wallets are down more than this (25%) on what they paid
SMARTBUY_MAX_AGE_DAYS = env_float("SMARTBUY_MAX_AGE_DAYS", 30)    # the smart-wallet alert skips coins older than this (0 = no limit)
SMARTBUY_FRESH_HOURS = env_float("SMARTBUY_FRESH_HOURS", 36)      # ...and wallets whose latest buy is older than this
LSHADOW_MAX = 60                                                  # crowds turned down (too old, stale, already up...) that are followed quietly at once
# Patient follow-ups: a call (or a wallet's buy, or a coin you sent) that is still ALIVE when its window ends gets another week, up to a limit that
# depends on the size of the coin; it ends early when the coin breaks down.
EXTEND_ON = os.getenv("EXTEND_FOLLOWUPS", "true").lower() != "false"
EXTEND_STEP_HOURS = env_float("EXTEND_STEP_HOURS", 168)           # one extension = one more week
EXTEND_DAYS = ((1_000_000, env_float("EXTEND_DAYS_SMALL", 21)),   # (call market cap up to, longest total follow-up in days)
               (10_000_000, env_float("EXTEND_DAYS_MID", 30)),
               (float("inf"), env_float("EXTEND_DAYS_BIG", 45)))
EXTEND_LINK_DAYS = env_float("EXTEND_LINK_DAYS", 60)              # a coin you sent me is followed up to this long
EXTEND_ALIVE = env_float("EXTEND_ALIVE", 0.7)                     # alive = at least this share of the call price when the window ends...
EXTEND_BREAK = env_float("EXTEND_BREAK", 0.9)                     # ...and while extended: broken = under this share of the lowest point of the first window
EXTEND_MAX_OPEN = int(env_float("EXTEND_MAX_OPEN", 250))          # extended follow-ups open at once (past this, a window just ends as before)
SLOW_LANE_EVERY = max(1, int(env_float("SLOW_LANE_EVERY", 5)))    # extended follow-ups are priced every Nth scan (every 5 minutes) to keep the scans light
# The coins you send: the market cap you sent each one at is remembered; when it reaches LINK_LOOK_X times that, I look at its wallets.
LINK_ON = os.getenv("LINK_MEMORY", "true").lower() != "false"
LINK_LOOK_X = max(1.2, env_float("LINK_LOOK_X", 2.0))
LINK_KEEP_DAYS = env_float("LINK_KEEP_DAYS", 90)
LINK_MAX = int(env_float("LINK_MAX", 300))
LINK_EARLY_X = env_float("LINK_EARLY_X", 1.3)                     # a wallet that paid up to this multiple of the price you sent a coin at is "early"
LINK_WALLETS_KEPT = 6                                             # early wallets remembered per coin
# Wallets: a hit the wallet already had when I first saw its buy (it bought low, long before) counts less than a hit I could have followed.
EARLY_WIN_WEIGHT = env_float("EARLY_WIN_WEIGHT", 0.35)
EARLY_FORWARD_LR = env_float("EARLY_FORWARD_LR", 1.5)             # the coin was already up this much on the wallet's price when I first saw the buy = "early"
EARLY_QUICK_SECONDS = 1800                                        # (older buys without that note: a win within 30 minutes of being saved = "early")
MUTE_ON = os.getenv("MUTE_LOSERS", "true").lower() != "false"
MUTE_MIN_BUYS = int(env_float("MUTE_MIN_BUYS", 5))                # a wallet is "cold" after this many finished buys...
MUTE_MIN_CLEAR = int(env_float("MUTE_MIN_CLEAR", 4))              # ...at least this many of them clear (a hit or a miss)...
MUTE_BELOW = env_float("MUTE_BELOW", 1.0)                         # ...with fewer stars than this (= half a star). A cold wallet no longer counts toward any alert (its buys are still followed).
DEMOTE_DAYS = env_float("DEMOTE_DAYS", 14)                        # a wallet the bot took off your list for losing cannot come back by itself for this long

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
    "robinhood": "0x0bd7d308f8e1639fab988df18a8011f41eacad73",   # v3.5: WETH on Robinhood Chain (18 decimals)
}
EVM_STABLES = {
    "ethereum": {"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   # USDC
                 "0xdac17f958d2ee523a2206206994597c13d831ec7",   # USDT
                 "0x6b175474e89094c44da98b954eedeac495271d0f"},  # DAI
    "base":     {"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"},  # USDC
    "bsc":      {"0x55d398326f99059ff775485246999027b3197955",   # USDT
                 "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",   # USDC
                 "0xe9e7cea3dedca5984780bafc599bd69add087d56"},  # BUSD
    "robinhood": {"0x5fc5360d0400a0fd4f2af552add042d716f1d168"},  # v3.5: USDG (6 decimals, ~$1); other stables fall back to DexScreener pricing
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


def pool_floor() -> float:
    """The smallest market cap a coin may have to be found and kept in the scan pool: the re-run floor, or lower when
    chart shapes are read too (smaller coins can still form a cup or a base)."""
    return min(mc_floor(), CHART_MIN_MC) if CHARTS else mc_floor()


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


def span_text(hours: float) -> str:
    """How long a call is followed, in words: '36h' for a short follow-up, '7 days' for a longer one."""
    return f"{hours:.0f}h" if hours < 48 else f"{hours / 24:g} days"


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
         "records": [], "tuning": {}, "backup": {}, "wallets": {}, "watch": {}, "supply": {}, "bottoms": {},
         "charts": {}, "book": {}, "steady": {}, "steady_seen": {}, "accum": {}, "kol": {}, "radar": {}, "links": {}}
DICT_BUCKETS = ("peaks", "cooldowns", "discovered", "dismissed", "backfilled", "bf_fails",
                "shadow_cd", "meta", "whale_buys", "supply", "bottoms", "charts", "steady", "steady_seen", "kol", "links")
OLD_RESULTS_SET_ASIDE = 0   # how many results from older versions were set aside when the memory was loaded


def alert_on(kind: str) -> bool:
    """Does this kind of alert go to your chat? What you set with /alerts wins over the server settings."""
    over = state["tuning"].get("alerts")
    if isinstance(over, dict) and kind in over:
        return bool(over[kind])
    return bool(ALERT_DEFAULT.get(kind, True))


def set_alert(kind: str, on: bool):
    over = state["tuning"].get("alerts")
    if not isinstance(over, dict):
        over = state["tuning"]["alerts"] = {}
    over[kind] = bool(on)
    save_state()


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


def sanitize_new_memory():
    """The notes added in v2.9 (chart readings, the wallet book) are checked when they are loaded, so a damaged or
    hand-edited memory file can never break a scan."""
    for bucket in ("bottoms", "charts"):
        for k in [k for k, v in state[bucket].items() if not isinstance(v, dict)]:
            del state[bucket][k]
    for bucket in ("steady", "steady_seen"):
        for k in [k for k, v in state[bucket].items() if isinstance(v, bool) or not isinstance(v, (int, float))]:
            del state[bucket][k]
    for w in list(state["book"]):
        d = state["book"][w]
        picks = d.get("picks")
        d["picks"] = [p for p in (picks if isinstance(picks, list) else [])
                      if isinstance(p, dict) and all(x in p for x in ("k", "st", "t", "px", "s", "c"))
                      and isinstance(p.get("px"), (int, float)) and p["px"] > 0]
        for f in ("n", "good", "win", "stop", "flat", "seen", "first", "last"):
            if not isinstance(d.get(f), (int, float)):
                d[f] = 0
        if not isinstance(d.get("chain"), str):
            d["chain"] = "solana"
    # v3.4: the notes on how much each watched wallet added or sold of each coin: [[time, change in coins], ...]
    acc = state["accum"]
    for w in list(acc):
        led = acc[w]
        if not isinstance(led, dict):
            del acc[w]
            continue
        for mint in list(led):
            rows = led[mint]
            led[mint] = [[r[0], r[1]] for r in (rows if isinstance(rows, list) else [])
                         if isinstance(r, (list, tuple)) and len(r) == 2
                         and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in r)]
            if not led[mint]:
                del led[mint]
        if not led:
            del acc[w]
    # v3.5: the dip-watch notes (coins a KOL bought or shilled) and the VIP marks on watched wallets
    for k in list(state["kol"]):
        e = state["kol"][k]
        if not isinstance(k, str) or ":" not in k or k.split(":", 1)[0] not in CHAINS or not isinstance(e, dict):
            del state["kol"][k]
            continue
        e["sym"] = str(e.get("sym") or "?")[:14]
        e["who"] = str(e.get("who") or "KOL")[:30]
        e["chain"] = k.split(":", 1)[0]
        for f in ("t", "mc0", "peak", "alerted", "seen", "dd", "mc", "gone", "bf", "chk"):
            if not isinstance(e.get(f), (int, float)) or isinstance(e.get(f), bool):
                e[f] = 0
    for w, info in list(state["watch"].items()):
        if isinstance(info, dict) and info.get("vip"):
            if info.get("tier") not in ("vip", "hunter"):
                info["tier"] = "vip"
            if not isinstance(info.get("chain"), str):
                info["chain"] = "solana" if info.get("kind") == "solana" else "robinhood"
    cur = state["tuning"].get("vip_cur")
    if isinstance(cur, dict):
        for w in [w for w, b in cur.items() if not isinstance(b, int) or isinstance(b, bool)]:
            del cur[w]
    elif cur is not None:
        state["tuning"].pop("vip_cur", None)
    # v3.6: a wallet's style word, the log of its buys, and the radar's readings
    for w, info in list(state["watch"].items()):
        if isinstance(info, dict):
            if info.get("style") not in (None, "selective", "spray", "normal"):
                info.pop("style", None)
            if "vip_t" in info and (not isinstance(info["vip_t"], (int, float)) or isinstance(info["vip_t"], bool)):
                info.pop("vip_t", None)
    buys = state["tuning"].get("vip_buys")
    if isinstance(buys, dict):
        for w in list(buys):
            rows = buys[w]
            buys[w] = [[r[0], r[1], r[2]] for r in (rows if isinstance(rows, list) else [])
                       if isinstance(r, (list, tuple)) and len(r) == 3 and isinstance(r[1], str)
                       and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in (r[0], r[2]))][-STYLE_MAX_ROWS:]
            if not isinstance(w, str) or not buys[w]:
                del buys[w]
    elif buys is not None:
        state["tuning"].pop("vip_buys", None)
    since = state["tuning"].get("vip_buys_since")
    if since is not None and (not isinstance(since, (int, float)) or isinstance(since, bool)):
        state["tuning"].pop("vip_buys_since", None)
    rad = state["radar"]
    if not isinstance(rad, dict):
        state["radar"] = rad = {}
    coins = rad.get("coins")
    if not isinstance(coins, dict):
        rad["coins"] = coins = {}
    for k in list(coins):
        e = coins[k]
        if not isinstance(k, str) or ":" not in k or not isinstance(e, dict):
            del coins[k]
            continue
        e["sym"] = str(e.get("sym") or "?")[:14]
        for f in ("t", "seen", "acc", "usd", "mc", "liq", "vol", "top1", "top3", "n6", "n1", "net6", "dca", "ch", "age", "since", "ok2", "okt",
                  "first", "skip", "px", "last_h"):
            if not isinstance(e.get(f), (int, float)) or isinstance(e.get(f), bool):
                e[f] = 0
        e["why"] = str(e.get("why") or "")[:80]
        rows = e.get("top")
        e["top"] = [[r[0], r[1], r[2]] for r in (rows if isinstance(rows, list) else [])
                    if isinstance(r, (list, tuple)) and len(r) == 3 and isinstance(r[0], str)
                    and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in (r[1], r[2]))][:RADAR_TOP_KEEP]
    for f in ("last_list", "last_round"):
        if not isinstance(rad.get(f), (int, float)) or isinstance(rad.get(f), bool):
            rad[f] = 0
    if len(coins) > RADAR_MAX_COINS * 3:                         # (a damaged or very old file: only the newest readings are kept)
        for k in sorted(coins, key=lambda k_: coins[k_].get("seen", 0))[:len(coins) - RADAR_MAX_COINS * 3]:
            del coins[k]
    # v3.8: the real buy times on a wallet's buys, the extra-time notes on buys and calls, the coins you sent, the wallets taken off the list
    for w, d in list(state["book"].items()):
        for p in d.get("picks", []):
            for f in ("bt", "bf", "lr0", "mn", "lo1", "ext"):
                if f in p and (not isinstance(p[f], (int, float)) or isinstance(p[f], bool) or not math.isfinite(p[f])):
                    p.pop(f, None)
            p.pop("slow", None)                    # (a flag a first draft of v3.8 used: the extra-time mark is 'ext' now)
            if "why" in p and not isinstance(p["why"], str):
                p.pop("why", None)
        if "found" in d and not isinstance(d["found"], str):
            d.pop("found", None)
    for r in state["records"]:
        if not isinstance(r, dict):
            continue
        for f in ("ext", "lo1"):
            if f in r and (not isinstance(r[f], (int, float)) or isinstance(r[f], bool) or not math.isfinite(r[f])):
                r.pop(f, None)
        if "why" in r and not isinstance(r["why"], str):
            r.pop("why", None)
    links = state["links"]
    if not isinstance(links, dict):
        state["links"] = links = {}
    for k in list(links):
        e = links[k]
        if (not isinstance(k, str) or ":" not in k or k.split(":", 1)[0] not in CHAINS or not isinstance(e, dict)
                or not (isinstance(e.get("mc0"), (int, float)) and not isinstance(e.get("mc0"), bool) and e["mc0"] > 0)):
            del links[k]
            continue
        e["sym"] = str(e.get("sym") or "?")[:14]
        e["chain"] = k.split(":", 1)[0]
        for f in ("t0", "tl", "tr", "p0", "n", "last", "max_mc", "min_mc", "ms", "look", "lo1", "z"):
            if not isinstance(e.get(f), (int, float)) or isinstance(e.get(f), bool) or not math.isfinite(e.get(f)):
                e.pop(f, None) if f in ("lo1", "z") else e.__setitem__(f, 0)
        if e["t0"] <= 0:
            e["t0"] = time.time()
        e["tl"] = e["tl"] or e["t0"]
        e["last"] = e["last"] or e["mc0"]
        e["max_mc"] = e["max_mc"] or e["mc0"]
        e["min_mc"] = e["min_mc"] or e["mc0"]
        e["n"] = int(e["n"]) or 1
        if e.get("st") not in ("open", "stop", "ended"):
            e["st"] = "open"
        e["pa"] = str(e.get("pa") or "")[:80]
        for f in ("w0", "found"):
            rows = e.get(f)
            e[f] = [[r[0], r[1], r[2]] for r in (rows if isinstance(rows, list) else [])
                    if isinstance(r, (list, tuple)) and len(r) == 3 and isinstance(r[0], str)
                    and all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in r[1:])][:LINK_WALLETS_KEPT]
    for name in ("demoted", "warned"):
        v = state["tuning"].get(name)
        if isinstance(v, dict):
            for w in [w for w, t in v.items() if not isinstance(w, str) or not isinstance(t, (int, float)) or isinstance(t, bool)]:
                del v[w]
        elif v is not None:
            state["tuning"].pop(name, None)


def valid_record(r) -> bool:
    """A tracked call that is complete enough to be followed and counted (a damaged one would break a scan)."""
    return (isinstance(r, dict) and isinstance(r.get("key"), str) and isinstance(r.get("kind"), str)
            and isinstance(r.get("status"), str) and all(isinstance(r.get(f), (int, float)) for f in ("t0", "mc0")))


def record_id(r: dict) -> tuple:
    """What makes a tracked call 'the same call' (reading the same memory twice must not double it)."""
    return (r["key"], r["kind"], r.get("wallet"), r["t0"])


def merge_state(loaded: dict):
    """Read a saved memory into this (empty) one - at start-up. The file wins; nothing in it is dropped."""
    for bucket in DICT_BUCKETS:
        for k, v in (loaded.get(bucket) or {}).items():
            state[bucket][k if ":" in k else f"solana:{k}"] = v  # migrate v1 keys
    state["wallets"].update(loaded.get("wallets") or {})
    state["watch"].update(loaded.get("watch") or {})
    state["book"].update({w: d for w, d in (loaded.get("book") or {}).items() if isinstance(d, dict)})   # (wallet addresses: no key migration)
    state["accum"].update({w: d for w, d in (loaded.get("accum") or {}).items() if isinstance(d, dict)})
    if isinstance(loaded.get("radar"), dict):                   # v3.6: the accumulation radar's last readings
        state["radar"].update(loaded["radar"])
    for k, v in loaded.items():     # v3.4: notes this version does not know (a newer version's, after a roll-back) travel along untouched
        if isinstance(k, str) and k not in state:
            state[k] = v
    sanitize_new_memory()
    have = {record_id(r) for r in state["records"] if valid_record(r)}
    for r in loaded.get("records") or []:                       # v3.4: ...and the same call is never added twice
        if valid_record(r) and record_id(r) not in have:
            have.add(record_id(r))
            state["records"].append(r)
    quarantine_old_results()
    state["tuning"].update(loaded.get("tuning") or {})
    sanitize_new_memory()                                       # v3.6: (again, now that the saved settings - the buy notes among them - are in)
    gone = set(state["tuning"].get("watch_removed") or [])      # v3.2: a wallet you removed with /unwatch must not come back
    for k in [k for k in state["watch"] if k in gone]:          # from the built-in list when the memory is read in late
        del state["watch"][k]
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


def merge_book_entry(mine: dict, d: dict):
    """Two notes on the same wallet (the one here, and one from a memory file): keep the bigger counts and every followed buy."""
    for f in ("n", "good", "win", "stop", "flat", "seen"):
        if isinstance(d.get(f), (int, float)) and d[f] > (mine.get(f) or 0):
            mine[f] = d[f]
    if isinstance(d.get("first"), (int, float)) and d["first"] and (not mine.get("first") or d["first"] < mine["first"]):
        mine["first"] = d["first"]
    if isinstance(d.get("last"), (int, float)) and d["last"] > (mine.get("last") or 0):
        mine["last"] = d["last"]
    picks = [p for p in (mine.get("picks") or []) if isinstance(p, dict)]
    seen = {(p.get("k"), p.get("t")) for p in picks}
    picks += [p for p in (d.get("picks") or []) if isinstance(p, dict) and (p.get("k"), p.get("t")) not in seen]
    picks.sort(key=lambda p: p.get("t") or 0)
    while len(picks) > BOOK_PICKS:                      # the same rule as everywhere else: the oldest finished buy goes first
        i = next((j for j, p in enumerate(picks) if p.get("st") != "o"), None)
        if i is None:
            break
        picks.pop(i)
    mine["picks"] = picks


def merge_state_live(loaded: dict) -> dict:
    """v3.4: a memory file the owner sent into the chat. It only FILLS IN - a wallet, result or setting that is already here is
    never removed, and newer data here is never replaced by older data from the file. Returns counts for the reply."""
    out = {"watch": 0, "wallets": 0, "book": 0, "records": 0}
    removed = set(state["tuning"].get("watch_removed") or [])            # a wallet taken off with /unwatch stays off
    for w, d in (loaded.get("watch") or {}).items():
        if not isinstance(w, str) or not isinstance(d, dict):
            continue
        mine = state["watch"].get(w)
        if mine is None:
            if w in removed or recently_demoted(w):          # (v3.8: a wallet the bot took off the list for losing does not come back from an older file either)
                continue
            state["watch"][w] = dict(d)
            out["watch"] += 1
            if d.get("kind") == "solana":
                _sweep["now"] = True                                     # look inside it in the next minute
        elif d.get("name") and not mine.get("name"):
            mine["name"] = d["name"]
    for w, d in (loaded.get("wallets") or {}).items():                   # how each wallet's buys turned out: the bigger count wins
        if not isinstance(w, str) or not isinstance(d, dict):
            continue
        mine = state["wallets"].get(w)
        if mine is None:
            state["wallets"][w] = dict(d)
            out["wallets"] += 1
            continue
        for f in ("w", "s", "e"):
            if isinstance(d.get(f), (int, float)) and d[f] > (mine.get(f) or 0):
                mine[f] = d[f]
    for w, d in (loaded.get("book") or {}).items():                      # the wallet book
        if not isinstance(w, str) or not isinstance(d, dict):
            continue
        if w in state["book"]:
            merge_book_entry(state["book"][w], d)
        else:
            state["book"][w] = d
            out["book"] += 1
    have = {record_id(r) for r in state["records"] if valid_record(r)}   # tracked calls and finished results not here yet
    for r in loaded.get("records") or []:
        if valid_record(r) and record_id(r) not in have:
            have.add(record_id(r))
            state["records"].append(r)
            out["records"] += 1
    for bucket in DICT_BUCKETS:                                          # coin notes (past highs, supply...): only what is missing
        for k, v in (loaded.get(bucket) or {}).items():
            if isinstance(k, str):
                state[bucket].setdefault(k if ":" in k else f"solana:{k}", v)
    for w, d in (loaded.get("accum") or {}).items():
        if isinstance(w, str) and isinstance(d, dict):
            state["accum"].setdefault(w, d)
    if isinstance(loaded.get("radar"), dict):                            # v3.6: the radar's readings: only what is missing
        for k, v in loaded["radar"].items():
            if isinstance(k, str):
                state["radar"].setdefault(k, v)
    lt = loaded.get("tuning") if isinstance(loaded.get("tuning"), dict) else {}
    for k, v in lt.items():                                              # settings: only those not set here
        if k not in state["tuning"]:
            state["tuning"][k] = v
    gone = state["tuning"].get("watch_removed")
    if not isinstance(gone, list):
        gone = state["tuning"]["watch_removed"] = []
    for g in (lt.get("watch_removed") if isinstance(lt.get("watch_removed"), list) else []):
        if isinstance(g, str) and g not in gone and g not in state["watch"]:
            gone.append(g)
    for k, v in loaded.items():
        if isinstance(k, str) and k not in state:
            state[k] = v
    sanitize_new_memory()
    quarantine_old_results()
    return out


restore_pending = False   # True if a memory backup exists but could not be read yet (never overwrite it!)


def load_state():
    global restore_pending, _restore_failing_since
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
                _fresh_start["on"] = not _pin_problem["other"]    # (if something else is pinned, the warning about that is shown instead)
        except Exception as e:
            restore_pending = True  # Telegram hiccup: retry later and don't overwrite the saved memory meanwhile
            _restore_failing_since = time.time()
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
    try:
        seed_vip_wallets()          # v3.5: the named wallets (cryptoklotz, 0xdetweiler, unipcs; v3.7: notanicecat69, angery)
    except Exception:
        log.exception("the VIP wallets could not be set up - ignored")
    try:
        style_start()               # v3.6: the day the bot starts counting which coins each VIP wallet buys
    except Exception:
        log.exception("the wallet-style start day could not be set - ignored")
    state["tuning"]["ver"] = BOT_VERSION     # v3.4: which version last saved this memory (an update leaves a safety copy first)


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


def send_telegram(text: str, silent: bool = False) -> bool:
    """silent=True (v3.6): the message arrives without a sound or a buzz (Telegram's 'disable_notification') - used for a sprayer's buys."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text,
               "parse_mode": "Markdown", "disable_web_page_preview": True}
    if silent:
        payload["disable_notification"] = True
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


def chunk_lines(lines: list[str], limit: int = 3800) -> list[str]:
    """v3.8: lines of text cut into messages of at most `limit` characters, only ever between lines (a long list - the watched wallets - used
    to be cut in the middle of a line, and its last wallets were never shown)."""
    parts, cur = [], ""
    for line in lines:
        if len(line) > limit:
            line = line[:limit - 1] + "…"
        if cur and len(cur) + len(line) + 1 > limit:
            parts.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    return parts


def send_telegram_long(text: str, limit: int = 3800) -> bool:
    """A message that may be longer than Telegram allows (the start-up message can be, with several notes in it) is cut at line ends
    into parts that fit. True only if every part was sent."""
    if len(text) <= limit:
        return send_telegram(text)
    parts, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:                     # (one huge line: cut it at a space)
            cut = line.rfind(" ", 0, limit)
            cut = cut if cut > 0 else limit
            if cur:
                parts.append(cur)
                cur = ""
            parts.append(line[:cut])
            line = line[cut:].lstrip()
        if cur and len(cur) + len(line) + 1 > limit:
            parts.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    ok = True
    for p in parts:
        if p.strip():
            ok = send_telegram(p) and ok
    return ok


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


def fnum(x, default=0.0) -> float:
    """A number from a single value (a damaged memory file can hold anything there): the default when it is not a usable number."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


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


def build_rerun_message(chain: str, pair: dict, m: dict, extra: str = "", title: str = "🔁 ZIPCOIN RE-RUN SETUP",
                        track_hours: float | None = None) -> str:
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
        f"📌 Tracking this call for {follow_text(track_hours or TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x)\n"
        + extra + "\n"
        f"`{addr}`\n\n"
        f"[DexScreener](https://dexscreener.com/{chain}/{pair_addr}) | "
        f"[Explorer]({link(chain, 'token', addr)})"
    )


_slow_round = {"n": 0}      # v3.8: counts the scans; the slow lane (follow-ups on extra time) is priced when it is a multiple of SLOW_LANE_EVERY


def watch_list() -> list[tuple[str, str]]:
    """Manual TOKEN_LIST plus everything auto-discovered."""
    seen, out = set(), []
    tracked = list(dict.fromkeys(r["key"] for r in state["records"] if r["status"] == "open" and not r.get("ext")))
    bottomed = [k for k, b in list(state["bottoms"].items()) if bottom_active(b)]
    picked = sorted(open_pick_keys())             # coins a wallet in the wallet book bought: followed until the result is in
    slow = sorted(slow_keys()) if _slow_round["n"] % SLOW_LANE_EVERY == 0 else []     # v3.8: follow-ups on extra time: every Nth scan only
    for chain, addr in (list(TOKENS) + [tuple(k.split(":", 1)) for k in list(state["discovered"])]
                        + [tuple(k.split(":", 1)) for k in tracked] + [tuple(k.split(":", 1)) for k in bottomed]
                        + [tuple(k.split(":", 1)) for k in picked] + [tuple(k.split(":", 1)) for k in slow]):
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
                    if not (pool_floor() <= mc <= MAX_COIN_MC):
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


# ----------------------------------------------------------------------------
# v3.1 - STEADY ASSETS. Stock tokens, gold, wrapped coins and stable coins barely move, so they cannot dip and re-run. But their
# thin pools show an odd price now and then, and that looked like a deep dip, a "chart setup" or a 2x win - and it counted
# toward the wallets that held them. They are recognised from the coin's daily history (one GeckoTerminal request per coin),
# left out of every alert, and taken out of every wallet's record.
# ----------------------------------------------------------------------------
_steady_fails: Counter = Counter()     # coin key -> failed look-ups of its daily history


def daily_candles(chain: str, pair: dict) -> list:
    """The coin's daily candles from GeckoTerminal: [time, open, high, low, close, volume] rows, newest first."""
    net, pool = GECKO_NETWORK.get(chain), pair.get("pairAddress")
    if not (net and pool):
        raise ValueError("no pool to read")
    r = gecko_get(
        f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}/ohlcv/day",
        params={"aggregate": 1, "limit": 1000, "currency": "usd"}, timeout=20)
    return r.json()["data"]["attributes"]["ohlcv_list"]


def steady_from_candles(candles) -> bool:
    """Did the open and the close of every day of this coin's whole history stay inside STEADY_BAND (highest / lowest)?
    A coin that once fell 60% or rose 60% is not steady, however calm it is now."""
    prices: list[float] = []
    days = 0
    for c in candles if isinstance(candles, list) else []:
        try:
            o, cl = float(c[1]), float(c[4])
        except (TypeError, ValueError, IndexError):
            continue
        if o > 0 and cl > 0 and math.isfinite(o) and math.isfinite(cl):
            days += 1
            prices += [o, cl]
    return days >= STEADY_MIN_DAYS and max(prices) / min(prices) <= STEADY_BAND


def purge_steady(key: str, now: float):
    """A coin turned out to be a steady asset: what was recorded on it is set aside (it stops counting and nothing more is
    sent about it) and the wallet buys on it are taken back out of every wallet's numbers."""
    for r in state["records"]:
        if r["key"] == key and not r.get("void"):
            r["void"] = True
            if r["status"] == "open":
                r["status"], r["t1"] = "expired", now
    for d in list(state["book"].values()):
        if not any(p.get("k") == key for p in d.get("picks", [])):
            continue
        keep = []
        for p in d["picks"]:
            if p.get("k") != key:
                keep.append(p)
                continue
            d["seen"] = max(0, d.get("seen", 0) - 1)
            if p.get("st") != "o":                       # a finished buy had been counted: take it back out
                d["n"] = max(0, d.get("n", 0) - 1)
                fld = {"w": "win", "s": "stop", "f": "flat"}.get(p.get("st"))
                if fld:
                    d[fld] = max(0, d.get(fld, 0) - 1)
                if p.get("st") == "w" or p.get("mx", 1.0) >= SMART_GOOD_MULT:
                    d["good"] = max(0, d.get("good", 0) - 1)
        d["picks"] = keep
    state["charts"].pop(key, None)
    state["peaks"].pop(key, None)                         # its "past high" was only bad data


def note_steady(key: str, steady: bool, now: float | None = None):
    """Remember what the daily history said about a coin."""
    now = now or time.time()
    state["steady_seen"][key] = now
    if steady:
        newly = key not in state["steady"]
        state["steady"][key] = now
        if newly:
            log.info("%s barely moves (stock token, gold, wrapped coin or stable): left out of the alerts and of every wallet's record", key)
            bot_stats["steady"] += 1
            purge_steady(key, now)
    else:
        state["steady"].pop(key, None)


def ensure_steady(key: str, chain: str, pair: dict) -> bool | None:
    """Is this coin a steady asset? True or False - or None when that cannot be found out just now (GeckoTerminal busy).
    The answer is remembered, so a coin's daily history is asked for once (a steady coin again after STEADY_RECHECK_DAYS)."""
    now = time.time()
    seen = state["steady_seen"].get(key)
    if seen is not None and (key not in state["steady"] or now - seen < STEADY_RECHECK_DAYS * 86400):
        return key in state["steady"]
    if not (GECKO_NETWORK.get(chain) and pair.get("pairAddress")):
        note_steady(key, False, now)                       # nothing to look at: an ordinary coin
        return False
    if gecko_backlog() > 20:
        return None                                        # the shared connection is busy: ask again on a later scan
    try:
        candles = daily_candles(chain, pair)
    except requests.RequestException as e:
        if getattr(getattr(e, "response", None), "status_code", None) in (None, 429, 500, 502, 503, 504):
            bot_stats["gecko_busy"] += 1
            return None
        _steady_fails[key] += 1
    except (KeyError, ValueError, TypeError, AttributeError, IndexError):
        _steady_fails[key] += 1
    else:
        _steady_fails.pop(key, None)
        steady = steady_from_candles(candles)
        note_steady(key, steady, now)
        if key not in state["backfilled"]:           # the same candles give the coin's past high: no second request needed
            try:
                peak = peak_from_candles(key, pair, candles)
            except (ValueError, TypeError, IndexError):
                peak = None
            if peak:
                store_peak(key, peak)
                state["backfilled"][key] = now
        return steady
    if _steady_fails[key] >= 3:                            # no usable history after three tries: an ordinary coin
        _steady_fails.pop(key, None)
        note_steady(key, False, now)
        return False
    return None


def steady_due(key: str, now: float) -> bool:
    """Has this coin's daily history never been looked at - or is it a steady coin whose last look is old?"""
    seen = state["steady_seen"].get(key)
    return seen is None or (key in state["steady"] and now - seen >= STEADY_RECHECK_DAYS * 86400)


def clear_to_alert(key: str, chain: str, pair: dict) -> bool:
    """The last check before an alert goes out: the coin is not a steady asset. (If that is not known yet, it is looked up
    now; if the look-up is busy, the alert simply waits for a later scan.)"""
    return ensure_steady(key, chain, pair) is False


def store_peak(key: str, peak: float):
    """Save a coin's past high. A steady asset has no real one (the number would only be bad data), so its is simply 'now'."""
    if key in state["steady"]:
        state["peaks"][key] = peak
    else:
        state["peaks"][key] = max(state["peaks"].get(key, 0.0), peak)


def peak_from_candles(key: str | None, pair: dict, candles) -> float | None:
    """A coin's past high as a market cap from its daily candles: highest daily high x (current market cap / current price).
    A steady asset has no real past high to fall from, so its market cap today is returned."""
    price = num(pair, "priceUsd")
    mc = pair_mc(key, pair) if key else (num(pair, "marketCap") or num(pair, "fdv"))
    if not (price > 0 and mc > 0):
        return None
    if key and key in state["steady"]:
        return mc
    highs = [float(c[2]) for c in candles if len(c) >= 3]
    return max(highs) * (mc / price) if highs else None


def fetch_peak_mc(chain: str, pair: dict, key: str | None = None):
    """
    Estimate all-time-high market cap from GeckoTerminal daily candles:
    highest daily high x (current market cap / current price).
    The same candles tell whether the coin is a steady asset (see above).
    """
    if not (GECKO_NETWORK.get(chain) and pair.get("pairAddress") and num(pair, "priceUsd") > 0):
        return None
    candles = daily_candles(chain, pair)
    if key:
        note_steady(key, steady_from_candles(candles))
    return peak_from_candles(key, pair, candles)


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
        store_peak(key, peak)
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
    _slow_round["n"] += 1
    if not watch:
        log.info("Nothing to scan yet - waiting for discovery to find coins")
        return
    now = time.time()
    manual = set(TOKENS)
    budget = [MAX_BACKFILL_PER_CYCLE]
    probes = [STEADY_PROBES_PER_SCAN]   # coins whose daily history may be looked at during this scan, to find steady assets
    caps = [BUYER_PER_SCAN]        # coins whose buyers may be looked at during this scan
    book_index()                   # which wallet buys are being followed (by coin)
    chart_cands: list = []         # coins whose chart may be read / whose buyers may be looked at once the pass is done
    accum_cands: list = []         # v3.4: coins that several of your watched wallets keep adding to (looked at once the pass is done)
    lowcap_cands: list = []        # v3.5: coins that several saved wallets bought (checked for the low-cap crowd alert once the pass is done)
    link_cands: list = []          # v3.8: coins you sent that just reached LINK_LOOK_X times the market cap you sent them at (their wallets are looked at once the pass is done)
    try:
        accum_keys = accum_candidates(now)
    except Exception:
        accum_keys = set()
        log.exception("wallet accumulation check error")
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
                    if _book_idx.get(key):
                        book_track(key, None, None, now)
                    if key in state["links"]:
                        try:
                            link_track(key, 0.0, None, None, now, link_cands)
                        except Exception:
                            log.exception("link tracking error")
                    if open_records(key):
                        try:
                            track(key, 0.0, now)  # liquidity gone -> counts as a rug once it has lasted a few minutes
                        except Exception:
                            log.exception("tracking error for %s", key)
                    elif not is_manual and not bottom_entry(key):
                        _missing[key] += 1
                        if _missing[key] >= 2:  # gone on two scans in a row: not just a one-off hiccup
                            _missing.pop(key, None)
                            dismiss(key)
                    continue
                _missing.pop(key, None)
                mc = pair_mc(key, pair)
                note_mc(key, mc, num(pair, "liquidity", "usd"), age_days_of(pair))     # v3.5: market caps shown next to coins in the chat come from here
                sym = pair["baseToken"].get("symbol", "?")
                # v3.1: a coin that barely moves (stock token, gold, wrapped coin, stable) is not a runner. Coins that matter
                # (followed, bought by a saved wallet, on Sparsity's map, or big enough to read) get one look at their daily
                # history, a few per scan; a steady one is left out of everything from here on (this comes first, so an odd
                # price on such a coin can never end a call or a wallet's buy as a "win").
                if (steady_due(key, now) and probes[0] > 0 and gecko_backlog() < 12
                        and (open_records(key) or _book_idx.get(key) or bottom_entry(key) or chart_eligible(chain, pair, mc))):
                    probes[0] -= 1
                    ensure_steady(key, chain, pair)
                if key in state["steady"]:
                    if not is_manual:
                        dismiss(key)
                    continue
                try:
                    track(key, mc, now, pair, pairs_here)  # update outcomes of earlier calls on this coin
                except Exception:
                    log.exception("tracking error for %s", key)       # (v3.8: one odd coin must never stop the whole scan)
                if key in state["links"]:
                    try:
                        link_track(key, mc, pair, pairs_here, now, link_cands)       # v3.8: ...and the coins you sent
                    except Exception:
                        log.exception("link tracking error")
                if _book_idx.get(key):
                    book_track(key, pair, pairs_here, now)   # ...and of the buys of wallets in the wallet book
                    if len(_book_idx[key]) >= LOWCAP_MIN_WALLETS and mc > 0:
                        lowcap_cands.append((key, chain, pair, mc))      # v3.5: several saved wallets are in it
                if key in accum_keys:
                    accum_cands.append((key, chain, pair, mc))   # v3.4: your wallets keep adding to it

                bt = bottom_entry(key)   # Sparsity lists this coin right now: never thrown out for size or age
                picked = bool(_book_idx.get(key))
                ce = chart_eligible(chain, pair, mc)     # big and liquid enough for its chart to be read
                if ce or bt:                             # (every coin on Sparsity's map is read and looked at, whatever its size)
                    chart_cands.append((key, chain, pair, mc))
                if not is_manual and not bt and mc > MAX_COIN_MC and not open_records(key) and not picked:
                    dismiss(key)
                    continue
                if not is_manual and not bt and too_old(pair) and not open_records(key) and not picked:
                    bot_stats["old"] += 1
                    dismiss(key)  # older than the age limit (a year) - and it can only get older
                    continue
                if not is_manual and not bt and mc < mc_floor() and not open_records(key):
                    # too small for a re-run setup. A themed coin goes to the narrative radar, a coin whose chart can be
                    # read stays for the chart reader, anything else is dropped
                    if alert_on("narrative") and mc >= NARR_MIN_MC and themes_for(key, pair):
                        maybe_narrative(key, chain, pair, now)
                    elif not ce and not picked:
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
                        if alert_on("walletdip") and state["watch"]:
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
                if not alert_on("rerun"):
                    # v3.0: a re-run setup is no longer sent to your chat. It is still followed in silence and scored (so the
                    # report keeps showing how this idea does), and it is only sent when wallets you trust are holding it.
                    if not clear_to_alert(key, chain, pair):
                        continue      # v3.1: a steady asset, or not checked yet (the check was busy): not followed this time
                    state["cooldowns"][key] = now
                    if alert_on("walletdip") and wallet_backed_rerun(key, chain, pair, m, held, sig, now):
                        alerts += 1
                    else:
                        add_record("call", key, chain, pair, m, "", now, extra={"sig": sig, "quiet": True})
                        bot_stats["rerun_quiet"] += 1
                        log.info("re-run setup for %s followed in silence (that alert is switched off)", key)
                    continue
                if not clear_to_alert(key, chain, pair):
                    continue          # a steady asset, or not checked yet (the check was busy): no alert this time
                if send_telegram(build_rerun_message(chain, pair, m, extra)):
                    state["cooldowns"][key] = now
                    add_record("call", key, chain, pair, m, "", now, extra={"sig": sig})
                    alerts += 1
                    bot_stats["rerun"] += 1
                    log.info("ALERT sent for %s", key)

    try:
        chart_pass(chart_cands, now)          # read a few charts, send chart-setup alerts
    except Exception:
        log.exception("chart pass error")
    try:
        capture_pass(chart_cands, now, caps)  # who is buying the other coins (whatever is left of this scan's allowance)
    except Exception:
        log.exception("buyer capture pass error")
    try:
        slow_accum_pass(accum_cands, now)     # v3.4: several of your wallets keep adding to a coin over days
    except Exception:
        log.exception("slow-accumulation pass error")
    try:
        lowcap_pass(lowcap_cands, now)        # v3.5: several saved wallets bought the same low-cap coin
    except Exception:
        log.exception("low-cap crowd pass error")
    try:
        link_pass(link_cands, now)            # v3.8: a coin you sent reached 2x: who was in early?
    except Exception:
        log.exception("link pass error")
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
ALERT_KINDS = ("call", "narrative", "wallet", "bottom", "chart", "smartbuy", "vip", "lowcap", "kol", "radar")   # records that were sent to you as alerts (the rest are silent experiments)


def open_records(key: str | None = None, kind: str | None = None) -> list[dict]:
    return [r for r in state["records"] if r["status"] == "open"
            and (key is None or r["key"] == key) and (kind is None or r["kind"] == kind)]


def fresh_open(kind: str) -> list[dict]:
    """Open follow-ups of this kind that are still in their first window (v3.8: the ones that were given extra time do not use up the
    room for new experiments)."""
    return [r for r in state["records"] if r["status"] == "open" and r["kind"] == kind and not r.get("ext")]


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
    if key in state["steady"] and kind != "check":
        return                   # v3.1: a steady asset (stock token, gold, wrapped coin, stable) is never followed
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
    if len(fresh_open("shadow")) >= MAX_SHADOWS_OPEN:
        return
    if not clear_to_alert(key, chain, pair):
        return                        # v3.1: a steady asset (or not checked yet): not worth an experiment
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


def extend_cap_hours(mc0: float, kind: str = "call") -> float:
    """v3.8: the longest a follow-up may be extended, in hours - by the size of the coin when it was called (bigger coins need longer to
    double): up to $1M 21 days, up to $10M 30 days, bigger 45 days; a coin you sent me (kind 'check') up to EXTEND_LINK_DAYS."""
    if kind == "check":
        return EXTEND_LINK_DAYS * 24
    for limit, days in EXTEND_DAYS:
        if mc0 < limit:
            return days * 24
    return EXTEND_DAYS[-1][1] * 24


def follow_text(hours: float, mc: float = 0.0, kind: str = "call") -> str:
    """'7 days, and longer while it holds up (up to 21 days)' - how long a call is followed."""
    base = span_text(hours)
    cap = extend_cap_hours(mc or 0.0, kind)
    if not EXTEND_ON or cap <= hours + 1:
        return base
    return f"{base}, and longer while it holds up (up to {cap / 24:g} days)"


def extended_open() -> int:
    """How many follow-ups (calls, wallet buys) are open on extra time right now."""
    n = sum(1 for r in state["records"] if r["status"] == "open" and r.get("ext"))
    for d in state["book"].values():
        n += sum(1 for p in d.get("picks", []) if isinstance(p, dict) and p.get("st") == "o" and p.get("ext"))
    return n


def extend_followup(r: dict, cur: float, now: float) -> bool:
    """v3.8: a follow-up's window has ended and the coin is still ALIVE (at least EXTEND_ALIVE x the call price): instead of closing it
    'flat' it gets another EXTEND_STEP_HOURS, up to the limit for a coin of that size (extend_cap_hours). While extended it is priced less
    often (the slow lane) and it ends early if the coin breaks below the lowest point of its first window (see track). True = extended."""
    if not EXTEND_ON or r.get("void") or r.get("kind") == "whale" or cur <= 0 or r.get("mc0", 0) <= 0:
        return False
    th = float(r.get("th") or TRACK_HOURS)
    cap = extend_cap_hours(r["mc0"], r.get("kind", "call"))
    if th >= cap - 1:
        return False
    if cur < EXTEND_ALIVE * r["mc0"]:
        return False
    if not r.get("ext") and extended_open() >= EXTEND_MAX_OPEN:
        return False
    r["th"] = min(th + EXTEND_STEP_HOURS, cap)
    r["ext"] = int(r.get("ext") or 0) + 1        # (ext = how many times it was given another week; above 0 = it is on extra time = priced in the slow lane)
    if not r.get("lo1"):
        r["lo1"] = r.get("min_mc") or cur       # the lowest point of the first window: breaking under it ends the follow-up
    log.info("%s %s still alive at %.2fx after %s: followed %s more (until day %.0f of at most %.0f)", r.get("kind"), r.get("key"),
             cur / r["mc0"], ago(now - r["t0"]), span_text(EXTEND_STEP_HOURS), r["th"] / 24, cap / 24)
    return True


def track(key: str, mc: float, now: float, pair: dict | None = None, pairs: list[dict] | None = None):
    """Update every open record on this coin; close it on win / stop / expiry. pair=None means no live pool.
    A stop or a win only counts when TWO readings in a row agree, so one odd reading can't end a call.
    v3.8: a call that is still alive when its window ends is followed on (extend_followup) until it wins, is stopped, runs out of
    extra time, or breaks below the lowest point of its first window (then it ends as 'flat', with the reason noted)."""
    for r in open_records(key):
        cur = read_mc(r, mc, pair, pairs)
        hours = float(r.get("th") or TRACK_HOURS)      # bottom calls are followed longer than re-runs
        if cur is None:  # nothing usable this time - only the clock can end the call
            if now - r["t0"] >= hours * 3600 and not extend_followup(r, float(r.get("last", r["mc0"])), now):
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
        # (v3.6: a radar alert or a picky wallet's buy of a bigger coin carries its own, gentler win / stop marks: 'wx' and 'sx')
        win_x = float(r["wx"]) if isinstance(r.get("wx"), (int, float)) and r["wx"] > 1 else WIN_MULTIPLE
        stop_x = float(r["sx"]) if isinstance(r.get("sx"), (int, float)) and 0 < r["sx"] < 1 else STOP_MULTIPLE
        # "no live pool" must last a few minutes before it counts as a rug (a data outage looks the same)
        if hi <= stop_x * r["mc0"] and (cur > 0 or now - r["z"] >= RUG_CONFIRM_SECONDS):
            log.info("STOP %s: reading %s (%.2fx) twice in a row; pool %s", key, fmt_usd(cur),
                     cur / r["mc0"], r.get("pa") or "?")
            close_record(r, "stop", cur, now)
        elif r["max_mc"] >= win_x * r["mc0"]:
            close_record(r, "win", cur, now)
        elif r.get("lo1") and cur > 0 and hi <= EXTEND_BREAK * r["lo1"]:
            r["why"] = "broke"                   # v3.8: on extra time and now under the lowest point of its first window, two readings in a row
            close_record(r, "expired", cur, now)
        elif now - r["t0"] >= hours * 3600:
            if not extend_followup(r, cur, now):
                close_record(r, "expired", cur, now)
        elif cur <= stop_x * r["mc0"]:
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
    if r["kind"] not in ALERT_KINDS or r.get("quiet"):
        return  # experiments (and setups followed in silence) close silently
    tag = {"narrative": "NARRATIVE ", "wallet": "WALLET-BACKED ", "bottom": "BOTTOM ", "chart": "CHART ",
           "smartbuy": "SMART-WALLET ", "vip": "VIP LOW-CAP ", "lowcap": "LOW-CAP CROWD ", "kol": "DIP-WATCH ",
           "radar": "ACCUMULATION "}.get(r["kind"], "")
    broke = outcome == "expired" and r.get("why") == "broke"
    head = {"win": f"🎯 *{tag}CALL HIT*", "stop": f"🛑 *{tag}CALL STOPPED OUT*",
            "expired": f"🪫 *{tag}CALL ENDED*" if broke else f"⏱ *{tag}CALL EXPIRED*"}[outcome]
    fast = ("\n⚡ That was very fast - look at the chart: it is either a rug or bad price data."
            if outcome == "stop" and now - r["t0"] < 900 else "")
    if broke:                                       # v3.8
        fast = "\n📉 It fell under the lowest point of its first week, so I stopped following it."
    elif outcome == "win" and now - r["t0"] > TRACK_HOURS * 3600 * 1.05:
        fast = f"\n⏳ It needed more than {span_text(TRACK_HOURS)} - this is why I now keep following a call that is still alive."
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
        L.append(f"Re-run setups finished: *{c['n']}*  ✅ {c['w']} wins | 🛑 {c['s']} stopped | ⏱ {c['e']} flat")
        L.append(f"Win rate: *{c['wr']:.0%}* | average best move: *{c['avg_best']:.2f}x*")
        quiet_n = sum(1 for r in calls if r.get("quiet"))
        if quiet_n:
            L.append(f"(Since v3.0 these are no longer sent to you: {quiet_n} of the {c['n']} were followed quietly.)")
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
    L.append(f"(Win = reaches {WIN_MULTIPLE:g}x before falling to {STOP_MULTIPLE:g}x, within {span_text(TRACK_HOURS)}; "
             f"a wallet's buy gets {span_text(WALLET_TRACK_HOURS)})")

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
    steady_n = sum(1 for r in state["records"] if r.get("void") and r["key"] in state["steady"])
    set_aside = sum(1 for r in state["records"] if r.get("void")) - steady_n
    if set_aside:
        L += ["", f"🧹 {set_aside} older result(s) from before the price-glitch fixes were set aside - not counted."]
    if state["steady"]:
        L += ["", f"🪙 {len(state['steady'])} coin(s) that barely move (stock tokens, gold, wrapped coins) are left out: their "
                  f"\"dips\" and \"wins\" were bad price data" + (f" ({steady_n} result(s) taken back out)." if steady_n else ".")]
    L += ["", f"Following now: {len(open_records(kind='call'))} calls, {len(open_records(kind='narrative'))} "
              f"narrative, {len(open_records(kind='wallet'))} wallet-backed, {len(open_records(kind='bottom'))} bottom, "
              f"{len(open_records(kind='chart'))} chart, {len(open_records(kind='smartbuy'))} smart-wallet, "
              f"{sum(len(open_records(kind=k_)) for k_ in ('shadow', 'bshadow', 'cshadow', 'lshadow'))} "
              f"experiments, {len(open_records(kind='whale'))} wallet buys" + (f" ({extended_open()} of all these on extra time)" if extended_open() else "")]

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


def build_report_part2() -> str:
    """The second report message: which chart shapes paid, and how the wallet book is doing."""
    L = lowcap_report_lines() + chart_report_lines() + book_report_lines() + scorecard_report_lines()      # (v3.5: the low-cap numbers come first)
    if not L:
        return ""
    L += ["", f"(Charts are read for coins of {fmt_usd(CHART_MIN_MC)}+; a wallet buy is followed from the price the wallet paid.)"]
    return "\n".join(["📊 *CHARTS & WALLETS*", ""] + L)[:7500]       # (v3.8: longer than one message is possible now - it is sent in parts, cut at line ends)


def learning_cycle():
    now = time.time()
    tun = state["tuning"]
    closed_alerts = sum(1 for k in ALERT_KINDS for r in closed_records(k) if not r.get("quiet"))
    due_calls = closed_alerts - tun.get("reported_calls", 0) >= REPORT_EVERY_CALLS
    total_closed = sum(1 for r in state["records"] if r["status"] != "open" and not r.get("void"))
    due_week = total_closed >= 5 and now - tun.get("last_report", 0) >= 7 * 86400
    if due_calls or due_week:
        sugg = suggestions()
        applied = apply_tuning(sugg)
        if send_telegram(build_report(sugg, applied)):
            tun["reported_calls"], tun["last_report"] = closed_alerts, now
            try:
                extra = build_report_part2()
                if extra:
                    send_telegram_long(extra)
            except Exception:
                log.exception("second part of the report failed")
    # housekeeping: forget old finished records, stale wallet buys and unused descriptions
    cutoff = now - 45 * 86400
    recs = [r for r in state["records"] if r["status"] == "open" or (r["t1"] or 0) > cutoff]
    drop = len(recs) - 1500
    if drop > 0:                      # (v3.8: a follow-up still open on extra time is never the one that goes - the oldest FINISHED results go first)
        keep_recs = []
        for r in recs:
            if drop > 0 and r["status"] != "open":
                drop -= 1
                continue
            keep_recs.append(r)
        recs = keep_recs
    state["records"] = recs
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
    open_keys = {r["key"] for r in state["records"] if r["status"] == "open"}
    for k in [k for k, t in state["steady_seen"].items()
              if k not in state["steady"] and now - t > 60 * 86400 and k not in state["discovered"] and k not in open_keys]:
        state["steady_seen"].pop(k, None)     # an old "not steady" answer: it is simply asked again if the coin comes back
    for k in [k for k, c in state["charts"].items()
              if now - max(c.get("seen", 0), c.get("t", 0)) > CHART_KEEP_HOURS * 3600 and k not in open_keys]:
        state["charts"].pop(k, None)      # a coin that has not been in the scan for days: its chart notes are forgotten
    try:
        book_prune(now)
    except Exception:
        log.exception("wallet book pruning error")
    try:
        promote_smart_wallets()
    except Exception:
        log.exception("smart-wallet promotion error")
    try:
        wallet_checkup(now)
    except Exception:
        log.exception("wallet check-up error")


# ----------------------------------------------------------------------------
# NARRATIVE RADAR: themed coins (stocks, AI, politics...) waking up BEFORE a big run
# ----------------------------------------------------------------------------
def maybe_narrative(key: str, chain: str, pair: dict, now: float, m: dict | None = None) -> bool:
    if not alert_on("narrative"):
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
    if not clear_to_alert(key, chain, pair):
        return False                  # v3.1: a steady asset (a stock token, say) is not a theme play
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
        + f"\n⚠️ Early-stage theme play, NOT a re-run setup - higher risk. Tracked for {follow_text(TRACK_HOURS, m['mc'])}.\n"
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
                store_peak(key, peak)
                state["backfilled"][key] = time.time()
        except (requests.RequestException, KeyError, ValueError, TypeError):
            pass
    m = compute_metrics(key, pair)
    return {"pair": pair, "m": m, "key": key} if m else None


def handle_whale_swap(swap: dict, quiet: bool = False) -> bool:
    """Send the whale alert, tag re-run accumulation, detect clusters, and start scoring this wallet.
    quiet = (v3.2) everything happens except the one-wallet message itself (the daily limit on those was reached)."""
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
            extra = (f"💰 Market cap *{fmt_usd(m['mc'])}* · 📉 {m['retrace']:.0f}% below its peak of {fmt_usd(m['peak'])}\n")   # (v3.5: the market cap comes first)
            note_mc(key, m["mc"], m["liq"])
            if zone:
                extra = "🔥 *SMART MONEY RE-RUN ACCUMULATION*\n" + extra
            add_record("whale", key, chain, ctx["pair"], m, "", now, extra={"wallet": wallet, "zone": zone, "th": WALLET_TRACK_HOURS})
    ok = False if quiet else send_telegram(build_whale_message(swap, extra))
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
    state["accum"].pop(key, None)       # v3.4: ...and the notes of what it added
    cur = state["tuning"].get("vip_cur")
    if isinstance(cur, dict):
        cur.pop(key, None)              # v3.5: ...and the place where the last look at a VIP wallet stopped
    buys = state["tuning"].get("vip_buys")
    if isinstance(buys, dict):
        buys.pop(key, None)             # v3.6: ...and the log of the coins it bought (its style)
    _snaps.pop(key, None)
    for k in [k for k in list(_hold_cache) if k[0] == key]:
        _hold_cache.pop(k, None)
    gone = state["tuning"].setdefault("watch_removed", [])
    if key not in gone:
        gone.append(key)    # so that WATCH_WALLETS does not bring it back after a restart


def seed_watch_wallets():
    """The built-in wallet list (v3.2, grown in v3.3) and WATCH_WALLETS="Whale1:address,address2" join the watch list - an extra on top of
    adding wallets in the chat. A wallet you removed with /unwatch stays removed, and one already on the list keeps your name."""
    gone = set(state["tuning"].get("watch_removed") or [])
    STARTER_ADDED.clear()
    if STARTER_WALLETS_ON:
        for name, addr in STARTER_WALLETS:
            kind = wallet_kind(addr)
            key = addr.lower() if kind == "evm" else addr
            if kind and key not in gone and key not in state["watch"]:
                add_watch(addr, name)
                STARTER_ADDED.append(name)
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
    {"in": [(name, usd, amount)], "out": [names], "unknown": [names], "total": how many wallets could hold it,
     "in_w": the wallet address behind each entry of "in" (v3.8.1)}
    An earlier answer younger than max_age seconds is reused instead of asking the blockchain again."""
    res = {"in": [], "out": [], "unknown": [], "total": 0, "in_w": []}
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
                res["in_w"].append(w)               # v3.8.1: the wallet behind each name (the scorecard skips a 🧊 cooling one)
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


def _last_day(stamps, now: float) -> list:
    """The time stamps from the last 24 hours (anything odd in a damaged memory file is simply dropped)."""
    return [t for t in (stamps if isinstance(stamps, list) else []) if isinstance(t, (int, float)) and now - t < 86400]


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
    # v3.2: busy wallets can move a lot of coins in a day. After WATCH_MOVES_MAX_PER_DAY messages in 24 hours the rest of
    # the moves are still followed and scored (and a cluster of wallets buying one coin still gets its alert) - just not announced one by one.
    stamps = _last_day(state["tuning"].get("wmove_ts"), now)
    state["tuning"]["wmove_ts"] = stamps
    # v3.3: ...and one very busy wallet may not use up the whole day's messages: at most WATCH_MOVES_PER_WALLET per wallet per 24h
    by = state["tuning"].get("wmove_by")
    if not isinstance(by, dict):
        by = state["tuning"]["wmove_by"] = {}
    for w_ in list(by):
        by[w_] = _last_day(by[w_], now)
        if not by[w_]:
            del by[w_]
    mine = by.get(wallet, [])
    day_full = WATCH_MOVES_MAX_PER_DAY > 0 and len(stamps) >= WATCH_MOVES_MAX_PER_DAY
    wallet_full = WATCH_MOVES_PER_WALLET > 0 and len(mine) >= WATCH_MOVES_PER_WALLET
    quiet = day_full or wallet_full
    if day_full and now - state["cooldowns"].get("wmove_cap", 0) > 20 * 3600:
        state["cooldowns"]["wmove_cap"] = now
        send_telegram(
            f"🔕 *Wallet moves: quiet for now.* That was {len(stamps)} 'wallet bought / sold' messages in 24 hours, so I'll stop "
            "sending them one by one until some of those hours have passed. I still follow every coin your wallets buy, still "
            "score them, and still warn you when two or more of them buy the same coin together. "
            "(To allow more per day, change the `WATCH_MOVES_MAX_PER_DAY` setting on Render, or ask Claude.)")
    ok = handle_whale_swap(swap, quiet=quiet)
    if ok:
        stamps.append(now)
        if WATCH_MOVES_PER_WALLET > 0:
            mine.append(now)
            by[wallet] = mine
    return ok


def wallet_sweep() -> int:
    """Look inside every watched Solana wallet. Coins they hold join the list the scanner follows; a coin that is
    new in a wallet (or that it mostly dumped) is announced. The first look at a wallet only sets the baseline."""
    now, sent = time.time(), 0
    for w in [w for w, d in list(state["watch"].items()) if d.get("kind") == "solana" and not (VIP_SWEEPS_ON and d.get("vip"))]:   # (v3.5: the VIP wallets have their own, faster look)
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
        # v3.4: a coin whose amount changed by even a little is priced too, so that small top-ups can be added up over days
        # (a single look only catches a big jump: a new position, or +50%)
        moved = [] if first else accum_moved(snap, prev)
        priced = price_coins(need + [m_ for m_ in moved if m_ not in need])
        if moved:
            try:
                accum_note(w, snap, prev, moved, priced, now)
                for mint in moved:      # a coin it only topped up a little still joins the scanner's list once the position is big enough
                    pair = priced.get(mint) if mint not in need else None
                    key = sk("solana", mint)
                    if (pair and snap.get(mint, 0.0) * num(pair, "priceUsd") >= WATCH_MIN_HOLD_USD
                            and pool_floor() <= (num(pair, "marketCap") or num(pair, "fdv")) <= MAX_COIN_MC and not too_old(pair)
                            and key not in state["discovered"] and key not in state["dismissed"] and not manual_coin("solana", mint)):
                        state["discovered"][key] = now
            except Exception:
                log.exception("could not note a wallet's changes")
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
    try:                                                     # v3.4: tidy the notes of what the wallets changed
        for w_ in list(state["accum"]):
            if w_ in state["watch"]:
                accum_trim(w_, now)
            else:
                state["accum"].pop(w_, None)                 # (a wallet that is not watched any more)
    except Exception:
        log.exception("could not tidy the wallet change notes")
    return sent


# --- v3.4: wallets that keep ADDING to a coin over several days ---
# One look inside a wallet only catches a big jump (a new coin, or +50% since the last look). A wallet that builds a position in
# many small top-ups over days never trips that. So what each watched wallet changes is written down (in the memory file, so it
# survives every update) and added up over the last ACCUM_DAYS days. Solana wallets only: EVM wallets cannot be looked into for free.
def accum_moved(snap: dict, prev: dict) -> list:
    """Coins whose amount in a wallet changed by at least ACCUM_STEP_MIN of what it holds (less than that is dust or rounding)."""
    out = []
    for mint in set(snap) | set(prev):
        a, b = snap.get(mint, 0.0), prev.get(mint, 0.0)
        if a != b and abs(a - b) >= ACCUM_STEP_MIN * max(a, b) and not is_quote_token("solana", mint):
            out.append(mint)
    return out


def accum_note(wallet: str, snap: dict, prev: dict, moved: list, priced: dict, now: float):
    """Write down what changed in a watched wallet since the last look: [when, change in tokens] per coin. Only coins with a live
    pool and a change worth at least ACCUM_NOTE_MIN_USD are kept (junk airdrops and dust are not worth remembering)."""
    ledger = state["accum"].setdefault(wallet, {})
    for mint in moved:
        pair = priced.get(mint)
        px = num(pair, "priceUsd") if pair else 0.0
        d = snap.get(mint, 0.0) - prev.get(mint, 0.0)
        if px > 0 and abs(d) * px >= ACCUM_NOTE_MIN_USD:
            ledger.setdefault(mint, []).append([round(now), float(f"{d:.8g}")])
    accum_trim(wallet, now)


def accum_trim(wallet: str, now: float):
    """Forget changes older than ACCUM_DAYS days. A coin with too many notes gets its two oldest merged into one."""
    ledger = state["accum"].get(wallet)
    if not isinstance(ledger, dict):
        state["accum"].pop(wallet, None)
        return
    horizon = now - ACCUM_DAYS * 86400
    for mint in list(ledger):
        rows = [r for r in ledger[mint] if r[0] >= horizon]
        while len(rows) > ACCUM_MAX_ROWS:
            rows[1] = [rows[1][0], rows[0][1] + rows[1][1]]
            del rows[0]
        if rows:
            ledger[mint] = rows
        else:
            del ledger[mint]
    if not ledger:
        state["accum"].pop(wallet, None)


def accum_view(mint: str, now: float, since: float = 0.0) -> dict:
    """Which watched wallets have been adding to this coin over the last ACCUM_DAYS days (what they sold is taken off):
    {wallet: {"net": coins added, "held": coins held now, "steps": separate top-ups, "first": when the first top-up was}}.
    A wallet only counts while it still holds the coin, and when what it added is a real share of what it holds.
    since = only look at changes after this time (a coin that was already announced needs NEW adds to be announced again)."""
    out: dict = {}
    horizon = max(now - ACCUM_DAYS * 86400, since)
    for w, coins in list(state["accum"].items()):
        rows = [r for r in list((coins or {}).get(mint) or []) if r[0] >= horizon]
        ups = [r for r in rows if r[1] > 0]
        net = sum(r[1] for r in rows)
        snap = _snaps.get(w)
        held = snap[1].get(mint, 0.0) if snap else 0.0
        if not ups or net <= 0 or held <= 0:
            continue
        before = max(0.0, held - net)
        if before > 0 and net < ACCUM_MIN_GROWTH * before:
            continue                      # a big holder adding a sliver of its position is not "accumulating"
        out[w] = {"net": net, "held": held, "steps": len(ups), "first": min(r[0] for r in ups)}
    return out


def accum_candidates(now: float) -> set:
    """Coins (as state keys) worth a closer look this scan: two or more watched wallets adding to the same one (at least one of
    them in more than one step), or one wallet adding in several separate steps. (A quick check on coin counts only - what it is
    worth in dollars is looked at later.)"""
    if not (ACCUM_ON and state["accum"]):
        return set()
    mints: set = set()
    for coins in list(state["accum"].values()):
        if isinstance(coins, dict):
            mints.update(list(coins))
    out = set()
    for mint in mints:
        steps = sorted((x["steps"] for x in accum_view(mint, now).values()), reverse=True)
        if steps and ((len(steps) >= ACCUM_MIN_WALLETS and steps[0] >= 2) or steps[0] >= ACCUM_SOLO_STEPS):
            out.add(sk("solana", mint))
    return out


def accum_lines(pair: dict) -> list:
    """For the scorecard: what the wallets you watch added to / sold from this coin over the last ACCUM_DAYS days (from the notes
    taken at every look inside a wallet). Information only - it never changes the points."""
    try:
        px = num(pair, "priceUsd")
        mint = (pair.get("baseToken") or {}).get("address", "")
        if not (ACCUM_ON and px > 0 and mint and state["accum"]):
            return []
        horizon = time.time() - ACCUM_DAYS * 86400
        adds, sells = [], []
        for w, coins in list(state["accum"].items()):
            rows = [r for r in list((coins or {}).get(mint) or []) if r[0] >= horizon]
            up, down = sum(r[1] for r in rows if r[1] > 0) * px, -sum(r[1] for r in rows if r[1] < 0) * px
            if up >= ACCUM_SHOW_USD:
                adds.append((up, wallet_label(w)))
            if down >= ACCUM_SHOW_USD:
                sells.append((down, wallet_label(w)))
        span, out = span_text(ACCUM_DAYS * 24), []
        if adds:
            out.append(f"👛 In the last {span} wallets you watch added: "
                       + ", ".join(f"{md_escape(n)} {fmt_usd(u)}" for u, n in sorted(adds, reverse=True)[:4]))
        if sells:
            out.append((f"🔻 ...and these sold: " if adds else f"🔻 In the last {span} wallets you watch sold: ")
                       + ", ".join(f"{md_escape(n)} {fmt_usd(u)}" for u, n in sorted(sells, reverse=True)[:4]))
        return out
    except Exception:
        return []


def since_text(seconds: float) -> str:
    """'40 min', '5 h' or '3 days': how long ago something was."""
    if seconds < 3600:
        return f"{max(1, round(seconds / 60))} min"
    if seconds < 48 * 3600:
        return f"{seconds / 3600:.0f} h"
    return f"{seconds / 86400:.0f} days"


def slow_accum_pass(cands: list, now: float):
    """Every scan: a coin that several of your wallets (or one wallet, step by step) keep adding to gets one message."""
    if not (ACCUM_ON and alert_on("smartbuy")):
        return
    for key, chain, pair, mc in cands:
        if chain != "solana":
            continue
        if (now - state["cooldowns"].get("sbs:" + key, 0) < ACCUM_COOLDOWN_DAYS * 86400
                or now - state["cooldowns"].get("sb:" + key, 0) < 24 * 3600):
            continue                      # announced lately (as a slow build-up, or as a smart-wallet buy)
        try:
            maybe_slow_accum(key, chain, pair, now)
        except Exception:
            log.exception("slow-accumulation check error")


def maybe_slow_accum(key: str, chain: str, pair: dict, now: float) -> bool:
    mint = key.split(":", 1)[1]
    px = num(pair, "priceUsd")
    if px <= 0 or num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD or too_old(pair):
        return False
    since = state["cooldowns"].get("sbs:" + key, 0.0)       # announced before? then only NEW adds count
    rows = []                             # (wallet, $ added, $ held now, top-ups, when the first was)
    for w, x in accum_view(mint, now, since).items():
        added, held_usd = x["net"] * px, x["held"] * px
        if added >= ACCUM_MIN_WALLET_USD and held_usd >= WATCH_MIN_HOLD_USD:
            rows.append((w, added, held_usd, x["steps"], x["first"]))
    # several wallets adding (one of them more than once: a single buy by each is already announced as it happens), or ONE wallet adding a lot in several steps
    many = len(rows) >= ACCUM_MIN_WALLETS and max((r[3] for r in rows), default=0) >= 2
    solo = any(r[1] >= ACCUM_SOLO_USD and r[3] >= ACCUM_SOLO_STEPS for r in rows)
    if not (many or solo):
        return False
    rows.sort(key=lambda r: -r[1])
    # a bottom or chart alert for this coin just went out (or is on its way): it already shows who is buying
    b = state["bottoms"].get(key)
    if now - state["cooldowns"].get("chart:" + key, 0) < 12 * 3600 or (b and now - b.get("alerted", 0) < 12 * 3600):
        return False
    if bottom_alert_coming(key, now) or chart_alert_coming(key, now):
        return False
    stamps = _last_day(state["tuning"].get("sbs_ts"), now)
    state["tuning"]["sbs_ts"] = stamps
    if len(stamps) >= ACCUM_MAX_PER_DAY:
        return False
    m = compute_metrics(key, pair)
    if not m or m["mc"] < pool_floor() or (m["mc"] > MAX_COIN_MC and not manual_coin(chain, mint)):
        return False
    if not confirmed_twice("sbs", key, m["mc"], now):
        return False
    if not clear_to_alert(key, chain, pair):
        return False                      # a steady asset (or not checked yet)
    held = watch_holders(chain, mint, px, max_age=1800)
    shown = rows[:5]
    if not send_telegram(build_slow_accum_message(chain, pair, m, shown, held, now, since)):
        return False
    state["cooldowns"]["sbs:" + key] = now
    state["cooldowns"]["sb:" + key] = now    # (the ordinary smart-wallet message waits too: one message, not two)
    stamps.append(now)
    c = state["charts"].get(key) or {}
    add_record("smartbuy", key, chain, pair, m, "", now,
               extra={"th": WALLET_TRACK_HOURS, "sig": signals_for(m, len(held["in"])), "strong": len(rows), "soft": 0,
                      "wal": [r[0] for r in shown][:4], "signs": (b.get("signs") if b and bottom_active(b) else None),
                      "pat": c.get("pat", "") if c.get("ok") else "", "slow": True})
    bot_stats["smartbuy"] += 1
    log.info("SLOW-ACCUMULATION alert for %s (%d wallets adding)", key, len(rows))
    return True


def build_slow_accum_message(chain: str, pair: dict, m: dict, rows: list, held: dict, now: float, since: float = 0.0) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(base.get("symbol", "?"))
    key = sk(chain, addr)
    span = (f"since my last message about it ({since_text(now - since)} ago)" if since > 0
            else f"over the last {span_text(ACCUM_DAYS * 24)}")
    L = [f"*🧠 SMART WALLETS KEEP BUYING: ${sym}*  |  {CHAINS[chain]['name']}", "",
         (f"{len(rows)} of your watched wallets have been adding to it {span}:" if len(rows) > 1 else
          f"One of your watched wallets has been adding to it, step by step, {span}:")]
    for w, added, held_usd, steps, first in rows:
        when = since_text(now - first)
        L.append(f"• *{md_escape(wallet_label(w))}* added about {fmt_usd(added)}"
                 + (f" in {steps} top-ups, the first {when} ago" if steps > 1 else f", {when} ago")
                 + f" · holds {fmt_usd(held_usd)} now")
    if len(rows) > 1:
        L.append(f"   Together about {fmt_usd(sum(r[1] for r in rows))} added")
    down = f"down {m['retrace']:.0f}% from its high of {fmt_usd(m['peak'])}" if m["retrace"] >= 5 else "close to its high"
    L += ["",
          f"💰 Market cap *{fmt_usd(m['mc'])}* · {down}",
          f"💧 liquidity {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC) · 📊 24h volume {fmt_usd(m['vol24'])} "
          f"({m['vol_ratio'] * 100:.0f}% of MC) · 👥 {m['traders']:,} txns · ⏱ {fmt_age(m['age_h'])}"]
    body = "\n".join(L) + "\n"
    c = state["charts"].get(key)
    if c and c.get("ok"):
        body += chart_lines(key, c, m, num(pair, "priceUsd"))
    body += wallets_line(held)
    body += ("ℹ️ This is the slow build-up over days, which one 'wallet bought' message can miss. "
             "Wallets also sell later, so it is a clue, not a guarantee.\n")
    body += (f"📌 Tracking this for {follow_text(WALLET_TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x).\n\n"
             f"`{addr}`\n\n"
             f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', addr)})")
    return body


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
    if not clear_to_alert(key, chain, pair):
        return False   # v3.1: a steady asset (or not checked yet)
    sig = signals_for(m, len(held["in"]))
    extra = (wallets_line(held) + signals_text(sig)
             + f"ℹ️ Not a full re-run setup ({'; '.join(fail_label(c) for c in sorted(codes))}), "
               "but wallets you trust are still holding it.\n" + chain_note(chain))
    if not send_telegram(build_rerun_message(chain, pair, m, extra, title="👛 WALLET-BACKED DIP", track_hours=WALLET_TRACK_HOURS)):
        return False
    state["cooldowns"][ckey] = now
    stamps.append(now)
    bot_stats["walletdip"] += 1
    add_record("wallet", key, chain, pair, m, "", now, extra={"sig": sig, "th": WALLET_TRACK_HOURS})
    log.info("WALLET-BACKED DIP alert for %s", key)
    return True


def wallet_backed_rerun(key: str, chain: str, pair: dict, m: dict, held: dict, sig: list, now: float) -> bool:
    """v3.0: the re-run alert is off, but this coin passes every re-run rule AND wallets you trust are holding it - that is
    still worth a message. It goes out as a wallet-backed alert (same daily limit as the other wallet-backed ones)."""
    if len(held["in"]) < WATCH_MIN_HOLDERS:
        return False
    ckey = "wal:" + key
    if now - state["cooldowns"].get(ckey, 0) < COOLDOWN_SECONDS:
        return False
    stamps = [t for t in state["tuning"].get("wal_ts", []) if now - t < 86400]
    state["tuning"]["wal_ts"] = stamps
    if len(stamps) >= WATCH_DIP_MAX_PER_DAY:
        return False
    if not clear_to_alert(key, chain, pair):
        return False   # v3.1: a steady asset (or not checked yet)
    extra = (wallets_line(held) + signals_text(sig)
             + "ℹ️ It also passes every re-run rule, and wallets you trust are still holding it.\n" + chain_note(chain))
    if not send_telegram(build_rerun_message(chain, pair, m, extra, title="👛 WALLET-BACKED DIP", track_hours=WALLET_TRACK_HOURS)):
        return False
    state["cooldowns"][ckey] = now
    stamps.append(now)
    bot_stats["walletdip"] += 1
    add_record("wallet", key, chain, pair, m, "", now, extra={"sig": sig, "th": WALLET_TRACK_HOURS})
    log.info("WALLET-BACKED alert for %s (it also passes the re-run rules)", key)
    return True


# ----------------------------------------------------------------------------
# v3.5 - NAMED WALLETS ("VIPs"), THE LOUD LOW-CAP PING, MARKET CAPS NEXT TO COINS, THE LOW-CAP CROWD ALERT, AND THE DIP-WATCH FOR
# COINS A KOL (unipcs) BOUGHT OR SHILLED.
# Why: in the owner's own numbers (12-25 Oct) the one big recent hit, MOTION, was a coin under $1M that five saved wallets bought at about
# $340K market cap (two of them wallets the bot had promoted itself), and of the coins under $1M that three or more saved wallets bought, 15 had
# doubled and 3 had halved first (the rest were still undecided). So: (1) wallets worth following are named and looked at every minute or two,
# (2) a buy of a low-cap coin by one of them is a loud message that repeats, (3) a crowd of saved wallets in a low-cap coin is an alert of its own,
# and (4) every coin the chat lists shows its market cap. Everything here only READS public blockchain data and DexScreener: nothing is signed or sent.
# ----------------------------------------------------------------------------
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"     # ERC-20 Transfer(address,address,uint256)
RH_QUOTES = {WRAPPED_NATIVE["robinhood"]} | EVM_STABLES["robinhood"]                       # WETH and USDG on Robinhood Chain: money, not coins
FOMO_ROUTERS = {"0x0005ea38eb0a69d1253508ebdbdb9ea8cb26b5ef", "0xb92fe925dc43a0ecde6c8b1a2709c170ec4fff4f"}   # contracts the Fomo app trades through
_mc_live: dict[str, tuple[float, float, float, float]] = {}      # coin key -> (when, market cap, liquidity, age of the pool in days; 0 = not known) at the latest look at its pool
_vip_rt: dict = {"reminders": [], "retry": [], "look": {}, "err": {}, "skip": {}, "code": {}, "last": {}, "bf": {}, "started": 0.0}


def note_mc(key: str, mc: float, liq: float = 0.0, age_d: float = 0.0):
    """Remember a coin's market cap as just seen (so lists in the chat can show it without asking again). v3.8: and how old its pool is."""
    if not isinstance(key, str) or not (isinstance(mc, (int, float)) and mc > 0):
        return
    old = _mc_live.get(key)
    if not age_d and old and len(old) > 3:
        age_d = old[3] + (time.time() - old[0]) / 86400      # (no age this time: the last one, moved on by the time that has passed)
    _mc_live[key] = (time.time(), float(mc), float(liq or 0.0), float(age_d or 0.0))
    if len(_mc_live) > 4000:
        for k in sorted(_mc_live, key=lambda k_: _mc_live[k_][0])[:500]:
            _mc_live.pop(k, None)


def mc_cached(key: str, max_age: float = 6 * 3600) -> float:
    """A coin's market cap from the latest scan (0 if it was not seen lately). No network."""
    hit = _mc_live.get(key)
    return hit[1] if hit and time.time() - hit[0] <= max_age else 0.0


def live_mcs(keys, fetch: bool = True, max_age: float = 900.0) -> dict[str, float]:
    """{coin key: market cap now}. What the latest scans saw (younger than max_age) is used as it is; the other coins are asked from
    DexScreener (30 per request, one chain per request) when fetch is on. Never raises: a coin it cannot price is simply left out."""
    now, out, todo = time.time(), {}, {}
    for k in dict.fromkeys(k_ for k_ in keys if isinstance(k_, str) and ":" in k_):
        hit = _mc_live.get(k)
        if hit and now - hit[0] <= max_age:
            out[k] = hit[1]
            continue
        chain, addr = k.split(":", 1)
        if fetch and chain in CHAINS:
            todo.setdefault(chain, []).append(addr)
        elif hit:
            out[k] = hit[1]                      # (not fetching: an older reading is better than none)
    for chain, addrs in todo.items():
        for i in range(0, min(len(addrs), 60), 30):
            batch = addrs[i:i + 30]
            try:
                time.sleep(0.5)
                pairs = fetch_pairs(chain, batch)
            except requests.RequestException:
                break
            grouped: dict[str, list[dict]] = {}
            for p in pairs:
                grouped.setdefault(norm_addr(chain, p.get("baseToken", {}).get("address", "")), []).append(p)
            for a in batch:
                pair = best_live_pair(grouped.get(norm_addr(chain, a), []))
                if not pair:
                    continue
                k = sk(chain, a)
                mc = pair_mc(k, pair)
                if mc > 0:
                    note_mc(k, mc, num(pair, "liquidity", "usd"))
                    out[k] = mc
    for k in keys:                               # whatever could not be asked: an older reading, if there is one
        if isinstance(k, str) and k not in out and k in _mc_live:
            out[k] = _mc_live[k][1]
    return out


def _tune_num(name: str, default: float) -> float:
    v = state["tuning"].get(name)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0 else float(default)


def lowcap_limit() -> float:
    """The market cap at or under which a coin counts as 'low cap' (LOWCAP_MC, or what /lowcap set)."""
    v = _tune_num("lowcap_mc", 0)
    return v if v > 0 else LOWCAP_MC


def vip_min_usd() -> float:
    return _tune_num("vip_min", VIP_MIN_BUY_USD)


def vip_pings() -> int:
    return int(min(len(VIP_REMIND_AFTER), max(0, _tune_num("vip_ping", VIP_REMINDERS))))


def day_count(name: str, now: float) -> list:
    """The time stamps (last 24 hours) kept under this name in the memory - used for the daily limits."""
    stamps = _last_day(state["tuning"].get(name), now)
    state["tuning"][name] = stamps
    return stamps


def vip_cursors() -> dict:
    """Where the last look at each VIP wallet on an Ethereum-style chain stopped: {wallet: block number}."""
    cur = state["tuning"].get("vip_cur")
    if not isinstance(cur, dict):
        cur = state["tuning"]["vip_cur"] = {}
    return cur


# --- the VIP list ---
def seed_vip_wallets():
    """The named wallets (VIP_WALLETS) join the watch list, marked as VIP. A name you gave one yourself is kept, and one you
    removed with /unwatch stays removed."""
    gone = set(state["tuning"].get("watch_removed") or [])
    off = state["tuning"].get("vip_off")
    gone |= set(off) if isinstance(off, list) else set()          # taken off the VIP list with /vip remove
    for row in VIP_WALLETS:
        name, addr, chain, sure, kol, note = row[:6]
        style = row[6] if len(row) > 6 else None
        kind = wallet_kind(addr)
        if not kind:
            continue
        key = addr.lower() if kind == "evm" else addr
        if key in gone:
            continue
        info = state["watch"].setdefault(key, {"kind": kind, "t": time.time()})
        if not info.get("name"):
            info["name"] = name
        info.update(vip=True, tier="vip", chain=chain, kol=bool(kol), sure=sure, note=note)
        if style in ("selective", "spray", "normal") and "style" not in info and not info.get("style_seeded"):
            info["style"] = style              # (v3.7) a default style, given once: a style you set yourself - or reset to 'auto' - is never overwritten
            info["style_seeded"] = True


def vip_items(kind: str | None = None) -> list[tuple[str, dict]]:
    """The VIP wallets (named ones and the hunters the bot found): [(address, info)]."""
    return [(w, d) for w, d in list(state["watch"].items())
            if isinstance(d, dict) and d.get("vip") and (kind is None or d.get("kind") == kind)]


def vip_note_ok(w: str, text: str):
    _vip_rt["look"][w] = (time.time(), True, text)
    _vip_rt["err"].pop(w, None)


def vip_note_error(w: str, e):
    n = _vip_rt["err"].get(w, 0) + 1
    _vip_rt["err"][w] = n
    _vip_rt["look"][w] = (time.time(), False, scrub(e)[:80])
    if n in (1, 5, 20, 100):
        log.warning("could not look at VIP wallet %s (%d in a row): %s", short(w), n, scrub(e))


def vip_status_text(w: str, d: dict | None = None) -> str:
    hit = _vip_rt["look"].get(w)
    if not hit:
        return "not looked at yet"
    t, ok, text = hit
    if ok:
        return f"looked {ago(time.time() - t)} ago, {text}"
    return f"couldn't look inside it {ago(time.time() - t)} ago ({text}) - I keep trying"


def vip_log_event(w: str, sym: str, usd: float, mc: float, low: bool, key: str, style: str = ""):
    """The last few buys seen (shown by /vip)."""
    rows = state["tuning"].get("vip_log")
    if not isinstance(rows, list):
        rows = state["tuning"]["vip_log"] = []
    rows.append({"t": round(time.time()), "w": w, "s": str(sym)[:12], "usd": round(usd), "mc": round(mc), "low": bool(low), "k": key})
    if style in ("selective", "spray"):
        rows[-1]["st"] = style
    del rows[:-40]


def vip_last_buy(w: str) -> dict | None:
    rows = [r for r in (state["tuning"].get("vip_log") or []) if isinstance(r, dict) and r.get("w") == w]
    return rows[-1] if rows else None


# --- v3.6: how picky is this wallet? (its "style") ---
def style_start(now: float | None = None):
    """The day this version started writing down which coins each VIP wallet buys. A wallet is only called picky after it has been
    counted for STYLE_PICKY_MIN_DAYS - 'it bought nothing' means nothing before that."""
    if not isinstance(state["tuning"].get("vip_buys_since"), (int, float)) or isinstance(state["tuning"].get("vip_buys_since"), bool):
        state["tuning"]["vip_buys_since"] = round(now or time.time())


def style_rows(w: str) -> list:
    buys = state["tuning"].get("vip_buys")
    rows = buys.get(w) if isinstance(buys, dict) else None
    return rows if isinstance(rows, list) else []


def style_note_buy(w: str, key: str, usd: float, now: float):
    """Write down that this wallet bought this coin: one row per coin [last buy time, coin, biggest buy in dollars]. The style is judged on these."""
    style_start(now)
    buys = state["tuning"].get("vip_buys")
    if not isinstance(buys, dict):
        buys = state["tuning"]["vip_buys"] = {}
    rows = buys.get(w)
    if not isinstance(rows, list):
        rows = buys[w] = []
    cut = now - STYLE_KEEP_DAYS * 86400
    rows[:] = [r for r in rows if r[0] >= cut]
    for r in rows:
        if r[1] == key:
            r[0], r[2] = round(now), max(r[2], round(usd))
            return
    rows.append([round(now), key, round(usd)])
    del rows[:-STYLE_MAX_ROWS]


def style_forget_buy(w: str, key: str):
    """Take a coin out of the count (it turned out to be a stock token, a wrapped coin or a stable: not a coin pick)."""
    rows = style_rows(w)
    rows[:] = [r for r in rows if r[1] != key]


def style_counts(w: str, now: float) -> tuple[int, int]:
    """(different coins bought in the last 7 days, in the last 24 hours) - counting buys of at least STYLE_MIN_BUY_USD."""
    rows = [r for r in style_rows(w) if r[2] >= STYLE_MIN_BUY_USD]
    return (len({r[1] for r in rows if now - r[0] <= 7 * 86400}), len({r[1] for r in rows if now - r[0] <= 86400}))


def style_days(w: str, info: dict, now: float) -> float:
    """How many days this wallet's buys have been counted."""
    t0 = state["tuning"].get("vip_buys_since")
    t0 = float(t0) if isinstance(t0, (int, float)) and not isinstance(t0, bool) else now
    mine = info.get("vip_t") or info.get("t") or now
    mine = float(mine) if isinstance(mine, (int, float)) and not isinstance(mine, bool) else now
    return max(0.0, (now - max(t0, mine)) / 86400)


def vip_style(w: str, info: dict | None = None, now: float | None = None) -> tuple[str, str]:
    """(style, why) of a VIP wallet. 'selective' = a picky wallet: few different coins, so each buy means more; 'spray' = buys a lot of
    different coins, so each buy means less; 'normal' = neither (or too early to say).
    A word you gave it (/vip style <name> picky|spray|normal) always wins; otherwise the bot judges by the coins the wallet bought lately -
    and a wallet whose followed buys mostly failed is never picky."""
    now = now or time.time()
    info = info if isinstance(info, dict) else (state["watch"].get(w) or {})
    manual = info.get("style")
    if manual in ("selective", "spray", "normal"):
        return manual, "you set it"
    if not STYLE_ON or info.get("kol"):
        return "normal", ""
    n7, n1 = style_counts(w, now)
    if n7 >= STYLE_SPRAY_7D:
        return "spray", f"{n7} different coins in 7 days"
    if n1 >= STYLE_SPRAY_24H:
        return "spray", f"{n1} different coins in one day"
    try:
        sc = wallet_scores().get(w)
    except Exception:
        sc = None
    if sc and sc.get("n", 0) >= 8 and sc.get("good", 0) / sc["n"] <= 0.2:
        return "spray", f"only {sc['good']} of its {sc['n']} followed buys turned out good"
    days = style_days(w, info, now)
    if days >= STYLE_PICKY_MIN_DAYS and n7 <= STYLE_PICKY_7D and n1 <= STYLE_PICKY_24H and _vip_rt["err"].get(w, 0) < 3:
        span = "the last 7 days" if days >= 7 else f"the {days:.0f} days I have watched it"
        return "selective", f"only {n7} different coin{'' if n7 == 1 else 's'} in {span}"
    return "normal", ""


def style_icon(style: str) -> str:
    return STYLE_LABEL.get(style, "")


def style_tag(w: str, info: dict, now: float) -> str:
    """The short style note shown after a VIP wallet's name in the lists."""
    st, why = vip_style(w, info, now)
    if st == "normal":
        n7, _ = style_counts(w, now)
        return f" · ▫️ normal ({n7} coin{'' if n7 == 1 else 's'} in 7 days)" if n7 else ""
    return f" · {style_icon(st)}" + (f" ({md_escape(why)})" if why else "")


# --- a VIP wallet bought a coin ---
def vip_retry_later(ev: dict, now: float, why: str):
    """A buy that could not be priced yet (a brand-new coin DexScreener has not listed, or DexScreener busy) is tried again a few times."""
    ev["tries"] = ev.get("tries", 0) + 1
    if ev["tries"] > VIP_RETRY_TRIES:
        _vip_rt["skip"][(ev["w"], sk(ev["chain"], ev["token"]))] = now
        log.info("VIP buy of %s by %s dropped after %d tries: %s", ev["token"], short(ev["w"]), VIP_RETRY_TRIES, why)
        return
    ev["due"] = now + VIP_RETRY_SECONDS
    _vip_rt["retry"].append(ev)


def vip_retry_due(now: float):
    due = [e for e in _vip_rt["retry"] if now >= e.get("due", 0)]
    if not due:
        return
    _vip_rt["retry"] = [e for e in _vip_rt["retry"] if now < e.get("due", 0)]
    for e in due:
        try:
            vip_buy_event(e["w"], e["chain"], e["token"], e["amount"], e["usd"], e["at"], e.get("tx", ""), tries=e["tries"])
        except Exception:
            log.exception("VIP retry error")


def vip_why_line(info: dict) -> str:
    """Why this wallet is on the list, in one line."""
    note = str(info.get("note") or "").strip()
    sure = info.get("sure")
    how = {"certain": "its address matched his Fomo profile exactly",
           "likely": "a probable match for his Fomo profile, about 90% sure",
           "found": "found by the bot from results",
           "added by you": "added by you"}.get(sure, "")
    bits = [x for x in (note, f"({how})" if how and how != note else "") if x]
    return ("🏷 " + md_escape(" ".join(bits)) + "\n") if bits else ""


def vip_when(bought_at: float, now: float) -> str:
    if not bought_at:
        return "seen just now"
    age = now - bought_at
    return "just now" if age < 150 else f"about {ago(age)} ago"


def build_vip_message(w: str, info: dict, chain: str, pair: dict, mc: float, usd: float, bought_at: float, now: float,
                      low: bool, loud: bool, late: bool, style: str = "normal", why: str = "") -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(str(base.get("symbol", "?")))
    name = md_escape(wallet_label(w))
    liq, vol = num(pair, "liquidity", "usd"), num(pair, "volume", "h24")
    created = num(pair, "pairCreatedAt")
    age_h = (now * 1000 - created) / 3_600_000 if created else 0.0
    wchain = info.get("chain") if info.get("chain") in CHAINS else ("solana" if info.get("kind") == "solana" else "ethereum")
    picky, spray = style == "selective", style == "spray"
    hot = low or (picky and 0 < mc <= PICKY_MAX_MC)             # a coin this wallet's buy is announced loudly for
    if picky and hot:
        head = (("🚨🚨 *PICKY WALLET BUY: LOW-CAP COIN* 🚨🚨" if loud else "🚨 *PICKY WALLET BUY: LOW-CAP COIN*") if low else
                ("🚨🚨 *PICKY WALLET BUY* 🚨🚨" if loud else "🚨 *PICKY WALLET BUY*"))
    elif spray:
        head = "🔕 *Sprayer buy: low-cap coin* (quiet)" if low else "🔕 *Sprayer buy* (quiet)"
    elif low:
        head = "🚨🚨 *VIP BUY: LOW-CAP COIN* 🚨🚨" if loud else "🚨 *VIP BUY: LOW-CAP COIN*"
    else:
        head = "👤 *VIP BUY*"
    L = [head, ""]
    if late:
        L.append(f"🕓 Found late: it was {vip_when(bought_at, now)}.")
    L.append(f"👤 *{name}* bought *${sym}*  |  {CHAINS[chain]['name']}")
    if picky:
        rec = wallet_record_text(w)
        L.append(f"🎯 A *picky wallet* ({md_escape(why or 'it buys few coins')}). A buy from it means more than one from a wallet that buys everything."
                 + (f"\n📒 Its record with me: {rec}" if rec and "no finished buys yet" not in rec else ""))
    elif spray:
        L.append(f"🔕 A *sprayer* ({md_escape(why or 'it buys a lot of different coins')}). A weak clue on its own - that is why this is quiet.")
    L.append(f"💵 about *{fmt_usd(usd)}* · {vip_when(bought_at, now)}")
    if mc > 0:
        if low:
            cap_note = f"  (under your {fmt_usd(lowcap_limit())} low-cap line)"
        elif picky and hot:
            cap_note = f"  (above your {fmt_usd(lowcap_limit())} low-cap line, but a picky wallet gets the loud ping up to {fmt_usd(PICKY_MAX_MC)})"
        elif picky:
            cap_note = f"  (above {fmt_usd(PICKY_MAX_MC)}, so no loud ping even for a picky wallet)"
        else:
            cap_note = f"  (above your {fmt_usd(lowcap_limit())} low-cap line, so no loud ping)"
        L.append(f"💰 Market cap *{fmt_usd(mc)}*" + cap_note)
    else:
        L.append("💰 Market cap: not known yet (a very new coin?)")
    L.append(f"💧 liquidity {fmt_usd(liq)} · 📊 24h volume {fmt_usd(vol)}" + (f" · ⏱ pool {fmt_age(age_h)} old" if age_h > 0 else ""))
    ch = pair.get("priceChange") or {}
    if isinstance(ch, dict) and any(ch.get(x) is not None for x in ("h1", "h6", "h24")):
        L.append("📈 price " + " · ".join(f"{num(ch, x):+.0f}% {lab}" for x, lab in (("h1", "1h"), ("h6", "6h"), ("h24", "24h")) if ch.get(x) is not None))
    body = "\n".join(L) + "\n" + vip_why_line(info)
    body += chain_note(chain)
    if low and not spray:
        body += (f"📌 I follow this for {follow_text(WALLET_TRACK_HOURS, mc or 0.0)} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). A wallet's buy is a clue, "
                 "not a guarantee - low-cap coins can also drop fast or be hard to sell.\n")
    elif picky and hot:
        body += (f"📌 I follow this for {follow_text(WALLET_TRACK_HOURS, mc or 0.0)} (win = +{(RADAR_WIN_X - 1) * 100:.0f}% before -{(1 - RADAR_STOP_X) * 100:.0f}%, "
                 "a fairer yardstick for a bigger coin). A wallet's buy is a clue, not a guarantee.\n")
    body += (f"\n`{addr}`\n\n[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Wallet]({link(wchain, 'addr', w)}) | [Token]({link(chain, 'token', addr)})")
    return body


def vip_buy_event(w: str, chain: str, token: str, amount: float, usd_hint: float, bought_at: float, tx: str = "",
                  pair: dict | None = None, tries: int = 0) -> bool:
    """A VIP wallet (a named one, or a hunter the bot found) bought a coin. Looks the coin up, and: a low-cap coin -> a loud message that
    repeats; any other coin -> one short message; a wallet marked KOL (unipcs) -> the coin joins the dip-watch. Returns True when the buy
    was handled (announced or deliberately passed over); False when it could not be handled now."""
    now = time.time()
    info = state["watch"].get(w)
    if not isinstance(info, dict):
        return False
    key = sk(chain, token)
    if key in state["steady"] or is_quote_token(chain, norm_addr(chain, token)):
        return False
    skip = _vip_rt["skip"]
    if now - skip.get((w, key), 0) < 1800:
        return False                                          # looked at and set aside a moment ago
    last = state["tuning"].get("vip_last")
    if not isinstance(last, dict):
        last = state["tuning"]["vip_last"] = {}
    for k_ in [k_ for k_, t_ in last.items() if not isinstance(t_, (int, float)) or now - t_ > 3 * 86400]:
        del last[k_]
    lk = f"{w}|{key}"
    if now - last.get(lk, 0) < VIP_REBUY_HOURS * 3600:
        return False                                          # the same wallet adding to the same coin: not announced again
    ev = {"w": w, "chain": chain, "token": token, "amount": amount, "usd": usd_hint, "at": bought_at, "tx": tx, "tries": tries}
    if pair is None:
        try:
            want = norm_addr(chain, token)
            pairs = [p for p in fetch_pairs(chain, [token]) if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == want]
        except requests.RequestException:
            vip_retry_later(ev, now, "DexScreener busy")
            return False
        pair = best_live_pair(pairs)
        if not pair:
            vip_retry_later(ev, now, "no live trading pool yet")
            return False
    sym = str(pair.get("baseToken", {}).get("symbol", "?"))
    px, liq = num(pair, "priceUsd"), num(pair, "liquidity", "usd")
    if sym.upper() in QUOTE_SYMBOLS or px <= 0:
        skip[(w, key)] = now
        return False
    mc = pair_mc(key, pair) or num(pair, "fdv")
    note_mc(key, mc, liq)
    usd = usd_hint if usd_hint and usd_hint > 0 else amount * px
    if usd >= STYLE_MIN_BUY_USD:
        style_note_buy(w, key, usd, now)                      # v3.6: how many DIFFERENT coins a wallet buys decides its style
    kol = bool(info.get("kol"))
    min_usd = vip_min_usd()
    if usd < (min(min_usd, KOL_MIN_BUY_USD) if kol else min_usd) or liq < (min(VIP_MIN_LIQ_USD, KOL_MIN_LIQ_USD) if kol else VIP_MIN_LIQ_USD):
        skip[(w, key)] = now
        log.info("VIP %s bought %s but it is too small to matter (%s, liquidity %s)", short(w), key, fmt_usd(usd), fmt_usd(liq))
        return False
    low = mc <= 0 or mc <= lowcap_limit()
    if not low and gecko_backlog() <= 8:                      # v3.6: a stock token / wrapped coin / stable is not a coin pick (a loud low-cap ping is never held up for this)
        try:
            steady = ensure_steady(key, chain, pair)
        except Exception:
            steady = None
            log.exception("could not check whether %s is a steady asset", key)
        if steady:
            skip[(w, key)] = now
            style_forget_buy(w, key)                          # (it does not count as one of the coins this wallet picks)
            log.info("VIP %s bought %s, a steady asset (stock token, wrapped coin or stable) - not announced", short(w), key)
            return False
    if kol and usd >= KOL_MIN_BUY_USD and liq >= KOL_MIN_LIQ_USD and mc > 0:
        try:
            kol_add(key, chain, pair, wallet_label(w), mc, now, how="bought", usd=usd)      # the dip-watch (unipcs)
        except Exception:
            log.exception("could not put the coin on the dip-watch")
    if usd < min_usd or liq < VIP_MIN_LIQ_USD:
        last[lk] = now                                        # (only the dip-watch cared about this one)
        return True
    late = bool(bought_at) and now - bought_at > VIP_STALE_MIN * 60
    style, why = vip_style(w, info, now)                      # v3.6: picky wallets get a better ping, sprayers a quiet one
    picky, spray = style == "selective", style == "spray"
    hot = low or (picky and 0 < mc <= PICKY_MAX_MC)           # coins this wallet's buy is announced loudly for
    loud_day, plain_day = day_count("vip_ts", now), day_count("vip_pts", now)
    picky_day, spray_day = day_count("vip_sts", now), day_count("vip_sps", now)
    if picky:
        loud = hot and not late and len(picky_day) < PICKY_LOUD_MAX_PER_DAY
        reminders = min(len(VIP_REMIND_AFTER), vip_pings() + 1) if loud and vip_pings() > 0 else 0     # (one more repeat than the others)
        stamps = picky_day
        announce = hot or len(plain_day) < VIP_PLAIN_MAX_PER_DAY
    elif spray:
        loud, reminders, stamps = False, 0, spray_day
        announce = low and len(spray_day) < SPRAY_MAX_PER_DAY        # a sprayer's bigger-coin buys are only written down
    else:
        loud = low and not late and len(loud_day) < VIP_LOUD_MAX_PER_DAY
        reminders = vip_pings() if loud else 0
        stamps = loud_day if low else plain_day
        announce = low or len(plain_day) < VIP_PLAIN_MAX_PER_DAY
    if picky and not hot:
        stamps = plain_day
    sent = True
    if alert_on("vip"):
        if announce:
            sent = send_telegram(build_vip_message(w, info, chain, pair, mc, usd, bought_at, now, low, loud, late, style, why), silent=spray)
            if sent:
                stamps.append(now)
        if not sent:
            vip_retry_later(ev, now, "Telegram did not take the message")
            return False
        bot_stats["vip"] += 1 if (hot and announce and not spray) else 0
    last[lk] = now
    if low:
        state["cooldowns"]["vip:" + key] = now
    vip_log_event(w, sym, usd, mc, low, key, style)
    log.info("VIP BUY: %s bought %s (%s, market cap %s, %s, %s)", wallet_label(w), key, fmt_usd(usd), fmt_usd(mc),
             "LOW-CAP" if low else "not low-cap", {"selective": "picky wallet", "spray": "sprayer"}.get(style, "normal wallet"))
    if reminders > 0 and alert_on("vip"):
        _vip_rt["reminders"].append({"w": w, "key": key, "chain": chain, "token": token, "sym": sym, "mc0": mc, "px0": px,
                                     "t0": now, "n": 0, "total": reminders})
    m = compute_metrics(key, pair)
    if m and (low or (picky and hot)):
        extra = {"wallet": w, "who": wallet_label(w)[:30], "th": WALLET_TRACK_HOURS, "usd": round(usd),
                 "sig": signals_for(m), "quiet": (not alert_on("vip")) or spray or not announce, "style": style}
        if not low:                                           # a bigger coin a picky wallet bought: judged by a fairer yardstick than 2x
            extra.update(wx=RADAR_WIN_X, sx=RADAR_STOP_X, big=True)
        add_record("vip", key, chain, pair, m, "", now, extra=extra)
    try:        # the same buy is scored like any other watched wallet's, and two watched wallets in one coin make a cluster
        handle_whale_swap({"chain": chain, "wallet": w, "side": "BUY", "token": token, "symbol": sym, "usd": usd,
                           "signature": f"vip:{w}:{tx or key}", "src": "watch"}, quiet=True)
    except Exception:
        log.exception("could not note the VIP buy")
    save_state()
    return True


def vip_reminders_due(now: float):
    """The repeats of a loud message: a short note with the coin's price now."""
    keep = []
    for r in _vip_rt["reminders"]:
        due = r["t0"] + VIP_REMIND_AFTER[min(r["n"], len(VIP_REMIND_AFTER) - 1)]
        if now < due:
            keep.append(r)
            continue
        mc_now = 0.0
        try:
            want = norm_addr(r["chain"], r["token"])
            pairs = [p for p in fetch_pairs(r["chain"], [r["token"]]) if norm_addr(r["chain"], p.get("baseToken", {}).get("address", "")) == want]
            pair = best_live_pair(pairs)
            if pair:
                mc_now = pair_mc(r["key"], pair) or 0.0
                note_mc(r["key"], mc_now, num(pair, "liquidity", "usd"))
        except requests.RequestException:
            pass
        name = md_escape(wallet_label(r["w"]))
        move = f" · now *{fmt_usd(mc_now)}*" + (f" ({mc_now / r['mc0'] - 1:+.0%})" if r["mc0"] > 0 and mc_now > 0 else "") if mc_now > 0 else ""
        send_telegram(f"🔔 *Reminder {r['n'] + 1} of {r['total']}:* {name} bought *${md_escape(r['sym'])}* {ago(now - r['t0'])} ago "
                      f"at a market cap of {fmt_usd(r['mc0'])}{move}\n`{r['token']}`\n"
                      f"[DexScreener](https://dexscreener.com/{r['chain']}/{r['token']})")
        r["n"] += 1
        if r["n"] < r["total"]:
            keep.append(r)
    _vip_rt["reminders"] = keep


# --- the dip-watch: coins a KOL (unipcs) bought or shilled ---
def kol_add(key: str, chain: str, pair: dict, who: str, mc: float, now: float, how: str = "bought", usd: float = 0.0) -> bool:
    """Put a coin on the dip-watch. It is alerted when it is down KOL_DIP_MIN-KOL_DIP_MAX % from its high. Returns True when new."""
    kol = state["kol"]
    e = kol.get(key)
    if e:
        e["gone"] = 0
        return False
    if len(kol) >= KOL_MAX_COINS:                                   # full: the oldest one goes
        del kol[min(kol, key=lambda k_: kol[k_].get("t", 0))]
    kol[key] = {"sym": str(pair.get("baseToken", {}).get("symbol", "?"))[:14], "who": str(who or "KOL")[:30], "chain": chain,
                "t": now, "mc0": mc, "peak": max(mc, state["peaks"].get(key, 0.0)), "alerted": 0, "seen": now, "dd": 0, "mc": mc,
                "gone": 0, "bf": 0, "chk": 0, "how": how, "usd": round(usd)}
    log.info("dip-watch: %s joined (%s %s it)", key, who, how)
    return True


def build_kol_message(key: str, chain: str, pair: dict, e: dict, mc: float, peak: float, dd: float, now: float) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(str(base.get("symbol", "?")))
    who = md_escape(str(e.get("who") or "KOL"))
    liq, vol = num(pair, "liquidity", "usd"), num(pair, "volume", "h24")
    created = num(pair, "pairCreatedAt")
    age_h = (now * 1000 - created) / 3_600_000 if created else 0.0
    verb = "shilled" if e.get("how") == "shilled" else "bought"
    L = [f"*📉 DIP-WATCH: ${sym}*  |  {CHAINS[chain]['name']}", "",
         f"{who} {verb} it {ago(now - e.get('t', now))} ago" + (f" (about {fmt_usd(e['usd'])})" if e.get("usd") else "")
         + (f", when its market cap was {fmt_usd(e['mc0'])}" if e.get("mc0") else "") + ".",
         f"💰 Now *{fmt_usd(mc)}* - down *{dd:.0f}%* from its high of {fmt_usd(peak)}  (your dip zone: {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% down)",
         f"💧 liquidity {fmt_usd(liq)} · 📊 24h volume {fmt_usd(vol)}" + (f" · ⏱ pool {fmt_age(age_h)} old" if age_h > 0 else "")]
    ch = pair.get("priceChange") or {}
    if isinstance(ch, dict) and any(ch.get(x) is not None for x in ("h1", "h6", "h24")):
        L.append("📈 price " + " · ".join(f"{num(ch, x):+.0f}% {lab}" for x, lab in (("h1", "1h"), ("h6", "6h"), ("h24", "24h")) if ch.get(x) is not None))
    body = "\n".join(L) + "\n" + chain_note(chain)
    body += (f"📌 I follow it for {follow_text(KOL_TRACK_HOURS, mc)} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). A coin that fell this far can keep "
             "falling or be dead - this is a chance to look, not a promise.\n\n"
             f"`{addr}`\n\n[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | [Token]({link(chain, 'token', addr)})")
    return body


def kol_pass() -> int:
    """Price every coin on the dip-watch (one DexScreener request per 30 coins) and alert the ones that are down KOL_DIP_MIN-KOL_DIP_MAX %
    from their high. Its high = the best of what I have seen and the past high from the price history (looked up once, a few at a time)."""
    kol = state["kol"]
    if not kol:
        return 0
    now, sent = time.time(), 0
    for k in [k for k, e in kol.items() if now - e.get("t", 0) > KOL_KEEP_DAYS * 86400]:
        del kol[k]
    by_chain: dict[str, list[str]] = {}
    for k in list(kol):
        chain, addr = k.split(":", 1)
        by_chain.setdefault(chain, []).append(addr)
    stamps = day_count("kol_ts", now)
    bf_budget = 1
    for chain, addrs in by_chain.items():
        for i in range(0, len(addrs), 30):
            batch = addrs[i:i + 30]
            time.sleep(1.0)
            try:
                pairs = fetch_pairs(chain, batch)
            except requests.RequestException:
                continue
            if len(batch) >= 3 and not pairs:
                continue
            grouped: dict[str, list[dict]] = {}
            for p in pairs:
                grouped.setdefault(norm_addr(chain, p.get("baseToken", {}).get("address", "")), []).append(p)
            for a in batch:
                key = sk(chain, a)
                e = kol.get(key)
                if not e:
                    continue
                pair = best_live_pair(grouped.get(norm_addr(chain, a), []))
                if not pair:
                    e["gone"] = e.get("gone", 0) + 1
                    continue
                e["gone"] = 0
                mc = pair_mc(key, pair)
                if mc <= 0:
                    continue
                liq = num(pair, "liquidity", "usd")
                note_mc(key, mc, liq)
                if not e.get("bf") and key in state["backfilled"]:
                    e["bf"] = now                                   # the scanner already looked up its past high
                if (not e.get("bf") and bf_budget > 0 and gecko_backlog() < 10 and GECKO_NETWORK.get(chain) and pair.get("pairAddress")
                        and _vip_rt["bf"].get(key, 0) < 3):
                    bf_budget -= 1
                    try:
                        peak0 = fetch_peak_mc(chain, pair, key)
                        if peak0:
                            store_peak(key, peak0)
                            state["backfilled"][key] = now
                            e["bf"] = now
                        else:
                            _vip_rt["bf"][key] = _vip_rt["bf"].get(key, 0) + 1
                    except (requests.RequestException, KeyError, ValueError, TypeError):
                        _vip_rt["bf"][key] = _vip_rt["bf"].get(key, 0) + 1
                    if _vip_rt["bf"].get(key, 0) >= 3 and not e.get("bf"):
                        e["bf"] = 1                                 # gave up: its own readings will have to do
                peak = max(e.get("peak", 0.0), state["peaks"].get(key, 0.0), mc)
                dd = (peak - mc) / peak * 100 if peak > 0 else 0.0
                e.update(seen=now, mc=mc, peak=peak, dd=round(dd, 1))
                if not (KOL_DIP_MIN <= dd <= KOL_DIP_MAX) or liq < KOL_MIN_LIQ_USD:
                    continue
                if now - e.get("alerted", 0) < KOL_COOLDOWN_DAYS * 86400 or len(stamps) >= KOL_MAX_PER_DAY or key in state["steady"]:
                    continue
                if not e.get("bf") and now - e.get("t", now) < 600:
                    continue                                        # its past high is not known yet: wait a few minutes before deciding
                if not confirmed_twice("kol", key, mc, now):
                    continue
                if not clear_to_alert(key, chain, pair):
                    continue
                m = compute_metrics(key, pair)
                if not m:
                    continue
                quiet = not alert_on("kol")
                if not quiet and not send_telegram(build_kol_message(key, chain, pair, e, mc, peak, dd, now)):
                    continue
                e["alerted"] = now
                if not quiet:
                    stamps.append(now)
                    bot_stats["kol"] += 1
                    sent += 1
                add_record("kol", key, chain, pair, m, "", now,
                           extra={"th": KOL_TRACK_HOURS, "who": e.get("who", "KOL"), "dd": round(dd), "quiet": quiet, "sig": signals_for(m)})
                log.info("DIP-WATCH alert for %s: down %.0f%% from %s", key, dd, fmt_usd(peak))
    return sent


# --- looking inside the VIP wallets ---
def vip_sweep_solana(tier: str) -> int:
    """A look inside every VIP wallet of this tier on Solana: a coin that is new in it, or whose amount grew by half or more, is a buy.
    (The first look at a wallet only sets the baseline.)"""
    n = 0
    for w, d in vip_items("solana"):
        if (d.get("tier") or "vip") != tier:
            continue
        try:
            snap = sol_snapshot(w)
        except Exception as e:
            vip_note_error(w, e)
            continue
        vip_note_ok(w, f"holds {len(snap)} coin{'' if len(snap) == 1 else 's'}")
        old = _snaps.get(w)
        _snaps[w] = (time.time(), snap)
        if old is None:
            continue
        prev = old[1]
        need = [m_ for m_, a in snap.items() if a > prev.get(m_, 0.0) and a >= 1.5 * prev.get(m_, 0.0) and not is_quote_token("solana", m_)]
        if not need:
            continue
        priced = price_coins(need)
        for mint in need:
            pair = priced.get(mint)
            if not pair:
                continue
            added = snap[mint] - prev.get(mint, 0.0)
            try:
                if vip_buy_event(w, "solana", mint, added, added * num(pair, "priceUsd"), 0.0, "sweep:%d" % int(time.time() // 60), pair=pair):
                    n += 1
            except Exception:
                log.exception("VIP buy on Solana could not be handled")
    return n


def evm_is_contract(url: str, addr: str) -> bool:
    """Is this address a contract (a pool, a router) and not a person's wallet? Remembered."""
    if addr in FOMO_ROUTERS:
        return True
    cache = _vip_rt["code"]
    if addr in cache:
        return cache[addr]
    code = rpc_call(url, "eth_getCode", [addr, "latest"])
    res = isinstance(code, str) and len(code) > 2
    if len(cache) > 5000:
        cache.clear()
    cache[addr] = res
    return res


def evm_token_decimals(url: str, chain: str, token: str) -> int:
    dec = _decimals.get((chain, token))
    if dec is None:
        try:
            dec = int(rpc_call(url, "eth_call", [{"to": token, "data": "0x313ce567"}, "latest"]), 16)
        except Exception:
            return 18                      # (not remembered: it is asked again next time)
        if not 0 <= dec <= 36:
            return 18
        _decimals[(chain, token)] = dec
    return dec


def evm_vip_wallet(url: str, chain: str, w: str, latest: int, now: float) -> int:
    """Read the coins that arrived in / left an Ethereum-style VIP wallet since the last look (the token-transfer log of the chain).
    A coin that arrived from a contract (a pool or a router - not from a person) and did not also leave in the same transaction is a buy."""
    cursors = vip_cursors()
    cur = cursors.get(w)
    if not isinstance(cur, int) or isinstance(cur, bool) or cur <= 0 or cur > latest:
        start = latest - int(VIP_FIRST_LOOKBACK_MIN * 60 * VIP_BLOCKS_PER_SECOND)       # the first look: the last few minutes
    else:
        start = cur + 1
    start = max(start, latest - VIP_MAX_CATCHUP_BLOCKS, 0)
    pad = "0x" + w[2:].lower().rjust(64, "0")
    ins: dict[str, list] = {}
    outs: dict[str, list] = {}
    a = start
    while a <= latest:
        b = min(latest, a + VIP_LOG_WINDOW - 1)
        for topics, store, idx in (([TRANSFER_TOPIC, None, pad], ins, 1), ([TRANSFER_TOPIC, pad, None], outs, 2)):
            logs = rpc_call(url, "eth_getLogs", [{"fromBlock": hex(a), "toBlock": hex(b), "topics": topics}], timeout=30) or []
            for lg in logs:
                t = lg.get("topics") or []
                data = lg.get("data") or ""
                if len(t) != 3 or len(data) < 3:
                    continue                                   # (not a plain coin transfer - an NFT, say)
                try:
                    val, blk = int(data, 16), int(lg["blockNumber"], 16)
                except (ValueError, TypeError, KeyError):
                    continue
                other = "0x" + str(t[idx])[-40:].lower()      # the other side of the transfer
                store.setdefault(str(lg.get("transactionHash") or ""), []).append(((lg.get("address") or "").lower(), val, other, blk))
        a = b + 1
        if a <= latest:
            time.sleep(0.3)
    events = []
    for tx, rows in ins.items():
        gone = {t_ for t_, _, _, _ in outs.get(tx, [])}
        per: dict[str, list] = {}
        for token, val, other, blk in rows:
            if not token or token in RH_QUOTES or token in gone:
                continue
            e = per.setdefault(token, [0, other, blk])
            e[0] += val
        for token, (val, other, blk) in per.items():
            events.append((blk, tx, token, val, other))
    events.sort(reverse=True)
    n = 0
    for blk, tx, token, val, other in events[:8]:               # (the newest few: a flood of airdrops cannot flood the chat)
        if not (evm_is_contract(url, other)):
            continue
        amount = val / 10 ** evm_token_decimals(url, chain, token)
        at = now - max(0, latest - blk) / VIP_BLOCKS_PER_SECOND
        try:
            if vip_buy_event(w, chain, token, amount, 0.0, at, tx):
                n += 1
        except Exception:
            log.exception("VIP buy on %s could not be handled", chain)
    cursors[w] = latest
    vip_note_ok(w, f"up to block {latest:,}")
    return n


def vip_sweep_evm() -> int:
    """Robinhood Chain: the VIP wallets there, one look for all of them (the chain's own free node)."""
    wallets = [(w, d) for w, d in vip_items("evm") if (d.get("chain") or "robinhood") == "robinhood"]
    url = EVM_RPC.get("robinhood", "")
    if not wallets or not url:
        return 0
    try:
        latest = int(rpc_call(url, "eth_blockNumber", []), 16)
    except Exception as e:
        for w, _ in wallets:
            vip_note_error(w, e)
        return 0
    n, now = 0, time.time()
    for w, _ in wallets:
        try:
            n += evm_vip_wallet(url, "robinhood", w, latest, now)
        except Exception as e:
            vip_note_error(w, e)
    return n


# --- hunters: wallets that keep buying low-cap coins that double ---
def hunter_stats() -> dict[str, dict]:
    """Per wallet in the wallet book: its finished buys of low-cap coins (the market cap at the buy = the price it paid x the coin's supply)."""
    lim, crowd, out = lowcap_limit(), crowd_sizes(), {}
    for w, d in list(state["book"].items()):
        if not isinstance(d, dict):
            continue
        wins = stops = 0
        wsum, coins = 0.0, []
        for p in d.get("picks", []):
            if not isinstance(p, dict) or p.get("st") not in ("w", "s"):
                continue
            sup, px = state["supply"].get(p.get("k")), p.get("px")
            if not isinstance(sup, (int, float)) or not isinstance(px, (int, float)) or sup <= 0 or px <= 0 or px * sup > lim:
                continue
            if p["st"] == "w":
                wins += 1
                wsum += crowd_weight(crowd.get(p["k"], 1)) * (EARLY_WIN_WEIGHT if is_early(p) else 1.0)   # a coin a whole crowd bought counts less (they win or lose together); so does a hit that was already in the bag when I saw the buy (v3.8)
                coins.append(str(p.get("s", "?")))
            else:
                stops += 1
        if wins or stops:
            out[w] = {"wins": wins, "stops": stops, "wsum": round(wsum, 2), "coins": coins, "chain": d.get("chain", "solana")}
    return out


def hunters_pass() -> int:
    """Wallets with at least HUNTER_MIN_WINS low-cap buys that doubled (counted fairly: crowded coins count less) and a win rate of
    HUNTER_MIN_RATE or better join the VIP list by themselves, so a buy of theirs gets the loud message too."""
    if not HUNTER_ON:
        return 0
    removed = set(state["tuning"].get("watch_removed") or [])
    off = state["tuning"].get("vip_off")
    removed |= set(off) if isinstance(off, list) else set()       # a hunter taken off the list with /vip remove does not come back
    have = sum(1 for _, d in vip_items() if d.get("tier") == "hunter")
    found = 0
    ranked = sorted(hunter_stats().items(), key=lambda kv: (-kv[1]["wsum"], -kv[1]["wins"]))
    scores = wallet_scores() if ranked else {}
    for w, s in ranked:
        if have + found >= HUNTER_MAX or found >= 3:
            break
        decided = s["wins"] + s["stops"]
        if (s["wins"] < HUNTER_MIN_WINS or s["wsum"] < 0.6 * HUNTER_MIN_WINS or s["wins"] / decided < HUNTER_MIN_RATE
                or s["chain"] not in ("solana", "robinhood")):
            continue
        kind = wallet_kind(w)
        key = w.lower() if kind == "evm" else w
        if not kind or key in removed or key in EVM_WHALES or (kind == "solana") != (s["chain"] == "solana") or recently_demoted(key):
            continue
        if MUTE_ON and cold_record(w, scores):
            continue                         # v3.8: a few low-cap wins do not outweigh a record that keeps losing overall
        cur = state["watch"].get(key)
        if isinstance(cur, dict) and cur.get("vip"):
            continue
        info = state["watch"].setdefault(key, {"kind": kind, "t": time.time()})
        note = f"low-cap hunter found by the bot: {s['wins']} of its {decided} finished low-cap buys doubled"
        info.update(vip=True, tier="hunter", chain=s["chain"], note=note, sure="found")
        if not info.get("name"):
            info["name"] = "hunter-" + (key[2:8] if kind == "evm" else key[:6])
        found += 1
        bot_stats["hunter"] += 1
        names = ", ".join("✅$" + md_escape(c) for c in s["coins"][:5])
        wchain = s["chain"] if s["chain"] in CHAINS else "solana"
        send_telegram(
            f"🎯 *NEW LOW-CAP HUNTER FOUND*\n\n`{key}`\n"
            f"{s['wins']} of its {decided} finished buys of coins under {fmt_usd(lowcap_limit())} doubled: {names}\n"
            f"It is on the VIP list now, so when it buys a low-cap coin you get the loud message too. Take it off with /vip remove {md_escape(info['name'])}.\n"
            f"[Wallet]({link(wchain, 'addr', key)})")
        log.info("low-cap hunter promoted: %s (%d/%d)", short(key), s["wins"], decided)
    return found


# --- the low-cap crowd alert: several saved wallets bought the same coin while it is still small ---
def lowcap_crowd(key: str, now: float, scores: dict | None = None, old_way: bool = False, cold_ok: bool = False) -> list[tuple[str, dict]]:
    """The saved wallets that are in this coin right now: [(wallet, buy)].
    v3.8: a wallet counts only if its LATEST buy of the coin really happened within the last LOWCAP_FRESH_HOURS ('bt' = the trade's own
    time; a buy saved by an older version has no such time and does not count) and it has not gone cold (a record that keeps losing).
    old_way=True gives the crowd the way versions up to 3.7 counted it (any buy I saved in the last 36 hours) - only used to tell
    which crowds the new rules turned down."""
    out = []
    fresh = lowcap_fresh_hours() * 3600
    sc = [scores]                                  # (worked out only if a wallet's buy is fresh enough to matter)

    def cold(w: str) -> bool:
        if cold_ok or not MUTE_ON:
            return False
        if sc[0] is None:
            sc[0] = wallet_scores()
        return wallet_cold(w, sc[0])

    for w in list(_book_idx.get(key, [])):
        d = state["book"].get(w)
        for p in (d or {}).get("picks", []):
            if not (isinstance(p, dict) and p.get("k") == key and p.get("st") == "o"):
                continue
            if old_way:
                ok = now - num(p, "t") <= 36 * 3600
            else:
                bt = num(p, "bt")
                ok = bt > 0 and now - bt <= fresh and not cold(w)
            if ok:
                out.append((w, p))
                break
    return out


def lowcap_age_cap() -> float:
    """The oldest coin (days) the crowd alert will talk about: LOWCAP_MAX_AGE_DAYS, or what /lowcap age set (0 = no limit)."""
    return _tune_num("lowcap_age", LOWCAP_MAX_AGE_DAYS)


def lowcap_fresh_hours() -> float:
    return max(1.0, _tune_num("lowcap_fresh", LOWCAP_FRESH_HOURS))


def lowcap_ready(key: str, mc: float, liq: float, now: float, scores: dict | None = None, age_days: float | None = None,
                 why: list | None = None) -> dict | None:
    """Does this coin qualify for the low-cap crowd alert right now? -> {"rows", "total", "run"} or None.
    v3.8: the buys must be FRESH (see lowcap_crowd), the coin not older than lowcap_age_cap() days, and the wallets not far up (LOWCAP_MAX_RUN)
    or far down (LOWCAP_MAX_DOWN) on what they paid. When a crowd that the old rules would have pinged is turned down, the reason ('stale',
    'cold', 'old', 'up', 'down') is appended to 'why' - those crowds are followed quietly (lshadow) to see whether the rule was right."""
    if not alert_on("lowcap") or mc <= 0 or mc > lowcap_limit() or key in state["steady"]:
        return None
    cd = state["cooldowns"]
    if (now - cd.get("lc:" + key, 0) < 3 * 86400 or now - cd.get("vip:" + key, 0) < 12 * 3600
            or now - cd.get("sb:" + key, 0) < 12 * 3600 or liq < LOWCAP_MIN_LIQ_USD):
        return None
    if len(lowcap_crowd(key, now, old_way=True)) < LOWCAP_MIN_WALLETS:
        return None                                   # not a crowd at all, by any way of counting

    def no(reason: str):
        if why is not None:
            why.append(reason)
        return None

    if scores is None:
        scores = wallet_scores()
    rows = lowcap_crowd(key, now, scores=scores)
    if len(rows) < LOWCAP_MIN_WALLETS:
        return no("cold" if len(lowcap_crowd(key, now, scores=scores, cold_ok=True)) >= LOWCAP_MIN_WALLETS else "stale")
    cap = lowcap_age_cap()
    if cap > 0 and age_days and age_days > cap:
        return no("old")
    total = sum(num(p, "usd") for _, p in rows)
    if total < LOWCAP_MIN_USD:
        return None
    run = median([num(p, "lr", default=1.0) or 1.0 for _, p in rows]) - 1
    if run > LOWCAP_MAX_RUN:
        return no("up")
    if run < -LOWCAP_MAX_DOWN:
        return no("down")
    return {"rows": rows, "total": total, "run": run}


def lowcap_alert_coming(key: str, now: float, scores: dict | None = None) -> bool:
    """Is a low-cap crowd alert about to go out for this coin? Then that alert already shows who is buying."""
    hit = _mc_live.get(key)
    if not hit or now - hit[0] > 900 or len(day_count("lc_ts", now)) >= LOWCAP_MAX_PER_DAY:
        return False
    return lowcap_ready(key, hit[1], hit[2], now, scores, (hit[3] if len(hit) > 3 else 0.0) or None) is not None


def maybe_add_lshadow(key: str, chain: str, pair: dict, reason: str, now: float):
    """v3.8: a crowd the new rules turned down (a coin that is too old, buys that are not fresh, wallets gone cold, a coin already up or
    down) is followed in silence, so the report can show whether the rule was right (the turned-down crowds that go on to double would
    show it was too strict)."""
    if open_records(key) or now - state["shadow_cd"].get("l:" + key, 0) < SHADOW_COOLDOWN:
        return
    if len(fresh_open("lshadow")) >= LSHADOW_MAX:
        return
    if not clear_to_alert(key, chain, pair):
        return
    m = compute_metrics(key, pair)
    if not m:
        return
    state["shadow_cd"]["l:" + key] = now
    add_record("lshadow", key, chain, pair, m, reason, now, extra={"th": BOOK_TRACK_HOURS})


def build_lowcap_message(chain: str, pair: dict, m: dict, ready: dict, scores: dict, held: dict, now: float) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(str(base.get("symbol", "?")))
    key = sk(chain, addr)
    rows = sorted(ready["rows"], key=lambda r: (-{"watched": 3, "proven": 2, "promising": 1}.get(book_grade(r[0], scores), 0), -num(r[1], "usd")))
    px = num(pair, "priceUsd")
    L = [f"*🎯 LOW-CAP CROWD: ${sym}*  |  {CHAINS[chain]['name']}", "",
         f"{len(rows)} saved wallets bought it within the last {span_text(lowcap_fresh_hours())} (together about {fmt_usd(ready['total'])}) "
         "while it is still small:"]
    for w, p in rows[:4]:
        paid = num(p, "px")
        vs = f", now {px / paid - 1:+.0%} on that price" if paid > 0 and px > 0 and 0.2 <= px / paid <= 5 else ""
        bt = num(p, "bt")
        when = f" (last buy {since_text(max(0.0, now - bt))} ago)" if bt > 0 else ""
        more = also_buying(None, w, key)
        L.append(f"• `{w}`  {wallet_record_text(w, scores)}\n   bought about {fmt_usd(num(p, 'usd'))}{when}{vs}"
                 + (" · also holding " + ", ".join(more[:3]) if more else ""))
    if len(rows) > 4:
        L.append(f"   ...and {len(rows) - 4} more")
    L += ["", f"💰 Market cap *{fmt_usd(m['mc'])}* (a low cap: {fmt_usd(lowcap_limit())} or less) · down {m['retrace']:.0f}% from its high of {fmt_usd(m['peak'])}",
          f"💧 liquidity {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC) · 📊 24h volume {fmt_usd(m['vol24'])} "
          f"({m['vol_ratio'] * 100:.0f}% of MC) · 👥 {m['traders']:,} txns · ⏱ {fmt_age(m['age_h'])}"]
    body = "\n".join(L) + "\n" + wallets_line(held) + chain_note(chain)
    body += (f"📌 I follow it for {follow_text(BOOK_TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). Wallets that buy the same coin "
             "can all be wrong together - a crowd is one clue, not several.\n\n"
             f"`{addr}`\n\n[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | [Token]({link(chain, 'token', addr)})")
    return body


def lowcap_pass(cands: list, now: float):
    """After every scan: coins that three or more saved wallets bought, that are still low-cap and have not run away, get one alert."""
    if not alert_on("lowcap") or not cands:
        return
    stamps = day_count("lc_ts", now)
    scores = wallet_scores()
    for key, chain, pair, mc in cands:
        if len(stamps) >= LOWCAP_MAX_PER_DAY:
            break
        why: list[str] = []
        ready = lowcap_ready(key, mc, num(pair, "liquidity", "usd"), now, scores, age_days_of(pair) or None, why)
        if not ready:
            if why:
                try:
                    maybe_add_lshadow(key, chain, pair, why[0], now)        # v3.8: turned down - but still followed, quietly
                except Exception:
                    log.exception("quiet follow of a turned-down crowd failed")
            continue
        if not confirmed_twice("lc", key, mc, now) or not clear_to_alert(key, chain, pair):
            continue
        m = compute_metrics(key, pair)
        if not m:
            continue
        held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
        if not send_telegram(build_lowcap_message(chain, pair, m, ready, scores, held, now)):
            continue
        state["cooldowns"]["lc:" + key] = now
        stamps.append(now)
        drop_quiet_follow(key, now)
        add_record("lowcap", key, chain, pair, m, "", now,
                   extra={"th": BOOK_TRACK_HOURS, "sig": signals_for(m, len(held["in"])), "n": len(ready["rows"]), "usd": round(ready["total"])})
        bot_stats["lowcap"] += 1
        log.info("LOW-CAP CROWD alert for %s (%d wallets)", key, len(ready["rows"]))


# --- chat commands: /vip /lowcap /shill /kol ---
def parse_money(text: str) -> float | None:
    """'500k', '1.5m', '$750,000' -> dollars."""
    t = str(text).strip().lower().replace(",", "").replace("$", "").replace("_", "")
    mult = 1.0
    for suf, mu in (("k", 1e3), ("m", 1e6), ("b", 1e9)):
        if t.endswith(suf):
            t, mult = t[:-1], mu
            break
    try:
        v = float(t) * mult
    except ValueError:
        return None
    return v if math.isfinite(v) and v > 0 else None


def vip_add(rest: list[str]) -> str:
    addr = next((a for a in rest if wallet_kind(a)), None)
    if not addr:
        return ("Send it like this: /vip add <wallet address> <a name you choose> [kol] [picky|spray|normal]\n"
                "• kol at the end: every coin it buys also goes on the dip-watch.\n"
                "• picky: a wallet that rarely trades (you get the loud ping on bigger coins too) · spray: a wallet that buys everything (quiet messages) · "
                "no word: I judge it myself from how many different coins it buys.")
    words = [a for a in rest if a != addr]
    kol, style = False, None
    while words and (words[-1].lower() == "kol" or (len(words) > 1 and words[-1].lower() in STYLE_WORDS)):    # (the words may come in either order)
        wd = words.pop().lower()
        if wd == "kol":
            kol = True
        else:
            style = STYLE_WORDS[wd]
    kind = wallet_kind(addr)
    new = (addr.lower() if kind == "evm" else addr) not in state["watch"]
    key = add_watch(addr, " ".join(words).strip())
    info = state["watch"][key]
    kol = kol or bool(info.get("kol"))                 # (adding it again without the word kol does not switch the dip-watch off)
    info.update(vip=True, tier="vip", chain="solana" if kind == "solana" else "robinhood", kol=kol, sure="added by you")
    info.setdefault("note", "added by you")
    info.setdefault("vip_t", time.time())
    if style == "auto":
        info.pop("style", None)
    elif style:
        info["style"] = style
    style_start()
    off = state["tuning"].get("vip_off")
    if isinstance(off, list) and key in off:
        off.remove(key)
    if kind == "solana":
        how = "I look inside it every ~90 seconds."
    else:
        how = ("I read its coin transfers on Robinhood Chain every ~90 seconds. (For a VIP wallet I only look at Robinhood Chain; "
               "if this one trades on another chain, use /watch for that.)")
    cur = info.get("style")
    if cur == "selective":
        st = (f"\n🎯 Style: *picky* (you set it) - when it buys, you get the loud message on any coin up to {fmt_usd(PICKY_MAX_MC)} market cap, "
              "with one extra repeat.")
    elif cur == "spray":
        st = (f"\n🔕 Style: *sprayer* (you set it) - its buys of coins under {fmt_usd(lowcap_limit())} arrive as a silent message "
              f"(at most {SPRAY_MAX_PER_DAY} a day); its other buys are only written down.")
    elif cur == "normal":
        st = "\n▫️ Style: normal (you set it) - the same message for every low-cap buy."
    else:
        st = ("\n🎯 Style: I judge it myself from how many different coins it buys (a picky wallet gets a better ping, a sprayer a quiet one). "
              "That needs a few days; tell me now with /vip style " + md_escape(wallet_label(key)) + " picky|spray|normal.")
    return (f"🚨 *{md_escape(wallet_label(key))}* is {'now' if new else 'already'} on the VIP list (`{short(key)}`). {how}\n"
            f"When it buys a coin under {fmt_usd(lowcap_limit())} market cap you get the loud message."
            + (" Coins it buys also go on the dip-watch." if kol else "") + st)


def vip_set_style(rest: list[str]) -> str:
    """/vip style <name> picky|spray|normal|auto - or just /vip style to see how the styles work and what each wallet is right now."""
    now = time.time()
    word = rest[-1].lower() if rest else ""
    if rest and word in STYLE_WORDS and len(rest) >= 2:
        key = find_watch(" ".join(rest[:-1]))
        if not key or not (state["watch"].get(key) or {}).get("vip"):
            return "I don't have a VIP wallet like that. Send /vip to see the list."
        info = state["watch"][key]
        style = STYLE_WORDS[word]
        if style == "auto":
            info.pop("style", None)
        else:
            info["style"] = style
        save_state()
        cur, why = vip_style(key, info, now)
        return (f"✅ *{md_escape(wallet_label(key))}* is now {'judged by me again' if style == 'auto' else 'set to ' + style_icon(cur)}"
                + (f" (right now I would call it {style_icon(cur)}" + (f": {md_escape(why)}" if why else "") + ")" if style == "auto" else "") + ".")
    L = ["🎯 *Wallet styles* - how many DIFFERENT coins a wallet buys decides how I ping you (big wallets that rarely trade tend to be right more often than ones that buy everything):",
         f"• 🎯 *picky* - {STYLE_PICKY_7D} coins or fewer in 7 days (and {STYLE_PICKY_24H} a day at most), after {STYLE_PICKY_MIN_DAYS:g} days of watching it: "
         f"loud message on any coin up to {fmt_usd(PICKY_MAX_MC)} market cap, one extra repeat, and it does not share the daily limit of the others",
         "• ▫️ *normal* - the loud message for coins under your low-cap line, a short one for bigger coins",
         f"• 🔕 *sprayer* - {STYLE_SPRAY_7D}+ coins in 7 days or {STYLE_SPRAY_24H}+ in one day: a silent message for low-cap buys only "
         f"(at most {SPRAY_MAX_PER_DAY} a day), no repeats"]
    items = vip_items()
    if items:
        L.append("\nRight now:")
        for w, d in items:
            st, why = vip_style(w, d, now)
            n7, n1 = style_counts(w, now)
            L.append(f"• {md_escape(wallet_label(w))} - {style_icon(st)}" + (f" ({md_escape(why)})" if why else "") + f" · {n7} coin{'' if n7 == 1 else 's'} in 7 days, {n1} today")
    L.append("\nSet one yourself: /vip style <name> picky|spray|normal|auto  (auto = I judge it)")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def vip_remove(rest: list[str]) -> str:
    key = find_watch(" ".join(rest))
    if not key or not (state["watch"].get(key) or {}).get("vip"):
        return "I don't have a VIP wallet like that. Send /vip to see the list."
    info = state["watch"][key]
    label = wallet_label(key)
    for f in ("vip", "tier", "kol", "sure", "note", "chain", "style", "style_seeded", "vip_t"):
        info.pop(f, None)
    buys = state["tuning"].get("vip_buys")
    if isinstance(buys, dict):
        buys.pop(key, None)
    off = state["tuning"].get("vip_off")
    if not isinstance(off, list):
        off = state["tuning"]["vip_off"] = []
    if key not in off:
        off.append(key)                   # (so that a built-in VIP does not come back at the next start-up)
    cursors = vip_cursors()
    cursors.pop(key, None)
    return (f"🗑 *{md_escape(label)}* is off the VIP list. I still watch it like your other wallets (/unwatch stops that too).")


def cmd_vip(args: list[str]) -> str:
    sub = args[0].lower() if args else ""
    if sub in ("add", "new"):
        return vip_add(args[1:])
    if sub in ("remove", "rm", "delete", "del", "off"):
        return vip_remove(args[1:])
    if sub in ("style", "styles", "type", "types"):
        return vip_set_style(args[1:])
    if sub in ("min", "minimum") or sub in ("ping", "pings", "repeat", "repeats"):
        if len(args) < 2:
            return "Send it like this: /vip min 300   (ignore buys under $300)   or   /vip ping 3   (repeat each loud message 3 times, 0-4)"
        if sub.startswith("min"):
            v = parse_money(args[1])
            if v is None or v > 1_000_000:
                return "Send an amount in dollars, like /vip min 300"
            state["tuning"]["vip_min"] = v
            save_state()
            return f"✅ A VIP buy under {fmt_usd(v)} will not be announced."
        try:
            n = int(float(args[1]))
        except ValueError:
            return "Send a number from 0 to 4, like /vip ping 3"
        n = max(0, min(len(VIP_REMIND_AFTER), n))
        state["tuning"]["vip_ping"] = n
        save_state()
        return f"✅ A loud VIP message is now repeated {n} time{'' if n == 1 else 's'}."
    items = vip_items()
    now = time.time()
    L = [f"🚨 *VIP wallets* - when one buys a coin under {fmt_usd(lowcap_limit())} market cap, I ping you loud"
         + (f" and repeat it {vip_pings()} time{'' if vip_pings() == 1 else 's'}" if vip_pings() else "") + "."
         + ("" if alert_on("vip") else "\n⚠️ The VIP alert is switched OFF (/alerts vip on). Buys are still followed quietly.")]
    if not items:
        L.append("\nNo VIP wallets yet. Add one: /vip add <wallet address> <name>")
    for i, (w, d) in enumerate(items, 1):
        chain = d.get("chain") if d.get("chain") in CHAINS else ("solana" if d.get("kind") == "solana" else "ethereum")
        tag = "🎯 hunter found by the bot" if d.get("tier") == "hunter" else {"certain": "✅ certain match", "likely": "🔶 probable match"}.get(d.get("sure"), "")
        last = vip_last_buy(w)
        lb = (f"\n   last buy seen: ${md_escape(str(last.get('s', '?')))} {fmt_usd(last.get('usd', 0))} at market cap {fmt_usd(last.get('mc', 0))}, "
              f"{ago(now - last.get('t', now))} ago" if last else "\n   no buy seen since I started looking")
        L.append(f"\n{i}. *{md_escape(wallet_label(w))}* · {CHAINS[chain]['name']} · [{short(w)}]({link(chain, 'addr', w)})"
                 + (f" · {tag}" if tag else "") + (" · 📉 dip-watch on" if d.get("kol") else "") + style_tag(w, d, now)
                 + f"\n   {vip_status_text(w, d)}{lb}")
    L.append("\n/vip add <address> <name> [kol] [picky|spray] · /vip style <name> picky|spray|normal|auto · /vip remove <name> · /vip min 300 · /vip ping 3 · /lowcap 500k · /alerts vip off")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def cmd_lowcap(args: list[str]) -> str:
    if args and args[0].lower() in ("reset", "default", "standard"):
        for name in ("lowcap_mc", "lowcap_age", "lowcap_fresh", "smartbuy_age", "smartbuy_fresh"):
            state["tuning"].pop(name, None)
        save_state()
        return (f"↩️ A low cap is back to {fmt_usd(LOWCAP_MC)} market cap or less, a coin counts as young up to {LOWCAP_MAX_AGE_DAYS:g} days old, "
                f"and a buy counts as fresh for {LOWCAP_FRESH_HOURS:g} hours.")
    if args and args[0].lower() in ("age", "old", "young"):
        if len(args) < 2:
            return (f"A coin older than {lowcap_age_cap():g} days gets no low-cap crowd alert (the crowd is followed quietly instead). "
                    "Change it with /lowcap age 30 (days, 2 to 365).")
        try:
            v = float(args[1].lower().rstrip("d"))
        except ValueError:
            return "Send a number of days from 2 to 365, like /lowcap age 14"
        if not 2 <= v <= 365:
            return "Send a number of days from 2 to 365, like /lowcap age 14"
        state["tuning"]["lowcap_age"] = v
        save_state()
        return (f"✅ The low-cap crowd alert now only fires for coins up to {v:g} days old. Older coins are still followed quietly, "
                "so /lowcap shows whether they would have paid.")
    if args and args[0].lower() in ("fresh", "freshness", "recent"):
        if len(args) < 2:
            return (f"A wallet's buy counts as fresh for {lowcap_fresh_hours():g} hours. Change it with /lowcap fresh 12 "
                    "(hours, 1 to 168).")
        try:
            v = float(args[1].lower().rstrip("h"))
        except ValueError:
            return "Send a number of hours from 1 to 168, like /lowcap fresh 24"
        if not 1 <= v <= 168:
            return "Send a number of hours from 1 to 168, like /lowcap fresh 24"
        state["tuning"]["lowcap_fresh"] = v
        save_state()
        return (f"✅ The low-cap crowd alert now only counts buys made in the last {v:g} hours. A crowd that bought earlier is "
                "followed quietly instead.")
    if args:
        v = parse_money(args[0])
        if v is None or not 50_000 <= v <= 100_000_000:
            return "Send an amount between $50K and $100M, like /lowcap 500k   (or /lowcap reset)"
        state["tuning"]["lowcap_mc"] = v
        save_state()
        return f"✅ A low cap is now {fmt_usd(v)} market cap or less. Loud VIP pings and the low-cap crowd alert use this line."
    return (f"🎯 A *low cap* is a market cap of {fmt_usd(lowcap_limit())} or less.\n"
            "• A VIP wallet buying one = the loud message (/vip)\n"
            f"• {LOWCAP_MIN_WALLETS}+ saved wallets that each bought it in the last {lowcap_fresh_hours():g} hours, on a coin up to "
            f"{lowcap_age_cap():g} days old, that has not already run up = the low-cap crowd alert (/alerts lowcap off switches it)\n"
            "Change the line with /lowcap 500k, the coin age with /lowcap age 30, the freshness of the buys with /lowcap fresh 12 "
            "(or /lowcap reset).")


def cmd_kol(args: list[str]) -> str:
    if args and args[0].lower() in ("remove", "rm", "delete", "del"):
        want = " ".join(args[1:]).strip().lstrip("$").lower()
        hit = next((k for k, e in state["kol"].items() if want and (want == str(e.get("sym", "")).lower() or want == k.split(":", 1)[1].lower())), None)
        if not hit:
            return "I don't have a coin like that on the dip-watch. Send /kol to see the list."
        sym = state["kol"].pop(hit).get("sym", "?")
        return f"🗑 ${md_escape(str(sym))} is off the dip-watch."
    kol = state["kol"]
    if not kol:
        return ("📉 *Dip-watch* is empty. Coins that unipcs buys go on it by themselves; /shill <coin address> adds one he talked about.\n"
                f"I alert when a coin on it is down {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% from its high.")
    now = time.time()
    mcs = live_mcs(list(kol), fetch=False)

    def near(item):
        e = item[1]
        dd = e.get("dd", 0)
        return 0 if KOL_DIP_MIN <= dd <= KOL_DIP_MAX else min(abs(dd - KOL_DIP_MIN), abs(dd - KOL_DIP_MAX))
    rows = sorted(kol.items(), key=near)
    L = [f"📉 *Dip-watch* - {len(kol)} coin{'' if len(kol) == 1 else 's'} a KOL bought or shilled. I alert when one is down "
         f"{KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% from its high." + ("" if alert_on("kol") else "\n⚠️ The dip-watch alert is switched OFF (/alerts kol on).")]
    for i, (k, e) in enumerate(rows[:12], 1):
        chain, addr = k.split(":", 1)
        dd, mc = e.get("dd", 0), mcs.get(k) or e.get("mc", 0)
        state_txt = ("🔔 in the zone now" if KOL_DIP_MIN <= dd <= KOL_DIP_MAX else "⏳ not down enough yet" if dd < KOL_DIP_MIN
                     else "⛔ fell past the zone")
        verb = "shilled" if e.get("how") == "shilled" else "bought"
        L.append(f"{i}. ${md_escape(str(e.get('sym', '?')))} · {CHAINS[chain]['name']} · "
                 + (f"MC {fmt_usd(mc)} · " if mc else "") + f"down {dd:.0f}% from {fmt_usd(e.get('peak', 0))} · {state_txt}\n"
                 f"   {md_escape(str(e.get('who', 'KOL')))} {verb} it {ago(now - e.get('t', now))} ago · `{addr}`")
    if len(rows) > 12:
        L.append(f"...and {len(rows) - 12} more")
    L.append("\n/shill <coin address> adds one · /kol remove <symbol or address> takes one off")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def cmd_shill(args: list[str], notify=None) -> str:
    refs = extract_refs(" ".join(args))
    if not refs:
        return ("Send it like this: /shill <coin address or DexScreener link> [name]\n"
                f"It goes on the dip-watch: I alert when it is down {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% from its high. "
                "The name is who talked about it (the default is unipcs).")
    chain, addr, err = resolve_coin_ref(refs[0])
    if err:
        return err.replace("/check", "/shill")
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
    if mc <= 0:
        return "That coin has no market cap on DexScreener yet, so I can't watch its dip."
    who = " ".join(a for a in args if not extract_refs(a)).strip()[:30] or "unipcs"
    now = time.time()
    sym = md_escape(str(pair.get("baseToken", {}).get("symbol", "?")))
    new = kol_add(key, chain, pair, who, mc, now, how="shilled")
    e = state["kol"][key]
    if not new:
        e["alerted"] = 0                      # asked for again by hand: it may alert again
    note_mc(key, mc, num(pair, "liquidity", "usd"))
    peak = max(e.get("peak", 0.0), state["peaks"].get(key, 0.0), mc)
    e.update(peak=peak, dd=round((peak - mc) / peak * 100, 1) if peak > 0 else 0)
    _vip_rt["kol_now"] = True
    save_state()
    return (f"{'✅' if new else 'ℹ️'} ${sym} is {'now' if new else 'already'} on the dip-watch ({md_escape(who)}). Market cap {fmt_usd(mc)}, "
            f"down {e['dd']:.0f}% from the highest I know of ({fmt_usd(peak)}).\n"
            f"I alert when it is down {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% from its high"
            + (" - it is in that zone right now, so the alert follows within a few minutes." if KOL_DIP_MIN <= e["dd"] <= KOL_DIP_MAX else
               " (its real past high is looked up in the next minutes)."))


# --- status lines, report lines, start-up note ---
def vip_status_lines() -> list[str]:
    """For the status check-in."""
    try:
        items = vip_items()
        if not items:
            return []
        now = time.time()
        named = [md_escape(wallet_label(w)) for w, d in items if d.get("tier") != "hunter"]
        hunters = sum(1 for _, d in items if d.get("tier") == "hunter")
        loud = len(_last_day(state["tuning"].get("vip_ts"), now))
        L = [f"🚨 VIP wallets: {', '.join(named[:5]) or 'none'}" + (f" + {hunters} low-cap hunter{'' if hunters == 1 else 's'}" if hunters else "")
             + f" · a low cap = {fmt_usd(lowcap_limit())} or less · loud pings in the last 24h: {loud} (/vip)"]
        bad = [w for w, _ in items if _vip_rt["err"].get(w, 0) >= 3]
        if bad:
            L.append(f"⚠️ I couldn't look inside {len(bad)} VIP wallet(s) lately: {md_escape(vip_status_text(bad[0]))}")
        if STYLE_ON:
            sty = Counter(vip_style(w, d, now)[0] for w, d in items)
            if sty.get("selective") or sty.get("spray"):
                L.append(f"🎯 Wallet styles: {sty.get('selective', 0)} picky (louder pings), {sty.get('spray', 0)} sprayer{'' if sty.get('spray', 0) == 1 else 's'} "
                         f"(quiet pings), {sty.get('normal', 0)} normal - /vip style")
        if state["kol"]:
            inz = sum(1 for e in state["kol"].values() if KOL_DIP_MIN <= e.get("dd", 0) <= KOL_DIP_MAX)
            L.append(f"📉 Dip-watch: {len(state['kol'])} coin{'' if len(state['kol']) == 1 else 's'} (alert at {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% down"
                     + (f"; {inz} in the zone now" if inz else "") + ") - /kol")
        return L
    except Exception:
        log.exception("VIP status lines failed")
        return []


def vip_startup_note() -> str:
    names = ", ".join(md_escape(wallet_label(w)) for w, d in vip_items() if d.get("tier") != "hunter") or "none yet"
    return (f"🆕 v3.5: I now look inside {names} (/vip). When one of them buys a coin under {fmt_usd(lowcap_limit())} market cap you get a loud "
            f"message that repeats (/lowcap changes that line). Every coin I list now shows its market cap. Coins unipcs buys go on a dip-watch: "
            f"you hear when one is down {KOL_DIP_MIN:g}-{KOL_DIP_MAX:g}% from its high (/kol; /shill adds one by hand). A new alert tells you when "
            f"{LOWCAP_MIN_WALLETS}+ saved wallets buy the same low-cap coin. Bottom and chart alerts are OFF now (only about 2 in 40 of each paid off; "
            "/alerts bottom on or /alerts chart on brings them back, and I still follow them quietly).\n")


def startup_note36() -> str:
    """The v3.6 start-up note (shown once)."""
    return ("🆕 v3.6: two new things.\n"
            f"🎯 Wallet styles: I count how many different coins each VIP wallet buys. A *picky* wallet (only a few coins, like a whale that "
            f"rarely trades) gets the loud ping even on a bigger coin, up to {fmt_usd(PICKY_MAX_MC)} market cap, with its record shown. "
            f"A *sprayer* (buys lots of coins) gets a quiet message with no sound, {SPRAY_MAX_PER_DAY} a day at most. I judge each wallet "
            f"myself after {STYLE_PICKY_MIN_DAYS:g} days of watching it, or you tell me: `/vip add <address> <name> picky` "
            "or `/vip style <name> picky|spray|normal|auto`.\n"
            + (f"📡 Accumulation radar: every couple of minutes I read who is buying a few Robinhood Chain coins. When many different wallets "
               f"keep buying one, hold it, and the price has not run away, you get an ACCUMULATION message (/radar shows the list, "
               "/alerts radar off silences it). It is a clue, not a promise: I follow each one for "
               f"{span_text(RADAR_TRACK_HOURS)} and /lowcap shows how they did.\n" if RADAR_ON else ""))


def startup_note37() -> str:
    """The v3.7 start-up note (shown once)."""
    return ("🆕 v3.7: wallets now get *stars*, out of 3. "
            "I follow every buy a wallet makes: a *hit* = the coin doubled before it halved, a *miss* = it halved first, flat = neither. "
            "The stars come from how many of the clear results were hits: about 7 in 10 = ⭐⭐⭐, 6 in 10 = ⭐⭐½, 5 in 10 = ⭐⭐, "
            "4 in 10 = ⭐½, 2-3 in 10 = ⭐, less = ½⭐. A wallet with few results is held back (nobody gets 3 stars before "
            f"{STAR_CAPS[1][0]} finished buys) and 1 hit in 3 already breaks even, so the lines sit lower than 'most of the time'. "
            "/stars shows the scale and every wallet on your list.\n"
            "The old 'N of M good' counted a coin that touched 1.5x and then halved as good, which was too generous, so it is gone: "
            "you now see how many buys doubled, halved and went flat. "
            "👀 now means 'on your watch list' (it used to be ⭐). A wallet I added to your list myself whose results slip is marked 🧊 "
            "cooling off: it stays, but it no longer counts as a good record. Wallets you add yourself are never cooled. "
            "The smart-money points in /check now follow the stars too.\n"
            "Also built in as VIP wallets, so you don't need to paste anything: 'Wood' (Fomo @notanicecat69, Top 100, picky) and 'angery' (Fomo). "
            "I can look inside a VIP wallet on Robinhood Chain for now, so I see angery's Robinhood buys but not his Ethereum ones (like CLAUS).\n")


def startup_note38() -> str:
    """The v3.8 start-up note (shown once)."""
    days = " / ".join(f"{d:g}" for _, d in EXTEND_DAYS)
    return ("🆕 v3.8: fewer and fresher pings, and more patience.\n"
            f"🕒 *Old coins and old buys no longer ping you.* Before, the low-cap crowd alert used the time I happened to look at a wallet, so a buy made "
            f"weeks ago looked new. Now it uses the time the wallet REALLY bought. A crowd only counts buys from the last {span_text(lowcap_fresh_hours())}, "
            f"on a coin up to {lowcap_age_cap():g} days old, that is not already up more than {LOWCAP_MAX_RUN:.0%} (or down {LOWCAP_MAX_DOWN:.0%}) from "
            "the price the wallets paid. A crowd I turn down is followed quietly, and the report shows whether I was right to skip it. "
            f"The smart-wallet alert checks the coin's age too ({SMARTBUY_MAX_AGE_DAYS:g} days; a wallet you named gets through).\n"
            f"⏳ *More than 7 days.* A call that is still alive when its week ends (at least {EXTEND_ALIVE:.0%} of its price) is now followed on, another "
            f"week at a time - up to {days} days, longer for a bigger coin - and it ends early if it falls under the lowest point of its first week. "
            "A late winner is counted as a winner, and the report shows how many came after day 7.\n"
            f"📌 *Coins you send me.* I remember the market cap you sent each one at (/links). Send it again and I tell you how it did since. "
            f"When it reaches {LINK_LOOK_X:g}x I look at which wallets were in early, learn from them, and message you.\n"
            "🧊 *Wallets that keep losing stop counting.* One I added myself comes off your list (it can earn its way back after "
            f"{DEMOTE_DAYS:g} days); one you added stays and I only warn you. A win that was mostly done before I saw the buy counts for less in the stars.\n"
            "Expect fewer low-cap pings than before. Every limit here is a first guess that your results will tune "
            "(/lowcap age, /lowcap fresh).\n")


def startup_note381() -> str:
    """The v3.8.1 start-up note (shown once)."""
    return ("🆕 v3.8.1: the scorecard and the coin lists now tell the same story about who is buying. /check counts every accumulating "
            "wallet (it only kept the biggest 6) and says how many of the buyers have a weak record or are 🧊 cooling. The coin lists "
            "(/coins and the check-in) show how many buyers are trusted, cooling, weak or not rated yet, so a medal can no longer hide "
            "a weak crowd. A 🧊 wallet that holds a coin no longer earns a smart-money point either.\n")


def lowcap_book_numbers() -> dict:
    """From the wallet book: coins that 3+ saved wallets bought while their market cap was low (the price the 3rd wallet paid x the supply).
    -> {"n", "w", "s", "o", "f"} counted by how that 3rd wallet's buy turned out, plus the low-cap coins that already doubled."""
    lim, coins = lowcap_limit(), {}
    for w, d in list(state["book"].items()):
        for p in (d.get("picks") or []) if isinstance(d, dict) else []:
            if isinstance(p, dict) and isinstance(p.get("k"), str) and isinstance(p.get("t"), (int, float)):
                coins.setdefault(p["k"], []).append((p["t"], w, p))
    res = Counter()
    hits: dict[str, int] = {}
    for k, lst in coins.items():
        lst.sort(key=lambda x: x[0])
        seen, third = [], None
        for t, w, p in lst:
            if w in seen:
                continue
            seen.append(w)
            if len(seen) == LOWCAP_MIN_WALLETS:
                third = p
                break
        sup = state["supply"].get(k)
        if third and isinstance(sup, (int, float)) and sup > 0 and isinstance(third.get("px"), (int, float)) and third["px"] * sup <= lim:
            res[third.get("st", "o")] += 1
            res["n"] += 1
        for t, w, p in lst:
            if p.get("st") == "w" and isinstance(sup, (int, float)) and sup > 0 and isinstance(p.get("px"), (int, float)) and p["px"] * sup <= lim:
                hits[str(p.get("s", "?"))] = hits.get(str(p.get("s", "?")), 0) + 1
    return {"n": res["n"], "w": res["w"], "s": res["s"], "o": res["o"], "f": res["f"], "hits": sorted(hits.items(), key=lambda kv: -kv[1])[:6]}


LSHADOW_LABEL = {"stale": "bought over a day ago", "cold": "wallets with a losing record", "old": "old coin", "up": "already up a lot",
                 "down": "already down", "sb-old": "smart-wallet buy on an old coin"}


def lshadow_report_lines() -> list[str]:
    """v3.8: the crowds the low-cap alert turned down (old coin, stale buys, ...) are followed quietly: did the turned-down ones pay?"""
    out: list[str] = []
    try:
        done, still = closed_records("lshadow"), len(open_records(kind="lshadow"))
        if not done and not still:
            return out
        by: dict[str, list[dict]] = {}
        for r in done:
            by.setdefault(r.get("failed") or "?", []).append(r)
        parts = []
        for code, lab in LSHADOW_LABEL.items():
            if by.get(code):
                y = summarize(by[code])
                parts.append(f"{lab} {y['w']}/{y['n']}")
        y = summarize(done) if done else None
        out.append("🚫 Crowds I turned down and followed quietly (doubled / finished): "
                   + (" | ".join(parts) if parts else "none finished yet")
                   + (f" → together {y['w']} of {y['n']} doubled" if y and y["n"] else "") + f" · {still} still open")
        al = closed_records("lowcap")
        if al and y and y["n"]:
            z = summarize(al)
            out.append(f"   For comparison the low-cap crowd alerts you did get: {z['w']} of {z['n']} doubled "
                       "(if the turned-down crowds do as well, my rules are too strict - tell me).")
    except Exception:
        log.exception("turned-down crowds report failed")
    return out


def patience_report_lines() -> list[str]:
    """v3.8: what the extra time did. A follow-up that is still alive when its first week ends is followed on instead of being filed as flat."""
    out: list[str] = []
    try:
        kinds = set(ALERT_KINDS) | {"shadow", "bshadow", "cshadow", "lshadow", "check"}
        ext = [r for r in state["records"] if r.get("kind") in kinds and r.get("ext") and not r.get("void")]
        if not ext:
            return out
        done = [r for r in ext if r["status"] != "open"]
        wins = [r for r in done if r["status"] == "win"]
        stops = [r for r in done if r["status"] == "stop"]
        broke = [r for r in done if r["status"] == "expired" and r.get("why") == "broke"]
        timed = [r for r in done if r["status"] == "expired" and r.get("why") != "broke"]
        days = [(r["t1"] - r["t0"]) / 86400 for r in wins if r.get("t1")]
        out.append(f"⏳ Extra time: {len(ext)} follow-ups were still alive when their first {span_text(TRACK_HOURS)} ended and were followed on "
                   f"(before, they were filed as flat). So far {len(wins)} went on to double"
                   + (f" (typically around day {median(days):.0f})" if days else "")
                   + f", {len(stops)} halved, {len(broke)} fell under their first-week low and were dropped, {len(timed)} ran out of time, "
                   f"{len(ext) - len(done)} are still open.")
        tiers = []
        for lab, lo, hi in (("under $1M", 0, 1e6), ("$1M to $10M", 1e6, 1e7), ("over $10M", 1e7, float("inf"))):
            rs = [r for r in done if lo <= r.get("mc0", 0) < hi]
            if rs:
                tiers.append(f"{lab}: {sum(1 for r in rs if r['status'] == 'win')}/{len(rs)}")
        if tiers:
            out.append("   Doubled on the extra time, by size of the coin when called: " + " | ".join(tiers)
                       + " (bigger coins are meant to need longer - this shows whether that is true).")
        every = [r for r in state["records"] if r.get("kind") in kinds and r["status"] == "win" and not r.get("void") and r.get("t1")]
        late = [r for r in every if r["t1"] - r["t0"] > TRACK_HOURS * 3600 * 1.05]
        if every and late:
            out.append(f"   Of all {len(every)} doubles I have seen, {len(late)} came after day {TRACK_HOURS / 24:g} - a plain 7-day window would have filed them as flat.")
    except Exception:
        log.exception("extra-time report failed")
    return out


def lowcap_report_lines() -> list[str]:
    out: list[str] = []
    try:
        x = lowcap_book_numbers()
        if x["n"]:
            decided = x["w"] + x["s"]
            out.append(f"🎯 Low-cap check: of {x['n']} coins under {fmt_usd(lowcap_limit())} that {LOWCAP_MIN_WALLETS}+ saved wallets bought, "
                       f"*{x['w']}* doubled and {x['s']} halved first" + (f" ({x['w'] / decided:.0%} of the decided ones)" if decided else "")
                       + f", {x['o'] + x['f']} still undecided.")
        if x["hits"]:
            out.append("   Low-cap coins that doubled for saved wallets: " + ", ".join(f"${md_escape(s)} ({n})" for s, n in x["hits"]))
        for kind, label in (("vip", "VIP low-cap pings"), ("lowcap", "Low-cap crowd alerts"), ("kol", "Dip-watch alerts"),
                            ("radar", "Accumulation radar alerts")):
            rs = closed_records(kind)
            if kind == "vip":
                rs = [r for r in rs if not r.get("big")]            # (a picky wallet's buy of a bigger coin has its own yardstick, below)
            if rs:
                y = summarize(rs)
                out.append(f"🚨 {label} finished: *{y['n']}*  ✅ {y['w']} wins | 🛑 {y['s']} stopped | ⏱ {y['e']} flat → win rate *{y['wr']:.0%}*, "
                           f"average best move *{y['avg_best']:.2f}x*  (open now: {len(open_records(kind=kind))})"
                           + (f"  [win = +{(RADAR_WIN_X - 1) * 100:.0f}% before -{(1 - RADAR_STOP_X) * 100:.0f}%]" if kind == "radar" else ""))
            elif open_records(kind=kind):
                out.append(f"🚨 {label}: {len(open_records(kind=kind))} being followed, none finished yet.")
        low_vip = [r for r in closed_records("vip") if not r.get("big")]
        by_style = {st: [r for r in low_vip if (r.get("style") or "normal") == st] for st in ("selective", "normal", "spray")}
        if by_style["selective"] or by_style["spray"]:
            parts = []
            for st, lab in (("selective", "🎯 picky wallets"), ("normal", "▫️ normal"), ("spray", "🔕 sprayers")):
                if by_style[st]:
                    y = summarize(by_style[st])
                    parts.append(f"{lab} {y['w']} of {y['n']} won ({y['wr']:.0%})")
            out.append("🎯 VIP low-cap pings by wallet style: " + " · ".join(parts))
        big = [r for r in closed_records("vip") if r.get("big")]
        if big:
            y = summarize(big)
            out.append(f"🎯 Picky wallets on bigger coins (win = +{(RADAR_WIN_X - 1) * 100:.0f}% before -{(1 - RADAR_STOP_X) * 100:.0f}%): "
                       f"{y['w']} of {y['n']} won, average best move {y['avg_best']:.2f}x")
    except Exception:
        log.exception("low-cap report lines failed")
    return out + lshadow_report_lines() + patience_report_lines()


# ----------------------------------------------------------------------------
# v3.6 - THE ACCUMULATION RADAR (Robinhood Chain): coins that many wallets keep buying and holding.
# Read-only: it only reads the chain's public transfer log (the same free node as the VIP wallets). Nothing is ever signed or sent.
# For each coin it takes every transfer of the last day and sorts them: pool/router -> wallet = a buy, wallet -> pool/router = a sell,
# wallet <-> wallet = a move. Pools and routers are not listed anywhere: an address that moves the coin to/from 10+ different addresses
# and is a contract is a hub. A wallet that bought, sold little and still holds most of it is an 'accumulator'.
# ----------------------------------------------------------------------------
ZERO_ADDR = "0x" + "0" * 40
DEAD_ADDR = "0x000000000000000000000000000000000000dead"
RADAR_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"      # the Uniswap v4 PoolManager on Robinhood Chain: the coins of every v4 pool sit in it
RADAR_SEED_HUBS = {RADAR_POOL_MANAGER, ZERO_ADDR, DEAD_ADDR} | set(FOMO_ROUTERS)
_radar_rt: dict = {"cands": [], "cand_t": 0.0, "latest": (0.0, 0), "last_err": "", "force": False, "ticks": 0}


def radar_coins() -> dict:
    """The radar's readings {coin key: reading} - the place is made if it is missing (a fresh start has no saved memory to bring it)."""
    rad = state["radar"]
    if not isinstance(rad, dict):
        rad = state["radar"] = {}
    coins = rad.get("coins")
    if not isinstance(coins, dict):
        coins = rad["coins"] = {}
    return coins


class RadarTooMany(Exception):
    """The node has more transfers than it returns in one answer (or the block range is too wide): ask for a smaller range."""


class RadarBusy(Exception):
    """This coin has too many transfers to read politely (it would take more node requests than RADAR_MAX_CALLS)."""


def radar_rpc(url: str, method: str, params: list, timeout: float = 30.0):
    """Like rpc_call, but (1) 'too many results' is not counted as the node being down (the range is simply cut in two and asked again) and
    (2) its failures are counted apart from the VIP wallets' (the radar must never make the VIP looks wait)."""
    fk = url + "#radar"
    n, t = _rpc_fail.get(fk, (0, 0.0))
    if n >= 3 and time.time() - t < 120:
        raise RuntimeError("node link is busy - skipped for a moment")
    try:
        r = http.post(url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=timeout)
        r.raise_for_status()
        d = r.json()
    except (requests.RequestException, ValueError):
        _rpc_fail[fk] = (n + 1, time.time())
        raise
    err = d.get("error") if isinstance(d, dict) else None
    if err:
        msg = str(err.get("message") if isinstance(err, dict) else err)
        low_msg = msg.lower()
        if any(x in low_msg for x in ("exceeds limit", "too many results", "result size", "block range", "range too large", "range is too large",
                                      "maximum block range", "query returned more than")):
            _rpc_fail.pop(fk, None)                                # (the node answered; the question was just too big)
            raise RadarTooMany(msg[:100])
        _rpc_fail[fk] = (n + 1, time.time())
        raise ValueError("node answered with an error: " + msg[:100])
    if not isinstance(d, dict) or "result" not in d:
        _rpc_fail[fk] = (n + 1, time.time())
        raise ValueError("node answered with something unexpected")
    _rpc_fail.pop(fk, None)
    return d["result"]


def radar_get_logs(url: str, token: str, a: int, b: int, left: list) -> list[tuple]:
    """Every Transfer of one coin in blocks a..b -> [(block, from, to, raw amount)]. A range with too many results is cut in two and read
    again; left[0] = how many node requests this coin may still use (RadarBusy when they run out)."""
    out: list[tuple] = []
    stack = [(a, b)]
    while stack:
        lo, hi = stack.pop()
        if left[0] <= 0:
            raise RadarBusy("more node requests than allowed")
        left[0] -= 1
        try:
            logs = radar_rpc(url, "eth_getLogs", [{"fromBlock": hex(lo), "toBlock": hex(hi), "address": token, "topics": [TRANSFER_TOPIC]}], timeout=45)
        except RadarTooMany:
            if hi - lo < 100:
                raise RadarBusy("too many transfers in a tiny range")
            mid = (lo + hi) // 2
            stack.append((mid + 1, hi))
            stack.append((lo, mid))
            continue
        for lg in logs or []:
            try:
                t = lg.get("topics") or []
                data = lg.get("data") or ""
                if len(t) != 3 or len(data) < 3:
                    continue                                       # (not a plain coin transfer - an NFT, say)
                out.append((int(lg["blockNumber"], 16), "0x" + str(t[1])[-40:].lower(), "0x" + str(t[2])[-40:].lower(), int(data, 16)))
            except (ValueError, TypeError, KeyError, AttributeError):
                continue
        if stack:
            time.sleep(0.25)
    return out


def radar_prefetch_codes(url: str, addrs: list[str]):
    """Which of these addresses are contracts? One batched request per 30 addresses (remembered in the same cache the VIP wallets use);
    anything the batch does not answer is asked for one by one later."""
    cache = _vip_rt["code"]
    need = [a for a in dict.fromkeys(addrs) if a not in cache and a not in FOMO_ROUTERS]
    for i in range(0, len(need), 30):
        chunk = need[i:i + 30]
        try:
            r = http.post(url, json=[{"jsonrpc": "2.0", "id": j, "method": "eth_getCode", "params": [a, "latest"]} for j, a in enumerate(chunk)], timeout=30)
            r.raise_for_status()
            d = r.json()
        except (requests.RequestException, ValueError):
            continue
        if not isinstance(d, list):
            continue
        for item in d:
            if isinstance(item, dict) and isinstance(item.get("id"), int) and 0 <= item["id"] < len(chunk) and isinstance(item.get("result"), str):
                if len(cache) > 5000:
                    cache.clear()
                cache[chunk[item["id"]]] = len(item["result"]) > 2
        time.sleep(0.2)


def radar_analyze(logs: list[tuple], dec: int, px: float, token: str, is_contract, prefetch=None) -> dict:
    """The transfers of one coin -> per-wallet numbers. logs = [(block, from, to, raw amount)]; px = the coin's price in dollars.
    Returns {"rows": [...], "hubs": set, "special": set}. Each row: a (address), buy / sell (coins), nb / ns (how many), xin / xout (coins moved
    between wallets), first / last (block of the first / last buy), buy_usd, sell_usd, held_usd (what it holds more of than a day ago)."""
    scale = 10.0 ** dec
    flows: dict[tuple, list] = {}
    for blk, frm, to, raw in logs:
        if frm == to:
            continue
        f = flows.get((frm, to))
        if f is None:
            flows[(frm, to)] = [raw / scale, 1, blk, blk]
        else:
            f[0] += raw / scale
            f[1] += 1
            f[2], f[3] = min(f[2], blk), max(f[3], blk)
    nb: dict[str, set] = {}
    for frm, to in flows:
        nb.setdefault(frm, set()).add(to)
        nb.setdefault(to, set()).add(frm)
    hubs = set(RADAR_SEED_HUBS) | {token.lower()}
    special: set = set()                                           # busy addresses that are not contracts (a distributor, an exchange)
    cands = sorted(((a, len(s)) for a, s in nb.items() if len(s) >= RADAR_HUB_FANOUT and a not in hubs), key=lambda x: -x[1])[:20]
    if prefetch and cands:
        prefetch([a for a, _ in cands])
    for a, _ in cands:
        (hubs if is_contract(a) else special).add(a)
    W: dict[str, dict] = {}

    def get(a):
        w = W.get(a)
        if w is None:
            w = W[a] = {"a": a, "buy": 0.0, "sell": 0.0, "nb": 0, "ns": 0, "xin": 0.0, "xout": 0.0, "first": 10 ** 12, "last": 0}
        return w
    for (frm, to), (v, n, first, last) in flows.items():
        fh, th = frm in hubs, to in hubs
        if fh and not th:
            if to in special:
                continue
            w = get(to)
            w["buy"] += v
            w["nb"] += n
            w["first"], w["last"] = min(w["first"], first), max(w["last"], last)
        elif th and not fh:
            if frm in special:
                continue
            w = get(frm)
            w["sell"] += v
            w["ns"] += n
        elif not fh and not th:
            if to not in special:
                get(to)["xin"] += v
            if frm not in special:
                get(frm)["xout"] += v
    rows = []
    for w in W.values():
        w["buy_usd"], w["sell_usd"] = w["buy"] * px, w["sell"] * px
        w["held_usd"] = (w["buy"] + w["xin"] - w["sell"] - w["xout"]) * px
        rows.append(w)
    return {"rows": rows, "hubs": hubs, "special": special}


def radar_is_accumulator(r: dict) -> bool:
    return (r["buy_usd"] >= RADAR_MIN_BUY_USD and r["sell_usd"] <= RADAR_MAX_SOLD * r["buy_usd"]
            and r["held_usd"] >= RADAR_MIN_HELD * r["buy_usd"] and r["nb"] <= RADAR_MAX_BUYS)


def radar_summarize(acc: list[dict], latest: int) -> dict:
    """The accumulators of one coin -> the numbers the radar judges: how many, how much, how spread out, how recent, how patient."""
    ranked = sorted(acc, key=lambda r: -(r["buy_usd"] - r["sell_usd"]))
    nets = [r["buy_usd"] - r["sell_usd"] for r in ranked]
    total = sum(nets)
    b6, b1 = 6 * 3600 * RADAR_BLOCKS_PER_SECOND, 3600 * RADAR_BLOCKS_PER_SECOND
    recent = [r for r in acc if latest - r["last"] <= b6]
    return {"acc": len(acc), "usd": total,
            "top1": (nets[0] / total) if nets and total > 0 else 0.0, "top3": (sum(nets[:3]) / total) if nets and total > 0 else 0.0,
            "n6": len(recent), "n1": sum(1 for r in acc if latest - r["last"] <= b1), "net6": sum(r["buy_usd"] - r["sell_usd"] for r in recent),
            "dca": sum(1 for r in acc if r["nb"] >= 3),
            "last_h": min(((latest - r["last"]) / RADAR_BLOCKS_PER_SECOND / 3600 for r in acc), default=0.0),
            "top": [[r["a"], round(r["buy_usd"] - r["sell_usd"]), r["nb"]] for r in ranked[:RADAR_TOP_KEEP]]}


def radar_latest(url: str) -> int:
    t, blk = _radar_rt["latest"]
    if blk and time.time() - t < 20:
        return blk
    blk = int(radar_rpc(url, "eth_blockNumber", [], timeout=15), 16)
    _radar_rt["latest"] = (time.time(), blk)
    return blk


def radar_scan_coin(url: str, token: str, px: float, latest: int, budget: int | None = None) -> dict:
    """Read one coin's transfers for the last RADAR_HOURS and find its accumulators -> radar_summarize() numbers (+ logs, hubs, calls)."""
    token = token.lower()
    budget = budget or RADAR_MAX_CALLS
    dec = evm_token_decimals(url, "robinhood", token)
    left = [budget]
    logs = radar_get_logs(url, token, max(0, latest - int(RADAR_HOURS * 3600 * RADAR_BLOCKS_PER_SECOND)), latest, left)

    def is_c(a):
        try:
            return evm_is_contract(url, a)
        except Exception:
            return True                                            # cannot tell: a busy address is far more likely a contract
    res = radar_analyze(logs, dec, px, token, is_c, lambda addrs: radar_prefetch_codes(url, addrs))
    acc = [r for r in res["rows"] if radar_is_accumulator(r)]
    acc.sort(key=lambda r: -(r["buy_usd"] - r["sell_usd"]))
    radar_prefetch_codes(url, [r["a"] for r in acc[:40]])
    contracts = {r["a"] for r in acc[:40] if is_c(r["a"])}        # a contract that bought a lot is a pool or a vault, not a person
    acc = [r for r in acc if r["a"] not in contracts]
    out = radar_summarize(acc, latest)
    out.update(logs=len(logs), hubs=len(res["hubs"]), calls=budget - left[0])
    return out


def radar_qualifies(e: dict) -> tuple[bool, str]:
    """Does a coin's latest reading look like real accumulation? -> (yes/no, what is missing). e = a state['radar']['coins'] entry."""
    base = e["mc"] if e.get("mc", 0) > 0 else 2 * e.get("liq", 0)      # (market cap not known: twice the liquidity stands in for it)
    if e.get("acc", 0) < RADAR_MIN_ACC:
        return False, f"only {e.get('acc', 0)} accumulators (need {RADAR_MIN_ACC})"
    if e.get("usd", 0) < RADAR_MIN_NET_USD:
        return False, f"they bought only {fmt_usd(e.get('usd', 0))} together (need {fmt_usd(RADAR_MIN_NET_USD)})"
    if base > 0 and e["usd"] / base < RADAR_MIN_SHARE_MC:
        return False, f"that is only {e['usd'] / base:.2%} of the coin's size (need {RADAR_MIN_SHARE_MC:.2%})"
    if e.get("top1", 0) > RADAR_MAX_TOP1:
        return False, f"one wallet is {e['top1']:.0%} of it (that is one big buyer, not many)"
    if e.get("n6", 0) < RADAR_MIN_RECENT:
        return False, f"only {e.get('n6', 0)} of them bought in the last 6 hours (need {RADAR_MIN_RECENT}) - it is cooling off"
    if e.get("ch", 0) > RADAR_MAX_RUN:
        return False, f"the price is already up {e['ch']:.0f}% in 24h - this alert is for buying BEFORE a run"
    if e.get("ch", 0) < RADAR_MIN_DROP:
        return False, f"the price fell {abs(e['ch']):.0f}% in 24h - that is a falling knife, not accumulation"
    return True, ""


def radar_is_small_steady(e: dict) -> bool:
    """Too few wallets for an alert, but a steady, patient build-up that is big for the coin's size (listed in /radar, never alerted)."""
    base = e["mc"] if e.get("mc", 0) > 0 else 2 * e.get("liq", 0)
    return (e.get("acc", 0) >= RADAR_SMALL_MIN_ACC and base > 0 and e.get("usd", 0) / base >= RADAR_SMALL_MIN_SHARE
            and e.get("top1", 1.0) <= RADAR_MAX_TOP1 and e.get("n6", 0) >= RADAR_SMALL_MIN_RECENT
            and RADAR_MIN_DROP <= e.get("ch", 0) <= RADAR_MAX_RUN)


# --- which coins are read ---
def radar_refresh_candidates(now: float) -> int:
    """Rebuild the list of coins to read: Robinhood Chain's busiest pools by 24h volume (GeckoTerminal, the same free feed the volume-leader
    search uses), one entry per coin. Launch-day coins, thin ones, tiny and huge ones, quote coins and steady assets are left out."""
    net = GECKO_NETWORK["robinhood"]
    by_tok: dict[str, dict] = {}
    for page in range(1, RADAR_LIST_PAGES + 1):
        try:
            r = gecko_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools", tries=2,
                          params={"page": page, "sort": "h24_volume_usd_desc"}, timeout=20)
            pools = r.json().get("data") or []
        except (requests.RequestException, ValueError) as e:
            bot_stats["gecko_busy"] += 1
            log.warning("radar: the pool list is busy (%s)", getattr(getattr(e, "response", None), "status_code", "error"))
            break
        if not pools:
            break
        for pool in pools:
            try:
                attrs = pool.get("attributes") or {}
                base_id = ((pool.get("relationships") or {}).get("base_token") or {}).get("data", {}).get("id", "")
                if "_" not in base_id:
                    continue
                addr = norm_addr("robinhood", base_id.split("_", 1)[1])
                sym = str(attrs.get("name") or "").split(" / ")[0].strip()[:14] or "?"
                liq = float(attrs.get("reserve_in_usd") or 0)
                mc = float(attrs.get("market_cap_usd") or attrs.get("fdv_usd") or 0)
                vol = float((attrs.get("volume_usd") or {}).get("h24") or 0)
                ch = float((attrs.get("price_change_percentage") or {}).get("h24") or 0)
                px = float(attrs.get("base_token_price_usd") or 0)
                age_h = (now - datetime.fromisoformat(str(attrs.get("pool_created_at", "")).replace("Z", "+00:00")).timestamp()) / 3600
                pool_addr = str(attrs.get("address") or "")
            except (ValueError, TypeError, AttributeError):
                continue
            c = by_tok.get(addr)
            if c is None:
                by_tok[addr] = {"addr": addr, "key": sk("robinhood", addr), "sym": sym, "mc": mc, "liq": liq, "vol": vol, "ch": ch, "px": px,
                                "age_h": age_h, "pool": pool_addr, "_main": liq}
            else:
                c["liq"] += liq
                c["vol"] += vol
                c["age_h"] = max(c["age_h"], age_h)
                if liq > c["_main"]:                                # the deepest pool gives the price, market cap and change
                    c.update(mc=mc, ch=ch, px=px, pool=pool_addr, _main=liq)
    out = []
    for addr, c in by_tok.items():
        if (addr in RH_QUOTES or c["sym"].upper() in QUOTE_SYMBOLS or is_quote_token("robinhood", addr) or c["key"] in state["steady"]
                or c["px"] <= 0 or c["liq"] < RADAR_MIN_LIQ_USD or c["age_h"] < RADAR_MIN_AGE_H
                or (c["mc"] > 0 and not RADAR_MIN_MC <= c["mc"] <= RADAR_MAX_MC)):
            continue
        out.append(c)
    out.sort(key=lambda c: -c["liq"])
    if by_tok:                                                       # (a failed look-up keeps the old list)
        _radar_rt["cands"] = out[:RADAR_MAX_COINS]
        _radar_rt["cand_t"] = now
        state["radar"]["last_list"] = now
    return len(out)


def radar_due(cands: list[dict], now: float) -> list[dict]:
    """The coins to read next: never read first, then the longest unread (a coin that qualifies is read again sooner)."""
    coins = radar_coins()
    scored = []
    for c in cands:
        e = coins.get(c["key"]) or {}
        if now - e.get("skip", 0) < 6 * 3600:
            continue
        every = (25 if e.get("since") else RADAR_READ_EVERY_MIN) * 60
        gap = now - e.get("seen", 0)
        if gap >= every:
            scored.append((gap / every, c))
    scored.sort(key=lambda x: -x[0])
    return [c for _, c in scored]


def radar_prune(now: float):
    coins = radar_coins()
    for k in [k for k, e in coins.items() if now - e.get("seen", 0) > RADAR_KEEP_HOURS * 3 * 3600]:
        del coins[k]


# --- reading a coin, judging it, telling you ---
def radar_read_coin(url: str, c: dict, latest: int, now: float) -> bool:
    """Read one listed coin, store what was found, and send the alert when it has qualified on two readings in a row."""
    coins = radar_coins()
    e = coins.get(c["key"])
    if not isinstance(e, dict):
        e = coins[c["key"]] = {"first": now}
    e.update(sym=c["sym"], mc=round(c["mc"]), liq=round(c["liq"]), vol=round(c["vol"]), ch=round(c["ch"], 1), px=c["px"], age=round(c["age_h"]))
    try:
        s = radar_scan_coin(url, c["addr"], c["px"], latest)
    except RadarBusy as ex:
        e.update(seen=now, skip=now, why="too many transfers to read politely", acc=0, usd=0, since=0, ok2=0)
        log.info("radar: %s skipped (%s)", c["sym"], ex)
        return False
    e.update(seen=now, skip=0, why="", acc=s["acc"], usd=round(s["usd"]), top1=round(s["top1"], 3), top3=round(s["top3"], 3), n6=s["n6"], n1=s["n1"],
             net6=round(s["net6"]), dca=s["dca"], last_h=round(s["last_h"], 2), top=s["top"])
    ok, why = radar_qualifies(e)
    if ok:
        if not e.get("since"):
            e.update(since=now, ok2=1, okt=now)
        elif now - e.get("okt", 0) >= 1200:                          # (a second reading must be at least 20 minutes after the first)
            e.update(ok2=e.get("ok2", 0) + 1, okt=now)
    else:
        e.update(since=0, ok2=0, why=why[:80])
    if ok and e.get("ok2", 0) >= 2:
        return radar_alert(c, e, now)
    return False


def radar_baskets() -> Counter:
    """Wallets that are among the biggest accumulators of many coins at once: {wallet: number of coins}."""
    cnt: Counter = Counter()
    for e in radar_coins().values():
        for row in e.get("top") or []:
            cnt[row[0]] += 1
    return cnt


def radar_known(e: dict) -> list[str]:
    """Wallets the bot already knows among a coin's biggest accumulators: your watched / VIP wallets and wallets with a good record."""
    out, scores = [], None
    for addr, usd, _nb in e.get("top") or []:
        if scores is None:
            scores = wallet_scores()
        st = wallet_stars(addr, scores)
        rec = stars_text(st) if st is not None else ""          # v3.7: stars instead of the old "N of M good"
        info = state["watch"].get(addr)
        if isinstance(info, dict):
            out.append(f"{md_escape(wallet_label(addr))} ({'VIP' if info.get('vip') else 'watched'}"
                       + (f", {rec}" if rec else "") + f", {fmt_usd(usd)})")
            continue
        g = record_grade(addr, scores)
        if g in ("proven", "promising"):
            out.append(f"`{short(addr)}` ({rec or GRADE_ICON[g] + ' ' + g}, {fmt_usd(usd)})")
    return out


def radar_price_line(ch: float) -> str:
    if ch <= -5:
        return f"📉 price {ch:+.0f}% in 24h - they are buying a dip"
    if ch < 5:
        return f"➖ price {ch:+.0f}% in 24h - quiet buying, nobody chasing it yet"
    return f"📈 price {ch:+.0f}% in 24h - already moving a bit"


def build_radar_message(key: str, pair: dict, e: dict, mc: float, now: float) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(str(base.get("symbol", "?")))
    liq, vol = num(pair, "liquidity", "usd"), num(pair, "volume", "h24")
    ref = mc if mc > 0 else 2 * liq
    share = (e["usd"] / ref) if ref > 0 else 0
    L = [f"📡 *ACCUMULATION: ${sym}*  |  {CHAINS['robinhood']['name']}", "",
         f"🧺 *{e['acc']} wallets* bought it in the last {RADAR_HOURS:g}h and kept it - together about *{fmt_usd(e['usd'])}*"
         + (f" ({share:.2%} of its market cap)" if mc > 0 else f" ({share:.1%} of twice its liquidity - market cap not known)") + ".",
         f"✋ None of them sold more than {RADAR_MAX_SOLD:.0%} of what they bought. {e['dca']} bought in 3+ steps (patient money); "
         f"the biggest is {e['top1']:.0%} of the total, the top 3 together {e['top3']:.0%}.",
         f"🔥 Still going: {e['n6']} of them bought again in the last 6 hours ({fmt_usd(e.get('net6', 0))})"
         + (f", the latest {e['last_h'] * 60:.0f} min ago" if e.get("last_h", 0) < 3 else "") + ".",
         radar_price_line(e.get("ch", 0.0))]
    known = radar_known(e)
    if known:
        L.append("👀 Wallets I know in it: " + ", ".join(known[:4]))
    bask = radar_baskets()
    nb = sum(1 for row in e.get("top") or [] if bask.get(row[0], 0) >= RADAR_BASKET_COINS)
    if nb:
        L.append(f"🧺 {nb} of its {len(e.get('top') or [])} biggest accumulators are 'basket' buyers (they accumulate {RADAR_BASKET_COINS}+ coins at once) - "
                 "more like a fund than a bet on this coin.")
    L.append("")
    L.append(f"💰 Market cap *{fmt_usd(mc)}*" if mc > 0 else "💰 Market cap: not known")
    L.append(f"💧 liquidity {fmt_usd(liq)} · 📊 24h volume {fmt_usd(vol)}")
    body = "\n".join(L) + "\n" + chain_note("robinhood")
    body += (f"📌 I follow this for {follow_text(RADAR_TRACK_HOURS, mc)} (win = +{(RADAR_WIN_X - 1) * 100:.0f}% before -{(1 - RADAR_STOP_X) * 100:.0f}%). "
             "Accumulation is a clue, not a promise: the buyers can be one person with many wallets, and a coin can still fall.\n"
             f"\n`{addr}`\n\n[DexScreener](https://dexscreener.com/robinhood/{pair.get('pairAddress', '')}) | [Token]({link('robinhood', 'token', addr)})")
    return body


def radar_alert(c: dict, e: dict, now: float) -> bool:
    """A coin that has qualified on two readings in a row: one alert (once per RADAR_COOLDOWN_DAYS, RADAR_MAX_PER_DAY a day)."""
    key = c["key"]
    cd = state["cooldowns"]
    quiet = not alert_on("radar")
    stamps = day_count("rd_ts", now)
    if now - cd.get("rd:" + key, 0) < RADAR_COOLDOWN_DAYS * 86400 or key in state["steady"]:
        return False
    if not quiet and len(stamps) >= RADAR_MAX_PER_DAY:
        return False
    try:
        want = norm_addr("robinhood", c["addr"])
        pairs = [p for p in fetch_pairs("robinhood", [c["addr"]]) if norm_addr("robinhood", p.get("baseToken", {}).get("address", "")) == want]
    except requests.RequestException:
        return False
    pair = best_live_pair(pairs)
    if not pair or not clear_to_alert(key, "robinhood", pair):         # (a steady asset - or not checked yet: a later reading tries again)
        return False
    mc = pair_mc(key, pair) or num(pair, "fdv")
    note_mc(key, mc, num(pair, "liquidity", "usd"))
    m = compute_metrics(key, pair)
    if not quiet and not send_telegram(build_radar_message(key, pair, e, mc, now)):
        return False
    cd["rd:" + key] = now
    if not quiet:
        stamps.append(now)
        bot_stats["radar"] += 1
    if m:
        add_record("radar", key, "robinhood", pair, m, "", now,
                   extra={"th": RADAR_TRACK_HOURS, "wx": RADAR_WIN_X, "sx": RADAR_STOP_X, "n": e["acc"], "usd": round(e["usd"]),
                          "sig": signals_for(m), "quiet": quiet})
    log.info("ACCUMULATION radar: %s - %d wallets, %s%s", key, e["acc"], fmt_usd(e["usd"]), " (quiet)" if quiet else "")
    save_state()
    return True


def radar_tick(force: bool = False):
    """One round of the radar: rebuild the coin list when it is old, then read the few coins that are due."""
    if not RADAR_ON or not EVM_RPC.get("robinhood"):
        return
    now = time.time()
    _radar_rt["ticks"] += 1
    if force or _radar_rt.pop("force", False) or not _radar_rt["cands"] or now - _radar_rt["cand_t"] >= RADAR_LIST_HOURS * 3600:
        try:
            n = radar_refresh_candidates(now)
            log.info("radar: %d coins on the list to read", n)
        except Exception:
            log.exception("radar: the coin list could not be built")
            if not _radar_rt["cands"]:
                _radar_rt["cand_t"] = now - RADAR_LIST_HOURS * 3600 + 600          # (try again in 10 minutes)
    due = radar_due(_radar_rt["cands"], now)[:RADAR_COINS_PER_ROUND]
    if not due:
        return
    url = EVM_RPC["robinhood"]
    bad, bad_t = _rpc_fail.get(url, (0, 0.0))
    if bad >= 2 and time.time() - bad_t < 120:                       # the VIP looks are having trouble with this node: do not pile on
        _radar_rt["last_err"] = "the node is busy with the VIP wallets - the radar waits"
        return
    try:
        latest = radar_latest(url)
    except Exception as ex:
        _radar_rt["last_err"] = scrub(ex)[:80]
        return
    for c in due:
        try:
            radar_read_coin(url, c, latest, time.time())
        except RuntimeError as ex:                                   # the node link is resting: leave it alone for now
            _radar_rt["last_err"] = scrub(ex)[:80]
            break
        except Exception as ex:
            _radar_rt["last_err"] = scrub(ex)[:80]
            log.warning("radar: could not read %s: %s", c.get("sym"), scrub(ex))
            e = radar_coins().setdefault(c["key"], {"first": now})
            e.update(sym=c["sym"], seen=time.time(), why="could not be read: " + scrub(ex)[:40])
        time.sleep(0.5)
    state["radar"]["last_round"] = time.time()
    radar_prune(now)


async def radar_loop():
    """Every couple of minutes: read a few coins for accumulation (nothing happens until the other loops have started)."""
    await asyncio.sleep(RADAR_LOOP_START_DELAY)
    while True:
        try:
            await asyncio.to_thread(radar_tick)
        except Exception:
            log.exception("radar loop error")
        await asyncio.sleep(RADAR_ROUND_SECONDS)


# --- the /radar command ---
def radar_fresh_coins() -> list[tuple[str, dict]]:
    now = time.time()
    return [(k, e) for k, e in radar_coins().items()
            if isinstance(e, dict) and e.get("seen") and now - e["seen"] <= RADAR_KEEP_HOURS * 3600 and e.get("acc", 0) > 0]


def radar_line(i: int, key: str, e: dict, now: float) -> str:
    mc = e.get("mc", 0)
    ref = mc if mc > 0 else 2 * e.get("liq", 0)
    sh = f" ({e['usd'] / ref:.2%} of MC)" if mc > 0 and ref > 0 else ""
    return (f"{i}. *${md_escape(str(e.get('sym', '?')))}*" + (f" · MC {fmt_usd(mc)}" if mc > 0 else f" · liq {fmt_usd(e.get('liq', 0))}")
            + f" - {e['acc']} wallets, {fmt_usd(e['usd'])}{sh}, {e['n6']} still buying (6h), top wallet {e['top1']:.0%}, price {e.get('ch', 0):+.0f}%"
            + f"\n   `{key.split(':', 1)[1]}`")


def radar_list_text() -> str:
    now = time.time()
    rows = radar_fresh_coins()
    rad = state["radar"]
    if not RADAR_ON:
        return "📡 The accumulation radar is switched off (RADAR=false on the server)."
    if not rows:
        err = str(_radar_rt.get("last_err") or "")
        return ("📡 *Accumulation radar* - no readings yet. It reads a few Robinhood Chain coins every couple of minutes, so the first full list is "
                f"ready about {int(RADAR_MAX_COINS / RADAR_COINS_PER_ROUND * RADAR_ROUND_SECONDS / 60) + 3} minutes after start-up.\n"
                + (f"⚠️ Last problem: {md_escape(err[:90])}\n" if err else "")
                + "Send /radar <coin address> to read one coin right now.")
    good, bigone, small, other = [], [], [], 0
    for k, e in rows:
        ok, why = radar_qualifies(e)
        if ok:
            good.append((k, e))
        elif e.get("top1", 0) > RADAR_MAX_TOP1 and e.get("acc", 0) >= 15 and e.get("usd", 0) >= RADAR_MIN_NET_USD:
            bigone.append((k, e))
        elif radar_is_small_steady(e):
            small.append((k, e))
        else:
            other += 1
    good.sort(key=lambda x: -x[1]["usd"])
    bigone.sort(key=lambda x: -x[1]["usd"])
    small.sort(key=lambda x: -(x[1]["usd"] / max(1.0, x[1]["mc"] if x[1].get("mc", 0) > 0 else 2 * x[1].get("liq", 0))))
    L = [f"📡 *Accumulation radar* - Robinhood Chain coins that many wallets bought and KEPT in the last {RADAR_HOURS:g}h "
         f"(read {len(rows)} coins; the oldest reading is {ago(now - min(e['seen'] for _, e in rows))} old)."
         + ("" if alert_on("radar") else "\n⚠️ The radar alert is switched OFF (/alerts radar on). It still reads and follows quietly.")]
    if good:
        L.append("\n*Accumulating now* (" + f"{RADAR_MIN_ACC}+ wallets, not one big buyer, still buying, price not running):")
        for i, (k, e) in enumerate(good[:8], 1):
            L.append(radar_line(i, k, e, now))
    else:
        L.append("\nNothing meets all the rules right now.")
    if small:
        L.append(f"\n*Smaller but steady* (not alerted: fewer than {RADAR_MIN_ACC} wallets, but a patient build-up that is big for the coin):")
        for i, (k, e) in enumerate(small[:4], 1):
            L.append(radar_line(i, k, e, now))
    if bigone:
        L.append("\n*One big wallet is most of it* (not alerted):")
        for i, (k, e) in enumerate(bigone[:4], 1):
            L.append(radar_line(i, k, e, now))
    L.append(f"\n{other} other coin{'' if other == 1 else 's'} read: too few accumulators, cooling off, running, or falling.")
    L.append("Launch-day coins (younger than " + f"{RADAR_MIN_AGE_H:g}h" + "), thin pools and stock / wrapped tokens are not read. "
             "/radar <coin address> reads one coin now · /alerts radar off")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def radar_check_one(ref: str, notify=None) -> str:
    """/radar <coin address>: read that one coin now (takes about 20 seconds)."""
    chain, addr, err = resolve_coin_ref(ref)
    if err:
        return err.replace("/check", "/radar")
    if chain != "robinhood":
        return "The accumulation radar reads Robinhood Chain coins only (that address is on " + CHAINS[chain]["name"] + ")."
    url = EVM_RPC.get("robinhood")
    if not url:
        return "I have no node link for Robinhood Chain, so I can't read its transfers."
    want = norm_addr(chain, addr)
    try:
        pairs = [p for p in fetch_pairs(chain, [addr]) if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == want]
    except requests.RequestException:
        return "DexScreener is busy right now - try again in a minute."
    pair = best_live_pair(pairs)
    px = num(pair, "priceUsd") if pair else 0.0
    if not pair or px <= 0:
        return "I couldn't find a live trading pool with a price for that address on Robinhood Chain."
    if notify:
        notify("🔎 Reading that coin's transfers from the chain now - about 20 seconds.")
    sym = md_escape(str(pair.get("baseToken", {}).get("symbol", "?")))
    try:
        latest = radar_latest(url)
        s = radar_scan_coin(url, addr, px, latest, budget=RADAR_MAX_CALLS * 2)
    except RadarBusy:
        return f"${sym} has too many transfers for me to read politely with the free node. I skipped it."
    except (RuntimeError, ValueError, requests.RequestException) as ex:
        return f"I couldn't read ${sym}'s transfers just now ({md_escape(scrub(ex)[:60])}). Try again in a minute."
    key = sk(chain, addr)
    mc = pair_mc(key, pair) or num(pair, "fdv")
    liq = num(pair, "liquidity", "usd")
    e = {"acc": s["acc"], "usd": s["usd"], "top1": s["top1"], "top3": s["top3"], "n6": s["n6"], "n1": s["n1"], "net6": s["net6"], "dca": s["dca"],
         "last_h": s["last_h"], "mc": mc, "liq": liq, "ch": num(pair, "priceChange", "h24"), "top": s["top"]}
    ok, why = radar_qualifies(e)
    created = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3_600_000 if created else 0.0
    L = [f"📡 *${sym}* - accumulation over the last {RADAR_HOURS:g}h (read {s['logs']:,} transfers, {s['hubs']} pools / routers found)",
         f"🧺 {s['acc']} accumulating wallets · {fmt_usd(s['usd'])} net bought" + (f" ({s['usd'] / mc:.2%} of MC)" if mc > 0 else ""),
         f"✋ biggest wallet {s['top1']:.0%} of it, top 3 {s['top3']:.0%} · {s['dca']} bought in 3+ steps",
         f"🔥 {s['n6']} bought in the last 6h ({fmt_usd(s['net6'])}), {s['n1']} in the last hour",
         radar_price_line(e["ch"]),
         f"💰 MC {fmt_usd(mc) if mc > 0 else 'not known'} · 💧 liquidity {fmt_usd(liq)}" + (f" · ⏱ pool {fmt_age(age_h)} old" if age_h > 0 else "")]
    known = radar_known(e)
    if known:
        L.append("👀 Wallets I know in it: " + ", ".join(known[:4]))
    if age_h and age_h < RADAR_MIN_AGE_H:
        L.append(f"⚠️ It is only {fmt_age(age_h)} old: a launch-day coin's buying is mostly pump and wash trading, so I would not call this accumulation.")
    L.append("✅ This meets every rule of an accumulation alert." if ok and not (age_h and age_h < RADAR_MIN_AGE_H) else f"ℹ️ Not an alert: {why or 'too young'}.")
    return "\n".join(L)


def cmd_radar(args: list[str], notify=None) -> str:
    sub = args[0].lower() if args else ""
    if sub in ("on", "off"):
        set_alert("radar", sub == "on")
        return "✅ The radar alert is " + ("ON." if sub == "on" else "OFF. It keeps reading and following in silence.")
    if sub in ("scan", "refresh", "now", "rebuild"):
        _radar_rt["force"] = True
        _radar_rt["cand_t"] = 0.0
        return "🔄 I'll rebuild the list of coins to read in the next minutes."
    refs = extract_refs(" ".join(args)) if args else []
    if refs:
        return radar_check_one(refs[0], notify)
    return radar_list_text()


def radar_status_lines() -> list[str]:
    """For the status check-in."""
    try:
        if not RADAR_ON:
            return []
        rows = radar_fresh_coins()
        good = sorted(((e["usd"], e.get("sym", "?")) for _, e in rows if radar_qualifies(e)[0]), reverse=True)
        sent = len(_last_day(state["tuning"].get("rd_ts"), time.time()))
        if not rows:
            err = str(_radar_rt.get("last_err") or "")
            if err and time.time() - STARTED_AT > 1800:
                return [f"📡 Accumulation radar: no coin read yet (last problem: {md_escape(err[:70])}) - I keep trying (/radar)"]
            return ["📡 Accumulation radar: starting up (it reads a few Robinhood Chain coins every couple of minutes) - /radar"]
        return [f"📡 Accumulation radar: {len(rows)} coins read lately" + (f", accumulating now: {', '.join('$' + md_escape(str(s)) for _, s in good[:4])}" if good else ", none accumulating right now")
                + f" · alerts in the last 24h: {sent} (/radar)"]
    except Exception:
        log.exception("radar status lines failed")
        return []


# --- the loop that runs it all ---
def vip_tick():
    """One pass of the VIP loop: what is due (repeats of loud messages, retries, the looks inside the VIP wallets, the dip-watch, hunters)."""
    now = time.time()
    r = _vip_rt
    try:
        vip_reminders_due(now)
        vip_retry_due(now)
    except Exception:
        log.exception("VIP reminders error")
    if VIP_SWEEPS_ON and vip_items():
        if now - r.get("sol_vip", 0) >= VIP_SWEEP_SECONDS:
            r["sol_vip"] = now
            try:
                vip_sweep_solana("vip")
            except Exception:
                log.exception("VIP sweep (Solana) error")
        if now - r.get("sol_hunt", 0) >= HUNTER_SWEEP_SECONDS:
            r["sol_hunt"] = now
            try:
                vip_sweep_solana("hunter")
            except Exception:
                log.exception("hunter sweep (Solana) error")
        if now - r.get("evm", 0) >= VIP_SWEEP_SECONDS:
            r["evm"] = now
            try:
                vip_sweep_evm()
            except Exception:
                log.exception("VIP sweep (Robinhood Chain) error")
    if state["kol"] and (r.pop("kol_now", False) or now - r.get("kol", 0) >= KOL_SECONDS):
        r["kol"] = now
        try:
            kol_pass()
        except Exception:
            log.exception("dip-watch error")
    if now - r.get("hunt", 0) >= 1800:
        r["hunt"] = now
        try:
            hunters_pass()
        except Exception:
            log.exception("hunter search error")


async def vip_loop():
    """Every few seconds: do whatever is due for the VIP wallets and the dip-watch (nothing happens until the first minute is over)."""
    await asyncio.sleep(VIP_LOOP_START_DELAY)
    while True:
        try:
            await asyncio.to_thread(vip_tick)
        except Exception:
            log.exception("VIP loop error")
        await asyncio.sleep(5)


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
# CHART SHAPES (pure maths, no network): candles in, a reading of the chart out.
# ----------------------------------------------------------------------------
CHART_MAX_CANDLES = 400


def chart_clean(raw, step_s: int, max_n: int = CHART_MAX_CANDLES) -> list:
    """GeckoTerminal's candle list (newest first) -> [(start, open, high, low, close, volume)], oldest first, one per
    time slot. Broken rows are dropped, one-trade wicks are trimmed, and slots with no trades are filled flat."""
    rows = {}
    for c in raw if isinstance(raw, list) else []:
        try:
            ts = int(c[0])
            o, h, l, cl, v = (float(x) for x in c[1:6])
        except (TypeError, ValueError, IndexError):
            continue
        if ts <= 0 or not all(math.isfinite(x) for x in (o, h, l, cl, v)) or min(o, h, l, cl) <= 0 or v < 0:
            continue
        body_lo, body_hi = min(o, cl), max(o, cl)
        h = min(max(h, body_hi), body_hi * 2.5)
        l = max(min(l, body_lo), body_lo / 2.5)
        rows[ts - ts % step_s] = (o, h, l, cl, v)
    if len(rows) < 2:
        return []
    last = max(rows)
    first = max(min(rows), last - (max_n - 1) * step_s)
    out, prev, t = [], None, first
    while t <= last:
        r = rows.get(t)
        if r is None:
            if prev is None:
                t += step_s
                continue
            r = (prev, prev, prev, prev, 0.0)
        prev = r[3]
        out.append((t,) + r)
        t += step_s
    return out


def _med(xs):
    xs = sorted(xs)
    if not xs:
        return 0.0
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def _clamp(x, lo=0.0, hi=1.0):
    return lo if x < lo else hi if x > hi else x


def _parabola(xs, ys):
    """Least-squares y = a*x^2 + b*x + c  ->  (a, b, c, r2)."""
    n = len(xs)
    s = [sum(x ** p for x in xs) for p in range(5)]
    t = [sum((x ** p) * y for x, y in zip(xs, ys)) for p in range(3)]
    m = [[s[4], s[3], s[2], t[2]], [s[3], s[2], s[1], t[1]], [s[2], s[1], float(n), t[0]]]
    for i in range(3):                                   # Gauss elimination with partial pivoting
        p = max(range(i, 3), key=lambda r: abs(m[r][i]))
        if abs(m[p][i]) < 1e-12:
            return 0.0, 0.0, sum(ys) / n, 0.0
        m[i], m[p] = m[p], m[i]
        for r in range(i + 1, 3):
            f = m[r][i] / m[i][i]
            for c in range(i, 4):
                m[r][c] -= f * m[i][c]
    sol = [0.0] * 3
    for i in (2, 1, 0):
        sol[i] = (m[i][3] - sum(m[i][c] * sol[c] for c in range(i + 1, 3))) / m[i][i]
    a, b, c = sol
    mean = sum(ys) / n
    sst = sum((y - mean) ** 2 for y in ys)
    sse = sum((y - (a * x * x + b * x + c)) ** 2 for x, y in zip(xs, ys))
    return a, b, c, (1 - sse / sst) if sst > 1e-12 else 0.0


def _fmt_days(h):
    return f"{h:.0f}h" if h < 48 else f"{h / 24:.1f}d"


# --- the three bottom signs (the same three Sparsity reads, from the price chart) ---
def own_signs(O, H, L, C, V, step_h, dd_known=None):
    """-> (signs 0..3, details). Signs count in order: 1 sellers capitulated (a deep fall), 2 the low is holding
    (time has passed and price stays above it), 3 demand returning (a higher low and more volume on up-candles)."""
    n = len(C)
    d = {"dd": 0.0, "since_h": 0.0, "above_low": 0.0, "hi": C[-1], "lo": C[-1], "notes": [], "retests": 0, "updown": 0.0,
         "higher_low": False}
    if n < 12:
        return 0, d
    ih = max(range(n), key=lambda i: C[i])
    hi = C[ih]
    if ih >= n - 2:
        d["notes"].append("at its highs: no fall to read")
        return 0, d
    il = min(range(ih + 1, n), key=lambda i: C[i])
    lo = C[il]
    px = C[-1]
    dd = 1 - lo / hi
    since_h = (n - 1 - il) * step_h
    d.update(dd=dd, hi=hi, lo=lo, since_h=since_h, above_low=px / lo - 1)
    s1 = dd >= 0.5 or (dd_known is not None and dd_known >= 0.6 and dd >= 0.3)
    if not s1:
        d["notes"].append(f"fell {dd:.0%} from its high (needs 50%)")
        return 0, d
    d["notes"].append(f"fell {dd:.0%} from its high")
    recent = max(6, int(round(48 / step_h)))
    lastk = min(C[-max(3, int(round(24 / step_h))):])
    s2 = since_h >= 24 and px >= lo * 1.04 and lastk >= lo * 1.02
    if not s2:
        d["notes"].append("the low is not proven yet" if px >= lo * 1.04 else "still at its low")
        return 1, d
    k = max(2, int(round(12 / step_h)))                   # retests: later dips back to within 12% of the low
    tests, last_t = 0, -99
    for i in range(il + 3, n - 1):
        if L[i] <= lo * 1.12 and L[i] == min(L[max(0, i - k): i + k + 1]) and i - last_t > 2 * k:
            tests += 1
            last_t = i
    d["retests"] = tests
    d["notes"].append(f"low held for {_fmt_days(since_h)}" + (f", retested {tests}x" if tests else ""))
    early = min(L[: n - recent]) if n > recent + 3 else lo
    higher_low = min(L[-recent:]) > early * 1.02
    up = sum(v for o, c, v in zip(O[-recent:], C[-recent:], V[-recent:]) if c >= o)
    down = sum(v for o, c, v in zip(O[-recent:], C[-recent:], V[-recent:]) if c < o)
    ratio = up / down if down > 0 else (9.9 if up > 0 else 0.0)
    d.update(higher_low=higher_low, updown=ratio)
    if higher_low and ratio >= 1.1:
        d["notes"].append(f"higher low and {ratio:.1f}x more volume on up-candles")
        return 3, d
    d["notes"].append("no higher low yet" if not higher_low else f"up-volume only {ratio:.1f}x down-volume")
    return 2, d


# --- cup (and handle) ---
def _smooth(C, k=3):
    out = []
    for i in range(len(C)):
        a, b = max(0, i - k // 2), min(len(C), i + k // 2 + 1)
        out.append(sum(C[a:b]) / (b - a))
    return out


def find_cup(O, H, L, C, V, step_h):
    n = len(C)
    kd = 24.0 / step_h
    if n < max(24, int(3 * kd)):
        return None
    S = _smooth(C)
    k = max(2, int(round(kd / 2)))
    peaks = [i for i in range(1, n - 3) if C[i] >= max(C[max(0, i - k): i + k + 1])]
    peaks = sorted(set(peaks), key=lambda i: -C[i])[:6]
    px = C[-1]
    best = None
    for iL in peaks:
        rim = C[iL]
        seg = C[iL + 1:]
        if len(seg) < 4:
            continue
        bot = min(seg)
        iB = iL + 1 + seg.index(bot)
        depth = 1 - bot / rim
        nb, nr, span = iB - iL, n - 1 - iB, n - 1 - iL
        if not (0.22 <= depth <= 0.88) or nb < kd or nr < max(3, 0.5 * kd) or span < 2 * kd or span > 300:
            continue
        rise = rim - bot
        rec = (px - bot) / rise
        if rec < 0.30:
            continue
        band = bot + 0.25 * rise
        near_frac = sum(1 for c in C[iL:] if c <= band) / (span + 1)
        w = max(2, int(round(0.12 * span)))
        around = S[max(iL, iB - w): min(n - 1, iB + w) + 1]
        flat = (max(around) - min(around)) / rise                 # a U is flat around its bottom, a V is not
        run, bounce = S[iL + 1], 0.0
        for j in range(iL + 1, iB + 1):                           # rebounds on the way down (a W has a big one)
            run = min(run, S[j])
            bounce = max(bounce, (S[j] - run) / rise)
        xs = [(i - iL) / span for i in range(iL, n)]
        ys = [(C[i] - bot) / rise for i in range(iL, n)]
        a, b, _, r2 = _parabola(xs, ys)
        xv = -b / (2 * a) if a > 1e-9 else None
        if flat > 0.26 or a < 0.5 or xv is None or not (0.2 <= xv <= 0.92) or r2 < 0.4 or bounce > 0.55:
            continue
        wcl = max(2, int(0.2 * nb))                                # how much of the fall came in its steepest fifth
        cliff = max(((C[j] - C[min(iB, j + wcl)]) / rise for j in range(iL, max(iL + 1, iB - wcl + 1))), default=0.0)
        v_all = _med(V[iL:])
        v_bot = _med([v for c, v in zip(C[iL:], V[iL:]) if c <= band])
        dry = (v_bot / v_all) if v_all > 0 else 1.0
        # the handle: after the price got back near the rim, a shallow pull-back that stays in the upper half
        handle, stage = None, "right side climbing"
        if rec >= 0.75:
            seg2 = C[iB + 1:]
            R = max(seg2)
            iR = iB + 1 + seg2.index(R)
            after = C[iR + 1:]
            if len(after) >= 2:
                hmin = min(after)
                pull = (R - hmin) / (R - bot) if R > bot else 1.0
                hlen = n - 1 - iR
                if 0.03 <= (R - hmin) / R and pull <= 0.38 and hmin >= bot + 0.5 * rise and hlen <= max(span * 0.5, 8):
                    handle = {"high": R, "low": hmin, "len": hlen, "pull": pull}
            if px > rim * 1.03:
                stage = "breaking out above the rim"
            elif handle:
                stage = "handle forming" if px < handle["high"] * 0.99 else "pressing the right rim"
            else:
                stage = "back at the rim" if px >= rim * 0.92 else "right side climbing"
        score = (30 * _clamp((0.27 - flat) / 0.12)
                 + 10 * _clamp((near_frac - 0.20) / 0.12)
                 + 10 * _clamp((r2 - 0.45) / 0.35)
                 + 10 * (1 - _clamp((bounce - 0.10) / 0.35))
                 + 10 * _clamp(1 - (0.30 - depth) / 0.08 if depth < 0.30 else 1 - (depth - 0.65) / 0.23 if depth > 0.65 else 1.0)
                 + (15 * _clamp(0.2 + (rec - 0.30) / 0.15 * 0.8) if rec < 0.45 else 15 if rec <= 0.95 else 8)
                 + 5 * (1 - _clamp(abs(xv - 0.55) / 0.35))
                 + 5 * (1 - _clamp((dry - 0.8) / 0.4))
                 + (5 if handle else 0))
        if flat > 0.22:                                            # a pointed bottom is a V, not a cup
            score -= 12 * _clamp((flat - 0.22) / 0.04)
        if cliff > 0.55:
            score -= 25 * _clamp((cliff - 0.55) / 0.35)
        if best is None or score > best["score"]:
            best = {"pat": "cup_handle" if handle else "cup", "stage": stage, "score": int(round(_clamp(score, 0, 100))),
                    "rim": rim, "bot": bot, "rec": rec, "depth": depth, "near_frac": near_frac, "r2": r2, "xv": xv,
                    "flat": flat, "bounce": bounce,
                    "level": (handle["high"] if handle else rim), "stop": (handle["low"] if handle else bot),
                    "days": span * step_h / 24, "handle": handle, "dry": dry, "iL": iL, "iB": iB}
    return best


# --- flat base at the bottom ---
def find_base(O, H, L, C, V, step_h, dd_known=None, liq=None):
    n = len(C)
    min_len = max(6, int(round(48 / step_h)))
    if n < min_len + 6:
        return None
    best_k = 0
    for k in range(min_len, int(n * 0.7) + 1):
        seg_c, seg_h, seg_l = C[n - k:], H[n - k:], L[n - k:]
        if max(seg_c) / min(seg_c) - 1 > 0.30 or max(seg_h) / min(seg_l) - 1 > 0.55:
            break
        best_k = k
    k = best_k
    if k < min_len:
        return None
    seg_c, seg_h, seg_l, seg_v = C[n - k:], H[n - k:], L[n - k:], V[n - k:]
    base_low, base_top = min(seg_l), max(seg_c)
    pre = max(C[: n - k]) if n - k >= 3 else 0.0
    dd_pre = 1 - min(seg_c) / pre if pre > 0 else 0.0
    if dd_pre < 0.5 and not (dd_known is not None and dd_known >= 0.65):
        return None
    if base_low > 1.12 * min(L):
        return None
    days = k * step_h / 24
    if liq and sum(seg_v) / max(days, 0.1) < 0.02 * liq:
        return None                                   # barely traded: quiet because nobody is there
    xs = list(range(k))
    mx, my = (k - 1) / 2, sum(seg_c) / k
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, seg_c)) / max(1e-12, sum((x - mx) ** 2 for x in xs))
    slope_day = slope * (24 / step_h) / my
    if slope_day < -0.03:
        return None                                   # still sliding
    rng_c = max(seg_c) / min(seg_c) - 1
    # quiet compared with the fall before it
    def swing(cs):
        return _med([abs(cs[i] / cs[i - 1] - 1) for i in range(1, len(cs))]) if len(cs) > 3 else 0.0
    before = C[max(0, n - k - max(k, 12)): n - k]
    calm = swing(seg_c) / swing(before) if swing(before) > 0 else 1.0
    prev_v = V[max(0, n - 2 * k): n - k]
    dry = (_med(seg_v) / _med(prev_v)) if prev_v and _med(prev_v) > 0 else 1.0
    kk = max(2, int(round(12 / step_h)))
    tests, last_t = 0, -99
    for i in range(n - k, n - 1):
        if L[i] <= base_low * 1.06 and L[i] == min(L[max(0, i - kk): i + kk + 1]) and i - last_t > 2 * kk:
            tests += 1
            last_t = i
    pos = (C[-1] - base_low) / max(1e-12, base_top - base_low)
    score = ((22 if rng_c <= 0.12 else 17 if rng_c <= 0.2 else 10 if rng_c <= 0.27 else 4)
             + 20 * _clamp((days - 1.5) / 2.5)
             + (15 if 0.55 <= dd_pre <= 0.93 else 8)
             + (15 if calm <= 0.5 else 9 if calm <= 0.75 else 3 if calm <= 1.0 else 0)
             + (10 if dry <= 0.6 else 7 if dry <= 0.8 else 3 if dry <= 1.0 else 0)
             + (8 if slope_day >= 0 else 4 if slope_day >= -0.015 else 0)
             + (10 if tests >= 2 else 5 if tests == 1 else 0))
    stage = "pressing the top of the base" if pos >= 0.85 else "base building"
    if C[-1] > base_top * 1.04:
        stage = "breaking out of the base"
    return {"pat": "base", "stage": stage, "score": int(round(_clamp(score, 0, 100))), "level": base_top, "stop": base_low,
            "days": days, "range": rng_c, "dd_pre": dd_pre, "dry": dry, "calm": calm, "tests": tests, "slope_day": slope_day}


# --- double bottom ---
def find_double_bottom(O, H, L, C, V, step_h):
    n = len(C)
    kd = 24.0 / step_h
    if n < max(30, int(5 * kd)):
        return None
    k = max(2, int(round(kd / 2)))
    piv = [i for i in range(k, n - 2) if L[i] == min(L[max(0, i - k): i + k + 1])]
    px = C[-1]
    best = None
    for a in range(len(piv)):
        for b in range(a + 1, len(piv)):
            i1, i2 = piv[a], piv[b]
            gap = i2 - i1
            if gap < 4 * kd or gap > 12 * kd:
                continue
            l1, l2 = L[i1], L[i2]
            if abs(l1 - l2) / min(l1, l2) > 0.05:
                continue
            neck = max(C[i1: i2 + 1])
            if neck / max(l1, l2) - 1 < 0.35:
                continue
            pre = max(C[max(0, i1 - int(12 * kd)): i1 + 1])
            if pre / l1 - 1 < 1.0:
                continue
            if n - 1 - i2 < max(3, 0.5 * kd) or px < l2 * 1.05 or px > neck * 1.25 or min(L[i2:]) < min(l1, l2) * 0.97:
                continue
            v1 = _med(V[max(0, i1 - 2): i1 + 3])
            v2 = _med(V[max(0, i2 - 2): i2 + 3])
            score = (22 * (1 - _clamp(abs(l1 - l2) / min(l1, l2) / 0.05))
                     + 15 * _clamp((neck / max(l1, l2) - 1 - 0.3) / 0.3 + 0.4)
                     + 12 * (1 - _clamp(abs(gap / kd - 6) / 6))
                     + (12 if v1 > 0 and v2 < 0.85 * v1 else 5 if v1 > 0 and v2 < v1 else 0)
                     + 17 * _clamp((px - l2) / max(1e-12, neck - l2))
                     + (12 if pre / l1 - 1 >= 1.0 else 7)
                     + 10 * _clamp((l1 - min(L[max(0, i1 - 3): i1 + 1])) * 0 + 1))
            stage = "breaking out above the neckline" if px > neck * 1.02 else "testing the neckline" if px >= neck * 0.97 else "second low is holding"
            if best is None or score > best["score"]:
                best = {"pat": "double_bottom", "stage": stage, "score": int(round(_clamp(score, 0, 100))), "level": neck,
                        "stop": min(l1, l2), "gap_days": gap / kd, "i1": i1, "i2": i2}
    return best


PATTERN_RANK = {"cup_handle": 4, "cup": 3, "double_bottom": 2, "base": 1}
PATTERN_LABEL = {"cup_handle": "cup & handle", "cup": "cup", "double_bottom": "double bottom", "base": "bottom base"}


def analyze_chart(candles, step_h, dd_known=None, liq=None):
    """Read a coin's candles (from chart_clean). Returns a dict, or None when there is too little to read."""
    n = len(candles)
    if n < 24:
        return None
    O = [c[1] for c in candles]; H = [c[2] for c in candles]; L = [c[3] for c in candles]
    C = [c[4] for c in candles]; V = [c[5] for c in candles]
    recent = candles[-max(12, n // 2):]
    if sum(1 for c in recent if c[5] <= 0) > 0.4 * len(recent):
        return None                                   # mostly empty candles: too thin to read a shape from
    signs, sd = own_signs(O, H, L, C, V, step_h, dd_known)
    found = [x for x in (find_cup(O, H, L, C, V, step_h), find_base(O, H, L, C, V, step_h, dd_known, liq),
                         find_double_bottom(O, H, L, C, V, step_h)) if x]
    db = next((x for x in found if x["pat"] == "double_bottom"), None)
    if db and db["score"] >= 55:
        found = [x for x in found if not (x["pat"] in ("cup", "cup_handle") and db["i1"] < x["iL"] < db["i2"])]
    found.sort(key=lambda x: (x["score"] >= 60, x["score"], PATTERN_RANK.get(x["pat"], 0)), reverse=True)
    top = found[0] if found else None
    out = {"n": n, "step_h": step_h, "t_last": candles[-1][0], "px": C[-1], "signs": signs, "dd": sd["dd"], "since_h": sd["since_h"],
           "above_low": sd["above_low"], "notes": sd["notes"], "retests": sd["retests"], "updown": sd["updown"],
           "pat": top["pat"] if top else "", "stage": top["stage"] if top else "", "score": top["score"] if top else 0,
           "level": top["level"] if top else 0.0, "stop": top["stop"] if top else 0.0, "detail": top or {},
           "others": [(x["pat"], x["score"]) for x in found[1:]]}
    return out


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
        if alert_on("bottom") and len(ready) >= 3:
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


def acc_threshold(mc: float) -> float:
    """How much a wallet must have bought to count as 'accumulating': a small slice of the coin's size, never under
    BUYER_MIN_USD and never more than BUYER_MAX_USD_THRESHOLD (about the $1K buys that matter on a mid-size coin)."""
    return min(max(BUYER_MIN_USD, mc * BUYER_MC_FRACTION), BUYER_MAX_USD_THRESHOLD)


def analyze_trades(chain: str, trades: list, pool: str, token: str, mc: float, since: float = 0.0) -> dict:
    """Group a coin's latest trades by wallet, set bots and routers aside, and pick out the wallets that are
    ACCUMULATING (bought a meaningful amount and sold little). since = only count trades newer than this time
    (used to add up what several looks at the same coin have seen)."""
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
        ts = 0.0
        try:
            ts = datetime.fromisoformat(str(a.get("block_timestamp", "")).replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
        if since and not (ts > since):
            continue
        w = norm_addr(chain, w)
        n += 1
        if ts:
            first, last = (min(first, ts) if first else ts), max(last, ts)
        d = per.setdefault(w, {"w": w, "buy": 0.0, "sell": 0.0, "n": 0, "ns": 0, "pxw": 0.0, "t0": 0.0, "t1": 0.0, "b0": 0.0, "b1": 0.0})
        d["n"] += 1
        if ts:
            d["t0"], d["t1"] = (min(d["t0"], ts) if d["t0"] else ts), max(d["t1"], ts)
        if kind == "buy":
            d["buy"] += usd
            if ts:        # v3.8: when the wallet REALLY bought (its first and its latest buy in this look), not when I looked
                d["b0"], d["b1"] = (min(d["b0"], ts) if d["b0"] else ts), max(d["b1"], ts)
            try:
                d["pxw"] += float(a.get("price_to_in_usd") or 0) * usd
            except (TypeError, ValueError):
                pass
        else:
            d["sell"] += usd
            d["ns"] += 1
        kinds.setdefault((w, str(a.get("tx_hash") or "")), set()).add(kind)
    for (w, _), ks in kinds.items():
        if len(ks) > 1:
            per[w]["arb"] = True          # bought AND sold in one transaction: a router / arbitrage bot
    for d in per.values():
        # a bot trades a lot AND sells a lot; someone who only keeps buying (even in many small pieces) is accumulating
        d["bot"] = bool((d["n"] > BUYER_MAX_TRADES and d["ns"] >= 0.2 * d["n"]) or d["n"] > BUYER_HARD_MAX_TRADES
                        or d.get("arb") or d["w"] in skip or is_bundler(chain, d["w"]))
        d["px"] = d["pxw"] / d["buy"] if d["buy"] > 0 else 0.0
    people = [d for d in per.values() if not d["bot"]]
    thr = acc_threshold(mc)
    acc = sorted((d for d in people if d["buy"] >= thr and d["sell"] <= BUYER_MAX_SELL_SHARE * d["buy"]),
                 key=lambda d: d["buy"] - d["sell"], reverse=True)
    top = sorted((d for d in people if d["buy"] > d["sell"]), key=lambda d: d["buy"] - d["sell"], reverse=True)
    return {"n": n, "span": (last - first) / 3600 if first and last > first else 0.0, "last": last,
            "buy": sum(d["buy"] for d in people), "sell": sum(d["sell"] for d in people),
            "nb": sum(1 for d in people if d["buy"] > d["sell"]), "ns": sum(1 for d in people if d["sell"] > d["buy"]),
            "bots": sum(1 for d in per.values() if d["bot"]), "thr": thr, "acc": acc, "top": top[:10], "per": per}


def fmt_span(hours: float) -> str:
    return "less than an hour" if hours < 1 else f"~{hours:.0f}h" if hours < 48 else f"~{hours / 24:.0f}d"


FLOW_MIN_USD = 100           # a wallet that has bought less than this in total is not remembered
FLOW_MAX_WALLETS = 20        # wallets remembered per coin
CAP_ACC_KEPT = 12            # v3.8.1: accumulating wallets kept per look (it was 6: a wallet with a record that ranked 7th by size was never seen)


def flow_merge(holder: dict, new: dict, full: dict, now: float):
    """Add what the latest look saw to this coin's running totals per wallet. Small buys that repeat ('$100 here, $100
    there') add up over several looks; each trade is only counted once (the look remembers where the last one ended).
    Entry = [bought $, sold $, trades, sells, first trade, last trade, price x amount bought, bot flag, first buy, last buy]
    (v3.8: the last two are the times the wallet REALLY made its first and its latest buy)."""
    flow = holder.setdefault("flow", {})
    for w, d in new["per"].items():
        bot = bool(full["per"].get(w, d).get("bot"))
        f = flow.get(w)
        if bot:
            if f:
                f[7] = 1
            continue
        if f is None and d["buy"] < FLOW_MIN_USD:
            continue
        t0, t1 = d["t0"] or now, d["t1"] or now
        b0, b1 = d.get("b0") or 0.0, d.get("b1") or 0.0
        if f is None:
            flow[w] = [d["buy"], d["sell"], d["n"], d["ns"], t0, t1, d["pxw"], 0, b0, b1]
        else:
            while len(f) < 10:        # a note from before v3.8: its first / latest trade stand in for the buy times until a newer look has them
                f.append(f[4] if len(f) == 8 else f[5])
            f[0] += d["buy"]
            f[1] += d["sell"]
            f[2] += d["n"]
            f[3] += d["ns"]
            f[4] = min(f[4] or t0, t0)
            f[5] = max(f[5], t1)
            f[6] += d["pxw"]
            if b0:
                f[8] = min(f[8] or b0, b0)
            if b1:
                f[9] = max(f[9], b1)
    horizon = now - FLOW_KEEP_DAYS * 86400
    for w in [w for w, f in flow.items() if f[5] < horizon]:
        del flow[w]
    if len(flow) > FLOW_MAX_WALLETS:
        keep = set(sorted(flow, key=lambda w: -(flow[w][0] - flow[w][1]))[:FLOW_MAX_WALLETS])
        for w in [w for w in flow if w not in keep]:
            del flow[w]
    for f in flow.values():
        f[0], f[1], f[4], f[5] = round(f[0]), round(f[1]), round(f[4]), round(f[5])
        f[6] = float(f"{f[6]:.6g}")
        if len(f) > 9:
            f[8], f[9] = round(f[8]), round(f[9])


def flow_accumulators(holder: dict, mc: float, now: float) -> list:
    """Wallets that have bought a meaningful amount of this coin (all their buys add up) and sold little, and bought
    in the last 3 days: [wallet, bought $, sold $, trades, average price paid, time of its latest buy, time of its first buy]
    (v3.8: the last two are real trade times - before, the 'last trade' was any trade, and nothing else knew the first one)."""
    thr = acc_threshold(mc)
    rows = []
    for w, f in (holder.get("flow") or {}).items():
        try:
            buy, sell, n, ns, t0, t1, pxw = f[:7]
            bot = f[7] if len(f) > 7 else 0
            b0 = (f[8] if len(f) > 9 else 0) or t0
            b1 = (f[9] if len(f) > 9 else 0) or t1
        except (TypeError, ValueError):
            continue
        if bot or buy < thr or sell > BUYER_MAX_SELL_SHARE * buy or now - b1 > 72 * 3600:
            continue
        rows.append([w, round(buy), round(sell), n, (pxw / buy) if buy > 0 else 0.0, round(b1), round(b0)])
    rows.sort(key=lambda r: r[1] - r[2], reverse=True)
    return rows


def capture_buyers(key: str, chain: str, pair: dict, m: dict, b: dict, now: float) -> bool:
    """Look at who has been buying this coin and remember it. Every accumulating wallet is saved in the wallet book
    and its buy is followed from the price it paid, so the bot learns which wallets actually make money."""
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
    pool, token = pair.get("pairAddress", ""), key.split(":", 1)[1]
    full = analyze_trades(chain, trades, pool, token, m["mc"])
    wm = float(b.get("wm") or 0)
    new = analyze_trades(chain, trades, pool, token, m["mc"], since=wm) if wm else full
    flow_merge(b, new, full, now)
    if full["last"]:
        b["wm"] = full["last"]
    acc = flow_accumulators(b, m["mc"], now)
    b["cap"] = {"t": now, "n": full["n"], "span": round(full["span"], 1), "buy": round(full["buy"]),
                "sell": round(full["sell"]), "nb": full["nb"], "ns": full["ns"], "bots": full["bots"],
                "acc": acc[:CAP_ACC_KEPT], "na": len(acc)}
    b["cap_tries"] = 0
    for row in acc[:BOOK_ADD_PER_COIN]:
        try:
            book_add(row[0], key, chain, pair, row, now)
        except Exception:
            log.exception("wallet book error")
        if row[0] in state["watch"]:      # a wallet that is already on your list: that counts as a tracked buy
            buys = state["whale_buys"].setdefault(key, [])
            if not any(x[1] == row[0] and now - x[0] < 3600 for x in buys):
                buys.append([now, row[0], row[1]])
    bot_stats["captured"] += 1
    return True


def smart_in_cap(b: dict) -> list[str]:
    """Wallets with a good record (yours, or earned by results) among this coin's latest accumulators."""
    scores = wallet_scores()
    return [r[0] for r in (b.get("cap") or {}).get("acc", []) if r and trusted_wallet(r[0], scores)]


def buyers_lines(b: dict, key: str | None = None) -> str:
    cap = b.get("cap")
    if not cap:
        return ""
    key = key or (sk(b["chain"], b["ca"]) if b.get("ca") and b.get("chain") else None)
    scores = wallet_scores()
    L = [f"🧠 Who's buying (latest {cap.get('n', 0)} trades, spanning {fmt_span(cap.get('span', 0))}): "
         f"*{cap.get('nb', 0)}* wallets net-buying vs {cap.get('ns', 0)} net-selling - bought "
         f"{fmt_usd(cap.get('buy', 0))}, sold {fmt_usd(cap.get('sell', 0))}"]
    for r in (cap.get("acc") or [])[:3]:
        if not r:
            continue
        w, buy, sell = r[0], r[1], r[2]
        more = also_buying(b, w, key)
        graded = book_grade(w, scores) or (scores.get(w) or {}).get("n")
        L.append(f"   • `{w}` bought {fmt_usd(buy)}" + (f", sold {fmt_usd(sell)}" if sell else "")
                 + (f" · {wallet_record_text(w, scores)}" if graded else "")
                 + (" · also holding " + ", ".join(more[:3]) if more else ""))
    if cap.get("acc"):
        L.append("   (/watch <address> <name> follows one of them; I save every accumulating wallet and score it by results)")
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
    key = sk(chain, addr)
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
    body = "\n".join(L) + "\n"
    c = state["charts"].get(key)
    if c and c.get("ok") and time.time() - c.get("t", 0) < 24 * 3600:
        body += chart_lines(key, c, m, num(pair, "priceUsd"))
    body += buyers_lines(b, key)
    if smart:
        sc_all = wallet_scores()
        body += "👀 Wallets I trust (on your list, or earned by results) are accumulating it: " + ", ".join(
            "*" + md_escape(wallet_label(w)) + "*" + (f" ({stars_text(sc_all[w]['stars'])})" if (sc_all.get(w) or {}).get("stars") is not None else "")
            for w in smart) + "\n"
    body += wallets_line(held)
    if chain == "robinhood":
        body += ("⚠️ Robinhood Chain is brand new: many of its wallets are airdrop-farming bots, so wallet signals are "
                 "weaker here, and DexScreener's numbers can jump around.\n")
    body += (f"📌 Tracking this for {follow_text(BOTTOM_TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). "
             "A base forming is a shortlist, not a buy signal on its own.\n"
             f"ℹ️ Sparsity data through {md_escape(b.get('through') or b.get('upd') or '?')}.\n\n"
             f"`{addr}`\n\n"
             f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', addr)}) | [Sparsity]({SPARSITY_URL})")
    return body


def following_coin(key: str) -> bool:
    """Is a call or an experiment (not just a wallet buy) already following this coin?"""
    return any(r["status"] == "open" and r["key"] == key
               and r["kind"] in ("bottom", "bshadow", "shadow", "call", "narrative", "wallet", "chart", "cshadow", "smartbuy",
                                 "vip", "lowcap", "kol", "lshadow")
               for r in state["records"])


def drop_quiet_follow(key: str, now: float):
    """The coin just got a real alert: its quiet follow-up is closed and not counted, so it is not counted twice."""
    for r in open_records(key):
        if r["kind"] in ("bshadow", "lshadow"):
            r["status"], r["t1"], r["void"] = "expired", now, True


def maybe_add_bottom_shadow(key: str, chain: str, pair: dict, m: dict, b: dict, now: float):
    """Follow a coin quietly (any coin on Sparsity's map that did not earn an alert) so the report can compare
    3 of 3, 2 of 3, 1 of 3 and 0 of 3, and the alerts."""
    if following_coin(key) or now - state["shadow_cd"].get("b:" + key, 0) < SHADOW_COOLDOWN:
        return
    if len(fresh_open("bshadow")) >= MAX_BOTTOM_SHADOWS:
        return
    if not clear_to_alert(key, chain, pair):
        return                        # v3.1: a steady asset (or not checked yet)
    state["shadow_cd"]["b:" + key] = now
    add_record("bshadow", key, chain, pair, m, "", now, extra={"signs": b.get("signs"), "th": BOTTOM_TRACK_HOURS})


def bottom_cap_hours(b: dict) -> float:
    """How often to look at the buyers of a coin on Sparsity's map: more often once it shows 2+ signs."""
    return SETUP_CAPTURE_HOURS if int(b.get("signs") or 0) >= 2 else BUYER_RECAPTURE_HOURS


def capture_due(b: dict, now: float, hours: float | None = None) -> bool:
    """Is it time to look at who is buying this coin (never looked at, or the last look is old), and has the
    last attempt not just failed twice?"""
    hours = BUYER_RECAPTURE_HOURS if hours is None else hours
    if not BUYER_CAPTURE or now - (b.get("cap") or {}).get("t", 0) <= hours * 3600:
        return False
    return b.get("cap_tries", 0) < 2 or now - b.get("cap_try", 0) > 6 * 3600


def bottom_backing(key: str, b: dict, now: float):
    """What backs a bottom signal: -> (trusted wallets accumulating, number of accumulating wallets, a cup/base chart shape?, backed?)"""
    cap = b.get("cap") or {}
    smart = smart_in_cap(b)
    acc_n = sum(1 for r in cap.get("acc", []) if r)
    c = state["charts"].get(key) or {}
    shape = bool(c.get("ok") and c.get("pat") and c.get("score", 0) >= CHART_MIN_SCORE and now - c.get("t", 0) < 24 * 3600)
    return smart, acc_n, shape, bool(smart or (acc_n >= 2 and cap.get("nb", 0) > cap.get("ns", 0)) or shape)


def bottom_alert_coming(key: str, now: float) -> bool:
    """Is a bottom alert about to go out for this coin? Then that alert already shows who is buying."""
    b = state["bottoms"].get(key)
    if not (alert_on("bottom") and b and bottom_active(b)) or now - b.get("alerted", 0) < BOTTOM_COOLDOWN_DAYS * 86400:
        return False
    if len([t for t in state["tuning"].get("bottom_ts", []) if now - t < 86400]) >= BOTTOM_MAX_PER_DAY:
        return False
    signs = int(b.get("signs") or 0)
    return signs >= BOTTOM_MIN_SIGNS or (signs == 2 and bottom_backing(key, b, now)[3])


def maybe_bottom(key: str, chain: str, pair: dict, b: dict, now: float, caps: list | None = None) -> bool:
    """Runs on every scan for each coin on Sparsity's map (3 of 3 down to 0 of 3). The buyers are looked at at the end of
    the scan (coins with more signs first). An alert goes out at 3 of 3, or at 2 of 3 when something else backs it:
    a wallet with a good record, two or more wallets accumulating it, or a cup / base on its chart. Every other coin
    is followed quietly, so the report can show whether 2 of 3, 1 of 3 and 0 of 3 are worth acting on."""
    signs = int(b.get("signs") or 0)
    if signs < BOTTOM_FOLLOW_SIGNS or num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD:
        return False
    m = bottom_metrics(key, pair, b)
    if not m:
        return False
    cooling = now - b.get("alerted", 0) < BOTTOM_COOLDOWN_DAYS * 86400
    if b.get("cap_tries", 0) >= 2 and now - b.get("cap_try", 0) > 6 * 3600:
        b["cap_tries"] = 0                        # it has been a while since it last failed: try again
    smart, acc_n, shape, backed = bottom_backing(key, b, now)
    c = state["charts"].get(key) or {}
    eligible = signs >= BOTTOM_MIN_SIGNS or (signs == 2 and backed)
    stamps = [t for t in state["tuning"].get("bottom_ts", []) if now - t < 86400]
    state["tuning"]["bottom_ts"] = stamps
    if alert_on("bottom") and eligible and not cooling and len(stamps) < BOTTOM_MAX_PER_DAY:
        if capture_due(b, now, bottom_cap_hours(b)):
            return False                          # the buyers are looked at at the end of this scan: the alert follows
        if not confirmed_twice("bottom", key, m["mc"], now):
            return False
        if not clear_to_alert(key, chain, pair):
            return False                          # v3.1: a steady asset (or not checked yet)
        held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
        title = ("🧭 BOTTOM SIGNAL" if signs >= 3 else "🧭 BOTTOM + WALLET YOU TRUST" if smart
                 else f"🧭 BOTTOM ({signs} of 3) + BACKING")
        if not send_telegram(build_bottom_message(chain, pair, m, b, held, smart, title)):
            return False
        b["alerted"] = now
        stamps.append(now)
        drop_quiet_follow(key, now)
        sig = signals_for(m, len(held["in"]))
        add_record("bottom", key, chain, pair, m, "", now,
                   extra={"signs": signs, "th": BOTTOM_TRACK_HOURS, "sig": sig, "smart": len(smart), "acc": acc_n,
                          "pat": c.get("pat", "") if shape else ""})
        bot_stats["bottom"] += 1
        log.info("BOTTOM alert for %s (%d of 3 signs)", key, signs)
        return True
    maybe_add_bottom_shadow(key, chain, pair, m, b, now)
    return False


# ----------------------------------------------------------------------------
# v2.9 - THE WALLET BOOK: every wallet that is seen accumulating a coin is saved in the background. Each buy is followed
# from the price the wallet itself paid, so the book knows who is WINNING. Wallets that keep winning earn a grade
# ("promising", then "proven" and a place on your watch list), and when one of them is accumulating a coin, you hear about it.
# ----------------------------------------------------------------------------
BOOK_FOLLOW_MAX = 150          # coins that are kept in the scan only because a wallet in the book bought them
BOOK_ADD_PER_COIN = 8          # accumulators of one coin saved per look
_book_idx: dict[str, list[str]] = {}      # coin key -> wallets with an open buy on it (rebuilt at the start of every scan)


def book_index():
    global _book_idx
    idx: dict[str, list[str]] = {}
    for w, d in list(state["book"].items()):
        for p in d.get("picks", []):
            if p.get("st") == "o":
                idx.setdefault(p["k"], []).append(w)
    _book_idx = idx


def open_pick_keys() -> set:
    """Coins that a wallet in the book bought and whose result is not in yet: the scan keeps following them."""
    latest: dict[str, float] = {}
    for d in list(state["book"].values()):
        for p in d.get("picks", []):
            if p.get("st") == "o" and not p.get("ext"):           # (v3.8: buys on extra time are looked at in the slow lane - see slow_keys)
                latest[p["k"]] = max(latest.get(p["k"], 0.0), p.get("t", 0.0))
    return set(sorted(latest, key=lambda k: -latest[k])[:BOOK_FOLLOW_MAX])


def slow_keys() -> set:
    """v3.8: coins that are only followed on extra time (a call, a wallet's buy, a coin you sent me, past its first window): they are priced
    every SLOW_LANE_EVERY-th scan, which keeps the scans light however many there are."""
    out = {r["key"] for r in state["records"] if r["status"] == "open" and r.get("ext")}
    for d in list(state["book"].values()):
        for p in d.get("picks", []):
            if isinstance(p, dict) and p.get("st") == "o" and p.get("ext") and p.get("k"):
                out.add(p["k"])
    out |= link_open_keys()
    return out


def row_time(row: list, i: int, now: float) -> int:
    """A real trade time from position i of an accumulator row (0 = not known / not believable)."""
    try:
        t = float(row[i])
    except (IndexError, TypeError, ValueError):
        return 0
    return int(t) if math.isfinite(t) and 0 < t <= now + 600 else 0


def book_add(w: str, key: str, chain: str, pair: dict, row: list, now: float) -> bool:
    """Save one accumulating wallet and its buy: row = [wallet, bought $, sold $, trades, average price paid, time of its latest buy,
    time of its first buy]. v3.8: the buy remembers when the wallet REALLY bought ('bt' = latest, 'bf' = first; before, only the time I
    looked was saved, so a buy from weeks ago looked new) and how far the coin had already moved on the wallet's price when I first saw
    the buy ('lr0': a hit that was already in the bag by then is an 'early' hit, see wallet_scores)."""
    px_now = num(pair, "priceUsd")
    if not w or px_now <= 0 or key in state["steady"]:      # (v3.1: a steady asset says nothing about a wallet)
        return False
    book = state["book"]
    d = book.get(w)
    if d is None:
        if len(book) >= BOOK_MAX:
            book_prune(now)
            if len(book) >= BOOK_MAX:
                return False
        d = book[w] = {"first": round(now), "last": round(now), "chain": chain, "seen": 0, "n": 0, "good": 0,
                       "win": 0, "stop": 0, "flat": 0, "picks": []}
    d["last"] = round(now)
    bt, bf = row_time(row, 5, now), row_time(row, 6, now)
    for p in d["picks"]:
        if p["k"] == key and (p["st"] == "o" or now - (p.get("t1") or p["t"]) < 14 * 86400):
            if p["st"] == "o":
                p["usd"] = max(p.get("usd", 0), round(row[1]))     # still accumulating: keep the bigger total
                if bt > num(p, "bt"):
                    p["bt"] = bt                                   # (the wallet bought again: its latest buy moves forward)
                if bf and not p.get("bf"):
                    p["bf"] = bf
            return False
    paid = float(row[4] or 0)
    own = 1 if (paid > 0 and 0.2 <= paid / px_now <= 5.0) else 0   # the wallet's own price, when it makes sense
    px = paid if own else px_now
    d["picks"].append({"k": key, "s": str(pair.get("baseToken", {}).get("symbol", "?"))[:12], "c": chain, "t": round(now),
                       "px": float(f"{px:.6g}"), "pa": pair.get("pairAddress", ""), "usd": round(row[1]), "st": "o",
                       "mx": 1.0, "lr": round(px_now / px, 3), "lr0": round(px_now / px, 3), "own": own, "th": BOOK_TRACK_HOURS,
                       "bt": bt, "bf": bf or bt})
    d["seen"] += 1
    while len(d["picks"]) > BOOK_PICKS:       # keep the memory small: the oldest finished buy goes first
        i = next((j for j, p in enumerate(d["picks"]) if p["st"] != "o"), 0 if len(d["picks"]) > 2 * BOOK_PICKS else None)
        if i is None:
            break
        d["picks"].pop(i)
    return True


def pick_price(p: dict, pair: dict | None, pairs: list | None) -> float | None:
    """Where is the coin now, on the pool the wallet bought on (or the best pool if that one is drained)?
    0.0 = no live pool; None = no usable reading this time."""
    if pair is None:
        return 0.0
    floor = max(MIN_LIVE_LIQUIDITY_USD, 0.2 * num(pair, "liquidity", "usd"))
    same = next((x for x in (pairs or []) if x.get("pairAddress") == p.get("pa") and num(x, "liquidity", "usd") >= floor), None)
    px = num(same or pair, "priceUsd")
    return px if px > 0 else None


def book_track(key: str, pair: dict | None, pairs: list | None, now: float):
    """Update every open buy on this coin: from the price the wallet paid. Same two-readings-in-a-row rule as the alerts:
    a win (WIN_MULTIPLE x) or a stop (STOP_MULTIPLE x) only counts when two readings agree."""
    for w in list(_book_idx.get(key, [])):
        d = state["book"].get(w)
        if not d:
            continue
        for p in d["picks"]:
            if p["k"] != key or p["st"] != "o":
                continue
            hours = float(p.get("th") or BOOK_TRACK_HOURS)
            cur = pick_price(p, pair, pairs)
            if cur is None:
                if now - p["t"] >= hours * 3600:
                    book_close(w, p, "f", now)
                continue
            ratio = min(cur / p["px"], 1000.0) if p["px"] > 0 else 0.0
            prev = p.get("lr", ratio)
            p["lr"] = round(ratio, 3)
            if ratio > 0:
                p.pop("z", None)
            else:
                p.setdefault("z", now)
            hi, lo = max(ratio, prev), min(ratio, prev)
            p["mx"] = round(max(p.get("mx", 1.0), lo), 3)
            if hi > 0:
                p["mn"] = round(min(p.get("mn", hi), hi), 3)         # v3.8: the lowest point so far (two readings in a row at or under it)
            if hi <= STOP_MULTIPLE and (ratio > 0 or now - p["z"] >= RUG_CONFIRM_SECONDS):
                book_close(w, p, "s", now)
            elif p["mx"] >= WIN_MULTIPLE:
                book_close(w, p, "w", now)
            elif p.get("lo1") and ratio > 0 and hi <= EXTEND_BREAK * p["lo1"]:
                p["why"] = "broke"                                    # v3.8: on extra time and now under the lowest point of its first window
                book_close(w, p, "f", now)
            elif now - p["t"] >= hours * 3600:
                if not extend_pick(p, ratio, now):
                    book_close(w, p, "f", now)


def extend_pick(p: dict, ratio: float, now: float) -> bool:
    """v3.8: the same patience for a wallet's buy as for a call (see extend_followup): when its window ends and the coin is still at least
    EXTEND_ALIVE x the price the wallet paid, it is followed another week - up to a limit that grows with the size of the coin - instead
    of ending 'flat'. A wallet that bought something slow but good is no longer rated as if it had bought nothing."""
    if not EXTEND_ON or ratio <= 0 or ratio < EXTEND_ALIVE or num(p, "px") <= 0:
        return False
    th = float(p.get("th") or BOOK_TRACK_HOURS)
    cap = extend_cap_hours(mc_cached(p.get("k", ""), 3600) or 0.0, "pick")
    if th >= cap - 1:
        return False
    if not p.get("ext") and extended_open() >= EXTEND_MAX_OPEN:
        return False
    p["th"] = min(th + EXTEND_STEP_HOURS, cap)
    p["ext"] = int(p.get("ext") or 0) + 1
    if not p.get("lo1"):
        p["lo1"] = p.get("mn") or ratio
    return True


def book_close(w: str, p: dict, outcome: str, now: float):
    p["st"], p["t1"] = outcome, round(now)
    d = state["book"].get(w)
    if not d:
        return
    d["n"] += 1
    d[{"w": "win", "s": "stop", "f": "flat"}[outcome]] += 1
    if outcome == "w" or p.get("mx", 1.0) >= SMART_GOOD_MULT:
        d["good"] += 1


def crowd_sizes() -> dict[str, int]:
    """How many saved wallets bought each coin (finished buys and open ones)."""
    sizes: dict[str, int] = {}
    for d in list(state["book"].values()):
        for k in {p.get("k") for p in d.get("picks", []) if p.get("k")}:
            sizes[k] = sizes.get(k, 0) + 1
    return sizes


def crowd_weight(crowd: int) -> float:
    """How much one wallet's result on a coin counts. When many saved wallets bought the same coin, they all win or all lose
    together - that is one piece of evidence, not many. A group of CROWD_FULL_WEIGHT wallets or fewer counts in full."""
    return min(1.0, CROWD_FULL_WEIGHT / max(1, crowd))


def star_rating(hits: float, misses: float, n: int) -> float | None:
    """v3.7: 1-3 stars in half steps from a wallet's finished buys: 'hits' (the coin doubled before it halved) and 'misses'
    (it halved first) among n finished buys; flat buys count in n but not in the share. None = not rated yet.
    See the STAR_* notes at the top for why the lines sit where they do."""
    if n < STAR_MIN_BUYS or hits + misses < STAR_MIN_CLEAR - 1e-9:
        return None
    share = (hits + STAR_PRIOR * STAR_PRIOR_WEIGHT) / (hits + misses + STAR_PRIOR_WEIGHT)
    stars = 0.5
    for line, val in STAR_STEPS:
        if share >= line - 1e-9:
            stars = val
            break
    for below, cap in STAR_CAPS:
        if n < below:
            stars = min(stars, cap)
            break
    return stars


def stars_text(stars: float | None) -> str:
    """'⭐⭐½ 2.5/3' (empty for a wallet that is not rated yet)."""
    if stars is None:
        return ""
    full = int(stars + 1e-9)
    half = stars - full >= 0.5 - 1e-9
    bar = "⭐" * full + ("½" if half else "")
    if not full:
        bar = "½⭐"
    return f"{bar} {stars:g}/3"


def is_early(p: dict) -> bool:
    """v3.8: was this buy 'early' - already well in profit when I first saw it? The coin was up EARLY_FORWARD_LR x or more on the wallet's own
    price at that moment ('lr0'), so a doubling from the wallet's price needed only a small move AFTER I could have acted. (A buy saved by an
    older version has no 'lr0': a win within 30 minutes of being saved counts as early.) Such hits show the wallet bought low - but not that
    following it would have paid - so they count EARLY_WIN_WEIGHT instead of 1 in the wallet's rating."""
    lr0 = p.get("lr0")
    if isinstance(lr0, (int, float)) and not isinstance(lr0, bool):
        return lr0 >= EARLY_FORWARD_LR
    t1, t0 = p.get("t1"), p.get("t")
    return bool(p.get("st") == "w" and isinstance(t1, (int, float)) and isinstance(t0, (int, float)) and t1 - t0 <= EARLY_QUICK_SECONDS)


def bot_found(info) -> bool:
    """A wallet the BOT put on the watch list by itself (a smart wallet found by results, or a low-cap hunter) - not one you added."""
    return isinstance(info, dict) and bool(info.get("auto") or info.get("tier") == "hunter")


def cold_record(w: str, scores: dict | None = None) -> bool:
    """Has this wallet's record gone clearly bad? MUTE_MIN_BUYS+ finished buys, MUTE_MIN_CLEAR+ of them clear (a hit or a miss), and under
    MUTE_BELOW stars (half a star = about 1 hit in 5 clear results, or worse)."""
    sc = (scores if scores is not None else wallet_scores()).get(w)
    return bool(sc and sc["n"] >= MUTE_MIN_BUYS and sc.get("win", 0) + sc.get("stop", 0) >= MUTE_MIN_CLEAR
                and sc.get("stars") is not None and sc["stars"] < MUTE_BELOW - 1e-9)


def wallet_cold(w: str, scores: dict | None = None) -> bool:
    """v3.8: 'stop paying attention to wallets that keep losing'. A wallet with a cold record no longer counts toward any alert (a crowd of
    saved wallets, a wallet with a good record, ...). Its buys are still followed in the background and it counts again by itself when its
    record recovers. A wallet YOU added is never muted by the bot - wallet_checkup tells you about it instead."""
    if not MUTE_ON:
        return False
    info = state["watch"].get(w)
    if info is not None and not bot_found(info):
        return False
    return cold_record(w, scores)


def wallet_scores() -> dict[str, dict]:
    """Per wallet: how many of its followed buys were GOOD (the coin reached SMART_GOOD_MULT x the price it paid, or hit
    the win mark). From the wallet book, plus the older 'whale buy' records. One count per coin.
    'wg' is the number of good buys with each one weighted by crowd_weight (a coin that a big crowd of saved wallets
    bought counts less); the grades below are judged on that.
    v3.7: also how the finished buys ENDED - 'win' (doubled before halving), 'stop' (halved first), 'flat' - and the star
    rating made from them ('ww' / 'ws' are the hits and misses with the same crowd weighting)."""
    out: dict[str, dict] = {}
    seen = set()
    crowd = crowd_sizes()
    for w, d in list(state["book"].items()):
        for p in d.get("picks", []):
            if p.get("st") != "o":
                seen.add((w, p["k"]))
        if d.get("n"):
            fin = [p for p in d["picks"] if p.get("st") != "o"]
            wts = [crowd_weight(crowd.get(p["k"], 1)) for p in fin]
            # v3.8: a hit I could not have followed (the coin was already up on the wallet's price when I first saw the buy) counts less
            ew = [EARLY_WIN_WEIGHT if is_early(p) else 1.0 for p in fin]
            goods = [bool(p["st"] == "w" or p.get("mx", 1.0) >= SMART_GOOD_MULT) for p in fin]
            old_n = max(0, d["n"] - len(fin))                       # finished buys no longer in the list (the oldest are forgotten)
            old_g = min(old_n, max(0, d.get("good", 0) - sum(goods)))
            ww = sum(x * e for x, e, p in zip(wts, ew, fin) if p["st"] == "w") + max(0, d.get("win", 0) - sum(1 for p in fin if p["st"] == "w"))
            ws = sum(x for x, p in zip(wts, fin) if p["st"] == "s") + max(0, d.get("stop", 0) - sum(1 for p in fin if p["st"] == "s"))
            out[w] = {"n": d["n"], "good": d.get("good", 0), "win": d.get("win", 0), "chain": d.get("chain", "solana"),
                      "stop": d.get("stop", 0), "flat": d.get("flat", 0), "ww": round(ww, 2), "ws": round(ws, 2),
                      "wg": round(sum(x * e for x, e, g in zip(wts, ew, goods) if g) + old_g, 2),
                      "early": sum(1 for p in fin if p["st"] == "w" and is_early(p)),
                      "coins": [(p["s"], p["c"], g) for p, g in zip(fin, goods)]}
    for r in state["records"]:
        w = r.get("wallet")
        if r["kind"] != "whale" or r["status"] == "open" or r.get("void") or not w or (w, r["key"]) in seen:
            continue
        seen.add((w, r["key"]))
        d = out.setdefault(w, {"n": 0, "good": 0, "win": 0, "coins": [], "chain": r["chain"], "wg": 0.0})
        good = r["status"] == "win" or r.get("best", 0) >= SMART_GOOD_MULT
        d["n"] += 1
        d["good"] += bool(good)
        d["win"] += r["status"] == "win"
        d["stop"] = d.get("stop", 0) + (r["status"] == "stop")
        d["flat"] = d.get("flat", 0) + (r["status"] not in ("win", "stop"))
        d["ww"] = round(d.get("ww", 0.0) + (r["status"] == "win"), 2)
        d["ws"] = round(d.get("ws", 0.0) + (r["status"] == "stop"), 2)
        d["wg"] = round(d.get("wg", 0.0) + bool(good), 2)
        d["coins"].append((r["sym"], r["chain"], bool(good)))
    for d in out.values():
        d["stars"] = star_rating(d.get("ww", 0.0), d.get("ws", 0.0), d["n"])
    return out


def book_grade(w: str, scores: dict | None = None) -> str:
    """'watched' (on your list), 'cooling' (a wallet the bot put on your list by itself whose results have gone bad), 'proven'
    (a good record: 2+ stars), 'promising' (a good start) or '' (nothing yet)."""
    info = state["watch"].get(w)
    if info is not None:
        # v3.7: a smart wallet the bot added by itself (not a VIP, not a hunter) whose good run is over stays on the list but stops counting as
        # a wallet with a good record. v3.8: ANY wallet the bot found (hunters too) whose record keeps losing stops counting (and is taken off
        # the list at the next check-up) - a much stricter line (half a star), so a hunter with a middling record is not touched.
        if (bot_found(info) and wallet_cold(w, scores)) or (isinstance(info, dict) and info.get("auto") and cooled_off(w, scores)):
            return "cooling"
        return "watched"
    return record_grade(w, scores)


def cooled_off(w: str, scores: dict | None = None) -> bool:
    """Has this wallet's record gone bad? STAR_COOL_BUYS+ finished buys, STAR_COOL_CLEAR+ of them clear (a hit or a miss) and
    fewer than STAR_COOL_BELOW stars. (book_grade only applies it to wallets the bot added to the list by itself.)"""
    sc = (scores if scores is not None else wallet_scores()).get(w)
    return bool(sc and sc["n"] >= STAR_COOL_BUYS and sc.get("win", 0) + sc.get("stop", 0) >= STAR_COOL_CLEAR
                and sc.get("stars") is not None and sc["stars"] < STAR_COOL_BELOW - 1e-9)


def grade_of(d: dict | None) -> str:
    """'proven', 'promising' or '' for one wallet's score. Judged on the crowd-weighted good buys (v3.1): a good result on a coin
    that a whole crowd of saved wallets bought barely counts, so a wallet has to be right on coins of its own - while every
    finished buy, good or bad, counts in full in its hit rate.
    v3.7: 'proven' also needs STAR_GOOD stars (touching 1.5x on the way is not enough if the coin then halved), and a wallet that
    is rated and has under STAR_COOL_BELOW stars is not 'promising' either."""
    if not d or not d["n"]:
        return ""
    wg = float(d.get("wg", d["good"]))
    rate = wg / d["n"]
    stars = d["stars"] if "stars" in d else star_rating(d.get("ww", d.get("win", 0)), d.get("ws", d.get("stop", 0)), d["n"])
    if (d["n"] >= SMART_MIN_BUYS and wg >= SMART_MIN_GOOD - 1e-9 and rate >= SMART_MIN_RATE
            and stars is not None and stars >= STAR_GOOD - 1e-9):
        return "proven"
    hits, misses = float(d.get("ww", d.get("win", 0))), float(d.get("ws", d.get("stop", 0)))
    if (wg >= PROMISING_MIN_WEIGHT - 1e-9 and rate >= SMART_MIN_RATE and hits >= PROMISING_MIN_WEIGHT - 1e-9 and hits >= misses
            and (stars is None or stars >= STAR_COOL_BELOW - 1e-9)):
        return "promising"           # a good start = at least one real hit (doubled before halving) and no more misses than hits
    return ""


def record_grade(w: str, scores: dict | None = None) -> str:
    """The same, judged by results only (a wallet on your list is graded too)."""
    return grade_of((scores if scores is not None else wallet_scores()).get(w))


def trusted_wallet(w: str, scores: dict | None = None) -> bool:
    """On your watch list (and not cooled off), or earned it by results."""
    return book_grade(w, scores) in ("watched", "proven")


GRADE_ICON = {"watched": "👀", "cooling": "🧊", "proven": "🏅", "promising": "🌱"}


def wallet_stars(w: str, scores: dict | None = None) -> float | None:
    sc = (scores if scores is not None else wallet_scores()).get(w)
    return sc.get("stars") if sc else None


def record_counts(sc: dict) -> str:
    """'5 doubled, 2 halved, 4 flat' - how a wallet's finished buys ended."""
    return f"{sc.get('win', 0)} doubled, {sc.get('stop', 0)} halved, {sc.get('flat', 0)} flat"


def trusted_points(stars: float | None) -> float:
    """Smart-money points (of 4, in the /check scorecard) for ONE trusted wallet that is accumulating a coin, by its stars: a good record
    gets 2, an average one - or none yet - 1.5, a weak one 0.5."""
    if stars is None:
        return 1.5
    if stars >= STAR_GOOD - 1e-9:
        return 2.0
    if stars >= STAR_COOL_BELOW - 1e-9:
        return 1.5
    return 0.5


def wallet_stars_line(sc: dict) -> str:
    """'⭐⭐½ 2.5/3 · 5 doubled, 2 halved, 4 flat' (or 'not rated yet · ...') for one wallet's score."""
    st = sc.get("stars")
    return (stars_text(st) if st is not None else "not rated yet") + " · " + record_counts(sc)


def wallet_record_text(w: str, scores: dict | None = None) -> str:
    """The wallet's stars and how its followed buys ended (v3.7; the old 'N of M good' counted a coin that touched 1.5x and then
    halved as good, which was too generous)."""
    sc = (scores if scores is not None else wallet_scores()).get(w)
    if not sc or not sc["n"]:
        return "no finished buys yet"
    rec = wallet_stars_line(sc)
    if sc.get("ww", 0) + sc.get("ws", 0) < sc.get("win", 0) + sc.get("stop", 0) - 0.05:
        rec += " (coins that many saved wallets bought count less)"
    return rec


def acc_count(cap: dict | None) -> int:
    """How many wallets were accumulating a coin in a look. (v3.8.1: the true count - only the biggest CAP_ACC_KEPT are kept as rows,
    and a look saved by an older version kept 6.)"""
    cap = cap or {}
    return max(int(num(cap, "na")), sum(1 for r in (cap.get("acc") or []) if r))


def buyer_mix(rows: list, scores: dict) -> dict[str, list]:
    """v3.8.1: split a coin's accumulating wallets by what I know about them: 'good' (a medal: on your list, or a good record, or a promising
    start), 'cool' (was on your list, its results turned bad), 'weak' (rated, under STAR_COOL_BELOW stars), 'mid' (rated, average, no medal)
    and 'new' (not rated yet)."""
    out: dict[str, list] = {"good": [], "cool": [], "weak": [], "mid": [], "new": []}
    for r in rows:
        if not r:
            continue
        g = book_grade(r[0], scores)
        st = wallet_stars(r[0], scores)
        kind = ("good" if g in ("watched", "proven", "promising") else "cool" if g == "cooling" else "new" if st is None
                else "weak" if st < STAR_COOL_BELOW - 1e-9 else "mid")
        out[kind].append(r)
    return out


def buyer_mix_text(rows: list, scores: dict) -> str:
    """'2 🧊 cooling (smart-0x188e25 ⭐½ 0.5/3, ...), 6 with a weak record, 4 not rated yet' - the buyers that have no medal, and why."""
    mix = buyer_mix(rows, scores)
    parts = []
    if mix["cool"]:
        names = ", ".join(f"{md_escape(wallet_label(r[0]))} {stars_text(wallet_stars(r[0], scores))}".strip() for r in mix["cool"][:2])
        parts.append(f"{len(mix['cool'])} 🧊 cooling ({names}{', ...' if len(mix['cool']) > 2 else ''})")
    if mix["weak"]:
        parts.append(f"{len(mix['weak'])} with a weak record")
    if mix["mid"]:
        parts.append(f"{len(mix['mid'])} with an average record")
    if mix["new"]:
        parts.append(f"{len(mix['new'])} not rated yet")
    return ", ".join(parts)


def wallet_open_picks(w: str, exclude: str | None = None) -> list[dict]:
    d = state["book"].get(w)
    return [p for p in (d or {}).get("picks", []) if p.get("st") == "o" and p["k"] != exclude]


def also_buying(b: dict | None, wallet: str, key: str | None = None) -> list[str]:
    """The OTHER coins this wallet bought that are still being followed, with how each is doing since its buy."""
    out = []
    for p in sorted(wallet_open_picks(wallet, key), key=lambda p: -p.get("t", 0)):
        mc = mc_cached(p["k"])          # v3.5: market cap next to every coin
        out.append(f"${md_escape(p['s'])} ({p.get('lr', 1.0) - 1:+.0%}" + (f", MC {fmt_usd(mc)}" if mc else "") + ")")
    return out


def book_prune(now: float):
    """Keep the book small: forget wallets that did nothing useful for weeks, and the weakest ones if it is full."""
    book = state["book"]
    cutoff = now - BOOK_KEEP_DAYS * 86400
    for w, d in list(book.items()):
        has_open = any(p.get("st") == "o" for p in d.get("picks", []))
        if not has_open and d.get("good", 0) == 0 and w not in state["watch"] and d.get("last", 0) < cutoff:
            del book[w]
            continue
        while len(d.get("picks", [])) > BOOK_PICKS:               # finished buys beyond the limit: oldest first
            i = next((j for j, p in enumerate(d["picks"]) if p.get("st") != "o"), None)
            if i is None:
                break
            d["picks"].pop(i)
    if len(book) > BOOK_MAX - 20:
        def value(item):
            w, d = item
            return (w in state["watch"], d.get("good", 0) >= SMART_MIN_GOOD, d.get("good", 0), any(p.get("st") == "o" for p in d.get("picks", [])),
                    d.get("last", 0))
        for w, _ in sorted(book.items(), key=value)[:max(0, len(book) - (BOOK_MAX - 20))]:
            del book[w]


def book_stats() -> dict:
    scores = wallet_scores()
    grades = Counter(record_grade(w, scores) for w in state["book"])
    return {"wallets": len(state["book"]), "finished": sum(1 for d in state["book"].values() if d.get("n")),
            "open": sum(1 for d in state["book"].values() for p in d.get("picks", []) if p.get("st") == "o"),
            "proven": grades.get("proven", 0), "promising": grades.get("promising", 0)}


def book_status_line() -> str:
    s = book_stats()
    if not s["wallets"]:
        return "🧠 Wallet book: empty so far (it fills as I look at who is buying each coin)."
    return (f"🧠 Wallet book: {s['wallets']} wallets saved, {s['open']} of their buys being followed, {s['finished']} with finished "
            f"results ({s['proven']} proven, {s['promising']} promising); {len(auto_wallets())} on your watch list by results.")


def auto_wallets() -> list:
    return [(w, d) for w, d in state["watch"].items() if d.get("auto")]


# --- the alert: a wallet with a record is accumulating a coin ---
def smart_buy_pass(cands: list, now: float):
    """Every scan: a coin whose latest look at the buyers shows a wallet with a good record (or two promising ones)
    accumulating it gets an alert."""
    if not alert_on("smartbuy"):
        return
    scores = None
    for key, chain, pair, mc in cands:
        cap = cap_of(key)
        if not cap or not cap.get("acc") or now - cap.get("t", 0) > 36 * 3600:
            continue
        if now - state["cooldowns"].get("sb:" + key, 0) < 24 * 3600:
            continue
        if scores is None:
            scores = wallet_scores()
        try:
            maybe_smart_buy(key, chain, pair, cap, scores, now)
        except Exception:
            log.exception("smart-buy check error")


def maybe_smart_buy(key: str, chain: str, pair: dict, cap: dict, scores: dict, now: float) -> bool:
    if num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD:
        return False
    # a bottom or chart alert for this coin just went out: it already shows who is buying
    b = state["bottoms"].get(key)
    if now - state["cooldowns"].get("chart:" + key, 0) < 12 * 3600 or (b and now - b.get("alerted", 0) < 12 * 3600):
        return False
    if bottom_alert_coming(key, now) or chart_alert_coming(key, now):
        return False                   # that alert is on its way and says who is buying: one message, not two
    if lowcap_alert_coming(key, now, scores):
        return False                   # v3.5: the low-cap crowd alert is on its way and says who is buying
    # v3.8: r[5] is the time of the wallet's latest BUY - it must be recent (SMARTBUY_FRESH_HOURS)
    rows = [r for r in (cap.get("acc") or []) if r and now - (r[5] if len(r) > 5 else cap.get("t", 0)) < _tune_num("smartbuy_fresh", SMARTBUY_FRESH_HOURS) * 3600]
    strong = [r for r in rows if book_grade(r[0], scores) in ("watched", "proven")]
    soft = [r for r in rows if book_grade(r[0], scores) == "promising"]
    if not (strong or len(soft) >= 2):
        return False
    # v3.8: an old coin is skipped (own data: wallet-driven calls on coins over 14-30 days old almost never double) - unless a wallet YOU named is in it
    age_cap = _tune_num("smartbuy_age", SMARTBUY_MAX_AGE_DAYS)
    named = any(isinstance(state["watch"].get(r[0]), dict) and not bot_found(state["watch"][r[0]]) for r in strong)
    if age_cap > 0 and not named and age_days_of(pair) > age_cap:
        maybe_add_lshadow(key, chain, pair, "sb-old", now)
        return False
    stamps = [t for t in state["tuning"].get("sb_ts", []) if now - t < 86400]
    state["tuning"]["sb_ts"] = stamps
    if len(stamps) >= SMART_BUY_MAX_PER_DAY:
        return False
    m = bottom_metrics(key, pair, b) if b and bottom_active(b) else compute_metrics(key, pair)
    if not m or m["mc"] < pool_floor():
        return False
    if not confirmed_twice("sb", key, m["mc"], now):
        return False
    if not clear_to_alert(key, chain, pair):
        return False                   # v3.1: a steady asset (or not checked yet)
    held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
    shown = (strong + soft)[:4]
    if not send_telegram(build_smart_buy_message(chain, pair, m, shown, scores, held, bool(strong))):
        return False
    state["cooldowns"]["sb:" + key] = now
    stamps.append(now)
    c = state["charts"].get(key) or {}
    add_record("smartbuy", key, chain, pair, m, "", now,
               extra={"th": BOOK_TRACK_HOURS, "sig": signals_for(m, len(held["in"])), "strong": len(strong), "soft": len(soft),
                      "wal": [r[0] for r in shown][:4], "signs": (b.get("signs") if b and bottom_active(b) else None),
                      "pat": c.get("pat", "") if c.get("ok") else ""})
    bot_stats["smartbuy"] += 1
    log.info("SMART-WALLET alert for %s (%d strong, %d promising)", key, len(strong), len(soft))
    return True


def build_smart_buy_message(chain: str, pair: dict, m: dict, rows: list, scores: dict, held: dict, strong: bool) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(base.get("symbol", "?"))
    key = sk(chain, addr)
    good = [r for r in rows if (wallet_stars(r[0], scores) or 0) >= STAR_GOOD - 1e-9]
    L = [f"*🧠 SMART WALLET BUYING: ${sym}*  |  {CHAINS[chain]['name']}", "",
         ("Wallets with a good record (" + format(STAR_GOOD, 'g') + "+ stars) are accumulating it:" if good else
          "Wallets on your list are accumulating it (their record with me is thin or mixed - see the stars):" if strong else
          "Two wallets with a promising start are accumulating it:")]
    for r in rows:
        w, buy, sell = r[0], r[1], r[2]
        paid = float(r[4]) if len(r) > 4 and r[4] else 0.0
        px = num(pair, "priceUsd")
        vs = f", now {px / paid - 1:+.0%} on that price" if paid > 0 and px > 0 and 0.2 <= paid / px <= 5 else ""
        more = also_buying(None, w, key)
        L.append(f"• `{w}`  {wallet_record_text(w, scores)}\n   bought {fmt_usd(buy)}" + (f", sold {fmt_usd(sell)}" if sell else "") + vs
                 + (" · also holding " + ", ".join(more[:3]) if more else ""))
    b = state["bottoms"].get(key)
    c = state["charts"].get(key)
    L += ["",
          f"💰 Market cap *{fmt_usd(m['mc'])}* · down {m['retrace']:.0f}% from its high of {fmt_usd(m['peak'])}",
          f"💧 liquidity {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC) · 📊 24h volume {fmt_usd(m['vol24'])} "
          f"({m['vol_ratio'] * 100:.0f}% of MC) · 👥 {m['traders']:,} txns · ⏱ {fmt_age(m['age_h'])}"]
    if b and bottom_active(b) and isinstance(b.get("signs"), int):
        L.append(f"🧭 Sparsity's bottom list: {b['signs']} of 3 signs")
    body = "\n".join(L) + "\n"
    if c and c.get("ok"):
        body += chart_lines(key, c, m, num(pair, "priceUsd"))
    body += wallets_line(held)
    if chain == "robinhood":
        body += ("⚠️ Robinhood Chain is brand new: many of its wallets are airdrop-farming bots, so wallet signals are "
                 "weaker here, and DexScreener's numbers can jump around.\n")
    body += (f"📌 Tracking this for {follow_text(BOOK_TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). "
             "A wallet with a record is a clue, not a guarantee - it can be wrong.\n\n"
             f"`{addr}`\n\n"
             f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', addr)})")
    return body


def promote_smart_wallets():
    """Wallets that keep buying coins shortly before they rise join the watch list on their own."""
    if not SMART_AUTO:
        return
    removed = set(state["tuning"].get("watch_removed") or [])
    auto_n = sum(1 for d in state["watch"].values() if d.get("auto"))
    for w, d in sorted(wallet_scores().items(), key=lambda kv: (-kv[1].get("wg", kv[1]["good"]), kv[1]["n"])):
        if auto_n >= SMART_MAX_AUTO or grade_of(d) != "proven":      # (v3.1: judged on the crowd-weighted record)
            continue
        kind = wallet_kind(w)
        key = w.lower() if kind == "evm" else w
        if not kind or key in state["watch"] or key in removed or key in EVM_WHALES or recently_demoted(key):
            continue
        info = state["watch"].setdefault(key, {"kind": kind, "t": time.time()})
        info.update(name=f"smart-{short(key)[:4]}{short(key)[-4:]}", auto=True, good=d["good"], n=d["n"], stars=d.get("stars"))
        if kind == "solana":
            _sweep["now"] = True
        auto_n += 1
        bot_stats["smart"] += 1
        names = ", ".join(("✅" if g else "▫️") + "$" + md_escape(s) for s, _, g in d["coins"][:6])
        send_telegram(
            f"🧠 *NEW SMART WALLET FOUND*\n\n`{key}`\n"
            f"Its record with me: {wallet_stars_line(d)}\n"
            f"Its latest followed buys (✅ = the coin rose {SMART_GOOD_MULT:g}x or more after the wallet bought it): {names}\n"
            f"I'm now watching it like the wallets you add yourself: you'll see it on alerts when it holds a coin"
            + (", and I'll message you when it buys a new coin (I look inside it every few minutes)" if kind == "solana"
               else ", and an alert can fire when it accumulates a coin I'm following")
            + f". Remove it any time with /unwatch.\n"
            f"[Wallet]({link(d['chain'] if d['chain'] in CHAINS else ('solana' if kind == 'solana' else 'ethereum'), 'addr', key)})")
        log.info("smart wallet promoted: %s (%d/%d)", short(key), d["good"], d["n"])


def recently_demoted(key: str, now: float | None = None) -> bool:
    """Did the bot take this wallet off the watch list for losing less than DEMOTE_DAYS ago? (Then it does not come back by itself yet.)"""
    dem = state["tuning"].get("demoted")
    t = dem.get(key) if isinstance(dem, dict) else None
    return isinstance(t, (int, float)) and not isinstance(t, bool) and (time.time() if now is None else now) - t < DEMOTE_DAYS * 86400


def demote_wallet(key: str, now: float):
    """Take a wallet the bot added to the watch list itself off it (the wallet book keeps following its buys). Unlike /unwatch this is not
    forever: after DEMOTE_DAYS days it can earn its place again if its record has recovered."""
    state["watch"].pop(key, None)
    state["accum"].pop(key, None)
    cur = state["tuning"].get("vip_cur")
    if isinstance(cur, dict):
        cur.pop(key, None)
    buys = state["tuning"].get("vip_buys")
    if isinstance(buys, dict):
        buys.pop(key, None)
    _snaps.pop(key, None)
    for k in [k for k in list(_hold_cache) if k[0] == key]:
        _hold_cache.pop(k, None)
    dem = state["tuning"].get("demoted")
    if not isinstance(dem, dict):
        dem = state["tuning"]["demoted"] = {}
    dem[key] = round(now)


def wallet_checkup(now: float):
    """v3.8: 'stop paying attention to smart wallets that keep losing'. Every few hours: a wallet the BOT put on the watch list (a smart wallet
    found by results, a low-cap hunter) whose record has gone cold is taken off it - its buys are still followed in the background, and it can
    come back after DEMOTE_DAYS days if it wins again. A wallet YOU added is never removed by the bot, but you are told once if its record is poor."""
    if not MUTE_ON:
        return
    tun = state["tuning"]
    if now - num(tun, "checkup_t") < 6 * 3600:
        return
    tun["checkup_t"] = now
    scores = wallet_scores()
    warned = tun.get("warned")
    if not isinstance(warned, dict):
        warned = tun["warned"] = {}
    dropped, yours = [], []
    for w, info in list(state["watch"].items()):
        if not isinstance(info, dict) or not cold_record(w, scores):
            continue
        label = wallet_label(w)
        line = f"• *{md_escape(label)}* `{short(w)}` - {wallet_stars_line(scores[w])}"
        if bot_found(info):
            demote_wallet(w, now)
            dropped.append(line)
            log.info("wallet taken off the list for losing: %s", short(w))
        elif now - num(warned, w) > 14 * 86400:
            warned[w] = round(now)
            yours.append(line + (f"  → /vip remove {md_escape(label)}" if info.get("vip") else f"  → /unwatch {md_escape(label)}"))
    for w in [w for w, t in warned.items() if not isinstance(t, (int, float)) or now - t > 60 * 86400]:
        warned.pop(w, None)
    if not (dropped or yours):
        return
    L = ["🧊 *Wallet check-up*", ""]
    if dropped:
        L += [f"I took these off your list and stopped paying attention to them (wallets I had put there myself): their record kept losing. "
              f"I still follow what they buy in the background, so they can earn their place back after {DEMOTE_DAYS:g} days:"] + dropped[:8] + [""]
    if yours:
        L += ["These are wallets YOU added and their record with me is poor. I never remove those on my own - your call:"] + yours[:8] + [""]
    send_telegram("\n".join(L).rstrip())


def book_crowd_note() -> str:
    """An honest warning when the finished wallet buys are really a few coins that many saved wallets bought together."""
    per: Counter = Counter()
    wins: Counter = Counter()
    names: dict[str, str] = {}
    for d in list(state["book"].values()):
        for p in d.get("picks", []):
            if p.get("st") != "o":
                per[p["k"]] += 1
                names[p["k"]] = str(p.get("s", "?"))
                if p.get("st") == "w":
                    wins[p["k"]] += 1
    total = sum(per.values())
    if total < 10 or total < 1.5 * len(per):
        return ""
    line = (f"   ⚠️ Those {total} finished wallet buys are only {len(per)} different coins (about {total / len(per):.1f} wallets per coin)")
    n_win = sum(wins.values())
    top = wins.most_common(3)
    if n_win >= 10 and sum(n for _, n in top) / n_win >= 0.4:
        line += (f", and {sum(n for _, n in top) / n_win:.0%} of the wins sit on just 3 coins ("
                 + ", ".join("$" + md_escape(names[k]) for k, _ in top) + ")")
    return line + ": wallets that bought the same coin win or lose together, so a crowd counts as one piece of evidence, not many."


def book_report_lines() -> list[str]:
    """The wallet part of the learning report."""
    s = book_stats()
    out: list[str] = []
    if s["wallets"]:
        out.append(f"🧠 Wallet book: *{s['wallets']}* wallets saved · {s['open']} buys followed · {s['finished']} with finished "
                   f"results · {s['proven']} proven · {s['promising']} promising")
    scores = wallet_scores()
    bestw = sorted(((w, d) for w, d in scores.items() if d.get("stars") is not None and d["stars"] >= STAR_GOOD - 1e-9),
                   key=lambda kv: (-kv[1]["stars"], -kv[1]["n"]))[:4]
    if bestw:
        out.append("   Best rated so far (stars out of 3, from buys that doubled before halving; /stars explains): " + ", ".join(
            f"{short(w)} {stars_text(d['stars'])} ({d.get('win', 0)} doubled, {d.get('stop', 0)} halved)" for w, d in bestw))
    try:                                                    # v3.8: how many wallet "wins" were already in the bag, and who stopped counting
        wins_all = [p for d in state["book"].values() for p in d.get("picks", []) if isinstance(p, dict) and p.get("st") == "w"]
        early_n = sum(1 for p in wins_all if is_early(p))
        cold = [w for w in scores if cold_record(w, scores)]
        off = [w for w in (state["tuning"].get("demoted") or {}) if w not in state["watch"] and recently_demoted(w)] \
            if isinstance(state["tuning"].get("demoted"), dict) else []
        if wins_all and early_n:
            out.append(f"   {early_n} of the {len(wins_all)} wallet wins were mostly done before I first saw the buy - they count "
                       f"{EARLY_WIN_WEIGHT:g} of a win each in the stars.")
        if cold:
            out.append(f"   🧊 {len(cold)} wallet{'' if len(cold) == 1 else 's'} with a losing record no longer {'counts' if len(cold) == 1 else 'count'} toward any alert"
                       + (f" ({len(off)} taken off your list; /stars)" if off else " (/stars)"))
    except Exception:
        log.exception("wallet quality report lines failed")
    crowd = book_crowd_note()
    if crowd:
        out.append(crowd)
    sb = closed_records("smartbuy")
    if sb:
        x = summarize(sb)
        out.append(f"🧠 Smart-wallet alerts finished: *{x['n']}*  ✅ {x['w']} wins | 🛑 {x['s']} stopped | ⏱ {x['e']} flat "
                   f"→ win rate *{x['wr']:.0%}*, average best move *{x['avg_best']:.2f}x*")
        strong = [r for r in sb if r.get("strong")]
        weak = [r for r in sb if not r.get("strong")]
        if len(strong) >= 2 and len(weak) >= 2:
            a, c = summarize(strong), summarize(weak)
            out.append(f"   Proven/watched wallet involved: {a['w']}/{a['n']} won | two promising wallets: {c['w']}/{c['n']}")
        slow = [r for r in sb if r.get("slow")]
        if slow:      # v3.4: the "wallets keep adding over days" messages, on their own
            z = summarize(slow)
            out.append(f"   Of those, 'wallets keep adding' messages: {z['w']}/{z['n']} won, average best move {z['avg_best']:.2f}x")
    return out


# ----------------------------------------------------------------------------
# v2.9 - READING EVERY SCANNED COIN'S CHART: cup & handle, bottom base, double bottom, and my own three bottom signs.
# The chart reader (above) is plain maths on GeckoTerminal's candles. Here it is wired into the scan: a few charts are
# read every minute (most interesting first, each coin again every few hours), a coin whose chart shows a setup AND has
# something else going for it (bottom signs, wallets accumulating, a wallet with a good record) gets an alert, and every
# setup is followed so the report can say which shapes actually paid.
# ----------------------------------------------------------------------------
def gecko_backlog() -> float:
    """Seconds until GeckoTerminal's shared connection is free again (0 = free right now)."""
    with _gecko_lock:
        return max(0.0, _gecko_next - time.time())


def chart_eligible(chain: str, pair: dict, mc: float) -> bool:
    """Is this coin big, liquid and old enough for its chart to be worth reading?"""
    if not CHARTS or not (GECKO_NETWORK.get(chain) and pair.get("pairAddress")):
        return False
    created = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3_600_000 if created else 0.0
    return (CHART_MIN_MC <= mc <= MAX_COIN_MC and num(pair, "liquidity", "usd") >= CHART_MIN_LIQ_USD
            and age_h >= CHART_MIN_AGE_HOURS)


def chart_candles(chain: str, pair: dict):
    """The coin's candles from GeckoTerminal (one request): 4-hour candles for about 30 days, or hourly ones for a
    pool younger than 8 days. -> (candles, hours per candle)"""
    net, pool = GECKO_NETWORK.get(chain), pair.get("pairAddress")
    if not (net and pool):
        raise ValueError("no pool to read")
    created = num(pair, "pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3_600_000 if created else 999.0
    if age_h < 8 * 24:
        agg, step_h, limit = 1, 1.0, int(min(200, max(48, age_h + 2)))
    else:
        agg, step_h, limit = 4, 4.0, 180
    r = gecko_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{pool}/ohlcv/hour",
                  params={"aggregate": agg, "limit": limit, "currency": "usd"}, timeout=20)
    raw = r.json()["data"]["attributes"]["ohlcv_list"]
    return chart_clean(raw, int(step_h * 3600)), step_h


def chart_priority(key: str, m: dict | None, c: dict | None, now: float) -> float:
    """Which due coin's chart is read first: coins Sparsity lists, coins with a setup or bottom signs, deep dips,
    and any coin never read before."""
    pr = 0.0
    b = state["bottoms"].get(key)
    if b and bottom_active(b):
        pr += 40 + 5 * int(b.get("signs") or 0)
    if c and c.get("t"):
        pr += min(30.0, c.get("score", 0) / 3) + (10 if c.get("signs", 0) >= 2 else 0)
        pr += min(12.0, (now - c["t"]) / 7200)           # the longer since the last reading, the sooner
    else:
        pr += 25                                         # never read
    if m:
        pr += min(15.0, m["retrace"] / 6) + (5 if m["vol_ratio"] >= 0.2 else 0)
    cap = cap_of(key)
    if cap and cap.get("acc") and now - cap.get("t", 0) < 6 * 3600 and not (c and c.get("t", 0) > cap.get("t", 0)):
        scores = wallet_scores()            # wallets were just seen accumulating it: read its chart next, so one alert can say it all
        if any(r and book_grade(r[0], scores) in ("watched", "proven", "promising") for r in cap["acc"]):
            pr += 60
    return pr


def read_chart(key: str, chain: str, pair: dict, m: dict | None, now: float) -> bool:
    """Read one coin's chart and keep a small summary of it. Returns False when GeckoTerminal is busy (stop for now)."""
    c = state["charts"].setdefault(key, {})
    c.update(sym=str(pair.get("baseToken", {}).get("symbol", "?"))[:14], chain=chain, seen=now)
    try:
        candles, step_h = chart_candles(chain, pair)
    except requests.RequestException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status in (None, 429, 500, 502, 503, 504):
            c["fail_t"] = now - 1500           # busy, not the coin's fault: try this coin again in about five minutes
            bot_stats["gecko_busy"] += 1
            log.warning("could not read the chart of %s (busy: %s)", key, status or "no answer")
            return False
        log.warning("no chart data for %s (%s)", key, status)
        c.update(t=now, ok=0, pat="", stage="", score=0, signs=0, fail_t=now)
        return True
    except (KeyError, ValueError, TypeError, AttributeError, IndexError) as e:
        log.warning("chart data for %s was not readable: %s", key, e)
        c.update(t=now, ok=0, pat="", stage="", score=0, signs=0, fail_t=now)
        return True
    px_d = num(pair, "priceUsd")
    dd_known = (m["retrace"] / 100) if (m and key in state["backfilled"]) else None
    ch = analyze_chart(candles, step_h, dd_known=dd_known, liq=num(pair, "liquidity", "usd"))
    if ch and px_d > 0 and not (0.5 <= px_d / ch["px"] <= 2.0):
        ch = None            # the chart and the live price disagree (another pool, or bad data): do not trust the reading
    c.pop("fail_t", None)
    if not ch:
        c.update(t=now, ok=0, pat="", stage="", score=0, signs=0)
        return True
    det = ch.get("detail") or {}
    c.update(t=now, ok=1, step=ch["step_h"], n=ch["n"], signs=ch["signs"], dd=round(ch["dd"], 3),
             since_h=round(ch["since_h"]), up=round(ch["above_low"], 3), pat=ch["pat"], stage=ch["stage"],
             score=ch["score"], lvl=float(f"{ch['level']:.6g}"), stp=float(f"{ch['stop']:.6g}"), px=float(f"{ch['px']:.6g}"),
             k=(m["mc"] / px_d) if (m and px_d > 0) else 0.0, notes=[str(x)[:70] for x in ch["notes"][:4]],
             d={k: float(f"{v:.5g}") for k, v in det.items()
                if k in ("rim", "bot", "rec", "depth", "days", "range", "dd_pre", "gap_days", "dry", "calm", "tests")
                and isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)})
    bot_stats["charts"] += 1
    return True


def chart_pass(cands: list, now: float):
    """After the scan: read the most worth-reading due charts (CHART_PER_SCAN), then look for chart-setup alerts."""
    if not (CHARTS and cands):
        return
    todo = []
    for key, chain, pair, mc in cands:
        c = state["charts"].get(key)
        if c:
            c["seen"] = now
        if c and ((c.get("fail_t") and now - c["fail_t"] < 1800) or now - c.get("t", 0) < CHART_REFRESH_HOURS * 3600):
            continue
        if key in state["steady"] or not (chart_eligible(chain, pair, mc) or bottom_entry(key)):
            continue
        m = compute_metrics(key, pair)
        todo.append((chart_priority(key, m, c, now), key, chain, pair, m))
    todo.sort(key=lambda x: -x[0])
    reads = 0
    for _, key, chain, pair, m in todo:
        if reads >= CHART_PER_SCAN or gecko_backlog() > 15:
            break
        reads += 1
        if not read_chart(key, chain, pair, m, now):
            break
    for key, chain, pair, mc in cands:
        c = state["charts"].get(key)
        if c and c.get("pat") and c.get("score", 0) >= CHART_MIN_SCORE - 5 and now - c.get("t", 0) < 12 * 3600:
            try:
                maybe_chart_alert(key, chain, pair, c, now)
            except Exception:
                log.exception("chart alert error")


def cap_of(key: str) -> dict | None:
    """The latest look at who is buying this coin (from the bottom list or from the ordinary scan)."""
    best = None
    for src in (state["bottoms"].get(key), state["charts"].get(key)):
        cap = (src or {}).get("cap")
        if cap and (best is None or cap.get("t", 0) > best.get("t", 0)):
            best = cap
    return best


def chart_support(key: str, c: dict) -> tuple[float, list[str]]:
    """What else is going for a chart setup: Sparsity's bottom signs, my own, wallets accumulating, a wallet with a record."""
    bonus, why = 0.0, []
    b = state["bottoms"].get(key)
    if b and bottom_active(b) and isinstance(b.get("signs"), int) and b["signs"] >= 2:
        bonus += 10 if b["signs"] >= 3 else 5
        why.append(f"Sparsity shows {b['signs']} of 3 bottom signs")
    if c.get("signs", 0) >= 2:
        bonus += 5
        why.append(f"my own reading finds {c['signs']} of 3 bottom signs")
    cap = cap_of(key)
    if cap and time.time() - cap.get("t", 0) < 48 * 3600:
        acc = [r for r in (cap.get("acc") or []) if r]
        if len(acc) >= 2 and cap.get("nb", 0) > cap.get("ns", 0):
            bonus += 5
            why.append(f"{len(acc)} wallets are accumulating it")
        scores = wallet_scores()
        trusted = [r[0] for r in acc if trusted_wallet(r[0], scores)]
        if trusted:
            bonus += 15
            why.append(f"{len(trusted)} wallet(s) I trust (on your list, or with a good record) are accumulating it")
    return bonus, why


def lvl_text(c: dict, price: float) -> str:
    """A price level as a market cap (the way you read charts)."""
    return fmt_usd(price * c["k"]) if c.get("k") and price else "?"


def shape_line(c: dict) -> str:
    d, pat = c.get("d") or {}, c.get("pat")
    if pat in ("cup", "cup_handle") and d.get("rim") and d.get("bot"):
        return (f"fell {d.get('depth', 0):.0%} from about {lvl_text(c, d['rim'])} to a low near {lvl_text(c, d['bot'])}, "
                f"now back up {d.get('rec', 0):.0%} of the way, over {d.get('days', 0):.1f} days")
    if pat == "base":
        return (f"{d.get('days', 0):.1f} days of quiet trading in a {d.get('range', 0):.0%} range after a "
                f"{d.get('dd_pre', 0):.0%} fall" + (", volume drying up" if d.get("dry", 1) <= 0.8 else ""))
    if pat == "double_bottom":
        return f"two lows about {d.get('gap_days', 0):.0f} days apart, within a few % of each other"
    return ""


def chart_lines(key: str, c: dict | None, m: dict | None = None, price: float = 0.0) -> str:
    """The chart part of an alert or a /check: the shape, how far it has got, and my own three bottom signs."""
    if not c or not c.get("ok"):
        return ""
    c = dict(c)
    if m and price > 0:
        c["k"] = m["mc"] / price                     # market cap per $1 of price, from right now
    signs = int(c.get("signs", 0))
    marks = " ".join(f"{'✅' if i < signs else '▫️'}{step}" for i, step in enumerate(SPARSITY_STEPS))
    L = []
    if c.get("pat"):
        L.append(f"📈 Chart: *{PATTERN_LABEL.get(c['pat'], c['pat'])}* · {md_escape(c.get('stage', ''))} · shape score *{c['score']}/100*")
        sl = shape_line(c)
        if sl:
            L.append("   " + md_escape(sl))
    else:
        L.append("📈 Chart: no cup, base or double-bottom shape right now")
    L.append(f"🧭 My own bottom signs: *{signs} of 3*  {marks}")
    if c.get("pat") and c.get("lvl") and c.get("k"):
        L.append(f"🎯 A move above *{lvl_text(c, c['lvl'])}* would be the breakout; the shape is wrong under {lvl_text(c, c.get('stp'))}")
    return "\n".join(L) + "\n"


def build_chart_message(chain: str, pair: dict, m: dict, c: dict, why: list[str], held: dict) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(base.get("symbol", "?"))
    key = sk(chain, addr)
    body = (f"*📈 CHART SETUP: ${sym}*  |  {CHAINS[chain]['name']}\n\n"
            + chart_lines(key, c, m, num(pair, "priceUsd")))
    b = state["bottoms"].get(key)
    if b and bottom_active(b) and isinstance(b.get("signs"), int):
        body += f"🧭 Sparsity's page shows {b['signs']} of 3 for it\n"
    body += (f"💰 Market cap *{fmt_usd(m['mc'])}* · 💧 liquidity {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC)\n"
             f"📊 24h volume {fmt_usd(m['vol24'])} ({m['vol_ratio'] * 100:.0f}% of MC) · 👥 {m['traders']:,} txns · "
             f"⏱ pair age {fmt_age(m['age_h'])}\n")
    if why:
        body += "✅ Why it made the list: " + md_escape("; ".join(why)) + "\n"
    cap = cap_of(key)
    if cap and cap.get("acc"):
        body += buyers_lines({"cap": cap}, key=key)
    body += wallets_line(held)
    if chain == "robinhood":
        body += ("⚠️ Robinhood Chain is brand new: many of its wallets are airdrop-farming bots, so wallet signals are "
                 "weaker here, and DexScreener's numbers can jump around.\n")
    body += (f"📌 Tracking this for {follow_text(CHART_TRACK_HOURS, m['mc'])} (win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x). "
             "A chart shape is a hunch to test, not a promise - the report will show which shapes really paid.\n\n"
             f"`{addr}`\n\n"
             f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', addr)})")
    return body


def maybe_add_chart_shadow(key: str, chain: str, pair: dict, m: dict, c: dict, bonus: float, now: float):
    """Follow a chart setup quietly when it did not earn an alert, so the report can compare it with the alerts."""
    if following_coin(key) or now - state["shadow_cd"].get("c:" + key, 0) < 3 * 86400:
        return
    if len(fresh_open("cshadow")) >= MAX_CHART_SHADOWS:
        return
    if not clear_to_alert(key, chain, pair):
        return                        # v3.1: a steady asset (or not checked yet)
    state["shadow_cd"]["c:" + key] = now
    b = state["bottoms"].get(key)
    add_record("cshadow", key, chain, pair, m, "", now,
               extra={"pat": c["pat"], "score": c["score"], "signs": c.get("signs", 0), "bonus": bonus,
                      "th": CHART_TRACK_HOURS,
                      "sp": (b.get("signs") if b and bottom_active(b) else None)})


def alerted_just_now(key: str, now: float, hours: float = 12.0) -> bool:
    """Did this coin get a bottom or smart-wallet alert in the last few hours? Those messages already show the chart reading."""
    b = state["bottoms"].get(key)
    return bool((b and now - b.get("alerted", 0) < hours * 3600) or now - state["cooldowns"].get("sb:" + key, 0) < hours * 3600)


def chart_alert_coming(key: str, now: float) -> bool:
    """Is a chart-setup alert about to go out for this coin? Then that alert already shows who is buying."""
    c = state["charts"].get(key)
    if not (CHARTS and alert_on("chart") and c and c.get("ok") and c.get("pat") and c.get("score", 0) >= CHART_MIN_SCORE
            and now - c.get("t", 0) < 12 * 3600):
        return False
    if now - state["cooldowns"].get("chart:" + key, 0) < CHART_COOLDOWN_DAYS * 86400 or any(r["kind"] == "bottom" for r in open_records(key)) \
            or alerted_just_now(key, now):
        return False
    bonus, _ = chart_support(key, c)
    if not (bonus >= 5 and c["score"] + bonus >= CHART_ALERT_RANK):
        return False
    return len([t for t in state["tuning"].get("chart_ts", []) if now - t < 86400]) < CHART_MAX_PER_DAY


def maybe_chart_alert(key: str, chain: str, pair: dict, c: dict, now: float) -> bool:
    """A coin whose chart shows a setup, with something else going for it, gets an alert."""
    if not c.get("pat") or c.get("score", 0) < CHART_MIN_SCORE - 5 or num(pair, "liquidity", "usd") < CHART_MIN_LIQ_USD:
        return False
    if bottom_alert_coming(key, now):
        return False          # a bottom alert is on its way: it shows the chart reading too (one message, not two)
    m = compute_metrics(key, pair)
    if not m or m["mc"] < CHART_MIN_MC:
        return False
    bonus, why = chart_support(key, c)
    ok = c["score"] >= CHART_MIN_SCORE and bonus >= 5 and c["score"] + bonus >= CHART_ALERT_RANK
    cool = now - state["cooldowns"].get("chart:" + key, 0) < CHART_COOLDOWN_DAYS * 86400
    b = state["bottoms"].get(key)
    if ok and (any(r["kind"] == "bottom" for r in open_records(key)) or alerted_just_now(key, now)):
        ok = False        # a bottom alert on this coin is still being followed: it already shows the chart reading
    stamps = [t for t in state["tuning"].get("chart_ts", []) if now - t < 86400]
    state["tuning"]["chart_ts"] = stamps
    if ok and alert_on("chart") and not cool and len(stamps) < CHART_MAX_PER_DAY:
        if not confirmed_twice("chart", key, m["mc"], now):
            return False
        if not clear_to_alert(key, chain, pair):
            return False                          # v3.1: a steady asset (or not checked yet)
        held = watch_holders(chain, key.split(":", 1)[1], num(pair, "priceUsd"), max_age=1800)
        if not send_telegram(build_chart_message(chain, pair, m, c, why, held)):
            return False
        state["cooldowns"]["chart:" + key] = now
        stamps.append(now)
        for r in open_records(key):
            if r["kind"] == "cshadow":
                r["status"], r["t1"], r["void"] = "expired", now, True
        cap = cap_of(key) or {}
        scores = wallet_scores()
        add_record("chart", key, chain, pair, m, "", now,
                   extra={"pat": c["pat"], "score": c["score"], "signs": c.get("signs", 0), "bonus": bonus,
                          "th": CHART_TRACK_HOURS, "sig": signals_for(m, len(held["in"])),
                          "acc": acc_count(cap),
                          "smart": sum(1 for r in (cap.get("acc") or []) if r and trusted_wallet(r[0], scores)),
                          "sp": (b.get("signs") if b and bottom_active(b) else None)})
        bot_stats["chart"] += 1
        log.info("CHART alert for %s (%s, score %s, +%s support)", key, c["pat"], c["score"], bonus)
        return True
    if not cool and c["score"] >= CHART_MIN_SCORE:
        maybe_add_chart_shadow(key, chain, pair, m, c, bonus, now)
    return False


def cmd_patterns() -> str:
    """/patterns - the coins whose charts show a setup right now."""
    if not CHARTS:
        return "Chart reading is switched off (`CHARTS=false`)."
    now = time.time()
    fresh = [(k, c) for k, c in list(state["charts"].items()) if c.get("ok") and now - c.get("t", 0) < CHART_KEEP_HOURS * 3600]
    if not fresh:
        return ("I haven't read any charts yet. I read about one a minute (the most interesting coins first, each coin "
                "again every few hours), so the first results appear within the hour.")
    shapes = sorted(((k, c) for k, c in fresh if c.get("pat")), key=lambda kc: -(kc[1].get("score", 0) + 3 * kc[1].get("signs", 0)))
    L = [f"📈 *Chart shapes* - I have read {len(fresh)} charts in the last {CHART_KEEP_HOURS // 24} days; "
         f"{len(shapes)} show a setup (cup, cup & handle, bottom base or double bottom)."]
    if not shapes:
        L.append("None right now. A shape needs a deep fall first, then a base or a rounded recovery.")
    for i, (k, c) in enumerate(shapes[:7], 1):
        chain, ca = c.get("chain", k.split(":", 1)[0]), k.split(":", 1)[1]
        cap = cap_of(k)
        bits = [f"${md_escape(c.get('sym', '?'))}", CHAINS.get(chain, {"name": chain})["name"],
                f"*{PATTERN_LABEL.get(c['pat'], c['pat'])}* ({md_escape(c.get('stage', ''))})", f"{c['score']}/100",
                f"own signs {c.get('signs', 0)}/3"]
        b = state["bottoms"].get(k)
        if b and bottom_active(b) and isinstance(b.get("signs"), int):
            bits.append(f"Sparsity {b['signs']}/3")
        if cap and cap.get("acc") and now - cap.get("t", 0) < 72 * 3600:
            bits.append(f"{acc_count(cap)} wallet(s) accumulating")
        sl = shape_line(c)
        L.append(f"{i}. " + " · ".join(bits) + (f"\n    {md_escape(sl)}" if sl else "")
                 + f"\n    `{ca}`  [chart](https://dexscreener.com/{chain}/{ca})")
    L.append("\nA shape is a hunch to test, not a promise: /report shows which shapes have actually paid.")
    return "\n".join(L)


def chart_report_lines() -> list[str]:
    """Which chart shapes paid? Won/followed for the alerts and for the setups followed quietly."""
    ch, sh = closed_records("chart"), closed_records("cshadow")
    out: list[str] = []
    if ch:
        s = summarize(ch)
        out.append(f"📈 Chart-setup alerts finished: *{s['n']}*  ✅ {s['w']} wins | 🛑 {s['s']} stopped | ⏱ {s['e']} flat "
                   f"→ win rate *{s['wr']:.0%}*, average best move *{s['avg_best']:.2f}x*")
    allr = ch + sh
    parts = []
    for pat, lab in PATTERN_LABEL.items():
        rs = [r for r in allr if r.get("pat") == pat]
        if rs:
            x = summarize(rs)
            parts.append(f"{lab} {x['w']}/{x['n']}")
    if parts:
        out.append("   By shape (won/followed, alerts and quiet follow-ups): " + " | ".join(parts))
    backed = [r for r in allr if r.get("acc", 0) >= 2 or r.get("smart")]
    plain = [r for r in allr if not (r.get("acc", 0) >= 2 or r.get("smart"))]
    if len(backed) >= 2 and len(plain) >= 2:
        a, b = summarize(backed), summarize(plain)
        out.append(f"   With wallets accumulating: {a['w']}/{a['n']} won | without: {b['w']}/{b['n']}")
    own = [r for r in allr if r.get("signs", 0) >= 2]
    rest = [r for r in allr if r.get("signs", 0) < 2]
    if len(own) >= 2 and len(rest) >= 2:
        a, b = summarize(own), summarize(rest)
        out.append(f"   With 2+ of my own bottom signs: {a['w']}/{a['n']} won | fewer: {b['w']}/{b['n']}")
    if sh and not ch:
        x = summarize(sh)
        out.append(f"📈 Chart setups followed quietly: {x['w']}/{x['n']} won")
    return out


def capture_pass(cands: list, now: float, caps: list):
    """Spend this scan's allowance on looking at who is buying: the coins on Sparsity's map first (3 of 3 down to 0 of 3),
    then ordinary coins with a chart setup, then the rest by how busy they are."""
    if not BUYER_CAPTURE:
        return
    todo = []
    for key, chain, pair, mc in cands:
        if key in state["steady"]:
            continue
        b = bottom_entry(key)
        if b:
            if num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD or not capture_due(b, now, bottom_cap_hours(b)):
                continue
            m = bottom_metrics(key, pair, b)
            if m:
                age = (now - (b.get("cap") or {}).get("t", 0)) / 86400
                todo.append((1000 + 10 * int(b.get("signs") or 0) + min(9.0, age), key, chain, pair, m, b))
            continue
        if GEN_CAPTURE_HOURS <= 0 or not chart_eligible(chain, pair, mc):
            continue
        c = state["charts"].setdefault(key, {"t": 0, "ok": None, "pat": "", "score": 0, "signs": 0,
                                             "sym": str(pair.get("baseToken", {}).get("symbol", "?"))[:14], "chain": chain})
        c["seen"] = now
        hot = c.get("score", 0) >= CHART_MIN_SCORE - 5 or c.get("signs", 0) >= 2
        low_in = bool(_book_idx.get(key)) and 0 < mc <= lowcap_limit()    # v3.5: a low-cap coin that saved wallets are already in: who joins them is looked at more often
        hours = LOWCAP_RECAPTURE_HOURS if low_in else (SETUP_CAPTURE_HOURS if hot else GEN_CAPTURE_HOURS)
        if not capture_due(c, now, hours) or num(pair, "liquidity", "usd") < BOTTOM_MIN_LIQ_USD:
            continue
        m = compute_metrics(key, pair)
        if not m or m["traders"] < 100:                      # too few trades in a day to learn anything from
            continue
        young = 40 if 0 < m["age_h"] <= 14 * 24 else 15 if 0 < m["age_h"] <= 30 * 24 else 0      # v3.8: young coins pay best in my data: their buyers are looked at first
        todo.append(((800 if low_in else 500 if hot else 0) + c.get("score", 0) + 10 * c.get("signs", 0) + min(20.0, m["vol_ratio"] * 10) + young,
                     key, chain, pair, m, c))
    todo.sort(key=lambda x: -x[0])
    for _, key, chain, pair, m, holder in todo:
        if caps[0] <= 0 or gecko_backlog() > 15:
            break
        caps[0] -= 1
        capture_buyers(key, chain, pair, m, holder, now)
    smart_buy_pass(cands, now)


# --- chat commands for all of this ---
def resolve_coin_ref(ref: str):
    """'<address>' or 'base:0x...' -> (chain, address, None), or (None, None, a message explaining the problem)."""
    ref = ref.strip()
    if ref.lower().startswith("dex:") and ref.count(":") >= 2:       # a DexScreener link: its last part is a trading pair, not the coin
        _, ch, pid = ref.split(":", 2)
        chain0 = norm_chain(ch)
        if chain0:
            ref = f"{chain0}:{pair_to_token(chain0, pid) or pid}"
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
        return "The bottomed-coin list is switched off (`SPARSITY_FEED=false`)."
    now = time.time()
    items = [(k, b) for k, b in list(state["bottoms"].items()) if bottom_active(b)]
    if not items:
        if _sp["fails"]:
            return f"I can't read Sparsity's bottom list right now: {_sp['err']}. I keep trying."
        return "No coin on Sparsity's list is being followed yet (or I haven't read the page yet)."
    groups: dict[int, list] = {3: [], 2: [], 1: [], 0: []}
    for k, b in items:
        groups[min(3, max(0, int(b.get("signs") or 0)))].append((k, b))
    for g in groups.values():
        g.sort(key=lambda kb: (-(kb[1].get("mc") or 0), kb[1].get("name", "")))
    L = [f"🧭 *Coins on Sparsity's bottom map*  ({len(items)} followed; page data {md_escape(_sp['updated'] or '?')}, "
         f"read {ago(now - _sp['last_ok'])} ago)" if _sp["last_ok"] else f"🧭 *Coins on Sparsity's bottom map* ({len(items)} followed)"]
    scores = wallet_scores()
    titles = {3: "*All 3 signs* - sellers capitulated, the low is holding, demand returning (I alert on these)",
              2: "*2 of 3* - the low is holding, demand not back yet (I alert only when a wallet with a record, 2+ buyers or a chart shape backs it)",
              1: "*1 of 3* - sellers capitulated, the low is not proven yet (followed quietly)",
              0: "*0 of 3* - on the map, no sign yet (followed quietly)"}
    for s in (3, 2, 1, 0):
        g = groups[s]
        if not g:
            continue
        L.append(f"\n{titles[s]}: {len(g)}")
        if s >= 2:
            for i, (k, b) in enumerate(g[:7], 1):
                chain, ca = b["chain"], b["ca"]
                bits = [f"${md_escape(b.get('name', '?'))}", CHAINS[chain]["name"]]
                if b.get("mc"):
                    bits.append("MC " + fmt_usd(b["mc"]))
                if b.get("above_low") is not None and b.get("low"):
                    bits.append(f"{b['above_low']:.0f}% above its low")
                cap = b.get("cap") or {}
                acc = [r for r in cap.get("acc", []) if r]
                if acc:
                    top = any(trusted_wallet(r[0], scores) for r in acc)
                    bits.append(f"{len(acc)} wallet(s) accumulating" + (" (incl. one I trust 👀)" if top else ""))
                c = state["charts"].get(k) or {}
                if c.get("ok") and c.get("pat"):
                    bits.append(f"chart: {PATTERN_LABEL.get(c['pat'], c['pat'])} {c.get('score', 0)}")
                mark = " ✅alerted" if b.get("alerted") else ""
                L.append(f"{i}. " + " · ".join(bits) + mark + f"\n    `{ca}`  [chart](https://dexscreener.com/{chain}/{ca})")
            if len(g) > 7:
                L.append("   …and " + ", ".join("$" + md_escape(b.get("name", "?")) for _, b in g[7:16]))
        else:
            L.append("   " + ", ".join("$" + md_escape(b.get("name", "?")) for _, b in g[:25]) + (" …" if len(g) > 25 else ""))
    L.append("\nA base forming is a shortlist, not a buy signal. /buyers <address> shows who is buying a coin; "
             "/report shows whether 2, 1 and 0 of 3 have paid.")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def cmd_smart() -> str:
    scores = wallet_scores()
    s = book_stats()
    auto = auto_wallets()
    L = ["🧠 *Smart-money tracker*", "",
         f"Every wallet I see accumulating a coin (real buys that add up to ${BUYER_MIN_USD:,.0f}+ and little selling, bots set "
         f"aside) is saved in the background. I follow each buy from the price that wallet paid for {BOOK_TRACK_HOURS / 24:g} days "
         f"(win = {WIN_MULTIPLE:g}x before {STOP_MULTIPLE:g}x; good = {SMART_GOOD_MULT:g}x or more).",
         f"Right now: *{s['wallets']}* wallets saved · {s['open']} buys being followed · {s['finished']} wallets with finished results · "
         f"*{s['proven']}* proven ({format(STAR_GOOD, 'g')}+ stars) · {s['promising']} promising. What the stars mean: /stars"]
    # wallets whose open buys are up right now
    up = []
    for w, d in list(state["book"].items()):
        best = None
        for p in d.get("picks", []):
            if p.get("st") == "o" and p.get("lr", 1.0) >= 1.25 and (best is None or p["lr"] > best["lr"]):
                best = p
        if best:
            up.append((best["lr"], w, best))
    multi = sorted(((w, [p for p in d.get("picks", []) if p.get("st") == "o"]) for w, d in state["book"].items()),
                   key=lambda wp: -sum(p.get("usd", 0) for p in wp[1]))
    multi = [(w, ps) for w, ps in multi if len(ps) >= 2][:4]
    mcs = live_mcs([p["k"] for _, _, p in sorted(up, reverse=True)[:5]] + [p["k"] for _, ps in multi for p in ps[:4]])   # v3.5: market caps
    if up:
        L.append("\n*Winning right now* (an open buy is up 25%+ from what the wallet paid):")
        for lr, w, p in sorted(up, reverse=True)[:5]:
            L.append(f"• `{w}` ${md_escape(p['s'])} {lr - 1:+.0%}" + (f" · MC {fmt_usd(mcs[p['k']])}" if mcs.get(p["k"]) else "")
                     + f" (bought {fmt_usd(p.get('usd', 0))}) · {wallet_record_text(w, scores)}")
    if auto:
        L.append("\n*Put on your watch list by the bot, from results:*")
        for w, d in auto[:8]:
            sc = scores.get(w)
            cool = " · 🧊 cooling off (its record has slipped; it no longer counts as a good record)" if book_grade(w, scores) == "cooling" else ""
            L.append(f"• *{md_escape(wallet_label(w))}* " + (wallet_stars_line(sc) if sc else f"{d.get('good', '?')}/{d.get('n', '?')} good when added")
                     + cool + f"\n   `{w}`")
    prom = sorted(((w, d) for w, d in scores.items() if book_grade(w, scores) == "promising" and w not in state["watch"]),
                  key=lambda kv: (-(kv[1].get("stars") or 0), -kv[1]["good"], kv[1]["n"]))[:5]
    if prom:
        L.append("\n*Promising* (a good start, not enough history to rate it yet):")
        for w, d in prom:
            L.append(f"• {wallet_stars_line(d)}: " + ", ".join("$" + md_escape(x) for x, _, g in d["coins"][:4] if g) + f"\n   `{w}`")
    if not (up or auto or prom):
        L.append("\nNo wallet has earned a place yet. Results come in over the next days as the buys I'm following play out.")
    if multi:
        L.append("\n*Buying more than one coin right now* (worth a look, not proof):")
        for w, ps in multi:
            L.append(f"• `{w}`{' 👀' if w in state['watch'] else ''}\n   " + ", ".join(
                f"${md_escape(p['s'])} ({fmt_usd(p.get('usd', 0))}" + (f", MC {fmt_usd(mcs[p['k']])}" if mcs.get(p["k"]) else "") + ")"
                for p in ps[:4]))
    L.append("\nWhich coins they are buying right now: /coins. Wallets you add yourself: /watch <address> <name>. "
             "Who is buying one coin: /buyers <address>.")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


def cmd_stars() -> str:
    """/stars (v3.7) - what the 1-3 stars mean, the wallets on your list with their stars, and the best rated wallets that are not on it."""
    scores = wallet_scores()
    L = ["⭐ *Wallet stars (out of 3)*", "",
         f"I follow every buy a wallet makes for {BOOK_TRACK_HOURS / 24:g} days. A buy is a *hit* if the coin doubled before it halved, a *miss* if it "
         "halved first, and *flat* if it did neither (flat buys are shown, but count neither way). The stars come from how many of the clear results "
         "were hits - and winning just 1 time in 3 already breaks even, so the lines sit lower than 'most of the time':",
         "⭐⭐⭐ 3 - about 7 hits in 10 (excellent)",
         "⭐⭐½ 2.5 - about 6 in 10",
         "⭐⭐ 2 - about 5 in 10 (a good record)",
         "⭐½ 1.5 - about 4 in 10 (around break-even)",
         "⭐ 1 - about 2 to 3 in 10 (more misses than hits)",
         "½⭐ 0.5 - mostly misses",
         f"That is for a wallet with about 20 clear results. With fewer, I ask for more proof (3 lucky buys can't earn 3 stars): 2 stars is the most "
         f"before {STAR_CAPS[0][0]} finished buys, 2.5 before {STAR_CAPS[1][0]}. Under {STAR_MIN_BUYS} finished buys it says 'not rated yet'.",
         f"A hit that was mostly done before I first saw the buy (the coin was already up {EARLY_FORWARD_LR:g}x on the wallet's price) shows the wallet "
         f"bought low, but not that following it would have paid - so it counts {EARLY_WIN_WEIGHT:g} of a hit, not a whole one."]
    rows = []
    for w, info in state["watch"].items():
        if isinstance(info, dict):
            rows.append((w, scores.get(w)))
    rows.sort(key=lambda t: (-(t[1]["stars"] if t[1] and t[1].get("stars") is not None else -1), -(t[1]["n"] if t[1] else 0)))
    if rows:
        L.append(f"\n👀 *Wallets on your list* ({len(rows)})")
        for w, sc in rows[:12]:
            cool = (" · 🧊 cooling off" + (" - comes off your list at my next check-up" if cold_record(w, scores) else "")) if book_grade(w, scores) == "cooling" else ""
            if not cool and cold_record(w, scores) and not bot_found(state["watch"].get(w)):
                cool = " · ⚠️ poor record - your call (/unwatch)"
            L.append(f"• {md_escape(wallet_label(w))}: " + (wallet_stars_line(sc) if sc and sc["n"] else "no finished buys yet") + cool)
        if len(rows) > 12:
            L.append(f"   ...and {len(rows) - 12} more (/wallets lists them all)")
        if any(book_grade(w, scores) == "cooling" for w, _ in rows):
            L.append("🧊 = a wallet I added to your list myself whose results have slipped, so it no longer counts as a good record. If it keeps "
                     f"losing (half a star after {MUTE_MIN_BUYS}+ finished buys) it comes off your list at my next check-up (every 6 hours) and no longer counts "
                     f"toward any alert. I still follow what it buys, and it can earn its place back after {DEMOTE_DAYS:g} days. "
                     "A wallet you added yourself is never removed by me - I only warn you.")
    dem = state["tuning"].get("demoted")
    off = [w for w in (dem if isinstance(dem, dict) else {}) if w not in state["watch"] and recently_demoted(w)]
    if off:
        L.append(f"\n🧊 *Taken off your list for losing* ({len(off)}) - still followed in the background:")
        for w in off[:6]:
            L.append(f"• `{short(w)}`: " + (wallet_stars_line(scores[w]) if scores.get(w) and scores[w]["n"] else "no finished buys"))
    best = sorted(((w, d) for w, d in scores.items() if w not in state["watch"] and d.get("stars") is not None and d["stars"] >= STAR_GOOD - 1e-9),
                  key=lambda kv: (-kv[1]["stars"], -kv[1]["n"]))[:5]
    if best:
        L.append("\n🔎 *Best rated wallets that are not on your list* (follow one with /watch <address> <name>)")
        for w, d in best:
            L.append(f"• `{w}`\n   {wallet_stars_line(d)}")
    else:
        L.append(f"\n🔎 No wallet outside your list has earned {STAR_GOOD:g} stars yet. Ratings fill in as the buys I follow finish (7 days each).")
    dist = Counter(d["stars"] for d in scores.values() if d.get("stars") is not None)
    unrated = sum(1 for d in scores.values() if d.get("stars") is None and d["n"])
    if dist or unrated:
        L.append("\n📊 Wallets by stars: " + " · ".join(f"{k:g}: {dist[k]}" for k in (3.0, 2.5, 2.0, 1.5, 1.0, 0.5) if dist.get(k))
                 + (f" · not rated yet: {unrated}" if unrated else ""))
    L.append("\nThe stars describe the last few weeks in this market. A wallet that shone when everything was pumping can look poor in a cold "
             "market, so treat them as a rough guide, not a promise.")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


# --- v3.0: which coins are the saved wallets buying? ---
def book_coins_now(hours: float = 72.0, limit: int = 8) -> list[dict]:
    """The coins the wallets in the wallet book are buying right now: one row per coin, with the wallets whose buy of it
    (from the last `hours`) is still being followed. Coins with the most wallets that have a record come first."""
    now = time.time()
    scores = wallet_scores()
    coins: dict[str, dict] = {}
    for w, d in list(state["book"].items()):
        for p in (d.get("picks") or []) if isinstance(d, dict) else []:
            if not isinstance(p, dict) or p.get("st") != "o" or not isinstance(p.get("k"), str) or ":" not in p["k"] \
                    or now - num(p, "t") > hours * 3600:
                continue
            c = coins.setdefault(p["k"], {"key": p["k"], "sym": p.get("s") or "?", "chain": p.get("c") or "solana", "wallets": [],
                                          "usd": 0.0, "lr": [], "t": 0.0, "strong": 0, "soft": 0, "cool": 0, "weak": 0})
            c["wallets"].append(w)
            c["usd"] += num(p, "usd")
            c["lr"].append(num(p, "lr", default=1.0) or 1.0)
            c["t"] = max(c["t"], num(p, "t"))
            g = book_grade(w, scores)
            if g in ("watched", "proven"):
                c["strong"] += 1
            elif g == "promising":
                c["soft"] += 1
            elif g == "cooling":
                c["cool"] += 1
            else:
                st = wallet_stars(w, scores)           # v3.8.1: rated and under STAR_COOL_BELOW stars = a weak record
                if st is not None and st < STAR_COOL_BELOW - 1e-9:
                    c["weak"] += 1
    rows = sorted(coins.values(), key=lambda c: (-(3 * c["strong"] + 2 * c["soft"] + len(c["wallets"])), -c["usd"]))
    return rows[:limit]


def book_paid_off(days: float = 7.0, limit: int = 6) -> list[dict]:
    """Coins that rose SMART_GOOD_MULT x or more from the price a saved wallet paid, within the last `days` days."""
    now = time.time()
    coins: dict[str, dict] = {}
    for w, d in list(state["book"].items()):
        for p in (d.get("picks") or []) if isinstance(d, dict) else []:
            if not isinstance(p, dict) or not isinstance(p.get("k"), str) or ":" not in p["k"] \
                    or now - num(p, "t") > days * 86400 or num(p, "mx", default=1.0) < SMART_GOOD_MULT:
                continue
            c = coins.setdefault(p["k"], {"key": p["k"], "sym": p.get("s") or "?", "chain": p.get("c") or "solana",
                                          "n": 0, "mx": 0.0, "won": 0})
            c["n"] += 1
            c["mx"] = max(c["mx"], num(p, "mx", default=1.0))
            c["won"] += 1 if p.get("st") == "w" else 0
    return sorted(coins.values(), key=lambda c: (-c["mx"], -c["n"]))[:limit]


def grade_bits(c: dict) -> str:
    """v3.8.1: who the buyers of a coin are, in words: '🏅🏅 2 trusted · 🧊 1 cooling · 3 weak record · 2 not rated'. (Before, a medal was
    all it said - and a medal next to '8 wallets' read like 'smart wallets', although the other wallets were unrated or weak.)"""
    n = len(c["wallets"])
    strong, soft, cool, weak = c["strong"], c["soft"], c.get("cool", 0), c.get("weak", 0)
    parts = []
    if strong:
        parts.append("🏅" * min(strong, 3) + f" {strong} trusted")
    if soft:
        parts.append("🌱" * min(soft, 3) + f" {soft} promising")
    if cool:
        parts.append(f"🧊 {cool} cooling")
    if weak:
        parts.append(f"{weak} weak record")
    rest = n - strong - soft - cool - weak
    if rest > 0:
        parts.append("none rated yet" if rest == n else f"{rest} not rated")
    return " · ".join(parts)


def coin_row(i: int, c: dict, now: float, mcs: dict | None = None) -> str:
    chain, ca = c["chain"], c["key"].split(":", 1)[1]
    n = len(c["wallets"])
    gb = grade_bits(c)
    mc = (mcs or {}).get(c["key"]) or mc_cached(c["key"])        # v3.5: the coin's market cap, right after its name
    bits = [f"${md_escape(str(c['sym']))}", CHAINS.get(chain, {"name": chain})["name"]]
    if mc:
        bits.append(f"*MC {fmt_usd(mc)}*")
    bits += [f"{n} wallet{'s' if n != 1 else ''} bought {fmt_usd(c['usd'])}" + (f" ({gb})" if gb else ""),
             f"{median(c['lr']) - 1:+.0%} since their buys", f"last buy {ago(now - c['t'])} ago"]
    return f"{i}. " + " · ".join(bits) + f"\n    `{ca}`  [chart](https://dexscreener.com/{chain}/{ca})"


GRADE_LEGEND = ("🏅 trusted (on your list, or a good record) · 🌱 promising start · 🧊 cooling (was trusted, results turned bad) · "
                "weak record = under 1.5 stars. Every saved wallet counts here, rated or not.")


def smart_coin_lines(limit: int = 3, fetch: bool = True) -> list[str]:
    """A short 'what are the saved wallets buying' block for the status check-in."""
    try:
        rows = book_coins_now(limit=limit)
        if not rows:
            return []
        now = time.time()
        mcs = live_mcs([c["key"] for c in rows], fetch=fetch)
        return (["", "🧠 *Coins the saved wallets are buying right now* (/coins shows more):", GRADE_LEGEND]
                + [coin_row(i, c, now, mcs) for i, c in enumerate(rows, 1)])
    except Exception:
        log.exception("could not list the coins the saved wallets are buying")
        return []


def cmd_coins() -> str:
    """/coins - which coins the wallets in the wallet book are buying right now, and which ones paid off lately."""
    s = book_stats()
    if not s["wallets"]:
        return ("The wallet book is still empty - it fills up as I look at who is buying each coin (about one coin a minute). "
                "Check back in a few hours.")
    now = time.time()
    L = [f"🪙 *Coins the saved wallets are buying right now*  ({s['wallets']} wallets saved · {s['proven']} proven · "
         f"{s['promising']} promising)"]
    rows = book_coins_now()
    paid = book_paid_off()
    mcs = live_mcs([c["key"] for c in rows] + [c["key"] for c in paid])      # v3.5: market cap next to every coin
    if rows:
        L.append("Open buys from the last 3 days. " + GRADE_LEGEND + " (/stars explains). "
                 f"MC = the coin's market cap now (a low cap is {fmt_usd(lowcap_limit())} or less).")
        L += [coin_row(i, c, now, mcs) for i, c in enumerate(rows, 1)]
    else:
        L.append("None of the saved wallets has an open buy from the last 3 days yet.")
    if paid:
        L += ["", f"🏆 *Paid off lately* (the coin rose {SMART_GOOD_MULT:g}x+ from what a saved wallet paid, last 7 days):"]
        for c in paid:
            chain, ca = c["chain"], c["key"].split(":", 1)[1]
            L.append(f"• ${md_escape(str(c['sym']))} · {CHAINS.get(chain, {'name': chain})['name']}"
                     + (f" · *MC now {fmt_usd(mcs[c['key']])}*" if mcs.get(c["key"]) else "") + f" · best {c['mx']:.1f}x · "
                     f"{c['n']} wallet{'s' if c['n'] != 1 else ''} in it" + (" · ✅ hit the win mark" if c["won"] else "")
                     + f"\n    `{ca}`")
    L.append("\n/buyers <address> shows who is buying one coin · /smart shows the wallets · paste an address for a scorecard.")
    out = "\n".join(L)
    return out if len(out) <= 3800 else out[:3800].rsplit("\n", 1)[0]


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
    now_ = time.time()
    scores = wallet_scores()
    for d in info["acc"][:BOOK_ADD_PER_COIN]:           # everything I see is saved in the wallet book too
        try:
            book_add(d["w"], key, chain, pair, [d["w"], d["buy"], d["sell"], d["n"], d["px"],
                                                d.get("b1") or d.get("t1") or 0, d.get("b0") or d.get("t0") or 0], now_)
        except Exception:
            log.exception("wallet book error")
    if not info["n"]:
        return f"🔎 *${sym}* | {CHAINS[chain]['name']}\nNo recent trades above ${BUYER_TRADE_FLOOR_USD:g} came back."
    L = [f"🔎 *Who is buying ${sym}*  |  {CHAINS[chain]['name']}",
         (f"💰 Market cap *{fmt_usd(mc)}*" + (" (a low cap)" if 0 < mc <= lowcap_limit() else "")
          + f" · liquidity {fmt_usd(num(pair, 'liquidity', 'usd'))}") if mc > 0 else "💰 Market cap: not known",
         f"Latest {info['n']} trades (above ${BUYER_TRADE_FLOOR_USD:g}), spanning {fmt_span(info['span'])}: "
         f"*{info['nb']}* wallets net-buying vs {info['ns']} net-selling - bought {fmt_usd(info['buy'])}, "
         f"sold {fmt_usd(info['sell'])} (net {'+' if info['buy'] >= info['sell'] else '-'}{fmt_usd(abs(info['buy'] - info['sell']))}).",
         f"Set aside as bots/routers: {info['bots']} address(es). Accumulating = bought at least {fmt_usd(info['thr'])} and sold under "
         f"{BUYER_MAX_SELL_SHARE * 100:.0f}% of it.", ""]
    if not info["top"]:
        L.append("No wallet is net-buying in this window.")
    for i, d in enumerate(info["top"][:8], 1):
        star = " 👀" if d["w"] in state["watch"] else ""          # 👀 = on your watch list (v3.7: ⭐ is now only used for the rating)
        acc = " 🟢accumulating" if d in info["acc"] else ""
        rec = wallet_record_text(d["w"], scores) if (book_grade(d["w"], scores) or (scores.get(d["w"]) or {}).get("n")) else ""
        L.append(f"{i}. `{d['w']}`{star}{acc}\n    bought {fmt_usd(d['buy'])} in {d['n']} trade{'s' if d['n'] != 1 else ''}"
                 + (f", sold {fmt_usd(d['sell'])}" if d["sell"] else "")
                 + (f", avg price ${d['px']:.6g}" if d["px"] else "")
                 + (f"\n    record: {rec}" if rec else "")
                 + f"  [wallet]({link(chain, 'addr', d['w'])})")
    L.append("\nThese are wallets that bought recently - not proof they are smart. /watch <address> <name> follows one; "
             "the bot scores wallets it follows by what happens next (⭐ = that rating out of 3, /stars explains; 👀 = on your watch list).")
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
    both = bt + bsh
    parts = []
    for sg in (3, 2, 1, 0):
        rs = [r for r in both if r.get("signs") == sg]
        if rs:
            x = summarize(rs)
            parts.append(f"{sg} of 3: {x['w']}/{x['n']}")
    if parts:
        out.append(("" if bt else "\n") + "🧭 By Sparsity signs (won/followed, alerts and quiet follow-ups): " + " | ".join(parts))
    wal = [r for r in both if r.get("acc", 0) >= 2 or r.get("smart")]
    if len(wal) >= 2 and len(both) - len(wal) >= 2:
        a, b2 = summarize(wal), summarize([r for r in both if not (r.get("acc", 0) >= 2 or r.get("smart"))])
        out.append(f"   With 2+ wallets accumulating at the time: {a['w']}/{a['n']} won | fewer: {b2['w']}/{b2['n']}")
    auto = sum(1 for d in state["watch"].values() if d.get("auto"))
    if auto:
        out.append(f"🧠 Smart wallets found by results and now watched: {auto}")
    return out


def chart_status_line() -> str:
    if not CHARTS:
        return "📈 Chart reader: switched off."
    now = time.time()
    fresh = [c for c in state["charts"].values() if c.get("ok") and now - c.get("t", 0) < CHART_KEEP_HOURS * 3600]
    shapes = sorted((c for c in fresh if c.get("pat") and c.get("score", 0) >= CHART_MIN_SCORE - 5), key=lambda c: -c["score"])
    top = ", ".join(f"${md_escape(c.get('sym', '?'))} {c['score']}" for c in shapes[:4])
    return (f"📈 Chart reader: {len(fresh)} charts read in the last {CHART_KEEP_HOURS // 24} days, {len(shapes)} with a cup / base / "
            f"double-bottom shape" + (f" (strongest: {top})" if top else "") + f"; chart alerts since this copy started: {bot_stats['chart']}. /patterns lists them.")


# ----------------------------------------------------------------------------
# CHAT COMMANDS (only from your own chat): /watch /unwatch /wallets /check /status /report /help
# ----------------------------------------------------------------------------
HELP_TEXT = (
    "🤖 *What you can tell me*\n\n"
    "*Paste a coin's contract address* (up to 3 at a time, or a DexScreener link) and I'll give it a scorecard out of 10: wallets "
    "with a record buying it, the bottom signs, the chart shape, liquidity and volume, red flags, and how each sign has paid so far. "
    "It is a checklist, not a prediction. I follow every coin you ask about and later compare its score with what happened.\n\n"
    "/vip - the named wallets (cryptoklotz, 0xdetweiler, unipcs, notanicecat69, angery and the low-cap hunters I find). When one buys a low-cap coin I ping you "
    "loud and repeat it. /vip add `<address>` `<name>` adds one, /vip remove `<name>` takes it off the list\n"
    "/vip style - 🎯 a picky wallet (buys few coins, so each buy means more) gets the loud ping on bigger coins too, a 🔕 sprayer (buys everything) "
    "gets a silent message. I judge it from how many different coins a wallet buys, or you set it: /vip style `<name>` picky|spray|normal|auto "
    "(or /vip add `<address>` `<name>` picky)\n"
    "/radar - 📡 coins on Robinhood Chain that many wallets keep buying and holding (accumulation); /radar `<coin address>` reads one coin now; "
    "/alerts radar off silences the alert\n"
    "/lowcap - what counts as a low cap (now under $1M); /lowcap 500k changes it. The low-cap crowd alert only fires for a young coin whose "
    "wallets bought it in the last day: /lowcap age 30 and /lowcap fresh 12 change those two limits\n"
    "/links - 📌 the coins you sent me: the market cap you sent each at, where it is now, and the wallets I found buying early when one "
    "went 2x or more. Send a coin again any time and I tell you how it did since\n"
    "/shill `<coin address>` - put a coin unipcs shilled on the dip-watch; /kol lists the dip-watch (I alert when a coin on it is down 60-80% from its high)\n"
    "/coins - which coins the saved wallets are buying right now (with each coin's market cap), and which paid off lately\n"
    "/smart - the wallet book: every accumulating wallet I have saved, who is winning, who earned a place on your list\n"
    "/stars - ⭐ what the 1 to 3 star rating of a wallet means (a buy is a hit when the coin doubles before it halves; a hit that was mostly "
    "done before I first saw the buy counts less), plus your wallets and the best rated ones with their stars. A wallet I added myself "
    "that keeps losing is taken off your list; one you added stays, and I warn you about it\n"
    "/alerts - which alerts are on; switch one with /alerts rerun off (names: vip, radar, lowcap, kol, smart, wallet, bottom, chart, rerun, narrative)\n"
    "/data - sends you a fresh copy of my memory file (attach it when you ask Claude what worked)\n"
    "/backup - sends my memory file now, plus your wallet list as lines to paste back. I also leave a 🗄 safety copy in this chat "
    "before every update and once a week. To restore, just send me one of those files here: I only add what is missing and "
    "never delete anything\n"
    "/watch `<wallet address>` `[name]` - add a wallet you trust. I'll say which of your wallets are still in each "
    "coin I flag, add the coins they hold to my watch list, and tell you when one buys or dumps a coin "
    "(paste several /watch lines in one message to add many at once).\n"
    "/unwatch `<name or address>` - stop watching it\n"
    "/wallets - your watched wallets (the ones marked 🧠 were found by the bot from results)\n"
    "/bottoms - every coin on Sparsity's bottom map, 3 of 3 down to 0 of 3 (I follow them all and look at who is buying)\n"
    "/patterns - coins whose chart shows a cup, cup & handle, bottom base or double bottom right now\n"
    "/buyers `<coin address>` - who has been buying a coin recently (accumulating wallets, bots set aside)\n"
    "/check `<coin address>` - the same scorecard as pasting the address (for an Ethereum-style address I find the chain; "
    "or write it like base:0x...)\n"
    "/status - the status check-in, right now\n"
    "/report - the learning report, right now")


def find_chain_for(addr: str) -> str | None:
    """Which of our EVM chains does this coin trade on? (the deepest pool wins)"""
    return lookup_chain(addr)[0]


# ----------------------------------------------------------------------------
# v3.0 - PASTE A COIN ADDRESS, GET A SCORECARD. Every sign this bot knows how to look at, in one checklist: wallets with a
# record buying it, the bottom signs, the chart shape, and the basics (liquidity, volume, age), with red flags. It is a
# checklist, not a prediction: every coin you ask about is followed in silence and, a week later, its score is compared with
# what really happened - so the track record under each scorecard is the bot's own, not a promise.
# ----------------------------------------------------------------------------
REF_CHAINED = re.compile(r"(?<![0-9A-Za-z])((?i:solana|sol|ethereum|eth|bsc|bnb|base|robinhood|hood))\s*:\s*"
                         r"(0x[0-9a-fA-F]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})(?![0-9A-Za-z])")
REF_EVM = re.compile(r"(?<![0-9A-Za-z])0x[0-9a-fA-F]{40}(?![0-9A-Za-z])")
REF_SOL = re.compile(r"(?<![0-9A-Za-z])[1-9A-HJ-NP-Za-km-z]{32,44}(?![0-9A-Za-z])")
REF_DEX = re.compile(r"dexscreener\.com/([a-z0-9\-]+)/([0-9A-Za-z]{20,70})", re.I)


def pair_to_token(chain: str, pair_id: str) -> str | None:
    """A DexScreener link ends in a trading pair, not in the coin: ask DexScreener which coin the pair trades."""
    try:
        data = get_with_retry(f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{pair_id}", tries=2).json()
    except (requests.RequestException, ValueError):
        return None
    pairs = data.get("pairs") if isinstance(data, dict) else None
    for p in pairs or []:
        a = (p.get("baseToken") or {}).get("address") if isinstance(p, dict) else None
        if a:
            return norm_addr(chain, str(a))
    return None


def extract_refs(text: str) -> list[str]:
    """The coin addresses in a pasted message, in the order written, no repeats: 'base:0x...', a bare address, or a
    DexScreener link (those become 'dex:<chain>:<pair>' and are turned into the coin later). At most CHECK_MAX_PER_MESSAGE."""
    makers = (
        (REF_DEX, lambda mt: f"dex:{norm_chain(mt.group(1))}:{mt.group(2)}" if norm_chain(mt.group(1)) else None),
        (REF_CHAINED, lambda mt: (f"{norm_chain(mt.group(1))}:{norm_addr(norm_chain(mt.group(1)), mt.group(2))}"
                                  if norm_chain(mt.group(1)) else None)),
        (REF_EVM, lambda mt: mt.group(0).lower()),
        (REF_SOL, lambda mt: mt.group(0)),
    )
    found: list[tuple[int, int, str]] = []
    for rx, make in makers:                      # the more specific forms first: a longer match hides the shorter ones inside it
        for mt in rx.finditer(text):
            ref = make(mt)
            a, b = mt.span()
            if ref and not any(a < y and x < b for x, y, _ in found):
                found.append((a, b, ref))
    out: list[str] = []
    for _, _, ref in sorted(found):
        if ref not in out:
            out.append(ref)
    return out[:CHECK_MAX_PER_MESSAGE]


def chart_for_check(key: str, chain: str, pair: dict, m: dict, now: float) -> dict | None:
    """The coin's chart summary: the latest reading, or a fresh one when there is none (or it is old)."""
    if not CHARTS:
        return None
    c = state["charts"].get(key)
    fresh = bool(c and c.get("ok") is not None and c.get("t") and now - c["t"] < CHART_REFRESH_HOURS * 3600)
    if not fresh and GECKO_NETWORK.get(chain) and pair.get("pairAddress") and gecko_backlog() < 45:
        try:
            read_chart(key, chain, pair, m, now)
        except Exception:
            log.exception("chart look-up for the scorecard failed")
        c = state["charts"].get(key)
    return c


def buyers_for_check(key: str, chain: str, pair: dict, m: dict, now: float) -> dict | None:
    """Who has been buying: the latest look when it is recent, otherwise look now (the wallets are saved in the wallet book)."""
    cap = cap_of(key)
    if cap and now - cap.get("t", 0) < 6 * 3600:
        return cap
    if BUYER_CAPTURE and GECKO_NETWORK.get(chain) and pair.get("pairAddress") and gecko_backlog() < 45:
        holder = bottom_entry(key) or state["charts"].setdefault(
            key, {"t": 0, "ok": None, "pat": "", "score": 0, "signs": 0,
                  "sym": str(pair.get("baseToken", {}).get("symbol", "?"))[:14], "chain": chain})
        try:
            capture_buyers(key, chain, pair, m, holder, now)
        except Exception:
            log.exception("buyer look-up for the scorecard failed")
        cap = cap_of(key)
    return cap if cap and now - cap.get("t", 0) < 48 * 3600 else None


def score_coin(m: dict, pair: dict, c: dict | None, b: dict | None, cap: dict | None, held: dict, scores: dict) -> dict:
    """The scorecard maths (no network). Four groups worth 10 points in all: smart money (4), bottom signs (3), chart shape (2),
    the basics (1). Red flags can cap the verdict whatever the points are."""
    groups: list[tuple[str, float, float, list[str]]] = []
    pts: dict[str, float] = {}
    flags: list[tuple[int, str]] = []
    missing: list[str] = []

    # --- 1. smart money (up to 4)
    p, lines = 0.0, []
    acc = [r for r in ((cap or {}).get("acc") or []) if r]
    n_acc = max(acc_count(cap), len(acc))
    strong = [r for r in acc if book_grade(r[0], scores) in ("watched", "proven")]
    soft = [r for r in acc if book_grade(r[0], scores) == "promising"]
    if cap is None:
        lines.append("▫️ couldn't look at who is buying just now (the trade data was busy) - ask again in a minute")
    else:
        if strong:
            # v3.7: the points follow the wallet's star rating - a wallet on your list with a weak record no longer scores like a proven one
            best = max(trusted_points(wallet_stars(r[0], scores)) for r in strong)
            p += best
            who = "; ".join(f"`{r[0]}` ({wallet_record_text(r[0], scores)}, bought {fmt_usd(r[1])})" for r in strong[:2])
            if best >= 2.0:
                lines.append(f"✅ {len(strong)} wallet(s) with a good record ({format(STAR_GOOD, 'g')}+ stars) are accumulating it: {who}")
            elif best >= 1.5:
                lines.append(f"✅ {len(strong)} wallet(s) you watch (or earned a place) are accumulating it; their record with me is average or not rated yet: {who}")
            else:
                lines.append(f"▫️ {len(strong)} wallet(s) you watch are accumulating it, but their record with me is weak: {who}")
        elif len(soft) >= 2:
            p += 1.5
            lines.append(f"✅ {len(soft)} wallets with a promising start are accumulating it: "
                         + "; ".join(f"`{r[0]}` (bought {fmt_usd(r[1])})" for r in soft[:2]))
        elif soft:
            p += 1.0
            lines.append(f"✅ 1 wallet with a promising start is accumulating it: `{soft[0][0]}` (bought {fmt_usd(soft[0][1])})")
        else:
            lines.append("❌ no wallet with a good record is buying it")
            mixt = buyer_mix_text(acc, scores)         # v3.8.1: who the buyers ARE (a medal in an older check-in could say otherwise)
            if mixt:
                lines.append(f"   the {len(acc)} biggest buyers: {mixt}")
            missing.append("a wallet with a good record buying it")
        nb, ns = int(cap.get("nb", 0)), int(cap.get("ns", 0))
        flow = (f"{nb} wallets net-buying vs {ns} net-selling - bought {fmt_usd(cap.get('buy', 0))}, sold "
                f"{fmt_usd(cap.get('sell', 0))} (latest {cap.get('n', 0)} trades, {fmt_span(cap.get('span', 0))})")
        if len(acc) >= 2 and nb > ns:
            p += 1.0
            lines.append(f"✅ {n_acc} wallets are accumulating it (last 3 days); {flow}")
        elif ns > nb and cap.get("sell", 0) > cap.get("buy", 0):
            flags.append((1, f"more wallets are selling than buying - {flow}"))
        else:
            lines.append(f"▫️ {flow}")
    hin, hw = list(held.get("in") or []), list(held.get("in_w") or [])
    good_in = [e for i, e in enumerate(hin) if not (i < len(hw) and book_grade(hw[i], scores) == "cooling")]
    cool_in = [e for i, e in enumerate(hin) if i < len(hw) and book_grade(hw[i], scores) == "cooling"]
    if good_in:
        p += 1.0
        lines.append("✅ wallets you watch are in it: " + ", ".join(md_escape(n) for n, _, _ in good_in[:3]))
    elif held.get("total") and not cool_in:
        lines.append(f"▫️ none of your {held['total']} watched wallet(s) hold it")
    if cool_in:        # v3.8.1: a wallet whose results turned bad does not earn points for holding a coin either
        lines.append("🧊 in it, but their results turned bad so they don't count: " + ", ".join(md_escape(n) for n, _, _ in cool_in[:3]))
    lines += accum_lines(pair)       # v3.4: what the wallets you watch added / sold lately (information only - no points)
    pts["smart"] = min(p, 4.0)
    groups.append(("🧠 Smart money", pts["smart"], 4.0, lines))

    # --- 2. bottom signs (up to 3): Sparsity's three signs and the bot's own reading of the chart
    sp = int(b["signs"]) if b and isinstance(b.get("signs"), int) else None
    own = int(c.get("signs", 0)) if c and c.get("ok") else None
    p, lines = 0.0, []
    if sp is not None:
        p = max(p, {3: 3.0, 2: 2.0, 1: 0.5}.get(sp, 0.0))
        marks = " · ".join(f"{step} {'✅' if i < sp else '▫️'}" for i, step in enumerate(SPARSITY_STEPS))
        low = (f" - {b['above_low']:.0f}% above its low of {fmt_usd(b['low'])}"
               if b.get("low") and b.get("above_low") is not None else "")
        lines.append(f"{'✅' if sp >= 2 else '▫️'} Sparsity's bottom map: *{sp} of 3* ({marks}){low}")
    if own is not None:
        p = max(p, {3: 2.5, 2: 1.5, 1: 0.5}.get(own, 0.0))
        marks = " ".join(f"{'✅' if i < own else '▫️'}{step}" for i, step in enumerate(SPARSITY_STEPS))
        lines.append(f"{'✅' if own >= 2 else '▫️'} my own reading of the chart: *{own} of 3* ({marks})")
    if sp is not None and own is not None and min(sp, own) >= 2:
        p = min(3.0, p + 0.5)
    if sp is None and own is None:
        lines.append("▫️ no bottom reading yet (not on Sparsity's map, and the chart is too short or not readable just now)")
    if p < 2.0:
        missing.append("bottom signs (sellers worn out → the low holding → demand coming back)")
    pts["bottom"] = min(p, 3.0)
    groups.append(("🧭 Bottom signs", pts["bottom"], 3.0, lines))

    # --- 3. chart shape (up to 2)
    p, lines = 0.0, []
    if c and c.get("ok"):
        if c.get("pat"):
            sc_ = float(c.get("score", 0))
            base = {"cup_handle": 2.0, "cup": 1.8}.get(c["pat"], 1.5)
            p = base if sc_ >= 70 else base * 0.75 if sc_ >= 60 else base * 0.35 if sc_ >= 45 else 0.0
            price = num(pair, "priceUsd")
            cc = dict(c)
            if price > 0:
                cc["k"] = m["mc"] / price
            lines.append(f"{'✅' if sc_ >= 60 else '▫️'} *{PATTERN_LABEL.get(c['pat'], c['pat'])}* · {md_escape(c.get('stage', ''))}"
                         f" · shape score *{c.get('score', 0)}/100*")
            sl = shape_line(cc)
            if sl:
                lines.append("   " + md_escape(sl))
            if c.get("lvl") and cc.get("k"):
                lines.append(f"   a move above *{lvl_text(cc, c['lvl'])}* would be the breakout; the shape is wrong under "
                             f"{lvl_text(cc, c.get('stp'))}")
        else:
            lines.append("❌ no cup, base or double-bottom shape right now")
        if p < 1.2:
            missing.append("a cup, base or double-bottom shape on the chart")
    else:
        lines.append("▫️ chart not readable (too little trading history, or the chart service was busy)")
    pts["chart"] = min(p, 2.0)
    groups.append(("📈 Chart shape", pts["chart"], 2.0, lines))

    # --- 4. the basics (up to 1) and the red flags
    liq, mc = float(m.get("liq", 0) or 0), float(m.get("mc", 0) or 0)
    ok_liq = liq >= 15_000 and m["liq_ratio"] >= 0.04
    ok_vol = m["vol_ratio"] >= 0.15 and m["traders"] >= 200
    ok_age = m["age_h"] >= 24
    p = (0.5 if ok_liq else 0.0) + (0.25 if ok_vol else 0.0) + (0.25 if ok_age else 0.0)
    lines = [f"{'✅' if ok_liq else '❌'} liquidity {fmt_usd(liq)} ({m['liq_ratio'] * 100:.1f}% of market cap)",
             f"{'✅' if ok_vol else '▫️'} 24h volume {fmt_usd(m['vol24'])} ({m['vol_ratio'] * 100:.0f}% of market cap) · "
             f"{m['traders']:,} trades · pair {fmt_age(m['age_h'])} old"]
    pts["basics"] = p
    groups.append(("🛡 The basics", p, 1.0, lines))
    if liq < 5_000:
        flags.append((2, f"liquidity is only {fmt_usd(liq)} - one sale can crash the price and you may not get out"))
    elif m["liq_ratio"] < 0.02:
        flags.append((1, f"liquidity is only {m['liq_ratio'] * 100:.1f}% of the market cap - the price is easy to push around"))
    if 0 < m["age_h"] < 6:
        flags.append((1, f"the pool is only {fmt_age(m['age_h'])} old - far too early to judge anything"))
    h24 = num(pair, "priceChange", "h24")
    if h24 >= 150:
        flags.append((1, f"already up {h24:.0f}% in 24 hours - chasing a pump is the classic way to lose"))
    if liq > 0 and m["vol24"] > liq * NARR_MAX_VOL_LIQ:
        flags.append((1, f"24h volume is {m['vol24'] / liq:.0f}x the liquidity - often bots trading with themselves"))

    total = round(sum(pts.values()), 1)
    worst = max((s for s, _ in flags), default=0)
    if worst >= 2:
        icon, name = "⛔", "AVOID"
    elif total >= 7:
        icon, name = ("🟢", "STRONG SETUP") if worst == 0 else ("🟡", "PROMISING - mind the red flags")
    elif total >= 5:
        icon, name = ("🟡", "PROMISING") if worst == 0 else ("🟠", "MIXED - mind the red flags")
    elif total >= 3:
        icon, name = "🟠", "EARLY / MIXED"
    else:
        icon, name = "🔴", "WEAK"
    return {"total": total, "pts": {k: round(v, 2) for k, v in pts.items()}, "groups": groups, "flags": flags,
            "missing": missing, "icon": icon, "name": name}


def sign_track_record() -> str:
    """How each kind of sign has actually paid in this bot's own follow-ups (won = 2x before 0.5x)."""
    parts = []
    sb = closed_records("smartbuy")
    if sb:
        x = summarize(sb)
        parts.append(f"smart-wallet alerts {x['w']}/{x['n']}")
    both = [r for r in closed_records("bottom") + closed_records("bshadow") if (r.get("signs") or 0) >= 2]
    if both:
        x = summarize(both)
        parts.append(f"bottom map with 2-3 signs {x['w']}/{x['n']}")
    ch = closed_records("chart") + closed_records("cshadow")
    if ch:
        x = summarize(ch)
        parts.append(f"chart shapes {x['w']}/{x['n']}")
    rr = closed_records("call")
    if rr:
        x = summarize(rr)
        parts.append(f"old re-run setups {x['w']}/{x['n']}")
    hi = [r for r in closed_records("check") if r.get("score", 0) >= 7]
    if hi:
        x = summarize(hi)
        parts.append(f"coins you asked about that scored 7+ {x['w']}/{x['n']}")
    if not parts:
        return "📊 No track record yet: the first results come in over the next days."
    return ("📊 How these signs have paid so far in the bot's own follow-ups (won/followed; win = " + f"{WIN_MULTIPLE:g}x before "
            f"{STOP_MULTIPLE:g}x): " + " · ".join(parts) + ". Small numbers - treat them as hints.")


def follow_checked(key: str, chain: str, pair: dict, m: dict, sc: dict, now: float) -> str:
    """Follow the coin you asked about in silence, so its score can be compared with what happens next."""
    mine = next((r for r in open_records(key) if r["kind"] == "check"), None)
    if mine:
        s0 = mine.get("score")
        return (f"📌 I'm already following it from {fmt_usd(mine['mc0'])} (asked {ago(now - mine['t0'])} ago"
                + (f", it scored {s0:g} then" if isinstance(s0, (int, float)) else "")
                + f") - now {mine.get('final', 1.0):.2f}x of that price, best {mine.get('best', 1.0):.2f}x.")
    add_record("check", key, chain, pair, m, "", now,
               extra={"th": CHECK_TRACK_HOURS, "score": sc["total"], "flags": len(sc["flags"]), "grp": sc["pts"]})
    return (f"📌 I'll follow it quietly for {follow_text(CHECK_TRACK_HOURS, m['mc'], 'check')} from {fmt_usd(m['mc'])} (win = {WIN_MULTIPLE:g}x before "
            f"{STOP_MULTIPLE:g}x) and add the result to the track record above.")


# ----------------------------------------------------------------------------
# v3.8 - THE COINS YOU SEND: the market cap each one was sent at is remembered, the coin is followed in the background (up to
# EXTEND_LINK_DAYS days), and when it reaches LINK_LOOK_X times that market cap I look at who is in it - the wallets that were there early are saved
# in the wallet book (as early hits, which count for little until they win again) and you get a message. Send it again and you see what happened since.
# ----------------------------------------------------------------------------
def link_open_keys() -> set:
    out = set()
    for k, e in list(state["links"].items()):
        if isinstance(e, dict) and e.get("st") == "open":
            if k in state["steady"]:
                e["st"] = "ended"              # (a coin that turned out to barely move is not a runner: it is no longer followed)
                continue
            out.add(k)
    return out


def link_prune(now: float):
    links = state["links"]
    cutoff = now - LINK_KEEP_DAYS * 86400
    for k in [k for k, e in links.items() if not isinstance(e, dict) or (e.get("st") != "open" and num(e, "tl") < cutoff)]:
        del links[k]
    if len(links) >= LINK_MAX:
        for k in sorted(links, key=lambda k_: (links[k_].get("st") == "open", num(links[k_], "tl")))[:len(links) - LINK_MAX + 1]:
            del links[k]


def link_state_text(e: dict) -> str:
    return {"open": "still following it", "stop": "I stopped following it after it fell to half or less",
            "ended": "I stopped following it" + (" after it broke under its low" if e.get("why") == "broke" else " (time ran out)")}.get(e.get("st"), "")


def link_snapshot(key: str) -> list:
    """Who was accumulating the coin around the time you sent it (the latest look at its buyers): [[wallet, bought $, average price paid]]."""
    cap = cap_of(key)
    return [[r[0], r[1], float(f"{float(r[4] or 0):.6g}")] for r in ((cap or {}).get("acc") or [])[:LINK_WALLETS_KEPT]
            if r and len(r) > 4 and isinstance(r[0], str)]


def link_note(key: str, chain: str, pair: dict, m: dict, now: float) -> str:
    """Called when you send a coin. First time: remember its market cap and price (and who was accumulating it). Again: say what happened since
    you first sent it, and - when it is up LINK_LOOK_X times or more - look at who is in it."""
    if not LINK_ON or key in state["steady"]:
        return ""
    px = num(pair, "priceUsd")
    links = state["links"]
    e = links.get(key)
    if not isinstance(e, dict) or num(e, "mc0") <= 0:
        link_prune(now)
        links[key] = {"sym": str(pair.get("baseToken", {}).get("symbol", "?"))[:14], "chain": chain, "t0": now, "tl": now, "tr": now,
                      "mc0": m["mc"], "p0": px, "pa": pair.get("pairAddress", ""), "n": 1, "last": m["mc"], "max_mc": m["mc"],
                      "min_mc": m["mc"], "st": "open", "ms": 0, "look": 0, "w0": link_snapshot(key)}
        return (f"📌 *Saved:* you sent this at a market cap of *{fmt_usd(m['mc'])}*. I keep an eye on it in the background (up to "
                f"{EXTEND_LINK_DAYS:g} days). If it reaches {LINK_LOOK_X:g}x that, I look at who is in it and message you; send it again any time "
                "to see how it did since.")
    e["n"] = int(num(e, "n")) + 1
    e["tl"] = now
    cur = read_mc(e, m["mc"], pair, [pair])
    cur = m["mc"] if cur is None or cur <= 0 else cur
    x = cur / e["mc0"]
    best = max(num(e, "max_mc"), cur) / e["mc0"]
    L = [f"🔁 *You first sent this {ago(now - e['t0'])} ago*, at a market cap of *{fmt_usd(e['mc0'])}* (you have sent it {e['n']} times). "
         f"Now *{fmt_usd(m['mc'])}* = *{x:.2f}x* what it was then (best I saw: {best:.2f}x)."]
    if e.get("st") != "open":
        L.append(f"   {link_state_text(e).capitalize()}" + (" - but it is above where it was when you sent it, so I follow it again." if x > STOP_MULTIPLE * 1.2 else "."))
        if x > STOP_MULTIPLE * 1.2:
            e.update(st="open", tr=now)
            for f in ("lo1", "why", "z"):
                e.pop(f, None)
    e["last"] = cur
    e["max_mc"] = max(num(e, "max_mc"), cur if x >= 1 else 0)
    if best >= LINK_LOOK_X:
        try:
            found = wallet_look(key, chain, pair, m, e, now)
            # (you are being shown the look now, so the scan does not send the same milestone again a minute later)
            e["ms"] = max(int(num(e, "ms")), 1 + (best >= LINK_LOOK_X * 2.5) + (best >= LINK_LOOK_X * 5))
        except Exception:
            log.exception("wallet look failed for %s", key)
            found = None
        L += look_lines(e, found, pair, now)
    return "\n".join(L)


def wallet_look(key: str, chain: str, pair: dict, m: dict, e: dict, now: float, capture: bool = True) -> list[dict]:
    """Who was in this coin EARLY? Looks at the latest trades once more (when they have not been read in the last 3 hours) and puts together
    everything I have on the coin's wallets - the running totals of the last days, the wallet book, who was accumulating it when you sent it,
    the radar's readings - then keeps the wallets that paid up to LINK_EARLY_X times the price you sent it at (or bought before you sent it),
    bought at least $300 and still hold most of it. The best of them are saved in the wallet book as an early hit. -> [{w, usd, paid, ...}]"""
    px_now, p0 = num(pair, "priceUsd"), num(e, "p0")
    if capture and BUYER_CAPTURE and GECKO_NETWORK.get(chain) and pair.get("pairAddress") and gecko_backlog() < 45:
        cap = cap_of(key)
        if not cap or now - cap.get("t", 0) > 3 * 3600:
            holder = bottom_entry(key) or state["charts"].setdefault(
                key, {"t": 0, "ok": None, "pat": "", "score": 0, "signs": 0, "sym": str(pair.get("baseToken", {}).get("symbol", "?"))[:14], "chain": chain})
            try:
                capture_buyers(key, chain, pair, m, holder, now)
            except Exception:
                log.exception("buyer look-up for the wallet look failed")
    cands: dict[str, dict] = {}

    def add(w, usd, paid=0.0, first=0.0, last=0.0, sold=0.0):
        if not isinstance(w, str) or not w or usd <= 0:
            return
        c = cands.get(w)
        if c is None:
            cands[w] = {"w": w, "usd": float(usd), "paid": float(paid or 0), "first": float(first or 0), "last": float(last or 0), "sold": float(sold or 0)}
            return
        if usd > c["usd"]:
            c["usd"] = float(usd)
        if paid and not c["paid"]:
            c["paid"] = float(paid)
        if first and (not c["first"] or first < c["first"]):
            c["first"] = float(first)
        c["last"] = max(c["last"], float(last or 0))
        c["sold"] = max(c["sold"], float(sold or 0))

    for holder in (state["bottoms"].get(key), state["charts"].get(key)):
        for w, f in ((holder or {}).get("flow") or {}).items():
            try:
                buy, sell, n, ns, t0, t1, pxw = f[:7]
                if (f[7] if len(f) > 7 else 0) or buy <= 0:
                    continue
                add(w, buy, pxw / buy, (f[8] if len(f) > 9 else 0) or t0, (f[9] if len(f) > 9 else 0) or t1, sell)
            except (TypeError, ValueError, ZeroDivisionError):
                continue
    for w, d in list(state["book"].items()):
        for p in d.get("picks", []):
            if isinstance(p, dict) and p.get("k") == key and p.get("own"):
                add(w, num(p, "usd"), num(p, "px"), num(p, "bf") or num(p, "t"), num(p, "bt"))
    for row in e.get("w0") or []:
        if isinstance(row, (list, tuple)) and len(row) >= 3 and isinstance(row[0], str):
            add(row[0], fnum(row[1]), fnum(row[2]), num(e, "t0"), num(e, "t0"))
    for row in ((state["radar"].get("coins") or {}).get(key) or {}).get("top") or []:
        if isinstance(row, (list, tuple)) and len(row) >= 2 and isinstance(row[0], str):
            add(row[0], fnum(row[1]))
    out = []
    for c in cands.values():
        early = (p0 > 0 and 0 < c["paid"] <= LINK_EARLY_X * p0) or (c["paid"] <= 0 and 0 < c["first"] <= num(e, "t0") + 3600)
        if (not early or c["usd"] < 300 or c["sold"] > 0.5 * c["usd"] or is_bundler(chain, c["w"])
                or c["w"] in (norm_addr(chain, pair.get("pairAddress", "")), norm_addr(chain, key.split(":", 1)[1]))):
            continue
        c["gain"] = (px_now / c["paid"]) if c["paid"] > 0 and px_now > 0 else 0.0
        out.append(c)
    scores = wallet_scores()
    out.sort(key=lambda c: (-(wallet_stars(c["w"], scores) or 0), -c["usd"]))
    out = out[:LINK_WALLETS_KEPT]
    sym = str(pair.get("baseToken", {}).get("symbol", "?"))[:12]
    for c in out:
        try:
            save_early_hit(c, key, sym, chain, pair, px_now, now)
        except Exception:
            log.exception("could not save an early wallet")
    e["look"] = now
    e["found"] = [[c["w"], round(c["usd"]), float(f"{c['paid']:.6g}")] for c in out]
    return out


def save_early_hit(c: dict, key: str, sym: str, chain: str, pair: dict, px_now: float, now: float):
    """Put a wallet found on a coin that went up into the wallet book, as a finished buy that doubled - an EARLY hit (it bought low, long before I
    looked), which counts for EARLY_WIN_WEIGHT in its rating until it wins on coins I could follow. Nothing to do if its buy of this coin is
    already in the book, or if the price it paid is not known."""
    w, paid = c["w"], c["paid"]
    if paid <= 0 or px_now <= 0 or px_now / paid < WIN_MULTIPLE:
        return
    book = state["book"]
    d = book.get(w)
    if d is None:
        if len(book) >= BOOK_MAX:
            book_prune(now)
            if len(book) >= BOOK_MAX:
                return
        d = book[w] = {"first": round(now), "last": round(now), "chain": chain, "seen": 0, "n": 0, "good": 0,
                       "win": 0, "stop": 0, "flat": 0, "picks": []}
    if any(p.get("k") == key for p in d["picks"] if isinstance(p, dict)):
        return
    d["last"] = round(now)
    d["picks"].append({"k": key, "s": sym, "c": chain, "t": round(now), "t1": round(now), "px": float(f"{paid:.6g}"), "pa": pair.get("pairAddress", ""),
                       "usd": round(c["usd"]), "st": "w", "mx": round(px_now / paid, 3), "lr": round(px_now / paid, 3), "lr0": round(px_now / paid, 3),
                       "own": 1, "th": BOOK_TRACK_HOURS, "bt": round(c["last"]) if c["last"] else 0, "bf": round(c["first"]) if c["first"] else 0, "src": "look"})
    d["seen"] += 1
    d["n"] += 1
    d["win"] += 1
    d["good"] += 1
    d.setdefault("found", key)
    while len(d["picks"]) > BOOK_PICKS:
        i = next((j for j, p in enumerate(d["picks"]) if p.get("st") != "o"), None)
        if i is None:
            break
        d["picks"].pop(i)


def look_lines(e: dict, found: list | None, pair: dict, now: float) -> list[str]:
    """The wallet look as lines of text."""
    if found is None:
        return ["   (I couldn't look at the wallets just now - the trade data was busy. Ask again in a minute.)"]
    if not found:
        return ["🔎 *Who was in early?* No wallet stands out among the trades I can see (GeckoTerminal only shows the latest trades, so the "
                "earliest buyers are often out of view). I keep watching for them."]
    scores = wallet_scores()
    p0, px = num(e, "p0"), num(pair, "priceUsd")
    L = ["🔎 *Wallets that were in early* (they paid close to the price you sent it at, or less, and still hold most of it):"]
    for c in found[:5]:
        rel = ""
        if p0 > 0 and c["paid"] > 0:
            rel = (f", paid about {1 - c['paid'] / p0:.0%} under the price you sent it at" if c["paid"] < 0.95 * p0 else
                   ", paid about the price you sent it at" if c["paid"] <= 1.05 * p0 else f", paid {c['paid'] / p0 - 1:.0%} over the price you sent it at")
        gain = f", up {c['gain'] - 1:+.0%} on its price" if c.get("gain") else ""
        rec = wallet_record_text(c["w"], scores) if (book_grade(c["w"], scores) or (scores.get(c["w"]) or {}).get("n")) else "no record with me yet"
        L.append(f"• `{c['w']}`  {rec}\n   bought {fmt_usd(c['usd'])}{rel}{gain}")
    L.append("   I saved them in the wallet book; a hit like this counts for little until they win again on coins I can follow. "
             "(/watch <address> <name> follows one.)")
    return L


def link_track(key: str, mc: float, pair: dict | None, pairs: list | None, now: float, cands: list):
    """Every scan, for each coin you sent: update it (the same two-readings-in-a-row rule as everywhere), end the story when it fell to half or
    broke under the low of its first week, or when time ran out, and queue it for the wallet look when it first reaches LINK_LOOK_X times."""
    e = state["links"].get(key)
    if not isinstance(e, dict) or e.get("st") != "open" or num(e, "mc0") <= 0:
        return
    cur = read_mc(e, mc, pair, pairs)
    if cur is None:
        return
    prev = e.get("last", cur)
    e["last"] = cur
    if cur > 0:
        e.pop("z", None)
    else:
        e.setdefault("z", now)
    hi, lo = max(cur, prev), min(cur, prev)
    e["max_mc"] = max(num(e, "max_mc"), lo)
    if hi > 0:
        e["min_mc"] = min(num(e, "min_mc") or hi, hi)
    start = num(e, "tr") or num(e, "t0")
    if not e.get("lo1") and now - start >= TRACK_HOURS * 3600:
        e["lo1"] = e["min_mc"]                       # the lowest point of the first week
    if hi <= STOP_MULTIPLE * e["mc0"] and (cur > 0 or now - num(e, "z") >= RUG_CONFIRM_SECONDS):
        e["st"] = "stop"
    elif e.get("lo1") and cur > 0 and hi <= EXTEND_BREAK * e["lo1"]:
        e.update(st="ended", why="broke")
    elif now - start >= EXTEND_LINK_DAYS * 86400:
        e["st"] = "ended"
    if e["st"] == "open" and e["max_mc"] >= LINK_LOOK_X * e["mc0"] * (2.5 if num(e, "ms") >= 1 else 1.0) * (2.0 if num(e, "ms") >= 2 else 1.0):
        cands.append((key, e))


def link_pass(cands: list, now: float, caps: list | None = None):
    """After a scan: coins you sent that just reached LINK_LOOK_X times (and then 5x and 10x) - look at their wallets and message you.
    At most two per scan, so the trade data is not flooded."""
    done = 0
    for key, e in cands:
        if done >= 2 or e.get("st") != "open":
            break
        ms = int(num(e, "ms"))
        if ms >= 3 or (ms >= 1 and now - num(e, "look") < 6 * 3600):
            continue
        chain = e.get("chain")
        token = key.split(":", 1)[1]
        pairs = []
        try:
            pairs = [p for p in fetch_pairs(chain, [token]) if norm_addr(chain, p.get("baseToken", {}).get("address", "")) == norm_addr(chain, token)]
        except requests.RequestException:
            continue
        pair = best_live_pair(pairs)
        m = compute_metrics(key, pair) if pair else None
        if not pair or not m:
            continue
        try:
            found = wallet_look(key, chain, pair, m, e, now)
        except Exception:
            log.exception("wallet look failed for %s", key)
            continue
        x = max(num(e, "max_mc"), m["mc"]) / e["mc0"]
        if send_telegram(build_link_message(key, chain, pair, m, e, found, x, now)):
            e["ms"] = ms + 1
            bot_stats["link_hit"] += 1
            done += 1
            log.info("a coin you sent reached %.1fx: %s (%d early wallets)", x, key, len(found))


def build_link_message(key: str, chain: str, pair: dict, m: dict, e: dict, found: list, x: float, now: float) -> str:
    base = pair.get("baseToken", {})
    addr, sym = base.get("address", ""), md_escape(str(base.get("symbol", "?")))
    L = [f"*🔔 A COIN YOU SENT IS UP {x:.1f}x: ${sym}*  |  {CHAINS[chain]['name']}", "",
         f"You sent it {ago(now - e['t0'])} ago, when its market cap was *{fmt_usd(e['mc0'])}*. It is *{fmt_usd(m['mc'])}* now "
         f"({m['mc'] / e['mc0']:.2f}x; best {x:.2f}x)."]
    L += look_lines(e, found, pair, now)
    L += ["", f"💧 liquidity {fmt_usd(m['liq'])} ({m['liq_ratio'] * 100:.1f}% of MC) · 📊 24h volume {fmt_usd(m['vol24'])} · ⏱ {fmt_age(m['age_h'])}",
          "I keep following it. Nothing here is a buy signal - the coin already ran; the wallets are what I learn from.", "",
          f"`{addr}`", f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | [Token]({link(chain, 'token', addr)})"]
    out = "\n".join(L)
    return out if len(out) <= 3900 else out[:3900].rsplit("\n", 1)[0]


def cmd_links() -> str:
    """/links: the coins you sent, with the market cap each was sent at."""
    items = [(k, e) for k, e in state["links"].items() if isinstance(e, dict) and num(e, "mc0") > 0]
    if not items:
        return ("You haven't sent me any coin yet. Paste a coin address and I save the market cap it had then, follow it in the background, "
                f"and tell you if it reaches {LINK_LOOK_X:g}x (and which wallets were in early).")
    items.sort(key=lambda kv: -num(kv[1], "tl"))
    now = time.time()
    L = [f"📌 *Coins you sent me* ({len(items)})", ""]
    for k, e in items[:15]:
        cur = mc_cached(k, 12 * 3600) or num(e, "last")
        x = cur / e["mc0"] if cur > 0 else 0.0
        best = max(num(e, "max_mc"), cur) / e["mc0"]
        L.append(f"• ${md_escape(str(e.get('sym', '?')))} · {CHAINS.get(e.get('chain'), {'name': e.get('chain')})['name']} · sent {ago(now - e['t0'])} ago at "
                 f"{fmt_usd(e['mc0'])} → {fmt_usd(cur) if cur else '?'} (*{x:.2f}x*, best {best:.2f}x) · {link_state_text(e) or '?'}"
                 + (f" · looked at its wallets ({len(e.get('found') or [])} early)" if e.get("look") else ""))
    if len(items) > 15:
        L.append(f"...and {len(items) - 15} older ones.")
    L.append(f"\nI look at a coin's wallets when it reaches {LINK_LOOK_X:g}x the market cap you sent it at (and again at 5x and 10x). Send an address again to see how it did.")
    return "\n".join(L)[:3900]


def scorecard_text(chain: str, addr: str, pair: dict, m: dict, key: str, sc: dict, follow: str, linked: str = "") -> str:
    base = pair.get("baseToken", {})
    sym = md_escape(str(base.get("symbol", "?")))
    L = [f"🔍 *${sym}*  |  {CHAINS[chain]['name']}", f"`{addr}`", "",
         f"{sc['icon']} *{sc['name']}* - {sc['total']:g} of 10   (a checklist of signs, not a prediction)", ""]
    if linked:
        L += [linked, ""]
    for title, got, top, lines in sc["groups"]:
        L.append(f"*{title}*  {got:g}/{top:g}")
        L += lines
        L.append("")
    if sc["flags"]:
        L.append("*Red flags*")
        L += [("⛔ " if sev >= 2 else "🚩 ") + t for sev, t in sc["flags"]]
        L.append("")
    if sc["missing"]:
        L += ["*What would make the case stronger:* " + "; ".join(sc["missing"][:3]) + ".", ""]
    peak_note = "estimated from its price history" if key in state["backfilled"] else "not looked up - treat with care"
    L.append(f"💰 Market cap *{fmt_usd(m['mc'])}* · down *{m['retrace']:.0f}%* from its high of {fmt_usd(m['peak'])} ({peak_note})")
    L.append(sign_track_record())
    L.append(follow)
    if chain == "robinhood":
        L.append("⚠️ Robinhood Chain is brand new: many of its wallets are airdrop-farming bots, so wallet signals are weaker "
                 "here, and DexScreener's numbers can jump around.")
    L.append(f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
             f"[Explorer]({link(chain, 'token', base.get('address', addr))})")
    out = "\n".join(L)
    return out if len(out) <= 3900 else out[:3900].rsplit("\n", 1)[0]


def steady_text(chain: str, addr: str, pair: dict, m: dict) -> str:
    """The reply for a coin that barely moves (stock token, gold, wrapped coin, stable): no scorecard, and why."""
    base = pair.get("baseToken", {})
    sym = md_escape(str(base.get("symbol", "?")))
    return "\n".join([
        f"🔍 *${sym}*  |  {CHAINS[chain]['name']}", f"`{addr}`", "",
        "⚪ *NOT A RUNNER - it barely moves*", "",
        f"Its price has stayed inside a narrow band for its whole life (the highest day and the lowest day are within about "
        f"{(STEADY_BAND - 1) * 100:.0f}% of each other). That is how a stock token, gold, a wrapped coin or a stable coin behaves. "
        "Dips, bottoms, cup shapes and \"smart money\" don't apply to it, and the odd prices its thin pool shows now and then are bad data, "
        "not real moves - so I don't score it or follow it.", "",
        f"💰 Market cap *{fmt_usd(m['mc'])}* · liquidity {fmt_usd(m['liq'])}",
        f"[DexScreener](https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}) | "
        f"[Explorer]({link(chain, 'token', base.get('address', addr))})"])


def cmd_check(ref: str) -> str:
    """/check <coin address> (or just paste the address): the scorecard."""
    chain, addr, err = resolve_coin_ref(ref)
    if err:
        return err
    ctx = token_context(chain, addr)
    if not ctx:
        return (f"I couldn't find a live trading pool for that address on {CHAINS[chain]['name']}. "
                "If it is a wallet, add it with /watch instead.")
    pair, m, key = ctx["pair"], ctx["m"], ctx["key"]
    now = time.time()
    if ensure_steady(key, chain, pair):
        return steady_text(chain, addr, pair, m)
    c = chart_for_check(key, chain, pair, m, now)
    cap = buyers_for_check(key, chain, pair, m, now)
    held = watch_holders(chain, addr, num(pair, "priceUsd"))
    sc = score_coin(m, pair, c, bottom_entry(key), cap, held, wallet_scores())
    linked = ""
    try:
        linked = link_note(key, chain, pair, m, now)         # v3.8: the market cap you sent it at is remembered; sent before? what happened since
    except Exception:
        log.exception("link memory error")
    follow = follow_checked(key, chain, pair, m, sc, time.time())
    return scorecard_text(chain, addr, pair, m, key, sc, follow, linked)


def cmd_check_many(refs: list[str], notify=None):
    """One scorecard per coin address (at most CHECK_MAX_PER_MESSAGE). One reply, or a list of replies."""
    refs = refs[:CHECK_MAX_PER_MESSAGE]
    if notify:
        try:
            notify("🔎 Looking at " + ("it" if len(refs) == 1 else f"{len(refs)} coins")
                   + " now: price, chart, who is buying and your wallets. The first read can take up to a minute.")
        except Exception:
            log.exception("could not send the 'looking' note")
    out: list[str] = []
    for ref in refs:
        try:
            out.append(cmd_check(ref)[:3900])
        except Exception:
            log.exception("scorecard failed for %s", str(ref)[:20])
            out.append("Sorry - that coin hit a snag on my side. Try again in a minute.")
    return out[0] if len(out) == 1 else out


def scorecard_report_lines() -> list[str]:
    """For the report: did the scores of the coins you asked about mean anything?"""
    out: list[str] = []
    try:                                                    # v3.8: the coins you sent, with the market cap each was sent at
        items = [e for e in state["links"].values() if isinstance(e, dict) and num(e, "mc0") > 0]
        if items:
            hit = sum(1 for e in items if num(e, "max_mc") >= LINK_LOOK_X * e["mc0"])
            looked = sum(1 for e in items if e.get("look"))
            out.append(f"📌 Coins you sent me: *{len(items)}* remembered, {hit} reached {LINK_LOOK_X:g}x or more from the market cap you sent "
                       f"them at, {looked} had their wallets looked at (/links)")
    except Exception:
        log.exception("links report line failed")
    chk = closed_records("check")
    if not chk:
        return out
    parts = []
    for lab, lo, hi in (("7+ of 10", 7, 11), ("5 to 7", 5, 7), ("3 to 5", 3, 5), ("under 3", -1, 3)):
        rs = [r for r in chk if lo <= r.get("score", 0) < hi]
        if rs:
            x = summarize(rs)
            parts.append(f"{lab}: {x['w']}/{x['n']}")
    return out + (["🔍 Coins you asked about, by scorecard (won/followed): " + " | ".join(parts)] if parts else [])


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
    scores = wallet_scores()
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
        if d.get("vip") and d.get("chain") in CHAINS:
            chain = d["chain"]                  # v3.5: a VIP wallet on Robinhood Chain links to that chain's explorer
            info = vip_status_text(w, d) or info
        kind_name = "Solana" if chain == "solana" else "Robinhood Chain" if chain == "robinhood" else "Ethereum-style"
        tag = (" · 🚨 VIP" if d.get("tier") == "vip" else " · 🎯 low-cap hunter") if d.get("vip") else ""
        st = wallet_stars(w, scores)
        L.append(f"{i}. *{md_escape(wallet_label(w))}* · {kind_name} · "
                 f"[{short(w)}]({link(chain, 'addr', w)}) · {info}" + (" · 🧠 found by results" if d.get("auto") else "") + tag
                 + (f" · {stars_text(st)}" if st is not None else "") + (" · 🧊 cooling off" if book_grade(w, scores) == "cooling" else ""))
    out = chunk_lines(L)                       # (v3.8: more than one message when the list is long)
    return out[0] if len(out) == 1 else out


def alerts_state_line() -> str:
    on = [w for w, k, _ in ALERT_MENU if alert_on(k)]
    off = [w for w, k, _ in ALERT_MENU if not alert_on(k)]
    return ("🔔 Alerts on: " + (", ".join(on) if on else "none") + (" · off: " + ", ".join(off) if off else "")
            + " (/alerts to change)")


def cmd_alerts(args: list[str]) -> str:
    """/alerts - which alerts are switched on; /alerts <name> on|off changes one; /alerts reset goes back to the standard."""
    head = ""
    if args and args[0].lower() in ("reset", "default", "defaults", "standard"):
        state["tuning"].pop("alerts", None)
        save_state()
        args, head = [], "↩️ Back to the standard settings.\n\n"
    if len(args) >= 2:
        word, val = args[0].lower().strip(":,"), args[1].lower().strip(".,!")
        kind = ALERT_WORDS.get(word)
        if not kind or val not in ("on", "off", "yes", "no", "true", "false", "1", "0"):
            return ("Send it like this: /alerts rerun off   (names: " + ", ".join(w for w, _, _ in ALERT_MENU)
                    + "; then on or off). /alerts on its own shows the list.")
        on = val in ("on", "yes", "true", "1")
        set_alert(kind, on)
        label = next(t for w, k, t in ALERT_MENU if k == kind).split(" - ")[0].split(" ", 1)[1]
        note = ""
        if kind == "rerun" and not on:
            note = ("\nI still follow those setups quietly and score them (see /report), and a setup that wallets you trust "
                    "are holding still reaches you as a wallet-backed alert.")
        elif kind in ("bottom", "chart") and not on:
            note = "\nI still follow those setups quietly and score them (see /report)."
        head = f"{'🔔' if on else '🔕'} {label} alerts are now *{'ON' if on else 'OFF'}*.{note}\n\n"
    elif len(args) == 1:
        return "Send it like this: /alerts " + args[0] + " off   (or on)"
    return head + "🔔 *Alerts*  (change one with /alerts <name> on  or  /alerts <name> off)\n\n" + "\n".join(
        f"{'✅' if alert_on(k) else '❌'} `{w}` - {t}" for w, k, t in ALERT_MENU)


def cmd_data() -> str:
    """/data - send a fresh copy of the bot's memory file into the chat, to hand to Claude (or keep)."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return "I can't send files here: the Telegram settings are missing."
    blob = b""
    for _ in range(3):
        try:
            blob = json.dumps(state).encode()
            break
        except RuntimeError:        # the scan changed the memory while it was being copied: try again
            time.sleep(0.2)
    if not blob:
        return "I couldn't copy my memory just now - try again in a minute."
    name = "bot_data_" + time.strftime("%Y-%m-%d_%H%M", time.gmtime()) + ".json"
    try:
        tg_api("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": "📦 A copy of everything the bot has learned so far. "
                                     "Save it and attach it when you ask Claude what worked."},
               files={"document": (name, blob)})
    except requests.RequestException as e:
        return f"I couldn't send the file: {scrub(e)}"
    return f"📦 Sent: {name} ({len(blob) / 1024:.0f} KB). Save it, then attach it to your chat with Claude."


RESTORE_HINT = ("To restore my memory, send me the file right here in this chat (an attachment): the pinned `bot_memory.json`, "
                "one of the 🗄 safety copies I leave, or a file from /backup or /data. I only ADD what is missing - wallets, "
                "results, notes - and never delete or overwrite anything newer. Your /watch lines pasted in one message work too.")


def cmd_backup():
    """/backup (v3.4): the memory file now, plus the wallet list as lines you can paste back. A file sent back restores it."""
    blob = memory_blob()
    if not blob:
        return "I couldn't copy my memory just now - try again in a minute."
    sent = send_memory_copy(blob, "backup", "🗄 A copy of my memory made on request. Keep it. To restore, send this file back to me in this chat.")
    out = [(f"🗄 Sent my memory file ({len(blob) / 1024:.0f} KB). To restore from it, send it back to me in this chat - I only add "
            "what is missing and never delete anything.") if sent else "I couldn't send the file just now - try again in a minute."]
    lines = []
    for w, d in list(state["watch"].items()):
        name = re.sub(r"[`\r\n]+", " ", str((d or {}).get("name") or "")).strip()
        if isinstance(d, dict) and d.get("vip"):           # v3.5: a VIP wallet comes back as a VIP (v3.6: with the style word you gave it)
            sw = {"selective": " picky", "spray": " spray", "normal": " normal"}.get(d.get("style"), "")
            lines.append(f"/vip add {w} {name or (short(w) if sw else '')}".strip() + (" kol" if d.get("kol") else "") + sw)
        else:
            lines.append(f"/watch {w} {name}".strip())
    if lines:
        out.append(f"👛 Your {len(lines)} watched wallet(s) as lines. Tap a box to copy it, then paste it back to me in one "
                   "message to add them again:")
        for i in range(0, len(lines), MAX_PASTED_COMMANDS):
            out.append("```\n" + "\n".join(lines[i:i + MAX_PASTED_COMMANDS]) + "\n```")
    return out


def restore_from_file(doc: dict) -> str:
    """v3.4: the owner sent a file into the chat. If it is one of my memory files it is read in: whatever is missing is added,
    nothing is removed, and newer data is never replaced by older data."""
    global restore_pending
    name = str(doc.get("file_name") or "that file")
    if not name.lower().endswith(".json"):
        return "That doesn't look like one of my memory files (they end in .json). " + RESTORE_HINT
    if int(doc.get("file_size") or 0) > MAX_MEMORY_FILE_MB * 1024 * 1024:
        return "That file is too big for me to read (Telegram lets me read files up to 20 MB). I changed nothing."
    info = tg_api("getFile", json={"file_id": doc["file_id"]})
    r = http.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=60)
    r.raise_for_status()
    try:
        loaded = r.json()
    except ValueError:
        loaded = None
    if not isinstance(loaded, dict) or not any(isinstance(loaded.get(k), (dict, list)) for k in ("watch", "book", "wallets", "records")):
        return f"I couldn't read {md_escape(name)} as one of my memory files, so I changed nothing."
    if restore_pending:
        retry_restore()          # my own pinned copy may be readable again by now - that one comes first
    got = merge_state_live(loaded)
    restore_pending = False      # I have a memory to work from now, so saving is safe again
    state["tuning"]["ver"] = BOT_VERSION
    save_state()
    try:
        backup_state_to_telegram(True)
    except Exception as e:
        log.warning("Backup after a restore from a file failed: %s", scrub(e))
    return (f"✅ Restored from *{md_escape(name)}*. I only added what was missing and removed nothing.\n"
            f"👛 {got['watch']} wallet(s) added to the watch list (now {len(state['watch'])})\n"
            f"🧠 {got['book']} wallet(s) added to the wallet book (now {len(state['book'])})\n"
            f"📊 {got['records']} followed calls / results added (now {len(state['records'])})")


def handle_command(text: str, notify=None):
    """The reply for one chat message: a text, a list of texts, or None (nothing to say). notify = a way to send a quick
    'looking at it now' note before a slow answer."""
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    if 1 < len(lines) <= MAX_PASTED_COMMANDS and all(ln.startswith("/") for ln in lines):
        # v3.2: several commands pasted in one message (say, a block of /watch lines) - each line is its own command
        out: list = []
        for ln in lines:
            r = handle_command(ln, notify)
            out.extend(r if isinstance(r, list) else [r])
        return [x for x in out if x]
    parts = text.strip().split()
    if not parts:
        return None
    if not parts[0].startswith("/"):
        refs = extract_refs(text) if len(text) <= 1000 else []     # a pasted coin address (or two or three) is scored
        return cmd_check_many(refs, notify) if refs else None
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
    if cmd in ("stars", "star", "rating", "ratings"):
        return cmd_stars()
    if cmd == "coins":
        return cmd_coins()
    if cmd == "alerts":
        return cmd_alerts(args)
    if cmd in ("vip", "vips"):
        return cmd_vip(args)
    if cmd in ("radar", "accum", "accumulating", "accumulation"):
        return cmd_radar(args, notify)
    if cmd in ("lowcap", "low-cap", "lowcaps"):
        return cmd_lowcap(args)
    if cmd in ("links", "linked", "sent", "mycoins"):
        return cmd_links()
    if cmd in ("shill", "dipwatch"):
        return cmd_shill(args, notify)
    if cmd == "kol":
        return cmd_kol(args)
    if cmd == "data":
        return cmd_data()
    if cmd == "backup":
        return cmd_backup()
    if cmd == "restore":
        return RESTORE_HINT
    if cmd in ("patterns", "charts"):
        return cmd_patterns()
    if cmd == "buyers":
        return cmd_buyers(args[0]) if args else "Send it like this: /buyers <coin contract address>"
    if cmd == "check":
        if not args:
            return "Send it like this: /check <coin contract address>   (or just paste the address)"
        refs = extract_refs(" ".join(args))
        if not refs:
            return cmd_check(args[0])        # says what is wrong with it - no 'looking at it now' note for a typo
        return cmd_check_many(refs, notify)
    return "I don't know that command. Send /help to see what I understand."


def handle_update(u: dict):
    msg = u.get("message") or {}
    if str((msg.get("chat") or {}).get("id")) != str(TELEGRAM_CHAT_ID):
        return    # only the owner can give commands
    text = (msg.get("text") or "").strip()
    if not text:
        if isinstance(msg.get("document"), dict):      # v3.4: a file sent into the chat = a memory file to restore from
            try:
                note = restore_from_file(msg["document"])
            except Exception:
                log.exception("memory file could not be read")
                note = "Sorry - I couldn't read that file, and I changed nothing. Try again in a minute."
            send_telegram(note[:3900])
        return
    try:
        reply = handle_command(text, notify=send_telegram)
    except Exception:
        log.exception("chat command failed: %s", text.split()[0][:20])
        reply = "Sorry - that hit a snag on my side. Try again in a minute."
    for part in (reply if isinstance(reply, list) else [reply]):
        if part:
            send_telegram(part[:3900])
    if text.split()[0].split("@", 1)[0].lower() == "/report":
        try:
            extra = build_report_part2()
            if extra:
                send_telegram_long(extra)
        except Exception:
            log.exception("second part of the report failed")


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
            sorted((k, round(b.get("alerted", 0))) for k, b in state["bottoms"].items()),
            [len(state["book"]) // 25, sum(d.get("n", 0) for d in state["book"].values())]]
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
    if b.get("hash") == "":     # the first save after an update (or a late restore): first see what the file in the chat has gained
        merge_chat_file_before_first_save()
        h = core_hash()
    data, caption, old = memory_blob(), backup_caption(_hb_seen), b.get("msg_id")
    if not data:
        return              # (the scan kept changing the memory while it was being copied - nothing is sent, the next check tries again)
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


# --- v3.4: more ways to never lose the wallets and results ------------------------------------------------------------------
# 1) the pinned file (above) is the automatic copy; 2) before every update a dated safety copy of the OLD memory is left in
# the chat, and one more every week (never edited, never deleted); 3) a memory file sent into the chat is read back in
# (it only adds what is missing); 4) /backup sends the file and the wallet list on request; 5) if something else gets pinned
# over the memory file, it is pinned again; 6) the built-in wallet list in this file.
_pin_problem = {"other": False}     # at start-up something else (not my memory file) was pinned, so no memory file could be found
_fresh_start = {"on": False}        # at start-up the chat held no memory file at all (nothing pinned): I begin with only the built-in wallets
_restore_failing_since = 0.0        # since when the memory could not be read (0 = it can)
_restore_warned_at = 0.0
SAFETY_CAPTION_UPDATE = ("🗄 Safety copy: my memory exactly as it was BEFORE the update to v" + BOT_VERSION + ". I leave these here on "
                         "purpose and never delete them. If anything ever goes wrong, send this file back to me in this chat and "
                         "I'll restore from it.")
SAFETY_CAPTION_WEEKLY = ("🗄 Weekly safety copy of my memory (wallets, results, everything). I never delete these. If I ever lose my "
                         "memory, send the newest one back to me in this chat and I'll restore from it.")


def memory_blob() -> bytes:
    """The memory as a file's worth of bytes (b'' if the scan kept changing it while it was copied - just ask again later)."""
    for _ in range(3):
        try:
            return json.dumps(state).encode()
        except RuntimeError:        # the scan changed the memory while it was being copied: try again
            time.sleep(0.2)
    return b""


def send_memory_copy(blob: bytes, label: str, caption: str) -> bool:
    """Leave a dated copy of the memory in the chat. It is never edited and never deleted: it is the way back."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and blob):
        return False
    name = f"bot_memory_{label}_" + time.strftime("%Y-%m-%d", time.gmtime()) + ".json"
    try:
        tg_api("sendDocument", data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "disable_notification": "true"},
               files={"document": (name, blob)})
        return True
    except requests.RequestException as e:
        log.warning("Could not leave a safety copy of the memory in the chat: %s", scrub(e))
        return False


def restore_from_telegram() -> bool:
    chat = tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}
    pm = chat.get("pinned_message") or {}
    doc = pm.get("document")
    if not doc or not (pm.get("caption") or "").startswith(BACKUP_TAG):
        _pin_problem["other"] = bool(pm)     # something else is pinned: my memory file may be further up in the chat
        return False
    info = tg_api("getFile", json={"file_id": doc["file_id"]})
    r = http.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=60)
    r.raise_for_status()
    loaded = r.json()
    old_ver = (loaded.get("tuning") or {}).get("ver") if isinstance(loaded, dict) else None
    merge_state(loaded)
    _pin_problem["other"] = False
    state["backup"].update(msg_id=pm["message_id"], ts=time.time(), hash=core_hash())
    if old_ver != BOT_VERSION:
        state["backup"]["hash"] = ""         # the file in the chat is from another version: save this memory over it right at start-up
    state["tuning"]["ver"] = BOT_VERSION
    if old_ver != BOT_VERSION and SNAPSHOT_DAYS > 0:
        # an update: leave the memory exactly as the old version saved it, so there is always a way back
        if send_memory_copy(r.content, "before_v" + BOT_VERSION, SAFETY_CAPTION_UPDATE):
            state["backup"]["snap_t"] = time.time()
    return True


_pre_save_merge = {"done": False}   # (once per start-up)


def merge_chat_file_before_first_save():
    """The first save after an update: the copy of the bot that this one replaced saves its memory once more as it stops, a minute or
    two AFTER this one started. So before saving over the file in the chat, read it and add whatever it has gained that this memory
    does not have (it only adds - nothing here is removed or replaced), so even what the old copy learned in its last minutes is kept.
    Tried once per start-up. Any problem is skipped: the save then goes ahead exactly as before."""
    if _pre_save_merge["done"]:
        return
    _pre_save_merge["done"] = True
    b = state["backup"]
    if not b.get("msg_id"):
        return
    try:
        pm = (tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}).get("pinned_message") or {}
        doc = pm.get("document")
        if not doc or pm.get("message_id") != b["msg_id"] or not (pm.get("caption") or "").startswith(BACKUP_TAG):
            return
        info = tg_api("getFile", json={"file_id": doc["file_id"]})
        r = http.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{info['file_path']}", timeout=60)
        r.raise_for_status()
        loaded = r.json()
        if isinstance(loaded, dict):
            added = merge_state_live(loaded)
            if any(added.values()):
                log.info("Added what the previous copy saved while it was stopping: %s", added)
    except Exception as e:
        log.warning("Could not read the previous copy's last save (saving goes ahead): %s", scrub(e))


def startup_backup():
    """Save the memory now if it differs from what the pinned file holds (a new version number, new built-in wallets, or no file
    yet). Used after a memory that could not be read at start-up is finally read (minutes later, so the old copy is long gone);
    on a normal start the first regular backup does the same about 5 minutes after start-up."""
    if state["backup"].get("hash") != core_hash():
        backup_state_to_telegram(True)


def weekly_copy():
    """Leave a dated safety copy in the chat every SNAPSHOT_DAYS (the first one a period after a brand-new memory began)."""
    if SNAPSHOT_DAYS <= 0 or restore_pending or not (TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    b, now = state["backup"], time.time()
    if not b.get("snap_t"):
        b["snap_t"] = now
        return
    if now - b["snap_t"] >= SNAPSHOT_DAYS * 86400 and send_memory_copy(memory_blob(), "weekly", SAFETY_CAPTION_WEEKLY):
        b["snap_t"] = now


def repin_memory_file():
    """The chat's pinned message is not my memory file any more (something else was pinned, or it was unpinned). The pin is how I
    find my memory after an update, and Telegram shows me only the NEWEST pinned message. So: pin the file again; and if a newer
    message is pinned too (so my file stays hidden behind it), send a fresh copy - the newest message in the chat - and pin that.
    (If the file itself was deleted, a fresh one is sent and pinned.)"""
    b, now = state["backup"], time.time()
    mid = b.get("msg_id")
    if not mid or now - b.get("repin_t", 0) < 6 * 3600:
        return
    b["repin_t"] = now
    try:
        tg_api("pinChatMessage", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": mid, "disable_notification": True})
    except requests.RequestException as e:
        body = getattr(getattr(e, "response", None), "text", "") or ""
        if "not found" in body.lower():
            log.info("The pinned memory file is gone from the chat - sending a fresh one")
            b["msg_id"] = 0
            backup_state_to_telegram(True)
        else:
            log.warning("Could not pin the memory file again: %s", scrub(body or e))
        return
    try:
        seen = ((tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}).get("pinned_message") or {}).get("message_id")
    except requests.RequestException:
        seen = mid                            # (could not check: the next check, in 5 minutes, will tell)
    hidden = seen is not None and seen != mid
    if hidden:                                # a newer message is pinned too: my file still is not the one I would find
        try:
            b["msg_id"] = 0
            backup_state_to_telegram(True)    # a fresh file = the newest message in the chat, pinned
        except Exception as e:
            b["msg_id"] = b.get("msg_id") or mid
            log.warning("Could not send a fresh memory file: %s", scrub(e))
            return
        if b.get("msg_id") and b["msg_id"] != mid:
            try:
                tg_api("deleteMessage", json={"chat_id": TELEGRAM_CHAT_ID, "message_id": mid})
            except requests.RequestException:
                pass                          # (Telegram only lets a message be removed for 48 hours: a stale copy stays, harmless)
    if now - b.get("repin_note", 0) > 24 * 3600:
        b["repin_note"] = now
        send_telegram(
            ("📌 Another message was pinned in this chat after my memory file, and Telegram only shows me the newest pinned message - "
             "so I could not find my file. I sent a fresh copy of my memory and pinned it (your own pin stays pinned). "
             "If you pin something new later, I will do this again."
             if hidden else
             "📌 Something else was pinned over my memory file, so I pinned the file again. Please leave it pinned - "
             "it is how I get my wallets and results back after an update."))


def retry_restore():
    """If the memory couldn't be read at start-up (Telegram hiccup), keep trying - and don't overwrite it."""
    global restore_pending, _restore_failing_since, _restore_warned_at
    if not restore_pending:
        return
    now = time.time()
    try:
        if restore_from_telegram():
            log.info("Restored memory from Telegram backup on retry (%d tracked calls)", len(state["records"]))
        restore_pending = False
        _restore_failing_since = 0.0
        try:
            seed_watch_wallets()          # (the built-in wallets were added to the empty memory at start-up; this fills any gap)
            startup_backup()
        except Exception as e:
            log.warning("Backup after a late restore failed: %s", scrub(e))
    except Exception as e:
        log.warning("Memory restore still failing: %s", scrub(e))
        _restore_failing_since = _restore_failing_since or now
        if now - _restore_failing_since > 1800 and now - _restore_warned_at > 12 * 3600:
            _restore_warned_at = now
            send_telegram("⚠️ I can't read my saved memory right now. I am NOT saving anything new in the meantime, so the old "
                          "copy cannot be overwritten. This usually fixes itself within minutes. If it is still stuck later, "
                          "send me the newest `bot_memory` file here (the pinned one, or a safety copy) and I'll restore from it.")


def heartbeat_check():
    """Note on the pinned file that this copy is alive, and warn if ANOTHER copy is alive too."""
    global _hb_seen
    if not (TELEGRAM_BACKUP and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID) or restore_pending:
        return
    pm = (tg_api("getChat", json={"chat_id": TELEGRAM_CHAT_ID}) or {}).get("pinned_message") or {}
    caption = pm.get("caption") or ""
    if not pm.get("document") or not caption.startswith(BACKUP_TAG):
        repin_memory_file()     # v3.4: if I already have a memory file in this chat, pin it again (else: none pinned yet - the first one appears after the first scan)
        return
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
    if seconds >= 2 * 86400:
        return f"{seconds / 86400:.1f} days"       # (v3.4: calls are followed for days now - '252.0 h' is hard to read)
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
        best = closest_coins(rows) if alert_on("rerun") else []
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
        elif s["pool"] and alert_on("rerun"):
            L += ["", "No coin in the pool is large enough to examine closely yet - discovery keeps adding more."]
        if s["fails"] and alert_on("rerun"):
            top = sorted(s["fails"].items(), key=lambda kv: -kv[1])[:5]
            L += ["", "📋 *Why coins were skipped (how many broke each rule):*",
                  "; ".join(f"{fail_label(c)} ×{n}" for c, n in top)]
    if SPARSITY_FEED:
        L += ["", sparsity_status_line()]
    L += [chart_status_line(), book_status_line()]
    L += vip_status_lines()
    L += radar_status_lines()
    L += smart_coin_lines(fetch=with_wallets)
    sent = ", ".join(f"{n} {k}" for k, n in (("re-run", bot_stats["rerun"]), ("narrative", bot_stats["narrative"]),
                                              ("wallet-backed", bot_stats["walletdip"]), ("bottom", bot_stats["bottom"]),
                                              ("chart", bot_stats["chart"]), ("smart-wallet", bot_stats["smartbuy"]),
                                              ("VIP low-cap", bot_stats["vip"]), ("low-cap crowd", bot_stats["lowcap"]),
                                              ("dip-watch", bot_stats["kol"]), ("accumulation", bot_stats["radar"]),
                                              ("whale", bot_stats["whale"])))
    rules_line = (f"⚙️ Rules in use: past high of at least {fmt_usd(MIN_PEAK_MC)}, now down {T['retrace_min']:.0f}-"
                  f"{T['retrace_max']:.0f}% from it, liquidity at least {T['liq'] * 100:.0f}% and 24h volume at least "
                  f"{T['vol'] * 100:.0f}% of market cap, {T['traders']:.0f}+ trades in 24h, pair at least "
                  f"{MIN_PAIR_AGE_HOURS}h old" + (f" and at most {MAX_COIN_AGE_DAYS:g} days old." if MAX_COIN_AGE_DAYS > 0 else "."))
    if not alert_on("rerun"):
        rules_line = ("⚙️ Zipcoin re-run alerts are OFF: those setups are followed quietly and scored "
                      "(/alerts rerun on brings the alert back).")
    L += ["", alerts_state_line(), rules_line,
          f"📣 Alerts sent since this copy started: {sent}.",
          f"👀 Following now: {len(open_records(kind='call'))} calls, {len(open_records(kind='narrative'))} narrative, "
          f"{len(open_records(kind='wallet'))} wallet-backed, {len(open_records(kind='bottom'))} bottom, "
          f"{len(open_records(kind='chart'))} chart, {len(open_records(kind='smartbuy'))} smart-wallet, "
          f"{len(open_records(kind='vip'))} VIP, {len(open_records(kind='lowcap'))} low-cap crowd, {len(open_records(kind='kol'))} dip-watch, "
          f"{len(open_records(kind='radar'))} accumulation, "
          f"{sum(len(open_records(kind=k_)) for k_ in ('shadow', 'bshadow', 'cshadow', 'lshadow'))} experiments"
          + (f" ({extended_open()} of all these on extra time - a call that is still alive after its first week is followed on)" if extended_open() else "") + ".",
          *([f"📌 Coins you sent me: {len(link_open_keys())} followed in the background (/links)."] if link_open_keys() else []),
          (f"👛 Watching {len(state['watch'])} wallet(s) - /wallets to see them." if state["watch"]
           else "👛 Tip: add wallets you trust with /watch <wallet address> and I'll show whether they're still in each coin."),
          "ℹ️ No alert just means nothing met an alert's rules at once - that is by design.",
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
        try:
            weekly_copy()
        except Exception as e:
            log.warning("weekly safety copy failed: %s", scrub(e))
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
    # (The update is saved to the pinned file by the first backup, about 5 minutes after start-up: NOT at once, because the old copy
    # that this one replaces is still running for a minute or two and its last save, when it stops, must not land on top of this one.)
    show34 = not state["tuning"].get("note34")
    show35 = not state["tuning"].get("note35")
    show36 = not state["tuning"].get("note36")
    show37 = not state["tuning"].get("note37")
    show38 = not state["tuning"].get("note38")
    show381 = not state["tuning"].get("note381")
    log.info("Bot v%s copy %s starting at %s", BOT_VERSION, INSTANCE_ID, my_host())
    log.info("Tracking %d tokens across %s", len(TOKENS), sorted({c for c, _ in TOKENS}))
    log.info("EVM whale wallets configured: %d", len(EVM_WHALES))
    online_sent = await asyncio.to_thread(
        send_telegram_long,
        f"✅ Bot is online (v{BOT_VERSION}, copy {INSTANCE_ID}). "
        + ("Auto-discovery ON. " if AUTO_DISCOVER else "")
        + (f"Manually watching {len(TOKENS)} coin(s). " if TOKENS else "")
        + f"Learning ON (self-tuning {'ON' if AUTO_TUNE else 'OFF'}).\n"
        + alerts_state_line() + "\n"
        + (f"😴 Stay-awake ON: it visits {md_escape(my_host())} every {KEEPALIVE_SECONDS // 60} min.\n"
           if PUBLIC_URL else
           "😴 Stay-awake OFF (no web address found) - on Render's free plan it will sleep when idle.\n")
        + (f"📋 First status check-in in about {FIRST_CHECKIN_MINUTES} min, then every {CHECKIN_HOURS:g}h.\n"
           if CHECKIN_HOURS > 0 else "")
        + (f"📉 Looking for coins {T['retrace_min']:.0f}-{T['retrace_max']:.0f}% below a past high of {fmt_usd(MIN_PEAK_MC)}+"
           + (f", at most {MAX_COIN_AGE_DAYS:g} days old" if MAX_COIN_AGE_DAYS > 0 else "") + ".\n"
           if alert_on("rerun") else "")
        + (f"🧭 Bottomed-coin list ON (Sparsity, checked every {SPARSITY_REFRESH_HOURS:g}h): I follow every coin on its map "
           f"(3 of 3 down to 0 of 3), alert at {BOTTOM_MIN_SIGNS} of 3 (or 2 of 3 with backing), and look at who is buying each one.\n"
           if SPARSITY_FEED else "")
        + ("📈 Chart reader ON (cup & handle, bottom base, double bottom). 🧠 Wallet book ON: every accumulating wallet is saved "
           "and scored by results.\n" if CHARTS else "")
        + (f"🧹 Set aside {OLD_RESULTS_SET_ASIDE} old result(s) from earlier versions (they could be fooled by price glitches).\n"
           if OLD_RESULTS_SET_ASIDE else "")
        + ("🆕 v3.1: coins that barely move (stock tokens, gold, wrapped coins) are now left out, and when many saved wallets "
           "bought the same coin it counts as one piece of evidence, not many.\n" if not state["steady_seen"] else "")
        + (f"🆕 I'm now following {len(STARTER_ADDED)} wallet(s) from the built-in list (they bought the DOPE or baton dips and have a good "
           "30-day record; /wallets shows them, /unwatch removes one). Several /watch lines can be pasted in one message"
           + (f", and I send at most {WATCH_MOVES_MAX_PER_DAY} 'wallet bought / sold' messages a day" if WATCH_MOVES_MAX_PER_DAY > 0 else "")
           + (f" (no more than {WATCH_MOVES_PER_WALLET} from any one wallet)" if WATCH_MOVES_PER_WALLET > 0 and WATCH_MOVES_MAX_PER_DAY > 0
              else f", and at most {WATCH_MOVES_PER_WALLET} 'wallet bought / sold' messages a day from any one wallet" if WATCH_MOVES_PER_WALLET > 0
              else "")
           + ".\n" if STARTER_ADDED else "")
        + (f"🆕 v3.4: a wallet's buy is now followed for {span_text(WALLET_TRACK_HOURS)} and every other call for "
           f"{span_text(TRACK_HOURS)} (it was 3 days: some coins take longer to cook). When several of your watched wallets keep "
           "adding to a coin over days, even in small steps, I'll tell you. Your wallets and results are kept safe through every "
           "update: "
           + (f"I leave a dated safety copy of my memory in this chat before each update and every {SNAPSHOT_DAYS:g} days, "
              if SNAPSHOT_DAYS > 0 else "")
           + "any `bot_memory` file you send back here is restored, and /backup sends you the file and your wallet list.\n"
           if show34 else "")
        + (vip_startup_note() if show35 else "")
        + (startup_note36() if show36 else "")
        + (startup_note37() if show37 else "")
        + (startup_note38() if show38 else "")
        + (startup_note381() if show381 else "")
        + ("⚠️ I couldn't find my memory file: something else is pinned in this chat instead of it. If I had wallets or results saved "
           "here before, scroll up, find the newest `bot_memory` file and send it to me here (or pin it again) and I'll restore from it.\n"
           if _pin_problem["other"] else "")
        + ("🆕 Fresh start: I found no saved memory in this chat, so I'm starting with only the built-in wallets. If I had wallets or "
           "results saved here before, scroll up, find the newest `bot_memory` file (any safety copy works too) and send it to me here - "
           "I'll add everything back.\n"
           if _fresh_start["on"] and not restore_pending else "")
        + (f"👛 Watching {len(state['watch'])} wallet(s). " if state["watch"] else "")
        + ("Send /help to see the chat commands (e.g. /watch to add a wallet, /check to look at a coin).\n"
           if COMMANDS else "")
        + "Paste any coin address and I'll score it; /coins shows what the saved wallets are buying; /alerts switches alerts on and off.",
    )
    if show34 and online_sent:
        state["tuning"]["note34"] = True       # (the v3.4 note is only shown once)
    if show35 and online_sent:
        state["tuning"]["note35"] = True       # (...and so is the v3.5 note)
    if show36 and online_sent:
        state["tuning"]["note36"] = True       # (...and the v3.6 note)
    if show37 and online_sent:
        state["tuning"]["note37"] = True       # (...and the v3.7 note)
    if show38 and online_sent:
        state["tuning"]["note38"] = True       # (...and the v3.8 note)
    if show381 and online_sent:
        state["tuning"]["note381"] = True      # (...and the v3.8.1 note)
    tasks = [asyncio.create_task(scanner_loop()), asyncio.create_task(keepalive_loop()),
             asyncio.create_task(wallet_loop()), asyncio.create_task(command_loop()),
             asyncio.create_task(vip_loop())]
    if RADAR_ON:
        tasks.append(asyncio.create_task(radar_loop()))      # v3.6: the accumulation radar
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
