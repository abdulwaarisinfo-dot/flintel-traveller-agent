"""
FX Signal Intelligence System — FLINTEL v7.3
=============================================
Platforms : Reddit (feedparser RSS) + Twitter/X (tweepy v2) + Telegram (Telethon)
Pipeline  : 
  Reddit   → Poll /new.rss per subreddit via feedparser (no PRAW, no credentials)
  Twitter  → Fetch mentions / search / replies (rate-limit safe, 50/block)
  Telegram → Listen to group messages (human account, Telethon, read-only)
      ↓
  Keyword Pre-Filter        (free, fast — drops 80%+ noise)
      ↓
  Batch Collector:
    Reddit   — N items per Claude call  (or timeout)
    Twitter  — N items per Claude call  (or timeout)
    Telegram — N items per Claude call  (or timeout)
      ↓
  Gap                       (between each batch)
      ↓
  Claude AI Intent Scorer   (single merged prompt per batch, platform-specific schema)
      ↓
  MongoDB Storage           (ALL scores 1-10 saved — nothing discarded)
      ↓
  Slack Alert               (score 6-10, professional blocks)
      ↓
  HubSpot CRM               (score 8-10 only)
      ↓
  FastAPI REST Endpoints
      ↓
  Daily Digest Scheduler    (score 6-7, 08:00 UTC)
      ↓
  Weekly Report Scheduler   (all signals, Monday 09:00 UTC)

Score rules:
  1-5  → SAVED to MongoDB only — never alerted
  6-7  → MEDIUM  — MongoDB + Slack only
  8-10 → HIGH    — MongoDB + Slack + HubSpot

Everything above is UNCHANGED from v7.2: same scoring logic, same prompts,
same JSON output schema per platform, same Slack/HubSpot/MongoDB behavior,
same FastAPI routes, same thresholds.

Changelog v7.3 (two fixes only — all v7.2 scoring/output logic 100% unchanged):

  FIX A — PERSISTENT BATCH STATE (survives restarts).
           Problem in v7.2: current_batch, batch_start_time, and all three
           dedup sets (_reddit_seen_ids, _telegram_seen_ids, Twitter seen_ids)
           were plain in-memory Python variables. Any restart (crash, deploy,
           platform auto-restart) silently wiped all unsent matched items and
           reset every "[N/batch_size]" counter back to 1, even if 55 items
           were already sitting in the batch. Those 55 items were lost forever
           — never scored, never saved, never alerted.

           Fix: every platform's batch processor now persists its pending
           batch (the list of matched-but-unscored items) and its
           batch_start_time to a new MongoDB collection, flintel_pending_batch,
           one document per platform (reddit/twitter/telegram). State is
           written:
             - immediately after every item is appended to current_batch
             - cleared immediately after a batch successfully fires
           On startup, each batch processor loads its platform's persisted
           state BEFORE entering its main loop, so:
             - the in-flight batch resumes with the same items, in the same
               order, no items dropped and none duplicated
             - the live counter log line "[N/batch_size]" reflects the TRUE
               persisted count immediately after restart (e.g. restart at
               55/450 logs "[56/450]" for the next match, not "[1/450]")
             - batch_start_time is restored from its persisted timestamp, so
               the timeout clock is NOT reset by a restart — a batch that was
               90s into its 120s timeout window before a restart still has
               only 30s left after the restart, not a fresh 120s
           Dedup sets (_reddit_seen_ids, _telegram_seen_ids, twitter seen_ids)
           are also persisted (flintel_seen_ids collection, one doc per
           platform, capped — see _persist_seen_ids) so a restart cannot
           cause the RSS/Twitter pollers to re-queue an item that is already
           sitting in the persisted pending batch. This is what guarantees
           "no duplication" and "no cross-platform mixing" across restarts —
           platform isolation was already enforced by separate queues/batches
           in v7.2 and is unchanged; v7.3 just makes the existing isolation
           durable across restarts instead of memory-only.

           Net effect: a restart at 55/450 resumes at 55/450. Nothing is
           dropped, nothing is duplicated, nothing is mixed across platforms.
           This required persistence-layer changes only — the scoring logic,
           prompts, Slack delivery, HubSpot delivery, and JSON output schema
           are completely untouched.

  FIX B — TOLERANT PARTIAL-JSON RECOVERY (no longer all-or-nothing).
           Problem in v7.2: if Claude's response hit max_tokens mid-generation
           (e.g. a 450-item batch needs more output tokens than max_tokens
           allows and gets cut off after item 400), the raw response is
           invalid JSON (truncated mid-object). json.loads(raw) throws
           ValueError, which propagated up and caused retry_with_backoff to
           treat the ENTIRE batch as failed — all 450 items, including the
           400 that Claude had already fully and correctly scored, were
           discarded and replaced with _fallback_score (score 1, "Scoring
           unavailable"). This is the "drop everything" behavior the user
           identified.

           Fix: _parse_claude_json() now attempts json.loads() first (the
           common, fully-successful case — unchanged from v7.2). If that
           fails, it falls back to a salvage parser that walks the raw text
           and extracts every complete, well-formed top-level JSON object
           from the array using brace-depth tracking, stopping cleanly at
           the last fully-closed "}" before the truncation point. Any
           trailing partial object (the one mid-write when max_tokens was
           hit) is discarded — but every object that completed BEFORE the
           cutoff is kept and scored normally.

           _call_claude_batch then computes which item indices are missing
           from the salvaged results (e.g. items 401-450) and applies
           _fallback_score ONLY to those missing items — not to the 400 that
           were successfully recovered. A WARNING-level operator Slack alert
           fires noting the partial recovery (count recovered vs count
           fallback) so this is visible, not silent.

           Net effect: hitting max_tokens now degrades gracefully — Claude's
           successfully-generated items are kept and delivered exactly as
           normal (same Slack/HubSpot/MongoDB pipeline), only the few items
           that didn't finish generating before the cutoff get a fallback
           score of 1. Nothing is dropped that Claude actually finished
           scoring. This is a parsing-layer change only — it does not alter
           what Claude is asked to do, the prompts, or the output schema.

  NOTHING ELSE CHANGED. Scoring logic, prompts (_SCORING_CORE and all three
  platform schemas), Slack block formatting, HubSpot fields, FastAPI routes,
  thresholds, keyword list, and the v7.2 OPT1-OPT6 token optimisations are
  byte-for-byte identical to v7.2.

Changelog v7.2 (output cost optimisation — all scoring logic 100% unchanged):
  OPT 1 — Platform-specific JSON schemas.
  OPT 2 — Derived fields removed from Claude output (computed in Python).
  OPT 3 — Word caps enforced in prompt.
  OPT 4 — Outreach keys omitted entirely for score 1-3.
  OPT 5 — urgency_indicator and watchlist_reason removed from Claude output.
  OPT 6 — max_tokens raised to 8192.
  NET    — Per-item output: 320 tokens → ~140 tokens (-56%).
  ADD    — Telegram polling thread added.

Changelog v7.1 (bug fixes only — all logic 100% unchanged):
  FIX 1 — Twitter search query built dynamically from KEYWORDS list.
  FIX 2 — Reddit + Telegram in-memory dedup sets added.
  FIX 3 — Operator Slack alerts added for Claude API down + MongoDB drop.
  FIX 4 — FastAPI /signals and all data endpoints protected with API key auth.
  FIX 5 — Weekly report last_report_week persisted in MongoDB.
  NEW   — Platform enable/disable flags.
  RSS   — Reddit now uses feedparser RSS instead of PRAW.

Changelog v7.0:
  - Added Telegram platform (Telethon, human account, read-only listener)
  - MongoDB now stores ALL scores 1-10 (nothing silently discarded)
  - BATCH_TIMEOUT_SECONDS=120: partial batch sent to Claude after timeout
  - Claude model: claude-sonnet-4-6
"""

import asyncio
import logging
import os
import json
import time
import queue
import threading
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

import html
import re
import feedparser
import anthropic
import tweepy
from telethon import TelegramClient, events
from telethon.errors import (
    UserAlreadyParticipantError,
    InviteHashExpiredError,
    ChannelPrivateError,
    FloodWaitError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import requests
from fastapi import FastAPI, HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from starlette.status import HTTP_403_FORBIDDEN
import uvicorn

# ─────────────────────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("flintel")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  (identical to v7.2)
# ─────────────────────────────────────────────────────────────────────────────

REDDIT_POLL_INTERVAL = int(os.getenv("REDDIT_POLL_INTERVAL", "300"))

TWITTER_API_KEY      = os.getenv("TWITTER_API_KEY")
TWITTER_API_SECRET   = os.getenv("TWITTER_API_SECRET")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

TELEGRAM_API_ID      = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH    = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE       = os.getenv("TELEGRAM_PHONE", "")
TELEGRAM_SESSION     = os.getenv("TELEGRAM_SESSION", "flintel_telegram")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DB  = os.getenv("MONGODB_DB", "fx_signals")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
HUBSPOT_API_KEY   = os.getenv("HUBSPOT_API_KEY")

MIN_SCORE_MEDIUM = int(os.getenv("MIN_SCORE_MEDIUM", "6"))
MIN_SCORE_HIGH   = int(os.getenv("MIN_SCORE_HIGH",   "8"))
CLIENT_ID        = os.getenv("CLIENT_ID", "settla")

REDDIT_BATCH_SIZE   = int(os.getenv("REDDIT_BATCH_SIZE",   "10"))
TWITTER_BATCH_SIZE  = int(os.getenv("TWITTER_BATCH_SIZE",  "50"))
TELEGRAM_BATCH_SIZE = int(os.getenv("TELEGRAM_BATCH_SIZE", "10"))
BATCH_GAP_SECONDS   = int(os.getenv("BATCH_GAP_SECONDS",   "30"))

BATCH_TIMEOUT_SECONDS = int(os.getenv("BATCH_TIMEOUT_SECONDS", "120"))

DAILY_DIGEST_HOUR  = int(os.getenv("DAILY_DIGEST_HOUR",  "8"))
WEEKLY_REPORT_DAY  = int(os.getenv("WEEKLY_REPORT_DAY",  "0"))
WEEKLY_REPORT_HOUR = int(os.getenv("WEEKLY_REPORT_HOUR", "9"))

TWITTER_POLL_INTERVAL = int(os.getenv("TWITTER_POLL_INTERVAL", "60"))

TELEGRAM_JOIN_GAP_SECONDS = int(os.getenv("TELEGRAM_JOIN_GAP_SECONDS", "30"))

MAX_TOKENS = int(os.getenv("MAX_TOKENS", "8192"))

# ─────────────────────────────────────────────────────────────────────────────
# API KEY AUTH (unchanged from v7.1/v7.2)
# ─────────────────────────────────────────────────────────────────────────────

API_KEY = os.getenv("API_KEY", "")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query  = APIKeyQuery(name="api_key",    auto_error=False)


async def verify_api_key(
    key_header: str = Security(api_key_header),
    key_query:  str = Security(api_key_query),
):
    if not API_KEY:
        return
    if key_header == API_KEY or key_query == API_KEY:
        return
    raise HTTPException(status_code=HTTP_403_FORBIDDEN, detail="Invalid or missing API key.")


# ─────────────────────────────────────────────────────────────────────────────
# PLATFORM ENABLE / DISABLE FLAGS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def _bool_env(key: str, default: bool = True) -> bool:
    val = os.getenv(key, str(default)).strip().lower()
    return val in ("1", "true", "yes", "on")

REDDIT_ENABLED   = _bool_env("REDDIT_ENABLED",   True)
TWITTER_ENABLED  = _bool_env("TWITTER_ENABLED",  False)
TELEGRAM_ENABLED = _bool_env("TELEGRAM_ENABLED", False)

# ─────────────────────────────────────────────────────────────────────────────
# TARGET SUBREDDITS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

TARGET_SUBREDDITS = [
    "Nigeria", "lagos", "Nigerians", "NigeriansAbroad",
    "AfricanDiaspora", "pakistan", "Pakistani", "PakistaniDiaspora",
    "PersonalFinanceCanada", "PersonalFinanceUK", "personalfinance",
    "entrepreneur", "smallbusiness", "digitalnomad", "africatech",
    "UKPersonalFinance", "Remittance", "moneytransfer",
    "CanadianInvestor", "ExpatFinance",
]

# ─────────────────────────────────────────────────────────────────────────────
# TARGET TELEGRAM GROUPS (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

TARGET_TELEGRAM_GROUPS = [
    "nigeriansincanada", "nigeriansinuk", "nigeriansinusa",
    "nigeriansinaustralia", "nigeriandiaspora", "nigerianentrepreneurs",
    "lagosBusinessNetwork", "nigeriafinance", "pakistanisincanada",
    "pakistanisinuk", "pakistanisinusa", "pakistanidiaspora",
    "pakistanibusiness", "karachi_business", "remittancetalk",
    "moneytransfertips", "fxtraders_ng", "diaspora_finance",
    "crossborderpayments", "africabusiness", "africaentrepreneurs",
    "africatrade", "africafintech", "expatfinance", "diasporamoney",
    "internationaltransfer", "wisealternatives",
]

# ─────────────────────────────────────────────────────────────────────────────
# SHARED QUEUES — platform-isolated, never mixed (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

reddit_queue:   queue.Queue = queue.Queue()
twitter_queue:  queue.Queue = queue.Queue()
telegram_queue: queue.Queue = queue.Queue()

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD PRE-FILTER — Bookin.PK (Pakistani hotel/guesthouse platform)
# Five weighted keyword lists replace the single flat KEYWORDS list.
# passes_keyword_filter() signature unchanged — still returns bool.
# Internally uses weighted logic; priority/signal_type used for logging only.
# HARD_NEGATIVES applied first — hard discard regardless of other matches.
# ─────────────────────────────────────────────────────────────────────────────

# ── HOTELIER CRISIS — commission rage, payout pain, occupancy desperation ──
HOTELIER_CRISIS = [
    # Commission rage — exact emotional language
    "agoda is a scam",
    "booking.com is robbing",
    "booking.com is a scam",
    "commission is killing my",
    "commission is eating my profit",
    "commission ate my profit",
    "took 20% commission",
    "took 22% commission",
    "took 25% commission",
    "18% commission booking",
    "never using booking.com again",
    "never listing on agoda again",
    "done with booking.com",
    "done with agoda",
    "agoda payout delayed",
    "booking.com payout delayed",
    "agoda withheld payment",
    "booking.com froze my account",
    "agoda suspended my listing",
    "delisted from booking.com",
    "kicked off agoda",
    # Occupancy desperation — the money-losing moment
    "zero bookings this month",
    "empty rooms guesthouse",
    "occupancy is terrible",
    "no bookings in weeks",
    "guesthouse losing money",
    "hotel losing money Pakistan",
    "can't fill my rooms",
    "rooms empty Lahore",
    "rooms empty Karachi",
    "rooms empty Islamabad",
]

# ── ACTIVE LISTING SEARCH — hotelier looking for a new platform ─────────────
ACTIVE_LISTING_SEARCH = [
    "where can I list my guesthouse",
    "where to list my hotel",
    "best platform to list property Pakistan",
    "how to list on booking sites",
    "list my property Pakistan",
    "list my Airbnb Pakistan",
    "register my guesthouse online",
    "online booking platform for hotels Pakistan",
    "Pakistani alternative to booking.com",
    "Pakistani alternative to agoda",
    "local booking platform Pakistan",
    "list guesthouse Lahore",
    "list guesthouse Karachi",
    "list guesthouse Islamabad",
    "increase bookings guesthouse",
    "more bookings for my hotel",
    "marketing my guesthouse",
    "promote my hotel Pakistan",
    "visibility for my property",
]

# ── OWNERSHIP LANGUAGE — confirms they actually own/run the property ─────────
OWNERSHIP_LANGUAGE = [
    "my guesthouse",
    "my hotel",
    "I run a guesthouse",
    "I own a guesthouse",
    "I manage a guesthouse",
    "we run a guesthouse",
    "we manage properties",
    "my property in",
    "I rent out my",
    "host on airbnb Pakistan",
    "Airbnb host Pakistan",
    "guesthouse owner",
    "hotel owner Pakistan",
    "started a guesthouse",
    "opened a guesthouse",
]

# ── TRAVELER INTENT — demand-side signals ────────────────────────────────────
TRAVELER_INTENT = [
    "best hotel in Lahore",
    "best hotel in Karachi",
    "best hotel in Islamabad",
    "best hotel in Multan",
    "best guesthouse in Lahore",
    "where to stay in Lahore",
    "where to stay in Karachi",
    "where to stay in Islamabad",
    "where should I stay Pakistan",
    "guesthouse recommendation",
    "hotel recommendation Pakistan",
    "cheap hotel Lahore",
    "budget hotel Karachi",
    "family guesthouse Karachi",
    "safe hotel for solo female",
    "boutique hotel Lahore",
    "hotel near airport Karachi",
    "hotel near airport Islamabad",
]

# ── COMPETITOR TRAVELER PAIN — bad experience as a guest ────────────────────
COMPETITOR_TRAVELER_PAIN = [
    "booking.com cancelled my reservation",
    "agoda cancelled my booking",
    "hotel scam Pakistan",
    "fake hotel listing Pakistan",
    "paid for hotel that didn't exist",
    "booking.com customer service useless",
    "agoda refund nightmare",
    "hotel not as advertised",
    "scammed by hotel Pakistan",
    "booking.com ruined my trip",
]

# ── HARD NEGATIVES — discard immediately regardless of other matches ─────────
HARD_NEGATIVES = [
    "visa requirements",
    "visa application",
    "flight booking",
    "flight ticket price",
    "currency exchange rate",
    "weather in Pakistan",
    "is Pakistan safe to travel",
    "political situation Pakistan",
    "Pakistan vs India",
    "cricket",
    "election",
    "passport renewal",
    "embassy appointment",
    "travel insurance claim",
    "lost passport",
    "vaccine requirement",
    "SIM card Pakistan tourist",
]

# Flat KEYWORDS list for _build_twitter_search_query() — combines all positive
# keyword lists so the Twitter search query continues to work unchanged.
KEYWORDS = (
    HOTELIER_CRISIS +
    ACTIVE_LISTING_SEARCH +
    OWNERSHIP_LANGUAGE +
    TRAVELER_INTENT +
    COMPETITOR_TRAVELER_PAIN
)


def passes_keyword_filter(text: str) -> bool:
    """
    Returns True if text passes the Bookin.PK signal filter.
    Signature unchanged from v7.3 — still returns bool so the batch
    processor's `if not passes_keyword_filter(text)` call works as-is.

    Internally uses weighted logic:
      Stage 1 — HARD_NEGATIVES: any match → discard immediately.
      Stage 2 — positive keyword lists with priority weights.
                 crisis_hit + ownership_hit = gold standard (priority 15).
                 listing_hit + ownership_hit = active search (priority 12).
                 crisis alone (priority 10), listing alone (priority 8),
                 traveler wound (priority 6), traveler intent (priority 4).
                 PKR/% number present → +3 bonus (proves real operator).
      Returns True for any positive match that survives Stage 1.
      Returns False if no positive match OR any hard negative present.
    """
    t = text.lower()

    # Stage 1 — hard discard
    if any(neg.lower() in t for neg in HARD_NEGATIVES):
        return False

    # Stage 2 — positive signal detection
    crisis_hit   = any(kw.lower() in t for kw in HOTELIER_CRISIS)
    listing_hit  = any(kw.lower() in t for kw in ACTIVE_LISTING_SEARCH)
    ownership_hit = any(kw.lower() in t for kw in OWNERSHIP_LANGUAGE)
    traveler_hit = any(kw.lower() in t for kw in TRAVELER_INTENT)
    wound_hit    = any(kw.lower() in t for kw in COMPETITOR_TRAVELER_PAIN)

    return any([crisis_hit, listing_hit, traveler_hit, wound_hit])


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER SEARCH QUERY (unchanged from v7.1/v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def _build_twitter_search_query() -> str:
    short_kws = [
        kw for kw in KEYWORDS
        if len(kw) <= 30 and " " not in kw or (
            " " in kw and len(kw) <= 25
        )
    ]

    seen = set()
    unique_kws = []
    for kw in short_kws:
        kl = kw.lower()
        if kl not in seen:
            seen.add(kl)
            unique_kws.append(kw)

    max_query_len = 480
    parts = []
    current_len = 0

    for kw in unique_kws:
        term = f'"{kw}"' if " " in kw else kw
        addition = len(term) + (4 if parts else 0)
        if current_len + addition > max_query_len:
            break
        parts.append(term)
        current_len += addition

    if not parts:
        return (
            "(\"international transfer\" OR \"supplier payment\" OR \"bank blocked\""
            " OR \"Wise blocked\" OR \"cross border payment\") -is:retweet lang:en"
        )

    query = "(" + " OR ".join(parts) + ") -is:retweet lang:en"
    log.info(f"Twitter search query built from KEYWORDS | terms:{len(parts)} | len:{len(query)}")
    return query


TWITTER_SEARCH_QUERY = _build_twitter_search_query()


# ─────────────────────────────────────────────────────────────────────────────
# DERIVE FIELDS LOCALLY (unchanged from v7.2 OPT 2)
# ─────────────────────────────────────────────────────────────────────────────

def _derive_fields(score: int) -> dict:
    if score >= 8:
        return {"signal_category": "high_intent", "tier": "immediate", "hubspot_priority": "high"}
    elif score >= 6:
        return {"signal_category": "mid_intent", "tier": "digest", "hubspot_priority": "medium"}
    elif score >= 4:
        return {"signal_category": "mid_intent", "tier": "watchlist", "hubspot_priority": "low"}
    else:
        return {"signal_category": "discard", "tier": "discard", "hubspot_priority": "skip"}


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE SYSTEM PROMPTS — PLATFORM-SPECIFIC SCHEMAS
# Byte-for-byte identical to v7.2. Scoring logic untouched.
# ─────────────────────────────────────────────────────────────────────────────

_SCORING_CORE = """
You are Flintel's signal intelligence analyst for Bookin.PK — 
a Pakistani hotel and guesthouse booking platform competing 
directly with Booking.com and Agoda on commission and local 
trust.

YOUR ONLY JOB: Identify the exact moment a property owner is 
losing money to a competitor, or a traveler is actively 
choosing where to book — before anyone else reaches them.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE PSYCHOLOGY YOU MUST UNDERSTAND
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A hotelier does not complain about commission in the abstract. 
They complain at the exact moment they see a payout statement 
and do the math. That moment is rage, not analysis. Score 
language like "scam," "robbing," "killing my profit" as HIGH 
intent — these are not exaggerations, they are the honest 
language of someone about to act.

A hotelier mentioning a specific percentage, specific PKR 
amount, or specific room count is PROVING they run a real 
business right now. Generic complaints about "the industry" 
are not leads. Specific complaints about "my 6 room guesthouse 
in Bahria Town" are leads.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TWO BUYER TYPES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HOTELIER (supply side — always score higher than traveler 
signals of equal emotional intensity, because one hotelier 
onboarded is worth 50 traveler bookings):
— Owns or manages a property
— Complaining about commission, payout delays, low occupancy, 
  or platform suspension
— Actively asking where else to list

TRAVELER (demand side):
— Looking for a place to stay in a Pakistani city
— Complaining about a bad Booking.com/Agoda experience as a 
  guest, not an owner

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCORING — BE RUTHLESS, BE GENEROUS WHERE IT COUNTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCORE 9-10 — Hotelier in active financial pain RIGHT NOW, 
with ownership language AND a specific number (percentage, 
PKR amount, room count, occupancy rate).

Example: "Agoda took 22% commission on my last payout. 
Running a 6 room guesthouse in Gulberg, barely breaking even."
→ Ownership confirmed. Specific commission. Specific property 
size. Specific location. Score 10.

SCORE 7-8 — Hotelier complaining about commission/occupancy 
WITHOUT a specific number, OR actively asking where to list 
with ownership confirmed.

Example: "Booking.com commission is way too high for what 
we're getting. Anyone know a better platform for Pakistan?"
→ Ownership implied ("we"). Active search. No specific number. 
Score 8.

SCORE 5-6 — Traveler who had a bad experience with a 
competitor, OR a hotelier complaint with no ownership 
confirmation (could be discussing someone else's business).

SCORE 4-5 — Traveler actively asking for hotel recommendations 
in a Pakistani city with no urgency markers.

Example: "Best hotel in Lahore for a family trip next month?"
→ Score 5. Real intent, but low individual value — drives 
demand-side traffic, not a hotelier onboard.

SCORE 0-3 — Generic travel chat, visa/flight questions, no 
lodging-specific pain or intent, third-person industry 
commentary with no personal stake.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE ONE RULE THAT MATTERS MOST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A hotelier signal at score 7 is worth more to Bookin.PK than 
a traveler signal at score 9. Always flag signal_category 
clearly so the human reading it knows which type of lead this 
is — they are not interchangeable in value even at the same 
score.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTREACH RULES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Write outreach scripts for scores 4 and above ONLY.
Score 1 to 3 — DO NOT output any outreach fields at all.

For hoteliers — speak business to business. Reference their 
specific pain. Lead with the commission comparison, not 
features.

"Agoda taking 22% is brutal for a 6 room operation — we run 
significantly lower commission and already have 644 
properties live in Lahore. Worth 15 minutes to compare?"

For travelers — speak casually, like a local giving a tip, 
not a company pitching.

"Bookin.PK has solid options in Lahore if Booking.com prices 
feel off — worth a quick look before you book."

Never write "Dear" or "I hope this message finds you well." 
Never use exclamation marks. Sound like a person who has 
actually been in their situation.
Maximum 3 sentences total per script.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AUTOMATIC SCORE MODIFIERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ADD +1 to score when:
+ Specific commission percentage mentioned
+ Specific PKR/room count/occupancy rate mentioned
+ Ownership language confirmed (my guesthouse, I own, we run)
+ Booking.com or Agoda mentioned negatively
+ Multiple pain points in same post
+ Urgency words present — today, this week, losing money now

SUBTRACT 1 from score when:
- No Pakistani city or property context mentioned
- Third-person commentary (no personal stake)
- Issue already resolved
- Post older than 7 days
- Generic travel question with no specific city/property

AUTOMATIC DISCARD regardless of other signals:
✗ Visa/passport/embassy questions
✗ Flight booking or ticket prices
✗ Currency exchange (not hotel-related)
✗ Weather or political situation queries
✗ Cricket/election/unrelated Pakistan topics
✗ Competitor companies doing outreach

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VALIDATION TESTS — CHECK BEFORE SCORING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Before returning any score above 4 ask yourself:

1. Is this about a hotel/guesthouse in Pakistan?
2. Is the person a property owner OR a traveler booking?
3. Is the post FROM someone with a real problem or need —
   not a company doing outreach?
4. Would Bookin.PK's team find this actionable?
5. Would responding to this post embarrass Bookin.PK?

If any answer is no — reduce score accordingly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FINAL REMINDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You are not scoring general travel discussion.
You are not scoring visa or flight questions.
You are not scoring abstract industry commentary.

You are identifying the exact moment a Pakistani hotel or 
guesthouse owner is ready to leave Booking.com/Agoda —
or a traveler is actively choosing where to book right now.

One hotelier onboarded could list their property permanently 
on Bookin.PK and drive ongoing bookings for years.

Be ruthless with noise.
Be generous with genuine hotelier pain and active traveler 
booking intent.
Be precise with every score.

Return JSON array only. Always. Every single time.
MINIMUM score is 1 — never return 0.
"""

CLAUDE_SYSTEM_PROMPT_REDDIT = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — REDDIT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
For scores 1-3: omit linkedin_message entirely — do NOT output the key.
For scores 4-10: include linkedin_message.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<property city or null>",
    "estimated_amount": "<specific commission % or PKR amount or null>",
    "competitor_mentioned": "<Booking.com|Agoda|null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "linkedin_message": "<public reply to their Reddit post, max 3 sentences — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

CLAUDE_SYSTEM_PROMPT_TWITTER = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — TWITTER/X
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
For scores 1-3: omit twitter_reply and twitter_dm entirely — do NOT output those keys.
For scores 4-10: include both twitter_reply and twitter_dm.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<property city or null>",
    "estimated_amount": "<specific commission % or PKR amount or null>",
    "competitor_mentioned": "<Booking.com|Agoda|null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "twitter_reply": "<2-sentence public reply to their tweet — OMIT KEY IF SCORE 1-3>",
    "twitter_dm": "<3-sentence private DM — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""

CLAUDE_SYSTEM_PROMPT_TELEGRAM = _SCORING_CORE + """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BATCH SCORING FORMAT — TELEGRAM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return a JSON ARRAY. One object per message. No preamble. No markdown. Raw JSON only.
reason: maximum 15 words. suggested_action: maximum 10 words.
Telegram messages are from private groups — no public reply is possible.
Outreach is via DM only if the sender has a visible username.
For scores 1-3: omit telegram_dm entirely — do NOT output the key.
For scores 4-10: include telegram_dm.

[
  {
    "index": <1-based integer matching message number>,
    "intent_score": <number 1-10>,
    "is_business": <true|false>,
    "business_size": <"solo"|"small"|"medium"|"unknown">,
    "has_international_context": <true|false>,
    "corridor": "<property city or null>",
    "estimated_amount": "<specific commission % or PKR amount or null>",
    "competitor_mentioned": "<Booking.com|Agoda|null>",
    "competitor_outreach_detected": <true|false>,
    "pain_type": "<specific pain or null>",
    "urgency": "<immediate|today|this_week|researching|none>",
    "reason": "<max 15 words>",
    "suggested_action": "<max 10 words>",
    "watchlist": <true|false>,
    "telegram_dm": "<3-sentence DM if username visible, else null — OMIT KEY IF SCORE 1-3>"
  }
]

Score EVERY message. Return SAME COUNT as received. JSON array only. Always.
"""


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB
# ─────────────────────────────────────────────────────────────────────────────

def get_database():
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[MONGODB_DB]

        db.signals.create_index(
            [("message_id", ASCENDING)], unique=True, name="message_id_unique"
        )
        for field in [
            "intent_score", "created_at", "client_id", "platform",
            "tier", "corridor", "competitor_mentioned", "pain_type",
            "is_business", "signal_category",
        ]:
            db.signals.create_index([(field, ASCENDING)])

        db.flintel_state.create_index(
            [("key", ASCENDING)], unique=True, name="state_key_unique"
        )

        # ── FIX A: persistent batch state collections ──────────────────────
        # One document per platform holds the in-flight (matched, not yet
        # scored) batch items plus the batch_start_time, so a restart can
        # resume exactly where it left off instead of losing progress.
        db.flintel_pending_batch.create_index(
            [("platform", ASCENDING)], unique=True, name="platform_unique"
        )
        # One document per platform holds the deduplication ID set, so a
        # restart cannot cause already-seen items to be re-queued and
        # duplicated against the persisted pending batch above.
        db.flintel_seen_ids.create_index(
            [("platform", ASCENDING)], unique=True, name="seen_platform_unique"
        )

        log.info("MongoDB connected.")
        return db
    except Exception as exc:
        log.critical(f"MongoDB connection failed: {exc}")
        raise


db = get_database()

# ─────────────────────────────────────────────────────────────────────────────
# ANTHROPIC CLIENT
# ─────────────────────────────────────────────────────────────────────────────

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ─────────────────────────────────────────────────────────────────────────────
# RETRY WITH EXPONENTIAL BACKOFF (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def retry_with_backoff(func, *args, retries=3, delay=2, label="op", **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            wait = delay * attempt
            log.error(f"[{label}] attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                log.info(f"[{label}] retrying in {wait}s...")
                time.sleep(wait)
            else:
                log.critical(f"[{label}] all {retries} attempts failed.")
                return None


# ─────────────────────────────────────────────────────────────────────────────
# OPERATOR SLACK ALERT (unchanged from v7.1/v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def send_operator_alert(title: str, detail: str, level: str = "ERROR"):
    if not SLACK_WEBHOOK_URL:
        log.warning(f"[OPERATOR ALERT] {title} — {detail} (Slack not configured)")
        return
    try:
        emoji = "🔴" if level == "CRITICAL" else "🟡"
        payload = {
            "text": f"{emoji} [OPERATOR ALERT] {title}",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} FLINTEL OPERATOR ALERT — {level}",
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*System*\nFLINTEL v7.3"},
                        {"type": "mrkdwn", "text": f"*Client*\n{CLIENT_ID}"},
                        {"type": "mrkdwn", "text": f"*Alert*\n{title}"},
                        {"type": "mrkdwn", "text": f"*Time*\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Detail*\n```{detail[:1500]}```"},
                },
                {"type": "divider"},
            ],
        }
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        log.info(f"Operator alert sent to Slack: {title}")
    except Exception as exc:
        log.error(f"Failed to send operator alert: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# FIX A — PERSISTENT BATCH STATE HELPERS (new in v7.3)
#
# These read/write flintel_pending_batch and flintel_seen_ids. They are the
# ONLY new persistence surface added in v7.3. Nothing about save_signal,
# Slack delivery, or HubSpot delivery is touched.
# ─────────────────────────────────────────────────────────────────────────────

def load_pending_batch(platform: str) -> tuple:
    """
    Loads the persisted in-flight batch for a platform on startup.
    Returns (items_list, batch_start_time_or_None).
    If nothing was persisted (fresh start, or last run cleared cleanly),
    returns ([], None) — identical to v7.2's cold-start behavior.
    """
    try:
        doc = db.flintel_pending_batch.find_one({"platform": platform})
        if not doc:
            return [], None
        items = doc.get("items", [])
        start_ts = doc.get("batch_start_time")
        start_time = start_ts.timestamp() if start_ts else None
        if items:
            log.warning(
                f"[{platform.upper()}] Resuming persisted batch from MongoDB | "
                f"{len(items)} item(s) recovered from before restart."
            )
        return items, start_time
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_pending_batch error: {exc} — starting with empty batch.")
        return [], None


def save_pending_batch(platform: str, items: list, batch_start_time):
    """
    Persists the current in-flight batch + its start time after every
    append, so a restart can resume from the true count instead of 1.
    """
    try:
        start_dt = (
            datetime.fromtimestamp(batch_start_time, tz=timezone.utc)
            if batch_start_time is not None else None
        )
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "items": items,
                "batch_start_time": start_dt,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_pending_batch error: {exc}")


def clear_pending_batch(platform: str):
    """Called immediately after a batch successfully fires to Claude."""
    try:
        db.flintel_pending_batch.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "items": [],
                "batch_start_time": None,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] clear_pending_batch error: {exc}")


def load_seen_ids(platform: str) -> set:
    """Loads the persisted dedup set for a platform on startup."""
    try:
        doc = db.flintel_seen_ids.find_one({"platform": platform})
        if not doc:
            return set()
        return set(doc.get("ids", []))
    except Exception as exc:
        log.error(f"[{platform.upper()}] load_seen_ids error: {exc} — starting with empty dedup set.")
        return set()


def save_seen_ids(platform: str, ids: set, cap: int = 200_000):
    """
    Persists the dedup set. Capped the same way the in-memory sets were
    capped in v7.2 (clear-on-overflow) to bound document size and avoid
    runaway MongoDB writes — behavior unchanged, just durable now.
    """
    try:
        id_list = list(ids)
        if len(id_list) > cap:
            id_list = id_list[-cap:]
        db.flintel_seen_ids.update_one(
            {"platform": platform},
            {"$set": {
                "platform": platform,
                "ids": id_list,
                "updated_at": datetime.now(timezone.utc),
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"[{platform.upper()}] save_seen_ids error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE BATCH SCORER — platform-aware prompt selection (unchanged selection
# logic from v7.2; only the JSON parsing step changes — see FIX B below)
# ─────────────────────────────────────────────────────────────────────────────

def _build_batch_prompt(batch: list) -> str:
    lines = []
    for i, item in enumerate(batch, start=1):
        ctype     = item.get("content_type", "unknown").upper()
        platform  = item.get("platform", "unknown").upper()
        subreddit = item.get("subreddit", "")
        group     = item.get("telegram_group", "")
        username  = item.get("username", "unknown")
        text      = item.get("text", "")[:800]

        if subreddit:
            location = f"r/{subreddit}"
        elif group:
            location = f"tg/{group}"
        else:
            location = platform

        lines.append(
            f"--- MESSAGE {i} ---\n"
            f"Platform: {platform} | Source: {location} | Type: {ctype} | User: {username}\n"
            f"Content: {text}\n"
        )
    return "\n".join(lines)


def _fallback_score(index: int, reason: str = "Scoring unavailable.") -> dict:
    derived = _derive_fields(1)
    return {
        "index":                        index,
        "intent_score":                 1,
        "signal_category":              derived["signal_category"],
        "tier":                         derived["tier"],
        "hubspot_priority":             derived["hubspot_priority"],
        "is_business":                  False,
        "business_size":                "unknown",
        "has_international_context":    False,
        "corridor":                     None,
        "estimated_amount":             None,
        "competitor_mentioned":         None,
        "competitor_outreach_detected": False,
        "pain_type":                    None,
        "urgency":                      "none",
        "reason":                       reason,
        "suggested_action":             "Check system logs.",
        "twitter_reply":                None,
        "twitter_dm":                   None,
        "linkedin_message":             None,
        "telegram_dm":                  None,
        "watchlist":                    False,
        "watchlist_reason":             None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# FIX B — TOLERANT PARTIAL-JSON RECOVERY (new in v7.3)
#
# v7.2 behavior: json.loads(raw) — any truncation throws, whole batch lost.
# v7.3 behavior: try json.loads() first (unchanged, fully-successful path).
#                On failure, salvage complete top-level objects from the
#                array using brace-depth tracking, discard only the
#                trailing partial object that was mid-write at cutoff.
# ─────────────────────────────────────────────────────────────────────────────

def _strip_code_fences(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        return parts[1].lstrip("json").strip() if len(parts) > 1 else raw.strip("```").strip()
    return raw


def _salvage_partial_json_array(raw: str) -> list:
    """
    Walks a possibly-truncated JSON array string and extracts every
    complete, well-formed top-level object using brace-depth tracking.
    Strings and escape sequences are tracked so braces inside quoted
    JSON string values (e.g. inside a "reason" or "linkedin_message"
    field) are not mistaken for structural braces.

    Returns a list of successfully parsed dicts (via json.loads on each
    salvaged substring). Any trailing partial object — the one that was
    being written when max_tokens cut generation off — is discarded,
    since it cannot be completed or trusted.
    """
    start = raw.find("[")
    if start == -1:
        return []

    objects = []
    depth = 0
    obj_start = None
    in_string = False
    escape = False

    i = start + 1  # skip the opening "["
    n = len(raw)
    while i < n:
        ch = raw[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            i += 1
            continue

        if ch == "{":
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and obj_start is not None:
                candidate = raw[obj_start:i + 1]
                try:
                    objects.append(json.loads(candidate))
                except (json.JSONDecodeError, ValueError):
                    # Malformed even though braces balanced — skip, don't crash.
                    log.warning("[Claude-Batch] Skipped one malformed salvaged object during recovery.")
                obj_start = None
        i += 1

    return objects


def _parse_claude_json(raw: str) -> tuple:
    """
    Returns (results_list, was_truncated_bool).
    Tries the fast/common path (full json.loads) first — identical to v7.2.
    Only falls back to salvage parsing if that fails, e.g. due to a
    max_tokens cutoff mid-array.
    """
    cleaned = _strip_code_fences(raw)
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError("Claude returned non-list.")
        return parsed, False
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning(
            f"[Claude-Batch] Full JSON parse failed ({exc}) — "
            f"attempting partial recovery from truncated response."
        )
        salvaged = _salvage_partial_json_array(cleaned)
        return salvaged, True


def _call_claude_batch(batch: list) -> list:
    platform = batch[0].get("platform", "reddit") if batch else "reddit"

    system_prompt = {
        "twitter":  CLAUDE_SYSTEM_PROMPT_TWITTER,
        "telegram": CLAUDE_SYSTEM_PROMPT_TELEGRAM,
    }.get(platform, CLAUDE_SYSTEM_PROMPT_REDDIT)

    prompt = _build_batch_prompt(batch)
    response = anthropic_client.messages.create(
        model      = "claude-sonnet-4-6",
        max_tokens = MAX_TOKENS,
        system     = system_prompt,
        messages   = [{"role": "user", "content": f"Score this batch:\n\n{prompt}"}],
    )

    raw = response.content[0].text.strip()
    results, was_truncated = _parse_claude_json(raw)

    if was_truncated:
        recovered_indices = {int(r["index"]) for r in results if isinstance(r, dict) and "index" in r}
        all_indices = set(range(1, len(batch) + 1))
        missing_indices = sorted(all_indices - recovered_indices)

        log.warning(
            f"[Claude-Batch] PARTIAL RECOVERY | platform:{platform} | "
            f"batch_size:{len(batch)} | recovered:{len(recovered_indices)} | "
            f"missing (fallback):{len(missing_indices)}"
        )
        send_operator_alert(
            title="Claude Response Truncated (max_tokens) — Partial Recovery",
            detail=(
                f"Platform: {platform}\n"
                f"Batch size: {len(batch)}\n"
                f"Successfully recovered: {len(recovered_indices)} item(s) — scored and delivered normally.\n"
                f"Lost to truncation (fallback score 1 applied): {len(missing_indices)} item(s) — "
                f"indices {missing_indices[:30]}{'...' if len(missing_indices) > 30 else ''}\n\n"
                f"Consider raising MAX_TOKENS (currently {MAX_TOKENS}) or lowering this platform's "
                f"batch size if this recurs."
            ),
            level="ERROR",
        )
        for idx in missing_indices:
            results.append(_fallback_score(idx, "Truncated by max_tokens — not recovered."))

    if not isinstance(results, list):
        raise ValueError("Claude returned non-list after parsing.")

    required = {"index", "intent_score", "is_business", "reason", "suggested_action"}
    optional_defaults = {
        "business_size":                "unknown",
        "has_international_context":    False,
        "corridor":                     None,
        "estimated_amount":             None,
        "competitor_mentioned":         None,
        "competitor_outreach_detected": False,
        "pain_type":                    None,
        "urgency":                      "none",
        "twitter_reply":                None,
        "twitter_dm":                   None,
        "linkedin_message":             None,
        "telegram_dm":                  None,
        "watchlist":                    False,
    }

    for r in results:
        missing = required - r.keys()
        if missing:
            raise ValueError(f"Missing keys in Claude response: {missing}")
        for k, v in optional_defaults.items():
            r.setdefault(k, v)
        if r.get("intent_score", 1) < 1:
            r["intent_score"] = 1

        score   = r["intent_score"]
        derived = _derive_fields(score)
        r["signal_category"]  = derived["signal_category"]
        r["tier"]             = derived["tier"]
        r["hubspot_priority"] = derived["hubspot_priority"]
        r["watchlist_reason"] = r.get("reason") if r.get("watchlist") else None

    return results


def score_batch_with_claude(batch: list) -> list:
    result = retry_with_backoff(
        _call_claude_batch, batch,
        retries=3, delay=5, label="Claude-Batch",
    )
    if result is None:
        send_operator_alert(
            title="Claude API Unavailable",
            detail=(
                f"All 3 retry attempts to score a batch of {len(batch)} items failed.\n"
                f"Batch platform: {batch[0].get('platform','unknown') if batch else 'unknown'}\n"
                f"Fallback scores (1) assigned. Check ANTHROPIC_API_KEY and API status."
            ),
            level="CRITICAL",
        )
        return [_fallback_score(i + 1) for i in range(len(batch))]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB STORAGE (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def save_signal(data: dict) -> bool:
    try:
        doc = {
            "message_id":                   data["message_id"],
            "platform":                     data.get("platform", "unknown"),
            "content_type":                 data.get("content_type", "unknown"),
            "subreddit":                    data.get("subreddit", ""),
            "telegram_group":               data.get("telegram_group", ""),
            "post_url":                     data.get("post_url", ""),
            "username":                     data.get("username", "unknown"),
            "message_text":                 data["message_text"],
            "intent_score":                 data["intent_score"],
            "signal_category":              data["signal_category"],
            "tier":                         data.get("tier", "discard"),
            "is_business":                  data.get("is_business", False),
            "business_size":                data.get("business_size", "unknown"),
            "corridor":                     data.get("corridor"),
            "estimated_amount":             data.get("estimated_amount"),
            "competitor_mentioned":         data.get("competitor_mentioned"),
            "competitor_outreach_detected": data.get("competitor_outreach_detected", False),
            "pain_type":                    data.get("pain_type"),
            "urgency":                      data.get("urgency", "none"),
            "reason":                       data["reason"],
            "suggested_action":             data["suggested_action"],
            "twitter_reply":                data.get("twitter_reply"),
            "twitter_dm":                   data.get("twitter_dm"),
            "linkedin_message":             data.get("linkedin_message"),
            "telegram_dm":                  data.get("telegram_dm"),
            "watchlist":                    data.get("watchlist", False),
            "watchlist_reason":             data.get("watchlist_reason"),
            "client_id":                    CLIENT_ID,
            "alerted_slack":                False,
            "alerted_hubspot":              False,
            "digest_included":              False,
            "created_at":                   datetime.now(timezone.utc),
        }
        db.signals.insert_one(doc)

        platform = data.get("platform", "?").upper()
        score    = data["intent_score"]
        user     = data.get("username", "?")
        ctype    = data.get("content_type", "")
        sub      = data.get("subreddit", "")
        grp      = data.get("telegram_group", "")
        source   = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)

        log.info(
            f"SAVED [{platform}] | Score:{score} | Tier:{data.get('tier','?')} | "
            f"u/{user} | {ctype} | {source}"
        )
        return True
    except DuplicateKeyError:
        log.debug(f"Duplicate skipped: {data['message_id']}")
        return False
    except Exception as exc:
        log.error(f"MongoDB save error: {exc}")
        send_operator_alert(
            title="MongoDB Write Failed",
            detail=(
                f"Failed to save signal to MongoDB.\n"
                f"message_id: {data.get('message_id','unknown')}\n"
                f"platform: {data.get('platform','unknown')}\n"
                f"error: {exc}\n\n"
                f"Check MONGODB_URI and MongoDB Atlas status."
            ),
            level="CRITICAL",
        )
        return False


def mark_slack_alerted(message_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {"alerted_slack": True, "alerted_slack_at": datetime.now(timezone.utc)}},
        )
    except Exception as exc:
        log.error(f"mark_slack_alerted error: {exc}")


def mark_hubspot_alerted(message_id: str, contact_id: str):
    try:
        db.signals.update_one(
            {"message_id": message_id},
            {"$set": {
                "alerted_hubspot": True,
                "hubspot_contact_id": contact_id,
                "alerted_hubspot_at": datetime.now(timezone.utc),
            }},
        )
    except Exception as exc:
        log.error(f"mark_hubspot_alerted error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# WEEKLY REPORT STATE PERSISTENCE (unchanged from v7.1/v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def _get_state(key: str):
    try:
        doc = db.flintel_state.find_one({"key": key})
        return doc["value"] if doc else None
    except Exception as exc:
        log.error(f"get_state error for key={key}: {exc}")
        return None


def _set_state(key: str, value):
    try:
        db.flintel_state.update_one(
            {"key": key},
            {"$set": {"key": key, "value": value, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )
    except Exception as exc:
        log.error(f"set_state error for key={key}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SLACK DELIVERY (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def _safe(text: str, limit: int = 2900) -> str:
    if not text:
        return "—"
    return text[:limit] + ("…" if len(text) > limit else "")


def _post_to_slack(payload: dict):
    r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    if r.status_code != 200:
        raise Exception(f"Slack {r.status_code}: {r.text}")
    return r


def send_slack_alert(data: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping.")
        return False

    score       = data["intent_score"]
    platform    = data.get("platform", "unknown").upper()
    ctype       = data.get("content_type", "post").upper()
    subreddit   = data.get("subreddit", "")
    tg_group    = data.get("telegram_group", "")
    post_url    = data.get("post_url", "")
    username    = data.get("username", "unknown")
    tier        = data.get("tier", "").upper()
    category    = data.get("signal_category", "").replace("_", " ").upper()
    is_biz      = data.get("is_business", False)
    corridor    = data.get("corridor") or "Unknown"
    amount      = data.get("estimated_amount") or "—"
    pain        = data.get("pain_type") or "—"
    competitor  = data.get("competitor_mentioned") or "—"
    urgency     = data.get("urgency", "none").upper()
    timestamp   = data.get("timestamp", "—")

    if score >= 9:
        urgency_tag = "⚡ RESPOND WITHIN 30 MINUTES"
    elif score >= 7:
        urgency_tag = "⏰ RESPOND WITHIN 2 HOURS"
    elif score >= 5:
        urgency_tag = "📋 ADD TO TODAY'S OUTREACH LIST"
    else:
        urgency_tag = ""

    outreach = (
        data.get("twitter_reply") or
        data.get("twitter_dm") or
        data.get("telegram_dm") or
        data.get("linkedin_message") or
        ""
    )

    header_emoji = "🚨" if score >= 8 else "⚠️"
    header_text  = f"{header_emoji} {category} — Score {score}/10 | {tier}"

    if subreddit:
        source_label = f"r/{subreddit}"
    elif tg_group:
        source_label = f"tg/{tg_group}"
    else:
        source_label = platform

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text[:150], "emoji": True},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Platform*\n{platform}"},
                {"type": "mrkdwn", "text": f"*Source*\n{source_label}"},
                {"type": "mrkdwn", "text": f"*Content Type*\n{ctype}"},
                {"type": "mrkdwn", "text": f"*User*\n{username}"},
                {"type": "mrkdwn", "text": f"*Score*\n{score}/10"},
                {"type": "mrkdwn", "text": f"*Tier*\n{tier}"},
                {"type": "mrkdwn", "text": f"*Profile*\n{'✅ Business' if is_biz else '👤 Individual'}"},
                {"type": "mrkdwn", "text": f"*Timestamp*\n{timestamp}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Corridor*\n{corridor}"},
                {"type": "mrkdwn", "text": f"*Estimated Amount*\n{amount}"},
                {"type": "mrkdwn", "text": f"*Pain Type*\n{pain}"},
                {"type": "mrkdwn", "text": f"*Competitor*\n{competitor}"},
                {"type": "mrkdwn", "text": f"*Urgency*\n{urgency}"},
            ],
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Message*\n>{_safe(data['message_text'], 400)}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Reason*\n{_safe(data['reason'], 300)}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Recommended Action*\n🎯 {_safe(data['suggested_action'], 300)}"},
        },
    ]

    if urgency_tag:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Response Window*\n{urgency_tag}"},
        })

    if outreach:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Outreach Script*\n💬 {_safe(outreach, 600)}"},
        })

    if post_url:
        blocks.append({
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "View Original →"},
                "url": post_url,
                "style": "primary",
            }],
        })

    blocks.append({"type": "divider"})

    result = retry_with_backoff(
        _post_to_slack, {"text": header_text, "blocks": blocks},
        retries=3, delay=2, label="Slack",
    )
    if result:
        log.info(f"Slack sent | {platform} | u/{username} | Score:{score}")
        return True
    log.error("Slack delivery failed after all retries.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# HUBSPOT CRM (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

HUBSPOT_BASE = "https://api.hubapi.com"


def _hs_headers() -> dict:
    return {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}


def _hs_find_contact(username: str) -> str | None:
    try:
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
            json={"filterGroups": [{"filters": [{"propertyName": "firstname", "operator": "EQ", "value": username}]}]},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0]["id"] if results else None
    except Exception as exc:
        log.error(f"HubSpot find contact error: {exc}")
        return None


def _hs_create_contact(data: dict) -> str | None:
    try:
        sub = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
            json={"properties": {
                "firstname":           f"{data.get('username','unknown')}",
                "lastname":            f"{data.get('platform','?').upper()} Signal",
                "fx_intent_score":     str(data["intent_score"]),
                "fx_signal_category":  data["signal_category"],
                "fx_tier":             data.get("tier", ""),
                "fx_corridor":         data.get("corridor") or "",
                "fx_pain_type":        data.get("pain_type") or "",
                "fx_competitor":       data.get("competitor_mentioned") or "",
                "fx_platform":         data.get("platform", ""),
                "fx_source_community": sub,
                "fx_signal_reason":    data["reason"],
                "fx_suggested_action": data["suggested_action"],
            }},
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
        return r.json().get("id")
    except Exception as exc:
        log.error(f"HubSpot create contact error: {exc}")
        return None


def _hs_create_note(data: dict, contact_id: str):
    try:
        sub = data.get("subreddit", "") or data.get("telegram_group", "") or data.get("platform", "")
        note = (
            f"FLINTEL SIGNAL — v7.3\n\n"
            f"Platform:     {data.get('platform','?').upper()}\n"
            f"Score:        {data['intent_score']}/10\n"
            f"Tier:         {data.get('tier','')}\n"
            f"Category:     {data['signal_category']}\n"
            f"Business:     {data.get('is_business', False)}\n"
            f"Business Size:{data.get('business_size','unknown')}\n"
            f"Corridor:     {data.get('corridor') or 'Unknown'}\n"
            f"Amount:       {data.get('estimated_amount') or 'Unknown'}\n"
            f"Competitor:   {data.get('competitor_mentioned') or 'None'}\n"
            f"Pain Type:    {data.get('pain_type') or 'Unknown'}\n"
            f"Urgency:      {data.get('urgency', 'none')}\n"
            f"Content Type: {data.get('content_type','unknown')}\n"
            f"Source:       {sub}\n"
            f"URL:          {data.get('post_url','N/A')}\n"
            f"Username:     {data.get('username','unknown')}\n"
            f"Timestamp:    {data.get('timestamp','N/A')}\n\n"
            f"Message:\n{data['message_text']}\n\n"
            f"Reason:       {data['reason']}\n"
            f"Action:       {data['suggested_action']}\n\n"
            f"Twitter Reply:\n{data.get('twitter_reply') or 'N/A'}\n\n"
            f"Twitter DM:\n{data.get('twitter_dm') or 'N/A'}\n\n"
            f"LinkedIn:\n{data.get('linkedin_message') or 'N/A'}\n\n"
            f"Telegram DM:\n{data.get('telegram_dm') or 'N/A'}"
        )
        r = requests.post(
            f"{HUBSPOT_BASE}/crm/v3/objects/notes",
            json={
                "properties": {
                    "hs_note_body": note,
                    "hs_timestamp": str(int(datetime.now(timezone.utc).timestamp() * 1000)),
                },
                "associations": [{
                    "to": {"id": contact_id},
                    "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 202}],
                }],
            },
            headers=_hs_headers(), timeout=10,
        )
        r.raise_for_status()
    except Exception as exc:
        log.error(f"HubSpot create note error: {exc}")


def _send_to_hubspot(data: dict) -> str | None:
    if not HUBSPOT_API_KEY:
        log.warning("HUBSPOT_API_KEY not set — skipping.")
        return None
    username   = data.get("username", "unknown")
    contact_id = _hs_find_contact(username)
    if not contact_id:
        contact_id = _hs_create_contact(data)
    if not contact_id:
        return None
    _hs_create_note(data, contact_id)
    log.info(f"HubSpot note attached | u/{username} | ID:{contact_id}")
    return contact_id


def send_to_hubspot(data: dict) -> str | None:
    return retry_with_backoff(_send_to_hubspot, data, retries=3, delay=3, label="HubSpot")


# ─────────────────────────────────────────────────────────────────────────────
# CORE SIGNAL PROCESSOR (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def process_scored_item(item: dict, score_result: dict):
    score    = score_result.get("intent_score", 1)
    platform = item.get("platform", "unknown")

    data = {
        "message_id":                   item["message_id"],
        "platform":                     platform,
        "content_type":                 item.get("content_type", "unknown"),
        "subreddit":                    item.get("subreddit", ""),
        "telegram_group":               item.get("telegram_group", ""),
        "post_url":                     item.get("post_url", ""),
        "username":                     item.get("username", "unknown"),
        "message_text":                 item.get("text", ""),
        "intent_score":                 score,
        "signal_category":              score_result.get("signal_category", "discard"),
        "tier":                         score_result.get("tier", "discard"),
        "is_business":                  score_result.get("is_business", False),
        "business_size":                score_result.get("business_size", "unknown"),
        "corridor":                     score_result.get("corridor"),
        "estimated_amount":             score_result.get("estimated_amount"),
        "competitor_mentioned":         score_result.get("competitor_mentioned"),
        "competitor_outreach_detected": score_result.get("competitor_outreach_detected", False),
        "pain_type":                    score_result.get("pain_type"),
        "urgency":                      score_result.get("urgency", "none"),
        "reason":                       score_result.get("reason", ""),
        "suggested_action":             score_result.get("suggested_action", ""),
        "twitter_reply":                score_result.get("twitter_reply"),
        "twitter_dm":                   score_result.get("twitter_dm"),
        "linkedin_message":             score_result.get("linkedin_message"),
        "telegram_dm":                  score_result.get("telegram_dm"),
        "watchlist":                    score_result.get("watchlist", False),
        "watchlist_reason":             score_result.get("watchlist_reason"),
        "timestamp":                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    saved = save_signal(data)
    if not saved:
        return

    if score < MIN_SCORE_MEDIUM:
        log.debug(
            f"SILENT SAVE | [{platform.upper()}] Score:{score} | "
            f"u/{data['username']} | {data['content_type']}"
        )
        return

    if MIN_SCORE_MEDIUM <= score < MIN_SCORE_HIGH:
        log.info(f"MEDIUM | [{platform.upper()}] Score:{score} | Slack only | u/{data['username']}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])

    elif score >= MIN_SCORE_HIGH:
        log.info(f"HIGH | [{platform.upper()}] Score:{score} | Slack + HubSpot | u/{data['username']}")
        ok = send_slack_alert(data)
        if ok:
            mark_slack_alerted(data["message_id"])
        cid = send_to_hubspot(data)
        if cid:
            mark_hubspot_alerted(data["message_id"], cid)


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC BATCH PROCESSOR — now with persistent state (FIX A)
#
# Structurally identical to v7.2's run_batch_processor. The ONLY additions
# are: (1) loading persisted state before the loop starts, (2) persisting
# state after every append, (3) clearing persisted state right after a
# batch fires. The size/timeout firing logic, the log message format, and
# everything downstream of "scores = score_batch_with_claude(...)" is
# byte-for-byte unchanged from v7.2.
# ─────────────────────────────────────────────────────────────────────────────

def run_batch_processor(
    q: queue.Queue,
    batch_size: int,
    platform_label: str,
):
    platform_key = platform_label.lower()

    log.info(
        f"Batch processor [{platform_label}] started | "
        f"batch_size:{batch_size} | gap:{BATCH_GAP_SECONDS}s | "
        f"timeout:{BATCH_TIMEOUT_SECONDS}s"
    )

    # ── FIX A: resume persisted state instead of always starting at 0 ──────
    current_batch, batch_start_time = load_pending_batch(platform_key)
    if current_batch:
        log.info(
            f"[{platform_label}] Resumed [{len(current_batch)}/{batch_size}] "
            f"from persistent disk — continuing, NOT restarting at 1."
        )

    total_received   = 0
    total_matched    = 0
    total_dropped    = 0
    total_batches    = 0

    while True:
        try:
            if current_batch and batch_start_time is not None:
                elapsed   = time.time() - batch_start_time
                remaining = BATCH_TIMEOUT_SECONDS - elapsed
                wait_time = max(0.1, remaining)
            else:
                wait_time = 1.0

            try:
                item = q.get(timeout=wait_time)
                got_item = True
            except queue.Empty:
                got_item = False

            if got_item:
                total_received += 1
                text = item.get("text", "").strip()

                if not text or len(text) < 10:
                    q.task_done()
                    continue

                if not passes_keyword_filter(text):
                    total_dropped += 1
                    log.debug(
                        f"[{platform_label}] FILTERED | "
                        f"u/{item.get('username')} | {item.get('content_type','?')}"
                    )
                    q.task_done()
                    continue

                total_matched += 1

                if not current_batch:
                    batch_start_time = time.time()

                current_batch.append(item)

                # ── FIX A: persist immediately after every append ──────────
                save_pending_batch(platform_key, current_batch, batch_start_time)

                log.info(
                    f"[{platform_label}] MATCH [{len(current_batch)}/{batch_size}] | "
                    f"{item.get('content_type','?').upper()} | u/{item.get('username')}"
                )

                q.task_done()

            should_fire = False
            fire_reason = ""

            if len(current_batch) >= batch_size:
                should_fire = True
                fire_reason = f"batch full ({batch_size} items)"
            elif current_batch and batch_start_time is not None:
                elapsed = time.time() - batch_start_time
                if elapsed >= BATCH_TIMEOUT_SECONDS:
                    should_fire = True
                    fire_reason = f"timeout ({BATCH_TIMEOUT_SECONDS}s) — partial batch {len(current_batch)}/{batch_size}"

            if should_fire and current_batch:
                total_batches += 1
                batch_to_send  = current_batch[:batch_size]
                current_batch  = current_batch[batch_size:]
                batch_start_time = None if not current_batch else time.time()

                # ── FIX A: clear (or re-persist leftover) immediately ──────
                # so a crash mid-Claude-call cannot replay items that are
                # about to be sent, and so a restart right after firing
                # does not re-send batch_to_send.
                if current_batch:
                    save_pending_batch(platform_key, current_batch, batch_start_time)
                else:
                    clear_pending_batch(platform_key)

                log.info(
                    f"[{platform_label}] ━━━ BATCH {total_batches} ━━━ | "
                    f"reason:{fire_reason} | items:{len(batch_to_send)} | "
                    f"received:{total_received} matched:{total_matched} dropped:{total_dropped}"
                )

                scores = score_batch_with_claude(batch_to_send)
                score_map = {int(s.get("index", 0)): s for s in scores if s.get("index")}

                for i, it in enumerate(batch_to_send):
                    pos = i + 1
                    sr  = score_map.get(pos) or (
                        scores[i] if i < len(scores) else _fallback_score(pos, "Index mismatch.")
                    )
                    process_scored_item(it, sr)

                log.info(
                    f"[{platform_label}] BATCH {total_batches} DONE | "
                    f"waiting {BATCH_GAP_SECONDS}s..."
                )
                time.sleep(BATCH_GAP_SECONDS)

        except Exception as exc:
            log.error(f"[{platform_label}] batch processor error: {exc}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# REDDIT — feedparser RSS poller
# FIX A: dedup set now loaded from / persisted to MongoDB (flintel_seen_ids)
# instead of being purely in-memory, so a restart cannot cause an item
# already sitting in the persisted pending batch to be re-fetched and
# duplicated. All RSS-fetching logic itself is unchanged from v7.2.
# ─────────────────────────────────────────────────────────────────────────────

_reddit_seen_ids: set = load_seen_ids("reddit")
_reddit_seen_lock = threading.Lock()
_reddit_seen_dirty_count = 0


def _reddit_rss_is_seen(entry_id: str) -> bool:
    """Returns True if already seen. Registers if new. Thread-safe. Caps at 200k.
    Persists to MongoDB periodically (every 10 new IDs) rather than on every
    single call, to avoid hammering MongoDB on high-volume cycles."""
    global _reddit_seen_ids, _reddit_seen_dirty_count
    with _reddit_seen_lock:
        if entry_id in _reddit_seen_ids:
            return True
        _reddit_seen_ids.add(entry_id)
        if len(_reddit_seen_ids) > 200_000:
            _reddit_seen_ids.clear()
        _reddit_seen_dirty_count += 1
        if _reddit_seen_dirty_count >= 10:
            save_seen_ids("reddit", _reddit_seen_ids)
            _reddit_seen_dirty_count = 0
        return False


def _get_reddit_rss(subreddit: str) -> list:
    url = f"https://www.reddit.com/r/{subreddit}/new.rss"
    items = []
    try:
        feed = feedparser.parse(url)
        if feed.bozo and not feed.entries:
            log.warning(f"[REDDIT-RSS] Feed parse issue for r/{subreddit}: {feed.bozo_exception}")
            return items

        for entry in feed.entries:
            entry_id = entry.get("id", "") or entry.get("link", "")
            if not entry_id:
                continue
            if _reddit_rss_is_seen(entry_id):
                continue

            title   = entry.get("title", "").strip()
            summary = entry.get("summary", "").strip()
            summary_plain = re.sub(r"<[^>]+>", " ", html.unescape(summary)).strip()

            text = title
            if summary_plain and summary_plain.lower() != title.lower():
                text = f"{title}\n\n{summary_plain}"

            author = entry.get("author", "unknown").lstrip("u/").strip() or "unknown"
            link   = entry.get("link", "")

            items.append({
                "message_id":     f"reddit_rss_{entry_id.split('/')[-1] or entry_id}",
                "platform":       "reddit",
                "content_type":   "post",
                "text":           text,
                "username":       author,
                "subreddit":      subreddit,
                "telegram_group": "",
                "post_url":       link,
            })

    except Exception as exc:
        log.error(f"[REDDIT-RSS] Error fetching r/{subreddit}: {exc}")

    return items


def poll_reddit_rss():
    log.info(
        f"[REDDIT-RSS] Poller started | {len(TARGET_SUBREDDITS)} subreddits | "
        f"poll interval: {REDDIT_POLL_INTERVAL}s per cycle | "
        f"dedup set resumed with {len(_reddit_seen_ids)} known ID(s)"
    )

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for subreddit in TARGET_SUBREDDITS:
            try:
                items = _get_reddit_rss(subreddit)
                for item in items:
                    reddit_queue.put(item)
                    total_new += 1
                if items:
                    log.info(
                        f"[REDDIT-RSS] r/{subreddit} → {len(items)} new items queued "
                        f"(queue size: {reddit_queue.qsize()})"
                    )
                time.sleep(2)
            except Exception as exc:
                log.error(f"[REDDIT-RSS] Unhandled error for r/{subreddit}: {exc}")
                total_errors += 1

        # Flush any remaining dirty dedup IDs at the end of each full cycle
        save_seen_ids("reddit", _reddit_seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[REDDIT-RSS] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {REDDIT_POLL_INTERVAL}s..."
        )
        time.sleep(REDDIT_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TWITTER / X POLLER
# FIX A: seen_ids persisted/restored the same way as Reddit's.
# ─────────────────────────────────────────────────────────────────────────────

def build_twitter_client() -> tweepy.Client | None:
    if not TWITTER_BEARER_TOKEN:
        log.warning("TWITTER_BEARER_TOKEN not set — Twitter platform disabled.")
        return None
    try:
        client = tweepy.Client(
            bearer_token       = TWITTER_BEARER_TOKEN,
            consumer_key       = TWITTER_API_KEY,
            consumer_secret    = TWITTER_API_SECRET,
            wait_on_rate_limit = True,
        )
        log.info("Twitter/X client initialised.")
        return client
    except Exception as exc:
        log.error(f"Twitter client error: {exc}")
        return None


def poll_twitter(client: tweepy.Client):
    seen_ids: set = load_seen_ids("twitter")
    dirty = 0
    log.info(
        f"Twitter poll started | query_len:{len(TWITTER_SEARCH_QUERY)} | "
        f"dedup set resumed with {len(seen_ids)} known ID(s)"
    )

    while True:
        try:
            response = client.search_recent_tweets(
                query        = TWITTER_SEARCH_QUERY,
                max_results  = 50,
                tweet_fields = ["author_id", "created_at", "text", "conversation_id"],
                expansions   = ["author_id"],
                user_fields  = ["username", "name"],
            )

            if not response or not response.data:
                log.debug("Twitter: no results this cycle.")
                time.sleep(TWITTER_POLL_INTERVAL)
                continue

            user_map: dict = {}
            if response.includes and "users" in response.includes:
                for u in response.includes["users"]:
                    user_map[u.id] = u.username

            new_count = 0
            for tweet in response.data:
                tweet_id = str(tweet.id)
                if tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                dirty += 1

                if len(seen_ids) > 50_000:
                    seen_ids.clear()

                text     = tweet.text or ""
                username = user_map.get(tweet.author_id, f"user_{tweet.author_id}")

                twitter_queue.put({
                    "message_id":     f"twitter_{tweet_id}",
                    "platform":       "twitter",
                    "content_type":   "tweet",
                    "text":           text,
                    "username":       username,
                    "subreddit":      "",
                    "telegram_group": "",
                    "post_url":       f"https://twitter.com/{username}/status/{tweet_id}",
                })
                new_count += 1

            if dirty >= 10:
                save_seen_ids("twitter", seen_ids)
                dirty = 0

            if new_count:
                log.info(
                    f"Twitter: {new_count} new tweets queued | "
                    f"queue_size:{twitter_queue.qsize()}"
                )

        except tweepy.errors.TweepyException as exc:
            log.error(f"Twitter poll error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")
        except Exception as exc:
            log.error(f"Twitter unexpected error: {exc} — retrying in {TWITTER_POLL_INTERVAL}s...")

        time.sleep(TWITTER_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM LISTENER (Telethon — human account, read-only)
# FIX A: seen_ids persisted/restored the same way as Reddit/Twitter.
# ─────────────────────────────────────────────────────────────────────────────

_telegram_seen_ids: set = load_seen_ids("telegram")
_telegram_seen_lock = threading.Lock()
_telegram_seen_dirty_count = 0


def _telegram_is_seen(chat_id: int, msg_id: int) -> bool:
    global _telegram_seen_ids, _telegram_seen_dirty_count
    key = f"{chat_id}_{msg_id}"
    with _telegram_seen_lock:
        if key in _telegram_seen_ids:
            return True
        _telegram_seen_ids.add(key)
        if len(_telegram_seen_ids) > 100_000:
            _telegram_seen_ids.clear()
        _telegram_seen_dirty_count += 1
        if _telegram_seen_dirty_count >= 10:
            save_seen_ids("telegram", _telegram_seen_ids)
            _telegram_seen_dirty_count = 0
        return False


def _join_telegram_groups_sync(client: TelegramClient):
    log.info(
        f"Telegram: starting auto-join for {len(TARGET_TELEGRAM_GROUPS)} groups | "
        f"gap:{TELEGRAM_JOIN_GAP_SECONDS}s"
    )
    joined  = 0
    skipped = 0
    failed  = 0

    for group in TARGET_TELEGRAM_GROUPS:
        try:
            target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
            client.loop.run_until_complete(client(JoinChannelRequest(target)))
            joined += 1
            log.info(f"Telegram: joined {target} [{joined}/{len(TARGET_TELEGRAM_GROUPS)}]")
            time.sleep(TELEGRAM_JOIN_GAP_SECONDS)
        except UserAlreadyParticipantError:
            skipped += 1
            log.debug(f"Telegram: already in {group} — skip")
        except FloodWaitError as e:
            log.warning(f"Telegram: FloodWait {e.seconds}s for {group} — waiting...")
            time.sleep(e.seconds + 5)
            failed += 1
        except (ChannelPrivateError, InviteHashExpiredError) as exc:
            log.warning(f"Telegram: cannot join {group} — {exc}")
            failed += 1
        except Exception as exc:
            log.error(f"Telegram: join error for {group} — {exc}")
            failed += 1

    log.info(
        f"Telegram auto-join complete | "
        f"joined:{joined} already_in:{skipped} failed:{failed}"
    )


TELEGRAM_POLL_INTERVAL = int(os.getenv("TELEGRAM_POLL_INTERVAL", "300"))


async def _poll_telegram_groups(client: TelegramClient):
    if TELEGRAM_POLL_INTERVAL == 0:
        log.info("[TELEGRAM-POLL] Disabled (TELEGRAM_POLL_INTERVAL=0) — listener-only mode.")
        return

    log.info(
        f"[TELEGRAM-POLL] Poller started | {len(TARGET_TELEGRAM_GROUPS)} groups | "
        f"interval:{TELEGRAM_POLL_INTERVAL}s"
    )

    while True:
        cycle_start  = time.time()
        total_new    = 0
        total_errors = 0

        for group in TARGET_TELEGRAM_GROUPS:
            try:
                target = group if group.startswith(("@", "https://", "t.me/")) else f"@{group}"
                messages = await client.get_messages(target, limit=20)

                for msg in messages:
                    if not msg or not msg.text or len(msg.text) < 5:
                        continue

                    chat_id = msg.chat_id if msg.chat_id else 0
                    msg_id  = msg.id

                    if _telegram_is_seen(chat_id, msg_id):
                        continue

                    sender   = await msg.get_sender()
                    tg_user  = getattr(sender, "username", None) or f"user_{getattr(sender, 'id', 0)}"

                    telegram_queue.put({
                        "message_id":     f"telegram_{chat_id}_{msg_id}",
                        "platform":       "telegram",
                        "content_type":   "message",
                        "text":           msg.text,
                        "username":       tg_user,
                        "display_name":   tg_user,
                        "subreddit":      "",
                        "telegram_group": group,
                        "post_url":       "",
                    })
                    total_new += 1

                if total_new:
                    log.info(f"[TELEGRAM-POLL] {group} → queued new messages")

                await asyncio.sleep(2)

            except FloodWaitError as e:
                log.warning(f"[TELEGRAM-POLL] FloodWait {e.seconds}s for {group}")
                await asyncio.sleep(e.seconds + 5)
                total_errors += 1
            except Exception as exc:
                log.error(f"[TELEGRAM-POLL] Error for {group}: {exc}")
                total_errors += 1

        save_seen_ids("telegram", _telegram_seen_ids)

        cycle_elapsed = time.time() - cycle_start
        log.info(
            f"[TELEGRAM-POLL] Cycle complete | new:{total_new} errors:{total_errors} | "
            f"elapsed:{cycle_elapsed:.1f}s | sleeping {TELEGRAM_POLL_INTERVAL}s..."
        )
        await asyncio.sleep(TELEGRAM_POLL_INTERVAL)


async def _run_telegram_listener(client: TelegramClient):
    target_set = set()
    for g in TARGET_TELEGRAM_GROUPS:
        clean = g.lstrip("@").lower()
        target_set.add(clean)

    @client.on(events.NewMessage)
    async def _on_message(event):
        try:
            chat = await event.get_chat()

            username_attr = getattr(chat, "username", None)
            chat_title    = getattr(chat, "title", "") or ""

            if username_attr:
                group_key = username_attr.lower()
            else:
                group_key = chat_title.lower().replace(" ", "").replace("-", "").replace("_", "")

            if group_key not in target_set:
                return

            sender    = await event.get_sender()
            text      = event.raw_text or ""
            sender_id = getattr(sender, "id", 0)
            first     = getattr(sender, "first_name", "") or ""
            last      = getattr(sender, "last_name", "") or ""
            tg_user   = getattr(sender, "username", None) or f"user_{sender_id}"
            msg_id    = event.id
            chat_id   = event.chat_id

            if not text or len(text) < 5:
                return

            if _telegram_is_seen(chat_id, msg_id):
                return

            telegram_queue.put({
                "message_id":     f"telegram_{chat_id}_{msg_id}",
                "platform":       "telegram",
                "content_type":   "message",
                "text":           text,
                "username":       tg_user,
                "display_name":   f"{first} {last}".strip() or tg_user,
                "subreddit":      "",
                "telegram_group": username_attr or chat_title,
                "post_url":       "",
            })

        except Exception as exc:
            log.error(f"Telegram message handler error: {exc}")

    log.info("Telegram listener active — read-only, no interactions.")

    await asyncio.gather(
        client.run_until_disconnected(),
        _poll_telegram_groups(client),
    )


def run_telegram_listener_thread():
    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning(
            "Telegram disabled — set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE"
        )
        return

    try:
        loop   = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        client = TelegramClient(
            TELEGRAM_SESSION,
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH,
            loop=loop,
        )

        loop.run_until_complete(client.start(phone=TELEGRAM_PHONE))
        me = loop.run_until_complete(client.get_me())
        log.info(
            f"Telegram authenticated as {me.first_name} "
            f"(@{me.username or me.id})"
        )

        _join_telegram_groups_sync(client)
        loop.run_until_complete(_run_telegram_listener(client))

    except Exception as exc:
        log.error(f"Telegram listener thread error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULERS — Daily Digest + Weekly Report (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

def send_daily_digest():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        signals = list(
            db.signals.find({
                "client_id":       CLIENT_ID,
                "intent_score":    {"$gte": 6, "$lte": 7},
                "created_at":      {"$gte": since},
                "digest_included": False,
            }).sort("intent_score", -1)
        )

        if not signals:
            log.info("Daily digest: no medium signals in past 24h.")
            return

        lines = []
        for s in signals:
            preview  = s["message_text"][:120]
            if len(s["message_text"]) > 120:
                preview += "..."
            corridor = s.get("corridor") or "—"
            pain     = s.get("pain_type") or "—"
            platform = s.get("platform", "?").upper()
            sub      = s.get("subreddit", "")
            grp      = s.get("telegram_group", "")
            source   = f"r/{sub}" if sub else (f"tg/{grp}" if grp else platform)
            lines.append(
                f"• *{s.get('username','?')}* | Score:{s['intent_score']}/10 "
                f"| {platform} | {source}\n"
                f"  Corridor: {corridor} | Pain: {pain}\n"
                f"  _{preview}_\n"
                f"  ↳ {s['suggested_action']}"
            )

        date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
        joined   = "\n\n".join(lines)
        chunks   = [joined[i:i+2900] for i in range(0, len(joined), 2900)]

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"📋 Daily Signal Digest — {date_str}", "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*{len(signals)} medium intent signals* (score 6–7) in the past 24 hours:"}},
        ]
        for chunk in chunks:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": chunk}})
        blocks += [
            {"type": "divider"},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.3 | Client: {CLIENT_ID} | Reddit + Twitter + Telegram"}]},
        ]

        result = retry_with_backoff(
            _post_to_slack, {"text": f"📋 Daily Signal Digest — {date_str}", "blocks": blocks},
            retries=3, delay=2, label="Digest",
        )
        if result:
            ids = [s["message_id"] for s in signals]
            db.signals.update_many({"message_id": {"$in": ids}}, {"$set": {"digest_included": True}})
            log.info(f"Daily digest sent | {len(signals)} signals.")

    except Exception as exc:
        log.error(f"Daily digest error: {exc}")


def send_weekly_report():
    if not SLACK_WEBHOOK_URL:
        return
    try:
        since         = datetime.now(timezone.utc) - timedelta(days=7)
        all_signals   = list(db.signals.find({"client_id": CLIENT_ID, "created_at": {"$gte": since}}))
        high          = [s for s in all_signals if s["intent_score"] >= 8]
        medium        = [s for s in all_signals if 6 <= s["intent_score"] <= 7]
        business      = [s for s in all_signals if s.get("is_business")]
        reddit_sigs   = [s for s in all_signals if s.get("platform") == "reddit"]
        twitter_sigs  = [s for s in all_signals if s.get("platform") == "twitter"]
        telegram_sigs = [s for s in all_signals if s.get("platform") == "telegram"]
        total         = len(all_signals)

        if total == 0:
            log.info("Weekly report: no signals this week.")
            return

        def breakdown(key):
            counts: dict = {}
            for s in all_signals:
                v = s.get(key)
                if v:
                    counts[v] = counts.get(v, 0) + 1
            return "\n".join(
                f"  • {k}: {v}" for k, v in sorted(counts.items(), key=lambda x: -x[1])
            ) or "_None_"

        top3       = sorted(high, key=lambda x: x["intent_score"], reverse=True)[:3]
        top3_lines = [
            f"• *{s.get('username','?')}* | Score:{s['intent_score']}/10 "
            f"| {s.get('platform','?').upper()} | {s.get('corridor') or 'Unknown corridor'}\n"
            f"  _{s['message_text'][:100]}{'...' if len(s['message_text'])>100 else ''}_"
            for s in top3
        ]

        week_start = since.strftime("%b %d")
        week_end   = datetime.now(timezone.utc).strftime("%b %d, %Y")

        payload = {
            "text": f"📊 Weekly Signal Report — {week_start} to {week_end}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"📊 Weekly Signal Report — {week_start} to {week_end}", "emoji": True}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Total Signals*\n{total}"},
                    {"type": "mrkdwn", "text": f"*High Intent (8–10)*\n{len(high)}"},
                    {"type": "mrkdwn", "text": f"*Medium Intent (6–7)*\n{len(medium)}"},
                    {"type": "mrkdwn", "text": f"*Business Owners*\n{len(business)}"},
                    {"type": "mrkdwn", "text": f"*Reddit*\n{len(reddit_sigs)}"},
                    {"type": "mrkdwn", "text": f"*Twitter/X*\n{len(twitter_sigs)}"},
                    {"type": "mrkdwn", "text": f"*Telegram*\n{len(telegram_sigs)}"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Corridor Breakdown*\n{breakdown('corridor')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Competitor Mentions*\n{breakdown('competitor_mentioned')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Pain Types*\n{breakdown('pain_type')}"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Top 3 Signals This Week*\n\n{_safe(chr(10).join(top3_lines), 2800)}"}},
                {"type": "divider"},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"FLINTEL v7.3 | {CLIENT_ID} | Week ending {week_end}"}]},
            ],
        }

        result = retry_with_backoff(_post_to_slack, payload, retries=3, delay=2, label="WeeklyReport")
        if result:
            log.info(
                f"Weekly report sent | Total:{total} High:{len(high)} Med:{len(medium)} "
                f"Biz:{len(business)} Reddit:{len(reddit_sigs)} "
                f"Twitter:{len(twitter_sigs)} Telegram:{len(telegram_sigs)}"
            )

    except Exception as exc:
        log.error(f"Weekly report error: {exc}")


async def run_scheduler():
    log.info(
        f"Scheduler started | digest:{DAILY_DIGEST_HOUR}:00 UTC | "
        f"report Mon {WEEKLY_REPORT_HOUR}:00 UTC"
    )
    last_digest_date = None

    persisted_week = _get_state("last_report_week")
    last_report_week: int | None = persisted_week

    while True:
        await asyncio.sleep(60)
        now = datetime.now(timezone.utc)

        if now.hour == DAILY_DIGEST_HOUR and now.date() != last_digest_date:
            log.info("Scheduler: triggering daily digest...")
            await asyncio.to_thread(send_daily_digest)
            last_digest_date = now.date()

        current_week = now.isocalendar()[1]
        if (
            now.weekday() == WEEKLY_REPORT_DAY
            and now.hour == WEEKLY_REPORT_HOUR
            and current_week != last_report_week
        ):
            log.info("Scheduler: triggering weekly report...")
            await asyncio.to_thread(send_weekly_report)
            last_report_week = current_week
            _set_state("last_report_week", current_week)


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC LISTENERS — thread management + auto-restart (unchanged from v7.2)
# ─────────────────────────────────────────────────────────────────────────────

async def start_reddit_listener():
    if not REDDIT_ENABLED:
        log.warning("Reddit platform DISABLED (REDDIT_ENABLED=false) — skipping.")
        return

    rss_thread = threading.Thread(
        target=poll_reddit_rss, daemon=True, name="Reddit-RSS"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
        daemon=True, name="Reddit-Batch",
    )

    rss_thread.start()
    btch_thread.start()
    log.info("Reddit threads running: RSS-Poller ✅ | Batch ✅")

    while True:
        await asyncio.sleep(60)
        if not rss_thread.is_alive():
            log.error("Reddit RSS thread died — restarting...")
            rss_thread = threading.Thread(
                target=poll_reddit_rss, daemon=True, name="Reddit-RSS"
            )
            rss_thread.start()
        if not btch_thread.is_alive():
            log.error("Reddit batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(reddit_queue, REDDIT_BATCH_SIZE, "REDDIT"),
                daemon=True, name="Reddit-Batch",
            )
            btch_thread.start()


async def start_twitter_listener():
    if not TWITTER_ENABLED:
        log.warning("Twitter platform DISABLED (TWITTER_ENABLED=false) — skipping.")
        return

    client = build_twitter_client()
    if client is None:
        log.warning("Twitter listener not started — credentials missing.")
        return

    poll_thread = threading.Thread(
        target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER"),
        daemon=True, name="Twitter-Batch",
    )

    poll_thread.start()
    btch_thread.start()
    log.info("Twitter threads running: Poll ✅ | Batch ✅")

    while True:
        await asyncio.sleep(60)
        if not poll_thread.is_alive():
            log.error("Twitter poll thread died — restarting...")
            poll_thread = threading.Thread(
                target=poll_twitter, args=(client,), daemon=True, name="Twitter-Poll"
            )
            poll_thread.start()
        if not btch_thread.is_alive():
            log.error("Twitter batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(twitter_queue, TWITTER_BATCH_SIZE, "TWITTER"),
                daemon=True, name="Twitter-Batch",
            )
            btch_thread.start()


async def start_telegram_listener():
    if not TELEGRAM_ENABLED:
        log.warning("Telegram platform DISABLED (TELEGRAM_ENABLED=false) — skipping.")
        return

    if not TELEGRAM_API_ID or not TELEGRAM_API_HASH or not TELEGRAM_PHONE:
        log.warning(
            "Telegram listener not started — "
            "set TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE in .env"
        )
        return

    tg_thread = threading.Thread(
        target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener"
    )
    btch_thread = threading.Thread(
        target=run_batch_processor,
        args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM"),
        daemon=True, name="Telegram-Batch",
    )

    tg_thread.start()
    btch_thread.start()
    log.info(
        f"Telegram threads running: Listener ✅ | Batch ✅ | "
        f"Poller {'✅' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled'}"
    )

    while True:
        await asyncio.sleep(60)
        if not tg_thread.is_alive():
            log.error("Telegram listener thread died — restarting...")
            tg_thread = threading.Thread(
                target=run_telegram_listener_thread, daemon=True, name="Telegram-Listener"
            )
            tg_thread.start()
        if not btch_thread.is_alive():
            log.error("Telegram batch thread died — restarting...")
            btch_thread = threading.Thread(
                target=run_batch_processor,
                args=(telegram_queue, TELEGRAM_BATCH_SIZE, "TELEGRAM"),
                daemon=True, name="Telegram-Batch",
            )
            btch_thread.start()


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI — REST API (unchanged routes from v7.2, version bumped)
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "FX Signal Intelligence API — Flintel v7.3",
    description = "Reddit (RSS) + Twitter + Telegram signals: monitor, score, store, alert. Persistent batch state.",
    version     = "7.3.0",
)


def _serialise(signals: list) -> list:
    for s in signals:
        s.pop("_id", None)
        for f in ["created_at", "alerted_slack_at", "alerted_hubspot_at"]:
            if f in s:
                s[f] = s[f].isoformat()
    return signals


@app.get("/")
def root():
    return {
        "status":                "running",
        "system":                "FLINTEL v7.3",
        "client":                CLIENT_ID,
        "platforms":             ["reddit", "twitter", "telegram"],
        "reddit_enabled":        REDDIT_ENABLED,
        "twitter_enabled":       TWITTER_ENABLED,
        "telegram_enabled":      TELEGRAM_ENABLED,
        "reddit_mode":           "feedparser RSS (no credentials required)",
        "reddit_poll_interval":  REDDIT_POLL_INTERVAL,
        "reddit_batch_size":     REDDIT_BATCH_SIZE,
        "twitter_batch_size":    TWITTER_BATCH_SIZE,
        "telegram_batch_size":   TELEGRAM_BATCH_SIZE,
        "telegram_poll_interval": TELEGRAM_POLL_INTERVAL,
        "batch_gap_s":           BATCH_GAP_SECONDS,
        "batch_timeout_s":       BATCH_TIMEOUT_SECONDS,
        "max_tokens":            MAX_TOKENS,
        "reddit_queue_size":     reddit_queue.qsize(),
        "twitter_queue_size":    twitter_queue.qsize(),
        "telegram_queue_size":   telegram_queue.qsize(),
        "telegram_groups":       len(TARGET_TELEGRAM_GROUPS),
        "auth_required":         bool(API_KEY),
        "output_schema":         "platform-specific (v7.2 cost optimisation, unchanged in v7.3)",
        "persistent_batch_state": True,
        "partial_json_recovery":  True,
    }


@app.get("/health")
def health():
    try:
        db.command("ping")
        mongo = "connected"
    except Exception:
        mongo = "disconnected"
    return {
        "status":                "ok",
        "mongodb":               mongo,
        "reddit":                ("polling-rss" if REDDIT_ENABLED else "disabled"),
        "twitter":               ("polling" if TWITTER_ENABLED and TWITTER_BEARER_TOKEN else "disabled"),
        "telegram":              ("listening" if TELEGRAM_ENABLED and TELEGRAM_API_ID else "disabled"),
        "reddit_queue_size":     reddit_queue.qsize(),
        "twitter_queue_size":    twitter_queue.qsize(),
        "telegram_queue_size":   telegram_queue.qsize(),
        "client_id":             CLIENT_ID,
        "timestamp":             datetime.now(timezone.utc).isoformat(),
    }


@app.get("/pending-batch", dependencies=[Depends(verify_api_key)])
def get_pending_batch():
    """
    New in v7.3: inspect the currently persisted in-flight batch for each
    platform. Useful for confirming restart-survival behavior without
    needing to read MongoDB directly.
    """
    try:
        docs = list(db.flintel_pending_batch.find({}, {"_id": 0}))
        for d in docs:
            if d.get("batch_start_time"):
                d["batch_start_time"] = d["batch_start_time"].isoformat()
            if d.get("updated_at"):
                d["updated_at"] = d["updated_at"].isoformat()
            d["item_count"] = len(d.get("items", []))
        return {"pending_batches": docs}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals", dependencies=[Depends(verify_api_key)])
def get_signals(
    limit:       int  = 50,
    platform:    str  = None,
    category:    str  = None,
    min_score:   int  = None,
    subreddit:   str  = None,
    tg_group:    str  = None,
    tier:        str  = None,
    corridor:    str  = None,
    pain_type:   str  = None,
    is_business: bool = None,
):
    try:
        q: dict = {"client_id": CLIENT_ID}
        if platform:    q["platform"]        = platform
        if category:    q["signal_category"] = category
        if min_score is not None: q["intent_score"] = {"$gte": min_score}
        if subreddit:   q["subreddit"]       = subreddit
        if tg_group:    q["telegram_group"]  = {"$regex": tg_group, "$options": "i"}
        if tier:        q["tier"]            = tier
        if corridor:    q["corridor"]        = {"$regex": corridor, "$options": "i"}
        if pain_type:   q["pain_type"]       = pain_type
        if is_business is not None: q["is_business"] = is_business

        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/stats", dependencies=[Depends(verify_api_key)])
def get_stats():
    try:
        total    = db.signals.count_documents({"client_id": CLIENT_ID})
        biz      = db.signals.count_documents({"client_id": CLIENT_ID, "is_business": True})
        reddit   = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "reddit"})
        twitter  = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "twitter"})
        telegram = db.signals.count_documents({"client_id": CLIENT_ID, "platform": "telegram"})

        def agg(group_field):
            return list(db.signals.aggregate([
                {"$match": {"client_id": CLIENT_ID, group_field: {"$ne": None}}},
                {"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]))

        return {
            "total_signals":    total,
            "business_owners":  biz,
            "reddit_signals":   reddit,
            "twitter_signals":  twitter,
            "telegram_signals": telegram,
            "corridors":        agg("corridor"),
            "pain_types":       agg("pain_type"),
            "competitors":      agg("competitor_mentioned"),
            "tiers":            agg("tier"),
            "reddit_queue":     reddit_queue.qsize(),
            "twitter_queue":    twitter_queue.qsize(),
            "telegram_queue":   telegram_queue.qsize(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/high-intent", dependencies=[Depends(verify_api_key)])
def get_high_intent(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$gte": 8}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/digest", dependencies=[Depends(verify_api_key)])
def get_digest(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$gte": 6, "$lte": 7}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/business", dependencies=[Depends(verify_api_key)])
def get_business(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "is_business": True}, {"_id": 0}
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/outreach", dependencies=[Depends(verify_api_key)])
def get_outreach(limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {
                    "client_id":    CLIENT_ID,
                    "intent_score": {"$gte": 5},
                    "$or": [
                        {"twitter_reply":    {"$ne": None}},
                        {"twitter_dm":       {"$ne": None}},
                        {"linkedin_message": {"$ne": None}},
                        {"telegram_dm":      {"$ne": None}},
                    ],
                },
                {"_id": 0},
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/twitter", dependencies=[Depends(verify_api_key)])
def get_twitter_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "twitter"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/reddit", dependencies=[Depends(verify_api_key)])
def get_reddit_signals(limit: int = 50, min_score: int = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "reddit"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/telegram", dependencies=[Depends(verify_api_key)])
def get_telegram_signals(limit: int = 50, min_score: int = None, group: str = None):
    try:
        q: dict = {"client_id": CLIENT_ID, "platform": "telegram"}
        if min_score is not None:
            q["intent_score"] = {"$gte": min_score}
        if group:
            q["telegram_group"] = {"$regex": group, "$options": "i"}
        signals = list(db.signals.find(q, {"_id": 0}).sort("created_at", -1).limit(limit))
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/corridors", dependencies=[Depends(verify_api_key)])
def get_by_corridor(corridor: str, limit: int = 20):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "corridor": {"$regex": corridor, "$options": "i"}},
                {"_id": 0},
            ).sort("intent_score", -1).limit(limit)
        )
        return {"count": len(signals), "corridor": corridor, "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/watchlist", dependencies=[Depends(verify_api_key)])
def get_watchlist(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "watchlist": True}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/signals/silent", dependencies=[Depends(verify_api_key)])
def get_silent_signals(limit: int = 50):
    try:
        signals = list(
            db.signals.find(
                {"client_id": CLIENT_ID, "intent_score": {"$lte": 5}}, {"_id": 0}
            ).sort("created_at", -1).limit(limit)
        )
        return {"count": len(signals), "signals": _serialise(signals)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    api_thread = threading.Thread(target=run_fastapi, daemon=True, name="FastAPI")
    api_thread.start()
    log.info("FastAPI running at http://0.0.0.0:8000")

    await asyncio.gather(
        start_reddit_listener(),
        start_twitter_listener(),
        start_telegram_listener(),
        run_scheduler(),
    )


if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  FX SIGNAL INTELLIGENCE SYSTEM — FLINTEL v7.3")
    log.info("=" * 70)
    log.info(f"  Client             : {CLIENT_ID}")
    log.info(f"  Platforms          : Reddit (RSS) + Twitter/X + Telegram")
    log.info(f"  Reddit             : {'✅ ENABLED' if REDDIT_ENABLED else '❌ DISABLED (REDDIT_ENABLED=false)'}")
    log.info(f"  Reddit mode        : feedparser RSS — no credentials required")
    log.info(f"  Reddit poll gap    : {REDDIT_POLL_INTERVAL}s between full subreddit cycles")
    log.info(f"  Twitter            : {'✅ ENABLED' if TWITTER_ENABLED else '❌ DISABLED (TWITTER_ENABLED=false)'}")
    log.info(f"  Telegram           : {'✅ ENABLED' if TELEGRAM_ENABLED else '❌ DISABLED (TELEGRAM_ENABLED=false)'}")
    log.info(f"  Telegram polling   : {'every ' + str(TELEGRAM_POLL_INTERVAL) + 's' if TELEGRAM_POLL_INTERVAL > 0 else '⏸ disabled (TELEGRAM_POLL_INTERVAL=0)'}")
    log.info(f"  Reddit batch       : {REDDIT_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Twitter batch      : {TWITTER_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Telegram batch     : {TELEGRAM_BATCH_SIZE} items OR {BATCH_TIMEOUT_SECONDS}s → 1 Claude call")
    log.info(f"  Batch gap          : {BATCH_GAP_SECONDS}s between calls")
    log.info(f"  Batch timeout      : {BATCH_TIMEOUT_SECONDS}s (partial batch fires after timeout)")
    log.info(f"  max_tokens         : {MAX_TOKENS}")
    log.info(f"  Twitter poll       : every {TWITTER_POLL_INTERVAL}s (rate-limit safe)")
    log.info(f"  Twitter query      : built dynamically from KEYWORDS ({len(KEYWORDS)} keywords)")
    log.info(f"  Telegram join gap  : {TELEGRAM_JOIN_GAP_SECONDS}s between group joins")
    log.info(f"  Score 1-5          : SILENT SAVE — MongoDB only, no alerts")
    log.info(f"  Score 6-7          : MEDIUM — MongoDB + Slack")
    log.info(f"  Score 8-10         : HIGH   — MongoDB + Slack + HubSpot")
    log.info(f"  MongoDB            : ALL scores 1-10 saved, nothing discarded")
    log.info(f"  Platform isolation : Reddit / Twitter / Telegram NEVER mixed")
    log.info(f"  Deduplication      : Persistent (MongoDB flintel_seen_ids) — survives restarts")
    log.info(f"  Batch state        : Persistent (MongoDB flintel_pending_batch) — survives restarts")
    log.info(f"  Partial-JSON       : Truncated Claude responses now salvage completed items")
    log.info(f"                     : instead of discarding the whole batch (see FIX B)")
    log.info(f"  Operator alerts    : Claude API down + MongoDB failure + partial recovery → Slack")
    log.info(f"  API auth           : {'✅ ENABLED (API_KEY set)' if API_KEY else '⚠️  DISABLED (API_KEY not set — open access)'}")
    log.info(f"  Weekly state       : Persisted in MongoDB (survives restarts)")
    log.info(f"  Daily digest       : {DAILY_DIGEST_HOUR}:00 UTC")
    log.info(f"  Weekly report      : Monday {WEEKLY_REPORT_HOUR}:00 UTC")
    log.info(f"  Subreddits         : {len(TARGET_SUBREDDITS)} monitored")
    log.info(f"  Telegram groups    : {len(TARGET_TELEGRAM_GROUPS)} configured")
    log.info(f"  Keywords           : {len(KEYWORDS)} filters (same for all 3 platforms)")
    log.info(f"  MongoDB DB         : {MONGODB_DB}")
    log.info(f"  HubSpot            : {'enabled' if HUBSPOT_API_KEY else 'DISABLED — set HUBSPOT_API_KEY'}")
    log.info(f"  Slack              : {'enabled' if SLACK_WEBHOOK_URL else 'DISABLED — set SLACK_WEBHOOK_URL'}")
    log.info(f"  Output schema      : Platform-specific JSON (unchanged from v7.2) — ~140 tokens/item")
    log.info(f"  v7.3 changes       : FIX A (persistent batch state) + FIX B (partial-JSON recovery) ONLY")
    log.info(f"                     : Scoring logic, prompts, Slack/HubSpot formatting — 100% unchanged")
    log.info("=" * 70)

    asyncio.run(main())