"""STM PRO BOT main.py — Quotex candle-based signals and result tracking.
Read-only market data; no order execution. Actual API fields depend on PyQuotex version.
"""
import os
import time
import asyncio
import logging
import secrets
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

from database import init_db, add_user, save_signal, recent_signals, stats, record_result
from stm_strategy import analyze as stm_analyze

# Existing project strategies only. PVIP/PVIP2 are not imported as extra modules;
# keep the strategy list aligned with the user's existing STM/AI/RANDOM files.
try:
    from ai_strategy import analyze as ai_analyze
except Exception as exc:
    ai_analyze = None
    log_import_error = f"AI strategy unavailable: {exc}"
else:
    log_import_error = None

try:
    from random_strategy import analyze as random_analyze
except Exception as exc:
    random_analyze = None
    log_import_error = (log_import_error + " | " if log_import_error else "") + f"RANDOM strategy unavailable: {exc}"

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("stm_pro_bot")
TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "").strip()
BOT_NAME = "STM PRO BOT"
PAGE_SIZE = 10
SIGNAL_EXPIRY_SECONDS = 60
SIGNAL_LEAD_SECONDS = 5  # publish 5 seconds before the next minute starts
FEEDBACK_ID = os.getenv("FEEDBACK_ID", "").strip()
signals_enabled = True
session_signals = []  # current process session; persisted outcomes remain in SQLite

# Display labels are kept exactly as requested. Quotex symbol support can vary by account/session.
MARKETS = {
    "FOREX": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD"],
    "COMMODITIES": ["UKBrent (OTC)", "Silver (OTC)", "USCrude (OTC)", "Gold (OTC)"],
    "CRYPTO": ["Bitcoin (OTC)", "Ethereum Classic (OTC)", "Chainlink (OTC)", "Litecoin (OTC)", "Toncoin (OTC)", "Trump (OTC)", "Ripple (OTC)", "Zcash (OTC)", "Avalanche (OTC)", "Dash (OTC)", "Axie Infinity (OTC)", "Polkadot (OTC)", "Binance Coin (OTC)", "Ethereum (OTC)", "Cosmos (OTC)", "Bitcoin Cash (OTC)"],
    "CURRENCY": ["USD/ARS (OTC)", "USD/BDT (OTC)", "USD/BRL (OTC)", "USD/CAD (OTC)", "USD/CHF (OTC)", "USD/COP (OTC)", "USD/DZD (OTC)", "USD/EGP (OTC)", "USD/IDR (OTC)", "USD/INR (OTC)", "USD/JPY (OTC)", "USD/MXN (OTC)", "USD/NGN (OTC)", "USD/PHP (OTC)", "USD/PKR (OTC)", "USD/ZAR (OTC)", "AUD/CAD (OTC)", "AUD/CHF (OTC)", "AUD/JPY (OTC)", "AUD/NZD (OTC)", "AUD/USD (OTC)", "CAD/CHF (OTC)", "CAD/JPY (OTC)", "CHF/JPY (OTC)", "EUR/AUD (OTC)", "EUR/CAD (OTC)", "EUR/CHF (OTC)", "EUR/GBP (OTC)", "EUR/JPY (OTC)", "EUR/NZD (OTC)", "EUR/USD (OTC)", "GBP/AUD (OTC)", "GBP/CAD (OTC)", "GBP/CHF (OTC)", "GBP/JPY (OTC)", "GBP/NZD (OTC)", "GBP/USD (OTC)", "NZD/CAD (OTC)", "NZD/CHF (OTC)", "NZD/JPY (OTC)", "NZD/USD (OTC)"],
}
CATEGORY_ORDER = ["FOREX", "COMMODITIES", "CRYPTO", "CURRENCY"]
CATEGORY_LABELS = {"FOREX": "FOREX", "COMMODITIES": "COMMODITIES", "CRYPTO": "CRYPTO", "CURRENCY": "CURRENCY"}
STRATEGY_LABELS = {"AI": "AI STRATEGY", "STM": "STM STRATEGY", "RANDOM": "RANDOM STRATEGY", "BLACK4X": "BLACK4X STRATEGY", "ALL": "SELECT ALL"}

# Best-effort names for PyQuotex. Unsupported names are skipped, never fabricated into a signal.
SYMBOLS = {
    "EUR/USD":"EURUSD", "GBP/USD":"GBPUSD", "USD/JPY":"USDJPY", "USD/CHF":"USDCHF", "USD/CAD":"USDCAD", "AUD/USD":"AUDUSD", "NZD/USD":"NZDUSD",
    "UKBrent (OTC)":"UKBrent_otc", "Silver (OTC)":"XAGUSD_otc", "USCrude (OTC)":"USOIL_otc", "Gold (OTC)":"XAUUSD_otc",
    "Bitcoin (OTC)":"BTCUSD_otc", "Ethereum Classic (OTC)":"ETCUSD_otc", "Chainlink (OTC)":"LINKUSD_otc", "Litecoin (OTC)":"LTCUSD_otc", "Toncoin (OTC)":"TONUSD_otc", "Trump (OTC)":"TRUMPUSD_otc", "Ripple (OTC)":"XRPUSD_otc", "Zcash (OTC)":"ZECUSD_otc", "Avalanche (OTC)":"AVAXUSD_otc", "Dash (OTC)":"DASHUSD_otc", "Axie Infinity (OTC)":"AXSUSD_otc", "Polkadot (OTC)":"DOTUSD_otc", "Binance Coin (OTC)":"BNBUSD_otc", "Ethereum (OTC)":"ETHUSD_otc", "Cosmos (OTC)":"ATOMUSD_otc", "Bitcoin Cash (OTC)":"BCHUSD_otc",
}
for _pair in MARKETS["CURRENCY"]:
    _bare = _pair.replace(" (OTC)", "")
    SYMBOLS[_pair] = _bare.replace("/", "") + "_otc"

class MarketDataError(RuntimeError):
    pass

class QuotexDemoFeed:
    """Read-only PyQuotex candle adapter. Does not place orders."""
    def __init__(self):
        self.email = os.getenv("QUOTEX_EMAIL", "").strip()
        self.password = os.getenv("QUOTEX_PASSWORD", "").strip()
        self.host = os.getenv("QUOTEX_HOST", "quotex.com").strip()
        self.client = None
        self.connected = False

    async def connect(self):
        if self.connected and self.client:
            return True
        if not self.email or not self.password:
            raise MarketDataError(".env-এ QUOTEX_EMAIL এবং QUOTEX_PASSWORD সেট করা নেই। গোপন তথ্য কাউকে পাঠাবেন না।")
        try:
            from pyquotex.stable_api import Quotex
        except Exception as exc:
            raise MarketDataError(f"PyQuotex import হয়নি: {exc}")
        try:
            self.client = Quotex(email=self.email, password=self.password, lang="en", host=self.host)
            result = await self.client.connect()
            ok = bool(result[0]) if isinstance(result, tuple) else bool(result)
            if not ok:
                raise MarketDataError(f"Quotex connection নিশ্চিত হয়নি: {result!r}")
            try:
                await self.client.change_account("PRACTICE")
            except Exception:
                try:
                    from pyquotex.utils.account_type import AccountType
                    await self.client.change_account(AccountType.DEMO)
                except Exception as exc:
                    log.warning("Demo account switch not confirmed: %s", exc)
            self.connected = True
            return True
        except MarketDataError:
            raise
        except Exception as exc:
            self.client, self.connected = None, False
            raise MarketDataError(f"Quotex সংযোগ ব্যর্থ: {type(exc).__name__}: {exc}")

    async def candles(self, asset, period=60, count=100, min_valid=2):
        await self.connect()
        try:
            raw = await self.client.get_candles(asset, time.time(), count, period)
            if not raw:
                raise MarketDataError(f"{asset}: Quotex candle দেয়নি (symbol/session unsupported হতে পারে)।")
            rows = []
            for c in raw:
                try:
                    ts = float(c.get("time", c.get("from", 0)))
                    op, hi, lo, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
                    if ts <= 0 or not all(map(lambda v: v == v and abs(v) != float("inf"), (op, hi, lo, cl))):
                        continue
                    if hi < max(op, cl, lo) or lo > min(op, cl, hi):
                        continue
                    rows.append({"time": ts, "open": op, "high": hi, "low": lo, "close": cl})
                except (KeyError, TypeError, ValueError):
                    continue
            rows.sort(key=lambda x: x["time"])
            # Refuse empty, stale, future, or malformed feed data. This does not prove
            # the broker's displayed label; successful fresh candles are the availability check.
            now = time.time()
            rows = [r for r in rows if r["time"] <= now + 2]
            if len(rows) < min_valid:
                raise MarketDataError(f"{asset}: পর্যাপ্ত valid candle নেই ({len(rows)}টি; দরকার {min_valid}টি)।")
            if now - rows[-1]["time"] > max(180, period * 3):
                raise MarketDataError(f"{asset}: candle feed stale (সর্বশেষ candle {int(now - rows[-1]['time'])}s পুরোনো)।")
            return rows
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(f"{asset}: candle read failed: {type(exc).__name__}: {exc}")

    async def payout(self, asset):
        """Best-effort read-only payout lookup; returns a real value or None.

        PyQuotex versions expose different APIs, so do not fabricate a payout.
        """
        await self.connect()
        for method_name in ("get_payment", "get_payout", "get_payments"):
            method = getattr(self.client, method_name, None)
            if not callable(method):
                continue
            try:
                data = method()
                if asyncio.iscoroutine(data):
                    data = await data
                if isinstance(data, dict):
                    candidates = (asset, asset.lower(), asset.upper())
                    for key in candidates:
                        value = data.get(key)
                        if isinstance(value, dict):
                            value = value.get("profit", value.get("payout", value.get("payment")))
                        if isinstance(value, (int, float)) and 0 <= value <= 100:
                            return float(value)
                if isinstance(data, list):
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        name = item.get("asset", item.get("symbol", item.get("name")))
                        value = item.get("profit", item.get("payout", item.get("payment")))
                        if name == asset and isinstance(value, (int, float)) and 0 <= value <= 100:
                            return float(value)
            except Exception as exc:
                log.debug("Payout lookup via %s unavailable: %s", method_name, exc)
        return None

    async def close(self):
        if self.client:
            try:
                await self.client.close()
            except Exception:
                pass
        self.client, self.connected = None, False

def black4x_analyze(candles):
    """BLACK4X: follow the last fully closed candle's direction for the next candle."""
    if len(candles) < 2:
        return None
    last = candles[-1]
    if last["close"] > last["open"]:
        direction = "CALL"
    elif last["close"] < last["open"]:
        direction = "PUT"
    else:
        return None  # do not invent a direction for a doji candle
    return {"direction": direction, "confidence": 50,
            "reason": "BLACK4X: follows the direction of the latest completed Quotex candle; not a guarantee."}

feed = QuotexDemoFeed()
# per-user wizard state; reset if bot process restarts
user_prefs = {}

WARNINGS = [
    "MUST FOLLOW MONEY MANAGEMENT !!",
    "USE ONLY 2% OF YOU BALANCE !!",
    "TRY TO TAKE A SAFTEY MARGIN !!",
    "IF LOSS THEN USE ONLY 1 MTG !!",
    "IF MTG THEN NO NEED SAFTEY OK!!",
    "IF 2 SIGNAL LOSS IN A SEASON THEN OFF AND START 6 HOURS LATER!!",
    "ARE YOU AGREE, IF NOT ,THEN IGNORE !! STM PRO BOT SIGNAL!",
    "HAVE A SUCCESS WITH US!!",
    "WE WILL DO PROFIT INSHA ALLAH",
    "LET'S START WITH BISMILLAH!",
]

async def member_ok(context, user_id):
    # Fail closed: without a channel ID we cannot verify membership, so no signal access.
    if not CHANNEL_ID:
        return False
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        log.exception("Channel membership check failed")
        return False

def verified(context, uid):
    return context.user_data.get("channel_verified") is True

def setup_user(uid):
    return user_prefs.setdefault(uid, {"categories": [], "pairs": [], "strategy": None, "pair_page": 0, "pair_stage": "categories"})

def welcome_keyboard():
    rows = []
    if CHANNEL_URL:
        rows.append([InlineKeyboardButton("📢 Join Channel", url=CHANNEL_URL)])
    rows.append([InlineKeyboardButton("✅ Verify Membership", callback_data="check_member")])
    rows.append([InlineKeyboardButton("ℹ️ Help", callback_data="help")])
    return InlineKeyboardMarkup(rows)

def categories_keyboard(prefs):
    rows = []
    for key in CATEGORY_ORDER:
        mark = "☑️" if key in prefs["categories"] else "▫️"
        rows.append([InlineKeyboardButton(f"{mark} {CATEGORY_LABELS[key]}", callback_data=f"cat:{key}")])
    rows.append([InlineKeyboardButton("☑️ SELECT ALL CATEGORIES" if len(prefs["categories"]) == len(CATEGORY_ORDER) else "SELECT ALL CATEGORIES", callback_data="catall")])
    rows.append([InlineKeyboardButton("➡️ Continue to Pair Selection", callback_data="pairs_open")])
    rows.append([InlineKeyboardButton("ℹ️ Help", callback_data="help")])
    return InlineKeyboardMarkup(rows)

def selected_pairs(prefs):
    # Pair selection is explicit; choosing a category alone does not silently select every pair.
    return list(prefs["pairs"])

def pair_keyboard(prefs):
    pairs = [p for cat in CATEGORY_ORDER if cat in prefs["categories"] for p in MARKETS[cat]]
    page = max(0, min(prefs["pair_page"], max(0, (len(pairs)-1)//PAGE_SIZE)))
    prefs["pair_page"] = page
    start = page * PAGE_SIZE
    rows = []
    chosen = set(prefs["pairs"])
    for i, pair in enumerate(pairs[start:start+PAGE_SIZE], start=start):
        mark = "☑️" if pair in chosen else "▫️"
        rows.append([InlineKeyboardButton(f"{mark} {pair}", callback_data=f"pair:{i}")])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Previous", callback_data="pairpage:prev"))
    if start + PAGE_SIZE < len(pairs): nav.append(InlineKeyboardButton("Next ➡️", callback_data="pairpage:next"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("☑️ SELECT ALL PAIRS", callback_data="pairall")])
    rows.append([InlineKeyboardButton("↩️ Categories", callback_data="back_categories"), InlineKeyboardButton("➡️ Choose Strategy", callback_data="strategy_open")])
    return InlineKeyboardMarkup(rows)

def strategy_keyboard(prefs):
    rows = [[InlineKeyboardButton(STRATEGY_LABELS[k], callback_data=f"strategy:{k}")] for k in ("AI", "STM", "RANDOM", "BLACK4X", "ALL")]
    rows.append([InlineKeyboardButton("⬅️ Back to Pairs", callback_data="pairs_open")])
    return InlineKeyboardMarkup(rows)

def ready_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🚀 START ANALYSIS", callback_data="start_analysis")], [InlineKeyboardButton("⚙️ Change Selection", callback_data="categories_open"), InlineKeyboardButton("ℹ️ Help", callback_data="help")]])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user, message = update.effective_user, update.effective_message
    if not user or not message: return
    await add_user(user.id, user.username or "", user.first_name or "")
    await message.reply_text(f"👋 Welcome to {BOT_NAME}!\n\nপ্রথমে আমাদের Telegram channel-এ Join করুন, তারপর Verify Membership চাপুন।\n\nসিগন্যাল পেতে channel membership verify করতে হবে।", reply_markup=welcome_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message:
        await message.reply_text("ℹ️ STM PRO BOT Help\n\n1. Channel join করুন।\n2. Verify Membership চাপুন।\n3. Category নির্বাচন করুন।\n4. Pair নির্বাচন করুন।\n5. Strategy নির্বাচন করে START ANALYSIS চাপুন।\n\nBot Quotex থেকে candle data পড়ার চেষ্টা করে। এটি order place করে না এবং signal বা profit নিশ্চিত নয়।", reply_markup=welcome_keyboard())

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg:
        await msg.reply_text(f"🤖 {BOT_NAME}\nQuotex feed: {'CONNECTED' if feed.connected else 'NOT CONNECTED'}\nAccount mode: DEMO intended; check library response/account manually.\nAuto trading: DISABLED\nChannel ID configured: {'YES' if CHANNEL_ID else 'NO — verification unavailable'}")

async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user, msg = update.effective_user, update.effective_message
    if not user or not msg: return
    rows = await recent_signals(user.id, 10)
    if not rows:
        await msg.reply_text("এখনো কোনো tracked signal নেই।"); return
    await msg.reply_text("📜 Recent signals\n" + "\n".join(f"#{r['id']} {r['asset']} {r['direction']} | {r['strategy']} | {r['outcome'] or 'PENDING'}" for r in rows))

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user, msg = update.effective_user, update.effective_message
    if not user or not msg: return
    s = await stats(user.id)
    await msg.reply_text(f"📊 Signal Tracking\nTotal: {s['total']}\nWins: {s['wins']}\nLosses: {s['losses']}\nPending: {s['pending']}\nSettled win rate: {s['rate']:.1f}%")

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Legacy admin command; admin identity is always checked before actions."""
    user, msg = update.effective_user, update.effective_message
    if not user or not msg:
        return
    if user.id not in ADMIN_IDS:
        await msg.reply_text("⛔ Admin only.")
        return
    if not context.args:
        await msg.reply_text("Usage: /secret status | /secret scan | /secret on")
        return
    action = context.args[0].lower()
    if action == "status":
        await status_cmd(update, context)
    elif action == "scan":
        await run_analysis(msg, user.id, context)
    elif action == "on":
        global signals_enabled
        signals_enabled = True
        await msg.reply_text("✅ Signals ON করা হয়েছে।")
    else:
        await msg.reply_text("Available: status, scan, on")


def _today_session_rows():
    today = datetime.now().strftime("%Y-%m-%d")
    return [x for x in session_signals if x.get("date") == today]


def _format_final_reports():
    rows = _today_session_rows()
    date_text = datetime.now().strftime("%Y-%m-%d")
    market_lines = []
    for row in rows:
        mark = "✅" if row.get("outcome") == "WIN" else ("✖️" if row.get("outcome") == "LOSS" else "⌛")
        market_lines.append(f"❒ {row['time']} - {row['pair']} - {row['direction']} {mark}")
    if not market_lines:
        market_lines = ["আজ এই bot session-এ কোনো signal record হয়নি।"]
    settled = [x for x in rows if x.get("outcome") in ("WIN", "LOSS")]
    wins = sum(x.get("outcome") == "WIN" for x in settled)
    losses = sum(x.get("outcome") == "LOSS" for x in settled)
    rate = (wins / len(settled) * 100) if settled else 0
    report1 = (
        "========== STM PRO BOT ==========\n"
        "━━━━━━━━━・━━━━━━━━━\n"
        f"📆 - {date_text}\n"
        "━━━━━━━━━・━━━━━━━━━\n"
        "💶 OTC / SELECTED MARKETS FINAL 💶\n"
        + "\n".join(market_lines)
        + "\n━━━━━━━━━・━━━━━━━━━\n"
    )
    report2 = (
        "📊 STMPRO BOT FINAL RESULT\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 WIN: {wins} | 😣 LOSS: {losses} | ⌛ PENDING: {sum(x.get('outcome') not in ('WIN','LOSS') for x in rows)}\n"
        f"💎 STM PRO BOT ACCURACY: {rate:.1f}% (settled signals only)\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "Please send your feedback with a fresh mind.\n"
        f"Feedback ID: {FEEDBACK_ID or 'Not configured in .env'}\n"
        "If feedback is insufficient, the bot owner may pause the service.\n"
        "⚠️ Past results do not guarantee future outcomes."
    )
    return report1, report2


async def stmop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Secret stop command. Verify admin ID before stopping signals or posting reports."""
    user, msg = update.effective_user, update.effective_message
    if not user or not msg:
        return
    if user.id not in ADMIN_IDS:
        await msg.reply_text("⛔ Admin only.")
        return
    global signals_enabled
    signals_enabled = False
    report1, report2 = _format_final_reports()
    target = CHANNEL_ID if CHANNEL_ID else msg.chat_id
    try:
        await context.bot.send_message(chat_id=target, text=report1)
        await context.bot.send_message(chat_id=target, text=report2)
        await msg.reply_text("🛑 Signals OFF করা হয়েছে। Final report-এর ২টি message পাঠানো হয়েছে। আবার চালু করতে /secret on দিন।")
    except Exception as exc:
        log.exception("Could not send final reports")
        await msg.reply_text(f"🛑 Signals OFF হয়েছে, কিন্তু report পাঠানো যায়নি: {type(exc).__name__}: {exc}")


async def _candle_outcome(symbol, candle_start, direction):
    """Return WIN/LOSS/DRAW for the requested 60-second candle, or UNKNOWN."""
    candles = await feed.candles(symbol, 60, 40)
    # Quotex timestamps are epoch seconds; allow small rounding differences.
    # Only accept the candle whose opening timestamp matches the requested minute.
    # Do not accidentally substitute the following minute's candle.
    candidates = [c for c in candles if abs(c.get("time", 0) - candle_start) <= 2]
    if not candidates:
        raise MarketDataError(f"{symbol}: exact target candle {int(candle_start)} not found")
    candle = candidates[0]
    if time.time() < candle_start + SIGNAL_EXPIRY_SECONDS:
        raise MarketDataError(f"{symbol}: target candle has not closed yet")
    if candle["close"] == candle["open"]:
        outcome = "DRAW"
    else:
        candle_direction = "CALL" if candle["close"] > candle["open"] else "PUT"
        outcome = "WIN" if candle_direction == direction else "LOSS"
    return {"outcome": outcome, "open": candle["open"], "close": candle["close"], "time": candle["time"]}


async def settle_signal(app, signal_id, pair, symbol, direction, destination, trade_start):
    """Check the scheduled candle; if it loses, check one following candle for MTG.

    The bot does not execute trades or know whether a user actually took MTG. The
    reported second-candle recovery is a candle-based proxy, not broker P/L.
    """
    outcome = "UNKNOWN"
    result_stage = "FIRST CANDLE"
    first = second = None
    try:
        await asyncio.sleep(max(0, trade_start + SIGNAL_EXPIRY_SECONDS - time.time() + 2))
        first = await _candle_outcome(symbol, trade_start, direction)
        if first["outcome"] == "WIN":
            outcome = "WIN"
        elif first["outcome"] == "DRAW":
            outcome = "DRAW"
        else:
            result_stage = "MTG CANDLE"
            await asyncio.sleep(max(0, trade_start + 2 * SIGNAL_EXPIRY_SECONDS - time.time() + 2))
            second = await _candle_outcome(symbol, trade_start + SIGNAL_EXPIRY_SECONDS, direction)
            outcome = "WIN" if second["outcome"] == "WIN" else ("LOSS" if second["outcome"] == "LOSS" else "DRAW")
    except Exception as exc:
        log.warning("Result could not be verified for signal #%s: %s", signal_id, exc)

    for row in reversed(session_signals):
        if row.get("id") == signal_id:
            row["outcome"] = outcome
            break
    if outcome in ("WIN", "LOSS", "DRAW"):
        try:
            await record_result(signal_id, outcome)
        except Exception:
            log.exception("Could not save result for signal #%s", signal_id)

    direction_text = "🔴 𝗣𝗨𝗧 ▼" if direction == "PUT" else "🟢 𝗖𝗔𝗟𝗟 ▲"
    result_text = "🎉 𝗪𝗜𝗡 ✅" if outcome == "WIN" else ("😣 𝗟𝗢𝗦𝗦 ❌" if outcome == "LOSS" else ("➖ DRAW" if outcome == "DRAW" else "⌛ RESULT UNVERIFIED"))
    today_rows = _today_session_rows()
    settled = [x for x in today_rows if x.get("outcome") in ("WIN", "LOSS")]
    wins = sum(x.get("outcome") == "WIN" for x in settled)
    losses = sum(x.get("outcome") == "LOSS" for x in settled)
    pair_rows = [x for x in settled if x.get("pair") == pair]
    pair_wins = sum(x.get("outcome") == "WIN" for x in pair_rows)
    pair_losses = sum(x.get("outcome") == "LOSS" for x in pair_rows)
    accuracy = wins / len(settled) * 100 if settled else 0
    feedback = FEEDBACK_ID or "Not configured (set FEEDBACK_ID in .env)"
    result_post = (
        "📊 𝗦𝗧𝗠 𝗣𝗥𝗢 𝗕𝗢𝗧 — 𝗥𝗘𝗦𝗨𝗟𝗧\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Pair: {pair}\n"
        f"⏱️ Trade time: {datetime.fromtimestamp(trade_start).strftime('%H:%M')}–{datetime.fromtimestamp(trade_start + 60).strftime('%H:%M')}\n"
        f"🔮 Direction: {direction_text}\n"
        f"🧾 Result check: {result_stage}\n"
        + (f"📈 FIRST CANDLE — Open: {first['open']} | Close: {first['close']}\n" if first else "📈 FIRST CANDLE — Open: unavailable | Close: unavailable\n")
        + (f"📉 MTG CANDLE — Open: {second['open']} | Close: {second['close']}\n" if second else "")
        + "\n"
        + f"{result_text}\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔥 WIN: {wins} | 😣 LOSS: {losses}\n"
        f"💎 THIS PAIR: {pair_wins}W × {pair_losses}L\n"
        f"📊 ACCURACY: {accuracy:.1f}% (verified WIN/LOSS only)\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"💬 Feedback: {feedback}\n"
        "Please share honest feedback to help keep the bot running.\n"
        "⚠️ Result is calculated from Quotex 1-minute candles; MTG uses the next candle after a first-candle LOSS. The bot does not execute trades or verify broker P/L."
    )
    for target in destination if isinstance(destination, (list, tuple, set)) else [destination]:
        try:
            await app.bot.send_message(chat_id=target, text=result_post)
        except Exception:
            log.exception("Could not post result for signal #%s to %s", signal_id, target)

async def run_analysis(message, uid, context):
    global signals_enabled
    if not signals_enabled:
        await message.reply_text("🛑 Signals বর্তমানে OFF। Admin চালু না করা পর্যন্ত নতুন signal দেওয়া হবে না।")
        return
    prefs = setup_user(uid)
    if not await member_ok(context, uid):
        context.user_data.pop("channel_verified", None)
        await message.reply_text("⛔ আগে Telegram channel membership verify করুন।", reply_markup=welcome_keyboard())
        return
    context.user_data["channel_verified"] = True
    pairs = selected_pairs(prefs)
    strategy = prefs.get("strategy")
    if not pairs or not strategy:
        await message.reply_text("আগে category, pair ও strategy নির্বাচন সম্পূর্ণ করুন।", reply_markup=categories_keyboard(prefs))
        return
    for line in WARNINGS:
        try:
            sent = await message.reply_text("⚠️ " + line)
            await asyncio.sleep(1.5)
            await sent.delete()
        except Exception:
            log.warning("Could not delete warning message", exc_info=True)
    await message.reply_text("🔎 Analysis চলছে… এক cycle-এ সর্বোচ্চ ১টি signal পাঠানো হবে।")
    methods = {
        "AI": ("AI STRATEGY", ai_analyze),
        "STM": ("STM STRATEGY", stm_analyze),
        "RANDOM": ("RANDOM STRATEGY", random_analyze),
        "BLACK4X": ("BLACK4X STRATEGY", black4x_analyze),
    }
    method_order = ["AI", "STM", "BLACK4X", "RANDOM"] if strategy == "ALL" else [strategy]
    errors, found, valid_markets = [], None, []
    # First establish which selected symbols actually return fresh, valid Quotex candles.
    # RANDOM chooses direction only after this validation; it cannot invent a market.
    for pair in pairs:
        symbol = SYMBOLS.get(pair)
        if not symbol:
            errors.append(f"{pair}: symbol mapping নেই")
            continue
        try:
            candles = await feed.candles(symbol, 60, 100, min_valid=2)
            # Strategies receive completed candles only, avoiding a still-forming candle.
            closed = [c for c in candles if c["time"] + 60 <= time.time() + 1]
            if len(closed) < 2:
                raise MarketDataError(f"{symbol}: 2টি completed candle পাওয়া যায়নি")
            valid_markets.append((pair, symbol, closed))
        except Exception as exc:
            log.warning("Market validation failed for %s (%s): %s", pair, symbol, exc)
            errors.append(f"{pair} [{symbol}]: {type(exc).__name__}: {exc}")
    if not valid_markets:
        await message.reply_text(
            "❌ কোনো নির্বাচিত pair-এর fresh Quotex candle যাচাই করা যায়নি। তাই ভুল/অপ্রাপ্য market দিয়ে signal পাঠানো হয়নি.\n\n" + "\n".join(errors[:6])
        )
        return
    for method_key in method_order:
        label, method = methods.get(method_key, (None, None))
        if method is None:
            errors.append(f"Unknown strategy: {method_key}")
            continue
        candidates = valid_markets.copy()
        if method_key in ("RANDOM", "BLACK4X"):
            eligible = [(p, sym, cs) for p, sym, cs in candidates if len(cs) >= 2]
            if not eligible:
                errors.append(f"{method_key}: 2টি completed candle-সহ valid market নেই")
                continue
            pair, symbol, candles = secrets.choice(eligible)
            result = method(candles)
            if result and result.get("direction") in ("CALL", "PUT"):
                found = {"pair": pair, "symbol": symbol, "strategy": label, **result}
            break
        for pair, symbol, candles in candidates:
            try:
                result = method(candles)
            except Exception as exc:
                log.warning("%s strategy failed for %s: %s", method_key, pair, exc)
                errors.append(f"{pair}/{method_key}: {type(exc).__name__}: {exc}")
                continue
            if result and result.get("direction") in ("CALL", "PUT"):
                found = {"pair": pair, "symbol": symbol, "strategy": label, **result}
                break
        if found:
            break
    if not found:
        await message.reply_text("❌ Fresh Quotex candle পাওয়া গেছে, কিন্তু নির্বাচিত strategy কোনো valid setup দেয়নি।\n\n" + "\n".join(errors[:6]))
        return
    if not signals_enabled:
        await message.reply_text("🛑 Analysis চলাকালে admin signals OFF করেছেন; signal পোস্ট করা হয়নি।")
        return
    # Fetch payout before scheduling so a slow payout API call does not delay the signal.
    payout_value = None
    try:
        payout_value = await feed.payout(found["symbol"]) if found.get("symbol") else None
    except Exception as exc:
        log.info("Quotex payout unavailable for %s: %s", found.get("symbol"), exc)

    # Schedule the signal exactly 5 seconds before the next minute boundary.
    # Example: if the trade minute is 20:19:00, post at 20:18:55.
    current_ts = time.time()
    trade_start = (int(current_ts // 60) + 1) * 60
    signal_ts = trade_start - SIGNAL_LEAD_SECONDS
    # If analysis finishes too late to give a full 5-second warning, use the next minute.
    if signal_ts < current_ts + 0.5:
        trade_start += 60
        signal_ts = trade_start - SIGNAL_LEAD_SECONDS
    await asyncio.sleep(max(0, signal_ts - time.time()))
    if not signals_enabled:
        await message.reply_text("🛑 Admin signal OFF করেছেন; scheduled signal পাঠানো হয়নি।")
        return

    # Re-check the chosen pair shortly before posting so a disconnected/stale feed
    # cannot silently pass as a live market. Direction stays from selected strategy.
    try:
        fresh = await feed.candles(found["symbol"], 60, 30, min_valid=2)
        completed = [c for c in fresh if c["time"] + 60 <= time.time() + 1]
        if len(completed) < 2 or time.time() - completed[-1]["time"] > 180:
            raise MarketDataError("latest completed candle is stale or missing")
    except Exception as exc:
        await message.reply_text(f"❌ Signal বাতিল: {found['pair']} এর live Quotex candle পুনরায় যাচাই করা যায়নি ({type(exc).__name__}: {exc})")
        return
    sid = await save_signal(uid, found["pair"], found["direction"], found["strategy"], int(found.get("confidence", 50)), found.get("reason", "Analysis"))
    signal_now = datetime.now()
    trade_dt = datetime.fromtimestamp(trade_start)
    session_signals.append({"id": sid, "date": trade_dt.strftime("%Y-%m-%d"), "time": trade_dt.strftime("%H:%M"),
                            "pair": found["pair"], "direction": found["direction"], "outcome": None})
    direction_text = "🔴 𝗣𝗨𝗧 ▼" if found["direction"] == "PUT" else "🟢 𝗖𝗔𝗟𝗟 ▲"
    destinations = []
    if CHANNEL_ID:
        destinations.append(CHANNEL_ID)
    # Also send to the requesting user's bot chat; user must have started the bot.
    if uid not in destinations:
        destinations.append(uid)
    if not destinations:
        destinations = [message.chat_id]
    payout_text = f"{payout_value:g}% (Quotex)" if payout_value is not None else "Unavailable from Quotex API"
    signal_post = (
        "🤖 𝗦𝗧𝗠 𝗣𝗥𝗢 𝗕𝗢𝗧\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Pair: {found['pair']}\n"
        f"📣 Signal time: {signal_now.strftime('%H:%M:%S')}\n"
        f"⏱️ 𝗧𝗥𝗔𝗗𝗘 𝗧𝗜𝗠𝗘: {trade_dt.strftime('%H:%M:00')}–{datetime.fromtimestamp(trade_start + 59).strftime('%H:%M:%S')}\n"
        f"🔮 Direction: {direction_text}\n"
        f"💰 Payout: {payout_text}\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🎯 𝗦𝗜𝗚𝗡𝗔𝗟 𝗖𝗢𝗡𝗙𝗜𝗗𝗘𝗡𝗖𝗘\n"
        f"{'█' * 10} {int(found.get('confidence', 50))}%\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        f"Strategy: {found['strategy']} | Signal ID: #{sid}\n"
        "⚠️ 1-minute candle. Direction is not a guaranteed prediction."
    )
    sent_any = False
    send_errors = []
    for target in destinations:
        try:
            await context.bot.send_message(chat_id=target, text=signal_post)
            sent_any = True
        except Exception as exc:
            send_errors.append(f"{target}: {type(exc).__name__}: {exc}")
            log.warning("Signal post failed to %s: %s", target, exc)
    if not sent_any:
        await message.reply_text("Signal তৈরি হয়েছে, কিন্তু channel/bot—কোনো জায়গায় পোস্ট যায়নি.\n" + "\n".join(send_errors))
        return
    if send_errors:
        await message.reply_text("⚠️ Signal অন্তত এক জায়গায় পোস্ট হয়েছে; কিছু destination-এ পাঠানো যায়নি.\n" + "\n".join(send_errors))
    if found.get("symbol"):
        context.application.create_task(settle_signal(context.application, sid, found["pair"], found["symbol"], found["direction"], destinations, trade_start))
    else:
        await message.reply_text("ℹ️ Signal post হয়েছে; pair symbol mapping না থাকায় result verify করা যাবে না।")

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q: return
    await q.answer()
    uid, data = q.from_user.id, q.data or ""
    prefs = setup_user(uid)
    if data == "help":
        await q.message.reply_text("ℹ️ Channel join + verify করার পর category, pairs এবং strategy নির্বাচন করুন। Bot কোনো trade execute করে না.")
        return
    if data == "check_member":
        if await member_ok(context, uid):
            context.user_data["channel_verified"] = True
            prefs.update({"categories": [], "pairs": [], "strategy": None, "pair_page": 0, "pair_stage": "categories"})
            await q.edit_message_text("✅ Telegram channel membership verified.\n\nপ্রথমে category নির্বাচন করুন:", reply_markup=categories_keyboard(prefs))
        else:
            context.user_data.pop("channel_verified", None)
            await q.edit_message_text("❌ Membership verify করা যায়নি। Channel join করুন, CHANNEL_ID ঠিক আছে কিনা দেখুন, তারপর আবার Verify চাপুন।", reply_markup=welcome_keyboard())
        return
    if data == "categories_open":
        if not verified(context, uid):
            await q.edit_message_text("আগে channel membership verify করুন।", reply_markup=welcome_keyboard()); return
        await q.edit_message_text("📂 Category নির্বাচন করুন (একাধিক নেওয়া যাবে):", reply_markup=categories_keyboard(prefs)); return
    if not verified(context, uid):
        await q.message.reply_text("⛔ Signals ব্যবহারের আগে channel membership verify করতে হবে।", reply_markup=welcome_keyboard()); return
    if data.startswith("cat:"):
        cat = data.split(":", 1)[1]
        if cat in CATEGORY_ORDER:
            if cat in prefs["categories"]: prefs["categories"].remove(cat)
            else: prefs["categories"].append(cat)
            prefs["categories"] = [c for c in CATEGORY_ORDER if c in prefs["categories"]]
            prefs["pairs"] = []
            await q.edit_message_reply_markup(reply_markup=categories_keyboard(prefs))
    elif data == "catall":
        prefs["categories"] = [] if len(prefs["categories"]) == len(CATEGORY_ORDER) else CATEGORY_ORDER.copy()
        prefs["pairs"] = []
        await q.edit_message_reply_markup(reply_markup=categories_keyboard(prefs))
    elif data == "pairs_open":
        if not prefs["categories"]:
            await q.answer("আগে অন্তত একটি category নির্বাচন করুন।", show_alert=True); return
        prefs["pair_page"] = 0
        await q.edit_message_text("💱 Pair নির্বাচন করুন। ☑️ মানে selected; SELECT ALL PAIRS দিয়ে সব নিতে পারবেন।", reply_markup=pair_keyboard(prefs))
    elif data.startswith("pairpage:"):
        all_pairs = [p for c in CATEGORY_ORDER if c in prefs["categories"] for p in MARKETS[c]]
        max_page = max(0, (len(all_pairs)-1)//PAGE_SIZE)
        prefs["pair_page"] = max(0, min(max_page, prefs["pair_page"] + (1 if data.endswith("next") else -1)))
        await q.edit_message_reply_markup(reply_markup=pair_keyboard(prefs))
    elif data.startswith("pair:"):
        all_pairs = [p for c in CATEGORY_ORDER if c in prefs["categories"] for p in MARKETS[c]]
        try: pair = all_pairs[int(data.split(":", 1)[1])]
        except (ValueError, IndexError): return
        if pair in prefs["pairs"]: prefs["pairs"].remove(pair)
        else: prefs["pairs"].append(pair)
        prefs["pairs"] = [p for p in all_pairs if p in prefs["pairs"]]
        await q.edit_message_reply_markup(reply_markup=pair_keyboard(prefs))
    elif data == "pairall":
        all_pairs = [p for c in CATEGORY_ORDER if c in prefs["categories"] for p in MARKETS[c]]
        prefs["pairs"] = [] if len(prefs["pairs"]) == len(all_pairs) else all_pairs.copy()
        await q.edit_message_reply_markup(reply_markup=pair_keyboard(prefs))
    elif data == "back_categories":
        await q.edit_message_text("📂 Category নির্বাচন করুন:", reply_markup=categories_keyboard(prefs))
    elif data == "strategy_open":
        if not selected_pairs(prefs):
            await q.answer("আগে অন্তত একটি pair নির্বাচন করুন।", show_alert=True); return
        await q.edit_message_text("🎯 Strategy নির্বাচন করুন:", reply_markup=strategy_keyboard(prefs))
    elif data.startswith("strategy:"):
        key = data.split(":", 1)[1]
        if key not in STRATEGY_LABELS: return
        if not selected_pairs(prefs):
            await q.answer("আগে pair নির্বাচন করুন।", show_alert=True); return
        prefs["strategy"] = key
        await q.edit_message_text(f"✅ Selection complete\n\nCategories: {', '.join(prefs['categories'])}\nPairs selected: {len(selected_pairs(prefs))}\nStrategy: {STRATEGY_LABELS[key]}\n\nAnalysis শুরু করতে নিচের button চাপুন।", reply_markup=ready_keyboard())
    elif data == "start_analysis":
        await run_analysis(q.message, uid, context)
    elif data == "stats":
        s = await stats(uid)
        await q.message.reply_text(f"Total: {s['total']} | Wins: {s['wins']} | Losses: {s['losses']} | Pending: {s['pending']}")

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and update.effective_message:
        await run_analysis(update.effective_message, update.effective_user.id, context)

async def post_init(app: Application):
    await init_db()
    log.info("Database initialized")

async def post_shutdown(app: Application):
    await feed.close()

def main():
    if not TOKEN:
        raise SystemExit("BOT_TOKEN missing. আপনার local .env ফাইলে BOT_TOKEN সেট করুন।")
    app = Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("secret", admin_cmd))
    app.add_handler(CommandHandler("stmop", stmop_cmd))
    app.add_handler(MessageHandler(filters.Regex(r"^/STMOP(?:@\w+)?(?:\s|$)"), stmop_cmd))
    app.add_handler(CallbackQueryHandler(callback))
    log.info("Starting %s", BOT_NAME)
    if log_import_error:
        log.warning(log_import_error)
    # Python 3.14 no longer guarantees an implicit event loop in MainThread.
    # python-telegram-bot's run_polling() expects one to exist before it starts.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
