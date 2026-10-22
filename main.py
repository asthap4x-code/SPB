"""STM PRO BOT - Option B main file.
Quotex read-only demo feed and Telegram workflow live here. Strategy logic and DB stay separate.
No order execution is implemented.
"""
import os
import time
import asyncio
import logging
import secrets
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from database import init_db, add_user, save_signal, recent_signals, stats
from ai_strategy import analyze as ai_analyze
from stm_strategy import analyze as stm_analyze
from random_strategy import analyze as random_analyze

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("stm_pro_bot")
TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "").strip()
BOT_NAME = "STM PRO BOT"
PAGE_SIZE = 10

# Display labels are kept exactly as requested. Quotex symbol support can vary by account/session.
MARKETS = {
    "FOREX": ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "USD/CAD", "AUD/USD", "NZD/USD"],
    "COMMODITIES": ["UKBrent (OTC)", "Silver (OTC)", "USCrude (OTC)", "Gold (OTC)"],
    "CRYPTO": ["Bitcoin (OTC)", "Ethereum Classic (OTC)", "Chainlink (OTC)", "Litecoin (OTC)", "Toncoin (OTC)", "Trump (OTC)", "Ripple (OTC)", "Zcash (OTC)", "Avalanche (OTC)", "Dash (OTC)", "Axie Infinity (OTC)", "Polkadot (OTC)", "Binance Coin (OTC)", "Ethereum (OTC)", "Cosmos (OTC)", "Bitcoin Cash (OTC)"],
    "CURRENCY": ["USD/ARS (OTC)", "USD/BDT (OTC)", "USD/BRL (OTC)", "USD/CAD (OTC)", "USD/CHF (OTC)", "USD/COP (OTC)", "USD/DZD (OTC)", "USD/EGP (OTC)", "USD/IDR (OTC)", "USD/INR (OTC)", "USD/JPY (OTC)", "USD/MXN (OTC)", "USD/NGN (OTC)", "USD/PHP (OTC)", "USD/PKR (OTC)", "USD/ZAR (OTC)", "AUD/CAD (OTC)", "AUD/CHF (OTC)", "AUD/JPY (OTC)", "AUD/NZD (OTC)", "AUD/USD (OTC)", "CAD/CHF (OTC)", "CAD/JPY (OTC)", "CHF/JPY (OTC)", "EUR/AUD (OTC)", "EUR/CAD (OTC)", "EUR/CHF (OTC)", "EUR/GBP (OTC)", "EUR/JPY (OTC)", "EUR/NZD (OTC)", "EUR/USD (OTC)", "GBP/AUD (OTC)", "GBP/CAD (OTC)", "GBP/CHF (OTC)", "GBP/JPY (OTC)", "GBP/NZD (OTC)", "GBP/USD (OTC)", "NZD/CAD (OTC)", "NZD/CHF (OTC)", "NZD/JPY (OTC)", "NZD/USD (OTC)"],
}
CATEGORY_ORDER = ["FOREX", "COMMODITIES", "CRYPTO", "CURRENCY"]
CATEGORY_LABELS = {"FOREX": "FOREX", "COMMODITIES": "COMMODITIES", "CRYPTO": "CRYPTO", "CURRENCY": "CURRENCY"}
STRATEGY_LABELS = {"AI": "AI STRATEGY", "STM": "STM STRATEGY", "RANDOM": "RANDOM STRATEGY", "ALL": "SELECT ALL"}

# Best-effort names for PyQuotex. Unsupported names are skipped, never fabricated into a signal.
SYMBOLS = {
    "EUR/USD":"EURUSD", "GBP/USD":"GBPGBP" if False else "GBPUSD", "USD/JPY":"USDJPY", "USD/CHF":"USDCHF", "USD/CAD":"USDCAD", "AUD/USD":"AUDUSD", "NZD/USD":"NZDUSD",
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

    async def candles(self, asset, period=60, count=100):
        await self.connect()
        try:
            raw = await self.client.get_candles(asset, time.time(), count, period)
            if not raw:
                raise MarketDataError(f"{asset}: Quotex candle দেয়নি (symbol/session unsupported হতে পারে)।")
            rows = []
            for c in raw:
                try:
                    rows.append({"time": float(c.get("time", c.get("from", 0))), "open": float(c["open"]), "high": float(c["high"]), "low": float(c["low"]), "close": float(c["close"])})
                except (KeyError, TypeError, ValueError):
                    continue
            rows.sort(key=lambda x: x["time"])
            if len(rows) < 25:
                raise MarketDataError(f"{asset}: পর্যাপ্ত valid candle নেই ({len(rows)}টি)।")
            return rows
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(f"{asset}: candle read failed: {type(exc).__name__}: {exc}")

    async def close(self):
        if self.client:
            try:
                await self.client.close()
            except Exception:
                pass
        self.client, self.connected = None, False

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
    rows = [[InlineKeyboardButton(STRATEGY_LABELS[k], callback_data=f"strategy:{k}")] for k in ("AI", "STM", "RANDOM", "ALL")]
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
    user, msg = update.effective_user, update.effective_message
    if not user or not msg: return
    if user.id not in ADMIN_IDS:
        await msg.reply_text("⛔ Admin only."); return
    if not context.args:
        await msg.reply_text("Usage: /secret status | /secret scan"); return
    if context.args[0].lower() == "status": await status_cmd(update, context)
    elif context.args[0].lower() == "scan": await run_analysis(msg, user.id, context)
    else: await msg.reply_text("Available: status, scan")

async def run_analysis(message, uid, context):
    prefs = setup_user(uid)
    # Re-check Telegram membership at signal time; cached verification alone is insufficient.
    if not await member_ok(context, uid):
        context.user_data.pop("channel_verified", None)
        await message.reply_text("⛔ আগে Telegram channel membership verify করুন।", reply_markup=welcome_keyboard()); return
    context.user_data["channel_verified"] = True
    pairs = selected_pairs(prefs)
    strategy = prefs.get("strategy")
    if not pairs or not strategy:
        await message.reply_text("আগে category, pair ও strategy নির্বাচন সম্পূর্ণ করুন।", reply_markup=categories_keyboard(prefs)); return
    for line in WARNINGS:
        try:
            sent = await message.reply_text("⚠️ " + line)
            await asyncio.sleep(1.5)
            await sent.delete()
        except Exception:
            log.warning("Could not delete warning message", exc_info=True)
    await message.reply_text("🔎 Quotex market data যাচাই করে analysis চলছে…\nএকটি analysis cycle-এ সর্বোচ্চ ১টি signal পাঠানো হবে।")
    methods = {"AI": ("AI STRATEGY", ai_analyze), "STM": ("STM STRATEGY", stm_analyze), "RANDOM": ("RANDOM STRATEGY", random_analyze)}
    if strategy == "ALL":
        method_order = ["AI", "STM", "RANDOM"]
    else:
        method_order = [strategy]
    errors = []
    found = None
    # ALL means scan all chosen pairs with AI first, then STM, then Random as fallback.
    for method_key in method_order:
        label, method = methods[method_key]
        for pair in pairs:
            symbol = SYMBOLS.get(pair)
            if not symbol:
                continue
            try:
                candles = await feed.candles(symbol, 60, 100)
            except MarketDataError as exc:
                errors.append(f"{pair}: {exc}")
                continue
            try:
                result = method(candles)
            except Exception as exc:
                log.exception("Strategy %s failed on %s", method_key, pair)
                errors.append(f"{pair}: strategy error {type(exc).__name__}")
                continue
            if method_key == "RANDOM":
                # Random fallback must still require successfully retrieved candles.
                result = result or {"direction": secrets.choice(("CALL", "PUT")), "confidence": 50, "reason": "Random fallback direction; not market-derived."}
            if result and result.get("direction") in ("CALL", "PUT"):
                found = {"pair": pair, "symbol": symbol, "strategy": label, **result}
                break
        if found:
            break
    if not found:
        detail = "\n".join(errors[:4])
        await message.reply_text("❌ কোনো signal তৈরি করা যায়নি। Quotex candle data পাওয়া যায়নি বা strategy qualify করেনি।\n\n" + (detail if detail else "No qualified setup found."))
        return
    sid = await save_signal(uid, found["pair"], found["direction"], found["strategy"], int(found.get("confidence", 50)), found.get("reason", "Rule-based analysis"))
    direction = "UP ⬆️" if found["direction"] == "CALL" else "DOWN ⬇️"
    await message.reply_text(f"📡 {BOT_NAME} — SIGNAL\n\nPair: {found['pair']}\nDirection: {direction}\nStrategy: {found['strategy']}\nScore: {int(found.get('confidence', 50))}/100\nReason: {found.get('reason', 'Rule-based analysis')}\nSignal ID: #{sid}\nExpiry: 1 minute (analysis only)\n\n⚠️ Score win probability নয়। Signal নিশ্চিত নয়; bot trade execute করে না।")

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
    app.add_handler(CallbackQueryHandler(callback))
    log.info("Starting %s", BOT_NAME)
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
