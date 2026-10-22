import os
import logging
import asyncio

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from database import (
    init_db,
    add_user,
    save_signal,
    recent_signals,
    stats,
)
from market_data import QuotexDemoFeed, MarketDataError
from scanner import scan_market


# =========================
# CONFIGURATION
# =========================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

log = logging.getLogger("stm")

TOKEN = os.getenv("BOT_TOKEN", "").strip()

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "").strip()

BOT_NAME = "STM PRO BOT"

STRATEGIES = {
    "AI": "AI Strategy",
    "STM": "STM Strategy",
    "RANDOM": "Random (disabled)",
    "ALL": "All strategies",
}

MARKETS = {
    "FOREX": "Forex",
    "OTC": "OTC",
    "CRYPTO": "Crypto",
    "COMMODITIES": "Commodities",
}

feed = QuotexDemoFeed()
user_prefs = {}


# =========================
# KEYBOARD
# =========================

def home_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📡 Get Signals",
                callback_data="signals",
            ),
            InlineKeyboardButton(
                "🔎 Scan Market",
                callback_data="scan",
            ),
        ],
        [
            InlineKeyboardButton(
                "🎯 Strategy",
                callback_data="strategy",
            ),
            InlineKeyboardButton(
                "🌐 Market",
                callback_data="market",
            ),
        ],
        [
            InlineKeyboardButton(
                "📊 My Stats",
                callback_data="stats",
            ),
            InlineKeyboardButton(
                "ℹ️ Help",
                callback_data="help",
            ),
        ],
    ])


# =========================
# MEMBERSHIP CHECK
# =========================

async def member_ok(context, user_id):
    if not CHANNEL_ID:
        return True

    try:
        member = await context.bot.get_chat_member(
            CHANNEL_ID,
            user_id,
        )

        return member.status in (
            "member",
            "administrator",
            "creator",
        )

    except Exception:
        log.exception("Channel membership check failed")
        return False


# =========================
# START
# =========================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user
    message = update.effective_message

    if user is None or message is None:
        return

    await add_user(
        user.id,
        user.username or "",
        user.first_name or "",
    )

    if CHANNEL_ID and not await member_ok(
        context,
        user.id,
    ):
        buttons = []

        if CHANNEL_URL:
            buttons.append([
                InlineKeyboardButton(
                    "📢 Join Channel",
                    url=CHANNEL_URL,
                )
            ])

        buttons.append([
            InlineKeyboardButton(
                "✅ I Joined — Check",
                callback_data="check_member",
            )
        ])

        await message.reply_text(
            f"Welcome to {BOT_NAME}!\n\n"
            "প্রথমে আমাদের Telegram channel-এ Join করুন।",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    await message.reply_text(
        f"🚀 {BOT_NAME}\n\n"
        "Demo market analysis and signal tracking bot.\n\n"
        "⚠️ Signals are not guaranteed. "
        "Demo analysis only.",
        reply_markup=home_keyboard(),
    )


# =========================
# HELP
# =========================

async def help_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.effective_message.reply_text(
        "📖 STM PRO BOT Help\n\n"
        "/start — Main menu\n"
        "/status — Connection status\n"
        "/scan — Scan selected market\n"
        "/history — Recent signals\n"
        "/stats — Tracked results\n"
        "/secret status — Admin status\n"
        "/secret scan — Admin scan\n\n"
        "This bot does not place trades.\n"
        "Signal outcomes are not guaranteed.",
        reply_markup=home_keyboard(),
    )


# =========================
# STATUS
# =========================

async def status_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await update.effective_message.reply_text(
        f"🤖 {BOT_NAME}\n\n"
        f"Quotex feed: "
        f"{'CONNECTED' if feed.connected else 'NOT CONNECTED'}\n"
        "Account mode: DEMO ONLY\n"
        "Automatic trading: DISABLED\n\n"
        "Connection status does not guarantee "
        "that market data is available."
    )


# =========================
# HISTORY
# =========================

async def history_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user

    rows = await recent_signals(user.id, 10)

    if not rows:
        await update.effective_message.reply_text(
            "এখনো কোনো tracked signal নেই।"
        )
        return

    lines = ["📜 Recent signals"]

    for row in rows:
        outcome = row["outcome"] or "PENDING"

        lines.append(
            f"#{row['id']} {row['asset']} "
            f"{row['direction']} | "
            f"{row['strategy']} | {outcome}"
        )

    await update.effective_message.reply_text(
        "\n".join(lines)
    )


# =========================
# STATS
# =========================

async def stats_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    result = await stats(update.effective_user.id)

    await update.effective_message.reply_text(
        "📊 Signal Tracking\n\n"
        f"Total: {result['total']}\n"
        f"Wins: {result['wins']}\n"
        f"Losses: {result['losses']}\n"
        f"Pending: {result['pending']}\n"
        f"Settled win rate: {result['rate']:.1f}%"
    )


# =========================
# MARKET SCANNER
# =========================

async def do_scan(message, user_id):
    prefs = user_prefs.get(
        user_id,
        {
            "strategy": "STM",
            "market": "FOREX",
        },
    )

    await message.reply_text(
        "🔎 Scanning market data...\n"
        "Please wait."
    )

    try:
        results = await scan_market(
            feed,
            prefs["market"],
            prefs["strategy"],
        )

    except MarketDataError as error:
        await message.reply_text(
            "❌ Market data unavailable.\n\n"
            f"Details: {error}\n\n"
            "Check your local .env configuration, "
            "PyQuotex installation and demo connection."
        )
        return

    except Exception:
        log.exception("Unexpected scanner error")

        await message.reply_text(
            "❌ Unexpected scanner error. "
            "Check the Termux terminal for details."
        )
        return

    if not results:
        await message.reply_text(
            "No qualified setup found.\n"
            "No signal was generated."
        )
        return

    output = [
        f"📡 {BOT_NAME} — Analysis Candidates",
        "",
    ]

    for result in results[:5]:
        signal_id = await save_signal(
            user_id,
            result["asset"],
            result["direction"],
            result["strategy"],
            result["confidence"],
            result["reason"],
        )

        output.append(
            f"{result['asset']} — {result['direction']}\n"
            f"Score: {result['confidence']}/100\n"
            f"Reason: {result['reason']}\n"
            f"Signal ID: #{signal_id}\n"
            f"Expiry: 1 minute (analysis only)\n"
        )

    output.append(
        "⚠️ The score is a rule-based rating, "
        "not a verified win probability."
    )

    await message.reply_text("\n".join(output))


async def scan_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await do_scan(
        update.effective_message,
        update.effective_user.id,
    )


# =========================
# ADMIN
# =========================

async def admin_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.effective_user.id not in ADMIN_IDS:
        await update.effective_message.reply_text(
            "⛔ Admin only."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n"
            "/secret status\n"
            "/secret scan"
        )
        return

    action = context.args[0].lower()

    if action == "status":
        await status_cmd(update, context)

    elif action == "scan":
        await do_scan(
            update.effective_message,
            update.effective_user.id,
        )

    else:
        await update.effective_message.reply_text(
            "Available commands: status, scan"
        )


# =========================
# BUTTON CALLBACKS
# =========================

async def callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if query is None:
        return

    await query.answer()

    user_id = query.from_user.id
    data = query.data

    if data == "check_member":
        if await member_ok(context, user_id):
            await query.edit_message_text(
                "✅ Membership verified.\n"
                "Send /start to open the menu."
            )
        else:
            await query.edit_message_text(
                "Membership not verified.\n"
                "Join the channel and try again."
            )

    elif data in ("signals", "scan"):
        if CHANNEL_ID and not await member_ok(
            context,
            user_id,
        ):
            await query.message.reply_text(
                "Please join the required channel first."
            )
            return

        await do_scan(query.message, user_id)

    elif data == "stats":
        result = await stats(user_id)

        await query.message.reply_text(
            f"Total: {result['total']}\n"
            f"Wins: {result['wins']}\n"
            f"Losses: {result['losses']}\n"
            f"Pending: {result['pending']}"
        )

    elif data == "help":
        await help_cmd(update, context)

    elif data == "strategy":
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"setstr:{key}",
                )
            ]
            for key, label in STRATEGIES.items()
        ])

        await query.message.reply_text(
            "Choose a strategy:",
            reply_markup=keyboard,
        )

    elif data.startswith("setstr:"):
        key = data.split(":", 1)[1]

        if key not in STRATEGIES:
            return

        user_prefs.setdefault(
            user_id,
            {"strategy": "STM", "market": "FOREX"},
        )["strategy"] = key

        await query.edit_message_text(
            f"Strategy selected: {STRATEGIES[key]}"
        )

    elif data == "market":
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"setmarket:{key}",
                )
            ]
            for key, label in MARKETS.items()
        ])

        await query.message.reply_text(
            "Choose a market:",
            reply_markup=keyboard,
        )

    elif data.startswith("setmarket:"):
        key = data.split(":", 1)[1]

        if key not in MARKETS:
            return

        user_prefs.setdefault(
            user_id,
            {"strategy": "STM", "market": "FOREX"},
        )["market"] = key

        await query.edit_message_text(
            f"Market selected: {MARKETS[key]}"
        )


# =========================
# APPLICATION LIFECYCLE
# =========================

async def run_bot():
    if not TOKEN:
        raise RuntimeError(
            "BOT_TOKEN missing. Set BOT_TOKEN in your .env file."
        )

    # Create database tables before starting Telegram polling.
    await init_db()

    app = (
        Application.builder()
        .token(TOKEN)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("scan", scan_cmd))
    app.add_handler(CommandHandler("secret", admin_cmd))
    app.add_handler(CallbackQueryHandler(callback))

    initialized = False
    started = False
    polling = False

    try:
        await app.initialize()
        initialized = True

        await app.start()
        started = True

        if app.updater is None:
            raise RuntimeError(
                "Telegram updater is unavailable."
            )

        await app.updater.start_polling(
            drop_pending_updates=True
        )
        polling = True

        log.info("%s started successfully.", BOT_NAME)

        # Keep the application running until stopped.
        await asyncio.Event().wait()

    finally:
        if polling and app.updater:
            await app.updater.stop()

        if started:
            await app.stop()

        if initialized:
            await app.shutdown()

        await feed.close()


def main():
    try:
        asyncio.run(run_bot())

    except KeyboardInterrupt:
        log.info("Bot stopped by user.")

    except Exception:
        log.exception("Bot startup/runtime failed.")
        raise


if __name__ == "__main__":
    main()
