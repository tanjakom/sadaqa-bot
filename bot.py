import os
import logging
import asyncio
from datetime import datetime

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    LabeledPrice,
    PreCheckoutQuery,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

# ---------------- Config ----------------

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DEFAULT_EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")
PORT = int(os.getenv("PORT", "10000"))

# Payment details (keep out of GitHub!)
PAYPAL_LINK = os.getenv("PAYPAL_LINK", "")  # e.g. https://paypal.me/TaisiraK
SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "")  # e.g. Sadaqa Jar (Tanja K.)
SEPA_IBAN = os.getenv("SEPA_IBAN", "")
SEPA_BIC = os.getenv("SEPA_BIC", "")  # optional
ZEN_NAME = os.getenv("ZEN_NAME", "")  # optional display name
ZEN_PHONE = os.getenv("ZEN_PHONE", "")  # optional (Zen-to-Zen)
ZEN_CARD = os.getenv("ZEN_CARD", "")  # optional card number (if you really want)
USDT_TRC20 = os.getenv("USDT_TRC20", "")  # keep private in env
USDC_ERC20 = os.getenv("USDC_ERC20", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DB_PATH = "data.db"

# user_id -> {"type": "water"|"iftar"}
PENDING: dict[int, dict] = {}


# ---------------- Helpers ----------------

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en


def admin_only(message: Message) -> bool:
    return bool(ADMIN_ID) and message.from_user and message.from_user.id == ADMIN_ID


async def notify_admin(text: str):
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, text)
        except Exception:
            logging.exception("Failed to notify admin")


def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None, parse_mode=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


def mask(s: str, head: int = 6, tail: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= head + tail + 3:
        return s
    return s[:head] + "…" + s[-tail:]


# ---------------- DB ----------------

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_prefs (
            user_id INTEGER PRIMARY KEY,
            lang TEXT NOT NULL
        )
        """)

        # defaults
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")

        await db.execute(
            "INSERT OR IGNORE INTO kv(k,v) VALUES('eur_to_stars', ?)",
            (str(DEFAULT_EUR_TO_STARS),)
        )

        await db.commit()


async def kv_get(key: str) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT v FROM kv WHERE k=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""


async def kv_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        await db.commit()


async def kv_inc_int(key: str, delta: int):
    val = int(await kv_get(key) or "0")
    val += int(delta)
    await kv_set(key, str(val))


async def get_rate() -> int:
    return int(await kv_get("eur_to_stars") or str(DEFAULT_EUR_TO_STARS))


async def set_user_lang(user_id: int, lang: str):
    lang = "ru" if lang == "ru" else "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO user_prefs(user_id, lang) VALUES(?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (user_id, lang),
        )
        await db.commit()


async def get_user_lang(user_id: int) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT lang FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


# ---------------- Keyboards ----------------

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()


def kb_main(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Сборы", "📋 Campaigns"), callback_data="list")
    kb.button(text=t(lang, "💳 Способы поддержки", "💳 Ways to support"), callback_data="support")
    kb.button(text=t(lang, "ℹ️ О Stars", "ℹ️ About Stars"), callback_data="stars_info")
    kb.button(text=t(lang, "🌐 Язык", "🌐 Language"), callback_data="lang_menu")
    kb.adjust(1)
    return kb.as_markup()


def kb_list(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💧 Сукья-ль-ма (вода)", "💧 Water (Sukya-l-ma)"), callback_data="water")
    kb.button(text=t(lang, "🍲 Программа ифтаров", "🍲 Iftars Program"), callback_data="iftar")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_water_pay(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ 10€", callback_data="pay_water_10")
    kb.button(text="⭐ 25€", callback_data="pay_water_25")
    kb.button(text="⭐ 50€", callback_data="pay_water_50")
    kb.button(text=t(lang, "⭐ Другая сумма", "⭐ Other amount"), callback_data="pay_water_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def kb_iftar_pay(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⭐ 5 порций", "⭐ 5 portions"), callback_data="pay_iftar_5")
    kb.button(text=t(lang, "⭐ 10 порций", "⭐ 10 portions"), callback_data="pay_iftar_10")
    kb.button(text=t(lang, "⭐ 20 порций", "⭐ 20 portions"), callback_data="pay_iftar_20")
    kb.button(text=t(lang, "⭐ Другое количество", "⭐ Other qty"), callback_data="pay_iftar_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def kb_support(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Telegram Stars", callback_data="support_stars")
    kb.button(text="🏦 SEPA (Europe)", callback_data="support_sepa")
    kb.button(text="💙 PayPal", callback_data="support_paypal")
    kb.button(text="💎 Crypto (USDT/USDC)", callback_data="support_crypto")
    kb.button(text="🟣 ZEN", callback_data="support_zen")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_support_back(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


def kb_sepa(lang: str, tag: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Скопировать IBAN", "📋 Copy IBAN"), callback_data="copy_sepa_iban")
    kb.button(text=t(lang, "📋 Скопировать назначение", "📋 Copy reference"), callback_data=f"copy_ref_{tag}")
    kb.button(text=t(lang, "✅ Я отправил(а) перевод", "✅ I sent the transfer"), callback_data=f"sent_sepa_{tag}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


def kb_crypto(lang: str, tag: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Скопировать USDT (TRC20)", "📋 Copy USDT (TRC20)"), callback_data="copy_usdt")
    kb.button(text=t(lang, "📋 Скопировать USDC (ERC20)", "📋 Copy USDC (ERC20)"), callback_data="copy_usdc")
    kb.button(text=t(lang, "✅ Я отправил(а)", "✅ I sent it"), callback_data=f"sent_crypto_{tag}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


def kb_paypal(lang: str, tag: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Скопировать ссылку PayPal", "📋 Copy PayPal link"), callback_data="copy_paypal")
    kb.button(text=t(lang, "📋 Скопировать сообщение", "📋 Copy message"), callback_data=f"copy_ref_{tag}")
    kb.button(text=t(lang, "✅ Я отправил(а)", "✅ I sent it"), callback_data=f"sent_paypal_{tag}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


def kb_zen(lang: str, tag: str):
    kb = InlineKeyboardBuilder()
    if ZEN_PHONE:
        kb.button(text=t(lang, "📋 Скопировать телефон (ZEN)", "📋 Copy phone (ZEN)"), callback_data="copy_zen_phone")
    if ZEN_CARD:
        kb.button(text=t(lang, "📋 Скопировать номер карты", "📋 Copy card number"), callback_data="copy_zen_card")
    kb.button(text=t(lang, "📋 Скопировать сообщение", "📋 Copy message"), callback_data=f"copy_ref_{tag}")
    kb.button(text=t(lang, "✅ Я отправил(а)", "✅ I sent it"), callback_data=f"sent_zen_{tag}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


# ---------------- Text builders ----------------

async def water_text(lang: str) -> str:
    target = int(await kv_get("water_target_eur") or "235")
    raised = int(await kv_get("water_raised_eur") or "0")
    bar = battery(raised, target)

    if lang == "ru":
        return (
            "💧 *Сукья-ль-ма*\n"
            "Раздача *5000 л* питьевой воды.\n\n"
            f"Нужно: *{target}€*\n"
            f"Собрано: *{raised}€* из *{target}€*\n"
            f"{bar}\n\n"
            "Выберите сумму:"
        )

    return (
        "💧 *Sukya-l-ma (Water)*\n"
        "Drinking water distribution (*5000 L*).\n\n"
        f"Goal: *{target}€*\n"
        f"Raised: *{raised}€* of *{target}€*\n"
        f"{bar}\n\n"
        "Choose amount:"
    )


async def iftar_text(lang: str) -> str:
    day = int(await kv_get("iftar_day") or "1")
    target = int(await kv_get("iftar_target_portions") or "100")
    raised = int(await kv_get("iftar_raised_portions") or "0")
    rate = await get_rate()
    bar = battery(raised, target)

    portion_stars = 4 * rate

    if lang == "ru":
        return (
            f"🍲 *Программа ифтаров — {day} Рамадана*\n\n"
            f"Цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n\n"
            f"1 порция = 4€ (≈ {portion_stars}⭐ при курсе 1€={rate}⭐)\n"
            "Выберите количество порций:"
        )

    return (
        f"🍲 *Iftars — {day} of Ramadan*\n\n"
        f"Goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n\n"
        f"1 portion = 4€ (≈ {portion_stars}⭐ at 1€={rate}⭐)\n"
        "Choose quantity:"
    )


async def current_tag_for_campaign(campaign: str) -> str:
    # Simple tags for statements/notes (neutral accounting labels)
    if campaign == "water":
        return "WATER"
    if campaign == "iftar":
        day = int(await kv_get("iftar_day") or "1")
        return f"IFTAR-{day}"
    return "SUPPORT"


# ---------------- Handlers ----------------

@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    saved = await get_user_lang(user_id)

    if not saved:
        await message.answer(
            "Ассаляму алейкум 🤍\n\nВыберите язык / Choose language:",
            reply_markup=kb_lang_select()
        )
        return

    lang = saved
    text = t(
        lang,
        "Ассаляму алейкум 🤍\n\n"
        "Добро пожаловать.\n"
        "Этот бот принимает поддержку через Telegram Stars.\n\n"
        "Нажмите «Сборы», чтобы выбрать сбор.",
        "Assalamu alaykum 🤍\n\n"
        "Welcome.\n"
        "This bot accepts support via Telegram Stars.\n\n"
        "Tap “Campaigns” to choose a campaign."
    )
    await message.answer(text, reply_markup=kb_main(lang))


@dp.message(Command("lang"))
async def lang_cmd(message: Message):
    await message.answer("Выберите язык / Choose language:", reply_markup=kb_lang_select())


@dp.callback_query(lambda c: c.data in {"lang_ru", "lang_en"})
async def choose_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()

    text = t(
        lang,
        "Язык установлен: Русский.\n\nНажмите «Сборы», чтобы выбрать сбор.",
        "Language set: English.\n\nTap “Campaigns” to choose a campaign."
    )
    await safe_edit(call, text, reply_markup=kb_main(lang))


@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "stars_info", "water", "iftar", "support"})
async def menu(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"

    if call.data == "lang_menu":
        await call.answer()
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    if call.data == "back":
        await call.answer()
        await safe_edit(call, t(lang, "Меню:", "Menu:"), reply_markup=kb_main(lang))
        return

    if call.data == "support":
        await call.answer()
        txt = t(
            lang,
            "💳 *Способы поддержки*\n\n"
            "Выберите удобный способ оплаты.\n"
            "Прогресс сборов через Stars обновляется автоматически.\n"
            "Для банковских/крипто переводов используйте кнопки «Я отправил(а)», чтобы уведомить нас.",
            "💳 *Ways to support*\n\n"
            "Choose the easiest payment method.\n"
            "Campaign progress updates automatically for Stars.\n"
            "For bank/crypto transfers tap “I sent it” to notify us."
        )
        await safe_edit(call, txt, reply_markup=kb_support(lang), parse_mode="Markdown")
        return

    if call.data == "list":
        await call.answer()
        await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_list(lang))
        return

    if call.data == "stars_info":
        rate = await get_rate()
        msg = t(
            lang,
            "Оплата проходит внутри Telegram через *Stars*.\n"
            "Для пользователя это выглядит как обычная покупка в Telegram.\n"
            "Обычно доступны: карта / Apple Pay / Google Pay (зависит от страны и Telegram).\n\n"
            f"Текущий курс в боте: *1€ = {rate}⭐*",
            "Payments happen inside Telegram via *Stars*.\n"
            "For the user it looks like a regular Telegram purchase.\n"
            "Typically: card / Apple Pay / Google Pay (depends on country & Telegram).\n\n"
            f"Current bot rate: *1€ = {rate}⭐*"
        )
        await call.answer()
        await safe_edit(call, msg, reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "water":
        await call.answer()
        await safe_edit(call, await water_text(lang), reply_markup=kb_water_pay(lang), parse_mode="Markdown")
        return

    if call.data == "iftar":
        await call.answer()
        await safe_edit(call, await iftar_text(lang), reply_markup=kb_iftar_pay(lang), parse_mode="Markdown")
        return


# ---------------- Support screens ----------------

@dp.callback_query(lambda c: c.data.startswith("support_"))
async def support_screens(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"

    # Determine tag from last viewed campaign: simplest = user chooses from campaign screens
    # For now: tag from current iftar day, or WATER by default
    # We'll show a neutral message tag that helps accounting.
    tag = await current_tag_for_campaign("iftar")  # default to current iftar tag
    # If user came from water/iftar screens, it's ok anyway; user can copy correct tag via buttons.

    if call.data == "support_stars":
        await call.answer()
        rate = await get_rate()
        txt = t(
            lang,
            "⭐ *Telegram Stars*\n\n"
            "Оплата происходит внутри Telegram как обычная покупка.\n"
            "Обычно доступны: карта / Apple Pay / Google Pay (зависит от страны).\n\n"
            f"Текущий курс: *1€ = {rate}⭐*.\n\n"
            "Чтобы поддержать сбор — откройте «Сборы» и выберите сумму/порции.",
            "⭐ *Telegram Stars*\n\n"
            "Payment happens inside Telegram like a regular purchase.\n"
            "Typically: card / Apple Pay / Google Pay (depends on country).\n\n"
            f"Current rate: *1€ = {rate}⭐*.\n\n"
            "To support, open “Campaigns” and choose amount/portions."
        )
        await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
        return

    if call.data == "support_sepa":
        await call.answer()
        tag = await current_tag_for_campaign("iftar")
        # Show minimal, copy-friendly. If missing env vars, show setup note.
        if not SEPA_IBAN or not SEPA_RECIPIENT:
            txt = t(
                lang,
                "🏦 *SEPA (Европа)*\n\n"
                "Реквизиты пока не настроены. (Нужно добавить SEPA_RECIPIENT и SEPA_IBAN в Render → Environment Variables.)",
                "🏦 *SEPA (Europe)*\n\n"
                "Details are not configured yet. (Add SEPA_RECIPIENT and SEPA_IBAN in Render → Environment Variables.)"
            )
            await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
            return

        bic_line = f"\nBIC: `{SEPA_BIC}`" if SEPA_BIC else ""
        txt = t(
            lang,
            "🏦 *Банковский перевод (SEPA / Европа)*\n\n"
            f"Получатель: `{SEPA_RECIPIENT}`\n"
            f"IBAN: `{SEPA_IBAN}`"
            f"{bic_line}\n"
            "Сумма: любая\n\n"
            "Назначение/сообщение (коротко):\n"
            f"`{tag}`\n\n"
            "⚠️ Пожалуйста, не пишите длинные комментарии.",
            "🏦 *Bank transfer (SEPA / Europe)*\n\n"
            f"Recipient: `{SEPA_RECIPIENT}`\n"
            f"IBAN: `{SEPA_IBAN}`"
            f"{bic_line}\n"
            "Amount: any\n\n"
            "Reference/message (short):\n"
            f"`{tag}`\n\n"
            "⚠️ Please avoid long comments."
        )
        await safe_edit(call, txt, reply_markup=kb_sepa(lang, tag), parse_mode="Markdown")
        return

    if call.data == "support_paypal":
        await call.answer()
        tag = await current_tag_for_campaign("iftar")
        if not PAYPAL_LINK:
            txt = t(
                lang,
                "💙 *PayPal*\n\n"
                "PayPal-ссылка пока не настроена. (Нужно добавить PAYPAL_LINK в Render → Environment Variables.)",
                "💙 *PayPal*\n\n"
                "PayPal link is not configured yet. (Add PAYPAL_LINK in Render → Environment Variables.)"
            )
            await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
            return

        txt = t(
            lang,
            "💙 *PayPal*\n\n"
            f"Ссылка: `{PAYPAL_LINK}`\n\n"
            "Сообщение (коротко):\n"
            f"`{tag}`\n\n"
            "После оплаты нажмите «Я отправил(а)».",
            "💙 *PayPal*\n\n"
            f"Link: `{PAYPAL_LINK}`\n\n"
            "Message (short):\n"
            f"`{tag}`\n\n"
            "After paying tap “I sent it”."
        )
        await safe_edit(call, txt, reply_markup=kb_paypal(lang, tag), parse_mode="Markdown")
        return

    if call.data == "support_crypto":
        await call.answer()
        tag = await current_tag_for_campaign("iftar")
        if not USDT_TRC20 and not USDC_ERC20:
            txt = t(
                lang,
                "💎 *Криптовалюта*\n\n"
                "Адреса пока не настроены. (Добавьте USDT_TRC20 и/или USDC_ERC20 в Render → Environment Variables.)",
                "💎 *Crypto*\n\n"
                "Addresses are not configured yet. (Add USDT_TRC20 and/or USDC_ERC20 in Render → Environment Variables.)"
            )
            await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
            return

        usdt_line = f"USDT (TRC20): `{mask(USDT_TRC20)}`" if USDT_TRC20 else "USDT (TRC20): —"
        usdc_line = f"USDC (ERC20): `{mask(USDC_ERC20)}`" if USDC_ERC20 else "USDC (ERC20): —"

        txt = t(
            lang,
            "💎 *Криптовалюта*\n\n"
            f"{usdt_line}\n"
            f"{usdc_line}\n\n"
            "⚠️ Важно: отправляйте строго в указанной сети (TRC20 / ERC20).\n\n"
            "Сообщение (коротко):\n"
            f"`{tag}`",
            "💎 *Crypto*\n\n"
            f"{usdt_line}\n"
            f"{usdc_line}\n\n"
            "⚠️ Important: send only via the specified network (TRC20 / ERC20).\n\n"
            "Message (short):\n"
            f"`{tag}`"
        )
        await safe_edit(call, txt, reply_markup=kb_crypto(lang, tag), parse_mode="Markdown")
        return

    if call.data == "support_zen":
        await call.answer()
        tag = await current_tag_for_campaign("iftar")

        if not (ZEN_PHONE or ZEN_CARD):
            txt = t(
                lang,
                "🟣 *ZEN*\n\n"
                "Данные ZEN пока не настроены. (Добавьте ZEN_PHONE и/или ZEN_CARD в Render → Environment Variables.)",
                "🟣 *ZEN*\n\n"
                "ZEN details are not configured. (Add ZEN_PHONE and/or ZEN_CARD in Render → Environment Variables.)"
            )
            await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
            return

        lines = []
        if ZEN_NAME:
            lines.append(f"{t(lang,'Получатель','Recipient')}: `{ZEN_NAME}`")
        if ZEN_PHONE:
            lines.append(f"ZEN {t(lang,'телефон','phone')}: `{ZEN_PHONE}`")
        if ZEN_CARD:
            lines.append(f"{t(lang,'Карта','Card')}: `{mask(ZEN_CARD, 6, 4)}`")

        txt = t(
            lang,
            "🟣 *ZEN*\n\n" + "\n".join(lines) + "\n\n"
            "Сообщение (коротко):\n"
            f"`{tag}`\n\n"
            "После оплаты нажмите «Я отправил(а)».",
            "🟣 *ZEN*\n\n" + "\n".join(lines) + "\n\n"
            "Message (short):\n"
            f"`{tag}`\n\n"
            "After paying tap “I sent it”."
        )
        await safe_edit(call, txt, reply_markup=kb_zen(lang, tag), parse_mode="Markdown")
        return


# ---------------- Copy / Sent callbacks ----------------

@dp.callback_query(lambda c: c.data.startswith("copy_") or c.data.startswith("sent_"))
async def copy_and_sent(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"

    data = call.data

    # COPY
    if data == "copy_sepa_iban":
        await call.answer("OK", show_alert=False)
        if SEPA_IBAN:
            await call.message.answer(f"`{SEPA_IBAN}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "IBAN не настроен.", "IBAN not configured."))
        return

    if data == "copy_paypal":
        await call.answer("OK", show_alert=False)
        if PAYPAL_LINK:
            await call.message.answer(f"`{PAYPAL_LINK}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "PayPal не настроен.", "PayPal not configured."))
        return

    if data == "copy_usdt":
        await call.answer("OK", show_alert=False)
        if USDT_TRC20:
            await call.message.answer(f"`{USDT_TRC20}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "USDT адрес не настроен.", "USDT address not configured."))
        return

    if data == "copy_usdc":
        await call.answer("OK", show_alert=False)
        if USDC_ERC20:
            await call.message.answer(f"`{USDC_ERC20}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "USDC адрес не настроен.", "USDC address not configured."))
        return

    if data == "copy_zen_phone":
        await call.answer("OK", show_alert=False)
        if ZEN_PHONE:
            await call.message.answer(f"`{ZEN_PHONE}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "Телефон ZEN не настроен.", "ZEN phone not configured."))
        return

    if data == "copy_zen_card":
        await call.answer("OK", show_alert=False)
        if ZEN_CARD:
            await call.message.answer(f"`{ZEN_CARD}`", parse_mode="Markdown")
        else:
            await call.message.answer(t(lang, "Карта не настроена.", "Card not configured."))
        return

    if data.startswith("copy_ref_"):
        tag = data.replace("copy_ref_", "").strip() or "SUPPORT"
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{tag}`", parse_mode="Markdown")
        return

    # SENT (manual notify admin)
    if data.startswith("sent_"):
        # Determine what kind and tag
        parts = data.split("_", 2)  # sent, method, tag...
        method = parts[1] if len(parts) >= 2 else "unknown"
        tag = parts[2] if len(parts) >= 3 else "SUPPORT"

        when = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        user = call.from_user
        await notify_admin(
            "📩 Manual payment notification\n"
            f"Method: {method}\n"
            f"Tag: {tag}\n"
            f"Time: {when}\n"
            f"User: @{user.username or '-'} / {user.id}"
        )

        await call.answer("OK", show_alert=False)
        await call.message.answer(
            t(lang, "✅ Спасибо! Мы получили отметку. При подтверждении оплаты обновим отчёт/архив.",
               "✅ Thank you! We got your note. We’ll confirm and update the archive.")
        )
        return


# ---------------- Pay callbacks (Stars) ----------------

@dp.callback_query(lambda c: c.data.startswith("pay_water_"))
async def pay_water(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"
    rate = await get_rate()

    if call.data == "pay_water_other":
        PENDING[call.from_user.id] = {"type": "water"}
        await call.answer()
        await call.message.answer(
            t(lang, "Введите сумму в евро (целое число), например 12:",
               "Enter amount in EUR (whole number), e.g. 12:")
        )
        return

    eur = int(call.data.split("_")[-1])
    stars = eur * rate
    payload = f"water:eur:{eur}"

    await call.answer()
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=t(lang, "Сукья-ль-ма (вода)", "Sukya-l-ma (Water)"),
        description=t(lang, f"Пожертвование: {eur}€ (≈ {stars}⭐)", f"Donation: {eur}€ (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
        provider_token="",  # Stars: empty
    )


@dp.callback_query(lambda c: c.data.startswith("pay_iftar_"))
async def pay_iftar(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"
    rate = await get_rate()

    if call.data == "pay_iftar_other":
        PENDING[call.from_user.id] = {"type": "iftar"}
        await call.answer()
        await call.message.answer(
            t(lang, "Введите количество порций (целое число), например 7:",
               "Enter number of portions (whole number), e.g. 7:")
        )
        return

    portions = int(call.data.split("_")[-1])
    stars = portions * 4 * rate
    payload = f"iftar:portions:{portions}"

    day = int(await kv_get("iftar_day") or "1")
    title_ru = f"Программа ифтаров — {day} Рамадана"
    title_en = f"Iftars — {day} of Ramadan"

    await call.answer()
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=(title_ru if lang == "ru" else title_en),
        description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
        provider_token="",  # Stars: empty
    )


# ---------------- Other amount input ----------------

@dp.message()
async def other_input(message: Message):
    if not message.from_user:
        return
    st = PENDING.get(message.from_user.id)
    if not st:
        return

    saved = await get_user_lang(message.from_user.id)
    lang = saved or "ru"
    rate = await get_rate()

    raw = (message.text or "").strip()

    try:
        n = int(raw)
        if n <= 0:
            raise ValueError
    except Exception:
        await message.answer(
            t(lang, "Нужно целое число > 0. Попробуйте ещё раз:",
               "Please send a whole number > 0. Try again:")
        )
        return

    if st["type"] == "water":
        eur = n
        stars = eur * rate
        payload = f"water:eur:{eur}"

        await bot.send_invoice(
            chat_id=message.from_user.id,
            title=t(lang, "Сукья-ль-ма (вода)", "Sukya-l-ma (Water)"),
            description=t(lang, f"Пожертвование: {eur}€ (≈ {stars}⭐)", f"Donation: {eur}€ (≈ {stars}⭐)"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
            provider_token="",
        )
        PENDING.pop(message.from_user.id, None)
        return

    if st["type"] == "iftar":
        portions = n
        stars = portions * 4 * rate
        payload = f"iftar:portions:{portions}"

        day = int(await kv_get("iftar_day") or "1")
        title_ru = f"Программа ифтаров — {day} Рамадана"
        title_en = f"Iftars — {day} of Ramadan"

        await bot.send_invoice(
            chat_id=message.from_user.id,
            title=(title_ru if lang == "ru" else title_en),
            description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
            provider_token="",
        )
        PENDING.pop(message.from_user.id, None)
        return


# ---------------- Payments (Stars) ----------------

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)


@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""

    saved = await get_user_lang(message.from_user.id)
    lang = saved or "ru"

    stars_total = sp.total_amount  # for XTR this is stars count
    rate = await get_rate()
    when = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    try:
        typ, unit, val = payload.split(":")
        val_i = int(val)
    except Exception:
        await notify_admin(
            "✅ Stars payment (unknown payload)\n"
            f"Stars: {stars_total}⭐\n"
            f"Time: {when}\n"
            f"User: @{message.from_user.username or '-'} / {message.from_user.id}\n"
            f"Payload: {payload}"
        )
        await message.answer(t(lang, "✅ Спасибо! Платёж получен.", "✅ Thank you! Payment received."))
        return

    if typ == "water" and unit == "eur":
        await kv_inc_int("water_raised_eur", val_i)

        await notify_admin(
            "✅ Оплата Stars (ВОДА)\n"
            f"Сумма: {val_i}€\n"
            f"Stars: {stars_total}⭐ (курс 1€={rate}⭐)\n"
            f"Время: {when}\n"
            f"User: @{message.from_user.username or '-'} / {message.from_user.id}\n"
            f"Payload: {payload}"
        )

        await message.answer(t(lang, "✅ Спасибо! Платёж получен.", "✅ Thank you! Payment received."))
        return

    if typ == "iftar" and unit == "portions":
        await kv_inc_int("iftar_raised_portions", val_i)
        day = int(await kv_get("iftar_day") or "1")

        await notify_admin(
            "✅ Оплата Stars (ИФТАРЫ)\n"
            f"День Рамадана: {day}\n"
            f"Порций: {val_i}\n"
            f"Stars: {stars_total}⭐ (курс 1€={rate}⭐)\n"
            f"Время: {when}\n"
            f"User: @{message.from_user.username or '-'} / {message.from_user.id}\n"
            f"Payload: {payload}"
        )

        await message.answer(t(lang, "✅ Спасибо! Платёж получен.", "✅ Thank you! Payment received."))
        return

    await notify_admin(
        "✅ Stars payment (unknown type)\n"
        f"Stars: {stars_total}⭐\n"
        f"Time: {when}\n"
        f"User: @{message.from_user.username or '-'} / {message.from_user.id}\n"
        f"Payload: {payload}"
    )
    await message.answer(t(lang, "✅ Спасибо! Платёж получен.", "✅ Thank you! Payment received."))


# ---------------- Admin commands ----------------

@dp.message(Command("set_rate"))
async def cmd_set_rate(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_rate 50")
        return
    rate = int(parts[1])
    if rate <= 0:
        await message.answer("Нужно число больше 0.")
        return
    await kv_set("eur_to_stars", str(rate))
    await message.answer(f"OK. Новый курс: 1€ = {rate}⭐")


@dp.message(Command("set_iftar_target"))
async def cmd_set_iftar_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_target 100")
        return
    await kv_set("iftar_target_portions", str(int(parts[1])))
    await message.answer("OK")


@dp.message(Command("set_iftar_day"))
async def cmd_set_iftar_day(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_day 10")
        return
    await kv_set("iftar_day", str(int(parts[1])))
    await kv_set("iftar_raised_portions", "0")
    await message.answer("OK")


@dp.message(Command("set_water_target"))
async def cmd_set_water_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_water_target 235")
        return
    await kv_set("water_target_eur", str(int(parts[1])))
    await message.answer("OK")


# ---------------- Health server for Render ----------------

async def health_server():
    app = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


# ---------------- Main ----------------

async def main():
    await db_init()
    await health_server()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())



if __name__ == "__main__":
    asyncio.run(main())

