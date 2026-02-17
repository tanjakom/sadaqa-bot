import os
import logging
import asyncio
from datetime import datetime
from typing import Optional, Tuple

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

# Python 3.9+
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


# =========================
# CONFIG
# =========================

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")  # REQUIRED
DEFAULT_EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")
PORT = int(os.getenv("PORT", "10000") or "10000")

# Render env var, e.g. "Europe/Helsinki"
TIMEZONE = os.getenv("TIMEZONE", "UTC").strip() or "UTC"

# Payment details (keep out of GitHub)
PAYPAL_LINK = os.getenv("PAYPAL_LINK", "").strip()
SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "").strip()
SEPA_IBAN = os.getenv("SEPA_IBAN", "").strip()
SEPA_BIC = os.getenv("SEPA_BIC", "").strip()
ZEN_NAME = os.getenv("ZEN_NAME", "").strip()
ZEN_PHONE = os.getenv("ZEN_PHONE", "").strip()
ZEN_CARD = os.getenv("ZEN_CARD", "").strip()
USDT_TRC20 = os.getenv("USDT_TRC20", "").strip()
USDC_ERC20 = os.getenv("USDC_ERC20", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing (set it in env)")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DB_PATH = "data.db"

# user_id -> pending state
# {"type": "water"|"iftar"|"water_full"|"iftar_full", ...}
PENDING: dict[int, dict] = {}

# admin_id -> {"action": "...", "meta": {...}}
ADMIN_PENDING: dict[int, dict] = {}


# =========================
# CHEATSHEETS (never lost)
# =========================

ADMIN_CHEATSHEET_RU = """\
🛠️ *Админ-подсказка (команды)*

*Быстрый статус*
/status

*Открыть/закрыть сборы (если нужно вручную)*
/open_water
/close_water
/open_iftar
/close_iftar

*Включить/выключить способы оплаты*
/stars_on  /stars_off
/manual_on /manual_off

*Настройки*
/set_rate 50
/set_water_target 235
/set_iftar_target 100
/set_iftar_day 10   (сбрасывает порции на 0, ставит target=100, открывает день, фиксирует дату дня)

*Ручная корректировка прогресса*
/add_water 10            (+10€ к текущей цистерне воды)
/add_iftar 5             (+5 порций к текущему дню)
/set_water_raised 120    (установить собранное по воде)
/set_iftar_raised 80     (установить собранное по ифтарам)

*Автоматизация*
— Как только закрывается день ифтаров (достигли цели) → автоматически открывается следующий день.
— Как только собрали на 1 цистерну воды → автоматически открывается сбор на следующую цистерну.

*Кнопка +50 порций*
— В админ-панели есть “➕ Iftar +50 (до 00:00)”.
— Увеличивает цель текущего дня на 50 (например 100 → 150), только до 00:00 по TIMEZONE.

*Админ-панель кнопками*
/admin
"""

USER_HELP_RU = """\
ℹ️ *Подсказка*

— В «Сборы» выбирайте кампанию и сумму/порции.
— Оплата Stars проходит внутри Telegram как обычная покупка.
— Для SEPA/PayPal/Crypto/ZEN используйте кнопки «Скопировать…» и «Я отправил(а)»,
чтобы админ получил уведомление.
— Если хотите закрыть день ифтаров или цистерну воды полностью — выберите соответствующую кнопку и оставьте подпись для видеоотчёта.
"""


# =========================
# HELPERS
# =========================

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en


def admin_only(message: Message) -> bool:
    return bool(message.from_user) and message.from_user.id == ADMIN_ID


def tzinfo():
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(TIMEZONE)
    except Exception:
        return ZoneInfo("UTC")


def now_local() -> datetime:
    tz = tzinfo()
    if tz is None:
        return datetime.utcnow()
    return datetime.now(tz)


def today_local_str() -> str:
    return now_local().date().isoformat()


def utc_now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


def user_link_html(user_id: int) -> str:
    return f'<a href="tg://user?id={user_id}">Написать плательщику</a>'


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None, parse_mode=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)


def mask(s: str, head: int = 6, tail: int = 6) -> str:
    if not s:
        return ""
    if len(s) <= head + tail + 3:
        return s
    return s[:head] + "…" + s[-tail:]


async def notify_admin_payment(
    *,
    kind: str,
    campaign: str,
    tag: str,
    amount_eur: Optional[int],
    portions: Optional[int],
    stars: Optional[int],
    method: str,
    user_id: int,
    username: str,
    when: str,
    note: str = "",
    extra: str = "",
):
    link = user_link_html(user_id)
    lines = [
        f"💰 {kind}",
        f"Method: {method}",
        f"Campaign: {campaign}",
        f"Tag: {tag}",
    ]
    if amount_eur is not None:
        lines.append(f"Amount: {amount_eur} EUR")
    if portions is not None:
        lines.append(f"Portions: {portions}")
    if stars is not None:
        lines.append(f"Stars: {stars}⭐")

    if note:
        safe_note = html_escape(note)
        lines.append("Video note:")
        lines.append(f"<pre>{safe_note}</pre>")

    lines += [
        f"Time: {when}",
        f"User: @{username} / {user_id}",
        link,
    ]
    if extra:
        lines.append(extra)

    await bot.send_message(
        ADMIN_ID,
        "\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


# =========================
# DB
# =========================

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

        # Sponsor notes for full-closure payments
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sponsor_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            campaign TEXT NOT NULL,         -- "iftar" or "water"
            day_or_batch INTEGER NOT NULL,  -- iftar_day or water_batch
            note TEXT NOT NULL,
            portions INTEGER,               -- for iftar
            amount_eur INTEGER,             -- for water
            created_utc TEXT NOT NULL
        )
        """)

        # defaults
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eur_to_stars', ?)", (str(DEFAULT_EUR_TO_STARS),))

        # WATER (per cistern/batch)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        # IFTAR (per day)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")

        # open/close flags and payment toggles
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_open','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_open','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('pay_stars_enabled','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('pay_manual_enabled','1')")

        # Date markers (for “+50 until 00:00”)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day_date', ?)", (today_local_str(),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch_date', ?)", (today_local_str(),))

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


async def kv_get_int(key: str, default: int = 0) -> int:
    s = (await kv_get(key) or "").strip()
    try:
        return int(s)
    except Exception:
        return default


async def kv_set_int(key: str, value: int):
    await kv_set(key, str(int(value)))


async def kv_inc_int(key: str, delta: int):
    val = await kv_get_int(key, 0)
    val += int(delta)
    await kv_set_int(key, val)


async def get_rate() -> int:
    return await kv_get_int("eur_to_stars", DEFAULT_EUR_TO_STARS)


async def set_user_lang(user_id: int, lang: str):
    lang = "ru" if lang == "ru" else "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO user_prefs(user_id, lang) VALUES(?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (user_id, lang),
        )
        await db.commit()


async def get_user_lang(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT lang FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None


async def sponsor_note_insert(
    *,
    user_id: int,
    campaign: str,
    day_or_batch: int,
    note: str,
    portions: Optional[int],
    amount_eur: Optional[int],
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO sponsor_notes(user_id, campaign, day_or_batch, note, portions, amount_eur, created_utc)
            VALUES(?,?,?,?,?,?,?)
            """,
            (user_id, campaign, day_or_batch, note, portions, amount_eur, utc_now_str()),
        )
        await db.commit()
        return int(cur.lastrowid)


async def sponsor_note_get(note_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, user_id, campaign, day_or_batch, note, portions, amount_eur, created_utc FROM sponsor_notes WHERE id=?",
            (note_id,),
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "user_id": row[1],
                "campaign": row[2],
                "day_or_batch": row[3],
                "note": row[4],
                "portions": row[5],
                "amount_eur": row[6],
                "created_utc": row[7],
            }


# =========================
# TAGS
# =========================

async def water_tag() -> str:
    batch = await kv_get_int("water_batch", 1)
    return f"WATER-{batch}"


async def iftar_tag() -> str:
    day = await kv_get_int("iftar_day", 1)
    return f"IFTAR-{day}"


# =========================
# AUTO-ADVANCE LOGIC
# =========================

async def advance_iftar_day():
    """
    Close current day and open next day immediately.
    """
    day = await kv_get_int("iftar_day", 1)
    await kv_set_int("iftar_day", day + 1)
    await kv_set_int("iftar_raised_portions", 0)
    await kv_set_int("iftar_target_portions", 100)
    await kv_set_int("iftar_open", 1)
    await kv_set("iftar_day_date", today_local_str())


async def advance_water_batch():
    """
    Close current cistern and open next cistern immediately.
    """
    batch = await kv_get_int("water_batch", 1)
    await kv_set_int("water_batch", batch + 1)
    await kv_set_int("water_raised_eur", 0)
    await kv_set_int("water_open", 1)
    await kv_set("water_batch_date", today_local_str())


async def maybe_auto_advance_iftar() -> bool:
    """
    If raised >= target -> advance to next day.
    Returns True if advanced.
    """
    raised = await kv_get_int("iftar_raised_portions", 0)
    target = await kv_get_int("iftar_target_portions", 100)
    if target > 0 and raised >= target:
        old_day = await kv_get_int("iftar_day", 1)
        await kv_set_int("iftar_open", 0)
        await advance_iftar_day()
        new_day = await kv_get_int("iftar_day", 1)
        await bot.send_message(
            ADMIN_ID,
            f"✅ IFTAR DAY COMPLETED & ADVANCED\nDay {old_day} reached {raised}/{target} -> opened Day {new_day}",
            disable_web_page_preview=True,
        )
        return True
    return False


async def maybe_auto_advance_water() -> bool:
    """
    If raised >= target -> advance to next cistern.
    Returns True if advanced.
    """
    raised = await kv_get_int("water_raised_eur", 0)
    target = await kv_get_int("water_target_eur", 235)
    if target > 0 and raised >= target:
        old_batch = await kv_get_int("water_batch", 1)
        await kv_set_int("water_open", 0)
        await advance_water_batch()
        new_batch = await kv_get_int("water_batch", 1)
        await bot.send_message(
            ADMIN_ID,
            f"✅ WATER CISTERN COMPLETED & ADVANCED\nBatch {old_batch} reached {raised}/{target}€ -> opened Batch {new_batch}",
            disable_web_page_preview=True,
        )
        return True
    return False


async def iftar_plus50_allowed_now() -> bool:
    """
    Allowed ONLY until local midnight of the local date stored in iftar_day_date.
    """
    day_date = (await kv_get("iftar_day_date") or "").strip()
    if not day_date:
        return False
    return today_local_str() == day_date


# =========================
# KEYBOARDS
# =========================

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
    kb.button(text=t(lang, "❓ Помощь", "❓ Help"), callback_data="help_user")
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
    kb.button(text=t(lang, "🤍 Закрыть цистерну полностью", "🤍 Close this cistern fully"), callback_data="water_full")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def kb_iftar_pay(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⭐ 5 порций", "⭐ 5 portions"), callback_data="pay_iftar_5")
    kb.button(text=t(lang, "⭐ 10 порций", "⭐ 10 portions"), callback_data="pay_iftar_10")
    kb.button(text=t(lang, "⭐ 20 порций", "⭐ 20 portions"), callback_data="pay_iftar_20")
    kb.button(text=t(lang, "⭐ Другое количество", "⭐ Other qty"), callback_data="pay_iftar_other")
    kb.button(text=t(lang, "🤍 Закрыть день полностью", "🤍 Close this day fully"), callback_data="iftar_full")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1, 1)
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


def kb_admin_panel():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Status", callback_data="adm_status")

    kb.button(text="💧 Water: OPEN/CLOSE", callback_data="adm_toggle_water")
    kb.button(text="🍲 Iftar: OPEN/CLOSE", callback_data="adm_toggle_iftar")

    kb.button(text="⭐ Stars: ON/OFF", callback_data="adm_toggle_stars")
    kb.button(text="💳 Manual: ON/OFF", callback_data="adm_toggle_manual")

    kb.button(text="➕ Iftar +50 (до 00:00)", callback_data="adm_iftar_plus50")

    kb.button(text="🧾 Admin help", callback_data="adm_help")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# TEXT BUILDERS
# =========================

async def water_text(lang: str) -> str:
    batch = await kv_get_int("water_batch", 1)
    target = await kv_get_int("water_target_eur", 235)
    raised = await kv_get_int("water_raised_eur", 0)
    is_open = await kv_get_int("water_open", 1)
    bar = battery(raised, target)

    status = t(lang, "ОТКРЫТ", "OPEN") if is_open else t(lang, "ЗАКРЫТ", "CLOSED")

    if lang == "ru":
        return (
            f"💧 *Сукья-ль-ма — цистерна #{batch}*\n"
            "Раздача *5000 л* питьевой воды.\n\n"
            f"Статус: *{status}*\n"
            f"Нужно: *{target}€*\n"
            f"Собрано: *{raised}€* из *{target}€*\n"
            f"{bar}\n\n"
            "Выберите сумму:"
        )
    return (
        f"💧 *Sukya-l-ma (Water) — cistern #{batch}*\n"
        "Drinking water distribution (*5000 L*).\n\n"
        f"Status: *{status}*\n"
        f"Goal: *{target}€*\n"
        f"Raised: *{raised}€* of *{target}€*\n"
        f"{bar}\n\n"
        "Choose amount:"
    )


async def iftar_text(lang: str) -> str:
    day = await kv_get_int("iftar_day", 1)
    target = await kv_get_int("iftar_target_portions", 100)
    raised = await kv_get_int("iftar_raised_portions", 0)
    is_open = await kv_get_int("iftar_open", 1)
    rate = await get_rate()
    bar = battery(raised, target)

    status = t(lang, "ОТКРЫТ", "OPEN") if is_open else t(lang, "ЗАКРЫТ", "CLOSED")
    portion_stars = 4 * rate

    if lang == "ru":
        return (
            f"🍲 *Программа ифтаров — День {day}*\n\n"
            f"Статус: *{status}*\n"
            f"Цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n\n"
            f"1 порция = 4€ (≈ {portion_stars}⭐ при курсе 1€={rate}⭐)\n"
            "Выберите количество порций:"
        )
    return (
        f"🍲 *Iftars — Day {day}*\n\n"
        f"Status: *{status}*\n"
        f"Goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n\n"
        f"1 portion = 4€ (≈ {portion_stars}⭐ at 1€={rate}⭐)\n"
        "Choose quantity:"
    )


# =========================
# START / LANGUAGE
# =========================

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


# =========================
# MAIN MENU CALLBACKS
# =========================

@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "stars_info", "water", "iftar", "support", "help_user"})
async def menu(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"

    if call.data == "lang_menu":
        await call.answer()
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    if call.data == "help_user":
        await call.answer()
        await safe_edit(call, USER_HELP_RU, reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "back":
        await call.answer()
        await safe_edit(call, t(lang, "Меню:", "Menu:"), reply_markup=kb_main(lang))
        return

    if call.data == "support":
        await call.answer()
        manual_enabled = await kv_get_int("pay_manual_enabled", 1)
        stars_enabled = await kv_get_int("pay_stars_enabled", 1)

        txt = t(
            lang,
            "💳 *Способы поддержки*\n\n"
            f"Stars: *{'ON' if stars_enabled else 'OFF'}*\n"
            f"Manual (SEPA/PayPal/Crypto/ZEN): *{'ON' if manual_enabled else 'OFF'}*\n\n"
            "Выберите удобный способ.\n"
            "Прогресс сборов через Stars обновляется автоматически.\n"
            "Для банковских/крипто переводов используйте «Я отправил(а)», чтобы уведомить админа.",
            "💳 *Ways to support*\n\n"
            f"Stars: *{'ON' if stars_enabled else 'OFF'}*\n"
            f"Manual (SEPA/PayPal/Crypto/ZEN): *{'ON' if manual_enabled else 'OFF'}*\n\n"
            "Choose the easiest payment method.\n"
            "Campaign progress updates automatically for Stars.\n"
            "For bank/crypto transfers tap “I sent it” to notify admin."
        )

        await safe_edit(call, txt, reply_markup=kb_support(lang), parse_mode="Markdown")
        return

    if call.data == "list":
        await call.answer()
        await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_list(lang))
        return

    if call.data == "stars_info":
        rate = await get_rate()
        stars_enabled = await kv_get_int("pay_stars_enabled", 1)
        msg = t(
            lang,
            "Оплата проходит внутри Telegram через *Stars*.\n"
            "Для пользователя это выглядит как обычная покупка в Telegram.\n"
            "Обычно доступны: карта / Apple Pay / Google Pay (зависит от страны и Telegram).\n\n"
            f"Stars сейчас: *{'ON' if stars_enabled else 'OFF'}*\n"
            f"Текущий курс в боте: *1€ = {rate}⭐*",
            "Payments happen inside Telegram via *Stars*.\n"
            "For the user it looks like a regular Telegram purchase.\n"
            "Typically: card / Apple Pay / Google Pay (depends on country & Telegram).\n\n"
            f"Stars now: *{'ON' if stars_enabled else 'OFF'}*\n"
            f"Current bot rate: *1€ = {rate}⭐*"
        )
        await call.answer()
        await safe_edit(call, msg, reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "water":
        await call.answer()
        is_open = await kv_get_int("water_open", 1)
        if not is_open:
            await safe_edit(call, t(lang, "💧 Сбор временно закрыт.", "💧 This campaign is temporarily closed."), reply_markup=kb_list(lang))
            return
        await safe_edit(call, await water_text(lang), reply_markup=kb_water_pay(lang), parse_mode="Markdown")
        return

    if call.data == "iftar":
        await call.answer()
        is_open = await kv_get_int("iftar_open", 1)
        if not is_open:
            await safe_edit(call, t(lang, "🍲 Сбор временно закрыт.", "🍲 This campaign is temporarily closed."), reply_markup=kb_list(lang))
            return
        await safe_edit(call, await iftar_text(lang), reply_markup=kb_iftar_pay(lang), parse_mode="Markdown")
        return


# =========================
# SUPPORT SCREENS
# =========================

@dp.callback_query(lambda c: c.data.startswith("support_"))
async def support_screens(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    stars_enabled = await kv_get_int("pay_stars_enabled", 1)
    manual_enabled = await kv_get_int("pay_manual_enabled", 1)

    if call.data == "support_stars":
        await call.answer()
        rate = await get_rate()
        txt = t(
            lang,
            "⭐ *Telegram Stars*\n\n"
            f"Статус: *{'ON' if stars_enabled else 'OFF'}*\n\n"
            "Оплата происходит внутри Telegram как обычная покупка.\n"
            "Обычно доступны: карта / Apple Pay / Google Pay (зависит от страны).\n\n"
            f"Текущий курс: *1€ = {rate}⭐*.\n\n"
            "Чтобы поддержать сбор — откройте «Сборы» и выберите сумму/порции.",
            "⭐ *Telegram Stars*\n\n"
            f"Status: *{'ON' if stars_enabled else 'OFF'}*\n\n"
            "Payment happens inside Telegram like a regular purchase.\n"
            "Typically: card / Apple Pay / Google Pay (depends on country).\n\n"
            f"Current rate: *1€ = {rate}⭐*.\n\n"
            "To support, open “Campaigns” and choose amount/portions."
        )
        await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
        return

    if manual_enabled == 0:
        await call.answer()
        txt = t(
            lang,
            "💳 Ручные способы оплаты (SEPA/PayPal/Crypto/ZEN) сейчас *выключены*.\n\n"
            "Пожалуйста, используйте Stars или попробуйте позже.",
            "💳 Manual payment methods (SEPA/PayPal/Crypto/ZEN) are currently *disabled*.\n\n"
            "Please use Stars or try later."
        )
        await safe_edit(call, txt, reply_markup=kb_support_back(lang), parse_mode="Markdown")
        return

    tag = await iftar_tag()

    if call.data == "support_sepa":
        await call.answer()
        if not SEPA_IBAN or not SEPA_RECIPIENT:
            txt = t(
                lang,
                "🏦 *SEPA (Европа)*\n\n"
                "Реквизиты пока не настроены. (Нужно добавить SEPA_RECIPIENT и SEPA_IBAN в переменные окружения.)",
                "🏦 *SEPA (Europe)*\n\n"
                "Details are not configured yet. (Add SEPA_RECIPIENT and SEPA_IBAN in env vars.)"
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
        if not PAYPAL_LINK:
            txt = t(
                lang,
                "💙 *PayPal*\n\n"
                "PayPal-ссылка пока не настроена. (Нужно добавить PAYPAL_LINK в переменные окружения.)",
                "💙 *PayPal*\n\n"
                "PayPal link is not configured yet. (Add PAYPAL_LINK in env vars.)"
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
        if not USDT_TRC20 and not USDC_ERC20:
            txt = t(
                lang,
                "💎 *Криптовалюта*\n\n"
                "Адреса пока не настроены. (Добавьте USDT_TRC20 и/или USDC_ERC20 в переменные окружения.)",
                "💎 *Crypto*\n\n"
                "Addresses are not configured yet. (Add USDT_TRC20 and/or USDC_ERC20 in env vars.)"
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
        if not (ZEN_PHONE or ZEN_CARD):
            txt = t(
                lang,
                "🟣 *ZEN*\n\n"
                "Данные ZEN пока не настроены. (Добавьте ZEN_PHONE и/или ZEN_CARD в переменные окружения.)",
                "🟣 *ZEN*\n\n"
                "ZEN details are not configured. (Add ZEN_PHONE and/or ZEN_CARD in env vars.)"
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


# =========================
# COPY / SENT CALLBACKS
# =========================

@dp.callback_query(lambda c: c.data.startswith("copy_") or c.data.startswith("sent_"))
async def copy_and_sent(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    data = call.data

    if data == "copy_sepa_iban":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{SEPA_IBAN}`" if SEPA_IBAN else t(lang, "IBAN не настроен.", "IBAN not configured."), parse_mode="Markdown")
        return

    if data == "copy_paypal":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{PAYPAL_LINK}`" if PAYPAL_LINK else t(lang, "PayPal не настроен.", "PayPal not configured."), parse_mode="Markdown")
        return

    if data == "copy_usdt":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{USDT_TRC20}`" if USDT_TRC20 else t(lang, "USDT адрес не настроен.", "USDT address not configured."), parse_mode="Markdown")
        return

    if data == "copy_usdc":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{USDC_ERC20}`" if USDC_ERC20 else t(lang, "USDC адрес не настроен.", "USDC address not configured."), parse_mode="Markdown")
        return

    if data == "copy_zen_phone":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{ZEN_PHONE}`" if ZEN_PHONE else t(lang, "Телефон ZEN не настроен.", "ZEN phone not configured."), parse_mode="Markdown")
        return

    if data == "copy_zen_card":
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{ZEN_CARD}`" if ZEN_CARD else t(lang, "Карта не настроена.", "Card not configured."), parse_mode="Markdown")
        return

    if data.startswith("copy_ref_"):
        tag = data.replace("copy_ref_", "").strip() or "SUPPORT"
        await call.answer("OK", show_alert=False)
        await call.message.answer(f"`{tag}`", parse_mode="Markdown")
        return

    if data.startswith("sent_"):
        parts = data.split("_", 2)  # sent, method, tag...
        method = parts[1] if len(parts) >= 2 else "unknown"
        tag = parts[2] if len(parts) >= 3 else "SUPPORT"

        when = utc_now_str()
        user_id = call.from_user.id
        username = call.from_user.username or "-"

        await notify_admin_payment(
            kind="MANUAL PAYMENT MARKED",
            method=method,
            campaign="manual",
            tag=tag,
            amount_eur=None,
            portions=None,
            stars=None,
            user_id=user_id,
            username=username,
            when=when,
            extra="(User pressed “I sent it”)",
        )

        await call.answer("OK", show_alert=False)
        await call.message.answer(
            t(lang, "✅ Спасибо! Мы получили отметку. При подтверждении оплаты обновим отчёт.",
               "✅ Thank you! We got your note. We’ll confirm and update the report.")
        )
        return


# =========================
# FULL-CLOSE (ask donor for video note)
# =========================

VIDEO_NOTE_TEMPLATE_RU = """\
Пожалуйста, отправьте подпись для видеоотчёта одним сообщением.

Шаблон:
С любовью от братьев/сестер [национальность — по желанию] из [город/страна/или название группы/джамаата].

Пример:
С любовью от братьев из Турции из Стамбула.
"""

@dp.callback_query(lambda c: c.data in {"iftar_full", "water_full"})
async def full_close_start(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"

    if await kv_get_int("pay_stars_enabled", 1) == 0:
        await call.answer(t(lang, "Оплата Stars временно выключена.", "Stars payments are disabled."), show_alert=True)
        return

    if call.data == "iftar_full":
        if await kv_get_int("iftar_open", 1) == 0:
            await call.answer(t(lang, "День закрыт.", "Day is closed."), show_alert=True)
            return

        target = await kv_get_int("iftar_target_portions", 100)
        raised = await kv_get_int("iftar_raised_portions", 0)
        remaining = max(0, target - raised)
        if remaining <= 0:
            await call.answer(t(lang, "Цель уже достигнута.", "Already reached."), show_alert=True)
            return

        PENDING[call.from_user.id] = {
            "type": "iftar_full",
            "remaining_portions": remaining,
            "day": await kv_get_int("iftar_day", 1),
            "tag": await iftar_tag(),
        }
        await call.answer()
        await call.message.answer(VIDEO_NOTE_TEMPLATE_RU)
        return

    if call.data == "water_full":
        if await kv_get_int("water_open", 1) == 0:
            await call.answer(t(lang, "Цистерна закрыта.", "Cistern is closed."), show_alert=True)
            return

        target = await kv_get_int("water_target_eur", 235)
        raised = await kv_get_int("water_raised_eur", 0)
        remaining = max(0, target - raised)
        if remaining <= 0:
            await call.answer(t(lang, "Цель уже достигнута.", "Already reached."), show_alert=True)
            return

        PENDING[call.from_user.id] = {
            "type": "water_full",
            "remaining_eur": remaining,
            "batch": await kv_get_int("water_batch", 1),
            "tag": await water_tag(),
        }
        await call.answer()
        await call.message.answer(VIDEO_NOTE_TEMPLATE_RU)
        return


# =========================
# PAY CALLBACKS (Stars)
# =========================

@dp.callback_query(lambda c: c.data.startswith("pay_water_"))
async def pay_water(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"

    if await kv_get_int("pay_stars_enabled", 1) == 0:
        await call.answer(t(lang, "Оплата Stars временно выключена.", "Stars payments are disabled."), show_alert=True)
        return
    if await kv_get_int("water_open", 1) == 0:
        await call.answer(t(lang, "Сбор закрыт.", "Campaign is closed."), show_alert=True)
        return

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
    lang = (await get_user_lang(call.from_user.id)) or "ru"

    if await kv_get_int("pay_stars_enabled", 1) == 0:
        await call.answer(t(lang, "Оплата Stars временно выключена.", "Stars payments are disabled."), show_alert=True)
        return
    if await kv_get_int("iftar_open", 1) == 0:
        await call.answer(t(lang, "Сбор закрыт.", "Campaign is closed."), show_alert=True)
        return

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

    day = await kv_get_int("iftar_day", 1)
    title_ru = f"Программа ифтаров — День {day}"
    title_en = f"Iftars — Day {day}"

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


# =========================
# ROUTER: pending inputs (admin + user)
# =========================

@dp.message()
async def any_message_router(message: Message):
    if not message.from_user:
        return

    # 1) Admin pending input
    if message.from_user.id == ADMIN_ID and ADMIN_PENDING.get(ADMIN_ID):
        await handle_admin_pending(message)
        return

    # 2) User pending
    if PENDING.get(message.from_user.id):
        await handle_user_pending(message)
        return

    # else ignore random messages


async def handle_user_pending(message: Message):
    st = PENDING.get(message.from_user.id)
    if not st:
        return

    lang = (await get_user_lang(message.from_user.id)) or "ru"
    rate = await get_rate()
    raw = (message.text or "").strip()

    # FULL CLOSE: donor note
    if st["type"] in {"iftar_full", "water_full"}:
        note = raw
        if len(note) < 10:
            await message.answer("Пожалуйста, отправьте полноценную подпись (минимум 10 символов).")
            return

        user_id = message.from_user.id

        if st["type"] == "iftar_full":
            # re-check remaining in case it changed
            if await kv_get_int("iftar_open", 1) == 0:
                PENDING.pop(user_id, None)
                await message.answer("Этот день уже закрыт. Попробуйте снова в новом дне.")
                return

            day = await kv_get_int("iftar_day", 1)
            target = await kv_get_int("iftar_target_portions", 100)
            raised = await kv_get_int("iftar_raised_portions", 0)
            remaining = max(0, target - raised)
            if remaining <= 0:
                PENDING.pop(user_id, None)
                await message.answer("Цель уже достигнута.")
                return

            note_id = await sponsor_note_insert(
                user_id=user_id,
                campaign="iftar",
                day_or_batch=day,
                note=note,
                portions=remaining,
                amount_eur=None,
            )

            stars = remaining * 4 * rate
            payload = f"iftar_full:note:{note_id}"

            await bot.send_invoice(
                chat_id=user_id,
                title=f"🤍 Закрыть день ифтаров полностью (День {day})",
                description=f"{remaining} порций (≈ {stars}⭐). Подпись для видео сохранена.",
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=f"{remaining} portions", amount=stars)],
                provider_token="",
            )
            PENDING.pop(user_id, None)
            return

        if st["type"] == "water_full":
            if await kv_get_int("water_open", 1) == 0:
                PENDING.pop(user_id, None)
                await message.answer("Эта цистерна уже закрыта. Попробуйте снова в новой цистерне.")
                return

            batch = await kv_get_int("water_batch", 1)
            target = await kv_get_int("water_target_eur", 235)
            raised = await kv_get_int("water_raised_eur", 0)
            remaining = max(0, target - raised)
            if remaining <= 0:
                PENDING.pop(user_id, None)
                await message.answer("Цель уже достигнута.")
                return

            note_id = await sponsor_note_insert(
                user_id=user_id,
                campaign="water",
                day_or_batch=batch,
                note=note,
                portions=None,
                amount_eur=remaining,
            )

            stars = remaining * rate
            payload = f"water_full:note:{note_id}"

            await bot.send_invoice(
                chat_id=user_id,
                title=f"🤍 Закрыть цистерну воды полностью (Цистерна #{batch})",
                description=f"{remaining}€ (≈ {stars}⭐). Подпись для видео сохранена.",
                payload=payload,
                currency="XTR",
                prices=[LabeledPrice(label=f"{remaining} EUR", amount=stars)],
                provider_token="",
            )
            PENDING.pop(user_id, None)
            return

    # OTHER AMOUNT INPUT
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
        if await kv_get_int("pay_stars_enabled", 1) == 0 or await kv_get_int("water_open", 1) == 0:
            PENDING.pop(message.from_user.id, None)
            await message.answer(t(lang, "Сбор закрыт или Stars выключены.", "Campaign closed or Stars disabled."))
            return

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
        if await kv_get_int("pay_stars_enabled", 1) == 0 or await kv_get_int("iftar_open", 1) == 0:
            PENDING.pop(message.from_user.id, None)
            await message.answer(t(lang, "Сбор закрыт или Stars выключены.", "Campaign closed or Stars disabled."))
            return

        portions = n
        stars = portions * 4 * rate
        payload = f"iftar:portions:{portions}"

        day = await kv_get_int("iftar_day", 1)
        title_ru = f"Программа ифтаров — День {day}"
        title_en = f"Iftars — Day {day}"

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


# =========================
# PAYMENTS (Stars)
# =========================

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)


@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""

    stars_total = sp.total_amount  # for XTR this is stars count
    rate = await get_rate()
    when = utc_now_str()

    user_id = message.from_user.id
    username = message.from_user.username or "-"

    # default donor thanks
    donor_thanks = "✅ Джазак Аллаху хейр! 🤍"

    try:
        p1, p2, p3 = payload.split(":")
    except Exception:
        await notify_admin_payment(
            kind="STARS PAYMENT (BAD PAYLOAD)",
            method="stars",
            campaign="unknown",
            tag="UNKNOWN",
            amount_eur=None,
            portions=None,
            stars=stars_total,
            user_id=user_id,
            username=username,
            when=when,
            extra=f"Payload: {payload}",
        )
        await message.answer(donor_thanks)
        return

    # Regular water: water:eur:10
    if p1 == "water" and p2 == "eur":
        val_i = int(p3)
        batch = await kv_get_int("water_batch", 1)

        await kv_inc_int("water_raised_eur", val_i)

        await notify_admin_payment(
            kind="STARS PAYMENT",
            method="stars",
            campaign="water",
            tag=f"WATER-{batch}",
            amount_eur=val_i,
            portions=None,
            stars=stars_total,
            user_id=user_id,
            username=username,
            when=when,
            extra=f"(rate 1€={rate}⭐)",
        )

        await maybe_auto_advance_water()
        await message.answer(donor_thanks)
        return

    # Regular iftar: iftar:portions:5
    if p1 == "iftar" and p2 == "portions":
        val_i = int(p3)
        day = await kv_get_int("iftar_day", 1)

        await kv_inc_int("iftar_raised_portions", val_i)

        await notify_admin_payment(
            kind="STARS PAYMENT",
            method="stars",
            campaign="iftar",
            tag=f"IFTAR-{day}",
            amount_eur=val_i * 4,
            portions=val_i,
            stars=stars_total,
            user_id=user_id,
            username=username,
            when=when,
            extra=f"(rate 1€={rate}⭐)",
        )

        await maybe_auto_advance_iftar()
        await message.answer(donor_thanks)
        return

    # Full-close by note: iftar_full:note:123
    if p1 in {"iftar_full", "water_full"} and p2 == "note":
        note_id = int(p3)
        note_row = await sponsor_note_get(note_id)
        if not note_row:
            await notify_admin_payment(
                kind="STARS PAYMENT (NOTE NOT FOUND)",
                method="stars",
                campaign=p1,
                tag="UNKNOWN",
                amount_eur=None,
                portions=None,
                stars=stars_total,
                user_id=user_id,
                username=username,
                when=when,
                extra=f"Payload: {payload}",
            )
            await message.answer(donor_thanks)
            return

        if note_row["campaign"] == "iftar":
            day = note_row["day_or_batch"]
            portions = int(note_row["portions"] or 0)
            # apply to current day only if still same day; otherwise still log and also add to current day (best-effort)
            current_day = await kv_get_int("iftar_day", 1)
            applied_day = current_day

            # If day changed, we still add to current day (so payment doesn't disappear), but admin will see note row day.
            await kv_inc_int("iftar_raised_portions", portions)

            await notify_admin_payment(
                kind="STARS PAYMENT (FULL DAY)",
                method="stars",
                campaign="iftar",
                tag=f"IFTAR-{current_day}",
                amount_eur=portions * 4,
                portions=portions,
                stars=stars_total,
                user_id=user_id,
                username=username,
                when=when,
                note=note_row["note"],
                extra=f"Note day: {day} | Applied to day: {applied_day} | (rate 1€={rate}⭐)",
            )

            await maybe_auto_advance_iftar()
            await message.answer(donor_thanks)
            return

        if note_row["campaign"] == "water":
            batch = note_row["day_or_batch"]
            amount_eur = int(note_row["amount_eur"] or 0)
            current_batch = await kv_get_int("water_batch", 1)
            applied_batch = current_batch

            await kv_inc_int("water_raised_eur", amount_eur)

            await notify_admin_payment(
                kind="STARS PAYMENT (FULL CISTERN)",
                method="stars",
                campaign="water",
                tag=f"WATER-{current_batch}",
                amount_eur=amount_eur,
                portions=None,
                stars=stars_total,
                user_id=user_id,
                username=username,
                when=when,
                note=note_row["note"],
                extra=f"Note batch: {batch} | Applied to batch: {applied_batch} | (rate 1€={rate}⭐)",
            )

            await maybe_auto_advance_water()
            await message.answer(donor_thanks)
            return

    # Unknown
    await notify_admin_payment(
        kind="STARS PAYMENT (UNKNOWN TYPE)",
        method="stars",
        campaign=p1,
        tag="UNKNOWN",
        amount_eur=None,
        portions=None,
        stars=stars_total,
        user_id=user_id,
        username=username,
        when=when,
        extra=f"Payload: {payload}",
    )
    await message.answer(donor_thanks)


# =========================
# ADMIN: COMMANDS + PANEL
# =========================

@dp.message(Command("help"))
async def cmd_help(message: Message):
    if admin_only(message):
        await message.answer(ADMIN_CHEATSHEET_RU, parse_mode="Markdown")
        return
    await message.answer(USER_HELP_RU, parse_mode="Markdown")


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not admin_only(message):
        return
    await message.answer("🛠️ Admin panel:", reply_markup=kb_admin_panel())


async def admin_status_text() -> str:
    rate = await get_rate()

    stars_on = await kv_get_int("pay_stars_enabled", 1)
    manual_on = await kv_get_int("pay_manual_enabled", 1)

    water_open = await kv_get_int("water_open", 1)
    water_batch_i = await kv_get_int("water_batch", 1)
    wt = await kv_get_int("water_target_eur", 0)
    wr = await kv_get_int("water_raised_eur", 0)

    iftar_open = await kv_get_int("iftar_open", 1)
    iftar_day_i = await kv_get_int("iftar_day", 1)
    it = await kv_get_int("iftar_target_portions", 0)
    ir = await kv_get_int("iftar_raised_portions", 0)

    day_date = (await kv_get("iftar_day_date") or "").strip()
    plus50 = await iftar_plus50_allowed_now()

    return (
        "📊 STATUS\n"
        f"TZ: {TIMEZONE}\n"
        f"Rate: 1€={rate}⭐\n"
        f"Stars: {'ON' if stars_on else 'OFF'} | Manual: {'ON' if manual_on else 'OFF'}\n\n"
        f"💧 Water batch #{water_batch_i}: {'OPEN' if water_open else 'CLOSED'} | {wr}/{wt} EUR\n"
        f"🍲 Iftar day {iftar_day_i}: {'OPEN' if iftar_open else 'CLOSED'} | {ir}/{it} portions\n"
        f"Iftar day date: {day_date or '—'} | +50 allowed now: {'YES' if plus50 else 'NO'}"
    )


@dp.callback_query(lambda c: c.data.startswith("adm_"))
async def admin_panel_callbacks(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("No access", show_alert=True)
        return

    if call.data == "adm_help":
        await call.answer()
        await call.message.answer(ADMIN_CHEATSHEET_RU, parse_mode="Markdown")
        return

    if call.data == "adm_status":
        await call.answer()
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_toggle_water":
        cur = await kv_get_int("water_open", 1)
        await kv_set_int("water_open", 0 if cur else 1)
        await call.answer("OK")
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_toggle_iftar":
        cur = await kv_get_int("iftar_open", 1)
        await kv_set_int("iftar_open", 0 if cur else 1)
        await call.answer("OK")
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_toggle_stars":
        cur = await kv_get_int("pay_stars_enabled", 1)
        await kv_set_int("pay_stars_enabled", 0 if cur else 1)
        await call.answer("OK")
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_toggle_manual":
        cur = await kv_get_int("pay_manual_enabled", 1)
        await kv_set_int("pay_manual_enabled", 0 if cur else 1)
        await call.answer("OK")
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_iftar_plus50":
        await call.answer()
        if not await iftar_plus50_allowed_now():
            await call.message.answer("⛔ +50 нельзя: окно закрыто (после 00:00 по TIMEZONE).")
            await call.message.answer(await admin_status_text())
            return

        target = await kv_get_int("iftar_target_portions", 100)
        if target >= 150:
            await call.message.answer("ℹ️ Цель уже 150+ порций. +50 не нужен.")
            await call.message.answer(await admin_status_text())
            return

        await kv_inc_int("iftar_target_portions", 50)
        await kv_set_int("iftar_open", 1)

        new_target = await kv_get_int("iftar_target_portions", 150)
        await call.message.answer(f"✅ IFTAR: цель увеличена до {new_target} и сбор открыт.")
        await call.message.answer(await admin_status_text())
        return


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if not admin_only(message):
        return
    await message.answer(await admin_status_text())


@dp.message(Command("open_water"))
async def cmd_open_water(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("water_open", 1)
    await message.answer("OK: water OPEN")


@dp.message(Command("close_water"))
async def cmd_close_water(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("water_open", 0)
    await message.answer("OK: water CLOSED")


@dp.message(Command("open_iftar"))
async def cmd_open_iftar(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("iftar_open", 1)
    await message.answer("OK: iftar OPEN")


@dp.message(Command("close_iftar"))
async def cmd_close_iftar(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("iftar_open", 0)
    await message.answer("OK: iftar CLOSED")


@dp.message(Command("stars_on"))
async def cmd_stars_on(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("pay_stars_enabled", 1)
    await message.answer("OK: Stars ON")


@dp.message(Command("stars_off"))
async def cmd_stars_off(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("pay_stars_enabled", 0)
    await message.answer("OK: Stars OFF")


@dp.message(Command("manual_on"))
async def cmd_manual_on(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("pay_manual_enabled", 1)
    await message.answer("OK: Manual payments ON")


@dp.message(Command("manual_off"))
async def cmd_manual_off(message: Message):
    if not admin_only(message):
        return
    await kv_set_int("pay_manual_enabled", 0)
    await message.answer("OK: Manual payments OFF")


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
    await kv_set_int("eur_to_stars", rate)
    await message.answer(f"OK. Новый курс: 1€ = {rate}⭐")


@dp.message(Command("set_iftar_target"))
async def cmd_set_iftar_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_target 100")
        return
    await kv_set_int("iftar_target_portions", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("set_iftar_day"))
async def cmd_set_iftar_day(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_day 10")
        return

    await kv_set_int("iftar_day", int(parts[1]))
    await kv_set_int("iftar_raised_portions", 0)
    await kv_set_int("iftar_target_portions", 100)  # daily minimum
    await kv_set_int("iftar_open", 1)
    await kv_set("iftar_day_date", today_local_str())

    await message.answer("OK (day set, portions reset, target=100, opened, day_date=today)")


@dp.message(Command("set_water_target"))
async def cmd_set_water_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_water_target 235")
        return
    await kv_set_int("water_target_eur", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("add_water"))
async def cmd_add_water(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_water 10")
        return
    delta = int(parts[1])
    await kv_inc_int("water_raised_eur", delta)
    await maybe_auto_advance_water()
    await message.answer("OK\n" + await admin_status_text())


@dp.message(Command("add_iftar"))
async def cmd_add_iftar(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_iftar 5")
        return
    delta = int(parts[1])
    await kv_inc_int("iftar_raised_portions", delta)
    await maybe_auto_advance_iftar()
    await message.answer("OK\n" + await admin_status_text())


@dp.message(Command("set_water_raised"))
async def cmd_set_water_raised(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_water_raised 120")
        return
    v = int(parts[1])
    await kv_set_int("water_raised_eur", v)
    await maybe_auto_advance_water()
    await message.answer("OK\n" + await admin_status_text())


@dp.message(Command("set_iftar_raised"))
async def cmd_set_iftar_raised(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_raised 80")
        return
    v = int(parts[1])
    await kv_set_int("iftar_raised_portions", v)
    await maybe_auto_advance_iftar()
    await message.answer("OK\n" + await admin_status_text())


# =========================
# HEALTH SERVER (Render)
# =========================

async def health_server():
    app = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


# =========================
# MAIN
# =========================

async def main():
    await db_init()
    await health_server()

    try:
        await bot.send_message(ADMIN_ID, "✅ Bot started", disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to notify admin on startup")

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
