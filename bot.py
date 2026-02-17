import os
import re
import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
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
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
DEFAULT_EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")
PORT = int(os.getenv("PORT", "10000") or "10000")
TIMEZONE = os.getenv("TIMEZONE", "UTC").strip() or "UTC"

# Where to post public stuff
PUBLIC_GROUP_ID = int(os.getenv("PUBLIC_GROUP_ID", "0") or "0")  # daily reports for all campaigns (except ZF list)
ZF_GROUP_ID = int(os.getenv("ZF_GROUP_ID", "0") or "0")          # ZF list updates

# Payment details (already in Render env)
PAYPAL_LINK = os.getenv("PAYPAL_LINK", "").strip()
SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "").strip()
SEPA_IBAN = os.getenv("SEPA_IBAN", "").strip()
SEPA_BIC = os.getenv("SEPA_BIC", "").strip()
ZEN_NAME = os.getenv("ZEN_NAME", "").strip()
ZEN_PHONE = os.getenv("ZEN_PHONE", "").strip()
ZEN_CARD = os.getenv("ZEN_CARD", "").strip()
USDT_TRC20 = os.getenv("USDT_TRC20", "").strip()
USDC_ERC20 = os.getenv("USDC_ERC20", "").strip()

# Optional SWIFT (you said you can add later)
SWIFT_RECIPIENT = os.getenv("SWIFT_RECIPIENT", "").strip()
SWIFT_ACCOUNT = os.getenv("SWIFT_ACCOUNT", "").strip()
SWIFT_BIC = os.getenv("SWIFT_BIC", "").strip()
SWIFT_BANK = os.getenv("SWIFT_BANK", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing (set it in env)")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DB_PATH = "data.db"

# Per-user pending state
PENDING: dict[int, dict] = {}

# Remember last campaign user looked at (for default ref marks)
LAST_CAMPAIGN: dict[int, str] = {}  # "water"|"iftar"|"zf"|"id"


# =========================
# CONSTANTS / MARKS
# =========================

MARK_IFTAR = "MIMAX"
MARK_WATER = "GREENMAX"
MARK_ZF = "ZF"
MARK_ID = "ID"

# ZF fixed params
ZF_EUR_PER_PERSON = 9
ZF_KG_RICE_PER_PERSON = 3

# Daily report time (local): choose 21:00 to be safely after daytime ops; adjust if you want.
DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "21") or "21")


# =========================
# HELPERS
# =========================

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


def today_local() -> date:
    return now_local().date()


def utc_now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en


def admin_only(message: Message) -> bool:
    return bool(message.from_user) and message.from_user.id == ADMIN_ID


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


async def send_admin_html(text_html: str):
    await bot.send_message(
        ADMIN_ID,
        text_html,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def send_group_markdown(chat_id: int, text_md: str):
    if not chat_id:
        return
    try:
        await bot.send_message(chat_id, text_md, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to send to group %s", chat_id)


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

        # ZF list entries
        await db.execute("""
        CREATE TABLE IF NOT EXISTS zf_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            label TEXT NOT NULL,
            people INTEGER NOT NULL,
            method TEXT NOT NULL,       -- "sepa"|"paypal"|"zen"|"crypto"|"swift"|"stars"
            status TEXT NOT NULL        -- "marked" or "confirmed"
        )
        """)

        # defaults
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eur_to_stars', ?)", (str(DEFAULT_EUR_TO_STARS),))

        # Payment toggles
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('pay_stars_enabled','0')")   # start without stars by default
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('pay_manual_enabled','1')")

        # Water (collective, per cistern)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_open','1')")

        # Iftar (collective, per day)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_open','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day_date', ?)", (today_local().isoformat(),))

        # Transparency markers (public)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_last_closed_day','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_last_closed_at','')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_last_paid_batch','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_last_paid_at','')")

        # ZF & ID schedule
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_end','2026-03-20')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_end','2026-03-20')")

        # ID is unlimited, but we keep internal totals for admins
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_total_marked_people','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_raised_eur','0')")

        # Daily report state
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('last_daily_report_date','')")

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
    await kv_set_int(key, val + int(delta))


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


# =========================
# ZF DB ops + formatting
# =========================

def parse_zf_code(s: str) -> Optional[int]:
    """
    Accepts: "ZF-5", "ZF - 5", "zf-10" etc.
    Returns people count or None.
    """
    s = (s or "").strip()
    m = re.search(r"\bZF\s*[-–]\s*(\d{1,3})\b", s, flags=re.IGNORECASE)
    if not m:
        # also allow "ZF 5"
        m = re.search(r"\bZF\s+(\d{1,3})\b", s, flags=re.IGNORECASE)
    if not m:
        return None
    n = int(m.group(1))
    if n <= 0 or n > 999:
        return None
    return n


async def zf_add_entry(user_id: int, username: str, label: str, people: int, method: str, status: str = "marked") -> int:
    label = (label or "").strip()
    if len(label) > 60:
        label = label[:60].rstrip()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO zf_entries(created_utc, user_id, username, label, people, method, status)
            VALUES(?,?,?,?,?,?,?)
            """,
            (utc_now_str(), user_id, username or "-", label, int(people), method, status),
        )
        await db.commit()
        return int(cur.lastrowid)


async def zf_totals() -> Tuple[int, int]:
    """
    returns (total_people, total_rice_kg)
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(SUM(people),0) FROM zf_entries WHERE status IN ('marked','confirmed')") as cur:
            row = await cur.fetchone()
            total_people = int(row[0] or 0)
    return total_people, total_people * ZF_KG_RICE_PER_PERSON


async def zf_list_text() -> str:
    """
    Group public list: label + people only; then total rice kg.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT label, people
            FROM zf_entries
            WHERE status IN ('marked','confirmed')
            ORDER BY id ASC
            """
        ) as cur:
            rows = await cur.fetchall()

    lines = ["*Закят-уль-Фитр*"]
    for i, (label, people) in enumerate(rows, start=1):
        lines.append(f"{i}. {label} — *{int(people)} чел.*")

    total_people, total_kg = await zf_totals()
    lines.append("")
    lines.append(f"*Всего: {total_kg} кг риса*")
    return "\n".join(lines)


async def zf_post_update():
    """
    Posts list to ZF group each time an entry is added (your requirement).
    If ZF_GROUP_ID not set, does nothing.
    """
    if not ZF_GROUP_ID:
        return
    text = await zf_list_text()
    await send_group_markdown(ZF_GROUP_ID, text)


# =========================
# SCHEDULE: ZF / ID open-close + daily reports
# =========================

def parse_iso_date(s: str) -> Optional[date]:
    try:
        y, m, d = s.strip().split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


async def schedule_tick():
    """
    Opens/closes ZF & ID by local date (inclusive start/end).
    """
    today = today_local()

    zf_start = parse_iso_date(await kv_get("zf_start") or "")
    zf_end = parse_iso_date(await kv_get("zf_end") or "")
    id_start = parse_iso_date(await kv_get("id_start") or "")
    id_end = parse_iso_date(await kv_get("id_end") or "")

    if zf_start and zf_end:
        should_open = 1 if (zf_start <= today <= zf_end) else 0
        prev = await kv_get_int("zf_open", 0)
        if prev != should_open:
            await kv_set_int("zf_open", should_open)
            await send_admin_html(f"📅 ZF status changed: {'OPEN' if should_open else 'CLOSED'} (local {today.isoformat()})")

    if id_start and id_end:
        should_open = 1 if (id_start <= today <= id_end) else 0
        prev = await kv_get_int("id_open", 0)
        if prev != should_open:
            await kv_set_int("id_open", should_open)
            await send_admin_html(f"📅 ID status changed: {'OPEN' if should_open else 'CLOSED'} (local {today.isoformat()})")


async def daily_report_tick():
    """
    Sends daily reports once per day after DAILY_REPORT_HOUR (local time).
    Reports for ALL campaigns except ZF list (ZF list is posted on each entry).
    """
    now = now_local()
    if now.hour < DAILY_REPORT_HOUR:
        return

    today_str = today_local().isoformat()
    last = (await kv_get("last_daily_report_date") or "").strip()
    if last == today_str:
        return

    report = await build_daily_report()
    # send to group + admin
    if PUBLIC_GROUP_ID:
        await send_group_markdown(PUBLIC_GROUP_ID, report)
    await bot.send_message(ADMIN_ID, report, parse_mode="Markdown", disable_web_page_preview=True)

    await kv_set("last_daily_report_date", today_str)


async def scheduler_loop():
    while True:
        try:
            await schedule_tick()
            await daily_report_tick()
        except Exception:
            logging.exception("scheduler_loop tick failed")
        await asyncio.sleep(60)


# =========================
# AUTO-ADVANCE water/iftar when reaching target
# =========================

async def advance_iftar_day():
    old_day = await kv_get_int("iftar_day", 1)
    await kv_set_int("iftar_last_closed_day", old_day)
    await kv_set("iftar_last_closed_at", utc_now_str())

    await kv_set_int("iftar_day", old_day + 1)
    await kv_set_int("iftar_raised_portions", 0)
    await kv_set_int("iftar_target_portions", 100)
    await kv_set_int("iftar_open", 1)
    await kv_set("iftar_day_date", today_local().isoformat())


async def advance_water_batch():
    old_batch = await kv_get_int("water_batch", 1)
    await kv_set_int("water_last_paid_batch", old_batch)
    await kv_set("water_last_paid_at", utc_now_str())

    await kv_set_int("water_batch", old_batch + 1)
    await kv_set_int("water_raised_eur", 0)
    await kv_set_int("water_open", 1)


async def maybe_auto_advance_iftar() -> bool:
    raised = await kv_get_int("iftar_raised_portions", 0)
    target = await kv_get_int("iftar_target_portions", 100)
    if target > 0 and raised >= target:
        old_day = await kv_get_int("iftar_day", 1)
        await kv_set_int("iftar_open", 0)
        await advance_iftar_day()
        new_day = await kv_get_int("iftar_day", 1)
        await send_admin_html(f"✅ IFTAR DAY CLOSED: day {old_day} reached {raised}/{target} -> opened day {new_day}")
        return True
    return False


async def maybe_auto_advance_water() -> bool:
    raised = await kv_get_int("water_raised_eur", 0)
    target = await kv_get_int("water_target_eur", 235)
    if target > 0 and raised >= target:
        old_batch = await kv_get_int("water_batch", 1)
        await kv_set_int("water_open", 0)
        await advance_water_batch()
        new_batch = await kv_get_int("water_batch", 1)
        await send_admin_html(f"✅ WATER PAID: cistern #{old_batch} reached {raised}/{target} -> opened #{new_batch}")
        return True
    return False


async def iftar_plus50_allowed_now() -> bool:
    day_date = (await kv_get("iftar_day_date") or "").strip()
    return bool(day_date) and today_local().isoformat() == day_date


# =========================
# REPORTS
# =========================

async def public_transparency_block() -> str:
    last_iftar = await kv_get_int("iftar_last_closed_day", 0)
    last_iftar_at = (await kv_get("iftar_last_closed_at") or "").strip()

    last_water = await kv_get_int("water_last_paid_batch", 0)
    last_water_at = (await kv_get("water_last_paid_at") or "").strip()

    lines = ["📌 *Прозрачность*"]
    if last_iftar > 0:
        lines.append(f"✅ Ифтары закрыт день: *{last_iftar}* ({last_iftar_at or '—'})")
    else:
        lines.append("— Ифтары: пока нет закрытых дней")

    if last_water > 0:
        lines.append(f"✅ Вода оплачена цистерна: *#{last_water}* ({last_water_at or '—'})")
    else:
        lines.append("— Вода: пока нет оплаченных цистерн")

    return "\n".join(lines)


async def build_daily_report() -> str:
    # Water
    water_batch = await kv_get_int("water_batch", 1)
    water_target = await kv_get_int("water_target_eur", 235)
    water_raised = await kv_get_int("water_raised_eur", 0)
    water_bar = battery(water_raised, water_target)
    water_rem = max(0, water_target - water_raised)

    # Iftar
    iftar_day = await kv_get_int("iftar_day", 1)
    iftar_target = await kv_get_int("iftar_target_portions", 100)
    iftar_raised = await kv_get_int("iftar_raised_portions", 0)
    iftar_bar = battery(iftar_raised, iftar_target)
    iftar_rem = max(0, iftar_target - iftar_raised)

    # ZF totals (kg only is important; but this is daily report (not list))
    zf_open = await kv_get_int("zf_open", 0)
    zf_people, zf_kg = await zf_totals()

    # ID totals (internal eur for admin; public: show eur collected for candies)
    id_open = await kv_get_int("id_open", 0)
    id_raised = await kv_get_int("id_raised_eur", 0)

    transparency = await public_transparency_block()
    now_str = now_local().strftime("%Y-%m-%d %H:%M")

    lines = [
        f"📣 *Ежедневный отчёт* ({now_str} {TIMEZONE})",
        "",
        transparency,
        "",
        f"💧 *Вода — цистерна #{water_batch}*",
        f"Собрано: *{water_raised}€* / *{water_target}€* | Осталось: *{water_rem}€*",
        water_bar,
        "",
        f"🍲 *Ифтары — день {iftar_day}*",
        f"Собрано: *{iftar_raised}* / *{iftar_target}* порций | Осталось: *{iftar_rem}*",
        iftar_bar,
        "",
        f"🟢 *ZF (Закят-уль-Фитр)* {'(ОТКРЫТ)' if zf_open else '(ЗАКРЫТ)'}",
        f"Всего отмечено: *{zf_kg} кг риса*",
        "",
        f"🎉 *ID (Ид)* {'(ОТКРЫТ)' if id_open else '(ЗАКРЫТ)'}",
        f"Собрано (учёт): *{id_raised}€*",
    ]
    return "\n".join(lines)


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
    kb.button(text=t(lang, "💳 Оплата", "💳 Payment"), callback_data="support")
    kb.button(text=t(lang, "❓ Помощь", "❓ Help"), callback_data="help_user")
    kb.button(text=t(lang, "🌐 Язык", "🌐 Language"), callback_data="lang_menu")
    kb.adjust(1)
    return kb.as_markup()


def kb_list(lang: str, zf_open: bool, id_open: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "🍲 Программа ифтаров", "🍲 Iftars Program"), callback_data="iftar")
    kb.button(text=t(lang, "💧 Сукья-ль-ма (вода)", "💧 Water (Sukya-l-ma)"), callback_data="water")
    kb.button(text=("🟢 Закят-уль-Фитр (ZF)" if zf_open else "🔒 Закят-уль-Фитр (ZF)"), callback_data="zf")
    kb.button(text=("🟢 Ид (ID)" if id_open else "🔒 Ид (ID)"), callback_data="id")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_support(lang: str, stars_enabled: bool, manual_enabled: bool):
    kb = InlineKeyboardBuilder()
    if manual_enabled:
        kb.button(text="🏦 SEPA", callback_data="support_sepa")
        kb.button(text="💙 PayPal", callback_data="support_paypal")
        kb.button(text="🟣 ZEN", callback_data="support_zen")
        kb.button(text="💎 Crypto", callback_data="support_crypto")
        if SWIFT_ACCOUNT or SWIFT_BIC:
            kb.button(text="🌍 SWIFT", callback_data="support_swift")
    if stars_enabled:
        kb.button(text="⭐ Telegram Stars", callback_data="support_stars")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_support_back(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="support")
    kb.adjust(1)
    return kb.as_markup()


def kb_zf_choose_method():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏦 SEPA", callback_data="zf_method_sepa")
    kb.button(text="💙 PayPal", callback_data="zf_method_paypal")
    kb.button(text="🟣 ZEN", callback_data="zf_method_zen")
    kb.button(text="💎 Crypto", callback_data="zf_method_crypto")
    if SWIFT_ACCOUNT or SWIFT_BIC:
        kb.button(text="🌍 SWIFT", callback_data="zf_method_swift")
    kb.button(text="⬅️ Назад", callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_id_choose_method():
    kb = InlineKeyboardBuilder()
    kb.button(text="🏦 SEPA", callback_data="id_method_sepa")
    kb.button(text="💙 PayPal", callback_data="id_method_paypal")
    kb.button(text="🟣 ZEN", callback_data="id_method_zen")
    kb.button(text="💎 Crypto", callback_data="id_method_crypto")
    if SWIFT_ACCOUNT or SWIFT_BIC:
        kb.button(text="🌍 SWIFT", callback_data="id_method_swift")
    kb.button(text="⬅️ Назад", callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_zf_people():
    kb = InlineKeyboardBuilder()
    # 1..10 + other
    for n in range(1, 11):
        kb.button(text=str(n), callback_data=f"zf_people_{n}")
    kb.button(text="Другое", callback_data="zf_people_other")
    kb.button(text="⬅️ Назад", callback_data="zf_back_method")
    kb.adjust(5, 5, 1, 1)
    return kb.as_markup()


def kb_id_amounts():
    kb = InlineKeyboardBuilder()
    for eur in (5, 10, 20, 50):
        kb.button(text=f"{eur}€", callback_data=f"id_amt_{eur}")
    kb.button(text="Другое", callback_data="id_amt_other")
    kb.button(text="⬅️ Назад", callback_data="id_back_method")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def kb_confirm_zf_entry():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Я оплатил(а) и добавить в список", callback_data="zf_confirm_paid")
    kb.button(text="✏️ Изменить людей", callback_data="zf_edit_people")
    kb.button(text="✏️ Изменить название", callback_data="zf_edit_label")
    kb.button(text="⬅️ Назад", callback_data="zf_back_method")
    kb.adjust(1)
    return kb.as_markup()


def kb_confirm_id_marked():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Я оплатил(а)", callback_data="id_confirm_paid")
    kb.button(text="⬅️ Назад", callback_data="id_back_method")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_panel():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Status", callback_data="adm_status")
    kb.button(text="⭐ Stars ON/OFF", callback_data="adm_toggle_stars")
    kb.button(text="💳 Manual ON/OFF", callback_data="adm_toggle_manual")
    kb.button(text="➕ Iftar +50 (до 00:00)", callback_data="adm_iftar_plus50")
    kb.button(text="🔁 Daily report NOW", callback_data="adm_report_now")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# TEXTS
# =========================

async def water_text_ru() -> str:
    batch = await kv_get_int("water_batch", 1)
    target = await kv_get_int("water_target_eur", 235)
    raised = await kv_get_int("water_raised_eur", 0)
    is_open = await kv_get_int("water_open", 1)
    bar = battery(raised, target)
    remaining = max(0, target - raised)
    status = "ОТКРЫТ" if is_open else "ЗАКРЫТ"

    return (
        f"💧 *Сукья-ль-ма — цистерна #{batch}*\n"
        f"Статус: *{status}*\n\n"
        f"Нужно: *{target}€*\n"
        f"Собрано: *{raised}€* / *{target}€*\n"
        f"Осталось: *{remaining}€*\n"
        f"{bar}\n\n"
        f"Отметка для банка: *{MARK_WATER}*\n"
        "Оплата: выберите способ в меню «Оплата»."
    )


async def iftar_text_ru() -> str:
    day = await kv_get_int("iftar_day", 1)
    target = await kv_get_int("iftar_target_portions", 100)
    raised = await kv_get_int("iftar_raised_portions", 0)
    is_open = await kv_get_int("iftar_open", 1)
    bar = battery(raised, target)
    remaining = max(0, target - raised)
    status = "ОТКРЫТ" if is_open else "ЗАКРЫТ"

    return (
        f"🍲 *Программа ифтаров — День {day}*\n"
        f"Статус: *{status}*\n\n"
        f"Цель: *{target} порций*\n"
        f"Собрано: *{raised}* / *{target}*\n"
        f"Осталось: *{remaining} порций*\n"
        f"{bar}\n\n"
        f"Отметка для банка: *{MARK_IFTAR}*\n"
        "Оплата: выберите способ в меню «Оплата»."
    )


async def zf_intro_ru() -> str:
    zf_open = await kv_get_int("zf_open", 0)
    start = (await kv_get("zf_start") or "").strip()
    end = (await kv_get("zf_end") or "").strip()
    total_people, total_kg = await zf_totals()
    status = "ОТКРЫТ" if zf_open else "ЗАКРЫТ"

    return (
        "🟢 *Закят-уль-Фитр (ZF)*\n\n"
        f"Статус: *{status}*  (период: {start} → {end})\n\n"
        f"Норма: *{ZF_KG_RICE_PER_PERSON} кг риса* на человека (1 са`а)\n"
        f"Цена: *{ZF_EUR_PER_PERSON}€* на человека\n\n"
        "Порядок:\n"
        "1) Сначала выберите способ оплаты\n"
        "2) Оплатите, указав в назначении *ZF-ЧИСЛО* (например `ZF-5`)\n"
        "3) Затем внесите себя в список (как вы хотите отображаться)\n\n"
        f"Сейчас отмечено: *{total_kg} кг риса*\n"
    )


async def id_intro_ru() -> str:
    id_open = await kv_get_int("id_open", 0)
    start = (await kv_get("id_start") or "").strip()
    end = (await kv_get("id_end") or "").strip()
    raised = await kv_get_int("id_raised_eur", 0)
    status = "ОТКРЫТ" if id_open else "ЗАКРЫТ"

    return (
        "🎉 *Ид (ID) — сладости/выпечка детям*\n\n"
        f"Статус: *{status}*  (период: {start} → {end})\n\n"
        "Сбор без лимита: сколько соберём — столько и раздадим в день Ид.\n\n"
        f"Отметка для банка: *{MARK_ID}*\n"
        f"Собрано (учёт): *{raised}€*\n\n"
        "Порядок: выберите способ оплаты → оплатите → нажмите «Я оплатил(а)» (для уведомления)."
    )


def manual_copy_block(method: str, ref_text: str) -> str:
    """
    Copy-friendly line-by-line. ref_text already includes desired code, e.g. "ZF-5" or "MIMAX".
    """
    warn_greenmax = ""
    if ref_text.strip().upper().startswith(MARK_WATER):
        warn_greenmax = "\n⚠️ *Убедительная просьба:* укажите *только* `GREENMAX` и ничего больше.\n"

    if method == "paypal":
        return (
            "💙 *PayPal*\n\n"
            "Link:\n"
            f"`{PAYPAL_LINK}`\n\n"
            "Message/Reference:\n"
            f"`{ref_text}`\n"
            f"{warn_greenmax}"
        )

    if method == "sepa":
        bic_line = f"\nBIC:\n`{SEPA_BIC}`\n" if SEPA_BIC else ""
        return (
            "🏦 *SEPA (Europe)*\n\n"
            "Recipient:\n"
            f"`{SEPA_RECIPIENT}`\n\n"
            "IBAN:\n"
            f"`{SEPA_IBAN}`\n"
            f"{bic_line}\n"
            "Message/Reference:\n"
            f"`{ref_text}`\n"
            f"{warn_greenmax}"
        )

    if method == "zen":
        parts = ["🟣 *ZEN*\n"]
        if ZEN_NAME:
            parts.append("Recipient:\n" + f"`{ZEN_NAME}`\n")
        if ZEN_PHONE:
            parts.append("Phone:\n" + f"`{ZEN_PHONE}`\n")
        if ZEN_CARD:
            parts.append("Card:\n" + f"`{mask(ZEN_CARD, 6, 4)}`\n")
        parts.append("\nMessage/Reference:\n" + f"`{ref_text}`\n")
        if warn_greenmax:
            parts.append(warn_greenmax)
        return "\n".join(parts)

    if method == "crypto":
        usdt_line = f"USDT (TRC20):\n`{USDT_TRC20}`\n" if USDT_TRC20 else "USDT (TRC20):\n`—`\n"
        usdc_line = f"USDC (ERC20):\n`{USDC_ERC20}`\n" if USDC_ERC20 else "USDC (ERC20):\n`—`\n"
        return (
            "💎 *Crypto*\n\n"
            f"{usdt_line}\n{usdc_line}\n"
            "Message/Reference:\n"
            f"`{ref_text}`\n"
            f"{warn_greenmax}"
        )

    if method == "swift":
        parts = ["🌍 *SWIFT*\n"]
        if SWIFT_RECIPIENT:
            parts.append("Recipient:\n" + f"`{SWIFT_RECIPIENT}`\n")
        if SWIFT_BANK:
            parts.append("Bank:\n" + f"`{SWIFT_BANK}`\n")
        if SWIFT_ACCOUNT:
            parts.append("Account:\n" + f"`{SWIFT_ACCOUNT}`\n")
        if SWIFT_BIC:
            parts.append("BIC/SWIFT:\n" + f"`{SWIFT_BIC}`\n")
        parts.append("\nMessage/Reference:\n" + f"`{ref_text}`\n")
        if warn_greenmax:
            parts.append(warn_greenmax)
        return "\n".join(parts)

    return "—"


# =========================
# REFERENCE MARK BY CAMPAIGN
# =========================

def ref_mark_for_campaign(campaign: str) -> str:
    if campaign == "iftar":
        return MARK_IFTAR
    if campaign == "water":
        return MARK_WATER
    if campaign == "zf":
        return MARK_ZF
    if campaign == "id":
        return MARK_ID
    return "SUPPORT"


# =========================
# START / LANGUAGE
# =========================

@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    saved = await get_user_lang(user_id)
    if not saved:
        await message.answer("Ассаляму алейкум 🤍\n\nВыберите язык / Choose language:", reply_markup=kb_lang_select())
        return
    await message.answer("Ассаляму алейкум 🤍\n\nНажмите «Сборы», чтобы выбрать сбор.", reply_markup=kb_main(saved))


@dp.message(Command("lang"))
async def lang_cmd(message: Message):
    await message.answer("Выберите язык / Choose language:", reply_markup=kb_lang_select())


@dp.callback_query(lambda c: c.data in {"lang_ru", "lang_en"})
async def choose_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    await safe_edit(call, "Язык установлен." if lang == "ru" else "Language set.", reply_markup=kb_main(lang))


# =========================
# MENUS
# =========================

@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "support", "help_user", "water", "iftar", "zf", "id"})
async def menu(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"

    if call.data == "lang_menu":
        await call.answer()
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    if call.data == "help_user":
        await call.answer()
        txt = (
            "ℹ️ *Подсказка*\n\n"
            "— Выберите сбор в «Сборы».\n"
            "— Оплата: PayPal/SEPA/ZEN/Crypto (и Stars, если включены админом).\n"
            "— Для банка/кошелька копируйте строки и вставляйте.\n"
            f"— Отметки: ифтары `{MARK_IFTAR}`, вода `{MARK_WATER}`, ZF `ZF-5`, ID `{MARK_ID}`.\n"
        )
        await safe_edit(call, txt, reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "back":
        await call.answer()
        await safe_edit(call, "Меню:", reply_markup=kb_main(lang))
        return

    if call.data == "list":
        await call.answer()
        zf_open = bool(await kv_get_int("zf_open", 0))
        id_open = bool(await kv_get_int("id_open", 0))
        await safe_edit(call, "Выберите сбор:", reply_markup=kb_list(lang, zf_open, id_open))
        return

    if call.data == "support":
        await call.answer()
        stars_enabled = bool(await kv_get_int("pay_stars_enabled", 0))
        manual_enabled = bool(await kv_get_int("pay_manual_enabled", 1))
        await safe_edit(call, "💳 Выберите способ оплаты:", reply_markup=kb_support(lang, stars_enabled, manual_enabled))
        return

    if call.data == "water":
        await call.answer()
        LAST_CAMPAIGN[call.from_user.id] = "water"
        await safe_edit(call, await water_text_ru(), reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "iftar":
        await call.answer()
        LAST_CAMPAIGN[call.from_user.id] = "iftar"
        await safe_edit(call, await iftar_text_ru(), reply_markup=kb_main(lang), parse_mode="Markdown")
        return

    if call.data == "zf":
        await call.answer()
        if await kv_get_int("zf_open", 0) == 0:
            await safe_edit(call, "🔒 ZF сейчас закрыт (вне периода).", reply_markup=kb_main(lang))
            return
        LAST_CAMPAIGN[call.from_user.id] = "zf"
        await safe_edit(call, await zf_intro_ru(), reply_markup=kb_zf_choose_method(), parse_mode="Markdown")
        return

    if call.data == "id":
        await call.answer()
        if await kv_get_int("id_open", 0) == 0:
            await safe_edit(call, "🔒 ID сейчас закрыт (вне периода).", reply_markup=kb_main(lang))
            return
        LAST_CAMPAIGN[call.from_user.id] = "id"
        await safe_edit(call, await id_intro_ru(), reply_markup=kb_id_choose_method(), parse_mode="Markdown")
        return


# =========================
# SUPPORT (generic, from main "Оплата")
# =========================

@dp.callback_query(lambda c: c.data.startswith("support_"))
async def support_generic(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    stars_enabled = bool(await kv_get_int("pay_stars_enabled", 0))
    manual_enabled = bool(await kv_get_int("pay_manual_enabled", 1))

    if call.data == "support_stars":
        await call.answer()
        if not stars_enabled:
            await safe_edit(call, "⭐ Stars сейчас выключены.", reply_markup=kb_support_back(lang))
            return
        rate = await get_rate()
        await safe_edit(call, f"⭐ Stars включены. Курс: 1€={rate}⭐", reply_markup=kb_support_back(lang))
        return

    if not manual_enabled:
        await call.answer()
        await safe_edit(call, "💳 Другие способы оплаты сейчас выключены.", reply_markup=kb_support_back(lang))
        return

    # Use last campaign mark
    campaign = LAST_CAMPAIGN.get(call.from_user.id, "iftar")
    ref = ref_mark_for_campaign(campaign)
    # For ZF, we do NOT use generic support screen; but if user opens, show simple ZF reminder
    if campaign == "zf":
        ref = "ZF-5"

    method = call.data.replace("support_", "")
    await call.answer()
    await safe_edit(call, manual_copy_block(method, ref), reply_markup=kb_support_back(lang), parse_mode="Markdown")


# =========================
# ZF FLOW: choose method -> show details + ask ZF code -> ask label -> confirm -> add to list
# =========================

@dp.callback_query(lambda c: c.data.startswith("zf_method_"))
async def zf_choose_method(call: CallbackQuery):
    if await kv_get_int("zf_open", 0) == 0:
        await call.answer("ZF закрыт.", show_alert=True)
        return

    method = call.data.replace("zf_method_", "").strip()
    PENDING[call.from_user.id] = {"type": "zf_wait_code", "method": method}
    await call.answer()

    txt = (
        "✅ Вы выбрали способ оплаты.\n\n"
        "1) Оплатите.\n"
        "2) В назначении/сообщении укажите *ZF-ЧИСЛО* (например `ZF-5`).\n"
        "3) Потом пришлите сюда ровно эту строку (например `ZF-5`).\n\n"
        "Реквизиты:\n\n" + manual_copy_block(method, "ZF-5")
    )
    await call.message.answer(txt, parse_mode="Markdown")


@dp.callback_query(lambda c: c.data == "zf_back_method")
async def zf_back_method(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    await call.answer()
    await safe_edit(call, await zf_intro_ru(), reply_markup=kb_zf_choose_method(), parse_mode="Markdown")


@dp.callback_query(lambda c: c.data.startswith("zf_people_") or c.data in {"zf_people_other", "zf_edit_people", "zf_edit_label", "zf_confirm_paid"})
async def zf_people_buttons(call: CallbackQuery):
    st = PENDING.get(call.from_user.id, {})
    if call.data == "zf_edit_people":
        # go to choose people again
        st["type"] = "zf_wait_people"
        PENDING[call.from_user.id] = st
        await call.answer()
        await call.message.answer("Сколько человек вы оплатили по ZF?", reply_markup=kb_zf_people())
        return

    if call.data == "zf_edit_label":
        st["type"] = "zf_wait_label"
        PENDING[call.from_user.id] = st
        await call.answer()
        await call.message.answer("Напишите, как вас показать в списке (коротко):")
        return

    if call.data == "zf_confirm_paid":
        # finalize: add entry + post list
        if not st or st.get("type") != "zf_ready_confirm":
            await call.answer("Нет данных. Начните заново: ZF → выбрать метод.", show_alert=True)
            return

        people = int(st["people"])
        label = st["label"]
        method = st.get("method", "unknown")
        user_id = call.from_user.id
        username = call.from_user.username or "-"

        # Add entry
        await zf_add_entry(user_id, username, label, people, method, status="marked")
        # notify admin (with expected EUR, for checking)
        eur = people * ZF_EUR_PER_PERSON
        kg = people * ZF_KG_RICE_PER_PERSON

        await send_admin_html(
            "\n".join([
                "✅ ZF MARKED",
                f"Label: <b>{html_escape(label)}</b>",
                f"People: {people} (expected {eur}€) | Rice: {kg} kg",
                f"Method: {html_escape(method)}",
                f"Time: {utc_now_str()}",
                f"User: @{html_escape(username)} / {user_id}",
                user_link_html(user_id),
            ])
        )

        # post updated list to group
        await zf_post_update()

        PENDING.pop(call.from_user.id, None)
        await call.answer("OK")
        await call.message.answer("✅ Записано. Джазак Аллаху хейр! 🤍")
        return

    # people selection
    if call.data.startswith("zf_people_"):
        n = int(call.data.replace("zf_people_", ""))
        st["people"] = n
        st["type"] = "zf_wait_label"
        PENDING[call.from_user.id] = st
        await call.answer()
        await call.message.answer("Напишите, как вас показать в списке (коротко):")
        return

    if call.data == "zf_people_other":
        st["type"] = "zf_wait_people_text"
        PENDING[call.from_user.id] = st
        await call.answer()
        await call.message.answer("Введите число людей (например 12):")
        return


# =========================
# ID FLOW: choose method -> show details -> choose amount -> confirm "paid"
# =========================

@dp.callback_query(lambda c: c.data.startswith("id_method_"))
async def id_choose_method(call: CallbackQuery):
    if await kv_get_int("id_open", 0) == 0:
        await call.answer("ID закрыт.", show_alert=True)
        return

    method = call.data.replace("id_method_", "").strip()
    PENDING[call.from_user.id] = {"type": "id_wait_amount", "method": method}
    await call.answer()

    txt = (
        "✅ Вы выбрали способ оплаты.\n\n"
        f"В назначении/сообщении укажите *{MARK_ID}*.\n\n"
        "Реквизиты:\n\n" + manual_copy_block(method, MARK_ID) +
        "\n\nТеперь выберите сумму (для уведомления):"
    )
    await call.message.answer(txt, parse_mode="Markdown", reply_markup=kb_id_amounts())


@dp.callback_query(lambda c: c.data in {"id_back_method"} or c.data.startswith("id_amt_") or c.data == "id_confirm_paid")
async def id_amount_buttons(call: CallbackQuery):
    st = PENDING.get(call.from_user.id, {})
    if call.data == "id_back_method":
        lang = (await get_user_lang(call.from_user.id)) or "ru"
        PENDING.pop(call.from_user.id, None)
        await call.answer()
        await safe_edit(call, await id_intro_ru(), reply_markup=kb_id_choose_method(), parse_mode="Markdown")
        return

    if call.data.startswith("id_amt_"):
        x = call.data.replace("id_amt_", "")
        if x == "other":
            st["type"] = "id_wait_amount_text"
            PENDING[call.from_user.id] = st
            await call.answer()
            await call.message.answer("Введите сумму в евро (целое число), например 25:")
            return

        eur = int(x)
        st["amount_eur"] = eur
        st["type"] = "id_ready_confirm"
        PENDING[call.from_user.id] = st
        await call.answer()
        await call.message.answer(
            f"Проверьте:\nСумма: {eur}€\nОтметка: {MARK_ID}\n\n"
            "Нажмите «Я оплатил(а)», чтобы уведомить.",
            reply_markup=kb_confirm_id_marked(),
        )
        return

    if call.data == "id_confirm_paid":
        if not st or st.get("type") != "id_ready_confirm":
            await call.answer("Нет данных. Начните заново: ID → выбрать метод.", show_alert=True)
            return
        eur = int(st.get("amount_eur", 0) or 0)
        method = st.get("method", "unknown")

        # update internal total
        if eur > 0:
            await kv_inc_int("id_raised_eur", eur)

        user_id = call.from_user.id
        username = call.from_user.username or "-"

        await send_admin_html(
            "\n".join([
                "✅ ID MARKED",
                f"Amount: {eur} EUR",
                f"Method: {html_escape(method)}",
                f"Time: {utc_now_str()}",
                f"User: @{html_escape(username)} / {user_id}",
                user_link_html(user_id),
            ])
        )

        PENDING.pop(call.from_user.id, None)
        await call.answer("OK")
        await call.message.answer("✅ Спасибо! Джазак Аллаху хейр! 🤍")
        return


# =========================
# OTHER INPUT ROUTER
# =========================

@dp.message()
async def any_message_router(message: Message):
    if not message.from_user:
        return
    st = PENDING.get(message.from_user.id)
    if not st:
        return

    raw = (message.text or "").strip()

    # ZF: waiting code like ZF-5
    if st.get("type") == "zf_wait_code":
        people = parse_zf_code(raw)
        if not people:
            await message.answer("Пожалуйста, пришлите в формате `ZF-5` (или `ZF - 5`).", parse_mode="Markdown")
            return
        st["people"] = people
        st["type"] = "zf_wait_label"
        PENDING[message.from_user.id] = st
        await message.answer("Теперь напишите, как вас показать в списке (коротко):")
        return

    # ZF: waiting people as text
    if st.get("type") == "zf_wait_people_text":
        try:
            n = int(raw)
            if n <= 0 or n > 999:
                raise ValueError
        except Exception:
            await message.answer("Нужно число от 1 до 999. Попробуйте ещё раз:")
            return
        st["people"] = n
        st["type"] = "zf_wait_label"
        PENDING[message.from_user.id] = st
        await message.answer("Теперь напишите, как вас показать в списке (коротко):")
        return

    # ZF: waiting label
    if st.get("type") == "zf_wait_label":
        label = raw
        if len(label) < 2:
            await message.answer("Слишком коротко. Напишите хотя бы 2 символа:")
            return
        if len(label) > 60:
            label = label[:60].rstrip()

        people = int(st["people"])
        kg = people * ZF_KG_RICE_PER_PERSON
        eur = people * ZF_EUR_PER_PERSON

        st["label"] = label
        st["type"] = "zf_ready_confirm"
        PENDING[message.from_user.id] = st

        await message.answer(
            "Проверьте, пожалуйста:\n\n"
            f"Как в списке: *{label}*\n"
            f"Количество: *{people} чел.*\n"
            f"Это = *{kg} кг риса*\n"
            f"(Для самопроверки суммы: {eur}€)\n\n"
            "Если всё верно — нажмите кнопку ниже.",
            parse_mode="Markdown",
            reply_markup=kb_confirm_zf_entry(),
        )
        return

    # ID: other amount text
    if st.get("type") == "id_wait_amount_text":
        try:
            eur = int(raw)
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        st["amount_eur"] = eur
        st["type"] = "id_ready_confirm"
        PENDING[message.from_user.id] = st
        await message.answer(
            f"Проверьте:\nСумма: {eur}€\nОтметка: {MARK_ID}\n\nНажмите «Я оплатил(а)».",
            reply_markup=kb_confirm_id_marked(),
        )
        return


# =========================
# ADMIN COMMANDS
# =========================

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not admin_only(message):
        return
    await message.answer("🛠 Admin panel:", reply_markup=kb_admin_panel())


async def admin_status_text() -> str:
    stars_on = await kv_get_int("pay_stars_enabled", 0)
    manual_on = await kv_get_int("pay_manual_enabled", 1)

    water_batch = await kv_get_int("water_batch", 1)
    wt = await kv_get_int("water_target_eur", 235)
    wr = await kv_get_int("water_raised_eur", 0)

    iftar_day = await kv_get_int("iftar_day", 1)
    it = await kv_get_int("iftar_target_portions", 100)
    ir = await kv_get_int("iftar_raised_portions", 0)

    zf_open = await kv_get_int("zf_open", 0)
    id_open = await kv_get_int("id_open", 0)
    zf_people, zf_kg = await zf_totals()
    id_raised = await kv_get_int("id_raised_eur", 0)

    return (
        "📊 STATUS\n"
        f"TZ: {TIMEZONE}\n"
        f"Stars: {'ON' if stars_on else 'OFF'} | Manual: {'ON' if manual_on else 'OFF'}\n"
        f"PUBLIC_GROUP_ID: {PUBLIC_GROUP_ID} | ZF_GROUP_ID: {ZF_GROUP_ID}\n\n"
        f"💧 Water cistern #{water_batch}: {wr}/{wt} EUR | ref={MARK_WATER}\n"
        f"🍲 Iftar day {iftar_day}: {ir}/{it} portions | ref={MARK_IFTAR}\n\n"
        f"🟢 ZF: {'OPEN' if zf_open else 'CLOSED'} | {zf_kg} kg rice\n"
        f"🎉 ID: {'OPEN' if id_open else 'CLOSED'} | raised={id_raised} EUR\n"
    )


@dp.callback_query(lambda c: c.data.startswith("adm_"))
async def admin_panel_callbacks(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("No access", show_alert=True)
        return

    if call.data == "adm_status":
        await call.answer()
        await call.message.answer(await admin_status_text())
        return

    if call.data == "adm_toggle_stars":
        cur = await kv_get_int("pay_stars_enabled", 0)
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
            await call.message.a
