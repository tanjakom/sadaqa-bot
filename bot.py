import os
import re
import logging
import asyncio
from datetime import datetime, date
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

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


# =========================
# CONFIG
# =========================

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")

# DB + timezone
DB_PATH = os.getenv("DB_PATH", "/var/data/data.db").strip() or "data.db"
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki").strip() or "Europe/Helsinki"
PORT = int(os.getenv("PORT", "10000") or "10000")

# Ramadan start (you said: 18.02.2026)
RAMADAN_START = os.getenv("RAMADAN_START", "2026-02-18").strip()  # YYYY-MM-DD

# Optional group ids for reports (if not set -> admin only)
PUBLIC_GROUP_ID = int(os.getenv("PUBLIC_GROUP_ID", "0") or "0")  # daily reports for general campaigns
ZF_GROUP_ID = int(os.getenv("ZF_GROUP_ID", "0") or "0")          # ZF list updates

# Stars rate
EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")

# Marks (bank reference codes)
MARK_IFTAR = "MIMAX"
MARK_WATER = "GREENMAX"
MARK_ID = "Id"
MARK_ZF_PREFIX = "ZF"

# ZF math
ZF_EUR_PER_PERSON = float(os.getenv("ZF_EUR_PER_PERSON", "9") or "9")
ZF_KG_PER_PERSON = int(os.getenv("ZF_KG_PER_PERSON", "3") or "3")

# Iftar price (for explanation + Stars math)
IFTAR_EUR_PER_PORTION = float(os.getenv("IFTAR_EUR_PER_PORTION", "4") or "4")

# Water target defaults
DEFAULT_WATER_TARGET_EUR = int(os.getenv("WATER_TARGET_EUR", "235") or "235")

# Iftar target defaults
DEFAULT_IFTAR_TARGET_PORTIONS = int(os.getenv("IFTAR_TARGET_PORTIONS", "100") or "100")

# Daily report time (local)
DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "21") or "21")

# We collect for "tomorrow" by default:
IFTAR_COLLECT_OFFSET_DAYS = int(os.getenv("IFTAR_COLLECT_OFFSET_DAYS", "1") or "1")


# Payment details
# "Bank transfer" (SEPA)
SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "").strip()
SEPA_IBAN = os.getenv("SEPA_IBAN", "").strip()
SEPA_BIC = os.getenv("SEPA_BIC", "").strip()

# Turkey bank transfer (new)
TR_BANK_NAME = os.getenv("TR_BANK_NAME", "").strip()
TR_RECIPIENT = os.getenv("TR_RECIPIENT", "").strip()
TR_IBAN = os.getenv("TR_IBAN", "").strip()
TR_ACCOUNT = os.getenv("TR_ACCOUNT", "").strip()  # optional
TR_SWIFT = os.getenv("TR_SWIFT", "").strip()      # optional

# PayPal
PAYPAL_LINK = os.getenv("PAYPAL_LINK", "").strip()

# ZEN
ZEN_NAME = os.getenv("ZEN_NAME", "").strip()
ZEN_PHONE = os.getenv("ZEN_PHONE", "").strip()
ZEN_CARD = os.getenv("ZEN_CARD", "").strip()

# Crypto
USDT_TRC20 = os.getenv("USDT_TRC20", "").strip()
USDC_ERC20 = os.getenv("USDC_ERC20", "").strip()
BYBIT_ID = os.getenv("BYBIT_ID", "").strip()

# SWIFT (optional)
SWIFT_RECIPIENT = os.getenv("SWIFT_RECIPIENT", "").strip()
SWIFT_BANK = os.getenv("SWIFT_BANK", "").strip()
SWIFT_ACCOUNT = os.getenv("SWIFT_ACCOUNT", "").strip()
SWIFT_BIC = os.getenv("SWIFT_BIC", "").strip()
SWIFT_BANK_ADDRESS = os.getenv("SWIFT_BANK_ADDRESS", "").strip()

# Card-to-card (optional)
CARD_RECIPIENT = os.getenv("CARD_RECIPIENT", "").strip()
CARD_NUMBER = os.getenv("CARD_NUMBER", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# =========================
# TIME HELPERS
# =========================

def _tzinfo():
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(TIMEZONE)
    except Exception:
        return ZoneInfo("UTC")


def now_local() -> datetime:
    tz = _tzinfo()
    if tz is None:
        return datetime.utcnow()
    return datetime.now(tz)


def today_local() -> date:
    return now_local().date()


def parse_iso_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def get_ramadan_day() -> int:
    """
    Returns 1.. based on RAMADAN_START in local timezone.
    If RAMADAN_START invalid -> 1.
    """
    start = parse_iso_date(RAMADAN_START)
    if not start:
        return 1
    day = (today_local() - start).days + 1
    return max(1, day)


def iftar_collect_day() -> int:
    """
    "Tomorrow" logic: if today is Ramadan day 7 -> collect for day 8.
    """
    return max(1, get_ramadan_day() + IFTAR_COLLECT_OFFSET_DAYS)


def utc_now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


# =========================
# TEXT HELPERS / UI
# =========================

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en


def admin_only(user_id: int) -> bool:
    return user_id == ADMIN_ID


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup)


async def send_admin(text: str):
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to send admin message")


async def send_to_chat(chat_id: int, text: str):
    if not chat_id:
        return
    try:
        await bot.send_message(chat_id, text, disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to send to chat_id=%s", chat_id)


def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)


# =========================
# DB
# =========================

async def _ensure_db_dir():
    # ensure parent folder exists (important for /var/data on Render disks)
    parent = os.path.dirname(DB_PATH)
    if parent:
        try:
            os.makedirs(parent, exist_ok=True)
        except Exception:
            pass


async def db_init():
    await _ensure_db_dir()

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
        await db.execute("""
        CREATE TABLE IF NOT EXISTS zf_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            label TEXT NOT NULL,
            people INTEGER NOT NULL,
            bank_code TEXT NOT NULL,
            method TEXT NOT NULL
        )
        """)

        # toggles
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('stars_enabled','1')")

        # Water collective campaign state
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur',?)", (str(DEFAULT_WATER_TARGET_EUR),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        # Iftar collective campaign state
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions',?)", (str(DEFAULT_IFTAR_TARGET_PORTIONS),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day_date',?)", (today_local().isoformat(),))

        # ID campaign (accounting only)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_raised_eur','0')")

        # ZF/ID open/close by date window (can edit via /set_dates later if needed)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_end','2026-03-20')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_end','2026-03-20')")

        # descriptions (editable)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_iftar_ru',?)", (
            "Десятый Рамадан подряд программа ифтаров кормит семьи бедняков в палаточном лагере.\n"
            "Отметка для оплаты: MIMAX (укажите только это).",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_iftar_en',?)", (
            "For the 10th Ramadan in a row, our iftar program feeds poor families in a tent camp.\n"
            "Payment reference: MIMAX (please write only this).",
        ))

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_water_ru',?)", (
            "Раздача питьевой воды (цистерна 5000л).\n"
            "Отметка для оплаты: GREENMAX (укажите только это).",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_water_en',?)", (
            "Drinking water distribution (5000L tank).\n"
            "Payment reference: GREENMAX (please write only this).",
        ))

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_zf_ru',?)", (
            "Закят-уль-Фитр (ZF). 1 человек = 3 кг (1 са`а). Учёт: 9€ за человека.\n"
            "В назначении: ZF5 / ZF8 (цифра = количество людей).",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_zf_en',?)", (
            "Zakat al-Fitr (ZF). 1 person = 3 kg (1 sa‘). Accounting: 9€ per person.\n"
            "Reference: ZF5 / ZF8 (number = people).",
        ))

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_id_ru',?)", (
            "Ид аль-Фитр (Id) — сбор на сладости/выпечку детям в день праздника.\n"
            "Отметка для оплаты: Id (укажите только это).",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_id_en',?)", (
            "Eid al-Fitr (Id) — sweets & pastries for children on Eid day.\n"
            "Payment reference: Id (please write only this).",
        ))

        # daily report
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
    v = await kv_get_int(key, 0)
    await kv_set_int(key, v + int(delta))


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
# ZF helpers
# =========================

def parse_zf_bank_code(s: str) -> Optional[int]:
    s = (s or "").strip()
    m = re.search(r"\bZF\s*[-–]?\s*(\d{1,3})\b", s, flags=re.IGNORECASE)
    if not m:
        return None
    n = int(m.group(1))
    if n <= 0 or n > 999:
        return None
    return n


async def zf_add_entry(user_id: int, username: str, label: str, people: int, bank_code: str, method: str):
    label = (label or "").strip()
    if len(label) > 80:
        label = label[:80].rstrip()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO zf_entries(created_utc, user_id, username, label, people, bank_code, method)
            VALUES(?,?,?,?,?,?,?)
            """,
            (utc_now_str(), user_id, username or "-", label, int(people), bank_code, method),
        )
        await db.commit()


async def zf_totals() -> Tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(SUM(people),0) FROM zf_entries") as cur:
            row = await cur.fetchone()
            total_people = int(row[0] or 0)
    return total_people, total_people * ZF_KG_PER_PERSON


async def zf_list_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT label, people FROM zf_entries ORDER BY id ASC") as cur:
            rows = await cur.fetchall()

    lines = ["Закят-уль-Фитр"]
    for i, (label, people) in enumerate(rows, start=1):
        lines.append(f"{i}. {label} — {int(people)} чел.")

    total_people, total_kg = await zf_totals()
    lines.append("")
    lines.append(f"Всего: {total_kg} кг риса")
    return "\n".join(lines)


async def zf_post_update():
    text = await zf_list_text()
    if ZF_GROUP_ID:
        await send_to_chat(ZF_GROUP_ID, text)
    else:
        await send_to_chat(ADMIN_ID, text)


# =========================
# Scheduler: open/close ZF & ID + iftar rollover + daily report
# =========================

async def schedule_tick():
    today = today_local()

    zf_start = parse_iso_date(await kv_get("zf_start") or "")
    zf_end = parse_iso_date(await kv_get("zf_end") or "")
    id_start = parse_iso_date(await kv_get("id_start") or "")
    id_end = parse_iso_date(await kv_get("id_end") or "")

    if zf_start and zf_end:
        should_open = 1 if (zf_start <= today <= zf_end) else 0
        cur = await kv_get_int("zf_open", 0)
        if cur != should_open:
            await kv_set_int("zf_open", should_open)
            await send_admin(f"ZF status: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")

    if id_start and id_end:
        should_open = 1 if (id_start <= today <= id_end) else 0
        cur = await kv_get_int("id_open", 0)
        if cur != should_open:
            await kv_set_int("id_open", should_open)
            await send_admin(f"Id status: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")


async def iftar_rollover_tick():
    """
    At 00:00 local: reset iftar raised for new day.
    We always collect for "tomorrow" day number, so we store only date stamp
    and reset raised when date changes.
    """
    today_str = today_local().isoformat()
    last_date = (await kv_get("iftar_day_date") or "").strip()
    if last_date != today_str:
        await kv_set_int("iftar_raised_portions", 0)
        await kv_set("iftar_day_date", today_str)
        await send_admin(f"Iftar rollover: new date {today_str}, raised reset.")


async def build_daily_report() -> str:
    water_batch = await kv_get_int("water_batch", 1)
    water_target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
    water_raised = await kv_get_int("water_raised_eur", 0)
    water_rem = max(0, water_target - water_raised)

    iftar_day = iftar_collect_day()
    iftar_target = await kv_get_int("iftar_target_portions", DEFAULT_IFTAR_TARGET_PORTIONS)
    iftar_raised = await kv_get_int("iftar_raised_portions", 0)
    iftar_rem = max(0, iftar_target - iftar_raised)

    zf_people, zf_kg = await zf_totals()
    id_raised = await kv_get_int("id_raised_eur", 0)

    now_str = now_local().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"Ежедневный отчёт ({now_str} {TIMEZONE})",
        "",
        f"Ифтары — собираем на день {iftar_day}",
        f"Собрано: {iftar_raised} / {iftar_target} порций | Осталось: {iftar_rem}",
        battery(iftar_raised, iftar_target),
        "",
        f"Вода — цистерна #{water_batch}",
        f"Собрано: {water_raised}€ / {water_target}€ | Осталось: {water_rem}€",
        battery(water_raised, water_target),
        "",
        "ZF",
        f"Отмечено: {zf_kg} кг риса",
        "",
        "Id",
        f"Собрано (учёт): {id_raised}€",
    ]
    return "\n".join(lines)


async def daily_report_tick():
    now = now_local()
    if now.hour < DAILY_REPORT_HOUR:
        return

    today_str = today_local().isoformat()
    last = (await kv_get("last_daily_report_date") or "").strip()
    if last == today_str:
        return

    report = await build_daily_report()
    if PUBLIC_GROUP_ID:
        await send_to_chat(PUBLIC_GROUP_ID, report)
    await send_to_chat(ADMIN_ID, report)
    await kv_set("last_daily_report_date", today_str)


async def scheduler_loop():
    while True:
        try:
            await schedule_tick()
            await iftar_rollover_tick()
            await daily_report_tick()
        except Exception:
            logging.exception("scheduler tick failed")
        await asyncio.sleep(60)


# =========================
# Keyboards
# =========================

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()


def kb_main(lang: str, is_admin: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Сборы", "📋 Campaigns"), callback_data="list")
    kb.button(text=t(lang, "❓ Помощь", "❓ Help"), callback_data="help")
    kb.button(text=t(lang, "🌐 Язык", "🌐 Language"), callback_data="lang_menu")
    if is_admin:
        kb.button(text="🛠 Админ", callback_data="admin_menu")
    kb.adjust(1)
    return kb.as_markup()


def kb_campaigns(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "🍲 Ифтары", "🍲 Iftars"), callback_data="c_iftar")
    kb.button(text=t(lang, "💧 Вода", "💧 Water"), callback_data="c_water")
    kb.button(text=t(lang, "🌾 Закят-уль-Фитр (ZF)", "🌾 Zakat al-Fitr (ZF)"), callback_data="c_zf")
    kb.button(text=t(lang, "🍬 Ид (Id)", "🍬 Eid (Id)"), callback_data="c_id")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_campaign_actions(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💳 Способы оплаты", "💳 Payment methods"), callback_data="pay_methods")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_methods(stars_enabled: bool, lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "🏦 Банковский перевод (EU)", "🏦 Bank transfer (EU)"), callback_data="pay_bank")
    kb.button(text=t(lang, "🇹🇷 Турецкий счёт", "🇹🇷 Turkish bank"), callback_data="pay_tr")
    kb.button(text=t(lang, "🌍 SWIFT", "🌍 SWIFT"), callback_data="pay_swift")
    kb.button(text=t(lang, "💙 PayPal", "💙 PayPal"), callback_data="pay_paypal")
    kb.button(text=t(lang, "⚡ ZEN Express", "⚡ ZEN Express"), callback_data="pay_zen")
    kb.button(text=t(lang, "💳 С карты на карту", "💳 Card to card"), callback_data="pay_card")
    kb.button(text=t(lang, "💎 Криптовалюта", "💎 Crypto"), callback_data="pay_crypto")
    if stars_enabled:
        kb.button(text=t(lang, "⭐ Telegram Stars", "⭐ Telegram Stars"), callback_data="pay_stars")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="pay_back")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📌 Команды (шпаргалка)", callback_data="adm_help")
    kb.button(text="⭐ activate_stars", callback_data="adm_activate_stars")
    kb.button(text="⭐ deactivate_stars", callback_data="adm_deactivate_stars")
    kb.button(text="📣 Отчёт сейчас", callback_data="adm_report_now")
    kb.button(text="🆔 Показать chat_id (мне)", callback_data="adm_show_my_id")
    kb.adjust(1)
    return kb.as_markup()


def kb_zf_after_payment(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "✅ Я оплатил(а) — внести в список ZF", "✅ I paid — add to ZF list"), callback_data="zf_mark")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="pay_back")
    kb.adjust(1)
    return kb.as_markup()


def kb_id_after_payment(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "✅ Я оплатил(а) (уведомить)", "✅ I paid (notify)"), callback_data="id_mark")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="pay_back")
    kb.adjust(1)
    return kb.as_markup()


def kb_stars_iftar(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⭐ 5 порций", "⭐ 5 portions"), callback_data="stars_iftar_5")
    kb.button(text=t(lang, "⭐ 10 порций", "⭐ 10 portions"), callback_data="stars_iftar_10")
    kb.button(text=t(lang, "⭐ 20 порций", "⭐ 20 portions"), callback_data="stars_iftar_20")
    kb.button(text=t(lang, "⭐ Другое", "⭐ Other"), callback_data="stars_iftar_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="pay_methods")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def kb_stars_water(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ 10€", callback_data="stars_water_10")
    kb.button(text="⭐ 25€", callback_data="stars_water_25")
    kb.button(text="⭐ 50€", callback_data="stars_water_50")
    kb.button(text=t(lang, "⭐ Другое", "⭐ Other"), callback_data="stars_water_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="pay_methods")
    kb.adjust(2, 1, 1, 1)
    return kb.as_markup()


# =========================
# State
# =========================

PENDING: dict[int, dict] = {}
LAST_CAMPAIGN: dict[int, str] = {}  # "iftar"|"water"|"zf"|"id"


def code_for_campaign(c: str) -> str:
    if c == "iftar":
        return MARK_IFTAR
    if c == "water":
        return MARK_WATER
    if c == "id":
        return MARK_ID
    if c == "zf":
        return "ZF5"
    return "SUPPORT"


# =========================
# /start + language
# =========================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)

    if not lang:
        await message.answer("Ассаляму алейкум 🤍\n\nВыберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    is_admin = admin_only(uid)
    txt = t(
        lang,
        "Ассаляму алейкум 🤍\n\n1) Выберите сбор\n2) Затем выберите способ оплаты",
        "Assalamu alaykum 🤍\n\n1) Choose a campaign\n2) Then choose a payment method",
    )
    await message.answer(txt, reply_markup=kb_main(lang, is_admin))


@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    await message.answer("Выберите язык / Choose language:", reply_markup=kb_lang_select())


@dp.callback_query(lambda c: c.data in {"lang_ru", "lang_en"})
async def cb_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    is_admin = admin_only(call.from_user.id)
    await safe_edit(call, t(lang, "Язык установлен.", "Language set."), reply_markup=kb_main(lang, is_admin))


# =========================
# Menus
# =========================

@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "help", "admin_menu"})
async def cb_menus(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    is_admin = admin_only(uid)

    if call.data == "lang_menu":
        await call.answer()
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    if call.data == "back":
        await call.answer()
        await safe_edit(call, t(lang, "Меню:", "Menu:"), reply_markup=kb_main(lang, is_admin))
        return

    if call.data == "list":
        await call.answer()
        await safe_edit(call, t(lang, "Выберите сбор:", "Choose a campaign:"), reply_markup=kb_campaigns(lang))
        return

    if call.data == "help":
        await call.answer()
        txt = t(
            lang,
            "Помощь\n\nЛогика:\n1) Выберите сбор\n2) Нажмите «Способы оплаты»\n3) Скопируйте реквизиты и укажите только отметку сбора\n\nОтметки:\n— Ифтары: MIMAX\n— Вода: GREENMAX\n— ZF: ZF5 (цифра = люди)\n— Id: Id",
            "Help\n\nHow it works:\n1) Choose a campaign\n2) Tap “Payment methods”\n3) Copy details and write only the campaign reference\n\nReferences:\n— Iftars: MIMAX\n— Water: GREENMAX\n— ZF: ZF5 (number = people)\n— Id: Id",
        )
        await safe_edit(call, txt, reply_markup=kb_main(lang, is_admin))
        return

    if call.data == "admin_menu":
        await call.answer()
        if not is_admin:
            await safe_edit(call, t(lang, "Нет доступа.", "No access."), reply_markup=kb_main(lang, False))
            return
        await safe_edit(call, "Админ-меню:", reply_markup=kb_admin_menu())
        return


# =========================
# Campaign screens (description-only)
# =========================

@dp.callback_query(lambda c: c.data.startswith("c_"))
async def cb_campaign(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    key = call.data.replace("c_", "").strip()
    LAST_CAMPAIGN[uid] = key

    if key == "iftar":
        day = iftar_collect_day()
        desc = await kv_get("desc_iftar_ru" if lang == "ru" else "desc_iftar_en")
        desc = desc + (f"\n\nСобираем на день {day}." if lang == "ru" else f"\n\nCollecting for Ramadan day {day}.")
    elif key == "water":
        desc = await kv_get("desc_water_ru" if lang == "ru" else "desc_water_en")
    elif key == "zf":
        if await kv_get_int("zf_open", 0) == 0:
            await call.answer()
            await safe_edit(call, t(lang, "ZF сейчас закрыт.", "ZF is closed now."), reply_markup=kb_campaigns(lang))
            return
        desc = await kv_get("desc_zf_ru" if lang == "ru" else "desc_zf_en")
    elif key == "id":
        if await kv_get_int("id_open", 0) == 0:
            await call.answer()
            await safe_edit(call, t(lang, "Id сейчас закрыт.", "Id is closed now."), reply_markup=kb_campaigns(lang))
            return
        desc = await kv_get("desc_id_ru" if lang == "ru" else "desc_id_en")
    else:
        desc = "—"

    await call.answer()
    await safe_edit(call, desc, reply_markup=kb_campaign_actions(lang))


@dp.callback_query(lambda c: c.data == "pay_methods")
async def cb_pay_methods(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    stars_enabled = bool(await kv_get_int("stars_enabled", 1))
    await call.answer()
    await call.message.answer(t(lang, "Выберите способ оплаты:", "Choose a payment method:"), reply_markup=kb_payment_methods(stars_enabled, lang))


@dp.callback_query(lambda c: c.data == "pay_back")
async def cb_pay_back(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    await call.answer()
    await call.message.answer(t(lang, "Назад к сбору:", "Back to campaign:"), reply_markup=kb_campaign_actions(lang))


# =========================
# Payment texts (copy-friendly, NO parse_mode)
# =========================

def payment_text_bank_eu(code: str) -> str:
    lines = [
        "Банковский перевод (EU)",
        "",
        "Получатель:",
        SEPA_RECIPIENT or "—",
        "",
        "IBAN:",
        SEPA_IBAN or "—",
    ]
    if SEPA_BIC:
        lines += ["", "BIC:", SEPA_BIC]
    lines += ["", "Отметка (только это):", code]
    return "\n".join(lines)


def payment_text_bank_tr(code: str) -> str:
    lines = [
        "Турецкий счёт",
        "",
        "Банк:",
        TR_BANK_NAME or "—",
        "",
        "Получатель:",
        TR_RECIPIENT or "—",
    ]
    if TR_IBAN:
        lines += ["", "IBAN:", TR_IBAN]
    if TR_ACCOUNT:
        lines += ["", "Счёт:", TR_ACCOUNT]
    if TR_SWIFT:
        lines += ["", "SWIFT:", TR_SWIFT]
    lines += ["", "Отметка (только это):", code]
    return "\n".join(lines)


def payment_text_paypal(code: str) -> str:
    return "\n".join([
        "PayPal",
        "",
        "Ссылка:",
        PAYPAL_LINK or "—",
        "",
        "Отметка (только это):",
        code,
    ])


def payment_text_zen(code: str) -> str:
    lines = ["ZEN Express", ""]
    if ZEN_NAME:
        lines += ["Получатель:", ZEN_NAME, ""]
    if ZEN_PHONE:
        lines += ["Телефон:", ZEN_PHONE, ""]
    if ZEN_CARD:
        lines += ["Карта:", ZEN_CARD, ""]
    lines += ["Отметка (только это):", code]
    return "\n".join(lines)


def payment_text_card(code: str) -> str:
    lines = ["С карты на карту", ""]
    lines += ["Получатель:", (CARD_RECIPIENT or "—"), ""]
    lines += ["Номер карты:", (CARD_NUMBER or "—"), ""]
    lines += ["Отметка (только это):", code]
    return "\n".join(lines)


def payment_text_crypto(code: str) -> str:
    lines = [
        "Криптовалюта",
        "",
        "USDT (TRC20):",
        (USDT_TRC20 or "—"),
        "",
        "USDC (ERC20):",
        (USDC_ERC20 or "—"),
    ]
    if BYBIT_ID:
        lines += ["", "Bybit ID:", BYBIT_ID]
    lines += ["", "Отметка (только это):", code]
    return "\n".join(lines)


def payment_text_swift(code: str) -> str:
    lines = ["SWIFT", ""]
    if SWIFT_RECIPIENT:
        lines += ["Получатель:", SWIFT_RECIPIENT, ""]
    if SWIFT_BANK:
        lines += ["Банк:", SWIFT_BANK, ""]
    if SWIFT_BANK_ADDRESS:
        lines += ["Адрес банка:", SWIFT_BANK_ADDRESS, ""]
    if SWIFT_ACCOUNT:
        lines += ["Счёт/IBAN:", SWIFT_ACCOUNT, ""]
    if SWIFT_BIC:
        lines += ["BIC/SWIFT:", SWIFT_BIC, ""]
    lines += ["Отметка (только это):", code]
    return "\n".join(lines)


# =========================
# Payment method callbacks
# =========================

@dp.callback_query(lambda c: c.data.startswith("pay_"))
async def cb_pay(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"

    method = call.data.replace("pay_", "").strip()
    campaign = LAST_CAMPAIGN.get(uid, "iftar")
    code = code_for_campaign(campaign)

    stars_enabled = bool(await kv_get_int("stars_enabled", 1))
    if method == "stars" and not stars_enabled:
        await call.answer(t(lang, "Stars выключены.", "Stars disabled."), show_alert=True)
        return

    # ZF special
    if campaign == "zf":
        intro = t(
            lang,
            "ZF — Закят-уль-Фитр\n\n1) Оплатите\n2) В назначении: ZF5 / ZF8 (цифра = люди)\n3) После оплаты нажмите кнопку и внесите себя в список",
            "ZF — Zakat al-Fitr\n\n1) Pay\n2) Reference: ZF5 / ZF8 (number = people)\n3) After payment tap the button to add yourself to the list",
        )
        code = "ZF5"
        if method == "bank":
            txt = intro + "\n\n" + payment_text_bank_eu(code)
        elif method == "tr":
            txt = intro + "\n\n" + payment_text_bank_tr(code)
        elif method == "swift":
            txt = intro + "\n\n" + payment_text_swift(code)
        elif method == "paypal":
            txt = intro + "\n\n" + payment_text_paypal(code)
        elif method == "zen":
            txt = intro + "\n\n" + payment_text_zen(code)
        elif method == "card":
            txt = intro + "\n\n" + payment_text_card(code)
        elif method == "crypto":
            txt = intro + "\n\n" + payment_text_crypto(code)
        elif method == "stars":
            txt = intro + "\n\n" + t(lang, "Stars можно включить, но этот сбор срочный — лучше банк/крипто.", "Stars can be enabled, but for urgent campaigns bank/crypto is better.")
        else:
            txt = intro

        await call.answer()
        await call.message.answer(txt, reply_markup=kb_zf_after_payment(lang))
        return

    # ID special
    if campaign == "id":
        intro = t(lang, "Id — Ид аль-Фитр\n\nОплатите и укажите только отметку.", "Id — Eid\n\nPay and write only the reference.")
        if method == "bank":
            txt = intro + "\n\n" + payment_text_bank_eu(code)
        elif method == "tr":
            txt = intro + "\n\n" + payment_text_bank_tr(code)
        elif method == "swift":
            txt = intro + "\n\n" + payment_text_swift(code)
        elif method == "paypal":
            txt = intro + "\n\n" + payment_text_paypal(code)
        elif method == "zen":
            txt = intro + "\n\n" + payment_text_zen(code)
        elif method == "card":
            txt = intro + "\n\n" + payment_text_card(code)
        elif method == "crypto":
            txt = intro + "\n\n" + payment_text_crypto(code)
        elif method == "stars":
            await call.answer()
            await call.message.answer(
                t(lang, f"Оплата Stars. Курс: 1€={EUR_TO_STARS}⭐. Выберите сумму:", f"Stars payment. Rate: 1€={EUR_TO_STARS}⭐. Choose amount:"),
                reply_markup=kb_stars_water(lang),
            )
            return
        else:
            txt = intro

        await call.answer()
        await call.message.answer(txt, reply_markup=kb_id_after_payment(lang))
        return

    # Iftar / Water
    if campaign == "iftar" and method == "stars":
        await call.answer()
        await call.message.answer(
            t(lang, f"Оплата Stars. Курс: 1€={EUR_TO_STARS}⭐. 1 порция = {IFTAR_EUR_PER_PORTION}€.", f"Stars payment. Rate: 1€={EUR_TO_STARS}⭐. 1 portion = {IFTAR_EUR_PER_PORTION}€."),
            reply_markup=kb_stars_iftar(lang),
        )
        return

    if campaign == "water" and method == "stars":
        await call.answer()
        await call.message.answer(
            t(lang, f"Оплата Stars. Курс: 1€={EUR_TO_STARS}⭐. Выберите сумму:", f"Stars payment. Rate: 1€={EUR_TO_STARS}⭐. Choose amount:"),
            reply_markup=kb_stars_water(lang),
        )
        return

    # Non-stars methods
    intro = t(lang, "Оплатите и укажите только отметку сбора.", "Pay and write only the campaign reference.")
    if method == "bank":
        txt = intro + "\n\n" + payment_text_bank_eu(code)
    elif method == "tr":
        txt = intro + "\n\n" + payment_text_bank_tr(code)
    elif method == "swift":
        txt = intro + "\n\n" + payment_text_swift(code)
    elif method == "paypal":
        txt = intro + "\n\n" + payment_text_paypal(code)
    elif method == "zen":
        txt = intro + "\n\n" + payment_text_zen(code)
    elif method == "card":
        txt = intro + "\n\n" + payment_text_card(code)
    elif method == "crypto":
        txt = intro + "\n\n" + payment_text_crypto(code)
    else:
        txt = intro

    await call.answer()
    await call.message.answer(txt)


# =========================
# Stars payment handlers (REAL payments)
# =========================

@dp.callback_query(lambda c: c.data.startswith("stars_iftar_"))
async def stars_iftar_choose(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"

    if call.data == "stars_iftar_other":
        PENDING[uid] = {"type": "stars_iftar_other"}
        await call.answer()
        await call.message.answer(t(lang, "Введите количество порций (целое число), например 7:", "Enter portions (whole number), e.g. 7:"))
        return

    portions = int(call.data.split("_")[-1])
    stars = int(round(portions * IFTAR_EUR_PER_PORTION * EUR_TO_STARS))
    payload = f"stars:iftar:{portions}"

    await call.answer()
    await bot.send_invoice(
        chat_id=uid,
        title=t(lang, "Ифтары (Stars)", "Iftars (Stars)"),
        description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
        provider_token="",
    )


@dp.callback_query(lambda c: c.data.startswith("stars_water_"))
async def stars_water_choose(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"

    if call.data == "stars_water_other":
        PENDING[uid] = {"type": "stars_water_other"}
        await call.answer()
        await call.message.answer(t(lang, "Введите сумму в евро (целое число), например 12:", "Enter amount in EUR (whole number), e.g. 12:"))
        return

    eur = int(call.data.split("_")[-1])
    stars = int(round(eur * EUR_TO_STARS))
    payload = f"stars:water:{eur}"

    await call.answer()
    await bot.send_invoice(
        chat_id=uid,
        title=t(lang, "Вода (Stars)", "Water (Stars)"),
        description=t(lang, f"{eur}€ (≈ {stars}⭐)", f"{eur}€ (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
        provider_token="",
    )


@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)


@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def stars_success(message: Message):
    lang = (await get_user_lang(message.from_user.id)) or "ru"
    payload = message.successful_payment.invoice_payload or ""

    try:
        _, typ, val = payload.split(":")
        n = int(val)
    except Exception:
        await message.answer(t(lang, "✅ Спасибо! Джазак Аллаху хейр! 🤍", "✅ Thank you! Jazak Allahu khayr! 🤍"))
        return

    if typ == "iftar":
        await kv_inc_int("iftar_raised_portions", n)
        day = iftar_collect_day()
        await send_admin(f"STARS payment: IFTAR day {day}, portions {n}, time {utc_now_str()}, user @{message.from_user.username or '-'} / {message.from_user.id}")
    elif typ == "water":
        await kv_inc_int("water_raised_eur", n)
        await send_admin(f"STARS payment: WATER {n} EUR, time {utc_now_str()}, user @{message.from_user.username or '-'} / {message.from_user.id}")

    await message.answer(t(lang, "✅ Платёж получен. Джазак Аллаху хейр! 🤍", "✅ Payment received. Jazak Allahu khayr! 🤍"))


# =========================
# Pending text inputs
# =========================

@dp.callback_query(lambda c: c.data == "zf_mark")
async def cb_zf_mark(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    PENDING[call.from_user.id] = {"type": "zf_wait_code"}
    await call.answer()
    await call.message.answer(t(lang, "Пришлите код, который вы указали при оплате (пример: ZF5).", "Send the code you used for payment (example: ZF5)."))


@dp.callback_query(lambda c: c.data == "id_mark")
async def cb_id_mark(call: CallbackQuery):
    lang = (await get_user_lang(call.from_user.id)) or "ru"
    PENDING[call.from_user.id] = {"type": "id_wait_amount"}
    await call.answer()
    await call.message.answer(t(lang, "Введите сумму в евро (целое число), чтобы мы могли учесть (пример: 20):", "Enter amount in EUR (whole number) for accounting (example: 20):"))


@dp.message()
async def pending_router(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    st = PENDING.get(uid)
    if not st:
        return
    raw = (message.text or "").strip()

    lang = (await get_user_lang(uid)) or "ru"

    if st.get("type") == "stars_iftar_other":
        try:
            portions = int(raw)
            if portions <= 0:
                raise ValueError
        except Exception:
            await message.answer(t(lang, "Нужно целое число > 0. Попробуйте ещё раз:", "Please send a whole number > 0. Try again:"))
            return

        stars = int(round(portions * IFTAR_EUR_PER_PORTION * EUR_TO_STARS))
        payload = f"stars:iftar:{portions}"
        PENDING.pop(uid, None)

        await bot.send_invoice(
            chat_id=uid,
            title=t(lang, "Ифтары (Stars)", "Iftars (Stars)"),
            description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
            provider_token="",
        )
        return

    if st.get("type") == "stars_water_other":
        try:
            eur = int(raw)
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer(t(lang, "Нужно целое число > 0. Попробуйте ещё раз:", "Please send a whole number > 0. Try again:"))
            return

        stars = int(round(eur * EUR_TO_STARS))
        payload = f"stars:water:{eur}"
        PENDING.pop(uid, None)

        await bot.send_invoice(
            chat_id=uid,
            title=t(lang, "Вода (Stars)", "Water (Stars)"),
            description=t(lang, f"{eur}€ (≈ {stars}⭐)", f"{eur}€ (≈ {stars}⭐)"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
            provider_token="",
        )
        return

    if st.get("type") == "zf_wait_code":
        n = parse_zf_bank_code(raw)
        if not n:
            await message.answer(t(lang, "Нужен код вида ZF5 (или ZF 5, ZF-5). Попробуйте ещё раз:", "Need code like ZF5 (or ZF 5, ZF-5). Try again:"))
            return
        st["people"] = n
        st["bank_code"] = f"ZF{n}"
        st["type"] = "zf_wait_label"
        PENDING[uid] = st
        await message.answer(t(lang, "Теперь напишите, как вас показать в списке (коротко):", "Now write how to show you in the list (short):"))
        return

    if st.get("type") == "zf_wait_label":
        label = raw
        if len(label) < 2:
            await message.answer(t(lang, "Слишком коротко. Напишите хотя бы 2 символа:", "Too short. Write at least 2 characters:"))
            return
        if len(label) > 80:
            label = label[:80].rstrip()

        people = int(st["people"])
        bank_code = st["bank_code"]

        await zf_add_entry(uid, message.from_user.username or "-", label, people, bank_code, "manual")
        await zf_post_update()

        exp_eur = people * ZF_EUR_PER_PERSON
        exp_kg = people * ZF_KG_PER_PERSON
        await send_admin(f"ZF marked: {label} | {bank_code} | people {people} | expected {exp_eur} EUR | rice {exp_kg} kg | time {utc_now_str()}")

        PENDING.pop(uid, None)
        await message.answer(t(lang, "✅ Записано. Джазак Аллаху хейр! 🤍", "✅ Saved. Jazak Allahu khayr! 🤍"))
        return

    if st.get("type") == "id_wait_amount":
        try:
            eur = int(raw)
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer(t(lang, "Нужно целое число > 0. Попробуйте ещё раз:", "Please send a whole number > 0. Try again:"))
            return

        await kv_inc_int("id_raised_eur", eur)
        await send_admin(f"Id marked: {eur} EUR | time {utc_now_str()} | user @{message.from_user.username or '-'} / {uid}")

        PENDING.pop(uid, None)
        await message.answer(t(lang, "✅ Спасибо! Джазак Аллаху хейр! 🤍", "✅ Thank you! Jazak Allahu khayr! 🤍"))
        return


# =========================
# Admin callbacks + commands
# =========================

@dp.callback_query(lambda c: c.data.startswith("adm_"))
async def cb_admin(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    if call.data == "adm_help":
        await call.answer()
        txt = (
            "Админ-шпаргалка\n\n"
            "Stars:\n"
            "— /activate_stars\n"
            "— /deactivate_stars\n\n"
            "Редактирование описаний:\n"
            "— /set_desc iftar ru (текст)\n"
            "— /set_desc iftar en (text)\n"
            "— /set_desc water ru (текст)\n"
            "— /set_desc water en (text)\n"
            "— /set_desc zf ru (текст)\n"
            "— /set_desc zf en (text)\n"
            "— /set_desc id ru (текст)\n"
            "— /set_desc id en (text)\n\n"
            "Цели и поступления:\n"
            "— /set_iftar_target 150\n"
            "— /set_iftar_raised 40\n"
            "— /add_iftar 50\n"
            "— /set_water_target 235\n"
            "— /set_water_raised 120\n"
            "— /add_water 20\n"
            "— /add_id 50\n"
            "— /add_zf 5 \"семья Умм Мухаммад\"\n\n"
            "Отчёт:\n"
            "— /report_now\n\n"
            "chat_id:\n"
            "— /chat_id (в группе)\n"
        )
        await call.message.answer(txt)
        return

    if call.data == "adm_activate_stars":
        await kv_set_int("stars_enabled", 1)
        await call.answer("OK")
        await call.message.answer("Stars включены.")
        return

    if call.data == "adm_deactivate_stars":
        await kv_set_int("stars_enabled", 0)
        await call.answer("OK")
        await call.message.answer("Stars выключены.")
        return

    if call.data == "adm_report_now":
        await call.answer("OK")
        report = await build_daily_report()
        if PUBLIC_GROUP_ID:
            await send_to_chat(PUBLIC_GROUP_ID, report)
        await call.message.answer(report)
        return

    if call.data == "adm_show_my_id":
        await call.answer()
        await call.message.answer(f"Ваш user_id: {call.from_user.id}\nPUBLIC_GROUP_ID={PUBLIC_GROUP_ID}\nZF_GROUP_ID={ZF_GROUP_ID}")
        return


@dp.message(Command("activate_stars"))
async def cmd_activate_stars(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await kv_set_int("stars_enabled", 1)
    await message.answer("Stars включены.")


@dp.message(Command("deactivate_stars"))
async def cmd_deactivate_stars(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await kv_set_int("stars_enabled", 0)
    await message.answer("Stars выключены.")


@dp.message(Command("set_desc"))
async def cmd_set_desc(message: Message):
    """
    /set_desc iftar ru <text>
    /set_desc iftar en <text>
    """
    if message.from_user.id != ADMIN_ID:
        return

    m = re.match(r"^/set_desc\s+(iftar|water|zf|id)\s+(ru|en)\s+([\s\S]+)$", (message.text or "").strip())
    if not m:
        await message.answer("Пример: /set_desc iftar ru Новый текст")
        return

    camp = m.group(1)
    lang = m.group(2)
    text = m.group(3).strip()
    await kv_set(f"desc_{camp}_{lang}", text)
    await message.answer("Описание обновлено.")


@dp.message(Command("set_iftar_target"))
async def cmd_set_iftar_target(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_target 150")
        return
    await kv_set_int("iftar_target_portions", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("set_iftar_raised"))
async def cmd_set_iftar_raised(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_raised 40")
        return
    await kv_set_int("iftar_raised_portions", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("add_iftar"))
async def cmd_add_iftar(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_iftar 50")
        return
    await kv_inc_int("iftar_raised_portions", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("set_water_target"))
async def cmd_set_water_target(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_water_target 235")
        return
    await kv_set_int("water_target_eur", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("set_water_raised"))
async def cmd_set_water_raised(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_water_raised 120")
        return
    await kv_set_int("water_raised_eur", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("add_water"))
async def cmd_add_water(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_water 20")
        return
    await kv_inc_int("water_raised_eur", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("add_id"))
async def cmd_add_id(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_id 50")
        return
    await kv_inc_int("id_raised_eur", int(parts[1]))
    await message.answer("OK")


@dp.message(Command("add_zf"))
async def cmd_add_zf(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    m = re.match(r'^/add_zf\s+(\d+)\s+(.+)$', (message.text or "").strip())
    if not m:
        await message.answer('Использование: /add_zf 5 "семья Умм Мухаммад"')
        return

    people = int(m.group(1))
    label = m.group(2).strip().strip('"').strip()
    if people <= 0:
        await message.answer("People must be > 0")
        return

    await zf_add_entry(message.from_user.id, message.from_user.username or "-", label, people, f"ZF{people}", "manual_by_admin")
    await zf_post_update()
    await message.answer("OK (ZF entry added + list updated)")


@dp.message(Command("report_now"))
async def cmd_report_now(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    report = await build_daily_report()
    if PUBLIC_GROUP_ID:
        await send_to_chat(PUBLIC_GROUP_ID, report)
    await message.answer(report)


@dp.message(Command("chat_id"))
async def cmd_chat_id(message: Message):
    # Works in private and in groups. In group you will see group id.
    await message.answer(f"chat_id = {message.chat.id}")


# =========================
# Health server (Render)
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
# Main
# =========================

async def main():
    await db_init()

    # Important: if webhook was set earlier, remove it to allow polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    asyncio.create_task(scheduler_loop())

    await health_server()

    await send_admin("✅ Bot started")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
