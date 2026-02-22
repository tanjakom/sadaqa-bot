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
from aiogram.types import Message, CallbackQuery
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
PORT = int(os.getenv("PORT", "10000") or "10000")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki").strip() or "Europe/Helsinki"
RAMADAN_START = os.getenv("RAMADAN_START", "").strip()  # YYYY-MM-DD (например 2026-02-18)

# Optional: where to post public daily reports and ZF list.
# If not set (0), bot will send reports to ADMIN only.
PUBLIC_GROUP_ID = int(os.getenv("PUBLIC_GROUP_ID", "0") or "0")
ZF_GROUP_ID = int(os.getenv("ZF_GROUP_ID", "0") or "0")

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

# Optional SWIFT (separate method)
SWIFT_RECIPIENT = os.getenv("SWIFT_RECIPIENT", "").strip()
SWIFT_BANK = os.getenv("SWIFT_BANK", "").strip()
SWIFT_ACCOUNT = os.getenv("SWIFT_ACCOUNT", "").strip()
SWIFT_BIC = os.getenv("SWIFT_BIC", "").strip()
SWIFT_BANK_ADDRESS = os.getenv("SWIFT_BANK_ADDRESS", "").strip()

# Optional "Card to card" (you can add later)
CARD_RECIPIENT = os.getenv("CARD_RECIPIENT", "").strip()
CARD_NUMBER = os.getenv("CARD_NUMBER", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing (set it in env)")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# If you attach a Disk in Render, set DB_PATH=/var/data/data.db (recommended)
DB_PATH = os.getenv("DB_PATH", "/var/data/data.db").strip() or "/var/data/data.db"

# Pending state per user
PENDING: dict[int, dict] = {}

# Remember what campaign user is in (to show correct payment code)
LAST_CAMPAIGN: dict[int, str] = {}  # "iftar"|"water"|"zf"|"id"


# =========================
# MARKS / CONSTANTS
# =========================

MARK_IFTAR = "MIMAX"
MARK_WATER = "GREENMAX"
MARK_ID = "Id"     # per your request: "Id"
MARK_ZF_PREFIX = "ZF"  # actual bank mark will be ZF5 / ZF10 etc.

ZF_EUR_PER_PERSON = 9
ZF_KG_PER_PERSON = 3

# Daily report time in local timezone
DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "21") or "21")


# =========================
# TIME HELPERS
# =========================

def tzinfo():
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(TIMEZONE)
    except Exception:
        try:
            return ZoneInfo("UTC")
        except Exception:
            return None


def now_local() -> datetime:
    tz = tzinfo()
    if tz is None:
        return datetime.utcnow()
    return datetime.now(tz)


def today_local() -> date:
    return now_local().date()


def utc_now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")


def get_ramadan_day() -> int:
    """
    Auto-calc Ramadan day from RAMADAN_START (YYYY-MM-DD) in local TIMEZONE.
    Day changes at 00:00 local time.
    """
    if not RAMADAN_START:
        return 1
    try:
        start_date = datetime.strptime(RAMADAN_START, "%Y-%m-%d").date()
        day = (today_local() - start_date).days + 1
        return max(1, day)
    except Exception:
        return 1


# =========================
# TEXT HELPERS
# =========================

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en


def admin_only(user_id: int) -> bool:
    return user_id == ADMIN_ID


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def user_link_html(user_id: int) -> str:
    return f'<a href="tg://user?id={user_id}">Написать</a>'


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None, parse_mode=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


async def send_admin_html(text_html: str):
    await bot.send_message(
        ADMIN_ID,
        text_html,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def send_md(chat_id: int, text_md: str):
    if not chat_id:
        return
    try:
        await bot.send_message(chat_id, text_md, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to send to chat_id=%s", chat_id)


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

        # payment toggles
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('stars_enabled','0')")  # OFF by default
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('manual_enabled','1')")

        # Campaign state (collective)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        # We keep iftar_day in DB for compatibility, but display uses get_ramadan_day()
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day_date', ?)", (today_local().isoformat(),))

        # ZF/ID open/close (can be controlled by date or admin)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_end','2026-03-20')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_end','2026-03-20')")

        # ID internal accounting (optional)
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_raised_eur','0')")

        # Campaign descriptions editable by admin
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_iftar', ?)", (
            "🍲 *Программа ифтаров*\n"
            "Помогаем кормить людей в лагере.\n\n"
            f"Отметка для оплаты: *{MARK_IFTAR}*\n"
            "⚠️ Укажите *ТОЛЬКО* отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_water', ?)", (
            "💧 *Сукья-ль-ма (вода)*\n"
            "Раздача питьевой воды (цистерна 5000л).\n\n"
            f"Отметка для оплаты: *{MARK_WATER}*\n"
            "⚠️ Укажите *ТОЛЬКО* отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_zf', ?)", (
            "🌾 *Закят-уль-Фитр (ZF)*\n"
            f"1 человек = {ZF_KG_PER_PERSON} кг (1 са`а), цена учёта: {ZF_EUR_PER_PERSON}€ / человек.\n\n"
            "Отметка для оплаты: *ZF5 / ZF8* (цифра = кол-во людей)\n"
            "⚠️ Укажите *ТОЛЬКО* отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_id', ?)", (
            "🍬 *Ид аль-Фитр (Id)*\n"
            "Сбор на сладости/выпечку детям в день праздника.\n\n"
            f"Отметка для оплаты: *{MARK_ID}*\n"
            "⚠️ Укажите *ТОЛЬКО* отметку сбора и ничего больше.",
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
    """
    Accepts: ZF5, ZF 5, ZF-5, ZF- 5, zf8
    Returns people count.
    """
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
    if len(label) > 60:
        label = label[:60].rstrip()

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

    lines = ["*Закят-уль-Фитр*"]
    for i, (label, people) in enumerate(rows, start=1):
        lines.append(f"{i}. {label} — *{int(people)} чел.*")

    total_people, total_kg = await zf_totals()
    lines.append("")
    lines.append(f"*Всего: {total_kg} кг риса*")
    return "\n".join(lines)


async def zf_post_update():
    text = await zf_list_text()
    if ZF_GROUP_ID:
        await send_md(ZF_GROUP_ID, text)
    else:
        await bot.send_message(ADMIN_ID, text, parse_mode="Markdown")


# =========================
# SCHEDULE: open/close ZF & ID + daily reports
# =========================

def parse_iso_date(s: str) -> Optional[date]:
    try:
        y, m, d = s.strip().split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


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
            await send_admin_html(f"📅 ZF status: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")

    if id_start and id_end:
        should_open = 1 if (id_start <= today <= id_end) else 0
        cur = await kv_get_int("id_open", 0)
        if cur != should_open:
            await kv_set_int("id_open", should_open)
            await send_admin_html(f"📅 ID status: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")


def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)


async def build_daily_report() -> str:
    water_batch = await kv_get_int("water_batch", 1)
    water_target = await kv_get_int("water_target_eur", 235)
    water_raised = await kv_get_int("water_raised_eur", 0)
    water_rem = max(0, water_target - water_raised)

    # IMPORTANT: Ramadan day auto
    iftar_day = get_ramadan_day()

    iftar_target = await kv_get_int("iftar_target_portions", 100)
    iftar_raised = await kv_get_int("iftar_raised_portions", 0)
    iftar_rem = max(0, iftar_target - iftar_raised)

    zf_people, zf_kg = await zf_totals()
    id_raised = await kv_get_int("id_raised_eur", 0)

    now_str = now_local().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📣 *Ежедневный отчёт* ({now_str} {TIMEZONE})",
        "",
        f"🍲 *Ифтары — день {iftar_day}*",
        f"Собрано: *{iftar_raised}* / *{iftar_target}* порций | Осталось: *{iftar_rem}*",
        battery(iftar_raised, iftar_target),
        "",
        f"💧 *Вода — цистерна #{water_batch}*",
        f"Собрано: *{water_raised}€* / *{water_target}€* | Осталось: *{water_rem}€*",
        battery(water_raised, water_target),
        "",
        f"🌾 *ZF*",
        f"Отмечено: *{zf_kg} кг риса*",
        "",
        f"🍬 *Id*",
        f"Собрано (учёт): *{id_raised}€*",
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
        await send_md(PUBLIC_GROUP_ID, report)
    await bot.send_message(ADMIN_ID, report, parse_mode="Markdown")

    await kv_set("last_daily_report_date", today_str)


async def scheduler_loop():
    while True:
        try:
            await schedule_tick()
            await daily_report_tick()
        except Exception:
            logging.exception("scheduler tick failed")
        await asyncio.sleep(60)


# =========================
# PAYMENT SCREEN TEXTS (copy-friendly)
# =========================

def warn_only_code(code: str) -> str:
    return (
        "⚠️ *Важно:* укажите *ТОЛЬКО* отметку сбора и ничего больше.\n"
        f"Отметка:\n`{code}`"
    )


def payment_text_bank(code: str) -> str:
    bic = f"\nBIC:\n`{SEPA_BIC}`\n" if SEPA_BIC else ""
    return (
        "🏦 *Банковский перевод*\n\n"
        "Получатель:\n"
        f"`{SEPA_RECIPIENT}`\n\n"
        "IBAN:\n"
        f"`{SEPA_IBAN}`\n"
        f"{bic}\n"
        + warn_only_code(code)
    )


def payment_text_paypal(code: str) -> str:
    return (
        "💙 *PayPal*\n\n"
        "Ссылка:\n"
        f"`{PAYPAL_LINK}`\n\n"
        + warn_only_code(code)
    )


def payment_text_zen_express(code: str) -> str:
    parts = ["⚡ *ZEN Express*\n"]
    if ZEN_NAME:
        parts.append("Получатель:\n" + f"`{ZEN_NAME}`\n")
    if ZEN_PHONE:
        parts.append("Телефон:\n" + f"`{ZEN_PHONE}`\n")
    if ZEN_CARD:
        parts.append("Карта:\n" + f"`{ZEN_CARD}`\n")
    parts.append("\n" + warn_only_code(code))
    return "\n".join(parts)


def payment_text_crypto(code: str) -> str:
    usdt = f"USDT (TRC20):\n`{USDT_TRC20}`\n" if USDT_TRC20 else "USDT (TRC20):\n`—`\n"
    usdc = f"USDC (ERC20):\n`{USDC_ERC20}`\n" if USDC_ERC20 else "USDC (ERC20):\n`—`\n"
    return (
        "💎 *Криптовалюта*\n\n"
        f"{usdt}\n{usdc}\n"
        + warn_only_code(code)
        + "\n\nЕсли ваша биржа/кошелёк не поддерживает memo/назначение — просто отправьте оплату, а затем пришлите в бот код (например `ZF5`)."
    )


def payment_text_swift(code: str) -> str:
    parts = ["🌍 *SWIFT*\n"]
    if SWIFT_RECIPIENT:
        parts.append("Получатель:\n" + f"`{SWIFT_RECIPIENT}`\n")
    if SWIFT_BANK:
        parts.append("Банк:\n" + f"`{SWIFT_BANK}`\n")
    if SWIFT_BANK_ADDRESS:
        parts.append("Адрес банка:\n" + f"`{SWIFT_BANK_ADDRESS}`\n")
    if SWIFT_ACCOUNT:
        parts.append("Счёт/IBAN:\n" + f"`{SWIFT_ACCOUNT}`\n")
    if SWIFT_BIC:
        parts.append("BIC/SWIFT:\n" + f"`{SWIFT_BIC}`\n")
    parts.append("\n" + warn_only_code(code))
    return "\n".join(parts)


def payment_text_card_to_card(code: str) -> str:
    parts = ["💳 *С карты на карту*\n"]
    if CARD_RECIPIENT:
        parts.append("Получатель:\n" + f"`{CARD_RECIPIENT}`\n")
    if CARD_NUMBER:
        parts.append("Номер карты:\n" + f"`{CARD_NUMBER}`\n")
    parts.append("\n" + warn_only_code(code))
    if not (CARD_RECIPIENT or CARD_NUMBER):
        parts.append("\n(Реквизиты карты пока не заданы в env. Можно добавить позже.)")
    return "\n".join(parts)


# =========================
# KEYBOARDS
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
    kb.button(text="🍲 Ифтары", callback_data="c_iftar")
    kb.button(text="💧 Вода", callback_data="c_water")
    kb.button(text="🌾 Закят-уль-Фитр (ZF)", callback_data="c_zf")
    kb.button(text="🍬 Ид (Id)", callback_data="c_id")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_campaign_actions(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💳 Способы оплаты", "💳 Payment methods"), callback_data="pay_methods")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_methods(stars_enabled: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏦 Банковский перевод", callback_data="pay_bank")
    kb.button(text="🌍 SWIFT", callback_data="pay_swift")
    kb.button(text="💙 PayPal", callback_data="pay_paypal")
    kb.button(text="⚡ ZEN Express", callback_data="pay_zen")
    kb.button(text="💳 С карты на карту", callback_data="pay_card")
    kb.button(text="💎 Криптовалюта", callback_data="pay_crypto")
    if stars_enabled:
        kb.button(text="⭐ Telegram Stars", callback_data="pay_stars")
    kb.button(text="⬅️ Назад", callback_data="pay_back")
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


def kb_zf_after_payment():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Я оплатил(а) — внести в список ZF", callback_data="zf_mark")
    kb.button(text="⬅️ Назад", callback_data="pay_back")
    kb.adjust(1)
    return kb.as_markup()


def kb_id_after_payment():
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Я оплатил(а) (уведомить)", callback_data="id_mark")
    kb.button(text="⬅️ Назад", callback_data="pay_back")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# /START + LANGUAGE
# =========================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)

    if not lang:
        await message.answer("Ассаляму алейкум 🤍\n\nВыберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    is_admin = admin_only(uid)
    txt = "Ассаляму алейкум 🤍\n\n1) Выберите сбор\n2) Затем выберите способ оплаты"
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
    await safe_edit(call, "Язык установлен.", reply_markup=kb_main(lang, is_admin))


# =========================
# MENUS
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
        await safe_edit(call, "Меню:", reply_markup=kb_main(lang, is_admin))
        return

    if call.data == "list":
        await call.answer()
        await safe_edit(call, "Выберите сбор:", reply_markup=kb_campaigns(lang))
        return

    if call.data == "help":
        await call.answer()
        txt = (
            "❓ *Помощь*\n\n"
            "Логика:\n"
            "1) Выберите сбор\n"
            "2) Нажмите «Способы оплаты»\n"
            "3) Скопируйте реквизиты и укажите *ТОЛЬКО* отметку сбора\n\n"
            "Отметки:\n"
            f"— Ифтары: `{MARK_IFTAR}`\n"
            f"— Вода: `{MARK_WATER}`\n"
            f"— ZF: `ZF5` (цифра = люди)\n"
            f"— Id: `{MARK_ID}`\n"
        )
        await safe_edit(call, txt, reply_markup=kb_main(lang, is_admin), parse_mode="Markdown")
        return

    if call.data == "admin_menu":
        await call.answer()
        if not is_admin:
            await safe_edit(call, "Нет доступа.", reply_markup=kb_main(lang, False))
            return
        await safe_edit(call, "🛠 Админ-меню:", reply_markup=kb_admin_menu())
        return


# =========================
# CAMPAIGNS (description-only)
# =========================

@dp.callback_query(lambda c: c.data.startswith("c_"))
async def cb_campaign(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    key = call.data.replace("c_", "").strip()

    LAST_CAMPAIGN[uid] = key

    if key == "iftar":
        desc = await kv_get("desc_iftar")
    elif key == "water":
        desc = await kv_get("desc_water")
    elif key == "zf":
        if await kv_get_int("zf_open", 0) == 0:
            await call.answer()
            await safe_edit(call, "🔒 ZF сейчас закрыт (вне периода).", reply_markup=kb_campaigns(lang))
            return
        desc = await kv_get("desc_zf")
    elif key == "id":
        if await kv_get_int("id_open", 0) == 0:
            await call.answer()
            await safe_edit(call, "🔒 Id сейчас закрыт (вне периода).", reply_markup=kb_campaigns(lang))
            return
        desc = await kv_get("desc_id")
    else:
        desc = "—"

    await call.answer()
    await safe_edit(call, desc, reply_markup=kb_campaign_actions(lang), parse_mode="Markdown")


@dp.callback_query(lambda c: c.data == "pay_methods")
async def cb_pay_methods(call: CallbackQuery):
    uid = call.from_user.id
    stars_enabled = bool(await kv_get_int("stars_enabled", 0))
    await call.answer()
    await call.message.answer("💳 Выберите способ оплаты:", reply_markup=kb_payment_methods(stars_enabled))


@dp.callback_query(lambda c: c.data in {"pay_back"})
async def cb_pay_back(call: CallbackQuery):
    uid = call.from_user.id
    lang = (await get_user_lang(uid)) or "ru"
    await call.answer()
    await call.message.answer("⬅️ Назад к сбору:", reply_markup=kb_campaign_actions(lang))


# =========================
# PAYMENT METHOD -> show details by campaign
# =========================

def code_for_campaign(uid: int) -> str:
    c = LAST_CAMPAIGN.get(uid, "iftar")
    if c == "iftar":
        return MARK_IFTAR
    if c == "water":
        return MARK_WATER
    if c == "id":
        return MARK_ID
    if c == "zf":
        return "ZF5"
    return "SUPPORT"


@dp.callback_query(lambda c: c.data.startswith("pay_"))
async def cb_pay(call: CallbackQuery):
    uid = call.from_user.id
    method = call.data.replace("pay_", "").strip()
    campaign = LAST_CAMPAIGN.get(uid, "iftar")

    stars_enabled = bool(await kv_get_int("stars_enabled", 0))
    if method == "stars" and not stars_enabled:
        await call.answer("Stars выключены", show_alert=True)
        return

    if campaign == "zf":
        base_text = (
            "🌾 *ZF — Закят-уль-Фитр*\n\n"
            "1) Оплатите\n"
            "2) В назначении укажите *ТОЛЬКО* `ZF5` / `ZF8` (цифра = кол-во людей)\n"
            "3) После оплаты нажмите кнопку ниже и внесите себя в список\n\n"
        )
        code_example = "ZF5"
        if method == "bank":
            txt = base_text + payment_text_bank(code_example)
        elif method == "swift":
            txt = base_text + payment_text_swift(code_example)
        elif method == "paypal":
            txt = base_text + payment_text_paypal(code_example)
        elif method == "zen":
            txt = base_text + payment_text_zen_express(code_example)
        elif method == "card":
            txt = base_text + payment_text_card_to_card(code_example)
        elif method == "crypto":
            txt = base_text + payment_text_crypto(code_example)
        else:
            txt = base_text + "_Stars сейчас не используем для срочных сборов._"

        await call.answer()
        await call.message.answer(txt, parse_mode="Markdown", reply_markup=kb_zf_after_payment())
        return

    if campaign == "id":
        code = MARK_ID
        base_text = "🍬 *Id — Ид аль-Фитр*\n\nОплатите и укажите *ТОЛЬКО* отметку.\n\n"
        if method == "bank":
            txt = base_text + payment_text_bank(code)
        elif method == "swift":
            txt = base_text + payment_text_swift(code)
        elif method == "paypal":
            txt = base_text + payment_text_paypal(code)
        elif method == "zen":
            txt = base_text + payment_text_zen_express(code)
        elif method == "card":
            txt = base_text + payment_text_card_to_card(code)
        elif method == "crypto":
            txt = base_text + payment_text_crypto(code)
        else:
            txt = base_text + "_Stars: можно включить позже командой /activate_stars._"

        await call.answer()
        await call.message.answer(txt, parse_mode="Markdown", reply_markup=kb_id_after_payment())
        return

    code = code_for_campaign(uid)
    title = "🍲 *Ифтары*" if campaign == "iftar" else "💧 *Вода*"
    base_text = f"{title}\n\nОплатите и укажите *ТОЛЬКО* отметку.\n\n"
    if method == "bank":
        txt = base_text + payment_text_bank(code)
    elif method == "swift":
        txt = base_text + payment_text_swift(code)
    elif method == "paypal":
        txt = base_text + payment_text_paypal(code)
    elif method == "zen":
        txt = base_text + payment_text_zen_express(code)
    elif method == "card":
        txt = base_text + payment_text_card_to_card(code)
    elif method == "crypto":
        txt = base_text + payment_text_crypto(code)
    else:
        txt = base_text + "_Stars: можно включить позже командой /activate_stars._"

    await call.answer()
    await call.message.answer(txt, parse_mode="Markdown")


# =========================
# ZF: after payment -> user provides ZF code + label -> post list
# =========================

@dp.callback_query(lambda c: c.data == "zf_mark")
async def cb_zf_mark(call: CallbackQuery):
    uid = call.from_user.id
    PENDING[uid] = {"type": "zf_wait_code"}
    await call.answer()
    await call.message.answer("Пришлите код, который вы указали при оплате (пример: `ZF5`).", parse_mode="Markdown")


@dp.callback_query(lambda c: c.data == "id_mark")
async def cb_id_mark(call: CallbackQuery):
    uid = call.from_user.id
    PENDING[uid] = {"type": "id_wait_amount"}
    await call.answer()
    await call.message.answer("Введите сумму в евро (целое число), чтобы мы могли учесть (пример: 20):")


@dp.message()
async def pending_router(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    st = PENDING.get(uid)
    if not st:
        return
    raw = (message.text or "").strip()

    if st.get("type") == "zf_wait_code":
        n = parse_zf_bank_code(raw)
        if not n:
            await message.answer("Нужен код вида `ZF5` (или `ZF 5`, `ZF-5`). Попробуйте ещё раз:", parse_mode="Markdown")
            return
        st["people"] = n
        st["bank_code"] = f"ZF{n}"
        st["type"] = "zf_wait_label"
        PENDING[uid] = st
        await message.answer("Теперь напишите, как вас показать в списке (коротко):")
        return

    if st.get("type") == "zf_wait_label":
        label = raw
        if len(label) < 2:
            await message.answer("Слишком коротко. Напишите хотя бы 2 символа:")
            return
        if len(label) > 60:
            label = label[:60].rstrip()

        people = int(st["people"])
        bank_code = st["bank_code"]
        method = "manual"

        await zf_add_entry(uid, message.from_user.username or "-", label, people, bank_code, method)

        eur = people * ZF_EUR_PER_PERSON
        kg = people * ZF_KG_PER_PERSON
        await send_admin_html(
            "\n".join([
                "✅ ZF MARKED",
                f"Label: <b>{html_escape(label)}</b>",
                f"Bank code: <code>{html_escape(bank_code)}</code>",
                f"People: {people} | expected {eur}€ | rice {kg} kg",
                f"Time: {utc_now_str()}",
                f"User: @{html_escape(message.from_user.username or '-') } / {uid}",
                user_link_html(uid),
            ])
        )

        await zf_post_update()

        PENDING.pop(uid, None)
        await message.answer("✅ Записано. Джазак Аллаху хейр! 🤍")
        return

    if st.get("type") == "id_wait_amount":
        try:
            eur = int(raw)
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return

        await kv_inc_int("id_raised_eur", eur)
        await send_admin_html(
            "\n".join([
                "✅ ID MARKED",
                f"Amount: {eur} EUR",
                f"Time: {utc_now_str()}",
                f"User: @{html_escape(message.from_user.username or '-') } / {uid}",
                user_link_html(uid),
            ])
        )

        PENDING.pop(uid, None)
        await message.answer("✅ Спасибо! Джазак Аллаху хейр! 🤍")
        return


# =========================
# ADMIN: commands + in-bot cheat sheet
# =========================

@dp.callback_query(lambda c: c.data.startswith("adm_"))
async def cb_admin(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Нет доступа", show_alert=True)
        return

    if call.data == "adm_help":
        await call.answer()
        txt = (
            "🛠 *Админ-шпаргалка*\n\n"
            "*Stars*\n"
            "— /activate_stars\n"
            "— /deactivate_stars\n\n"
            "*Редактирование описаний*\n"
            "— /set_desc iftar <текст>\n"
            "— /set_desc water <текст>\n"
            "— /set_desc zf <текст>\n"
            "— /set_desc id <текст>\n\n"
            "*Ручные начисления (вне бота)*\n"
            "— /add_iftar 15\n"
            "— /add_water 20\n"
            "— /add_id 50\n"
            '— /add_zf 5 "семья Умм Мухаммад"\n\n'
            "*Отчёт*\n"
            "— /report_now\n\n"
            "*Узнать chat_id группы*\n"
            "— добавьте бота в группу и напишите /chat_id\n"
        )
        await call.message.answer(txt, parse_mode="HTML")
        return

    if call.data == "adm_activate_stars":
        await kv_set_int("stars_enabled", 1)
        await call.answer("OK")
        await call.message.answer("⭐ Stars включены (появятся в способах оплаты).")
        return

    if call.data == "adm_deactivate_stars":
        await kv_set_int("stars_enabled", 0)
        await call.answer("OK")
        await call.message.answer("⭐ Stars выключены.")
        return

    if call.data == "adm_report_now":
        await call.answer("OK")
        report = await build_daily_report()
        if PUBLIC_GROUP_ID:
            await send_md(PUBLIC_GROUP_ID, report)
        await call.message.answer(report, parse_mode="Markdown")
        return

    if call.data == "adm_show_my_id":
        await call.answer()
        await call.message.answer(f"Ваш user_id: {call.from_user.id}\nPUBLIC_GROUP_ID: {PUBLIC_GROUP_ID}\nZF_GROUP_ID: {ZF_GROUP_ID}")
        return


@dp.message(Command("activate_stars"))
async def cmd_activate_stars(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await kv_set_int("stars_enabled", 1)
    await message.answer("⭐ Stars включены.")


@dp.message(Command("deactivate_stars"))
async def cmd_deactivate_stars(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await kv_set_int("stars_enabled", 0)
    await message.answer("⭐ Stars выключены.")


@dp.message(Command("set_desc"))
async def cmd_set_desc(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    m = re.match(r"^/set_desc\s+(iftar|water|zf|id)\s+([\s\S]+)$", (message.text or "").strip())
    if not m:
        await message.answer("Использование: /set_desc iftar <текст>")
        return
    key = m.group(1)
    text = m.group(2).strip()
    await kv_set(f"desc_{key}", text)
    await message.answer("✅ Описание обновлено.")


@dp.message(Command("report_now"))
async def cmd_report_now(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    report = await build_daily_report()
    if PUBLIC_GROUP_ID:
        await send_md(PUBLIC_GROUP_ID, report)
    await message.answer(report, parse_mode="Markdown")


@dp.message(Command("add_iftar"))
async def cmd_add_iftar(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_iftar 15")
        return
    await kv_inc_int("iftar_raised_portions", int(parts[1]))
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
    m = re.match(r"^/add_zf\s+(\d+)\s+(.+)$", (message.text or "").strip())
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


@dp.message(Command("chat_id"))
async def cmd_chat_id(message: Message):
    await message.answer(f"chat_id = {message.chat.id}")


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

    asyncio.create_task(scheduler_loop())

    try:
        await bot.send_message(ADMIN_ID, "✅ Bot started", disable_web_page_preview=True)
    except Exception:
        logging.exception("Failed to notify admin on startup")

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
