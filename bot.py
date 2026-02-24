import os
import re
import html
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
PORT = int(os.getenv("PORT", "10000") or "10000")

TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki").strip() or "Europe/Helsinki"
RAMADAN_START = os.getenv("RAMADAN_START", "2026-02-18").strip()  # YYYY-MM-DD

PUBLIC_GROUP_ID = int(os.getenv("PUBLIC_GROUP_ID", "0") or "0")
ZF_GROUP_ID = int(os.getenv("ZF_GROUP_ID", "0") or "0")

DB_PATH = os.getenv("DB_PATH", "/var/data/data.db").strip() or "/var/data/data.db"

MARK_IFTAR = os.getenv("MARK_IFTAR", "MIMAX").strip() or "MIMAX"
MARK_WATER = os.getenv("MARK_WATER", "GREENMAX").strip() or "GREENMAX"
MARK_ID = os.getenv("MARK_ID", "Id").strip() or "Id"
MARK_ZF_PREFIX = os.getenv("MARK_ZF_PREFIX", "ZF").strip() or "ZF"

DEFAULT_EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")
DEFAULT_IFTAR_PORTION_EUR = float(os.getenv("IFTAR_PORTION_EUR", "4") or "4")
DEFAULT_ZF_EUR_PER_PERSON = float(os.getenv("ZF_EUR_PER_PERSON", "9") or "9")
DEFAULT_ZF_KG_PER_PERSON = int(os.getenv("ZF_KG_PER_PERSON", "3") or "3")

DEFAULT_WATER_TARGET_EUR = int(os.getenv("WATER_TARGET_EUR", "235") or "235")
DEFAULT_IFTAR_TARGET_PORTIONS = int(os.getenv("IFTAR_TARGET_PORTIONS", "100") or "100")

# Individual campaigns (your requirements)
IND_IFTAR_FULL_PORTIONS = int(os.getenv("IND_IFTAR_FULL_PORTIONS", "150") or "150")  # fully closes a day (with video)
IND_WATER_FULL_EUR = int(os.getenv("IND_WATER_FULL_EUR", "235") or "235")

PAYPAL_LINK = os.getenv("PAYPAL_LINK", "").strip()

SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "").strip()
SEPA_IBAN = os.getenv("SEPA_IBAN", "").strip()
SEPA_BIC = os.getenv("SEPA_BIC", "").strip()

ZEN_NAME = os.getenv("ZEN_NAME", "").strip()
ZEN_PHONE = os.getenv("ZEN_PHONE", "").strip()
ZEN_CARD = os.getenv("ZEN_CARD", "").strip()

USDT_TRC20 = os.getenv("USDT_TRC20", "").strip()
USDC_ERC20 = os.getenv("USDC_ERC20", "").strip()

SWIFT_RECIPIENT = os.getenv("SWIFT_RECIPIENT", "").strip()
SWIFT_BANK = os.getenv("SWIFT_BANK", "").strip()
SWIFT_ACCOUNT = os.getenv("SWIFT_ACCOUNT", "").strip()
SWIFT_BIC = os.getenv("SWIFT_BIC", "").strip()
SWIFT_BANK_ADDRESS = os.getenv("SWIFT_BANK_ADDRESS", "").strip()

CARD_RECIPIENT = os.getenv("CARD_RECIPIENT", "").strip()
CARD_NUMBER = os.getenv("CARD_NUMBER", "").strip()

TR_RECIPIENT = os.getenv("TR_RECIPIENT", "").strip()
TR_BIC = os.getenv("TR_BIC", "").strip()
TR_IBAN_EUR = os.getenv("TR_IBAN_EUR", "").strip()
TR_IBAN_TL = os.getenv("TR_IBAN_TL", "").strip()
TR_IBAN_USD = os.getenv("TR_IBAN_UD", os.getenv("TR_IBAN_USD", "")).strip()

DAILY_REPORT_HOUR = int(os.getenv("DAILY_REPORT_HOUR", "21") or "21")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

PENDING: dict[int, dict] = {}
LAST_CAMPAIGN: dict[int, str] = {}  # iftar|water|zf|id|ind_iftar|ind_water


# =========================
# TIME HELPERS
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


def parse_iso_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def get_ramadan_day() -> int:
    start = parse_iso_date(RAMADAN_START) or today_local()
    d = (today_local() - start).days + 1
    return max(1, d)


def iftar_campaign_day() -> int:
    d = get_ramadan_day() + 1  # always for tomorrow
    return 30 if d > 30 else d


# =========================
# TEXT HELPERS
# =========================

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def h(s: str) -> str:
    return html.escape(s or "")


def user_link_html(uid: int) -> str:
    return f'<a href="tg://user?id={uid}">Написать</a>'


async def safe_edit(call: CallbackQuery, text: str, reply_markup=None, parse_mode: Optional[str] = None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)


async def send_admin_html(text_html: str):
    await bot.send_message(ADMIN_ID, text_html, parse_mode="HTML", disable_web_page_preview=True)


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
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

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

        # Iftar by day
        await db.execute("""
        CREATE TABLE IF NOT EXISTS iftar_days (
            day INTEGER PRIMARY KEY,
            target_portions INTEGER NOT NULL,
            raised_portions INTEGER NOT NULL,
            is_closed INTEGER NOT NULL DEFAULT 0,
            last_update_utc TEXT NOT NULL
        )
        """)

        # ZF list
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

        # Individual campaigns (iftar/water) requests
        await db.execute("""
        CREATE TABLE IF NOT EXISTS individual_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc TEXT NOT NULL,
            kind TEXT NOT NULL,               -- ind_iftar | ind_water
            reserved_day INTEGER,             -- for ind_iftar
            reserved_batch INTEGER,           -- for ind_water
            amount_eur REAL NOT NULL,
            amount_portions INTEGER NOT NULL,
            label_for_video TEXT,
            note_for_print TEXT,
            user_id INTEGER NOT NULL,
            username TEXT,
            method TEXT NOT NULL
        )
        """)

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('stars_enabled','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eur_to_stars', ?)", (str(DEFAULT_EUR_TO_STARS),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_portion_eur', ?)", (str(DEFAULT_IFTAR_PORTION_EUR),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_eur_per_person', ?)", (str(DEFAULT_ZF_EUR_PER_PERSON),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_kg_per_person', ?)", (str(DEFAULT_ZF_KG_PER_PERSON),))

        # Water collective
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_batch','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur', ?)", (str(DEFAULT_WATER_TARGET_EUR),))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        # Id accounting
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_raised_eur','0')")

        # ZF/ID windows
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_open','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('zf_end','2026-03-20')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_start','2026-03-10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('id_end','2026-03-20')")

        # Descriptions
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_iftar', ?)", (
            "🍲 Программа ифтаров\n"
            "Десятый Рамадан подряд мы кормим семьи бедняков в палаточном лагере.\n\n"
            f"Отметка для оплаты: {MARK_IFTAR}\n"
            "Важно: укажите ТОЛЬКО отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_water', ?)", (
            "💧 Сукья-ль-ма (вода)\n"
            "Раздача питьевой воды (цистерна 5000л).\n\n"
            f"Отметка для оплаты: {MARK_WATER}\n"
            "Важно: укажите ТОЛЬКО отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_zf', ?)", (
            "🌾 Закят-уль-Фитр (ZF)\n"
            "Оплата фард-обязанности. Цифра в коде = сколько людей.\n\n"
            "Отметка для оплаты: ZF5 / ZF8\n"
            "Важно: укажите ТОЛЬКО отметку сбора и ничего больше.",
        ))
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('desc_id', ?)", (
            "🍬 Ид аль-Фитр (Id)\n"
            "Праздничный сбор на сладости/выпечку детям.\n\n"
            f"Отметка для оплаты: {MARK_ID}\n"
            "Важно: укажите ТОЛЬКО отметку сбора и ничего больше.",
        ))

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('last_daily_report_date','')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('last_rollover_date','')")

        # Next available for individual
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('ind_iftar_next_day','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('ind_water_next_batch','1')")

        await db.commit()

    await ensure_iftar_day(iftar_campaign_day())


async def kv_get(key: str) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT v FROM kv WHERE k=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""


async def kv_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )
        await db.commit()


async def kv_get_int(key: str, default: int = 0) -> int:
    s = (await kv_get(key) or "").strip()
    try:
        return int(float(s))
    except Exception:
        return default


async def kv_get_float(key: str, default: float = 0.0) -> float:
    s = (await kv_get(key) or "").strip()
    try:
        return float(s)
    except Exception:
        return default


async def kv_set_int(key: str, value: int):
    await kv_set(key, str(int(value)))


async def kv_set_float(key: str, value: float):
    await kv_set(key, str(float(value)))


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
# IFTAR per-day accounting + instant close/rollover
# =========================

async def ensure_iftar_day(day: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT day FROM iftar_days WHERE day=?", (day,)) as cur:
            row = await cur.fetchone()
        if row:
            return
        await db.execute(
            "INSERT INTO iftar_days(day, target_portions, raised_portions, is_closed, last_update_utc) "
            "VALUES(?,?,?,?,?)",
            (day, DEFAULT_IFTAR_TARGET_PORTIONS, 0, 0, utc_now_str()),
        )
        await db.commit()


async def iftar_get(day: int) -> Tuple[int, int, int]:
    await ensure_iftar_day(day)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT target_portions, raised_portions, is_closed FROM iftar_days WHERE day=?", (day,)) as cur:
            row = await cur.fetchone()
            if not row:
                return DEFAULT_IFTAR_TARGET_PORTIONS, 0, 0
            return int(row[0]), int(row[1]), int(row[2])


async def iftar_set_target(day: int, target: int):
    await ensure_iftar_day(day)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE iftar_days SET target_portions=?, last_update_utc=? WHERE day=?",
            (int(target), utc_now_str(), day),
        )
        await db.commit()


async def iftar_add_with_autoclose(day: int, portions: int) -> Tuple[int, int, int]:
    """
    Adds portions to day.
    If reaches/exceeds target -> mark day closed, carry overflow into next day (day+1),
    and ensures next day exists/open. Returns (final_day, added_to_final_day, overflow_used)
    (mostly for logging; UI doesn't depend on it).
    """
    await ensure_iftar_day(day)

    target, raised, closed = await iftar_get(day)
    new_raised = raised + int(portions)

    # write new_raised first
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE iftar_days SET raised_portions=?, last_update_utc=? WHERE day=?",
            (int(new_raised), utc_now_str(), day),
        )
        await db.commit()

    # autoclose loop if overflows
    cur_day = day
    overflow = 0

    while True:
        target, raised, closed = await iftar_get(cur_day)
        if raised < target:
            break

        overflow = raised - target

        # close this day
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE iftar_days SET raised_portions=?, is_closed=1, last_update_utc=? WHERE day=?",
                (int(target), utc_now_str(), cur_day),
            )
            await db.commit()

        # move overflow to next day (immediately)
        next_day = min(30, cur_day + 1)
        await ensure_iftar_day(next_day)

        # if next day already has something, add overflow
        nt, nr, nc = await iftar_get(next_day)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE iftar_days SET raised_portions=?, is_closed=0, last_update_utc=? WHERE day=?",
                (int(nr + overflow), utc_now_str(), next_day),
            )
            await db.commit()

        # notify admin once per closure
        try:
            await send_admin_html(
                "✅ Ифтары закрыты автоматически<br>"
                f"День {cur_day} закрыт: {target}/{target} порций.<br>"
                + (f"Перенос излишка: {overflow} порций → день {next_day}." if overflow > 0 else "")
            )
        except Exception:
            pass

        cur_day = next_day
        if cur_day >= 30:
            break
        if overflow <= 0:
            break

    return cur_day, int(portions), overflow


async def rollover_iftar_if_needed():
    """
    At date change: if today's date differs from last_rollover_date, do:
    prev_day = today's ramadan day (we were collecting for it yesterday)
    new_day = tomorrow's ramadan day (we collect for it now)
    If prev_day is not closed and has raised > 0 -> move raised to new_day.
    """
    today_str = today_local().isoformat()
    last = (await kv_get("last_rollover_date") or "").strip()
    if last == today_str:
        return

    prev_day = get_ramadan_day()
    new_day = iftar_campaign_day()

    await ensure_iftar_day(prev_day)
    await ensure_iftar_day(new_day)

    prev_target, prev_raised, prev_closed = await iftar_get(prev_day)
    if prev_closed == 0 and prev_raised > 0:
        # move all to new day
        nt, nr, nc = await iftar_get(new_day)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE iftar_days SET raised_portions=0, last_update_utc=? WHERE day=?", (utc_now_str(), prev_day))
            await db.execute("UPDATE iftar_days SET raised_portions=?, last_update_utc=? WHERE day=?", (int(nr + prev_raised), utc_now_str(), new_day))
            await db.commit()

        await send_admin_html(
            "🔁 Перенос ифтаров (00:00)<br>"
            f"День {prev_day} не закрыт — перенесли {prev_raised} порций на день {new_day}."
        )

    await kv_set("last_rollover_date", today_str)


# =========================
# WATER collective + instant close/rollover
# =========================

async def water_add_with_autoclose(eur: int) -> Tuple[int, int]:
    """
    Adds EUR to current water batch.
    If reaches/exceeds target -> closes batch, increments batch, carries overflow to next batch.
    Returns (current_batch_after, overflow_left).
    """
    batch = await kv_get_int("water_batch", 1)
    target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
    raised = await kv_get_int("water_raised_eur", 0)

    raised += int(eur)
    overflow = 0

    while raised >= target and target > 0:
        overflow = raised - target

        # close current batch => move to next
        try:
            await send_admin_html(
                "✅ Вода закрыта автоматически<br>"
                f"Цистерна #{batch} закрыта: {target}/{target}€.<br>"
                + (f"Перенос излишка: {overflow}€ → цистерна #{batch+1}." if overflow > 0 else "")
            )
        except Exception:
            pass

        batch += 1
        raised = overflow
        overflow = 0

    await kv_set_int("water_batch", batch)
    await kv_set_int("water_raised_eur", raised)

    return batch, raised


# =========================
# ZF
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
    kg_per = int(await kv_get_float("zf_kg_per_person", DEFAULT_ZF_KG_PER_PERSON))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(SUM(people),0) FROM zf_entries") as cur:
            row = await cur.fetchone()
            total_people = int(row[0] or 0)
    return total_people, total_people * kg_per


async def zf_list_text_md() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT label, people FROM zf_entries ORDER BY id ASC") as cur:
            rows = await cur.fetchall()

    lines = ["*Закят-уль-Фитр*"]
    for i, (label, people) in enumerate(rows, start=1):
        lines.append(f"{i}. {label} — *{int(people)} чел.*")

    _, total_kg = await zf_totals()
    lines.append("")
    lines.append(f"*Всего: {total_kg} кг риса*")
    return "\n".join(lines)


async def zf_post_update():
    text = await zf_list_text_md()
    if ZF_GROUP_ID:
        await send_md(ZF_GROUP_ID, text)
    else:
        await bot.send_message(ADMIN_ID, text, parse_mode="Markdown", disable_web_page_preview=True)


# =========================
# Individual scheduling
# =========================

async def ind_iftar_next_day() -> int:
    cur = await kv_get_int("ind_iftar_next_day", 0)
    if cur <= 0:
        cur = iftar_campaign_day()
        await kv_set_int("ind_iftar_next_day", cur)
    return cur


async def ind_iftar_reserve_day() -> int:
    d = await ind_iftar_next_day()
    await kv_set_int("ind_iftar_next_day", min(30, d + 1))
    return d


async def ind_water_next_batch() -> int:
    cur = await kv_get_int("ind_water_next_batch", 1)
    if cur <= 0:
        cur = 1
        await kv_set_int("ind_water_next_batch", cur)
    return cur


async def ind_water_reserve_batch() -> int:
    b = await ind_water_next_batch()
    await kv_set_int("ind_water_next_batch", b + 1)
    return b


async def create_individual_order(kind: str, user_id: int, username: str, method: str,
                                  reserved_day: int, reserved_batch: int,
                                  amount_eur: float, amount_portions: int,
                                  label_for_video: str, note_for_print: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO individual_orders(
                created_utc, kind, reserved_day, reserved_batch,
                amount_eur, amount_portions,
                label_for_video, note_for_print,
                user_id, username, method
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now_str(),
                kind,
                int(reserved_day) if reserved_day else None,
                int(reserved_batch) if reserved_batch else None,
                float(amount_eur),
                int(amount_portions),
                (label_for_video or "").strip()[:300],
                (note_for_print or "").strip()[:300],
                int(user_id),
                username or "-",
                method or "manual",
            )
        )
        await db.commit()


# =========================
# SCHEDULE: open/close ZF & ID + daily report + rollover
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
            await send_admin_html(f"📅 ZF: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")

    if id_start and id_end:
        should_open = 1 if (id_start <= today <= id_end) else 0
        cur = await kv_get_int("id_open", 0)
        if cur != should_open:
            await kv_set_int("id_open", should_open)
            await send_admin_html(f"📅 Id: {'OPEN' if should_open else 'CLOSED'} ({today.isoformat()})")


def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)


async def build_daily_report_md() -> str:
    day = iftar_campaign_day()
    target, raised, closed = await iftar_get(day)
    rem = max(0, target - raised)

    water_batch = await kv_get_int("water_batch", 1)
    water_target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
    water_raised = await kv_get_int("water_raised_eur", 0)
    water_rem = max(0, water_target - water_raised)

    _, zf_kg = await zf_totals()
    id_raised = await kv_get_int("id_raised_eur", 0)

    now_str = now_local().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📣 *Ежедневный отчёт* ({now_str} {TIMEZONE})",
        "",
        f"🍲 *Ифтары — собираем на день {day}*",
        f"Собрано: *{raised}* / *{target}* порций | Осталось: *{rem}*",
        battery(raised, target),
        "",
        f"💧 *Вода — цистерна #{water_batch}*",
        f"Собрано: *{water_raised}€* / *{water_target}€* | Осталось: *{water_rem}€*",
        battery(water_raised, water_target),
        "",
        f"🌾 *ZF*: *{zf_kg} кг риса*",
        f"🍬 *Id*: учёт *{id_raised}€*",
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

    report = await build_daily_report_md()
    if PUBLIC_GROUP_ID:
        await send_md(PUBLIC_GROUP_ID, report)
    await bot.send_message(ADMIN_ID, report, parse_mode="Markdown", disable_web_page_preview=True)

    await kv_set("last_daily_report_date", today_str)


async def scheduler_loop():
    while True:
        try:
            await rollover_iftar_if_needed()
            await schedule_tick()
            await daily_report_tick()
            await ensure_iftar_day(iftar_campaign_day())
        except Exception:
            logging.exception("scheduler tick failed")
        await asyncio.sleep(60)


# =========================
# PAYMENT TEXTS (plain text)
# =========================

def warn_only_code(code: str) -> str:
    return (
        "ВАЖНО: укажите ТОЛЬКО отметку сбора и ничего больше.\n"
        f"Отметка: {code}"
    )


def payment_text_bank(code: str) -> str:
    bic = f"\nBIC: {SEPA_BIC}\n" if SEPA_BIC else "\n"
    return (
        "🏦 Банковский перевод (SEPA)\n\n"
        f"Получатель: {SEPA_RECIPIENT}\n"
        f"IBAN: {SEPA_IBAN}\n"
        f"{bic}\n"
        + warn_only_code(code)
    )


def payment_text_turkey(code: str) -> str:
    lines = ["🇹🇷 Турецкий банк\n"]
    if TR_RECIPIENT:
        lines.append(f"Получатель: {TR_RECIPIENT}")
    if TR_BIC:
        lines.append(f"BIC: {TR_BIC}")
    if TR_IBAN_EUR:
        lines.append(f"IBAN EUR: {TR_IBAN_EUR}")
    if TR_IBAN_TL:
        lines.append(f"IBAN TL: {TR_IBAN_TL}")
    if TR_IBAN_USD:
        lines.append(f"IBAN USD: {TR_IBAN_USD}")
    if len(lines) == 1:
        lines.append("(Реквизиты не заданы в env)")
    lines.append("")
    lines.append(warn_only_code(code))
    return "\n".join(lines)


def payment_text_paypal(code: str) -> str:
    return (
        "💙 PayPal\n\n"
        f"Ссылка: {PAYPAL_LINK}\n\n"
        + warn_only_code(code)
    )


def payment_text_zen(code: str) -> str:
    parts = ["⚡ ZEN Express\n"]
    if ZEN_NAME:
        parts.append(f"Получатель: {ZEN_NAME}")
    if ZEN_PHONE:
        parts.append(f"Телефон: {ZEN_PHONE}")
    if ZEN_CARD:
        parts.append(f"Карта: {ZEN_CARD}")
    if len(parts) == 1:
        parts.append("(Реквизиты не заданы в env)")
    parts.append("")
    parts.append(warn_only_code(code))
    return "\n".join(parts)


def payment_text_crypto(code: str) -> str:
    usdt = f"USDT (TRC20): {USDT_TRC20}" if USDT_TRC20 else "USDT (TRC20): —"
    usdc = f"USDC (ERC20): {USDC_ERC20}" if USDC_ERC20 else "USDC (ERC20): —"
    return (
        "💎 Криптовалюта\n\n"
        f"{usdt}\n{usdc}\n\n"
        + warn_only_code(code)
        + "\n\nЕсли назначение (memo) невозможно — просто отправьте оплату, затем пришлите в бот код (например ZF5)."
    )


def payment_text_swift(code: str) -> str:
    parts = ["🌍 SWIFT\n"]
    if SWIFT_RECIPIENT:
        parts.append(f"Получатель: {SWIFT_RECIPIENT}")
    if SWIFT_BANK:
        parts.append(f"Банк: {SWIFT_BANK}")
    if SWIFT_BANK_ADDRESS:
        parts.append(f"Адрес банка: {SWIFT_BANK_ADDRESS}")
    if SWIFT_ACCOUNT:
        parts.append(f"Счёт/IBAN: {SWIFT_ACCOUNT}")
    if SWIFT_BIC:
        parts.append(f"BIC/SWIFT: {SWIFT_BIC}")
    if len(parts) == 1:
        parts.append("(SWIFT реквизиты не заданы)")
    parts.append("")
    parts.append(warn_only_code(code))
    return "\n".join(parts)


def payment_text_card(code: str) -> str:
    parts = ["💳 С карты на карту\n"]
    if CARD_RECIPIENT:
        parts.append(f"Получатель: {CARD_RECIPIENT}")
    if CARD_NUMBER:
        parts.append(f"Номер карты: {CARD_NUMBER}")
    if len(parts) == 1:
        parts.append("(Реквизиты карты не заданы)")
    parts.append("")
    parts.append(warn_only_code(code))
    return "\n".join(parts)


# =========================
# STARS helpers
# =========================

async def get_rate() -> int:
    return int(await kv_get_float("eur_to_stars", DEFAULT_EUR_TO_STARS))


def eur_to_stars_amount(eur: float, rate: int) -> int:
    return int(round(float(eur) * rate))


async def send_stars_invoice(chat_id: int, title: str, description: str, payload: str, eur_amount: float):
    rate = await get_rate()
    stars = eur_to_stars_amount(eur_amount, rate)
    desc = f"{description}\n\n≈ {stars}⭐ (1€ = {rate}⭐)"
    await bot.send_invoice(
        chat_id=chat_id,
        title=title,
        description=desc,
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur_amount:.2f} EUR", amount=stars)],
        provider_token="",  # Stars
    )


# =========================
# KEYBOARDS
# =========================

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()


def kb_main(is_admin_user: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Сборы", callback_data="list")
    kb.button(text="❓ Помощь", callback_data="help")
    kb.button(text="🌐 Язык", callback_data="lang_menu")
    if is_admin_user:
        kb.button(text="🛠 Админ", callback_data="admin_menu")
    kb.adjust(1)
    return kb.as_markup()


def kb_campaigns():
    kb = InlineKeyboardBuilder()
    kb.button(text="🍲 Ифтары (коллектив)", callback_data="c_iftar")
    kb.button(text="🎥 Индивидуальная раздача ифтаров", callback_data="c_ind_iftar")
    kb.button(text="💧 Вода (коллектив)", callback_data="c_water")
    kb.button(text="🎥 Индивидуальная раздача воды", callback_data="c_ind_water")
    kb.button(text="🌾 Закят-уль-Фитр (ZF)", callback_data="c_zf")
    kb.button(text="🍬 Ид (Id)", callback_data="c_id")
    kb.button(text="⬅️ Назад", callback_data="back")
    kb.adjust(1)
    return kb.as_markup()


def kb_campaign_actions(kind: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Способы оплаты", callback_data="pay_methods")

    if kind == "iftar":
        kb.button(text="✅ Оплатить остаток (закрыть день)", callback_data="quick_close")
        kb.button(text="💯 Оплатить 1 день полностью", callback_data="quick_full")
        kb.button(text="✅ Я оплатил(а) — учесть", callback_data="mark_paid")

    if kind == "water":
        kb.button(text="✅ Оплатить остаток (закрыть цистерну)", callback_data="quick_close")
        kb.button(text="🚰 Оплатить цистерну целиком", callback_data="quick_full")
        kb.button(text="✅ Я оплатил(а) — учесть", callback_data="mark_paid")

    if kind == "id":
        kb.button(text="✅ Я оплатил(а) — учесть", callback_data="mark_paid")

    kb.button(text="⬅️ Назад", callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_individual_actions():
    kb = InlineKeyboardBuilder()
    kb.button(text="💳 Способы оплаты", callback_data="pay_methods")
    kb.button(text="✅ Я оплатил(а) — оформить индивидуальную раздачу", callback_data="ind_mark_paid")
    kb.button(text="⬅️ Назад", callback_data="list")
    kb.adjust(1)
    return kb.as_markup()


def kb_payment_methods(stars_enabled: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text="🏦 Банковский перевод", callback_data="pay_bank")
    kb.button(text="🇹🇷 Турецкий банк", callback_data="pay_tr")
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


def kb_stars_quick(kind: str):
    # kind: iftar|water
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Оплатить остаток", callback_data=f"stars_quick:close:{kind}")
    kb.button(text="💯 Оплатить полностью", callback_data=f"stars_quick:full:{kind}")
    kb.button(text="✍️ Другое", callback_data=f"stars_quick:other:{kind}")
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


def kb_admin_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📌 Шпаргалка команд", callback_data="adm_help")
    kb.button(text="✏️ Изменить текст сборов", callback_data="adm_edit_desc")
    kb.button(text="💰 Изменить цены/курс", callback_data="adm_edit_prices")
    kb.button(text="➕ Добавить поступление вручную", callback_data="adm_manual_add")
    kb.button(text="📣 Отчёт сейчас", callback_data="adm_report_now")
    kb.button(text="⭐ Stars ON/OFF", callback_data="adm_toggle_stars")
    kb.button(text="🆔 Показать chat_id (мне)", callback_data="adm_show_my_id")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_pick_campaign(prefix: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="🍲 Ифтары", callback_data=f"{prefix}:iftar")
    kb.button(text="💧 Вода", callback_data=f"{prefix}:water")
    kb.button(text="🌾 ZF", callback_data=f"{prefix}:zf")
    kb.button(text="🍬 Id", callback_data=f"{prefix}:id")
    kb.button(text="⬅️ Назад", callback_data="admin_menu")
    kb.adjust(1)
    return kb.as_markup()


def kb_admin_prices():
    kb = InlineKeyboardBuilder()
    kb.button(text="🍲 Цена порции ифтара (EUR)", callback_data="adm_set_iftar_price")
    kb.button(text="🌾 Цена ZF на человека (EUR)", callback_data="adm_set_zf_price")
    kb.button(text="⭐ Курс EUR→Stars", callback_data="adm_set_rate")
    kb.button(text="💧 Цель воды (EUR)", callback_data="adm_set_water_target")
    kb.button(text="🍲 Цель порций (на день)", callback_data="adm_set_iftar_target")
    kb.button(text="⬅️ Назад", callback_data="admin_menu")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# START / LANGUAGE
# =========================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    uid = message.from_user.id
    lang = await get_user_lang(uid)
    if not lang:
        await message.answer("Ассаляму алейкум 🤍\n\nВыберите язык / Choose language:", reply_markup=kb_lang_select())
        return
    await message.answer(
        "Ассаляму алейкум 🤍\n\n1) Выберите сбор\n2) Затем выберите способ оплаты",
        reply_markup=kb_main(is_admin(uid))
    )


@dp.message(Command("lang"))
async def cmd_lang(message: Message):
    await message.answer("Выберите язык / Choose language:", reply_markup=kb_lang_select())


@dp.callback_query(lambda c: c.data in {"lang_ru", "lang_en"})
async def cb_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    await safe_edit(call, "Язык установлен.", reply_markup=kb_main(is_admin(call.from_user.id)))


# =========================
# MENUS
# =========================

@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "help", "admin_menu"})
async def cb_menus(call: CallbackQuery):
    uid = call.from_user.id

    if call.data == "lang_menu":
        await call.answer()
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        return

    if call.data == "back":
        await call.answer()
        await safe_edit(call, "Меню:", reply_markup=kb_main(is_admin(uid)))
        return

    if call.data == "list":
        await call.answer()
        await safe_edit(call, "Выберите сбор:", reply_markup=kb_campaigns())
        return

    if call.data == "help":
        await call.answer()
        day = iftar_campaign_day()
        rate = await get_rate()
        txt = (
            "❓ Помощь\n\n"
            "Логика:\n"
            "1) Выберите сбор\n"
            "2) Нажмите «Способы оплаты»\n"
            "3) Скопируйте реквизиты и укажите ТОЛЬКО отметку сбора\n\n"
            f"Ифтары всегда собираем на ЗАВТРА: сейчас сбор на день {day}.\n\n"
            "Отметки:\n"
            f"— Ифтары: {MARK_IFTAR}\n"
            f"— Вода: {MARK_WATER}\n"
            "— ZF: ZF5 (цифра = люди)\n"
            f"— Id: {MARK_ID}\n\n"
            f"Stars курс: 1€ = {rate}⭐"
        )
        await safe_edit(call, txt, reply_markup=kb_main(is_admin(uid)))
        return

    if call.data == "admin_menu":
        await call.answer()
        if not is_admin(uid):
            await safe_edit(call, "Нет доступа.", reply_markup=kb_main(False))
            return
        await safe_edit(call, "🛠 Админ-меню:", reply_markup=kb_admin_menu())
        return


# =========================
# CAMPAIGNS
# =========================

@dp.callback_query(lambda c: c.data.startswith("c_"))
async def cb_campaign(call: CallbackQuery):
    uid = call.from_user.id
    key = call.data.replace("c_", "").strip()
    LAST_CAMPAIGN[uid] = key

    if key == "iftar":
        desc = await kv_get("desc_iftar")
        day = iftar_campaign_day()
        target, raised, _ = await iftar_get(day)
        rem = max(0, target - raised)
        desc += f"\n\nСобрано: {raised}/{target} порций. Осталось: {rem}."
        desc += f"\nСобираем на день Рамадана: {day}."
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_campaign_actions("iftar"))
        return

    if key == "water":
        desc = await kv_get("desc_water")
        batch = await kv_get_int("water_batch", 1)
        target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
        raised = await kv_get_int("water_raised_eur", 0)
        rem = max(0, target - raised)
        desc += f"\n\nЦистерна #{batch}\nСобрано: {raised}€ / {target}€. Осталось: {rem}€."
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_campaign_actions("water"))
        return

    if key == "zf":
        if await kv_get_int("zf_open", 0) == 0:
            await call.answer()
            await safe_edit(call, "🔒 ZF сейчас закрыт (вне периода).", reply_markup=kb_campaigns())
            return
        desc = await kv_get("desc_zf")
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_campaign_actions("zf"))
        return

    if key == "id":
        if await kv_get_int("id_open", 0) == 0:
            await call.answer()
            await safe_edit(call, "🔒 Id сейчас закрыт (вне периода).", reply_markup=kb_campaigns())
            return
        desc = await kv_get("desc_id")
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_campaign_actions("id"))
        return

    if key == "ind_iftar":
        day = await ind_iftar_next_day()
        desc = (
            "🎥 Индивидуальная раздача ифтаров\n\n"
            "Вы закрываете один день полностью, будет видео-отчёт.\n"
            f"Объём: {IND_IFTAR_FULL_PORTIONS} порций.\n"
            f"Следующая свободная дата: день Рамадана {day}.\n\n"
            "После оплаты нажмите «оформить индивидуальную раздачу» и оставьте сообщение."
        )
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_individual_actions())
        return

    if key == "ind_water":
        batch = await ind_water_next_batch()
        desc = (
            "🎥 Индивидуальная раздача воды\n\n"
            "Вы закрываете одну раздачу воды полностью, будет видео-отчёт.\n"
            f"Объём: {IND_WATER_FULL_EUR}€ (цистерна/раздача 5000л).\n"
            f"Следующая индивидуальная раздача №: {batch}.\n\n"
            "После оплаты нажмите «оформить индивидуальную раздачу» и оставьте сообщение."
        )
        await call.answer()
        await safe_edit(call, desc, reply_markup=kb_individual_actions())
        return

    await call.answer()
    await safe_edit(call, "—", reply_markup=kb_campaigns())


@dp.callback_query(lambda c: c.data == "pay_methods")
async def cb_pay_methods(call: CallbackQuery):
    uid = call.from_user.id
    stars_enabled = bool(await kv_get_int("stars_enabled", 1))
    await call.answer()
    await call.message.answer("💳 Выберите способ оплаты:", reply_markup=kb_payment_methods(stars_enabled))


@dp.callback_query(lambda c: c.data == "pay_back")
async def cb_pay_back(call: CallbackQuery):
    await call.answer()
    await call.message.answer("⬅️ Назад к сбору:", reply_markup=kb_campaigns())


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
    if c == "ind_iftar":
        return f"{MARK_IFTAR} (IND)"
    if c == "ind_water":
        return f"{MARK_WATER} (IND)"
    return "SUPPORT"


# =========================
# QUICK buttons: close/full (for non-stars too)
# =========================

@dp.callback_query(lambda c: c.data in {"quick_close", "quick_full"})
async def cb_quick(call: CallbackQuery):
    uid = call.from_user.id
    camp = LAST_CAMPAIGN.get(uid, "iftar")

    if camp == "iftar":
        day = iftar_campaign_day()
        target, raised, _ = await iftar_get(day)
        rem = max(0, target - raised)
        if call.data == "quick_close":
            if rem <= 0:
                await call.answer("Уже закрыто/достаточно", show_alert=True)
                return
            PENDING[uid] = {"type": "mark_iftar_portions_fixed", "day": day, "portions": rem, "note": "close_remaining"}
            await call.answer()
            await call.message.answer(f"Остаток на день {day}: {rem} порций.\nНапишите: OK чтобы учесть, или 0 чтобы отменить.")
            return

        if call.data == "quick_full":
            PENDING[uid] = {"type": "mark_iftar_portions_fixed", "day": day, "portions": target, "note": "pay_full_day"}
            await call.answer()
            await call.message.answer(f"Полный день {day}: {target} порций.\nНапишите: OK чтобы учесть, или 0 чтобы отменить.")
            return

    if camp == "water":
        batch = await kv_get_int("water_batch", 1)
        target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
        raised = await kv_get_int("water_raised_eur", 0)
        rem = max(0, target - raised)

        if call.data == "quick_close":
            if rem <= 0:
                await call.answer("Уже закрыто/достаточно", show_alert=True)
                return
            PENDING[uid] = {"type": "mark_water_eur_fixed", "eur": rem, "note": "close_remaining"}
            await call.answer()
            await call.message.answer(f"Остаток по воде (цистерна #{batch}): {rem}€.\nНапишите: OK чтобы учесть, или 0 чтобы отменить.")
            return

        if call.data == "quick_full":
            PENDING[uid] = {"type": "mark_water_eur_fixed", "eur": target, "note": "pay_full_tank"}
            await call.answer()
            await call.message.answer(f"Полная цистерна: {target}€.\nНапишите: OK чтобы учесть, или 0 чтобы отменить.")
            return

    await call.answer("Недоступно", show_alert=True)


# =========================
# PAYMENT METHODS
# =========================

@dp.callback_query(lambda c: c.data.startswith("pay_"))
async def cb_pay(call: CallbackQuery):
    uid = call.from_user.id
    method = call.data.replace("pay_", "").strip()
    campaign = LAST_CAMPAIGN.get(uid, "iftar")

    stars_enabled = bool(await kv_get_int("stars_enabled", 1))
    if method == "stars" and not stars_enabled:
        await call.answer("Stars выключены", show_alert=True)
        return

    # Stars: show quick menu for iftar/water
    if method == "stars":
        await call.answer()
        if campaign == "iftar":
            await call.message.answer("⭐ Stars — выберите вариант оплаты:", reply_markup=kb_stars_quick("iftar"))
            return
        if campaign == "water":
            await call.message.answer("⭐ Stars — выберите вариант оплаты:", reply_markup=kb_stars_quick("water"))
            return
        if campaign == "id":
            PENDING[uid] = {"type": "id_stars_amount"}
            await call.message.answer("Введите сумму в евро для Id (например 5 или 10):")
            return
        if campaign == "zf":
            PENDING[uid] = {"type": "zf_stars_people"}
            await call.message.answer("Сколько людей вы оплачиваете ZF Stars? (число)")
            return
        # individual fixed stars as before
        if campaign == "ind_iftar":
            price = await kv_get_float("iftar_portion_eur", DEFAULT_IFTAR_PORTION_EUR)
            reserved_day = await ind_iftar_next_day()
            eur_amount = IND_IFTAR_FULL_PORTIONS * price
            payload = f"ind_iftar:day:{reserved_day}:portions:{IND_IFTAR_FULL_PORTIONS}"
            await send_stars_invoice(
                chat_id=uid,
                title=f"Инд. ифтары — день {reserved_day}",
                description=f"{IND_IFTAR_FULL_PORTIONS} порций × {price:.2f}€",
                payload=payload,
                eur_amount=eur_amount,
            )
            return
        if campaign == "ind_water":
            reserved_batch = await ind_water_next_batch()
            payload = f"ind_water:batch:{reserved_batch}:eur:{IND_WATER_FULL_EUR}"
            await send_stars_invoice(
                chat_id=uid,
                title=f"Инд. вода — раздача #{reserved_batch}",
                description=f"Полная оплата {IND_WATER_FULL_EUR}€",
                payload=payload,
                eur_amount=float(IND_WATER_FULL_EUR),
            )
            return

    code = code_for_campaign(uid)

    if campaign == "zf":
        base = (
            "🌾 ZF — Закят-уль-Фитр\n\n"
            "1) Оплатите выбранным способом\n"
            "2) В назначении укажите ТОЛЬКО ZF5 / ZF8 (цифра = кол-во людей)\n"
            "3) После оплаты нажмите кнопку ниже и внесите себя в список\n\n"
        )
        if method == "bank":
            txt = base + payment_text_bank("ZF5")
        elif method == "tr":
            txt = base + payment_text_turkey("ZF5")
        elif method == "swift":
            txt = base + payment_text_swift("ZF5")
        elif method == "paypal":
            txt = base + payment_text_paypal("ZF5")
        elif method == "zen":
            txt = base + payment_text_zen("ZF5")
        elif method == "card":
            txt = base + payment_text_card("ZF5")
        elif method == "crypto":
            txt = base + payment_text_crypto("ZF5")
        else:
            txt = base + "Метод не настроен."
        await call.answer()
        await call.message.answer(txt, disable_web_page_preview=True, reply_markup=kb_zf_after_payment())
        return

    if campaign == "id":
        base = "🍬 Id — Ид аль-Фитр\n\nОплатите и укажите ТОЛЬКО отметку.\n\n"
        if method == "bank":
            txt = base + payment_text_bank(MARK_ID)
        elif method == "tr":
            txt = base + payment_text_turkey(MARK_ID)
        elif method == "swift":
            txt = base + payment_text_swift(MARK_ID)
        elif method == "paypal":
            txt = base + payment_text_paypal(MARK_ID)
        elif method == "zen":
            txt = base + payment_text_zen(MARK_ID)
        elif method == "card":
            txt = base + payment_text_card(MARK_ID)
        elif method == "crypto":
            txt = base + payment_text_crypto(MARK_ID)
        else:
            txt = base + "Метод не настроен."
        await call.answer()
        await call.message.answer(txt, disable_web_page_preview=True, reply_markup=kb_id_after_payment())
        return

    # individual campaigns (non-stars)
    if campaign == "ind_iftar":
        reserved_day = await ind_iftar_next_day()
        base = (
            f"🎥 Индивидуальная раздача ифтаров (день {reserved_day})\n\n"
            f"Объём: {IND_IFTAR_FULL_PORTIONS} порций.\n"
            "Оплатите и укажите отметку.\n\n"
        )
        if method == "bank":
            txt = base + payment_text_bank(f"{MARK_IFTAR}")
        elif method == "tr":
            txt = base + payment_text_turkey(f"{MARK_IFTAR}")
        elif method == "swift":
            txt = base + payment_text_swift(f"{MARK_IFTAR}")
        elif method == "paypal":
            txt = base + payment_text_paypal(f"{MARK_IFTAR}")
        elif method == "zen":
            txt = base + payment_text_zen(f"{MARK_IFTAR}")
        elif method == "card":
            txt = base + payment_text_card(f"{MARK_IFTAR}")
        elif method == "crypto":
            txt = base + payment_text_crypto(f"{MARK_IFTAR}")
        else:
            txt = base + "Метод не настроен."
        PENDING[uid] = {"type": "ind_pending_method", "method": method, "kind": "ind_iftar"}
        await call.answer()
        await call.message.answer(txt, disable_web_page_preview=True, reply_markup=kb_individual_actions())
        return

    if campaign == "ind_water":
        reserved_batch = await ind_water_next_batch()
        base = (
            f"🎥 Индивидуальная раздача воды (№{reserved_batch})\n\n"
            f"Объём: {IND_WATER_FULL_EUR}€.\n"
            "Оплатите и укажите отметку.\n\n"
        )
        if method == "bank":
            txt = base + payment_text_bank(f"{MARK_WATER}")
        elif method == "tr":
            txt = base + payment_text_turkey(f"{MARK_WATER}")
        elif method == "swift":
            txt = base + payment_text_swift(f"{MARK_WATER}")
        elif method == "paypal":
            txt = base + payment_text_paypal(f"{MARK_WATER}")
        elif method == "zen":
            txt = base + payment_text_zen(f"{MARK_WATER}")
        elif method == "card":
            txt = base + payment_text_card(f"{MARK_WATER}")
        elif method == "crypto":
            txt = base + payment_text_crypto(f"{MARK_WATER}")
        else:
            txt = base + "Метод не настроен."
        PENDING[uid] = {"type": "ind_pending_method", "method": method, "kind": "ind_water"}
        await call.answer()
        await call.message.answer(txt, disable_web_page_preview=True, reply_markup=kb_individual_actions())
        return

    # iftar/water standard instructions
    title = "🍲 Ифтары" if campaign == "iftar" else "💧 Вода"
    base = f"{title}\n\nОплатите и укажите ТОЛЬКО отметку.\n\n"
    if method == "bank":
        txt = base + payment_text_bank(code)
    elif method == "tr":
        txt = base + payment_text_turkey(code)
    elif method == "swift":
        txt = base + payment_text_swift(code)
    elif method == "paypal":
        txt = base + payment_text_paypal(code)
    elif method == "zen":
        txt = base + payment_text_zen(code)
    elif method == "card":
        txt = base + payment_text_card(code)
    elif method == "crypto":
        txt = base + payment_text_crypto(code)
    else:
        txt = base + "Метод не настроен."

    PENDING[uid] = {"type": "last_method", "method": method, "campaign": campaign}
    await call.answer()
    await call.message.answer(txt, disable_web_page_preview=True, reply_markup=kb_campaign_actions(campaign))


# =========================
# Stars quick callbacks
# =========================

@dp.callback_query(lambda c: c.data.startswith("stars_quick:"))
async def cb_stars_quick(call: CallbackQuery):
    uid = call.from_user.id
    _, action, kind = call.data.split(":", 2)

    if kind == "iftar":
        day = iftar_campaign_day()
        target, raised, _ = await iftar_get(day)
        rem = max(0, target - raised)
        price = await kv_get_float("iftar_portion_eur", DEFAULT_IFTAR_PORTION_EUR)

        if action == "close":
            if rem <= 0:
                await call.answer("Уже закрыто/достаточно", show_alert=True)
                return
            eur_amount = rem * price
            payload = f"iftar:day:{day}:portions:{rem}"
            await call.answer()
            await send_stars_invoice(uid, f"Ифтары — закрыть день {day}", f"Остаток {rem} порций × {price:.2f}€", payload, eur_amount)
            return

        if action == "full":
            eur_amount = target * price
            payload = f"iftar:day:{day}:portions:{target}"
            await call.answer()
            await send_stars_invoice(uid, f"Ифтары — полный день {day}", f"{target} порций × {price:.2f}€", payload, eur_amount)
            return

        if action == "other":
            PENDING[uid] = {"type": "iftar_stars_portions"}
            await call.answer()
            await call.message.answer("Сколько порций вы хотите оплатить Stars? (число)")
            return

    if kind == "water":
        batch = await kv_get_int("water_batch", 1)
        target = await kv_get_int("water_target_eur", DEFAULT_WATER_TARGET_EUR)
        raised = await kv_get_int("water_raised_eur", 0)
        rem = max(0, target - raised)

        if action == "close":
            if rem <= 0:
                await call.answer("Уже закрыто/достаточно", show_alert=True)
                return
            payload = f"water:eur:{rem}"
            await call.answer()
            await send_stars_invoice(uid, f"Вода — закрыть цистерну #{batch}", f"Остаток {rem}€", payload, float(rem))
            return

        if action == "full":
            payload = f"water:eur:{target}"
            await call.answer()
            await send_stars_invoice(uid, "Вода — цистерна целиком", f"{target}€", payload, float(target))
            return

        if action == "other":
            PENDING[uid] = {"type": "water_stars_eur"}
            await call.answer()
            await call.message.answer("Введите сумму в евро для воды (целое число), например 10:")
            return

    await call.answer("Недоступно", show_alert=True)


# =========================
# Mark paid buttons (manual accounting)
# =========================

@dp.callback_query(lambda c: c.data == "mark_paid")
async def cb_mark_paid(call: CallbackQuery):
    uid = call.from_user.id
    campaign = LAST_CAMPAIGN.get(uid, "iftar")
    m = PENDING.get(uid, {})
    method = m.get("method", "manual")

    if campaign == "iftar":
        day = iftar_campaign_day()
        PENDING[uid] = {"type": "mark_iftar_portions", "day": day, "method": method}
        await call.answer()
        await call.message.answer(f"Сколько порций учесть? (сбор на день {day})")
        return

    if campaign == "water":
        PENDING[uid] = {"type": "mark_water_eur", "method": method}
        await call.answer()
        await call.message.answer("Сколько евро учесть по воде? (целое число)")
        return

    if campaign == "id":
        PENDING[uid] = {"type": "id_wait_amount", "method": method}
        await call.answer()
        await call.message.answer("Введите сумму в евро (целое число), чтобы учесть Id:")
        return

    await call.answer("Недоступно", show_alert=True)


@dp.callback_query(lambda c: c.data == "ind_mark_paid")
async def cb_ind_mark_paid(call: CallbackQuery):
    uid = call.from_user.id
    campaign = LAST_CAMPAIGN.get(uid, "iftar")
    m = PENDING.get(uid, {})
    method = m.get("method", "manual")
    kind = "ind_iftar" if campaign == "ind_iftar" else "ind_water"

    if kind == "ind_iftar":
        reserved_day = await ind_iftar_reserve_day()
        PENDING[uid] = {"type": "ind_collect_video", "kind": "ind_iftar", "day": reserved_day, "method": method}
        await call.answer()
        await call.message.answer(
            "Напишите сообщение для видео-отчёта.\n"
            "Пример: С любовью от братьев/сестёр ... из ...\n"
            "Если не хотите — напишите: -"
        )
        return

    if kind == "ind_water":
        reserved_batch = await ind_water_reserve_batch()
        PENDING[uid] = {"type": "ind_collect_video", "kind": "ind_water", "batch": reserved_batch, "method": method}
        await call.answer()
        await call.message.answer("Напишите сообщение для видео-отчёта. Если не хотите — напишите: -")
        return

    await call.answer("Недоступно", show_alert=True)


@dp.callback_query(lambda c: c.data == "zf_mark")
async def cb_zf_mark(call: CallbackQuery):
    uid = call.from_user.id
    PENDING[uid] = {"type": "zf_wait_code"}
    await call.answer()
    await call.message.answer("Пришлите код, который вы указали при оплате (пример: ZF5).")


@dp.callback_query(lambda c: c.data == "id_mark")
async def cb_id_mark(call: CallbackQuery):
    uid = call.from_user.id
    PENDING[uid] = {"type": "id_wait_amount", "method": "manual"}
    await call.answer()
    await call.message.answer("Введите сумму в евро (целое число), чтобы мы могли учесть (пример: 20):")


# =========================
# ADMIN CALLBACKS (same as before, trimmed)
# =========================

@dp.callback_query(lambda c: c.data.startswith("adm_") or c.data.startswith("pickdesc:") or c.data.startswith("manualadd:"))
async def cb_admin(call: CallbackQuery):
    uid = call.from_user.id
    if not is_admin(uid):
        await call.answer("Нет доступа", show_alert=True)
        return

    data = call.data

    if data == "adm_help":
        await call.answer()
        txt = (
            "🛠 Админ-шпаргалка\n\n"
            "Отчёты:\n"
            "/report_now\n"
            "/report_day 8\n\n"
            "Групповой chat_id:\n"
            "/chat_id\n\n"
            "Stars:\n"
            "/activate_stars\n"
            "/deactivate_stars\n\n"
            "Тексты:\n"
            "/set_desc iftar <текст>\n"
            "/set_desc water <текст>\n"
            "/set_desc zf <текст>\n"
            "/set_desc id <текст>\n\n"
            "Ручные поступления:\n"
            "/add_iftar 15\n"
            "/add_water 20\n"
            "/add_id 50\n"
            "/add_zf 5 семья Умм Мухаммад\n"
        )
        await call.message.answer(txt)
        return

    if data == "adm_edit_desc":
        await call.answer()
        await call.message.answer("Выберите сбор для изменения текста:", reply_markup=kb_admin_pick_campaign("pickdesc"))
        return

    if data.startswith("pickdesc:"):
        campaign = data.split(":", 1)[1]
        PENDING[uid] = {"type": "admin_set_desc", "campaign": campaign}
        await call.answer()
        await call.message.answer("Пришлите новый текст одним сообщением (обычный текст).")
        return

    if data == "adm_edit_prices":
        await call.answer()
        await call.message.answer("Выберите что изменить:", reply_markup=kb_admin_prices())
        return

    if data == "adm_set_iftar_price":
        PENDING[uid] = {"type": "admin_set_price", "key": "iftar_portion_eur"}
        await call.answer()
        await call.message.answer("Введите цену 1 порции ифтара в евро (например 4 или 3.7):")
        return

    if data == "adm_set_zf_price":
        PENDING[uid] = {"type": "admin_set_price", "key": "zf_eur_per_person"}
        await call.answer()
        await call.message.answer("Введите цену ZF на 1 человека в евро (например 9):")
        return

    if data == "adm_set_rate":
        PENDING[uid] = {"type": "admin_set_int", "key": "eur_to_stars"}
        await call.answer()
        await call.message.answer("Введите курс: сколько Stars за 1€ (например 50):")
        return

    if data == "adm_set_water_target":
        PENDING[uid] = {"type": "admin_set_int", "key": "water_target_eur"}
        await call.answer()
        await call.message.answer("Введите цель воды (EUR), например 235:")
        return

    if data == "adm_set_iftar_target":
        PENDING[uid] = {"type": "admin_set_iftar_target"}
        await call.answer()
        await call.message.answer("Введите цель порций на день (например 100 или 150):")
        return

    if data == "adm_manual_add":
        await call.answer()
        await call.message.answer("Куда добавить поступление?", reply_markup=kb_admin_pick_campaign("manualadd"))
        return

    if data.startswith("manualadd:"):
        campaign = data.split(":", 1)[1]
        PENDING[uid] = {"type": "admin_manual_add", "campaign": campaign}
        if campaign == "iftar":
            day = iftar_campaign_day()
            await call.answer()
            await call.message.answer(f"Введите сколько порций добавить вручную (сбор на день {day}):")
        elif campaign == "water":
            await call.answer()
            await call.message.answer("Введите сколько EUR добавить вручную (вода):")
        elif campaign == "id":
            await call.answer()
            await call.message.answer("Введите сколько EUR добавить вручную (Id):")
        elif campaign == "zf":
            await call.answer()
            await call.message.answer("Введите: <кол-во людей> <как показать в списке>\nНапример: 5 семья Умм Мухаммад")
        return

    if data == "adm_report_now":
        await call.answer("OK")
        report = await build_daily_report_md()
        if PUBLIC_GROUP_ID:
            await send_md(PUBLIC_GROUP_ID, report)
        await call.message.answer(report, parse_mode="Markdown")
        return

    if data == "adm_toggle_stars":
        cur = await kv_get_int("stars_enabled", 1)
        new = 0 if cur == 1 else 1
        await kv_set_int("stars_enabled", new)
        await call.answer("OK")
        await call.message.answer(f"Stars теперь: {'ON' if new else 'OFF'}")
        return

    if data == "adm_show_my_id":
        await call.answer()
        await call.message.answer(
            f"Ваш user_id: {uid}\n"
            f"PUBLIC_GROUP_ID: {PUBLIC_GROUP_ID}\n"
            f"ZF_GROUP_ID: {ZF_GROUP_ID}\n"
            f"DB_PATH: {DB_PATH}"
        )
        return


@dp.message(Command("activate_stars"))
async def cmd_activate_stars(message: Message):
    if not is_admin(message.from_user.id):
        return
    await kv_set_int("stars_enabled", 1)
    await message.answer("⭐ Stars включены.")


@dp.message(Command("deactivate_stars"))
async def cmd_deactivate_stars(message: Message):
    if not is_admin(message.from_user.id):
        return
    await kv_set_int("stars_enabled", 0)
    await message.answer("⭐ Stars выключены.")


@dp.message(Command("set_desc"))
async def cmd_set_desc(message: Message):
    if not is_admin(message.from_user.id):
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
    if not is_admin(message.from_user.id):
        return
    report = await build_daily_report_md()
    if PUBLIC_GROUP_ID:
        await send_md(PUBLIC_GROUP_ID, report)
    await message.answer(report, parse_mode="Markdown")


@dp.message(Command("report_day"))
async def cmd_report_day(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /report_day 8")
        return
    try:
        day = int(parts[1])
        if day <= 0 or day > 30:
            raise ValueError
    except Exception:
        await message.answer("День должен быть 1..30")
        return

    target, raised, closed = await iftar_get(day)
    rem = max(0, target - raised)
    txt = (
        f"🍲 Ифтары — день {day}\n"
        f"Собрано: {raised}/{target} порций\n"
        f"Осталось: {rem}\n"
        f"Статус: {'закрыт' if closed else 'открыт'}"
    )
    await message.answer(txt)


@dp.message(Command("add_iftar"))
async def cmd_add_iftar(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_iftar 15")
        return
    day = iftar_campaign_day()
    await iftar_add_with_autoclose(day, int(float(parts[1])))
    await message.answer("OK")


@dp.message(Command("add_water"))
async def cmd_add_water(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_water 20")
        return
    await water_add_with_autoclose(int(float(parts[1])))
    await message.answer("OK")


@dp.message(Command("add_id"))
async def cmd_add_id(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /add_id 50")
        return
    await kv_inc_int("id_raised_eur", int(float(parts[1])))
    await message.answer("OK")


@dp.message(Command("add_zf"))
async def cmd_add_zf(message: Message):
    if not is_admin(message.from_user.id):
        return
    m = re.match(r"^/add_zf\s+(\d+)\s+(.+)$", (message.text or "").strip())
    if not m:
        await message.answer("Использование: /add_zf 5 семья Умм Мухаммад")
        return
    people = int(m.group(1))
    label = m.group(2).strip().strip('"').strip()
    if people <= 0:
        await message.answer("People must be > 0")
        return
    await zf_add_entry(message.from_user.id, message.from_user.username or "-", label, people, f"ZF{people}", "manual_by_admin")
    await zf_post_update()
    await message.answer("OK")


@dp.message(Command("chat_id"))
async def cmd_chat_id(message: Message):
    await message.answer(f"chat_id = {message.chat.id}")


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
    stars_total = sp.total_amount

    uid = message.from_user.id
    username = message.from_user.username or "-"
    when = utc_now_str()
    link = user_link_html(uid)

    parts = payload.split(":")
    typ = parts[0] if parts else "unknown"

    try:
        if typ == "iftar":
            day = int(parts[2])
            portions = int(parts[4])
            await iftar_add_with_autoclose(day, portions)
            await send_admin_html(
                "⭐ STARS PAYMENT<br>"
                f"Campaign: IFTAR ({MARK_IFTAR}) day {day}<br>"
                f"Portions: {portions}<br>"
                f"Stars: {stars_total}⭐<br>"
                f"Time: {when}<br>"
                f"User: @{h(username)} / {uid}<br>{link}"
            )
            await message.answer("✅ Джазак Аллаху хейр! 🤍")
            return

        if typ == "water":
            eur = int(parts[2])
            await water_add_with_autoclose(eur)
            await send_admin_html(
                "⭐ STARS PAYMENT<br>"
                f"Campaign: WATER ({MARK_WATER})<br>"
                f"Amount: {eur} EUR<br>"
                f"Stars: {stars_total}⭐<br>"
                f"Time: {when}<br>"
                f"User: @{h(username)} / {uid}<br>{link}"
            )
            await message.answer("✅ Джазак Аллаху хейр! 🤍")
            return

        if typ == "id":
            eur = int(parts[2])
            await kv_inc_int("id_raised_eur", eur)
            await send_admin_html(
                "⭐ STARS PAYMENT<br>"
                f"Campaign: ID ({MARK_ID})<br>"
                f"Amount: {eur} EUR<br>"
                f"Stars: {stars_total}⭐<br>"
                f"Time: {when}<br>"
                f"User: @{h(username)} / {uid}<br>{link}"
            )
            await message.answer("✅ Джазак Аллаху хейр! 🤍")
            return

        if typ == "ind_iftar":
            day = int(parts[2])
            portions = int(parts[4])
            await send_admin_html(
                "⭐ STARS PAYMENT<br>"
                f"Campaign: IND IFTAR day {day}<br>"
                f"Portions: {portions}<br>"
                f"Stars: {stars_total}⭐<br>"
                f"Time: {when}<br>"
                f"User: @{h(username)} / {uid}<br>{link}<br>"
                "⚠️ Пользователь должен нажать «оформить индивидуальную раздачу» и оставить сообщение."
            )
            await message.answer("✅ Оплата получена. Теперь нажмите «оформить индивидуальную раздачу» и оставьте сообщение. 🤍")
            return

        if typ == "ind_water":
            batch = int(parts[2])
            eur = int(parts[4])
            await send_admin_html(
                "⭐ STARS PAYMENT<br>"
                f"Campaign: IND WATER batch {batch}<br>"
                f"Amount: {eur} EUR<br>"
                f"Stars: {stars_total}⭐<br>"
                f"Time: {when}<br>"
                f"User: @{h(username)} / {uid}<br>{link}<br>"
                "⚠️ Пользователь должен нажать «оформить индивидуальную раздачу» и оставить сообщение."
            )
            await message.answer("✅ Оплата получена. Теперь нажмите «оформить индивидуальную раздачу» и оставьте сообщение. 🤍")
            return

    except Exception:
        logging.exception("successful_payment parse failed")

    await send_admin_html(
        "⭐ STARS PAYMENT (unhandled)<br>"
        f"Stars: {stars_total}⭐<br>Time: {when}<br>"
        f"User: @{h(username)} / {uid}<br>{link}<br>"
        f"Payload: <code>{h(payload)}</code>"
    )
    await message.answer("✅ Джазак Аллаху хейр! 🤍")


# =========================
# PENDING ROUTER
# =========================

@dp.message()
async def pending_router(message: Message):
    if not message.from_user:
        return
    uid = message.from_user.id
    st = PENDING.get(uid)
    if not st:
        return
    raw = (message.text or "").strip()

    # fixed OK confirm for quick_close/full
    if st.get("type") in {"mark_iftar_portions_fixed", "mark_water_eur_fixed"}:
        if raw.lower() in {"0", "нет", "no", "cancel"}:
            PENDING.pop(uid, None)
            await message.answer("Ок, отменено.")
            return
        if raw.lower() not in {"ok", "да", "yes"}:
            await message.answer("Напишите OK чтобы учесть, или 0 чтобы отменить.")
            return

        if st["type"] == "mark_iftar_portions_fixed":
            day = int(st["day"])
            portions = int(st["portions"])
            await iftar_add_with_autoclose(day, portions)
            PENDING.pop(uid, None)
            await send_admin_html(
                "✅ IFTAR QUICK MARKED<br>"
                f"Day: {day}<br>"
                f"Portions: {portions}<br>"
                f"Note: {h(st.get('note',''))}<br>"
                f"Time: {utc_now_str()}<br>"
                f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
                f"{user_link_html(uid)}"
            )
            await message.answer("✅ Учтено. Джазак Аллаху хейр! 🤍")
            return

        if st["type"] == "mark_water_eur_fixed":
            eur = int(st["eur"])
            await water_add_with_autoclose(eur)
            PENDING.pop(uid, None)
            await send_admin_html(
                "✅ WATER QUICK MARKED<br>"
                f"Amount: {eur} EUR<br>"
                f"Note: {h(st.get('note',''))}<br>"
                f"Time: {utc_now_str()}<br>"
                f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
                f"{user_link_html(uid)}"
            )
            await message.answer("✅ Учтено. Джазак Аллаху хейр! 🤍")
            return

    # Admin set desc
    if st.get("type") == "admin_set_desc":
        campaign = st.get("campaign")
        await kv_set(f"desc_{campaign}", raw)
        PENDING.pop(uid, None)
        await message.answer("✅ Текст обновлён.")
        return

    # Admin set float
    if st.get("type") == "admin_set_price":
        key = st.get("key")
        try:
            val = float(raw.replace(",", "."))
            if val <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно число > 0. Попробуйте ещё раз:")
            return
        await kv_set_float(key, val)
        PENDING.pop(uid, None)
        await message.answer("✅ Обновлено.")
        return

    # Admin set int
    if st.get("type") == "admin_set_int":
        key = st.get("key")
        try:
            val = int(float(raw.replace(",", ".")))
            if val <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        await kv_set_int(key, val)
        PENDING.pop(uid, None)
        await message.answer("✅ Обновлено.")
        return

    # Admin set iftar target for current campaign day
    if st.get("type") == "admin_set_iftar_target":
        try:
            target = int(float(raw.replace(",", ".")))
            if target <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        day = iftar_campaign_day()
        await iftar_set_target(day, target)
        PENDING.pop(uid, None)
        await message.answer(f"✅ Цель на день {day} обновлена: {target}")
        return

    # Admin manual add
    if st.get("type") == "admin_manual_add":
        campaign = st.get("campaign")
        if campaign == "iftar":
            try:
                portions = int(float(raw.replace(",", ".")))
                if portions <= 0:
                    raise ValueError
            except Exception:
                await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
                return
            day = iftar_campaign_day()
            await iftar_add_with_autoclose(day, portions)
            PENDING.pop(uid, None)
            await message.answer(f"✅ Добавлено {portions} порций на день {day}.")
            return

        if campaign == "water":
            try:
                eur = int(float(raw.replace(",", ".")))
                if eur <= 0:
                    raise ValueError
            except Exception:
                await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
                return
            await water_add_with_autoclose(eur)
            PENDING.pop(uid, None)
            await message.answer(f"✅ Добавлено {eur}€ к воде.")
            return

        if campaign == "id":
            try:
                eur = int(float(raw.replace(",", ".")))
                if eur <= 0:
                    raise ValueError
            except Exception:
                await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
                return
            await kv_inc_int("id_raised_eur", eur)
            PENDING.pop(uid, None)
            await message.answer(f"✅ Добавлено {eur}€ к Id.")
            return

        if campaign == "zf":
            m = re.match(r"^(\d+)\s+(.+)$", raw)
            if not m:
                await message.answer("Формат: 5 семья Умм Мухаммад")
                return
            people = int(m.group(1))
            label = m.group(2).strip().strip('"')
            if people <= 0:
                await message.answer("Людей должно быть > 0")
                return
            await zf_add_entry(uid, message.from_user.username or "-", label, people, f"ZF{people}", "manual_by_admin")
            PENDING.pop(uid, None)
            await zf_post_update()
            await message.answer("✅ Добавлено и список обновлён.")
            return

    # Mark iftar portions (manual)
    if st.get("type") == "mark_iftar_portions":
        try:
            portions = int(float(raw.replace(",", ".")))
            if portions <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        day = int(st["day"])
        method = st.get("method", "manual")
        await iftar_add_with_autoclose(day, portions)
        PENDING.pop(uid, None)
        await send_admin_html(
            "✅ IFTAR MARKED<br>"
            f"Day: {day}<br>"
            f"Portions: {portions}<br>"
            f"Method: {h(method)}<br>"
            f"Time: {utc_now_str()}<br>"
            f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
            f"{user_link_html(uid)}"
        )
        await message.answer("✅ Учтено. Джазак Аллаху хейр! 🤍")
        return

    # Mark water eur (manual)
    if st.get("type") == "mark_water_eur":
        try:
            eur = int(float(raw.replace(",", ".")))
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        method = st.get("method", "manual")
        await water_add_with_autoclose(eur)
        PENDING.pop(uid, None)
        await send_admin_html(
            "✅ WATER MARKED<br>"
            f"Amount: {eur} EUR<br>"
            f"Method: {h(method)}<br>"
            f"Time: {utc_now_str()}<br>"
            f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
            f"{user_link_html(uid)}"
        )
        await message.answer("✅ Учтено. Джазак Аллаху хейр! 🤍")
        return

    # ZF flow
    if st.get("type") == "zf_wait_code":
        n = parse_zf_bank_code(raw)
        if not n:
            await message.answer("Нужен код вида ZF5 (или ZF 5, ZF-5). Попробуйте ещё раз:")
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
        people = int(st["people"])
        bank_code = st["bank_code"]
        method = "manual"
        await zf_add_entry(uid, message.from_user.username or "-", label, people, bank_code, method)

        eur_per = await kv_get_float("zf_eur_per_person", DEFAULT_ZF_EUR_PER_PERSON)
        kg_per = int(await kv_get_float("zf_kg_per_person", DEFAULT_ZF_KG_PER_PERSON))
        eur = people * eur_per
        kg = people * kg_per

        await send_admin_html(
            "✅ ZF MARKED<br>"
            f"Label: <b>{h(label)}</b><br>"
            f"Bank code: <code>{h(bank_code)}</code><br>"
            f"People: {people} | expected {eur:.2f}€ | rice {kg} kg<br>"
            f"Time: {utc_now_str()}<br>"
            f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
            f"{user_link_html(uid)}"
        )

        await zf_post_update()
        PENDING.pop(uid, None)
        await message.answer("✅ Записано. Джазак Аллаху хейр! 🤍")
        return

    # ID amount
    if st.get("type") == "id_wait_amount":
        try:
            eur = int(float(raw.replace(",", ".")))
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно число > 0. Попробуйте ещё раз:")
            return
        method = st.get("method", "manual")
        await kv_inc_int("id_raised_eur", eur)
        PENDING.pop(uid, None)
        await send_admin_html(
            "✅ ID MARKED<br>"
            f"Amount: {eur} EUR<br>"
            f"Method: {h(method)}<br>"
            f"Time: {utc_now_str()}<br>"
            f"User: @{h(message.from_user.username or '-')} / {uid}<br>"
            f"{user_link_html(uid)}"
        )
        await message.answer("✅ Спасибо! Джазак Аллаху хейр! 🤍")
        return

    # Stars custom inputs
    if st.get("type") == "iftar_stars_portions":
        try:
            portions = int(float(raw.replace(",", ".")))
            if portions <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        price = await kv_get_float("iftar_portion_eur", DEFAULT_IFTAR_PORTION_EUR)
        day = iftar_campaign_day()
        eur_amount = portions * price
        payload = f"iftar:day:{day}:portions:{portions}"
        await send_stars_invoice(uid, f"Ифтары — день {day}", f"{portions} порций × {price:.2f}€", payload, eur_amount)
        PENDING.pop(uid, None)
        return

    if st.get("type") == "water_stars_eur":
        try:
            eur = int(float(raw.replace(",", ".")))
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        payload = f"water:eur:{eur}"
        await send_stars_invoice(uid, "Вода (цистерна 5000л)", f"Пожертвование {eur}€", payload, float(eur))
        PENDING.pop(uid, None)
        return

    if st.get("type") == "id_stars_amount":
        try:
            eur = float(raw.replace(",", "."))
            if eur <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно число > 0. Попробуйте ещё раз:")
            return
        payload = f"id:eur:{int(round(eur))}"
        await send_stars_invoice(uid, "Id — Ид аль-Фитр", f"Пожертвование {eur:.2f}€", payload, float(eur))
        PENDING.pop(uid, None)
        return

    if st.get("type") == "zf_stars_people":
        try:
            people = int(float(raw.replace(",", ".")))
            if people <= 0:
                raise ValueError
        except Exception:
            await message.answer("Нужно целое число > 0. Попробуйте ещё раз:")
            return
        eur_per = await kv_get_float("zf_eur_per_person", DEFAULT_ZF_EUR_PER_PERSON)
        eur_amount = people * eur_per
        payload = f"zf:people:{people}"
        await send_stars_invoice(uid, "ZF — Закят-уль-Фитр", f"{people} чел × {eur_per:.2f}€", payload, float(eur_amount))
        PENDING.pop(uid, None)
        return

    # Individual order messages
    if st.get("type") == "ind_collect_video":
        msg = "" if raw == "-" else raw
        st["video"] = msg
        st["type"] = "ind_collect_print"
        PENDING[uid] = st
        await message.answer(
            "Теперь (опционально) напишите короткую записку для распечатки на месте раздачи.\n"
            "Пример: В память о маме... / Выздоравливай, папа...\n\n"
            "Если не нужно — напишите: -"
        )
        return

    if st.get("type") == "ind_collect_print":
        note = "" if raw == "-" else raw
        kind = st.get("kind")
        method = st.get("method", "manual")
        video = st.get("video", "")
        username = message.from_user.username or "-"

        if kind == "ind_iftar":
            day = int(st.get("day", 0))
            price = await kv_get_float("iftar_portion_eur", DEFAULT_IFTAR_PORTION_EUR)
            eur_amount = IND_IFTAR_FULL_PORTIONS * price
            await create_individual_order(
                kind="ind_iftar",
                user_id=uid,
                username=username,
                method=method,
                reserved_day=day,
                reserved_batch=0,
                amount_eur=eur_amount,
                amount_portions=IND_IFTAR_FULL_PORTIONS,
                label_for_video=video,
                note_for_print=note
            )
            await send_admin_html(
                "🎥 INDIVIDUAL IFTAR ORDER<br>"
                f"Day: {day}<br>"
                f"Portions: {IND_IFTAR_FULL_PORTIONS}<br>"
                f"Expected EUR: {eur_amount:.2f}<br>"
                f"Method: {h(method)}<br>"
                f"Video msg: <b>{h(video or '-') }</b><br>"
                f"Print note: <b>{h(note or '-') }</b><br>"
                f"User: @{h(username)} / {uid}<br>{user_link_html(uid)}"
            )
            PENDING.pop(uid, None)
            await message.answer("✅ Индивидуальная раздача ифтаров оформлена. Джазак Аллаху хейр! 🤍")
            return

        if kind == "ind_water":
            batch = int(st.get("batch", 0))
            eur_amount = float(IND_WATER_FULL_EUR)
            await create_individual_order(
                kind="ind_water",
                user_id=uid,
                username=username,
                method=method,
                reserved_day=0,
                reserved_batch=batch,
                amount_eur=eur_amount,
                amount_portions=0,
                label_for_video=video,
                note_for_print=note
            )
            await send_admin_html(
                "🎥 INDIVIDUAL WATER ORDER<br>"
                f"Batch: {batch}<br>"
                f"Amount: {IND_WATER_FULL_EUR} EUR<br>"
                f"Method: {h(method)}<br>"
                f"Video msg: <b>{h(video or '-') }</b><br>"
                f"Print note: <b>{h(note or '-') }</b><br>"
                f"User: @{h(username)} / {uid}<br>{user_link_html(uid)}"
            )
            PENDING.pop(uid, None)
            await message.answer("✅ Индивидуальная раздача воды оформлена. Джазак Аллаху хейр! 🤍")
            return

    await message.answer("Не поняла ввод. Попробуйте ещё раз.")


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
