import os
import re
import logging
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, LabeledPrice, PreCheckoutQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))
TZ = ZoneInfo("Europe/Helsinki")
EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")

PAYPAL_LINK = os.getenv("PAYPAL_LINK", "")
SEPA_RECIPIENT = os.getenv("SEPA_RECIPIENT", "")
SEPA_IBAN = os.getenv("SEPA_IBAN", "")
SEPA_BIC = os.getenv("SEPA_BIC", "")

ZEN_NAME = os.getenv("ZEN_NAME", "")
ZEN_PHONE = os.getenv("ZEN_PHONE", "")
ZEN_CARD = os.getenv("ZEN_CARD", "")
ZEN_IBAN = os.getenv("ZEN_IBAN", "")
ZEN_BIC = os.getenv("ZEN_BIC", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
DB_PATH = "data.db"

# user_id -> state
PENDING: dict[int, dict] = {}

FITR_OPEN_DT = datetime(2026, 3, 9, 0, 0, tzinfo=TZ)
FITR_PAYPAL_CLOSE_DT = datetime(2026, 3, 17, 23, 59, tzinfo=TZ)
FITR_ZEN_CLOSE_DT = datetime(2026, 3, 18, 14, 0, tzinfo=TZ)

EID_OPEN_DT = datetime(2026, 3, 9, 0, 0, tzinfo=TZ)
EID_CLOSE_DT = datetime(2026, 3, 18, 0, 0, tzinfo=TZ)
EID_EXTRA_CLOSE_DT = datetime(2026, 3, 19, 0, 0, tzinfo=TZ)


# =========================
# Helpers
# =========================

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en

def now_hki() -> datetime:
    return datetime.now(TZ)

def admin_only(user_id: int) -> bool:
    return bool(ADMIN_ID) and user_id == ADMIN_ID

def extract_positive_int(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r"\d+", text)
    if not m:
        return None
    n = int(m.group())
    return n if n > 0 else None

def parse_fitr_code(code: str) -> int | None:
    code = (code or "").strip().upper()
    m = re.fullmatch(r"ZF(\d+)", code)
    if not m:
        return None
    n = int(m.group(1))
    return n if n > 0 else None

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

async def notify_admin(text: str):
    if not ADMIN_ID:
        return
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logging.exception("notify_admin failed")


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
        CREATE TABLE IF NOT EXISTS fitr_people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            method TEXT NOT NULL,
            display_name TEXT NOT NULL,
            country TEXT NOT NULL,
            city TEXT NOT NULL,
            people_count INTEGER NOT NULL,
            amount_eur INTEGER NOT NULL,
            rice_kg INTEGER NOT NULL,
            code TEXT NOT NULL,
            comment TEXT NOT NULL
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS text_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            k TEXT NOT NULL,
            old_v TEXT NOT NULL,
            new_v TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """)

        defaults = {
            "water_target_eur": "235",
            "water_raised_eur": "0",
            "water_open_mode": "on",

            "iftar_day": "27",
            "iftar_target_portions": "800",
            "iftar_raised_portions": "0",
            "iftar_open_mode": "on",

            "fitr_saa_eur": "10",
            "fitr_open_mode": "auto",
            "fitr_reported_10kg": "0",

            "eid_open_mode": "auto",
            "eid_raised_eur": "0",
            "eid_target_eur": "0",
            "eid_extra_day": "off",

            "desc_water_ru": "Раздача 5000 л питьевой воды.",
            "desc_water_en": "Distribution of 5000 L of drinking water.",

            "desc_iftar_ru": "Сбор на ифтары текущего дня Рамадана.",
            "desc_iftar_en": "Collection for the current Ramadan iftar day.",

            "desc_fitr_ru": (
                "Мы распределяем Закят-уль-Фитр в Газе и иногда для опоздавших в палестинских лагерях Иордании.\n\n"
                "Сумма закят-уль-фитр: 10€ / 1 человек.\n"
                "Это цена 1 са'а = 3 кг риса.\n\n"
                "При переводе используйте код сбора: ZF и количество человек.\n"
                "Пример: ZF5"
            ),
            "desc_fitr_en": (
                "We distribute Zakat al-Fitr in Gaza and sometimes for late payers in Palestinian camps in Jordan.\n\n"
                "Amount of zakat-ul-fitr: 10€ / 1 person.\n"
                "Equal to price of 1 sa'a = 3 kg of rice.\n\n"
                "Use code ZF with the number of persons.\n"
                "Example: ZF5"
            ),

            "desc_eid_ru": "Сбор на сладкую традиционную выпечку «кяки» или что-то подобное, в честь праздника.",
            "desc_eid_en": "Collection for traditional sweet pastry “kyaky” or something similar for the holiday."
        }

        for k, v in defaults.items():
            await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES(?,?)", (k, v))

        await db.commit()

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

async def set_user_lang(user_id: int, lang: str):
    lang = "ru" if lang == "ru" else "en"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO user_prefs(user_id, lang) VALUES(?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (user_id, lang),
        )
        await db.commit()

async def get_user_lang(user_id: int) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT lang FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def add_text_history(key: str, old_v: str, new_v: str):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO text_history(k,old_v,new_v,ts) VALUES(?,?,?,?)",
            (key, old_v, new_v, ts),
        )
        await db.commit()

async def undo_last_text_change() -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id,k,old_v FROM text_history ORDER BY id DESC LIMIT 1") as cur:
            row = await cur.fetchone()
        if not row:
            return False
        row_id, key, old_v = row
        await db.execute(
            "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, old_v),
        )
        await db.execute("DELETE FROM text_history WHERE id=?", (row_id,))
        await db.commit()
        return True

async def fitr_totals() -> tuple[int, int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COALESCE(SUM(amount_eur),0), COALESCE(SUM(people_count),0), COALESCE(SUM(rice_kg),0) FROM fitr_people"
        ) as cur:
            row = await cur.fetchone()
            return int(row[0]), int(row[1]), int(row[2])

async def fitr_count_rows() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM fitr_people") as cur:
            row = await cur.fetchone()
            return int(row[0])

async def add_fitr_person(user_id: int, username: str, method: str, display_name: str, country: str, city: str,
                          people_count: int, amount_eur: int, code: str, comment: str = "") -> int:
    rice_kg = people_count * 3
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO fitr_people(
                ts,user_id,username,method,display_name,country,city,
                people_count,amount_eur,rice_kg,code,comment
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (ts, user_id, username or "", method, display_name, country, city,
             people_count, amount_eur, rice_kg, code, comment),
        )
        await db.commit()
        return cur.lastrowid

async def get_fitr_rows(limit: int = 200) -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id,display_name,country,city,amount_eur,code,rice_kg,method,comment
            FROM fitr_people
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ) as cur:
            return await cur.fetchall()

async def update_fitr_row(row_id: int, display_name: str, country: str, city: str,
                          people_count: int, amount_eur: int, method: str, code: str, comment: str):
    rice_kg = people_count * 3
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE fitr_people
            SET display_name=?, country=?, city=?, people_count=?, amount_eur=?, rice_kg=?, method=?, code=?, comment=?
            WHERE id=?
            """,
            (display_name, country, city, people_count, amount_eur, rice_kg, method, code, comment, row_id),
        )
        await db.commit()

async def delete_fitr_row(row_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM fitr_people WHERE id=?", (row_id,))
        await db.commit()

async def find_fitr_rows(term: str) -> list[tuple]:
    q = f"%{term}%"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id,display_name,country,city,amount_eur,code,rice_kg,method
            FROM fitr_people
            WHERE display_name LIKE ? OR country LIKE ? OR city LIKE ? OR code LIKE ?
            ORDER BY id ASC
            LIMIT 50
            """,
            (q, q, q, q),
        ) as cur:
            return await cur.fetchall()

async def possible_fitr_dups() -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT a.id, a.display_name, a.code, b.id, b.display_name, b.code
            FROM fitr_people a
            JOIN fitr_people b
              ON a.id < b.id
             AND (
                 (a.display_name = b.display_name AND a.code = b.code)
                 OR (a.amount_eur = b.amount_eur AND a.code = b.code)
             )
            LIMIT 50
            """
        ) as cur:
            return await cur.fetchall()

async def fitr_report_if_needed():
    total_eur, total_people, total_kg = await fitr_totals()
    reported = int(await kv_get("fitr_reported_10kg") or "0")
    blocks = total_kg // 10
    if blocks > reported:
        await kv_set("fitr_reported_10kg", str(blocks))
        await notify_admin(
            "📊 FITR REPORT\n"
            f"Total EUR: {total_eur}\n"
            f"People: {total_people}\n"
            f"Rice: {total_kg} kg"
        )


# =========================
# Open/close logic
# =========================

async def is_fitr_visible() -> bool:
    mode = (await kv_get("fitr_open_mode") or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    return now_hki() >= FITR_OPEN_DT

def fitr_method_open(method: str) -> bool:
    now = now_hki()
    if now < FITR_OPEN_DT:
        return False
    if method == "paypal":
        return now <= FITR_PAYPAL_CLOSE_DT
    if method in {"zenbank", "zenfast"}:
        return now <= FITR_ZEN_CLOSE_DT
    return False

async def is_eid_open() -> bool:
    mode = (await kv_get("eid_open_mode") or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    extra = (await kv_get("eid_extra_day") or "off").lower() == "on"
    close_dt = EID_EXTRA_CLOSE_DT if extra else EID_CLOSE_DT
    return EID_OPEN_DT <= now_hki() <= close_dt


# =========================
# Text builders
# =========================

async def water_text(lang: str) -> str:
    target = int(await kv_get("water_target_eur") or "235")
    raised = int(await kv_get("water_raised_eur") or "0")
    desc = await kv_get(f"desc_water_{lang}")
    remain = max(0, target - raised)
    bar = battery(raised, target)
    if lang == "ru":
        return (
            "💧 *Сукья-ль-ма (вода)*\n\n"
            f"{desc}\n\n"
            f"Цистерна: *{target}€*\n"
            f"Собрано: *{raised}€*\n"
            f"Осталось: *{remain}€*\n"
            f"{bar}\n\n"
            "Код оплаты: `Greenmax`"
        )
    return (
        "💧 *Sukya-l-ma (Water)*\n\n"
        f"{desc}\n\n"
        f"Tanker: *{target}€*\n"
        f"Raised: *{raised}€*\n"
        f"Remaining: *{remain}€*\n"
        f"{bar}\n\n"
        "Payment code: `Greenmax`"
    )

async def iftar_text(lang: str) -> str:
    day = int(await kv_get("iftar_day") or "27")
    target = int(await kv_get("iftar_target_portions") or "800")
    raised = int(await kv_get("iftar_raised_portions") or "0")
    desc = await kv_get(f"desc_iftar_{lang}")
    bar = battery(min(raised, target), target)
    if lang == "ru":
        return (
            f"🍲 *Ифтары — {day} Рамадана*\n\n"
            f"{desc}\n\n"
            f"Цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n\n"
            "Цена порции: *4€*\n"
            "Код оплаты: `Mimax`"
        )
    return (
        f"🍲 *Iftars — {day} of Ramadan*\n\n"
        f"{desc}\n\n"
        f"Goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n\n"
        "Portion price: *4€*\n"
        "Payment code: `Mimax`"
    )

async def fitr_text(lang: str) -> str:
    desc = await kv_get(f"desc_fitr_{lang}")
    total_eur, total_people, total_kg = await fitr_totals()
    count_rows = await fitr_count_rows()
    if lang == "ru":
        return (
            "🕌 *Закят-уль-Фитр (ZF)*\n\n"
            f"{desc}\n\n"
            f"В списке: *{count_rows}*\n"
            f"Сумма: *{total_eur}€*\n"
            f"Людей: *{total_people}*\n"
            f"Рис: *{total_kg} кг*"
        )
    return (
        "🕌 *Zakat al-Fitr (ZF)*\n\n"
        f"{desc}\n\n"
        f"In list: *{count_rows}*\n"
        f"Total: *{total_eur}€*\n"
        f"People: *{total_people}*\n"
        f"Rice: *{total_kg} kg*"
    )

async def eid_text(lang: str) -> str:
    desc = await kv_get(f"desc_eid_{lang}")
    raised = int(await kv_get("eid_raised_eur") or "0")
    target = int(await kv_get("eid_target_eur") or "0")
    if lang == "ru":
        s = (
            "🎁 *Ид — сладости детям (Id)*\n\n"
            f"{desc}\n\n"
            f"Собрано: *{raised}€*\n"
        )
        if target > 0:
            s += f"Цель: *{target}€*\n"
        return s
    s = (
        "🎁 *Eid sweets for children (Id)*\n\n"
        f"{desc}\n\n"
        f"Raised: *{raised}€*\n"
    )
    if target > 0:
        s += f"Goal: *{target}€*\n"
    return s


# =========================
# Keyboards
# =========================

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()

def kb_campaigns(lang: str, show_fitr: bool, show_eid: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💧 Вода (Greenmax)", "💧 Water (Greenmax)"), callback_data="camp_water")
    kb.button(text=t(lang, "🍲 Ифтары (Mimax)", "🍲 Iftars (Mimax)"), callback_data="camp_iftar")
    if show_fitr:
        kb.button(text=t(lang, "🕌 Закят-уль-Фитр (ZF)", "🕌 Zakat al-Fitr (ZF)"), callback_data="camp_fitr")
    if show_eid:
        kb.button(text=t(lang, "🎁 Ид — сладости детям (Id)", "🎁 Eid sweets (Id)"), callback_data="camp_eid")
    kb.button(text=t(lang, "🌐 Язык", "🌐 Language"), callback_data="go_lang")
    kb.adjust(1)
    return kb.as_markup()

def kb_admin_tools(lang: str, campaign: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "✏️ Править RU", "✏️ Edit RU"), callback_data=f"admin_edit|{campaign}|ru")
    kb.button(text=t(lang, "✏️ Править EN", "✏️ Edit EN"), callback_data=f"admin_edit|{campaign}|en")
    kb.button(text=t(lang, "↩️ Откатить текст", "↩️ Undo text"), callback_data="admin_undo_text")
    kb.adjust(1)
    return kb.as_markup()

def kb_fitr_members(lang: str):
    kb = InlineKeyboardBuilder()
    for n in [1, 2, 3, 4, 5]:
        kb.button(text=t(lang, f"{n} человек", f"{n} people"), callback_data=f"fitr_people_{n}")
    kb.button(text=t(lang, "Другое количество", "Other qty"), callback_data="fitr_people_other")
    kb.button(text=t(lang, "Способы оплаты", "Payment methods"), callback_data="fitr_methods")
    kb.button(text=t(lang, "Назад", "Back"), callback_data="go_campaigns")
    kb.button(text=t(lang, "Сброс", "Reset"), callback_data="reset_flow")
    kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()

def kb_fitr_methods(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="💙 PayPal", callback_data="fitr_method_paypal")
    kb.button(text=t(lang, "🏦 Zen перевод", "🏦 Zen bank"), callback_data="fitr_method_zenbank")
    kb.button(text=t(lang, "⚡ Zen Express", "⚡ Zen Express"), callback_data="fitr_method_zenfast")
    kb.button(text=t(lang, "Назад", "Back"), callback_data="camp_fitr")
    kb.button(text=t(lang, "Сброс", "Reset"), callback_data="reset_flow")
    kb.adjust(1)
    return kb.as_markup()

def kb_hidden_payment_details(lang: str, campaign: str, method: str, amount_eur: int, note: str):
    kb = InlineKeyboardBuilder()

    if method == "paypal":
        kb.button(text=t(lang, "💙 Ссылка PayPal", "💙 PayPal link"), callback_data="show_paypal_link")
    elif method == "zenbank":
        kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_zen_name")
        kb.button(text=t(lang, "🏦 IBAN", "🏦 IBAN"), callback_data="show_zen_iban")
        if ZEN_BIC:
            kb.button(text="BIC", callback_data="show_zen_bic")
    elif method == "zenfast":
        if ZEN_PHONE:
            kb.button(text=t(lang, "📱 Телефон", "📱 Phone"), callback_data="show_zen_phone")
        if ZEN_CARD:
            kb.button(text=t(lang, "💳 Карта", "💳 Card"), callback_data="show_zen_card")
        if ZEN_NAME:
            kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_zen_name")
    elif method == "sepa":
        kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_sepa_recipient")
        kb.button(text=t(lang, "🏦 IBAN", "🏦 IBAN"), callback_data="show_sepa_iban")
        if SEPA_BIC:
            kb.button(text="BIC", callback_data="show_sepa_bic")

    kb.button(text=t(lang, "📋 Скопировать код", "📋 Copy code"), callback_data=f"copy_note|{note}")
    kb.button(text=t(lang, "✅ Оплатил", "✅ Paid"), callback_data=f"manual_sent|{method}|{campaign}|{amount_eur}|{note}")
    kb.button(text=t(lang, "Назад", "Back"), callback_data=f"back_to_{campaign}")
    kb.button(text=t(lang, "Сброс", "Reset"), callback_data="reset_flow")
    kb.adjust(1)
    return kb.as_markup()

def kb_fitr_name_format(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "Умм …", "Umm …"), callback_data="fitr_fmt_umm")
    kb.button(text=t(lang, "Абу …", "Abu …"), callback_data="fitr_fmt_abu")
    kb.button(text=t(lang, "Имя …", "Name …"), callback_data="fitr_fmt_name")
    kb.button(text=t(lang, "Сброс", "Reset"), callback_data="reset_flow")
    kb.adjust(1)
    return kb.as_markup()


# =========================
# Start / basic navigation
# =========================

@dp.message(Command("start"))
async def start(message: Message):
    lang = await get_user_lang(message.from_user.id)
    if not lang:
        await message.answer("Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await message.answer(t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"lang_ru", "lang_en"}))
async def choose_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"go_lang", "go_campaigns", "reset_flow"}))
async def basic_nav(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    PENDING.pop(call.from_user.id, None)
    await call.answer()
    if call.data == "go_lang":
        await safe_edit(call, "Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"camp_water", "camp_iftar", "camp_fitr", "camp_eid"}))
async def open_campaign(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    if call.data == "camp_fitr":
        await safe_edit(call, await fitr_text(lang), parse_mode="Markdown", reply_markup=kb_fitr_members(lang))
        if admin_only(call.from_user.id):
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "fitr"))
        return

    if call.data == "camp_water":
        await safe_edit(call, await water_text(lang), parse_mode="Markdown")
        if admin_only(call.from_user.id):
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "water"))
        return

    if call.data == "camp_iftar":
        await safe_edit(call, await iftar_text(lang), parse_mode="Markdown")
        if admin_only(call.from_user.id):
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "iftar"))
        return

    await safe_edit(call, await eid_text(lang), parse_mode="Markdown")
    if admin_only(call.from_user.id):
        await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "eid"))


# =========================
# Fitr user flow
# =========================

@dp.callback_query(F.data == "fitr_methods")
async def fitr_methods(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()
    await call.message.answer(t(lang, "Выберите способ оплаты:", "Choose payment method:"), reply_markup=kb_fitr_methods(lang))

@dp.callback_query(F.data.startswith("fitr_people_"))
async def fitr_people(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    if call.data == "fitr_people_other":
        PENDING[call.from_user.id] = {"kind": "fitr_people_other"}
        await call.message.answer(t(lang, "Введите только число. Пример: 5", "Enter only a number. Example: 5"))
        return

    people = int(call.data.split("_")[-1])
    PENDING[call.from_user.id] = {"fitr_people": people}
    price = int(await kv_get("fitr_saa_eur") or "10")
    eur = people * price
    kg = people * 3
    code = f"ZF{people}"
    await call.message.answer(
        t(
            lang,
            f"Вам необходимо раздать: *{kg} кг*\nСумма к оплате: *{eur}€*\nКод оплаты: `{code}`",
            f"You need to distribute: *{kg} kg*\nAmount to pay: *{eur}€*\nPayment code: `{code}`"
        ),
        parse_mode="Markdown",
        reply_markup=kb_fitr_methods(lang)
    )

@dp.callback_query(F.data.startswith("fitr_method_"))
async def fitr_method(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    method = call.data.replace("fitr_method_", "")
    people = PENDING.get(call.from_user.id, {}).get("fitr_people")

    await call.answer()

    if not people:
        await call.message.answer(t(lang, "Сначала выберите количество людей.", "Choose number of people first."))
        return

    if not fitr_method_open(method):
        await call.message.answer(fitr_close_text(method, lang))
        return

    price = int(await kv_get("fitr_saa_eur") or "10")
    eur = people * price
    code = f"ZF{people}"

    await call.message.answer(
        t(
            lang,
            f"Сумма к оплате: *{eur}€*\nКод оплаты: `{code}`",
            f"Amount to pay: *{eur}€*\nPayment code: `{code}`"
        ),
        parse_mode="Markdown",
        reply_markup=kb_hidden_payment_details(lang, "fitr", method, eur, code)
    )

@dp.callback_query(F.data.startswith("manual_sent|"))
async def manual_sent(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    _, method, campaign, amount_eur, note = call.data.split("|", 4)
    amount_eur = int(amount_eur)

    if campaign == "fitr":
        people = parse_fitr_code(note) or max(1, amount_eur // 10)
        PENDING[call.from_user.id] = {
            "kind": "fitr_identity",
            "method": method,
            "amount_eur": amount_eur,
            "people_count": people,
            "code": note,
        }
        await call.message.answer(
            t(lang, "Чтобы вы видели себя в списке на раздачу фитра, выберите формат.", "Choose how you want to appear in the fitr list."),
            reply_markup=kb_fitr_name_format(lang)
        )
        return

    await call.message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")

@dp.callback_query(F.data.in_({"fitr_fmt_umm", "fitr_fmt_abu", "fitr_fmt_name"}))
async def fitr_format_choice(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    ctx = PENDING.get(call.from_user.id)
    if not ctx or ctx.get("kind") != "fitr_identity":
        await call.answer()
        return
    mapping = {"fitr_fmt_umm": "umm", "fitr_fmt_abu": "abu", "fitr_fmt_name": "name"}
    ctx["fmt"] = mapping[call.data]
    ctx["step"] = "name"
    PENDING[call.from_user.id] = ctx
    await call.answer()
    await call.message.answer(t(lang, "Имя или инициалы (обязательно):", "Name or initials (required):"))


# =========================
# Hidden values
# =========================

@dp.callback_query(F.data.in_({
    "show_paypal_link",
    "show_zen_name", "show_zen_iban", "show_zen_bic", "show_zen_phone", "show_zen_card",
    "show_sepa_recipient", "show_sepa_iban", "show_sepa_bic",
}))
async def show_hidden_detail(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    mapping = {
        "show_paypal_link": PAYPAL_LINK,
        "show_zen_name": ZEN_NAME,
        "show_zen_iban": ZEN_IBAN,
        "show_zen_bic": ZEN_BIC,
        "show_zen_phone": ZEN_PHONE,
        "show_zen_card": ZEN_CARD,
        "show_sepa_recipient": SEPA_RECIPIENT,
        "show_sepa_iban": SEPA_IBAN,
        "show_sepa_bic": SEPA_BIC,
    }
    val = mapping.get(call.data, "")
    if not val:
        await call.message.answer(t(lang, "Не настроено.", "Not configured."))
        return
    await call.message.answer(f"`{val}`", parse_mode="Markdown")

@dp.callback_query(F.data.startswith("copy_note|"))
async def copy_note(call: CallbackQuery):
    await call.answer()
    note = call.data.split("|", 1)[1]
    await call.message.answer(f"`{note}`", parse_mode="Markdown")

@dp.callback_query(F.data.in_({"back_to_fitr", "back_to_water", "back_to_iftar", "back_to_eid"}))
async def back_to_campaign_short(call: CallbackQuery):
    await call.answer()
    target = call.data.replace("back_to_", "")
    mapping = {
        "fitr": "camp_fitr",
        "water": "camp_water",
        "iftar": "camp_iftar",
        "eid": "camp_eid",
    }
    fake = CallbackQuery(
        id=call.id,
        from_user=call.from_user,
        chat_instance=call.chat_instance,
        message=call.message,
        data=mapping[target]
    )
    await open_campaign(fake)


# =========================
# Text input states
# =========================

@dp.message(F.text)
async def text_input(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    ctx = PENDING.get(message.from_user.id)
    if not ctx:
        return

    raw = (message.text or "").strip()

    if ctx["kind"] == "edit_text":
        key = ctx["key"]
        old_v = await kv_get(key)
        await kv_set(key, raw)
        await add_text_history(key, old_v, raw)
        PENDING.pop(message.from_user.id, None)
        await message.answer("OK")
        return

    if ctx["kind"] == "fitr_people_other":
        n = extract_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Введите только число. Пример: 5", "Enter only a number. Example: 5"))
            return
        PENDING[message.from_user.id] = {"fitr_people": n}
        price = int(await kv_get("fitr_saa_eur") or "10")
        eur = n * price
        kg = n * 3
        code = f"ZF{n}"
        await message.answer(
            t(
                lang,
                f"Вам необходимо раздать: *{kg} кг*\nСумма к оплате: *{eur}€*\nКод оплаты: `{code}`",
                f"You need to distribute: *{kg} kg*\nAmount to pay: *{eur}€*\nPayment code: `{code}`"
            ),
            parse_mode="Markdown",
            reply_markup=kb_fitr_methods(lang)
        )
        return

    if ctx["kind"] == "fitr_identity":
        if ctx.get("step") == "name":
            if not raw:
                await message.answer(t(lang, "Введите имя или инициалы.", "Enter name or initials."))
                return
            ctx["name"] = raw
            ctx["step"] = "country"
            PENDING[message.from_user.id] = ctx
            await message.answer(t(lang, "Страна? Если не хотите указывать, отправьте -", "Country? Send - to skip"))
            return

        if ctx.get("step") == "country":
            ctx["country"] = "" if raw == "-" else raw
            ctx["step"] = "city"
            PENDING[message.from_user.id] = ctx
            await message.answer(t(lang, "Город? Если не хотите указывать, отправьте -", "City? Send - to skip"))
            return

        if ctx.get("step") == "city":
            city = "" if raw == "-" else raw
            fmt = ctx.get("fmt", "name")
            name = ctx.get("name", "")
            country = ctx.get("country", "")

            if fmt == "umm":
                display_name = f"Умм {name}" if lang == "ru" else f"Umm {name}"
            elif fmt == "abu":
                display_name = f"Абу {name}" if lang == "ru" else f"Abu {name}"
            else:
                display_name = name

            row_id = await add_fitr_person(
                message.from_user.id,
                message.from_user.username or "",
                ctx["method"],
                display_name,
                country,
                city,
                int(ctx["people_count"]),
                int(ctx["amount_eur"]),
                ctx["code"],
                "",
            )
            await fitr_report_if_needed()
            PENDING.pop(message.from_user.id, None)

            total_eur, total_people, total_kg = await fitr_totals()
            await notify_admin(
                "📩 FITR LIST UPDATED\n"
                f"№: {row_id}\n"
                f"Name: {display_name}\n"
                f"Country: {country or '-'}\n"
                f"City: {city or '-'}\n"
                f"Method: {ctx['method']}\n"
                f"Amount: {ctx['amount_eur']} EUR\n"
                f"People: {ctx['people_count']}\n"
                f"Kg: {int(ctx['people_count']) * 3}\n"
                f"Code: {ctx['code']}\n\n"
                f"TOTALS -> EUR: {total_eur}, PEOPLE: {total_people}, KG: {total_kg}"
            )
            await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
            return


# =========================
# Stars
# =========================

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")


# =========================
# Admin commands
# =========================

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not admin_only(message.from_user.id):
        return
    txt = (
        "/fitr\n"
        "/iftars\n"
        "/water\n"
        "/eid\n\n"
        "/fitr text\n"
        "/fitr list\n"
        "/fitr add Имя;ZF5;paypal;Страна;Город;коммент\n"
        "/fitr edit ID;Имя;ZF5;paypal;Страна;Город;коммент\n"
        "/fitr del ID\n"
        "/fitr find ТЕКСТ\n"
        "/fitr dup\n"
        "/fitr price 10\n\n"
        "/undo\n"
    )
    await message.answer(txt)

@dp.message(Command("undo"))
async def cmd_undo(message: Message):
    if not admin_only(message.from_user.id):
        return
    ok = await undo_last_text_change()
    await message.answer("OK" if ok else "No changes")

@dp.message(Command("fitr"))
async def cmd_fitr_admin_short(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    await message.answer(await fitr_text(lang), parse_mode="Markdown", reply_markup=kb_fitr_members(lang))

@dp.message(Command("iftars"))
async def cmd_iftars_admin_short(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    await message.answer(await iftar_text(lang), parse_mode="Markdown")

@dp.message(Command("water"))
async def cmd_water_admin_short(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    await message.answer(await water_text(lang), parse_mode="Markdown")

@dp.message(Command("eid"))
async def cmd_eid_admin_short(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    await message.answer(await eid_text(lang), parse_mode="Markdown")

@dp.message(F.text.regexp(r"^/fitr\s+text$"))
async def admin_fitr_text(message: Message):
    if not admin_only(message.from_user.id):
        return
    key = "desc_fitr_ru"
    current = await kv_get(key)
    PENDING[message.from_user.id] = {"kind": "edit_text", "key": key}
    await message.answer(f"Текущий текст:\n\n{current}\n\nОтправьте новый текст одним сообщением.")

@dp.message(F.text.regexp(r"^/fitr\s+price\s+\d+$"))
async def admin_fitr_price(message: Message):
    if not admin_only(message.from_user.id):
        return
    n = extract_positive_int(message.text)
    await kv_set("fitr_saa_eur", str(n))
    await message.answer("OK")

@dp.message(F.text.regexp(r"^/fitr\s+list$"))
async def admin_fitr_list(message: Message):
    if not admin_only(message.from_user.id):
        return
    rows = await get_fitr_rows()
    if not rows:
        await message.answer("Список пуст.")
        return
    lines = []
    for r in rows:
        row_id, display_name, country, city, amount_eur, code, rice_kg, method, comment = r
        place = ", ".join([x for x in [country, city] if x])
        s = f"{row_id}. {display_name}"
        if place:
            s += f" ({place})"
        s += f" — {amount_eur}€ — {code} — {rice_kg} кг — {method}"
        if comment:
            s += f" — {comment}"
        lines.append(s)
    await message.answer("\n".join(lines[:80]))

@dp.message(F.text.regexp(r"^/fitr\s+find\s+.+$"))
async def admin_fitr_find(message: Message):
    if not admin_only(message.from_user.id):
        return
    term = re.sub(r"^/fitr\s+find\s+", "", message.text.strip(), flags=re.I)
    rows = await find_fitr_rows(term)
    if not rows:
        await message.answer("Ничего не найдено.")
        return
    lines = []
    for r in rows:
        row_id, display_name, country, city, amount_eur, code, rice_kg, method = r
        place = ", ".join([x for x in [country, city] if x])
        s = f"{row_id}. {display_name}"
        if place:
            s += f" ({place})"
        s += f" — {amount_eur}€ — {code} — {rice_kg} кг — {method}"
        lines.append(s)
    await message.answer("\n".join(lines[:50]))

@dp.message(F.text.regexp(r"^/fitr\s+dup$"))
async def admin_fitr_dup(message: Message):
    if not admin_only(message.from_user.id):
        return
    rows = await possible_fitr_dups()
    if not rows:
        await message.answer("Дублей не найдено.")
        return
    lines = [f"{a}. {an} {ac}  <->  {b}. {bn} {bc}" for a, an, ac, b, bn, bc in rows]
    await message.answer("\n".join(lines[:50]))

@dp.message(F.text.regexp(r"^/fitr\s+add\s+.+$"))
async def admin_fitr_add(message: Message):
    if not admin_only(message.from_user.id):
        return
    raw = re.sub(r"^/fitr\s+add\s+", "", message.text.strip(), flags=re.I)
    parts = [x.strip() for x in raw.split(";")]

    if len(parts) < 2:
        await message.answer("Использование: /fitr add Имя;ZF5;paypal;Страна;Город;коммент")
        return

    display_name = parts[0]
    code = parts[1].upper()
    people = parse_fitr_code(code)
    if not people:
        await message.answer("Код должен быть вида ZF5")
        return

    method = parts[2] if len(parts) > 2 and parts[2] else "paypal"
    country = parts[3] if len(parts) > 3 and parts[3] != "-" else ""
    city = parts[4] if len(parts) > 4 and parts[4] != "-" else ""
    comment = parts[5] if len(parts) > 5 and parts[5] != "-" else ""
    amount = people * int(await kv_get("fitr_saa_eur") or "10")

    row_id = await add_fitr_person(ADMIN_ID, "admin", method, display_name, country, city, people, amount, code, comment)
    await fitr_report_if_needed()
    await message.answer(f"OK #{row_id}")

@dp.message(F.text.regexp(r"^/fitr\s+edit\s+.+$"))
async def admin_fitr_edit(message: Message):
    if not admin_only(message.from_user.id):
        return
    raw = re.sub(r"^/fitr\s+edit\s+", "", message.text.strip(), flags=re.I)
    parts = [x.strip() for x in raw.split(";")]

    if len(parts) < 3:
        await message.answer("Использование: /fitr edit ID;Имя;ZF5;paypal;Страна;Город;коммент")
        return

    row_id = int(parts[0])
    display_name = parts[1]
    code = parts[2].upper()
    people = parse_fitr_code(code)
    if not people:
        await message.answer("Код должен быть вида ZF5")
        return

    method = parts[3] if len(parts) > 3 and parts[3] else "paypal"
    country = parts[4] if len(parts) > 4 and parts[4] != "-" else ""
    city = parts[5] if len(parts) > 5 and parts[5] != "-" else ""
    comment = parts[6] if len(parts) > 6 and parts[6] != "-" else ""
    amount = people * int(await kv_get("fitr_saa_eur") or "10")

    await update_fitr_row(row_id, display_name, country, city, people, amount, method, code, comment)
    await fitr_report_if_needed()
    await message.answer("OK")

@dp.message(F.text.regexp(r"^/fitr\s+del\s+\d+$"))
async def admin_fitr_del(message: Message):
    if not admin_only(message.from_user.id):
        return
    row_id = extract_positive_int(message.text)
    await delete_fitr_row(row_id)
    await message.answer("OK")


# =========================
# Health
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

async def main():
    await db_init()
    await health_server()
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())
