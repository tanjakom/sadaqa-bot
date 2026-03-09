import os
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
ZEN_IBAN = os.getenv("ZEN_IBAN", SEPA_IBAN)
ZEN_BIC = os.getenv("ZEN_BIC", SEPA_BIC)

USDT_TRC20 = os.getenv("USDT_TRC20", "")
USDC_ERC20 = os.getenv("USDC_ERC20", "")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
DB_PATH = "data.db"
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

def now_hki() -> datetime:
    return datetime.now(TZ)

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en

def user_link(user_id: int) -> str:
    return f"tg://user?id={user_id}"

def admin_only_user(user_id: int) -> bool:
    return bool(ADMIN_ID) and user_id == ADMIN_ID

def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)

def done_list(s: str) -> list[int]:
    if not s:
        return []
    return [int(x) for x in s.split(",") if x.isdigit()]

def done_str(lst: list[int]) -> str:
    return ",".join(map(str, sorted(set(lst))))

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
        logging.exception("Failed to notify admin")


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
        CREATE TABLE IF NOT EXISTS text_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            k TEXT NOT NULL,
            old_v TEXT NOT NULL,
            new_v TEXT NOT NULL,
            ts TEXT NOT NULL
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS manual_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            method TEXT NOT NULL,
            campaign TEXT NOT NULL,
            amount_eur INTEGER NOT NULL,
            note TEXT NOT NULL
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
            code TEXT NOT NULL
        )
        """)

        defaults = {
            "water_target_eur": "235",
            "water_raised_eur": "0",

            "iftar_day": "27",
            "iftar_target_portions": "800",
            "iftar_raised_portions": "0",
            "iftar_done_days": "",

            "fitr_saa_eur": "10",
            "fitr_raised_eur": "0",
            "fitr_open_mode": "auto",
            "fitr_reported_10kg": "0",

            "eid_target_eur": "0",
            "eid_raised_eur": "0",
            "eid_open_mode": "auto",
            "eid_extra_day": "off",

            "test_mode": "off",

            "desc_water_ru": "Раздача 5000 л питьевой воды.",
            "desc_water_en": "Distribution of 5000 L of drinking water.",

            "desc_iftar_ru": "Сбор на ифтары текущего дня Рамадана.",
            "desc_iftar_en": "Collection for the current Ramadan iftar day.",

            "desc_fitr_ru": (
                "Мы распределяем Закят-уль-Фитр в Газе и иногда для опоздавших "
                "в палестинских лагерях Иордании.\n\n"
                "Сумма закят-уль-фитр: 10€ / 1 человек.\n"
                "Это цена 1 са'а = 3 кг риса.\n\n"
                "При переводе используйте код сбора: ZF и количество человек.\n"
                "Пример: ZF5"
            ),
            "desc_fitr_en": (
                "We distribute Zakat al-Fitr in Gaza and sometimes for late payers "
                "in Palestinian camps in Jordan.\n\n"
                "Amount of zakat-ul-fitr: 10€ / 1 person.\n"
                "Equal to price of 1 sa'a = 3 kg of rice.\n\n"
                "With transferring use code of the campaign: ZF with number of persons.\n"
                "Example: ZF5"
            ),

            "desc_eid_ru": (
                "Сбор на сладкую традиционную выпечку «кяки» "
                "или что-то подобное, в честь праздника."
            ),
            "desc_eid_en": (
                "Collection for traditional sweet pastry “kyaky” "
                "or something similar for the holiday."
            ),
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

async def kv_inc_int(key: str, delta: int):
    v = int(await kv_get(key) or "0")
    await kv_set(key, str(v + int(delta)))

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

async def add_manual_payment(user_id: int, username: str, method: str, campaign: str, amount_eur: int, note: str) -> int:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO manual_payments(ts,user_id,username,method,campaign,amount_eur,note) VALUES(?,?,?,?,?,?,?)",
            (ts, user_id, username or "", method, campaign, int(amount_eur), note),
        )
        await db.commit()
        return cur.lastrowid

async def add_fitr_person(user_id: int, username: str, method: str, display_name: str, country: str, city: str,
                          people_count: int, amount_eur: int, code: str) -> int:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rice_kg = people_count * 3
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO fitr_people(ts,user_id,username,method,display_name,country,city,people_count,amount_eur,rice_kg,code) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (ts, user_id, username or "", method, display_name, country, city, people_count, amount_eur, rice_kg, code),
        )
        await db.commit()
        return cur.lastrowid

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

async def get_fitr_rows(limit: int = 100) -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id,display_name,country,city,amount_eur,code,rice_kg,method FROM fitr_people ORDER BY id ASC LIMIT ?",
            (limit,),
        ) as cur:
            return await cur.fetchall()

async def update_fitr_row(row_id: int, display_name: str, country: str, city: str,
                          people_count: int, amount_eur: int, method: str, code: str):
    rice_kg = people_count * 3
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE fitr_people SET display_name=?, country=?, city=?, people_count=?, amount_eur=?, rice_kg=?, method=?, code=? WHERE id=?",
            (display_name, country, city, people_count, amount_eur, rice_kg, method, code, row_id),
        )
        await db.commit()

async def delete_fitr_row(row_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM fitr_people WHERE id=?", (row_id,))
        await db.commit()

async def reset_test_data():
    async with aiosqlite.connect(DB_PATH) as db:
        for key in ["water_raised_eur", "iftar_raised_portions", "fitr_raised_eur", "eid_raised_eur", "fitr_reported_10kg"]:
            await db.execute("UPDATE kv SET v='0' WHERE k=?", (key,))
        await db.execute("DELETE FROM fitr_people")
        await db.execute("DELETE FROM manual_payments")
        await db.commit()


# ================= Open/close logic =================

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

def fitr_close_text(method: str, lang: str) -> str:
    if method == "paypal":
        return t(lang, "PayPal для Закят-уль-Фитр закрыт 17 марта в 23:59.", "PayPal for Zakat al-Fitr closed on March 17 at 23:59.")
    if method in {"zenbank", "zenfast"}:
        return t(lang, "ZEN для Закят-уль-Фитр закрыт 18 марта в 14:00.", "ZEN for Zakat al-Fitr closed on March 18 at 14:00.")
    return t(lang, "Для этого сбора доступны только PayPal, Zen и Zen Express.", "Only PayPal, Zen and Zen Express are available for this campaign.")

async def is_eid_open() -> bool:
    mode = (await kv_get("eid_open_mode") or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    extra = (await kv_get("eid_extra_day") or "off").lower() == "on"
    close_dt = EID_EXTRA_CLOSE_DT if extra else EID_CLOSE_DT
    return EID_OPEN_DT <= now_hki() <= close_dt


# ================= Text builders =================

async def water_text(lang: str) -> str:
    target = int(await kv_get("water_target_eur") or "235")
    raised = int(await kv_get("water_raised_eur") or "0")
    desc = await kv_get(f"desc_water_{lang}")
    bar = battery(raised, target)
    remain = max(0, target - raised)
    code = "GREENMAX"
    if lang == "ru":
        return (
            "💧 *Сукья-ль-ма (вода)*\n\n"
            f"{desc}\n\n"
            f"Цистерна: *235€*\n"
            f"Собрано: *{raised}€*\n"
            f"Осталось: *{remain}€*\n"
            f"{bar}\n\n"
            f"Код: `{code}`"
        )
    return (
        "💧 *Sukya-l-ma (Water)*\n\n"
        f"{desc}\n\n"
        "Tanker: *235€*\n"
        f"Raised: *{raised}€*\n"
        f"Remaining: *{remain}€*\n"
        f"{bar}\n\n"
        f"Code: `{code}`"
    )

async def iftar_text(lang: str) -> str:
    day = int(await kv_get("iftar_day") or "27")
    target = int(await kv_get("iftar_target_portions") or "800")
    raised = int(await kv_get("iftar_raised_portions") or "0")
    desc = await kv_get(f"desc_iftar_{lang}")
    done = done_list(await kv_get("iftar_done_days"))
    bar = battery(min(raised, target), target)
    code = "Mimax"
    if lang == "ru":
        done_line = f"\nЗакрытые дни: {', '.join(map(str, done))}" if done else ""
        return (
            f"🍲 *Ифтары — {day} Рамадана*\n\n"
            f"{desc}\n\n"
            f"Цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n"
            f"{done_line}\n\n"
            "Цена порции: *4€*\n"
            f"Код оплаты: `{code}`"
        )
    done_line = f"\nClosed days: {', '.join(map(str, done))}" if done else ""
    return (
        f"🍲 *Iftars — {day} of Ramadan*\n\n"
        f"{desc}\n\n"
        f"Goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n"
        f"{done_line}\n\n"
        "Portion price: *4€*\n"
        f"Payment code: `{code}`"
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
    extra = (await kv_get("eid_extra_day") or "off").lower() == "on"
    close_ru = "19 марта 00:00" if extra else "18 марта 00:00"
    close_en = "March 19 00:00" if extra else "March 18 00:00"

    if lang == "ru":
        s = (
            "🎁 *Ид — сладости детям (Id)*\n\n"
            f"{desc}\n\n"
            f"Собрано: *{raised}€*\n"
        )
        if target > 0:
            s += f"Цель: *{target}€*\n"
        s += f"\nЗакрытие: *{close_ru}*"
        return s

    s = (
        "🎁 *Eid sweets for children (Id)*\n\n"
        f"{desc}\n\n"
        f"Raised: *{raised}€*\n"
    )
    if target > 0:
        s += f"Goal: *{target}€*\n"
    s += f"\nClose: *{close_en}*"
    return s


# ================= Keyboards =================

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()

def kb_campaigns(lang: str, show_fitr: bool, show_eid: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💧 Вода (GREENMAX)", "💧 Water (GREENMAX)"), callback_data="camp_water")
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
    kb.button(text=t(lang, "↩️ Сбросить последнее", "↩️ Undo last"), callback_data="admin_undo_text")
    kb.button(text=t(lang, "🧪 Сброс", "🧪 Reset"), callback_data="admin_reset_test")
    kb.adjust(1)
    return kb.as_markup()

def kb_choose_payment(lang: str, campaign: str):
    kb = InlineKeyboardBuilder()
    if campaign == "fitr":
        kb.button(text="💙 PayPal", callback_data=f"pm|{campaign}|paypal")
        kb.button(text=t(lang, "🏦 Zen перевод", "🏦 Zen bank transfer"), callback_data=f"pm|{campaign}|zenbank")
        kb.button(text=t(lang, "⚡ Zen Express", "⚡ Zen Express"), callback_data=f"pm|{campaign}|zenfast")
    else:
        kb.button(text="⭐ Telegram Stars", callback_data=f"pm|{campaign}|stars")
        kb.button(text="🏦 SEPA", callback_data=f"pm|{campaign}|sepa")
        kb.button(text="💙 PayPal", callback_data=f"pm|{campaign}|paypal")
        kb.button(text="💎 Crypto", callback_data=f"pm|{campaign}|crypto")
        kb.button(text=t(lang, "🏦 Zen перевод", "🏦 Zen bank transfer"), callback_data=f"pm|{campaign}|zenbank")
        kb.button(text=t(lang, "⚡ Zen Express", "⚡ Zen Express"), callback_data=f"pm|{campaign}|zenfast")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(1)
    return kb.as_markup()

def kb_amounts_eur(lang: str, campaign: str, amounts: list[int], extra_buttons: list[tuple[str, str]] | None = None):
    kb = InlineKeyboardBuilder()
    for a in amounts:
        kb.button(text=f"{a}€", callback_data=f"amt|{campaign}|eur|{a}")
    if extra_buttons:
        for txt, data in extra_buttons:
            kb.button(text=txt, callback_data=data)
    kb.button(text=t(lang, "Другая сумма", "Other amount"), callback_data=f"amt|{campaign}|eur|other")
    kb.button(text=t(lang, "Способы оплаты", "Payment methods"), callback_data=f"back_to_pm|{campaign}")
    kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()

def kb_iftar_options(lang: str, is_admin: bool, closed100: bool, remain_portions: int):
    kb = InlineKeyboardBuilder()
    for n in [5, 10, 20, 50]:
        kb.button(text=t(lang, f"{n} порций", f"{n} portions"), callback_data=f"amt|iftar|portions|{n}")
    kb.button(text=t(lang, "Указать порции", "Custom portions"), callback_data="amt|iftar|portions|other")
    kb.button(text=t(lang, "Указать сумму", "Custom amount"), callback_data="amt|iftar|eur|other")
    kb.button(text=t(lang, "Оплатить остаток", "Pay remaining"),
              callback_data=f"amt|iftar|portions|{max(1, remain_portions)}")
    if is_admin and closed100:
        kb.button(text=t(lang, "➕ +50 порций", "➕ +50 portions"), callback_data="admin_iftar_plus50")
    kb.button(text=t(lang, "Способы оплаты", "Payment methods"), callback_data="back_to_pm|iftar")
    kb.adjust(2, 2, 1, 1, 1, 1)
    return kb.as_markup()

def kb_fitr_members(lang: str):
    kb = InlineKeyboardBuilder()
    for n in [1, 2, 3, 4, 5]:
        kb.button(text=t(lang, f"{n} человек", f"{n} people"), callback_data=f"amt|fitr|people|{n}")
    kb.button(text=t(lang, "Другое количество", "Other qty"), callback_data="amt|fitr|people|other")
    kb.button(text=t(lang, "Способы оплаты", "Payment methods"), callback_data="back_to_pm|fitr")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()

def kb_hidden_payment_details(lang: str, campaign: str, method: str, amount_eur: int, note: str):
    kb = InlineKeyboardBuilder()
    if method == "sepa":
        kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_sepa_recipient")
        kb.button(text=t(lang, "🏦 IBAN", "🏦 IBAN"), callback_data="show_sepa_iban")
        if SEPA_BIC:
            kb.button(text="BIC", callback_data="show_sepa_bic")
    elif method == "paypal":
        kb.button(text=t(lang, "💙 Ссылка PayPal", "💙 PayPal link"), callback_data="show_paypal_link")
    elif method == "zenbank":
        kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_zen_name")
        kb.button(text=t(lang, "🏦 IBAN", "🏦 IBAN"), callback_data="show_zen_iban")
        if ZEN_BIC:
            kb.button(text="BIC", callback_data="show_zen_bic")
    elif method == "zenfast":
        if ZEN_PHONE:
            kb.button(text=t(lang, "📱 Телефон", "📱 Phone"), callback_data="show_zen_phone")
        if ZEN_NAME:
            kb.button(text=t(lang, "👤 Получатель", "👤 Recipient"), callback_data="show_zen_name")
        if ZEN_CARD:
            kb.button(text=t(lang, "💳 Карта", "💳 Card"), callback_data="show_zen_card")
    elif method == "crypto":
        kb.button(text="USDT (TRC20)", callback_data="show_usdt")
        kb.button(text="USDC (ERC20)", callback_data="show_usdc")

    kb.button(text=t(lang, "📋 Скопировать код", "📋 Copy code"), callback_data=f"copy_note|{note}")
    kb.button(text=t(lang, "✅ Оплатил", "✅ Paid"), callback_data=f"manual_sent|{method}|{campaign}|{amount_eur}|{note}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data=f"back_to_pm|{campaign}")
    kb.adjust(1)
    return kb.as_markup()

def kb_fitr_name_format(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "Умм …", "Umm …"), callback_data="fitr_fmt_umm")
    kb.button(text=t(lang, "Абу …", "Abu …"), callback_data="fitr_fmt_abu")
    kb.button(text=t(lang, "Имя …", "Name …"), callback_data="fitr_fmt_name")
    kb.adjust(1)
    return kb.as_markup()


# ================= Start / campaign flow =================

@dp.message(Command("start"))
async def start(message: Message):
    lang = await get_user_lang(message.from_user.id)
    if not lang:
        await message.answer("Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await message.answer(t(lang, "Выберите сбор:", "Choose campaign:"),
                         reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"lang_ru", "lang_en"}))
async def choose_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"),
                    reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"go_lang", "go_campaigns"}))
async def go_basic(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()
    if call.data == "go_lang":
        await safe_edit(call, "Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return
    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"),
                    reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(F.data.in_({"camp_water", "camp_iftar", "camp_fitr", "camp_eid"}))
async def open_campaign(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    if call.data == "camp_water":
        txt = await water_text(lang)
        raised = int(await kv_get("water_raised_eur") or "0")
        remain = max(0, 235 - raised)
        kb = kb_amounts_eur(lang, "water", [10, 25, 50], [
            (t(lang, "Индивидуально — 235€", "Individual — 235€"), "amt|water|eur|235"),
            (t(lang, f"Оплатить остаток — {remain}€", f"Pay remaining — {remain}€"), f"amt|water|eur|{remain}")
        ])
        if admin_only_user(call.from_user.id):
            await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "water"))
            return
        await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
        return

    if call.data == "camp_iftar":
        txt = await iftar_text(lang)
        raised = int(await kv_get("iftar_raised_portions") or "0")
        target = int(await kv_get("iftar_target_portions") or "800")
        remain_portions = max(1, target - raised)
        kb = kb_iftar_options(lang, admin_only_user(call.from_user.id), raised >= 100, remain_portions)
        if admin_only_user(call.from_user.id):
            await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "iftar"))
            return
        await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
        return

    if call.data == "camp_fitr":
        txt = await fitr_text(lang)
        kb = kb_fitr_members(lang)
        if admin_only_user(call.from_user.id):
            await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
            await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "fitr"))
            return
        await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
        return

    txt = await eid_text(lang)
    kb = kb_amounts_eur(lang, "eid", [5, 10, 25, 50])
    if admin_only_user(call.from_user.id):
        await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)
        await call.message.answer("Admin", reply_markup=kb_admin_tools(lang, "eid"))
        return
    await safe_edit(call, txt, parse_mode="Markdown", reply_markup=kb)


# ================= Admin editing / reset =================

@dp.callback_query(F.data.startswith("admin_edit|"))
async def admin_edit_start(call: CallbackQuery):
    if not admin_only_user(call.from_user.id):
        await call.answer()
        return
    _, campaign, lang = call.data.split("|")
    key = f"desc_{campaign}_{lang}"
    current = await kv_get(key)
    PENDING[call.from_user.id] = {"kind": "edit_text", "key": key}
    await call.answer()
    await call.message.answer(f"Текущий текст `{key}`:\n\n{current}\n\nОтправьте новый текст одним сообщением.", parse_mode="Markdown")

@dp.callback_query(F.data == "admin_undo_text")
async def admin_undo_text(call: CallbackQuery):
    if not admin_only_user(call.from_user.id):
        await call.answer()
        return
    ok = await undo_last_text_change()
    await call.answer("OK" if ok else "No changes", show_alert=True)

@dp.callback_query(F.data == "admin_reset_test")
async def admin_reset_test(call: CallbackQuery):
    if not admin_only_user(call.from_user.id):
        await call.answer()
        return
    await reset_test_data()
    await call.answer("OK", show_alert=True)


# ================= Back to payment methods =================

@dp.callback_query(F.data.startswith("back_to_pm|"))
async def back_to_pm(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    campaign = call.data.split("|")[1]
    await call.answer()
    await safe_edit(call, t(lang, "Выберите способ оплаты:", "Choose payment method:"),
                    reply_markup=kb_choose_payment(lang, campaign))


# ================= Payment method after campaign =================

@dp.callback_query(F.data.startswith("pm|"))
async def choose_payment_method(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    _, campaign, method = call.data.split("|")
    await call.answer()

    if campaign == "fitr":
        if method not in {"paypal", "zenbank", "zenfast"}:
            await call.message.answer(t(lang, "Для Закят-уль-Фитр доступны только PayPal, Zen и Zen Express.", "Only PayPal, Zen and Zen Express are available for Zakat al-Fitr."))
            return
        if not fitr_method_open(method):
            await call.message.answer(fitr_close_text(method, lang))
            return
        PENDING[call.from_user.id] = {"campaign": "fitr", "method": method}
        await call.message.answer(t(lang, "Теперь выберите количество членов семьи.", "Now choose number of family members."),
                                  reply_markup=kb_fitr_members(lang))
        return

    PENDING[call.from_user.id] = {"campaign": campaign, "method": method}
    if campaign == "water":
        raised = int(await kv_get("water_raised_eur") or "0")
        remain = max(0, 235 - raised)
        await call.message.answer(
            t(lang, "Теперь выберите сумму.", "Now choose amount."),
            reply_markup=kb_amounts_eur(lang, "water", [10, 25, 50], [
                (t(lang, "Индивидуально — 235€", "Individual — 235€"), "amt|water|eur|235"),
                (t(lang, f"Оплатить остаток — {remain}€", f"Pay remaining — {remain}€"), f"amt|water|eur|{remain}")
            ])
        )
    elif campaign == "iftar":
        raised = int(await kv_get("iftar_raised_portions") or "0")
        target = int(await kv_get("iftar_target_portions") or "800")
        remain_portions = max(1, target - raised)
        await call.message.answer(
            t(lang, "Теперь выберите количество порций или сумму.", "Now choose portions or amount."),
            reply_markup=kb_iftar_options(lang, admin_only_user(call.from_user.id), raised >= 100, remain_portions)
        )
    elif campaign == "eid":
        await call.message.answer(
            t(lang, "Теперь выберите сумму.", "Now choose amount."),
            reply_markup=kb_amounts_eur(lang, "eid", [5, 10, 25, 50])
        )


# ================= Amount selection =================

@dp.callback_query(F.data.startswith("amt|"))
async def choose_amount(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    _, campaign, unit, val = call.data.split("|")
    ctx = PENDING.get(call.from_user.id, {})
    method = ctx.get("method", "stars")
    await call.answer()

    if val == "other":
        if campaign == "iftar" and unit == "portions":
            PENDING[call.from_user.id] = {"kind": "other_portions", "method": method}
            await call.message.answer(t(lang, "Введите количество порций:", "Enter number of portions:"))
            return
        if campaign == "iftar" and unit == "eur":
            PENDING[call.from_user.id] = {"kind": "other_iftar_eur", "method": method}
            await call.message.answer(t(lang, "Введите сумму в евро:", "Enter amount in EUR:"))
            return
        if campaign == "fitr":
            PENDING[call.from_user.id] = {"kind": "other_members", "method": method}
            await call.message.answer(t(lang, "Введите количество членов семьи:", "Enter number of family members:"))
            return
        PENDING[call.from_user.id] = {"kind": "other_eur", "campaign": campaign, "method": method}
        await call.message.answer(t(lang, "Введите сумму в евро:", "Enter amount in EUR:"))
        return

    if campaign == "water":
        eur = int(val)
        note = "GREENMAX"
        await handle_payment_step(call, lang, method, campaign, eur, note)
        return

    if campaign == "eid":
        eur = int(val)
        note = "Id"
        await handle_payment_step(call, lang, method, campaign, eur, note)
        return

    if campaign == "iftar":
        if unit == "eur":
            eur = int(val)
            portions = max(1, eur // 4)
        else:
            portions = int(val)
            eur = portions * 4
        note = "Mimax"
        extra = t(lang, f"Порций: *{portions}*", f"Portions: *{portions}*")
        await handle_payment_step(call, lang, method, campaign, eur, note, extra)
        return

    if campaign == "fitr":
        people = int(val)
        price = int(await kv_get("fitr_saa_eur") or "10")
        eur = people * price
        kg = people * 3
        note = f"ZF{people}"
        summary = t(
            lang,
            f"Вам необходимо раздать: *{kg} кг*\nСумма к оплате: *{eur}€*\nКод оплаты: `{note}`",
            f"You need to distribute: *{kg} kg*\nAmount to pay: *{eur}€*\nPayment code: `{note}`"
        )
        await call.message.answer(
            summary,
            parse_mode="Markdown",
            reply_markup=kb_hidden_payment_details(lang, campaign, method, eur, note)
        )
        return

async def handle_payment_step(call: CallbackQuery, lang: str, method: str, campaign: str, eur: int, note: str, extra: str = ""):
    if method == "stars":
        title_map = {
            "water": t(lang, "Сукья-ль-ма (вода)", "Sukya-l-ma (Water)"),
            "iftar": t(lang, "Ифтары", "Iftars"),
            "eid": t(lang, "Ид — сладости детям", "Eid sweets"),
        }
        payload = f"{campaign}:eur:{eur}" if campaign != "iftar" else f"iftar:portions:{max(1, eur // 4)}"
        stars = eur * EUR_TO_STARS
        await bot.send_invoice(
            chat_id=call.from_user.id,
            title=title_map.get(campaign, "Donation"),
            description=t(lang, f"Пожертвование: {eur}€", f"Donation: {eur}€"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
            provider_token="",
        )
        return

    summary = (
        f"{t(lang,'Сумма к оплате','Amount to pay')}: *{eur}€*\n"
        f"{t(lang,'Код оплаты','Payment code')}: `{note}`\n"
    )
    if extra:
        summary += f"\n{extra}\n"

    method_title = {
        "sepa": t(lang, "🏦 Банковский перевод", "🏦 Bank transfer"),
        "paypal": "💙 PayPal",
        "zenbank": t(lang, "🏦 Банковский перевод (Zen)", "🏦 Bank transfer (Zen)"),
        "zenfast": t(lang, "⚡ Zen Express", "⚡ Zen Express"),
        "crypto": t(lang, "💎 Криптовалюта", "💎 Crypto"),
    }.get(method, method)

    await call.message.answer(
        f"{method_title}\n\n{summary}{t(lang, 'После оплаты нажмите «Оплатил».', 'After payment tap “Paid”.')}",
        parse_mode="Markdown",
        reply_markup=kb_hidden_payment_details(lang, campaign, method, eur, note),
    )


# ================= Hidden payment details =================

@dp.callback_query(F.data.in_({
    "show_sepa_recipient", "show_sepa_iban", "show_sepa_bic",
    "show_paypal_link", "show_zen_iban", "show_zen_bic",
    "show_zen_phone", "show_zen_card", "show_zen_name",
    "show_usdt", "show_usdc"
}))
async def show_hidden_detail(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    mapping = {
        "show_sepa_recipient": SEPA_RECIPIENT,
        "show_sepa_iban": SEPA_IBAN,
        "show_sepa_bic": SEPA_BIC,
        "show_paypal_link": PAYPAL_LINK,
        "show_zen_iban": ZEN_IBAN,
        "show_zen_bic": ZEN_BIC,
        "show_zen_phone": ZEN_PHONE,
        "show_zen_card": ZEN_CARD,
        "show_zen_name": ZEN_NAME,
        "show_usdt": USDT_TRC20,
        "show_usdc": USDC_ERC20,
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


# ================= Paid / list flow =================

@dp.callback_query(F.data.startswith("manual_sent|"))
async def manual_sent(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()

    _, method, campaign, amount_eur, note = call.data.split("|", 4)
    amount_eur = int(amount_eur)

    if campaign == "fitr":
        people = max(1, int(note.replace("ZF", "") or "1"))
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

    if campaign == "eid":
        PENDING[call.from_user.id] = {"kind": "eid_confirm_amount", "method": method, "note": note}
        await call.message.answer(t(lang, "После оплаты напишите цифру перевода в евро.", "After payment send the transfer amount in EUR."))
        return

    # water / iftar direct mark
    if campaign == "water":
        await kv_inc_int("water_raised_eur", amount_eur)
    elif campaign == "iftar":
        portions = max(1, amount_eur // 4)
        old_raised = int(await kv_get("iftar_raised_portions") or "0")
        new_raised = old_raised + portions
        await kv_set("iftar_raised_portions", str(new_raised))
        day = int(await kv_get("iftar_day") or "27")
        done = done_list(await kv_get("iftar_done_days"))
        if new_raised >= 100 and day not in done:
            done.append(day)
            await kv_set("iftar_done_days", done_str(done))

    username = call.from_user.username or ""
    pid = await add_manual_payment(call.from_user.id, username, method, campaign, amount_eur, note)
    await notify_admin(
        "📩 PAYMENT MARKED\n"
        f"ID: {pid}\n"
        f"Method: {method}\n"
        f"Campaign: {campaign}\n"
        f"Amount: {amount_eur} EUR\n"
        f"Code: {note}\n"
        f"User: @{username or '-'}\n"
        f"Link: {user_link(call.from_user.id)}\n"
        f"UserID: {call.from_user.id}"
    )
    await call.message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")

@dp.callback_query(F.data.in_({"fitr_fmt_umm", "fitr_fmt_abu", "fitr_fmt_name"}))
async def fitr_format_choice(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    ctx = PENDING.get(call.from_user.id)
    if not ctx or ctx.get("kind") != "fitr_identity":
        await call.answer()
        return

    mapping = {
        "fitr_fmt_umm": "umm",
        "fitr_fmt_abu": "abu",
        "fitr_fmt_name": "name",
    }
    ctx["fmt"] = mapping[call.data]
    ctx["step"] = "name"
    PENDING[call.from_user.id] = ctx
    await call.answer()
    await call.message.answer(t(lang, "Имя или инициалы (обязательно):", "Name or initials (required):"))

@dp.message(F.text)
async def text_input(message: Message):
    lang = await get_user_lang(message.from_user.id) or "ru"
    ctx = PENDING.get(message.from_user.id)
    if not ctx:
        return

    raw = (message.text or "").strip()

    def parse_positive_int(s: str) -> int | None:
        try:
            n = int(s)
            return n if n > 0 else None
        except Exception:
            return None

    if ctx["kind"] == "edit_text":
        key = ctx["key"]
        old_v = await kv_get(key)
        await kv_set(key, raw)
        await add_text_history(key, old_v, raw)
        PENDING.pop(message.from_user.id, None)
        await message.answer("OK")
        return

    if ctx["kind"] == "other_eur":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно число > 0.", "Need number > 0."))
            return
        campaign = ctx["campaign"]
        method = ctx["method"]
        note = {"water": "GREENMAX", "eid": "Id"}[campaign]
        PENDING.pop(message.from_user.id, None)
        class Dummy:
            from_user = message.from_user
            message = message
        await handle_payment_step(Dummy(), lang, method, campaign, n, note)
        return

    if ctx["kind"] == "other_portions":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно число > 0.", "Need number > 0."))
            return
        eur = n * 4
        method = ctx["method"]
        note = "Mimax"
        PENDING.pop(message.from_user.id, None)
        class Dummy:
            from_user = message.from_user
            message = message
        await handle_payment_step(Dummy(), lang, method, "iftar", eur, note, t(lang, f"Порций: *{n}*", f"Portions: *{n}*"))
        return

    if ctx["kind"] == "other_iftar_eur":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно число > 0.", "Need number > 0."))
            return
        method = ctx["method"]
        portions = max(1, n // 4)
        PENDING.pop(message.from_user.id, None)
        class Dummy:
            from_user = message.from_user
            message = message
        await handle_payment_step(Dummy(), lang, method, "iftar", n, "Mimax", t(lang, f"Порций: *{portions}*", f"Portions: *{portions}*"))
        return

    if ctx["kind"] == "other_members":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно число > 0.", "Need number > 0."))
            return
        price = int(await kv_get("fitr_saa_eur") or "10")
        eur = n * price
        method = ctx["method"]
        note = f"ZF{n}"
        PENDING.pop(message.from_user.id, None)
        class Dummy:
            from_user = message.from_user
            message = message
        summary = t(lang, f"Количество человек: *{n}*", f"People: *{n}*")
        await handle_payment_step(Dummy(), lang, method, "fitr", eur, note, summary)
        return

    if ctx["kind"] == "eid_confirm_amount":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Напишите сумму цифрой в евро.", "Send amount in EUR as digits."))
            return
        username = message.from_user.username or ""
        pid = await add_manual_payment(message.from_user.id, username, ctx["method"], "eid", n, ctx["note"])
        await kv_inc_int("eid_raised_eur", n)
        PENDING.pop(message.from_user.id, None)
        await notify_admin(
            "📩 EID PAYMENT MARKED\n"
            f"ID: {pid}\n"
            f"Method: {ctx['method']}\n"
            f"Amount: {n} EUR\n"
            f"Code: {ctx['note']}\n"
            f"User: @{username or '-'}\n"
            f"Link: {user_link(message.from_user.id)}"
        )
        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
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
            )
            await kv_inc_int("fitr_raised_eur", int(ctx["amount_eur"]))
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
                f"Code: {ctx['code']}\n"
                f"User: @{message.from_user.username or '-'}\n"
                f"Link: {user_link(message.from_user.id)}\n\n"
                f"TOTALS -> EUR: {total_eur}, PEOPLE: {total_people}, KG: {total_kg}"
            )
            await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
            return


# ================= Copy hidden values =================

@dp.callback_query(F.data.in_({
    "show_sepa_recipient", "show_sepa_iban", "show_sepa_bic",
    "show_paypal_link", "show_zen_iban", "show_zen_bic",
    "show_zen_phone", "show_zen_card", "show_zen_name",
    "show_usdt", "show_usdc"
}))
async def show_hidden_detail(call: CallbackQuery):
    lang = await get_user_lang(call.from_user.id) or "ru"
    await call.answer()
    mapping = {
        "show_sepa_recipient": SEPA_RECIPIENT,
        "show_sepa_iban": SEPA_IBAN,
        "show_sepa_bic": SEPA_BIC,
        "show_paypal_link": PAYPAL_LINK,
        "show_zen_iban": ZEN_IBAN,
        "show_zen_bic": ZEN_BIC,
        "show_zen_phone": ZEN_PHONE,
        "show_zen_card": ZEN_CARD,
        "show_zen_name": ZEN_NAME,
        "show_usdt": USDT_TRC20,
        "show_usdc": USDC_ERC20,
    }
    val = mapping.get(call.data, "")
    if not val:
        await call.message.answer(t(lang, "Не настроено.", "Not configured."))
        return
    await call.message.answer(f"`{val}`", parse_mode="Markdown")


# ================= Stars =================

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload or ""
    try:
        typ, unit, val = payload.split(":")
        val_i = int(val)
    except Exception:
        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        return

    if typ == "water" and unit == "eur":
        await kv_inc_int("water_raised_eur", val_i)
    elif typ == "eid" and unit == "eur":
        await kv_inc_int("eid_raised_eur", val_i)
    elif typ == "iftar" and unit == "portions":
        old_raised = int(await kv_get("iftar_raised_portions") or "0")
        new_raised = old_raised + val_i
        await kv_set("iftar_raised_portions", str(new_raised))
        day = int(await kv_get("iftar_day") or "27")
        done = done_list(await kv_get("iftar_done_days"))
        if new_raised >= 100 and day not in done:
            done.append(day)
            await kv_set("iftar_done_days", done_str(done))

    await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")


# ================= Admin commands =================

@dp.message(Command("set_water_target"))
async def cmd_set_water_target(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2:
        await kv_set("water_target_eur", str(int(parts[1])))
        await message.answer("OK")

@dp.message(Command("set_iftar_day"))
async def cmd_set_iftar_day(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2:
        await kv_set("iftar_day", str(int(parts[1])))
        await kv_set("iftar_raised_portions", "0")
        await kv_set("iftar_target_portions", "800")
        await message.answer("OK")

@dp.message(Command("set_iftar_target"))
async def cmd_set_iftar_target(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2:
        await kv_set("iftar_target_portions", str(int(parts[1])))
        await message.answer("OK")

@dp.message(Command("set_fitr_saa"))
async def cmd_set_fitr_saa(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2:
        await kv_set("fitr_saa_eur", str(int(parts[1])))
        await message.answer("OK")

@dp.message(Command("open_fitr"))
async def cmd_open_fitr(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2 and parts[1] in {"on", "off", "auto"}:
        await kv_set("fitr_open_mode", parts[1])
        await message.answer("OK")

@dp.message(Command("open_eid"))
async def cmd_open_eid(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2 and parts[1] in {"on", "off", "auto"}:
        await kv_set("eid_open_mode", parts[1])
        await message.answer("OK")

@dp.message(Command("eid_extra_day"))
async def cmd_eid_extra_day(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2 and parts[1] in {"on", "off"}:
        await kv_set("eid_extra_day", parts[1])
        await message.answer("OK")

@dp.message(Command("set_test"))
async def cmd_set_test(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) == 2 and parts[1] in {"on", "off"}:
        await kv_set("test_mode", parts[1])
        await message.answer("OK")

@dp.message(Command("reset_test"))
async def cmd_reset_test(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    await reset_test_data()
    await message.answer("OK")

@dp.message(Command("fitr_list"))
async def cmd_fitr_list(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    rows = await get_fitr_rows()
    if not rows:
        await message.answer("Список пуст.")
        return
    parts = []
    for r in rows:
        row_id, display_name, country, city, amount_eur, code, rice_kg, method = r
        place = ", ".join([x for x in [country, city] if x])
        line = f"{row_id}. {display_name}"
        if place:
            line += f" ({place})"
        line += f" — {amount_eur}€ — {code} — {rice_kg} кг — {method}"
        parts.append(line)
    await message.answer("\n".join(parts[:60]))

@dp.message(Command("fitr_add"))
async def cmd_fitr_add(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    raw = (message.text or "").replace("/fitr_add", "", 1).strip()
    parts = [x.strip() for x in raw.split(";")]
    if len(parts) != 6:
        await message.answer("Использование: /fitr_add Имя;Страна;Город;люди;сумма;paypal|zenbank|zenfast")
        return
    display_name, country, city, people_s, amount_s, method = parts
    people = int(people_s)
    amount = int(amount_s)
    code = f"ZF{people}"
    row_id = await add_fitr_person(ADMIN_ID, "admin", method, display_name, country if country != "-" else "", city if city != "-" else "", people, amount, code)
    await kv_inc_int("fitr_raised_eur", amount)
    await fitr_report_if_needed()
    await message.answer(f"OK #{row_id}")

@dp.message(Command("fitr_del"))
async def cmd_fitr_del(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /fitr_del ID")
        return
    await delete_fitr_row(int(parts[1]))
    await message.answer("OK")

@dp.message(Command("fitr_edit"))
async def cmd_fitr_edit(message: Message):
    if not admin_only_user(message.from_user.id):
        return
    raw = (message.text or "").replace("/fitr_edit", "", 1).strip()
    parts = [x.strip() for x in raw.split(";")]
    if len(parts) != 7:
        await message.answer("Использование: /fitr_edit ID;Имя;Страна;Город;люди;сумма;paypal|zenbank|zenfast")
        return
    row_id = int(parts[0])
    display_name = parts[1]
    country = "" if parts[2] == "-" else parts[2]
    city = "" if parts[3] == "-" else parts[3]
    people = int(parts[4])
    amount = int(parts[5])
    method = parts[6]
    code = f"ZF{people}"
    await update_fitr_row(row_id, display_name, country, city, people, amount, method, code)
    await message.answer("OK")


# ================= Health =================

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
