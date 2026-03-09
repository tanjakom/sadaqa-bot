import os
import logging
import asyncio
from datetime import datetime, date
from zoneinfo import ZoneInfo

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher
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


# ================= Helpers =================

def now_hki() -> datetime:
    return datetime.now(TZ)

def t(lang: str, ru: str, en: str) -> str:
    return ru if lang == "ru" else en

def user_link(user_id: int) -> str:
    return f"tg://user?id={user_id}"

def admin_only(message: Message) -> bool:
    return bool(ADMIN_ID) and message.from_user and message.from_user.id == ADMIN_ID

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

async def notify_admin(text: str):
    if not ADMIN_ID:
        return
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logging.exception("Failed to notify admin")

async def safe_edit(call: CallbackQuery, text: str, reply_markup=None, parse_mode=None):
    try:
        await call.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=reply_markup, parse_mode=parse_mode)


# ================= DB =================

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
            lang TEXT NOT NULL,
            pay_method TEXT NOT NULL
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
            people_count INTEGER NOT NULL,
            amount_eur INTEGER NOT NULL,
            rice_kg INTEGER NOT NULL
        )
        """)

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','20')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_done_days','')")

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('fitr_saa_eur','10')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('fitr_raised_eur','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('fitr_open_mode','auto')")

        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eid_raised_eur','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eid_target_eur','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eid_open_mode','auto')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eid_extra_day','off')")

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
    val = int(await kv_get(key) or "0")
    val += int(delta)
    await kv_set(key, str(val))

async def get_user_prefs(user_id: int) -> tuple[str, str] | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT lang, pay_method FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return (row[0], row[1]) if row else None

async def set_user_lang(user_id: int, lang: str):
    lang = "ru" if lang == "ru" else "en"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT pay_method FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            pay_method = row[0] if row else "stars"
        await db.execute(
            "INSERT INTO user_prefs(user_id, lang, pay_method) VALUES(?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET lang=excluded.lang",
            (user_id, lang, pay_method),
        )
        await db.commit()

async def set_user_pay_method(user_id: int, method: str):
    if method not in {"stars", "sepa", "paypal", "crypto", "zen"}:
        method = "stars"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT lang FROM user_prefs WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            lang = row[0] if row else "ru"
        await db.execute(
            "INSERT INTO user_prefs(user_id, lang, pay_method) VALUES(?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET pay_method=excluded.pay_method",
            (user_id, lang, method),
        )
        await db.commit()

async def add_manual_payment(user_id: int, username: str, method: str, campaign: str, amount_eur: int, note: str) -> int:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO manual_payments(ts,user_id,username,method,campaign,amount_eur,note) VALUES(?,?,?,?,?,?,?)",
            (ts, user_id, username or "", method, campaign, int(amount_eur), note),
        )
        await db.commit()
        return cur.lastrowid

async def add_fitr_person(user_id: int, username: str, method: str, display_name: str, people_count: int, amount_eur: int) -> int:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rice_kg = people_count * 3
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO fitr_people(ts,user_id,username,method,display_name,people_count,amount_eur,rice_kg) VALUES(?,?,?,?,?,?,?,?)",
            (ts, user_id, username or "", method, display_name, int(people_count), int(amount_eur), int(rice_kg)),
        )
        await db.commit()
        return cur.lastrowid

async def fitr_totals() -> tuple[int, int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COALESCE(SUM(amount_eur),0), COALESCE(SUM(people_count),0), COALESCE(SUM(rice_kg),0) FROM fitr_people") as cur:
            row = await cur.fetchone()
            return int(row[0]), int(row[1]), int(row[2])

async def fitr_count_rows() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM fitr_people") as cur:
            row = await cur.fetchone()
            return int(row[0])

def fitr_method_open(method: str) -> bool:
    now = now_hki()
    if now < FITR_OPEN_DT:
        return False
    if method == "paypal":
        return now <= FITR_PAYPAL_CLOSE_DT
    if method == "zen":
        return now <= FITR_ZEN_CLOSE_DT
    return False

def eid_open_now() -> bool:
    now = now_hki()
    extra = False
    return now >= EID_OPEN_DT  # actual close checked async below

async def is_eid_open() -> bool:
    mode = (await kv_get("eid_open_mode") or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    now = now_hki()
    extra = (await kv_get("eid_extra_day") or "off").lower() == "on"
    close_dt = EID_EXTRA_CLOSE_DT if extra else EID_CLOSE_DT
    return EID_OPEN_DT <= now <= close_dt

async def is_fitr_visible() -> bool:
    mode = (await kv_get("fitr_open_mode") or "auto").lower()
    if mode == "on":
        return True
    if mode == "off":
        return False
    return now_hki() >= FITR_OPEN_DT


# ================= Text builders =================

async def water_text(lang: str) -> str:
    target = int(await kv_get("water_target_eur") or "235")
    raised = int(await kv_get("water_raised_eur") or "0")
    bar = battery(raised, target)
    code = "GREENMAX"
    return (
        "💧 *Сукья-ль-ма (вода)*\n"
        "Раздача *5000 л* питьевой воды.\n\n"
        f"Нужно: *{target}€*\n"
        f"Собрано: *{raised}€* из *{target}€*\n"
        f"{bar}\n\n"
        f"Код: `{code}`\n"
        if lang == "ru" else
        "💧 *Sukya-l-ma (Water)*\n"
        "Drinking water distribution (*5000 L*).\n\n"
        f"Goal: *{target}€*\n"
        f"Raised: *{raised}€* of *{target}€*\n"
        f"{bar}\n\n"
        f"Code: `{code}`\n"
    )

async def iftar_text(lang: str) -> str:
    day = int(await kv_get("iftar_day") or "20")
    target = int(await kv_get("iftar_target_portions") or "100")
    raised = int(await kv_get("iftar_raised_portions") or "0")
    done = done_list(await kv_get("iftar_done_days"))
    bar = battery(min(raised, target), target)
    code = f"MIMAX-IFTAR-{day}"

    if lang == "ru":
        done_line = f"✅ Закрытые дни: {', '.join(map(str, done))}\n\n" if done else ""
        return (
            f"🍲 *Программа ифтаров — {day} Рамадана*\n\n"
            f"Минимальная цель: *100 порций*\n"
            f"Текущая цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n\n"
            f"{done_line}"
            f"Код: `{code}`\n\n"
            "Выберите количество порций:"
        )

    done_line = f"✅ Closed days: {', '.join(map(str, done))}\n\n" if done else ""
    return (
        f"🍲 *Iftars — {day} of Ramadan*\n\n"
        f"Minimum goal: *100 portions*\n"
        f"Current goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n\n"
        f"{done_line}"
        f"Code: `{code}`\n\n"
        "Choose number of portions:"
    )

async def fitr_text(lang: str) -> str:
    price = int(await kv_get("fitr_saa_eur") or "10")
    total_eur, total_people, total_kg = await fitr_totals()
    count_rows = await fitr_count_rows()

    if lang == "ru":
        return (
            "🕌 *Закят-уль-Фитр (ZF)*\n\n"
            "Мы распределяем Закят-уль-Фитр на территории *Газы*, "
            "в зависимости от текущей ситуации — или *Западного берега в Палестине*, "
            "иногда для опоздавших — в палестинских лагерях *Иордании*.\n\n"
            f"Цена на 1 человека: *{price}€*\n"
            "1 человек = *3 кг* риса\n\n"
            f"В списке оплативших: *{count_rows}*\n"
            f"Сумма: *{total_eur}€*\n"
            f"Количество человек: *{total_people}*\n"
            f"Количество риса: *{total_kg} кг*\n\n"
            "Выберите количество членов семьи:"
        )

    return (
        "🕌 *Zakat al-Fitr (ZF)*\n\n"
        "We distribute Zakat al-Fitr in *Gaza* or, depending on the situation, in the *West Bank*, "
        "and sometimes for late payers in Palestinian camps in *Jordan*.\n\n"
        f"Price per person: *{price}€*\n"
        "1 person = *3 kg* of rice\n\n"
        f"In list: *{count_rows}*\n"
        f"Total: *{total_eur}€*\n"
        f"People: *{total_people}*\n"
        f"Rice: *{total_kg} kg*\n\n"
        "Choose number of family members:"
    )

async def eid_text(lang: str) -> str:
    raised = int(await kv_get("eid_raised_eur") or "0")
    target = int(await kv_get("eid_target_eur") or "0")
    bar = battery(min(raised, target), target) if target > 0 else ""
    close_extra = (await kv_get("eid_extra_day") or "off").lower() == "on"
    close_str_ru = "19 марта 00:00" if close_extra else "18 марта 00:00"
    close_str_en = "March 19 00:00" if close_extra else "March 18 00:00"

    if lang == "ru":
        s = (
            "🎁 *Ид аль-Фитр — сладости детям (Id)*\n\n"
            "Сбор на сладкую традиционную выпечку *кяки* или что-то подобное, в честь праздника.\n\n"
            f"Собрано: *{raised}€*\n"
        )
        if target > 0:
            s += f"Цель: *{target}€*\n{bar}\n"
        s += f"\nЗакрытие: *{close_str_ru}*\n\nВыберите сумму:"
        return s

    s = (
        "🎁 *Eid al-Fitr — sweets for children (Id)*\n\n"
        "Collection for traditional sweet pastries *kyaky* or something similar for the holiday.\n\n"
        f"Raised: *{raised}€*\n"
    )
    if target > 0:
        s += f"Goal: *{target}€*\n{bar}\n"
    s += f"\nClose: *{close_str_en}*\n\nChoose amount:"
    return s


# ================= Keyboards =================

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()

def kb_payment_methods(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ Telegram Stars", callback_data="pm_stars")
    kb.button(text="🏦 SEPA", callback_data="pm_sepa")
    kb.button(text="💙 PayPal", callback_data="pm_paypal")
    kb.button(text="💎 Crypto", callback_data="pm_crypto")
    kb.button(text="🟣 ZEN", callback_data="pm_zen")
    kb.adjust(1)
    return kb.as_markup()

def kb_campaigns(lang: str, show_fitr: bool, show_eid: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "💧 Вода (GREENMAX)", "💧 Water (GREENMAX)"), callback_data="camp_water")
    kb.button(text=t(lang, "🍲 Ифтары (MIMAX)", "🍲 Iftars (MIMAX)"), callback_data="camp_iftar")
    if show_fitr:
        kb.button(text=t(lang, "🕌 Закят-уль-Фитр (ZF)", "🕌 Zakat al-Fitr (ZF)"), callback_data="camp_fitr")
    if show_eid:
        kb.button(text=t(lang, "🎁 Ид — сладости детям (Id)", "🎁 Eid sweets (Id)"), callback_data="camp_eid")
    kb.button(text=t(lang, "🔁 Сменить способ оплаты", "🔁 Change payment method"), callback_data="go_methods")
    kb.button(text=t(lang, "🌐 Язык", "🌐 Language"), callback_data="go_lang")
    kb.adjust(1)
    return kb.as_markup()

def kb_back_to_campaigns(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "⬅️ Назад к сборам", "⬅️ Back to campaigns"), callback_data="go_campaigns")
    kb.adjust(1)
    return kb.as_markup()

def kb_amounts_eur(lang: str, prefix: str, amounts: list[int]):
    kb = InlineKeyboardBuilder()
    for a in amounts:
        kb.button(text=f"{a}€", callback_data=f"{prefix}_eur_{a}")
    kb.button(text=t(lang, "Другая сумма", "Other amount"), callback_data=f"{prefix}_eur_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def kb_iftar_portions(lang: str, is_admin: bool, closed100: bool):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "5 порций", "5 portions"), callback_data="iftar_p_5")
    kb.button(text=t(lang, "10 порций", "10 portions"), callback_data="iftar_p_10")
    kb.button(text=t(lang, "20 порций", "20 portions"), callback_data="iftar_p_20")
    kb.button(text=t(lang, "50 порций", "50 portions"), callback_data="iftar_p_50")
    kb.button(text=t(lang, "Другое количество", "Other qty"), callback_data="iftar_p_other")
    if is_admin and closed100:
        kb.button(text=t(lang, "➕ +50 порций", "➕ +50 portions"), callback_data="admin_iftar_plus50")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(2, 2, 1, 1, 1)
    return kb.as_markup()

def kb_fitr_members(lang: str):
    kb = InlineKeyboardBuilder()
    for n in [1, 2, 3, 4, 5]:
        kb.button(text=t(lang, f"{n} человек", f"{n} people"), callback_data=f"fitr_m_{n}")
    kb.button(text=t(lang, "Другое количество", "Other qty"), callback_data="fitr_m_other")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()

def kb_manual_confirm(lang: str, method: str, campaign: str, amount_eur: int, note: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "✅ Я отправил(а)", "✅ I sent it"),
              callback_data=f"manual_sent|{method}|{campaign}|{amount_eur}|{note}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(1)
    return kb.as_markup()

def kb_crypto(lang: str, note: str, method: str, campaign: str, amount_eur: int):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "📋 Скопировать USDT (TRC20)", "📋 Copy USDT (TRC20)"), callback_data="copy_usdt")
    kb.button(text=t(lang, "📋 Скопировать USDC (ERC20)", "📋 Copy USDC (ERC20)"), callback_data="copy_usdc")
    kb.button(text=t(lang, "📋 Скопировать сообщение", "📋 Copy message"), callback_data=f"copy_note|{note}")
    kb.button(text=t(lang, "✅ Я отправил(а)", "✅ I sent it"),
              callback_data=f"manual_sent|{method}|{campaign}|{amount_eur}|{note}")
    kb.button(text=t(lang, "⬅️ Назад", "⬅️ Back"), callback_data="go_campaigns")
    kb.adjust(1)
    return kb.as_markup()

def kb_fitr_name_format(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=t(lang, "Умм …", "Umm …"), callback_data="fitr_fmt_umm")
    kb.button(text=t(lang, "Абу …", "Abu …"), callback_data="fitr_fmt_abu")
    kb.button(text=t(lang, "Имя …", "Name …"), callback_data="fitr_fmt_name")
    kb.adjust(1)
    return kb.as_markup()


# ================= Screens =================

@dp.message(Command("start"))
async def start(message: Message):
    prefs = await get_user_prefs(message.from_user.id)
    if not prefs:
        await message.answer("Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return
    lang, _ = prefs
    await message.answer(
        t(lang, "Выберите способ оплаты:", "Choose payment method:"),
        reply_markup=kb_payment_methods(lang)
    )

@dp.callback_query(lambda c: c.data in {"lang_ru", "lang_en"})
async def choose_lang(call: CallbackQuery):
    lang = "ru" if call.data == "lang_ru" else "en"
    await set_user_lang(call.from_user.id, lang)
    await call.answer()
    await safe_edit(call, t(lang, "Выберите способ оплаты:", "Choose payment method:"), reply_markup=kb_payment_methods(lang))

@dp.callback_query(lambda c: c.data in {"go_lang", "go_methods", "go_campaigns"})
async def nav(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang = prefs[0] if prefs else "ru"
    await call.answer()

    if call.data == "go_lang":
        await safe_edit(call, "Мир вам! Выберите язык дальнейшего общения", reply_markup=kb_lang_select())
        return

    if call.data == "go_methods":
        await safe_edit(call, t(lang, "Выберите способ оплаты:", "Choose payment method:"), reply_markup=kb_payment_methods(lang))
        return

    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(lambda c: c.data.startswith("pm_"))
async def set_method(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang = prefs[0] if prefs else "ru"

    mapping = {
        "pm_stars": "stars",
        "pm_sepa": "sepa",
        "pm_paypal": "paypal",
        "pm_crypto": "crypto",
        "pm_zen": "zen",
    }
    method = mapping.get(call.data, "stars")
    await set_user_pay_method(call.from_user.id, method)
    await call.answer()

    show_fitr = await is_fitr_visible()
    show_eid = await is_eid_open()
    await safe_edit(call, t(lang, "Выберите сбор:", "Choose campaign:"), reply_markup=kb_campaigns(lang, show_fitr, show_eid))

@dp.callback_query(lambda c: c.data in {"camp_water", "camp_iftar", "camp_fitr", "camp_eid"})
async def campaign(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
    await call.answer()

    if call.data == "camp_water":
        txt = await water_text(lang)
        txt += t(lang, "\nВыберите сумму:", "\nChoose amount:")
        await safe_edit(call, txt, reply_markup=kb_amounts_eur(lang, "water", [10, 25, 50]), parse_mode="Markdown")
        return

    if call.data == "camp_iftar":
        txt = await iftar_text(lang)
        raised = int(await kv_get("iftar_raised_portions") or "0")
        await safe_edit(call, txt, reply_markup=kb_iftar_portions(lang, call.from_user.id == ADMIN_ID, raised >= 100), parse_mode="Markdown")
        return

    if call.data == "camp_fitr":
        if method not in {"paypal", "zen"}:
            await safe_edit(call, t(lang, "Для Закят-уль-Фитр доступны только PayPal и ZEN.", "Only PayPal and ZEN are available for Zakat al-Fitr."),
                            reply_markup=kb_back_to_campaigns(lang))
            return
        if not fitr_method_open(method):
            await safe_edit(call, fitr_close_text(method, lang), reply_markup=kb_back_to_campaigns(lang))
            return
        await safe_edit(call, await fitr_text(lang), reply_markup=kb_fitr_members(lang), parse_mode="Markdown")
        return

    if call.data == "camp_eid":
        if not await is_eid_open():
            await safe_edit(call, t(lang, "Сбор на Ид сейчас закрыт.", "Eid collection is currently closed."),
                            reply_markup=kb_back_to_campaigns(lang))
            return
        await safe_edit(call, await eid_text(lang), reply_markup=kb_amounts_eur(lang, "eid", [5, 10, 25, 50]), parse_mode="Markdown")
        return


# ================= Payment routing =================

async def send_stars_invoice(user_id: int, lang: str, title: str, description: str, payload: str, eur: int):
    stars = eur * EUR_TO_STARS
    await bot.send_invoice(
        chat_id=user_id,
        title=title,
        description=description,
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
        provider_token="",
    )

async def send_manual_instructions(user: Message | CallbackQuery, lang: str, method: str, campaign: str, eur: int, note: str, extra_line: str = ""):
    if method == "sepa":
        header = "🏦 *SEPA (Европа)*" if lang == "ru" else "🏦 *SEPA (Europe)*"
        text = (
            f"{header}\n\n"
            f"{t(lang, 'Получатель', 'Recipient')}: `{SEPA_RECIPIENT}`\n"
            f"IBAN: `{SEPA_IBAN}`\n"
        )
        if SEPA_BIC:
            text += f"BIC: `{SEPA_BIC}`\n"
        text += (
            f"\n{t(lang,'Сумма','Amount')}: *{eur}€*\n"
            f"{t(lang,'Сообщение','Message')}: `{note}`\n"
        )
        if extra_line:
            text += f"\n{extra_line}\n"
        text += t(lang, "\n⚠️ Не пишите длинные комментарии.", "\n⚠️ Please avoid long comments.")
        markup = kb_manual_confirm(lang, method, campaign, eur, note)

    elif method == "paypal":
        header = "💙 *PayPal*"
        text = (
            f"{header}\n\n"
            f"{t(lang,'Ссылка','Link')}: `{PAYPAL_LINK}`\n"
            f"{t(lang,'Сумма','Amount')}: *{eur}€*\n"
            f"{t(lang,'Сообщение','Message')}: `{note}`\n"
        )
        if extra_line:
            text += f"\n{extra_line}\n"
        text += t(lang, "\nПосле оплаты нажмите «Я отправил(а)».", "\nAfter payment tap “I sent it”.")
        markup = kb_manual_confirm(lang, method, campaign, eur, note)

    elif method == "zen":
        header = "🟣 *ZEN*"
        lines = []
        if ZEN_NAME:
            lines.append(f"{t(lang,'Получатель','Recipient')}: `{ZEN_NAME}`")
        if ZEN_PHONE:
            lines.append(f"ZEN {t(lang,'телефон','phone')}: `{ZEN_PHONE}`")
        if ZEN_CARD:
            lines.append(f"{t(lang,'Карта','Card')}: `{ZEN_CARD}`")

        text = f"{header}\n\n" + ("\n".join(lines) + "\n\n" if lines else "")
        text += (
            f"{t(lang,'Сумма','Amount')}: *{eur}€*\n"
            f"{t(lang,'Сообщение','Message')}: `{note}`\n"
        )
        if extra_line:
            text += f"\n{extra_line}\n"
        text += t(lang, "\nПосле оплаты нажмите «Я отправил(а)».", "\nAfter payment tap “I sent it”.")
        markup = kb_manual_confirm(lang, method, campaign, eur, note)

    elif method == "crypto":
        header = "💎 *Криптовалюта*" if lang == "ru" else "💎 *Crypto*"
        text = (
            f"{header}\n\n"
            f"USDT (TRC20):\n`{USDT_TRC20}`\n\n"
            f"USDC (ERC20):\n`{USDC_ERC20}`\n\n"
            f"{t(lang,'Сумма (эквивалент)','Amount (equivalent)')}: *{eur}€*\n"
            f"{t(lang,'Сообщение','Message')}: `{note}`\n"
        )
        if extra_line:
            text += f"\n{extra_line}\n"
        text += t(lang, "\n⚠️ Важно: отправляйте строго в указанной сети.", "\n⚠️ Important: send only on the specified network.")
        markup = kb_crypto(lang, note, method, campaign, eur)

    else:
        text = t(lang, "Метод оплаты не настроен.", "Payment method is not configured.")
        markup = kb_back_to_campaigns(lang)

    if isinstance(user, CallbackQuery):
        await user.message.answer(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await user.answer(text, parse_mode="Markdown", reply_markup=markup)


# ================= Water handlers =================

@dp.callback_query(lambda c: c.data.startswith("water_eur_"))
async def water_amount(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
    await call.answer()

    if call.data.endswith("_other"):
        PENDING[call.from_user.id] = {"kind": "other_eur", "campaign": "water", "note": "GREENMAX"}
        await call.message.answer(t(lang, "Введите сумму в евро (целое число), например 12:", "Enter amount in EUR (whole number), e.g. 12:"))
        return

    eur = int(call.data.split("_")[-1])
    note = "GREENMAX"

    if method == "stars":
        await send_stars_invoice(call.from_user.id, lang, t(lang, "Сукья-ль-ма (вода)", "Sukya-l-ma (Water)"),
                                 t(lang, f"Пожертвование: {eur}€", f"Donation: {eur}€"), f"water:eur:{eur}", eur)
        return

    await send_manual_instructions(call, lang, method, "water", eur, note)


# ================= Iftar handlers =================

@dp.callback_query(lambda c: c.data.startswith("iftar_p_"))
async def iftar_amount(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
    await call.answer()

    if call.data.endswith("_other"):
        PENDING[call.from_user.id] = {"kind": "other_portions", "campaign": "iftar"}
        await call.message.answer(t(lang, "Введите количество порций (целое число), например 7:", "Enter number of portions (whole number), e.g. 7:"))
        return

    portions = int(call.data.split("_")[-1])
    eur = portions * 4
    day = int(await kv_get("iftar_day") or "20")
    note = f"MIMAX-IFTAR-{day}"

    if method == "stars":
        stars = eur * EUR_TO_STARS
        title_ru = f"Программа ифтаров — {day} Рамадана"
        title_en = f"Iftars — {day} of Ramadan"
        await bot.send_invoice(
            chat_id=call.from_user.id,
            title=(title_ru if lang == "ru" else title_en),
            description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
            payload=f"iftar:portions:{portions}",
            currency="XTR",
            prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
            provider_token="",
        )
        return

    extra = t(lang, f"Порций: *{portions}*", f"Portions: *{portions}*")
    await send_manual_instructions(call, lang, method, "iftar", eur, note, extra_line=extra)

@dp.callback_query(lambda c: c.data == "admin_iftar_plus50")
async def iftar_plus50(call: CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer()
        return

    raised = int(await kv_get("iftar_raised_portions") or "0")
    if raised < 100:
        await call.answer("Сначала нужно собрать 100 порций", show_alert=True)
        return

    target = int(await kv_get("iftar_target_portions") or "100")
    target += 50
    await kv_set("iftar_target_portions", str(target))
    lang = (await get_user_prefs(call.from_user.id) or ("ru", "stars"))[0]
    await call.answer("OK")
    txt = await iftar_text(lang)
    await safe_edit(call, txt, reply_markup=kb_iftar_portions(lang, True, True), parse_mode="Markdown")


# ================= Fitr handlers =================

@dp.callback_query(lambda c: c.data.startswith("fitr_m_"))
async def fitr_members(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
    await call.answer()

    if method not in {"paypal", "zen"}:
        await call.message.answer(t(lang, "Для Закят-уль-Фитр доступны только PayPal и ZEN.", "Only PayPal and ZEN are available for Zakat al-Fitr."))
        return

    if not fitr_method_open(method):
        await call.message.answer(fitr_close_text(method, lang))
        return

    if call.data.endswith("_other"):
        PENDING[call.from_user.id] = {"kind": "other_members", "campaign": "fitr"}
        await call.message.answer(t(lang, "Введите количество членов семьи (целое число), например 6:", "Enter number of family members (whole number), e.g. 6:"))
        return

    people = int(call.data.split("_")[-1])
    price = int(await kv_get("fitr_saa_eur") or "10")
    eur = people * price
    note = "ZF"
    extra = t(lang, f"Количество человек: *{people}* (× {price}€)", f"People: *{people}* (× {price}€)")
    await send_manual_instructions(call, lang, method, "fitr", eur, note, extra_line=extra)


# ================= Eid handlers =================

@dp.callback_query(lambda c: c.data.startswith("eid_eur_"))
async def eid_amount(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
    await call.answer()

    if not await is_eid_open():
        await call.message.answer(t(lang, "Сбор на Ид сейчас закрыт.", "Eid collection is currently closed."))
        return

    if call.data.endswith("_other"):
        PENDING[call.from_user.id] = {"kind": "other_eur", "campaign": "eid", "note": "Id"}
        await call.message.answer(t(lang, "Введите сумму в евро (целое число), например 12:", "Enter amount in EUR (whole number), e.g. 12:"))
        return

    eur = int(call.data.split("_")[-1])
    note = "Id"

    if method == "stars":
        await send_stars_invoice(
            call.from_user.id,
            lang,
            t(lang, "Ид аль-Фитр — сладости детям", "Eid sweets for children"),
            t(lang, f"Пожертвование: {eur}€", f"Donation: {eur}€"),
            f"eid:eur:{eur}",
            eur
        )
        return

    await send_manual_instructions(call, lang, method, "eid", eur, note)


# ================= Manual sent / identity =================

@dp.callback_query(lambda c: c.data.startswith("manual_sent|"))
async def manual_sent(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang = prefs[0] if prefs else "ru"
    await call.answer()

    try:
        _, method, campaign, amount_eur, note = call.data.split("|", 4)
        amount_eur = int(amount_eur)
    except Exception:
        await call.message.answer(t(lang, "Ошибка отметки. Попробуйте ещё раз.", "Marking error. Please try again."))
        return

    if campaign == "fitr":
        if method not in {"paypal", "zen"}:
            await call.message.answer(t(lang, "Для Закят-уль-Фитр доступны только PayPal и ZEN.", "Only PayPal and ZEN are available for Zakat al-Fitr."))
            return
        if not fitr_method_open(method):
            await call.message.answer(fitr_close_text(method, lang))
            return

        price = int(await kv_get("fitr_saa_eur") or "10")
        people_count = max(1, amount_eur // price)
        PENDING[call.from_user.id] = {
            "kind": "fitr_identity",
            "method": method,
            "campaign": "fitr",
            "amount_eur": amount_eur,
            "people_count": people_count,
        }
        await call.message.answer(t(lang, "Как вы хотите видеть себя в списке?", "How would you like to appear in the list?"),
                                  reply_markup=kb_fitr_name_format(lang))
        return

    if campaign == "eid":
        PENDING[call.from_user.id] = {
            "kind": "eid_confirm_amount",
            "method": method,
            "campaign": "eid",
            "note": note,
        }
        await call.message.answer(
            t(lang, "Напишите цифру перевода в евро.", "Please send the transfer amount in EUR.")
        )
        return

    if campaign == "water":
        await kv_inc_int("water_raised_eur", amount_eur)

    elif campaign == "iftar":
        portions = amount_eur // 4
        if portions > 0:
            old_raised = int(await kv_get("iftar_raised_portions") or "0")
            new_raised = old_raised + portions
            await kv_set("iftar_raised_portions", str(new_raised))

            day = int(await kv_get("iftar_day") or "20")
            done = done_list(await kv_get("iftar_done_days"))
            if new_raised >= 100 and day not in done:
                done.append(day)
                await kv_set("iftar_done_days", done_str(done))

    username = call.from_user.username or ""
    pid = await add_manual_payment(call.from_user.id, username, method, campaign, amount_eur, note)
    when = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    await notify_admin(
        "📩 MANUAL PAYMENT MARKED\n"
        f"ID: {pid}\n"
        f"Method: {method}\n"
        f"Campaign: {campaign}\n"
        f"Amount: {amount_eur} EUR\n"
        f"Note: {note}\n"
        f"Time: {when}\n"
        f"User: @{username or '-'}\n"
        f"Link: {user_link(call.from_user.id)}\n"
        f"UserID: {call.from_user.id}"
    )

    await call.message.answer(t(lang, "🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍", "🌸 JazakAllahu khayran! May your good deeds become a key to the gates of Paradise 🤍"))

    if campaign == "water":
        await call.message.answer(await water_text(lang), parse_mode="Markdown", reply_markup=kb_back_to_campaigns(lang))
    elif campaign == "iftar":
        raised = int(await kv_get("iftar_raised_portions") or "0")
        await call.message.answer(await iftar_text(lang), parse_mode="Markdown",
                                  reply_markup=kb_iftar_portions(lang, call.from_user.id == ADMIN_ID, raised >= 100))

@dp.callback_query(lambda c: c.data in {"fitr_fmt_umm", "fitr_fmt_abu", "fitr_fmt_name"})
async def fitr_format_choice(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang = prefs[0] if prefs else "ru"
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
    await call.message.answer(t(lang, "Введите имя:", "Enter the name:"))


# ================= Text input =================

@dp.message()
async def text_input(message: Message):
    if not message.from_user:
        return

    prefs = await get_user_prefs(message.from_user.id)
    lang, method = prefs if prefs else ("ru", "stars")
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

    if ctx["kind"] == "other_eur":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно целое число > 0.", "Please send a whole number > 0."))
            return

        campaign = ctx["campaign"]
        note = ctx["note"]
        eur = n
        PENDING.pop(message.from_user.id, None)

        if campaign == "eid" and not await is_eid_open():
            await message.answer(t(lang, "Сбор на Ид сейчас закрыт.", "Eid collection is currently closed."))
            return

        if method == "stars":
            title_map = {
                "water": t(lang, "Сукья-ль-ма (вода)", "Sukya-l-ma (Water)"),
                "eid": t(lang, "Ид аль-Фитр — сладости детям", "Eid sweets for children"),
            }
            payload_map = {
                "water": f"water:eur:{eur}",
                "eid": f"eid:eur:{eur}",
            }
            await send_stars_invoice(message.from_user.id, lang, title_map.get(campaign, "Donation"),
                                     t(lang, f"Пожертвование: {eur}€", f"Donation: {eur}€"),
                                     payload_map.get(campaign, f"don:eur:{eur}"), eur)
            return

        await send_manual_instructions(message, lang, method, campaign, eur, note)
        return

    if ctx["kind"] == "other_portions" and ctx["campaign"] == "iftar":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно целое число > 0.", "Please send a whole number > 0."))
            return

        portions = n
        eur = portions * 4
        day = int(await kv_get("iftar_day") or "20")
        note = f"MIMAX-IFTAR-{day}"
        PENDING.pop(message.from_user.id, None)

        if method == "stars":
            stars = eur * EUR_TO_STARS
            title_ru = f"Программа ифтаров — {day} Рамадана"
            title_en = f"Iftars — {day} of Ramadan"
            await bot.send_invoice(
                chat_id=message.from_user.id,
                title=(title_ru if lang == "ru" else title_en),
                description=t(lang, f"{portions} порций (≈ {stars}⭐)", f"{portions} portions (≈ {stars}⭐)"),
                payload=f"iftar:portions:{portions}",
                currency="XTR",
                prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
                provider_token="",
            )
            return

        extra = t(lang, f"Порций: *{portions}*", f"Portions: *{portions}*")
        await send_manual_instructions(message, lang, method, "iftar", eur, note, extra_line=extra)
        return

    if ctx["kind"] == "other_members" and ctx["campaign"] == "fitr":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Нужно целое число > 0.", "Please send a whole number > 0."))
            return

        if method not in {"paypal", "zen"}:
            await message.answer(t(lang, "Для Закят-уль-Фитр доступны только PayPal и ZEN.", "Only PayPal and ZEN are available for Zakat al-Fitr."))
            return

        if not fitr_method_open(method):
            await message.answer(fitr_close_text(method, lang))
            return

        people = n
        price = int(await kv_get("fitr_saa_eur") or "10")
        eur = people * price
        note = "ZF"
        PENDING.pop(message.from_user.id, None)

        extra = t(lang, f"Количество человек: *{people}* (× {price}€)", f"People: *{people}* (× {price}€)")
        await send_manual_instructions(message, lang, method, "fitr", eur, note, extra_line=extra)
        return

    if ctx["kind"] == "fitr_identity":
        if ctx.get("step") == "name":
            ctx["name"] = raw
            ctx["step"] = "country"
            PENDING[message.from_user.id] = ctx
            await message.answer(t(lang, "Введите страну:", "Enter the country:"))
            return

        if ctx.get("step") == "country":
            country = raw
            fmt = ctx.get("fmt", "name")
            name = ctx.get("name", "")

            if fmt == "umm":
                display_name = f"Умм {name}, {country}"
            elif fmt == "abu":
                display_name = f"Абу {name}, {country}"
            else:
                display_name = f"{name}, {country}"

            method2 = ctx["method"]
            amount_eur = int(ctx["amount_eur"])
            people_count = int(ctx["people_count"])

            username = message.from_user.username or ""
            row_id = await add_fitr_person(message.from_user.id, username, method2, display_name, people_count, amount_eur)
            await kv_inc_int("fitr_raised_eur", amount_eur)
            PENDING.pop(message.from_user.id, None)

            total_eur, total_people, total_kg = await fitr_totals()
            await notify_admin(
                "📩 ZAKAT AL-FITR PAID\n"
                f"№ in list: {row_id}\n"
                f"Display name: {display_name}\n"
                f"Method: {method2}\n"
                f"Amount: {amount_eur} EUR\n"
                f"People: {people_count}\n"
                f"Rice: {people_count * 3} kg\n"
                f"User: @{username or '-'}\n"
                f"Link: {user_link(message.from_user.id)}\n"
                f"UserID: {message.from_user.id}\n\n"
                f"TOTALS -> EUR: {total_eur}, PEOPLE: {total_people}, KG: {total_kg}"
            )

            await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
            await message.answer(await fitr_text(lang), parse_mode="Markdown", reply_markup=kb_back_to_campaigns(lang))
            return

    if ctx["kind"] == "eid_confirm_amount":
        n = parse_positive_int(raw)
        if not n:
            await message.answer(t(lang, "Напишите сумму цифрой в евро.", "Send the amount as digits in EUR."))
            return

        amount_eur = n
        method2 = ctx["method"]
        note = ctx["note"]
        username = message.from_user.username or ""
        pid = await add_manual_payment(message.from_user.id, username, method2, "eid", amount_eur, note)
        await kv_inc_int("eid_raised_eur", amount_eur)
        PENDING.pop(message.from_user.id, None)

        await notify_admin(
            "📩 EID PAYMENT MARKED\n"
            f"ID: {pid}\n"
            f"Method: {method2}\n"
            f"Campaign: eid\n"
            f"Amount: {amount_eur} EUR\n"
            f"Note: {note}\n"
            f"User: @{username or '-'}\n"
            f"Link: {user_link(message.from_user.id)}\n"
            f"UserID: {message.from_user.id}"
        )

        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        await message.answer(await eid_text(lang), parse_mode="Markdown", reply_markup=kb_back_to_campaigns(lang))
        return


# ================= Copy helpers =================

@dp.callback_query(lambda c: c.data in {"copy_usdt", "copy_usdc"} or c.data.startswith("copy_note|"))
async def copy_items(call: CallbackQuery):
    prefs = await get_user_prefs(call.from_user.id)
    lang = prefs[0] if prefs else "ru"
    await call.answer()

    if call.data == "copy_usdt":
        await call.message.answer(f"`{USDT_TRC20}`", parse_mode="Markdown")
        return

    if call.data == "copy_usdc":
        await call.message.answer(f"`{USDC_ERC20}`", parse_mode="Markdown")
        return

    if call.data.startswith("copy_note|"):
        note = call.data.split("|", 1)[1]
        await call.message.answer(f"`{note}`", parse_mode="Markdown")
        return


# ================= Stars payments =================

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""
    prefs = await get_user_prefs(message.from_user.id)
    lang = prefs[0] if prefs else "ru"

    try:
        typ, unit, val = payload.split(":")
        val_i = int(val)
    except Exception:
        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        return

    if typ == "water" and unit == "eur":
        await kv_inc_int("water_raised_eur", val_i)
        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        return

    if typ == "eid" and unit == "eur":
        await kv_inc_int("eid_raised_eur", val_i)
        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        return

    if typ == "iftar" and unit == "portions":
        old_raised = int(await kv_get("iftar_raised_portions") or "0")
        new_raised = old_raised + val_i
        await kv_set("iftar_raised_portions", str(new_raised))

        day = int(await kv_get("iftar_day") or "20")
        done = done_list(await kv_get("iftar_done_days"))
        if new_raised >= 100 and day not in done:
            done.append(day)
            await kv_set("iftar_done_days", done_str(done))

        await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")
        return

    await message.answer("🌸 Джазак Аллаху хейр! Пусть ваши благие дела станут ключом к вратам Рая 🤍")


# ================= Admin commands =================

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

@dp.message(Command("set_iftar_day"))
async def cmd_set_iftar_day(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_iftar_day 20")
        return
    await kv_set("iftar_day", str(int(parts[1])))
    await kv_set("iftar_raised_portions", "0")
    await kv_set("iftar_target_portions", "100")
    await message.answer("OK")

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

@dp.message(Command("set_fitr_saa"))
async def cmd_set_fitr_saa(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_fitr_saa 10")
        return
    await kv_set("fitr_saa_eur", str(int(parts[1])))
    await message.answer("OK")

@dp.message(Command("open_fitr"))
async def cmd_open_fitr(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or parts[1] not in {"on", "off", "auto"}:
        await message.answer("Использование: /open_fitr on|off|auto")
        return
    await kv_set("fitr_open_mode", parts[1])
    await message.answer("OK")

@dp.message(Command("open_eid"))
async def cmd_open_eid(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or parts[1] not in {"on", "off", "auto"}:
        await message.answer("Использование: /open_eid on|off|auto")
        return
    await kv_set("eid_open_mode", parts[1])
    await message.answer("OK")

@dp.message(Command("eid_extra_day"))
async def cmd_eid_extra_day(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or parts[1] not in {"on", "off"}:
        await message.answer("Использование: /eid_extra_day on|off")
        return
    await kv_set("eid_extra_day", parts[1])
    await message.answer("OK")

@dp.message(Command("set_eid_target"))
async def cmd_set_eid_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: /set_eid_target 0")
        return
    await kv_set("eid_target_eur", str(int(parts[1])))
    await message.answer("OK")

@dp.message(Command("add_fitr"))
async def cmd_add_fitr(message: Message):
    if not admin_only(message):
        return
    raw = (message.text or "").replace("/add_fitr", "", 1).strip()
    parts = [x.strip() for x in raw.split(";")]
    if len(parts) != 4:
        await message.answer("Использование: /add_fitr Имя, страна; people; amount; paypal|zen")
        return

    display_name = parts[0]
    people = int(parts[1])
    amount = int(parts[2])
    method = parts[3].lower()

    if method not in {"paypal", "zen"}:
        await message.answer("method: paypal|zen")
        return

    row_id = await add_fitr_person(ADMIN_ID, "admin", method, display_name, people, amount)
    await kv_inc_int("fitr_raised_eur", amount)
    total_eur, total_people, total_kg = await fitr_totals()
    await message.answer(f"OK #{row_id}\nTOTALS -> EUR: {total_eur}, PEOPLE: {total_people}, KG: {total_kg}")


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
