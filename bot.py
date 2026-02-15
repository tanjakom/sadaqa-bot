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

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
DEFAULT_EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "50") or "50")  # дефолт, если в базе ещё нет

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DB_PATH = "data.db"

# user_id -> {"type": "water"|"iftar"}
PENDING = {}


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
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('eur_to_stars', ?)", (str(DEFAULT_EUR_TO_STARS),))
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


# ---------------- Helpers ----------------

def admin_only(message: Message) -> bool:
    return ADMIN_ID != 0 and message.from_user and message.from_user.id == ADMIN_ID

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

def kb_lang_select():
    kb = InlineKeyboardBuilder()
    kb.button(text="Русский", callback_data="lang_ru")
    kb.button(text="English", callback_data="lang_en")
    kb.adjust(2)
    return kb.as_markup()

def kb_main(lang: str):
    kb = InlineKeyboardBuilder()
    if lang == "ru":
        kb.button(text="📋 Сборы", callback_data="list")
        kb.button(text="ℹ️ О Stars", callback_data="stars_info")
        kb.button(text="🌐 Язык", callback_data="lang_menu")
    else:
        kb.button(text="📋 Campaigns", callback_data="list")
        kb.button(text="ℹ️ About Stars", callback_data="stars_info")
        kb.button(text="🌐 Language", callback_data="lang_menu")
    kb.adjust(1)
    return kb.as_markup()

def kb_list(lang: str):
    kb = InlineKeyboardBuilder()
    if lang == "ru":
        kb.button(text="💧 Сукья-ль-ма (вода)", callback_data="water")
        kb.button(text="🍲 Программа ифтаров", callback_data="iftar")
        kb.button(text="⬅️ Назад", callback_data="back")
    else:
        kb.button(text="💧 Water (Sukya-l-ma)", callback_data="water")
        kb.button(text="🍲 Iftars Program", callback_data="iftar")
        kb.button(text="⬅️ Back", callback_data="back")
    kb.adjust(1)
    return kb.as_markup()

def kb_water_pay(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text="⭐ 10€", callback_data="pay_water_10")
    kb.button(text="⭐ 25€", callback_data="pay_water_25")
    kb.button(text="⭐ 50€", callback_data="pay_water_50")
    kb.button(text=("⭐ Другая сумма" if lang == "ru" else "⭐ Other amount"), callback_data="pay_water_other")
    kb.button(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def kb_iftar_pay(lang: str):
    kb = InlineKeyboardBuilder()
    kb.button(text=("⭐ 5 порций" if lang == "ru" else "⭐ 5 portions"), callback_data="pay_iftar_5")
    kb.button(text=("⭐ 10 порций" if lang == "ru" else "⭐ 10 portions"), callback_data="pay_iftar_10")
    kb.button(text=("⭐ 20 порций" if lang == "ru" else "⭐ 20 portions"), callback_data="pay_iftar_20")
    kb.button(text=("⭐ Другое количество" if lang == "ru" else "⭐ Other qty"), callback_data="pay_iftar_other")
    kb.button(text=("⬅️ Назад" if lang == "ru" else "⬅️ Back"), callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

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
    if lang == "ru":
        text = (
            "Ассаляму алейкум 🤍\n\n"
            "Добро пожаловать.\n"
            "Этот бот принимает поддержку через Telegram Stars.\n\n"
            "Нажмите «Сборы», чтобы выбрать сбор."
        )
    else:
        text = (
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

    if lang == "ru":
        text = "Язык установлен: Русский.\n\nНажмите «Сборы», чтобы выбрать сбор."
    else:
        text = "Language set: English.\n\nTap “Campaigns” to choose a campaign."

    await safe_edit(call, text, reply_markup=kb_main(lang))

@dp.callback_query(lambda c: c.data in {"lang_menu", "back", "list", "stars_info", "water", "iftar"})
async def menu(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"

    if call.data == "lang_menu":
        await safe_edit(call, "Выберите язык / Choose language:", reply_markup=kb_lang_select())
        await call.answer()
        return

    if call.data == "back":
        await safe_edit(call, ("Меню:" if lang == "ru" else "Menu:"), reply_markup=kb_main(lang))
        await call.answer()
        return

    if call.data == "list":
        await safe_edit(call, ("Выберите сбор:" if lang == "ru" else "Choose campaign:"), reply_markup=kb_list(lang))
        await call.answer()
        return

    if call.data == "stars_info":
        rate = await get_rate()
        if lang == "ru":
            msg = (
                "Оплата проходит внутри Telegram через *Stars*.\n"
                "Для пользователя это выглядит как обычная покупка в Telegram.\n\n"
                f"Текущий курс в боте: *1€ = {rate}⭐*"
            )
        else:
            msg = (
                "Payments happen inside Telegram via *Stars*.\n"
                "For the user it looks like a regular Telegram purchase.\n\n"
                f"Current bot rate: *1€ = {rate}⭐*"
            )
        await safe_edit(call, msg, reply_markup=kb_main(lang), parse_mode="Markdown")
        await call.answer()
        return

    if call.data == "water":
        await safe_edit(call, await water_text(lang), reply_markup=kb_water_pay(lang), parse_mode="Markdown")
        await call.answer()
        return

    if call.data == "iftar":
        await safe_edit(call, await iftar_text(lang), reply_markup=kb_iftar_pay(lang), parse_mode="Markdown")
        await call.answer()
        return


# ---------- Pay callbacks ----------

@dp.callback_query(lambda c: c.data.startswith("pay_water_"))
async def pay_water(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"
    rate = await get_rate()

    if call.data == "pay_water_other":
        PENDING[call.from_user.id] = {"type": "water"}
        await call.message.answer("Введите сумму в евро (целое число), например 12:" if lang == "ru" else "Enter amount in EUR (whole number), e.g. 12:")
        await call.answer()
        return

    eur = int(call.data.split("_")[-1])
    stars = eur * rate
    payload = f"water:eur:{eur}"

    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=("Сукья-ль-ма (вода)" if lang == "ru" else "Sukya-l-ma (Water)"),
        description=(f"Пожертвование: {eur}€ (≈ {stars}⭐)" if lang == "ru" else f"Donation: {eur}€ (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur} EUR", amount=stars)],
        provider_token="",  # Stars: empty
    )
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("pay_iftar_"))
async def pay_iftar(call: CallbackQuery):
    saved = await get_user_lang(call.from_user.id)
    lang = saved or "ru"
    rate = await get_rate()

    if call.data == "pay_iftar_other":
        PENDING[call.from_user.id] = {"type": "iftar"}
        await call.message.answer("Введите количество порций (целое число), например 7:" if lang == "ru" else "Enter number of portions (whole number), e.g. 7:")
        await call.answer()
        return

    portions = int(call.data.split("_")[-1])
    stars = portions * 4 * rate
    payload = f"iftar:portions:{portions}"

    day = int(await kv_get("iftar_day") or "1")
    title_ru = f"Программа ифтаров — {day} Рамадана"
    title_en = f"Iftars — {day} of Ramadan"

    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=(title_ru if lang == "ru" else title_en),
        description=(f"{portions} порций (≈ {stars}⭐)" if lang == "ru" else f"{portions} portions (≈ {stars}⭐)"),
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
        provider_token="",  # Stars: empty
    )
    await call.answer()


# ---------- Other amount input ----------

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
        await message.answer("Нужно целое число > 0. Попробуйте ещё раз:" if lang == "ru" else "Please send a whole number > 0. Try again:")
        return

    if st["type"] == "water":
        eur = n
        stars = eur * rate
        payload = f"water:eur:{eur}"

        await bot.send_invoice(
            chat_id=message.from_user.id,
            title=("Сукья-ль-ма (вода)" if lang == "ru" else "Sukya-l-ma (Water)"),
            description=(f"Пожертвование: {eur}€ (≈ {stars}⭐)" if lang == "ru" else f"Donation: {eur}€ (≈ {stars}⭐)"),
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
            description=(f"{portions} порций (≈ {stars}⭐)" if lang == "ru" else f"{portions} portions (≈ {stars}⭐)"),
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
            provider_token="",
        )
        PENDING.pop(message.from_user.id, None)
        return


# ---------- Payments ----------

@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""

    saved = await get_user_lang(message.from_user.id)
    lang = saved or "ru"

    try:
        t, unit, val = payload.split(":")
        val_i = int(val)
    except Exception:
        await message.answer("✅ Спасибо! Платёж получен." if lang == "ru" else "✅ Thank you! Payment received.")
        return

    if t == "water" and unit == "eur":
        await kv_inc_int("water_raised_eur", val_i)
        await message.answer("✅ Спасибо! Платёж получен." if lang == "ru" else "✅ Thank you! Payment received.")
        return

    if t == "iftar" and unit == "portions":
        await kv_inc_int("iftar_raised_portions", val_i)
        await message.answer("✅ Спасибо! Платёж получен." if lang == "ru" else "✅ Thank you! Payment received.")
        return


# ---------- Admin commands ----------

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


# ---------- Health server for Render ----------

async def health_server():
    app = web.Application()

    async def health(_request):
        return web.Response(text="ok")

    app.router.add_get("/", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


async def main():
    await db_init()
    await health_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
