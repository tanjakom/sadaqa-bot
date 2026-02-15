import os
import logging
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, LabeledPrice, PreCheckoutQuery
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Helsinki")
EUR_TO_STARS = int(os.getenv("EUR_TO_STARS", "10") or "10")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

DB_PATH = "data.db"

# --- Simple per-user pending input state (for "other amount") ---
PENDING = {}  # user_id -> {"type": "water"|"iftar", "prompted": True}

# -------------------- Helpers --------------------

def is_ru(lang: str | None) -> bool:
    return (lang or "").lower().startswith("ru")

def battery(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    return "▰" * filled + "▱" * (width - filled)

def eur_to_stars(eur: int) -> int:
    return max(1, int(eur) * EUR_TO_STARS)

def portions_to_stars(portions: int) -> int:
    # 1 portion = 4 EUR -> 4 * EUR_TO_STARS stars
    return max(1, int(portions) * 4 * EUR_TO_STARS)

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """)
        # defaults
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_target_eur','235')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('water_raised_eur','0')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_day','1')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_target_portions','100')")
        await db.execute("INSERT OR IGNORE INTO kv(k,v) VALUES('iftar_raised_portions','0')")
        await db.commit()

async def kv_get(key: str) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT v FROM kv WHERE k=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""

async def kv_set(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))
        await db.commit()

async def kv_inc_int(key: str, delta: int):
    val = int(await kv_get(key) or "0")
    val += int(delta)
    await kv_set(key, str(val))

def main_kb(lang: str | None = "ru"):
    kb = InlineKeyboardBuilder()
    if is_ru(lang):
        kb.button(text="📋 Сборы", callback_data="list")
        kb.button(text="🌍 Валюта", callback_data="currency")
    else:
        kb.button(text="📋 Campaigns / Сборы", callback_data="list")
        kb.button(text="🌍 Currency / Валюта", callback_data="currency")
    kb.adjust(1)
    return kb.as_markup()

def list_kb(lang: str | None = "ru"):
    kb = InlineKeyboardBuilder()
    if is_ru(lang):
        kb.button(text="💧 Сукья-ль-ма (вода)", callback_data="water")
        kb.button(text="🍲 Ифтары (завтра)", callback_data="iftar")
        kb.button(text="⬅️ Назад", callback_data="back")
    else:
        kb.button(text="💧 Water / Сукья-ль-ма", callback_data="water")
        kb.button(text="🍲 Iftar (tomorrow) / Ифтары", callback_data="iftar")
        kb.button(text="⬅️ Back / Назад", callback_data="back")
    kb.adjust(1)
    return kb.as_markup()

def water_pay_kb(lang: str | None = "ru"):
    kb = InlineKeyboardBuilder()
    if is_ru(lang):
        kb.button(text="⭐ 10€", callback_data="pay_water_10")
        kb.button(text="⭐ 25€", callback_data="pay_water_25")
        kb.button(text="⭐ 50€", callback_data="pay_water_50")
        kb.button(text="⭐ Другая сумма", callback_data="pay_water_other")
        kb.button(text="⬅️ Назад", callback_data="list")
    else:
        kb.button(text="⭐ 10€", callback_data="pay_water_10")
        kb.button(text="⭐ 25€", callback_data="pay_water_25")
        kb.button(text="⭐ 50€", callback_data="pay_water_50")
        kb.button(text="⭐ Other amount / Другая", callback_data="pay_water_other")
        kb.button(text="⬅️ Back / Назад", callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

def iftar_pay_kb(lang: str | None = "ru"):
    kb = InlineKeyboardBuilder()
    if is_ru(lang):
        kb.button(text="⭐ 5 порций", callback_data="pay_iftar_5")
        kb.button(text="⭐ 10 порций", callback_data="pay_iftar_10")
        kb.button(text="⭐ 20 порций", callback_data="pay_iftar_20")
        kb.button(text="⭐ Другое количество", callback_data="pay_iftar_other")
        kb.button(text="⬅️ Назад", callback_data="list")
    else:
        kb.button(text="⭐ 5 portions / порций", callback_data="pay_iftar_5")
        kb.button(text="⭐ 10 portions / порций", callback_data="pay_iftar_10")
        kb.button(text="⭐ 20 portions / порций", callback_data="pay_iftar_20")
        kb.button(text="⭐ Other qty / Другое", callback_data="pay_iftar_other")
        kb.button(text="⬅️ Back / Назад", callback_data="list")
    kb.adjust(2, 2, 1)
    return kb.as_markup()

async def water_text(lang: str | None):
    target = int(await kv_get("water_target_eur") or "235")
    raised = int(await kv_get("water_raised_eur") or "0")
    bar = battery(raised, target)
    if is_ru(lang):
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

async def iftar_text(lang: str | None):
    day = int(await kv_get("iftar_day") or "1")
    target = int(await kv_get("iftar_target_portions") or "100")
    raised = int(await kv_get("iftar_raised_portions") or "0")
    bar = battery(raised, target)
    if is_ru(lang):
        return (
            f"🍲 *Ифтары Рамадана — День {day} (завтра)*\n\n"
            f"Цель: *{target} порций*\n"
            f"Собрано: *{raised}* / *{target}*\n"
            f"{bar}\n\n"
            "1 порция = 4€ (≈ 40 ⭐ при курсе 1€=10⭐)\n"
            "Выберите количество порций:"
        )
    return (
        f"🍲 *Ramadan Iftar — Day {day} (tomorrow)*\n\n"
        f"Goal: *{target} portions*\n"
        f"Raised: *{raised}* / *{target}*\n"
        f"{bar}\n\n"
        "1 portion = 4€ (≈ 40 ⭐ at 1€=10⭐)\n"
        "Choose quantity:"
    )

def admin_only(message: Message) -> bool:
    return ADMIN_ID != 0 and message.from_user and message.from_user.id == ADMIN_ID

async def schedule_midnight_rollover():
    """
    At local midnight (Helsinki), increment iftar_day by 1 if current day is already closed (raised >= target).
    This is a gentle automation: you can still change targets manually any time.
    """
    tz = ZoneInfo(TIMEZONE)
    while True:
        now = datetime.now(tz)
        tomorrow = (now + timedelta(days=1)).date()
        next_midnight = datetime.combine(tomorrow, datetime.min.time(), tzinfo=tz)
        sleep_seconds = (next_midnight - now).total_seconds()
        await asyncio.sleep(max(1, int(sleep_seconds)))

        # after midnight: if day closed, advance
        try:
            target = int(await kv_get("iftar_target_portions") or "100")
            raised = int(await kv_get("iftar_raised_portions") or "0")
            if raised >= target:
                await kv_inc_int("iftar_day", 1)
                await kv_set("iftar_raised_portions", "0")
        except Exception as e:
            logging.exception("Midnight rollover error: %s", e)

# -------------------- Telegram handlers --------------------

@dp.message(Command("start"))
async def start_handler(message: Message):
    lang = message.from_user.language_code if message.from_user else "ru"
    if is_ru(lang):
        text = (
            "Ассаляму алейкум 🤍\n\n"
            "Добро пожаловать.\n"
            "Этот бот принимает поддержку через Telegram Stars.\n\n"
            "Выберите действие ниже:"
        )
    else:
        text = (
            "Ассаляму алейкум 🤍 / Assalamu alaykum 🤍\n\n"
            "Этот бот принимает поддержку через Telegram Stars.\n"
            "This bot accepts support via Telegram Stars.\n\n"
            "Выберите действие / Choose an action:"
        )
    await message.answer(text, reply_markup=main_kb(lang))

@dp.callback_query(lambda c: c.data in {"back", "list", "currency", "water", "iftar"})
async def menu_callbacks(call: CallbackQuery):
    lang = call.from_user.language_code
    if call.data == "back":
        await call.message.edit_text("Меню / Menu:", reply_markup=main_kb(lang))
        await call.answer()
        return

    if call.data == "list":
        await call.message.edit_text("Выберите сбор / Choose:", reply_markup=list_kb(lang))
        await call.answer()
        return

    if call.data == "currency":
        if is_ru(lang):
            msg = f"Курс в боте: *1€ = {EUR_TO_STARS}⭐*\nОплата проходит в Telegram Stars."
        else:
            msg = f"Bot rate: *1€ = {EUR_TO_STARS}⭐*\nPayments are via Telegram Stars."
        await call.message.edit_text(msg, reply_markup=main_kb(lang), parse_mode="Markdown")
        await call.answer()
        return

    if call.data == "water":
        await call.message.edit_text(await water_text(lang), reply_markup=water_pay_kb(lang), parse_mode="Markdown")
        await call.answer()
        return

    if call.data == "iftar":
        await call.message.edit_text(await iftar_text(lang), reply_markup=iftar_pay_kb(lang), parse_mode="Markdown")
        await call.answer()
        return

# --- Pay callbacks ---
@dp.callback_query(lambda c: c.data.startswith("pay_water_"))
async def pay_water(call: CallbackQuery):
    lang = call.from_user.language_code
    if call.data == "pay_water_other":
        PENDING[call.from_user.id] = {"type": "water"}
        if is_ru(lang):
            await call.message.answer("Введите сумму в евро (целое число), например 12:")
        else:
            await call.message.answer("Enter amount in EUR (whole number), e.g. 12:")
        await call.answer()
        return

    eur = int(call.data.split("_")[-1])
    stars = eur_to_stars(eur)
    payload = f"water:eur:{eur}"

    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="Сукья-ль-ма (вода) / Water",
        description=f"Пожертвование: {eur}€ (≈ {stars}⭐)",
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{eur} EUR donation", amount=stars)],
        provider_token="",  # Stars payments: empty
    )
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("pay_iftar_"))
async def pay_iftar(call: CallbackQuery):
    lang = call.from_user.language_code
    if call.data == "pay_iftar_other":
        PENDING[call.from_user.id] = {"type": "iftar"}
        if is_ru(lang):
            await call.message.answer("Введите количество порций (целое число), например 7:")
        else:
            await call.message.answer("Enter number of portions (whole number), e.g. 7:")
        await call.answer()
        return

    portions = int(call.data.split("_")[-1])
    stars = portions_to_stars(portions)
    payload = f"iftar:portions:{portions}"

    day = int(await kv_get("iftar_day") or "1")
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=f"Ифтары Рамадана — День {day} / Iftar Day {day}",
        description=f"{portions} порций (≈ {stars}⭐)",
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
        provider_token="",  # Stars payments: empty
    )
    await call.answer()

# --- "Other amount" input ---
@dp.message()
async def other_amount_input(message: Message):
    if not message.from_user:
        return
    st = PENDING.get(message.from_user.id)
    if not st:
        return

    lang = message.from_user.language_code
    raw = (message.text or "").strip()

    try:
        n = int(raw)
        if n <= 0:
            raise ValueError
    except Exception:
        if is_ru(lang):
            await message.answer("Нужно целое число больше 0. Попробуйте ещё раз:")
        else:
            await message.answer("Please send a whole number > 0. Try again:")
        return

    if st["type"] == "water":
        eur = n
        stars = eur_to_stars(eur)
        payload = f"water:eur:{eur}"
        await bot.send_invoice(
            chat_id=message.from_user.id,
            title="Сукья-ль-ма (вода) / Water",
            description=f"Пожертвование: {eur}€ (≈ {stars}⭐)",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{eur} EUR donation", amount=stars)],
            provider_token="",
        )
        PENDING.pop(message.from_user.id, None)
        return

    if st["type"] == "iftar":
        portions = n
        stars = portions_to_stars(portions)
        payload = f"iftar:portions:{portions}"
        day = int(await kv_get("iftar_day") or "1")
        await bot.send_invoice(
            chat_id=message.from_user.id,
            title=f"Ифтары Рамадана — День {day} / Iftar Day {day}",
            description=f"{portions} порций (≈ {stars}⭐)",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{portions} portions", amount=stars)],
            provider_token="",
        )
        PENDING.pop(message.from_user.id, None)
        return

# --- Payments flow ---
@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre.id, ok=True)

@dp.message(lambda m: getattr(m, "successful_payment", None) is not None)
async def successful_payment(message: Message):
    sp = message.successful_payment
    payload = sp.invoice_payload or ""
    lang = message.from_user.language_code if message.from_user else "ru"

    # payload format: type:unit:value
    try:
        t, unit, val = payload.split(":")
        val_i = int(val)
    except Exception:
        await message.answer("Платёж получен. Спасибо! / Payment received. Thank you!")
        return

    if t == "water" and unit == "eur":
        await kv_inc_int("water_raised_eur", val_i)
        txt = await water_text(lang)
        await message.answer("✅ Спасибо! Платёж получен.\n\n" + txt, reply_markup=water_pay_kb(lang), parse_mode="Markdown")
        return

    if t == "iftar" and unit == "portions":
        await kv_inc_int("iftar_raised_portions", val_i)
        txt = await iftar_text(lang)
        await message.answer("✅ Спасибо! Платёж получен.\n\n" + txt, reply_markup=iftar_pay_kb(lang), parse_mode="Markdown")
        return

    await message.answer("✅ Спасибо! / Thank you!")

# --- Admin commands (optional) ---
@dp.message(Command("set_iftar_day"))
async def cmd_set_iftar_day(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Usage: /set_iftar_day 7")
        return
    await kv_set("iftar_day", str(int(parts[1])))
    await message.answer("OK")

@dp.message(Command("set_iftar_target"))
async def cmd_set_iftar_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Usage: /set_iftar_target 100")
        return
    await kv_set("iftar_target_portions", str(int(parts[1])))
    await message.answer("OK")

@dp.message(Command("set_water_target"))
async def cmd_set_water_target(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Usage: /set_water_target 235")
        return
    await kv_set("water_target_eur", str(int(parts[1])))
    await message.answer("OK")

@dp.message(Command("set_rate"))
async def cmd_set_rate(message: Message):
    if not admin_only(message):
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Usage: /set_rate 10")
        return
    # rate is env-based in this MVP; we show message and rely on Render env update
    await message.answer("В этом MVP курс меняется через Render Environment Variable EUR_TO_STARS.\n"
                         "In this MVP, change EUR_TO_STARS in Render Environment.")

@dp.message(Command("close_iftar_day"))
async def cmd_close_iftar_day(message: Message):
    if not admin_only(message):
        return
    await kv_inc_int("iftar_day", 1)
    await kv_set("iftar_raised_portions", "0")
    await message.answer("OK: next day activated")

# --- Health server for Render port binding ---
async def health_server():
    app = web.Application()

    async def health(request):
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
    asyncio.create_task(schedule_midnight_rollover())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
