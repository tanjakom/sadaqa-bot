import os
import logging
import asyncio
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
def is_ru(lang: str | None) -> bool:
    return (lang or "").lower().startswith("ru")

dp = Dispatcher()

from aiogram.utils.keyboard import InlineKeyboardBuilder

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
   
@dp.message(Command("start"))
async def start_handler(message: Message):
    lang = message.from_user.language_code

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
    await health_server()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

