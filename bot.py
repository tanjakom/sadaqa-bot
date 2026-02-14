import os
import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

from aiogram.utils.keyboard import InlineKeyboardBuilder

def main_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 View campaigns", callback_data="list")
    kb.button(text="🌍 Display currency", callback_data="currency")
    kb.adjust(1)
    return kb.as_markup()
    
@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "Assalamu alaykum 🤍\n\n"
        "Welcome. This bot will accept support via Telegram Stars.\n"
        "For now we are preparing campaigns.\n\n"
        "Use the buttons below:",
        reply_markup=main_kb()
    )


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

