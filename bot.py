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
@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "Ассаляму алейкум 🤍\n\n"
        "Это бот для сбора садака.\n"
        "Поддержите наш сбор через Telegram Stars.\n\n"
        "Скоро здесь появятся активные сборы."
    )
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
