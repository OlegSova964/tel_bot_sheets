import asyncio
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F
import sys
import os
import json
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
load_dotenv()

# === НАСТРОЙКИ ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
USER_SETTINGS = {
    7749330261: {"sheet": "Name video Oleh Owl", "tag": "Oleh Owl"},
    7649695975: {"sheet": "Name video Iryna Techno", "tag": "Iryna Techno"}
}

# === Google Sheets Авторизация ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
client = gspread.authorize(creds)

# === Инициализация бота ===
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(
    parse_mode=ParseMode.HTML))
dp = Dispatcher()


@dp.message(F.text == "/stop")
async def stop_bot(message: Message):
    await message.answer("🛑 Бот остановлен.")
    await bot.session.close()
    await asyncio.sleep(0.5)
    sys.exit()


@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    if user_id not in USER_SETTINGS:
        await message.answer("⛔ Извините, доступ запрещён.")
        return

    text = message.text or message.caption

    try:
        lines = text.splitlines()

        file_line = next(
            line for line in lines if "Название файла:" in line or "Название крео:" in line or "Файл:" in line)
        card_line = next(
            line for line in lines if "Название карточки:" in line or "Название:" in line or "Карточка:" in line)

        file_value = file_line.split(":", 1)[1].strip()
        card_value = card_line.split(":", 1)[1].strip()

        # Получаем настройки пользователя
        user_settings = USER_SETTINGS[user_id]
        sheet = client.open(user_settings["sheet"]).sheet1
        tag = user_settings["tag"]

        # Добавляем в таблицу 3 столбец
        sheet.append_row([file_value, card_value, tag])

        await message.answer("✅ Данные успешно записаны!")
    except Exception as e:
        await message.answer("⚠️ Ошибка! Убедись, что формат такой:\n\n"
                             "Название файла: Example\nНазвание карточки : Example / 01.01 / ...")
        print("Ошибка:", e)


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
# source venv/bin/activate
# python3 main2user.py
