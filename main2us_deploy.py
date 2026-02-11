import asyncio
import sys
import os
import random
import time
from typing import Optional, Dict, Any, List, Tuple
from collections import deque

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ================== CONFIG ==================

BOT_TOKEN = os.getenv("BOT_TOKEN")

USER_SETTINGS: Dict[int, Dict[str, str]] = {
    7749330261: {"sheet": "Name video Oleh Owl", "tag": "Oleh Owl"},
    7649695975: {"sheet": "Name video Iryna Techno", "tag": "Iryna Techno"},
}

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
client = gspread.authorize(creds)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Последовательная запись в таблицу
sheet_lock = asyncio.Lock()

# ===== альбом-буфер =====
ALBUMS: Dict[str, Dict[str, Any]] = {}
ALBUM_WAIT_SEC = 1.2  # ждём "тишину" после последнего сообщения альбома

# ===== очередь записи (надежно дожимаем Google Sheets) =====
# items: (user_id, chat_id, file, creo, name, tag, future)
WRITE_QUEUE = deque()
QUEUE_WORKER_TASK: Optional[asyncio.Task] = None


# ================== HELPERS ==================

def now_ts() -> float:
    return time.time()


def format_hint() -> str:
    return (
        "⚠️ Не вижу данных для записи.\n\n"
        "Пришли так (можно просто текстом, без видео):\n"
        "Название крео: 55IEToday(GPT)O'Leary\n"
        "Название: Andrii Soprano / 05.02 / IE / 5\n\n"
        "Или отправь видео/файл с этой подписью."
    )


def strip_ext(name: str) -> str:
    # "a.b.c.mp4" -> "a.b.c"
    return os.path.splitext(name)[0]


def extract_meta(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Понимает:
      - Название крео:
      - Название:
    + терпит варианты:
      - Крео:
      - Creo:
      - Name:
    """
    if not text:
        return None, None

    creo = None
    nm = None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        low = ln.lower()

        if ("название крео" in low or low.startswith("крео:") or low.startswith("creo:")) and ":" in ln:
            v = ln.split(":", 1)[1].strip()
            if v:
                creo = v

        if (low.startswith("название:") or low.startswith("name:")) and ":" in ln:
            v = ln.split(":", 1)[1].strip()
            if v:
                nm = v

    return creo, nm


def get_filename_or_fallback(message: Message) -> Optional[str]:
    if message.document:
        if message.document.file_name:
            return message.document.file_name
        return f"{message.document.file_unique_id}.bin"

    if message.video:
        if getattr(message.video, "file_name", None):
            return message.video.file_name
        return f"{message.video.file_unique_id}.mp4"

    if message.audio:
        if message.audio.file_name:
            return message.audio.file_name
        return f"{message.audio.file_unique_id}.mp3"

    return None


def is_any_file(message: Message) -> bool:
    return bool(message.document or message.video or message.audio)


def sync_sleep(seconds: float) -> None:
    time.sleep(seconds)


def append_row_with_retry(sheet, row, retries: int = 10) -> None:
    for attempt in range(retries):
        try:
            sheet.append_row(row)
            return
        except Exception as e:
            msg = str(e)
            transient = (
                "429", "503", "Rate Limit", "RESOURCE_EXHAUSTED",
                "Quota", "quota", "Too Many Requests", "timeout", "Timeout"
            )
            if attempt == retries - 1 or not any(t in msg for t in transient):
                raise
            sync_sleep((2 ** attempt) * 0.6 + random.uniform(0, 0.6))


# ================== QUEUE ==================

async def ensure_queue_worker():
    global QUEUE_WORKER_TASK
    if QUEUE_WORKER_TASK is None or QUEUE_WORKER_TASK.done():
        QUEUE_WORKER_TASK = asyncio.create_task(queue_worker())


async def enqueue_write(
    user_id: int,
    chat_id: int,
    file_value: str,
    creo_value: str,
    name_value: str,
    tag: str
) -> asyncio.Future:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    WRITE_QUEUE.append((user_id, chat_id, file_value, creo_value, name_value, tag, fut))
    await ensure_queue_worker()
    return fut


async def queue_worker():
    while True:
        if not WRITE_QUEUE:
            await asyncio.sleep(0.2)
            continue

        user_id, chat_id, file_value, creo_value, name_value, tag, fut = WRITE_QUEUE[0]
        sheet_name = USER_SETTINGS[user_id]["sheet"]

        sheet = None
        for _ in range(6):
            try:
                sheet = client.open(sheet_name).sheet1
                break
            except Exception:
                await asyncio.sleep(0.7)

        if sheet is None:
            await asyncio.sleep(1.2)
            continue

        try:
            async with sheet_lock:
                # Колонки: file | creo | name | tag
                append_row_with_retry(sheet, [file_value, creo_value, name_value, tag])

            if fut and not fut.done():
                fut.set_result(True)

            WRITE_QUEUE.popleft()

        except Exception:
            await asyncio.sleep(1.0)


# ================== ALBUM FLUSH ==================

async def flush_album(media_group_id: str):
    await asyncio.sleep(ALBUM_WAIT_SEC)

    state = ALBUMS.get(media_group_id)
    if not state:
        return

    if now_ts() - float(state["last_ts"]) < ALBUM_WAIT_SEC:
        state["flush_task"] = asyncio.create_task(flush_album(media_group_id))
        return

    ALBUMS.pop(media_group_id, None)

    user_id = state["user_id"]
    chat_id = state["chat_id"]
    items: List[str] = state["items"]

    creo = state.get("creo")
    name = state.get("name")

    if not (creo and name):
        await bot.send_message(chat_id, format_hint())
        return

    tag = USER_SETTINGS[user_id]["tag"]
    await bot.send_message(chat_id, f"✅ Принято: {len(items)} файлов. Записываю...")

    futs = []
    for fname in items:
        clean = strip_ext(fname)
        futs.append(await enqueue_write(user_id, chat_id, clean, creo, name, tag))

    await asyncio.gather(*futs)
    await bot.send_message(chat_id, "Готово!")


# ================== HANDLERS ==================

@dp.message(F.text == "/stop")
async def stop_bot(message: Message):
    await message.answer("🛑 Бот остановлен.")
    await bot.session.close()
    await asyncio.sleep(0.2)
    sys.exit(0)


@dp.message()
async def handle_message(message: Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    if user_id not in USER_SETTINGS:
        await message.answer("⛔ Доступ запрещён.")
        return

    text = message.text or message.caption or ""
    creo, name = extract_meta(text)
    media_group_id = str(message.media_group_id) if message.media_group_id else None

    # ===== АЛЬБОМ =====
    if media_group_id:
        state = ALBUMS.get(media_group_id)
        if not state:
            state = {
                "user_id": user_id,
                "chat_id": chat_id,
                "creo": None,
                "name": None,
                "items": [],
                "flush_task": None,
                "last_ts": now_ts(),
            }
            ALBUMS[media_group_id] = state

        state["last_ts"] = now_ts()

        # meta из подписи любого элемента
        if creo:
            state["creo"] = creo
        if name:
            state["name"] = name

        fname = get_filename_or_fallback(message)
        if fname:
            state["items"].append(fname)

        task = state.get("flush_task")
        if task and not task.done():
            task.cancel()
        state["flush_task"] = asyncio.create_task(flush_album(media_group_id))
        return

    # ===== НЕ АЛЬБОМ =====

    tag = USER_SETTINGS[user_id]["tag"]

    # 1) Если это просто текст БЕЗ файла — СРАЗУ пишем строку и не ждём видео
    if not is_any_file(message):
        if not (creo and name):
            await message.answer(format_hint())
            return

        fut = await enqueue_write(user_id, chat_id, "", creo, name, tag)
        await message.answer("✅ Принято (без файла). Записываю...")
        await fut
        await message.answer("Готово! Жду следующую порцию.")
        return

    # 2) Если пришёл файл — требуем, чтобы meta была В ЭТОМ ЖЕ сообщении (caption)
    if not (creo and name):
        await message.answer("⚠️ Для файла нужно добавить подпись с данными.\n\n" + format_hint())
        return

    fname = get_filename_or_fallback(message)
    if not fname:
        await message.answer("⚠️ Пришли файл/видео документом/видео/аудио.")
        return

    clean = strip_ext(fname)
    fut = await enqueue_write(user_id, chat_id, clean, creo, name, tag)
    await message.answer("✅ Принято. Записываю...")
    await fut
    await message.answer("Готово! Жду следующую порцию.")


# ================== RUN ==================

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN в .env")

    await ensure_queue_worker()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
