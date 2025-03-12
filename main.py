#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import sqlite3

from aiogram import Bot, Dispatcher, Router, types, F
from vk_api import VkApi
import asyncio
from config import VK_GROUP_ID, VK_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

# Настройки
VK_GROUP_ID = VK_GROUP_ID
VK_ACCESS_TOKEN = VK_ACCESS_TOKEN
TELEGRAM_BOT_TOKEN = TELEGRAM_BOT_TOKEN
TELEGRAM_CHANNEL_ID = TELEGRAM_CHANNEL_ID

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Логирование
logging.basicConfig(level=logging.INFO)

# Подключение к ВКонтакте
vk_session = VkApi(token=VK_ACCESS_TOKEN)
vk_api = vk_session.get_api()

# Подключение к базе данных SQLite
conn = sqlite3.connect('parsed_posts.db')
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS posts (
id INTEGER PRIMARY KEY,
post_text TEXT,
published_in_telegram BOOLEAN
)
''')
conn.commit()


@router.message(F.command == 'start')
async def send_welcome(message: types.Message):
    """
    Команда для запуска бота.
    """
    await message.reply("Привет! Я бот для парсинга постов из группы ВКонтакте и их публикации в телеграм-канале.")


def get_new_posts():
    """
    Функция для получения новых постов из группы ВКонтакте.
    """
    wall_posts = vk_api.wall.get(owner_id=-int(VK_GROUP_ID), count=100)['items']
    return wall_posts


async def post_to_telegram(post):
    """
    Функция для публикации поста в телеграм-канале.
    """
    try:
        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=post['text'])
        logging.info(f"Пост успешно опубликован в телеграм-канале: {post['text']}")
        await asyncio.sleep(5)
        return True
    except Exception as e:
        logging.error(f"Ошибка при публикации поста в телеграм-канале: {e}")
        return False


def save_post_to_db(post_text, published):
    """
    Функция для сохранения поста в базу данных.
    """
    cursor.execute('''
    INSERT INTO posts (post_text, published_in_telegram) VALUES (?, ?)
    ''', (post_text, published))
    conn.commit()


def post_exists(post_text):
    """
    Функция для проверки существования поста в базе данных.
    """
    cursor.execute('''
    SELECT * FROM posts WHERE post_text = ?
    ''', (post_text,))
    return cursor.fetchone() is not None


async def check_and_post_new_posts():
    """
    Задача для проверки новых постов и их публикации в телеграм-канале.
    """
    while True:
        new_posts = get_new_posts()
        for post in new_posts:
            if not post_exists(post['text']):
                published = await post_to_telegram(post)
                save_post_to_db(post['text'], published)
                await asyncio.sleep(5)
        await asyncio.sleep(300)  # Проверка каждые 300 секунд(5 минут)


async def main():
    async with bot:
        loop = asyncio.get_running_loop()
        loop.create_task(check_and_post_new_posts())
        await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
