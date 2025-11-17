#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LEAVE GROUPS - Выход из всех групп (Telethon версия)
Переписано с Pyrogram на Telethon для унификации проекта
"""

import asyncio
import sys
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon.errors import FloodWaitError
import time

# ВАЖНО: вставь свои значения здесь
API_ID = 0  # YOUR_API_ID    # <-- замени на свой api_id
API_HASH = 'YOUR_API_HASH_HERE'  # <-- замени на свой api_hash
SESSION_NAME = 'session_name'  # файл сессии будет создан/использован

# Параметры поведения
SLEEP_BETWEEN = 1.0   # секунда паузы между выходами
MAX_ERRORS_SKIP = 5   # после такого количества ошибок скрипт завершится

async def main():
    error_count = 0
    left_count = 0
    skipped_count = 0

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    
    print("✅ Авторизован. Ожидаем 3 секунды для синхронизации...")
    await asyncio.sleep(3)
    print("📋 Получаем список диалогов...")
    
    dialogs = await client.get_dialogs()
    
    for dialog in dialogs:
        entity = dialog.entity
        
        # Проверяем что это Channel (супергруппа) или Chat (обычная группа)
        if isinstance(entity, Channel) and getattr(entity, 'megagroup', False):
            # Это супергруппа (публичная группа с @username)
            try:
                title = getattr(entity, 'title', 'Unknown')
                entity_id = entity.id
                print(f"🔄 Пробую выйти из: '{title}' (id={entity_id}) — SUPERGROUP")
                
                await client(LeaveChannelRequest(entity))
                
                left_count += 1
                print(f"✅ Вышел из '{title}' (id={entity_id})")
                
                # Небольшая пауза чтобы снизить риск FloodWait
                await asyncio.sleep(SLEEP_BETWEEN)
                
            except FloodWaitError as e:
                wait_seconds = e.seconds
                print(f"⏳ FloodWait: нужно подождать {wait_seconds} сек. Пауза...")
                await asyncio.sleep(wait_seconds + 1)
                # После ожидания продолжаем
                
            except Exception as e:
                error_count += 1
                skipped_count += 1
                print(f"❌ Ошибка при выходе из '{title}': {type(e).__name__}: {e}")
                if error_count >= MAX_ERRORS_SKIP:
                    print("🛑 Слишком много ошибок — прекращаю работу.")
                    await client.disconnect()
                    return
                    
        elif isinstance(entity, Chat):
            # Это обычная группа (приватная, без @username)
            try:
                title = getattr(entity, 'title', 'Unknown')
                entity_id = entity.id
                print(f"🔄 Пробую выйти из: '{title}' (id={entity_id}) — GROUP")
                
                # Для обычных групп тоже используем LeaveChannelRequest
                await client(LeaveChannelRequest(entity))
                
                left_count += 1
                print(f"✅ Вышел из '{title}' (id={entity_id})")
                
                await asyncio.sleep(SLEEP_BETWEEN)
                
            except FloodWaitError as e:
                wait_seconds = e.seconds
                print(f"⏳ FloodWait: нужно подождать {wait_seconds} сек. Пауза...")
                await asyncio.sleep(wait_seconds + 1)
                
            except Exception as e:
                error_count += 1
                skipped_count += 1
                print(f"❌ Ошибка при выходе из '{title}': {type(e).__name__}: {e}")
                if error_count >= MAX_ERRORS_SKIP:
                    print("🛑 Слишком много ошибок — прекращаю работу.")
                    await client.disconnect()
                    return
        else:
            # Пропускаем каналы (broadcast) и личные чаты
            title = getattr(entity, 'title', getattr(entity, 'username', 'Private'))
            entity_type = type(entity).__name__
            print(f"⏭️ Пропускаю: '{title}' — type={entity_type}")
            skipped_count += 1

    await client.disconnect()
    
    print("\n" + "="*50)
    print("✅ ГОТОВО!")
    print(f"📊 Статистика:")
    print(f"   ├─ Вышел из групп: {left_count}")
    print(f"   ├─ Пропущено: {skipped_count}")
    print(f"   └─ Ошибок: {error_count}")
    print("="*50)

if __name__ == "__main__":
    try:
        # Игнорируем предупреждения
        import logging
        logging.getLogger("telethon").setLevel(logging.ERROR)
        
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем.")
        sys.exit(0)
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        sys.exit(1)
