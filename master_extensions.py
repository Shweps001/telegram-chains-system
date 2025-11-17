#!/usr/bin/env python3
"""
РАСШИРЕНИЯ ДЛЯ MASTER BOT
Дополнительный функционал: очистка аккаунтов, парсер, редактирование ключевых слов
"""

import asyncio
from pathlib import Path
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat
from telethon.tl.functions.channels import LeaveChannelRequest, GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import re
import os

# ===================================
# ДИНАМИЧЕСКОЕ ОПРЕДЕЛЕНИЕ ЦЕПОЧЕК
# ===================================

def get_all_chains(only_unified=False):
    """
    Автоматически находит все .session файлы и возвращает список цепочек
    
    Args:
        only_unified (bool): Если True, возвращает только unified аккаунты (chain1/2/3)
                            Если False, возвращает все найденные сессии
    
    Returns:
        list: [
            {'num': 1, 'name': 'Chain 1', 'session': 'chain1_session.session', 'type': 'unified'},
            {'num': 2, 'name': 'Probiv 1', 'session': 'probiv1_session.session', 'type': 'probiv'},
            ...
        ]
    """
    sessions_path = Path(__file__).parent / 'sessions'
    chains = []
    
    # Ищем все .session файлы
    if sessions_path.exists():
        session_files = sorted(sessions_path.glob('*.session'))
        
        for session_file in session_files:
            # Пропускаем служебные сессии
            if 'master_bot' in session_file.name or 'anon' in session_file.name:
                continue
            
            # Определяем тип аккаунта
            account_type = 'unknown'
            display_name = session_file.stem.replace('_session', '')
            
            if 'unified' in session_file.name or ('chain' in session_file.name and not 'probiv' in session_file.name):
                # Unified аккаунты (unified_chain1, unified_chain2, unified_chain3)
                account_type = 'unified'
                match = re.search(r'chain(\d+)', session_file.name)
            elif 'probiv' in session_file.name:
                # Парсер аккаунты
                account_type = 'probiv'
                match = re.search(r'probiv(\d+)', session_file.name)
            elif 'filtr' in session_file.name:
                # Пробив аккаунты
                account_type = 'filtr'
                match = re.search(r'filtr(\d+)', session_file.name)
            elif 'invocer' in session_file.name:
                # Invocer аккаунты
                account_type = 'invocer'
                match = re.search(r'invocer(\d+)', session_file.name)
            elif 'deleter' in session_file.name:
                # Deleter аккаунты
                account_type = 'deleter'
                match = re.search(r'deleter(\d+)', session_file.name)
            else:
                # Неизвестный тип - пытаемся найти номер
                match = re.search(r'(\d+)', session_file.name)
            
            # Фильтрация: если нужны только unified, пропускаем остальные
            if only_unified and account_type != 'unified':
                continue
            
            chain_num = int(match.group(1)) if match else 0
            
            chains.append({
                'num': chain_num,
                'name': f'{account_type.capitalize()} {chain_num}',
                'session': session_file.name,
                'display_name': display_name,
                'type': account_type
            })
    
    return sorted(chains, key=lambda x: (x['type'] != 'unified', x['num']))


def get_keyword_templates():
    """
    Получает список доступных шаблонов ключевых слов
    
    Returns:
        dict: {
            'base_keywords': [{'file': 'forex.txt', 'name': 'Forex', 'count': 30}, ...],
            'history_keywords': [...],
            'stop_words': [...]
        }
    """
    templates_path = Path(__file__).parent / 'keyword_templates'
    
    result = {
        'base_keywords': [],
        'history_keywords': [],
        'stop_words': []
    }
    
    if not templates_path.exists():
        templates_path.mkdir(exist_ok=True)
        return result
    
    # Сканируем файлы шаблонов
    for file in sorted(templates_path.glob('*.txt')):
        # Читаем количество слов
        try:
            with open(file, 'r', encoding='utf-8') as f:
                words = [line.strip() for line in f if line.strip()]
                count = len(words)
        except:
            count = 0
        
        # Определяем тип по префиксу имени
        name = file.stem
        
        if name.startswith('base_keywords_'):
            display_name = name.replace('base_keywords_', '').replace('_', ' ').title()
            result['base_keywords'].append({
                'file': file.name,
                'name': display_name,
                'count': count,
                'path': str(file)
            })
        elif name.startswith('history_keywords_'):
            display_name = name.replace('history_keywords_', '').replace('_', ' ').title()
            result['history_keywords'].append({
                'file': file.name,
                'name': display_name,
                'count': count,
                'path': str(file)
            })
        elif name.startswith('stop_words_'):
            display_name = name.replace('stop_words_', '').replace('_', ' ').title()
            result['stop_words'].append({
                'file': file.name,
                'name': display_name,
                'count': count,
                'path': str(file)
            })
    
    return result


def load_template_words(template_path):
    """
    Загружает слова из файла шаблона
    
    Args:
        template_path: путь к файлу .txt
    
    Returns:
        list: список слов
    """
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            words = [line.strip().lower() for line in f if line.strip()]
        return words
    except Exception as e:
        print(f"Ошибка загрузки шаблона: {e}")
        return []


def apply_template_to_chain(chain_num, keyword_type, template_path):
    """
    Применяет шаблон ключевых слов к unified скрипту цепочки
    
    Args:
        chain_num: номер цепочки
        keyword_type: 'base_title_keywords', 'history_keywords', 'stop_words_in_title'
        template_path: путь к файлу шаблона
    
    Returns:
        bool: успешно или нет
    """
    try:
        # Загружаем слова из шаблона
        words = load_template_words(template_path)
        if not words:
            return False
        
        # Путь к unified скрипту
        unified_path = Path(__file__).parent / 'unified' / f'unified_chain{chain_num}.py'
        
        if not unified_path.exists():
            print(f"Файл {unified_path} не найден")
            return False
        
        # Применяем слова
        return update_keywords_in_file(str(unified_path), keyword_type, words)
        
    except Exception as e:
        print(f"Ошибка применения шаблона: {e}")
        return False

# ===================================
# ОЧИСТКА АККАУНТОВ ОТ ВСЕХ ГРУПП
# ===================================

async def clear_all_groups(client, chain_num, progress_callback=None):
    """
    Удаляет ВСЕ группы и супергруппы из аккаунта
    
    Args:
        client: TelegramClient
        chain_num: номер chain (1/2/3)
        progress_callback: функция для отправки прогресса (async)
    
    Returns:
        dict с статистикой
    """
    stats = {
        'left': 0,
        'skipped': 0,
        'errors': 0,
        'total_checked': 0
    }
    
    try:
        if progress_callback:
            await progress_callback("🔍 Получаю список диалогов...")
        
        dialogs = await client.get_dialogs()
        
        # Фильтруем только группы и супергруппы
        groups = []
        for d in dialogs:
            entity = d.entity
            # Проверяем что это группа или супергруппа
            if isinstance(entity, Chat):
                groups.append(entity)
            elif isinstance(entity, Channel) and getattr(entity, 'megagroup', False):
                groups.append(entity)
        
        if not groups:
            if progress_callback:
                await progress_callback("ℹ️ Аккаунт не состоит в группах")
            return stats
        
        if progress_callback:
            await progress_callback(f"📊 Найдено {len(groups)} групп. Начинаю выход...")
        
        for i, entity in enumerate(groups, 1):
            stats['total_checked'] += 1
            title = getattr(entity, 'title', 'Без названия')
            
            try:
                await client(LeaveChannelRequest(entity))
                stats['left'] += 1
                
                if progress_callback and i % 5 == 0:
                    await progress_callback(
                        f"⏳ Обработано: {i}/{len(groups)}\n"
                        f"✅ Вышел: {stats['left']}\n"
                        f"❌ Ошибок: {stats['errors']}"
                    )
                
                # Пауза между выходами для защиты от FloodWait
                await asyncio.sleep(1.5)
                
            except Exception as e:
                stats['errors'] += 1
                error_msg = str(e)
                
                # Если FloodWait - ждём больше
                if 'flood' in error_msg.lower():
                    # Извлекаем время ожидания
                    match = re.search(r'(\d+)', error_msg)
                    if match:
                        wait_time = int(match.group(1))
                        if progress_callback:
                            await progress_callback(
                                f"⏸️ FloodWait: жду {wait_time} сек...\n"
                                f"Прогресс: {i}/{len(groups)}"
                            )
                        await asyncio.sleep(wait_time + 5)
        
        if progress_callback:
            await progress_callback(
                f"✅ **ОЧИСТКА ЗАВЕРШЕНА**\n\n"
                f"Всего обработано: {stats['total_checked']}\n"
                f"✅ Вышел из групп: {stats['left']}\n"
                f"❌ Ошибок: {stats['errors']}"
            )
    
    except Exception as e:
        stats['errors'] += 1
        if progress_callback:
            await progress_callback(f"❌ Критическая ошибка: {str(e)}")
    
    return stats


# ===================================
# ПАРСЕР УЧАСТНИКОВ ГРУППЫ
# ===================================

async def parse_group_members(client, group_link, progress_callback=None):
    """
    Парсит участников группы и возвращает их username
    
    Args:
        client: TelegramClient
        group_link: ссылка на группу (https://t.me/... или @username)
        progress_callback: функция для отправки прогресса (async)
    
    Returns:
        list of usernames
    """
    usernames = []
    
    try:
        # Нормализуем ссылку
        if group_link.startswith('https://t.me/'):
            group_link = group_link.replace('https://t.me/', '')
        if group_link.startswith('@'):
            group_link = group_link[1:]
        
        if progress_callback:
            await progress_callback(f"🔍 Получаю информацию о группе...")
        
        # Получаем entity группы
        entity = await client.get_entity(group_link)
        
        if progress_callback:
            await progress_callback(f"📥 Парсинг участников: {getattr(entity, 'title', group_link)}...")
        
        # Парсим участников
        offset = 0
        limit = 200
        iteration = 0
        
        while True:
            iteration += 1
            
            try:
                participants = await client(GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsSearch(''),
                    offset=offset,
                    limit=limit,
                    hash=0
                ))
                
                if not participants.users:
                    break
                
                # Собираем username
                for user in participants.users:
                    if user.username:
                        usernames.append(f"@{user.username}")
                
                offset += len(participants.users)
                
                if progress_callback and iteration % 3 == 0:
                    await progress_callback(
                        f"⏳ Собрано username: {len(usernames)}\n"
                        f"Обработано пользователей: {offset}"
                    )
                
                # Если получили меньше чем limit - это конец
                if len(participants.users) < limit:
                    break
                
                # Пауза между запросами
                await asyncio.sleep(1)
                
            except Exception as e:
                error_msg = str(e)
                if 'flood' in error_msg.lower():
                    match = re.search(r'(\d+)', error_msg)
                    if match:
                        wait_time = int(match.group(1))
                        if progress_callback:
                            await progress_callback(f"⏸️ FloodWait: жду {wait_time} сек...")
                        await asyncio.sleep(wait_time + 5)
                        continue
                else:
                    raise
        
        if progress_callback:
            await progress_callback(
                f"✅ **ПАРСИНГ ЗАВЕРШЕН**\n\n"
                f"Всего username: {len(usernames)}"
            )
    
    except Exception as e:
        if progress_callback:
            await progress_callback(f"❌ Ошибка парсинга: {str(e)}")
        return []
    
    return usernames


# ===================================
# РЕДАКТИРОВАНИЕ КЛЮЧЕВЫХ/СТОП-СЛОВ
# ===================================

def get_keywords_from_file(filepath, keyword_type='base_title_keywords'):
    """
    Извлекает ключевые слова из unified скрипта
    
    Args:
        filepath: путь к unified_chain*.py
        keyword_type: тип слов (base_title_keywords, history_keywords, stop_words_in_title)
    
    Returns:
        list of keywords
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Ищем нужный список
        pattern = f"{keyword_type}\\s*=\\s*\\[(.*?)\\]"
        match = re.search(pattern, content, re.DOTALL)
        
        if match:
            list_content = match.group(1)
            # Извлекаем все строки в кавычках
            keywords = re.findall(r'["\']([^"\']+)["\']', list_content)
            return keywords
        
        return []
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")
        return []


def update_keywords_in_file(filepath, keyword_type, new_keywords):
    """
    Обновляет ключевые слова в unified скрипте
    
    Args:
        filepath: путь к unified_chain*.py
        keyword_type: тип слов
        new_keywords: новый список слов
    
    Returns:
        bool: успех операции
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Формируем новый список
        formatted_keywords = ',\n    '.join([f'"{kw}"' for kw in new_keywords])
        new_list = f"{keyword_type} = [\n    {formatted_keywords}\n]"
        
        # Заменяем старый список
        pattern = f"{keyword_type}\\s*=\\s*\\[.*?\\]"
        content = re.sub(pattern, new_list, content, flags=re.DOTALL)
        
        # Сохраняем
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True
    except Exception as e:
        print(f"Ошибка записи в файл: {e}")
        return False


def add_keyword(filepath, keyword_type, new_word):
    """Добавляет ключевое слово"""
    keywords = get_keywords_from_file(filepath, keyword_type)
    if new_word.lower() not in [k.lower() for k in keywords]:
        keywords.append(new_word.lower())
        return update_keywords_in_file(filepath, keyword_type, keywords)
    return False  # Уже есть


def remove_keyword(filepath, keyword_type, word_to_remove):
    """Удаляет ключевое слово"""
    keywords = get_keywords_from_file(filepath, keyword_type)
    keywords = [k for k in keywords if k.lower() != word_to_remove.lower()]
    return update_keywords_in_file(filepath, keyword_type, keywords)


# ===================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===================================

def get_chain_session_info(chain_num):
    """
    Возвращает информацию о сессии для unified chain аккаунта
    
    Args:
        chain_num (int): Номер цепочки (1, 2, 3)
    
    Returns:
        dict: {api_id, api_hash, session, phone} или None если не найдено
    """
    # Конфигурация UNIFIED аккаунтов (те, что вступают и фильтруют)
    # Данные из unified_chain1.py, unified_chain2.py, unified_chain3.py
    configs = {
        1: {
            'api_id': 0  # YOUR_API_ID,
            'api_hash': 'YOUR_API_HASH_HERE',
            'session': 'unified_chain1_session.session',  # Unified Chain 1
            'phone': '+1234567890'
        },
        2: {
            'api_id': 0  # YOUR_API_ID,
            'api_hash': 'YOUR_API_HASH_HERE',
            'session': 'unified_chain2_session.session',  # Unified Chain 2
            'phone': '+1234567890'
        },
        3: {
            'api_id': 0  # YOUR_API_ID,
            'api_hash': 'YOUR_API_HASH_HERE',
            'session': 'unified_chain3_session.session',  # Unified Chain 3
            'phone': '+1234567890'
        }
    }
    
    return configs.get(chain_num)


# ===================================
# ПРЕСЕТЫ - ГОТОВЫЕ КОМПЛЕКТЫ СЛОВ
# ===================================

def get_all_presets():
    """
    Получает список всех доступных пресетов (готовых комплектов)
    
    Returns:
        list: [
            {'file': 'preset_1_forex.txt', 'name': 'Forex & Trading', 'icon': '💹'},
            {'file': 'preset_2_crypto.txt', 'name': 'Crypto & Blockchain', 'icon': '₿'},
            ...
        ]
    """
    presets_path = Path(__file__).parent / 'keyword_templates'
    presets = []
    
    # Иконки для пресетов
    preset_icons = {
        'forex': '💹',
        'crypto': '₿',
        'hr': '👔',
        'jobs': '👔',
        'fintech': '💳',
        'affiliate': '🤝'
    }
    
    if not presets_path.exists():
        return presets
    
    # Ищем файлы пресетов
    for file in sorted(presets_path.glob('preset_*.txt')):
        name = file.stem.replace('preset_', '').replace('_', ' ').title()
        
        # Определяем иконку
        icon = '📦'
        for key, emoji in preset_icons.items():
            if key in file.stem.lower():
                icon = emoji
                break
        
        # Читаем метаданные из файла
        try:
            with open(file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                # Первая строка может содержать название: # 📦 КОМПЛЕКТ 1: FOREX & TRADING
                if first_line.startswith('#'):
                    name = first_line.split(':', 1)[-1].strip()
        except:
            pass
        
        presets.append({
            'file': file.name,
            'name': name,
            'icon': icon,
            'path': str(file)
        })
    
    return presets


def load_preset(preset_path):
    """
    Загружает пресет - все 3 группы слов (base, history, stop)
    
    Args:
        preset_path: путь к файлу пресета
    
    Returns:
        dict: {
            'base_title_keywords': ['word1', 'word2', ...],
            'history_keywords': ['word1', 'word2', ...],
            'stop_words_in_title': ['word1', 'word2', ...]
        }
    """
    result = {
        'base_title_keywords': [],
        'history_keywords': [],
        'stop_words_in_title': []
    }
    
    try:
        with open(preset_path, 'r', encoding='utf-8') as f:
            current_section = None
            
            for line in f:
                line = line.strip()
                
                # Пропускаем комментарии и пустые строки
                if not line or line.startswith('#'):
                    # Определяем секцию по заголовку
                    if 'Base Title Keywords' in line:
                        current_section = 'base_title_keywords'
                    elif 'History Keywords' in line:
                        current_section = 'history_keywords'
                    elif 'Stop Words' in line:
                        current_section = 'stop_words_in_title'
                    continue
                
                # Добавляем слово в текущую секцию
                if current_section:
                    result[current_section].append(line.lower())
        
        return result
        
    except Exception as e:
        print(f"Ошибка загрузки пресета: {e}")
        return result


def apply_preset_to_chain(chain_num, preset_path):
    """
    Применяет пресет ко всем 3 типам слов в unified скрипте цепочки
    
    Args:
        chain_num: номер цепочки (или 'all')
        preset_path: путь к файлу пресета
    
    Returns:
        dict: {
            'success': True/False,
            'chains_updated': [1, 2, 3],
            'errors': []
        }
    """
    result = {
        'success': True,
        'chains_updated': [],
        'errors': []
    }
    
    try:
        # Загружаем пресет
        preset_data = load_preset(preset_path)
        
        if not any(preset_data.values()):
            result['success'] = False
            result['errors'].append("Пресет пустой или не удалось загрузить")
            return result
        
        # Определяем цепочки для обновления
        if chain_num == 'all':
            chains = [c['num'] for c in get_all_chains()]
        else:
            chains = [chain_num]
        
        # Применяем к каждой цепочке
        for num in chains:
            unified_path = Path(__file__).parent / 'unified' / f'unified_chain{num}.py'
            
            if not unified_path.exists():
                result['errors'].append(f"Файл unified_chain{num}.py не найден")
                continue
            
            # Обновляем все 3 типа слов
            success_count = 0
            for keyword_type, words in preset_data.items():
                if words and update_keywords_in_file(str(unified_path), keyword_type, words):
                    success_count += 1
            
            if success_count == 3:
                result['chains_updated'].append(num)
            else:
                result['errors'].append(f"Chain {num}: обновлено только {success_count}/3")
        
        if not result['chains_updated']:
            result['success'] = False
        
        return result
        
    except Exception as e:
        result['success'] = False
        result['errors'].append(f"Ошибка применения пресета: {e}")
        return result


def merge_presets(preset_paths):
    """
    Объединяет несколько пресетов в один
    
    Args:
        preset_paths: список путей к файлам пресетов ['path1.txt', 'path2.txt', ...]
    
    Returns:
        dict: {
            'base_title_keywords': ['word1', 'word2', ...],  # Уникальные слова из всех пресетов
            'history_keywords': ['word1', 'word2', ...],
            'stop_words_in_title': ['word1', 'word2', ...]
        }
    """
    merged = {
        'base_title_keywords': set(),
        'history_keywords': set(),
        'stop_words_in_title': set()
    }
    
    # Загружаем каждый пресет и объединяем слова
    for preset_path in preset_paths:
        preset_data = load_preset(preset_path)
        
        for key in merged.keys():
            merged[key].update(preset_data.get(key, []))
    
    # Конвертируем set обратно в list и сортируем
    result = {
        key: sorted(list(words))
        for key, words in merged.items()
    }
    
    return result


def apply_multiple_presets_to_chain(chain_num, preset_paths):
    """
    Применяет несколько пресетов (объединяет их) к unified скрипту цепочки
    
    Args:
        chain_num: номер цепочки (или 'all')
        preset_paths: список путей к файлам пресетов
    
    Returns:
        dict: {
            'success': True/False,
            'chains_updated': [1, 2, 3],
            'merged_counts': {'base': 150, 'history': 120, 'stop': 500},
            'errors': []
        }
    """
    result = {
        'success': True,
        'chains_updated': [],
        'merged_counts': {},
        'errors': []
    }
    
    try:
        # Объединяем пресеты
        merged_data = merge_presets(preset_paths)
        
        if not any(merged_data.values()):
            result['success'] = False
            result['errors'].append("Не удалось загрузить пресеты")
            return result
        
        # Считаем количество слов
        result['merged_counts'] = {
            'base': len(merged_data['base_title_keywords']),
            'history': len(merged_data['history_keywords']),
            'stop': len(merged_data['stop_words_in_title'])
        }
        
        # Определяем цепочки для обновления
        if chain_num == 'all':
            chains = [c['num'] for c in get_all_chains()]
        else:
            chains = [chain_num]
        
        # Применяем к каждой цепочке
        for num in chains:
            unified_path = Path(__file__).parent / 'unified' / f'unified_chain{num}.py'
            
            if not unified_path.exists():
                result['errors'].append(f"Файл unified_chain{num}.py не найден")
                continue
            
            # Обновляем все 3 типа слов
            success_count = 0
            for keyword_type, words in merged_data.items():
                if words and update_keywords_in_file(str(unified_path), keyword_type, words):
                    success_count += 1
            
            if success_count == 3:
                result['chains_updated'].append(num)
            else:
                result['errors'].append(f"Chain {num}: обновлено только {success_count}/3")
        
        if not result['chains_updated']:
            result['success'] = False
        
        return result
        
    except Exception as e:
        result['success'] = False
        result['errors'].append(f"Ошибка применения пресетов: {e}")
        return result
