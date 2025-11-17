#!/usr/bin/env python3
"""
TELEGRAM БОТ МАСТЕР - УПРАВЛЕНИЕ ЦЕПОЧКАМИ
Полное управление всеми скриптами через Telegram бота
"""

import subprocess
import os
import sys
import time
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import json
import sqlite3
from collections import defaultdict, Counter
from telethon import TelegramClient, events, Button
import master_extensions as ext  # Модуль дополнительных утилит

# Добавляем путь к общей папке фильтров для доступа к базе данных
FILTRS_SHARED_PATH = Path(__file__).parent / 'filtrs' / '_shared'
sys.path.insert(0, str(FILTRS_SHARED_PATH))

# Telegram Bot
BOT_TOKEN = 'YOUR_BOT_TOKEN_HERE'
API_ID = 0  # YOUR_API_ID
API_HASH = 'YOUR_API_HASH_HERE'

# Канал для отправки username (для пробива)
PROBIV_CHANNEL_ID = -1001234567890  # Канал куда парсер отправляет username для пробива

# Создаем клиент бота
bot = TelegramClient('master_bot', API_ID, API_HASH)

# Файл для хранения активных пресетов
ACTIVE_PRESETS_FILE = 'active_presets.json'

# Глобальное хранилище активных пресетов
active_presets = {
    'chain1': None,
    'chain2': None,
    'chain3': None
}

def load_active_presets():
    """Загружает информацию об активных пресетах из файла"""
    global active_presets
    try:
        if os.path.exists(ACTIVE_PRESETS_FILE):
            with open(ACTIVE_PRESETS_FILE, 'r', encoding='utf-8') as f:
                active_presets = json.load(f)
    except Exception as e:
        print(f"⚠️ Ошибка загрузки активных пресетов: {e}")
        active_presets = {'chain1': None, 'chain2': None, 'chain3': None}

def save_active_presets():
    """Сохраняет информацию об активных пресетах в файл"""
    try:
        with open(ACTIVE_PRESETS_FILE, 'w', encoding='utf-8') as f:
            json.dump(active_presets, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ Ошибка сохранения активных пресетов: {e}")

# Пути к скриптам
CHAINS = {
    1: {
        'probiv': r'd:\ss\cycle\chain\probivs\probiv1\probiv1.py',
        'filtr': r'd:\ss\cycle\chain\filtrs\filtr1\filtr1.py',
        'unified': r'd:\ss\cycle\chain\unified\unified_chain1.py',  # НОВЫЙ объединенный скрипт
        'invocer': r'd:\ss\cycle\chain\invocer\invocer1\invocer1.py',  # Старый отдельный
        'deleter': r'd:\ss\cycle\chain\deleter\deleter1\deleter1.py'   # Старый отдельный
    },
    2: {
        'probiv': r'd:\ss\cycle\chain\probivs\probiv2\probiv2.py',
        'filtr': r'd:\ss\cycle\chain\filtrs\filtr2\filtr2.py',
        'unified': r'd:\ss\cycle\chain\unified\unified_chain2.py',  # НОВЫЙ объединенный скрипт
        'invocer': r'd:\ss\cycle\chain\invocer\invocer2\invocer2.py',  # Старый отдельный
        'deleter': r'd:\ss\cycle\chain\deleter\deleter2\deleter2.py'   # Старый отдельный
    },
    3: {
        'probiv': r'd:\ss\cycle\chain\probivs\probiv3\probiv3.py',
        'filtr': r'd:\ss\cycle\chain\filtrs\filtr3\filtr3.py',
        'unified': r'd:\ss\cycle\chain\unified\unified_chain3.py',  # НОВЫЙ объединенный скрипт
        'invocer': r'd:\ss\cycle\chain\invocer\invocer3\invocer3.py',  # Старый отдельный
        'deleter': r'd:\ss\cycle\chain\deleter\deleter3\deleter3.py'   # Старый отдельный
    },
    4: {
        'invocer': r'd:\ss\cycle\chain\invocer\invocer4\invocer4.py',
        'deleter': r'd:\ss\cycle\chain\deleter\deleter4\deleter4.py'
    }
}

# Глобальный словарь для хранения запущенных процессов
running_processes = {}

# Авторизованные пользователи (добавьте свой ID)
AUTHORIZED_USERS = [123456789]  # YOUR_TELEGRAM_ID  # Замените на ваш Telegram ID

# === ПЛАНИРОВЩИК ЗАДАЧ ===
task_scheduler = None  # Инициализируется в main()

# === ФЛУДВЕЙТ ЗАЩИТА ===
floodwait_stats = {
    1: {'count': 0, 'total_wait': 0, 'last_wait': None, 'level': 0},
    2: {'count': 0, 'total_wait': 0, 'last_wait': None, 'level': 0},
    3: {'count': 0, 'total_wait': 0, 'last_wait': None, 'level': 0}
}

# Функции для работы с процессами
def start_script(script_path, script_name):
    """Запуск скрипта"""
    try:
        if script_name in running_processes:
            return f"❌ {script_name} уже запущен"
        
        # Создаем новое окно терминала для скрипта
        cmd = f'wt new-tab --title "{script_name}" cmd /k "cd /d "{os.path.dirname(script_path)}" && python "{os.path.basename(script_path)}""'
        
        process = subprocess.Popen(cmd, shell=True)
        running_processes[script_name] = {
            'process': process,
            'start_time': datetime.now(),
            'path': script_path
        }
        
        return f"✅ {script_name} запущен"
    except Exception as e:
        return f"❌ Ошибка запуска {script_name}: {str(e)}"

def stop_script(script_name):
    """Остановка скрипта"""
    try:
        if script_name not in running_processes:
            return f"❌ {script_name} не запущен"
        
        process_info = running_processes[script_name]
        process_info['process'].terminate()
        del running_processes[script_name]
        
        return f"🛑 {script_name} остановлен"
    except Exception as e:
        return f"❌ Ошибка остановки {script_name}: {str(e)}"

def get_status():
    """Получение статуса всех процессов"""
    if not running_processes:
        return "🔴 Нет запущенных скриптов"
    
    status = "📊 **СТАТУС СКРИПТОВ:**\n\n"
    for name, info in running_processes.items():
        runtime = datetime.now() - info['start_time']
        hours, remainder = divmod(int(runtime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        status += f"🟢 **{name}** - работает {hours:02d}:{minutes:02d}:{seconds:02d}\n"
    
    return status

def start_chain(chain_num, start_from='probiv'):
    """Запуск цепочки с указанного звена"""
    if chain_num not in CHAINS:
        return f"❌ Цепочка {chain_num} не существует"
    
    chain = CHAINS[chain_num]
    script_order = ['probiv', 'filtr', 'invocer', 'deleter']
    
    # Найдем индекс стартового звена
    try:
        start_index = script_order.index(start_from)
    except ValueError:
        return f"❌ Неизвестное звено: {start_from}"
    
    results = []
    for script_type in script_order[start_index:]:
        if script_type in chain:
            script_name = f"{script_type}{chain_num}"
            script_path = chain[script_type]
            result = start_script(script_path, script_name)
            results.append(result)
        else:
            results.append(f"⚠️ {script_type}{chain_num} не найден в цепочке")
    
    return "\n".join(results)

def stop_chain(chain_num):
    """Остановка всей цепочки"""
    if chain_num not in CHAINS:
        return f"❌ Цепочка {chain_num} не существует"
    
    results = []
    script_order = ['probiv', 'filtr', 'invocer', 'deleter']
    
    for script_type in script_order:
        script_name = f"{script_type}{chain_num}"
        if script_name in running_processes:
            result = stop_script(script_name)
            results.append(result)
    
    if not results:
        return f"ℹ️ Цепочка {chain_num} не была запущена"
    
    return "\n".join(results)

# === РАСШИРЕННОЕ УПРАВЛЕНИЕ ЛОГАМИ ===

def get_log_paths():
    """Получить все пути к логам"""
    return {
        'master': r'd:\ss\cycle\chain\master.log',
        'probiv1': r'd:\ss\cycle\chain\probivs\probiv1\logs\probiv1.log',
        'probiv2': r'd:\ss\cycle\chain\probivs\probiv2\logs\probiv2.log',
        'probiv3': r'd:\ss\cycle\chain\probivs\probiv3\logs\probiv3.log',
        'filtr1': r'd:\ss\cycle\chain\filtrs\logs\filtr1.log',
        'filtr2': r'd:\ss\cycle\chain\filtrs\logs\filtr2.log',
        'filtr3': r'd:\ss\cycle\chain\filtrs\logs\filtr3.log',
        'unified1': r'd:\ss\cycle\chain\unified\logs\unified_chain1.log',
        'unified2': r'd:\ss\cycle\chain\unified\logs\unified_chain2.log',
        'unified3': r'd:\ss\cycle\chain\unified\logs\unified_chain3.log',
        'invocer1': r'd:\ss\cycle\chain\invocer\invocer1\logs\invocer1.log',
        'invocer2': r'd:\ss\cycle\chain\invocer\invocer2\logs\invocer2.log',
        'invocer3': r'd:\ss\cycle\chain\invocer\invocer3\logs\invocer3.log',
    }

def read_log_file(log_path, lines=20, search_term=None):
    """
    Чтение лог-файла
    
    Args:
        log_path: путь к файлу
        lines: количество последних строк (по умолчанию 20)
        search_term: поиск по ключевому слову
    
    Returns:
        list: список строк лога
    """
    try:
        if not os.path.exists(log_path):
            return [f"❌ Файл не найден: {log_path}"]
        
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            all_lines = f.readlines()
        
        if not all_lines:
            return ["📋 Лог пуст"]
        
        # Если есть поиск
        if search_term:
            filtered = [line for line in all_lines if search_term.lower() in line.lower()]
            if not filtered:
                return [f"🔍 Ничего не найдено по запросу: {search_term}"]
            return filtered[-lines:] if len(filtered) > lines else filtered
        
        # Последние N строк
        return all_lines[-lines:] if len(all_lines) > lines else all_lines
        
    except Exception as e:
        return [f"❌ Ошибка чтения: {str(e)}"]

def analyze_log_patterns(log_path, patterns):
    """
    Анализ лога на наличие паттернов (ошибки, предупреждения и т.д.)
    
    Args:
        log_path: путь к файлу
        patterns: словарь {название: regex_паттерн}
    
    Returns:
        dict: количество вхождений для каждого паттерна
    """
    try:
        if not os.path.exists(log_path):
            return {}
        
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        results = {}
        for name, pattern in patterns.items():
            matches = re.findall(pattern, content, re.IGNORECASE)
            results[name] = len(matches)
        
        return results
        
    except Exception as e:
        return {'error': str(e)}

# === ПЛАНИРОВЩИК ЗАДАЧ ===

class TaskScheduler:
    """Планировщик задач для автозапуска по расписанию"""
    
    def __init__(self):
        self.tasks = []
        self.running = False
        self.thread = None
    
    def add_task(self, name, action, schedule_time, chain_num=None, script_type=None, repeat=False):
        """
        Добавить задачу в планировщик
        
        Args:
            name: название задачи
            action: тип действия ('start_chain', 'stop_chain', 'start_script', 'stop_script')
            schedule_time: время выполнения (datetime или строка "HH:MM")
            chain_num: номер цепочки (1-3)
            script_type: тип скрипта ('probiv', 'filtr', 'unified' и т.д.)
            repeat: повторять ежедневно
        """
        task = {
            'id': len(self.tasks) + 1,
            'name': name,
            'action': action,
            'schedule_time': schedule_time,
            'chain_num': chain_num,
            'script_type': script_type,
            'repeat': repeat,
            'enabled': True,
            'last_run': None,
            'next_run': self._calculate_next_run(schedule_time)
        }
        self.tasks.append(task)
        return task['id']
    
    def _calculate_next_run(self, schedule_time):
        """Рассчитать следующее время выполнения"""
        if isinstance(schedule_time, datetime):
            return schedule_time
        
        # Парсим время из строки "HH:MM"
        try:
            hours, minutes = map(int, schedule_time.split(':'))
            now = datetime.now()
            next_run = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
            
            # Если время уже прошло сегодня, планируем на завтра
            if next_run < now:
                next_run += timedelta(days=1)
            
            return next_run
        except:
            return None
    
    def remove_task(self, task_id):
        """Удалить задачу"""
        self.tasks = [t for t in self.tasks if t['id'] != task_id]
    
    def get_tasks(self):
        """Получить список всех задач"""
        return self.tasks
    
    def enable_task(self, task_id):
        """Включить задачу"""
        for task in self.tasks:
            if task['id'] == task_id:
                task['enabled'] = True
                return True
        return False
    
    def disable_task(self, task_id):
        """Выключить задачу"""
        for task in self.tasks:
            if task['id'] == task_id:
                task['enabled'] = False
                return True
        return False
    
    def start(self):
        """Запустить планировщик"""
        if self.running:
            return False
        
        self.running = True
        self.thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.thread.start()
        return True
    
    def stop(self):
        """Остановить планировщик"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _scheduler_loop(self):
        """Основной цикл планировщика"""
        while self.running:
            now = datetime.now()
            
            for task in self.tasks:
                if not task['enabled']:
                    continue
                
                if task['next_run'] and now >= task['next_run']:
                    # Выполняем задачу
                    self._execute_task(task)
                    
                    # Обновляем время следующего запуска
                    if task['repeat']:
                        task['next_run'] += timedelta(days=1)
                    else:
                        task['enabled'] = False
                        task['next_run'] = None
            
            time.sleep(30)  # Проверяем каждые 30 секунд
    
    def _execute_task(self, task):
        """Выполнить задачу"""
        try:
            task['last_run'] = datetime.now()
            
            action = task['action']
            chain_num = task['chain_num']
            script_type = task['script_type']
            
            if action == 'start_chain' and chain_num and master_instance:
                master_instance.start_chain_from_stage(chain_num, 'probiv')
                send_notification_sync(f"⏰ Планировщик: запущена цепочка {chain_num}")
            
            elif action == 'stop_chain' and chain_num and master_instance:
                master_instance.stop_all_scripts()  # Можно доработать для отдельной цепочки
                send_notification_sync(f"⏰ Планировщик: остановлена цепочка {chain_num}")
            
            elif action == 'start_script' and chain_num and script_type and master_instance:
                master_instance.start_single_script(chain_num, script_type)
                send_notification_sync(f"⏰ Планировщик: запущен {script_type}{chain_num}")
            
            elif action == 'stop_script' and chain_num and script_type and master_instance:
                master_instance.stop_single_script(chain_num, script_type)
                send_notification_sync(f"⏰ Планировщик: остановлен {script_type}{chain_num}")
            
        except Exception as e:
            send_notification_sync(f"❌ Ошибка выполнения задачи: {str(e)}")

# === СТАТИСТИКА ПО БАЗЕ ДАННЫХ ===

def get_database_stats():
    """Получить расширенную статистику по базе данных"""
    try:
        from filtr_global_stats import DB_PATH
        
        if not DB_PATH.exists():
            return None
        
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        
        # Общая статистика
        cursor.execute('SELECT COUNT(*) FROM processed_chats')
        total = cursor.fetchone()[0]
        
        # По цепочкам
        cursor.execute('SELECT chain_num, COUNT(*) FROM processed_chats GROUP BY chain_num')
        by_chain = dict(cursor.fetchall())
        
        # По категориям
        cursor.execute('SELECT category, COUNT(*) FROM processed_chats GROUP BY category ORDER BY COUNT(*) DESC')
        by_category = dict(cursor.fetchall())
        
        # За последние 24 часа
        cursor.execute('''
            SELECT COUNT(*) FROM processed_chats 
            WHERE datetime(processed_at) >= datetime('now', '-1 day')
        ''')
        last_24h = cursor.fetchone()[0]
        
        # За последние 7 дней
        cursor.execute('''
            SELECT COUNT(*) FROM processed_chats 
            WHERE datetime(processed_at) >= datetime('now', '-7 days')
        ''')
        last_7d = cursor.fetchone()[0]
        
        # Топ ключевых слов
        cursor.execute('SELECT matched_keywords FROM processed_chats WHERE matched_keywords != ""')
        all_keywords = cursor.fetchall()
        keyword_counter = Counter()
        for (keywords_str,) in all_keywords:
            if keywords_str:
                for kw in keywords_str.split(','):
                    kw = kw.strip()
                    if kw:
                        keyword_counter[kw] += 1
        
        top_keywords = keyword_counter.most_common(10)
        
        # Последние добавленные чаты
        cursor.execute('''
            SELECT link, chain_num, category, processed_at 
            FROM processed_chats 
            ORDER BY processed_at DESC 
            LIMIT 5
        ''')
        recent = cursor.fetchall()
        
        conn.close()
        
        return {
            'total': total,
            'by_chain': by_chain,
            'by_category': by_category,
            'last_24h': last_24h,
            'last_7d': last_7d,
            'top_keywords': top_keywords,
            'recent': recent
        }
        
    except Exception as e:
        return {'error': str(e)}

# === ЗАЩИТА ОТ FLOODWAIT ===

def analyze_floodwait_logs():
    """Анализ логов на FloodWait ошибки"""
    results = {}
    
    for chain_num in [1, 2, 3]:
        log_path = f"d:\\ss\\cycle\\chain\\unified\\logs\\unified_chain{chain_num}.log"
        
        if not os.path.exists(log_path):
            results[chain_num] = {'count': 0, 'warnings': []}
            continue
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Ищем FloodWait упоминания
            floodwait_pattern = r'FloodWait.*?(\d+)\s*(секунд|минут|seconds|minutes)'
            matches = re.findall(floodwait_pattern, content, re.IGNORECASE)
            
            total_wait_time = 0
            warnings = []
            
            for match in matches:
                wait_time = int(match[0])
                unit = match[1].lower()
                
                if 'минут' in unit or 'minute' in unit:
                    wait_time *= 60
                
                total_wait_time += wait_time
                
                if wait_time > 300:  # Более 5 минут
                    warnings.append(f"Длительное ожидание: {wait_time}с")
            
            results[chain_num] = {
                'count': len(matches),
                'total_wait': total_wait_time,
                'warnings': warnings,
                'level': 'high' if len(matches) > 10 else 'medium' if len(matches) > 5 else 'low'
            }
            
        except Exception as e:
            results[chain_num] = {'error': str(e)}
    
    return results

def get_floodwait_recommendations(stats):
    """Получить рекомендации на основе FloodWait статистики"""
    recommendations = []
    
    for chain_num, data in stats.items():
        if isinstance(data, dict) and 'level' in data:
            level = data['level']
            count = data['count']
            
            if level == 'high':
                recommendations.append(
                    f"🚨 **Цепочка {chain_num}**: Критический уровень FloodWait ({count} раз).\n"
                    f"   • Рекомендуется пауза 2-4 часа\n"
                    f"   • Увеличьте задержки между действиями\n"
                    f"   • Используйте режим 'медленный'"
                )
            elif level == 'medium':
                recommendations.append(
                    f"⚠️ **Цепочка {chain_num}**: Повышенный FloodWait ({count} раз).\n"
                    f"   • Увеличьте задержки между батчами\n"
                    f"   • Рассмотрите паузу 30-60 минут"
                )
            elif level == 'low' and count > 0:
                recommendations.append(
                    f"✅ **Цепочка {chain_num}**: Нормальный уровень ({count} раз).\n"
                    f"   • Текущие настройки работают хорошо"
                )
    
    if not recommendations:
        recommendations.append("✅ Отличная работа! FloodWait ошибок не обнаружено.")
    
    return recommendations

# Триггерные фразы
TRIGGERS = {
    'probiv_finished': r'✅ Обработка завершена - обработано \d+ юзернеймов',
    'filtr_sent_links': r'✅ релевантные ссылки отправлены в канал|отправлено в канал invocer',
    'unified_ready': r'🚀 UNIFIED_CHAIN\d+ запущен|📦 Начало обработки батча|✅ Батч обработан',
    'invocer_deleter_ready': r'🚀 INVOCER_DELETER\d+ запущен|📦 Начало обработки батча|✅ Батч обработан',
    'invocer_waiting': r'ℹ️ нет ссылок для добавления|ожидаю новые ссылки на добавление|нет ссылок для добавления|📌 Сейчас аккаунт состоит в \d+ групповых чатах|достигнут лимит групп|превышен лимит групп|слишком много групп',
    'deleter_finished': r'=== DELETER\d+ ЗАВЕРШЕН ===|📈 Итог:|ИТОГОВАЯ СТАТИСТИКА DELETER'
}

class HybridTriggeredMaster:
    """Триггерный мастер с вкладками"""
    
    def __init__(self):
        self.active_processes = {}
        self.chain_status = {
            1: {'probiv': False, 'filtr': False, 'invocer': False, 'deleter': False, 'unified': False},
            2: {'probiv': False, 'filtr': False, 'invocer': False, 'deleter': False, 'unified': False},
            3: {'probiv': False, 'filtr': False, 'invocer': False, 'deleter': False, 'unified': False}
        }
        self.log_files = {}
        self.monitoring = True
        self.wt_available = self.check_windows_terminal()
        
        # Счетчики для probiv
        self.probiv_stats = {
            1: {'success': 0, 'fail_streak': 0, 'total': 0, 'target': 200},
            2: {'success': 0, 'fail_streak': 0, 'total': 0, 'target': 200},
            3: {'success': 0, 'fail_streak': 0, 'total': 0, 'target': 200}
        }
        
        # Автосканирование убрано - пользователь сам решает когда запускать цепочки
        if master_system_enabled:
            print("ℹ️ Мастер-система включена. Интерфейс готов к работе.")
            print("💡 Цепочки НЕ запускаются автоматически. Используйте команды бота для управления.")
        else:
            print("ℹ️ Мастер-система выключена. Автосканирование пропущено.")
            print("💡 Включите мастер-систему через бота: /start -> кнопка управления мастером")
    
    def log_event(self, chain_num, event, details=""):
        """Логирование событий"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        message = f"[{timestamp}] Цепочка {chain_num}: {event}"
        if details:
            message += f" - {details}"
        print(message)
        
        # Отправляем важные события в Telegram
        if any(keyword in event for keyword in ['ТРИГГЕР', 'КРИТИЧНО', 'ЗАВЕРШЕН', 'ОШИБКА']):
            send_notification_sync(message)
    
    def get_filtr_stats_from_db(self):
        """Получить статистику из общей базы фильтров и UNIFIED"""
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
            from filtr_global_stats import get_total_processed, get_stats_by_chain
            
            total = get_total_processed()
            chain1 = get_stats_by_chain(1)
            chain2 = get_stats_by_chain(2)
            chain3 = get_stats_by_chain(3)
            
            stats_text = (
                f"�️ **ОБЩАЯ БАЗА ДАННЫХ ЧАТОВ**\n\n"
                f"Эта база содержит все чаты, которые обработаны системой:\n"
                f"• FILTR скрипты записывают релевантные чаты\n"
                f"• UNIFIED скрипты записывают чаты после вступления\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📊 **СТАТИСТИКА:**\n\n"
                f"✅ **Всего уникальных чатов: {total}**\n\n"
                f"**По аккаунтам:**\n"
                f"👤 Аккаунт 1 (CHAIN1): {chain1} чатов\n"
                f"👤 Аккаунт 2 (CHAIN2): {chain2} чатов\n"
                f"👤 Аккаунт 3 (CHAIN3): {chain3} чатов\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💡 **Важно:**\n"
                f"• База автоматически исключает дубликаты\n"
                f"• Каждый чат обрабатывается только один раз\n"
                f"• Общая статистика помогает избежать повторов"
            )
            return stats_text
        except Exception as e:
            return (
                f"⚠️ **Не удалось получить статистику БД**\n\n"
                f"Возможные причины:\n"
                f"• База данных еще не создана\n"
                f"• Не запущены FILTR или UNIFIED скрипты\n"
                f"• Ошибка: {e}\n\n"
                f"**Решение:**\n"
                f"Запустите FILTR или UNIFIED скрипты,\n"
                f"чтобы начать заполнение базы данных."
            )
    
    def scan_existing_logs(self):
        """Сканирование существующих логов на предмет завершенных операций"""
        print("🔍 Сканирование существующих логов...")
        
        for chain_num in [1, 2, 3]:
            for script_type in ['probiv', 'filtr', 'invocer', 'deleter']:
                log_path = f"d:\\ss\\cycle\\chain\\{script_type}s\\{script_type}{chain_num}\\logs\\{script_type}{chain_num}.log"
                
                if os.path.exists(log_path):
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                            lines = f.readlines()
                            # Проверяем последние 50 строк на предмет триггеров
                            recent_lines = lines[-50:] if len(lines) > 50 else lines
                            
                            for line in recent_lines:
                                line = line.strip()
                                if line:
                                    # Проверяем триггеры завершения
                                    for trigger_name, pattern in TRIGGERS.items():
                                        if re.search(pattern, line):
                                            print(f"📋 Найден триггер {trigger_name} в {script_type}{chain_num}: {line}")
                                            # Обрабатываем найденный триггер
                                            self.process_found_trigger(chain_num, script_type, trigger_name, line)
                                            
                    except Exception as e:
                        print(f"⚠️ Ошибка чтения {log_path}: {e}")
    
    def process_found_trigger(self, chain_num, script_type, trigger_name, log_line):
        """Обработка найденного триггера в существующих логах"""
        if trigger_name == 'probiv_finished':
            # Извлекаем количество обработанных записей
            match = re.search(r'обработано (\d+)', log_line)
            if match:
                processed_count = int(match.group(1))
                self.probiv_stats[chain_num]['success'] = processed_count
                self.probiv_stats[chain_num]['total'] = processed_count
                
                self.log_event(chain_num, f"🔍 НАЙДЕН ЗАВЕРШЕННЫЙ PROBIV: {processed_count}/{self.probiv_stats[chain_num]['target']}")
                
                # Если цепочка не запущена, запускаем следующий этап (FILTR после PROBIV)
                if script_type not in [info['type'] for info in self.active_processes.values() if info.get('chain') == chain_num]:
                    self.log_event(chain_num, f"🚀 АВТОЗАПУСК FILTR после найденного завершения PROBIV")
                    self.trigger_next_stage(chain_num, 'filtr')  # Запускаем filtr, а не probiv
        
        elif trigger_name == 'filtr_sent_links':
            # После filtr можем запустить unified script для любой цепочки
            self.log_event(chain_num, f"🔍 НАЙДЕН СИГНАЛ ОТ FILTR: отправлены ссылки для цепочки {chain_num}")
            
            # Проверяем, есть ли unified скрипт для этой цепочки
            if 'unified' in CHAINS.get(chain_num, {}):
                unified_running = any(
                    info.get('type') == 'unified' and info.get('chain') == chain_num 
                    for info in self.active_processes.values()
                )
                if not unified_running:
                    self.log_event(chain_num, f"🚀 АВТОЗАПУСК UNIFIED после сигнала от FILTR")
                    self.trigger_next_stage(chain_num, 'unified')
                else:
                    self.log_event(chain_num, f"⚠️ UNIFIED уже запущен для цепочки {chain_num}")
            else:
                # Для цепочек без unified используем старую логику invocer
                if 'invocer' not in [info['type'] for info in self.active_processes.values() if info.get('chain') == chain_num]:
                    self.log_event(chain_num, f"🚀 АВТОЗАПУСК INVOCER после сигнала от FILTR")
                    self.trigger_next_stage(chain_num, 'invocer')

    def check_windows_terminal(self):
        """Проверка Windows Terminal"""
        try:
            subprocess.run(['wt', '--version'], capture_output=True, timeout=3)
            return True
        except:
            return False
    
    def start_script_in_new_tab(self, chain_num, script_type, script_path):
        """Запуск скрипта в новой вкладке окна своей цепочки"""
        # Определяем правильное имя скрипта
        if script_type == 'unified':
            script_name = f"unified_chain{chain_num}"
        else:
            script_name = f"{script_type}{chain_num}"
        
        # Дополнительная проверка: не запущен ли уже этот процесс
        if script_name in self.active_processes:
            existing_process = self.active_processes[script_name]['process']
            if existing_process.poll() is None:  # Процесс еще живой
                self.log_event(chain_num, f"⚠️ {script_type} уже запущен (процесс активен)")
                return False
        
        try:
            self.log_event(chain_num, f"Запуск {script_type} в новой вкладке")
            
            script_dir = os.path.dirname(script_path)
            script_file = os.path.basename(script_path)
            
            if self.wt_available:
                # Используем именованное окно через -w <имя>
                # Все скрипты одной цепочки идут в окно с именем "chain1", "chain2" или "chain3"
                python_exe = "D:/ss/cycle/.venv/Scripts/python.exe"
                wt_cmd = [
                    'wt', '-w', f'chain{chain_num}',
                    'new-tab',
                    'cmd', '/k', f'title Цепь {chain_num} - {script_type.upper()}{chain_num} && cd /d "{script_dir}" && "{python_exe}" "{script_file}"'
                ]
                
                process = subprocess.Popen(wt_cmd)
                self.log_event(chain_num, f"{script_type} запущен", f"новая вкладка в окне chain{chain_num}")
            else:
                # Резервный вариант - отдельное окно
                window_title = f"{script_type.upper()}{chain_num} - Telegram"
                python_exe = "D:/ss/cycle/.venv/Scripts/python.exe"
                cmd = f'cmd /c start "{window_title}" cmd /k "cd /d \\"{script_dir}\\" && \\"{python_exe}\\" \\"{script_file}\\""'
                process = subprocess.Popen(cmd, shell=True)
                self.log_event(chain_num, f"{script_type} запущен", "отдельное окно (резерв)")
            
            self.active_processes[script_name] = {
                'process': process,
                'start_time': datetime.now(),
                'chain': chain_num,
                'type': script_type,
                'status': 'running'
            }
            
            self.chain_status[chain_num][script_type] = True
            
            return True
            
        except Exception as e:
            self.log_event(chain_num, f"ОШИБКА запуска {script_type}", str(e))
            return False
    
    def setup_log_monitoring(self, chain_num, script_type):
        """Настройка мониторинга логов"""
        log_paths = {
            'probiv': f'd:\\ss\\cycle\\chain\\probivs\\probiv{chain_num}\\logs\\probiv{chain_num}.log',
            'filtr': f'd:\\ss\\cycle\\chain\\filtrs\\logs\\filtr{chain_num}.log',
            'unified': f'd:\\ss\\cycle\\chain\\unified\\logs\\unified_chain{chain_num}.log',  # Unified логи для всех цепочек
            'invocer': f'd:\\ss\\cycle\\chain\\invocer\\invocer{chain_num}\\logs\\invocer{chain_num}.log',
            'deleter': f'd:\\ss\\cycle\\chain\\deleter\\deleter{chain_num}\\chat_logs\\deleter{chain_num}.log'
        }
        
        if script_type in log_paths:
            log_path = log_paths[script_type]
            # Определяем правильный ключ для log_files
            if script_type == 'unified':
                log_key = f"unified_chain{chain_num}"
            else:
                log_key = f"{script_type}{chain_num}"
            self.log_files[log_key] = log_path
            
            thread = threading.Thread(
                target=self.monitor_log_file,
                args=(chain_num, script_type, log_path)
            )
            thread.daemon = True
            thread.start()
    
    def monitor_log_file(self, chain_num, script_type, log_path):
        """Мониторинг лог файла"""
        self.log_event(chain_num, f"Мониторинг логов {script_type}", log_path)
        
        # Ожидаем создания файла
        while not os.path.exists(log_path) and self.monitoring:
            time.sleep(5)
        
        if not self.monitoring:
            return
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(0, 2)  # В конец файла
                
                while self.monitoring:
                    line = f.readline()
                    if line:
                        self.check_triggers(chain_num, script_type, line)
                    else:
                        time.sleep(1)
                        
        except Exception as e:
            self.log_event(chain_num, f"Ошибка мониторинга {script_type}", str(e))
    
    def check_triggers(self, chain_num, script_type, log_line):
        """Проверка триггеров"""
        global master_system_enabled
        
        # Проверяем, включена ли мастер-система
        if not master_system_enabled:
            return  # Если мастер-система выключена, игнорируем все триггеры
        
        # Триггер: probiv - подсчет результатов и проверка лимитов
        if script_type == 'probiv':
            # Проверка лимитов бота
            if re.search(r'🔍 Осталось запросов: 0', log_line):
                print(f"\n🎯 ТРИГГЕР НАЙДЕН: лимиты закончились в цепочке {chain_num}")
                self.log_event(chain_num, "🎯 ТРИГГЕР: лимиты бота закончились", "запускаю filtr")
                
                success = self.probiv_stats[chain_num]['success']
                total = self.probiv_stats[chain_num]['total']
                
                # Отправляем итог probiv в бот
                probiv_report = (
                    f"📊 **ИТОГ PROBIV{chain_num}:**\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Неудач: {total - success}\n"
                    f"📈 Всего обработано: {total}\n"
                    f"🔍 Лимиты бота закончились\n"
                    f"➡️ Запускаю filtr{chain_num}..."
                )
                send_notification_sync(probiv_report)
                
                print(f"   Вызываю trigger_next_stage({chain_num}, 'filtr')")
                self.trigger_next_stage(chain_num, 'filtr')
                return  # Выходим, чтобы не продолжать подсчет
            
            # Триггер завершения обработки пробива (все юзернеймы обработаны)
            if re.search(TRIGGERS['probiv_finished'], log_line):
                print(f"\n🎯 ТРИГГЕР НАЙДЕН: probiv{chain_num} завершил обработку")
                self.log_event(chain_num, "🎯 ТРИГГЕР: обработка завершена", "запускаю filtr")
                
                success = self.probiv_stats[chain_num]['success']
                total = self.probiv_stats[chain_num]['total']
                
                # Отправляем итог probiv в бот
                probiv_report = (
                    f"📊 **ИТОГ PROBIV{chain_num}:**\n"
                    f"✅ Успешно: {success}\n"
                    f"❌ Неудач: {total - success}\n"
                    f"📈 Всего обработано: {total}\n"
                    f"🎯 Обработка завершена полностью\n"
                    f"➡️ Запускаю filtr{chain_num}..."
                )
                send_notification_sync(probiv_report)
                
                print(f"   Вызываю trigger_next_stage({chain_num}, 'filtr')")
                self.trigger_next_stage(chain_num, 'filtr')
                return  # Выходим, чтобы не продолжать подсчет
            
            # Успешный пробив
            if re.search(r'✅ Пробит \[\d+/\d+\] - чаты получены', log_line):
                self.probiv_stats[chain_num]['success'] += 1
                self.probiv_stats[chain_num]['total'] += 1
                self.probiv_stats[chain_num]['fail_streak'] = 0  # Сброс серии неудач
                
                success = self.probiv_stats[chain_num]['success']
                self.log_event(chain_num, f"✅ Пробив успешен [{success}]")
            
            # Неудачный пробив
            elif re.search(r'❌ Пробит \[\d+/\d+\] - чаты НЕ получены', log_line):
                self.probiv_stats[chain_num]['fail_streak'] += 1
                self.probiv_stats[chain_num]['total'] += 1
                
                streak = self.probiv_stats[chain_num]['fail_streak']
                self.log_event(chain_num, f"❌ Неудачный пробив [серия: {streak}]")
                
                # 3 неудачи подряд - останавливаем ТОЛЬКО ЭТУ цепочку
                if streak >= 3:
                    total_processed = self.probiv_stats[chain_num]['total']
                    success_count = self.probiv_stats[chain_num]['success']
                    
                    # Критичное уведомление в бот
                    critical_alert = (
                        f"🚨 **КРИТИЧЕСКАЯ ОШИБКА В ЦЕПОЧКЕ {chain_num}!**\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"⛔ **3 отказа подряд в PROBIV{chain_num}**\n\n"
                        f"❌ Последние 3 юзернейма:\n"
                        f"   • Не содержали ссылки на чаты\n"
                        f"   • Не содержали Excel файлы\n\n"
                        f"📊 **Статистика:**\n"
                        f"   ✅ Успешно: {success_count}\n"
                        f"   ❌ Неудач: {total_processed - success_count}\n"
                        f"   📈 Всего: {total_processed}\n\n"
                        f"🛑 **ЦЕПОЧКА {chain_num} ОСТАНОВЛЕНА**\n"
                        f"✅ Остальные цепочки продолжают работу\n"
                        f"━━━━━━━━━━━━━━━━━━"
                    )
                    send_notification_sync(critical_alert)
                    
                    self.log_event(chain_num, "🛑 КРИТИЧНО: 3 неудачи подряд", f"ОСТАНОВКА ЦЕПОЧКИ {chain_num}")
                    self.stop_chain_scripts(chain_num)
        
        # Триггер: filtr отправил ссылки → запуск invocer
        elif script_type == 'filtr' and re.search(TRIGGERS['filtr_sent_links'], log_line):
            self.log_event(chain_num, "🎯 ТРИГГЕР: filtr отправил ссылки", "релевантные ссылки в канале")
            
            # Получаем статистику из общей базы фильтров
            db_stats = self.get_filtr_stats_from_db()
            
            # Отправляем итог filtr в бот
            filtr_report = (
                f"📊 **ИТОГ FILTR{chain_num}:**\n"
                f"✅ Фильтрация завершена\n"
                f"📤 Релевантные ссылки отправлены в канал invocer\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"{db_stats}\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"➡️ Запускаю invocer{chain_num}..."
            )
            send_notification_sync(filtr_report)
            
            self.trigger_next_stage(chain_num, 'invocer')
        
        # Триггер: invocer ждет ссылки → запуск deleter
        elif script_type == 'invocer' and re.search(TRIGGERS['invocer_waiting'], log_line):
            self.log_event(chain_num, "🎯 ТРИГГЕР: invocer завершил", "нет ссылок для добавления")
            
            # Отправляем итог invocer в бот
            invocer_report = (
                f"📊 **ИТОГ INVOCER{chain_num}:**\n"
                f"✅ Вступление в группы завершено\n"
                f"📭 Нет ссылок для добавления\n"
                f"➡️ Запускаю deleter{chain_num}..."
            )
            send_notification_sync(invocer_report)
            
            self.trigger_next_stage(chain_num, 'deleter')
        
        # Триггер: deleter завершил → завершение цикла
        elif script_type == 'deleter' and re.search(TRIGGERS['deleter_finished'], log_line):
            self.log_event(chain_num, "🎯 ТРИГГЕР: deleter завершил", "получена итоговая статистика")
            
            # Отправляем итог deleter в бот
            deleter_report = (
                f"📊 **ИТОГ DELETER{chain_num}:**\n"
                f"✅ Очистка групп завершена\n"
                f"📈 Статистика получена\n"
                f"🏁 Цепочка {chain_num} завершена!"
            )
            send_notification_sync(deleter_report)
            
            self.complete_chain_cycle(chain_num)
    
    def get_probiv_detailed_stats(self, chain_num):
        """Получение детальной статистики пробива из логов с датами"""
        detailed_log_path = f'd:\\ss\\cycle\\chain\\probivs\\probiv{chain_num}\\processed_usernames_detailed.txt'
        
        if not os.path.exists(detailed_log_path):
            return {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'last_session_date': None,
                'sessions_today': 0
            }
        
        try:
            with open(detailed_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            total_processed = 0
            successful = 0
            failed = 0
            last_session_date = None
            sessions_today = 0
            today = datetime.now().strftime("%Y-%m-%d")
            
            for line in lines:
                line = line.strip()
                if not line or '======' in line or 'СЕССИЯ:' in line:
                    if 'СЕССИЯ:' in line and today in line:
                        sessions_today += 1
                    continue
                
                # Обрабатываем строки с юзернеймами в новом формате
                # [2025-11-06 21:30:45] 1. @username ✅ [Чаты получены]
                # или старом формате: 1. @username ✅ [Чаты получены]
                if '@' in line:
                    total_processed += 1
                    
                    # Извлекаем дату если есть
                    if line.startswith('[') and ']' in line:
                        try:
                            date_part = line.split(']')[0][1:]  # Убираем [ и ]
                            last_session_date = datetime.strptime(date_part, "%Y-%m-%d %H:%M:%S")
                        except:
                            pass
                    
                    # Определяем успешность
                    if '✅' in line and 'Чаты получены' in line:
                        successful += 1
                    elif '❌' in line:
                        failed += 1
            
            return {
                'total_processed': total_processed,
                'successful': successful,
                'failed': failed,
                'success_rate': round((successful / total_processed * 100) if total_processed > 0 else 0, 1),
                'last_session_date': last_session_date.strftime("%Y-%m-%d %H:%M:%S") if last_session_date else None,
                'sessions_today': sessions_today
            }
            
        except Exception as e:
            self.log_event(chain_num, f"Ошибка чтения детальных логов probiv{chain_num}", str(e))
            return {
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'last_session_date': None,
                'sessions_today': 0
            }
    
    def get_probiv_status(self):
        """Получение статистики всех probiv с учетом детальных логов"""
        status_text = "📈 **СТАТИСТИКА PROBIV СКРИПТОВ**\n\n"
        
        for chain_num in [1, 2, 3]:
            # Получаем детальную статистику из логов с датами
            detailed_stats = self.get_probiv_detailed_stats(chain_num)
            
            # Получаем текущую статистику из памяти
            memory_stats = self.probiv_stats[chain_num]
            
            # Определяем статус процесса
            is_running = any(
                info.get('type') == 'probiv' and info.get('chain') == chain_num 
                for info in self.active_processes.values()
            )
            
            status_emoji = "🟢" if is_running else "🔴"
            
            status_text += (
                f"🔗 **PROBIV {chain_num}** {status_emoji}\n"
                f"📊 Всего обработано: {detailed_stats['total_processed']}\n"
                f"✅ Успешно: {detailed_stats['successful']} ({detailed_stats['success_rate']}%)\n"
                f"❌ Неудач: {detailed_stats['failed']}\n"
                f"📅 Сессий сегодня: {detailed_stats['sessions_today']}\n"
            )
            
            if detailed_stats['last_session_date']:
                status_text += f"🕒 Последняя обработка: {detailed_stats['last_session_date']}\n"
            
            # Добавляем данные из памяти (текущая сессия)
            if memory_stats['total'] > 0:
                status_text += f"🔄 Текущая сессия: {memory_stats['success']}/{memory_stats['total']}\n"
            
            status_text += "\n"
        
        return status_text
    
    def stop_probiv(self, chain_num):
        """Остановка только probiv для конкретной цепочки"""
        probiv_script = f'probiv{chain_num}.py'
        print(f"\n🛑 Останавливаю probiv{chain_num}...")
        
        # Останавливаем процесс probiv
        for script_name, proc_info in list(self.active_processes.items()):
            if probiv_script in script_name:
                try:
                    process = proc_info['process']
                    if process.poll() is None:
                        print(f"   ⏹️ Останавливаю {script_name}...")
                        process.terminate()
                        process.wait(timeout=5)
                        print(f"   ✅ {script_name} остановлен")
                    # Удаляем из списка активных
                    del self.active_processes[script_name]
                except Exception as e:
                    print(f"   ⚠️ Ошибка остановки {script_name}: {e}")
        
        # Принудительная остановка через wmic
        try:
            subprocess.run(
                f'wmic process where "commandline like \'%{probiv_script}%\'" call terminate',
                shell=True,
                capture_output=True,
                timeout=3
            )
        except:
            pass
        
        self.log_event(chain_num, f"🛑 probiv{chain_num} остановлен после триггера")
    
    def trigger_next_stage(self, chain_num, next_stage):
        """Запуск следующего этапа по триггеру"""
        print(f"\n🔍 ОТЛАДКА: trigger_next_stage вызван для цепочки {chain_num}, этап {next_stage}")
        
        # Для unified скриптов используем специальную проверку
        if next_stage == 'unified':
            # Проверяем, не запущен ли unified скрипт
            unified_running = any(
                info.get('type') == 'unified' and info.get('chain') == chain_num 
                for info in self.active_processes.values()
            )
            if not unified_running:
                print(f"   Запуск unified скрипта для цепочки {chain_num}")
                self.log_event(chain_num, f"⏳ Пауза 5 сек перед unified")
                time.sleep(5)
                
                script_path = CHAINS[chain_num]['unified']
                print(f"   Путь к unified скрипту: {script_path}")
                
                if self.start_script_in_new_tab(chain_num, 'unified', script_path):
                    # Настраиваем мониторинг для unified скрипта
                    self.setup_log_monitoring(chain_num, 'unified')
                    self.log_event(chain_num, f"✅ unified успешно запущен по триггеру")
                else:
                    self.log_event(chain_num, f"❌ ОШИБКА: не удалось запустить unified")
            else:
                self.log_event(chain_num, f"⚠️ unified уже запущен, пропуск триггера")
                print(f"   ⚠️ unified уже был запущен ранее!")
            return
        
        # Для обычных скриптов используем старую логику
        print(f"   Текущий статус {next_stage}: {self.chain_status[chain_num].get(next_stage, False)}")
        
        if not self.chain_status[chain_num].get(next_stage, False):
            
            # Если запускаем filtr - сначала останавливаем probiv
            if next_stage == 'filtr':
                self.stop_probiv(chain_num)
            
            self.log_event(chain_num, f"⏳ Пауза 5 сек перед {next_stage}")
            time.sleep(5)
            
            script_path = CHAINS[chain_num][next_stage]
            print(f"   Путь к скрипту: {script_path}")
            
            if self.start_script_in_new_tab(chain_num, next_stage, script_path):
                
                # Настраиваем мониторинг для новых скриптов
                if next_stage in ['filtr', 'invocer', 'deleter']:
                    self.setup_log_monitoring(chain_num, next_stage)
                
                self.log_event(chain_num, f"✅ {next_stage} успешно запущен по триггеру")
            else:
                self.log_event(chain_num, f"❌ ОШИБКА: не удалось запустить {next_stage}")
        else:
            self.log_event(chain_num, f"⚠️ {next_stage} уже запущен, пропуск триггера")
            print(f"   ⚠️ {next_stage} уже был запущен ранее!")
    
    def start_single_script(self, chain_num, script_type):
        """Запуск отдельного скрипта без цепочки"""
        if script_type not in ['probiv', 'filtr', 'invocer', 'deleter', 'unified']:
            self.log_event(chain_num, f"❌ ОШИБКА: неизвестный тип скрипта {script_type}")
            return f"❌ Неизвестный тип скрипта: {script_type}"
        
        if chain_num not in [1, 2, 3]:
            self.log_event(chain_num, f"❌ ОШИБКА: неверный номер цепочки {chain_num}")
            return f"❌ Неверный номер цепочки: {chain_num}"
        
        # Проверяем, не запущен ли уже этот скрипт
        if self.chain_status[chain_num][script_type]:
            self.log_event(chain_num, f"⚠️ {script_type} уже запущен (chain_status)")
            return f"⚠️ {script_type.upper()}{chain_num} уже запущен"
        
        # Дополнительная проверка через active_processes
        for proc_name, proc_info in self.active_processes.items():
            if proc_info.get('chain') == chain_num and proc_info.get('type') == script_type:
                if proc_info.get('status') == 'running':
                    self.log_event(chain_num, f"⚠️ {script_type} уже запущен (active_processes)")
                    return f"⚠️ {script_type.upper()}{chain_num} уже запущен"
        
        script_path = CHAINS[chain_num][script_type]
        self.log_event(chain_num, f"🚀 Запуск отдельного скрипта {script_type}")
        
        if self.start_script_in_new_tab(chain_num, script_type, script_path):
            # Настраиваем мониторинг только для этого скрипта
            self.setup_log_monitoring(chain_num, script_type)
            self.log_event(chain_num, f"✅ {script_type} запущен как отдельный скрипт")
            return f"✅ {script_type.upper()}{chain_num} успешно запущен"
        else:
            self.log_event(chain_num, f"❌ ОШИБКА запуска {script_type}")
            return f"❌ Ошибка запуска {script_type.upper()}{chain_num}"

    def stop_single_script(self, chain_num, script_type):
        """Остановка отдельного скрипта"""
        if script_type not in ['probiv', 'filtr', 'invocer', 'deleter', 'unified']:
            self.log_event(chain_num, f"❌ ОШИБКА: неизвестный тип скрипта {script_type}")
            return f"❌ Неизвестный тип скрипта: {script_type}"
        
        # Определяем имя скрипта в active_processes и имя файла
        if script_type == 'unified':
            script_name_key = f'unified_chain{chain_num}'
            script_file = f'unified_chain{chain_num}.py'
        else:
            script_name_key = f'{script_type}{chain_num}'
            script_file = f'{script_type}{chain_num}.py'
        
        self.log_event(chain_num, f"🛑 Остановка {script_type}")
        
        # Останавливаем процесс
        script_stopped = False
        for script_name, proc_info in list(self.active_processes.items()):
            # Проверяем и по ключу в словаре, и по имени файла
            if script_name == script_name_key or script_file in script_name:
                try:
                    process = proc_info['process']
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=5)
                        self.log_event(chain_num, f"✅ {script_type} остановлен")
                    del self.active_processes[script_name]
                    script_stopped = True
                    break
                except Exception as e:
                    self.log_event(chain_num, f"⚠️ Ошибка остановки {script_type}: {e}")
        
        # Принудительная остановка через wmic
        try:
            subprocess.run(
                f'wmic process where "commandline like \'%{script_file}%\'" call terminate',
                shell=True,
                capture_output=True,
                timeout=3
            )
        except:
            pass
        
        # Обновляем статус
        self.chain_status[chain_num][script_type] = False
        
        if script_stopped:
            return f"✅ {script_type.upper()}{chain_num} успешно остановлен"
        else:
            return f"⚠️ {script_type.upper()}{chain_num} не был запущен или уже остановлен"

    def start_chain_from_stage(self, chain_num, start_stage):
        """Запуск цепочки с определенного звена"""
        stages = ['probiv', 'filtr', 'invocer', 'deleter']
        
        if start_stage not in stages:
            self.log_event(chain_num, f"❌ ОШИБКА: неизвестный этап {start_stage}")
            return f"❌ Неизвестный этап: {start_stage}"
        
        start_index = stages.index(start_stage)
        
        self.log_event(chain_num, f"🚀 Запуск цепочки {chain_num} с этапа {start_stage}")
        
        # Особая логика для invocer-deleter пары
        if start_stage == 'invocer':
            # Запускаем только invocer, deleter запустится по триггеру
            stage = 'invocer'
            if not self.chain_status[chain_num][stage]:
                script_path = CHAINS[chain_num][stage]
                
                self.log_event(chain_num, f"⏳ Запуск {stage}")
                self.log_event(chain_num, "ℹ️ DELETER запустится автоматически по сигналу invocer")
                
                if self.start_script_in_new_tab(chain_num, stage, script_path):
                    self.setup_log_monitoring(chain_num, stage)
                    self.log_event(chain_num, f"✅ {stage} запущен")
                    return f"✅ Цепочка {chain_num} запущена с INVOCER\n(DELETER запустится автоматически)"
                else:
                    self.log_event(chain_num, f"❌ ОШИБКА запуска {stage}")
                    return f"❌ Ошибка запуска INVOCER{chain_num}"
            else:
                self.log_event(chain_num, f"⚠️ {stage} уже запущен")
                return f"⚠️ INVOCER{chain_num} уже запущен"
        
        elif start_stage == 'deleter':
            # Запускаем только deleter (если invocer уже работал)
            stage = 'deleter'
            if not self.chain_status[chain_num][stage]:
                script_path = CHAINS[chain_num][stage]
                
                self.log_event(chain_num, f"⏳ Запуск {stage}")
                
                if self.start_script_in_new_tab(chain_num, stage, script_path):
                    self.setup_log_monitoring(chain_num, stage)
                    self.log_event(chain_num, f"✅ {stage} запущен")
                    return f"✅ DELETER{chain_num} запущен"
                else:
                    self.log_event(chain_num, f"❌ ОШИБКА запуска {stage}")
                    return f"❌ Ошибка запуска DELETER{chain_num}"
            else:
                self.log_event(chain_num, f"⚠️ {stage} уже запущен")
                return f"⚠️ DELETER{chain_num} уже запущен"
        
        else:
            # Обычная логика для probiv и filtr
            started_stages = []
            for i in range(start_index, len(stages)):
                stage = stages[i]
                
                # Останавливаемся на invocer, не запускаем deleter автоматически
                if stage == 'deleter':
                    self.log_event(chain_num, "ℹ️ DELETER запустится автоматически по сигналу invocer")
                    break
                
                if not self.chain_status[chain_num][stage]:
                    script_path = CHAINS[chain_num][stage]
                    
                    self.log_event(chain_num, f"⏳ Запуск {stage}")
                    
                    if self.start_script_in_new_tab(chain_num, stage, script_path):
                        self.setup_log_monitoring(chain_num, stage)
                        self.log_event(chain_num, f"✅ {stage} запущен")
                        started_stages.append(stage.upper())
                        
                        # Для probiv не ждем, для остальных - пауза между запусками
                        if stage != 'probiv' and i < len(stages) - 2:  # -2 чтобы не ждать перед deleter
                            time.sleep(3)
                    else:
                        self.log_event(chain_num, f"❌ ОШИБКА запуска {stage}")
                        return f"❌ Ошибка запуска {stage.upper()}{chain_num}"
                else:
                    self.log_event(chain_num, f"⚠️ {stage} уже запущен, пропуск")
                    started_stages.append(f"{stage.upper()} (уже был запущен)")
        
        if started_stages:
            stages_text = "\n".join([f"✅ {s}" for s in started_stages])
            return f"✅ Цепочка {chain_num} запущена:\n{stages_text}"
        else:
            return f"✅ Цепочка {chain_num} запущена с {start_stage.upper()}"

    def complete_chain_cycle(self, chain_num):
        """Завершение полного цикла цепочки"""
        self.log_event(chain_num, "🏁 ЦИКЛ ЗАВЕРШЕН", "Все этапы пройдены успешно")
        
        # Собираем статистику для финального отчета
        probiv_success = self.probiv_stats[chain_num]['success']
        probiv_total = self.probiv_stats[chain_num]['total']
        probiv_failed = probiv_total - probiv_success
        
        # Формируем полный итоговый отчет
        final_report = (
            f"🏆 **ПОЛНЫЙ ИТОГ ЦЕПОЧКИ {chain_num}:**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"1️⃣ **PROBIV{chain_num}:**\n"
            f"   ✅ Успешно: {probiv_success}\n"
            f"   ❌ Неудач: {probiv_failed}\n"
            f"   📊 Всего: {probiv_total}\n\n"
            f"2️⃣ **FILTR{chain_num}:**\n"
            f"   ✅ Фильтрация завершена\n"
            f"   📤 Ссылки отправлены в канал\n\n"
            f"3️⃣ **INVOCER{chain_num}:**\n"
            f"   ✅ Вступление в группы завершено\n"
            f"   📭 Все ссылки обработаны\n\n"
            f"4️⃣ **DELETER{chain_num}:**\n"
            f"   ✅ Очистка групп завершена\n"
            f"   📈 Статистика получена\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎉 **Цепочка {chain_num} полностью завершена!**"
        )
        send_notification_sync(final_report)
        
        # Сбрасываем статус цепочки для возможного перезапуска
        self.chain_status[chain_num] = {
            'probiv': False, 'filtr': False, 'invocer': False, 'deleter': False, 'unified': False
        }
        
        # Проверяем, завершились ли все цепочки
        all_chains_completed = all(
            not any(status.values()) for status in self.chain_status.values()
        )
        
        if all_chains_completed:
            self.log_event(0, "🎉 ВСЕ ЦЕПОЧКИ ЗАВЕРШЕНЫ", "Полный цикл выполнен")
            
            # Получаем статистику из общей базы фильтров
            db_stats = self.get_filtr_stats_from_db()
            
            # Отправляем финальный отчет по всем цепочкам
            all_chains_report = "🌟 **ВСЕ ЦЕПОЧКИ ЗАВЕРШЕНЫ:**\n━━━━━━━━━━━━━━━━━━\n"
            for chain in [1, 2, 3]:
                stats = self.probiv_stats[chain]
                all_chains_report += (
                    f"Цепочка {chain}: {stats['success']}/{stats['total']} успешно\n"
                )
            all_chains_report += f"━━━━━━━━━━━━━━━━━━\n{db_stats}\n━━━━━━━━━━━━━━━━━━\n💡 Мастер продолжает мониторинг"
            send_notification_sync(all_chains_report)
            
            self.log_event(0, "💡 Мастер продолжает мониторинг для новых циклов")
        else:
            active_chains = [
                chain for chain, status in self.chain_status.items() 
                if any(status.values())
            ]
            self.log_event(0, f"📊 Активные цепочки: {active_chains}")
    
    def start_probiv_scripts_manual(self):
        """Ручной запуск ТОЛЬКО probiv скриптов (БЕЗ автоматической цепочки)"""
        self.log_event(0, "🚀 РУЧНОЙ ЗАПУСК PROBIV: запускаю только probiv без автоцепочки")
        
        results = []
        
        if self.wt_available:
            # Создаем 3 отдельных ИМЕНОВАННЫХ окна Windows Terminal (по одному на цепочку)
            for chain_num in [1, 2, 3]:
                probiv_path = CHAINS[chain_num]['probiv']
                script_dir = os.path.dirname(probiv_path)
                script_file = os.path.basename(probiv_path)
                
                # Создаем именованное окно: -w chain1, chain2, chain3
                python_exe = "D:/ss/cycle/.venv/Scripts/python.exe"
                wt_cmd = [
                    'wt', '-w', f'chain{chain_num}',
                    'cmd', '/k', f'title Цепь {chain_num} - PROBIV{chain_num} && cd /d "{script_dir}" && "{python_exe}" "{script_file}"'
                ]
                
                try:
                    process = subprocess.Popen(wt_cmd)
                    
                    self.active_processes[f'probiv{chain_num}'] = {
                        'process': process, 
                        'chain': chain_num, 
                        'type': 'probiv',
                        'manual_mode': True  # Флаг ручного режима (без автоцепочки)
                    }
                    self.chain_status[chain_num]['probiv'] = True
                    # НЕ вызываем setup_log_monitoring - нет автоматических переходов!
                    
                    self.log_event(chain_num, f"probiv запущен (ручной режим)", f"Окно 'chain{chain_num}' создано")
                    results.append(f"✅ Probiv{chain_num} запущен (только probiv)")
                    time.sleep(2)  # Пауза между окнами
                    
                except Exception as e:
                    self.log_event(chain_num, f"Ошибка запуска probiv{chain_num}: {e}")
                    results.append(f"❌ Ошибка запуска Probiv{chain_num}: {e}")
        else:
            # Резервный вариант без Windows Terminal
            for chain_num in [1, 2, 3]:
                probiv_path = CHAINS[chain_num]['probiv']
                script_dir = os.path.dirname(probiv_path)
                script_file = os.path.basename(probiv_path)
                
                window_title = f"Цепь {chain_num} - PROBIV{chain_num}"
                cmd = f'cmd /c start "{window_title}" cmd /k "cd /d \\"{script_dir}\\" && python \\"{script_file}\\""'
                
                try:
                    process = subprocess.Popen(cmd, shell=True)
                    self.active_processes[f'probiv{chain_num}'] = {
                        'process': process,
                        'chain': chain_num,
                        'type': 'probiv',
                        'manual_mode': True  # Флаг ручного режима (без автоцепочки)
                    }
                    self.chain_status[chain_num]['probiv'] = True
                    # НЕ вызываем setup_log_monitoring - нет автоматических переходов!
                    self.log_event(chain_num, "probiv запущен (ручной режим)", "отдельное окно")
                    results.append(f"✅ Probiv{chain_num} запущен (только probiv)")
                    time.sleep(2)
                except Exception as e:
                    self.log_event(chain_num, f"Ошибка запуска probiv{chain_num}: {e}")
                    results.append(f"❌ Ошибка запуска Probiv{chain_num}: {e}")
        
        return "\n".join(results)
    
    def start_unified_scripts_manual(self):
        """Ручной запуск ТОЛЬКО unified скриптов (БЕЗ автоматической цепочки)
        
        Unified скрипты запускаются ПООЧЕРЕДНО с прогрессивными рандомными паузами:
        - Unified1: через 1-3 минуты после нажатия кнопки
        - Unified2: через 4-9 минут после Unified1
        - Unified3: через 11-17 минут после Unified2
        Это максимально имитирует поведение реального человека.
        """
        import random
        self.log_event(0, "🚀 РУЧНОЙ ЗАПУСК UNIFIED: запускаю unified с прогрессивными паузами")
        
        results = []
        
        # Определяем интервалы задержек для каждого аккаунта (в минутах)
        delay_ranges = {
            1: (1, 3),    # Unified1: 1-3 минуты после кнопки
            2: (4, 9),    # Unified2: 4-9 минут после Unified1
            3: (11, 17)   # Unified3: 11-17 минут после Unified2
        }
        
        if self.wt_available:
            # Создаем 3 отдельных ИМЕНОВАННЫХ окна Windows Terminal (по одному на аккаунт)
            for chain_num in [1, 2, 3]:
                # Пауза ПЕРЕД запуском (имитация "человек подумал, потом запустил")
                min_delay, max_delay = delay_ranges[chain_num]
                pause_minutes = random.randint(min_delay, max_delay)
                pause_seconds = pause_minutes * 60
                
                self.log_event(0, f"⏳ Пауза {pause_minutes} мин перед запуском unified{chain_num} (имитация раздумий)...")
                results.append(f"⏳ Ожидание {pause_minutes} мин перед unified{chain_num}...")
                time.sleep(pause_seconds)
                
                unified_path = CHAINS[chain_num]['unified']
                script_dir = os.path.dirname(unified_path)
                script_file = os.path.basename(unified_path)
                
                # Создаем именованное окно: -w unified1, unified2, unified3
                python_exe = "D:/ss/cycle/.venv/Scripts/python.exe"
                wt_cmd = [
                    'wt', '-w', f'unified{chain_num}',
                    'cmd', '/k', f'title UNIFIED{chain_num} (Аккаунт {chain_num}) && cd /d "{script_dir}" && "{python_exe}" "{script_file}"'
                ]
                
                try:
                    process = subprocess.Popen(wt_cmd)
                    
                    self.active_processes[f'unified{chain_num}'] = {
                        'process': process, 
                        'chain': chain_num, 
                        'type': 'unified',
                        'manual_mode': True  # Флаг ручного режима (без автоцепочки)
                    }
                    self.chain_status[chain_num]['unified'] = True
                    # НЕ вызываем setup_log_monitoring - нет автоматических переходов!
                    
                    self.log_event(chain_num, f"unified запущен (ручной режим)", f"Окно 'unified{chain_num}' создано")
                    results.append(f"✅ Unified{chain_num} (Аккаунт {chain_num}) запущен!")
                    
                except Exception as e:
                    self.log_event(chain_num, f"Ошибка запуска unified{chain_num}: {e}")
                    results.append(f"❌ Ошибка запуска Unified{chain_num}: {e}")
        else:
            # Резервный вариант без Windows Terminal
            for chain_num in [1, 2, 3]:
                # Пауза ПЕРЕД запуском (имитация "человек подумал, потом запустил")
                min_delay, max_delay = delay_ranges[chain_num]
                pause_minutes = random.randint(min_delay, max_delay)
                pause_seconds = pause_minutes * 60
                
                self.log_event(0, f"⏳ Пауза {pause_minutes} мин перед запуском unified{chain_num} (имитация раздумий)...")
                results.append(f"⏳ Ожидание {pause_minutes} мин перед unified{chain_num}...")
                time.sleep(pause_seconds)
                
                unified_path = CHAINS[chain_num]['unified']
                script_dir = os.path.dirname(unified_path)
                script_file = os.path.basename(unified_path)
                
                window_title = f"UNIFIED{chain_num} (Аккаунт {chain_num})"
                cmd = f'cmd /c start "{window_title}" cmd /k "cd /d \\"{script_dir}\\" && python \\"{script_file}\\""'
                
                try:
                    process = subprocess.Popen(cmd, shell=True)
                    self.active_processes[f'unified{chain_num}'] = {
                        'process': process,
                        'chain': chain_num,
                        'type': 'unified',
                        'manual_mode': True  # Флаг ручного режима (без автоцепочки)
                    }
                    self.chain_status[chain_num]['unified'] = True
                    # НЕ вызываем setup_log_monitoring - нет автоматических переходов!
                    self.log_event(chain_num, "unified запущен (ручной режим)", "отдельное окно")
                    results.append(f"✅ Unified{chain_num} (Аккаунт {chain_num}) запущен!")
                    
                except Exception as e:
                    self.log_event(chain_num, f"Ошибка запуска unified{chain_num}: {e}")
                    results.append(f"❌ Ошибка запуска Unified{chain_num}: {e}")
        
        return "\n".join(results)
    
    def show_status(self):
        """Показать статус"""
        print("\n" + "="*60)
        print("📊 СТАТУС ЦЕПОЧЕК:")
        print("="*60)
        
        for chain_num in [1, 2, 3]:
            status = self.chain_status[chain_num]
            print(f"\n🔗 Цепочка {chain_num}:")
            
            for stage, is_running in status.items():
                emoji = "🟢" if is_running else "⚪"
                print(f"   {emoji} {stage}: {'запущен' if is_running else 'ожидание триггера'}")
    
    def get_full_status(self):
        """Получить полный статус всех цепочек в виде строки"""
        status_lines = ["📊 **СТАТУС ВСЕХ ЦЕПОЧЕК:**\n"]
        
        for chain_num in [1, 2, 3]:
            status = self.chain_status[chain_num]
            status_lines.append(f"🔗 **Цепочка {chain_num}:**")
            
            for stage, is_running in status.items():
                emoji = "🟢" if is_running else "⚪"
                status_text = "запущен" if is_running else "ожидание триггера"
                status_lines.append(f"   {emoji} {stage}: {status_text}")
            status_lines.append("")  # Пустая строка между цепочками
        
        return "\n".join(status_lines)
    
    def get_chain_status(self, chain_num):
        """Получить статус конкретной цепочки в виде строки"""
        if chain_num not in self.chain_status:
            return f"❌ Цепочка {chain_num} не найдена"
        
        status = self.chain_status[chain_num]
        status_lines = [f"📊 **СТАТУС ЦЕПОЧКИ {chain_num}:**\n"]
        
        for stage, is_running in status.items():
            emoji = "🟢" if is_running else "⚪"
            status_text = "запущен" if is_running else "ожидание триггера"
            status_lines.append(f"   {emoji} {stage}: {status_text}")
        
        return "\n".join(status_lines)
    
    def run(self):
        """Главный цикл работы - только мониторинг триггеров"""
        try:
            print(f"\n🤖 МАСТЕР-ПАНЕЛЬ ЗАПУЩЕНА")
            print("=" * 50)
            print("📱 Telegram бот активен для управления скриптами")
            print("� Система мониторинга триггеров активна")
            print("🛑 Нажмите Ctrl+C для остановки")
            print("=" * 50)
            print("\n💡 Откройте бота в Telegram и отправьте /start")
            print("   Всё управление через удобные кнопки - команды не нужны!")
            
            # Главный цикл - только мониторинг триггеров
            while True:
                if self.monitoring:
                    time.sleep(30)
                    # Показываем статус только если есть запущенные процессы
                    if self.active_processes:
                        self.show_status()
                    else:
                        print(f"\n⏸️ [{datetime.now().strftime('%H:%M:%S')}] Мониторинг активен, ожидание команд от бота...")
                else:
                    # Если мониторинг остановлен ботом, просто ждем
                    print(f"\n⏸️ [{datetime.now().strftime('%H:%M:%S')}] Мониторинг остановлен. Ожидание команд от Telegram бота...")
                    time.sleep(10)
                
        except KeyboardInterrupt:
            print("\n🛑 Получен сигнал остановки...")
            self.monitoring = False
            self.stop_all_scripts()
            print("✅ Все скрипты остановлены")
    
    def restart_monitoring(self):
        """Перезапуск системы мониторинга"""
        global master_system_enabled
        
        if not master_system_enabled:
            print("⚠️ Мастер-система выключена - мониторинг не будет запущен")
            return "⚠️ Мастер-система выключена. Используйте /master on для включения"
        
        if not self.monitoring:
            self.monitoring = True
            print("🔄 Мониторинг триггеров перезапущен")
            return "✅ Мониторинг триггеров перезапущен"
        return "⚠️ Мониторинг уже активен"
    
    def stop_chain_scripts(self, chain_num):
        """Остановка скриптов конкретной цепочки"""
        print(f"\n🛑 Остановка цепочки {chain_num}...")
        
        # Скрипты этой цепочки
        chain_scripts = [
            f'probiv{chain_num}.py',
            f'filtr{chain_num}.py',
            f'invocer{chain_num}.py',
            f'deleter{chain_num}.py'
        ]
        
        stopped_scripts = []
        
        # Останавливаем процессы этой цепочки
        for script_name, proc_info in list(self.active_processes.items()):
            if any(script in script_name for script in chain_scripts):
                try:
                    process = proc_info['process']
                    if process.poll() is None:
                        print(f"   ⏹️ Останавливаю {script_name}...")
                        process.terminate()
                        process.wait(timeout=5)
                        stopped_scripts.append(script_name)
                    # Удаляем из списка активных
                    del self.active_processes[script_name]
                except Exception as e:
                    print(f"   ⚠️ Ошибка остановки {script_name}: {e}")
        
        # Принудительная остановка через wmic
        for script_name in chain_scripts:
            try:
                subprocess.run(
                    f'wmic process where "commandline like \'%{script_name}%\'" call terminate',
                    shell=True,
                    capture_output=True,
                    timeout=3
                )
            except:
                pass
        
        # Сбрасываем статус цепочки
        self.chain_status[chain_num] = {
            'probiv': False, 'filtr': False, 'invocer': False, 'deleter': False, 'unified': False
        }
        
        print(f"✅ Цепочка {chain_num} остановлена")
        
        if stopped_scripts:
            scripts_text = "\n".join([f"🛑 {script}" for script in stopped_scripts])
            return f"✅ Цепочка {chain_num} остановлена:\n{scripts_text}"
        else:
            return f"⚠️ Цепочка {chain_num} не была запущена или уже остановлена"
    
    def stop_all_scripts(self):
        """Остановка всех запущенных скриптов (БЕЗ остановки мастера)"""
        print("\n🛑 Остановка всех активных скриптов...")
        
        # Останавливаем все процессы
        for script_name, proc_info in self.active_processes.items():
            try:
                process = proc_info['process']
                if process.poll() is None:  # Процесс еще работает
                    print(f"   ⏹️ Останавливаю {script_name}...")
                    process.terminate()
                    process.wait(timeout=5)
            except Exception as e:
                print(f"   ⚠️ Ошибка остановки {script_name}: {e}")
        
        # Очищаем список активных процессов
        self.active_processes.clear()
        
        # Дополнительно убиваем все Python процессы с нашими скриптами через taskkill
        print("\n🔥 Принудительная остановка всех связанных процессов...")
        script_names = ['probiv1.py', 'probiv2.py', 'probiv3.py',
                       'filtr1.py', 'filtr2.py', 'filtr3.py',
                       'invocer1.py', 'invocer2.py', 'invocer3.py', 'invocer4.py',
                       'deleter1.py', 'deleter2.py', 'deleter3.py', 'deleter4.py']
        
        for script_name in script_names:
            try:
                # Используем wmic для поиска и завершения процессов
                subprocess.run(
                    f'wmic process where "commandline like \'%{script_name}%\'" call terminate',
                    shell=True,
                    capture_output=True,
                    timeout=3
                )
            except:
                pass
        
        # Останавливаем мониторинг, но НЕ выходим из программы
        self.monitoring = False
        print("✅ Все скрипты остановлены, мастер продолжает работу для управления ботом")
    
    def stop_all_chains(self):
        """Остановка всех цепочек (1, 2, 3)"""
        print("\n🛑 Массовая остановка ВСЕХ цепочек...")
        
        results = []
        for chain_num in [1, 2, 3]:
            print(f"\n📌 Останавливаю цепочку {chain_num}...")
            result = self.stop_chain_scripts(chain_num)
            results.append(f"Цепочка {chain_num}: {result}")
        
        summary = "\n".join(results)
        return f"🛑 **МАССОВАЯ ОСТАНОВКА ЗАВЕРШЕНА**\n\n{summary}"
    
    def stop_all_script_type(self, script_type):
        """Остановка всех скриптов определенного типа (probiv1, probiv2, probiv3 и т.д.)"""
        if script_type not in ['probiv', 'filtr', 'invocer', 'deleter', 'unified']:
            return f"❌ Неизвестный тип скрипта: {script_type}"
        
        print(f"\n🛑 Массовая остановка всех {script_type.upper()} скриптов...")
        
        stopped_count = 0
        results = []
        
        # Определяем цепочки для каждого типа скрипта
        if script_type in ['probiv', 'filtr']:
            chains = [1, 2, 3]
        elif script_type == 'unified':
            # Unified всегда останавливаем все 3, независимо от CHAINS
            chains = [1, 2, 3]
            for chain_num in chains:
                result = self.stop_single_script(chain_num, script_type)
                results.append(f"unified_chain{chain_num}: {result}")
                if "успешно остановлен" in result:
                    stopped_count += 1
        else:  # invocer, deleter
            chains = [1, 2, 3, 4]
        
        # Для не-unified скриптов проверяем наличие в CHAINS
        if script_type != 'unified':
            for chain_num in chains:
                # Проверяем, есть ли этот скрипт в цепочке
                if chain_num in CHAINS and script_type in CHAINS[chain_num]:
                    result = self.stop_single_script(chain_num, script_type)
                    results.append(f"{script_type}{chain_num}: {result}")
                    if "успешно остановлен" in result:
                        stopped_count += 1
        
        if stopped_count > 0:
            summary = "\n".join(results)
            return f"🛑 **ОСТАНОВЛЕНО {stopped_count} {script_type.upper()} скриптов**\n\n{summary}"
        else:
            return f"⚠️ Ни один {script_type.upper()} скрипт не был запущен"

# Глобальная переменная для master
master_instance = None
admin_chat_id = None  # ID чата администратора (устанавливается при первом /start)
master_system_enabled = False  # Состояние мастер-системы (триггеры и мониторинг) - выключена по умолчанию

# === TELEGRAM BOT HANDLERS ===

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    """Команда /start - приветствие и главное меню"""
    global admin_chat_id
    admin_chat_id = event.chat_id
    
    buttons = [
        [Button.inline("🚀 НАЧАТЬ РАБОТУ (первый запуск)", "quick_start_guide")],
        [Button.inline("⚙️ Запустить аккаунты", "unified_menu")],
        [Button.inline("📊 Посмотреть статистику", "unified_monitoring")],
        [Button.inline("🔧 Настройки (для опытных)", "chains_menu")],
        [Button.inline("🛠️ Утилиты (парсер, очистка, слова)", "utilities_menu")],
        [Button.inline("📋 Логи", "logs_menu")],
        [Button.inline("🛑 Остановить всё", "stop_all_confirm")],
        [Button.inline("❓ Помощь", "help_menu")]
    ]
    
    # Определяем статус системы
    system_status = "🟢 Работает" if master_system_enabled else "🔴 Выключена"
    scheduler_status = "🟢 Активен" if task_scheduler and task_scheduler.running else "⚪ Неактивен"
    
    # Подсчитываем запущенные аккаунты
    running_unified = 0
    if master_instance:
        for i in [1, 2, 3]:
            if master_instance.chain_status.get(i, {}).get('unified', False):
                running_unified += 1
    
    await event.respond(
        f"**🤖 УПРАВЛЕНИЕ БОТОМ**\n\n"
        f"Работает: {running_unified} из 3 аккаунтов\n\n"
        f"❓ **Первый раз здесь?** → Нажмите «🚀 НАЧАТЬ РАБОТУ»\n\n"
        f"Выберите действие:",
        buttons=buttons
    )

# ============= НОВЫЕ УПРОЩЁННЫЕ ОБРАБОТЧИКИ =============

@bot.on(events.CallbackQuery(pattern=b'quick_start_guide'))
async def quick_start_guide_handler(event):
    """Пошаговая инструкция для первого запуска"""
    await event.answer()
    
    buttons = [
        [Button.inline("✅ Шаг 1: Запустить аккаунты", "unified_menu")],
        [Button.inline("📊 Шаг 2: Проверить работу", "unified_monitoring")],
        [Button.inline("⏰ Шаг 3: Настроить автозапуск (опционально)", "scheduler_menu")],
        [Button.inline("❓ Объясните подробнее", "help_how_it_works")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "🚀 **БЫСТРЫЙ СТАРТ (для новичков)**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🎯 ЧТО ДЕЛАЕТ СИСТЕМА?**\n\n"
        "У вас есть 3 Telegram аккаунта.\n"
        "Каждый аккаунт:\n"
        "• 🔍 Ищет новые группы для вступления\n"
        "• ✅ Проверяет их на релевантность (подходят/не подходят)\n"
        "• 📥 Вступает только в подходящие группы\n"
        "• 🗑️ Удаляется из неподходящих групп\n"
        "• 🔄 Делает это автоматически 24/7\n\n"
        "**Простыми словами:**\n"
        "Это как помощник, который круглосуточно\n"
        "находит и вступает в нужные вам группы.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📝 ПОШАГОВАЯ ИНСТРУКЦИЯ:**\n\n"
        "**ШАГ 1: ЗАПУСК** (5 минут)\n"
        "1️⃣ Нажмите «Шаг 1» ниже\n"
        "2️⃣ Нажмите «🚀 Запустить CHAIN1» — запустится аккаунт 1\n"
        "3️⃣ Подождите 2 минуты\n"
        "4️⃣ Нажмите «🚀 Запустить CHAIN2» — запустится аккаунт 2\n"
        "5️⃣ Подождите 2 минуты\n"
        "6️⃣ Нажмите «🚀 Запустить CHAIN3» — запустится аккаунт 3\n\n"
        "❓ **Зачем ждать?**\n"
        "Чтобы не перегрузить систему.\n"
        "Запускаем аккаунты постепенно.\n\n"
        "**ШАГ 2: ПРОВЕРКА** (через 10-15 минут)\n"
        "Нажмите «Шаг 2» и увидите:\n"
        "✅ Сколько групп проверено\n"
        "✅ Сколько вступлений сделано\n"
        "✅ Есть ли проблемы (обычно нет)\n\n"
        "📊 **Что означают цифры:**\n"
        "• **Проверено: 15** — система проверила 15 групп\n"
        "• **Вступил: 3** — вступил в 3 подходящие группы\n"
        "• **Блокировок: 0** — нет проблем (отлично!)\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**✅ ГОТОВО! ЧТО ДАЛЬШЕ?**\n\n"
        "Система работает сама!\n"
        "Вам нужно только 2 раза в день проверять:\n"
        "• Утром в 8-9 часов\n"
        "• Вечером в 20-22 часа\n\n"
        "Просто откройте «📊 Проверить работу»\n"
        "и посмотрите на цифры.\n\n"
        "Если увидите 🎉 или ✅ — всё отлично!\n"
        "Если увидите ⚠️ или 🚨 — бот подскажет что делать.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**⏰ ШАГ 3: АВТОЗАПУСК (необязательно)**\n\n"
        "Хотите, чтобы система запускалась сама каждое утро?\n"
        "Нажмите «Шаг 3» и настройте автозапуск в 9:00.\n\n"
        "Команда:\n"
        "`/schedule add 09:00 start_chain 1`\n"
        "`/schedule add 09:10 start_chain 2`\n"
        "`/schedule add 09:20 start_chain 3`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 **ВАЖНЫЕ СОВЕТЫ:**\n\n"
        "✅ Проверяйте статистику 2 раза в день\n"
        "✅ Если цифры растут — всё работает\n"
        "✅ Если «Блокировок» больше 10 — остановите на 2-4 часа\n"
        "✅ Не паникуйте! Бот всё объяснит\n\n"
        "**🤔 НЕ ПОНЯТНО?**\n"
        "Нажмите «❓ Объясните подробнее»\n\n"
        "Готовы начать?",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'stats_menu'))
async def stats_menu_handler(event):
    """Меню статистики"""
    await event.answer()
    
    buttons = [
        [Button.inline("📊 UNIFIED - Статистика 3 аккаунтов", "unified_monitoring")],
        [Button.inline("📈 PROBIV - Статистика цепочек", "probiv_stats")],
        [Button.inline("🗄️ База данных чатов", "database_stats")],
        [Button.inline("🔍 Сканировать логи (детально)", "manual_scan")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "📊 **СТАТИСТИКА И ОТЧЁТЫ**\n\n"
        "Здесь можно посмотреть, как работает система:\n\n"
        "**📊 UNIFIED** — Главная статистика\n"
        "Показывает работу 3 аккаунтов за сегодня:\n"
        "• Сколько вступили в групп\n"
        "• Сколько было проверок\n"
        "• Есть ли блокировки FloodWait\n\n"
        "**📈 PROBIV** — Цепочки обработки\n"
        "Детальная информация по каждой цепочке\n\n"
        "**🗄️ База данных** — Сколько чатов собрано\n"
        "Детальная статистика по всем обработанным группам\n\n"
        "**🔍 Сканировать логи** — Поиск ошибок\n"
        "Автоматический анализ всех логов на наличие проблем\n\n"
        "Выберите нужный отчёт:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_menu'))
async def help_menu_handler(event):
    """Меню помощи"""
    await event.answer()
    
    buttons = [
        [Button.inline("📖 Как работает система? (объяснение)", "help_how_it_works")],
        [Button.inline("🚦 Символы: что означают 🟢⚠️❌", "help_symbols")],
        [Button.inline("⚠️ Решение проблем (что делать если...)", "help_errors")],
        [Button.inline("📞 Часто задаваемые вопросы (FAQ)", "help_faq")],
        [Button.inline("📋 Команды бота (список всех команд)", "help_commands")],
        [Button.inline("⏰ Как настроить автозапуск?", "help_scheduler")],
        [Button.inline("🛡️ Что такое FloodWait?", "help_floodwait_explain")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "❓ **ПОМОЩЬ И ИНСТРУКЦИИ**\n\n"
        "Здесь вы найдёте ответы на все вопросы:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📖 Как работает система?**\n"
        "Объяснение простым языком\n\n"
        "**🚦 Цвета и символы**\n"
        "Что означают зелёные/красные индикаторы\n\n"
        "**⚠️ Решение проблем**\n"
        "Что делать если что-то не работает\n\n"
        "**📞 FAQ**\n"
        "Ответы на популярные вопросы\n\n"
        "Выберите нужный раздел:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_how_it_works'))
async def help_how_it_works_handler(event):
    """Объяснение как работает система"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "📖 **КАК РАБОТАЕТ СИСТЕМА**\n\n"
        "**Простыми словами:**\n\n"
        "У вас есть 3 Telegram аккаунта (CHAIN1, CHAIN2, CHAIN3).\n"
        "Каждый аккаунт работает независимо и делает одно и то же:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 1: ПОИСК** 🔍\n"
        "Аккаунт получает ссылки на Telegram группы\n\n"
        "**ШАГ 2: ВСТУПЛЕНИЕ** 👋\n"
        "Аккаунт вступает в группу (медленно, чтобы Telegram не заблокировал)\n\n"
        "**ШАГ 3: ПРОВЕРКА** ✅\n"
        "Аккаунт читает последние сообщения в группе\n"
        "Проверяет: это нужная группа или нет?\n\n"
        "**ШАГ 4: РЕШЕНИЕ** 🎯\n"
        "• Если группа подходит — остаётся в ней\n"
        "• Если не подходит — выходит из группы\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ВАЖНО:**\n"
        "• Система работает медленно (10-15 групп в час)\n"
        "• Это специально, чтобы избежать блокировок\n"
        "• За день: 480-744 вступления (все 3 аккаунта)\n"
        "• Работает 24/7 автоматически\n\n"
        "**ВАША ЗАДАЧА:**\n"
        "Проверять статистику 2 раза в день:\n"
        "• Утром в 8:00\n"
        "• Вечером в 22:00-23:00",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'add_accounts_guide'))
async def add_accounts_guide_handler(event):
    """Инструкция по добавлению новых аккаунтов"""
    await event.answer()
    
    buttons = [
        [Button.inline("📝 Инструкция для PROBIV", "add_probiv_accounts")],
        [Button.inline("📝 Инструкция для UNIFIED", "add_unified_accounts")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "➕ **ДОБАВЛЕНИЕ НОВЫХ АККАУНТОВ**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Сейчас в системе:\n"
        "• **3 PROBIV аккаунта** (пробив групп)\n"
        "• **3 UNIFIED аккаунта** (фильтрация + действия)\n\n"
        "Вы можете добавить дополнительные аккаунты:\n\n"
        "**🔹 PROBIV** — для поиска новых групп\n"
        "Если нужно больше ссылок на группы\n\n"
        "**🔹 UNIFIED** — для работы с группами\n"
        "Если нужно обрабатывать больше групп\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ **ВАЖНО:**\n"
        "• Добавление требует изменения кода\n"
        "• Нужно создать новые файлы скриптов\n"
        "• Нужно добавить сессии в папку sessions/\n\n"
        "Выберите тип аккаунта для инструкции:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'add_probiv_accounts'))
async def add_probiv_accounts_handler(event):
    """Инструкция по добавлению PROBIV аккаунтов"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад", "add_accounts_guide")]]
    
    await event.respond(
        "📝 **ИНСТРУКЦИЯ: Добавление PROBIV аккаунта**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 1: Подготовка файлов**\n\n"
        "1️⃣ Скопируйте папку:\n"
        "`d:\\ss\\cycle\\chain\\probivs\\probiv3\\`\n"
        "Переименуйте в:\n"
        "`d:\\ss\\cycle\\chain\\probivs\\probiv4\\`\n\n"
        "2️⃣ В папке probiv4 откройте файл:\n"
        "`probiv4.py`\n\n"
        "3️⃣ Найдите и измените:\n"
        "```python\n"
        "# Было:\n"
        "CHAIN_NUM = 3\n"
        "# Стало:\n"
        "CHAIN_NUM = 4\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 2: Добавление сессии**\n\n"
        "1️⃣ Создайте новую сессию Telegram:\n"
        "• Запустите `create_shared_sessions.py`\n"
        "• Войдите с нового аккаунта\n"
        "• Сохраните как `probiv4_session.session`\n\n"
        "2️⃣ Скопируйте файл в:\n"
        "`d:\\ss\\cycle\\chain\\sessions\\probiv4_session.session`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 3: Регистрация в master.py**\n\n"
        "1️⃣ Откройте файл:\n"
        "`d:\\ss\\cycle\\chain\\master.py`\n\n"
        "2️⃣ Найдите словарь CHAINS (строка ~34):\n"
        "```python\n"
        "CHAINS = {\n"
        "    1: {...},\n"
        "    2: {...},\n"
        "    3: {...},\n"
        "    # Добавьте:\n"
        "    4: {\n"
        "        'probiv': r'd:\\ss\\cycle\\chain\\probivs\\probiv4\\probiv4.py'\n"
        "    }\n"
        "}\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 4: Тестирование**\n\n"
        "1️⃣ Перезапустите мастер-бота\n"
        "2️⃣ В меню выберите:\n"
        "   «🚀 Управление PROBIV» → «Probiv4»\n"
        "3️⃣ Нажмите «▶️ Запустить»\n\n"
        "✅ Если появится окно с probiv4 — готово!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ **ВАЖНЫЕ ЗАМЕЧАНИЯ:**\n\n"
        "• Каждый probiv требует отдельный аккаунт Telegram\n"
        "• Не используйте один аккаунт для нескольких probiv\n"
        "• Проверьте права доступа к папкам\n"
        "• Убедитесь что сессия валидна",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'add_unified_accounts'))
async def add_unified_accounts_handler(event):
    """Инструкция по добавлению UNIFIED аккаунтов"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад", "add_accounts_guide")]]
    
    await event.respond(
        "📝 **ИНСТРУКЦИЯ: Добавление UNIFIED аккаунта**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 1: Подготовка файлов**\n\n"
        "1️⃣ Скопируйте файл:\n"
        "`d:\\ss\\cycle\\chain\\unified\\unified_chain3.py`\n"
        "Переименуйте в:\n"
        "`d:\\ss\\cycle\\chain\\unified\\unified_chain4.py`\n\n"
        "2️⃣ Откройте `unified_chain4.py`\n\n"
        "3️⃣ Найдите и измените:\n"
        "```python\n"
        "# Было:\n"
        "CHAIN_NUM = 3\n"
        "# Стало:\n"
        "CHAIN_NUM = 4\n```\n\n"
        "4️⃣ Найдите и измените:\n"
        "```python\n"
        "# Было:\n"
        "session_name = 'sessions/unified3_session'\n"
        "# Стало:\n"
        "session_name = 'sessions/unified4_session'\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 2: Добавление сессии**\n\n"
        "1️⃣ Создайте новую сессию Telegram:\n"
        "• Запустите `create_shared_sessions.py`\n"
        "• Войдите с нового аккаунта\n"
        "• Сохраните как `unified4_session.session`\n\n"
        "2️⃣ Скопируйте файл в:\n"
        "`d:\\ss\\cycle\\chain\\sessions\\unified4_session.session`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 3: Регистрация в master.py**\n\n"
        "1️⃣ Откройте файл:\n"
        "`d:\\ss\\cycle\\chain\\master.py`\n\n"
        "2️⃣ Найдите словарь CHAINS (строка ~34):\n"
        "```python\n"
        "CHAINS = {\n"
        "    1: {...},\n"
        "    2: {...},\n"
        "    3: {...},\n"
        "    # Добавьте:\n"
        "    4: {\n"
        "        'unified': r'd:\\ss\\cycle\\chain\\unified\\unified_chain4.py',\n"
        "        'invocer': r'd:\\ss\\cycle\\chain\\invocer\\invocer4\\invocer4.py',\n"
        "        'deleter': r'd:\\ss\\cycle\\chain\\deleter\\deleter4\\deleter4.py'\n"
        "    }\n"
        "}\n```\n\n"
        "3️⃣ В unified_menu_handler добавьте кнопку:\n"
        "```python\n"
        "[Button.inline(\"🚀 Запустить CHAIN4 (Аккаунт 4)\", \"unified_chain_4\")],\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 4: Обновление start_unified_scripts_manual**\n\n"
        "В методе `start_unified_scripts_manual` измените:\n"
        "```python\n"
        "# Было:\n"
        "for chain_num in [1, 2, 3]:\n"
        "# Стало:\n"
        "for chain_num in [1, 2, 3, 4]:\n```\n\n"
        "И добавьте в delay_ranges:\n"
        "```python\n"
        "delay_ranges = {\n"
        "    1: (1, 3),\n"
        "    2: (4, 9),\n"
        "    3: (11, 17),\n"
        "    4: (18, 25)  # Новый аккаунт\n"
        "}\n```\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ШАГ 5: Тестирование**\n\n"
        "1️⃣ Перезапустите мастер-бота\n"
        "2️⃣ В меню выберите:\n"
        "   «🔄 Управление UNIFIED» → «CHAIN4»\n"
        "3️⃣ Нажмите «🚀 Запустить»\n\n"
        "✅ Если появится окно с unified_chain4 — готово!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ **ВАЖНЫЕ ЗАМЕЧАНИЯ:**\n\n"
        "• Каждый unified требует отдельный аккаунт Telegram\n"
        "• Unified4 будет запускаться последним (через 18-25 мин)\n"
        "• Проверьте что все пути к файлам правильные\n"
        "• Убедитесь что сессия валидна перед запуском",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_symbols'))
async def help_symbols_handler(event):
    """Объяснение символов и цветов"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "🚦 **ЧТО ОЗНАЧАЮТ СИМВОЛЫ И ЦВЕТА**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ИНДИКАТОРЫ СТАТУСА:**\n\n"
        "🟢 Зелёный круг — Работает нормально\n"
        "🔴 Красный круг — Выключено\n"
        "⚪ Белый круг — Ожидание\n"
        "🟡 Жёлтый круг — Предупреждение\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ГАЛОЧКИ И КРЕСТИКИ:**\n\n"
        "✅ Галочка — Успешно, всё хорошо\n"
        "❌ Крестик — Ошибка, требует внимания\n"
        "⚠️ Треугольник — Предупреждение\n"
        "ℹ️ Буква i — Информация\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ОЦЕНКИ В МОНИТОРИНГЕ:**\n\n"
        "🎉 **ОТЛИЧНО!** — 0 блокировок FloodWait\n"
        "Система работает идеально\n\n"
        "✅ **ХОРОШО!** — 1-2 блокировки\n"
        "Это нормально, не волнуйтесь\n\n"
        "⚠️ **ПРИЕМЛЕМО** — 3-5 блокировок\n"
        "Работает, но можно лучше\n\n"
        "🚨 **ВНИМАНИЕ!** — Больше 5 блокировок\n"
        "Нужно проверить систему\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ЦИФРЫ:**\n\n"
        "📥 **Вступлений** — Сколько вступили в групп\n"
        "🔍 **Проверок** — Сколько групп проверили\n"
        "⚡ **FloodWait** — Сколько раз заблокировал Telegram\n"
        "🛡️ **Адаптация** — Система замедлилась для защиты",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_errors'))
async def help_errors_handler(event):
    """Помощь при ошибках"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "⚠️ **ЧТО ДЕЛАТЬ ЕСЛИ ОШИБКА**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❌ «Лог файл не найден»**\n\n"
        "**Причина:** Аккаунт CHAIN не запущен\n\n"
        "**Решение:**\n"
        "1. Нажмите «🔄 Управление UNIFIED»\n"
        "2. Запустите все 3 аккаунта (CHAIN1/2/3)\n"
        "3. Подождите 5 минут\n"
        "4. Проверьте мониторинг снова\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**⚠️ «НЕТ ДАННЫХ» в мониторинге**\n\n"
        "**Причина:** Аккаунт только что запущен\n\n"
        "**Решение:**\n"
        "Подождите 10-15 минут — аккаунт начнёт работать\n"
        "Статистика появится после первого вступления\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🚨 Много FloodWait (больше 10)**\n\n"
        "**Причина:** Telegram временно ограничил аккаунт\n\n"
        "**Решение:**\n"
        "1. Система сама замедлится (адаптация)\n"
        "2. Подождите 2-4 часа\n"
        "3. FloodWait сами исчезнут\n"
        "4. Ничего делать не нужно!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔴 Аккаунт не работает**\n\n"
        "**Решение:**\n"
        "1. Остановите аккаунт: «🛑 Остановить всё»\n"
        "2. Подождите 30 секунд\n"
        "3. Запустите снова: «🔄 Управление UNIFIED»\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**💡 ЗОЛОТОЕ ПРАВИЛО:**\n\n"
        "Если что-то не работает:\n"
        "1. Остановите всё\n"
        "2. Подождите 1 минуту\n"
        "3. Запустите заново\n\n"
        "В 90% случаев это решает проблему!",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_faq'))
async def help_faq_handler(event):
    """Часто задаваемые вопросы"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "📞 **ЧАСТО ЗАДАВАЕМЫЕ ВОПРОСЫ**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Как часто проверять систему?**\n\n"
        "Достаточно 2 раза в день:\n"
        "• Утром в 8:00\n"
        "• Вечером в 22:00-23:00\n\n"
        "Просто откройте «📊 Мониторинг» и посмотрите цифры.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Сколько групп в день вступает система?**\n\n"
        "Все 3 аккаунта вместе: **480-744 группы/день**\n"
        "Каждый аккаунт: 160-248 групп/день\n"
        "Это медленно, но безопасно!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Что такое FloodWait?**\n\n"
        "Это защита Telegram от спама.\n"
        "Telegram говорит: «Стоп! Слишком быстро! Подожди»\n\n"
        "**Норма:** 0-4 FloodWait в день\n"
        "**Много:** Больше 10 в день\n\n"
        "Если много — система сама замедлится.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Что такое адаптация?**\n\n"
        "Когда Telegram блокирует (FloodWait),\n"
        "система АВТОМАТИЧЕСКИ замедляется.\n\n"
        "Вы увидите: **🛡️ АДАПТАЦИЯ: 1.5x**\n\n"
        "Это значит: задержки увеличены в 1.5 раза.\n"
        "Через 2 часа вернётся к норме.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Нужно ли что-то делать вручную?**\n\n"
        "**НЕТ!** Только 2 действия:\n\n"
        "1. Утром и вечером проверить мониторинг\n"
        "2. Если видите 🎉 или ✅ — всё отлично!\n\n"
        "Система работает сама 24/7.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Что будет если отключится свет?**\n\n"
        "Когда свет вернётся:\n"
        "1. Запустите master.py (PowerShell)\n"
        "2. Запустите 3 аккаунта через бота\n"
        "3. Всё продолжит работать!\n\n"
        "Автомониторинг настроится автоматически.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**❓ Как понять что всё работает?**\n\n"
        "Откройте «📊 Мониторинг»:\n\n"
        "✅ Вступлений больше 0 — работает\n"
        "✅ Проверок больше 0 — работает\n"
        "✅ Оценка 🎉 или ✅ — отлично!\n\n"
        "Если везде 0 — подождите 10-15 минут.",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_commands'))
async def help_commands_handler(event):
    """Список всех команд бота"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "📋 **ВСЕ КОМАНДЫ БОТА**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🎯 ОСНОВНЫЕ:**\n\n"
        "`/start` — Главное меню бота\n"
        "`/status` — Быстрый статус всех цепочек\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📋 ПРОСМОТР ЛОГОВ:**\n\n"
        "`/logs` — Меню просмотра логов\n"
        "`/logs unified1` — Последние 20 строк\n"
        "`/logs unified1 50` — Последние 50 строк\n"
        "`/logs unified1 search FloodWait` — Поиск\n\n"
        "**Доступные логи:**\n"
        "master, probiv1/2/3, filtr1/2/3, unified1/2/3\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**⏰ ПЛАНИРОВЩИК:**\n\n"
        "`/schedule` — Список всех задач\n"
        "`/schedule add 09:00 start_chain 1` — Добавить задачу\n"
        "`/schedule remove 3` — Удалить задачу #3\n"
        "`/schedule disable 1` — Выключить задачу #1\n"
        "`/schedule enable 1` — Включить задачу #1\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📊 СТАТИСТИКА:**\n\n"
        "`/database` — Краткая статистика БД\n"
        "`/dbstats` — Расширенная статистика БД\n"
        "`/floodwait` — Анализ блокировок\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🔧 УПРАВЛЕНИЕ:**\n\n"
        "`/unified 1 start` — Запустить unified_chain1\n"
        "`/unified 2 stop` — Остановить unified_chain2\n"
        "`/stop_all` — Остановить всё\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**💡 СОВЕТ:**\n"
        "Используйте кнопки — это удобнее команд!\n"
        "Команды нужны для автоматизации и скриптов.",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_scheduler'))
async def help_scheduler_handler(event):
    """Помощь по планировщику"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "⏰ **КАК НАСТРОИТЬ АВТОЗАПУСК**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ЧТО ТАКОЕ ПЛАНИРОВЩИК?**\n\n"
        "Это функция, которая запускает/останавливает\n"
        "аккаунты автоматически в заданное время.\n\n"
        "Например:\n"
        "• Каждое утро в 9:00 запускать все аккаунты\n"
        "• Каждый вечер в 23:00 останавливать\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ПРИМЕР 1: Утренний автозапуск**\n\n"
        "Отправьте эти команды боту:\n\n"
        "`/schedule add 09:00 start_chain 1`\n"
        "`/schedule add 09:10 start_chain 2`\n"
        "`/schedule add 09:20 start_chain 3`\n\n"
        "Теперь каждый день в 9 утра система запустится сама!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ПРИМЕР 2: Ночная остановка**\n\n"
        "`/schedule add 23:00 stop_script 1 unified`\n"
        "`/schedule add 23:00 stop_script 2 unified`\n"
        "`/schedule add 23:00 stop_script 3 unified`\n\n"
        "Каждый вечер в 23:00 всё остановится.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**КАК ПРОВЕРИТЬ ЗАДАЧИ?**\n\n"
        "`/schedule` — Увидите список всех задач\n\n"
        "Каждая задача показывает:\n"
        "• ✅ или ❌ — включена или нет\n"
        "• Название (например: start_chain 1)\n"
        "• Когда выполнится в следующий раз\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**КАК УДАЛИТЬ ЗАДАЧУ?**\n\n"
        "1. Смотрите список: `/schedule`\n"
        "2. Запомните номер задачи (например: #3)\n"
        "3. Удалите: `/schedule remove 3`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ВАЖНО:**\n"
        "• Задачи работают КАЖДЫЙ ДЕНЬ в одно время\n"
        "• Если хотите разовую задачу — лучше запускать вручную\n"
        "• Планировщик работает только когда включён master.py",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'help_floodwait_explain'))
async def help_floodwait_explain_handler(event):
    """Подробное объяснение FloodWait"""
    await event.answer()
    
    buttons = [[Button.inline("◀️ Назад в помощь", "help_menu")]]
    
    await event.respond(
        "🛡️ **ЧТО ТАКОЕ FLOODWAIT?**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ПРОСТЫМИ СЛОВАМИ:**\n\n"
        "Telegram следит, чтобы боты не спамили.\n"
        "Если система вступает в группы слишком быстро,\n"
        "Telegram говорит: «СТОП! Подожди!»\n\n"
        "Это и есть FloodWait.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ЧТО ПРОИСХОДИТ?**\n\n"
        "1️⃣ Аккаунт пытается вступить в группу\n"
        "2️⃣ Telegram блокирует: «Подожди 60 секунд»\n"
        "3️⃣ Система ждёт эти 60 секунд\n"
        "4️⃣ Потом пробует снова\n\n"
        "**Ничего страшного!** Это нормальная работа.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**УРОВНИ FLOODWAIT:**\n\n"
        "✅ **0-5 раз в день** — ОТЛИЧНО!\n"
        "Система работает идеально\n\n"
        "⚠️ **6-10 раз в день** — НОРМАЛЬНО\n"
        "Это допустимо, не волнуйтесь\n\n"
        "🚨 **Больше 10 раз** — МНОГО\n"
        "Нужно замедлить систему\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ЧТО ДЕЛАТЬ ЕСЛИ МНОГО?**\n\n"
        "**НИЧЕГО!** Система сама:\n"
        "1. Увеличит задержки между действиями\n"
        "2. Замедлит работу\n"
        "3. Через 2-4 часа FloodWait исчезнут\n\n"
        "Вы увидите: **🛡️ АДАПТАЦИЯ: 1.5x**\n"
        "Это значит система защищает себя.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**КАК ПРОВЕРИТЬ FLOODWAIT?**\n\n"
        "Команда: `/floodwait`\n\n"
        "Или через кнопки:\n"
        "Главное меню → «🛡️ Защита от блокировок»\n\n"
        "Вы увидите:\n"
        "• Сколько блокировок у каждого аккаунта\n"
        "• Уровень риска (Low/Medium/High)\n"
        "• Рекомендации что делать\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ГЛАВНОЕ:**\n\n"
        "❌ НЕ ПАНИКУЙТЕ! FloodWait — это нормально\n"
        "✅ Система сама себя защитит\n"
        "✅ Telegram НЕ заблокирует ваши аккаунты\n"
        "✅ Это временная защитная мера\n\n"
        "Просто проверяйте `/floodwait` раз в день.",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'show_status'))
async def status_button_handler(event):
    """Показать статус цепочек через кнопку"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    status_text = "📊 **СТАТУС ЦЕПОЧЕК**\n\n"
    
    for chain_num in [1, 2, 3]:
        status = master_instance.chain_status[chain_num]
        status_text += f"🔗 **Цепочка {chain_num}:**\n"
        
        for stage, is_running in status.items():
            emoji = "🟢" if is_running else "⚪"
            status_text += f"   {emoji} {stage}\n"
        status_text += "\n"
    
    buttons = [[Button.inline("🔙 Главное меню", "main_menu")]]
    await event.respond(status_text, buttons=buttons)

@bot.on(events.NewMessage(pattern='/status'))
async def status_handler(event):
    """Показать статус цепочек (команда для совместимости)"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    status_text = "📊 **СТАТУС ЦЕПОЧЕК**\n\n"
    
    for chain_num in [1, 2, 3]:
        status = master_instance.chain_status[chain_num]
        status_text += f"🔗 **Цепочка {chain_num}:**\n"
        
        for stage, is_running in status.items():
            emoji = "🟢" if is_running else "⚪"
            status_text += f"   {emoji} {stage}\n"
        status_text += "\n"
    
    await event.respond(status_text)

@bot.on(events.NewMessage(pattern='/run'))
async def run_script_handler(event):
    """Запустить отдельный скрипт"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    try:
        message_text = event.message.text
        parts = message_text.split()
        
        if len(parts) != 3:
            await event.respond(
                "📋 **Запуск отдельного скрипта:**\n"
                "/run <номер_цепочки> <тип_скрипта>\n\n"
                "**Примеры:**\n"
                "/run 1 probiv - запуск только probiv1\n"
                "/run 2 filtr - запуск только filtr2\n"
                "/run 3 invocer - запуск только invocer3\n"
                "/run 1 deleter - запуск только deleter1\n\n"
                "**Доступные скрипты:** probiv, filtr, invocer, deleter"
            )
            return
        
        chain_num = int(parts[1])
        script_type = parts[2]
        
        if chain_num not in [1, 2, 3]:
            await event.respond("❌ Номер цепочки должен быть 1, 2 или 3")
            return
        
        if script_type not in ['probiv', 'filtr', 'invocer', 'deleter', 'unified']:
            await event.respond("❌ Неизвестный тип скрипта. Доступные: probiv, filtr, invocer, deleter, unified")
            return
        
        success = master_instance.start_single_script(chain_num, script_type)
        await event.respond(success)
            
    except ValueError:
        await event.respond("❌ Неверный формат номера цепочки")
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.NewMessage(pattern='/stop'))
async def stop_script_handler(event):
    """Остановить отдельный скрипт"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    try:
        message_text = event.message.text
        parts = message_text.split()
        
        if len(parts) == 1:
            # Если просто /stop - показываем помощь
            await event.respond(
                "📋 **Остановка скрипта:**\n"
                "/stop <номер_цепочки> <тип_скрипта>\n"
                "/stop_all - остановить всё\n\n"
                "**Примеры:**\n"
                "/stop 1 probiv - остановка probiv1\n"
                "/stop 2 filtr - остановка filtr2\n"
                "/stop 3 invocer - остановка invocer3\n\n"
                "**Доступные скрипты:** probiv, filtr, invocer, deleter"
            )
            return
        
        if len(parts) != 3:
            await event.respond("❌ Неверный формат. Используйте: /stop <цепочка> <скрипт>")
            return
        
        chain_num = int(parts[1])
        script_type = parts[2]
        
        if chain_num not in [1, 2, 3]:
            await event.respond("❌ Номер цепочки должен быть 1, 2 или 3")
            return
        
        if script_type not in ['probiv', 'filtr', 'invocer', 'deleter', 'unified']:
            await event.respond("❌ Неизвестный тип скрипта. Доступные: probiv, filtr, invocer, deleter, unified")
            return
        
        success = master_instance.stop_single_script(chain_num, script_type)
        await event.respond(success)
            
    except ValueError:
        await event.respond("❌ Неверный формат номера цепочки")
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.NewMessage(pattern='/start_chain'))
async def start_chain_handler(event):
    """Запустить цепочку полностью или с определенного этапа"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    try:
        message_text = event.message.text
        parts = message_text.split()
        
        if len(parts) < 2:
            await event.respond(
                "📋 **Использование:**\n"
                "/start_chain <номер_цепочки> [этап]\n\n"
                "**Примеры:**\n"
                "/start_chain 1 - запуск цепочки 1 с начала\n"
                "/start_chain 2 filtr - запуск цепочки 2 с фильтра\n"
                "/start_chain 3 invocer - запуск цепочки 3 с инвосера\n\n"
                "**Доступные этапы:** probiv, filtr, invocer, deleter"
            )
            return
        
        chain_num = int(parts[1])
        start_stage = parts[2] if len(parts) > 2 else 'probiv'
        
        if chain_num not in [1, 2, 3]:
            await event.respond("❌ Номер цепочки должен быть 1, 2 или 3")
            return
        
        if start_stage not in ['probiv', 'filtr', 'invocer', 'deleter']:
            await event.respond("❌ Неизвестный этап. Доступные: probiv, filtr, invocer, deleter")
            return
        
        # Запускаем цепочку с нужного этапа
        result = master_instance.start_chain_from_stage(chain_num, start_stage)
        await event.respond(result)
            
    except ValueError:
        await event.respond("❌ Неверный формат номера цепочки")
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.NewMessage(pattern='/probiv'))
async def probiv_handler(event):
    """Показать статистику probiv"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    stats_text = "📈 **СТАТИСТИКА PROBIV**\n\n"
    
    for chain_num in [1, 2, 3]:
        stats = master_instance.probiv_stats[chain_num]
        stats_text += f"**Probiv {chain_num}:**\n"
        stats_text += f"✅ Успешно: {stats['success']}/{stats['target']}\n"
        stats_text += f"❌ Серия неудач: {stats['fail_streak']}\n"
        stats_text += f"📊 Всего: {stats['total']}\n\n"
    
    await event.respond(stats_text)

@bot.on(events.CallbackQuery(pattern=b'stop_all_confirm'))
async def stop_all_confirm_handler(event):
    """Подтверждение остановки всех цепочек"""
    await event.answer()
    
    buttons = [
        [Button.inline("✅ Да, остановить ВСЁ", "stop_all_execute")],
        [Button.inline("❌ Отмена", "main_menu")]
    ]
    
    await event.respond(
        "⚠️ **ПОДТВЕРЖДЕНИЕ ОСТАНОВКИ**\n\n"
        "Вы уверены, что хотите остановить ВСЕ процессы?\n\n"
        "Будут остановлены:\n"
        "• Все 3 цепочки (Probiv, Filtr, Invocer, Deleter)\n"
        "• Все 3 Unified аккаунта\n\n"
        "Мастер-бот продолжит работу для управления.",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'stop_all_execute'))
async def stop_all_execute_handler(event):
    """Выполнить остановку всех цепочек"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    await event.respond("🛑 Останавливаю все цепочки...")
    master_instance.stop_all_scripts()
    
    buttons = [[Button.inline("🏠 Главное меню", "main_menu")]]
    await event.respond(
        "✅ **ВСЁ ОСТАНОВЛЕНО**\n\n"
        "Все процессы остановлены.\n"
        "Мастер продолжает работу для управления ботом.",
        buttons=buttons
    )

@bot.on(events.NewMessage(pattern='/stop_all'))
async def stop_handler(event):
    """Остановить все цепочки (команда для совместимости)"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    await event.respond("🛑 Останавливаю все цепочки...")
    master_instance.stop_all_scripts()
    await event.respond("✅ Все цепочки остановлены. Мастер продолжает работу для управления ботом.")

@bot.on(events.NewMessage(pattern='/restart_monitoring'))
async def restart_monitoring_handler(event):
    """Перезапустить систему мониторинга триггеров"""
    if master_instance is None:
        await event.respond("❌ Master не запущен")
        return
    
    result = master_instance.restart_monitoring()
    await event.respond(result)

@bot.on(events.NewMessage(pattern='/unified'))
async def unified_handler(event):
    """Команда для управления unified скриптами"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    # Разбираем команду /unified <chain_num> <action>
    try:
        parts = event.message.message.split()
        if len(parts) < 3:
            await event.respond(
                "🔗 **UNIFIED СКРИПТЫ** - Доступно для всех цепочек\n\n"
                "Использование:\n"
                "`/unified <цепочка> <действие>`\n\n"
                "Цепочка: 1, 2, 3 (все поддерживают unified)\n"
                "Действие: start, stop, status\n\n"
                "Примеры:\n"
                "`/unified 1 start` - запуск unified_chain1\n"
                "`/unified 2 start` - запуск unified_chain2\n"
                "`/unified 3 start` - запуск unified_chain3\n\n"
                "💡 Unified = invocer + deleter в одном скрипте\n"
                "🔄 Батчевая обработка + умное удаление\n"
                "📊 Детальное логирование (5 типов логов)"
            )
            return
        
        chain_num = int(parts[1])
        action = parts[2].lower()
        
        if chain_num not in [1, 2, 3]:
            await event.respond("❌ Номер цепочки должен быть 1, 2 или 3")
            return
        
        # Проверяем, есть ли unified скрипт для этой цепочки
        if 'unified' not in CHAINS.get(chain_num, {}):
            await event.respond(f"❌ Unified скрипт для цепочки {chain_num} не найден")
            return
        
        if action == 'start':
            if master_instance:
                result = master_instance.trigger_next_stage(chain_num, 'unified')
                await event.respond(f"🚀 Запуск unified_chain{chain_num}...")
            else:
                await event.respond("❌ Master не запущен")
        
        elif action == 'stop':
            # Остановка unified скрипта
            script_name = f"unified{chain_num}"
            if script_name in running_processes:
                result = stop_script(script_name)
                await event.respond(result)
            else:
                await event.respond(f"❌ unified{chain_num} не запущен")
        
        elif action == 'status':
            # Статус unified скрипта
            script_name = f"unified{chain_num}"
            if script_name in running_processes:
                info = running_processes[script_name]
                runtime = datetime.now() - info['start_time']
                hours, remainder = divmod(int(runtime.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                await event.respond(
                    f"🟢 **unified{chain_num}** работает\n"
                    f"⏱️ Время: {hours:02d}:{minutes:02d}:{seconds:02d}\n"
                    f"📁 Логи: `unified\\logs\\`"
                )
            else:
                await event.respond(f"🔴 unified{chain_num} остановлен")
        
        else:
            await event.respond("❌ Неизвестное действие. Используйте: start, stop, status")
            
    except (ValueError, IndexError):
        await event.respond("❌ Ошибка в формате команды. Используйте: `/unified <цепочка> <действие>`")

@bot.on(events.NewMessage(pattern='/master'))
async def master_control_handler(event):
    """Команда для управления мастер-системой"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    global master_system_enabled, master_instance
    
    # Разбираем команду /master [action]
    try:
        parts = event.message.message.split()
        if len(parts) < 2:
            # Показываем статус и доступные действия
            status = "🟢 **ВКЛЮЧЕНА**" if master_system_enabled else "🔴 **ВЫКЛЮЧЕНА**"
            
            await event.respond(
                "🛡️ **УПРАВЛЕНИЕ МАСТЕР-СИСТЕМОЙ**\n\n"
                f"📊 **Текущий статус**: {status}\n\n"
                "**Доступные команды:**\n"
                "`/master on` - включить мастер-систему\n"
                "`/master off` - выключить мастер-систему\n"
                "`/master status` - показать статус\n"
                "`/master info` - подробная информация\n\n"
                "**Что контролирует мастер-система:**\n"
                "• 🔄 Автоматические триггеры\n"
                "• 📊 Мониторинг логов\n"
                "• 🚀 Автозапуск цепочек\n"
                "• 📱 Уведомления о событиях"
            )
            return
        
        action = parts[1].lower()
        
        if action in ['on', 'enable', 'start', '1']:
            if not master_system_enabled:
                master_system_enabled = True
                if master_instance:
                    master_instance.monitoring = True
                    master_instance.restart_monitoring()
                
                await event.respond(
                    "🟢 **МАСТЕР-СИСТЕМА ВКЛЮЧЕНА**\n\n"
                    "✅ Автоматические триггеры активированы\n"
                    "✅ Мониторинг логов запущен\n"
                    "✅ Автозапуск цепочек включен\n\n"
                    "📋 Активные триггеры:\n"
                    "• probiv завершен → запуск filtr\n"
                    "• filtr отправил ссылки → запуск unified\n"
                    "• invocer достиг лимита → запуск deleter"
                )
            else:
                await event.respond("ℹ️ Мастер-система уже включена")
        
        elif action in ['off', 'disable', 'stop', '0']:
            if master_system_enabled:
                master_system_enabled = False
                if master_instance:
                    master_instance.monitoring = False
                
                await event.respond(
                    "🔴 **МАСТЕР-СИСТЕМА ВЫКЛЮЧЕНА**\n\n"
                    "❌ Автоматические триггеры отключены\n"
                    "❌ Мониторинг логов остановлен\n"
                    "❌ Автозапуск цепочек выключен\n\n"
                    "⚠️ Ручное управление через бот остается доступным"
                )
            else:
                await event.respond("ℹ️ Мастер-система уже выключена")
        
        elif action in ['status', 'info', 'check']:
            status = "🟢 **ВКЛЮЧЕНА**" if master_system_enabled else "🔴 **ВЫКЛЮЧЕНА**"
            
            # Подсчитываем активные процессы
            active_processes = len(running_processes) if 'running_processes' in globals() else 0
            monitoring_threads = 0
            if master_instance:
                monitoring_threads = len(master_instance.log_files)
            
            await event.respond(
                "🛡️ **СТАТУС МАСТЕР-СИСТЕМЫ**\n\n"
                f"📊 **Состояние**: {status}\n"
                f"⚙️ **Активных процессов**: {active_processes}\n"
                f"🔍 **Мониторинг потоков**: {monitoring_threads}\n\n"
                "**Возможности системы:**\n"
                "• 🔄 Автоматические триггеры между этапами\n"
                "• 📊 Real-time мониторинг логов\n"
                "• 🚀 Автозапуск следующих этапов\n"
                "• 📱 Telegram уведомления\n"
                "• 🛡️ Обработка ошибок и перезапуск\n\n"
                f"⏰ **Последнее обновление**: {datetime.now().strftime('%H:%M:%S')}"
            )
        
        else:
            await event.respond(
                "❌ Неизвестное действие\n\n"
                "Используйте: `/master on/off/status/info`"
            )
            
    except (ValueError, IndexError):
        await event.respond("❌ Ошибка в команде. Используйте: `/master [on/off/status]`")

@bot.on(events.NewMessage(pattern='/database'))
async def database_handler(event):
    """Показать статистику общей базы чатов"""
    try:
        from filtr_global_stats import (
            get_total_processed, 
            get_stats_by_chain,
            DB_PATH
        )
        import sqlite3
        from datetime import datetime
        
        # Проверяем существование базы
        if not DB_PATH.exists():
            await event.respond("⚠️ База данных еще не создана\nФильтры еще не запускались")
            return
        
        # Получаем статистику
        total = get_total_processed()
        chain1 = get_stats_by_chain(1)
        chain2 = get_stats_by_chain(2)
        chain3 = get_stats_by_chain(3)
        
        # Получаем последние 5 чатов
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT link, chain_num, processed_at 
            FROM processed_chats 
            ORDER BY processed_at DESC 
            LIMIT 5
        ''')
        recent_chats = cursor.fetchall()
        conn.close()
        
        # Формируем отчет
        report = (
            f"🗄️ **БАЗА ОБРАБОТАННЫХ ЧАТОВ**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ **Всего уникальных:** {total} чатов\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **По цепочкам:**\n"
            f"   🔗 Цепочка 1: {chain1} чатов\n"
            f"   🔗 Цепочка 2: {chain2} чатов\n"
            f"   🔗 Цепочка 3: {chain3} чатов\n"
        )
        
        if recent_chats:
            report += f"\n📋 **Последние 5 обработанных:**\n"
            for i, (link, chain_num, processed_at) in enumerate(recent_chats, 1):
                # Обрезаем длинные ссылки
                short_link = link if len(link) <= 35 else link[:32] + "..."
                # Парсим дату
                try:
                    dt = datetime.strptime(processed_at, "%Y-%m-%d %H:%M:%S")
                    time_str = dt.strftime("%d.%m %H:%M")
                except:
                    time_str = processed_at[:16]
                
                report += f"{i}. `{short_link}`\n   └ Цепь {chain_num} | {time_str}\n"
        
        report += f"\n💡 Используйте `/database` для обновления статистики"
        
        await event.respond(report)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка получения данных:\n`{str(e)}`")

@bot.on(events.NewMessage(pattern='/logs'))
async def logs_handler(event):
    """Просмотр логов через бота"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    try:
        parts = event.message.message.split()
        
        if len(parts) < 2:
            log_paths = get_log_paths()
            buttons = []
            
            # Группируем кнопки по типам
            buttons.append([Button.inline("📋 Master", "view_log_master")])
            buttons.append([
                Button.inline("📊 Probiv 1", "view_log_probiv1"),
                Button.inline("📊 Probiv 2", "view_log_probiv2"),
                Button.inline("📊 Probiv 3", "view_log_probiv3")
            ])
            buttons.append([
                Button.inline("🔍 Filtr 1", "view_log_filtr1"),
                Button.inline("🔍 Filtr 2", "view_log_filtr2"),
                Button.inline("🔍 Filtr 3", "view_log_filtr3")
            ])
            buttons.append([
                Button.inline("🔄 Unified 1", "view_log_unified1"),
                Button.inline("🔄 Unified 2", "view_log_unified2"),
                Button.inline("🔄 Unified 3", "view_log_unified3")
            ])
            
            await event.respond(
                "📋 **ПРОСМОТР ЛОГОВ**\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "**ЧТО ТАКОЕ ЛОГИ?**\n\n"
                "Это файлы, куда записывается вся работа системы:\n"
                "• Какие группы обрабатываются\n"
                "• Есть ли ошибки или блокировки\n"
                "• Когда аккаунт вступил/покинул группу\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "**КАК ЧИТАТЬ ЛОГИ?**\n\n"
                "🟢 **[SUCCESS]** — Всё хорошо!\n"
                "⚠️ **[WARNING]** — Предупреждение\n"
                "❌ **[ERROR]** — Ошибка (но не критичная)\n"
                "🛑 **[CRITICAL]** — Серьёзная проблема!\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "**БЫСТРЫЙ ПРОСМОТР:**\n"
                "Нажмите кнопку ниже — увидите последние 20 строк\n\n"
                "**ИЛИ ИСПОЛЬЗУЙТЕ КОМАНДЫ:**\n\n"
                "`/logs unified1` → Последние 20 строк\n"
                "`/logs unified1 50` → Последние 50 строк\n"
                "`/logs unified1 search FloodWait` → Поиск слова\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "**КАКОЙ ЛОГ СМОТРЕТЬ?**\n\n"
                "**Master** — Общая работа системы\n"
                "**Probiv 1/2/3** — Поиск групп\n"
                "**Filtr 1/2/3** — Фильтрация групп\n"
                "**Unified 1/2/3** — Работа каждого аккаунта\n\n"
                "💡 Если что-то не работает — смотрите Unified!",
                buttons=buttons
            )
            return
        
        log_name = parts[1].lower()
        lines = 20
        search_term = None
        
        # Парсим дополнительные параметры
        if len(parts) > 2:
            if parts[2].lower() == 'search' and len(parts) > 3:
                search_term = ' '.join(parts[3:])
            else:
                try:
                    lines = int(parts[2])
                    lines = min(lines, 100)  # Максимум 100 строк
                except:
                    pass
        
        log_paths = get_log_paths()
        if log_name not in log_paths:
            await event.respond(f"❌ Неизвестный лог: {log_name}\nИспользуйте `/logs` для списка")
            return
        
        log_path = log_paths[log_name]
        log_lines = read_log_file(log_path, lines, search_term)
        
        if search_term:
            header = f"🔍 **ЛОГ: {log_name.upper()}** (поиск: {search_term})\n\n"
        else:
            header = f"📋 **ЛОГ: {log_name.upper()}** (последние {len(log_lines)} строк)\n\n"
        
        log_text = header + "```\n" + ''.join(log_lines) + "```"
        
        # Разбиваем на части если слишком длинное
        if len(log_text) > 4000:
            await event.respond(header + "```\n" + ''.join(log_lines[:30]) + "\n...\n(слишком много строк, показаны первые 30)\n```")
        else:
            await event.respond(log_text)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.CallbackQuery(pattern=b'view_log_(.+)'))
async def view_log_callback(event):
    """Обработчик кнопок просмотра логов"""
    await event.answer()
    
    log_name = event.pattern_match.group(1).decode()
    log_paths = get_log_paths()
    
    if log_name not in log_paths:
        await event.respond(f"❌ Лог не найден: {log_name}")
        return
    
    log_path = log_paths[log_name]
    log_lines = read_log_file(log_path, 20)
    
    log_text = f"📋 **{log_name.upper()}** (последние 20 строк)\n\n```\n" + ''.join(log_lines) + "```"
    
    if len(log_text) > 4000:
        log_text = f"📋 **{log_name.upper()}**\n\n```\n" + ''.join(log_lines[:15]) + "\n...\n```"
    
    buttons = [[Button.inline("🔙 Назад к списку", "logs_menu")]]
    await event.edit(log_text, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'logs_menu'))
async def logs_menu_callback(event):
    """Возврат к меню логов"""
    await event.answer()
    
    buttons = []
    buttons.append([Button.inline("📋 Master", "view_log_master")])
    buttons.append([
        Button.inline("📊 Probiv 1", "view_log_probiv1"),
        Button.inline("📊 Probiv 2", "view_log_probiv2"),
        Button.inline("📊 Probiv 3", "view_log_probiv3")
    ])
    buttons.append([
        Button.inline("🔍 Filtr 1", "view_log_filtr1"),
        Button.inline("🔍 Filtr 2", "view_log_filtr2"),
        Button.inline("🔍 Filtr 3", "view_log_filtr3")
    ])
    buttons.append([
        Button.inline("🔄 Unified 1", "view_log_unified1"),
        Button.inline("🔄 Unified 2", "view_log_unified2"),
        Button.inline("🔄 Unified 3", "view_log_unified3")
    ])
    buttons.append([Button.inline("🏠 Главное меню", "main_menu")])
    
    await event.edit(
        "📋 **ПРОСМОТР ЛОГОВ**\n\n"
        "Выберите лог для просмотра последних 20 строк.",
        buttons=buttons
    )

@bot.on(events.NewMessage(pattern='/schedule'))
async def schedule_handler(event):
    """Управление планировщиком задач"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    global scheduler_thread
    
    try:
        parts = event.message.message.split()
        
        if len(parts) < 2:
            # Показать список задач
            if not task_scheduler.tasks:
                await event.respond(
                    "⏰ **ПЛАНИРОВЩИК ЗАДАЧ**\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "📋 **У вас пока нет задач**\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "**ЧТО ТАКОЕ ПЛАНИРОВЩИК?**\n\n"
                    "Это автоматический запуск/остановка аккаунтов\n"
                    "в заданное время КАЖДЫЙ ДЕНЬ.\n\n"
                    "Например:\n"
                    "• В 9:00 запускать все аккаунты\n"
                    "• В 23:00 останавливать их\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "**КАК ДОБАВИТЬ ЗАДАЧУ?**\n\n"
                    "**ПРИМЕР 1:** Запуск в 9 утра\n"
                    "`/schedule add 09:00 start_chain 1`\n\n"
                    "**ПРИМЕР 2:** Остановка в 11 вечера\n"
                    "`/schedule add 23:00 stop_script 1 unified`\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "**ДРУГИЕ КОМАНДЫ:**\n\n"
                    "`/schedule` → Список всех задач\n"
                    "`/schedule remove 3` → Удалить задачу #3\n"
                    "`/schedule disable 1` → Выключить задачу #1\n"
                    "`/schedule enable 1` → Включить задачу #1\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "💡 **СОВЕТ:**\n"
                    "Лучше добавлять задержки между запусками!\n"
                    "Например: Chain1 в 9:00, Chain2 в 9:10, Chain3 в 9:20\n\n"
                    "📖 Подробнее: Главное меню → Помощь → Автозапуск"
                )
            else:
                tasks_text = "⏰ **ПЛАНИРОВЩИК ЗАДАЧ**\n\n"
                for task in task_scheduler.tasks:
                    status = "✅" if task['enabled'] else "❌"
                    next_run = task['next_run'].strftime('%d.%m %H:%M') if task['next_run'] else "---"
                    last_run = task['last_run'].strftime('%d.%m %H:%M') if task['last_run'] else "Не было"
                    
                    tasks_text += (
                        f"{status} **#{task['id']}**: {task['name']}\n"
                        f"   📅 След. запуск: {next_run}\n"
                        f"   🕐 Посл. запуск: {last_run}\n"
                        f"   🔄 Повтор: {'Да' if task['repeat'] else 'Нет'}\n\n"
                    )
                
                await event.respond(tasks_text)
            return
        
        action = parts[1].lower()
        
        if action == 'add' and len(parts) >= 4:
            # /schedule add HH:MM action chain [script]
            time_str = parts[2]
            task_action = parts[3]
            chain_num = int(parts[4]) if len(parts) > 4 else None
            script_type = parts[5] if len(parts) > 5 else None
            
            task_id = task_scheduler.add_task(
                name=f"{task_action} chain{chain_num}" if chain_num else task_action,
                action=task_action,
                schedule_time=time_str,
                chain_num=chain_num,
                script_type=script_type,
                repeat=True
            )
            
            await event.respond(f"✅ Задача #{task_id} добавлена: {time_str} - {task_action}")
        
        elif action == 'remove' and len(parts) >= 3:
            task_id = int(parts[2])
            task_scheduler.remove_task(task_id)
            await event.respond(f"✅ Задача #{task_id} удалена")
        
        elif action == 'enable' and len(parts) >= 3:
            task_id = int(parts[2])
            if task_scheduler.enable_task(task_id):
                await event.respond(f"✅ Задача #{task_id} включена")
            else:
                await event.respond(f"❌ Задача #{task_id} не найдена")
        
        elif action == 'disable' and len(parts) >= 3:
            task_id = int(parts[2])
            if task_scheduler.disable_task(task_id):
                await event.respond(f"✅ Задача #{task_id} выключена")
            else:
                await event.respond(f"❌ Задача #{task_id} не найдена")
        
        elif action == 'list':
            # Показать список (дублирование логики выше)
            if not task_scheduler.tasks:
                await event.respond("📋 Задач нет")
            else:
                tasks_text = "⏰ **СПИСОК ЗАДАЧ**\n\n"
                for task in task_scheduler.tasks:
                    status = "✅" if task['enabled'] else "❌"
                    next_run = task['next_run'].strftime('%d.%m %H:%M') if task['next_run'] else "---"
                    tasks_text += f"{status} #{task['id']}: {task['name']} → {next_run}\n"
                await event.respond(tasks_text)
        
        else:
            await event.respond("❌ Неизвестная команда. Используйте `/schedule` для справки")
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.NewMessage(pattern='/dbstats'))
async def dbstats_handler(event):
    """Расширенная статистика по базе данных"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    try:
        stats = get_database_stats()
        
        if stats is None:
            await event.respond("⚠️ База данных еще не создана")
            return
        
        if 'error' in stats:
            await event.respond(f"❌ Ошибка: {stats['error']}")
            return
        
        report = (
            f"📊 **РАСШИРЕННАЯ СТАТИСТИКА БД**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**ЧТО ЭТО?**\n\n"
            f"База данных хранит все группы, в которые\n"
            f"вступили ваши аккаунты. Здесь вы видите:\n"
            f"• Сколько всего групп обработано\n"
            f"• Какой аккаунт больше всех работал\n"
            f"• Какие темы групп чаще встречаются\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📈 **ОБЩАЯ СТАТИСТИКА:**\n\n"
            f"✅ **Всего чатов:** {stats['total']}\n"
            f"📅 **За 24 часа:** {stats['last_24h']}\n"
            f"📅 **За 7 дней:** {stats['last_7d']}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔗 **РАБОТА АККАУНТОВ:**\n\n"
        )
        
        for chain, count in sorted(stats['by_chain'].items()):
            percent = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            report += f"   🔗 Chain {chain}: {count} ({percent:.1f}%)\n"
        
        if stats['by_category']:
            report += f"\n**По категориям:**\n"
            for category, count in list(stats['by_category'].items())[:5]:
                report += f"   📁 {category}: {count}\n"
        
        if stats['top_keywords']:
            report += f"\n**Топ ключевых слов:**\n"
            for keyword, count in stats['top_keywords'][:5]:
                report += f"   🔑 {keyword}: {count}\n"
        
        await event.respond(report)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.NewMessage(pattern='/floodwait'))
async def floodwait_handler(event):
    """Анализ FloodWait ошибок"""
    if event.sender_id not in AUTHORIZED_USERS:
        return
    
    try:
        results = analyze_floodwait_logs()
        recommendations = get_floodwait_recommendations(results)
        
        report = (
            f"🛡️ **АНАЛИЗ FLOODWAIT**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**ЧТО ТАКОЕ FLOODWAIT?**\n\n"
            f"Это когда Telegram блокирует аккаунт на время,\n"
            f"потому что он слишком быстро вступает в группы.\n"
            f"Это НЕ бан! Это временная защитная мера.\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**УРОВНИ РИСКА:**\n\n"
            f"✅ LOW (0-5 раз) — Всё отлично!\n"
            f"⚠️ MEDIUM (6-10 раз) — Нормально\n"
            f"🚨 HIGH (>10 раз) — Много, но система адаптируется\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**СТАТИСТИКА ПО АККАУНТАМ:**\n\n"
        )
        
        for chain_num, data in results.items():
            if 'error' in data:
                report += f"⚠️ **Chain {chain_num}**: Ошибка анализа\n\n"
                continue
            
            level_emoji = {
                'low': '✅',
                'medium': '⚠️',
                'high': '🚨'
            }.get(data.get('level', 'low'), '❓')
            
            report += (
                f"{level_emoji} **Chain {chain_num}**\n"
                f"   📊 Количество: {data['count']}\n"
                f"   ⏱️ Общее время: {data['total_wait']}с ({data['total_wait']//60}м)\n"
            )
            
            if data['warnings']:
                report += f"   ⚠️ Предупреждения: {len(data['warnings'])}\n"
            
            report += "\n"
        
        report += "**💡 Рекомендации:**\n\n"
        report += "\n\n".join(recommendations)
        
        # Разбиваем на части если слишком длинное
        if len(report) > 4000:
            parts = [report[i:i+3900] for i in range(0, len(report), 3900)]
            for part in parts:
                await event.respond(part)
        else:
            await event.respond(report)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка анализа: {str(e)}")

async def send_bot_notification(message):
    """Отправка уведомлений админу"""
    if admin_chat_id and bot.is_connected():
        try:
            await bot.send_message(admin_chat_id, message)
        except:
            pass

# === ОБРАБОТЧИКИ ИНТЕРАКТИВНЫХ КНОПОК ===

@bot.on(events.CallbackQuery(pattern=b'status_all'))
async def status_all_handler(event):
    """Статус всех цепочек"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond(master_instance.get_full_status())

@bot.on(events.CallbackQuery(pattern=b'chains_menu'))
async def chains_menu_handler(event):
    """Меню управления цепочками"""
    await event.answer()
    
    buttons = [
        [Button.inline("🚀 ЗАПУСТИТЬ ВСЕ ПРОБИВЫ (1+2+3)", "start_all_probiv")],
        [Button.inline("🔗 Цепочка 1", "chain_1"), Button.inline("🔗 Цепочка 2", "chain_2"), Button.inline("🔗 Цепочка 3", "chain_3")],
        [Button.inline("🔙 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "**🔧 НАСТРОЙКИ СКРИПТОВ**\n\n"
        "Здесь можно управлять каждым скриптом отдельно.\n\n"
        "💡 Для новичков рекомендуем использовать кнопку:\n"
        "⚙️ Запустить аккаунты (в главном меню)\n\n"
        "Выберите:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'chain_(.+)'))
async def chain_handler(event):
    """Управление конкретной цепочкой"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Определяем состав цепочки и статусы
    chain_parts = []
    if chain_num in [1, 2, 3]:
        chain_parts = ["probiv", "filtr", "invocer", "deleter"]
    elif chain_num == 4:
        chain_parts = ["invocer", "deleter"]
    
    # Собираем информацию о статусах скриптов
    status_info = "📊 **СТАТУС СКРИПТОВ:**\n"
    for script_type in chain_parts:
        if script_type in master_instance.chain_status.get(chain_num, {}):
            is_running = master_instance.chain_status[chain_num][script_type]
            emoji = "🟢" if is_running else "🔴"
            status_info += f"{emoji} {script_type.upper()}\n"
    
    buttons = [
        [Button.inline("▶️ Запустить всю цепочку", f"start_chain_{chain_num}_probiv")],
        [Button.inline("� Остановить всю цепочку", f"stop_chain_{chain_num}")],
        [Button.inline("━━━━━━━━━━━━━━━━━━", "none")],
    ]
    
    # Добавляем кнопки управления отдельными скриптами
    if chain_num in [1, 2, 3]:
        buttons.extend([
            [Button.inline("⚙️ Probiv управление", f"manage_script_probiv{chain_num}")],
            [Button.inline("⚙️ Filtr управление", f"manage_script_filtr{chain_num}")],
        ])
    
    # Invocer и Deleter есть у всех цепочек
    buttons.extend([
        [Button.inline("⚙️ Invocer управление", f"manage_script_invocer{chain_num}")],
        [Button.inline("⚙️ Deleter управление", f"manage_script_deleter{chain_num}")],
        [Button.inline("━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline("📊 Статус цепочки", f"status_chain_{chain_num}")],
        [Button.inline("🔙 Назад", "chains_menu")]
    ])
    
    await event.respond(
        f"**🔗 ЦЕПОЧКА {chain_num}**\n\n"
        f"{status_info}\n\n"
        f"Управление:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'start_all_probiv'))
async def start_all_probiv_handler(event):
    """Запуск всех probiv скриптов одновременно (БЕЗ автоцепочки)"""
    print("🔍 DEBUG: start_all_probiv_handler вызван!")
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.start_probiv_scripts_manual()
    await event.respond(
        f"🚀 **ЗАПУСК ТОЛЬКО PROBIV**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{result}\n\n"
        f"ℹ️ Запущены только probiv скрипты\n"
        f"⚠️ Автоматическая цепочка НЕ включена\n"
        f"💡 Для запуска с автоцепочкой используйте:\n"
        f"   'Управление цепочками' → 'Запустить с PROBIV'"
    )

@bot.on(events.CallbackQuery(pattern=b'start_all_unified'))
async def start_all_unified_handler(event):
    """Запуск всех unified скриптов поочередно с рандомными паузами (БЕЗ автоцепочки)"""
    print("🔍 DEBUG: start_all_unified_handler вызван!")
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Отправляем предупреждение о начале запуска
    await event.respond(
        "🚀 **ЗАПУСК ВСЕХ UNIFIED АККАУНТОВ**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ Запускаю аккаунты ПООЧЕРЕДНО:\n\n"
        "1️⃣ Unified1 → через 1-3 мин\n"
        "2️⃣ Unified2 → через 5-12 мин\n"
        "3️⃣ Unified3 → через 16-29 мин\n\n"
        "💡 Максимальное время: ~29 минут\n"
        "🤖 Прогрессивные паузы (имитация раздумий)\n"
        "📱 Уведомлю когда все запустятся!\n\n"
        "✅ Бот продолжает работать — можете использовать другие кнопки"
    )
    
    # Запускаем в отдельном потоке, чтобы не блокировать бота
    import threading
    
    def run_unified_scripts():
        result = master_instance.start_unified_scripts_manual()
        # Отправляем финальное уведомление через asyncio
        import asyncio
        asyncio.run_coroutine_threadsafe(
            event.respond(
                f"✅ **ВСЕ UNIFIED ЗАПУЩЕНЫ!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{result}\n\n"
                f"🤖 **Имитация реального человека:**\n"
                f"• Прогрессивные паузы: 1-3 мин → 4-9 мин → 11-17 мин\n"
                f"• Рандомные паузы внутри каждого скрипта\n"
                f"• Разная скорость работы аккаунтов\n"
                f"• Каждый unified работает независимо\n\n"
                f"⚠️ Автоматическая цепочка НЕ включена\n"
                f"📊 Проверить статус: Главное меню → Статистика"
            ),
            bot.loop
        )
    
    thread = threading.Thread(target=run_unified_scripts, daemon=True)
    thread.start()

@bot.on(events.CallbackQuery(pattern=b'start_chain_(.+)_(.+)'))
async def start_chain_callback_handler(event):
    """Запуск цепочки с указанного звена"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    start_from = event.pattern_match.group(2).decode()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.start_chain_from_stage(chain_num, start_from)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_chain_(.+)'))
async def stop_chain_handler(event):
    """Остановка цепочки"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.stop_chain_scripts(chain_num)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'status_chain_(.+)'))
async def status_chain_handler(event):
    """Статус конкретной цепочки"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    status = master_instance.get_chain_status(chain_num)
    await event.respond(status)

@bot.on(events.CallbackQuery(pattern=b'scripts_menu'))
async def scripts_menu_handler(event):
    """Меню отдельных скриптов"""
    await event.answer()  # Подтверждаем получение события
    
    buttons = []
    
    # Группируем по типам скриптов (добавляем unified)
    script_types = ["probiv", "filtr", "unified", "invocer", "deleter"]
    for script_type in script_types:
        buttons.append([Button.inline(f"📄 {script_type.title()} скрипты", f"script_type_{script_type}")])
    
    buttons.append([Button.inline("🔙 Главное меню", "main_menu")])
    
    await event.respond(
        "⚙️ **ОТДЕЛЬНЫЕ СКРИПТЫ**\n\n"
        "Выберите тип скрипта:\n"
        "🆕 **UNIFIED** - объединенные invocer+deleter",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'unified_menu'))
async def unified_menu_handler(event):
    """Меню объединенных скриптов"""
    print("🔍 DEBUG: unified_menu_handler вызван!")
    await event.answer()  # Подтверждаем получение события
    
    buttons = [
        [Button.inline("🚀 ЗАПУСТИТЬ ВСЕ UNIFIED (1+2+3)", "start_all_unified")],
        [Button.inline("� Запустить CHAIN1 (Аккаунт 1)", "unified_chain_1")],
        [Button.inline("� Запустить CHAIN2 (Аккаунт 2)", "unified_chain_2")],
        [Button.inline("� Запустить CHAIN3 (Аккаунт 3)", "unified_chain_3")],
        [Button.inline("📊 Посмотреть статус", "unified_status_all")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "🔄 **УПРАВЛЕНИЕ 3 АККАУНТАМИ (UNIFIED)**\n\n"
        "Здесь вы управляете 3 Telegram аккаунтами.\n"
        "Каждый аккаунт работает независимо:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**👤 CHAIN1** — Аккаунт 1 (Умеренный)\n"
        "Скорость: 10-15 групп/час\n\n"
        "**👤 CHAIN2** — Аккаунт 2 (Осторожный)\n"
        "Скорость: 6-10 групп/час\n\n"
        "**👤 CHAIN3** — Аккаунт 3 (Очень осторожный)\n"
        "Скорость: 4-6 групп/час\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📋 ЧТО ДЕЛАТЬ:**\n\n"
        "**🚀 Быстрый запуск (рекомендуется):**\n"
        "Нажмите «🚀 ЗАПУСТИТЬ ВСЕ UNIFIED (1+2+3)»\n"
        "Аккаунты запустятся ПООЧЕРЕДНО:\n"
        "🤖 1-3 мин → 4-9 мин → 11-17 мин (прогрессивно)\n"
        "⏱️ Займет ~16-29 минут\n\n"
        "**⚙️ Ручной запуск (для опытных):**\n"
        "Запускайте каждый аккаунт отдельно\n"
        "для точного контроля\n\n"
        "Выберите действие:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'script_type_(.+)'))
async def script_type_handler(event):
    """Управление скриптами определенного типа"""
    await event.answer()  # Подтверждаем получение события
    
    script_type = event.pattern_match.group(1).decode()
    
    buttons = []
    for chain_num in [1, 2, 3, 4]:
        # Проверяем, есть ли этот тип скрипта в цепочке
        if chain_num in CHAINS and script_type in CHAINS[chain_num]:
            if script_type == 'unified':
                # Для unified используем специальный формат
                script_name = f"unified_chain{chain_num}"
                buttons.append([Button.inline(f"⚙️ {script_name}", f"script_{script_name}")])
            else:
                script_name = f"{script_type}{chain_num}"
                buttons.append([Button.inline(f"⚙️ {script_name}", f"script_{script_name}")])
    
    buttons.append([Button.inline("🔙 Назад", "scripts_menu")])
    
    await event.respond(
        f"📄 **{script_type.upper()} СКРИПТЫ**\n\n"
        "Выберите конкретный скрипт:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'unified_chain_(.+)'))
async def unified_chain_handler(event):
    """Управление конкретным unified скриптом"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Проверяем статус unified скрипта (двойная проверка для надежности)
    is_running = any(
        info.get('type') == 'unified' and info.get('chain') == chain_num 
        for info in master_instance.active_processes.values()
    )
    
    # Дополнительно проверяем через chain_status
    if not is_running and chain_num in master_instance.chain_status:
        is_running = master_instance.chain_status[chain_num].get('unified', False)
    
    status_emoji = "🟢" if is_running else "🔴"
    status_text = "работает" if is_running else "остановлен"
    
    buttons = []
    if is_running:
        buttons.append([Button.inline("🛑 Остановить", f"stop_unified_{chain_num}")])
        buttons.append([Button.inline("📊 Статус", f"status_unified_{chain_num}")])
    else:
        buttons.append([Button.inline("▶️ Запустить", f"start_unified_{chain_num}")])
    
    buttons.append([Button.inline("📋 Логи", f"logs_unified_{chain_num}")])
    buttons.append([Button.inline("🔙 Назад", "unified_menu")])
    
    await event.respond(
        f"🔄 **UNIFIED CHAIN {chain_num}**\n\n"
        f"Статус: {status_emoji} {status_text}\n"
        f"Тип: Invocer + Deleter\n"
        f"API: {get_api_for_chain(chain_num)}\n\n"
        "Выберите действие:",
        buttons=buttons
    )

def get_api_for_chain(chain_num):
    """Получить API ID для цепочки"""
    if chain_num == 1:
        return "0"  # YOUR_API_ID
    elif chain_num == 2:
        return "0"  # YOUR_API_ID
    elif chain_num == 3:
        return "0"  # YOUR_API_ID
    else:
        return "N/A"

@bot.on(events.CallbackQuery(pattern=b'start_unified_(.+)'))
async def start_unified_handler(event):
    """Запуск unified скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.start_single_script(chain_num, 'unified')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_unified_(.+)'))
async def stop_unified_handler(event):
    """Остановка unified скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.stop_single_script(chain_num, 'unified')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'unified_status_all'))
async def unified_status_all_handler(event):
    """Статус всех unified скриптов"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    status_text = "🔄 **СТАТУС UNIFIED СКРИПТОВ**\n\n"
    
    for chain_num in [1, 2, 3]:
        is_running = any(
            info.get('type') == 'unified' and info.get('chain') == chain_num 
            for info in master_instance.active_processes.values()
        )
        
        status_emoji = "🟢" if is_running else "🔴"
        status_text += f"Chain {chain_num}: {status_emoji} {'Работает' if is_running else 'Остановлен'}\n"
    
    await event.respond(status_text)

@bot.on(events.CallbackQuery(pattern=b'logs_unified_(.+)'))
async def logs_unified_handler(event):
    """Просмотр логов unified скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    try:
        log_path = f"d:\\ss\\cycle\\chain\\unified\\logs\\unified_chain{chain_num}.log"
        
        if not os.path.exists(log_path):
            await event.respond(f"❌ Лог файл не найден: {log_path}")
            return
        
        # Читаем последние 20 строк
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            recent_lines = lines[-20:] if len(lines) > 20 else lines
        
        if not recent_lines:
            await event.respond(f"📋 **ЛОГИ UNIFIED CHAIN {chain_num}**\n\n❌ Лог пуст")
            return
        
        log_text = f"📋 **ЛОГИ UNIFIED CHAIN {chain_num}**\n\n"
        log_text += "```\n"
        for line in recent_lines:
            log_text += line.strip() + "\n"
        log_text += "```"
        
        # Telegram имеет ограничение на длину сообщения
        if len(log_text) > 4000:
            log_text = log_text[:3900] + "\n...\n```"
        
        await event.respond(log_text)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка чтения логов: {str(e)}")

@bot.on(events.CallbackQuery(pattern=b'status_unified_(.+)'))
async def status_unified_handler(event):
    """Детальный статус unified скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    chain_num = int(event.pattern_match.group(1).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Находим процесс unified скрипта
    unified_process = None
    for proc_id, info in master_instance.active_processes.items():
        if info.get('type') == 'unified' and info.get('chain') == chain_num:
            unified_process = info
            break
    
    if unified_process is None:
        await event.respond(f"🔄 **UNIFIED CHAIN {chain_num}**\n\n🔴 **Остановлен**")
        return
    
    start_time = unified_process.get('start_time', 'Неизвестно')
    if isinstance(start_time, datetime):
        runtime = datetime.now() - start_time
        runtime_str = f"{runtime.seconds // 3600}ч {(runtime.seconds % 3600) // 60}м"
    else:
        runtime_str = "Неизвестно"
    
    status_text = (
        f"🔄 **UNIFIED CHAIN {chain_num}**\n\n"
        f"🟢 **Активен**\n"
        f"⏰ Время работы: {runtime_str}\n"
        f"📊 API: {get_api_for_chain(chain_num)}\n"
        f"🔄 Режим: Invocer + Deleter\n"
        f"📦 Батчи: по 4 ссылки\n"
        f"⏱️ Задержки: 30с между батчами"
    )
    
    await event.respond(status_text)

@bot.on(events.CallbackQuery(pattern=b'manage_script_(.+)'))
async def manage_script_handler(event):
    """Управление отдельным скриптом через меню цепочки"""
    await event.answer()
    
    script_name = event.pattern_match.group(1).decode()
    
    # Извлекаем тип и номер из имени
    if script_name.startswith('unified_chain'):
        script_type = 'unified'
        digits = ''.join([c for c in script_name if c.isdigit()])
    else:
        script_type = ''.join([c for c in script_name if not c.isdigit()])
        digits = ''.join([c for c in script_name if c.isdigit()])
    
    if not digits:
        await event.respond(f"❌ Не удается определить номер цепочки для {script_name}")
        return
    
    chain_num = int(digits)
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Проверяем статус скрипта
    is_running = master_instance.chain_status.get(chain_num, {}).get(script_type, False)
    status_emoji = "🟢" if is_running else "🔴"
    status_text = "работает" if is_running else "остановлен"
    
    # Формируем имя скрипта для команд
    if script_type == 'unified':
        full_script_name = f"unified_chain{chain_num}"
    else:
        full_script_name = f"{script_type}{chain_num}"
    
    buttons = []
    if is_running:
        buttons.append([Button.inline("🛑 Остановить", f"stop_single_{script_type}_{chain_num}")])
    else:
        buttons.append([Button.inline("▶️ Запустить", f"start_single_{script_type}_{chain_num}")])
    
    buttons.append([Button.inline("🔙 Назад к цепочке", f"chain_{chain_num}")])
    
    await event.respond(
        f"⚙️ **{full_script_name.upper()}**\n\n"
        f"Статус: {status_emoji} {status_text}\n"
        f"Цепочка: {chain_num}\n\n"
        "Выберите действие:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'start_single_(.+)_(.+)'))
async def start_single_script_handler(event):
    """Запуск одного скрипта"""
    await event.answer()
    
    script_type = event.pattern_match.group(1).decode()
    chain_num = int(event.pattern_match.group(2).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.start_single_script(chain_num, script_type)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_single_(.+)_(.+)'))
async def stop_single_script_handler(event):
    """Остановка одного скрипта"""
    await event.answer()
    
    script_type = event.pattern_match.group(1).decode()
    chain_num = int(event.pattern_match.group(2).decode())
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.stop_single_script(chain_num, script_type)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'script_(.+)'))
async def script_handler(event):
    """Управление конкретным скриптом"""
    await event.answer()  # Подтверждаем получение события
    
    script_name = event.pattern_match.group(1).decode()
    
    # Извлекаем тип и номер из имени
    if script_name.startswith('unified_chain'):
        # Специальная обработка для unified_chain1, unified_chain2, etc.
        script_type = 'unified'
        digits = ''.join([c for c in script_name if c.isdigit()])
    else:
        # Стандартная обработка для probiv1, filtr2, etc.
        script_type = ''.join([c for c in script_name if not c.isdigit()])
        digits = ''.join([c for c in script_name if c.isdigit()])
    
    if not digits:
        await event.respond(f"❌ Не удается определить номер цепочки для {script_name}")
        return
    
    chain_num = int(digits)
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    # Безопасная проверка существования ключей
    if chain_num not in master_instance.chain_status:
        await event.respond(f"❌ Цепочка {chain_num} не поддерживается")
        return
    
    if script_type not in master_instance.chain_status[chain_num]:
        await event.respond(f"❌ Тип скрипта '{script_type}' не поддерживается для цепочки {chain_num}")
        return
    
    is_running = master_instance.chain_status[chain_num][script_type]
    status_emoji = "🟢" if is_running else "🔴"
    status_text = "работает" if is_running else "остановлен"
    
    buttons = []
    if is_running:
        buttons.append([Button.inline("🛑 Остановить", f"stop_script_{script_name}")])
    else:
        buttons.append([Button.inline("▶️ Запустить", f"start_script_{script_name}")])
    
    buttons.append([Button.inline("🔙 Назад", f"script_type_{script_type}")])
    
    await event.respond(
        f"⚙️ **{script_name.upper()}**\n\n"
        f"Статус: {status_emoji} {status_text}\n\n"
        "Выберите действие:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'start_script_(.+)'))
async def start_script_handler(event):
    """Запуск отдельного скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    script_name = event.pattern_match.group(1).decode()
    
    # Извлекаем тип и номер из имени
    if script_name.startswith('unified_chain'):
        # Специальная обработка для unified_chain1, unified_chain2, etc.
        script_type = 'unified'
        digits = ''.join([c for c in script_name if c.isdigit()])
    else:
        # Стандартная обработка для probiv1, filtr2, etc.
        script_type = ''.join([c for c in script_name if not c.isdigit()])
        digits = ''.join([c for c in script_name if c.isdigit()])
    
    if not digits:
        await event.respond(f"❌ Не удается определить номер цепочки для {script_name}")
        return
    
    chain_num = int(digits)
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.start_single_script(chain_num, script_type)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_script_(.+)'))
async def stop_script_callback_handler(event):
    """Остановка отдельного скрипта"""
    await event.answer()  # Подтверждаем получение события
    
    script_name = event.pattern_match.group(1).decode()
    
    # Извлекаем тип и номер из имени
    if script_name.startswith('unified_chain'):
        # Специальная обработка для unified_chain1, unified_chain2, etc.
        script_type = 'unified'
        digits = ''.join([c for c in script_name if c.isdigit()])
    else:
        # Стандартная обработка для probiv1, filtr2, etc.
        script_type = ''.join([c for c in script_name if not c.isdigit()])
        digits = ''.join([c for c in script_name if c.isdigit()])
    
    if not digits:
        await event.respond(f"❌ Не удается определить номер цепочки для {script_name}")
        return
    
    chain_num = int(digits)
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.stop_single_script(chain_num, script_type)
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'probiv_stats'))
async def probiv_stats_handler(event):
    """Статистика Probiv"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    stats = master_instance.get_probiv_status()
    await event.respond(stats)

@bot.on(events.CallbackQuery(pattern=b'database_stats'))
async def database_stats_handler(event):
    """Статистика базы данных"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    stats = master_instance.get_filtr_stats_from_db()
    
    buttons = [
        [Button.inline("📊 Расширенная статистика БД", "dbstats_detailed")],
        [Button.inline("📝 Последние 20 записей", "db_recent_20")],
        [Button.inline("🔍 Поиск по ссылке", "db_search")],
        [Button.inline("📊 Статистика по категориям", "db_categories")],
        [Button.inline("📈 Форекс", "db_cat_forex"), Button.inline("₿ Криптовалюта", "db_cat_crypto")],
        [Button.inline("👔 HR/Вакансии", "db_cat_hr"), Button.inline("🤝 Партнёрки", "db_cat_affiliate")],
        [Button.inline("🚀 Трафик", "db_cat_traffic"), Button.inline("💰 Продажи", "db_cat_sales")],
        [Button.inline("📤 Экспорт БД в файл", "db_export"), Button.inline("🔄 Дубликаты", "db_duplicates")],
        [Button.inline("🗑️ Очистка БД", "db_cleanup_menu")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(stats, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'dbstats_detailed'))
async def dbstats_detailed_handler(event):
    """Расширенная статистика по базе данных через кнопку"""
    await event.answer()
    
    try:
        stats = get_database_stats()
        
        if stats is None:
            await event.respond("⚠️ База данных еще не создана")
            return
        
        if 'error' in stats:
            await event.respond(f"❌ Ошибка: {stats['error']}")
            return
        
        report = (
            f"📊 **РАСШИРЕННАЯ СТАТИСТИКА БД**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**ЧТО ЭТО?**\n\n"
            f"База данных хранит все группы, в которые\n"
            f"вступили ваши аккаунты. Здесь вы видите:\n"
            f"• Сколько всего групп обработано\n"
            f"• Какой аккаунт больше всех работал\n"
            f"• Какие темы групп чаще встречаются\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📈 **ОБЩАЯ СТАТИСТИКА:**\n\n"
            f"✅ **Всего чатов:** {stats['total']}\n"
            f"📅 **За 24 часа:** {stats['last_24h']}\n"
            f"📅 **За 7 дней:** {stats['last_7d']}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔗 **РАБОТА АККАУНТОВ:**\n\n"
        )
        
        for chain, count in sorted(stats['by_chain'].items()):
            percent = (count / stats['total'] * 100) if stats['total'] > 0 else 0
            report += f"   🔗 Chain {chain}: {count} ({percent:.1f}%)\n"
        
        if stats['by_category']:
            report += f"\n**По категориям:**\n"
            for category, count in list(stats['by_category'].items())[:5]:
                report += f"   📁 {category}: {count}\n"
        
        if stats['top_keywords']:
            report += f"\n**Топ ключевых слов:**\n"
            for keyword, count in stats['top_keywords'][:5]:
                report += f"   🔑 {keyword}: {count}\n"
        
        buttons = [[Button.inline("◀️ Назад к базе данных", "database_stats")]]
        await event.respond(report, buttons=buttons)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка анализа: {str(e)}")

@bot.on(events.CallbackQuery(pattern=b'db_recent_20'))
async def db_recent_20_handler(event):
    """Показать последние 20 записей из БД"""
    await event.answer()
    
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import get_recent_chats
        
        recent = get_recent_chats(20)
        
        if not recent:
            message = "📝 **ПОСЛЕДНИЕ ЗАПИСИ**\n\n⚠️ База данных пуста"
        else:
            message = "📝 **ПОСЛЕДНИЕ 20 ЗАПИСЕЙ**\n\n"
            for i, (link, chain_num, timestamp) in enumerate(recent, 1):
                # Сокращаем ссылку для читаемости
                short_link = link.replace('https://t.me/', '')
                if len(short_link) > 30:
                    short_link = short_link[:27] + '...'
                message += f"{i}. [CH{chain_num}] {short_link}\n"
                message += f"   ⏰ {timestamp}\n\n"
        
        buttons = [[Button.inline("◀️ Назад к статистике", "database_stats")]]
        await event.respond(message, buttons=buttons)
        
    except Exception as e:
        await event.respond(
            f"❌ **Ошибка получения записей**\n\n{str(e)}",
            buttons=[[Button.inline("◀️ Назад", "database_stats")]]
        )

@bot.on(events.CallbackQuery(pattern=b'db_search'))
async def db_search_handler(event):
    """Поиск в БД"""
    await event.answer()
    
    await event.respond(
        "🔍 **ПОИСК В БАЗЕ ДАННЫХ**\n\n"
        "Для поиска отправьте команду:\n"
        "`/search_db <текст>`\n\n"
        "**Примеры:**\n"
        "`/search_db forex`\n"
        "`/search_db https://t.me/example`\n"
        "`/search_db trading`\n\n"
        "Поиск найдет все ссылки, содержащие указанный текст.",
        buttons=[[Button.inline("◀️ Назад", "database_stats")]]
    )

@bot.on(events.NewMessage(pattern='/search_db'))
async def search_db_command_handler(event):
    """Команда поиска в БД"""
    try:
        parts = event.message.text.split(maxsplit=1)
        if len(parts) < 2:
            await event.respond(
                "⚠️ Укажите текст для поиска:\n"
                "`/search_db <текст>`"
            )
            return
        
        search_term = parts[1].strip()
        
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import search_chats
        
        results = search_chats(search_term)
        
        if not results:
            message = f"🔍 **ПОИСК: \"{search_term}\"**\n\n❌ Ничего не найдено"
        else:
            message = f"🔍 **ПОИСК: \"{search_term}\"**\n\n"
            message += f"Найдено записей: **{len(results)}**\n\n"
            
            # Показываем первые 15 результатов
            for i, (link, chain_num, timestamp) in enumerate(results[:15], 1):
                short_link = link.replace('https://t.me/', '')
                if len(short_link) > 35:
                    short_link = short_link[:32] + '...'
                message += f"{i}. [CH{chain_num}] {short_link}\n"
            
            if len(results) > 15:
                message += f"\n... и еще {len(results) - 15} результатов"
        
        await event.respond(message)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка поиска: {str(e)}")

@bot.on(events.CallbackQuery(pattern=b'db_categories'))
async def db_categories_handler(event):
    """Статистика по категориям"""
    await event.answer()
    
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import get_all_categories
        from categories import get_category_emoji, get_category_name  # type: ignore
        
        categories = get_all_categories()
        
        if not categories:
            message = "📊 **СТАТИСТИКА ПО КАТЕГОРИЯМ**\n\n❌ База данных пуста"
        else:
            message = "📊 **СТАТИСТИКА ПО КАТЕГОРИЯМ**\n\n"
            
            for category, count in categories:
                emoji = get_category_emoji(category)
                cat_name = get_category_name(category)
                message += f"{emoji} **{cat_name}**: {count} чатов\n"
        
        await event.respond(message, buttons=[[Button.inline("◀️ Назад", "database_stats")]])
        
    except Exception as e:
        await event.respond(
            f"❌ Ошибка получения статистики: {str(e)}",
            buttons=[[Button.inline("◀️ Назад", "database_stats")]]
        )

@bot.on(events.CallbackQuery(pattern=rb'db_cat_(\w+)'))
async def db_category_view_handler(event):
    """Просмотр чатов конкретной категории"""
    await event.answer()
    
    try:
        # Извлекаем название категории из callback data
        category = event.pattern_match.group(1).decode('utf-8')
        
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import get_chats_by_category, get_stats_by_category
        from categories import get_category_emoji, get_category_name  # type: ignore
        
        total = get_stats_by_category(category)
        chats = get_chats_by_category(category, limit=20)
        
        emoji = get_category_emoji(category)
        cat_name = get_category_name(category)
        
        if not chats:
            message = f"{emoji} **{cat_name}**\n\n❌ Чаты не найдены"
        else:
            message = f"{emoji} **{cat_name}**\n"
            message += f"Всего: {total} чатов\n\n"
            message += "📋 **Последние 20:**\n\n"
            
            for i, (link, chain_num, timestamp, keywords) in enumerate(chats, 1):
                short_link = link.replace('https://t.me/', '')
                if len(short_link) > 30:
                    short_link = short_link[:27] + '...'
                
                kw_display = f" | {keywords[:20]}" if keywords else ""
                message += f"{i}. [CH{chain_num}] {short_link}{kw_display}\n"
            
            if total > 20:
                message += f"\n... и еще {total - 20} чатов"
        
        await event.respond(message, buttons=[[Button.inline("◀️ Назад", "database_stats")]])
        
    except Exception as e:
        await event.respond(
            f"❌ Ошибка: {str(e)}",
            buttons=[[Button.inline("◀️ Назад", "database_stats")]]
        )

@bot.on(events.CallbackQuery(pattern=b'db_export'))
async def db_export_handler(event):
    """Экспорт базы данных в текстовый файл"""
    await event.answer()
    
    try:
        import sys
        import os
        from datetime import datetime
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import DB_PATH
        
        if not DB_PATH.exists():
            await event.respond("❌ База данных не найдена")
            return
        
        msg = await event.respond("⏳ Экспортирую базу данных...")
        
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        
        # Получаем все записи
        cursor.execute('''
            SELECT link, chain_num, category, matched_keywords, processed_at 
            FROM processed_chats 
            ORDER BY processed_at DESC
        ''')
        
        all_records = cursor.fetchall()
        conn.close()
        
        # Создаем файл
        export_file = f"database_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(export_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("ЭКСПОРТ БАЗЫ ДАННЫХ ОБРАБОТАННЫХ ЧАТОВ\n")
            f.write(f"Дата экспорта: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Всего записей: {len(all_records)}\n")
            f.write("=" * 80 + "\n\n")
            
            for i, (link, chain_num, category, keywords, timestamp) in enumerate(all_records, 1):
                f.write(f"#{i}\n")
                f.write(f"Ссылка: {link}\n")
                f.write(f"Аккаунт: Chain {chain_num}\n")
                f.write(f"Категория: {category or 'Не определена'}\n")
                f.write(f"Ключевые слова: {keywords or 'Нет'}\n")
                f.write(f"Дата добавления: {timestamp}\n")
                f.write("-" * 80 + "\n")
        
        await msg.delete()
        await event.respond(
            f"✅ **Экспорт завершён!**\n\n"
            f"📁 Файл: `{export_file}`\n"
            f"📊 Записей: {len(all_records)}\n\n"
            f"Файл находится в папке с ботом."
        )
        
        # Отправляем файл пользователю
        await bot.send_file(event.chat_id, export_file, caption="📤 Экспорт базы данных")
        
    except Exception as e:
        await event.respond(f"❌ Ошибка экспорта: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'db_duplicates'))
async def db_duplicates_handler(event):
    """Поиск дубликатов в базе данных"""
    await event.answer()
    
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import DB_PATH
        
        if not DB_PATH.exists():
            await event.respond("❌ База данных не найдена")
            return
        
        msg = await event.respond("⏳ Ищу дубликаты...")
        
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        
        # Ищем ссылки, которые встречаются более 1 раза
        cursor.execute('''
            SELECT link, COUNT(*) as count, GROUP_CONCAT(chain_num) as chains 
            FROM processed_chats 
            GROUP BY link 
            HAVING count > 1 
            ORDER BY count DESC
        ''')
        
        duplicates = cursor.fetchall()
        conn.close()
        
        await msg.delete()
        
        if not duplicates:
            await event.respond(
                "✅ **ДУБЛИКАТЫ НЕ НАЙДЕНЫ**\n\n"
                "Все записи уникальные!",
                buttons=[[Button.inline("◀️ Назад", "database_stats")]]
            )
            return
        
        message = "🔄 **НАЙДЕНЫ ДУБЛИКАТЫ**\n\n"
        message += f"Всего повторных записей: {len(duplicates)}\n\n"
        
        # Показываем первые 15
        for i, (link, count, chains) in enumerate(duplicates[:15], 1):
            short_link = link.replace('https://t.me/', '')
            if len(short_link) > 30:
                short_link = short_link[:27] + '...'
            message += f"{i}. {short_link}\n"
            message += f"   Повторов: {count} | Chains: {chains}\n\n"
        
        if len(duplicates) > 15:
            message += f"\n... и еще {len(duplicates) - 15} дубликатов"
        
        message += "\n\n⚠️ **Что это значит?**\n"
        message += "Один и тот же чат был обработан несколькими аккаунтами.\n"
        message += "Это нормально, если используется несколько источников ссылок."
        
        await event.respond(message, buttons=[[Button.inline("◀️ Назад", "database_stats")]])
        
    except Exception as e:
        await event.respond(f"❌ Ошибка поиска: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'db_cleanup_menu'))
async def db_cleanup_menu_handler(event):
    """Меню очистки базы данных"""
    await event.answer()
    
    buttons = [
        [Button.inline("🗑️ Удалить записи старше 30 дней", "db_cleanup_30d")],
        [Button.inline("🗑️ Удалить записи старше 60 дней", "db_cleanup_60d")],
        [Button.inline("🗑️ Удалить записи старше 90 дней", "db_cleanup_90d")],
        [Button.inline("⚠️ ПОЛНАЯ ОЧИСТКА БД", "db_cleanup_all_confirm")],
        [Button.inline("◀️ Назад", "database_stats")]
    ]
    
    await event.respond(
        "🗑️ **ОЧИСТКА БАЗЫ ДАННЫХ**\n\n"
        "⚠️ **Внимание!** Удаление необратимо!\n\n"
        "Выберите период:\n\n"
        "• **30 дней** - удалить старые записи\n"
        "• **60 дней** - удалить очень старые\n"
        "• **90 дней** - удалить древние записи\n"
        "• **Полная очистка** - удалить всё!\n\n"
        "💡 Рекомендуется делать экспорт перед очисткой.",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'db_cleanup_(\d+)d'))
async def db_cleanup_execute_handler(event):
    """Очистка записей старше N дней"""
    await event.answer()
    
    try:
        days = int(event.pattern_match.group(1).decode())
        
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import DB_PATH
        
        if not DB_PATH.exists():
            await event.respond("❌ База данных не найдена")
            return
        
        msg = await event.respond(f"⏳ Удаляю записи старше {days} дней...")
        
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        
        # Считаем сколько будет удалено
        cursor.execute('''
            SELECT COUNT(*) FROM processed_chats 
            WHERE datetime(processed_at) < datetime('now', ?)
        ''', (f'-{days} days',))
        
        count_to_delete = cursor.fetchone()[0]
        
        if count_to_delete == 0:
            conn.close()
            await msg.edit(f"ℹ️ Нет записей старше {days} дней")
            return
        
        # Удаляем
        cursor.execute('''
            DELETE FROM processed_chats 
            WHERE datetime(processed_at) < datetime('now', ?)
        ''', (f'-{days} days',))
        
        conn.commit()
        conn.close()
        
        await msg.edit(
            f"✅ **Очистка завершена!**\n\n"
            f"Удалено записей: {count_to_delete}\n"
            f"Период: старше {days} дней"
        )
        
    except Exception as e:
        await event.respond(f"❌ Ошибка очистки: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'db_cleanup_all_confirm'))
async def db_cleanup_all_confirm_handler(event):
    """Подтверждение полной очистки"""
    await event.answer()
    
    buttons = [
        [Button.inline("⚠️ ДА, УДАЛИТЬ ВСЁ", "db_cleanup_all_execute")],
        [Button.inline("❌ Отмена", "db_cleanup_menu")]
    ]
    
    await event.respond(
        "⚠️ **ПОЛНАЯ ОЧИСТКА БАЗЫ ДАННЫХ**\n\n"
        "🚨 **ЭТО УДАЛИТ ВСЕ ЗАПИСИ!**\n\n"
        "После этого база данных будет пустой.\n"
        "Восстановить данные будет невозможно!\n\n"
        "Вы уверены?",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'db_cleanup_all_execute'))
async def db_cleanup_all_execute_handler(event):
    """Полная очистка базы данных"""
    await event.answer()
    
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'filtrs', '_shared'))
        from filtr_global_stats import DB_PATH
        
        if not DB_PATH.exists():
            await event.respond("❌ База данных не найдена")
            return
        
        msg = await event.respond("⏳ Выполняю полную очистку...")
        
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        cursor = conn.cursor()
        
        # Считаем записи
        cursor.execute('SELECT COUNT(*) FROM processed_chats')
        total = cursor.fetchone()[0]
        
        # Удаляем всё
        cursor.execute('DELETE FROM processed_chats')
        conn.commit()
        conn.close()
        
        await msg.edit(
            f"✅ **База данных очищена!**\n\n"
            f"Удалено записей: {total}\n\n"
            f"База данных пуста."
        )
        
    except Exception as e:
        await event.respond(f"❌ Ошибка очистки: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'manual_scan'))
async def manual_scan_handler(event):
    """Ручное сканирование логов"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    if not master_system_enabled:
        await event.respond("❌ Мастер-система выключена. Включите её сначала.")
        return
    
    await event.respond("🔍 Запускаю ручное сканирование логов...")
    
    # Запускаем сканирование в отдельном потоке чтобы не блокировать бота
    import threading
    scan_thread = threading.Thread(target=master_instance.scan_existing_logs)
    scan_thread.daemon = True
    scan_thread.start()
    
    await event.respond(
        "✅ **Ручное сканирование запущено**\n\n"
        "🔍 Система проверяет логи на наличие:\n"
        "• Завершённых probiv операций\n"
        "• Готовых к запуску filtr процессов\n"
        "• Сигналов для unified скриптов\n\n"
        "📬 Результаты будут отправлены автоматически"
    )

@bot.on(events.CallbackQuery(pattern=b'restart_monitoring'))
async def restart_monitoring_callback_handler(event):
    """Перезапуск мониторинга триггеров"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    result = master_instance.restart_monitoring()
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'none'))
async def none_handler(event):
    """Обработчик для разделителей (не делает ничего)"""
    await event.answer()

@bot.on(events.CallbackQuery(pattern=b'mass_stop_menu'))
async def mass_stop_menu_handler(event):
    """Меню массовой остановки"""
    await event.answer()
    
    buttons = [
        [Button.inline("🛑 Остановить ВСЕ ЦЕПОЧКИ (1,2,3)", "stop_all_chains_confirm")],
        [Button.inline("⛔ Все PROBIV скрипты", "stop_all_probiv_confirm")],
        [Button.inline("⛔ Все FILTR скрипты", "stop_all_filtr_confirm")],
        [Button.inline("⛔ Все UNIFIED скрипты", "stop_all_unified_confirm")],
        [Button.inline("⛔ Все INVOCER скрипты", "stop_all_invocer_confirm")],
        [Button.inline("⛔ Все DELETER скрипты", "stop_all_deleter_confirm")],
        [Button.inline("🔙 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "⚠️ **МАССОВАЯ ОСТАНОВКА**\n\n"
        "Выберите что остановить:\n\n"
        "🛑 **Все цепочки** - останавливает все скрипты цепочек 1, 2, 3\n"
        "⛔ **По типу** - останавливает все скрипты определенного типа\n\n"
        "⚠️ Мастер-бот продолжит работу после остановки",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'stop_all_chains_confirm'))
async def stop_all_chains_handler(event):
    """Остановка всех цепочек"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю ВСЕ цепочки (1, 2, 3)...")
    result = master_instance.stop_all_chains()
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all_probiv_confirm'))
async def stop_all_probiv_handler(event):
    """Остановка всех PROBIV"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю все PROBIV скрипты...")
    result = master_instance.stop_all_script_type('probiv')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all_filtr_confirm'))
async def stop_all_filtr_handler(event):
    """Остановка всех FILTR"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю все FILTR скрипты...")
    result = master_instance.stop_all_script_type('filtr')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all_unified_confirm'))
async def stop_all_unified_handler(event):
    """Остановка всех UNIFIED"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю все UNIFIED скрипты...")
    result = master_instance.stop_all_script_type('unified')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all_invocer_confirm'))
async def stop_all_invocer_handler(event):
    """Остановка всех INVOCER"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю все INVOCER скрипты...")
    result = master_instance.stop_all_script_type('invocer')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all_deleter_confirm'))
async def stop_all_deleter_handler(event):
    """Остановка всех DELETER"""
    await event.answer()
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    await event.respond("🛑 Останавливаю все DELETER скрипты...")
    result = master_instance.stop_all_script_type('deleter')
    await event.respond(result)

@bot.on(events.CallbackQuery(pattern=b'stop_all'))
async def stop_all_handler(event):
    """Остановка всех процессов"""
    await event.answer()  # Подтверждаем получение события
    
    if master_instance is None:
        await event.respond("❌ Мастер еще не запущен")
        return
    
    master_instance.stop_all_scripts()
    await event.respond("🛑 Все процессы остановлены. Мастер продолжает работу.")

@bot.on(events.CallbackQuery(pattern=b'main_menu'))
async def main_menu_handler(event):
    """Возврат в главное меню"""
    await event.answer()
    
    buttons = [
        [Button.inline("▶️ СТАРТ: Запустить систему", "quick_start_guide")],
        [Button.inline("🎯 Сменить тематику поиска", "presets_menu")],
        [Button.inline("📊 Статус системы (быстрая проверка)", "show_status")],
        [Button.inline("📈 Мониторинг и статистика (детально)", "unified_monitoring")],
        [Button.inline("🗄️ База данных чатов", "database_stats"), Button.inline("🗑️ Очистить аккаунт", "clear_account_menu")],
        [Button.inline("🔄 Управление UNIFIED (3 аккаунта)", "unified_menu")],
        [Button.inline("🚀 Управление PROBIV (цепочки)", "chains_menu")],
        [Button.inline("📋 Просмотр логов", "logs_menu")],
        [Button.inline("⏰ Планировщик задач", "scheduler_menu")],
        [Button.inline("🛡️ FloodWait защита", "floodwait_menu")],
        [Button.inline("➕ Добавить новые аккаунты", "add_accounts_guide")],
        [Button.inline("🛑 Остановить всё", "stop_all_confirm")],
        [Button.inline("❓ Помощь и инструкции", "help_menu")]
    ]
    
    system_status = "🟢 Работает" if master_system_enabled else "🔴 Выключена"
    scheduler_status = "🟢 Активен" if task_scheduler and task_scheduler.running else "⚪ Неактивен"
    
    await event.respond(
        "👋 **Добро пожаловать в Панель Управления!**\n\n"
        "Это простой интерфейс для управления вашими Telegram аккаунтами.\n\n"
        f"🔌 Система: {system_status}\n"
        f"⏰ Планировщик: {scheduler_status}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📚 Что можно делать:**\n"
        "• Запускать и останавливать аккаунты\n"
        "• Смотреть статистику работы\n"
        "• Проверять логи в реальном времени\n"
        "• Планировать автозапуск по расписанию\n"
        "• Анализировать FloodWait блокировки\n\n"
        "**🎯 Начните здесь:**\n"
        "1️⃣ Нажмите «▶️ СТАРТ» для первого запуска\n"
        "2️⃣ Или «📊 Мониторинг» чтобы проверить работу\n\n"
        "Выберите нужное действие ниже:",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'scheduler_menu'))
async def scheduler_menu_handler(event):
    """Меню планировщика"""
    await event.answer()
    
    if not task_scheduler.tasks:
        tasks_text = "📋 **У вас пока нет задач**\n\n"
    else:
        tasks_text = "📋 **ЗАПЛАНИРОВАННЫЕ ЗАДАЧИ:**\n\n"
        for task in task_scheduler.tasks:
            status = "✅" if task['enabled'] else "❌"
            next_run = task['next_run'].strftime('%d.%m %H:%M') if task['next_run'] else "---"
            last_run = task['last_run'].strftime('%d.%m %H:%M') if task['last_run'] else "Не было"
            tasks_text += (
                f"{status} **#{task['id']}**: {task['name']}\n"
                f"   📅 След. запуск: {next_run}\n"
                f"   🕐 Посл. запуск: {last_run}\n\n"
            )
    
    buttons = [
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        f"⏰ **ПЛАНИРОВЩИК ЗАДАЧ**\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{tasks_text}"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**ЧТО ТАКОЕ ПЛАНИРОВЩИК?**\n\n"
        f"Это автоматический запуск/остановка аккаунтов\n"
        f"в заданное время КАЖДЫЙ ДЕНЬ.\n\n"
        f"Например:\n"
        f"• В 9:00 запускать все аккаунты\n"
        f"• В 23:00 останавливать их\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**КАК ДОБАВИТЬ ЗАДАЧУ?**\n\n"
        f"**ПРИМЕР 1:** Запуск в 9 утра\n"
        f"`/schedule add 09:00 start_chain 1`\n\n"
        f"**ПРИМЕР 2:** Остановка в 11 вечера\n"
        f"`/schedule add 23:00 stop_script 1 unified`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**ДРУГИЕ КОМАНДЫ:**\n\n"
        f"`/schedule` → Список всех задач\n"
        f"`/schedule remove 3` → Удалить задачу #3\n"
        f"`/schedule disable 1` → Выключить задачу #1\n"
        f"`/schedule enable 1` → Включить задачу #1\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡 **СОВЕТ:**\n"
        f"Лучше добавлять задержки между запусками!\n"
        f"Например: Chain1 в 9:00, Chain2 в 9:10, Chain3 в 9:20\n\n"
        f"📖 Подробнее: Главное меню → Помощь → Автозапуск",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'logs_menu'))
async def logs_menu_callback(event):
    """Меню просмотра логов"""
    await event.answer()
    
    buttons = []
    buttons.append([Button.inline("📋 Master", "view_log_master")])
    buttons.append([
        Button.inline("📊 Probiv 1", "view_log_probiv1"),
        Button.inline("📊 Probiv 2", "view_log_probiv2"),
        Button.inline("📊 Probiv 3", "view_log_probiv3")
    ])
    buttons.append([
        Button.inline("🔍 Filtr 1", "view_log_filtr1"),
        Button.inline("🔍 Filtr 2", "view_log_filtr2"),
        Button.inline("🔍 Filtr 3", "view_log_filtr3")
    ])
    buttons.append([
        Button.inline("🔄 Unified 1", "view_log_unified1"),
        Button.inline("🔄 Unified 2", "view_log_unified2"),
        Button.inline("🔄 Unified 3", "view_log_unified3")
    ])
    buttons.append([Button.inline("🏠 Главное меню", "main_menu")])
    
    await event.respond(
        "📋 **ПРОСМОТР ЛОГОВ**\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**ЧТО ТАКОЕ ЛОГИ?**\n\n"
        "Это файлы, куда записывается вся работа системы:\n"
        "• Какие группы обрабатываются\n"
        "• Есть ли ошибки или блокировки\n"
        "• Когда аккаунт вступил/покинул группу\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**КАК ЧИТАТЬ ЛОГИ?**\n\n"
        "🟢 [SUCCESS] — Всё хорошо!\n"
        "⚠️ [WARNING] — Предупреждение\n"
        "❌ [ERROR] — Ошибка (но не критичная)\n"
        "🛑 [CRITICAL] — Серьёзная проблема!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**БЫСТРЫЙ ПРОСМОТР:**\n"
        "Нажмите кнопку ниже — увидите последние 20 строк\n\n"
        "**ИЛИ ИСПОЛЬЗУЙТЕ КОМАНДЫ:**\n\n"
        "`/logs unified1` → Последние 20 строк\n"
        "`/logs unified1 50` → Последние 50 строк\n"
        "`/logs unified1 search FloodWait` → Поиск слова\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**КАКОЙ ЛОГ СМОТРЕТЬ?**\n\n"
        "**Master** — Общая работа системы\n"
        "**Probiv 1/2/3** — Поиск групп\n"
        "**Filtr 1/2/3** — Фильтрация групп\n"
        "**Unified 1/2/3** — Работа каждого аккаунта\n\n"
        "💡 Если что-то не работает — смотрите Unified!",
        buttons=buttons
    )

@bot.on(events.CallbackQuery(pattern=b'floodwait_menu'))
async def floodwait_menu_handler(event):
    """Меню FloodWait защиты"""
    await event.answer()
    
    try:
        results = analyze_floodwait_logs()
        
        status_text = "🛡️ **СТАТУС FLOODWAIT:**\n\n"
        
        for chain_num, data in results.items():
            if 'error' in data:
                continue
            
            level_emoji = {
                'low': '✅',
                'medium': '⚠️',
                'high': '🚨'
            }.get(data.get('level', 'low'), '❓')
            
            status_text += (
                f"{level_emoji} **Chain {chain_num}**: {data['count']} блокировок\n"
                f"   ⏱️ Ожидание: {data['total_wait']//60}м {data['total_wait']%60}с\n"
            )
        
        buttons = [
            [Button.inline("🔄 Обновить анализ", "floodwait_analyze")],
            [Button.inline("💡 Рекомендации", "floodwait_recommendations")],
            [Button.inline("📊 Детальный отчет", "floodwait_detailed")],
            [Button.inline("🏠 Главное меню", "main_menu")]
        ]
        
        await event.respond(
            f"{status_text}\n\n"
            f"**💡 Что это значит:**\n"
            f"✅ Low (0-5) - нормально, всё хорошо\n"
            f"⚠️ Medium (6-10) - увеличьте задержки\n"
            f"🚨 High (>10) - пауза 2-4 часа\n\n"
            f"Используйте `/floodwait` для полного анализа",
            buttons=buttons
        )
        
    except Exception as e:
        await event.respond(
            f"❌ Ошибка анализа: {str(e)}\n\n"
            f"Попробуйте команду `/floodwait`",
            buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
        )

@bot.on(events.CallbackQuery(pattern=b'floodwait_analyze'))
async def floodwait_analyze_handler(event):
    """Анализ FloodWait"""
    await event.answer()
    await event.respond("🔍 Анализирую логи на FloodWait...")
    
    try:
        results = analyze_floodwait_logs()
        recommendations = get_floodwait_recommendations(results)
        
        report = "🛡️ **АНАЛИЗ FLOODWAIT**\n\n"
        
        for chain_num, data in results.items():
            if 'error' in data:
                continue
            
            level_emoji = {
                'low': '✅',
                'medium': '⚠️',
                'high': '🚨'
            }.get(data.get('level', 'low'), '❓')
            
            report += (
                f"{level_emoji} **Chain {chain_num}**\n"
                f"   📊 Блокировок: {data['count']}\n"
                f"   ⏱️ Общее время: {data['total_wait']}с ({data['total_wait']//60}м)\n\n"
            )
        
        report += "**💡 Рекомендации:**\n\n"
        report += "\n\n".join(recommendations)
        
        buttons = [[Button.inline("🏠 Главное меню", "main_menu")]]
        await event.respond(report, buttons=buttons)
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")

@bot.on(events.CallbackQuery(pattern=b'floodwait_recommendations'))
async def floodwait_recommendations_handler(event):
    """Рекомендации по FloodWait"""
    await event.answer()
    
    recommendations_text = (
        "💡 **РЕКОМЕНДАЦИИ ПО FLOODWAIT**\n\n"
        "**Уровень LOW (0-5 блокировок):**\n"
        "✅ Всё отлично! Продолжайте в том же духе.\n"
        "• Текущие настройки оптимальны\n"
        "• Никаких изменений не требуется\n\n"
        "**Уровень MEDIUM (6-10 блокировок):**\n"
        "⚠️ Рекомендуется:\n"
        "• Увеличьте паузы между батчами на 50%\n"
        "• Уменьшите размер батча с 4 до 3 ссылок\n"
        "• Рассмотрите паузу 30-60 минут\n\n"
        "**Уровень HIGH (>10 блокировок):**\n"
        "🚨 Срочные меры:\n"
        "• Остановите все цепочки на 2-4 часа\n"
        "• Увеличьте все задержки в 2 раза\n"
        "• Работайте в 'медленном' режиме\n"
        "• Используйте только 1-2 аккаунта одновременно\n\n"
        "**Общие советы:**\n"
        "• Не работайте ночью (00:00-06:00)\n"
        "• Лучшее время: 10:00-22:00\n"
        "• Делайте перерывы каждые 4-6 часов\n"
        "• Не превышайте 200-250 групп/день на аккаунт"
    )
    
    buttons = [[Button.inline("🏠 Главное меню", "main_menu")]]
    await event.respond(recommendations_text, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'floodwait_detailed'))
async def floodwait_detailed_handler(event):
    """Детальный отчет FloodWait"""
    await event.answer()
    await event.respond("📊 Создаю детальный отчет...")
    
    # Отправляем пользователю команду для полного анализа
    await event.respond(
        "Используйте команду `/floodwait` для полного детального отчета\n\n"
        "Отчет включает:\n"
        "• Количество блокировок по каждой цепочке\n"
        "• Общее время ожидания\n"
        "• Предупреждения о длительных блокировках\n"
        "• Персональные рекомендации\n"
        "• Уровни риска"
    )

@bot.on(events.CallbackQuery(pattern=b'list_tasks'))
async def list_tasks_handler(event):
    """Список задач планировщика"""
    await event.answer()
    
    if not task_scheduler.tasks:
        await event.respond(
            "📋 **Задач нет**\n\n"
            "Добавьте задачи через команду:\n"
            "`/schedule add HH:MM action chain [script]`",
            buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
        )
        return
    
    tasks_text = "📋 **ЗАПЛАНИРОВАННЫЕ ЗАДАЧИ:**\n\n"
    for task in task_scheduler.tasks:
        status = "✅" if task['enabled'] else "❌"
        next_run = task['next_run'].strftime('%d.%m %H:%M') if task['next_run'] else "---"
        last_run = task['last_run'].strftime('%d.%m %H:%M') if task['last_run'] else "Не было"
        repeat = "🔄 Ежедневно" if task['repeat'] else "1 раз"
        
        tasks_text += (
            f"{status} **#{task['id']}**: {task['name']}\n"
            f"   ⏰ След. запуск: {next_run}\n"
            f"   🕐 Посл. запуск: {last_run}\n"
            f"   {repeat}\n\n"
        )
    
    buttons = [[Button.inline("🏠 Главное меню", "main_menu")]]
    await event.respond(tasks_text, buttons=buttons)

@bot.on(events.CallbackQuery(pattern=b'unified_monitoring'))
async def unified_monitoring_handler(event):
    """Мониторинг UNIFIED скриптов"""
    await event.answer()
    
    try:
        import json
        from datetime import datetime
        
        date = datetime.now().strftime('%Y%m%d')
        unified_path = Path(__file__).parent / 'unified' / 'logs'
        
        message = "📊 **МОНИТОРИНГ РАБОТЫ СИСТЕМЫ**\n\n"
        message += f"📅 Сегодня: {datetime.now().strftime('%d.%m.%Y')}\n"
        message += f"🕐 Время: {datetime.now().strftime('%H:%M')}\n\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        total_joined = 0
        total_checked = 0
        total_floodwait = 0
        alerts = []
        chains_working = 0
        
        for i in range(1, 4):
            stats_file = unified_path / f'unified_chain{i}_stats_{date}.json'
            
            message += f"**👤 АККАУНТ {i}:**\n"
            
            if stats_file.exists():
                with open(stats_file, 'r', encoding='utf-8') as f:
                    stats = json.load(f)
                
                chains_working += 1
                joined = stats['joined_count']
                checked = stats['checked_count']
                floodwait = stats['floodwait_count']
                
                # Индикатор работы
                if joined > 0:
                    status_icon = "🟢"
                    status_text = "Работает"
                elif checked > 0:
                    status_icon = "🟡"
                    status_text = "Запускается"
                else:
                    status_icon = "⚪"
                    status_text = "Ожидание"
                
                message += f"  {status_icon} Статус: **{status_text}**\n"
                message += f"  � Вступил в групп: **{joined}**\n"
                message += f"  🔍 Проверил: {checked}\n"
                
                # FloodWait с объяснением
                if floodwait == 0:
                    message += f"  ✅ Блокировок: 0 (отлично!)\n"
                elif floodwait <= 2:
                    message += f"  ✅ Блокировок: {floodwait} (норма)\n"
                elif floodwait <= 5:
                    message += f"  ⚠️ Блокировок: {floodwait} (приемлемо)\n"
                else:
                    message += f"  � Блокировок: {floodwait} (много!)\n"
                    alerts.append(f"Аккаунт {i}: Много блокировок")
                
                # Адаптация с объяснением
                if stats.get('adaptation_active'):
                    multiplier = stats.get('delay_multiplier', 1.0)
                    message += f"  �️ Защита: Активна (x{multiplier})\n"
                    message += f"     ℹ️ Система замедлилась для безопасности\n"
                
                total_joined += joined
                total_checked += checked
                total_floodwait += floodwait
                
            else:
                message += "  🔴 Статус: **НЕ ЗАПУЩЕН**\n"
                message += "  ℹ️ Запустите этот аккаунт через меню\n"
                alerts.append(f"Аккаунт {i}: Не запущен")
            
            message += "\n"
        
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"**📈 ИТОГИ ЗА СЕГОДНЯ:**\n\n"
        message += f"👥 Работает аккаунтов: **{chains_working} из 3**\n"
        message += f"📥 Всего вступлений: **{total_joined}**\n"
        message += f"🔍 Всего проверок: **{total_checked}**\n"
        message += f"⚡ Всего блокировок: **{total_floodwait}**\n\n"
        
        # Общая оценка с объяснением
        if chains_working == 0:
            message += "🔴 **СИСТЕМА НЕ ЗАПУЩЕНА**\n\n"
            message += "❗ Запустите все 3 аккаунта:\n"
            message += "1. Нажмите «🔄 Управление UNIFIED»\n"
            message += "2. Запустите CHAIN1, CHAIN2, CHAIN3\n"
        elif chains_working < 3:
            message += "🟡 **РАБОТАЕТ ЧАСТИЧНО**\n\n"
            message += f"❗ Запущено только {chains_working} из 3 аккаунтов\n"
            message += "Запустите остальные для полной эффективности\n"
        else:
            if not alerts or len(alerts) <= 1:
                if total_floodwait == 0:
                    message += "🎉 **ВСЁ ОТЛИЧНО!**\n\n"
                    message += "✅ Все аккаунты работают\n"
                    message += "✅ Нет блокировок от Telegram\n"
                    message += "✅ Система работает идеально!\n"
                elif total_floodwait <= 4:
                    message += "✅ **ВСЁ ХОРОШО!**\n\n"
                    message += "✅ Все аккаунты работают\n"
                    message += "✅ Блокировок мало (это нормально)\n"
                    message += "✅ Система в порядке!\n"
                elif total_floodwait <= 8:
                    message += "⚠️ **ПРИЕМЛЕМО**\n\n"
                    message += "✅ Все аккаунты работают\n"
                    message += "⚠️ Есть блокировки, но не критично\n"
                    message += "ℹ️ Система адаптируется автоматически\n"
                else:
                    message += "🚨 **ТРЕБУЕТСЯ ВНИМАНИЕ!**\n\n"
                    message += "⚠️ Много блокировок FloodWait\n"
                    message += "ℹ️ Система замедлилась для защиты\n"
                    message += "💡 Подождите 2-4 часа, станет лучше\n"
            else:
                message += "⚠️ **ЕСТЬ ЗАМЕЧАНИЯ:**\n\n"
                for alert in alerts:
                    message += f"  • {alert}\n"
        
        buttons = [
            [Button.inline("🔄 Обновить статистику", "unified_monitoring")],
            [Button.inline("🔄 Управление аккаунтами", "unified_menu")],
            [Button.inline("❓ Что делать?", "help_errors")],
            [Button.inline("🏠 Главное меню", "main_menu")]
        ]
        
        await event.edit(message, buttons=buttons)
        
    except Exception as e:
        error_msg = str(e)
        buttons = [
            [Button.inline("🔄 Управление аккаунтами", "unified_menu")],
            [Button.inline("❓ Помощь", "help_errors")],
            [Button.inline("🏠 Главное меню", "main_menu")]
        ]
        
        if "No such file or directory" in error_msg or "не найден" in error_msg.lower():
            await event.edit(
                "⚠️ **НЕТ ДАННЫХ О РАБОТЕ**\n\n"
                "Похоже, аккаунты UNIFIED не запущены.\n\n"
                "**Что делать:**\n\n"
                "1️⃣ Нажмите «🔄 Управление аккаунтами» ниже\n"
                "2️⃣ Запустите все 3 аккаунта (CHAIN1/2/3)\n"
                "3️⃣ Подождите 5-10 минут\n"
                "4️⃣ Вернитесь сюда и обновите статистику\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💡 **Подсказка:**\n"
                "Запускайте аккаунты по очереди:\n"
                "• Сначала CHAIN1, подождите 2 минуты\n"
                "• Затем CHAIN2, подождите 2 минуты\n"
                "• Затем CHAIN3\n\n"
                "Это снизит нагрузку на систему.",
                buttons=buttons
            )
        else:
            await event.edit(
                f"❌ **Ошибка при проверке статистики**\n\n"
                f"Техническая информация:\n```{error_msg}```\n\n"
                f"**Попробуйте:**\n"
                f"1. Перезапустить аккаунты\n"
                f"2. Подождать 5 минут\n"
                f"3. Обновить статистику снова",
                buttons=buttons
            )

def send_notification_sync(message):
    global bot_loop
    
    if bot_loop is None or admin_chat_id is None:
        return
    
    try:
        asyncio.run_coroutine_threadsafe(send_bot_notification(message), bot_loop)
    except Exception:
        pass

def main():
    global master_instance, bot_loop, master_system_enabled, task_scheduler
    
    print()
    print('═' + '═' * 68 + '═')
    print('║' + ' ' * 15 + ' TELEGRAM MASTER CONTROL SYSTEM' + ' ' * 20 + '║')
    print('═' + '═' * 68 + '═')
    
    print()
    print('⚙️  Инициализация мастер-системы...')
    
    # Загружаем активные пресеты
    print('📦 Загрузка информации об активных пресетах...')
    load_active_presets()
    
    master_instance = HybridTriggeredMaster()
    master_system_enabled = True
    
    # Планировщик задач (в фоне)
    task_scheduler = TaskScheduler()
    task_scheduler.start()
    
    print()
    print(' Запуск автоматического мониторинга UNIFIED...')
    
    # Запуск автоматического мониторинга
    def start_auto_monitoring():
        import subprocess
        unified_path = Path(__file__).parent / 'unified'
        setup_script = unified_path / 'setup_auto_monitor.ps1'
        
        if setup_script.exists():
            try:
                # Проверяем, не запущена ли уже задача
                result = subprocess.run(
                    ['powershell', '-Command', 
                     'Get-ScheduledTask -TaskName "UnifiedChain_AutoMonitor" -ErrorAction SilentlyContinue'],
                    capture_output=True, text=True
                )
                
                if not result.stdout.strip():
                    print(' Настройка автоматического мониторинга...')
                    subprocess.run(
                        ['powershell', '-ExecutionPolicy', 'Bypass', '-File', str(setup_script)],
                        capture_output=True
                    )
                    print(' ✅ Автомониторинг настроен!')
                else:
                    print(' ✅ Автомониторинг уже настроен')
            except Exception as e:
                print(f' ⚠️ Не удалось настроить автомониторинг: {e}')
        else:
            print(' ⚠️ Скрипт автомониторинга не найден')
    
    # Запускаем в отдельном потоке, чтобы не блокировать старт
    monitoring_thread = threading.Thread(target=start_auto_monitoring, daemon=True)
    monitoring_thread.start()
    
    print()
    print(' Запуск Telegram бота...')
    
    async def run_bot():
        global bot_loop
        bot_loop = asyncio.get_event_loop()
        await bot.start(bot_token=BOT_TOKEN)
        
        print()
        print('' + '' * 68 + '')
        print('' + ' ' * 20 + ' СИСТЕМА ГОТОВА К РАБОТЕ' + ' ' * 21 + '')
        print('' + '' * 68 + '')
        print()
        
        await bot.run_until_disconnected()
    
    bot_thread = threading.Thread(target=lambda: asyncio.run(run_bot()), daemon=True)
    bot_thread.start()
    time.sleep(3)
    master_instance.run()

# ================================================================
# ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ УТИЛИТ
# ================================================================

@bot.on(events.CallbackQuery(pattern=b'utilities_menu'))
async def utilities_menu_handler(event):
    """Главное меню утилит"""
    await event.answer()
    
    buttons = [
        [Button.inline("🗑️ Очистить аккаунт от всех групп", "clear_account_menu")],
        [Button.inline("👥 Парсер участников группы", "parser_menu")],
        [Button.inline("🔑 Управление ключевыми словами", "keywords_editor_menu")],
        [Button.inline("🏠 Главное меню", "main_menu")]
    ]
    
    await event.respond(
        "🛠️ **ДОПОЛНИТЕЛЬНЫЕ УТИЛИТЫ**\n\n"
        "**🗑️ Очистка** - удалить все группы из аккаунта\n"
        "**👥 Парсер** - собрать @username из любой группы\n"
        "**🔑 Ключевые слова** - настроить фильтрацию чатов\n\n"
        "Выберите инструмент:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'clear_account_menu'))
async def clear_account_menu_handler(event):
    """Меню очистки unified аккаунтов (chain1/2/3)"""
    await event.answer()
    
    # Получаем ТОЛЬКО unified цепочки (те, что вступают и фильтруют)
    unified_chains = ext.get_all_chains(only_unified=True)
    
    if not unified_chains:
        await event.respond(
            "❌ **Unified аккаунты не найдены!**\n\n"
            "Проверьте наличие chain1/2/3_session.session в папке sessions/",
            buttons=[[Button.inline("◀️ Назад", "utilities_menu")]]
        )
        return
    
    # Строим кнопки для unified аккаунтов
    buttons = []
    for chain in unified_chains:
        buttons.append([Button.inline(
            f"🗑️ Chain {chain['num']} ({chain['display_name']})", 
            f"do_clear_{chain['num']}"
        )])
    
    # Добавляем кнопку "Очистить все"
    if len(unified_chains) > 1:
        buttons.append([Button.inline("🗑️ Очистить ВСЕ unified аккаунты", "clear_all_unified")])
    
    buttons.append([Button.inline("◀️ Назад", "utilities_menu")])
    
    await event.respond(
        "🗑️ **ОЧИСТКА UNIFIED АККАУНТОВ**\n\n"
        f"⚠️ **Это удалит аккаунт из ВСЕХ групп!**\n\n"
        f"💡 **Что такое Unified?**\n"
        f"• Аккаунты, которые ВСТУПАЮТ в группы\n"
        f"• И одновременно их ФИЛЬТРУЮТ\n"
        f"• Парсеры и пробивы работают отдельно\n\n"
        f"Найдено unified аккаунтов: {len(unified_chains)}\n\n"
        "Выберите аккаунт для очистки:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'do_clear_(\d+)'))
async def do_clear_handler(event):
    """Подтверждение очистки"""
    await event.answer()
    chain_num = int(event.pattern_match.group(1).decode())
    
    buttons = [
        [Button.inline(f"✅ ДА, очистить Chain {chain_num}", f"clear_exec_{chain_num}")],
        [Button.inline("❌ Отмена", "clear_account_menu")]
    ]
    
    await event.respond(
        f"⚠️ **ВНИМАНИЕ!**\n\n"
        f"Вы точно хотите удалить Chain {chain_num}\n"
        f"из ВСЕХ групп и супергрупп?\n\n"
        f"**Это действие необратимо!**",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'clear_exec_(\d+)'))
async def clear_exec_handler(event):
    """Запуск очистки"""
    await event.answer()
    chain_num = int(event.pattern_match.group(1).decode())
    
    msg = await event.respond(f"🚀 Запускаю очистку Chain {chain_num}...")
    
    try:
        config = ext.get_chain_session_info(chain_num)
        if not config:
            await msg.edit("❌ Ошибка: конфигурация не найдена")
            return
        
        sessions_path = Path(__file__).parent / 'sessions'
        session_file = sessions_path / config['session']
        
        client = TelegramClient(str(session_file), config['api_id'], config['api_hash'])
        await client.connect()
        
        async def progress(text):
            try:
                await msg.edit(f"🗑️ **ОЧИСТКА Chain {chain_num}**\n\n{text}")
            except:
                pass
        
        stats = await ext.clear_all_groups(client, chain_num, progress)
        await client.disconnect()
        
        await event.respond(
            f"✅ **ГОТОВО!**\n\n"
            f"Обработано: {stats['total_checked']}\n"
            f"Вышел: {stats['left']}\n"
            f"Ошибок: {stats['errors']}",
            buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
        )
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'clear_all_unified'))
async def clear_all_unified_handler(event):
    """Подтверждение очистки ВСЕХ unified аккаунтов"""
    await event.answer()
    
    unified_chains = ext.get_all_chains(only_unified=True)
    chain_list = ", ".join([f"Chain {c['num']}" for c in unified_chains])
    
    buttons = [
        [Button.inline(f"✅ ДА, очистить ВСЕ ({len(unified_chains)} шт.)", "clear_all_unified_exec")],
        [Button.inline("❌ Отмена", "clear_account_menu")]
    ]
    
    await event.respond(
        f"⚠️⚠️⚠️ **МАССОВАЯ ОЧИСТКА** ⚠️⚠️⚠️\n\n"
        f"Вы собираетесь очистить ВСЕ unified аккаунты:\n"
        f"**{chain_list}**\n\n"
        f"Каждый аккаунт будет удалён из ВСЕХ групп!\n\n"
        f"**Это действие необратимо!**\n\n"
        f"Вы уверены?",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'clear_all_unified_exec'))
async def clear_all_unified_exec_handler(event):
    """Запуск массовой очистки всех unified аккаунтов"""
    await event.answer()
    
    unified_chains = ext.get_all_chains(only_unified=True)
    
    msg = await event.respond(
        f"🚀 **МАССОВАЯ ОЧИСТКА ЗАПУЩЕНА**\n\n"
        f"Обрабатываю {len(unified_chains)} аккаунтов...\n\n"
        f"⏳ Это может занять несколько минут..."
    )
    
    total_stats = {
        'success': 0,
        'errors': 0,
        'total_left': 0,
        'total_checked': 0
    }
    
    results = []
    
    for i, chain in enumerate(unified_chains, 1):
        try:
            chain_num = chain['num']
            
            await msg.edit(
                f"🚀 **МАССОВАЯ ОЧИСТКА**\n\n"
                f"Прогресс: {i}/{len(unified_chains)}\n"
                f"Текущий: Chain {chain_num}\n\n"
                f"⏳ Обработка..."
            )
            
            config = ext.get_chain_session_info(chain_num)
            if not config:
                results.append(f"❌ Chain {chain_num}: конфигурация не найдена")
                total_stats['errors'] += 1
                continue
            
            sessions_path = Path(__file__).parent / 'sessions'
            session_file = sessions_path / config['session']
            
            client = TelegramClient(str(session_file), config['api_id'], config['api_hash'])
            await client.connect()
            
            stats = await ext.clear_all_groups(client, chain_num, lambda x: None)
            await client.disconnect()
            
            total_stats['success'] += 1
            total_stats['total_left'] += stats['left']
            total_stats['total_checked'] += stats['total_checked']
            
            results.append(
                f"✅ Chain {chain_num}: вышел из {stats['left']}/{stats['total_checked']}"
            )
            
        except Exception as e:
            total_stats['errors'] += 1
            results.append(f"❌ Chain {chain_num}: {str(e)}")
    
    # Формируем итоговый отчёт
    report = "✅ **МАССОВАЯ ОЧИСТКА ЗАВЕРШЕНА!**\n\n"
    report += f"📊 **Статистика:**\n"
    report += f"• Успешно: {total_stats['success']}/{len(unified_chains)}\n"
    report += f"• Всего проверено: {total_stats['total_checked']}\n"
    report += f"• Всего вышел: {total_stats['total_left']}\n"
    report += f"• Ошибок: {total_stats['errors']}\n\n"
    report += "**Детали:**\n"
    report += "\n".join(results)
    
    await event.respond(
        report,
        buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
    )


# Глобальное состояние для парсера
parser_state = {}
parser_processes = {}  # Хранение запущенных процессов парсеров

@bot.on(events.CallbackQuery(pattern=b'parser_menu'))
async def parser_menu_handler(event):
    """Меню парсера - НОВАЯ СИСТЕМА"""
    await event.answer()
    
    buttons = [
        [Button.inline("🚀 Запустить ВСЕ парсеры", "start_all_parsers")],
        [Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline("▶️ Parser 1", "start_parser_1"), Button.inline("⏹️ Стоп 1", "stop_parser_1")],
        [Button.inline("▶️ Parser 2", "start_parser_2"), Button.inline("⏹️ Стоп 2", "stop_parser_2")],
        [Button.inline("▶️ Parser 3", "start_parser_3"), Button.inline("⏹️ Стоп 3", "stop_parser_3")],
        [Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline("🛑 Остановить ВСЕ", "stop_all_parsers")],
        [Button.inline("◀️ Назад", "utilities_menu")]
    ]
    
    await event.respond(
        "👥 **УПРАВЛЕНИЕ ПАРСЕРАМИ**\n\n"
        "Парсеры собирают @username из групп\n"
        "и отправляют их в каналы для пробива\n\n"
        "💡 Все 3 парсера работают параллельно",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'start_parser_(\d+)'))
async def start_parser_handler(event):
    """Запуск отдельного парсера"""
    await event.answer()
    parser_num = int(event.pattern_match.group(1).decode())
    
    # Проверяем, не запущен ли уже
    if f'parser{parser_num}' in parser_processes:
        await event.respond(f"ℹ️ Parser {parser_num} уже запущен")
        return
    
    try:
        parser_path = Path(__file__).parent / 'parsers' / f'parsilka{parser_num}' / f'parsilka{parser_num}.py'
        
        process = subprocess.Popen(
            ['python', str(parser_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == 'win32' else 0
        )
        
        parser_processes[f'parser{parser_num}'] = process
        
        await event.respond(f"✅ Parser {parser_num} запущен (PID: {process.pid})")
    except Exception as e:
        await event.respond(f"❌ Ошибка запуска Parser {parser_num}: {str(e)}")


@bot.on(events.CallbackQuery(pattern=rb'stop_parser_(\d+)'))
async def stop_parser_handler(event):
    """Остановка отдельного парсера"""
    await event.answer()
    parser_num = int(event.pattern_match.group(1).decode())
    
    key = f'parser{parser_num}'
    if key not in parser_processes:
        await event.respond(f"ℹ️ Parser {parser_num} не запущен")
        return
    
    try:
        process = parser_processes[key]
        process.terminate()
        process.wait(timeout=5)
        del parser_processes[key]
        
        await event.respond(f"🛑 Parser {parser_num} остановлен")
    except Exception as e:
        await event.respond(f"❌ Ошибка остановки Parser {parser_num}: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'start_all_parsers'))
async def start_all_parsers_handler(event):
    """Запуск всех 3 парсеров"""
    await event.answer()
    
    msg = await event.respond("🚀 Запускаю все парсеры...")
    
    results = []
    for i in range(1, 4):
        key = f'parser{i}'
        
        if key in parser_processes:
            results.append(f"ℹ️ Parser {i}: уже запущен")
            continue
        
        try:
            parser_path = Path(__file__).parent / 'parsers' / f'parsilka{i}' / f'parsilka{i}.py'
            
            process = subprocess.Popen(
                ['python', str(parser_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == 'win32' else 0
            )
            
            parser_processes[key] = process
            results.append(f"✅ Parser {i}: запущен (PID: {process.pid})")
        except Exception as e:
            results.append(f"❌ Parser {i}: {str(e)}")
    
    await msg.edit("\n".join(results))


@bot.on(events.CallbackQuery(pattern=b'stop_all_parsers'))
async def stop_all_parsers_handler(event):
    """Остановка всех парсеров"""
    await event.answer()
    
    if not parser_processes:
        await event.respond("ℹ️ Нет запущенных парсеров")
        return
    
    results = []
    for key in list(parser_processes.keys()):
        try:
            process = parser_processes[key]
            process.terminate()
            process.wait(timeout=5)
            del parser_processes[key]
            results.append(f"🛑 {key}: остановлен")
        except Exception as e:
            results.append(f"❌ {key}: {str(e)}")
    
    await event.respond("\n".join(results))


@bot.on(events.CallbackQuery(pattern=rb'parse_with_(\d+)'))
async def parse_with_handler(event):
    """Начало парсинга"""
    await event.answer()
    chain_num = int(event.pattern_match.group(1).decode())
    
    parser_state[event.chat_id] = chain_num
    
    await event.respond(
        f"📝 **ПАРСИНГ ЧЕРЕЗ Chain {chain_num}**\n\n"
        f"Отправьте ссылку на группу:\n"
        f"• https://t.me/groupname\n"
        f"• @groupname\n\n"
        f"Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "parser_menu")]]
    )


@bot.on(events.NewMessage(pattern=r'(https://t\.me/|@)[\w\d_]+'))
async def parser_input_handler(event):
    """Обработка ввода ссылки"""
    if event.chat_id not in parser_state:
        return
    
    chain_num = parser_state.pop(event.chat_id)
    link = event.raw_text.strip()
    
    msg = await event.respond("🚀 Запускаю парсинг...")
    
    try:
        config = ext.get_chain_session_info(chain_num)
        sessions_path = Path(__file__).parent / 'sessions'
        session_file = sessions_path / config['session']
        
        client = TelegramClient(str(session_file), config['api_id'], config['api_hash'])
        await client.connect()
        
        async def progress(text):
            try:
                await msg.edit(f"👥 **ПАРСИНГ**\n\n{text}")
            except:
                pass
        
        usernames = await ext.parse_group_members(client, link, progress)
        
        if usernames:
            # Сохраняем в файл
            filename = f"parsed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Группа: {link}\n\n")
                for u in usernames:
                    f.write(f"{u}\n")
            
            # ОТПРАВЛЯЕМ USERNAME В КАНАЛ ДЛЯ ПРОБИВА
            try:
                # Формируем сообщение порциями по 50 username
                batch_size = 50
                sent_batches = 0
                
                for i in range(0, len(usernames), batch_size):
                    batch = usernames[i:i + batch_size]
                    message = f"🎯 **ПАРСИНГ ИЗ ГРУППЫ**\n\n"
                    message += f"Источник: {link}\n"
                    message += f"Порция: {sent_batches + 1}/{(len(usernames) + batch_size - 1) // batch_size}\n\n"
                    message += "\n".join(batch)
                    
                    await client.send_message(PROBIV_CHANNEL_ID, message)
                    sent_batches += 1
                    await asyncio.sleep(2)  # Пауза между отправками
                
                await msg.edit(
                    f"✅ **ГОТОВО!**\n\n"
                    f"📊 Собрано: {len(usernames)} username\n"
                    f"📤 Отправлено в канал: {sent_batches} сообщений\n"
                    f"💾 Файл: {filename}"
                )
            except Exception as send_error:
                await msg.edit(
                    f"✅ **ПАРСИНГ ЗАВЕРШЁН**\n\n"
                    f"📊 Собрано: {len(usernames)} username\n"
                    f"⚠️ Ошибка отправки в канал: {str(send_error)}\n"
                    f"💾 Файл: {filename}"
                )
            
            await client.disconnect()
            await event.respond(file=filename)
        else:
            await client.disconnect()
            await event.respond("⚠️ Не удалось собрать username")
            
    except Exception as e:
        error_text = str(e).lower()
        
        # Обработка FloodWait
        if "wait" in error_text and "seconds" in error_text:
            # Пытаемся извлечь количество секунд
            import re
            match = re.search(r'(\d+)\s*seconds?', error_text)
            if match:
                wait_seconds = int(match.group(1))
                wait_minutes = wait_seconds // 60
                wait_hours = wait_minutes // 60
                
                if wait_hours > 0:
                    time_str = f"{wait_hours}ч {wait_minutes % 60}м"
                elif wait_minutes > 0:
                    time_str = f"{wait_minutes}м"
                else:
                    time_str = f"{wait_seconds}с"
                
                await event.respond(
                    f"⏳ **FLOODWAIT**\n\n"
                    f"Telegram ограничил запросы от Chain {chain_num}\n\n"
                    f"⏰ Нужно подождать: {time_str}\n"
                    f"📊 Точное время: {wait_seconds} секунд\n\n"
                    f"💡 Попробуйте:\n"
                    f"• Использовать другой Chain для парсинга\n"
                    f"• Подождать указанное время"
                )
            else:
                await event.respond(f"⏳ FloodWait: {str(e)}")
        else:
            await event.respond(f"❌ Ошибка парсинга: {str(e)}")


# Глобальное состояние для редактора слов
keywords_state = {}

@bot.on(events.CallbackQuery(pattern=b'keywords_editor_menu'))
async def keywords_editor_menu_handler(event):
    """Меню редактора ключевых слов"""
    await event.answer()
    
    # Получаем РЕАЛЬНОЕ количество слов из unified файлов
    try:
        base_count_chain1 = len(ext.get_keywords_from_file(str(Path(__file__).parent / 'unified' / 'unified_chain1.py'), 'base_title_keywords'))
        history_count_chain1 = len(ext.get_keywords_from_file(str(Path(__file__).parent / 'unified' / 'unified_chain1.py'), 'history_keywords'))
        stop_count_chain1 = len(ext.get_keywords_from_file(str(Path(__file__).parent / 'unified' / 'unified_chain1.py'), 'stop_words_in_title'))
    except:
        base_count_chain1 = 0
        history_count_chain1 = 0
        stop_count_chain1 = 0
    
    # Получаем количество пресетов
    presets = ext.get_all_presets()
    preset_count = len(presets)
    
    buttons = [
        [Button.inline(f"🎯 Готовые комплекты • {preset_count}", "presets_menu")],
        [Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline(f"📝 Ключевые слова (названия) • {base_count_chain1}", "manage_base_kw")],
        [Button.inline(f"📜 Ключевые слова (история) • {history_count_chain1}", "manage_history_kw")],
        [Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline(f"🚫 Стоп-слова • {stop_count_chain1}", "manage_stop_words")],
        [Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")],
        [Button.inline("◀️ Назад", "utilities_menu")]
    ]
    
    await event.respond(
        "🔑 **УПРАВЛЕНИЕ СЛОВАМИ**\n\n"
        f"**💡 Текущее состояние (Chain 1):**\n"
        f"   📝 Base keywords: **{base_count_chain1}** слов\n"
        f"   📜 History keywords: **{history_count_chain1}** слов\n"
        f"   🚫 Stop words: **{stop_count_chain1}** слов\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**🎯 Готовые комплекты**\n"
        "   └ Быстрое применение пресетов\n\n"
        "**📝 Ключевые слова (названия)**\n"
        "   └ Для фильтра по имени группы\n\n"
        "**📜 Ключевые слова (история)**\n"
        "   └ Для проверки содержимого сообщений\n\n"
        "**🚫 Стоп-слова**\n"
        "   └ Исключить нежелательные темы\n\n"
        "Выберите тип:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'manage_base_kw'))
async def manage_base_kw_handler(event):
    """Управление base_title_keywords - ШАБЛОНЫ"""
    await event.answer()
    
    # Получаем доступные шаблоны
    templates = ext.get_keyword_templates()
    base_templates = templates.get('base_keywords', [])
    
    # Получаем реальное количество из файлов
    try:
        filepath = Path(__file__).parent / 'unified' / 'unified_chain1.py'
        current_count = len(ext.get_keywords_from_file(str(filepath), 'base_title_keywords'))
    except:
        current_count = 0
    
    if not base_templates:
        await event.respond(
            "📝 **КЛЮЧЕВЫЕ СЛОВА (названия)**\n\n"
            "❌ Шаблоны не найдены!\n\n"
            "Создайте файлы вида:\n"
            "`base_keywords_forex.txt`\n"
            "в папке keyword_templates/",
            buttons=[[Button.inline("◀️ Назад", "keywords_editor_menu")]]
        )
        return
    
    # Строим кнопки для шаблонов (по 2 в ряд)
    buttons = []
    row = []
    
    for template in base_templates:
        row.append(Button.inline(
            f"📄 {template['name']} ({template['count']})",
            f"apply_base_{template['file']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:  # Добавляем последнюю неполную строку
        buttons.append(row)
    
    # Разделитель и кнопка просмотра
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("📊 Показать текущие слова", "show_base_kw")])
    buttons.append([Button.inline("◀️ Назад", "keywords_editor_menu")])
    
    await event.respond(
        f"📝 **КЛЮЧЕВЫЕ СЛОВА (названия)**\n\n"
        f"💾 **Сейчас в файлах: {current_count} слов**\n\n"
        f"📦 Доступно шаблонов: **{len(base_templates)}**\n\n"
        f"Выберите шаблон для замены:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'show_base_kw'))
async def show_base_kw_handler(event):
    """Показать все ключевые слова с точной статистикой"""
    await event.answer()
    
    # Получаем данные для всех 3 цепочек
    chain_stats = []
    for chain_num in [1, 2, 3]:
        filepath = Path(__file__).parent / 'unified' / f'unified_chain{chain_num}.py'
        keywords = ext.get_keywords_from_file(str(filepath), 'base_title_keywords')
        chain_stats.append((chain_num, len(keywords)))
    
    # Берем первую цепочку для примера слов
    filepath = Path(__file__).parent / 'unified' / 'unified_chain1.py'
    keywords = ext.get_keywords_from_file(str(filepath), 'base_title_keywords')
    
    text = f"📋 **КЛЮЧЕВЫЕ СЛОВА (названия)**\n\n"
    text += f"📊 **СТАТИСТИКА:**\n"
    for chain_num, count in chain_stats:
        text += f"   🔗 Chain {chain_num}: **{count} слов**\n"
    
    text += f"\n**Примеры (первые 30):**\n"
    text += ", ".join(keywords[:30])
    
    if len(keywords) > 30:
        text += f"\n\n... и ещё {len(keywords) - 30} слов"
    
    buttons = [[Button.inline("◀️ Назад", "manage_base_kw")]]
    await event.respond(text, buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b'add_base_kw'))
async def add_base_kw_handler(event):
    """Добавление слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'add_base', 'type': 'base_title_keywords'}
    
    await event.respond(
        "➕ **ДОБАВИТЬ КЛЮЧЕВОЕ СЛОВО**\n\n"
        "Отправьте слово или несколько слов через запятую:\n"
        "Например: forex, trading, jobs\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_base_kw")]]
    )


@bot.on(events.CallbackQuery(pattern=b'del_base_kw'))
async def del_base_kw_handler(event):
    """Удаление слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'del_base', 'type': 'base_title_keywords'}
    
    await event.respond(
        "➖ **УДАЛИТЬ КЛЮЧЕВОЕ СЛОВО**\n\n"
        "Отправьте слово или несколько слов через запятую:\n"
        "Например: forex, trading\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_base_kw")]]
    )


@bot.on(events.CallbackQuery(pattern=b'apply_base_(.+)'))
async def apply_base_template_handler(event):
    """Применение шаблона base_keywords - выбор цепочки"""
    await event.answer()
    
    template_file = event.pattern_match.group(1).decode()
    templates = ext.get_keyword_templates()
    
    template = None
    for t in templates.get('base_keywords', []):
        if t['file'] == template_file:
            template = t
            break
    
    if not template:
        await event.respond("❌ Шаблон не найден!")
        return
    
    chains = ext.get_all_chains()
    buttons = []
    row = []
    
    for chain in chains:
        row.append(Button.inline(f"🔗 Chain {chain['num']}", f"confirm_base_{template_file}_{chain['num']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([Button.inline("🔄 Применить ко всем", f"confirm_base_{template_file}_all")])
    buttons.append([Button.inline("◀️ Назад", "manage_base_kw")])
    
    await event.respond(
        f"📝 **Применение шаблона**\n\n"
        f"Шаблон: **{template['name']}**\n"
        f"Слов: {template['count']}\n\n"
        f"Выберите цепочку:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'confirm_base_(.+)_(\d+|all)'))
async def confirm_apply_base_handler(event):
    """Подтверждение применения base_keywords"""
    await event.answer()
    
    match = event.pattern_match
    template_file = match.group(1).decode()
    target = match.group(2).decode()
    
    templates_path = Path(__file__).parent / 'keyword_templates'
    template_path = templates_path / template_file
    
    if not template_path.exists():
        await event.respond("❌ Файл шаблона не найден!")
        return
    
    msg = await event.respond("🔄 Применяю шаблон...")
    
    try:
        success_count = 0
        
        if target == 'all':
            chains = ext.get_all_chains()
            for chain in chains:
                if ext.apply_template_to_chain(chain['num'], 'base_title_keywords', str(template_path)):
                    success_count += 1
            
            await event.respond(
                f"✅ **Шаблон применён!**\n\n"
                f"Успешно: {success_count}/{len(chains)} цепочек",
                buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
            )
        else:
            chain_num = int(target)
            if ext.apply_template_to_chain(chain_num, 'base_title_keywords', str(template_path)):
                await event.respond(
                    f"✅ **Шаблон применён!**\n\n"
                    f"Chain {chain_num} обновлён",
                    buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
                )
            else:
                await event.respond("❌ Ошибка применения!")
                
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'manage_history_kw'))
async def manage_history_kw_handler(event):
    """Управление history_keywords - ШАБЛОНЫ"""
    await event.answer()
    
    templates = ext.get_keyword_templates()
    history_templates = templates.get('history_keywords', [])
    
    # Получаем реальное количество из файлов
    try:
        filepath = Path(__file__).parent / 'unified' / 'unified_chain1.py'
        current_count = len(ext.get_keywords_from_file(str(filepath), 'history_keywords'))
    except:
        current_count = 0
    
    if not history_templates:
        await event.respond(
            "📜 **КЛЮЧЕВЫЕ СЛОВА (история)**\n\n"
            "❌ Шаблоны не найдены!\n\n"
            "Создайте файлы вида:\n"
            "`history_keywords_*.txt`\n"
            "в папке keyword_templates/",
            buttons=[[Button.inline("◀️ Назад", "keywords_editor_menu")]]
        )
        return
    
    # Кнопки по 2 в ряд
    buttons = []
    row = []
    
    for template in history_templates:
        row.append(Button.inline(
            f"📄 {template['name']} ({template['count']})",
            f"apply_history_{template['file']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Разделитель и кнопка просмотра
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("📊 Показать текущие слова", "show_history_kw")])
    buttons.append([Button.inline("◀️ Назад", "keywords_editor_menu")])
    
    await event.respond(
        f"📜 **КЛЮЧЕВЫЕ СЛОВА (история)**\n\n"
        f"💾 **Сейчас в файлах: {current_count} слов**\n\n"
        f"📦 Доступно шаблонов: **{len(history_templates)}**\n\n"
        f"Выберите шаблон для замены:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'show_history_kw'))
async def show_history_kw_handler(event):
    """Показать history_keywords для всех 3 chains"""
    await event.answer()
    
    # Получаем статистику для всех 3 chains
    chain_stats = []
    for chain_num in [1, 2, 3]:
        filepath = Path(__file__).parent / 'unified' / f'unified_chain{chain_num}.py'
        keywords = ext.get_keywords_from_file(str(filepath), 'history_keywords')
        chain_stats.append((chain_num, len(keywords)))
    
    # Получаем примеры из chain1
    filepath_chain1 = Path(__file__).parent / 'unified' / 'unified_chain1.py'
    keywords_chain1 = ext.get_keywords_from_file(str(filepath_chain1), 'history_keywords')
    
    # Показываем первые 30 слов как примеры
    display_keywords = keywords_chain1[:30]
    
    text = f"📋 **КЛЮЧЕВЫЕ СЛОВА ИСТОРИИ**\n\n"
    text += f"📊 **СТАТИСТИКА:**\n"
    for chain_num, count in chain_stats:
        text += f"   🔗 Chain {chain_num}: **{count} слов**\n"
    
    text += f"\n📝 **Примеры из Chain 1 (первые 30):**\n\n"
    text += ", ".join(display_keywords) + "\n\n"
    
    if len(keywords_chain1) > 30:
        text += f"... и ещё {len(keywords_chain1) - 30} слов"
    
    buttons = [[Button.inline("◀️ Назад", "manage_history_kw")]]
    await event.respond(text, buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b'add_history_kw'))
async def add_history_kw_handler(event):
    """Добавление history слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'add_history', 'type': 'history_keywords'}
    
    await event.respond(
        "➕ **ДОБАВИТЬ СЛОВО В ИСТОРИЮ**\n\n"
        "Отправьте слово или несколько слов через запятую:\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_history_kw")]]
    )


@bot.on(events.CallbackQuery(pattern=b'del_history_kw'))
async def del_history_kw_handler(event):
    """Удаление history слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'del_history', 'type': 'history_keywords'}
    
    await event.respond(
        "➖ **УДАЛИТЬ СЛОВО ИЗ ИСТОРИИ**\n\n"
        "Отправьте слово или несколько слов через запятую:\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_history_kw")]]
    )


@bot.on(events.CallbackQuery(pattern=b'apply_history_(.+)'))
async def apply_history_template_handler(event):
    """Применение шаблона history_keywords"""
    await event.answer()
    
    template_file = event.pattern_match.group(1).decode()
    templates = ext.get_keyword_templates()
    
    template = None
    for t in templates.get('history_keywords', []):
        if t['file'] == template_file:
            template = t
            break
    
    if not template:
        await event.respond("❌ Шаблон не найден!")
        return
    
    chains = ext.get_all_chains()
    buttons = []
    row = []
    
    for chain in chains:
        row.append(Button.inline(f"🔗 Chain {chain['num']}", f"confirm_history_{template_file}_{chain['num']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([Button.inline("🔄 Применить ко всем", f"confirm_history_{template_file}_all")])
    buttons.append([Button.inline("◀️ Назад", "manage_history_kw")])
    
    await event.respond(
        f"📜 **Применение шаблона**\n\n"
        f"Шаблон: **{template['name']}**\n"
        f"Слов: {template['count']}\n\n"
        f"Выберите цепочку:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'confirm_history_(.+)_(\d+|all)'))
async def confirm_apply_history_handler(event):
    """Подтверждение применения history_keywords"""
    await event.answer()
    
    match = event.pattern_match
    template_file = match.group(1).decode()
    target = match.group(2).decode()
    
    templates_path = Path(__file__).parent / 'keyword_templates'
    template_path = templates_path / template_file
    
    if not template_path.exists():
        await event.respond("❌ Файл шаблона не найден!")
        return
    
    msg = await event.respond("🔄 Применяю шаблон...")
    
    try:
        success_count = 0
        
        if target == 'all':
            chains = ext.get_all_chains()
            for chain in chains:
                if ext.apply_template_to_chain(chain['num'], 'history_keywords', str(template_path)):
                    success_count += 1
            
            await event.respond(
                f"✅ **Шаблон применён!**\n\n"
                f"Успешно: {success_count}/{len(chains)} цепочек",
                buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
            )
        else:
            chain_num = int(target)
            if ext.apply_template_to_chain(chain_num, 'history_keywords', str(template_path)):
                await event.respond(
                    f"✅ **Шаблон применён!**\n\n"
                    f"Chain {chain_num} обновлён",
                    buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
                )
            else:
                await event.respond("❌ Ошибка применения!")
                
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'manage_stop_words'))
async def manage_stop_words_handler(event):
    """Управление stop_words_in_title - ШАБЛОНЫ"""
    await event.answer()
    
    templates = ext.get_keyword_templates()
    stop_templates = templates.get('stop_words', [])
    
    # Получаем реальное количество стоп-слов из файлов
    try:
        filepath = Path(__file__).parent / 'unified' / 'unified_chain1.py'
        current_count = len(ext.get_keywords_from_file(str(filepath), 'stop_words_in_title'))
    except:
        current_count = 0
    
    if not stop_templates:
        await event.respond(
            "🚫 **СТОП-СЛОВА**\n\n"
            "❌ Шаблоны не найдены!\n\n"
            "Создайте файлы вида:\n"
            "`stop_words_*.txt`\n"
            "в папке keyword_templates/",
            buttons=[[Button.inline("◀️ Назад", "keywords_editor_menu")]]
        )
        return
    
    # Кнопки по 2 в ряд
    buttons = []
    row = []
    
    for template in stop_templates:
        row.append(Button.inline(
            f"🚫 {template['name']} ({template['count']})",
            f"apply_stop_{template['file']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Разделитель и кнопка просмотра
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("📊 Показать текущие стоп-слова", "show_stop_words")])
    buttons.append([Button.inline("◀️ Назад", "keywords_editor_menu")])
    
    await event.respond(
        f"🚫 **СТОП-СЛОВА**\n\n"
        f"💾 **Сейчас в файлах: {current_count} слов**\n"
        f"   (Включая все страны кроме UA/MD)\n\n"
        f"📦 Доступно шаблонов: **{len(stop_templates)}**\n\n"
        f"Выберите шаблон для замены:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b'show_stop_words'))
async def show_stop_words_handler(event):
    """Показать stop_words с точной статистикой"""
    await event.answer()
    
    # Получаем данные для всех 3 цепочек
    chain_stats = []
    for chain_num in [1, 2, 3]:
        filepath = Path(__file__).parent / 'unified' / f'unified_chain{chain_num}.py'
        keywords = ext.get_keywords_from_file(str(filepath), 'stop_words_in_title')
        chain_stats.append((chain_num, len(keywords)))
    
    # Берем первую цепочку для примера слов
    filepath = Path(__file__).parent / 'unified' / 'unified_chain1.py'
    keywords = ext.get_keywords_from_file(str(filepath), 'stop_words_in_title')
    
    text = f"🚫 **СТОП-СЛОВА**\n\n"
    text += f"📊 **СТАТИСТИКА:**\n"
    for chain_num, count in chain_stats:
        text += f"   🔗 Chain {chain_num}: **{count} слов**\n"
    
    text += f"\n💡 **Что это значит?**\n"
    text += f"В unified_chain1/2/3.py находятся:\n"
    text += f"• Базовые стоп-слова (~80 шт.)\n"
    text += f"• ВСЕ страны и города мира (~450 шт.)\n"
    text += f"• КРОМЕ Украины и Молдовы\n\n"
    
    text += f"**Примеры (первые 30):**\n"
    text += ", ".join(keywords[:30])
    
    if len(keywords) > 30:
        text += f"\n\n... и ещё {len(keywords) - 30} стоп-слов"
    
    buttons = [[Button.inline("◀️ Назад", "manage_stop_words")]]
    await event.respond(text, buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b'add_stop_word'))
async def add_stop_word_handler(event):
    """Добавление стоп-слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'add_stop', 'type': 'stop_words_in_title'}
    
    await event.respond(
        "➕ **ДОБАВИТЬ СТОП-СЛОВО**\n\n"
        "Отправьте слово или несколько слов через запятую:\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_stop_words")]]
    )


@bot.on(events.CallbackQuery(pattern=b'del_stop_word'))
async def del_stop_word_handler(event):
    """Удаление стоп-слова"""
    await event.answer()
    
    keywords_state[event.chat_id] = {'action': 'del_stop', 'type': 'stop_words_in_title'}
    
    await event.respond(
        "➖ **УДАЛИТЬ СТОП-СЛОВО**\n\n"
        "Отправьте слово или несколько слов через запятую:\n\n"
        "Жду ввод...",
        buttons=[[Button.inline("❌ Отмена", "manage_stop_words")]]
    )


@bot.on(events.CallbackQuery(pattern=b'apply_stop_(.+)'))
async def apply_stop_template_handler(event):
    """Применение шаблона stop_words"""
    await event.answer()
    
    template_file = event.pattern_match.group(1).decode()
    templates = ext.get_keyword_templates()
    
    template = None
    for t in templates.get('stop_words', []):
        if t['file'] == template_file:
            template = t
            break
    
    if not template:
        await event.respond("❌ Шаблон не найден!")
        return
    
    chains = ext.get_all_chains()
    buttons = []
    row = []
    
    for chain in chains:
        row.append(Button.inline(f"🔗 Chain {chain['num']}", f"confirm_stop_{template_file}_{chain['num']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([Button.inline("🔄 Применить ко всем", f"confirm_stop_{template_file}_all")])
    buttons.append([Button.inline("◀️ Назад", "manage_stop_words")])
    
    await event.respond(
        f"🚫 **Применение шаблона**\n\n"
        f"Шаблон: **{template['name']}**\n"
        f"Слов: {template['count']}\n\n"
        f"Выберите цепочку:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'confirm_stop_(.+)_(\d+|all)'))
async def confirm_apply_stop_handler(event):
    """Подтверждение применения stop_words"""
    await event.answer()
    
    match = event.pattern_match
    template_file = match.group(1).decode()
    target = match.group(2).decode()
    
    templates_path = Path(__file__).parent / 'keyword_templates'
    template_path = templates_path / template_file
    
    if not template_path.exists():
        await event.respond("❌ Файл шаблона не найден!")
        return
    
    msg = await event.respond("🔄 Применяю шаблон...")
    
    try:
        success_count = 0
        
        if target == 'all':
            chains = ext.get_all_chains()
            for chain in chains:
                if ext.apply_template_to_chain(chain['num'], 'stop_words_in_title', str(template_path)):
                    success_count += 1
            
            await event.respond(
                f"✅ **Шаблон применён!**\n\n"
                f"Успешно: {success_count}/{len(chains)} цепочек",
                buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
            )
        else:
            chain_num = int(target)
            if ext.apply_template_to_chain(chain_num, 'stop_words_in_title', str(template_path)):
                await event.respond(
                    f"✅ **Шаблон применён!**\n\n"
                    f"Chain {chain_num} обновлён",
                    buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
                )
            else:
                await event.respond("❌ Ошибка применения!")
                
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")


@bot.on(events.NewMessage(pattern=r'^[a-zA-Zа-яА-Я0-9,\s_\-+.]+$'))
async def keywords_input_handler(event):
    """Обработка ввода слов"""
    if event.chat_id not in keywords_state:
        return
    
    state = keywords_state.pop(event.chat_id)
    words = [w.strip().lower() for w in event.raw_text.split(',')]
    
    try:
        for chain_num in [1, 2, 3]:
            filepath = Path(__file__).parent / 'unified' / f'unified_chain{chain_num}.py'
            
            if 'add' in state['action']:
                ext.add_keyword(str(filepath), state['type'], words)
            elif 'del' in state['action']:
                ext.remove_keyword(str(filepath), state['type'], words)
        
        action_text = "добавлены" if 'add' in state['action'] else "удалены"
        keyword_type = "ключевые слова" if 'base' in state['action'] or 'history' in state['action'] else "стоп-слова"
        
        await event.respond(
            f"✅ {keyword_type.capitalize()} {action_text} во всех 3 цепочках:\n"
            f"{', '.join(words)}",
            buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
        )
        
    except Exception as e:
        await event.respond(f"❌ Ошибка: {str(e)}")


@bot.on(events.CallbackQuery(pattern=b'none'))
async def none_handler(event):
    """Обработчик пустых кнопок-разделителей"""
    await event.answer()  # Просто отвечаем, ничего не делаем


# ===================================
# ПРЕСЕТЫ - ГОТОВЫЕ КОМПЛЕКТЫ
# ===================================

@bot.on(events.CallbackQuery(pattern=b'presets_menu'))
async def presets_menu_handler(event):
    """Меню выбора пресетов"""
    await event.answer()
    
    # Получаем список пресетов
    presets = ext.get_all_presets()
    
    if not presets:
        await event.respond(
            "🎯 **ГОТОВЫЕ КОМПЛЕКТЫ**\n\n"
            "❌ Пресеты не найдены!\n\n"
            "Создайте файлы вида:\n"
            "`preset_1_forex.txt`\n"
            "в папке keyword_templates/",
            buttons=[[Button.inline("◀️ Назад", "keywords_editor_menu")]]
        )
        return
    
    # Строим статус активных пресетов
    status_lines = []
    for chain_key in ['chain1', 'chain2', 'chain3']:
        chain_num = chain_key[-1]
        active = active_presets.get(chain_key)
        if active:
            status_lines.append(f"Chain {chain_num}: ✅ **{active}**")
        else:
            status_lines.append(f"Chain {chain_num}: ⚪ _не установлен_")
    
    status_text = "\n".join(status_lines)
    
    # Строим кнопки для пресетов (по 2 в ряд)
    buttons = []
    row = []
    
    for preset in presets:
        row.append(Button.inline(
            f"{preset['icon']} {preset['name']}",
            f"select_preset_{preset['file']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:  # Добавляем последнюю неполную строку
        buttons.append(row)
    
    # Разделитель и кнопки действий
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("🔀 Объединить несколько пресетов", "merge_presets_menu")])
    buttons.append([Button.inline("◀️ Назад", "keywords_editor_menu")])
    
    await event.respond(
        "🎯 **СМЕНИТЬ ТЕМАТИКУ ПОИСКА**\n\n"
        f"📦 Доступно тематик: **{len(presets)}**\n\n"
        "💡 **Зачем это нужно?**\n"
        "Меняйте тематику в зависимости от задачи:\n"
        "• Сегодня ищем FOREX чаты\n"
        "• Завтра переключаемся на СТРОЙКУ\n"
        "• Послезавтра на АВТО\n\n"
        "**📊 Сейчас активны:**\n"
        f"{status_text}\n\n"
        "**Выберите действие:**\n"
        "• Нажмите на пресет для применения одного\n"
        "• Или нажмите **🔀 Объединить** для выбора нескольких\n\n"
        "_(каждый пресет включает base, history, stop слова)_",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'select_preset_(.+)'))
async def select_preset_handler(event):
    """Выбор цепочки для применения пресета"""
    await event.answer()
    
    preset_file = event.pattern_match.group(1).decode('utf-8')
    
    # Получаем информацию о пресете
    presets = ext.get_all_presets()
    preset_info = next((p for p in presets if p['file'] == preset_file), None)
    
    if not preset_info:
        await event.respond("❌ Пресет не найден!")
        return
    
    # Получаем список цепочек
    chains = ext.get_all_chains()
    
    # Строим кнопки для цепочек (по 2 в ряд)
    buttons = []
    row = []
    
    for chain in chains:
        row.append(Button.inline(
            f"🔗 {chain['display_name']}",
            f"apply_preset_{preset_file}_chain{chain['num']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Кнопка "Применить ко всем"
    buttons.append([Button.inline("🔄 Применить ко ВСЕМ цепочкам", f"apply_preset_{preset_file}_chainall")])
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("◀️ Назад", "presets_menu")])
    
    await event.respond(
        f"{preset_info['icon']} **{preset_info['name'].upper()}**\n\n"
        f"Выберите цепочку для применения:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'apply_preset_(.+)_chain(.+)'))
async def apply_preset_handler(event):
    """Применение пресета к цепочке"""
    await event.answer()
    
    preset_file = event.pattern_match.group(1).decode('utf-8')
    chain_id = event.pattern_match.group(2).decode('utf-8')
    
    # Получаем информацию о пресете
    presets = ext.get_all_presets()
    preset_info = next((p for p in presets if p['file'] == preset_file), None)
    
    if not preset_info:
        await event.respond("❌ Пресет не найден!")
        return
    
    # Подтверждение
    if chain_id == 'all':
        chain_text = "ВСЕМ цепочкам"
    else:
        chain_text = f"Chain {chain_id}"
    
    buttons = [
        [
            Button.inline("✅ Да, применить", f"confirm_preset_{preset_file}_chain{chain_id}"),
            Button.inline("❌ Отмена", "presets_menu")
        ]
    ]
    
    await event.respond(
        f"⚠️ **ПОДТВЕРЖДЕНИЕ**\n\n"
        f"Применить пресет\n"
        f"**{preset_info['icon']} {preset_info['name']}**\n"
        f"к **{chain_text}**?\n\n"
        f"_Это заменит все текущие слова (base, history, stop)_",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'confirm_preset_(.+)_chain(.+)'))
async def confirm_preset_handler(event):
    """Подтверждение и выполнение применения пресета"""
    await event.answer()
    
    preset_file = event.pattern_match.group(1).decode('utf-8')
    chain_id = event.pattern_match.group(2).decode('utf-8')
    
    # Получаем путь к пресету
    presets = ext.get_all_presets()
    preset_info = next((p for p in presets if p['file'] == preset_file), None)
    
    if not preset_info:
        await event.respond("❌ Пресет не найден!")
        return
    
    await event.respond("⏳ Применяю пресет...")
    
    try:
        # Применяем пресет
        if chain_id == 'all':
            result = ext.apply_preset_to_chain('all', preset_info['path'])
        else:
            result = ext.apply_preset_to_chain(int(chain_id), preset_info['path'])
        
        if result['success']:
            # Сохраняем информацию об активном пресете
            preset_display_name = f"{preset_info['icon']} {preset_info['name']}"
            for chain_num in result['chains_updated']:
                active_presets[f'chain{chain_num}'] = preset_display_name
            save_active_presets()
            
            chains_text = ', '.join([f"Chain {n}" for n in result['chains_updated']])
            
            await event.respond(
                f"✅ **УСПЕШНО!**\n\n"
                f"Пресет **{preset_info['name']}** применён к:\n"
                f"**{chains_text}**\n\n"
                f"Обновлены все 3 типа слов:\n"
                f"• 📝 Base Title Keywords\n"
                f"• 📜 History Keywords\n"
                f"• 🚫 Stop Words",
                buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
            )
        else:
            error_text = '\n'.join(result['errors'])
            await event.respond(
                f"❌ **ОШИБКА ПРИМЕНЕНИЯ**\n\n"
                f"{error_text}",
                buttons=[[Button.inline("🔄 Попробовать снова", "presets_menu")]]
            )
            
    except Exception as e:
        await event.respond(
            f"❌ Ошибка: {str(e)}",
            buttons=[[Button.inline("◀️ Назад", "presets_menu")]]
        )


# ===================================
# МНОЖЕСТВЕННЫЙ ВЫБОР ПРЕСЕТОВ
# ===================================

# Хранилище выбранных пресетов для каждого пользователя
user_selected_presets = {}

@bot.on(events.CallbackQuery(pattern=b'merge_presets_menu'))
async def merge_presets_menu_handler(event):
    """Меню для выбора нескольких пресетов"""
    await event.answer()
    
    user_id = event.sender_id
    
    # Инициализируем хранилище для пользователя
    if user_id not in user_selected_presets:
        user_selected_presets[user_id] = []
    
    # Получаем список пресетов
    presets = ext.get_all_presets()
    
    if not presets:
        await event.respond(
            "❌ Пресеты не найдены!",
            buttons=[[Button.inline("◀️ Назад", "presets_menu")]]
        )
        return
    
    # Строим кнопки для пресетов (по 2 в ряд)
    buttons = []
    row = []
    
    for preset in presets:
        # Проверяем выбран ли пресет
        is_selected = preset['file'] in user_selected_presets[user_id]
        check_mark = "✅ " if is_selected else ""
        
        row.append(Button.inline(
            f"{check_mark}{preset['icon']} {preset['name']}",
            f"toggle_preset_{preset['file']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Кнопки действий
    selected_count = len(user_selected_presets[user_id])
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    
    if selected_count >= 2:
        buttons.append([Button.inline(f"🔀 Применить {selected_count} пресетов", "apply_merged_presets")])
    
    buttons.append([Button.inline("🗑️ Очистить выбор", "clear_preset_selection")])
    buttons.append([Button.inline("◀️ Назад", "presets_menu")])
    
    await event.respond(
        "🔀 **ОБЪЕДИНЕНИЕ ПРЕСЕТОВ**\n\n"
        f"📦 Выбрано: **{selected_count}** из **{len(presets)}**\n\n"
        "Выберите 2 или более пресетов для объединения:\n"
        "_(все слова будут объединены в один комплект)_\n\n"
        "✅ = выбрано",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'toggle_preset_(.+)'))
async def toggle_preset_handler(event):
    """Переключение выбора пресета"""
    await event.answer()
    
    user_id = event.sender_id
    preset_file = event.pattern_match.group(1).decode('utf-8')
    
    # Инициализируем хранилище
    if user_id not in user_selected_presets:
        user_selected_presets[user_id] = []
    
    # Переключаем выбор
    if preset_file in user_selected_presets[user_id]:
        user_selected_presets[user_id].remove(preset_file)
    else:
        user_selected_presets[user_id].append(preset_file)
    
    # Обновляем меню
    await merge_presets_menu_handler(event)


@bot.on(events.CallbackQuery(pattern=b'clear_preset_selection'))
async def clear_preset_selection_handler(event):
    """Очистка выбора пресетов"""
    await event.answer()
    
    user_id = event.sender_id
    user_selected_presets[user_id] = []
    
    # Обновляем меню
    await merge_presets_menu_handler(event)


@bot.on(events.CallbackQuery(pattern=b'apply_merged_presets'))
async def apply_merged_presets_handler(event):
    """Выбор цепочки для применения объединённых пресетов"""
    await event.answer()
    
    user_id = event.sender_id
    selected_files = user_selected_presets.get(user_id, [])
    
    if len(selected_files) < 2:
        await event.respond(
            "❌ Выберите минимум 2 пресета!",
            buttons=[[Button.inline("◀️ Назад", "merge_presets_menu")]]
        )
        return
    
    # Получаем информацию о выбранных пресетах
    all_presets = ext.get_all_presets()
    selected_presets = [p for p in all_presets if p['file'] in selected_files]
    
    # Получаем список цепочек
    chains = ext.get_all_chains()
    
    # Строим кнопки для цепочек (по 2 в ряд)
    buttons = []
    row = []
    
    for chain in chains:
        row.append(Button.inline(
            f"🔗 {chain['display_name']}",
            f"confirm_merged_chain{chain['num']}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Кнопка "Применить ко всем"
    buttons.append([Button.inline("🔄 Применить ко ВСЕМ цепочкам", "confirm_merged_chainall")])
    buttons.append([Button.inline("━━━━━━━━━━━━━━━━━━━━", "none")])
    buttons.append([Button.inline("◀️ Назад", "merge_presets_menu")])
    
    # Формируем список выбранных пресетов
    preset_list = '\n'.join([f"  {p['icon']} {p['name']}" for p in selected_presets])
    
    await event.respond(
        f"🔀 **ОБЪЕДИНЕНИЕ {len(selected_files)} ПРЕСЕТОВ**\n\n"
        f"Выбранные пресеты:\n{preset_list}\n\n"
        f"Выберите цепочку для применения:",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=rb'confirm_merged_chain(.+)'))
async def confirm_merged_handler(event):
    """Подтверждение и выполнение применения объединённых пресетов"""
    await event.answer()
    
    user_id = event.sender_id
    chain_id = event.pattern_match.group(1).decode('utf-8')
    
    selected_files = user_selected_presets.get(user_id, [])
    
    if len(selected_files) < 2:
        await event.respond("❌ Выберите минимум 2 пресета!")
        return
    
    # Получаем пути к пресетам
    all_presets = ext.get_all_presets()
    preset_paths = [p['path'] for p in all_presets if p['file'] in selected_files]
    
    if chain_id == 'all':
        chain_text = "ВСЕМ цепочкам"
    else:
        chain_text = f"Chain {chain_id}"
    
    await event.respond(f"⏳ Объединяю {len(preset_paths)} пресетов и применяю к {chain_text}...")
    
    try:
        # Применяем объединённые пресеты
        if chain_id == 'all':
            result = ext.apply_multiple_presets_to_chain('all', preset_paths)
        else:
            result = ext.apply_multiple_presets_to_chain(int(chain_id), preset_paths)
        
        if result['success']:
            # Сохраняем информацию об активных пресетах (объединённых)
            preset_names = [p['name'] for p in all_presets if p['file'] in selected_files]
            merged_display = f"🔀 {' + '.join(preset_names[:2])}" + (f" +{len(preset_names)-2}" if len(preset_names) > 2 else "")
            for chain_num in result['chains_updated']:
                active_presets[f'chain{chain_num}'] = merged_display
            save_active_presets()
            
            chains_text = ', '.join([f"Chain {n}" for n in result['chains_updated']])
            counts = result['merged_counts']
            
            await event.respond(
                f"✅ **УСПЕШНО!**\n\n"
                f"Объединено **{len(preset_paths)} пресетов** и применено к:\n"
                f"**{chains_text}**\n\n"
                f"📊 **Итоговое количество слов:**\n"
                f"• 📝 Base Keywords: **{counts['base']}**\n"
                f"• 📜 History Keywords: **{counts['history']}**\n"
                f"• 🚫 Stop Words: **{counts['stop']}**\n\n"
                f"_(дубликаты удалены автоматически)_",
                buttons=[[Button.inline("🏠 Главное меню", "main_menu")]]
            )
            
            # Очищаем выбор пользователя
            user_selected_presets[user_id] = []
        else:
            error_text = '\n'.join(result['errors'])
            await event.respond(
                f"❌ **ОШИБКА ПРИМЕНЕНИЯ**\n\n"
                f"{error_text}",
                buttons=[[Button.inline("🔄 Попробовать снова", "merge_presets_menu")]]
            )
            
    except Exception as e:
        await event.respond(
            f"❌ Ошибка: {str(e)}",
            buttons=[[Button.inline("◀️ Назад", "merge_presets_menu")]]
        )


if __name__ == '__main__':
    main()
