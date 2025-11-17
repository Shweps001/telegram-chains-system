#!/usr/bin/env python3
"""
Модуль глобальной статистики для всех filtr скриптов
Централизованное ведение статистики обработанных и релевантных/нерелевантных ссылок
"""

import os
from datetime import datetime

# === НАСТРОЙКИ ГЛОБАЛЬНОЙ СТАТИСТИКИ ===
# Общая папка статистики (доступна всем filtr скриптам)
GLOBAL_STATS_DIR = os.path.join('..', 'filtr_global_stats')
if not os.path.exists(GLOBAL_STATS_DIR):
    os.makedirs(GLOBAL_STATS_DIR)

# Файлы общей статистики
GLOBAL_STATS_FILE = os.path.join(GLOBAL_STATS_DIR, 'filtr_global_statistics.txt')
GLOBAL_PROCESSED_FILE = os.path.join(GLOBAL_STATS_DIR, 'all_processed_links.txt')
GLOBAL_RELEVANT_FILE = os.path.join(GLOBAL_STATS_DIR, 'all_relevant_links.txt')
GLOBAL_IRRELEVANT_FILE = os.path.join(GLOBAL_STATS_DIR, 'all_irrelevant_links.txt')
GLOBAL_SUMMARY_FILE = os.path.join(GLOBAL_STATS_DIR, 'daily_summary.txt')

def update_global_statistics(filtr_name, processed_count, relevant_count, irrelevant_count):
    """Обновляет глобальную статистику всех filtr скриптов"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Читаем текущую статистику
    global_stats = {}
    if os.path.exists(GLOBAL_STATS_FILE):
        try:
            with open(GLOBAL_STATS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    if '|' in line and not line.startswith('#'):
                        parts = line.strip().split('|')
                        if len(parts) >= 4:
                            name = parts[0].strip()
                            if name in ['filtr1', 'filtr2', 'filtr3']:
                                global_stats[name] = {
                                    'processed': int(parts[1].strip()),
                                    'relevant': int(parts[2].strip()),
                                    'irrelevant': int(parts[3].strip())
                                }
        except:
            pass
    
    # Обновляем статистику для текущего filtr
    if filtr_name not in global_stats:
        global_stats[filtr_name] = {'processed': 0, 'relevant': 0, 'irrelevant': 0}
    
    global_stats[filtr_name]['processed'] += processed_count
    global_stats[filtr_name]['relevant'] += relevant_count
    global_stats[filtr_name]['irrelevant'] += irrelevant_count
    
    # Вычисляем общие итоги
    total_processed = sum(stats['processed'] for stats in global_stats.values())
    total_relevant = sum(stats['relevant'] for stats in global_stats.values())
    total_irrelevant = sum(stats['irrelevant'] for stats in global_stats.values())
    
    # Записываем обновленную статистику
    with open(GLOBAL_STATS_FILE, 'w', encoding='utf-8') as f:
        f.write("# ОБЩАЯ СТАТИСТИКА ВСЕХ FILTR СКРИПТОВ\n")
        f.write(f"# Последнее обновление: {timestamp}\n")
        f.write("#" + "="*70 + "\n\n")
        
        f.write("📊 СТАТИСТИКА ПО СКРИПТАМ:\n")
        f.write("-" * 60 + "\n")
        f.write("Скрипт   | Обработано | Релевантные      | Нерелевантные\n")
        f.write("-" * 60 + "\n")
        
        for filtr_name_iter in ['filtr1', 'filtr2', 'filtr3']:
            if filtr_name_iter in global_stats:
                stats = global_stats[filtr_name_iter]
                percentage = f"({stats['relevant']/stats['processed']*100:.1f}%)" if stats['processed'] > 0 else "(0%)"
                f.write(f"{filtr_name_iter:<8} | {stats['processed']:>10} | {stats['relevant']:>8} {percentage:<8} | {stats['irrelevant']:>13}\n")
            else:
                f.write(f"{filtr_name_iter:<8} | {0:>10} | {0:>8} (0%)     | {0:>13}\n")
        
        f.write("-" * 60 + "\n")
        total_percentage = f"({total_relevant/total_processed*100:.1f}%)" if total_processed > 0 else "(0%)"
        f.write(f"{'ИТОГО':<8} | {total_processed:>10} | {total_relevant:>8} {total_percentage:<8} | {total_irrelevant:>13}\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("📈 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:\n")
        f.write(f"Общая эффективность системы: {total_percentage}\n")
        f.write(f"Последняя активность: {filtr_name} в {timestamp}\n")
        f.write(f"Активных скриптов: {len(global_stats)}\n")
        f.write(f"Всего обработано ссылок: {total_processed:,}\n")
        f.write(f"Найдено релевантных: {total_relevant:,}\n")
        f.write(f"Отфильтровано нерелевантных: {total_irrelevant:,}\n\n")
        
        # Показываем эффективность каждого скрипта
        f.write("🎯 ЭФФЕКТИВНОСТЬ ПО СКРИПТАМ:\n")
        for filtr_name_iter in ['filtr1', 'filtr2', 'filtr3']:
            if filtr_name_iter in global_stats:
                stats = global_stats[filtr_name_iter]
                if stats['processed'] > 0:
                    eff = stats['relevant']/stats['processed']*100
                    status = "🟢 Отлично" if eff >= 20 else "🟡 Хорошо" if eff >= 10 else "🔴 Требует внимания"
                    f.write(f"  {filtr_name_iter}: {eff:.1f}% {status}\n")

def log_to_global_file(link, link_type, filtr_name):
    """Логирует ссылку в соответствующий глобальный файл"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if link_type == 'relevant':
        target_file = GLOBAL_RELEVANT_FILE
        prefix = f"[{timestamp}] [{filtr_name}] РЕЛЕВАНТНАЯ"
    else:
        target_file = GLOBAL_IRRELEVANT_FILE
        prefix = f"[{timestamp}] [{filtr_name}] НЕРЕЛЕВАНТНАЯ"
    
    # Всегда логируем в общий файл обработанных
    with open(GLOBAL_PROCESSED_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] [{filtr_name}] {link} - {link_type.upper()}\n")
    
    # Логируем в специализированный файл
    with open(target_file, 'a', encoding='utf-8') as f:
        f.write(f"{prefix}: {link}\n")

def create_daily_summary():
    """Создает ежедневную сводку статистики"""
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Читаем статистику
    global_stats = {}
    if os.path.exists(GLOBAL_STATS_FILE):
        try:
            with open(GLOBAL_STATS_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Извлекаем итоговую статистику
            lines = content.split('\n')
            for line in lines:
                if line.startswith('ИТОГО'):
                    parts = line.split('|')
                    if len(parts) >= 4:
                        total_processed = int(parts[1].strip())
                        relevant_part = parts[2].strip().split('(')[0].strip()
                        total_relevant = int(relevant_part)
                        total_irrelevant = int(parts[3].strip())
                        
                        # Записываем дневную сводку
                        with open(GLOBAL_SUMMARY_FILE, 'a', encoding='utf-8') as f:
                            f.write(f"\n📅 СВОДКА ЗА {today}:\n")
                            f.write(f"Обработано: {total_processed:,} ссылок\n")
                            f.write(f"Релевантные: {total_relevant:,} ({total_relevant/total_processed*100:.1f}%)\n")
                            f.write(f"Нерелевантные: {total_irrelevant:,} ({total_irrelevant/total_processed*100:.1f}%)\n")
                            f.write("-" * 40 + "\n")
                        break
        except:
            pass

def get_global_stats_summary():
    """Возвращает краткую сводку глобальной статистики"""
    if not os.path.exists(GLOBAL_STATS_FILE):
        return "Статистика еще не создана"
    
    try:
        with open(GLOBAL_STATS_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Извлекаем последние данные
        lines = content.split('\n')
        for line in lines:
            if line.startswith('ИТОГО'):
                parts = line.split('|')
                if len(parts) >= 4:
                    total_processed = int(parts[1].strip())
                    relevant_part = parts[2].strip().split('(')[0].strip()
                    total_relevant = int(relevant_part)
                    percentage = f"{total_relevant/total_processed*100:.1f}%" if total_processed > 0 else "0%"
                    
                    return f"📊 Всего обработано: {total_processed:,} | Релевантные: {total_relevant:,} ({percentage})"
        
        return "Не удалось прочитать статистику"
    except:
        return "Ошибка чтения статистики"

if __name__ == "__main__":
    # Тест функций
    print("🧪 Тестирование модуля глобальной статистики...")
    update_global_statistics('filtr_test', 100, 25, 75)
    print("✅ Тест завершен!")
    print(get_global_stats_summary())