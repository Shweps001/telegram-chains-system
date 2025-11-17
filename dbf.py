# -*- coding: utf-8 -*-
import sqlite3
from pathlib import Path
from datetime import datetime
import sys

# Импортируем categories для отображения названий
try:
    from categories import get_category_emoji, get_category_name
except ImportError:
    def get_category_emoji(cat):
        return '📋'
    def get_category_name(cat):
        return cat

DB_PATH = Path(__file__).parent / "shared_chats.db"

def view_recent():
    """Show last 50 chats"""
    if not DB_PATH.exists():
        print("DB not found")
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM processed_chats")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM processed_chats WHERE chain_num=1")
    c1 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM processed_chats WHERE chain_num=2")
    c2 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM processed_chats WHERE chain_num=3")
    c3 = cursor.fetchone()[0]
    print(f"\nTotal: {total}")
    print(f"Chain1: {c1}, Chain2: {c2}, Chain3: {c3}\n")
    cursor.execute("SELECT link,chain_num,processed_at,category,matched_keywords FROM processed_chats ORDER BY processed_at DESC LIMIT 50")
    for i,(link,ch,ts,cat,kws) in enumerate(cursor.fetchall(),1):
        emoji = get_category_emoji(cat) if cat else '📋'
        cat_name = get_category_name(cat) if cat else 'N/A'
        kws_display = f" | KW: {kws[:30]}" if kws else ""
        print(f"{i}. [{ch}] {emoji} {cat_name} | {link}{kws_display} ({ts})")
    conn.close()

def view_all():
    """Show ALL chats"""
    if not DB_PATH.exists():
        print("DB not found")
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM processed_chats")
    total = cursor.fetchone()[0]
    print(f"\nTotal chats: {total}\n")
    cursor.execute("SELECT link,chain_num,processed_at FROM processed_chats ORDER BY processed_at DESC")
    for i,(link,ch,ts) in enumerate(cursor.fetchall(),1):
        print(f"{i}. [{ch}] {link} ({ts})")
    conn.close()

def search_chat():
    """Search by link"""
    term = input("Enter search term: ").strip()
    if not term:
        return
    if not DB_PATH.exists():
        print("DB not found")
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT link,chain_num,processed_at FROM processed_chats WHERE link LIKE ? ORDER BY processed_at DESC", (f"%{term}%",))
    results = cursor.fetchall()
    if results:
        print(f"\nFound {len(results)} matches:\n")
        for i,(link,ch,ts) in enumerate(results,1):
            print(f"{i}. [{ch}] {link} ({ts})")
    else:
        print("No matches found")
    conn.close()

def view_by_chain():
    """Show chats by chain"""
    chain = input("Chain number (1/2/3): ").strip()
    if chain not in ['1','2','3']:
        print("Invalid chain")
        return
    if not DB_PATH.exists():
        print("DB not found")
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT link,processed_at,category,matched_keywords FROM processed_chats WHERE chain_num=? ORDER BY processed_at DESC", (int(chain),))
    results = cursor.fetchall()
    print(f"\nChain {chain}: {len(results)} chats\n")
    for i,(link,ts,cat,kws) in enumerate(results,1):
        emoji = get_category_emoji(cat) if cat else '📋'
        cat_name = get_category_name(cat) if cat else 'N/A'
        kws_display = f" | KW: {kws[:30]}" if kws else ""
        print(f"{i}. {emoji} {cat_name} | {link}{kws_display} ({ts})")
    conn.close()

def view_by_category():
    """Show chats by category"""
    if not DB_PATH.exists():
        print("DB not found")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Показываем доступные категории
    cursor.execute("SELECT category, COUNT(*) FROM processed_chats GROUP BY category ORDER BY COUNT(*) DESC")
    categories = cursor.fetchall()
    
    print("\n📊 ДОСТУПНЫЕ КАТЕГОРИИ:")
    print("=" * 60)
    for cat, cnt in categories:
        emoji = get_category_emoji(cat) if cat else '📋'
        cat_name = get_category_name(cat) if cat else cat
        print(f"{emoji} {cat_name}: {cnt} чатов")
    
    print("\n" + "=" * 60)
    category = input("Введите название категории (например: forex, crypto, hr): ").strip().lower()
    
    cursor.execute("SELECT link,chain_num,processed_at,matched_keywords FROM processed_chats WHERE LOWER(category)=? ORDER BY processed_at DESC LIMIT 100", (category,))
    results = cursor.fetchall()
    
    if results:
        emoji = get_category_emoji(category)
        print(f"\n{emoji} Категория '{category}': {len(results)} чатов\n")
        for i,(link,ch,ts,kws) in enumerate(results,1):
            kws_display = f" | KW: {kws[:40]}" if kws else ""
            print(f"{i}. [CH{ch}] {link}{kws_display} ({ts})")
    else:
        print(f"Чаты категории '{category}' не найдены")
    
    conn.close()

def clear_db():
    """Clear database"""
    confirm = input("Delete ALL data? (yes/no): ").strip().lower()
    if confirm == 'yes':
        if not DB_PATH.exists():
            print("DB not found")
            return
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM processed_chats")
        conn.commit()
        print(f"Deleted {cursor.rowcount} records")
        conn.close()
    else:
        print("Cancelled")

def export_txt():
    """Export to TXT"""
    if not DB_PATH.exists():
        print("DB not found")
        return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT link,chain_num,processed_at FROM processed_chats ORDER BY processed_at")
    results = cursor.fetchall()
    conn.close()
    filename = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"Exported: {datetime.now()}\n")
        f.write(f"Total: {len(results)}\n\n")
        for link,ch,ts in results:
            f.write(f"[{ch}] {link} | {ts}\n")
    print(f"Exported {len(results)} chats to {filename}")

if __name__ == "__main__":
    while True:
        print("\n" + "="*70)
        print("DATABASE VIEWER WITH CATEGORIES")
        print("="*70)
        print("1. Show last 50 chats")
        print("2. Show ALL chats")
        print("3. Search chat")
        print("4. Show by chain")
        print("5. 📊 Show by CATEGORY (forex, crypto, hr, etc.)")
        print("6. Export to TXT")
        print("7. Clear database")
        print("0. Exit")
        print("="*70)
        
        choice = input("\nChoice: ").strip()
        
        if choice == '0':
            print("Bye!")
            break
        elif choice == '1':
            view_recent()
        elif choice == '2':
            view_all()
        elif choice == '3':
            search_chat()
        elif choice == '4':
            view_by_chain()
        elif choice == '5':
            view_by_category()
        elif choice == '6':
            export_txt()
        elif choice == '7':
            clear_db()
        else:
            print("Invalid choice")
