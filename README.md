# 🤖 Telegram Chains System

Automated system for managing Telegram groups with intelligent filtering and monitoring.

## ⚠️ IMPORTANT SECURITY NOTICE

**Before running the system:**
1. Copy `config_template.py` to `config.py`
2. Fill in your real credentials in `config.py`
3. **NEVER** commit `config.py` to Git!

## 📋 Features

- **Master Bot**: Full control via Telegram bot interface
- **3 Independent Chains**: Parallel processing with separate accounts
- **Smart 3-Stage Filtering**:
  - Stage 1: Title keywords & stop words check
  - Stage 2: Message history analysis (100 messages)
  - Stage 3: Activity verification (unique users & messages)
- **FloodWait Protection**: Adaptive delays to avoid Telegram limits
- **Preset System**: 20+ ready keyword sets (Forex, IT, Construction, etc.)
- **Parser Integration**: Extract usernames from groups
- **Database Tracking**: SQLite database for processed chats
- **Account Cleaner**: Mass leave from groups

## 🏗️ Architecture

```
Parser → Probiv → Unified Chain → Database
   ↓         ↓           ↓
Usernames  Links    Joined Groups
```

### Components:

- **Parser (Parsilka)**: Extracts @username from groups
- **Probiv**: Extracts group links from usernames
- **Unified Chain**: Joins groups after 3-stage verification
- **Master Bot**: Central control system via Telegram

## 📦 Installation

### 1. Clone Repository

```bash
git clone https://github.com/your_username/telegram-chains-system.git
cd telegram-chains-system
```

### 2. Install Dependencies

```bash
pip install telethon asyncio sqlite3 rapidfuzz transliterate
```

### 3. Configure Credentials

```bash
# Copy template
cp config_template.py config.py

# Edit config.py with your data
nano config.py
```

**Get your credentials:**
- API credentials: https://my.telegram.org
- Bot token: @BotFather
- Telegram ID: @userinfobot

### 4. Create Sessions

Run each script once to authorize:

```bash
# Chain 1
python unified/unified_chain1.py

# Chain 2
python unified/unified_chain2.py

# Chain 3
python unified/unified_chain3.py
```

Enter phone number and verification code for each account.

### 5. Start Master Bot

```bash
python master.py
```

Send `/start` to your bot in Telegram.

## 🎯 Usage

### Quick Start via Bot:

1. Open Telegram → Your Bot
2. `/start` → Main Menu
3. **"▶️ СТАРТ: Запустить систему"**
4. **"🎯 Сменить тематику"** → Choose preset (e.g., Forex & Trading)
5. **"⚙️ Unified Chains управление"** → **"🚀 Запустить ВСЕ"**

System will automatically:
- Fetch links from channels
- Verify each chat (3-stage filtering)
- Join only relevant groups
- Track everything in database

### Monitor Progress:

```
📊 Статус системы → Real-time statistics
📋 Логи → View logs
🗄️ База данных → Database stats
⏳ FloodWait → Monitor limits
```

## 🔧 Configuration

### Chain Parameters

Each chain has unique settings:

**Chain 1 (Aggressive):**
- Links per cycle: 3-7
- Pause between cycles: 5-20 min
- Check delay: 25-50 sec

**Chain 2 (Cautious):**
- Links per cycle: 1-4
- Pause between cycles: 8-30 min
- Check delay: 40-75 sec

**Chain 3 (Balanced):**
- Links per cycle: 2-5
- Pause between cycles: 6-25 min
- Check delay: 30-60 sec

### Filtering Parameters

```python
min_messages = 5              # Min messages/users
lookback_days = 7             # Activity period (days)
min_keyword_hits = 5          # Min keyword matches
min_cyrillic_percent = 30     # Min cyrillic percentage
history_check_limit = 100     # Messages to analyze
```

## 🎨 Preset System

20 ready themes available:

1. 🏦 Forex & Trading
2. 🏗️ Construction
3. 💻 IT & Tech
4. 📢 Marketing & SMM
5. 🏠 Real Estate
6. 🚗 Auto & Transport
7. ⚕️ Healthcare
8. 📚 Education
9. 💰 Finance & Banking
10. 🛒 E-commerce
... and 10 more

### Quick Theme Switch:

```
Bot → 🎯 Сменить тематику → Choose preset → Apply to chains
```

Instantly updates:
- Base title keywords (~70 words)
- History keywords (~80 words)
- Stop words (~500 words)

## 📊 Database

SQLite database tracks all processed chats:

```sql
CREATE TABLE processed_chats (
    id INTEGER PRIMARY KEY,
    link TEXT UNIQUE,
    chain_num INTEGER,
    added_date TIMESTAMP
);
```

**Features:**
- View last 50 records
- Filter by chain
- Export to file
- Find duplicates
- Cleanup old records

## 🛡️ FloodWait Protection

Adaptive system reacts to Telegram limits:

**Light FloodWait** (< 5 min):
- Increases delays by 20% (1.2x)
- Duration: 1 hour

**Medium FloodWait** (5-60 min):
- Increases delays by 50% (1.5x)
- Duration: 2 hours

**Heavy FloodWait** (> 1 hour):
- Doubles all delays (2.0x)
- Duration: 2 hours

**Random Pauses** (human-like behavior):
- 10% chance: 30-60 min pause
- 5% chance: 60-120 min pause
- 2% chance: 180-360 min pause

## 📱 Mobile Access

Edit code from phone:

### Option 1: GitHub Mobile App
- Install: [iOS](https://apps.apple.com/app/github/id1477376905) | [Android](https://play.google.com/store/apps/details?id=com.github.android)
- Open repository → Edit file → Commit changes
- Pull on server: `git pull`

### Option 2: GitHub.dev (Web)
- Open: `https://github.dev/your_username/repo_name`
- Full VS Code in browser
- Edit → Commit & Push
- Pull on server: `git pull`

## 🗑️ Account Cleaner

Leave all groups from account:

```
Bot → 🗑️ Очистить аккаунт → Choose chain → Confirm
```

**What it cleans:**
- ✅ Supergroups (public)
- ✅ Regular groups (private)
- ❌ Channels (skipped)
- ❌ Personal chats (skipped)

## 👥 Parser (Parsilka)

Extract usernames from group:

```
Bot → 👥 Парсер → Choose chain → Send group link
```

**Output:**
- File with usernames
- Auto-send to channel
- Ready for Probiv processing

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

## 📝 License

Private project - All rights reserved

## ⚠️ Disclaimer

This tool is for educational purposes only. Use responsibly and respect Telegram's Terms of Service. The authors are not responsible for any misuse.

## 🆘 Support

- Issues: [GitHub Issues](https://github.com/your_username/telegram-chains-system/issues)
- Telegram: @your_support_username

## 🌟 Star History

If this project helped you, please ⭐ star it on GitHub!

---

**Made with ❤️ for automation enthusiasts**
