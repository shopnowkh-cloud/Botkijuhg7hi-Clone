# Telegram Bot — Bakong KHQR Payments

## Overview
A Python Telegram bot that accepts orders, generates Bakong KHQR payment QR codes, and tracks state in a Neon Postgres database via its HTTP `/sql` API. Single-file implementation in `telegram_bot_simple.py`.

## Stack
- **Python 3.11**
- **python-telegram-bot v20+** (Bot API HTTP polling — no MTProto, no API ID/Hash needed)
- `bakong-khqr`, `requests`, `pillow`, `qrcode`, `urllib3`
- `edge-tts`, `imageio-ffmpeg`, `langdetect` (optional TTS sub-bot)
- Neon Postgres (HTTP API, no driver required)

## Architecture
| Feature | Implementation |
|---|---|
| Transport | Bot API HTTPS polling (`run_polling`) |
| Concurrency | Full `asyncio` — concurrent updates enabled |
| Per-user safety | `asyncio.Lock` per user ID |
| Global data lock | `asyncio.Lock` |
| Blocking DB/HTTP calls | `run_sync` thread-pool wrapper |
| Background tasks | `asyncio.create_task` |
| Handler priority | PTB `group=` parameter |
| In-memory cache | `MemCache` (TTL-based, in-process) |

### Handler Groups
| Group | Purpose |
|---|---|
| `-10` | Channel posts |
| `0` | `/start`, `/cancel` commands + `CallbackQueryHandler` |
| `1` | All private text messages (dispatches internally by state) |

All private-message routing (maintenance check, admin states, payment pending, etc.) is handled inside the single `on_private_message` function.

## Required Secrets
Stored in environment / `.env`:
- `TELEGRAM_BOT_TOKEN` — from BotFather
- `NEON_DATABASE_URL` — Neon Postgres connection string

## Optional Env Vars
- `BAKONG_TOKEN` — Bakong KHQR API token (or configure via admin panel)
- `TELEGRAM_CHANNEL_ID` — Notification channel ID
- `DROPMAIL_API_TOKEN` — Dropmail throwaway email service token

## Run
```bash
pip install -r requirements.txt
python telegram_bot_simple.py
```

## Deploy to VPS (24/7 via systemd)

### Files included for VPS deployment
| File | Purpose |
|---|---|
| `setup.sh` | One-time setup script (run as root on Ubuntu/Debian) |
| `telegram-bot.service` | systemd service — auto-start, auto-restart on crash |
| `.env.example` | Template for environment variables |

### Step-by-step (Termius / any SSH client)

```bash
# 1. Upload files to VPS
scp telegram_bot_simple.py requirements.txt setup.sh telegram-bot.service .env.example root@YOUR_VPS_IP:/root/

# 2. SSH into VPS
ssh root@YOUR_VPS_IP

# 3. Run setup
chmod +x setup.sh && sudo bash setup.sh

# 4. Create your .env file
cp /root/.env.example /opt/telegram-bot/.env
nano /opt/telegram-bot/.env   # fill in TELEGRAM_BOT_TOKEN and NEON_DATABASE_URL

# 5. Start the bot
systemctl start telegram-bot

# 6. Check it's running
systemctl status telegram-bot

# 7. Watch live logs
journalctl -u telegram-bot -f
```

### Useful commands
```bash
systemctl stop telegram-bot        # Stop bot
systemctl restart telegram-bot     # Restart bot
journalctl -u telegram-bot -n 100  # Last 100 log lines
```

### Notes
- **Only 2 secrets required**: `TELEGRAM_BOT_TOKEN` and `NEON_DATABASE_URL` (no API ID/Hash needed)
- **BAKONG_TOKEN** is optional — loadable via admin panel ⚙️ Settings → 🔑 Bakong Token
- **Database data is preserved** — Neon Postgres is cloud-hosted

## Admin-Managed Settings (persisted in `bot_settings` DB table)
| Key | Description |
|---|---|
| `PAYMENT_NAME` | Merchant name shown on KHQR |
| `MAINTENANCE_MODE` | `true`/`false` — blocks non-admin users |
| `BAKONG_RELAY_TOKEN` | Relay token (takes priority) |
| `BAKONG_API_TOKEN` | Direct Bakong JWT token |
| `TELEGRAM_CHANNEL_ID` | Notification channel |
| `EXTRA_ADMIN_IDS` | JSON array of additional admin user IDs |
| `TTS_BOT_TOKEN` | Token for the standalone TTS sub-bot |

## Primary Admin
Hardcoded: `ADMIN_ID = 5002402843`. Additional admins managed via the ⚙️ settings menu.
