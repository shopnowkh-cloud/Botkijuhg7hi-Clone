# Telegram Bot — Bakong KHQR Payments

A Telegram bot that facilitates payments using the Bakong KHQR system (Cambodia).

## Run & Operate

- `python3.11 main.py` — run the bot (polling mode)
- Required secrets: `TELEGRAM_BOT_TOKEN`, `BAKONG_TOKEN`, `TELEGRAM_CHANNEL_ID`

## Stack

- Python 3.11
- python-telegram-bot (asyncio, job-queue)
- bakong-khqr — KHQR payment generation
- psycopg2 — PostgreSQL database
- aiohttp, requests, qrcode, pillow

## Where things live

- `main.py` — entire bot (single file)
- `requirements.txt` — Python dependencies

## User preferences

- Single Python file only, no Node.js or TypeScript.
