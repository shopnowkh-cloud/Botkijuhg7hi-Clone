import sys
import os

# Project root must be in path before importing main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Tell main.py to run in webhook mode (skips background pollers)
os.environ["BOT_WEBHOOK_MODE"] = "1"

import asyncio
import json
import logging
from http.server import BaseHTTPRequestHandler

from telegram import Update
import main as bot

logger = logging.getLogger(__name__)

# A single persistent event loop reused across all warm invocations
_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)

_initialized = False


async def _init_once():
    global _initialized
    if _initialized:
        return
    bot._register_handlers()          # registers handlers + sets application.post_init
    await bot.application.initialize()  # triggers _on_startup (DB init, load data, etc.)
    await bot.application.start()
    _initialized = True


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b""

        if body:
            async def process():
                await _init_once()
                data = json.loads(body)
                update = Update.de_json(data, bot.application.bot)
                await bot.application.process_update(update)

            try:
                _loop.run_until_complete(process())
            except Exception as e:
                logger.error(f"Webhook processing error: {e}", exc_info=True)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Telegram Bot Webhook OK")

    def log_message(self, format, *args):
        pass
