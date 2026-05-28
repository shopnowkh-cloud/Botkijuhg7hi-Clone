import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["BOT_WEBHOOK_MODE"] = "1"

import asyncio
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler

from telegram import Update
import main as bot

logger = logging.getLogger(__name__)

# ── Persistent background event loop (one per warm Vercel instance) ────────────
_loop = asyncio.new_event_loop()
threading.Thread(target=_loop.run_forever, daemon=True, name="bot-loop").start()

# Initialisation state — guarded by _initialising flag (event-loop is single-threaded)
_initialized   = False
_initialising  = False


def _run(coro, timeout: int = 28):
    """Submit a coroutine to the bot loop and block until done."""
    future = asyncio.run_coroutine_threadsafe(coro, _loop)
    return future.result(timeout=timeout)


async def _init_once():
    """Initialize PTB application exactly once per process lifetime."""
    global _initialized, _initialising
    if _initialized:
        return
    # Spin-wait if another coroutine is already initialising
    if _initialising:
        while _initialising:
            await asyncio.sleep(0.05)
        return
    _initialising = True
    try:
        bot._register_handlers()
        # Disable post_init — PTB skips it if application._initialized is already True
        # (happens on retry after a partial cold-start failure).
        # We call _on_startup explicitly below so it always runs.
        bot.application.post_init = None
        await bot.application.initialize()
        await bot._on_startup(bot.application)   # always explicit
        if not bot.application.running:
            await bot.application.start()
        _initialized = True
        logger.info("Bot application initialised (webhook mode)")
    except Exception:
        logger.exception("Bot initialisation failed")
        raise
    finally:
        _initialising = False


async def _process(body: bytes):
    await _init_once()
    data   = json.loads(body)
    update = Update.de_json(data, bot.application.bot)
    await bot.application.process_update(update)


# ── Vercel serverless handler ──────────────────────────────────────────────────
class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b""

        # ── Return 200 IMMEDIATELY so Telegram never retries ──────────────────
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "10")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')
        self.wfile.flush()
        # ─────────────────────────────────────────────────────────────────────

        if body:
            try:
                _run(_process(body))
            except Exception as e:
                logger.error(f"Webhook processing error: {e}", exc_info=True)

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Telegram Bot Webhook OK")

    def log_message(self, format, *args):
        pass
