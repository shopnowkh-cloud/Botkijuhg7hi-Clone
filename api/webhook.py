import sys
import os
import json
import asyncio
import logging
from http.server import BaseHTTPRequestHandler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'Telegram-Bot'))

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

_loop = asyncio.new_event_loop()
asyncio.set_event_loop(_loop)
_initialized = False


async def _init_app():
    global _initialized
    if _initialized:
        return
    try:
        from telegram_bot_simple import application, _register_handlers, _on_startup
        _register_handlers()
        await application.initialize()
        # post_init is NOT called by initialize() — only by run_polling/run_webhook
        # so we invoke _on_startup manually to init DB, load data, and settings
        await _on_startup(application)
        _initialized = True
        logger.info("Bot application initialized via webhook cold start")
    except Exception as e:
        logger.error(f"Failed to initialize bot application: {e}", exc_info=True)
        raise


async def _handle_update(body: bytes):
    from telegram import Update
    from telegram_bot_simple import application

    await _init_app()
    update_data = json.loads(body)
    update = Update.de_json(update_data, application.bot)
    await application.process_update(update)


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        try:
            _loop.run_until_complete(_handle_update(body))
        except Exception as e:
            logger.error(f"Webhook error: {e}", exc_info=True)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def do_GET(self):
        status = "initialized" if _initialized else "cold"
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"status": status, "ok": True}).encode())

    def log_message(self, format, *args):
        logger.info('[HTTP] ' + format % args)
