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
    from telegram_bot_simple import application, _register_handlers
    _register_handlers()
    await application.initialize()
    _initialized = True


class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        from telegram import Update
        from telegram_bot_simple import application

        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)

        try:
            update_data = json.loads(body)
            _loop.run_until_complete(_init_app())
            update = Update.de_json(update_data, application.bot)
            _loop.run_until_complete(application.process_update(update))
        except Exception as e:
            logger.error(f"Webhook error: {e}", exc_info=True)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'Telegram Bot Webhook - OK')

    def log_message(self, format, *args):
        logger.info('[HTTP] ' + format % args)
