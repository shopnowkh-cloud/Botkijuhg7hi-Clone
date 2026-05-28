#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Bot — Bakong KHQR Payments
Architecture: python-telegram-bot (Bot API) | Full asyncio | Priority handlers | Memory cache | Filters
"""

# ── 1. Imports ───────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import asyncio
import hashlib
import html
import io
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote as url_quote

import unicodedata
from collections import OrderedDict
from io import BytesIO

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from langdetect import detect as _langdetect_detect, detect_langs as _langdetect_langs, DetectorFactory as _DetectorFactory
    _DetectorFactory.seed = 0
    _LANGDETECT_AVAILABLE = True
except ImportError:
    _LANGDETECT_AVAILABLE = False

from telegram import (
    Bot, Update,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, BadRequest, Forbidden, RetryAfter
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes,
    filters as ptb_filters,
)

# ── 2. Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ── 2b. Environment Validation ────────────────────────────────────────────────
_REQUIRED_ENV_VARS = {
    "TELEGRAM_BOT_TOKEN": "Bot token from @BotFather on Telegram",
}

def _validate_env() -> None:
    missing = []
    for key, description in _REQUIRED_ENV_VARS.items():
        val = os.environ.get(key, "").strip()
        if not val:
            missing.append((key, description))

    if missing:
        logger.error("=" * 60)
        logger.error("STARTUP FAILED — Missing required environment variables:")
        logger.error("=" * 60)
        for key, description in missing:
            logger.error(f"  ❌  {key}")
            logger.error(f"       └─ {description}")
        logger.error("=" * 60)
        logger.error("Set these variables in your environment (e.g. .env file")
        logger.error("or VPS environment) and restart the bot.")
        logger.error("=" * 60)
        sys.exit(1)

    logger.info("All required environment variables are present. ✓")

_validate_env()

# ── 3. Config ────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_MODE = os.environ.get("BOT_WEBHOOK_MODE", "") == "1"

ADMIN_ID: int = 5002402843
MAINTENANCE_MODE = False
PAYMENT_TIMEOUT_SECONDS = 60
PAYMENT_POLL_INTERVAL   = 10
KHMER_MESSAGE = "ជ្រើសរើស គូប៉ុង ដើម្បីបញ្ជាទិញ"

RELAY_API_BASE = "https://bakong.cambo-kh.com/api/payment"

_out_of_stock_msg: dict = {}


DROPMAIL_API_TOKEN    = os.environ.get("DROPMAIL_API_TOKEN", "")
DROPMAIL_TOKEN_EXPIRY = ""
_DROPMAIL_URL         = f"https://dropmail.me/api/graphql/{DROPMAIL_API_TOKEN}"


def is_admin(uid) -> bool:
    try:
        return int(uid) == ADMIN_ID
    except (TypeError, ValueError):
        return False


# ── 4. Blocking HTTP session (DB + Bakong, run in thread pool) ────────────────
_retry = Retry(
    total=3, backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"], raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=50)
http = requests.Session()
http.headers.update({"Connection": "keep-alive"})
http.mount("https://", _adapter)
http.mount("http://",  _adapter)

# ── 5. In-Memory Cache ────────────────────────────────────────────────────────
class MemCache:
    """Fast TTL-based in-memory cache.  Thread-safe for asyncio (single event loop)."""

    def __init__(self):
        self._data: dict = {}
        self._exp:  dict = {}

    def get(self, key, default=None):
        if key in self._data:
            if self._exp.get(key, float("inf")) > time.monotonic():
                return self._data[key]
            del self._data[key]
            self._exp.pop(key, None)
        return default

    def set(self, key, value, ttl: float = None):
        self._data[key] = value
        if ttl is not None:
            self._exp[key] = time.monotonic() + ttl
        else:
            self._exp.pop(key, None)

    def delete(self, key):
        self._data.pop(key, None)
        self._exp.pop(key, None)

    def clear(self):
        self._data.clear()
        self._exp.clear()


cache = MemCache()

# ── 6. Async primitives ───────────────────────────────────────────────────────
_data_lock = asyncio.Lock()
_user_locks: dict = {}


def get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _user_locks:
        _user_locks[user_id] = asyncio.Lock()
    return _user_locks[user_id]


async def run_sync(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ── 7a. PostgreSQL (Neon) DB setup ────────────────────────────────────────────
import psycopg2
import psycopg2.extras
import psycopg2.pool

_DB_URL = os.environ.get("DATABASE_URL_BOT", "") or os.environ.get("DATABASE_URL", "")
_db_pool = None
_db_pool_lock = threading.Lock()


def _get_db_pool():
    global _db_pool
    if _db_pool is not None:
        return _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            _db_pool = psycopg2.pool.ThreadedConnectionPool(1, 10, _DB_URL)
    return _db_pool


def _db_query(query: str, params=None) -> dict:
    pool = _get_db_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or [])
            conn.commit()
            try:
                rows = [dict(r) for r in cur.fetchall()]
            except psycopg2.ProgrammingError:
                rows = []
        return {"rows": rows}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise RuntimeError(f"DB error: {e}") from e
    finally:
        pool.putconn(conn)


# ── 7b. Bot Application ────────────────────────────────────────────────────────
application: Application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .concurrent_updates(True)
    .build()
)
_bot: Bot = application.bot

# ── 8. Database layer ──────────────────────────────────────────────────────────

def _init_db():
    _ddl_statements = [
        """CREATE TABLE IF NOT EXISTS bot_accounts (
                id SERIAL PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}'
            )""",
        """CREATE TABLE IF NOT EXISTS bot_sessions (
                id SERIAL PRIMARY KEY,
                data TEXT NOT NULL DEFAULT '{}'
            )""",
        """CREATE TABLE IF NOT EXISTS bot_pending_payments (
                user_id BIGINT PRIMARY KEY, chat_id BIGINT NOT NULL,
                account_type TEXT, quantity INTEGER, total_price REAL,
                md5_hash TEXT, qr_message_id BIGINT,
                reserved_accounts TEXT DEFAULT '[]',
                created_at TIMESTAMP DEFAULT NOW()
            )""",
        """CREATE TABLE IF NOT EXISTS bot_purchase_history (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                account_type TEXT, quantity INTEGER, total_price REAL,
                accounts TEXT DEFAULT '[]',
                purchased_at TIMESTAMP DEFAULT NOW()
            )""",
        """CREATE TABLE IF NOT EXISTS bot_known_users (
                user_id BIGINT PRIMARY KEY, first_name TEXT, last_name TEXT,
                username TEXT,
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW(),
                admin_notified INTEGER DEFAULT 0
            )""",
        """CREATE TABLE IF NOT EXISTS bot_sent_verifications (
                email TEXT NOT NULL, code TEXT NOT NULL,
                first_sent_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (email, code)
            )""",
        """CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY, value TEXT
            )""",
        """CREATE TABLE IF NOT EXISTS bot_scheduled_deletions (
                id SERIAL PRIMARY KEY, chat_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL, delete_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE (chat_id, message_id)
            )""",
        """CREATE TABLE IF NOT EXISTS bot_email_buyer_map (
                email TEXT PRIMARY KEY, user_id BIGINT NOT NULL,
                account_type TEXT,
                purchased_at TIMESTAMP DEFAULT NOW()
            )""",
        """CREATE TABLE IF NOT EXISTS email_history (
                id SERIAL PRIMARY KEY,
                telegram_user_id BIGINT NOT NULL,
                email_address TEXT NOT NULL,
                dropmail_session_id TEXT,
                address_id TEXT,
                restore_key TEXT,
                last_mail_id TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )""",
        "CREATE INDEX IF NOT EXISTS idx_email_history_user ON email_history(telegram_user_id)",
    ]
    for stmt in _ddl_statements:
        try:
            _db_query(stmt)
        except Exception as e:
            logger.warning(f"DDL warning (non-fatal): {e}")
    try:
        r = _db_query("SELECT COUNT(*) as cnt FROM bot_accounts")
        if int(r["rows"][0]["cnt"]) == 0:
            _db_query("INSERT INTO bot_accounts (data) VALUES (%s)",
                      [json.dumps({"accounts": [], "account_types": {}, "prices": {}})])
    except Exception as e:
        logger.error(f"Failed to seed bot_accounts: {e}")
    try:
        r = _db_query("SELECT COUNT(*) as cnt FROM bot_sessions")
        if int(r["rows"][0]["cnt"]) == 0:
            _db_query("INSERT INTO bot_sessions (data) VALUES (%s)", [json.dumps({})])
    except Exception as e:
        logger.error(f"Failed to seed bot_sessions: {e}")
    logger.info("PostgreSQL DB initialized ✓")


def _get_setting(key, default=None):
    cached = cache.get(f"setting:{key}")
    if cached is not None:
        return cached
    try:
        r = _db_query("SELECT value FROM bot_settings WHERE key = %s", [key])
        rows = r.get("rows", [])
        val = rows[0].get("value") if rows else default
        if val is not None:
            cache.set(f"setting:{key}", val, ttl=300)
        return val
    except Exception as e:
        logger.error(f"Failed to read setting {key}: {e}")
        return default


def _set_setting(key, value):
    cache.set(f"setting:{key}", str(value), ttl=300)
    try:
        _db_query("""
            INSERT INTO bot_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
        """, [key, str(value)])
    except Exception as e:
        logger.error(f"Failed to save setting {key}: {e}")


def _load_data():
    try:
        r = _db_query("SELECT data FROM bot_accounts LIMIT 1")
        if r["rows"]:
            data = r["rows"][0]["data"]
            if isinstance(data, str):
                data = json.loads(data)
            data.setdefault("accounts", [])
            data.setdefault("account_types", {})
            data.setdefault("prices", {})
            logger.info("Loaded accounts data from PostgreSQL")
            return data
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
    return {"accounts": [], "account_types": {}, "prices": {}}


def _save_data():
    try:
        _db_query("UPDATE bot_accounts SET data = %s",
                  [json.dumps(accounts_data, ensure_ascii=False)])
    except Exception as e:
        logger.error(f"Failed to save data: {e}")


def _load_sessions():
    global user_sessions
    try:
        r = _db_query("SELECT data FROM bot_sessions LIMIT 1")
        if r["rows"]:
            data = r["rows"][0]["data"]
            if isinstance(data, str):
                data = json.loads(data)
            user_sessions = {int(k): v for k, v in data.items()}
            logger.info("Loaded sessions from PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to load sessions: {e}")


def _save_sessions():
    try:
        payload = {str(k): v for k, v in user_sessions.items()}
        _db_query("UPDATE bot_sessions SET data = %s",
                  [json.dumps(payload, ensure_ascii=False)])
    except Exception as e:
        logger.error(f"Failed to save sessions: {e}")


# ── Dropmail GraphQL API ──────────────────────────────────────────────────────
def _dropmail_gql(query: str, variables: dict = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = http.post(_DROPMAIL_URL, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _dropmail_create_session() -> dict:
    q = """mutation { introduceSession {
        id expiresAt
        addresses { id address restoreKey }
    } }"""
    data = _dropmail_gql(q)
    sess = data.get("data", {}).get("introduceSession")
    if not sess:
        return {}
    addr = sess["addresses"][0] if sess.get("addresses") else {}
    return {
        "session_id": sess["id"],
        "email":      addr.get("address"),
        "address_id": addr.get("id"),
        "restore_key": addr.get("restoreKey"),
    }


def _dropmail_restore_session(mail_address: str, restore_key: str) -> dict:
    new_q = """mutation { introduceSession(input: { withAddress: false }) { id } }"""
    data = _dropmail_gql(new_q)
    new_sess = data.get("data", {}).get("introduceSession")
    if not new_sess:
        return {}
    new_id = new_sess["id"]
    restore_q = """mutation Restore($m:String!,$r:String!,$s:ID!) {
        restoreAddress(input:{mailAddress:$m,restoreKey:$r,sessionId:$s}) {
            id address restoreKey
        }
    }"""
    r = _dropmail_gql(restore_q, {"m": mail_address, "r": restore_key, "s": new_id})
    addr = r.get("data", {}).get("restoreAddress")
    if not addr:
        return {}
    return {
        "session_id":  new_id,
        "email":       addr.get("address"),
        "address_id":  addr.get("id"),
        "restore_key": addr.get("restoreKey"),
    }


def _dropmail_get_mails(session_id: str, after_mail_id: str = None):
    if after_mail_id:
        q = """query G($id:ID!,$mid:ID!) {
            session(id:$id){ mailsAfterId(mailId:$mid){id fromAddr toAddr headerSubject text} }
        }"""
        v = {"id": session_id, "mid": after_mail_id}
    else:
        q = """query G($id:ID!) {
            session(id:$id){ mails{id fromAddr toAddr headerSubject text} }
        }"""
        v = {"id": session_id}
    data = _dropmail_gql(q, v)
    sess_data = data.get("data", {}).get("session")
    if sess_data is None:
        return None
    return sess_data.get("mailsAfterId") or sess_data.get("mails") or []


def _dropmail_delete_address(address_id: str) -> bool:
    q = """mutation D($a:ID!) { deleteAddress(input:{addressId:$a}) }"""
    try:
        data = _dropmail_gql(q, {"a": address_id})
        return bool(data.get("data", {}).get("deleteAddress"))
    except Exception:
        return False


def _dropmail_check_token_info() -> dict:
    try:
        q = """query { tokenInfo { expiresAt requestsRemaining } }"""
        data = _dropmail_gql(q)
        info = data.get("data", {}).get("tokenInfo") or {}
        if info:
            raw_exp = info.get("expiresAt") or "N/A"
            remaining = info.get("requestsRemaining")
            return {"valid": True, "expires": raw_exp, "remaining": remaining}
        q2 = """query { __typename }"""
        data2 = _dropmail_gql(q2)
        if data2.get("data"):
            return {"valid": True, "expires": "N/A", "remaining": None}
        return {"valid": False, "expires": "N/A", "remaining": None}
    except Exception as e:
        return {"valid": False, "expires": "N/A", "remaining": None, "error": str(e)}


# ── Email history DB helpers ──────────────────────────────────────────────────
def _email_history_add(user_id: int, email_address: str, session_id: str,
                       address_id: str, restore_key: str):
    try:
        _db_query("""
            INSERT INTO email_history
                (telegram_user_id, email_address, dropmail_session_id,
                 address_id, restore_key)
            VALUES (%s,%s,%s,%s,%s)
        """, [user_id, email_address, session_id, address_id, restore_key])
    except Exception as e:
        logger.error(f"_email_history_add failed: {e}")


def _email_history_list(user_id: int) -> list:
    try:
        r = _db_query(
            "SELECT email_address FROM email_history WHERE telegram_user_id=%s ORDER BY created_at DESC",
            [user_id])
        return [row["email_address"] for row in r.get("rows", [])]
    except Exception as e:
        logger.error(f"_email_history_list failed: {e}")
        return []


def _email_history_entries(user_id: int) -> list:
    try:
        r = _db_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history WHERE telegram_user_id=%s ORDER BY created_at DESC
        """, [user_id])
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"_email_history_entries failed: {e}")
        return []


def _email_history_all_entries() -> list:
    try:
        r = _db_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history WHERE restore_key IS NOT NULL
        """)
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"_email_history_all_entries failed: {e}")
        return []


def _email_history_get_by_id(entry_id: int) -> dict:
    try:
        r = _db_query("""
            SELECT id, email_address, address_id
            FROM email_history WHERE id=%s LIMIT 1
        """, [entry_id])
        rows = r.get("rows", [])
        return rows[0] if rows else {}
    except Exception as e:
        logger.error(f"_email_history_get_by_id failed: {e}")
        return {}


def _email_history_delete(entry_id: int):
    try:
        _db_query("DELETE FROM email_history WHERE id=%s", [entry_id])
    except Exception as e:
        logger.error(f"_email_history_delete failed: {e}")


def _email_history_update_session(entry_id: int, session_id: str,
                                  address_id: str, restore_key: str):
    try:
        _db_query("""
            UPDATE email_history
            SET dropmail_session_id=%s, address_id=%s, restore_key=%s, last_mail_id=NULL
            WHERE id=%s
        """, [session_id, address_id, restore_key, entry_id])
    except Exception as e:
        logger.error(f"_email_history_update_session failed: {e}")


def _email_history_update_last_mail(entry_id: int, mail_id: str):
    try:
        _db_query("UPDATE email_history SET last_mail_id=%s WHERE id=%s",
                  [mail_id, entry_id])
    except Exception as e:
        logger.error(f"_email_history_update_last_mail failed: {e}")


def _email_history_get_by_email(user_id: int, email_address: str) -> dict:
    try:
        r = _db_query("""
            SELECT id, telegram_user_id, email_address, dropmail_session_id,
                   address_id, restore_key, last_mail_id
            FROM email_history
            WHERE telegram_user_id=%s AND email_address=%s
            ORDER BY created_at DESC LIMIT 1
        """, [user_id, email_address])
        rows = r.get("rows", [])
        return rows[0] if rows else {}
    except Exception as e:
        logger.error(f"_email_history_get_by_email failed: {e}")
        return {}


def _save_pending_payment(user_id, chat_id, session):
    try:
        reserved = session.get("reserved_accounts") or []
        _db_query("""
            INSERT INTO bot_pending_payments
                (user_id, chat_id, account_type, quantity, total_price, md5_hash, qr_message_id, reserved_accounts, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                chat_id=excluded.chat_id, account_type=excluded.account_type,
                quantity=excluded.quantity, total_price=excluded.total_price,
                md5_hash=excluded.md5_hash, qr_message_id=excluded.qr_message_id,
                reserved_accounts=excluded.reserved_accounts, created_at=NOW()
        """, [user_id, chat_id,
              session.get("account_type"), session.get("quantity", 1),
              session.get("total_price", 0), session.get("md5_hash"),
              session.get("qr_message_id", 0),
              json.dumps(reserved, ensure_ascii=False)])
        logger.info(f"Saved pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to save pending payment: {e}")


def _delete_pending_payment(user_id):
    try:
        _db_query("DELETE FROM bot_pending_payments WHERE user_id = %s", [user_id])
        logger.info(f"Deleted pending payment for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to delete pending payment: {e}")


def _get_pending_payment(user_id):
    try:
        r = _db_query("SELECT * FROM bot_pending_payments WHERE user_id = %s", [user_id])
        if r["rows"]:
            row = r["rows"][0]
            reserved = row.get("reserved_accounts") or []
            if isinstance(reserved, str):
                try:
                    reserved = json.loads(reserved)
                except Exception:
                    reserved = []
            return {
                "state": "payment_pending",
                "account_type": row.get("account_type"),
                "quantity": int(row.get("quantity") or 1),
                "total_price": float(row.get("total_price") or 0),
                "md5_hash": row.get("md5_hash"),
                "qr_message_id": int(row.get("qr_message_id") or 0),
                "chat_id": int(row.get("chat_id") or 0),
                "reserved_accounts": reserved,
            }
    except Exception as e:
        logger.error(f"Failed to get pending payment: {e}")
    return None


def _save_purchase_history(user_id, account_type, quantity, total_price, accounts=None):
    try:
        accounts_list = accounts or []
        _db_query(
            "INSERT INTO bot_purchase_history (user_id,account_type,quantity,total_price,accounts) VALUES (%s,%s,%s,%s,%s)",
            [user_id, account_type, quantity, total_price,
             json.dumps(accounts_list, ensure_ascii=False)])
        for acc in accounts_list:
            if isinstance(acc, dict) and acc.get("email"):
                try:
                    _db_query("""
                        INSERT INTO bot_email_buyer_map (email, user_id, account_type, purchased_at)
                        VALUES (%s,%s,%s,NOW())
                        ON CONFLICT (email) DO UPDATE
                            SET user_id=excluded.user_id, account_type=excluded.account_type,
                                purchased_at=NOW()
                    """, [str(acc["email"]).strip().lower(), user_id, account_type])
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Failed to save purchase history: {e}")


def _get_purchase_history(user_id, limit=10):
    try:
        r = _db_query(
            "SELECT account_type,quantity,total_price,accounts,purchased_at "
            "FROM bot_purchase_history WHERE user_id=%s ORDER BY purchased_at DESC LIMIT %s",
            [user_id, limit])
        return r.get("rows", [])
    except Exception as e:
        logger.error(f"Failed to get purchase history: {e}")
        return []


def _find_buyer_by_email(email):
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        r = _db_query("SELECT user_id FROM bot_email_buyer_map WHERE LOWER(email)=%s", [email])
        if r.get("rows"):
            return int(r["rows"][0]["user_id"])
    except Exception:
        pass
    try:
        rows = _db_query(
            "SELECT user_id, accounts FROM bot_purchase_history ORDER BY purchased_at DESC"
        ).get("rows", [])
        for row in rows:
            accs = row.get("accounts") or "[]"
            if isinstance(accs, str):
                try:
                    accs = json.loads(accs)
                except Exception:
                    accs = []
            for a in accs:
                if isinstance(a, dict) and str(a.get("email", "")).strip().lower() == email:
                    uid = int(row["user_id"])
                    try:
                        _db_query("""
                            INSERT INTO bot_email_buyer_map (email, user_id, purchased_at)
                            VALUES (%s,%s,NOW())
                            ON CONFLICT (email) DO UPDATE SET user_id=excluded.user_id, purchased_at=NOW()
                        """, [email, uid])
                    except Exception:
                        pass
                    return uid
    except Exception as e:
        logger.error(f"Failed to find buyer by email: {e}")
    return None


def _find_all_buyers_by_email(email):
    email = (email or "").strip().lower()
    if not email:
        return []
    buyers, seen = [], set()
    try:
        rows = _db_query(
            "SELECT user_id, accounts, purchased_at FROM bot_purchase_history ORDER BY purchased_at DESC"
        ).get("rows", [])
        for row in rows:
            accs = row.get("accounts") or "[]"
            if isinstance(accs, str):
                try:
                    accs = json.loads(accs)
                except Exception:
                    accs = []
            for a in accs:
                if isinstance(a, dict) and str(a.get("email", "")).strip().lower() == email:
                    uid = int(row["user_id"])
                    if uid not in seen:
                        seen.add(uid)
                        buyers.append(uid)
                    break
    except Exception:
        pass
    return buyers


def _filter_out_already_sold(user_id, reserved):
    try:
        rows = _db_query(
            "SELECT accounts FROM bot_purchase_history WHERE user_id=%s ORDER BY purchased_at DESC LIMIT 50",
            [user_id]).get("rows", [])
    except Exception:
        return reserved
    sold_keys = set()
    for row in rows:
        accs = row.get("accounts") or []
        if isinstance(accs, str):
            try:
                accs = json.loads(accs)
            except Exception:
                accs = []
        for a in accs:
            if isinstance(a, dict):
                k = a.get("email") or a.get("phone")
                if k:
                    sold_keys.add(str(k))
    if not sold_keys:
        return reserved
    kept, dropped = [], 0
    for a in reserved:
        if not isinstance(a, dict):
            kept.append(a)
            continue
        k = a.get("email") or a.get("phone")
        if k and str(k) in sold_keys:
            dropped += 1
        else:
            kept.append(a)
    if dropped:
        logger.info(f"Skipped re-stocking {dropped} already-sold account(s) for user {user_id}")
    return kept


def _cleanup_expired_pending_payments():
    try:
        r = _db_query(
            "SELECT user_id, account_type, reserved_accounts FROM bot_pending_payments "
            "WHERE created_at + interval '1 second' * %s < NOW()",
            [PAYMENT_TIMEOUT_SECONDS])
        rows = r.get("rows", []) or []
        if not rows:
            return
        released = 0
        for row in rows:
            try:
                reserved = row.get("reserved_accounts") or []
                if isinstance(reserved, str):
                    try:
                        reserved = json.loads(reserved)
                    except Exception:
                        reserved = []
                user_id = row.get("user_id")
                if reserved and user_id is not None:
                    reserved = _filter_out_already_sold(user_id, reserved)
                fake_session = {"account_type": row.get("account_type"), "reserved_accounts": reserved}
                if reserved:
                    _release_reserved_accounts_sync(fake_session)
                    released += len(reserved)
                if user_id is not None:
                    _db_query("DELETE FROM bot_pending_payments WHERE user_id=%s", [user_id])
            except Exception as e:
                logger.warning(f"Bad expired payment row {row}: {e}")
        logger.info(f"Cleaned {len(rows)} expired payment(s); released {released} account(s)")
    except Exception as e:
        logger.error(f"Failed to clean expired payments: {e}")


def _record_scheduled_deletion(chat_id, message_id, delay_seconds):
    try:
        _db_query("""
            INSERT INTO bot_scheduled_deletions (chat_id, message_id, delete_at)
            VALUES (%s, %s, NOW() + interval '1 second' * %s)
            ON CONFLICT (chat_id, message_id) DO UPDATE SET delete_at=excluded.delete_at
        """, [chat_id, message_id, delay_seconds])
    except Exception as e:
        logger.error(f"Failed to record scheduled deletion: {e}")


def _clear_scheduled_deletion(chat_id, message_id):
    try:
        _db_query(
            "DELETE FROM bot_scheduled_deletions WHERE chat_id=%s AND message_id=%s",
            [chat_id, message_id])
    except Exception as e:
        logger.error(f"Failed to clear scheduled deletion: {e}")



# ── 9. KHQR / Payment helpers ──────────────────────────────────────────────────
def _generate_payment_qr(amount):
    try:
        resp = http.get(
            f"{RELAY_API_BASE}?type=generate_qr&user_tg_id={ADMIN_ID}&amount={amount:.2f}&expiry={PAYMENT_TIMEOUT_SECONDS}",
            timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            return None, data.get("message", "QR generation failed"), None
        payload = data["data"]
        qr  = payload["qr"]
        md5 = payload["md5"]
        img_bytes = None
        url_qr = payload.get("Url_qr_code")
        if url_qr:
            try:
                img_resp = http.get(url_qr, timeout=10)
                img_resp.raise_for_status()
                img_bytes = img_resp.content
            except Exception as e:
                logger.warning(f"Failed to download QR image: {e}")
        if not img_bytes:
            try:
                import qrcode as _qrcode
                buf = io.BytesIO()
                _qrcode.make(qr).save(buf, format="PNG")
                img_bytes = buf.getvalue()
            except Exception as e2:
                logger.warning(f"qrcode fallback failed: {e2}")
        if not img_bytes:
            try:
                img_resp = http.get(
                    f"https://api.qrserver.com/v1/create-qr-code/?size=500x500&data={url_quote(qr)}",
                    timeout=10)
                img_resp.raise_for_status()
                img_bytes = img_resp.content
            except Exception as e3:
                return None, f"All QR image methods failed: {e3}", None
        return img_bytes, md5, qr
    except Exception as e:
        return None, f"QR API error: {e}", None


def _check_payment_status(md5):
    try:
        resp = http.get(
            f"{RELAY_API_BASE}?type=check_md5&user_tg_id={ADMIN_ID}&md5={md5}",
            timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"check_payment relay: status={resp.status_code} result={data.get('status')}")
        if data.get("status") == "success":
            return True, data.get("data", {})
    except Exception as e:
        logger.warning(f"check_payment error: {e}")
    return False, None


# ── 10. Global state ──────────────────────────────────────────────────────────
accounts_data: dict = {"accounts": [], "account_types": {}, "prices": {}}
user_sessions: dict = {}
_data_initialized: bool = False


def _ensure_data_loaded():
    global accounts_data, user_sessions, _data_initialized
    if _data_initialized:
        return
    try:
        _init_db()
        data = _load_data()
        accounts_data.update(data)
        _load_sessions()
        _data_initialized = True
        logger.info("Lazy-loaded data from DB (serverless cold start)")
    except Exception as e:
        logger.error(f"_ensure_data_loaded failed: {e}")

# ── 11. Keyboard builders ─────────────────────────────────────────────────────
BTN_ADD_ACCOUNT       = "➕ បន្ថែម គូប៉ុង"
BTN_DELETE_TYPE       = "🗑 លុបប្រភេទ"
BTN_STOCK             = "📦 ស្តុក គូប៉ុង"
BTN_MAINTENANCE       = "🛠 Maintenance Mode"
BTN_BACK_SETTINGS     = "⬅️ ត្រឡប់ទៅកំណត់"
BTN_MAINT_ON          = "🔴 បិទ Bot"
BTN_MAINT_OFF         = "🟢 បើក Bot"
BTN_CANCEL_INPUT      = "🚫 បោះបង់"
BTN_DELETE_CONFIRM    = "✅ បញ្ជាក់លុប"
BTN_DELETE_CANCEL     = "🚫 បោះបង់ការលុប"
BTN_EMAIL_MGMT        = "📧 អ៊ីម៉ែល"
BTN_EMAIL_NEW         = "✉️ អ៊ីម៉ែលថ្មី"
BTN_EMAIL_INBOX       = "📥 ពិនិត្យប្រអប់"
BTN_EMAIL_LIST        = "📓 បញ្ជីអ៊ីម៉ែល"
BTN_EMAIL_DELETE      = "🗑️ លុបអ៊ីម៉ែល"
BTN_EMAIL_TOKEN_EDIT  = "✏️ ប្តូរ Dropmail Token"
BTN_EMAIL_TOKEN_INFO  = "📅 ព័ត៌មាន Token"


ADMIN_BUTTON_LABELS = {
    BTN_ADD_ACCOUNT, BTN_DELETE_TYPE, BTN_STOCK,
    BTN_MAINTENANCE, BTN_BACK_SETTINGS,
    BTN_MAINT_ON, BTN_MAINT_OFF,
    BTN_EMAIL_MGMT, BTN_EMAIL_NEW, BTN_EMAIL_LIST, BTN_EMAIL_DELETE,
    BTN_EMAIL_TOKEN_EDIT, BTN_EMAIL_TOKEN_INFO,
}

MAIN_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("💵 ទិញគូប៉ុង")]],
    resize_keyboard=True, is_persistent=True)


ADMIN_SETTINGS_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_ADD_ACCOUNT),  KeyboardButton(BTN_DELETE_TYPE)],
    [KeyboardButton(BTN_STOCK)],
    [KeyboardButton(BTN_EMAIL_MGMT),   KeyboardButton(BTN_MAINTENANCE)],
], resize_keyboard=True, is_persistent=True)

CANCEL_INPUT_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_CANCEL_INPUT)]], resize_keyboard=True, is_persistent=True)

ADD_ACCOUNT_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_BACK_SETTINGS)]], resize_keyboard=True, is_persistent=True)

MAINTENANCE_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_MAINT_ON), KeyboardButton(BTN_MAINT_OFF)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)

BACK_SETTINGS_KB = ReplyKeyboardMarkup(
    [[KeyboardButton(BTN_BACK_SETTINGS)]], resize_keyboard=True, is_persistent=True)

EMAIL_SUBMENU_KB = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_EMAIL_NEW),         KeyboardButton(BTN_EMAIL_LIST)],
    [KeyboardButton(BTN_EMAIL_DELETE)],
    [KeyboardButton(BTN_EMAIL_TOKEN_EDIT),  KeyboardButton(BTN_EMAIL_TOKEN_INFO)],
    [KeyboardButton(BTN_BACK_SETTINGS)],
], resize_keyboard=True, is_persistent=True)


CHECK_PAYMENT_INLINE = InlineKeyboardMarkup([
    [InlineKeyboardButton("🚫 បោះបង់", callback_data="cancel_purchase")]
])


def _main_kb(uid):
    return ReplyKeyboardRemove()


def _type_callback_id(account_type: str) -> str:
    return hashlib.sha1(account_type.encode("utf-8")).hexdigest()[:12]


def _account_type_from_callback_id(cid: str):
    for at in accounts_data.get("account_types", {}):
        if _type_callback_id(at) == cid:
            return at
    return None


def _short_label(text, limit=36):
    clean = " ".join(str(text).split())
    return clean if len(clean) <= limit else clean[: limit - 1] + "…"


# ── 12. Async send helpers ────────────────────────────────────────────────────
def _botapi_send_copy_button(chat_id, text, code: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "reply_markup": {
            "inline_keyboard": [[
                {"text": "📋 Copy Code", "copy_text": {"text": code}}
            ]]
        },
    }
    try:
        http.post(url, json=payload, timeout=10)
    except Exception as e:
        logger.warning(f"[botapi_send_copy_button] failed: {e}")


async def send_msg(chat_id, text, parse_mode=ParseMode.HTML, reply_markup=None,
                   reply_to_message_id=None, message_effect_id=None):
    try:
        kwargs = dict(chat_id=chat_id, text=text, parse_mode=parse_mode)
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        if reply_to_message_id:
            kwargs["reply_to_message_id"] = reply_to_message_id
        if message_effect_id:
            kwargs["message_effect_id"] = message_effect_id
        try:
            return await _bot.send_message(**kwargs)
        except TypeError:
            kwargs.pop("message_effect_id", None)
            return await _bot.send_message(**kwargs)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await send_msg(chat_id, text, parse_mode, reply_markup, reply_to_message_id, message_effect_id)
    except Forbidden:
        pass
    except BadRequest as e:
        logger.warning(f"send_msg BadRequest({chat_id}): {e}")
    except TelegramError as e:
        logger.error(f"send_msg({chat_id}) error: {e}")
    return None


async def delete_msg(chat_id, message_id):
    if not message_id:
        return
    try:
        await _bot.delete_message(chat_id, message_id)
    except (BadRequest, Forbidden, TelegramError):
        pass
    except Exception as e:
        logger.warning(f"delete_msg({chat_id},{message_id}): {e}")


async def delete_msg_later(chat_id, message_id, delay_seconds=120):
    if not message_id:
        return
    await run_sync(_record_scheduled_deletion, chat_id, message_id, delay_seconds)

    async def _delayed():
        await asyncio.sleep(delay_seconds)
        await delete_msg(chat_id, message_id)
        await run_sync(_clear_scheduled_deletion, chat_id, message_id)

    asyncio.create_task(_delayed())


async def send_photo(chat_id, img_bytes, caption=None, parse_mode=ParseMode.HTML, reply_markup=None):
    try:
        buf = io.BytesIO(img_bytes)
        buf.name = "qr.png"
        kwargs = dict(chat_id=chat_id, photo=buf)
        if caption:
            kwargs["caption"] = caption
            kwargs["parse_mode"] = parse_mode
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        return await _bot.send_photo(**kwargs)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        return await send_photo(chat_id, img_bytes, caption, parse_mode, reply_markup)
    except Exception as e:
        logger.error(f"send_photo({chat_id}) error: {e}")
    return None


async def send_document(chat_id, data_bytes, filename, caption=None):
    try:
        buf = io.BytesIO(data_bytes)
        buf.name = filename
        return await _bot.send_document(chat_id, document=buf, caption=caption)
    except Exception as e:
        logger.error(f"send_document({chat_id}) error: {e}")
    return None


async def copy_msg(to_chat_id, from_chat_id, message_id):
    try:
        return await _bot.copy_message(to_chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.error(f"copy_msg error: {e}")
    return None


async def forward_msg(to_chat_id, from_chat_id, message_id):
    try:
        return await _bot.forward_message(to_chat_id, from_chat_id, message_id)
    except Exception as e:
        logger.error(f"forward_msg error: {e}")
    return None


async def edit_caption(chat_id, message_id, caption, parse_mode=ParseMode.HTML, reply_markup=None):
    try:
        kwargs = dict(chat_id=chat_id, message_id=message_id, caption=caption, parse_mode=parse_mode)
        if reply_markup:
            kwargs["reply_markup"] = reply_markup
        return await _bot.edit_message_caption(**kwargs)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.warning(f"edit_caption error: {e}")
    except Exception as e:
        logger.warning(f"edit_caption error: {e}")
    return None


# ── 13. Business logic helpers ────────────────────────────────────────────────
async def _has_active_purchase(user_id: int) -> bool:
    async with _data_lock:
        sess = user_sessions.get(user_id)
        if sess and sess.get("state") == "payment_pending":
            return True
    pp = await run_sync(_get_pending_payment, user_id)
    return bool(pp)


async def _release_reserved_accounts(session):
    if not session:
        return
    reserved = session.get("reserved_accounts") or []
    if not reserved:
        return
    account_type = session.get("account_type")
    if not account_type:
        session["reserved_accounts"] = []
        return
    async with _data_lock:
        pool = accounts_data.setdefault("account_types", {}).setdefault(account_type, [])
        accounts_data["account_types"][account_type] = list(reserved) + list(pool)
        session["reserved_accounts"] = []
    await run_sync(_save_data)
    logger.info(f"Released {len(reserved)} reserved {account_type} account(s) back to pool")


def _release_reserved_accounts_sync(session):
    if not session:
        return
    reserved = session.get("reserved_accounts") or []
    if not reserved:
        return
    account_type = session.get("account_type")
    if not account_type:
        session["reserved_accounts"] = []
        return
    pool = accounts_data.setdefault("account_types", {}).setdefault(account_type, [])
    accounts_data["account_types"][account_type] = list(reserved) + list(pool)
    session["reserved_accounts"] = []
    _save_data()
    logger.info(f"Released {len(reserved)} {account_type} account(s) back (sync)")


async def _reset_user_session(user_id: int, save=True):
    async with _data_lock:
        session = user_sessions.pop(user_id, None)
    target = session if (session and session.get("reserved_accounts")) else None
    if target is None:
        target = await run_sync(_get_pending_payment, user_id)
    if target:
        await _release_reserved_accounts(target)
    asyncio.create_task(run_sync(_delete_pending_payment, user_id))
    if save and session is not None:
        asyncio.create_task(run_sync(_save_sessions))
    return session


async def show_account_selection(chat_id):
    await run_sync(_ensure_data_loaded)
    async with _data_lock:
        available = [
            (at, len(accs), accounts_data["prices"].get(at, 0))
            for at, accs in accounts_data["account_types"].items()
            if len(accs) > 0
        ]
    if not available:
        old_mid = _out_of_stock_msg.get(chat_id)
        if old_mid:
            asyncio.create_task(delete_msg(chat_id, old_mid))
        sent = await send_msg(chat_id, "<i>សូមអភ័យទោស អស់ពីស្តុក 🪤</i>",
                              parse_mode=ParseMode.HTML)
        if sent:
            _out_of_stock_msg[chat_id] = sent.message_id
        return
    _out_of_stock_msg.pop(chat_id, None)
    rows = []
    for at, count, price in available:
        label = f"{at} – មានក្នុងស្តុក {count}"
        rows.append([InlineKeyboardButton(label, callback_data=f"buy:{_type_callback_id(at)}")])
    await send_msg(chat_id, "<b>សូមជ្រើសរើសគូប៉ុងដើម្បីទិញ៖</b>",
                   reply_markup=InlineKeyboardMarkup(rows))


async def send_admin_settings_menu(chat_id):
    await send_msg(chat_id,
                   "<b>⚙️ ការកំណត់ Admin</b>\n\nសូមជ្រើសរើសប្រតិបត្តិការខាងក្រោម៖",
                   reply_markup=ADMIN_SETTINGS_KB)


async def _prompt_admin_input(chat_id, user_id, key, prompt_text):
    async with _data_lock:
        user_sessions[user_id] = {"state": f"admin_input:{key}"}
    asyncio.create_task(run_sync(_save_sessions))
    await send_msg(chat_id, prompt_text + "\n\n<i>ចុច 🚫 បោះបង់ ដើម្បីបោះបង់</i>",
                   reply_markup=CANCEL_INPUT_KB)




def _upsert_known_user(user_id, first_name, last_name, username):
    try:
        _db_query("""
            INSERT INTO bot_known_users (user_id, first_name, last_name, username, first_seen, last_seen, admin_notified)
            VALUES (%s,%s,%s,%s,NOW(),NOW(),1)
            ON CONFLICT (user_id) DO UPDATE SET
                first_name=excluded.first_name, last_name=excluded.last_name,
                username=excluded.username, last_seen=NOW(), admin_notified=1
        """, [user_id, first_name or "", last_name or "", username or ""])
    except Exception as e:
        logger.error(f"_upsert_known_user failed: {e}")


async def _notify_must_finish_order(chat_id):
    await send_msg(
        chat_id,
        "⏳ <b>សូមបញ្ចប់ការទិញបច្ចុប្បន្នជាមុនសិន</b>\n\n"
        "អ្នកមានការបញ្ជាទិញមួយកំពុងដំណើរការ។ "
        "សូមបញ្ចប់ការទូទាត់ ឬចុច /cancel មុននឹងចាប់ផ្តើមការទិញថ្មី។")


# ── 14. Payment flow ──────────────────────────────────────────────────────────
async def _start_payment_for_session(chat_id, user_id, session, callback_query=None):
    account_type = session.get("account_type")
    quantity = session.get("quantity", 1)

    async with _data_lock:
        pool = accounts_data.get("account_types", {}).get(account_type, [])
        available = len(pool)
        if available < quantity:
            reserved = None
        else:
            reserved = pool[:quantity]
            accounts_data["account_types"][account_type] = pool[quantity:]
            session["reserved_accounts"] = list(reserved)
            session["available_count"] = len(accounts_data["account_types"][account_type])

    if reserved is None:
        if callback_query:
            try:
                await callback_query.answer(
                    f"សូមអភ័យទោស! មានត្រឹមតែ {available} គូប៉ុង នៅក្នុងស្តុក", show_alert=True)
            except Exception:
                pass
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        return False

    asyncio.create_task(run_sync(_save_data))
    if callback_query:
        try:
            await callback_query.answer("កំពុងបង្កើត QR...")
        except Exception:
            pass
    async with _data_lock:
        session["state"] = "payment_pending"

    img_bytes, md5_or_err, qr_string = await run_sync(_generate_payment_qr, session["total_price"])
    if not img_bytes:
        if is_admin(user_id):
            await send_msg(chat_id, f"❌ *QR បរាជ័យ (Admin Debug):*\n`{md5_or_err}`",
                           parse_mode=ParseMode.MARKDOWN)
        else:
            await send_msg(chat_id, "❌ *មានបញ្ហាក្នុងការបង្កើត QR Code*\n\nសូមព្យាយាមម្តងទៀត។",
                           parse_mode=ParseMode.MARKDOWN)
            await send_msg(ADMIN_ID, f"⚠️ *QR Error (user {user_id}):*\n`{md5_or_err}`",
                           parse_mode=ParseMode.MARKDOWN)
        await _release_reserved_accounts(session)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        return False

    md5_hash = md5_or_err
    session["md5_hash"] = md5_hash
    started_at = time.time()
    session["qr_sent_at"] = started_at

    photo_msg = await send_photo(
        chat_id, img_bytes,
        caption=f"សូមធ្វើការ scan khqr សុពលភាព {PAYMENT_TIMEOUT_SECONDS // 60}នាទីប៉ុណ្ណោះ\n💬យើងនឹងធ្វើការផ្ទៀងផ្ទាត់ចំនួន {PAYMENT_TIMEOUT_SECONDS // PAYMENT_POLL_INTERVAL}ដង",
        reply_markup=CHECK_PAYMENT_INLINE)
    if photo_msg:
        session["photo_message_id"] = photo_msg.message_id
        session["qr_message_id"] = photo_msg.message_id
        asyncio.create_task(_schedule_qr_expiry(chat_id, user_id, photo_msg.message_id, md5_hash, started_at))
        asyncio.create_task(run_sync(_record_scheduled_deletion, chat_id, photo_msg.message_id, PAYMENT_TIMEOUT_SECONDS))

    asyncio.create_task(run_sync(_save_sessions))
    asyncio.create_task(run_sync(_save_pending_payment, user_id, chat_id, session))
    logger.info(f"Generated QR for user {user_id}: Amount ${session['total_price']}, MD5: {md5_hash}")
    return True


async def _schedule_qr_expiry(chat_id, user_id, msg_id, md5_hash, started_at):
    try:
        while True:
            elapsed   = time.time() - started_at
            remaining = PAYMENT_TIMEOUT_SECONDS - elapsed
            sleep_for = min(max(remaining, 0), PAYMENT_POLL_INTERVAL)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

            async with _data_lock:
                sess = user_sessions.get(user_id)
                still_active = bool(
                    sess and sess.get("md5_hash") == md5_hash and sess.get("state") == "payment_pending")
            if not still_active:
                return

            timed_out = time.time() - started_at >= PAYMENT_TIMEOUT_SECONDS

            async with get_user_lock(user_id):
                async with _data_lock:
                    sess_now = user_sessions.get(user_id)
                    still_active = bool(
                        sess_now and sess_now.get("md5_hash") == md5_hash
                        and sess_now.get("state") == "payment_pending")
                if not still_active:
                    return

                is_paid, payment_data = await run_sync(_check_payment_status, md5_hash)

                if is_paid:
                    logger.info(f"Auto-poll detected payment for user {user_id}")
                    async with _data_lock:
                        delivered_session = user_sessions.get(user_id)
                    if delivered_session and delivered_session.get("md5_hash") == md5_hash:
                        await deliver_accounts(chat_id, user_id, delivered_session,
                                               payment_data=payment_data)
                        asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                        asyncio.create_task(run_sync(_save_sessions))
                    return

                if not timed_out:
                    continue

                await delete_msg(chat_id, msg_id)
                asyncio.create_task(run_sync(_clear_scheduled_deletion, chat_id, msg_id))
                async with _data_lock:
                    expired_session = None
                    if (user_id in user_sessions
                            and user_sessions[user_id].get("md5_hash") == md5_hash):
                        expired_session = user_sessions.pop(user_id)
                if expired_session:
                    await _release_reserved_accounts(expired_session)
                else:
                    pp = await run_sync(_get_pending_payment, user_id)
                    if pp:
                        await _release_reserved_accounts(pp)
                asyncio.create_task(run_sync(_save_sessions))
                asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                await send_msg(
                    chat_id,
                    "⌛ <b>QR Code បានផុតកំណត់</b>\n\nសូមបង្កើតការទិញម្តងទៀត។")
                try:
                    await show_account_selection(chat_id)
                except Exception:
                    pass
                return
    except Exception as e:
        logger.error(f"QR expiry task failed for user {user_id}: {e}")


async def deliver_accounts(chat_id, user_id, session, payment_data=None, user_name=""):
    account_type = session["account_type"]
    quantity     = session["quantity"]

    for key in ("photo_message_id", "qr_message_id"):
        mid = session.get(key)
        if mid:
            asyncio.create_task(delete_msg(chat_id, mid))
            asyncio.create_task(run_sync(_clear_scheduled_deletion, chat_id, mid))

    reserved = session.get("reserved_accounts") or []
    async with _data_lock:
        if reserved and len(reserved) >= quantity:
            delivered = list(reserved)[:quantity]
            session["reserved_accounts"] = []
            user_sessions.pop(user_id, None)
        elif account_type not in accounts_data["account_types"]:
            delivered = None
        else:
            pool = accounts_data["account_types"][account_type]
            if len(pool) < quantity:
                delivered = None
            else:
                delivered = pool[:quantity]
                accounts_data["account_types"][account_type] = pool[quantity:]
                user_sessions.pop(user_id, None)

    if delivered is None:
        await send_msg(chat_id, f"❌ *មានបញ្ហា!*\n\nគ្មាន គូប៉ុង ប្រភេទ {account_type} ក្នុងស្តុក។",
                       parse_mode=ParseMode.MARKDOWN)
        return

    await run_sync(_save_data)
    await run_sync(_delete_pending_payment, user_id)
    asyncio.create_task(run_sync(_save_purchase_history, user_id, account_type, quantity,
                                 session.get("total_price", 0), delivered))

    msg = (
        f'<tg-emoji emoji-id="5436040291507247633">🎉</tg-emoji> '
        f'<b>ការទិញបានបញ្ជាក់ដោយជោគជ័យ</b>\n\n'
        f"<blockquote>🔹 ប្រភេទ: {account_type}\n🔹 ចំនួន: {quantity}</blockquote>\n\n"
        f"<b>គូប៉ុង របស់អ្នក៖</b>\n\n"
    )
    for acc in delivered:
        if "email" in acc:
            msg += f"{acc['email']}\n"
        else:
            msg += f"{acc.get('phone','')} | {acc.get('password','')}\n"
    msg += f'\n<i>សូមអរគុណសម្រាប់ការទិញ <tg-emoji emoji-id="5897474556834091884">🙏</tg-emoji></i>'

    await send_msg(chat_id, msg, message_effect_id="5046509860389126442",
                   reply_markup=_main_kb(user_id))

    try:
        cambodia_tz = timezone(timedelta(hours=7))
        now_str = datetime.now(cambodia_tz).strftime("%d/%m/%Y %H:%M")
        pd = payment_data or {}
        from_account = pd.get("fromAccountId") or pd.get("hash") or "N/A"
        memo = pd.get("memo") or "គ្មាន"
        ref  = pd.get("externalRef") or pd.get("transactionId") or pd.get("md5") or "N/A"
        amount = session.get("total_price", 0)
        buyer_label = f"{user_name} ({user_id})" if user_name else str(user_id)
        admin_msg = (
            "🎉 <b>ទទួលបានការបង់ប្រាក់ជោគជ័យ</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"🆔 <b>ឈ្មោះអ្នកទិញ(ID):</b> {buyer_label}\n"
            f"💵 <b>ទឹកប្រាក់:</b> {amount} USD\n"
            f"👤 <b>ពីធនាគារ:</b> <code>{from_account}</code>\n"
            f"📝 <b>ចំណាំ:</b> {memo}\n"
            f"🧾 <b>លេខយោង:</b> <code>{ref}</code>\n"
            f"⏰ <b>ម៉ោង:</b> {now_str}"
        )
        await send_msg(ADMIN_ID, admin_msg)
    except Exception as e:
        logger.error(f"Failed to send admin payment notification: {e}")

    asyncio.create_task(run_sync(_save_sessions))
    logger.info(f"Payment confirmed and {quantity} accounts delivered to user {user_id}")


# ── 15. Admin helper functions ────────────────────────────────────────────────

async def _show_delete_type_menu_inline(chat_id, user_id):
    async with _data_lock:
        types = list(accounts_data.get("account_types", {}).keys())
    if not types:
        await send_msg(chat_id, "⚠️ <b>មិនមានប្រភេទ គូប៉ុង ណាមួយទេ!</b>")
        return
    rows_kb, labels_map = [], {}
    for t in types:
        async with _data_lock:
            count = len(accounts_data["account_types"].get(t, []))
        label = f"{_short_label(t)} – មានក្នុងស្តុក {count}"
        rows_kb.append([KeyboardButton(label)])
        labels_map[label] = t
    rows_kb.append([KeyboardButton(BTN_BACK_SETTINGS)])
    async with _data_lock:
        user_sessions[user_id] = {"state": "delete_type_select", "labels": labels_map}
    asyncio.create_task(run_sync(_save_sessions))
    await send_msg(chat_id, "🗑 <b>ជ្រើសរើសប្រភេទ គូប៉ុង ដែលចង់លុប៖</b>",
                   reply_markup=ReplyKeyboardMarkup(rows_kb, resize_keyboard=True, is_persistent=True))


async def _export_stock_inline(chat_id):
    try:
        async with _data_lock:
            types  = dict(accounts_data.get("account_types", {}))
            prices = dict(accounts_data.get("prices", {}))
            reserved_by_type = {}
            for sess in user_sessions.values():
                if not isinstance(sess, dict) or sess.get("state") != "payment_pending":
                    continue
                t = sess.get("account_type")
                if not t:
                    continue
                for acc in (sess.get("reserved_accounts") or []):
                    if isinstance(acc, dict) and acc.get("email"):
                        reserved_by_type.setdefault(t, []).append(str(acc["email"]))
        type_names = sorted(types)
        if not type_names:
            await send_msg(chat_id, "📦 មិនមានប្រភេទ គូប៉ុង ឡើយទេ។",
                           reply_markup=ADMIN_SETTINGS_KB)
            return
        total_avail, total_res = 0, 0
        for t in type_names:
            total_avail += len(types.get(t) or [])
            total_res   += len(reserved_by_type.get(t, []))
        header = (f"📦 <b>ស្តុក គូប៉ុង</b> — {len(type_names)} ប្រភេទ, {total_avail} នៅសល់"
                  + (f", {total_res} កំពុងកក់ទុក" if total_res else ""))
        await send_msg(chat_id, header)
        for t in type_names:
            pool  = types.get(t) or []
            avail = len(pool)
            res   = reserved_by_type.get(t, [])
            email_lines = []
            for acc in pool:
                if isinstance(acc, dict):
                    em = acc.get("email")
                    if em:
                        email_lines.append(f"• {html.escape(em)}")
                    else:
                        email_lines.append(f"• {html.escape(acc.get('phone',''))} | {html.escape(acc.get('password',''))}")
            if res:
                email_lines.append(f"\n🔒 <i>កំពុងកក់ទុក ({len(res)})</i>")
                for em in res:
                    email_lines.append(f"· {html.escape(em)}")
            block = (f"<b>{html.escape(t)}</b>  💰 ${prices.get(t, 0)}  📦 {avail}\n"
                     + ("\n".join(email_lines) if email_lines else "<i>(គ្មាន)</i>"))
            MAX = 4000
            while len(block) > MAX:
                cut = block.rfind("\n", 0, MAX)
                if cut == -1:
                    cut = MAX
                await send_msg(chat_id, block[:cut])
                block = block[cut:].lstrip("\n")
            if block:
                await send_msg(chat_id, block)
        await send_admin_settings_menu(chat_id)
    except Exception as e:
        logger.error(f"stock export failed: {e}")
        await send_msg(chat_id, f"❌ Error: <code>{html.escape(str(e))}</code>")


def _days_status(days_left) -> str:
    if days_left is None:
        return "✅ Active"
    if days_left < 0:
        return f"❌ ផុតកំណត់រួចហើយ ({abs(days_left)} ថ្ងៃមុន)"
    if days_left == 0:
        return "⚠️ ផុតកំណត់ថ្ងៃនេះ!"
    if days_left <= 7:
        return f"⚠️ នឹងផុតក្នុង {days_left} ថ្ងៃ"
    return f"✅ នៅសល់ {days_left} ថ្ងៃ"


async def _send_combined_token_info(chat_id: int, reply_markup) -> None:
    lines = ["🔑 <b>Token Info</b>\n"]
    lines.append("━━━ 📧 Dropmail ━━━")
    if not DROPMAIL_API_TOKEN:
        lines.append("❌ មិនទាន់មាន Dropmail Token ទេ។")
    else:
        dm_masked = DROPMAIL_API_TOKEN[:6] + "…" + DROPMAIL_API_TOKEN[-4:]
        lines.append(f"Token: <code>{html.escape(dm_masked)}</code>")
        if DROPMAIL_TOKEN_EXPIRY:
            try:
                exp_dt2 = datetime.strptime(DROPMAIL_TOKEN_EXPIRY, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                dm_days = (exp_dt2 - datetime.now(tz=timezone.utc)).days
                lines.append(f"📅 Expire: <b>{DROPMAIL_TOKEN_EXPIRY}</b>")
                lines.append(f"⏳ ស្ថានភាព: {_days_status(dm_days)}")
            except Exception:
                lines.append(f"📅 Expire: <b>{html.escape(DROPMAIL_TOKEN_EXPIRY)}</b>")
                lines.append("⏳ ស្ថានភាព: ✅ Active")
        else:
            lines.append("📅 Expire: <b>មិន​ទាន់​កំណត់</b> — ចុច ✏️ ប្តូរ Token ដើម្បីកំណត់")
    await send_msg(chat_id, "\n".join(lines), reply_markup=reply_markup)


async def _show_maintenance_inline(chat_id):
    status = "🔴 បិទ" if MAINTENANCE_MODE else "🟢 បើក"
    await send_msg(chat_id, f"🛠 <b>ស្ថានភាព Bot បច្ចុប្បន្ន៖</b> {status}",
                   reply_markup=MAINTENANCE_SUBMENU_KB)


async def _dispatch_admin_button(update: Update, user_id, chat_id, btn):
    global MAINTENANCE_MODE
    if btn == BTN_BACK_SETTINGS:
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_admin_settings_menu(chat_id)
    elif btn == BTN_ADD_ACCOUNT:
        async with _data_lock:
            user_sessions[user_id] = {"state": "waiting_for_accounts"}
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(chat_id, "<b>បញ្ចូលគូប៉ុងសម្រាប់លក់</b>", reply_markup=ADD_ACCOUNT_KB)
    elif btn == BTN_DELETE_TYPE:
        await _show_delete_type_menu_inline(chat_id, user_id)
    elif btn == BTN_STOCK:
        await _export_stock_inline(chat_id)
    elif btn == BTN_MAINTENANCE:
        await _show_maintenance_inline(chat_id)
    elif btn == BTN_MAINT_ON:
        MAINTENANCE_MODE = True
        await run_sync(_set_setting, "MAINTENANCE_MODE", "true")
        await send_msg(chat_id, "🔴 បានបិទ Bot", reply_markup=ADMIN_SETTINGS_KB)
    elif btn == BTN_MAINT_OFF:
        MAINTENANCE_MODE = False
        await run_sync(_set_setting, "MAINTENANCE_MODE", "false")
        await send_msg(chat_id, "🟢 បានបើក Bot", reply_markup=ADMIN_SETTINGS_KB)
    elif btn == BTN_EMAIL_MGMT:
        if not DROPMAIL_API_TOKEN:
            await send_msg(chat_id,
                "⚠️ <b>DROPMAIL_API_TOKEN</b> មិនទាន់កំណត់។\n\n"
                "ចុច <b>✏️ ប្តូរ Dropmail Token</b> ដើម្បីកំណត់ token ។",
                reply_markup=EMAIL_SUBMENU_KB)
        else:
            await send_msg(chat_id,
                "📧 <b>ការគ្រប់គ្រងអ៊ីម៉ែល</b>\n\nជ្រើសរើសប្រតិបត្តិការ៖",
                reply_markup=EMAIL_SUBMENU_KB)
    elif btn == BTN_EMAIL_NEW:
        await _email_handle_new(chat_id, user_id)
    elif btn == BTN_EMAIL_LIST:
        await _email_handle_list(chat_id, user_id)
    elif btn == BTN_EMAIL_DELETE:
        await _email_handle_delete_picker(chat_id, user_id)
    elif btn == BTN_EMAIL_TOKEN_EDIT:
        await _prompt_admin_input(
            chat_id, user_id, "dropmail_token",
            "🔑 សូមផ្ញើ <b>Dropmail API Token</b> ថ្មី:\n\n"
            "<i>⚠️ Token នឹងត្រូវបានលុបចោលស្វ័យប្រវត្តិ — ផ្ញើដោយប្រុងប្រយ័ត្ន!</i>")
    elif btn == BTN_EMAIL_TOKEN_INFO:
        await _email_show_token_info(chat_id)


async def _handle_admin_settings_input(chat_id, user_id, message_id, key, text):
    global DROPMAIL_API_TOKEN, DROPMAIL_TOKEN_EXPIRY, _DROPMAIL_URL
    raw = (text or "").strip()
    cancel_words = {"បោះបង់", "🚫 បោះបង់"}
    if raw in cancel_words or raw == BTN_BACK_SETTINGS:
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_admin_settings_menu(chat_id)
        return True

    if key == "dropmail_token":
        if not raw:
            await send_msg(chat_id, "🔑 សូមផ្ញើ <b>Dropmail API Token</b> ថ្មី (ឬចុច 🚫 បោះបង់)")
            return True
        DROPMAIL_API_TOKEN = raw
        _DROPMAIL_URL = f"https://dropmail.me/api/graphql/{raw}"
        await run_sync(_set_setting, "DROPMAIL_API_TOKEN", raw)
        asyncio.create_task(delete_msg(chat_id, message_id))
        async with _data_lock:
            user_sessions[user_id] = {"state": "admin_input:dropmail_expiry"}
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(
            chat_id,
            f"✅ បានប្តូរ <b>Dropmail API Token</b>\n"
            f"Prefix: <code>{html.escape(raw[:8])}…</code>\n\n"
            f"📅 សូមផ្ញើ <b>ថ្ងៃផុតកំណត់</b> (YYYY-MM-DD)\n"
            f"ឧ. <code>2026-12-31</code>\n"
            f"ឬចុច <b>🚫 បោះបង់</b> ដើម្បីរំលង",
            reply_markup=CANCEL_INPUT_KB)
        return True

    if key == "dropmail_expiry":
        if not raw:
            await send_msg(chat_id, "📅 សូមផ្ញើ​ថ្ងៃ​ផុត​កំណត់ (YYYY-MM-DD) ឬចុច 🚫 បោះបង់")
            return True
        try:
            datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            await send_msg(chat_id, "❌ ទម្រង់ថ្ងៃ​មិន​ត្រឹម​ត្រូវ។ សូម​ប្រើ​ទម្រង់ <code>YYYY-MM-DD</code> (ឧ. <code>2026-12-31</code>)")
            return True
        DROPMAIL_TOKEN_EXPIRY = raw
        await run_sync(_set_setting, "DROPMAIL_TOKEN_EXPIRY", raw)
        async with _data_lock:
            user_sessions.pop(user_id, None)
        asyncio.create_task(run_sync(_save_sessions))
        await send_msg(
            chat_id,
            f"✅ បានកំណត់ <b>Dropmail Token Expire</b>: <code>{html.escape(raw)}</code>",
            reply_markup=EMAIL_SUBMENU_KB)
        return True


    return False


# ── 16. Channel post handler ──────────────────────────────────────────────────
def _parse_verification_message(text):
    email_match = re.search(r"[\w.+%-]+@[\w.-]+\.[A-Za-z]{2,}", text or "")
    code_match  = re.search(r"(?<!\d)\d{4,8}(?!\d)", text or "")
    if not email_match or not code_match:
        return None, None
    return email_match.group(0).strip().lower(), code_match.group(0)


async def handle_channel_post(message):
    text = message.text or message.caption or ""
    email, code = _parse_verification_message(text)
    if email and code:
        buyers = await run_sync(_find_all_buyers_by_email, email)
        formatted = (
            "📩 <b>លេខកូដផ្ទៀងផ្ទាត់ E-GetS</b>\n\n"
            f"{html.escape(email)}\n\n<code>{html.escape(code)}</code>")
        delivered_to = []
        for bid in buyers:
            sent = await send_msg(bid, formatted, reply_markup=False)
            if sent:
                await delete_msg_later(bid, sent.message_id, 60)
                delivered_to.append(bid)
        if not delivered_to:
            sent = await send_msg(ADMIN_ID, formatted)
            if sent:
                await delete_msg_later(ADMIN_ID, sent.message_id, 60)
        return
    copied = await copy_msg(ADMIN_ID, chat_id, message_id)
    if copied:
        return
    if text:
        await send_msg(ADMIN_ID, text)


# ── 17. Email sub-menu helpers ────────────────────────────────────────────────
async def _email_handle_new(chat_id: int, user_id: int):
    if not DROPMAIL_API_TOKEN:
        await send_msg(chat_id, "❌ DROPMAIL_API_TOKEN មិនទាន់កំណត់។", reply_markup=EMAIL_SUBMENU_KB)
        return
    info = await run_sync(_dropmail_check_token_info)
    expires_val = info.get("expires") or "N/A"
    remaining_val = info.get("remaining")
    days_left = None
    exp_display = expires_val
    if expires_val and expires_val != "N/A":
        try:
            exp_dt = datetime.fromisoformat(expires_val.replace("Z", "+00:00"))
            days_left = (exp_dt - datetime.now(tz=timezone.utc)).days
            exp_display = exp_dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    if days_left is not None and days_left < 0:
        await send_msg(
            chat_id,
            f"❌ <b>Dropmail Token ផុតកំណត់រួចហើយ!</b>\n"
            f"📅 Expire: <b>{exp_display}</b> ({abs(days_left)} ថ្ងៃមុន)\n\n"
            f"ចុច <b>✏️ ប្តូរ Dropmail Token</b> ដើម្បីធ្វើបច្ចុប្បន្នភាព។",
            reply_markup=EMAIL_SUBMENU_KB)
        return
    if days_left is not None and days_left <= 7:
        token_status = f"⚠️ Token នឹងផុតក្នុង <b>{days_left} ថ្ងៃ</b> ({exp_display}) — សូមធ្វើបច្ចុប្បន្នភាព!"
    elif days_left is not None:
        rem_str = f" | 📊 Requests: {remaining_val}" if remaining_val is not None else ""
        token_status = f"✅ Token ត្រឹមត្រូវ — នៅសល់ <b>{days_left} ថ្ងៃ</b> ({exp_display}){rem_str}"
    else:
        rem_str = f" | 📊 Requests: {remaining_val}" if remaining_val is not None else ""
        token_status = f"✅ Token ត្រឹមត្រូវ{rem_str}"
    try:
        result = await run_sync(_dropmail_create_session)
    except Exception as e:
        await send_msg(chat_id, f"❌ បង្កើតមិនបានទេ: <code>{html.escape(str(e))}</code>",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    if not result or not result.get("email"):
        await send_msg(chat_id, "❌ មិនអាចបង្កើត session បានទេ។ សូមព្យាយាមម្ដងទៀត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    await run_sync(_email_history_add, user_id, result["email"],
                   result.get("session_id", ""), result.get("address_id", ""),
                   result.get("restore_key", ""))
    await send_msg(chat_id,
                   f"✅ <b>អ៊ីម៉ែលថ្មីបានបង្កើត!</b>\n\n"
                   f"📧 <code>{result['email']}</code>\n\n"
                   f"👆 ចុចលើអ៊ីម៉ែលដើម្បីចម្លង។ Bot នឹងជូនដំណឹងភ្លាមៗពីសំបុត្រថ្មី។\n\n"
                   f"🔑 {token_status}",
                   reply_markup=EMAIL_SUBMENU_KB)


async def _email_handle_inbox(chat_id: int, user_id: int):
    entries = await run_sync(_email_history_entries, user_id)
    if not entries:
        await send_msg(chat_id,
                       "📭 មិនទាន់មានអ៊ីម៉ែលទេ។ ចុច <b>✉️ អ៊ីម៉ែលថ្មី</b> ដើម្បីបង្កើត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    entry = entries[0]
    session_id = entry.get("dropmail_session_id")
    if not session_id:
        await send_msg(chat_id, "❌ Session ID ត្រូវបានបាត់។ សូមបង្កើតអ៊ីម៉ែលថ្មី។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    await send_msg(chat_id, "⏳ កំពុងពិនិត្យប្រអប់…", reply_markup=EMAIL_SUBMENU_KB)
    try:
        mails = await run_sync(_dropmail_get_mails, session_id, None)
    except Exception as e:
        await send_msg(chat_id, f"❌ កំហុសក្នុងការពិនិត្យ: <code>{html.escape(str(e))}</code>",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    email_addr = entry.get("email_address", "?")
    if mails is None:
        await send_msg(chat_id,
                       f"⚠️ Session ផុតកំណត់។\n📧 <code>{email_addr}</code>\n\n"
                       f"Bot នឹងស្តារវិញដោយស្វ័យប្រវត្តិ។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    if not mails:
        await send_msg(chat_id,
                       f"📭 <b>ប្រអប់ទទេ</b>\n\n📧 <code>{email_addr}</code>\n\n"
                       f"មិនទាន់មានអ៊ីម៉ែលចូលទេ។ Bot នឹងជូនដំណឹងភ្លាមៗ។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    text = f"📬 <b>ប្រអប់ — {len(mails)} សំបុត្រ</b>\n📧 <code>{email_addr}</code>\n\n"
    for i, mail in enumerate(mails[-5:], 1):
        subject   = mail.get("headerSubject") or "(គ្មានប្រធានបទ)"
        from_addr = mail.get("fromAddr") or "unknown"
        body      = (mail.get("text") or "").strip()
        preview   = body[:200] + "…" if len(body) > 200 else body
        text += (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>#{i} {html.escape(subject)}</b>\n"
            f"From: <code>{html.escape(from_addr)}</code>\n"
            f"{html.escape(preview) if preview else '<i>(ទទេ)</i>'}\n\n"
        )
    await send_msg(chat_id, text, reply_markup=EMAIL_SUBMENU_KB)


async def _email_handle_list(chat_id: int, user_id: int):
    emails = await run_sync(_email_history_list, user_id)
    if not emails:
        await send_msg(chat_id,
                       "📭 មិនទាន់មានអ៊ីម៉ែលទេ។ ចុច <b>✉️ អ៊ីម៉ែលថ្មី</b> ដើម្បីបង្កើត។",
                       reply_markup=EMAIL_SUBMENU_KB)
        return
    lines = "\n".join(f"{i+1}. <code>{em}</code>" for i, em in enumerate(emails))
    await send_msg(chat_id,
                   f"📧 <b>បញ្ជីអ៊ីម៉ែល ({len(emails)})</b>\n\n{lines}",
                   reply_markup=EMAIL_SUBMENU_KB)


async def _email_show_token_info(chat_id: int):
    await _send_combined_token_info(chat_id, EMAIL_SUBMENU_KB)


async def _email_handle_delete_picker(chat_id: int, user_id: int):
    entries = await run_sync(_email_history_entries, user_id)
    if not entries:
        await send_msg(chat_id, "📭 មិនទាន់មានអ៊ីម៉ែលទេ។", reply_markup=EMAIL_SUBMENU_KB)
        return
    async with _data_lock:
        user_sessions[user_id] = {"state": "email_delete_picker"}
    asyncio.create_task(run_sync(_save_sessions))
    rows = [[KeyboardButton(e['email_address'])] for e in entries]
    rows.append([KeyboardButton(BTN_BACK_SETTINGS)])
    kb = ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)
    await send_msg(chat_id, "🗑 <b>ជ្រើសរើសអ៊ីម៉ែលដែលចង់លុប៖</b>", reply_markup=kb)


# ── 18. Handlers ──────────────────────────────────────────────────────────────

async def on_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.channel_post:
        await handle_channel_post(update.channel_post)


async def on_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_admin(user_id):
        return
    async with _data_lock:
        sess = user_sessions.get(user_id, {})
        if str(sess.get("state", "")).startswith("admin_input:"):
            user_sessions.pop(user_id, None)
    asyncio.create_task(run_sync(_save_sessions))
    await send_admin_settings_menu(chat_id)


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    asyncio.create_task(run_sync(_upsert_known_user, user.id, user.first_name, user.last_name, user.username))
    async with get_user_lock(user.id):
        if await _has_active_purchase(user.id):
            await _notify_must_finish_order(chat_id)
            return
        await _reset_user_session(user.id)
        logger.info(f"User {user.id} triggered account selection")
        await show_account_selection(chat_id)


async def on_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    async with get_user_lock(user_id):
        session = user_sessions.get(user_id) or await run_sync(_get_pending_payment, user_id)
        if not session or session.get("state") not in ("waiting_for_quantity", "payment_pending"):
            await show_account_selection(chat_id)
            return
        for key in ("photo_message_id", "qr_message_id", "dot_message_id"):
            mid = session.get(key)
            if mid:
                asyncio.create_task(delete_msg(chat_id, mid))
        await _reset_user_session(user_id)
        await show_account_selection(chat_id)


async def on_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    await run_sync(_ensure_data_loaded)
    user    = update.effective_user
    user_id = user.id
    chat_id = update.effective_chat.id
    message_id = update.message.message_id
    text    = update.message.text or ""

    # Maintenance mode block for non-admins
    if MAINTENANCE_MODE and not is_admin(user_id):
        await send_msg(chat_id, "🔧 <b>Bot កំពុង Update សូមរង់ចាំមួយភ្លែត...</b>")
        return

    if not is_admin(user_id):
        asyncio.create_task(run_sync(_upsert_known_user, user_id, user.first_name, user.last_name, user.username))

    btn = text.strip()

    # Admin pending input session
    if is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        state = str(sess.get("state", ""))
        if state.startswith("admin_input:"):
            key = state.split(":", 1)[1]
            async with get_user_lock(user_id):
                await _handle_admin_settings_input(chat_id, user_id, message_id, key, text)
            return

    # Admin state: delete_type_select
    if is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        if sess.get("state") == "delete_type_select":
            async with get_user_lock(user_id):
                labels = sess.get("labels", {}) or {}
                if btn == BTN_BACK_SETTINGS:
                    async with _data_lock:
                        user_sessions.pop(user_id, None)
                    asyncio.create_task(run_sync(_save_sessions))
                    await send_admin_settings_menu(chat_id)
                    return
                type_name = labels.get(btn)
                if type_name and type_name in accounts_data.get("account_types", {}):
                    async with _data_lock:
                        count = len(accounts_data["account_types"].get(type_name, []))
                        price = accounts_data.get("prices", {}).get(type_name, 0)
                        user_sessions[user_id] = {"state": "delete_type_confirm", "type_name": type_name}
                    asyncio.create_task(run_sync(_save_sessions))
                    await send_msg(
                        chat_id,
                        f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ គូប៉ុង នេះមែនទេ?</b>\n\n"
                        f"<blockquote>🔹 ប្រភេទ: {html.escape(type_name)}\n"
                        f"🔹 ចំនួន: {count}\n🔹 តម្លៃ: ${price}</blockquote>",
                        reply_markup=ReplyKeyboardMarkup([
                            [KeyboardButton(BTN_DELETE_CONFIRM)],
                            [KeyboardButton(BTN_DELETE_CANCEL)],
                        ], resize_keyboard=True, is_persistent=True))
            return

    # Admin state: delete_type_confirm
    if is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        if sess.get("state") == "delete_type_confirm":
            async with get_user_lock(user_id):
                async with _data_lock:
                    type_name = user_sessions.get(user_id, {}).get("type_name")
                if btn == BTN_DELETE_CONFIRM:
                    async with _data_lock:
                        user_sessions.pop(user_id, None)
                    asyncio.create_task(run_sync(_save_sessions))
                    if not type_name or type_name not in accounts_data.get("account_types", {}):
                        await send_msg(chat_id, "⚠️ <b>ប្រភេទនេះមិនមានទៀតហើយ!</b>",
                                       reply_markup=ADMIN_SETTINGS_KB)
                        return
                    async with _data_lock:
                        count = len(accounts_data["account_types"].pop(type_name, []))
                        accounts_data.get("prices", {}).pop(type_name, None)
                        accounts_data["accounts"] = [
                            a for a in accounts_data.get("accounts", []) if a.get("type") != type_name]
                    asyncio.create_task(run_sync(_save_data))
                    await send_msg(chat_id,
                                   f"✅ <b>បានលុបប្រភេទ <code>{html.escape(type_name)}</code> ចំនួន {count} records!</b>",
                                   reply_markup=ADMIN_SETTINGS_KB)
                    logger.info(f"Admin {user_id} deleted type '{type_name}' ({count} records)")
                elif btn == BTN_DELETE_CANCEL:
                    async with _data_lock:
                        user_sessions.pop(user_id, None)
                    asyncio.create_task(run_sync(_save_sessions))
                    await send_msg(chat_id, "🚫 <b>បានបោះបង់ការលុប</b>", reply_markup=ADMIN_SETTINGS_KB)
            return

    # Admin state: email_delete_picker
    if is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id, {})
        if sess.get("state") == "email_delete_picker":
            async with get_user_lock(user_id):
                if btn == BTN_BACK_SETTINGS:
                    async with _data_lock:
                        user_sessions.pop(user_id, None)
                    asyncio.create_task(run_sync(_save_sessions))
                    await send_msg(chat_id, "📧 <b>ការគ្រប់គ្រងអ៊ីម៉ែល</b>\n\nជ្រើសរើសប្រតិបត្តិការ៖",
                                   reply_markup=EMAIL_SUBMENU_KB)
                    return
                entry = await run_sync(_email_history_get_by_email, user_id, btn)
                if not entry:
                    await send_msg(chat_id, "❌ មិនឃើញអ៊ីម៉ែលនេះទេ។", reply_markup=EMAIL_SUBMENU_KB)
                    async with _data_lock:
                        user_sessions.pop(user_id, None)
                    asyncio.create_task(run_sync(_save_sessions))
                    return
                address_id = entry.get("address_id", "")
                entry_id   = entry.get("id")
                if address_id:
                    await run_sync(_dropmail_delete_address, address_id)
                if entry_id:
                    await run_sync(_email_history_delete, entry_id)
                async with _data_lock:
                    user_sessions.pop(user_id, None)
                asyncio.create_task(run_sync(_save_sessions))
                await send_msg(chat_id,
                               f"✅ <b>លុបអ៊ីម៉ែលបានសម្រេច។</b>\n<code>{html.escape(btn)}</code>",
                               reply_markup=EMAIL_SUBMENU_KB)
            return

    # Admin button labels dispatch
    if is_admin(user_id) and btn in ADMIN_BUTTON_LABELS:
        async with get_user_lock(user_id):
            await _dispatch_admin_button(update, user_id, chat_id, btn)
        return

    # Non-admin: payment_pending block
    if not is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id)
        if sess and sess.get("state") == "payment_pending":
            await _notify_must_finish_order(chat_id)
            return

    # Admin session messages (account upload flow)
    if is_admin(user_id):
        async with _data_lock:
            sess = user_sessions.get(user_id)
        if sess:
            async with get_user_lock(user_id):
                await _handle_admin_session_message(update, user_id, chat_id, message_id, text)
            return
        if text.strip().startswith("/"):
            return
        await show_account_selection(chat_id)
        return

    await show_account_selection(chat_id)


async def _handle_admin_session_message(update: Update, user_id, chat_id, message_id, text):
    global accounts_data
    async with _data_lock:
        sess = user_sessions.get(user_id)
    if not sess:
        await show_account_selection(chat_id)
        return

    state = sess.get("state", "")

    if state == "waiting_for_accounts":
        email_pat = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        accounts  = []
        for line in text.strip().split("\n"):
            em = line.strip()
            if em and email_pat.match(em):
                accounts.append({"email": em})
        async with _data_lock:
            all_existing = {
                a.get("email", "").lower()
                for accs in accounts_data.get("account_types", {}).values()
                for a in accs if a.get("email")
            }
        seen, deduped, intra_dupes = set(), [], []
        for a in accounts:
            k = a.get("email", "").lower()
            if k in seen:
                intra_dupes.append(a["email"])
            else:
                seen.add(k)
                deduped.append(a)
        stock_dupes = [a["email"] for a in deduped if a.get("email", "").lower() in all_existing]
        new_accounts = [a for a in deduped if a.get("email", "").lower() not in all_existing]
        if new_accounts:
            warnings = []
            if intra_dupes:
                warnings.append(f"⚠️ *អ៊ីមែលដដែល (រំលង)៖*\n```\n{chr(10).join(intra_dupes)}\n```")
            if stock_dupes:
                warnings.append(f"⚠️ *អ៊ីមែលមានស្រាប់ (រំលង)៖*\n```\n{chr(10).join(stock_dupes)}\n```")
            if warnings:
                await send_msg(chat_id, "\n\n".join(warnings), parse_mode=ParseMode.MARKDOWN)
            async with _data_lock:
                sess["accounts"] = new_accounts
                sess["state"]    = "waiting_for_account_type"
                existing_types = list(accounts_data.get("account_types", {}).keys())
            asyncio.create_task(run_sync(_save_sessions))
            type_rows = [[KeyboardButton(t)] for t in existing_types]
            type_rows.append([KeyboardButton(BTN_BACK_SETTINGS)])
            type_kb = ReplyKeyboardMarkup(type_rows, resize_keyboard=True, is_persistent=True)
            await send_msg(chat_id,
                           f"<b>បានបញ្ចូល គូប៉ុង ចំនួន {len(new_accounts)}\n\nសូមជ្រើសរើស ឬបញ្ចូលប្រភេទ គូប៉ុង៖</b>",
                           reply_markup=type_kb)
        elif accounts:
            await send_msg(chat_id, "<b>មិនអាចបញ្ចូលបាន</b>", reply_markup=ADD_ACCOUNT_KB)
        else:
            await send_msg(chat_id, "<b>អ៊ីមែលមិនត្រឹមត្រូវតាមទម្រង់</b>", reply_markup=ADD_ACCOUNT_KB)
        return

    if state == "waiting_for_account_type":
        account_type_input = text.strip()
        async with _data_lock:
            existing_price = accounts_data.get("prices", {}).get(account_type_input)
            sess["account_type"] = account_type_input
            sess["state"]        = "waiting_for_price"
        asyncio.create_task(run_sync(_save_sessions))
        if existing_price is not None:
            await send_msg(
                chat_id,
                f"<b>ប្រភេទ <code>{account_type_input}</code> មានស្រាប់ ដែលមានតម្លៃ {existing_price}$\n\nតម្លៃត្រូវតែដូចគ្នា ({existing_price}$) ដើម្បីបន្ថែម គូប៉ុង</b>",
                reply_markup=ADD_ACCOUNT_KB)
        else:
            await send_msg(chat_id,
                           f"<b>សូមដាក់តម្លៃក្នុងប្រភេទ គូប៉ុង {account_type_input}</b>",
                           reply_markup=ADD_ACCOUNT_KB)
        return

    if state == "waiting_for_price":
        try:
            price = float(text.strip().replace("$", ""))
            account_type = sess["account_type"]
            accs_to_add  = sess["accounts"]
            async with _data_lock:
                existing_price = accounts_data.get("prices", {}).get(account_type)
                all_existing   = {
                    a.get("email", "").lower()
                    for pool in accounts_data.get("account_types", {}).values()
                    for a in pool if a.get("email")
                }
            if existing_price is not None and round(existing_price, 4) != round(price, 4):
                await send_msg(
                    chat_id,
                    f"❌ <b>មិនអាចបញ្ចូលបាន!</b>\n\nប្រភេទ <code>{account_type}</code> មានតម្លៃ <b>{existing_price}$</b> ស្រាប់។\nតម្លៃ <b>{price}$</b> មិនដូចគ្នា។ សូមប្រើ <b>{existing_price}$</b>",
                    reply_markup=ADD_ACCOUNT_KB)
                return
            seen, deduped = set(), []
            for a in accs_to_add:
                k = a.get("email", "").lower()
                if k not in seen:
                    seen.add(k)
                    deduped.append(a)
            dup_emails  = [a["email"] for a in deduped if a.get("email", "").lower() in all_existing]
            new_accounts = [a for a in deduped if a.get("email", "").lower() not in all_existing]
            if dup_emails and not new_accounts:
                await send_msg(chat_id,
                               f"❌ *មិនអាចបញ្ចូលបាន!*\n\nEmail ទាំងអស់មានស្រាប់:\n```\n{chr(10).join(dup_emails)}\n```",
                               parse_mode=ParseMode.MARKDOWN)
                return
            if dup_emails:
                await send_msg(chat_id,
                               f"⚠️ *Email ខាងក្រោមមានស្រាប់ ហើយត្រូវបានរំលង:*\n```\n{chr(10).join(dup_emails)}\n```",
                               parse_mode=ParseMode.MARKDOWN)
            async with _data_lock:
                accounts_data["accounts"].extend(new_accounts)
                if account_type in accounts_data["account_types"]:
                    accounts_data["account_types"][account_type].extend(new_accounts)
                else:
                    accounts_data["account_types"][account_type] = new_accounts
                accounts_data["prices"][account_type] = price
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_data))
            asyncio.create_task(run_sync(_save_sessions))
            await send_msg(
                chat_id,
                f"*✅ បានបញ្ចូល គូប៉ុង ដោយជោគជ័យ*\n\n"
                f"```\n🔹 ចំនួន: {len(new_accounts)}\n🔹 ប្រភេទ: {account_type}\n🔹 តម្លៃ: {price}$\n```",
                parse_mode=ParseMode.MARKDOWN)
            logger.info(f"Admin {user_id} added {len(new_accounts)} accounts of type {account_type} @ ${price}")
            await send_admin_settings_menu(chat_id)
        except ValueError:
            await send_msg(chat_id, "តម្លៃមិនត្រឹមត្រូវ។ សូមបញ្ចូលតម្លៃជាលេខ (ឧ: 5.99)")
        return

    # Unrecognized admin message
    async with _data_lock:
        user_sessions.pop(user_id, None)
    asyncio.create_task(run_sync(_save_sessions))
    await show_account_selection(chat_id)



async def on_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_sync(_ensure_data_loaded)
    callback_query = update.callback_query
    user    = callback_query.from_user
    user_id = user.id
    chat_id = callback_query.message.chat.id
    data    = callback_query.data or ""
    logger.info(f"Callback from {user.first_name} (ID:{user_id}): {data}")

    asyncio.create_task(run_sync(_upsert_known_user, user_id, user.first_name, user.last_name, user.username))

    async with get_user_lock(user_id):
        await _handle_callback_locked(callback_query, user, user_id, chat_id, data)


async def _handle_callback_locked(cq, user, user_id, chat_id, data):
    try:
        if data.startswith("buy:") or data.startswith("buy_"):
            at = (_account_type_from_callback_id(data[4:]) if data.startswith("buy:")
                  else data.replace("buy_", ""))
            if not at:
                await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ។", show_alert=True)
                return
            if await _has_active_purchase(user_id):
                await cq.answer("សូមបញ្ចប់ការទិញបច្ចុប្បន្នជាមុនសិន", show_alert=True)
                return
            await cq.answer()
            async with _data_lock:
                count = len(accounts_data.get("account_types", {}).get(at, []))
                price = accounts_data.get("prices", {}).get(at, 0)
            if count <= 0:
                await send_msg(chat_id, f"សុំទោស! គូប៉ុង {at} អស់ស្តុក។")
                return
            await _reset_user_session(user_id, save=False)
            async with _data_lock:
                count = len(accounts_data["account_types"].get(at, []))
                user_sessions[user_id] = {
                    "state": "waiting_for_quantity", "account_type": at,
                    "price": price, "available_count": count, "started_at": time.time(),
                }
            asyncio.create_task(run_sync(_save_sessions))
            type_cb_id = _type_callback_id(at)
            qty_buttons = [
                InlineKeyboardButton(str(n), callback_data=f"qty:{type_cb_id}:{n}")
                for n in range(1, count + 1)
            ]
            rows_inline = [qty_buttons[i:i+5] for i in range(0, len(qty_buttons), 5)]
            rows_inline.append([InlineKeyboardButton("🚫 បោះបង់", callback_data="cancel_buy")])
            await send_msg(chat_id, "<b>សូមជ្រើសរើសចំនួនដែលចង់ទិញ៖</b>",
                           reply_markup=InlineKeyboardMarkup(rows_inline))
            return

        if data.startswith("out_of_stock"):
            await cq.answer()
            at = (_account_type_from_callback_id(data[13:]) if data.startswith("out_of_stock:")
                  else data.replace("out_of_stock_", "")) or "នេះ"
            await send_msg(chat_id, f"<i>សូមអភ័យទោស គូប៉ុង {at} អស់ពីស្តុក 🪤</i>",
                           parse_mode=ParseMode.HTML)
            return

        if data.startswith("dts:") and is_admin(user_id):
            type_name = _account_type_from_callback_id(data[4:]) or data[4:]
            if type_name not in accounts_data.get("account_types", {}):
                await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ!", show_alert=True)
                return
            await cq.answer()
            async with _data_lock:
                count = len(accounts_data["account_types"].get(type_name, []))
                price = accounts_data.get("prices", {}).get(type_name, 0)
            confirm_cb = f"dtc:{_type_callback_id(type_name)}"
            await send_msg(
                chat_id,
                f"⚠️ <b>តើអ្នកពិតជាចង់លុបប្រភេទ គូប៉ុង នេះមែនទេ?</b>\n\n"
                f"<blockquote>🔹 ប្រភេទ: {html.escape(type_name)}\n"
                f"🔹 ចំនួន: {count}\n🔹 តម្លៃ: ${price}</blockquote>",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ បញ្ជាក់លុប", callback_data=confirm_cb),
                    InlineKeyboardButton("🚫 បោះបង់", callback_data="cancel_delete_type"),
                ]]))
            return

        if data.startswith("dtc:") and is_admin(user_id):
            type_name = _account_type_from_callback_id(data[4:]) or data[4:]
            await cq.answer()
            if type_name not in accounts_data.get("account_types", {}):
                await send_msg(chat_id, "⚠️ <b>ប្រភេទនេះមិនមានទៀតហើយ!</b>",
                               reply_markup=ADMIN_SETTINGS_KB)
                return
            async with _data_lock:
                count = len(accounts_data["account_types"].pop(type_name, []))
                accounts_data.get("prices", {}).pop(type_name, None)
                accounts_data["accounts"] = [
                    a for a in accounts_data.get("accounts", []) if a.get("type") != type_name]
            asyncio.create_task(run_sync(_save_data))
            await send_msg(chat_id,
                           f"✅ <b>បានលុបប្រភេទ <code>{html.escape(type_name)}</code> ចំនួន {count} records!</b>",
                           reply_markup=ADMIN_SETTINGS_KB)
            return

        if data == "cancel_delete_type":
            await cq.answer()
            await send_admin_settings_menu(chat_id)
            return

        if data.startswith("qty:"):
            parts = data.split(":")
            if len(parts) != 3:
                await cq.answer()
                return
            type_cb_id = parts[1]
            try:
                quantity = int(parts[2])
            except ValueError:
                await cq.answer()
                return
            target_type = _account_type_from_callback_id(type_cb_id)
            async with _data_lock:
                session = user_sessions.get(user_id)
            if not session or session.get("state") != "waiting_for_quantity":
                if target_type:
                    if target_type not in accounts_data.get("account_types", {}):
                        await cq.answer("ប្រភេទនេះមិនមានទៀតហើយ។", show_alert=True)
                        return
                    await _reset_user_session(user_id, save=False)
                    async with _data_lock:
                        available = len(accounts_data["account_types"].get(target_type, []))
                        price     = accounts_data.get("prices", {}).get(target_type, 0)
                    if available <= 0:
                        await cq.answer(f"សូមអភ័យទោស គូប៉ុង {target_type} អស់ពីស្តុក 🪤", show_alert=True)
                        return
                    async with _data_lock:
                        user_sessions[user_id] = {
                            "state": "waiting_for_quantity", "account_type": target_type,
                            "price": price, "available_count": available, "started_at": time.time(),
                        }
                        session = user_sessions[user_id]
                elif not session or session.get("state") != "waiting_for_quantity":
                    await cq.answer()
                    return
            if quantity > session["available_count"]:
                await cq.answer(f"សុំទោស! មានត្រឹមតែ {session['available_count']} នៅក្នុងស្តុក", show_alert=True)
                return
            async with _data_lock:
                session["quantity"]    = quantity
                session["total_price"] = quantity * session["price"]
            asyncio.create_task(delete_msg(chat_id, cq.message.message_id))
            await _start_payment_for_session(chat_id, user_id, session, callback_query=cq)
            return

        if data == "check_payment":
            async with _data_lock:
                session = user_sessions.get(user_id)
            if not session or session.get("state") != "payment_pending":
                session = await run_sync(_get_pending_payment, user_id)
            if not session:
                await cq.answer()
                return
            md5 = session.get("md5_hash")
            if not md5:
                await cq.answer("មានបញ្ហា។ សូមចាប់ផ្តើមម្តងទៀត។", show_alert=True)
                return
            is_paid, payment_data = await run_sync(_check_payment_status, md5)
            if is_paid:
                await cq.answer("✅ បានទទួលការបង់ប្រាក់!")
                user_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                await deliver_accounts(chat_id, user_id, session,
                                       payment_data=payment_data, user_name=user_name)
                asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                asyncio.create_task(run_sync(_save_sessions))
            else:
                await cq.answer(
                    "⏳ មិនទាន់បានទទួលការបង់ប្រាក់។\nសូមបង់ប្រាក់ហើយចុចពិនិត្យម្ដងទៀត។",
                    show_alert=True)
            return

        if data.startswith("copy_otp:"):
            code = data.split(":", 1)[1]
            await cq.answer(code, show_alert=True)
            return

        if data == "cancel_buy":
            await cq.answer()
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            asyncio.create_task(delete_msg(chat_id, cq.message.message_id))
            await show_account_selection(chat_id)
            return

        if data == "cancel_purchase":
            async with _data_lock:
                session = user_sessions.get(user_id)
            if not session:
                session = await run_sync(_get_pending_payment, user_id)
            md5 = session.get("md5_hash") if session else None
            if md5:
                try:
                    is_paid, payment_data = await run_sync(_check_payment_status, md5)
                except Exception:
                    is_paid, payment_data = False, None
                if is_paid:
                    await cq.answer("✅ បានទទួលការបង់ប្រាក់!")
                    user_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                    await deliver_accounts(chat_id, user_id, session,
                                           payment_data=payment_data, user_name=user_name)
                    asyncio.create_task(run_sync(_delete_pending_payment, user_id))
                    asyncio.create_task(run_sync(_save_sessions))
                    return
            await cq.answer()
            for key in ("photo_message_id", "qr_message_id", "dot_message_id"):
                mid = session.get(key) if session else None
                if mid:
                    asyncio.create_task(delete_msg(chat_id, mid))
            if session:
                await _release_reserved_accounts(session)
            async with _data_lock:
                user_sessions.pop(user_id, None)
            asyncio.create_task(run_sync(_save_sessions))
            asyncio.create_task(run_sync(_delete_pending_payment, user_id))
            await show_account_selection(chat_id)
            return

    except Exception as e:
        logger.error(f"Callback handler error for user {user_id}: {e}")


# ── 19. Background periodic sweeper ──────────────────────────────────────────
async def _check_active_pending_payments():
    try:
        r = await run_sync(
            _db_query,
            "SELECT user_id, chat_id, account_type, quantity, total_price, md5_hash, reserved_accounts "
            "FROM bot_pending_payments "
            "WHERE created_at + interval '1 second' * %s >= NOW()",
            [PAYMENT_TIMEOUT_SECONDS])
        rows = r.get("rows", []) or []
    except Exception as e:
        logger.warning(f"Failed to query active pending payments: {e}")
        return

    for row in rows:
        try:
            user_id = int(row["user_id"])
            async with _data_lock:
                active_session = user_sessions.get(user_id)
            if active_session and active_session.get("state") == "payment_pending":
                continue
            md5 = row.get("md5_hash")
            if not md5:
                continue
            is_paid, payment_data = await run_sync(_check_payment_status, md5)
            if not is_paid:
                continue
            reserved = row.get("reserved_accounts") or []
            if isinstance(reserved, str):
                try:
                    reserved = json.loads(reserved)
                except Exception:
                    reserved = []
            session = {
                "state": "payment_pending",
                "account_type": row.get("account_type"),
                "quantity": int(row.get("quantity") or 1),
                "total_price": float(row.get("total_price") or 0),
                "md5_hash": md5,
                "reserved_accounts": reserved,
            }
            chat_id = int(row.get("chat_id") or user_id)
            logger.info(f"Sweeper detected paid payment for user {user_id}, delivering accounts")
            await deliver_accounts(chat_id, user_id, session, payment_data=payment_data)
            asyncio.create_task(run_sync(_delete_pending_payment, user_id))
            asyncio.create_task(run_sync(_save_sessions))
        except Exception as e:
            logger.warning(f"Sweeper failed to process payment row {row}: {e}")


async def _pending_payment_sweeper(interval: int = 60):
    while True:
        await asyncio.sleep(interval)
        try:
            await _check_active_pending_payments()
        except Exception as e:
            logger.warning(f"Active payment check failed: {e}")
        try:
            await run_sync(_cleanup_expired_pending_payments)
        except Exception as e:
            logger.warning(f"Sweeper iteration failed: {e}")


async def _email_poller(interval: int = 10):
    while True:
        try:
            await asyncio.sleep(interval)
            if not DROPMAIL_API_TOKEN:
                continue
            entries = await run_sync(_email_history_all_entries)
            for entry in entries:
                entry_id    = entry.get("id")
                user_id     = int(entry.get("telegram_user_id") or 0)
                email_addr  = entry.get("email_address", "")
                session_id  = entry.get("dropmail_session_id")
                restore_key = entry.get("restore_key")
                last_mail_id = entry.get("last_mail_id")
                if not session_id:
                    continue
                try:
                    mails = await run_sync(_dropmail_get_mails, session_id, last_mail_id)
                except Exception as e:
                    logger.debug(f"[email_poller] poll error [{email_addr}]: {e}")
                    continue
                if mails is None:
                    if not restore_key:
                        continue
                    try:
                        restored = await run_sync(_dropmail_restore_session, email_addr, restore_key)
                        if restored and restored.get("session_id"):
                            await run_sync(_email_history_update_session, entry_id,
                                           restored["session_id"],
                                           restored.get("address_id", ""),
                                           restored.get("restore_key", ""))
                            logger.info(f"[email_poller] Restored [{email_addr}] → {restored['session_id']}")
                    except Exception as e:
                        logger.debug(f"[email_poller] restore error [{email_addr}]: {e}")
                    continue
                if not mails:
                    continue
                newest_id = None
                for mail in mails:
                    mail_id   = mail.get("id")
                    if last_mail_id and mail_id == last_mail_id:
                        continue
                    subject   = mail.get("headerSubject") or "(គ្មានប្រធានបទ)"
                    from_addr = mail.get("fromAddr") or "unknown"
                    to_addr   = mail.get("toAddr") or email_addr
                    body      = (mail.get("text") or "").strip()
                    preview = body[:1200] + "\n…" if len(body) > 1200 else body
                    text = (
                        f"📬 <b>អ៊ីម៉ែលថ្មីចូលមកដល់!</b>\n\n"
                        f"📨 ប្រធានបទ: <b>{html.escape(subject)}</b>\n"
                        f"📧 ពី: <code>{html.escape(from_addr)}</code>\n"
                        f"📥 ទៅ: <code>{html.escape(to_addr)}</code>\n\n"
                        f"{html.escape(preview) if preview else '<i>(ទទេ)</i>'}"
                    )
                    try:
                        await send_msg(user_id, text)
                    except Exception as e:
                        logger.warning(f"[email_poller] notify failed: {e}")
                    if user_id != ADMIN_ID:
                        admin_text = (
                            f"📧 <b>សារអ៊ីម៉ែលថ្មីរបស់ User <code>{user_id}</code></b>\n\n"
                            + text
                        )
                        try:
                            await send_msg(ADMIN_ID, admin_text)
                        except Exception as e:
                            logger.warning(f"[email_poller] admin notify failed: {e}")
                    newest_id = mail_id
                if newest_id:
                    await run_sync(_email_history_update_last_mail, entry_id, newest_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[email_poller] outer error: {e}")


async def _resume_scheduled_deletions():
    try:
        r = await run_sync(
            _db_query,
            "SELECT chat_id, message_id, "
            "GREATEST(0, EXTRACT(EPOCH FROM (delete_at - NOW()))::INTEGER) AS remaining "
            "FROM bot_scheduled_deletions")
        rows = r.get("rows", []) or []
        for row in rows:
            try:
                cid = int(row["chat_id"])
                mid = int(row["message_id"])
                rem = int(row.get("remaining") or 0)
                asyncio.create_task(delete_msg_later(cid, mid, rem))
            except Exception as e:
                logger.warning(f"Bad scheduled deletion row {row}: {e}")
        if rows:
            logger.info(f"Resumed {len(rows)} scheduled deletion(s)")
    except Exception as e:
        logger.error(f"Failed to resume scheduled deletions: {e}")




# ── 20. Startup ───────────────────────────────────────────────────────────────
async def _on_startup(app_: Application):
    global accounts_data, MAINTENANCE_MODE
    global DROPMAIL_API_TOKEN, DROPMAIL_TOKEN_EXPIRY, _DROPMAIL_URL
    global _data_initialized

    await run_sync(_init_db)

    _sv = await run_sync(_get_setting, "MAINTENANCE_MODE")
    if _sv is not None:
        MAINTENANCE_MODE = str(_sv).lower() == "true"
        logger.info(f"Loaded MAINTENANCE_MODE: {MAINTENANCE_MODE}")

    _sv = await run_sync(_get_setting, "DROPMAIL_API_TOKEN")
    if _sv:
        DROPMAIL_API_TOKEN = _sv
        _DROPMAIL_URL = f"https://dropmail.me/api/graphql/{DROPMAIL_API_TOKEN}"
        logger.info(f"Loaded DROPMAIL_API_TOKEN from DB: {DROPMAIL_API_TOKEN[:6]}…")

    _sv = await run_sync(_get_setting, "DROPMAIL_TOKEN_EXPIRY")
    if _sv:
        DROPMAIL_TOKEN_EXPIRY = _sv
        logger.info(f"Loaded DROPMAIL_TOKEN_EXPIRY from DB: {DROPMAIL_TOKEN_EXPIRY}")

    data = await run_sync(_load_data)
    accounts_data.update(data)
    await run_sync(_load_sessions)
    _data_initialized = True

    await _resume_scheduled_deletions()
    await run_sync(_cleanup_expired_pending_payments)

    if not WEBHOOK_MODE:
        asyncio.create_task(_pending_payment_sweeper(60))
        logger.info("Pending-payment sweeper started (every 60s)")
        asyncio.create_task(_email_poller(10))
        logger.info("Email poller started (every 10s)")
    else:
        logger.info("Webhook mode — background pollers skipped")

    me = await _bot.get_me()
    logger.info(f"Bot connected: @{me.username}")
    logger.info("Bot is now listening for updates (python-telegram-bot Bot API)...")


# ── 21. Handler registration ──────────────────────────────────────────────────
def _register_handlers():
    application.add_handler(
        MessageHandler(ptb_filters.ChatType.CHANNEL, on_channel_post), group=-10)
    application.add_handler(
        CommandHandler("start",  on_start,  filters=ptb_filters.ChatType.PRIVATE), group=0)
    application.add_handler(
        CommandHandler("cancel",   on_cancel,   filters=ptb_filters.ChatType.PRIVATE), group=0)
    application.add_handler(
        CommandHandler("settings", on_settings, filters=ptb_filters.ChatType.PRIVATE), group=0)
    application.add_handler(
        MessageHandler(ptb_filters.ChatType.PRIVATE & ptb_filters.TEXT, on_private_message), group=1)
    application.add_handler(
        CallbackQueryHandler(on_callback_query), group=0)
    application.post_init = _on_startup


# ── 22. Main ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _register_handlers()
    logger.info("Starting bot with python-telegram-bot (Bot API polling)...")
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )
