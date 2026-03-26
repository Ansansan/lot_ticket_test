import json
import os
import telebot 
import logging
# Suppress noisy logs
telebot.logger.setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL) 
import sqlite3 
import datetime
import pytz 
import math
import random
import hashlib
import time
import urllib.parse
import re
import requests
import mimetypes
from requests.exceptions import ReadTimeout, ConnectionError
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image, ImageDraw, ImageFont 
import io
import base64
import queue
import threading
import shutil
from types import SimpleNamespace
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import sys

# Force UTF-8 console output and auto-repair common mojibake in log prints.
import io as _io
for _stream_name in ('stdout', 'stderr'):
    _stream = getattr(sys, _stream_name)
    if hasattr(_stream, 'encoding') and _stream.encoding and _stream.encoding.lower().replace('-','') == 'utf8':
        continue  # already UTF-8
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        try:
            wrapped = _io.TextIOWrapper(
                _stream.buffer, encoding="utf-8", errors="replace", line_buffering=True
            )
            setattr(sys, _stream_name, wrapped)
        except Exception:
            pass

_MOJIBAKE_HINTS = ("\u00C2", "\u00C3", "\u00E2", "\u00F0", "\u00EF")
_BUILTIN_PRINT = print


def _fix_mojibake_text(value):
    """Safety net: repair double-encoded UTF-8 that may come from .env or external sources."""
    if not isinstance(value, str):
        return value
    if not any(ch in value for ch in _MOJIBAKE_HINTS):
        return value
    try:
        fixed = value.encode("latin1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return value
    orig_noise = sum(value.count(ch) for ch in _MOJIBAKE_HINTS)
    fixed_noise = sum(fixed.count(ch) for ch in _MOJIBAKE_HINTS)
    return fixed if fixed_noise < orig_noise else value


def print(*args, **kwargs):
    repaired_args = tuple(_fix_mojibake_text(arg) if isinstance(arg, str) else arg for arg in args)
    _BUILTIN_PRINT(*repaired_args, **kwargs)

# Global Queue for OCR tasks
OCR_QUEUE = queue.Queue()
REACTION_LOCK = threading.Lock()

MIRROR_LOCK = threading.Lock()
MIRROR_QUEUE_LOCK = threading.Lock()
MIRROR_QUEUE_BY_USER = {}
MIRROR_WORKERS = {}
MIRROR_CACHE_LOCK = threading.Lock()
SUPPORT_THREAD_CACHE = {}
MESSAGE_MAP_CACHE = OrderedDict()
MESSAGE_MAP_CACHE_LIMIT = 10000
MIRROR_REORDER_GRACE_SECONDS = 0.25
MIRROR_RETRY_BASE_SECONDS = 2.0
MIRROR_RETRY_MAX_SECONDS = 300.0
MIRROR_QUEUE_RECHECK_SECONDS = 5.0
MIRROR_ENQUEUED_TASK_IDS = set()
RECEIPT_REMINDER_MINUTES = 10
RECEIPT_REMINDER_POLL_SECONDS = 30
RECEIPT_PROCESSED_LABEL = "Procesado"
LISTA_DRAFT_TTL_MINUTES = 30
LISTA_APP_SCHEMA_VERSION = 2
LISTA_SOURCE_MANUAL = "manual_chat"
LISTA_SOURCE_ANDROID = "android_app"
LISTA_SOURCE_WEBAPP = "webapp"
LISTA_SOURCE_WEBAPP_EDIT = "webapp_edit"
LISTA_WAITING_USERS = {}
LISTA_WAITING_LOCK = threading.Lock()
HTTP_SESSION_LOCAL = threading.local()
RESOURCE_CACHE_LOCK = threading.Lock()
FONT_CACHE = {}
FLAG_IMAGE_CACHE = {}
DEFAULT_TICKET_FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
DEFAULT_TICKET_FONT_BOLD_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

# Load environment variables from .env file
# Determine Base Directory (Force absolute path)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from .env file (Explicit path)
dotenv_path = os.path.join(BASE_DIR, '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"✅ Loaded .env from: {dotenv_path}")
else:
    print(f"⚠️ .env NOT found at: {dotenv_path}")
    # Fallback: Try implicit load
    load_dotenv()

# --- CONFIGURACIÓN TEST BOT 1 (Loaded from .env) ---
TOKEN = os.getenv('BOT_TOKEN')
ADMIN_GROUP_ID = int(os.getenv('ADMIN_GROUP_ID', -1003595738966))
ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', 8550582981))
HISTORY_API_BASE = os.getenv('HISTORY_API_BASE', 'https://tel.pythonanywhere.com/')

# 🔐 CLAVE SECRETA (SALT)
SECURITY_SALT = os.getenv('SECURITY_SALT', 'TicaPanama857')

# --- TOPIC MAPPING (Loaded from .env) ---
TOPIC_MAPPING = {
    "Nacional": int(os.getenv('TOPIC_NACIONAL', 3)),
    "Tica": int(os.getenv('TOPIC_TICA', 4)),
    "Nica": int(os.getenv('TOPIC_NICA', 5)),
    "Primera": int(os.getenv('TOPIC_PRIMERA', 6))
}

# --- YAPPY PAYMENT VERIFICATION (New) ---
# Legacy OCR.space fallback remains available, but Gemini is preferred for current fallback testing.
OCR_API_KEY = os.getenv('OCR_API_KEY', '').strip()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '').strip()
GEMINI_API_BASE = os.getenv('GEMINI_API_BASE', 'https://generativelanguage.googleapis.com/v1beta/models').strip()
GEMINI_MODEL = os.getenv('GEMINI_MODEL', 'gemini-2.5-flash-lite').strip()

# --- DeepInfra Vision OCR ---
DEEPINFRA_API_KEY = (os.getenv('DEEPINFRA_API_KEY', '') or os.getenv('DEEPSEEK_API_KEY', '')).strip()
DEEPINFRA_API_BASE = (os.getenv('DEEPINFRA_API_BASE', '') or os.getenv('DEEPSEEK_API_BASE', 'https://api.deepinfra.com/v1/openai/chat/completions')).strip()
DEEPSEEK_MODEL = os.getenv('DEEPSEEK_MODEL', 'deepseek-ai/DeepSeek-OCR').strip()
QWEN_MODEL = os.getenv('QWEN_MODEL', '').strip()

YAPPY_PAYMENTS_GROUP_ID = int(os.getenv('YAPPY_PAYMENTS_GROUP_ID', 0)) if os.getenv('YAPPY_PAYMENTS_GROUP_ID') else None
RECEIPT_FORWARD_CHANNEL_ID = int(os.getenv('RECEIPT_FORWARD_CHANNEL_ID', 0)) if os.getenv('RECEIPT_FORWARD_CHANNEL_ID') else None
YAPPY_DIRECT_INGEST_ENABLED = os.getenv('YAPPY_DIRECT_INGEST_ENABLED', '0').strip().lower() in ('1', 'true', 'yes', 'on')
MONEY_REQUEST_WARNING_GROUP_ID = int(os.getenv('MONEY_REQUEST_WARNING_GROUP_ID', -1003795391458))
WEBAPP_BASE_URL = os.getenv('WEBAPP_BASE_URL', 'https://ansansan.github.io/LotTicket/test/')

# Shared bridge (producer bots -> lot ticket bot consumer)
YAPPY_BRIDGE_DB_PATH = os.getenv('YAPPY_BRIDGE_DB_PATH', os.path.join(BASE_DIR, 'yappy_bridge.db')).strip()
YAPPY_BRIDGE_ENABLED = os.getenv('YAPPY_BRIDGE_ENABLED', '1').strip().lower() in ('1', 'true', 'yes', 'on')
YAPPY_BRIDGE_POLL_SECONDS = max(1, int(os.getenv('YAPPY_BRIDGE_POLL_SECONDS', '3')))
YAPPY_BRIDGE_BATCH_SIZE = max(1, int(os.getenv('YAPPY_BRIDGE_BATCH_SIZE', '50')))
YAPPY_BRIDGE_BUSY_TIMEOUT_MS = max(1000, int(os.getenv('YAPPY_BRIDGE_BUSY_TIMEOUT_MS', '20000')))
YAPPY_BRIDGE_CONSUMER_KEY = (os.getenv('YAPPY_BRIDGE_CONSUMER_KEY', 'lot_ticket_test') or 'lot_ticket_test').strip()
_bridge_allow = (os.getenv('YAPPY_BRIDGE_SOURCE_ALLOWLIST', '') or '').strip()
YAPPY_BRIDGE_SOURCE_ALLOWLIST = {x.strip().lower() for x in _bridge_allow.split(',') if x.strip()} if _bridge_allow else None
YAPPY_BRIDGE_REACTION_EMOJI = (os.getenv('YAPPY_BRIDGE_REACTION_EMOJI', '👍') or '👍').strip()
YAPPY_REACTION_BOT_TOKEN = os.getenv('YAPPY_REACTION_BOT_TOKEN', '').strip()

# Per-user receipt reaction emojis (format: "user_id:emoji,user_id:emoji,...")
_admin_emoji_raw = os.getenv('ADMIN_RECEIPT_EMOJIS', '')
ADMIN_RECEIPT_EMOJIS = {}
for _pair in _admin_emoji_raw.split(','):
    if ':' in _pair:
        _uid, _em = _pair.strip().split(':', 1)
        if _uid.strip().isdigit():
            ADMIN_RECEIPT_EMOJIS[int(_uid.strip())] = _em.strip()
DEFAULT_RECEIPT_EMOJI = (os.getenv('DEFAULT_RECEIPT_EMOJI', '🎉') or '🎉').strip()

def is_admin_user(user_id):
    """Check if a user is an admin (present in ADMIN_RECEIPT_EMOJIS).
    Admin users don't need to pay for tickets and their tickets are never invalidated."""
    return int(user_id) in ADMIN_RECEIPT_EMOJIS


def should_mirror_user(user_id):
    """Skip admin users from topic mirroring to reduce admin-only overhead."""
    try:
        return not is_admin_user(user_id)
    except Exception:
        return True


def get_thread_http_session():
    """Reuse HTTP connections per worker thread for OCR providers."""
    session = getattr(HTTP_SESSION_LOCAL, 'session', None)
    if session is None:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        HTTP_SESSION_LOCAL.session = session
    return session


def prepare_ocr_image_payload(file_path):
    """Read the original screenshot once and reuse the exact bytes across OCR providers."""
    with open(file_path, "rb") as image_file:
        image_bytes = image_file.read()
    mime_type = mimetypes.guess_type(file_path)[0] or 'image/jpeg'
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    return {
        'file_path': file_path,
        'filename': os.path.basename(file_path),
        'mime_type': mime_type,
        'bytes': image_bytes,
        'base64': base64_image,
        'data_url': f"data:{mime_type};base64,{base64_image}"
    }


def get_ticket_fonts(scale):
    """Cache ticket fonts by scale so every ticket doesn't reopen font files."""
    with RESOURCE_CACHE_LOCK:
        cached = FONT_CACHE.get(scale)
    if cached:
        return cached

    try:
        fonts = (
            ImageFont.truetype(DEFAULT_TICKET_FONT_PATH, 22 * scale),
            ImageFont.truetype(DEFAULT_TICKET_FONT_PATH, 18 * scale),
            ImageFont.truetype(DEFAULT_TICKET_FONT_BOLD_PATH, 35 * scale),
            ImageFont.truetype(DEFAULT_TICKET_FONT_BOLD_PATH, 24 * scale),
            ImageFont.truetype(DEFAULT_TICKET_FONT_PATH, 24 * scale),
        )
    except Exception:
        fallback = ImageFont.load_default()
        fonts = (fallback, fallback, fallback, fallback, fallback)

    with RESOURCE_CACHE_LOCK:
        FONT_CACHE[scale] = fonts
    return fonts


def get_ticket_flag_image(flag_filename, flag_size):
    """Cache resized flag images keyed by source file and rendered size."""
    if not flag_filename:
        return None
    cache_key = (flag_filename, flag_size)
    with RESOURCE_CACHE_LOCK:
        cached = FLAG_IMAGE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        flag_img = Image.open(flag_filename).convert("RGBA")
        flag_img = flag_img.resize((flag_size, flag_size), Image.Resampling.LANCZOS)
    except Exception:
        flag_img = None

    with RESOURCE_CACHE_LOCK:
        FLAG_IMAGE_CACHE[cache_key] = flag_img
    return flag_img


def get_db_connection(timeout=30.0):
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    return sqlite3.connect(db_path, timeout=timeout)


def build_ticket_message_context(chat_id, from_user, reply_message_id=None):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=from_user,
        message_id=reply_message_id
    )


def remember_message_map(chat_id, user_msg_id, admin_msg_id):
    if chat_id is None or user_msg_id is None or admin_msg_id is None:
        return
    key = (int(chat_id), int(user_msg_id))
    with MIRROR_CACHE_LOCK:
        MESSAGE_MAP_CACHE[key] = int(admin_msg_id)
        MESSAGE_MAP_CACHE.move_to_end(key)
        while len(MESSAGE_MAP_CACHE) > MESSAGE_MAP_CACHE_LIMIT:
            MESSAGE_MAP_CACHE.popitem(last=False)

YAPPY_BRIDGE_RECOVERY_COOLDOWN_SECONDS = max(5, int(os.getenv('YAPPY_BRIDGE_RECOVERY_COOLDOWN_SECONDS', '30')))
YAPPY_BRIDGE_RETENTION_DAYS = max(7, int(os.getenv('YAPPY_BRIDGE_RETENTION_DAYS', '45')))
YAPPY_BRIDGE_MAINTENANCE_SECONDS = max(60, int(os.getenv('YAPPY_BRIDGE_MAINTENANCE_SECONDS', '600')))
_LAST_BRIDGE_RECOVERY_TS = 0
_BRIDGE_RECOVERY_LOCK = threading.Lock()

# Validate required environment variables
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN not found in .env file! Please set it.")
if not SECURITY_SALT:
    raise ValueError("❌ SECURITY_SALT not found in .env file! Please set it.")

# PREMIOS
AWARDS = {
    '2_digit_1': 14.00,
    '2_digit_2': 3.00,
    '2_digit_3': 2.00,
    '4_digit_12': 1000.00,
    '4_digit_13': 1000.00,
    '4_digit_23': 200.00
}

# ⏳ DEADLINE CONFIGURATION
DEADLINE_GRACE_MINUTES = 1
# Note: The specific times are embedded in the lottery_type string (e.g., "Nica 1:00 pm")
# But we can define overrides or closing windows here if needed.
# For now, we relies on parsing the time from the ticket type.
# EXACT CLOSING TIMES (For deadline enforcement)
LOTTERY_SCHEDULE = {
    'La Primera': ["11:00 AM", "06:00 PM"],
    'Nica': ["01:00 PM", "04:00 PM", "07:00 PM", "10:00 PM"],
    'Tica': ["01:55 PM", "05:30 PM", "08:30 PM"],
    'Nacional': ["03:00 PM"]
}

bot = telebot.TeleBot(TOKEN, num_threads=5, threaded=True)
PANAMA_TZ = pytz.timezone('America/Panama')

# 🔥 AUTO-UPDATE SYSTEM
# Version loaded from .env to match index.html
BOT_VERSION = os.getenv('BOT_VERSION', 'TEST_1_V10')

print(f"🚀 TEST BOT 1 started with Version ID: {BOT_VERSION}")

def init_db():
    conn = get_db_connection(timeout=30.0)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS tickets_v3 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, date TEXT, lottery_type TEXT, numbers_json TEXT, is_nacional INTEGER DEFAULT 0,
                  cost REAL DEFAULT 0, status TEXT DEFAULT 'PENDING', amount_paid REAL DEFAULT 0,
                  notif_stage INTEGER DEFAULT 0, tg_message_id INTEGER, tg_chat_id INTEGER,
                  source TEXT, request_id TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS ticket_drafts_v1
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  source TEXT NOT NULL,
                  raw_text TEXT,
                  items_json TEXT NOT NULL,
                  ticket_date TEXT,
                  lottery_type TEXT,
                  client_total REAL,
                  server_total REAL NOT NULL DEFAULT 0,
                  request_id TEXT,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  expires_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS draw_results 
                 (date TEXT, lottery_type TEXT, w1 TEXT, w2 TEXT, w3 TEXT, UNIQUE(date, lottery_type))''')
    c.execute('''CREATE TABLE IF NOT EXISTS nacional_dates (date_str TEXT PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS nacional_exclusions (date_str TEXT PRIMARY KEY)''')
    
    # 🆕 WALLET SYSTEM
    c.execute('''CREATE TABLE IF NOT EXISTS user_wallets 
                 (user_id INTEGER PRIMARY KEY, balance REAL DEFAULT 0.0)''')
                 
    conn.commit()
    conn.close()
    
    # Run migrations for existing databases
    migrate_db_v3()
    migrate_db_v4()
    migrate_db_v5()
    migrate_db_v6()

def migrate_db_v3():
    """Add new columns to tickets_v3 if they don't exist"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        
        # Check if columns exist
        c.execute("PRAGMA table_info(tickets_v3)")
        columns = [info[1] for info in c.fetchall()]
        
        if 'cost' not in columns:
            print("📦 Migrating DB: Adding 'cost' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN cost REAL DEFAULT 0")
            
        if 'status' not in columns:
            print("📦 Migrating DB: Adding 'status' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN status TEXT DEFAULT 'PENDING'")
            
        if 'amount_paid' not in columns:
            print("📦 Migrating DB: Adding 'amount_paid' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN amount_paid REAL DEFAULT 0")
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ Migration Warning: {e}")

def migrate_db_v4():
    """Add notification stage column"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        
        c.execute("PRAGMA table_info(tickets_v3)")
        columns = [info[1] for info in c.fetchall()]
        
        if 'notif_stage' not in columns:
            print("📦 Migrating DB: Adding 'notif_stage' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN notif_stage INTEGER DEFAULT 0")
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Migration V4 Error: {e}")

def migrate_db_v5():
    """Add telegram message tracking columns for inline button updates"""
    try:
        conn = get_db_connection(timeout=30.0)
        c = conn.cursor()
        
        c.execute("PRAGMA table_info(tickets_v3)")
        columns = [info[1] for info in c.fetchall()]
        
        if 'tg_message_id' not in columns:
            print("📦 Migrating DB: Adding 'tg_message_id' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN tg_message_id INTEGER")
        if 'tg_chat_id' not in columns:
            print("📦 Migrating DB: Adding 'tg_chat_id' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN tg_chat_id INTEGER")
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Migration V5 Error: {e}")


def migrate_db_v6():
    """Add /lista draft storage and idempotency metadata."""
    try:
        conn = get_db_connection(timeout=30.0)
        c = conn.cursor()

        c.execute("PRAGMA table_info(tickets_v3)")
        columns = [info[1] for info in c.fetchall()]

        if 'source' not in columns:
            print("📦 Migrating DB: Adding 'source' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN source TEXT")
        if 'request_id' not in columns:
            print("📦 Migrating DB: Adding 'request_id' column...")
            c.execute("ALTER TABLE tickets_v3 ADD COLUMN request_id TEXT")

        c.execute('''CREATE TABLE IF NOT EXISTS ticket_drafts_v1
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER NOT NULL,
                      source TEXT NOT NULL,
                      raw_text TEXT,
                      items_json TEXT NOT NULL,
                      ticket_date TEXT,
                      lottery_type TEXT,
                      client_total REAL,
                      server_total REAL NOT NULL DEFAULT 0,
                      request_id TEXT,
                      status TEXT NOT NULL,
                      created_at TEXT NOT NULL,
                      updated_at TEXT NOT NULL,
                      expires_at TEXT)''')
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tickets_v3_request_id ON tickets_v3(request_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ticket_drafts_user_status ON ticket_drafts_v1(user_id, status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ticket_drafts_request_id ON ticket_drafts_v1(request_id)")

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Migration V6 Error: {e}")

# ---------------------------------------------------------
# 💰 WALLET & TRANSACTION LOGIC
# ---------------------------------------------------------

def get_wallet_balance(user_id):
    """Get current wallet balance for a user"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0.0
    except Exception as e:
        print(f"❌ Wallet Read Error: {e}")
        return 0.0

def update_wallet_balance(user_id, amount_change):
    """Add or subtract funds from wallet"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        
        # Ensure wallet exists
        c.execute("INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 0)", (user_id,))
        
        # Update balance
        c.execute("UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?", (amount_change, user_id))
        
        # Get new balance
        c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
        new_bal = c.fetchone()[0]
        
        conn.commit()
        conn.close()
        return new_bal
    except Exception as e:
        print(f"❌ Wallet Update Error: {e}")
        return 0.0

def calculate_ticket_cost(items):
    """Calculate total cost of a ticket from items list (Serverside Validation)"""
    total = 0.0
    for item in items:
        # 🛡️ SECURITY FIX: Do NOT trust frontend 'totalLine'
        # Recalculate based on rules
        try:
            num = str(item.get('num', '')).strip()
            qty = int(item.get('qty', 0))
            if qty < 1: continue
            
            total += calculate_ticket_line_total(num, qty)
        except Exception as e:
            print(f"⚠️ Skipping invalid ticket item in calculate_ticket_cost: item={item!r} error={e}")
            continue
    return total

def calculate_ticket_line_total(num, qty):
    """Recalculate a single ticket line total using server-side pricing rules only."""
    try:
        safe_num = str(num or '').strip()
        safe_qty = int(qty)
    except Exception:
        return 0.0

    if safe_qty < 1:
        return 0.0
    if len(safe_num) == 2:
        return 0.25 * safe_qty
    if len(safe_num) == 4:
        return 1.00 * safe_qty
    return 0.0


def calculate_server_total(items):
    return round(calculate_ticket_cost(items), 2)


def validate_normalized_items(items):
    if not isinstance(items, list):
        raise ValueError("items_must_be_list")

    normalized = []
    actual_items = 0
    for raw_item in items:
        if isinstance(raw_item, dict) and (raw_item.get('separator') or str(raw_item.get('num', '')).strip() == '---'):
            normalized.append({'num': '---', 'qty': 0, 'separator': True})
            continue

        if not isinstance(raw_item, dict):
            raise ValueError("item_must_be_object")

        num = re.sub(r'\D+', '', str(raw_item.get('num', '')).strip())
        qty_raw = raw_item.get('qty')
        try:
            qty = int(qty_raw)
        except Exception:
            raise ValueError("item_qty_invalid")

        if len(num) == 1:
            num = f"0{num}"
        if len(num) not in (2, 4) or not num.isdigit():
            raise ValueError("item_num_invalid")
        if qty < 1:
            raise ValueError("item_qty_invalid")

        normalized.append({'num': num, 'qty': qty})
        actual_items += 1

    if actual_items == 0:
        raise ValueError("items_empty")

    return normalized


def _manual_add_parsed_pair(results, left, right, fmt_name, qty_order):
    left_value = str(left).strip()
    right_value = str(right).strip()

    if fmt_name == 'equals':
        try:
            qty = int(left_value)
        except Exception:
            return
        num = right_value
    elif fmt_name == 'vil':
        try:
            qty = int(left_value)
        except Exception:
            return
        num = right_value
    else:
        left_is_4 = len(left_value) == 4
        right_is_4 = len(right_value) == 4
        if left_is_4 and not right_is_4:
            num = left_value
            try:
                qty = int(right_value)
            except Exception:
                qty = 1
        elif right_is_4 and not left_is_4:
            num = right_value
            try:
                qty = int(left_value)
            except Exception:
                qty = 1
        else:
            try:
                if qty_order == 'left':
                    qty = int(left_value)
                    num = right_value
                else:
                    num = left_value
                    qty = int(right_value)
            except Exception:
                return

    if len(num) == 1:
        num = f"0{num}"

    if len(num) in (2, 4) and num.isdigit() and qty >= 1:
        results.append({'num': num, 'qty': qty})


def _manual_detect_paste_order(text):
    has_left = re.search(r'\b(iz|izq|izquierda)\b', text or '', flags=re.IGNORECASE)
    has_right = re.search(r'\b(der|derecha)\b', text or '', flags=re.IGNORECASE)
    if has_left and not has_right:
        return 'left'
    if has_right and not has_left:
        return 'right'

    cleaned = re.sub(r'\[[\d/]+,\s*[\d:]+\]\s*[^:\n]+:\s*', '', text or '')
    pairs = []
    for line in [part.strip() for part in cleaned.splitlines() if part.strip()]:
        dot_matches = list(re.finditer(r'(\d+)\.{2,}(\d+)', line))
        if dot_matches:
            for match in dot_matches:
                if len(match.group(1)) != 4 and len(match.group(2)) != 4:
                    pairs.append((match.group(1), match.group(2)))
            continue

        dash_matches = list(re.finditer(r'(\d+)\s*[-/]\s*(\d+)', line))
        if dash_matches:
            for match in dash_matches:
                if len(match.group(1)) != 4 and len(match.group(2)) != 4:
                    pairs.append((match.group(1), match.group(2)))
            continue

        space_pair = re.match(r'^\s*(\d+)\s+(\d+)\s*$', line)
        if space_pair and len(space_pair.group(1)) != 4 and len(space_pair.group(2)) != 4:
            pairs.append((space_pair.group(1), space_pair.group(2)))

    if len(pairs) < 2:
        return None

    lefts = [pair[0] for pair in pairs]
    rights = [pair[1] for pair in pairs]
    score = 0

    score += (sum(1 for item in lefts if len(item) == 1) - sum(1 for item in rights if len(item) == 1)) * 3
    score += (
        sum(1 for item in rights if len(item) > 1 and item.startswith('0'))
        - sum(1 for item in lefts if len(item) > 1 and item.startswith('0'))
    ) * 3

    left_all_mult5 = all(((int(item) if item.isdigit() else 1) % 5) == 0 for item in lefts)
    right_all_mult5 = all(((int(item) if item.isdigit() else 1) % 5) == 0 for item in rights)
    if left_all_mult5 and not right_all_mult5:
        score += 2
    elif right_all_mult5 and not left_all_mult5:
        score -= 2

    left_unique = len(set(lefts))
    right_unique = len(set(rights))
    if left_unique < right_unique:
        score += 2
    elif right_unique < left_unique:
        score -= 2

    left_mean = sum(int(item) for item in lefts if item.isdigit()) / max(1, len([item for item in lefts if item.isdigit()]))
    right_mean = sum(int(item) for item in rights if item.isdigit()) / max(1, len([item for item in rights if item.isdigit()]))
    if left_mean < right_mean:
        score += 1
    elif right_mean < left_mean:
        score -= 1

    if score >= 2:
        return 'left'
    if score <= -2:
        return 'right'
    return None


def parse_manual_ticket_text(raw_text):
    if not str(raw_text or '').strip():
        return []

    qty_order = _manual_detect_paste_order(raw_text) or 'left'
    cleaned = re.sub(r'\b(izquierda|derecha|iz|izq|der)\b', '', raw_text or '', flags=re.IGNORECASE)
    cleaned = re.sub(r'\[[\d/]+,\s*[\d:]+\]\s*[^:\n]+:\s*', '', cleaned)
    groups = [group.strip() for group in re.split(r'[*&]', cleaned) if group.strip()]
    results = []
    sticky_qty = 0

    for group_index, group in enumerate(groups):
        if group_index > 0:
            results.append({'num': '---', 'qty': 0, 'separator': True})
            sticky_qty = 0

        raw_lines = [line.strip() for line in group.splitlines() if line.strip()]
        lines = []
        for raw_line in raw_lines:
            parts = [part.strip() for part in re.split(r'\s+y\s+', raw_line, flags=re.IGNORECASE) if part.strip()]
            lines.extend(parts)

        for line in lines:
            sticky_match = re.match(
                r'^\s*(\d+)\s*(?:(?:vil(?:es)?|bil(?:es)?|billete(?:s)?|biles?|viles?)\s*(?:de\s+)?|de\s+)cada\s*(?:uno|una)?\s*$',
                line,
                flags=re.IGNORECASE
            )
            if sticky_match:
                sticky_qty = int(sticky_match.group(1))
                continue

            dot_matches = list(re.finditer(r'(\d+)\.{2,}(\d+)', line))
            if dot_matches:
                for match in dot_matches:
                    _manual_add_parsed_pair(results, match.group(1), match.group(2), 'dots', qty_order)
                continue

            eq_matches = list(re.finditer(r'(\d+)\s*=\s*(\d+)', line))
            if eq_matches:
                for match in eq_matches:
                    _manual_add_parsed_pair(results, match.group(2), match.group(1), 'equals', qty_order)
                continue

            vil_matches = list(re.finditer(
                r'(\d+)\s*(?:vil(?:es)?|bil(?:es)?|billete(?:s)?|biles?|viles?)\s*(?:de\s+)?[-]?\s*(\d+)',
                line,
                flags=re.IGNORECASE
            ))
            if vil_matches:
                for match in vil_matches:
                    _manual_add_parsed_pair(results, match.group(1), match.group(2), 'vil', qty_order)
                continue

            de_matches = list(re.finditer(r'(\d+)\s+de\s+(\d+)', line, flags=re.IGNORECASE))
            if de_matches:
                for match in de_matches:
                    _manual_add_parsed_pair(results, match.group(1), match.group(2), 'vil', qty_order)
                continue

            dash_matches = list(re.finditer(r'(\d+)\s*[-/]\s*(\d+)', line))
            if dash_matches:
                for match in dash_matches:
                    _manual_add_parsed_pair(results, match.group(1), match.group(2), 'dash', qty_order)
                continue

            space_pair = re.match(r'^\s*(\d+)\s+(\d+)\s*$', line)
            if space_pair:
                _manual_add_parsed_pair(results, space_pair.group(1), space_pair.group(2), 'space', qty_order)
                continue

            lone_number = re.match(r'^\s*(\d{1,4})\s*$', line)
            if lone_number:
                num = lone_number.group(1)
                if len(num) == 1:
                    num = f"0{num}"
                if len(num) == 2 and sticky_qty > 0:
                    results.append({'num': num, 'qty': sticky_qty})
                    continue
                if len(num) == 4:
                    results.append({'num': num, 'qty': sticky_qty if sticky_qty > 0 else 1})
                    continue

            tokens = re.findall(r'\d+', line)
            if len(tokens) >= 2 and len(tokens) % 2 == 0:
                for token_index in range(0, len(tokens), 2):
                    _manual_add_parsed_pair(results, tokens[token_index], tokens[token_index + 1], 'inline', qty_order)

    return results


def _get_lista_available_lotteries(date_str):
    standard_lotteries = [
        {'lottery_type': "La Primera 11:00 am", 'minutes': 11 * 60},
        {'lottery_type': "Nica 1:00 pm", 'minutes': 13 * 60},
        {'lottery_type': "Tica 1:55 pm", 'minutes': 13 * 60 + 55},
        {'lottery_type': "Nica 4:00 pm", 'minutes': 16 * 60},
        {'lottery_type': "Tica 5:30 pm", 'minutes': 17 * 60 + 30},
        {'lottery_type': "La Primera 6:00 pm", 'minutes': 18 * 60},
        {'lottery_type': "Nica 7:00 pm", 'minutes': 19 * 60},
        {'lottery_type': "Tica 8:30 pm", 'minutes': 20 * 60 + 30},
        {'lottery_type': "Nica 10:00 pm", 'minutes': 22 * 60},
    ]
    nacional_lottery = {'lottery_type': "Nacional 3:00 pm", 'minutes': 15 * 60}
    active_nacional_dates = {item for item in get_nacional_dates_string().split(',') if item}
    today = get_today_panama()

    if date_str == today:
        now_panama = datetime.datetime.now(PANAMA_TZ)
        current_minutes = (now_panama.hour * 60) + now_panama.minute
        available = list(standard_lotteries)
        if date_str in active_nacional_dates:
            available.insert(3, nacional_lottery)
        filtered = []
        for lot in available:
            if lot['lottery_type'].startswith("Nacional"):
                if current_minutes < 901:
                    filtered.append(lot)
            elif current_minutes < lot['minutes']:
                filtered.append(lot)
        return filtered

    available = [lot for lot in standard_lotteries if lot['lottery_type'] in ("La Primera 11:00 am", "Nica 1:00 pm")]
    if date_str in active_nacional_dates:
        available = [nacional_lottery] + available
    return available


def _pick_preferred_lista_lottery(available_lotteries):
    if not available_lotteries:
        return None
    for lot in available_lotteries:
        if str(lot.get('lottery_type') or '').startswith("Nacional"):
            return lot
    return available_lotteries[0]


def get_default_lista_ticket_context():
    today = get_today_panama()
    available_today = _get_lista_available_lotteries(today)
    if available_today:
        preferred = _pick_preferred_lista_lottery(available_today)
        return today, preferred['lottery_type']

    today_date = datetime.datetime.strptime(today, "%Y-%m-%d").date()
    tomorrow = (today_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    available_tomorrow = _get_lista_available_lotteries(tomorrow)
    if available_tomorrow:
        preferred = _pick_preferred_lista_lottery(available_tomorrow)
        return tomorrow, preferred['lottery_type']

    return None, None


def create_ticket_draft(user_id, source, items, ticket_date=None, lottery_type=None, raw_text=None,
                        client_total=None, server_total=0.0, request_id=None, status='PREVIEW',
                        expires_minutes=None):
    items_json = json.dumps(items, ensure_ascii=False)
    created_at = utcnow_text()
    expires_at = utc_minutes_from_now_text(expires_minutes) if expires_minutes is not None else None

    conn = get_db_connection(timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        '''INSERT INTO ticket_drafts_v1
           (user_id, source, raw_text, items_json, ticket_date, lottery_type, client_total,
            server_total, request_id, status, created_at, updated_at, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (
            user_id,
            source,
            raw_text,
            items_json,
            ticket_date,
            lottery_type,
            client_total,
            server_total,
            request_id,
            status,
            created_at,
            created_at,
            expires_at
        )
    )
    draft_id = c.lastrowid
    conn.commit()
    conn.close()
    return draft_id


def update_ticket_draft(draft_id, **fields):
    if not fields:
        return False

    fields = dict(fields)
    if 'items' in fields:
        fields['items_json'] = json.dumps(fields.pop('items'), ensure_ascii=False)
    fields['updated_at'] = utcnow_text()

    allowed_columns = {
        'source', 'raw_text', 'items_json', 'ticket_date', 'lottery_type', 'client_total',
        'server_total', 'request_id', 'status', 'updated_at', 'expires_at'
    }
    update_fields = [(key, value) for key, value in fields.items() if key in allowed_columns]
    if not update_fields:
        return False

    assignments = ", ".join(f"{key} = ?" for key, _ in update_fields)
    params = [value for _, value in update_fields] + [draft_id]

    conn = get_db_connection(timeout=30.0)
    c = conn.cursor()
    c.execute(f"UPDATE ticket_drafts_v1 SET {assignments} WHERE id = ?", params)
    updated = c.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def get_ticket_draft(draft_id):
    conn = get_db_connection(timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_drafts_v1 WHERE id = ?", (draft_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def get_ticket_draft_by_request_id(request_id):
    if not request_id:
        return None
    conn = get_db_connection(timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM ticket_drafts_v1 WHERE request_id = ? ORDER BY id DESC LIMIT 1", (request_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def expire_ticket_draft_if_needed(draft):
    if not draft:
        return draft
    expires_at = draft.get('expires_at')
    if draft.get('status') == 'PREVIEW' and expires_at and expires_at < utcnow_text():
        update_ticket_draft(draft['id'], status='EXPIRED')
        draft = dict(draft)
        draft['status'] = 'EXPIRED'
    return draft


def create_ticket_record(user_id, ticket_date, lottery_type, items, source=None, request_id=None, conn=None):
    normalized_items = validate_normalized_items(items)
    own_conn = conn is None
    conn = conn or get_db_connection(timeout=30.0)
    c = conn.cursor()
    c.execute(
        '''INSERT INTO tickets_v3
           (user_id, date, lottery_type, numbers_json, is_nacional, cost, status, amount_paid, source, request_id)
           VALUES (?, ?, ?, ?, ?, 0, 'PENDING', 0, ?, ?)''',
        (
            user_id,
            ticket_date,
            lottery_type,
            json.dumps(normalized_items, ensure_ascii=False),
            1 if "Nacional" in str(lottery_type) else 0,
            source,
            request_id
        )
    )
    ticket_id = c.lastrowid
    if own_conn:
        conn.commit()
        conn.close()
    return ticket_id, normalized_items


def apply_wallet_or_admin_payment(user_id, ticket_id, total_cost, conn=None, source=None, request_id=None):
    own_conn = conn is None
    conn = conn or get_db_connection(timeout=30.0)
    c = conn.cursor()
    try:
        if is_admin_user(user_id):
            pay_amount = total_cost
            status = 'PAID'
            wallet_balance = None
        else:
            c.execute("INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 0)", (user_id,))
            c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
            wallet_row = c.fetchone()
            wallet_balance = float(wallet_row[0] if wallet_row else 0.0)
            pay_amount = min(wallet_balance, total_cost)
            status = 'PAID' if pay_amount >= total_cost - 0.01 else 'PENDING'
            if pay_amount > 0:
                c.execute("UPDATE user_wallets SET balance = balance - ? WHERE user_id = ?", (pay_amount, user_id))
                wallet_balance -= pay_amount

        set_parts = ["cost = ?", "amount_paid = ?", "status = ?"]
        params = [total_cost, pay_amount, status]
        if source is not None:
            set_parts.append("source = ?")
            params.append(source)
        if request_id is not None:
            set_parts.append("request_id = ?")
            params.append(request_id)
        params.append(ticket_id)
        c.execute(f"UPDATE tickets_v3 SET {', '.join(set_parts)} WHERE id = ?", params)

        if own_conn:
            conn.commit()

        return {
            'pay_amount': pay_amount,
            'status': status,
            'wallet_balance': wallet_balance,
            'total_cost': total_cost
        }
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()


def send_ticket_payment_notice(user_id, ticket_id, payment_result, is_edit=False, old_cost=None):
    total_cost = payment_result.get('total_cost', 0.0)
    pay_amount = payment_result.get('pay_amount', 0.0)
    status = payment_result.get('status', 'PENDING')

    if is_admin_user(user_id):
        msg = f"✏️ Ticket #{ticket_id} editado (Admin)" if is_edit else f"✅ Ticket #{ticket_id} (Admin)"
    else:
        new_bal = get_wallet_balance(user_id)
        if is_edit:
            previous_cost = float(old_cost or 0.0)
            if status == 'PAID':
                msg = f"✏️ Ticket #{ticket_id} editado\nAnterior: ${previous_cost:.2f} → Nuevo: ${total_cost:.2f}\n💰 Fondo: ${new_bal:.2f}"
            else:
                diff = total_cost - pay_amount
                msg = f"✏️ Ticket #{ticket_id} editado\nCosto: ${total_cost:.2f} — Faltaría: ${diff:.2f}\n💰 Fondo: ${new_bal:.2f}"
        else:
            if status == 'PAID':
                initial_bal = new_bal + pay_amount
                msg = f"Ticket #{ticket_id}\n${initial_bal:.2f} - ${total_cost:.2f} = ${new_bal:.2f} fondo"
            else:
                diff = total_cost - pay_amount
                msg = f"Ticket #{ticket_id}\n${total_cost:.2f} - ${pay_amount:.2f} = ${diff:.2f} faltaría"
            msg += f"\n💰 Fondo: ${new_bal:.2f}"

    try:
        sent_notif = bot.send_message(user_id, msg, parse_mode="Markdown")
        mirror_to_topic(user_id, sent_notif)
    except Exception as e:
        print(f"Error sending wallet notif: {e}")


def generate_and_send_ticket(message, ticket_id, ticket_date, lottery_type, items, payment_result, is_edit=False, old_cost=None):
    generate_ticket_image(message, ticket_id, ticket_date, lottery_type, items)
    send_ticket_payment_notice(message.chat.id, ticket_id, payment_result, is_edit=is_edit, old_cost=old_cost)

def process_wallet_deposit(user_id, amount_deposited):
    """
    Main Logic — all wallet + ticket operations in a SINGLE connection/transaction
    to prevent phantom money on crash:
    1. Add funds to Wallet.
    2. Check for OLDEST 'PENDING' tickets.
    3. Pay them off one by one until funds run out.
    4. Debit wallet in same transaction.
    5. Return summary of actions.
    """
    summary = []
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    conn = sqlite3.connect(db_path, timeout=30.0)
    c = conn.cursor()

    try:
        # 1. Credit wallet (atomic with ticket payments below)
        c.execute("INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 0)", (user_id,))
        c.execute("UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?", (amount_deposited, user_id))

        # 2. Read current balance and pending tickets
        c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
        remaining_wallet = c.fetchone()[0]

        c.execute("SELECT id, cost, amount_paid FROM tickets_v3 WHERE user_id = ? AND status = 'PENDING' ORDER BY id ASC", (user_id,))
        pending_tickets = c.fetchall()

        tickets_paid = []
        total_deducted = 0.0

        for t_id, cost, paid_so_far in pending_tickets:
            if remaining_wallet <= 0.00:
                break

            amount_needed = cost - paid_so_far
            if amount_needed <= 0:
                c.execute("UPDATE tickets_v3 SET status = 'PAID' WHERE id = ?", (t_id,))
                continue

            payment = min(remaining_wallet, amount_needed)
            new_paid = paid_so_far + payment
            new_status = 'PENDING'

            if new_paid >= cost - 0.01:
                new_status = 'PAID'
                summary.append(f"✅ **Ticket #{t_id} PAGADO**")
                tickets_paid.append(t_id)
            else:
                summary.append(f"🔋 Ticket #{t_id}: Abonado ${payment:.2f} (Resta: ${(cost - new_paid):.2f})")

            c.execute("UPDATE tickets_v3 SET amount_paid = ?, status = ? WHERE id = ?", (new_paid, new_status, t_id))
            remaining_wallet -= payment
            total_deducted += payment

        # 3. Debit wallet in same transaction
        if total_deducted > 0:
            c.execute("UPDATE user_wallets SET balance = balance - ? WHERE user_id = ?", (total_deducted, user_id))

        # 4. Read final balance
        c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
        final_bal = c.fetchone()[0]

        # COMMIT everything atomically
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    # Final Balance
    if final_bal > 0:
        summary.append(f"💰 **Fondo Disponible: ${final_bal:.2f}**")
    elif not tickets_paid and not summary:
        summary.append(f"💰 Fondo: $0.00")

    return "\n".join(summary)

# 🆕 SEPARATE DATABASE for Yappy payments (can be manually deleted)
YAPPY_DB_PATH = os.path.join(BASE_DIR, 'yappy_cache.db')

def init_yappy_db():
    """Initialize Yappy cache database - auto-creates if deleted"""
    conn = sqlite3.connect(YAPPY_DB_PATH, timeout=30.0)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS yappy_payments (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 date TEXT,
                 message_time TEXT,
                 amount REAL,
                 sender_name TEXT,
                 confirmation_letters TEXT,
                 phone_number TEXT,
                 account_tag TEXT,
                 tg_message_id INTEGER,
                 tg_group_id INTEGER,
                 source_bot TEXT,
                 source_event_id TEXT,
                 received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 verified INTEGER DEFAULT 0,
                 UNIQUE(date, message_time, amount, confirmation_letters)
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_yappy_search 
                 ON yappy_payments(date, amount, confirmation_letters)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_yappy_confirmation_recent
                 ON yappy_payments(confirmation_letters, verified, received_at DESC)''')
    
    # New table for pending verifications
    c.execute('''CREATE TABLE IF NOT EXISTS pending_verifications (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id INTEGER,
                 chat_id INTEGER,
                 message_id INTEGER,
                 confirmation_letters TEXT,
                 amount REAL,
                 receipt_time TEXT,
                 followup_id INTEGER,
                 timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_pending_conf 
                 ON pending_verifications(confirmation_letters)''')
    c.execute('''CREATE TABLE IF NOT EXISTS receipt_followups (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id INTEGER NOT NULL,
                 chat_id INTEGER NOT NULL,
                 receipt_message_id INTEGER NOT NULL,
                 action_message_id INTEGER,
                 scenario TEXT NOT NULL,
                 status TEXT DEFAULT 'OPEN',
                 confirmation_letters TEXT,
                 confirmation_full TEXT,
                 amount REAL,
                 receipt_time TEXT,
                 pending_verification_id INTEGER,
                 image_hash TEXT,
                 manual_amount REAL,
                 manual_confirmation TEXT,
                 manual_confirmation_full TEXT,
                 manual_receipt_time TEXT,
                 remind_at TEXT,
                 reminder_sent INTEGER DEFAULT 0,
                 handled_at TEXT,
                 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_receipt_followups_status
                 ON receipt_followups(status, remind_at)''')
    try:
        c.execute("ALTER TABLE receipt_followups ADD COLUMN manual_attempts INTEGER DEFAULT 0")
    except Exception:
        pass
    c.execute('''CREATE INDEX IF NOT EXISTS idx_receipt_followups_active_user
                 ON receipt_followups(user_id, status, id DESC)''')
    c.execute('''CREATE TABLE IF NOT EXISTS receipt_images (
                 image_hash TEXT PRIMARY KEY,
                 first_seen_at TEXT,
                 first_user_id INTEGER,
                 first_user_name TEXT,
                 receipt_kind TEXT,
                 first_chat_id INTEGER,
                 first_message_id INTEGER
    )''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_receipt_images_seen
                 ON receipt_images(first_seen_at)''')

    # Migration: Add message_id if it doesn't exist
    try:
        c.execute("ALTER TABLE pending_verifications ADD COLUMN message_id INTEGER")
    except:
        pass 
        
    # Migration: Add receipt_time if it doesn't exist
    try:
        c.execute("ALTER TABLE pending_verifications ADD COLUMN receipt_time TEXT")
    except:
        pass
    try:
        c.execute("ALTER TABLE pending_verifications ADD COLUMN followup_id INTEGER")
    except:
        pass
    try:
        c.execute("ALTER TABLE pending_verifications ADD COLUMN reply_message_id INTEGER")
    except:
        pass

    # Migration: Add first_user_name if it doesn't exist
    try:
        c.execute("ALTER TABLE receipt_images ADD COLUMN first_user_name TEXT")
    except:
        pass
    try:
        c.execute("ALTER TABLE receipt_images ADD COLUMN receipt_kind TEXT")
    except:
        pass
    try:
        c.execute("ALTER TABLE receipt_images ADD COLUMN confirmation_letters TEXT")
    except:
        pass

    # Migration: Add source provenance columns if missing
    try:
        c.execute("ALTER TABLE yappy_payments ADD COLUMN source_bot TEXT")
    except:
        pass
    try:
        c.execute("ALTER TABLE yappy_payments ADD COLUMN source_event_id TEXT")
    except:
        pass
                  
    conn.commit()
    conn.close()

def get_yappy_db():
    """Get Yappy DB connection - auto-creates if file doesn't exist"""
    if not os.path.exists(YAPPY_DB_PATH):
        init_yappy_db()
    # Increase timeout to 30s to handle concurrent writes better (e.g. 100 images at once)
    return sqlite3.connect(YAPPY_DB_PATH, timeout=30.0)

def get_bridge_db():
    """Get bridge DB connection used to ingest payment events from producer bots."""
    conn = sqlite3.connect(YAPPY_BRIDGE_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
    except Exception as e:
        logging.warning("Bridge DB PRAGMA WAL/synchronous failed: %s", e)
    try:
        conn.execute(f"PRAGMA busy_timeout = {YAPPY_BRIDGE_BUSY_TIMEOUT_MS}")
    except Exception as e:
        logging.warning("Bridge DB PRAGMA busy_timeout failed: %s", e)
    return conn

def init_bridge_db():
    """Initialize shared bridge DB schema."""
    try:
        conn = get_bridge_db()
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS bridge_payments (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     schema_version INTEGER NOT NULL DEFAULT 1,
                     source_bot TEXT NOT NULL,
                     event_id TEXT NOT NULL,
                     date TEXT,
                     message_time TEXT,
                     amount REAL NOT NULL,
                     sender_name TEXT,
                     confirmation_letters TEXT NOT NULL,
                     phone_number TEXT,
                     account_tag TEXT,
                     tg_message_id INTEGER,
                     tg_group_id INTEGER,
                     payload_json TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     UNIQUE(source_bot, event_id)
        )''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_bridge_payments_created
                     ON bridge_payments(created_at)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_bridge_payments_confirm
                     ON bridge_payments(confirmation_letters, amount)''')

        c.execute('''CREATE TABLE IF NOT EXISTS bridge_consumed_events (
                     consumer_key TEXT NOT NULL,
                     source_bot TEXT NOT NULL,
                     event_id TEXT NOT NULL,
                     status TEXT NOT NULL DEFAULT 'SUCCESS',
                     error_text TEXT,
                     processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     PRIMARY KEY(consumer_key, source_bot, event_id)
        )''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_bridge_consumed_status
                     ON bridge_consumed_events(consumer_key, status, processed_at)''')

        c.execute('''CREATE TABLE IF NOT EXISTS bridge_dead_letters (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     consumer_key TEXT NOT NULL,
                     source_bot TEXT,
                     event_id TEXT,
                     error_text TEXT,
                     raw_payload TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        # NOTE: bridge_reaction_requests table removed — lot bot now reacts directly via Telegram API

        conn.commit()
        conn.close()
        print(f"Bridge DB ready: {YAPPY_BRIDGE_DB_PATH}")
    except Exception as e:
        print(f"Bridge DB init error: {e}")

def _bridge_error_looks_malformed(err) -> bool:
    text = str(err).lower()
    return (
        'database disk image is malformed' in text
        or 'malformed' in text
        or 'database corruption' in text
        or 'corrupt' in text
    )

def _attempt_bridge_db_recovery(reason: str) -> bool:
    global _LAST_BRIDGE_RECOVERY_TS

    if not YAPPY_BRIDGE_DB_PATH:
        return False

    now_ts = int(time.time())
    with _BRIDGE_RECOVERY_LOCK:
        if (now_ts - _LAST_BRIDGE_RECOVERY_TS) < YAPPY_BRIDGE_RECOVERY_COOLDOWN_SECONDS:
            return False
        _LAST_BRIDGE_RECOVERY_TS = now_ts

    db_path = YAPPY_BRIDGE_DB_PATH
    backup_path = f"{db_path}.malformed.{now_ts}"
    recovered_path = f"{db_path}.recovered.{now_ts}"
    print(f"⚠️ Bridge DB appears corrupted ({reason}). Attempting recovery...")

    try:
        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)
    except Exception as exc:
        print(f"⚠️ Bridge DB backup before recovery failed: {exc}")

    src = None
    dst = None
    try:
        src = sqlite3.connect(db_path, timeout=30.0)
        dst = sqlite3.connect(recovered_path, timeout=30.0)
        for line in src.iterdump():
            try:
                dst.execute(line)
            except Exception:
                pass
        dst.commit()

        check = dst.execute("PRAGMA integrity_check(1)").fetchone()
        if not (check and check[0] == "ok"):
            print("❌ Bridge DB recovery output failed integrity_check.")
            return False

        dst.close()
        src.close()
        dst = None
        src = None

        os.replace(recovered_path, db_path)
        for suffix in ("-wal", "-shm"):
            stale = db_path + suffix
            if os.path.exists(stale):
                try:
                    os.remove(stale)
                except Exception:
                    pass
        print(f"✅ Bridge DB recovery succeeded: {db_path}")
        return True
    except Exception as exc:
        print(f"❌ Bridge DB recovery failed: {exc}")
        return False
    finally:
        try:
            if src:
                src.close()
        except Exception:
            pass
        try:
            if dst:
                dst.close()
        except Exception:
            pass
        try:
            if os.path.exists(recovered_path):
                os.remove(recovered_path)
        except Exception:
            pass

def _bridge_maintenance_once():
    conn = None
    try:
        conn = get_bridge_db()
        retention_expr = f"-{int(YAPPY_BRIDGE_RETENTION_DAYS)} days"
        conn.execute(
            "DELETE FROM bridge_consumed_events WHERE processed_at < datetime('now', ?)",
            (retention_expr,),
        )
        conn.execute(
            "DELETE FROM bridge_dead_letters WHERE created_at < datetime('now', ?)",
            (retention_expr,),
        )
        conn.commit()
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
    except Exception as exc:
        print(f"⚠️ Bridge maintenance failed: {exc}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def _normalize_bridge_time(value):
    raw = str(value or '').strip()
    m = re.search(r'(\d{1,2}):(\d{2})', raw)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return f"{hh}:{mm:02d}"

def _normalize_bridge_date(value):
    raw = str(value or '').strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', raw):
        return raw
    return get_today_panama()

def _extract_bridge_confirmation(value):
    raw_text = str(value or '').strip().upper()
    if not raw_text:
        return None
    m = re.search(r'#?\s*([A-Z]{5})(?:\b|[^A-Z])', raw_text)
    if m:
        return m.group(1)
    only_letters = re.sub(r'[^A-Z]', '', raw_text)
    if len(only_letters) >= 5:
        return only_letters[:5]
    return None

def _bridge_mark_consumed(conn, source_bot, event_id, status='SUCCESS', error_text=None, _commit=True):
    conn.execute(
        '''INSERT OR REPLACE INTO bridge_consumed_events
           (consumer_key, source_bot, event_id, status, error_text, processed_at)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
        (YAPPY_BRIDGE_CONSUMER_KEY, str(source_bot), str(event_id), str(status), (str(error_text)[:500] if error_text else None))
    )
    if _commit:
        conn.commit()

def _bridge_add_dead_letter(conn, source_bot, event_id, error_text, raw_payload):
    conn.execute(
        '''INSERT INTO bridge_dead_letters
           (consumer_key, source_bot, event_id, error_text, raw_payload)
           VALUES (?, ?, ?, ?, ?)''',
        (
            YAPPY_BRIDGE_CONSUMER_KEY,
            (str(source_bot) if source_bot is not None else None),
            (str(event_id) if event_id is not None else None),
            (str(error_text)[:1000] if error_text else None),
            (str(raw_payload)[:10000] if raw_payload else None)
        )
    )
    conn.commit()

def _bridge_row_to_payment(row):
    source_bot = str(row['source_bot'] or '').strip()
    event_id = str(row['event_id'] or '').strip()
    if not source_bot or not event_id:
        return None, "missing source_bot/event_id"

    msg_time = _normalize_bridge_time(row['message_time'])
    if not msg_time:
        return None, f"invalid message_time: {row['message_time']}"

    try:
        amount = float(row['amount'])
    except Exception:
        return None, f"invalid amount: {row['amount']}"
    if amount <= 0:
        return None, f"non-positive amount: {amount}"

    confirmation = _extract_bridge_confirmation(row['confirmation_letters'])
    if not confirmation:
        return None, f"invalid confirmation_letters: {row['confirmation_letters']}"

    sender_name = re.sub(r'\s+', ' ', str(row['sender_name'] or '')).strip() or "Desconocido"
    phone_number = re.sub(r'\D', '', str(row['phone_number'] or ''))

    account_tag = str(row['account_tag'] or '').strip().upper()
    if account_tag and not account_tag.startswith('#'):
        account_tag = f"#{account_tag}"
    if not account_tag:
        account_tag = "#BR"

    parsed_data = {
        'time': msg_time,
        'amount': amount,
        'sender_name': sender_name,
        'confirmation': confirmation,
        'phone': phone_number,
        'account_tag': account_tag
    }
    return {
        'source_bot': source_bot,
        'event_id': event_id,
        'payment_date': _normalize_bridge_date(row['date']),
        'tg_message_id': row['tg_message_id'],
        'tg_group_id': row['tg_group_id'],
        'parsed_data': parsed_data
    }, None

def bridge_ingest_once():
    """Pull new bridge events and ingest into yappy_payments."""
    conn = None
    consumed = 0
    try:
        conn = get_bridge_db()
        rows = conn.execute(
            '''SELECT p.id, p.source_bot, p.event_id, p.date, p.message_time, p.amount,
                      p.sender_name, p.confirmation_letters, p.phone_number, p.account_tag,
                      p.tg_message_id, p.tg_group_id, p.payload_json
               FROM bridge_payments p
               LEFT JOIN bridge_consumed_events c
                 ON c.consumer_key = ?
                AND c.source_bot = p.source_bot
                AND c.event_id = p.event_id
               WHERE c.event_id IS NULL
               ORDER BY p.id ASC
               LIMIT ?''',
            (YAPPY_BRIDGE_CONSUMER_KEY, YAPPY_BRIDGE_BATCH_SIZE)
        ).fetchall()

        if not rows:
            return 0

        for row in rows:
            source_bot = str(row['source_bot'] or '').strip()
            event_id = str(row['event_id'] or '').strip()
            try:
                if YAPPY_BRIDGE_SOURCE_ALLOWLIST and source_bot.lower() not in YAPPY_BRIDGE_SOURCE_ALLOWLIST:
                    _bridge_mark_consumed(conn, source_bot, event_id, status='SKIPPED', error_text='source not allowlisted', _commit=False)
                    consumed += 1
                    continue

                normalized, err = _bridge_row_to_payment(row)
                if err:
                    raw_payload = row['payload_json'] if row['payload_json'] else json.dumps(dict(row), ensure_ascii=False)
                    _bridge_add_dead_letter(conn, source_bot, event_id, err, raw_payload)
                    _bridge_mark_consumed(conn, source_bot, event_id, status='INVALID', error_text=err, _commit=False)
                    consumed += 1
                    continue

                ok = store_yappy_payment(
                    normalized['parsed_data'],
                    normalized['tg_message_id'],
                    normalized['tg_group_id'],
                    payment_date=normalized['payment_date'],
                    source_bot=normalized['source_bot'],
                    source_event_id=normalized['event_id']
                )
                if not ok:
                    print(f"Bridge ingest failed for {source_bot}:{event_id}; will retry.")
                    continue

                _bridge_mark_consumed(conn, source_bot, event_id, status='SUCCESS', _commit=False)
                consumed += 1
                print(f"✅ Bridge ingested: {source_bot}:{event_id} conf={normalized['parsed_data'].get('confirmation')} amt={normalized['parsed_data'].get('amount')}")
            except Exception as row_err:
                import traceback
                print(f"Bridge row error for {source_bot}:{event_id}: {row_err}")
                traceback.print_exc()
                if _is_retryable_bridge_error(row_err):
                    raise
                err_text = f"row processing error: {row_err}"
                try:
                    raw_payload = row['payload_json'] if row['payload_json'] else json.dumps(dict(row), ensure_ascii=False)
                    _bridge_add_dead_letter(conn, source_bot, event_id, err_text, raw_payload)
                    _bridge_mark_consumed(conn, source_bot, event_id, status='INVALID', error_text=err_text, _commit=False)
                    consumed += 1
                except Exception as inner_err:
                    print(f"Bridge row hard-failure for {source_bot}:{event_id}: {row_err} (inner: {inner_err})")
                    if _is_retryable_bridge_error(inner_err):
                        raise

        # Batch commit after processing all rows (reduces WAL fragmentation)
        if consumed > 0:
            conn.commit()
        return consumed
    except Exception as e:
        print(f"Bridge ingest loop error: {e}")
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            conn = None
        if _bridge_error_looks_malformed(e):
            _attempt_bridge_db_recovery(str(e))
        return consumed
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def bridge_ingest_worker():
    """Background bridge consumer thread."""
    if not YAPPY_BRIDGE_ENABLED:
        print("Bridge consumer disabled by env.")
        return

    print("Bridge consumer started.")
    next_maintenance_ts = 0
    while True:
        now_ts = int(time.time())
        if now_ts >= next_maintenance_ts:
            _bridge_maintenance_once()
            next_maintenance_ts = now_ts + YAPPY_BRIDGE_MAINTENANCE_SECONDS
        try:
            done = bridge_ingest_once()
            if done > 0:
                time.sleep(0.25)
            else:
                time.sleep(YAPPY_BRIDGE_POLL_SECONDS)
        except Exception as e:
            print(f"Bridge worker error: {e}")
            time.sleep(YAPPY_BRIDGE_POLL_SECONDS)

def _is_retryable_bridge_error(err):
    text = str(err).lower()
    return (
        ('locked' in text)
        or ('busy' in text)
        or ('disk i/o' in text)
        or ('database disk image is malformed' in text)
        or ('malformed' in text)
        or ('database corruption' in text)
        or ('corrupt' in text)
    )

def trigger_payment_reaction(payment_id, emoji=None, user_id=None):
    """
    React directly to the payment message in the Yappy groupchat via Telegram API.
    Works for both bridge-sourced and local payments — no bridge DB writes needed.
    If user_id is provided, uses per-user emoji from ADMIN_RECEIPT_EMOJIS config.
    """
    if emoji:
        emoji_value = emoji.strip() or '👍'
    elif user_id and user_id in ADMIN_RECEIPT_EMOJIS:
        emoji_value = ADMIN_RECEIPT_EMOJIS[user_id]
    elif ADMIN_RECEIPT_EMOJIS or DEFAULT_RECEIPT_EMOJI:
        emoji_value = DEFAULT_RECEIPT_EMOJI or YAPPY_BRIDGE_REACTION_EMOJI or '👍'
    else:
        emoji_value = (YAPPY_BRIDGE_REACTION_EMOJI or '👍').strip() or '👍'
    result = react_to_payment_message(payment_id, emoji=emoji_value)
    if result:
        print(f"✅ Direct reaction sent for payment {payment_id}")
    else:
        print(f"⚠️ Direct reaction failed for payment {payment_id}")
    return result

def sanitize_wallet_summary_for_ocr(summary_text):
    """Remove non-essential emojis from wallet summary in OCR replies."""
    if not summary_text:
        return ""
    cleaned = str(summary_text)
    for token in ['✅', '💰', '🔋', '💵', '🔐', '👤', '🏦', '🕐']:
        cleaned = cleaned.replace(token, '')
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    return cleaned.strip()

def register_receipt_image_hash(image_hash, user_id, chat_id, message_id, user_name=None):
    """
    Register an OCR receipt image hash.
    Returns: (is_duplicate, first_seen_at_str, inserted_now, first_user_name, receipt_kind, confirmation_letters)
    """
    if not image_hash:
        return False, None, False, None, None, None
    try:
        now_panama = datetime.datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        safe_user_name = format_topic_user_name(user_id, user_name=user_name)
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute(
            '''INSERT OR IGNORE INTO receipt_images
               (image_hash, first_seen_at, first_user_id, first_user_name, first_chat_id, first_message_id)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (image_hash, now_panama, user_id, safe_user_name, chat_id, message_id)
        )
        inserted = c.rowcount == 1
        conn.commit()
        if inserted:
            conn.close()
            return False, now_panama, True, safe_user_name, None, None

        c.execute(
            "SELECT first_seen_at, first_user_id, first_user_name, receipt_kind, confirmation_letters FROM receipt_images WHERE image_hash = ?",
            (image_hash,)
        )
        row = c.fetchone()
        conn.close()
        stored_user_id = row[1] if row and len(row) > 1 else None
        stored_user_name = row[2] if row and len(row) > 2 else None
        stored_receipt_kind = row[3] if row and len(row) > 3 else None
        stored_confirmation = row[4] if row and len(row) > 4 else None
        resolved_user_name = (
            (stored_user_name.strip() if isinstance(stored_user_name, str) and stored_user_name.strip() else None)
            or lookup_support_user_name(stored_user_id)
            or (format_topic_user_name(stored_user_id) if stored_user_id else None)
        )
        return True, (row[0] if row and row[0] else now_panama), False, resolved_user_name, stored_receipt_kind, stored_confirmation
    except Exception as e:
        print(f"⚠️ Receipt hash registration failed: {e}")
        return False, None, False, None, None, None


def set_receipt_image_kind(image_hash, receipt_kind):
    """Persist the resolved classification for a cached receipt image."""
    if not image_hash or not receipt_kind:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute(
            "UPDATE receipt_images SET receipt_kind = COALESCE(receipt_kind, ?) WHERE image_hash = ?",
            (receipt_kind, image_hash)
        )
        updated = c.rowcount > 0
        conn.commit()
        conn.close()
        return updated
    except Exception as e:
        print(f"⚠️ Receipt kind update failed ({image_hash[:12] if image_hash else '?'}): {e}")
        return False


def set_receipt_image_confirmation(image_hash, confirmation_letters):
    """Store the confirmation letters extracted by OCR for a cached receipt image."""
    if not image_hash or not confirmation_letters:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute(
            "UPDATE receipt_images SET confirmation_letters = COALESCE(confirmation_letters, ?) WHERE image_hash = ?",
            (confirmation_letters, image_hash)
        )
        updated = c.rowcount > 0
        conn.commit()
        conn.close()
        return updated
    except Exception as e:
        print(f"⚠️ Receipt confirmation update failed: {e}")
        return False


def build_duplicate_receipt_notice(first_seen_at, first_sender_name, receipt_kind=None, confirmation_letters=None):
    """Build the duplicate-image notice shown before OCR reruns."""
    when_text = format_short_received_time(first_seen_at)
    sender_text = first_sender_name or "Nombre no disponible"
    if receipt_kind == "MONEY_REQUEST":
        lines = ["🔴 **Imagen de pedido ya recibida antes**", "No es comprobante de envio, sino de pedido."]
    else:
        lines = ["🟡 **Comprobante ya recibido antes**"]
    if confirmation_letters:
        lines.append(f"Confirmación: #{confirmation_letters}")
    lines.append(f"Primero enviado por: {sender_text}")
    lines.append(f"Recibido por primera vez: {when_text}.")
    return "\n".join(lines)


def release_receipt_image_hash(image_hash):
    """Release a temporary receipt hash reservation so the same image can be retried."""
    if not image_hash:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("DELETE FROM receipt_images WHERE image_hash = ?", (image_hash,))
        deleted = c.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    except Exception as e:
        print(f"⚠️ Receipt hash release failed: {e}")
        return False

def format_short_received_time(ts_text):
    """Format DB timestamp (UTC) as MM-DD HH:MM:SS in Panama time."""
    if not ts_text:
        return "desconocida"
    raw = str(ts_text).strip().replace('T', ' ')
    try:
        dt = datetime.datetime.fromisoformat(raw)
        # DB stores UTC – convert to Panama time for display
        dt_utc = pytz.utc.localize(dt)
        dt_panama = dt_utc.astimezone(PANAMA_TZ)
        return dt_panama.strftime("%m-%d %H:%M:%S")
    except Exception:
        m = re.match(r'^\d{4}-(\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', raw)
        if m:
            return m.group(1)
        return raw

init_db()
init_yappy_db()
if YAPPY_BRIDGE_ENABLED:
    init_bridge_db()

# --- HELPERS ---
def is_admin_chat(message):
    is_group = str(message.chat.id) == str(ADMIN_GROUP_ID)
    is_admin_user = str(message.from_user.id) == str(ADMIN_USER_ID)
    return is_group or is_admin_user


def format_topic_user_name(user_id, user_name=None, first_name=None, last_name=None):
    """Build the same human-readable name used for admin support topics."""
    raw_name = str(user_name).strip() if user_name else ""
    if not raw_name:
        parts = []
        if first_name:
            parts.append(str(first_name).strip())
        if last_name:
            parts.append(str(last_name).strip())
        raw_name = " ".join(part for part in parts if part).strip()
    if not raw_name:
        raw_name = f"User {user_id}"
    return re.sub(r'\s+', ' ', raw_name)[:60].strip()


def lookup_support_user_name(user_id):
    """Fetch the stored admin-topic display name for a user, if available."""
    if not user_id:
        return None
    try:
        with MIRROR_CACHE_LOCK:
            cached = SUPPORT_THREAD_CACHE.get(int(user_id))
        if cached:
            return cached[1]

        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT thread_id, user_name FROM support_threads WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        if row and row[1]:
            with MIRROR_CACHE_LOCK:
                SUPPORT_THREAD_CACHE[int(user_id)] = (row[0], row[1].strip())
        return (row[1].strip() if row and row[1] else None)
    except Exception as e:
        print(f"⚠️ Support name lookup failed: {e}")
        return None

# --- YAPPY PAYMENT VERIFICATION HELPERS ---

# Yappy account mapping
YAPPY_ACCOUNTS = {
    "#JV": "Javier Chen",
    "#LI": "Li Chen",
    "#YP": "Yuhuan Pan"
}

def utcnow_text():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def utc_minutes_from_now_text(minutes):
    return (datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def format_confirmation_display(confirmation=None, confirmation_full=None):
    raw = str(confirmation or "").strip().upper()
    if raw:
        return raw
    raw_full = str(confirmation_full or "").strip().upper()
    if raw_full.startswith("#"):
        raw_full = raw_full[1:]
    match = re.match(r'([A-Z]{5})', raw_full)
    if match:
        return match.group(1)
    return raw_full or "?"


def build_receipt_manual_webapp_url(followup_id, amount=None, confirmation=None, confirmation_full=None, receipt_time=None):
    params = {
        "v": BOT_VERSION,
        "mode": "receipt_manual",
        "followup_id": str(followup_id)
    }
    if amount is not None:
        try:
            params["amount"] = f"{float(amount):.2f}"
        except Exception:
            params["amount"] = str(amount)
    confirmation_display = format_confirmation_display(confirmation=confirmation, confirmation_full=confirmation_full)
    if confirmation_display and confirmation_display != "?":
        params["confirmation"] = confirmation_display
    if receipt_time:
        params["receipt_time"] = str(receipt_time)
    return f"{WEBAPP_BASE_URL}index.html?{urllib.parse.urlencode(params)}"


def get_receipt_followup_markup(followup_id, amount=None, confirmation=None, confirmation_full=None, receipt_time=None):
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("Corregir confirmación", callback_data=f"receipt_manual_{followup_id}"),
        InlineKeyboardButton("No es comprobante", callback_data=f"receipt_ignore_{followup_id}")
    )
    return markup


def get_receipt_manual_launcher_markup(user_id, followup_id, amount=None, confirmation=None, confirmation_full=None, receipt_time=None):
    web_app_url = build_receipt_manual_webapp_url(
        followup_id,
        amount=amount,
        confirmation=confirmation,
        confirmation_full=confirmation_full,
        receipt_time=receipt_time
    )
    markup = get_menu_markup(user_id)
    if markup is None:
        markup = ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row(
        KeyboardButton("📝 Abrir formulario", web_app=WebAppInfo(url=web_app_url))
    )
    return markup


def get_receipt_processed_markup():
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(RECEIPT_PROCESSED_LABEL, callback_data="noop"))
    return markup


def update_receipt_followup(followup_id, **fields):
    if not followup_id or not fields:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        assignments = []
        values = []
        for key, value in fields.items():
            assignments.append(f"{key} = ?")
            values.append(value)
        values.append(followup_id)
        c.execute(f"UPDATE receipt_followups SET {', '.join(assignments)} WHERE id = ?", values)
        updated = c.rowcount > 0
        conn.commit()
        conn.close()
        return updated
    except Exception as e:
        print(f"⚠️ Receipt followup update failed ({followup_id}): {e}")
        return False


def get_receipt_followup(followup_id):
    if not followup_id:
        return None
    try:
        conn = get_yappy_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM receipt_followups WHERE id = ?", (followup_id,))
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"⚠️ Receipt followup lookup failed ({followup_id}): {e}")
        return None


def get_active_manual_receipt_followup(user_id):
    try:
        conn = get_yappy_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            '''SELECT * FROM receipt_followups
               WHERE user_id = ?
                 AND status IN ('MANUAL_WAIT_AMOUNT', 'MANUAL_WAIT_CONFIRMATION', 'MANUAL_WAIT_TIME')
                 AND created_at > datetime('now', '-24 hours')
               ORDER BY id DESC LIMIT 1''',
            (user_id,)
        )
        row = c.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"⚠️ Active manual receipt lookup failed ({user_id}): {e}")
        return None


def create_receipt_followup(
    user_id,
    chat_id,
    receipt_message_id,
    scenario,
    action_message_id=None,
    confirmation=None,
    confirmation_full=None,
    amount=None,
    receipt_time=None,
    pending_verification_id=None,
    image_hash=None
):
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute(
            '''INSERT INTO receipt_followups
               (user_id, chat_id, receipt_message_id, action_message_id, scenario, status,
                confirmation_letters, confirmation_full, amount, receipt_time,
                pending_verification_id, image_hash, remind_at, reminder_sent)
               VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, 0)''',
            (
                user_id,
                chat_id,
                receipt_message_id,
                action_message_id,
                scenario,
                confirmation,
                confirmation_full,
                amount,
                receipt_time,
                pending_verification_id,
                image_hash,
                utc_minutes_from_now_text(RECEIPT_REMINDER_MINUTES)
            )
        )
        followup_id = c.lastrowid
        conn.commit()
        conn.close()
        return followup_id
    except Exception as e:
        print(f"❌ Error creating receipt followup: {e}")
        return None


def delete_receipt_followup(followup_id):
    if not followup_id:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("DELETE FROM receipt_followups WHERE id = ?", (followup_id,))
        deleted = c.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    except Exception as e:
        print(f"⚠️ Receipt followup delete failed ({followup_id}): {e}")
        return False


def set_pending_followup_id(pending_id, followup_id):
    if not pending_id:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("UPDATE pending_verifications SET followup_id = ? WHERE id = ?", (followup_id, pending_id))
        updated = c.rowcount > 0
        conn.commit()
        conn.close()
        return updated
    except Exception as e:
        print(f"⚠️ Pending followup link failed ({pending_id} -> {followup_id}): {e}")
        return False


def delete_pending_verification(pending_id):
    if not pending_id:
        return False
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("DELETE FROM pending_verifications WHERE id = ?", (pending_id,))
        deleted = c.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    except Exception as e:
        print(f"⚠️ Pending delete failed ({pending_id}): {e}")
        return False


def disable_receipt_followup_buttons(followup):
    if not followup or not followup.get('action_message_id'):
        return False
    try:
        bot.edit_message_reply_markup(
            followup['chat_id'],
            followup['action_message_id'],
            reply_markup=get_receipt_processed_markup()
        )
        return True
    except Exception as e:
        print(f"⚠️ Receipt followup button update failed ({followup.get('id')}): {e}")
        return False


def edit_receipt_followup_message(followup, text, parse_mode=None, reply_markup=None):
    if not followup or not followup.get('action_message_id') or text is None:
        return False
    try:
        bot.edit_message_text(
            text + "\n_(Editado por el bot)_",
            followup['chat_id'],
            followup['action_message_id'],
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
        return True
    except Exception as e:
        print(f"⚠️ Receipt followup text edit failed ({followup.get('id')}): {e}")
        return False


def mirror_receipt_followup_action_message(followup):
    if not followup or not followup.get('action_message_id'):
        return
    enqueue_mirror_message(
        user_id=followup['user_id'],
        source_chat_id=followup['chat_id'],
        source_message_id=followup['action_message_id'],
        user_name=lookup_support_user_name(followup['user_id']),
        source_reply_to_id=followup.get('receipt_message_id')
    )


def refresh_open_receipt_followup_markups():
    try:
        conn = get_yappy_db()
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            '''SELECT * FROM receipt_followups
               WHERE status = 'OPEN'
                 AND action_message_id IS NOT NULL'''
        )
        open_followups = [dict(row) for row in c.fetchall()]
        conn.close()
    except Exception as e:
        print(f"⚠️ Receipt followup refresh lookup failed: {e}")
        return

    refreshed = 0
    for followup in open_followups:
        try:
            bot.edit_message_reply_markup(
                followup['chat_id'],
                followup['action_message_id'],
                reply_markup=get_receipt_followup_markup(
                    followup['id'],
                    amount=followup.get('amount'),
                    confirmation=followup.get('confirmation_letters'),
                    confirmation_full=followup.get('confirmation_full'),
                    receipt_time=followup.get('receipt_time')
                )
            )
            refreshed += 1
        except Exception as e:
            err_str = str(e).lower()
            if "message is not modified" not in err_str:
                print(f"⚠️ Receipt followup refresh failed ({followup.get('id')}): {e}")
            if "retry after" in err_str:
                try:
                    wait = int(err_str.split("retry after")[-1].strip().split()[0]) + 1
                except Exception:
                    wait = 10
                time.sleep(wait)
        time.sleep(0.15)

    if refreshed:
        print(f"🔁 Refreshed {refreshed} open receipt followup markup(s)")


def complete_receipt_followup(followup_id, status="PROCESSED", release_hash=False, update_markup=True):
    followup = get_receipt_followup(followup_id)
    if not followup:
        return False
    update_receipt_followup(
        followup_id,
        status=status,
        reminder_sent=1,
        handled_at=utcnow_text()
    )
    if update_markup:
        disable_receipt_followup_buttons(followup)
    if release_hash and followup.get('image_hash'):
        release_receipt_image_hash(followup['image_hash'])
    return True


def send_receipt_followup_reply(
    message,
    text,
    scenario,
    parse_mode=None,
    confirmation=None,
    confirmation_full=None,
    amount=None,
    receipt_time=None,
    pending_verification_id=None,
    image_hash=None
):
    followup_id = create_receipt_followup(
        user_id=message.from_user.id,
        chat_id=message.chat.id,
        receipt_message_id=message.message_id,
        scenario=scenario,
        confirmation=confirmation,
        confirmation_full=confirmation_full,
        amount=amount,
        receipt_time=receipt_time,
        pending_verification_id=pending_verification_id,
        image_hash=image_hash
    )
    if not followup_id:
        bot.reply_to(message, text, parse_mode=parse_mode)
        return None

    try:
        sent_msg = bot.reply_to(
            message,
            text,
            parse_mode=parse_mode,
            reply_markup=get_receipt_followup_markup(
                followup_id,
                amount=amount,
                confirmation=confirmation,
                confirmation_full=confirmation_full,
                receipt_time=receipt_time
            )
        )
    except Exception:
        delete_receipt_followup(followup_id)
        raise

    update_receipt_followup(followup_id, action_message_id=sent_msg.message_id)
    if pending_verification_id:
        set_pending_followup_id(pending_verification_id, followup_id)
    return followup_id


def normalize_manual_amount(raw_text):
    cleaned = str(raw_text or "").strip().replace(",", ".")
    match = re.search(r'([0-9]+(?:\.[0-9]{1,2})?)', cleaned)
    if not match:
        return None
    try:
        amount = float(match.group(1))
        if amount <= 0:
            return None
        return amount
    except Exception:
        return None


def normalize_manual_confirmation(raw_text):
    cleaned = str(raw_text or "").strip().upper().replace(" ", "").replace("–", "-")
    if cleaned.startswith("#"):
        cleaned = cleaned[1:]
    match = re.fullmatch(r'([A-Z]{5})(?:-(\d+))?', cleaned)
    if not match:
        return None, None
    letters = match.group(1)
    digits = match.group(2)
    full = f"#{letters}-{digits}" if digits else f"#{letters}"
    return letters, full


def normalize_manual_receipt_time(raw_text):
    cleaned = str(raw_text or "").strip()
    if cleaned.lower() in {"saltar", "omitir", "skip", "ninguna", "ninguno", "na", "n/a"}:
        return ""
    match = re.search(r'(\d{1,2}):(\d{2})', cleaned)
    if not match:
        return None
    return f"{int(match.group(1))}:{match.group(2)}"


def send_followup_reply_to_receipt(followup, text, parse_mode=None, reply_markup=None):
    if not followup:
        return None
    try:
        return bot.send_message(
            followup['chat_id'],
            text,
            reply_to_message_id=followup['receipt_message_id'],
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
    except Exception:
        return bot.send_message(
            followup['chat_id'],
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )


def process_manual_receipt_submission(followup, amount, confirmation, confirmation_full=None, receipt_time=None):
    user_id = followup['user_id']
    payment_info = {
        'amount': amount,
        'confirmation': confirmation,
        'confirmation_full': confirmation_full or f"#{confirmation}",
        'time': receipt_time or None
    }
    confirmation_display = format_confirmation_display(
        confirmation=payment_info.get('confirmation'),
        confirmation_full=payment_info.get('confirmation_full')
    )
    match = search_yappy_payment(payment_info)

    if isinstance(match, tuple) and match[0] == "ALREADY_VERIFIED":
        verified_payment = match[1]
        verified_at = verified_payment[10] if len(verified_payment) > 10 else None
        verified_at_text = verified_at if verified_at else "desconocida"
        msg = (
            f"🟡 **Comprobante ya procesado**\n"
            f"Pago: {confirmation_display}\n"
            f"Monto: ${verified_payment[3]:.2f}\n"
            f"Recibido: {verified_at_text}"
        )
        if edit_receipt_followup_message(
            followup,
            msg,
            parse_mode="Markdown",
            reply_markup=get_receipt_processed_markup()
        ):
            mirror_receipt_followup_action_message(followup)
        update_receipt_followup(
            followup['id'],
            status="PROCESSED",
            manual_amount=amount,
            manual_confirmation=confirmation,
            manual_confirmation_full=payment_info['confirmation_full'],
            manual_receipt_time=receipt_time,
            handled_at=utcnow_text()
        )
        return True

    if not match:
        pending_id = add_pending_verification(
            user_id,
            followup['chat_id'],
            followup['receipt_message_id'],
            confirmation,
            amount,
            receipt_time,
            followup_id=followup['id']
        )
        if not pending_id:
            edit_receipt_followup_message(
                followup,
                "No pude guardar la verificación pendiente. Inténtalo de nuevo.",
                reply_markup=get_receipt_followup_markup(
                    followup['id'],
                    amount=amount,
                    confirmation=confirmation,
                    confirmation_full=payment_info.get('confirmation_full'),
                    receipt_time=receipt_time
                )
            )
            return False

        amount_text = f"${amount:.2f}" if amount is not None else "?"
        msg = (
            f"🔵 **Verificando pago...**\n"
            f"Monto: {amount_text}\n"
            f"Confirmación: {confirmation_display}"
        )
        if edit_receipt_followup_message(
            followup,
            msg,
            parse_mode="Markdown",
            reply_markup=get_receipt_processed_markup()
        ):
            mirror_receipt_followup_action_message(followup)
        else:
            return False
        update_receipt_followup(
            followup['id'],
            status="PROCESSED",
            manual_amount=amount,
            manual_confirmation=confirmation,
            manual_confirmation_full=payment_info['confirmation_full'],
            manual_receipt_time=receipt_time,
            pending_verification_id=pending_id,
            handled_at=utcnow_text()
        )
        return True

    payment_id = match[0]
    db_time = match[2]
    db_amount = match[3]
    db_sender = match[4]
    account_tag = match[7]
    account_name = YAPPY_ACCOUNTS.get(account_tag, account_tag)

    if not mark_payment_verified(payment_id, user_id=user_id):
        edit_receipt_followup_message(
            followup,
            "No pude finalizar la verificación del pago. Inténtalo de nuevo.",
            reply_markup=get_receipt_followup_markup(
                followup['id'],
                amount=amount,
                confirmation=confirmation,
                confirmation_full=payment_info.get('confirmation_full'),
                receipt_time=receipt_time
            )
        )
        return False

    if is_admin_user(user_id):
        wallet_summary = ""
    else:
        wallet_summary = process_wallet_deposit(user_id, db_amount)
        wallet_summary = sanitize_wallet_summary_for_ocr(wallet_summary)

    msg = (
        f"✅ **Pago Verificado**\n"
        f"Monto: ${db_amount:.2f}\n"
        f"Hora: {db_time}\n"
        f"De: {db_sender}\n"
        f"Cuenta: {account_name}"
    )
    if wallet_summary:
        msg += "\n" + wallet_summary

    if edit_receipt_followup_message(
        followup,
        msg,
        parse_mode="Markdown",
        reply_markup=get_receipt_processed_markup()
    ):
        mirror_receipt_followup_action_message(followup)
    else:
        return False

    update_receipt_followup(
        followup['id'],
        status="PROCESSED",
        manual_amount=amount,
        manual_confirmation=confirmation,
        manual_confirmation_full=payment_info['confirmation_full'],
        manual_receipt_time=receipt_time,
        handled_at=utcnow_text()
    )
    return True

def add_pending_verification(user_id, chat_id, message_id, confirmation, amount, receipt_time, followup_id=None, reply_message_id=None):
    """Store a pending verification request and return its row id."""
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute('''INSERT INTO pending_verifications
                     (user_id, chat_id, message_id, confirmation_letters, amount, receipt_time, followup_id, reply_message_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, chat_id, message_id, confirmation, amount, receipt_time, followup_id, reply_message_id))
        pending_id = c.lastrowid
        conn.commit()
        conn.close()
        return pending_id
    except Exception as e:
        print(f"❌ Error adding pending: {e}")
        return None

def check_and_notify_pending(payment_data, payment_id):
    """
    Check if a new payment matches any pending verification requests.
    payment_data is a dict or row with: confirmation, amount, sender_name, etc.
    payment_id is the ID of the newly inserted payment record.
    """
    try:
        confirmation = payment_data['confirmation']
        amount = payment_data['amount']
        payment_time = payment_data['time']

        # Channel line had no real confirmation code; skip pending-match lookup.
        if str(confirmation).startswith("NOCONF_"):
            return
        
        conn = get_yappy_db()
        c = conn.cursor()
        
        # Find matching pending requests
        c.execute('''SELECT id, user_id, chat_id, message_id, amount, receipt_time, followup_id, reply_message_id FROM pending_verifications
                     WHERE confirmation_letters = ?''', (confirmation,))
        pending_requests = c.fetchall()
        
        if not pending_requests:
            # Debug: show we checked but found nothing
            print(f"🔎 No pending requests found for {confirmation}")
            conn.close()
            return
            
        print(f"🔎 Found {len(pending_requests)} pending request(s) for {confirmation}")
        should_react = False
        
        for req in pending_requests:
            req_id, user_id, chat_id, message_id, req_amount, req_time, followup_id, reply_message_id = req
            
            # 1. Check Amount (Allow small mismatch)
            if req_amount and abs(req_amount - amount) > 0.05:
                print(f"⚠️ Pending {req_id}: Amount mismatch (${req_amount} vs ${amount})")
                continue 
            
            # 2. Check Time (if receipt had time) - normalized comparison
            if req_time and payment_time:
                # Normalize: strip leading zeros from hours so "01:06" == "1:06"
                def _norm_time(t):
                    m = re.match(r'0*(\d{1,2}):(\d{2})', str(t).strip())
                    return f"{int(m.group(1))}:{m.group(2)}" if m else str(t).strip()
                if _norm_time(req_time) != _norm_time(payment_time):
                    print(f"⚠️ Pending {req_id}: Time mismatch ({req_time} vs {payment_time})")
                    continue

            # SUCCESS! Matches.
            try:
                # 💰 WALLET DEPOSIT — skip for admin users (no fondo needed)
                if is_admin_user(user_id):
                    wallet_summary = ""
                else:
                    wallet_summary = process_wallet_deposit(user_id, amount)
                    wallet_summary = sanitize_wallet_summary_for_ocr(wallet_summary)

                msg = (f"✅ **Pago Verificado**\n\n"
                       f"Monto: ${amount:.2f}\n"
                       f"Confirmación: {format_confirmation_display(confirmation=confirmation)}\n"
                       f"De: {payment_data['sender_name']}\n"
                       f"Hora: {payment_time}")
                if wallet_summary:
                    msg += f"\n{wallet_summary}"

                followup = get_receipt_followup(followup_id) if followup_id else None
                edited_existing_followup = False
                if followup:
                    edited_existing_followup = edit_receipt_followup_message(
                        followup,
                        msg,
                        parse_mode="Markdown",
                        reply_markup=get_receipt_processed_markup()
                    )
                    if edited_existing_followup:
                        mirror_receipt_followup_action_message(followup)
                        complete_receipt_followup(followup_id, status="PROCESSED", update_markup=False)

                # Try editing the channel blue reply message directly
                if not edited_existing_followup and reply_message_id and chat_id:
                    try:
                        bot.edit_message_text(
                            msg + "\n_(Editado por el bot)_",
                            chat_id,
                            reply_message_id,
                            parse_mode="Markdown"
                        )
                        edited_existing_followup = True
                    except Exception as e:
                        print(f"⚠️ Failed to edit channel blue reply {reply_message_id} in {chat_id}: {e}")

                if not edited_existing_followup:
                    if message_id:
                        try:
                            sent_msg = bot.send_message(chat_id, msg, reply_to_message_id=message_id, parse_mode="Markdown")
                        except Exception as e:
                            print(f"⚠️ Reply-to send failed for pending verification {req_id}: {e}")
                            sent_msg = bot.send_message(chat_id, msg, parse_mode="Markdown")
                    else:
                        sent_msg = bot.send_message(chat_id, msg, parse_mode="Markdown")

                    mirror_to_topic(chat_id, sent_msg)
                    if followup_id:
                        complete_receipt_followup(followup_id, status="PROCESSED")

                # 2. Mark payment as verified in DB
                c.execute("UPDATE yappy_payments SET verified = 1 WHERE id = ?", (payment_id,))
                should_react = True
                
                # 3. Remove from pending
                c.execute("DELETE FROM pending_verifications WHERE id = ?", (req_id,))
                
            except Exception as e:
                print(f"⚠️ Failed to process pending {req_id}: {e}")

        conn.commit()
        conn.close()

        if should_react:
            trigger_payment_reaction(payment_id, user_id=user_id)

    except Exception as e:
        print(f"❌ Error checking pending: {e}")

def _normalize_reaction_emoji(emoji):
    """Normalize configured reaction emoji and repair common mojibake values."""
    value = (str(emoji or "").strip() or "👍")
    known_map = {
        "🎉": "🎉",
        "ðŸ‘": "👍",
        "✅": "✅",
        "âŒ": "❌",
        "⚠ï¸": "⚠️",
    }
    if value in known_map:
        return known_map[value]
    # Generic mojibake repair: utf-8 bytes decoded as latin1.
    if any(ch in value for ch in ("ð", "â", "Ã")):
        try:
            repaired = value.encode("latin1", errors="ignore").decode("utf-8", errors="ignore").strip()
            if repaired:
                return repaired
        except Exception:
            pass
    return value


def set_message_reaction_with_retry(chat_id, message_id, emoji='👍', max_retries=5, bot_token=None):
    """Retry Telegram reactions via raw HTTP API."""
    import requests as _req
    token = bot_token or TOKEN
    url = f"https://api.telegram.org/bot{token}/setMessageReaction"
    emoji_value = _normalize_reaction_emoji(emoji)
    fallback_used = False
    payload = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "reaction": [{"type": "emoji", "emoji": emoji_value}],
        "is_big": False
    }
    delay = 0.4
    for attempt in range(max_retries):
        try:
            resp = _req.post(url, json=payload, timeout=10)
            data = resp.json()
            if resp.status_code == 200 and data.get("ok"):
                return True
            err_desc = data.get("description", "unknown error")
            if attempt < max_retries - 1:
                retry_match = re.search(r'retry after\s+(\d+)', err_desc, re.IGNORECASE)
                if retry_match:
                    wait_s = int(retry_match.group(1)) + 0.2
                elif "Too Many Requests" in err_desc:
                    wait_s = delay
                    delay = min(delay * 2, 5.0)
                elif "REACTION_INVALID" in err_desc and not fallback_used:
                    # Invalid reaction for this chat or malformed emoji value: fallback to thumbs up.
                    payload["reaction"] = [{"type": "emoji", "emoji": "👍"}]
                    fallback_used = True
                    wait_s = 0.2
                else:
                    # Non-retryable error — stop immediately, log full response for diagnosis
                    print(
                        f"⚠️ Reaction failed for {chat_id}/{message_id}: {err_desc} "
                        f"| emoji={payload['reaction'][0]['emoji']} | full={data}"
                    )
                    return False
                time.sleep(wait_s)
                continue
            print(
                f"⚠️ Reaction failed for {chat_id}/{message_id}: {err_desc} "
                f"| emoji={payload['reaction'][0]['emoji']}"
            )
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
                continue
            print(f"⚠️ Reaction exception for {chat_id}/{message_id}: {e}")
    return False

def react_to_payment_message(payment_id, emoji='👍'):
    """React to the original payment message (channel/group) linked to this payment ID."""
    conn = None
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("SELECT tg_group_id, tg_message_id, source_bot FROM yappy_payments WHERE id = ?", (payment_id,))
        row = c.fetchone()
        if row and row[0] and row[1]:
            emoji_value = (emoji or YAPPY_BRIDGE_REACTION_EMOJI or '👍').strip() or '👍'
            # Use yahoo bot token for bridge-sourced payments (lot ticket bot can't see those messages)
            react_token = YAPPY_REACTION_BOT_TOKEN if (row[2] and YAPPY_REACTION_BOT_TOKEN) else None
            print(f"🔄 Attempting reaction: chat={row[0]} msg={row[1]} emoji={emoji_value} bridge={bool(row[2])}")
            return set_message_reaction_with_retry(row[0], row[1], emoji=emoji_value, bot_token=react_token)
        print(f"⚠️ No tg_group_id/tg_message_id for payment {payment_id} (row={row})")
        return False
    except Exception as e:
        print(f"⚠️ Could not fetch payment message for reaction ({payment_id}): {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def cleanup_old_payments():
    """Remove payments older than 24 hours"""
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("DELETE FROM yappy_payments WHERE datetime(received_at) < datetime('now', '-24 hours')")
        deleted = c.rowcount
        conn.commit()
        conn.close()
        if deleted > 0:
            print(f"🧹 Cleaned up {deleted} old Yappy payment(s)")
    except Exception as e:
        print(f"❌ Cleanup error: {e}")

def parse_yappy_message(text):
    """
    Parse Yappy payment message format from channel/group feed.

    Supported:
    - "HH:MM 🧧AMOUNT NAME CONFIRMATION_LETTERS #lPHONE #TAG"
    - "HH:MM 🧧AMOUNT NAME #lPHONE #TAG" (no confirmation token)
    """
    import re

    if not text:
        return None

    normalized = re.sub(r"\s+", " ", text.replace("\xa0", " ").replace("\u202f", " ")).strip()

    # Parse stable suffix first (#lPHONE #TAG), then split middle segment.
    # Accept both proper emoji and old mojibake token for compatibility.
    pattern = (
        r"^(?P<time>\d{1,2}:\d{2})\s+"
        r"(?:\U0001F9E7|🧧)\s*"
        r"(?P<amount>[0-9]+(?:[.,][0-9]{1,2})?)\s+"
        r"(?P<middle>.+?)\s+"
        r"#[lL](?P<phone>\d+)\s+"
        r"(?P<tag>#[A-Za-z]{2,4})\b"
    )

    match = re.search(pattern, normalized)
    if not match:
        return None

    middle = re.sub(r"\s+", " ", match.group("middle")).strip()
    if not middle:
        return None

    tokens = middle.split(" ")
    confirmation = None
    sender_name = middle

    # Treat trailing token as confirmation only when it clearly looks like one.
    # This avoids stealing last names (e.g. "Rodriguez") as confirmation.
    if len(tokens) >= 2:
        last_token = tokens[-1]
        if re.fullmatch(r"[A-Za-z]{3,7}", last_token) and last_token.upper() == last_token and last_token != last_token.title():
            confirmation = last_token.upper()
            sender_name = " ".join(tokens[:-1]).strip()

    if not sender_name:
        return None

    # Fallback synthetic confirmation for no-code channel lines.
    if not confirmation:
        phone_key = re.sub(r"\D", "", match.group("phone") or "")
        confirmation = f"NOCONF_{phone_key}" if phone_key else "NOCONF_00000"

    return {
        'time': match.group("time"),
        'amount': float(match.group("amount").replace(',', '.')),
        'sender_name': sender_name,
        'confirmation': confirmation,
        'phone': match.group("phone"),
        'account_tag': match.group("tag").upper()
    }

def store_yappy_payment(parsed_data, message_id, group_id, payment_date=None, source_bot=None, source_event_id=None):
    """Store Yappy payment in database for verification"""
    # Bridge-only mode guard: reject local/direct ingest unless explicitly enabled.
    if not YAPPY_DIRECT_INGEST_ENABLED and not (source_bot and source_event_id):
        print(f"⚠️ Skipping non-bridge payment ingest (chat={group_id}, msg={message_id})")
        return False

    conn = None
    try:
        conn = get_yappy_db()
        c = conn.cursor()

        # Use source date when provided; fallback to current Panama date
        today = payment_date or get_today_panama()

        c.execute('''INSERT OR IGNORE INTO yappy_payments
                     (date, message_time, amount, sender_name, confirmation_letters,
                      phone_number, account_tag, tg_message_id, tg_group_id, source_bot, source_event_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (today, parsed_data['time'], parsed_data['amount'],
                   parsed_data['sender_name'], parsed_data['confirmation'],
                   parsed_data['phone'], parsed_data['account_tag'],
                   message_id, group_id, source_bot, source_event_id))

        inserted = c.rowcount > 0
        payment_id = c.lastrowid if inserted else None

        # If this row already exists and new data came from bridge, promote bridge metadata
        # so later reactions target the original group message from producer bots.
        _already_verified = False
        if not inserted and source_bot and source_event_id:
            c.execute('''SELECT id, verified FROM yappy_payments
                         WHERE date = ? AND message_time = ? AND amount = ? AND confirmation_letters = ?
                         ORDER BY id DESC LIMIT 1''',
                      (today, parsed_data['time'], parsed_data['amount'], parsed_data['confirmation']))
            existing = c.fetchone()
            if existing:
                payment_id = existing[0]
                _already_verified = bool(existing[1])
                c.execute('''UPDATE yappy_payments
                             SET tg_message_id = ?,
                                 tg_group_id = ?,
                                 source_bot = ?,
                                 source_event_id = ?
                             WHERE id = ?''',
                          (message_id, group_id, source_bot, source_event_id, payment_id))

        conn.commit()

        # If bridge metadata upgraded a payment that was already verified, trigger reaction
        # so the emoji gets applied to the original producer message.
        if _already_verified and payment_id:
            trigger_payment_reaction(payment_id)

        # Check if this payment was waiting to be verified
        if payment_id:
            check_and_notify_pending(parsed_data, payment_id)

        return True
    except Exception as e:
        print(f"❌ Error storing payment: {e}")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def ocr_image_ocrspace(file_path, prepared_image=None):
    """
    Send image to OCR.space API and return extracted text
    Includes retry logic for reliability
    """
    if not OCR_API_KEY or OCR_API_KEY == "YOUR_OCR_API_KEY_HERE":
        return {"success": False, "error": "OCR API key not configured"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            prepared = prepared_image or prepare_ocr_image_payload(file_path)
            response = get_thread_http_session().post(
                'https://api.ocr.space/parse/image',
                files={
                    'file': (
                        prepared['filename'],
                        prepared['bytes'],
                        prepared['mime_type']
                    )
                },
                data={
                    'apikey': OCR_API_KEY,
                    'language': 'spa',  # Spanish
                    'isOverlayRequired': False,
                    'detectOrientation': True,
                    'scale': True
                },
                timeout=45  # Increased timeout
            )
            
            if response.status_code == 429 or response.status_code >= 500:
                print(f"⚠️ OCR.space API error {response.status_code}, retrying ({attempt+1}/{max_retries})...")
                time.sleep(2 * (attempt + 1))
                continue
                
            result = response.json()
            
            if result.get('IsErroredOnProcessing'):
                return {"success": False, "error": "OCR processing failed"}
            
            if result.get('ParsedResults'):
                text = result['ParsedResults'][0].get('ParsedText', '')
                return {"success": True, "text": text}
            
            return {"success": False, "error": "No text found"}
        
        except Exception as e:
            if attempt == max_retries - 1:
                return {"success": False, "error": str(e)}
            print(f"⚠️ OCR Network error: {e}, retrying...")
            time.sleep(2)
            
    return {"success": False, "error": "Max retries exceeded"}

OCR_SYSTEM_PROMPT = (
    "You are a robotic OCR engine. Focus ONLY on the 'Yappy' payment receipt. "
    "IGNORE background chat messages, batteries, or UI elements. Extract the Amount and "
    "Confirmation Code exactly. IMPORTANT: The confirmation code is a '#' followed by "
    "exactly 5 UPPERCASE letters, a dash, and digits (e.g. #YPYBI-91416276). Pay careful "
    "attention to distinguish I (capital i) from L (capital el) because the Yappy font "
    "makes them look similar."
)
OCR_USER_PROMPT = "Scan this receipt. Output every single line of text found. Do not miss the total amount."


def ocr_image_openai_compatible(file_path, provider_name, api_key, api_base, model_name, frequency_penalty=None, prepared_image=None):
    """
    Send image to an OpenAI-compatible multimodal endpoint acting as a strict OCR scanner.
    Includes retry logic for reliability.
    """
    if not api_key:
        return {"success": False, "error": f"{provider_name} API key not configured"}
    if not api_base:
        return {"success": False, "error": f"{provider_name} API base not configured"}
    if not model_name:
        return {"success": False, "error": f"{provider_name} model not configured"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            prepared = prepared_image or prepare_ocr_image_payload(file_path)

            if attempt > 0:
                print(f"{provider_name} OCR Attempt {attempt+1}/{max_retries}...")

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }

            payload = {
                "model": model_name,
                "temperature": 0.1,
                "messages": [
                    {
                        "role": "system",
                        "content": OCR_SYSTEM_PROMPT
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": OCR_USER_PROMPT
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": prepared['data_url']
                                }
                            }
                        ]
                    }
                ],
                "max_tokens": 1000
            }
            if frequency_penalty is not None:
                payload["frequency_penalty"] = frequency_penalty

            try:
                response = get_thread_http_session().post(api_base, headers=headers, json=payload, timeout=60)
            except Exception as e:
                print(f"{provider_name} network request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                return {"success": False, "error": f"Network Error: {e}"}

            if response.status_code == 429 or response.status_code >= 500:
                print(f"{provider_name} API error {response.status_code}, retrying ({attempt+1}/{max_retries})...")
                time.sleep(3 * (attempt + 1))
                continue

            if response.status_code != 200:
                print(f"{provider_name} API status {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return {"success": False, "error": f"API Error {response.status_code}"}

            result = response.json()

            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                if isinstance(content, list):
                    content = "\n".join(
                        part.get('text', '')
                        for part in content
                        if isinstance(part, dict) and part.get('text')
                    )
                elif content is None:
                    content = ""
                else:
                    content = str(content)
                content = content.replace("```text", "").replace("```", "").strip()

                if not content:
                    print(f"{provider_name} returned empty content, retrying...")
                    time.sleep(2)
                    continue

                if len(content) > 50:
                    tokens = content.split()[:20]
                    if len(set(tokens)) < 5:
                        print(f"{provider_name} produced repetitive OCR content, retrying...")
                        time.sleep(2)
                        continue
                    content_lines = content.strip().splitlines()[:15]
                    struct_count = sum(1 for ln in content_lines if re.match(r'\s*-\s*(line\s+)?\d', ln.strip()))
                    if struct_count >= 3:
                        print(f"{provider_name} produced overly structured OCR content, retrying...")
                        time.sleep(2)
                        continue

                if "Output every single line" in content:
                    print(f"{provider_name} echoed the OCR prompt, retrying...")
                    time.sleep(1)
                    continue

                if len(content) < 40 and not any(k in content.lower() for k in ['yappy', '$', 'enviado', 'confirm', 'fecha', 'realiz']):
                    print(f"{provider_name} returned low-information OCR, retrying...")
                    time.sleep(2)
                    continue

                return {"success": True, "text": content, "provider": provider_name}

            print(f"{provider_name} returned no choices, retrying...")
            time.sleep(2)

        except Exception as e:
            print(f"{provider_name} OCR exception: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return {"success": False, "error": str(e)}

    return {"success": False, "error": "Max retries exceeded"}


def ocr_image_gemini(file_path, prepared_image=None):
    """Send image to Gemini as a strict OCR scanner using the official REST API."""
    provider_name = "Gemini 2.5 Flash-Lite"
    if not GEMINI_API_KEY:
        return {"success": False, "error": f"{provider_name} API key not configured"}
    if not GEMINI_API_BASE:
        return {"success": False, "error": f"{provider_name} API base not configured"}
    if not GEMINI_MODEL:
        return {"success": False, "error": f"{provider_name} model not configured"}

    endpoint = f"{GEMINI_API_BASE.rstrip('/')}/{urllib.parse.quote(GEMINI_MODEL, safe='')}:generateContent"
    max_retries = 3

    for attempt in range(max_retries):
        try:
            prepared = prepared_image or prepare_ocr_image_payload(file_path)

            if attempt > 0:
                print(f"{provider_name} OCR Attempt {attempt+1}/{max_retries}...")

            headers = {
                "Content-Type": "application/json",
                "x-goog-api-key": GEMINI_API_KEY
            }
            payload = {
                "system_instruction": {
                    "parts": [
                        {"text": OCR_SYSTEM_PROMPT}
                    ]
                },
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": OCR_USER_PROMPT},
                            {
                                "inline_data": {
                                    "mime_type": prepared['mime_type'],
                                    "data": prepared['base64']
                                }
                            }
                        ]
                    }
                ],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 1000
                }
            }

            try:
                response = get_thread_http_session().post(endpoint, headers=headers, json=payload, timeout=60)
            except Exception as e:
                print(f"{provider_name} network request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                return {"success": False, "error": f"Network Error: {e}"}

            if response.status_code == 429 or response.status_code >= 500:
                print(f"{provider_name} API error {response.status_code}, retrying ({attempt+1}/{max_retries})...")
                time.sleep(3 * (attempt + 1))
                continue

            if response.status_code != 200:
                print(f"{provider_name} API status {response.status_code}: {response.text}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return {"success": False, "error": f"API Error {response.status_code}"}

            result = response.json()
            prompt_feedback = result.get('promptFeedback') or {}
            if prompt_feedback.get('blockReason'):
                return {"success": False, "error": f"Blocked: {prompt_feedback['blockReason']}"}

            candidates = result.get('candidates') or []
            if not candidates:
                print(f"{provider_name} returned no candidates, retrying...")
                time.sleep(2)
                continue

            parts = ((candidates[0].get('content') or {}).get('parts') or [])
            content = "\n".join(
                part.get('text', '')
                for part in parts
                if isinstance(part, dict) and part.get('text')
            ).replace("```text", "").replace("```", "").strip()

            if not content:
                print(f"{provider_name} returned empty content, retrying...")
                time.sleep(2)
                continue

            if len(content) > 50:
                tokens = content.split()[:20]
                if len(set(tokens)) < 5:
                    print(f"{provider_name} produced repetitive OCR content, retrying...")
                    time.sleep(2)
                    continue
                content_lines = content.strip().splitlines()[:15]
                struct_count = sum(1 for ln in content_lines if re.match(r'\s*-\s*(line\s+)?\d', ln.strip()))
                if struct_count >= 3:
                    print(f"{provider_name} produced overly structured OCR content, retrying...")
                    time.sleep(2)
                    continue

            if "Output every single line" in content:
                print(f"{provider_name} echoed the OCR prompt, retrying...")
                time.sleep(1)
                continue

            if len(content) < 40 and not any(k in content.lower() for k in ['yappy', '$', 'enviado', 'confirm', 'fecha', 'realiz']):
                print(f"{provider_name} returned low-information OCR, retrying...")
                time.sleep(2)
                continue

            return {"success": True, "text": content, "provider": provider_name}

        except Exception as e:
            print(f"{provider_name} OCR exception: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return {"success": False, "error": str(e)}

    return {"success": False, "error": "Max retries exceeded"}


def ocr_image_deepseek(file_path, prepared_image=None):
    """Send image to DeepSeek OCR via DeepInfra."""
    return ocr_image_openai_compatible(
        file_path=file_path,
        provider_name="DeepSeek OCR",
        api_key=DEEPINFRA_API_KEY,
        api_base=DEEPINFRA_API_BASE,
        model_name=DEEPSEEK_MODEL,
        frequency_penalty=0.5,
        prepared_image=prepared_image
    )


def ocr_image_qwen(file_path, prepared_image=None):
    """Send image to Qwen OCR via DeepInfra."""
    return ocr_image_openai_compatible(
        file_path=file_path,
        provider_name="Qwen OCR",
        api_key=DEEPINFRA_API_KEY,
        api_base=DEEPINFRA_API_BASE,
        model_name=QWEN_MODEL,
        prepared_image=prepared_image
    )


RECEIPT_MONTH_ALIASES = {
    'ENE': 1, 'ENERO': 1,
    'FEB': 2, 'FEBRERO': 2,
    'MAR': 3, 'MARZO': 3,
    'ABR': 4, 'ABRIL': 4,
    'MAY': 5, 'MAYO': 5,
    'JUN': 6, 'JUNIO': 6,
    'JUL': 7, 'JULIO': 7,
    'AGO': 8, 'AGOSTO': 8,
    'SEP': 9, 'SEPT': 9, 'SEPTIEMBRE': 9, 'SET': 9, 'SETIEMBRE': 9,
    'OCT': 10, 'OCTUBRE': 10,
    'NOV': 11, 'NOVIEMBRE': 11,
    'DIC': 12, 'DICIEMBRE': 12,
}
RECEIPT_MONTH_DISPLAY = {
    1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr', 5: 'may', 6: 'jun',
    7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic',
}


def _normalize_receipt_month_token(token):
    cleaned = re.sub(r'[^A-Za-zÁÉÍÓÚÑáéíóúñ]', '', str(token or '')).upper()
    return cleaned.translate(str.maketrans({
        'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'
    }))


def parse_receipt_date_text(text):
    """Extract and normalize a receipt date to YYYY-MM-DD."""
    if not text:
        return None

    numeric_match = re.search(r'\b(\d{1,2})\s*[\/\-]\s*(\d{1,2})\s*[\/\-]\s*(\d{2,4})\b', text)
    if numeric_match:
        day = int(numeric_match.group(1))
        month = int(numeric_match.group(2))
        year = int(numeric_match.group(3))
        year = 2000 + year if year < 100 else year
        try:
            return datetime.date(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return None

    month_match = re.search(r'\b(\d{1,2})\s+([A-Za-zÁÉÍÓÚÑáéíóúñ]{3,12})\s+(\d{2,4})\b', text, re.IGNORECASE)
    if not month_match:
        return None

    day = int(month_match.group(1))
    month_token = _normalize_receipt_month_token(month_match.group(2))
    year = int(month_match.group(3))
    year = 2000 + year if year < 100 else year
    month = RECEIPT_MONTH_ALIASES.get(month_token)
    if month is None:
        return None
    try:
        return datetime.date(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def format_receipt_date_display(date_text):
    """Render YYYY-MM-DD as a short Spanish-style display date."""
    if not date_text:
        return "desconocida"
    try:
        parsed = datetime.datetime.strptime(str(date_text), "%Y-%m-%d").date()
    except Exception:
        return str(date_text)
    month_text = RECEIPT_MONTH_DISPLAY.get(parsed.month, f"{parsed.month:02d}")
    return f"{parsed.day:02d} {month_text} {parsed.year}"


def build_stale_receipt_message(payment_info):
    """Explain that the receipt is not from the current Panama day."""
    receipt_date = format_receipt_date_display(payment_info.get('receipt_date'))
    panama_today = format_receipt_date_display(get_today_panama())
    return (
        "Por favor, mande un comprobante más reciente.\n"
        f"Fecha del comprobante: {receipt_date}\n"
        f"Fecha de hoy: {panama_today}"
    )


def is_receipt_from_today_panama(payment_info):
    """Only current-day receipts are accepted, using Panama time."""
    return bool(payment_info and payment_info.get('receipt_date') == get_today_panama())


def parse_yappy_screenshot(ocr_text):
    """
    Extract payment info from OCR text of Yappy screenshot
    Handles both "share" (with sender name) and "screenshot" (enviaste) types
    
    Yappy format examples:
    - Amount: $15.00
    - Time: 1:06 p.m. or 13:06
    - Confirmation: #EHTDV-25721322 (we extract EHTDV)
    """
    import re
    
    # Keep original for name extraction, uppercase for other patterns
    original_text = ocr_text.strip()

    # OCR often appends artifacts like "#015" at line endings. Strip those safely.
    cleaned_lines = []
    for raw_line in ocr_text.strip().split('\n'):
        line = re.sub(r'#\d{3}\s*$', '', raw_line).strip()
        if line:
            cleaned_lines.append(line)
    lines = [line.upper() for line in cleaned_lines]
    
    info = {
        'time': None,
        'receipt_date': None,
        'amount': None,
        'confirmation': None,
        'sender_name': None,
        'requester_name': None,
        'is_screenshot_type': False,
        'is_money_request': False,
        'recipient_name': None  # New: who received the payment
    }
    
    # Check if it's "enviaste" screenshot type
    if any('ENVIASTE' in line or 'LISTO' in line for line in lines):
        info['is_screenshot_type'] = True

    # Check if it's a money REQUEST (not a sent payment)
    if any('PEDISTE' in line or 'PIDIÓ' in line or 'PIDIO' in line or 'TE PIDIÓ' in line or 'TE PIDIO' in line for line in lines):
        info['is_money_request'] = True
    
    full_text = ' '.join(lines)

    def _parse_amount_token(token):
        """Normalize common OCR confusions (O->0, I/L->1, S->5) and parse float."""
        if not token:
            return None
        normalized = token.replace(',', '.').strip().translate(str.maketrans({
            'O': '0', 'I': '1', 'L': '1', 'S': '5'
        }))
        try:
            value = float(normalized)
            if 0 < value < 10000:
                return value
        except (TypeError, ValueError):
            return None
        return None
    
    # Extract time - multiple formats:
    # "1:06 p.m." or "1:06 P.M." or "13:06" or "1:06"
    time_match = re.search(r'(\d{1,2}:\d{2})\s*(?:P\.?M\.?|A\.?M\.?)?', full_text, re.IGNORECASE)
    if time_match:
        info['time'] = time_match.group(1)
    info['receipt_date'] = parse_receipt_date_text(original_text) or parse_receipt_date_text(full_text)
    
    # 1. EXTRACT AMOUNT
    
    # Priority A: Structural Search (Look for number above "Enviado a")
    # This is safer than finding any '$' because it's anchored to the receipt layout
    for i, line in enumerate(lines):
        if 'ENVIADO A' in line.upper() or 'PARA' in line.upper():
            # Check 1-2 lines ABOVE this one
            start_idx = max(0, i-2)
            for prev_line in lines[start_idx:i]:
                # Accept common OCR mistakes: 1<->I/L and 0<->O
                loose_match = re.search(r'([0-9OILS]+(?:[.,][0-9OILS]{2}))', prev_line)
                if loose_match:
                    value = _parse_amount_token(loose_match.group(1))
                    if value is not None:
                        info['amount'] = value
                        break
            if info['amount']: break

    # Priority B: Global Dollar Search (Fallback)
    if not info['amount']:
        for line in lines:
            # Match $5.00, $ 5.00, B/. 5.00
            amt_match = re.search(r'(?:\$|B/\.?)\s*([0-9OILS]+(?:[.,][0-9OILS]{2}))', line, re.IGNORECASE)
            if amt_match:
                value = _parse_amount_token(amt_match.group(1))
                if value is not None:
                    info['amount'] = value
                    break
    
    # Extract confirmation code - Yappy format: #XXXXX-NNNNNNNN
    # Store letters for matching, but also keep full code for display
    conf_match = re.search(r'#([A-Z]{5})\s*[-–]?\s*(\d+)', full_text)
    if conf_match:
        info['confirmation'] = conf_match.group(1)  # Letters only for matching
        info['confirmation_full'] = f"#{conf_match.group(1)}-{conf_match.group(2)}"  # Full code for display
    else:
        # Fallback: look for standalone 5 capital letters
        conf_match = re.search(r'\b([A-Z]{5})\b', full_text)
        if conf_match:
            # Make sure it's not a common word
            found = conf_match.group(1)
            common_words = ['LISTO', 'FECHA', 'ENVIO', 'YAPPY', 'BANCO', 'DESDE', 'HACIA', 'PAGAR', 'ENVIAR', 'PIDIO']
            if found not in common_words:
                info['confirmation'] = found
    
    name_token = r'[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+'
    full_name_pattern = rf'({name_token}(?:[ \t]+{name_token})+)'

    # Extract recipient name (who received the payment) from either payment or request layouts.
    recipient_match = re.search(
        rf'(?i:(?:ENVIADO\s*A|PARA|PEDIDO\s*A))\s*[:\s]*{full_name_pattern}',
        original_text
    )
    if recipient_match:
        info['recipient_name'] = recipient_match.group(1)

    requester_match = re.search(
        rf'(?m)^\s*{full_name_pattern}\s+(?:(?i:TE)\s+)?(?i:PIDI[OÓ])',
        original_text
    )
    if requester_match:
        info['requester_name'] = requester_match.group(1)
        info['sender_name'] = requester_match.group(1)

    # Extract sender name for normal payment layouts when we did not already
    # identify a requester from a money-request screenshot.
    if not info['sender_name'] and not info['is_screenshot_type']:
        name_matches = re.findall(full_name_pattern, original_text)
        for name in name_matches:
            if name != info.get('recipient_name'):
                info['sender_name'] = name
                break
    
    return info


def build_money_request_warning(payment_info, sender_name=None):
    """Build a warning message for money REQUEST screenshots (not payments sent)."""
    banner = "\u26a0\ufe0f\u26a0\ufe0f\u26d4\ufe0f\u26d4\ufe0f\U0001f92c\U0001f92c\U0001f92c\U0001f92c\U0001f92c\U0001f92c\U0001f92c"
    amount_text = f"${payment_info['amount']:.2f}" if payment_info.get('amount') else "monto desconocido"
    recipient = payment_info.get('recipient_name') or "desconocido"
    requester = (
        payment_info.get('requester_name')
        or payment_info.get('sender_name')
        or sender_name
        or "Desconocido"
    )
    parts = [requester, "\u8bc8\u9a97\uff0c\u5c0f\u5fc3"]
    if payment_info.get('time'):
        parts.append(payment_info['time'])
    parts.append(amount_text)
    parts.append(f"para {recipient}")
    body = " ".join(parts)
    return f"{banner}\n{body}\n{banner}"


def send_money_request_alert(message, warning_msg):
    """Send the money-request screenshot to the warning group with caption."""
    try:
        photo_variants = getattr(message, 'photo', None) or []
        if photo_variants:
            bot.send_photo(
                MONEY_REQUEST_WARNING_GROUP_ID,
                photo=photo_variants[-1].file_id,
                caption=warning_msg,
                parse_mode="Markdown"
            )
        else:
            bot.send_message(
                MONEY_REQUEST_WARNING_GROUP_ID,
                warning_msg,
                parse_mode="Markdown"
            )
        return True
    except Exception as e:
        print(f"⚠️ Failed to send money request warning to group: {e}")
        return False


def has_required_receipt_fields(payment_info):
    """True when OCR produced the minimum fields needed for receipt verification."""
    return bool(
        payment_info
        and payment_info.get('amount') is not None
        and payment_info.get('confirmation')
        and payment_info.get('receipt_date')
    )


def has_money_request_fields(payment_info):
    """True when OCR looks like a Yappy money-request screenshot."""
    return bool(payment_info and payment_info.get('is_money_request') and payment_info.get('amount') is not None)


def summarize_receipt_info(payment_info):
    """Compact log summary for OCR comparisons."""
    if not payment_info:
        return "amount=?, confirmation=?, date=?, time=?"
    amount = payment_info.get('amount')
    amount_text = f"{amount:.2f}" if isinstance(amount, (int, float)) else "?"
    confirmation = payment_info.get('confirmation_full') or payment_info.get('confirmation') or "?"
    receipt_date = payment_info.get('receipt_date') or "?"
    time_text = payment_info.get('time') or "?"
    return f"amount={amount_text}, confirmation={confirmation}, date={receipt_date}, time={time_text}"


def get_confirmation_numeric_suffix(payment_info):
    """Return the numeric half of a full Yappy confirmation like #ABCDE-12345678."""
    raw_full = str((payment_info or {}).get('confirmation_full') or "").strip().upper()
    if raw_full.startswith("#"):
        raw_full = raw_full[1:]
    match = re.fullmatch(r'[A-Z]{5}-(\d+)', raw_full)
    return match.group(1) if match else None


def summarize_money_request_info(payment_info):
    """Compact log summary for Yappy money-request screenshots."""
    if not payment_info:
        return "requester=?, recipient=?, amount=?, time=?"
    requester = payment_info.get('requester_name') or payment_info.get('sender_name') or "?"
    recipient = payment_info.get('recipient_name') or "?"
    amount = payment_info.get('amount')
    amount_text = f"{amount:.2f}" if isinstance(amount, (int, float)) else "?"
    time_text = payment_info.get('time') or "?"
    return f"requester={requester}, recipient={recipient}, amount={amount_text}, time={time_text}"


def analyze_ocr_result(provider_name, ocr_result):
    """Parse and score an OCR response so model outputs can be compared safely."""
    analysis = {
        'provider': provider_name,
        'success': bool(ocr_result.get('success')),
        'error': ocr_result.get('error'),
        'text': ocr_result.get('text', '') if ocr_result else '',
        'payment_info': {},
        'usable': False,
        'money_request_candidate': False
    }

    if analysis['success'] and analysis['text']:
        payment_info = parse_yappy_screenshot(analysis['text'])
        analysis['payment_info'] = payment_info
        analysis['usable'] = has_required_receipt_fields(payment_info)
        analysis['money_request_candidate'] = has_money_request_fields(payment_info)
        if not analysis['usable'] and analysis['money_request_candidate'] and not analysis['error']:
            analysis['error'] = "Money request screenshot detected"
        elif not analysis['usable'] and not analysis['error']:
            analysis['error'] = "Missing amount/confirmation/date"

    return analysis


def receipt_infos_match(info_a, info_b):
    """Compare the useful receipt fields extracted by two OCR models."""
    if not has_required_receipt_fields(info_a) or not has_required_receipt_fields(info_b):
        return False, "Missing amount/confirmation/date"

    if abs(info_a['amount'] - info_b['amount']) > 0.01:
        return False, f"Amount mismatch ({info_a['amount']:.2f} vs {info_b['amount']:.2f})"

    if info_a['confirmation'] != info_b['confirmation']:
        # Allow close matches (≤2 char difference) — OCR often confuses similar letters (X/Z, I/L, etc.)
        conf_a, conf_b = info_a['confirmation'], info_b['confirmation']
        if len(conf_a) != len(conf_b) or sum(a != b for a, b in zip(conf_a, conf_b)) > 2:
            suffix_a = get_confirmation_numeric_suffix(info_a)
            suffix_b = get_confirmation_numeric_suffix(info_b)
            if not suffix_a or not suffix_b or suffix_a != suffix_b:
                left_conf = info_a.get('confirmation_full', conf_a)
                right_conf = info_b.get('confirmation_full', conf_b)
                return False, f"Confirmation mismatch ({left_conf} vs {right_conf})"

    if info_a.get('receipt_date') != info_b.get('receipt_date'):
        return False, f"Date mismatch ({info_a.get('receipt_date')} vs {info_b.get('receipt_date')})"

    if info_a.get('time') and info_b.get('time') and time_difference_minutes(info_a['time'], info_b['time']) > 2:
        return False, f"Time mismatch ({info_a['time']} vs {info_b['time']})"

    return True, None


def money_request_infos_match(info_a, info_b):
    """Compare the useful fields extracted from money-request screenshots."""
    if not has_money_request_fields(info_a) or not has_money_request_fields(info_b):
        return False, "Missing money-request fields"

    if abs(info_a['amount'] - info_b['amount']) > 0.01:
        return False, f"Amount mismatch ({info_a['amount']:.2f} vs {info_b['amount']:.2f})"

    if info_a.get('time') and info_b.get('time') and time_difference_minutes(info_a['time'], info_b['time']) > 2:
        return False, f"Time mismatch ({info_a['time']} vs {info_b['time']})"

    return True, None


def choose_primary_ocr_analysis(primary_analyses):
    """Accept a primary OCR result only when configured primary models agree."""
    if not primary_analyses:
        return None, "No primary OCR configured"

    usable = [analysis for analysis in primary_analyses if analysis.get('usable')]

    if len(primary_analyses) == 1:
        if usable:
            return usable[0], None
        provider = primary_analyses[0]['provider']
        return None, primary_analyses[0].get('error') or f"{provider} did not extract amount/confirmation"

    if len(usable) != len(primary_analyses):
        missing = ', '.join(
            f"{analysis['provider']} ({analysis.get('error') or 'missing amount/confirmation'})"
            for analysis in primary_analyses if not analysis.get('usable')
        )
        return None, f"Primary OCR incomplete: {missing}"

    reference = usable[0]
    close_match = False
    for analysis in usable[1:]:
        matches, reason = receipt_infos_match(reference['payment_info'], analysis['payment_info'])
        if not matches:
            return None, f"{reference['provider']} vs {analysis['provider']}: {reason}"
        # If confirmations differ (close-enough match), prefer the last provider (Qwen)
        if reference['payment_info']['confirmation'] != analysis['payment_info']['confirmation']:
            close_match = True

    return usable[-1] if close_match else reference, None


def choose_majority_ocr_analysis(analyses):
    """Pick the winning OCR result by 2-of-3 vote only."""
    usable = [analysis for analysis in analyses if analysis.get('usable')]
    if not usable:
        return None, "No usable OCR results"

    groups = []
    for analysis in usable:
        for group in groups:
            matches, _reason = receipt_infos_match(analysis['payment_info'], group[0]['payment_info'])
            if matches:
                group.append(analysis)
                break
        else:
            groups.append([analysis])

    majority_group = max(groups, key=len)
    if len(majority_group) >= 2:
        chosen = majority_group[0]
        if any(
            analysis['payment_info'].get('confirmation') != chosen['payment_info'].get('confirmation')
            for analysis in majority_group[1:]
        ):
            # When the group only agrees via relaxed confirmation matching, prefer the later provider.
            chosen = majority_group[-1]
        providers = ' + '.join(analysis['provider'] for analysis in majority_group)
        return chosen, f"Majority vote ({providers})"

    return None, "No OCR majority available"


def choose_money_request_analysis(analyses):
    """Pick the OCR result that best represents a money-request screenshot."""
    classified = [analysis for analysis in analyses if analysis.get('success') and analysis.get('text')]
    candidates = [analysis for analysis in classified if analysis.get('money_request_candidate')]
    if not candidates:
        return None, "No money-request OCR results"

    if len(classified) == 1 and len(candidates) == 1:
        chosen = candidates[0]
        return chosen, f"Money request signal ({chosen['provider']})"

    if len(candidates) * 2 <= len(classified):
        return None, f"Money-request vote failed ({len(candidates)}/{len(classified)})"

    groups = []
    for analysis in candidates:
        for group in groups:
            matches, _reason = money_request_infos_match(analysis['payment_info'], group[0]['payment_info'])
            if matches:
                group.append(analysis)
                break
        else:
            groups.append([analysis])

    majority_group = max(groups, key=len)
    chosen = majority_group[0]
    if len(majority_group) >= 2:
        providers = ' + '.join(analysis['provider'] for analysis in majority_group)
        return chosen, f"Money request consensus ({providers})"

    return None, f"Money-request candidates disagreed ({len(candidates)}/{len(classified)} votes)"


def choose_lone_usable_primary_analysis(primary_analyses):
    """Allow one strong primary OCR result to survive if the others were only incomplete."""
    usable = [analysis for analysis in primary_analyses if analysis.get('usable')]
    if len(usable) != 1:
        return None, None

    chosen = usable[0]
    other_analyses = [analysis for analysis in primary_analyses if analysis is not chosen]
    if any(analysis.get('money_request_candidate') for analysis in other_analyses):
        return None, "Competing money-request signal present"

    if not other_analyses:
        return chosen, f"Sole usable primary ({chosen['provider']})"

    incomplete = ', '.join(
        f"{analysis['provider']} ({analysis.get('error') or 'missing amount/confirmation/date'})"
        for analysis in other_analyses
    )
    return chosen, f"Sole usable primary ({chosen['provider']}); others incomplete: {incomplete}"


def run_primary_ocr_models(file_path, prepared_image=None):
    """Run configured primary OCR models, in parallel when more than one is enabled."""
    primary_models = []
    if DEEPINFRA_API_KEY and DEEPSEEK_MODEL:
        primary_models.append(("DeepSeek OCR", ocr_image_deepseek))
    if DEEPINFRA_API_KEY and QWEN_MODEL:
        primary_models.append(("Qwen OCR", ocr_image_qwen))

    if not primary_models:
        return []

    analyses = []
    provider_order = {provider: idx for idx, (provider, _) in enumerate(primary_models)}

    if len(primary_models) == 1:
        provider_name, ocr_func = primary_models[0]
        print(f"Using {provider_name}...")
        analyses.append(analyze_ocr_result(provider_name, ocr_func(file_path, prepared_image=prepared_image)))
    else:
        print("Running primary OCR models in parallel...")
        with ThreadPoolExecutor(max_workers=len(primary_models)) as executor:
            future_map = {
                executor.submit(ocr_func, file_path, prepared_image=prepared_image): provider_name
                for provider_name, ocr_func in primary_models
            }
            for future in as_completed(future_map):
                provider_name = future_map[future]
                try:
                    ocr_result = future.result()
                except Exception as e:
                    ocr_result = {"success": False, "error": str(e)}
                analyses.append(analyze_ocr_result(provider_name, ocr_result))

    analyses.sort(key=lambda item: provider_order[item['provider']])

    for analysis in analyses:
        if analysis.get('usable'):
            print(f"{analysis['provider']} extracted {summarize_receipt_info(analysis['payment_info'])}")
        elif analysis.get('money_request_candidate'):
            print(f"{analysis['provider']} detected money request: {summarize_money_request_info(analysis['payment_info'])}")
        else:
            print(f"{analysis['provider']} unusable: {analysis.get('error') or 'missing amount/confirmation'}")

    return analyses


def run_receipt_ocr(file_path):
    """Select the OCR result to trust for receipt verification."""
    try:
        prepared_image = prepare_ocr_image_payload(file_path)
    except Exception as e:
        return {
            "success": False,
            "error": f"No pude abrir la imagen: {e}",
            "details": []
        }

    primary_analyses = run_primary_ocr_models(file_path, prepared_image=prepared_image)
    selected_analysis, primary_error = choose_primary_ocr_analysis(primary_analyses)

    if selected_analysis:
        source_label = selected_analysis['provider']
        if len(primary_analyses) > 1:
            providers = ' + '.join(analysis['provider'] for analysis in primary_analyses)
            print(f"Primary OCR consensus reached: {providers}")
            source_label = f"Consensus ({providers})"
        return {
            "success": True,
            "text": selected_analysis['text'],
            "payment_info": selected_analysis['payment_info'],
            "source": source_label,
            "details": primary_analyses
        }

    fallback_provider_name = None
    fallback_analysis = None
    if GEMINI_API_KEY:
        fallback_provider_name = "Gemini 2.5 Flash-Lite"
        print(f"{fallback_provider_name} fallback triggered: {primary_error}")
        fallback_analysis = analyze_ocr_result(fallback_provider_name, ocr_image_gemini(file_path, prepared_image=prepared_image))
    elif OCR_API_KEY:
        fallback_provider_name = "OCR.space"
        print(f"{fallback_provider_name} fallback triggered: {primary_error}")
        fallback_analysis = analyze_ocr_result(fallback_provider_name, ocr_image_ocrspace(file_path, prepared_image=prepared_image))

    if fallback_analysis is not None:
        all_analyses = primary_analyses + [fallback_analysis]

        voted_analysis, vote_source = choose_majority_ocr_analysis(all_analyses)
        if voted_analysis:
            print(f"OCR tie-break selected: {vote_source}")
            return {
                "success": True,
                "text": voted_analysis['text'],
                "payment_info": voted_analysis['payment_info'],
                "source": vote_source,
                "details": all_analyses
            }

        money_request_analysis, money_request_source = choose_money_request_analysis(all_analyses)
        if money_request_analysis:
            print(f"OCR selected money request: {money_request_source}")
            return {
                "success": True,
                "text": money_request_analysis['text'],
                "payment_info": money_request_analysis['payment_info'],
                "source": money_request_source,
                "details": all_analyses
            }

        if fallback_analysis.get('usable'):
            print(f"{fallback_provider_name} selected as tie-break result: {summarize_receipt_info(fallback_analysis['payment_info'])}")
            return {
                "success": True,
                "text": fallback_analysis['text'],
                "payment_info": fallback_analysis['payment_info'],
                "source": f"{fallback_provider_name} fallback",
                "details": all_analyses
            }

        if fallback_analysis.get('money_request_candidate'):
            print(f"{fallback_provider_name} selected as tie-break money request: {summarize_money_request_info(fallback_analysis['payment_info'])}")
            return {
                "success": True,
                "text": fallback_analysis['text'],
                "payment_info": fallback_analysis['payment_info'],
                "source": f"{fallback_provider_name} fallback",
                "details": all_analyses
            }

        lone_primary_analysis, lone_primary_source = choose_lone_usable_primary_analysis(primary_analyses)
        if lone_primary_analysis:
            print(
                f"{lone_primary_analysis['provider']} selected as sole usable primary after "
                f"{fallback_provider_name} fallback failure: {summarize_receipt_info(lone_primary_analysis['payment_info'])}"
            )
            return {
                "success": True,
                "text": lone_primary_analysis['text'],
                "payment_info": lone_primary_analysis['payment_info'],
                "source": lone_primary_source,
                "details": all_analyses
            }

        fallback_error = fallback_analysis.get('error') or f"{fallback_provider_name} did not extract amount/confirmation"
        return {
            "success": False,
            "error": f"{primary_error}; {fallback_provider_name} fallback failed: {fallback_error}",
            "details": all_analyses
        }

    money_request_analysis, money_request_source = choose_money_request_analysis(primary_analyses)
    if money_request_analysis:
        print(f"Primary OCR selected money request: {money_request_source}")
        return {
            "success": True,
            "text": money_request_analysis['text'],
            "payment_info": money_request_analysis['payment_info'],
            "source": money_request_source,
            "details": primary_analyses
        }

    lone_primary_analysis, lone_primary_source = choose_lone_usable_primary_analysis(primary_analyses)
    if lone_primary_analysis:
        print(
            f"{lone_primary_analysis['provider']} selected as sole usable primary: "
            f"{summarize_receipt_info(lone_primary_analysis['payment_info'])}"
        )
        return {
            "success": True,
            "text": lone_primary_analysis['text'],
            "payment_info": lone_primary_analysis['payment_info'],
            "source": lone_primary_source,
            "details": primary_analyses
        }

    return {
        "success": False,
        "error": primary_error or "No OCR configured",
        "details": primary_analyses
    }

def search_yappy_payment(ocr_info):
    """
    Search for matching Yappy payment in database
    
    Strategy: 
    1. First search by CONFIRMATION CODE (5 letters) - this is unique
    2. Then verify other details match (amount, time)
    """
    if not ocr_info.get('confirmation'):
        return None
    
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        
        # 🔍 PRIMARY SEARCH: By confirmation code only (last 48 hours)
        # Search across last 48 hours to handle edge cases (late-night payments, timezone issues)
        c.execute('''SELECT * FROM yappy_payments 
                     WHERE confirmation_letters = ? AND verified = 0
                     AND datetime(received_at) > datetime('now', '-48 hours')
                     ORDER BY received_at DESC LIMIT 10''',
                  (ocr_info['confirmation'],))
        
        results = c.fetchall()
        
        if not results:
            # Fuzzy retry: OCR often confuses I↔L in confirmation codes.
            # Generate variants by swapping each I↔L and retry once.
            conf = ocr_info['confirmation']
            if 'I' in conf or 'L' in conf:
                variants = set()
                for i, ch in enumerate(conf):
                    if ch == 'I':
                        variants.add(conf[:i] + 'L' + conf[i+1:])
                    elif ch == 'L':
                        variants.add(conf[:i] + 'I' + conf[i+1:])
                if variants:
                    placeholders = ','.join('?' for _ in variants)
                    c.execute(f'''SELECT * FROM yappy_payments
                                 WHERE confirmation_letters IN ({placeholders}) AND verified = 0
                                 AND datetime(received_at) > datetime('now', '-48 hours')
                                 ORDER BY received_at DESC LIMIT 10''',
                              tuple(variants))
                    results = c.fetchall()
                    if results:
                        corrected = results[0][5] if len(results[0]) > 5 else conf  # confirmation_letters col
                        print(f"🔀 I/L fuzzy match: OCR='{conf}' → DB='{corrected}'")

        if not results:
            # Check if it was already verified
            c.execute('''SELECT * FROM yappy_payments
                         WHERE confirmation_letters = ? AND verified = 1
                         AND datetime(received_at) > datetime('now', '-48 hours')
                         LIMIT 1''',
                      (ocr_info['confirmation'],))
            already_verified = c.fetchone()

            # Also check I/L variants for already-verified
            if not already_verified and ('I' in ocr_info['confirmation'] or 'L' in ocr_info['confirmation']):
                conf = ocr_info['confirmation']
                variants = set()
                for i, ch in enumerate(conf):
                    if ch == 'I':
                        variants.add(conf[:i] + 'L' + conf[i+1:])
                    elif ch == 'L':
                        variants.add(conf[:i] + 'I' + conf[i+1:])
                if variants:
                    placeholders = ','.join('?' for _ in variants)
                    c.execute(f'''SELECT * FROM yappy_payments
                                 WHERE confirmation_letters IN ({placeholders}) AND verified = 1
                                 AND datetime(received_at) > datetime('now', '-48 hours')
                                 LIMIT 1''', tuple(variants))
                    already_verified = c.fetchone()

            conn.close()

            if already_verified:
                print(f"✅ Payment {ocr_info['confirmation']} was already verified")
                return ("ALREADY_VERIFIED", already_verified)

            print(f"🔍 No payment found for confirmation: {ocr_info['confirmation']}")
            return None
        
        conn.close()
        
        print(f"🔍 Found {len(results)} payment(s) with confirmation {ocr_info['confirmation']}")
        
        # If only one result, verify it matches
        if len(results) == 1:
            match = results[0]
            db_amount = match[3]  # amount column
            
            # Verify amount matches (if we have OCR amount)
            if ocr_info.get('amount') and abs(db_amount - ocr_info['amount']) > 0.01:
                print(f"⚠️ Amount mismatch: OCR=${ocr_info['amount']}, DB=${db_amount}")
                # Still return the match but log the discrepancy
            
            return match
        
        # Multiple results with same confirmation (unlikely but possible).
        # If OCR extracted a time, require an exact HH:MM match before choosing.
        if ocr_info.get('time'):
            exact_time_matches = [row for row in results if times_match_exact(ocr_info['time'], row[2])]
            if not exact_time_matches:
                print(
                    f"⚠️ Multiple payments share confirmation {ocr_info['confirmation']}, "
                    f"but none matched exact time {ocr_info['time']}"
                )
                return None
            results = exact_time_matches

        # If several rows still remain, prefer an exact amount match; otherwise keep the newest row.
        if ocr_info.get('amount') is not None:
            exact_amount_matches = [row for row in results if abs(row[3] - ocr_info['amount']) < 0.01]
            if exact_amount_matches:
                return exact_amount_matches[0]

        return results[0]
    
    except Exception as e:
        print(f"❌ Search error: {e}")
        return None

def time_difference_minutes(time1_str, time2_str):
    """Calculate difference in minutes between two HH:MM times"""
    try:
        h1, m1 = map(int, time1_str.split(':'))
        h2, m2 = map(int, time2_str.split(':'))
        
        total1 = h1 * 60 + m1
        total2 = h2 * 60 + m2
        
        return abs(total1 - total2)
    except (TypeError, ValueError) as e:
        print(f"⚠️ time_difference_minutes parse error: {time1_str!r} vs {time2_str!r} -> {e}")
        return 999  # Return large number on error


def times_match_exact(time1_str, time2_str):
    """True when two HH:MM values represent the exact same minute."""
    try:
        h1, m1 = map(int, str(time1_str).strip().split(':'))
        h2, m2 = map(int, str(time2_str).strip().split(':'))
        return h1 == h2 and m1 == m2
    except (AttributeError, TypeError, ValueError) as e:
        print(f"⚠️ times_match_exact parse error: {time1_str!r} vs {time2_str!r} -> {e}")
        return False


def mark_payment_verified(payment_id, user_id=None):
    """Mark payment as verified in database and react to message"""
    try:
        conn = get_yappy_db()
        c = conn.cursor()

        # 1. Mark as verified
        c.execute("UPDATE yappy_payments SET verified = 1 WHERE id = ?", (payment_id,))

        conn.commit()
        conn.close()

        # 2. React directly or enqueue reaction request to source bot
        trigger_payment_reaction(payment_id, user_id=user_id)
                  
        return True
    except Exception as e:
        print(f"❌ Mark verified error: {e}")
        return False

def get_today_panama():
    return datetime.datetime.now(PANAMA_TZ).strftime("%Y-%m-%d")

def get_nacional_dates_string():
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    today = get_today_panama()
    c.execute("SELECT date_str FROM nacional_dates WHERE date_str >= ?", (today,))
    manual_dates = {row[0] for row in c.fetchall()}
    c.execute("SELECT date_str FROM nacional_exclusions WHERE date_str >= ?", (today,))
    excluded_dates = {row[0] for row in c.fetchall()}
    conn.close()

    base_date = datetime.datetime.strptime(today, "%Y-%m-%d").date()
    auto_dates = set()
    for i in range(0, 30):
        d = base_date + datetime.timedelta(days=i)
        if d.weekday() in (2, 6):  # 2 = Wednesday, 6 = Sunday
            auto_dates.add(d.strftime("%Y-%m-%d"))

    final_dates = (manual_dates | auto_dates) - excluded_dates
    return ",".join(sorted(final_dates))

def get_short_security_code(ticket_id):
    raw_str = f"{ticket_id}-{SECURITY_SALT}"
    hash_object = hashlib.sha256(raw_str.encode())
    return hash_object.hexdigest()[:5].upper()

def calculate_single_ticket(num, bet, w1, w2, w3, lottery_type=""):
    num = str(num)
    w1 = str(w1) if w1 is not None else ""
    w2 = str(w2) if w2 is not None else ""
    w3 = str(w3) if w3 is not None else ""
    win_4_12 = w1 + w2
    win_4_13 = w1 + w3
    win_4_23 = w2 + w3
    is_nacional = "Nacional" in str(lottery_type)
    total_win = 0
    breakdown = []

    if len(num) == 2:
        if is_nacional:
            if len(w1) >= 2 and num == w1[-2:]:
                win = bet * 14.00
                total_win += win
                breakdown.append(f"Chances (1er): $14.00 * {bet} = ${win:.2f}")
            if len(w2) >= 2 and num == w2[-2:]:
                win = bet * 3.00
                total_win += win
                breakdown.append(f"Chances (2do): $3.00 * {bet} = ${win:.2f}")
            if len(w3) >= 2 and num == w3[-2:]:
                win = bet * 2.00
                total_win += win
                breakdown.append(f"Chances (3er): $2.00 * {bet} = ${win:.2f}")
        else:
            if num == w1:
                win = bet * AWARDS['2_digit_1']
                total_win += win
                breakdown.append(f"1er Premio: ${AWARDS['2_digit_1']} * {bet} = ${win:.2f}")
            if num == w2:
                win = bet * AWARDS['2_digit_2']
                total_win += win
                breakdown.append(f"2do Premio: ${AWARDS['2_digit_2']} * {bet} = ${win:.2f}")
            if num == w3:
                win = bet * AWARDS['2_digit_3']
                total_win += win
                breakdown.append(f"3er Premio: ${AWARDS['2_digit_3']} * {bet} = ${win:.2f}")

    elif len(num) == 4:
        if is_nacional:
            # Stack across prizes (1ro/2do/3ro), but choose one best tier per prize.
            prize_hits = []

            if len(w1) == 4:
                if num == w1:
                    prize_hits.append(("1er Premio (Exacto)", 2000.00))
                elif num[:3] == w1[:3]:
                    prize_hits.append(("1er Premio (3 Primeras)", 50.00))
                elif num[-3:] == w1[-3:]:
                    prize_hits.append(("1er Premio (3 Ultimas)", 50.00))
                elif num[:2] == w1[:2]:
                    prize_hits.append(("1er Premio (2 Primeras)", 3.00))
                elif num[-2:] == w1[-2:]:
                    prize_hits.append(("1er Premio (2 Ultimas)", 3.00))
                elif num[-1] == w1[-1]:
                    prize_hits.append(("1er Premio (Ultima)", 1.00))

            if len(w2) == 4:
                if num == w2:
                    prize_hits.append(("2do Premio (Exacto)", 600.00))
                elif num[:3] == w2[:3]:
                    prize_hits.append(("2do Premio (3 Primeras)", 20.00))
                elif num[-3:] == w2[-3:]:
                    prize_hits.append(("2do Premio (3 Ultimas)", 20.00))
                elif num[-2:] == w2[-2:]:
                    prize_hits.append(("2do Premio (2 Ultimas)", 2.00))

            if len(w3) == 4:
                if num == w3:
                    prize_hits.append(("3er Premio (Exacto)", 300.00))
                elif num[:3] == w3[:3]:
                    prize_hits.append(("3er Premio (3 Primeras)", 10.00))
                elif num[-3:] == w3[-3:]:
                    prize_hits.append(("3er Premio (3 Ultimas)", 10.00))
                elif num[-2:] == w3[-2:]:
                    prize_hits.append(("3er Premio (2 Ultimas)", 1.00))

            for label, amount in prize_hits:
                win = bet * amount
                total_win += win
                breakdown.append(f"{label}: ${amount} * {bet} = ${win:.2f}")
        else:
            if num == win_4_12:
                win = bet * AWARDS['4_digit_12']
                total_win += win
                breakdown.append(f"Billete 1ro/2do: ${AWARDS['4_digit_12']} * {bet} = ${win:.2f}")
            if num == win_4_13:
                win = bet * AWARDS['4_digit_13']
                total_win += win
                breakdown.append(f"Billete 1ro/3ro: ${AWARDS['4_digit_13']} * {bet} = ${win:.2f}")
            if num == win_4_23:
                win = bet * AWARDS['4_digit_23']
                total_win += win
                breakdown.append(f"Billete 2do/3ro: ${AWARDS['4_digit_23']} * {bet} = ${win:.2f}")
            
    return total_win, breakdown

# --- COMANDOS ADMIN ---
@bot.message_handler(commands=['verificar'])
def check_specific_ticket(message):
    try:
        args = message.text.split()
        if len(args) < 3:
            bot.reply_to(message, "⚠️ Uso: /verificar [numero] [cantidad]")
            return
        user_num = args[1]
        user_qty = float(args[2])
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT * FROM draw_results ORDER BY rowid DESC LIMIT 1")
        last_result = c.fetchone()
        conn.close()
        if not last_result:
            bot.reply_to(message, "⚠️ No hay resultados guardados aún.")
            return
        r_date, r_type, w1, w2, w3 = last_result
        payout, breakdown = calculate_single_ticket(user_num, user_qty, w1, w2, w3, r_type)
        response = f"🔍 **VERIFICACIÓN RÁPIDA**\nSorteo: {r_type} ({r_date})\nGanadores: {w1} - {w2} - {w3}\nJugada: Num {user_num} x ${user_qty}\n----------------\n"
        if payout > 0:
            response += f"🎉 **GANASTE: ${payout:.2f}**\n\nDesglose:\n" + "\n".join(breakdown)
        else:
            response += "❌ No hubo suerte."
        bot.reply_to(message, response, parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['verificar_ticket'])
def check_ticket_by_id(message):
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "⚠️ Uso: /verificar_ticket [ticket_id]")
            return
        ticket_id = int(args[1])
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT date, lottery_type, numbers_json, status FROM tickets_v3 WHERE id = ?", (ticket_id,))
        ticket = c.fetchone()
        if not ticket:
            conn.close()
            bot.reply_to(message, f"⚠️ Ticket #{ticket_id} no encontrado.")
            return
        date, lottery_type, numbers_json, status = ticket
        if status in ('DELETED', 'INVALID'):
            conn.close()
            bot.reply_to(message, f"⚠️ Ticket #{ticket_id} está {status}.")
            return
        c.execute("SELECT w1, w2, w3 FROM draw_results WHERE date = ? AND lottery_type = ?", (date, lottery_type))
        result = c.fetchone()
        conn.close()
        if not result:
            bot.reply_to(message, f"⚠️ No hay resultados para {lottery_type} del {date}.")
            return
        w1, w2, w3 = result
        items = json.loads(numbers_json)
        total_win = 0
        all_lines = []
        for item in items:
            num = str(item['num'])
            bet = float(item['qty'])
            win, lines = calculate_single_ticket(num, bet, w1, w2, w3, lottery_type)
            if win > 0:
                total_win += win
                for line in lines:
                    all_lines.append(f"  • [{num}] {line}")
        response = f"🔍 **Ticket #{ticket_id}**\nSorteo: {lottery_type} ({date})\nGanadores: {w1} - {w2} - {w3}\n----------------\n"
        if total_win > 0:
            response += f"🎉 **GANÓ: ${total_win:.2f}**\n\nDesglose:\n" + "\n".join(all_lines)
        else:
            response += "❌ No ganó nada."
        bot.reply_to(message, response, parse_mode="Markdown")
    except ValueError:
        bot.reply_to(message, "⚠️ El ID del ticket debe ser un número.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['nacional'])
def add_nacional_date(message):
    if not is_admin_chat(message): return
    try:
        args = message.text.split()
        if len(args) != 2:
            bot.reply_to(message, "⚠️ Uso: /nacional YYYY-MM-DD")
            return
        date_str = args[1]
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO nacional_dates (date_str) VALUES (?)", (date_str,))
        c.execute("DELETE FROM nacional_exclusions WHERE date_str = ?", (date_str,))
        conn.commit()
        conn.close()
        bot.reply_to(message, f"✅ Sorteo Nacional activado para: {date_str}")
    except ValueError:
        bot.reply_to(message, "⚠️ Fecha incorrecta. Usa YYYY-MM-DD")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['nacional_disable'])
def remove_nacional_date(message):
    if not is_admin_chat(message): return
    try:
        args = message.text.split()
        if len(args) != 2:
            bot.reply_to(message, "⚠️ Uso: /nacional_disable YYYY-MM-DD")
            return
        date_str = args[1]
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO nacional_exclusions (date_str) VALUES (?)", (date_str,))
        c.execute("DELETE FROM nacional_dates WHERE date_str = ?", (date_str,))
        conn.commit()
        conn.close()
        bot.reply_to(message, f"🚫 Sorteo Nacional desactivado para: {date_str}")
    except ValueError:
        bot.reply_to(message, "⚠️ Fecha incorrecta. Usa YYYY-MM-DD")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
# --- YAPPY DEBUG COMMANDS ---

@bot.message_handler(commands=['yappy_status'])
def yappy_status(message):
    """Check stored Yappy payments in database"""
    if not is_admin_chat(message): return
    
    try:
        conn = get_yappy_db()
        c = conn.cursor()
        
        today = get_today_panama()
        c.execute("SELECT COUNT(*) FROM yappy_payments WHERE date = ?", (today,))
        count_today = c.fetchone()[0]
        
        c.execute("SELECT COUNT(*) FROM yappy_payments")
        count_total = c.fetchone()[0]
        
        c.execute("""SELECT confirmation_letters, amount, message_time, sender_name, account_tag, verified 
                     FROM yappy_payments ORDER BY received_at DESC LIMIT 5""")
        recent = c.fetchall()
        conn.close()
        
        msg = f"📊 Yappy Cache Status\n📁 File: yappy_cache.db\n\n"
        msg += f"📅 Today ({today}): {count_today}\n"
        msg += f"📁 Total: {count_total}\n\n"
        
        if recent:
            msg += "Últimos 5 pagos:\n"
            for r in recent:
                verified = "✅" if r[5] else "⏳"
                msg += f"{verified} {r[0]} | ${r[1]} | {r[2]} | {r[4]}\n"
        else:
            msg += "❌ No hay pagos almacenados"
        
        bot.reply_to(message, msg)
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

@bot.message_handler(commands=['yappy_test'])
def yappy_test_insert(message):
    """Manually insert a test payment: /yappy_test XXXXX 15.00 14:30"""
    if not is_admin_chat(message): return
    
    try:
        args = message.text.split()
        if len(args) < 4:
            bot.reply_to(message, "⚠️ Uso: /yappy_test CODIGO MONTO HORA\nEj: /yappy_test EHTDV 15.00 1:06")
            return
        
        conf = args[1].upper()
        amount = float(args[2])
        time_str = args[3]
        
        conn = get_yappy_db()
        c = conn.cursor()
        
        today = get_today_panama()
        c.execute('''INSERT INTO yappy_payments 
                     (date, message_time, amount, sender_name, confirmation_letters, account_tag)
                     VALUES (?, ?, ?, ?, ?, ?)''',
                  (today, time_str, amount, "Test User", conf, "#JV"))
        
        conn.commit()
        conn.close()
        
        bot.reply_to(message, f"✅ Test payment inserted:\n🔐 {conf}\n💰 ${amount}\n🕐 {time_str}")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")

# --- SUPPORT REPLY HANDLER (Must be before debug handler) ---
@bot.message_handler(func=lambda m: is_admin_chat(m) and m.message_thread_id, content_types=['text', 'photo', 'voice', 'sticker', 'video', 'document'])
def reply_to_user_topic(message):
    """
    Handle admin replies inside a topic. 
    It sends the message BACK to the user associated with this topic.
    Supports Contextual Replies: If admin replies to a mirrored message, bot replies to the original user message.
    """
    # Ignore commands or if it's the specific "General" topic (thread_id=None usually, but distinct in forums)
    if message.text and message.text.startswith('/'): return
    
    try:
        thread_id = message.message_thread_id
        
        # Find user attached to this topic
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT user_id FROM support_threads WHERE thread_id = ?", (thread_id,))
        row = c.fetchone()
        
        if not row:
            conn.close()
            return
            
        user_id = row[0]
        
        # Check for Reply Context
        reply_to_id = None
        if message.reply_to_message:
            # Check if this admin message is a reply to a mirrored message
            c.execute("SELECT user_msg_id FROM message_map WHERE admin_msg_id = ?", (message.reply_to_message.message_id,))
            map_row = c.fetchone()
            if map_row:
                reply_to_id = map_row[0]
                
        conn.close()
        
        # Copy the message back to user (Copy preserves formatting, photos, etc.)
        bot.copy_message(user_id, message.chat.id, message.message_id, reply_to_message_id=reply_to_id)
        
        # React to confirm sent
        try:
             bot.set_message_reaction(message.chat.id, message.message_id, 
                                   [telebot.types.ReactionTypeEmoji('👍')])
        except:
             pass

    except Exception as e:
        print(f"Error replying to user from topic: {e}")

# --- YAPPY PAYMENT HANDLERS ---

@bot.channel_post_handler(
    func=lambda message: bool(
        YAPPY_DIRECT_INGEST_ENABLED
        and YAPPY_PAYMENTS_GROUP_ID
        and str(message.chat.id) == str(YAPPY_PAYMENTS_GROUP_ID)
    )
)
def ingest_yappy_channel_posts(message):
    """Ingest Yappy feed from configured channel only."""
    if not message.text:
        return

    has_emoji = '🧧' in message.text
    print(f"[YAPPY FEED] {message.chat.id}/{message.message_id}: {message.text[:70]}... | has_emoji={has_emoji}")

    if not has_emoji:
        return

    parsed = parse_yappy_message(message.text)
    if parsed:
        stored = False
        for _store_attempt in range(3):
            try:
                if store_yappy_payment(parsed, message.message_id, message.chat.id):
                    stored = True
                    break
            except Exception as _store_exc:
                print(f"WARN store_yappy_payment attempt {_store_attempt + 1}/3: {_store_exc}")
            time.sleep(0.5 * (_store_attempt + 1))

        if stored:
            account_name = YAPPY_ACCOUNTS.get(parsed['account_tag'], 'Unknown')
            print(f"Stored payment: ${parsed['amount']} -> {account_name} ({parsed['confirmation']})")
        else:
            print(f"ERROR PAYMENT LOSS RISK: failed to store after 3 retries: {message.text[:120]}")
    else:
        print(f"WARN Could not parse message: {message.text[:100]}")

    if message.message_id % 100 == 0:
        cleanup_old_payments()

@bot.message_handler(content_types=['photo'])
def handle_photo_verification(message):
    """Auto-verify Yappy payment screenshots sent by users (QUEUE BASED)"""
    try:
        # Ignore photos from admin group or yappy payments group
        if message.chat.id == ADMIN_GROUP_ID or (YAPPY_PAYMENTS_GROUP_ID and message.chat.id == YAPPY_PAYMENTS_GROUP_ID):
            return
            
        # 🟢 MIRROR TO TOPIC (Parallel processing)
        mirror_to_topic(message.chat.id, message)
        
        # Download photo with retry
        downloaded_file = None
        for attempt in range(3):
            try:
                file_info = bot.get_file(message.photo[-1].file_id)
                downloaded_file = bot.download_file(file_info.file_path)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"❌ Download failed after retries: {e}")
                    bot.reply_to(message, "Error descargando imagen. Telegram ocupado.")
                    return
                time.sleep(1)
        
        # Save temporarily (Use message_id to prevent concurrency collisions)
        temp_path = os.path.join(BASE_DIR, f"temp_yappy_{message.chat.id}_{message.message_id}.jpg")
        with open(temp_path, 'wb') as f:
            f.write(downloaded_file)
        image_hash = hashlib.sha256(downloaded_file).hexdigest()
             
        # 🚀 ADD TO QUEUE
        OCR_QUEUE.put({'message': message, 'temp_path': temp_path, 'image_hash': image_hash})
        
        # Optional: Feedback if queue is busy
        q_size = OCR_QUEUE.qsize()
        if q_size > 2:
             print(f"📥 Queued image (Position: {q_size})")

    except Exception as e:
        print(f"❌ Error queuing photo: {e}")

# --- CHANNEL TEXT REPLY HANDLER (Correct confirmation code by replying to blue message) ---
@bot.channel_post_handler(
    func=lambda message: bool(
        RECEIPT_FORWARD_CHANNEL_ID
        and str(message.chat.id) == str(RECEIPT_FORWARD_CHANNEL_ID)
        and message.text
        and message.reply_to_message
    ),
    content_types=['text']
)
def handle_channel_confirmation_correction(message):
    """Allow correcting a confirmation code by replying to the blue verificando message with 5 letters."""
    try:
        confirmation, confirmation_full = normalize_manual_confirmation(message.text)
        if not confirmation:
            return  # Not a valid confirmation code, ignore silently

        replied_msg_id = message.reply_to_message.message_id
        chat_id = message.chat.id

        # Look up pending verification whose blue reply matches the message being replied to
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute('''SELECT id, user_id, chat_id, message_id, confirmation_letters, amount,
                            receipt_time, followup_id, reply_message_id
                     FROM pending_verifications
                     WHERE reply_message_id = ? AND chat_id = ?''',
                  (replied_msg_id, chat_id))
        row = c.fetchone()
        conn.close()

        if not row:
            return  # Replied message is not a pending blue verification

        pending_id, user_id, p_chat_id, orig_msg_id, old_confirmation, amount, receipt_time, followup_id, reply_msg_id = row
        print(f"[CHANNEL CORRECTION] Pending {pending_id}: {old_confirmation} → {confirmation}")

        # Update pending with corrected confirmation code
        conn = get_yappy_db()
        c = conn.cursor()
        c.execute("UPDATE pending_verifications SET confirmation_letters = ? WHERE id = ?",
                  (confirmation, pending_id))
        conn.commit()
        conn.close()

        # Try to match a payment with the corrected code
        payment_info = {
            'confirmation': confirmation,
            'amount': amount,
            'time': receipt_time,
        }
        match = search_yappy_payment(payment_info)

        if match and not (isinstance(match, tuple) and match[0] == "ALREADY_VERIFIED"):
            # Payment found — verify it
            payment_id = match[0]
            db_time = match[2]
            db_amount = match[3]
            db_sender = match[4]
            account_tag = match[7]
            account_name = YAPPY_ACCOUNTS.get(account_tag, account_tag)

            if not mark_payment_verified(payment_id, user_id=user_id or None):
                bot.reply_to(message, "❌ No pude finalizar la verificación.")
                return

            wallet_summary = ""
            if user_id and not is_admin_user(user_id):
                wallet_summary = process_wallet_deposit(user_id, db_amount)
                wallet_summary = sanitize_wallet_summary_for_ocr(wallet_summary)

            success_msg = (
                f"✅ **Pago Verificado**\n\n"
                f"Monto: ${db_amount:.2f}\n"
                f"Confirmación: {format_confirmation_display(confirmation=confirmation)}\n"
                f"De: {db_sender}\n"
                f"Hora: {db_time}"
            )
            if wallet_summary:
                success_msg += f"\n{wallet_summary}"

            try:
                bot.edit_message_text(
                    success_msg + "\n_(Editado por el bot)_",
                    chat_id,
                    replied_msg_id,
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"⚠️ Failed to edit blue message after correction: {e}")
                bot.reply_to(message, success_msg, parse_mode="Markdown")

            # Remove from pending
            conn = get_yappy_db()
            c = conn.cursor()
            c.execute("DELETE FROM pending_verifications WHERE id = ?", (pending_id,))
            conn.commit()
            conn.close()

            if followup_id:
                complete_receipt_followup(followup_id, status="PROCESSED")

        else:
            # No payment match yet — edit blue message with corrected code, remove #问题
            conf_display = format_confirmation_display(confirmation=confirmation, confirmation_full=confirmation_full)
            # Try to preserve sender name from original blue message
            sender_line = ""
            orig_text = (message.reply_to_message.text or "") if message.reply_to_message else ""
            for line in orig_text.split("\n"):
                if line.startswith("Enviado por:"):
                    sender_line = line
                    break

            updated_msg = (
                f"🔵 **Verificando pago...**\n"
                f"Monto: ${amount:.2f}\n"
                f"Confirmación: {conf_display}"
            )
            if sender_line:
                updated_msg += f"\n{sender_line}"
            updated_msg += "\n_(Código corregido)_"

            try:
                bot.edit_message_text(
                    updated_msg,
                    chat_id,
                    replied_msg_id,
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"⚠️ Failed to edit blue message for correction: {e}")

        # Delete the admin's correction reply to keep channel clean
        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception:
            pass

    except Exception as e:
        print(f"❌ Channel confirmation correction error: {e}")

# --- CHANNEL PHOTO HANDLER (Forwarded receipts from another bot) ---
@bot.channel_post_handler(
    func=lambda message: bool(
        RECEIPT_FORWARD_CHANNEL_ID
        and str(message.chat.id) == str(RECEIPT_FORWARD_CHANNEL_ID)
    ),
    content_types=['photo']
)
def handle_channel_photo(message):
    """Process receipt images forwarded to the channel by another bot."""
    try:
        print(f"[CHANNEL PHOTO] {message.chat.id}/{message.message_id}")

        # Download photo with retry
        downloaded_file = None
        for attempt in range(3):
            try:
                file_info = bot.get_file(message.photo[-1].file_id)
                downloaded_file = bot.download_file(file_info.file_path)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"❌ Channel photo download failed after retries: {e}")
                    bot.reply_to(message, "Error descargando imagen.")
                    return
                time.sleep(1)

        temp_path = os.path.join(BASE_DIR, f"temp_channel_{message.chat.id}_{message.message_id}.jpg")
        with open(temp_path, 'wb') as f:
            f.write(downloaded_file)
        image_hash = hashlib.sha256(downloaded_file).hexdigest()

        OCR_QUEUE.put({
            'message': message,
            'temp_path': temp_path,
            'image_hash': image_hash,
            'channel_mode': True,
        })

        q_size = OCR_QUEUE.qsize()
        if q_size > 2:
            print(f"📥 Channel image queued (Position: {q_size})")

    except Exception as e:
        print(f"❌ Error queuing channel photo: {e}")

def worker_ocr():
    """Background worker to process OCR tasks sequentially"""
    print("🚀 OCR Worker Thread Started")
    while True:
        try:
            task = OCR_QUEUE.get()
            if task is None: break
            
            msg = task['message']
            path = task['temp_path']
            image_hash = task.get('image_hash')
            channel_mode = task.get('channel_mode', False)

            try:
                if channel_mode:
                    process_channel_ocr_task(msg, path, image_hash=image_hash)
                else:
                    process_ocr_task(msg, path, image_hash=image_hash)
            except Exception as e:
                print(f"❌ Worker Error processing task: {e}")
                
            OCR_QUEUE.task_done()
        except Exception as e:
            print(f"❌ Worker Loop Error: {e}")

def process_ocr_task(message, temp_path, image_hash=None):
    """Actual OCR Logic (Runs in Worker Thread)"""
    hash_reserved = False
    keep_receipt_hash = False
    try:
        user_id = message.from_user.id
        user_name = format_topic_user_name(
            user_id,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name
        )

        # Reserve the image hash BEFORE expensive OCR calls. If OCR/parsing fails,
        # the reservation is released in finally so the same screenshot can be retried.
        is_duplicate_image, first_seen_at, hash_reserved, first_sender_name, first_receipt_kind, first_confirmation = register_receipt_image_hash(
            image_hash=image_hash,
            user_id=user_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            user_name=user_name
        )
        if is_duplicate_image:
            try: os.remove(temp_path)
            except: pass
            markup = get_menu_markup(user_id)
            notice = build_duplicate_receipt_notice(
                first_seen_at,
                first_sender_name,
                receipt_kind=first_receipt_kind,
                confirmation_letters=first_confirmation
            )
            if is_admin_user(user_id):
                notice += "\n#问题"
            bot.reply_to(
                message,
                notice,
                parse_mode="Markdown",
                reply_markup=markup
            )
            return

        ocr_selection = run_receipt_ocr(temp_path)

        # Delete temp file
        try:
            os.remove(temp_path)
        except: pass

        if not ocr_selection.get('success'):
            error_msg = ocr_selection.get('error', 'Unknown error')
            followup_id = send_receipt_followup_reply(
                message,
                f"No pude leer la imagen (Error: {error_msg}).",
                scenario="OCR_FAILED",
                image_hash=image_hash
            )
            keep_receipt_hash = bool(followup_id)
            return

        ocr_text = ocr_selection['text']
        payment_info = ocr_selection.get('payment_info') or parse_yappy_screenshot(ocr_text)
        set_receipt_image_confirmation(image_hash, payment_info.get('confirmation'))

        print(f"OCR source selected: {ocr_selection.get('source', 'unknown')}")
        print(f"📝 OCR Text: {ocr_text[:200]}...")

        # Check for money REQUEST (not a sent payment)
        if payment_info.get('is_money_request'):
            keep_receipt_hash = True
            set_receipt_image_kind(image_hash, "MONEY_REQUEST")
            if is_admin_user(user_id):
                warning_msg = build_money_request_warning(payment_info, sender_name=user_name)
                send_money_request_alert(message, warning_msg)
                bot.reply_to(message, warning_msg)
            else:
                bot.reply_to(message, "Está pidiendo un yappy, y no enviando un yappy.")
            return

        if not has_required_receipt_fields(payment_info):
            followup_id = send_receipt_followup_reply(
                message,
                "No pude detectar toda la información (Monto/Confirmación/Fecha).",
                scenario="OCR_INCOMPLETE",
                parse_mode="Markdown",
                amount=payment_info.get('amount'),
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full'),
                receipt_time=payment_info.get('time'),
                image_hash=image_hash
            )
            keep_receipt_hash = bool(followup_id)
            return

        if not is_receipt_from_today_panama(payment_info):
            keep_receipt_hash = True
            bot.reply_to(message, build_stale_receipt_message(payment_info))
            return
        
        # 🟢 UX IMPROVEMENT: Single Message Logic
        # We DO NOT send "Datos Leídos" anymore (too noisy)
        
        # Search for payment first
        match = search_yappy_payment(payment_info)
        
        # Handle already verified case
        if isinstance(match, tuple) and match[0] == "ALREADY_VERIFIED":
            keep_receipt_hash = True
            verified_payment = match[1]
            conf_display = format_confirmation_display(
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full')
            )
            verified_at = verified_payment[10] if len(verified_payment) > 10 else None
            verified_at_text = verified_at if verified_at else "desconocida"
            
            # Get Menu Markup
            markup = get_menu_markup(user_id)

            ya_procesado_msg = (
                f"🟡 **Comprobante ya procesado**\n"
                f"Pago: {conf_display}\n"
                f"Monto: ${verified_payment[3]:.2f}\n"
                f"Recibido: {verified_at_text}"
            )
            if is_admin_user(user_id):
                ya_procesado_msg += "\n#问题"
            bot.reply_to(message,
                        ya_procesado_msg,
                        parse_mode="Markdown",
                        reply_markup=markup)
            return

        if not match:
            # Add to pending verification list
            pending_id = add_pending_verification(
                user_id,
                message.chat.id,
                message.message_id,
                payment_info['confirmation'],
                payment_info['amount'],
                payment_info.get('time')
            )
            if not pending_id:
                bot.reply_to(message, "No pude guardar la verificación pendiente. Inténtalo de nuevo.")
                return
            keep_receipt_hash = True

            # Pending Message
            conf_display = format_confirmation_display(
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full')
            )
            blue_text = (
                f"🔵 **Verificando pago...**\n"
                f"Monto: ${payment_info['amount']:.2f}\n"
                f"Confirmación: {conf_display}"
            )
            if is_admin_user(user_id):
                blue_text += "\n#问题"
            send_receipt_followup_reply(
                message,
                blue_text,
                scenario="PENDING_PAYMENT",
                parse_mode="Markdown",
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full'),
                amount=payment_info.get('amount'),
                receipt_time=payment_info.get('time'),
                pending_verification_id=pending_id,
                image_hash=image_hash
            )
            return
        
        # Match found! Extract details
        payment_id = match[0]
        db_time = match[2]
        db_amount = match[3]
        db_sender = match[4]
        account_tag = match[7]
        account_name = YAPPY_ACCOUNTS.get(account_tag, account_tag)
        
        # Mark as verified
        if not mark_payment_verified(payment_id, user_id=user_id):
            bot.reply_to(message, "No pude finalizar la verificación del pago. Inténtalo de nuevo.")
            return
        keep_receipt_hash = True

        # 💰 PROCESS WALLET DEPOSIT — skip for admin users (no fondo needed)
        if is_admin_user(user_id):
            wallet_summary = ""
        else:
            wallet_summary = process_wallet_deposit(user_id, db_amount)
            wallet_summary = sanitize_wallet_summary_for_ocr(wallet_summary)
        
        # Send success message (SINGLE MESSAGE)
        success_msg = (
            f"✅ **Pago Verificado**\n"
            f"Monto: ${db_amount:.2f}\n"
            f"Hora: {db_time}\n"
            f"De: {db_sender}\n"
            f"Cuenta: {account_name}"
        )
        
        if wallet_summary:
            success_msg += "\n" + wallet_summary
        
        # Get Menu Markup
        markup = get_menu_markup(user_id)

        # Use reply_to instead of edit_message_text since we didn't send an initial message
        sent_msg = bot.reply_to(message, success_msg, parse_mode="Markdown", reply_markup=markup)
        mirror_to_topic(message.chat.id, sent_msg, user_name=user_name)
        
    except Exception as e:
        print(f"❌ Worker Task Error: {e}")
        # try: bot.reply_to(message, "❌ Error procesando pago.")
        # except: pass
    finally:
        if hash_reserved and not keep_receipt_hash:
            release_receipt_image_hash(image_hash)
        # Always clean up temp file
        try:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def process_channel_ocr_task(message, temp_path, image_hash=None):
    """OCR processing for receipt images forwarded to the channel by another bot."""
    hash_reserved = False
    keep_receipt_hash = False
    try:
        # Extract sender info from forwarded message (if available)
        if getattr(message, 'forward_from', None):
            sender_user_id = message.forward_from.id
            sender_name = format_topic_user_name(
                sender_user_id,
                first_name=message.forward_from.first_name,
                last_name=getattr(message.forward_from, 'last_name', None)
            )
        elif getattr(message, 'forward_sender_name', None):
            sender_user_id = 0
            sender_name = message.forward_sender_name
        else:
            sender_user_id = 0
            sender_name = "Canal"

        # Duplicate image check
        is_duplicate_image, first_seen_at, hash_reserved, first_sender_name, first_receipt_kind, first_confirmation = register_receipt_image_hash(
            image_hash=image_hash,
            user_id=sender_user_id,
            chat_id=message.chat.id,
            message_id=message.message_id,
            user_name=sender_name
        )
        if is_duplicate_image:
            try: os.remove(temp_path)
            except: pass
            notice = build_duplicate_receipt_notice(
                first_seen_at,
                first_sender_name,
                receipt_kind=first_receipt_kind,
                confirmation_letters=first_confirmation
            ) + "\n#问题"
            bot.reply_to(
                message,
                notice,
                parse_mode="Markdown"
            )
            return

        ocr_selection = run_receipt_ocr(temp_path)

        try: os.remove(temp_path)
        except: pass

        if not ocr_selection.get('success'):
            error_msg = ocr_selection.get('error', 'Unknown error')
            bot.reply_to(message, f"❌ No pude leer la imagen (Error: {error_msg}).")
            return

        ocr_text = ocr_selection['text']
        payment_info = ocr_selection.get('payment_info') or parse_yappy_screenshot(ocr_text)
        set_receipt_image_confirmation(image_hash, payment_info.get('confirmation'))

        print(f"[CHANNEL OCR] source: {ocr_selection.get('source', 'unknown')}")
        print(f"[CHANNEL OCR] text: {ocr_text[:200]}...")

        # Check for money REQUEST (not a sent payment)
        if payment_info.get('is_money_request'):
            keep_receipt_hash = True
            set_receipt_image_kind(image_hash, "MONEY_REQUEST")
            warning_msg = build_money_request_warning(payment_info, sender_name=sender_name)
            if sender_user_id and is_admin_user(sender_user_id):
                send_money_request_alert(message, warning_msg)
                bot.reply_to(message, warning_msg)
            else:
                send_money_request_alert(message, warning_msg)
                bot.reply_to(message, "Está pidiendo un yappy, y no enviando un yappy.")
            return

        if not has_required_receipt_fields(payment_info):
            conf_display = format_confirmation_display(
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full')
            )
            amount_text = f"${payment_info['amount']:.2f}" if payment_info.get('amount') else "?"
            bot.reply_to(
                message,
                f"⚠️ **Información incompleta**\n"
                f"Monto: {amount_text}\n"
                f"Confirmación: {conf_display}\n"
                f"Fecha: {format_receipt_date_display(payment_info.get('receipt_date'))}\n"
                f"Un admin puede reenviar esta imagen al bot por DM para corrección manual.",
                parse_mode="Markdown"
            )
            keep_receipt_hash = True
            return

        if not is_receipt_from_today_panama(payment_info):
            keep_receipt_hash = True
            bot.reply_to(message, build_stale_receipt_message(payment_info))
            return

        # Search for payment
        match = search_yappy_payment(payment_info)

        # Already verified
        if isinstance(match, tuple) and match[0] == "ALREADY_VERIFIED":
            keep_receipt_hash = True
            verified_payment = match[1]
            conf_display = format_confirmation_display(
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full')
            )
            verified_at = verified_payment[10] if len(verified_payment) > 10 else None
            bot.reply_to(
                message,
                f"🟡 **Comprobante ya procesado**\n"
                f"Pago: {conf_display}\n"
                f"Monto: ${verified_payment[3]:.2f}\n"
                f"Recibido: {verified_at or 'desconocida'}\n"
                f"#问题",
                parse_mode="Markdown"
            )
            return

        # Not found — add to pending so it auto-matches when the payment arrives
        if not match:
            conf_display = format_confirmation_display(
                confirmation=payment_info.get('confirmation'),
                confirmation_full=payment_info.get('confirmation_full')
            )
            # Send blue reply FIRST so we can capture its message_id for later editing
            blue_reply = bot.reply_to(
                message,
                f"🔵 **Verificando pago...**\n"
                f"Monto: ${payment_info['amount']:.2f}\n"
                f"Confirmación: {conf_display}\n"
                f"Enviado por: {sender_name}\n"
                f"#问题",
                parse_mode="Markdown"
            )
            blue_reply_id = blue_reply.message_id if blue_reply else None
            pending_id = add_pending_verification(
                sender_user_id,
                message.chat.id,
                message.message_id,
                payment_info['confirmation'],
                payment_info['amount'],
                payment_info.get('time'),
                reply_message_id=blue_reply_id
            )
            if not pending_id:
                if blue_reply_id:
                    try:
                        bot.edit_message_text(
                            "❌ No pude guardar la verificación pendiente.",
                            message.chat.id,
                            blue_reply_id
                        )
                    except Exception as e:
                        print(f"⚠️ Failed to replace blue reply after pending save error: {e}")
                        bot.reply_to(message, "❌ No pude guardar la verificación pendiente.")
                else:
                    bot.reply_to(message, "❌ No pude guardar la verificación pendiente.")
                return
            keep_receipt_hash = True
            return

        # Match found — verify it
        payment_id = match[0]
        db_time = match[2]
        db_amount = match[3]
        db_sender = match[4]
        account_tag = match[7]
        account_name = YAPPY_ACCOUNTS.get(account_tag, account_tag)

        if not mark_payment_verified(payment_id, user_id=sender_user_id or None):
            bot.reply_to(message, "❌ No pude finalizar la verificación del pago.")
            return
        keep_receipt_hash = True

        # Wallet deposit only if we know the real sender
        wallet_summary = ""
        if sender_user_id and not is_admin_user(sender_user_id):
            wallet_summary = process_wallet_deposit(sender_user_id, db_amount)
            wallet_summary = sanitize_wallet_summary_for_ocr(wallet_summary)

        confirmation = payment_info.get('confirmation', '?')
        sender_short = sender_name[:3] if sender_name else "?"
        sender_compact = db_sender.replace(" ", "")
        success_msg = f"✅  {db_time} ${db_amount:.2f} {sender_compact} {account_tag} #{sender_short} #{confirmation}"
        if wallet_summary:
            success_msg += "\n" + wallet_summary

        bot.reply_to(message, success_msg)

    except Exception as e:
        print(f"❌ Channel OCR Task Error: {e}")
    finally:
        if hash_reserved and not keep_receipt_hash:
            release_receipt_image_hash(image_hash)
        try:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass

def set_lista_waiting_state(user_id):
    with LISTA_WAITING_LOCK:
        LISTA_WAITING_USERS[int(user_id)] = time.time() + (LISTA_DRAFT_TTL_MINUTES * 60)


def clear_lista_waiting_state(user_id):
    with LISTA_WAITING_LOCK:
        LISTA_WAITING_USERS.pop(int(user_id), None)


def is_lista_waiting(user_id):
    with LISTA_WAITING_LOCK:
        expires_at = LISTA_WAITING_USERS.get(int(user_id))
        if not expires_at:
            return False
        if expires_at <= time.time():
            LISTA_WAITING_USERS.pop(int(user_id), None)
            return False
        return True


def get_lista_done_markup(label):
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(label, callback_data="noop"))
    return markup


def build_lista_edit_webapp_url(user_id, draft):
    items = json.loads(draft.get('items_json') or '[]')
    params = {
        "v": BOT_VERSION,
        "mode": "draft_edit",
        "uid": str(user_id),
        "bal": f"{get_wallet_balance(user_id):.2f}",
        "nacional_dates": get_nacional_dates_string(),
        "draft_id": str(draft.get('id')),
        "draft_date": str(draft.get('ticket_date') or ''),
        "draft_lottery": str(draft.get('lottery_type') or ''),
        "draft_items": json.dumps(items, ensure_ascii=False, separators=(',', ':'))
    }
    return f"{WEBAPP_BASE_URL}index.html?{urllib.parse.urlencode(params)}"


def get_lista_preview_markup(user_id, draft):
    draft_id = draft['id']
    edit_url = build_lista_edit_webapp_url(user_id, draft)
    markup = InlineKeyboardMarkup()
    markup.row(
        InlineKeyboardButton("Confirmar", callback_data=f"lista_confirm_{draft_id}"),
        InlineKeyboardButton("Editar", web_app=WebAppInfo(url=edit_url)),
        InlineKeyboardButton("Cancelar", callback_data=f"lista_cancel_{draft_id}")
    )
    return markup


def build_lista_preview_text(draft):
    items = json.loads(draft.get('items_json') or '[]')
    lines = [
        f"Sorteo: {draft.get('lottery_type') or 'Pendiente'}",
        f"Fecha: {draft.get('ticket_date') or 'Pendiente'}",
        "",
        "Jugadas:"
    ]
    chance_count = 0
    billete_count = 0

    for item in items:
        if item.get('separator') or str(item.get('num', '')).strip() == '---':
            lines.append("-----")
            continue
        num = str(item.get('num'))
        qty = int(item.get('qty', 0))
        if len(num) == 2:
            chance_count += qty
        elif len(num) == 4:
            billete_count += qty
        line_total = calculate_ticket_line_total(num, qty)
        lines.append(f"{num} x {qty} = ${line_total:.2f}")

    lines.extend([
        "",
        f"Chance count: {chance_count}",
        f"Billetes count: {billete_count}",
        f"Total: ${float(draft.get('server_total') or 0.0):.2f}"
    ])
    return "\n".join(lines)


def send_app_ticket_ack(message, request_id, ok, ticket_id=None, ticket_date=None, lottery_type=None, code=None, reason=None):
    safe_request_id = str(request_id or "unknown").strip() or "unknown"
    payload = {"request_id": safe_request_id}
    if ok:
        if ticket_id is not None:
            payload["ticket_id"] = ticket_id
        if ticket_date:
            payload["ticket_date"] = str(ticket_date)
        if lottery_type:
            payload["lottery_type"] = str(lottery_type)
        ack_text = f"APP_TICKET_OK v2 {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    else:
        payload["code"] = code or "UNKNOWN"
        payload["reason"] = reason or "unknown_error"
        ack_text = f"APP_TICKET_ERROR v2 {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    bot.send_message(message.chat.id, ack_text)


def get_ticket_id_by_request_id(request_id):
    if not request_id:
        return None
    conn = get_db_connection(timeout=30.0)
    c = conn.cursor()
    c.execute("SELECT id FROM tickets_v3 WHERE request_id = ? LIMIT 1", (request_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def get_ticket_by_request_id(request_id):
    if not request_id:
        return None
    conn = get_db_connection(timeout=30.0)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        "SELECT id, date, lottery_type FROM tickets_v3 WHERE request_id = ? ORDER BY id DESC LIMIT 1",
        (request_id,)
    )
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


def parse_lista_app_payload(payload_text):
    try:
        payload = json.loads(payload_text)
    except Exception:
        raise ValueError(("INVALID_JSON", "invalid_json"))

    if not isinstance(payload, dict):
        raise ValueError(("INVALID_JSON", "payload_not_object"))

    if int(payload.get('schema_version', 0) or 0) != LISTA_APP_SCHEMA_VERSION:
        raise ValueError(("INVALID_SCHEMA", "schema_version_invalid"))

    if str(payload.get('source') or '').strip() != LISTA_SOURCE_ANDROID:
        raise ValueError(("INVALID_SCHEMA", "source_invalid"))

    allowed_keys = {'source', 'schema_version', 'request_id', 'items', 'client_total'}
    unexpected_keys = sorted(set(payload.keys()) - allowed_keys)
    if unexpected_keys:
        raise ValueError(("INVALID_SCHEMA", f"unexpected_fields_{'_'.join(unexpected_keys)}"))

    request_id = str(payload.get('request_id') or '').strip()
    if not request_id:
        raise ValueError(("INVALID_REQUEST_ID", "request_id_missing"))

    try:
        normalized_items = validate_normalized_items(payload.get('items'))
    except ValueError as e:
        raise ValueError(("INVALID_ITEMS", str(e)))

    try:
        client_total = round(float(payload.get('client_total')), 2)
    except Exception:
        raise ValueError(("INVALID_TOTAL", "client_total_invalid"))

    server_total = calculate_server_total(normalized_items)
    if abs(server_total - client_total) > 0.01:
        raise ValueError(("TOTAL_MISMATCH", f"server_total_{server_total:.2f}_client_total_{client_total:.2f}"))

    return {
        'request_id': request_id,
        'items': normalized_items,
        'client_total': client_total,
        'server_total': server_total
    }


def process_manual_lista_text(message, raw_text):
    try:
        parsed_items = validate_normalized_items(parse_manual_ticket_text(raw_text))
    except ValueError as e:
        bot.reply_to(message, f"⚠️ No pude parsear esa lista: {e}")
        return

    ticket_date, lottery_type = get_default_lista_ticket_context()
    if not ticket_date or not lottery_type:
        clear_lista_waiting_state(message.from_user.id)
        bot.reply_to(message, "⚠️ No hay sorteos disponibles ni hoy ni mañana para /lista.")
        return

    server_total = calculate_server_total(parsed_items)
    draft_id = create_ticket_draft(
        user_id=message.from_user.id,
        source=LISTA_SOURCE_MANUAL,
        raw_text=raw_text,
        items=parsed_items,
        ticket_date=ticket_date,
        lottery_type=lottery_type,
        client_total=None,
        server_total=server_total,
        status='PREVIEW',
        expires_minutes=LISTA_DRAFT_TTL_MINUTES
    )
    clear_lista_waiting_state(message.from_user.id)
    draft = get_ticket_draft(draft_id)
    bot.reply_to(
        message,
        build_lista_preview_text(draft),
        reply_markup=get_lista_preview_markup(message.from_user.id, draft)
    )


def process_app_lista_payload(message, payload_text):
    request_id = "unknown"
    try:
        rough_payload = json.loads(payload_text)
        if isinstance(rough_payload, dict):
            request_id = str(rough_payload.get('request_id') or request_id).strip() or request_id
    except Exception:
        rough_payload = None

    if not is_admin_user(message.from_user.id):
        send_app_ticket_ack(message, request_id, ok=False, code="ADMIN_ONLY", reason="not_admin_user")
        return

    try:
        parsed = parse_lista_app_payload(payload_text)
        request_id = parsed['request_id']
    except ValueError as e:
        payload = e.args[0] if e.args else ("VALIDATION_ERROR", "validation_failed")
        if isinstance(payload, tuple) and len(payload) == 2:
            code, reason = payload
        else:
            code, reason = "VALIDATION_ERROR", str(e)
        if request_id != "unknown":
            failed_draft = get_ticket_draft_by_request_id(request_id)
            if failed_draft:
                update_ticket_draft(
                    failed_draft['id'],
                    raw_text=payload_text,
                    status='FAILED_VALIDATION'
                )
            else:
                create_ticket_draft(
                    user_id=message.from_user.id,
                    source=LISTA_SOURCE_ANDROID,
                    raw_text=payload_text,
                    items=[],
                    request_id=request_id,
                    status='FAILED_VALIDATION'
                )
        send_app_ticket_ack(message, request_id, ok=False, code=code, reason=reason)
        return

    ticket_date, lottery_type = get_default_lista_ticket_context()
    if not ticket_date or not lottery_type:
        existing_draft = get_ticket_draft_by_request_id(parsed['request_id'])
        if existing_draft:
            update_ticket_draft(
                existing_draft['id'],
                raw_text=payload_text,
                items=parsed['items'],
                ticket_date=None,
                lottery_type=None,
                client_total=parsed['client_total'],
                server_total=parsed['server_total'],
                status='FAILED_VALIDATION'
            )
        else:
            create_ticket_draft(
                user_id=message.from_user.id,
                source=LISTA_SOURCE_ANDROID,
                raw_text=payload_text,
                items=parsed['items'],
                client_total=parsed['client_total'],
                server_total=parsed['server_total'],
                request_id=parsed['request_id'],
                status='FAILED_VALIDATION'
            )
        send_app_ticket_ack(message, parsed['request_id'], ok=False, code="NO_AVAILABLE_DRAW", reason="no_available_draw")
        return

    existing_ticket = get_ticket_by_request_id(parsed['request_id'])
    if existing_ticket:
        draft = get_ticket_draft_by_request_id(parsed['request_id'])
        if draft:
            update_ticket_draft(
                draft['id'],
                items=parsed['items'],
                ticket_date=existing_ticket['date'],
                lottery_type=existing_ticket['lottery_type'],
                client_total=parsed['client_total'],
                server_total=parsed['server_total'],
                raw_text=payload_text,
                status='GENERATED',
                expires_at=None
            )
        else:
            create_ticket_draft(
                user_id=message.from_user.id,
                source=LISTA_SOURCE_ANDROID,
                raw_text=payload_text,
                items=parsed['items'],
                ticket_date=existing_ticket['date'],
                lottery_type=existing_ticket['lottery_type'],
                client_total=parsed['client_total'],
                server_total=parsed['server_total'],
                request_id=parsed['request_id'],
                status='GENERATED'
            )
        send_app_ticket_ack(
            message,
            parsed['request_id'],
            ok=True,
            ticket_id=existing_ticket['id'],
            ticket_date=existing_ticket['date'],
            lottery_type=existing_ticket['lottery_type']
        )
        return

    existing_draft = get_ticket_draft_by_request_id(parsed['request_id'])
    if existing_draft:
        draft_id = existing_draft['id']
        update_ticket_draft(
            draft_id,
            raw_text=payload_text,
            items=parsed['items'],
            ticket_date=ticket_date,
            lottery_type=lottery_type,
            client_total=parsed['client_total'],
            server_total=parsed['server_total'],
            status='PREVIEW',
            expires_at=None
        )
    else:
        draft_id = create_ticket_draft(
            user_id=message.from_user.id,
            source=LISTA_SOURCE_ANDROID,
            raw_text=payload_text,
            items=parsed['items'],
            ticket_date=ticket_date,
            lottery_type=lottery_type,
            client_total=parsed['client_total'],
            server_total=parsed['server_total'],
            request_id=parsed['request_id'],
            status='PREVIEW'
        )

    conn = get_db_connection(timeout=30.0)
    try:
        conn.execute("BEGIN IMMEDIATE")
        ticket_id, normalized_items = create_ticket_record(
            user_id=message.from_user.id,
            ticket_date=ticket_date,
            lottery_type=lottery_type,
            items=parsed['items'],
            source=LISTA_SOURCE_ANDROID,
            request_id=parsed['request_id'],
            conn=conn
        )
        payment_result = apply_wallet_or_admin_payment(
            user_id=message.from_user.id,
            ticket_id=ticket_id,
            total_cost=parsed['server_total'],
            conn=conn,
            source=LISTA_SOURCE_ANDROID,
            request_id=parsed['request_id']
        )
        conn.execute(
            "UPDATE ticket_drafts_v1 SET status = 'GENERATED', updated_at = ?, expires_at = NULL WHERE id = ?",
            (utcnow_text(), draft_id)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        duplicate_ticket = get_ticket_by_request_id(parsed['request_id'])
        if duplicate_ticket:
            update_ticket_draft(
                draft_id,
                ticket_date=duplicate_ticket['date'],
                lottery_type=duplicate_ticket['lottery_type'],
                status='GENERATED',
                expires_at=None
            )
            send_app_ticket_ack(
                message,
                parsed['request_id'],
                ok=True,
                ticket_id=duplicate_ticket['id'],
                ticket_date=duplicate_ticket['date'],
                lottery_type=duplicate_ticket['lottery_type']
            )
            return
        send_app_ticket_ack(message, parsed['request_id'], ok=False, code="DUPLICATE_REQUEST", reason="duplicate_request")
        return
    except Exception as e:
        conn.rollback()
        print(f"❌ /lista app generation failed: {e}")
        send_app_ticket_ack(message, parsed['request_id'], ok=False, code="INTERNAL_ERROR", reason="internal_error")
        return
    finally:
        conn.close()

    context_message = build_ticket_message_context(message.chat.id, message.from_user, reply_message_id=message.message_id)
    generate_and_send_ticket(
        context_message,
        ticket_id,
        ticket_date,
        lottery_type,
        normalized_items,
        payment_result
    )
    send_app_ticket_ack(
        message,
        parsed['request_id'],
        ok=True,
        ticket_id=ticket_id,
        ticket_date=ticket_date,
        lottery_type=lottery_type
    )


def extract_lista_command_body(text):
    text = str(text or '')
    match = re.match(r'^/lista(?:@\w+)?(?:\s+([\s\S]*))?$', text.strip(), flags=re.IGNORECASE)
    return (match.group(1) if match else '').strip()


@bot.message_handler(commands=['lista'])
def handle_lista_command(message):
    if message.chat.type != 'private':
        return

    body = extract_lista_command_body(message.text)
    if not body:
        set_lista_waiting_state(message.from_user.id)
        bot.reply_to(message, "Envía el texto de la lista después de /lista.")
        return

    clear_lista_waiting_state(message.from_user.id)
    if body.startswith('{'):
        process_app_lista_payload(message, body)
    else:
        process_manual_lista_text(message, body)


@bot.message_handler(commands=['premios'])
def set_results_ui(message):
    if not is_admin_chat(message): 
        bot.reply_to(message, "⛔ Solo Grupo Admin.")
        return
    
    bot_username = bot.get_me().username
    deep_link = f"https://t.me/{bot_username}?start=admin_menu"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("打开管理面板", url=deep_link))
    
    bot.reply_to(message, "管理面板仅在私聊中可打开。", reply_markup=markup)

@bot.message_handler(commands=['start'])
def send_welcome(message):
    try:
        user_id = message.from_user.id
        dates_str = get_nacional_dates_string()
        
        args = message.text.split()

        # 🟢 ADMIN MENU
        if len(args) > 1 and args[1] == 'admin_menu':
            if not is_admin_chat(message):
                bot.reply_to(message, "⛔ No tienes permisos de administrador.")
                return

            web_app_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=admin_dashboard&nacional_dates={dates_str}&uid={user_id}"
            markup = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            markup.add(KeyboardButton("📊 Abrir Dashboard", web_app=WebAppInfo(url=web_app_url)))
            
            bot.send_message(message.chat.id, "管理员模式已启用。请点击按钮：", reply_markup=markup)
            return 

        # 🟢 NORMAL USER MENU
        web_app_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&nacional_dates={dates_str}&uid={user_id}&bal={get_wallet_balance(user_id):.2f}"
        api_base_param = urllib.parse.quote(HISTORY_API_BASE)
        history_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=history&api_base={api_base_param}&uid={user_id}"
        
        markup = ReplyKeyboardMarkup(resize_keyboard=True)
        markup.row(
            KeyboardButton("📝 Nuevo Ticket", web_app=WebAppInfo(url=web_app_url)),
            KeyboardButton("🏆 Chequear Premios", web_app=WebAppInfo(url=history_url))
        )
        
        sent_msg = bot.send_message(message.chat.id, f"¡Hola! Menú principal:", reply_markup=markup)
        
        # Mirror to Admin Topic
        mirror_to_topic(message.chat.id, sent_msg)
        
    except Exception as e:
        print(f"Error sending welcome: {e}")

def get_menu_markup(user_id):
    """Generate the Persistent Menu Keyboard"""
    try:
        dates_str = get_nacional_dates_string()
        web_app_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&nacional_dates={dates_str}&uid={user_id}&bal={get_wallet_balance(user_id):.2f}"
        api_base_param = urllib.parse.quote(HISTORY_API_BASE)
        history_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=history&api_base={api_base_param}&uid={user_id}"
        
        markup = ReplyKeyboardMarkup(resize_keyboard=True)
        markup.row(
            KeyboardButton("📝 Nuevo Ticket", web_app=WebAppInfo(url=web_app_url)),
            KeyboardButton("🏆 Chequear Premios", web_app=WebAppInfo(url=history_url))
        )
        return markup
    except Exception as e:
        print(f"⚠️ get_menu_markup error: {e}")
        return None

def ensure_menu_button(user_id, chat_id=None):
    """Re-send the persistent menu keyboard in the chat where the temporary keyboard was shown."""
    try:
        markup = get_menu_markup(user_id)
        if markup:
            target_chat_id = chat_id if chat_id is not None else user_id
            bot.send_message(target_chat_id, "Menú:", reply_markup=markup)
    except Exception as e:
        print(f"⚠️ ensure_menu_button error: {e}")

@bot.message_handler(content_types=['web_app_data'])
def handle_web_app(message):
    try:
        if not message.web_app_data.data: return
        payload = json.loads(message.web_app_data.data)
        action = payload.get('action') 

        if action == 'save_results':
            if str(message.from_user.id) != str(ADMIN_USER_ID) and str(message.chat.id) != str(ADMIN_GROUP_ID):
                 return

            db_path = os.path.join(BASE_DIR, 'tickets_test.db')
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO draw_results (date, lottery_type, w1, w2, w3) VALUES (?, ?, ?, ?, ?)", 
                      (payload['date'], payload['lottery'], payload['w1'], payload['w2'], payload['w3']))
            conn.commit()
            conn.close()

            # 🟢 1. Notify Group
            announcement = f"📢 *官方开奖结果*\n📅 {payload['date']} | {payload['lottery']}\n🏆 {payload['w1']} - {payload['w2']} - {payload['w3']}"
            bot.send_message(ADMIN_GROUP_ID, announcement, parse_mode="Markdown")
            
            # 🟢 2. Send Report to Group
            try:
                calculate_and_report(ADMIN_GROUP_ID, payload['date'], payload['lottery'], payload['w1'], payload['w2'], payload['w3'])
            except Exception as e:
                bot.send_message(ADMIN_GROUP_ID, f"⚠️ 报表错误: {e}")
            
            # 🟢 3. AUTO-SWITCH BACK TO NORMAL MENU
            user_id = message.from_user.id
            dates_str = get_nacional_dates_string()
            
            web_app_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&nacional_dates={dates_str}&uid={user_id}&bal={get_wallet_balance(user_id):.2f}"
            api_base_param = urllib.parse.quote(HISTORY_API_BASE)
            history_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=history&api_base={api_base_param}&uid={user_id}"

            markup = ReplyKeyboardMarkup(resize_keyboard=True)
            markup.row(
                KeyboardButton("📝 Nuevo Ticket", web_app=WebAppInfo(url=web_app_url)),
                KeyboardButton("🏆 Chequear Premios", web_app=WebAppInfo(url=history_url))
            )

            bot.send_message(message.chat.id, "✅ Datos guardados. Volviendo al menú principal 👇", reply_markup=markup)

        elif action == 'manual_receipt_submit':
            raw_followup_id = payload.get('followup_id')
            try:
                followup_id = int(raw_followup_id)
            except Exception:
                bot.reply_to(message, "No pude identificar este comprobante.")
                return

            followup = get_receipt_followup(followup_id)
            if not followup:
                bot.reply_to(message, "Este comprobante ya no está disponible.")
                return
            if str(followup['user_id']) != str(message.from_user.id):
                bot.reply_to(message, "Este comprobante no te pertenece.")
                return
            if followup.get('status') not in ('OPEN', 'MANUAL_WAIT_AMOUNT', 'MANUAL_WAIT_CONFIRMATION', 'MANUAL_WAIT_TIME'):
                bot.reply_to(message, "Este comprobante ya fue procesado.")
                return

            amount = normalize_manual_amount(payload.get('amount'))
            if amount is None:
                edit_receipt_followup_message(
                    followup,
                    "Monto inválido. Corrígelo y vuelve a intentarlo.",
                    reply_markup=get_receipt_followup_markup(
                        followup_id,
                        amount=followup.get('amount'),
                        confirmation=followup.get('confirmation_letters'),
                        confirmation_full=followup.get('confirmation_full'),
                        receipt_time=followup.get('receipt_time')
                    )
                )
                return

            confirmation, confirmation_full = normalize_manual_confirmation(payload.get('confirmation'))
            if not confirmation:
                edit_receipt_followup_message(
                    followup,
                    "Código inválido. Corrígelo y vuelve a intentarlo.",
                    reply_markup=get_receipt_followup_markup(
                        followup_id,
                        amount=amount,
                        confirmation=followup.get('confirmation_letters'),
                        confirmation_full=followup.get('confirmation_full'),
                        receipt_time=followup.get('receipt_time')
                    )
                )
                return

            raw_receipt_time = payload.get('receipt_time')
            receipt_time = normalize_manual_receipt_time(raw_receipt_time)
            if receipt_time is None:
                edit_receipt_followup_message(
                    followup,
                    "Hora inválida. Corrígela y vuelve a intentarlo o déjala vacía.",
                    reply_markup=get_receipt_followup_markup(
                        followup_id,
                        amount=amount,
                        confirmation=confirmation,
                        confirmation_full=confirmation_full,
                        receipt_time=followup.get('receipt_time')
                    )
                )
                return

            pending_id = followup.get('pending_verification_id')
            if pending_id:
                delete_pending_verification(pending_id)

            update_receipt_followup(
                followup_id,
                reminder_sent=1,
                pending_verification_id=None,
                manual_amount=amount,
                manual_confirmation=confirmation,
                manual_confirmation_full=confirmation_full,
                manual_receipt_time=receipt_time or None
            )
            refreshed_followup = get_receipt_followup(followup_id) or followup
            processed_ok = process_manual_receipt_submission(
                refreshed_followup,
                amount=amount,
                confirmation=confirmation,
                confirmation_full=confirmation_full,
                receipt_time=receipt_time or None
            )
            if processed_ok:
                ensure_menu_button(followup['user_id'], chat_id=followup['chat_id'])

        elif action == 'manual_receipt_closed':
            raw_followup_id = payload.get('followup_id')
            try:
                followup_id = int(raw_followup_id)
            except Exception:
                return

            followup = get_receipt_followup(followup_id)
            if not followup:
                return
            if str(followup['user_id']) != str(message.from_user.id):
                return

            ensure_menu_button(followup['user_id'], chat_id=followup['chat_id'])

        elif action == 'print_ticket':
            # Ticket was already saved/updated via API — read from DB, apply wallet, generate image
            ticket_id = payload.get('ticket_id')
            if not ticket_id:
                bot.reply_to(message, "Error: ticket_id missing")
                return
            user_id = message.chat.id
            db_path = os.path.join(BASE_DIR, 'tickets_test.db')
            conn = sqlite3.connect(db_path, timeout=30.0)
            c = conn.cursor()
            c.execute("SELECT date, lottery_type, numbers_json, cost, amount_paid, status, tg_message_id, tg_chat_id FROM tickets_v3 WHERE id = ?", (ticket_id,))
            row = c.fetchone()
            if not row:
                conn.close()
                bot.reply_to(message, f"Error: Ticket #{ticket_id} no encontrado.")
                return
            date, lottery_type, numbers_json, db_cost, db_amount_paid, db_status, old_msg_id, old_chat_id = row
            items = json.loads(numbers_json)
            draft_id = payload.get('draft_id')
            source_for_print = LISTA_SOURCE_WEBAPP_EDIT if draft_id else LISTA_SOURCE_WEBAPP

            # Determine if this is an edit (flag from web app) or new ticket
            is_edit = bool(payload.get('is_edit', False))
            if not is_edit:
                total_cost = calculate_server_total(items)
                try:
                    payment_result = apply_wallet_or_admin_payment(
                        user_id=user_id,
                        ticket_id=ticket_id,
                        total_cost=total_cost,
                        conn=conn,
                        source=source_for_print
                    )
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    print(f"❌ Ticket update failed for ticket #{ticket_id}: {e}")
                    bot.reply_to(message, "⚠️ Error procesando la transacción.")
                    conn.close()
                    return
            else:
                # Edit case — "go back in time": refund old payment, re-charge new cost
                old_amount_paid = db_amount_paid
                total_cost = calculate_ticket_cost(items)

                # Admin users: just update cost, always PAID, no wallet interaction
                if is_admin_user(user_id):
                    try:
                        pay_amount = total_cost
                        status = 'PAID'
                        c.execute("UPDATE tickets_v3 SET cost = ?, amount_paid = ?, status = ?, source = ? WHERE id = ?",
                                  (total_cost, pay_amount, status, LISTA_SOURCE_WEBAPP_EDIT, ticket_id))
                        conn.commit()
                        is_edit = True
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ Admin ticket edit failed for ticket #{ticket_id}: {e}")
                        bot.reply_to(message, "⚠️ Error procesando la transacción.")
                        conn.close()
                        return
                else:
                    try:
                        # Step 1: Refund old payment back to wallet
                        if old_amount_paid > 0:
                            c.execute("UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?", (old_amount_paid, user_id))

                        # Step 2: Re-charge with new cost (same logic as new ticket)
                        c.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
                        wal_row = c.fetchone()
                        wallet_bal = wal_row[0] if wal_row else 0.0
                        pay_amount = min(wallet_bal, total_cost)
                        status = 'PAID' if pay_amount >= total_cost - 0.01 else 'PENDING'

                        # Step 3: Deduct new cost from wallet
                        if pay_amount > 0:
                            c.execute("UPDATE user_wallets SET balance = balance - ? WHERE user_id = ?", (pay_amount, user_id))

                        # Step 4: Update ticket with new cost/payment/status
                        c.execute("UPDATE tickets_v3 SET cost = ?, amount_paid = ?, status = ?, source = ? WHERE id = ?",
                                  (total_cost, pay_amount, status, LISTA_SOURCE_WEBAPP_EDIT, ticket_id))
                        conn.commit()
                        is_edit = True
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ Wallet edit adjustment failed for ticket #{ticket_id}: {e}")
                        bot.reply_to(message, "⚠️ Error procesando la transacción.")
                        conn.close()
                        return

                payment_result = {
                    'pay_amount': pay_amount,
                    'status': status,
                    'wallet_balance': None,
                    'total_cost': total_cost
                }

            conn.close()

            # Mark old ticket message as edited (same pattern as deletion)
            print(f"🔍 Edit check: is_edit={is_edit}, old_msg_id={old_msg_id}, old_chat_id={old_chat_id}")
            if is_edit and old_msg_id and old_chat_id:
                try:
                    edited_markup = InlineKeyboardMarkup()
                    edited_markup.add(InlineKeyboardButton("✏️ Ticket Editado", callback_data="noop"))
                    bot.edit_message_reply_markup(old_chat_id, old_msg_id, reply_markup=edited_markup)
                    print(f"✅ Marked old message {old_msg_id} as edited")
                except Exception as e:
                    print(f"⚠️ Could not mark old ticket as edited: {e}")

            generate_and_send_ticket(
                message,
                ticket_id,
                date,
                lottery_type,
                items,
                payment_result,
                is_edit=is_edit,
                old_cost=db_cost
            )
            if draft_id:
                update_ticket_draft(draft_id, status='GENERATED', expires_at=None)

        elif action == 'create_ticket':
            # Legacy fallback (in case old JS version is cached)
            items = payload.get('items', [])
            lottery_type = payload.get('type', 'Desconocido')
            date = payload.get('date', get_today_panama())
            user_id = message.chat.id

            try:
                conn = get_db_connection(timeout=30.0)
                conn.execute("BEGIN IMMEDIATE")
                ticket_id, normalized_items = create_ticket_record(
                    user_id=user_id,
                    ticket_date=date,
                    lottery_type=lottery_type,
                    items=items,
                    source=LISTA_SOURCE_WEBAPP,
                    conn=conn
                )
                total_cost = calculate_server_total(normalized_items)
                payment_result = apply_wallet_or_admin_payment(
                    user_id=user_id,
                    ticket_id=ticket_id,
                    total_cost=total_cost,
                    conn=conn,
                    source=LISTA_SOURCE_WEBAPP
                )
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"❌ Ticket Transaction Failed: {e}")
                bot.reply_to(message, "⚠️ Error procesando la transacción. No se descontó dinero.")
                return
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            generate_and_send_ticket(
                message,
                ticket_id,
                date,
                lottery_type,
                normalized_items,
                payment_result
            )

    except Exception as e:
        print(f"Error: {e}")

# --- OPTIMIZED SECURITY PATTERN ---
def draw_security_pattern(draw, width, height, ticket_id, is_nacional):
    rng = random.Random(f"{ticket_id}_{SECURITY_SALT}")

    if is_nacional:
        line_color_1 = (70, 160, 190)
        line_color_2 = (100, 180, 200)
    else:
        line_color_1 = (200, 210, 230)
        line_color_2 = (190, 200, 220)

    step_size = 10

    freq_v_base = rng.uniform(0.01, 0.03)
    freq_v_ripple = freq_v_base * rng.uniform(3.0, 6.0)
    amp_v = rng.randint(15, 40)
    phase_v = rng.random() * math.pi * 2
    spacing_v = rng.randint(15, 25)
    y_values = list(range(0, height, step_size))
    x_offsets = [
        amp_v * math.sin(y * freq_v_base + phase_v) + (amp_v / 3) * math.cos(y * freq_v_ripple) + (y * 0.1)
        for y in y_values
    ]

    for x_base in range(-width // 4, width + width // 4, spacing_v):
        points = [(x_base + x_offset, y) for y, x_offset in zip(y_values, x_offsets)]
        if len(points) > 1:
            draw.line(points, fill=line_color_1, width=2)

    freq_h1 = rng.uniform(0.005, 0.015)
    freq_h2 = rng.uniform(0.02, 0.04)
    amp_h = rng.randint(10, 25)
    phase_h = rng.random() * math.pi * 2
    spacing_h = rng.randint(18, 30)
    x_values = list(range(0, width, step_size))
    y_offsets = [
        amp_h * math.sin(x * freq_h1 + phase_h) + (amp_h * 0.6) * math.cos(x * freq_h2)
        for x in x_values
    ]

    for y_base in range(0, height, spacing_h):
        points = [(x, y_base + y_offset) for x, y_offset in zip(x_values, y_offsets)]
        if len(points) > 1:
            draw.line(points, fill=line_color_2, width=2)

# --- OPTIMIZED IMAGE GENERATOR (SINGLE UPLOAD) ---
def generate_ticket_image(message, ticket_id, date, lottery_type, items):
    try:
        now_panama = datetime.datetime.now(PANAMA_TZ)
        time_str = now_panama.strftime("%I:%M %p") 
        sec_code = get_short_security_code(ticket_id)

        SCALE = 3
        width = 600 * SCALE 
        base_height = 800 * SCALE 
        item_height = 35 * SCALE 
        height = base_height + (len(items) * item_height)
        
        bg_color = 'white'
        is_nacional = False
        if "Nacional" in lottery_type:
            bg_color = '#c3e8f0'
            is_nacional = True

        img = Image.new('RGB', (width, height), bg_color)
        d = ImageDraw.Draw(img)
        
        draw_security_pattern(d, width, height, ticket_id, is_nacional)

        font_reg, font_small, font_large_bold, font_med_bold, font_num = get_ticket_fonts(SCALE)

        side_padding = 80 * SCALE 
        top_padding = 30 * SCALE
        current_y = top_padding
        
        d.text((side_padding, current_y), f"Ticket #{ticket_id}", fill="gray", font=font_small)
        sec_label = f"SEC: {sec_code}"
        sec_w = d.textlength(sec_label, font=font_small)
        d.text((width - side_padding - sec_w, current_y), sec_label, fill="#333", font=font_small)
        
        current_y += 30 * SCALE
        
        date_label = f"Sorteo: {date}"
        d.text((side_padding, current_y), date_label, fill="gray", font=font_small)
        
        time_label = f"Hora: {time_str}"
        time_w = d.textlength(time_label, font=font_small)
        d.text((width - side_padding - time_w, current_y), time_label, fill="gray", font=font_small)

        current_y += 50 * SCALE
        
        flag_filename = None
        if "Nacional" in lottery_type: flag_filename = os.path.join(BASE_DIR, "flag_panama.png")
        elif "Tica" in lottery_type: flag_filename = os.path.join(BASE_DIR, "flag_tica.png")
        elif "Nica" in lottery_type: flag_filename = os.path.join(BASE_DIR, "flag_nica.png")
        elif "Primera" in lottery_type: flag_filename = os.path.join(BASE_DIR, "flag_dom.png")

        text_w = d.textlength(lottery_type, font=font_large_bold)
        flag_size = 40 * SCALE
        spacing = 10 * SCALE
        
        total_content_width = text_w
        if flag_filename:
            total_content_width = flag_size + spacing + text_w + spacing + flag_size

        start_x = (width - total_content_width) / 2
        
        if flag_filename:
            try:
                flag_img = get_ticket_flag_image(flag_filename, flag_size)
                if flag_img is None:
                    raise ValueError("Flag image unavailable")
                img.paste(flag_img, (int(start_x), int(current_y - 5*SCALE)), flag_img)
                right_flag_x = start_x + flag_size + spacing + text_w + spacing
                img.paste(flag_img, (int(right_flag_x), int(current_y - 5*SCALE)), flag_img)
                text_x = start_x + flag_size + spacing
            except Exception as e:
                text_x = (width - text_w) / 2
        else:
             text_x = (width - text_w) / 2

        d.text((text_x, current_y), lottery_type, fill="#3390ec", font=font_large_bold)
        
        current_y += 70 * SCALE
        
        d.text((side_padding, current_y), "NUM", fill="black", font=font_med_bold)
        cant_text = "CANT."
        cant_w = d.textlength(cant_text, font=font_med_bold)
        d.text(((width - cant_w) / 2, current_y), cant_text, fill="black", font=font_med_bold)
        total_text = "Total"
        total_w = d.textlength(total_text, font=font_med_bold)
        d.text((width - side_padding - total_w, current_y), total_text, fill="black", font=font_med_bold)
        
        current_y += 35 * SCALE
        d.line([(side_padding, current_y), (width - side_padding, current_y)], fill="black", width=5)
        current_y += 30 * SCALE
        
        grand_total = 0
        qty_chances = 0
        qty_large = 0 
        
        for item in items:
            # Separator line (from * or & in pasted list)
            if item.get('separator') or str(item.get('num', '')) == '---':
                current_y += 10 * SCALE
                d.line([(side_padding, current_y), (width - side_padding, current_y)], fill="#999999", width=3)
                current_y += 15 * SCALE
                continue

            total = calculate_ticket_line_total(item.get('num'), item.get('qty'))
            qty = int(item['qty'])
            num_str = str(item['num'])
            if len(num_str) == 2: qty_chances += qty
            elif len(num_str) == 4: qty_large += qty
            grand_total += total

            display_num = f"*{num_str}*"
            d.text((side_padding, current_y), display_num, fill="black", font=font_num)

            qty_text = str(qty)
            qty_w = d.textlength(qty_text, font=font_num)
            d.text(((width - qty_w) / 2, current_y), qty_text, fill="black", font=font_num)
            
            line_total_str = f"{total:.2f}"
            total_w = d.textlength(line_total_str, font=font_num)
            d.text((width - side_padding - total_w, current_y), line_total_str, fill="black", font=font_num)
            
            current_y += item_height 

        current_y += 5 * SCALE 
        d.line([(side_padding, current_y), (width - side_padding, current_y)], fill="black", width=5)
        current_y += 20 * SCALE 
        
        total_label = "Total:"
        total_val = f"${grand_total:.2f}"
        d.text((side_padding, current_y), total_label, fill="black", font=font_large_bold)
        val_w = d.textlength(total_val, font=font_large_bold)
        d.text((width - side_padding - val_w, current_y), total_val, fill="black", font=font_large_bold)
        
        current_y += 40 * SCALE 
        
        if "Nacional" in lottery_type: label_large = "Billetes"
        else: label_large = "Palets"
        summary_text_1 = f"Chances: {qty_chances}"
        summary_text_2 = f"{label_large}: {qty_large}"
        w1 = d.textlength(summary_text_1, font=font_med_bold)
        w2 = d.textlength(summary_text_2, font=font_med_bold)
        
        d.text(((width - w1) / 2, current_y), summary_text_1, fill="gray", font=font_med_bold)
        current_y += 35 * SCALE 
        d.text(((width - w2) / 2, current_y), summary_text_2, fill="gray", font=font_med_bold)
        
        final_bottom = current_y + 40 * SCALE
        img = img.crop((0, 0, width, final_bottom))

        bio = io.BytesIO()
        img.save(bio, 'JPEG', quality=95)
        bio.seek(0)
        
        flag_emoji = "✅"
        if "Nacional" in lottery_type: flag_emoji = "🇵🇦"
        elif "Tica" in lottery_type: flag_emoji = "🇨🇷"
        elif "Nica" in lottery_type: flag_emoji = "🇳🇮"
        elif "Primera" in lottery_type: flag_emoji = "🇩🇴"
        
        # --- FAST SEND LOGIC (Send once, forward ID) ---
        # 1. Send to User with Edit + Delete buttons
        user_id = message.from_user.id
        api_base_param = urllib.parse.quote(HISTORY_API_BASE)
        edit_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=history&api_base={api_base_param}&uid={user_id}&edit_ticket={ticket_id}"
        delete_markup = InlineKeyboardMarkup()
        delete_markup.add(InlineKeyboardButton("✏️ Editar Ticket", web_app=WebAppInfo(url=edit_url)))
        delete_markup.add(InlineKeyboardButton("🗑️ Cancelar Ticket", callback_data=f"del1_{ticket_id}"))
        caption_text = f"Ticket #{ticket_id} | {lottery_type} {flag_emoji}"
        photo_size = bio.seek(0, 2)
        bio.seek(0)
        if photo_size > 10 * 1024 * 1024 or (width + height) > 10000:
            sent_msg = bot.send_document(message.chat.id, document=bio, caption=caption_text, reply_markup=delete_markup, visible_file_name=f"ticket_{ticket_id}.jpg")
        else:
            sent_msg = bot.send_photo(message.chat.id, photo=bio, caption=caption_text, reply_markup=delete_markup)
        
        # Store message_id for web app deletion of inline button
        try:
            db_path2 = os.path.join(BASE_DIR, 'tickets_test.db')
            conn2 = sqlite3.connect(db_path2, timeout=30.0)
            conn2.execute("UPDATE tickets_v3 SET tg_message_id = ?, tg_chat_id = ? WHERE id = ?", (sent_msg.message_id, message.chat.id, ticket_id))
            conn2.commit()
            conn2.close()
        except Exception as e:
            print(f"⚠️ Failed to save message_id: {e}")
        
        
        # 2. Send to Admin Group lottery-type sub-topic
        photo_id = sent_msg.photo[-1].file_id if sent_msg.photo else None
        user_name_str = message.from_user.username
        user_display = f"@{user_name_str}" if user_name_str else "Sin Alias"
        first_name = message.from_user.first_name or ""

        target_thread_id = None
        if "Nacional" in lottery_type: target_thread_id = TOPIC_MAPPING.get("Nacional")
        elif "Tica" in lottery_type: target_thread_id = TOPIC_MAPPING.get("Tica")
        elif "Nica" in lottery_type: target_thread_id = TOPIC_MAPPING.get("Nica")
        elif "Primera" in lottery_type: target_thread_id = TOPIC_MAPPING.get("Primera")

        admin_caption = f"👤 {user_display} ({first_name})\n🎫 Ticket #{ticket_id}\n🔐 Code: {sec_code}\n📅 {date} | {time_str}\n💰 {lottery_type}"
        try:
            if photo_id:
                bot.send_photo(ADMIN_GROUP_ID, photo=photo_id, caption=admin_caption, message_thread_id=target_thread_id)
            else:
                bot.send_message(ADMIN_GROUP_ID, admin_caption, message_thread_id=target_thread_id)
        except Exception as e:
            print(f"⚠️ Failed to send ticket to admin topic: {e}")
        
    except Exception as e:
        print(f"Error generating image: {e}")
        bot.reply_to(message, f"Ticket #{ticket_id} Guardado (Error imagen: {e})")

# ---------------------------------------------------------
# 🗑️ TICKET DELETION (Double Confirmation)
# ---------------------------------------------------------

@bot.callback_query_handler(func=lambda call: call.data.startswith('lista_'))
def handle_lista_callback(call):
    try:
        data = call.data or ''
        if data.startswith('lista_confirm_'):
            action = 'confirm'
        elif data.startswith('lista_cancel_'):
            action = 'cancel'
        else:
            bot.answer_callback_query(call.id, "Acción no soportada.")
            return

        draft_id = int(data.rsplit('_', 1)[1])
        draft = expire_ticket_draft_if_needed(get_ticket_draft(draft_id))
        if not draft:
            bot.answer_callback_query(call.id, "Este borrador ya no está disponible.")
            return

        if str(draft.get('user_id')) != str(call.from_user.id):
            bot.answer_callback_query(call.id, "Este borrador no te pertenece.")
            return

        current_status = draft.get('status')
        if action == 'cancel':
            if current_status == 'GENERATED':
                bot.answer_callback_query(call.id, "Ese ticket ya fue generado.")
                return
            if current_status == 'EXPIRED':
                bot.answer_callback_query(call.id, "Ese borrador ya expiró.")
                return
            update_ticket_draft(draft_id, status='CANCELLED', expires_at=None)
            try:
                bot.edit_message_reply_markup(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_lista_done_markup("Borrador cancelado")
                )
            except Exception as e:
                print(f"⚠️ Failed to mark /lista draft cancelled: {e}")
            bot.answer_callback_query(call.id, "Borrador cancelado.")
            return

        if current_status == 'GENERATED':
            bot.answer_callback_query(call.id, "Ese ticket ya fue generado.")
            return
        if current_status == 'EXPIRED':
            bot.answer_callback_query(call.id, "Ese borrador expiró. Envía /lista otra vez.")
            return
        if current_status != 'PREVIEW':
            bot.answer_callback_query(call.id, "Ese borrador ya no puede confirmarse.")
            return

        conn = get_db_connection(timeout=30.0)
        try:
            conn.execute("BEGIN IMMEDIATE")
            c = conn.cursor()
            c.execute(
                '''SELECT user_id, items_json, ticket_date, lottery_type, server_total, status, expires_at
                   FROM ticket_drafts_v1 WHERE id = ?''',
                (draft_id,)
            )
            row = c.fetchone()
            if not row:
                conn.rollback()
                bot.answer_callback_query(call.id, "Este borrador ya no está disponible.")
                return

            user_id, items_json, ticket_date, lottery_type, saved_total, db_status, expires_at = row
            if str(user_id) != str(call.from_user.id):
                conn.rollback()
                bot.answer_callback_query(call.id, "Este borrador no te pertenece.")
                return
            if db_status != 'PREVIEW':
                conn.rollback()
                bot.answer_callback_query(call.id, "Ese borrador ya no puede confirmarse.")
                return
            if expires_at and expires_at < utcnow_text():
                c.execute(
                    "UPDATE ticket_drafts_v1 SET status = 'EXPIRED', updated_at = ? WHERE id = ?",
                    (utcnow_text(), draft_id)
                )
                conn.commit()
                bot.answer_callback_query(call.id, "Ese borrador expiró. Envía /lista otra vez.")
                return

            normalized_items = validate_normalized_items(json.loads(items_json or '[]'))
            server_total = calculate_server_total(normalized_items)
            ticket_id, normalized_items = create_ticket_record(
                user_id=user_id,
                ticket_date=ticket_date,
                lottery_type=lottery_type,
                items=normalized_items,
                source=LISTA_SOURCE_MANUAL,
                conn=conn
            )
            payment_result = apply_wallet_or_admin_payment(
                user_id=user_id,
                ticket_id=ticket_id,
                total_cost=server_total,
                conn=conn,
                source=LISTA_SOURCE_MANUAL
            )
            c.execute(
                '''UPDATE ticket_drafts_v1
                   SET status = 'GENERATED', server_total = ?, updated_at = ?, expires_at = NULL
                   WHERE id = ?''',
                (server_total, utcnow_text(), draft_id)
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"❌ /lista confirm failed: {e}")
            bot.answer_callback_query(call.id, "No pude generar ese ticket.")
            return
        finally:
            conn.close()

        context_message = build_ticket_message_context(call.message.chat.id, call.from_user, reply_message_id=call.message.message_id)
        generate_and_send_ticket(
            context_message,
            ticket_id,
            ticket_date,
            lottery_type,
            normalized_items,
            payment_result
        )
        try:
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=get_lista_done_markup(f"Ticket #{ticket_id} generado")
            )
        except Exception as e:
            print(f"⚠️ Failed to mark /lista draft generated: {e}")
        bot.answer_callback_query(call.id, "Ticket generado.")
    except Exception as e:
        print(f"❌ /lista callback error: {e}")
        bot.answer_callback_query(call.id, "No pude procesar ese borrador.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('receipt_'))
def handle_receipt_followup_callback(call):
    """Handle manual-receipt and ignore actions for OCR followups."""
    try:
        data = call.data
        followup_id = int(data.rsplit('_', 1)[1])
        followup = get_receipt_followup(followup_id)

        if not followup:
            bot.answer_callback_query(call.id, "Este comprobante ya no está disponible.")
            return

        if str(followup['user_id']) != str(call.from_user.id):
            bot.answer_callback_query(call.id, "Este comprobante no te pertenece.")
            return

        if followup.get('status') != 'OPEN':
            if str(followup.get('status', '')).startswith('MANUAL_'):
                bot.answer_callback_query(call.id, "Ya estoy esperando los datos manuales.")
            elif followup.get('status') == 'IGNORED':
                bot.answer_callback_query(call.id, "Este comprobante ya fue descartado.")
            else:
                bot.answer_callback_query(call.id, "Este comprobante ya fue procesado.")
            return

        if data.startswith('receipt_manual_'):
            pending_id = followup.get('pending_verification_id')
            if pending_id:
                delete_pending_verification(pending_id)
                update_receipt_followup(followup_id, pending_verification_id=None, reminder_sent=1)

            # Pre-fill amount and time from OCR (which are always correct)
            # and ask user to reply with just the correct confirmation code
            update_receipt_followup(
                followup_id,
                status='MANUAL_WAIT_CONFIRMATION',
                manual_amount=followup.get('amount'),
                manual_receipt_time=followup.get('receipt_time') or ''
            )
            bot.answer_callback_query(call.id, "Envía el código de confirmación correcto.")
            send_followup_reply_to_receipt(
                followup,
                "Envía el código de confirmación correcto (5 letras, ej: `ABCDE` o `#ABCDE-12345678`).",
                parse_mode="Markdown"
            )
            return

        if data.startswith('receipt_ignore_'):
            pending_id = followup.get('pending_verification_id')
            if pending_id:
                delete_pending_verification(pending_id)
            complete_receipt_followup(followup_id, status="IGNORED", release_hash=True)
            bot.answer_callback_query(call.id, "Comprobante descartado.")
            return

        bot.answer_callback_query(call.id)

    except Exception as e:
        print(f"❌ Receipt followup callback error: {e}")
        bot.answer_callback_query(call.id, "No pude procesar esta acción.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('del'))
def handle_delete_callback(call):
    """Handle ticket deletion with double confirmation"""
    try:
        data = call.data
        user_id = call.from_user.id
        
        # --- STEP 1: First "Cancel" button pressed ---
        if data.startswith('del1_'):
            ticket_id = data.replace('del1_', '')
            markup = InlineKeyboardMarkup()
            markup.row(
                InlineKeyboardButton("✅ Sí, eliminar", callback_data=f"del2_{ticket_id}"),
                InlineKeyboardButton("❌ No", callback_data=f"delno_{ticket_id}")
            )
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id, reply_markup=markup
            )
            bot.answer_callback_query(call.id, "¿Seguro que deseas eliminar este ticket?")
        
        # --- STEP 2: Second confirmation ---
        elif data.startswith('del2_'):
            ticket_id = data.replace('del2_', '')
            markup = InlineKeyboardMarkup()
            markup.row(
                InlineKeyboardButton("⚠️ Confirmar Eliminación", callback_data=f"del3_{ticket_id}"),
                InlineKeyboardButton("❌ Cancelar", callback_data=f"delno_{ticket_id}")
            )
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id, reply_markup=markup
            )
            bot.answer_callback_query(call.id, "⚠️ Esta acción es irreversible")
        
        # --- STEP 3: Final confirmation — Execute deletion ---
        elif data.startswith('del3_'):
            ticket_id = int(data.replace('del3_', ''))
            
            db_path = os.path.join(BASE_DIR, 'tickets_test.db')
            conn = sqlite3.connect(db_path, timeout=30.0)
            c = conn.cursor()
            
            # Verify ownership
            c.execute("SELECT cost, amount_paid, status FROM tickets_v3 WHERE id = ? AND user_id = ?", 
                      (ticket_id, user_id))
            row = c.fetchone()
            
            if not row:
                bot.answer_callback_query(call.id, "❌ Ticket no encontrado")
                conn.close()
                return
            
            cost, paid, current_status = row
            
            if current_status in ('DELETED', 'INVALID'):
                bot.answer_callback_query(call.id, "Ya fue eliminado/anulado")
                conn.close()
                return
            
            # Refund wallet if any amount was paid
            refund_msg = ""
            if paid and paid > 0:
                c.execute("INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 0)", (user_id,))
                c.execute("UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?", (paid, user_id))
            
            # Mark as DELETED
            c.execute("UPDATE tickets_v3 SET status = 'DELETED' WHERE id = ?", (ticket_id,))
            conn.commit()
            conn.close()
            
            # Read balance AFTER commit so it reflects the refund
            if paid and paid > 0:
                new_bal = get_wallet_balance(user_id)
                refund_msg = f"\n💰 Reembolso: ${paid:.2f} → Fondo: ${new_bal:.2f}"
            
            # Replace inline button with "Ticket Eliminado" label
            done_markup = InlineKeyboardMarkup()
            done_markup.add(InlineKeyboardButton("✅ Ticket Eliminado", callback_data="noop"))
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id, reply_markup=done_markup
            )
            
            # Reply to the ticket message with deletion notice
            delete_notice = f"🚫 *Ticket #{ticket_id} Eliminado*{refund_msg}"
            reply_msg = bot.reply_to(call.message, delete_notice, parse_mode="Markdown")
            
            # Mirror deletion to admin topic
            try:
                user_name = format_topic_user_name(
                    user_id,
                    first_name=call.from_user.first_name,
                    last_name=call.from_user.last_name
                )
                mirror_to_topic(user_id, reply_msg, user_name=user_name)
            except Exception as e:
                print(f"⚠️ Mirror delete notice failed: {e}")
            
            bot.answer_callback_query(call.id, "✅ Ticket eliminado")
        
        # --- CANCEL: Restore original buttons ---
        elif data.startswith('delno_'):
            ticket_id = data.replace('delno_', '')
            api_base_param = urllib.parse.quote(HISTORY_API_BASE)
            edit_url = f"{WEBAPP_BASE_URL}index.html?v={BOT_VERSION}&mode=history&api_base={api_base_param}&uid={user_id}&edit_ticket={ticket_id}"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("✏️ Editar Ticket", web_app=WebAppInfo(url=edit_url)))
            markup.add(InlineKeyboardButton("🗑️ Cancelar Ticket", callback_data=f"del1_{ticket_id}"))
            bot.edit_message_reply_markup(
                call.message.chat.id, call.message.message_id, reply_markup=markup
            )
            bot.answer_callback_query(call.id, "Operación cancelada")
    
    except Exception as e:
        print(f"❌ Delete callback error: {e}")
        bot.answer_callback_query(call.id, f"Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data == 'noop')
def handle_noop_callback(call):
    """Ignore clicks on disabled label buttons"""
    bot.answer_callback_query(call.id)

def translate_admin_report_line_to_zh(line):
    """Translate payout breakdown labels for admin-group reports only."""
    if not line:
        return line

    translated = str(line)
    replacements = [
        ("1er Premio (Exacto)", "一等奖（整号）"),
        ("1er Premio (3 Primeras)", "一等奖（前三位）"),
        ("1er Premio (3 Ultimas)", "一等奖（后三位）"),
        ("1er Premio (2 Primeras)", "一等奖（前两位）"),
        ("1er Premio (2 Ultimas)", "一等奖（后两位）"),
        ("1er Premio (Ultima)", "一等奖（最后一位）"),
        ("2do Premio (Exacto)", "二等奖（整号）"),
        ("2do Premio (3 Primeras)", "二等奖（前三位）"),
        ("2do Premio (3 Ultimas)", "二等奖（后三位）"),
        ("2do Premio (2 Ultimas)", "二等奖（后两位）"),
        ("3er Premio (Exacto)", "三等奖（整号）"),
        ("3er Premio (3 Primeras)", "三等奖（前三位）"),
        ("3er Premio (3 Ultimas)", "三等奖（后三位）"),
        ("3er Premio (2 Ultimas)", "三等奖（后两位）"),
        ("Chances (1er)", "Chances（一等奖）"),
        ("Chances (2do)", "Chances（二等奖）"),
        ("Chances (3er)", "Chances（三等奖）"),
        ("Billete 1ro/2do", "Billete（1/2奖组合）"),
        ("Billete 1ro/3ro", "Billete（1/3奖组合）"),
        ("Billete 2do/3ro", "Billete（2/3奖组合）"),
        ("1er Premio", "一等奖"),
        ("2do Premio", "二等奖"),
        ("3er Premio", "三等奖")
    ]
    for src, dst in replacements:
        translated = translated.replace(src, dst)
    return translated

def _escape_md(text):
    """Escape special characters for Telegram legacy Markdown."""
    for ch in ('\\', '_', '*', '`', '['):
        text = text.replace(ch, '\\' + ch)
    return text

def calculate_and_report(chat_id, date, lottery_name, w1, w2, w3):
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT id, numbers_json FROM tickets_v3 WHERE date = ? AND lottery_type = ? AND (status IS NULL OR status NOT IN ('DELETED', 'INVALID'))", (date, lottery_name))
    tickets = c.fetchall()
    conn.close()
    if not tickets:
        bot.send_message(chat_id, f"🔍 {date} 的 {lottery_name} 没有售出任何票。")
        return
    report = f"💰 **详细报表**\nSorteo: {_escape_md(lottery_name)}\n日期: {date}\n🏆: {w1}-{w2}-{w3}\n====================\n"
    total_payout = 0
    winners_count = 0
    for ticket in tickets:
        t_id, data_json = ticket
        items = json.loads(data_json)
        ticket_total_win = 0
        ticket_breakdown_lines = []
        for item in items:
            num = str(item['num'])
            bet = float(item['qty'])
            win, lines = calculate_single_ticket(num, bet, w1, w2, w3, lottery_name)
            if win > 0:
                ticket_total_win += win
                for line in lines:
                    translated_line = translate_admin_report_line_to_zh(line)
                    ticket_breakdown_lines.append(f"   • \\[{num}] {_escape_md(translated_line)}")
        if ticket_total_win > 0:
            winners_count += 1
            total_payout += ticket_total_win
            header = f"🎫 **Ticket #{t_id}** | 中奖金额: **${ticket_total_win:.2f}**"
            # Compute dash separator length from the longest visible line in this ticket block
            all_lines = [header] + ticket_breakdown_lines
            max_len = max(len(re.sub(r'[*\\\[\]]', '', l)) for l in all_lines)
            dash_sep = "─" * max_len
            report += f"\n{header}\n"
            report += "\n".join(ticket_breakdown_lines) + "\n"
            report += f"{dash_sep}\n"
    report += "\n====================\n"
    report += f"👥 中奖票数: {winners_count}\n"
    report += f"💸 **总中奖金额: ${total_payout:.2f}**"
    try:
        bot.send_message(chat_id, report, parse_mode="Markdown")
    except Exception:
        bot.send_message(chat_id, report)

# --- TOPIC MANAGEMENT ---
def init_support_db():
    """Ensure support table and message map exist"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS support_threads 
                     (user_id INTEGER PRIMARY KEY, thread_id INTEGER, user_name TEXT)''')
        
        # New: Message Mapping
        c.execute('''CREATE TABLE IF NOT EXISTS message_map 
                     (admin_msg_id INTEGER PRIMARY KEY, user_msg_id INTEGER, chat_id INTEGER)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_message_map_lookup
                     ON message_map(chat_id, user_msg_id)''')
        c.execute('''CREATE TABLE IF NOT EXISTS mirror_outbox (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     user_id INTEGER NOT NULL,
                     source_chat_id INTEGER NOT NULL,
                     source_message_id INTEGER NOT NULL,
                     source_reply_to_id INTEGER,
                     user_name TEXT,
                     attempt_count INTEGER DEFAULT 0,
                     next_attempt_at REAL DEFAULT 0,
                     last_error TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     UNIQUE(source_chat_id, source_message_id)
        )''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_mirror_outbox_user_order
                     ON mirror_outbox(user_id, source_message_id, created_at)''')
        c.execute('''CREATE INDEX IF NOT EXISTS idx_mirror_outbox_attempt
                     ON mirror_outbox(next_attempt_at, user_id)''')
                      
        conn.commit()
        conn.close()
        print("✅ Support DB (Topics + MessageMap + MirrorOutbox) initialized.")
    except Exception as e:
        print(f"❌ Failed to init Support DB: {e}")

init_support_db()  # Run immediately


def _looks_like_missing_topic_error(exc) -> bool:
    text = str(exc).lower()
    return any(token in text for token in (
        'message thread not found',
        'thread not found',
        'message thread id is not valid',
        'topic not found',
        'forum topic',
        'topic was deleted',
        'thread was deleted',
    ))


def _looks_like_missing_reply_error(exc) -> bool:
    text = str(exc).lower()
    return any(token in text for token in (
        'reply message not found',
        'message to be replied not found',
        'replied message not found',
    ))


def invalidate_support_topic(user_id, thread_id=None, drop_db_mapping=False):
    """Clear cached topic info so a fresh admin topic can be created on next attempt."""
    try:
        with MIRROR_CACHE_LOCK:
            cached = SUPPORT_THREAD_CACHE.pop(int(user_id), None)
        target_thread_id = thread_id or (cached[0] if cached else None)

        if drop_db_mapping:
            db_path = os.path.join(BASE_DIR, 'tickets_test.db')
            conn = sqlite3.connect(db_path, timeout=30.0)
            c = conn.cursor()
            if target_thread_id:
                c.execute("DELETE FROM support_threads WHERE user_id = ? AND thread_id = ?", (user_id, target_thread_id))
            else:
                c.execute("DELETE FROM support_threads WHERE user_id = ?", (user_id,))
            conn.commit()
            conn.close()
    except Exception as e:
        print(f"⚠️ Failed to invalidate support topic cache for {user_id}: {e}")


def _build_mirror_task(task_id, user_id, source_chat_id, source_message_id, user_name=None, source_reply_to_id=None, not_before=None, attempt_count=0, enqueued_at=None):
    return {
        'task_id': int(task_id),
        'user_id': int(user_id),
        'source_chat_id': int(source_chat_id),
        'source_message_id': int(source_message_id),
        'source_reply_to_id': int(source_reply_to_id) if source_reply_to_id is not None else None,
        'user_name': user_name,
        'attempt_count': int(attempt_count or 0),
        'not_before': float(not_before or 0),
        'enqueued_at': float(enqueued_at if enqueued_at is not None else time.time()),
    }


def _queue_mirror_task(task):
    with MIRROR_QUEUE_LOCK:
        task_id = task.get('task_id')
        if task_id in MIRROR_ENQUEUED_TASK_IDS:
            worker = MIRROR_WORKERS.get(task['user_id'])
            if not worker or not worker.is_alive():
                worker = threading.Thread(target=_mirror_worker, args=(task['user_id'],), daemon=True)
                MIRROR_WORKERS[task['user_id']] = worker
                worker.start()
            return

        pending = MIRROR_QUEUE_BY_USER.setdefault(task['user_id'], [])
        pending.append(task)
        MIRROR_ENQUEUED_TASK_IDS.add(task_id)

        worker = MIRROR_WORKERS.get(task['user_id'])
        if not worker or not worker.is_alive():
            worker = threading.Thread(target=_mirror_worker, args=(task['user_id'],), daemon=True)
            MIRROR_WORKERS[task['user_id']] = worker
            worker.start()


def persist_mirror_task(user_id, source_chat_id, source_message_id, user_name=None, source_reply_to_id=None):
    """Persist mirror tasks so restart/crash doesn't lose pending admin copies."""
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    conn = sqlite3.connect(db_path, timeout=30.0)
    c = conn.cursor()
    c.execute(
        '''INSERT OR IGNORE INTO mirror_outbox
           (user_id, source_chat_id, source_message_id, source_reply_to_id, user_name, next_attempt_at)
           VALUES (?, ?, ?, ?, ?, 0)''',
        (user_id, source_chat_id, source_message_id, source_reply_to_id, user_name)
    )
    if c.rowcount > 0:
        task_id = c.lastrowid
        c.execute(
            '''SELECT id, user_id, source_chat_id, source_message_id, source_reply_to_id, user_name,
                      attempt_count, next_attempt_at
               FROM mirror_outbox WHERE id = ?''',
            (task_id,)
        )
        row = c.fetchone()
    else:
        c.execute(
            '''SELECT id, user_id, source_chat_id, source_message_id, source_reply_to_id, user_name,
                      attempt_count, next_attempt_at
               FROM mirror_outbox
               WHERE source_chat_id = ? AND source_message_id = ?''',
            (source_chat_id, source_message_id)
        )
        existing = c.fetchone()
        if existing and (
            (user_name and user_name != existing[5])
            or (source_reply_to_id is not None and source_reply_to_id != existing[4])
        ):
            c.execute(
                '''UPDATE mirror_outbox
                   SET user_name = COALESCE(?, user_name),
                       source_reply_to_id = COALESCE(?, source_reply_to_id)
                   WHERE id = ?''',
                (user_name, source_reply_to_id, existing[0])
            )
            c.execute(
                '''SELECT id, user_id, source_chat_id, source_message_id, source_reply_to_id, user_name,
                          attempt_count, next_attempt_at
                   FROM mirror_outbox WHERE id = ?''',
                (existing[0],)
            )
            row = c.fetchone()
        else:
            row = existing
    conn.commit()
    conn.close()
    return _build_mirror_task(
        row[0], row[1], row[2], row[3],
        user_name=row[5], source_reply_to_id=row[4],
        attempt_count=row[6], not_before=row[7]
    ) if row else None


def delete_mirror_task(task_id):
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("DELETE FROM mirror_outbox WHERE id = ?", (task_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"⚠️ Failed to delete mirror task {task_id}: {e}")
        return False


def reschedule_mirror_task(task_id, error_text, attempt_count):
    delay = min(MIRROR_RETRY_BASE_SECONDS * (2 ** max(0, attempt_count - 1)), MIRROR_RETRY_MAX_SECONDS)
    next_attempt_at = time.time() + delay
    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
    conn = sqlite3.connect(db_path, timeout=30.0)
    c = conn.cursor()
    c.execute(
        '''UPDATE mirror_outbox
           SET attempt_count = ?, next_attempt_at = ?, last_error = ?
           WHERE id = ?''',
        (attempt_count, next_attempt_at, str(error_text)[:500], task_id)
    )
    conn.commit()
    conn.close()
    return next_attempt_at


def load_pending_mirror_tasks():
    """Replay persisted mirror tasks left behind by retries or restarts."""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute(
            '''SELECT id, user_id, source_chat_id, source_message_id, source_reply_to_id, user_name,
                      attempt_count, next_attempt_at
               FROM mirror_outbox
               ORDER BY user_id ASC, source_message_id ASC, created_at ASC'''
        )
        rows = c.fetchall()
        conn.close()
        for row in rows:
            _queue_mirror_task(
                _build_mirror_task(
                    row[0], row[1], row[2], row[3],
                    user_name=row[5], source_reply_to_id=row[4],
                    attempt_count=row[6], not_before=row[7]
                )
            )
        if rows:
            print(f"🔁 Reloaded {len(rows)} pending mirror task(s)")
    except Exception as e:
        print(f"⚠️ Failed to reload pending mirror tasks: {e}")

def get_or_create_topic(user_id, user_name):
    """
    Get existing topic ID for user or create a new one in Admin Group.
    Returns: thread_id (int) or None
    """
    try:
        normalized_name = format_topic_user_name(user_id, user_name=user_name) if user_name else None
        with MIRROR_CACHE_LOCK:
            cached = SUPPORT_THREAD_CACHE.get(int(user_id))
        if cached:
            thread_id, stored_name = cached
            if normalized_name and stored_name != normalized_name:
                conn = None
                try:
                    bot.edit_forum_topic(ADMIN_GROUP_ID, thread_id, name=normalized_name)
                    db_path = os.path.join(BASE_DIR, 'tickets_test.db')
                    conn = sqlite3.connect(db_path, timeout=30.0)
                    c = conn.cursor()
                    c.execute("UPDATE support_threads SET user_name = ? WHERE user_id = ?", (normalized_name, user_id))
                    conn.commit()
                    with MIRROR_CACHE_LOCK:
                        SUPPORT_THREAD_CACHE[int(user_id)] = (thread_id, normalized_name)
                except Exception as e:
                    print(f"⚠️ Failed to rename topic: {e}")
                finally:
                    if conn:
                        conn.close()
            return thread_id

        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        
        # Check if topic exists
        c.execute("SELECT thread_id, user_name FROM support_threads WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        
        if row:
            thread_id, stored_name = row
            with MIRROR_CACHE_LOCK:
                SUPPORT_THREAD_CACHE[int(user_id)] = (thread_id, stored_name)
            
            # 🔥 OPTIMIZATION: Update Topic Name if changed (and valid)
            if normalized_name and stored_name != normalized_name:
                try:
                    safe_name = normalized_name
                    bot.edit_forum_topic(ADMIN_GROUP_ID, thread_id, name=safe_name)
                    c.execute("UPDATE support_threads SET user_name = ? WHERE user_id = ?", (safe_name, user_id))
                    conn.commit()
                    with MIRROR_CACHE_LOCK:
                        SUPPORT_THREAD_CACHE[int(user_id)] = (thread_id, safe_name)
                    print(f"✏️ Renamed topic {thread_id} to '{safe_name}'")
                except Exception as e:
                    print(f"⚠️ Failed to rename topic: {e}")

            conn.close()
            return thread_id
            
        # Create new topic
        try:
            safe_name = format_topic_user_name(user_id, user_name=user_name)
            topic = bot.create_forum_topic(ADMIN_GROUP_ID, safe_name)
            thread_id = topic.message_thread_id
            
            # Save to DB
            c.execute("INSERT INTO support_threads (user_id, thread_id, user_name) VALUES (?, ?, ?)", 
                      (user_id, thread_id, safe_name))
            conn.commit()
            with MIRROR_CACHE_LOCK:
                SUPPORT_THREAD_CACHE[int(user_id)] = (thread_id, safe_name)
            
            # Send initial message
            bot.send_message(ADMIN_GROUP_ID, 
                           f"🧵 **Chat**\n👤 {safe_name}\n🆔 `{user_id}`\n\nDirectly reply to messages here.", 
                           message_thread_id=thread_id,
                           parse_mode="Markdown")
                           
            conn.close()
            return thread_id
            
        except Exception as e:
            print(f"⚠️ Failed to create topic: {e}")
            conn.close()
            return None
            
    except Exception as e:
        print(f"❌ DB Error in topics: {e}")
        return None

def copy_message_with_retry(
    to_chat_id,
    from_chat_id,
    message_id,
    message_thread_id=None,
    reply_to_message_id=None,
    max_retries=5
):
    """Retry copy_message on Telegram 429 and transient API errors."""
    delay = 0.5
    for attempt in range(max_retries):
        try:
            with MIRROR_LOCK:
                return bot.copy_message(
                    to_chat_id,
                    from_chat_id,
                    message_id,
                    message_thread_id=message_thread_id,
                    reply_to_message_id=reply_to_message_id
                )
        except Exception as e:
            err = str(e)
            if attempt < max_retries - 1:
                retry_match = re.search(r'retry after\s+(\d+)', err, re.IGNORECASE)
                if retry_match:
                    wait_s = int(retry_match.group(1)) + 0.2
                else:
                    wait_s = delay
                    delay = min(delay * 2, 5.0)
                time.sleep(wait_s)
                continue
            raise

def get_mirrored_admin_message_id(chat_id, user_msg_id):
    """Get mirrored admin message ID for a source chat/message pair."""
    try:
        cache_key = (int(chat_id), int(user_msg_id))
        with MIRROR_CACHE_LOCK:
            cached = MESSAGE_MAP_CACHE.get(cache_key)
            if cached is not None:
                MESSAGE_MAP_CACHE.move_to_end(cache_key)
                return cached

        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute(
            "SELECT admin_msg_id FROM message_map WHERE chat_id = ? AND user_msg_id = ? ORDER BY admin_msg_id DESC LIMIT 1",
            (chat_id, user_msg_id)
        )
        row = c.fetchone()
        conn.close()
        if row:
            remember_message_map(chat_id, user_msg_id, row[0])
            return row[0]
        return None
    except Exception as e:
        print(f"⚠️ Failed reply-map lookup ({chat_id}/{user_msg_id}): {e}")
        return None

def _execute_mirror_task(task):
    """Copy one message into admin topic and preserve reply links when possible."""
    user_id = task['user_id']
    user_name = task.get('user_name')
    existing_admin_msg_id = get_mirrored_admin_message_id(task['source_chat_id'], task['source_message_id'])
    if existing_admin_msg_id:
        return

    for topic_attempt in range(2):
        thread_id = get_or_create_topic(user_id, user_name)
        if not thread_id:
            raise RuntimeError(f"No admin topic available for user {user_id}")

        admin_reply_to_id = None
        source_reply_to_id = task.get('source_reply_to_id')
        if source_reply_to_id:
            admin_reply_to_id = get_mirrored_admin_message_id(user_id, source_reply_to_id)

        try:
            admin_msg = copy_message_with_retry(
                ADMIN_GROUP_ID,
                task['source_chat_id'],
                task['source_message_id'],
                message_thread_id=thread_id,
                reply_to_message_id=admin_reply_to_id
            )
        except Exception as e:
            if admin_reply_to_id and _looks_like_missing_reply_error(e):
                admin_msg = copy_message_with_retry(
                    ADMIN_GROUP_ID,
                    task['source_chat_id'],
                    task['source_message_id'],
                    message_thread_id=thread_id,
                    reply_to_message_id=None
                )
            elif topic_attempt == 0 and _looks_like_missing_topic_error(e):
                print(f"⚠️ Mirror topic stale for user {user_id}; recreating topic.")
                invalidate_support_topic(user_id, thread_id=thread_id, drop_db_mapping=True)
                continue
            else:
                raise

        if admin_msg:
            save_message_map(admin_msg.message_id, task['source_message_id'], user_id)
        return

    raise RuntimeError(f"Mirror task exhausted topic recovery for user {user_id}")

def _mirror_worker(user_id):
    """Per-user worker to keep mirrored messages in source-message order."""
    while True:
        # Small grace window to collect near-simultaneous events and sort by source message_id.
        time.sleep(MIRROR_REORDER_GRACE_SECONDS)

        with MIRROR_QUEUE_LOCK:
            pending = MIRROR_QUEUE_BY_USER.get(user_id)
            if not pending:
                MIRROR_QUEUE_BY_USER.pop(user_id, None)
                MIRROR_WORKERS.pop(user_id, None)
                return

            pending.sort(key=lambda t: (t['source_message_id'], t['enqueued_at']))
            task = pending.pop(0)
            MIRROR_ENQUEUED_TASK_IDS.discard(task.get('task_id'))

        wait_until = float(task.get('not_before') or 0)
        if wait_until > time.time():
            time.sleep(min(wait_until - time.time(), MIRROR_QUEUE_RECHECK_SECONDS))
            _queue_mirror_task(task)
            continue

        try:
            _execute_mirror_task(task)
            delete_mirror_task(task['task_id'])
        except Exception as e:
            print(f"⚠️ Mirror queue task failed for {user_id}: {e}")
            task['attempt_count'] = int(task.get('attempt_count', 0)) + 1
            task['not_before'] = reschedule_mirror_task(task['task_id'], e, task['attempt_count'])
            task['enqueued_at'] = time.time()
            _queue_mirror_task(task)

def enqueue_mirror_message(user_id, source_chat_id, source_message_id, user_name=None, source_reply_to_id=None):
    """
    Queue a mirror task so each user/topic is mirrored in original message order.
    """
    task = persist_mirror_task(
        user_id=user_id,
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        user_name=user_name,
        source_reply_to_id=source_reply_to_id
    )
    if task:
        _queue_mirror_task(task)

def mirror_to_topic(user_id, message_obj, user_name=None):
    """
    Mirror a bot's sent message to the admin topic so admins can see what the bot sent.
    """
    if not message_obj: return
    try:
        if not should_mirror_user(user_id):
            return

        # 1. Try explicit name
        # 2. Try extraction from message target (private chat)
        if not user_name:
            if message_obj.chat.type == 'private':
                user_name = format_topic_user_name(
                    user_id,
                    first_name=message_obj.chat.first_name,
                    last_name=message_obj.chat.last_name
                )

        source_reply_to_id = None
        if getattr(message_obj, 'reply_to_message', None):
            source_reply_to_id = message_obj.reply_to_message.message_id

        enqueue_mirror_message(
            user_id=user_id,
            source_chat_id=message_obj.chat.id,
            source_message_id=message_obj.message_id,
            user_name=user_name,
            source_reply_to_id=source_reply_to_id
        )

    except Exception as e:
        print(f"⚠️ Mirror failed: {e}")

def save_message_map(admin_msg_id, user_msg_id, chat_id):
    """Link an admin group message to a user chat message"""
    try:
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO message_map VALUES (?, ?, ?)", (admin_msg_id, user_msg_id, chat_id))
        conn.commit()
        conn.close()
        remember_message_map(chat_id, user_msg_id, admin_msg_id)
    except Exception as e:
        print(f"❌ Map Error: {e}")

# --- REACTION MIRRORING (Admin -> User) ---
@bot.message_reaction_handler(func=lambda message: True)
def handle_reactions(reaction_event):
    """Mirror admin reactions to user"""
    try:
        print(f"🔔 Reaction Event: {reaction_event.chat.id} -> Msg {reaction_event.message_id}")
        
        # Only mirror from Admin Group
        if reaction_event.chat.id != ADMIN_GROUP_ID: 
            # print("  -> Not admin group")
            return
        
        # Get mapping
        db_path = os.path.join(BASE_DIR, 'tickets_test.db')
        conn = sqlite3.connect(db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT user_msg_id, chat_id FROM message_map WHERE admin_msg_id = ?", (reaction_event.message_id,))
        row = c.fetchone()
        conn.close()
        
        if row:
            user_msg_id, user_chat_id = row
            print(f"  -> Found Map! Mirroring reaction to User {user_chat_id} Msg {user_msg_id}")
            # Apply reaction to user message
            # NOTE: new_reaction is a list of ReactionTypeEmoji/CustomEmoji, or empty list if removed
            bot.set_message_reaction(user_chat_id, user_msg_id, reaction_event.new_reaction, is_big=False)
        else:
            print(f"  -> No map found for admin msg {reaction_event.message_id}")
            
    except Exception as e:
        print(f"Reaction mirror error: {e}")

# --- ADMIN CHAT MIRROR (Support Mode - Topics) ---
@bot.message_handler(func=lambda m: m.chat.type == 'private' and (m.text is None or not m.text.startswith('/')), content_types=['text', 'photo', 'voice', 'video', 'document', 'sticker'])
def forward_to_admin(message):
    """Forward user messages to their specific topic"""
    try:
        manual_followup = get_active_manual_receipt_followup(message.from_user.id)
        if manual_followup:
            if message.text and message.text.strip().lower() in ('cancelar', 'cancel'):
                complete_receipt_followup(manual_followup['id'], status="IGNORED", release_hash=True)
                bot.reply_to(message, "Verificación manual cancelada.")
                return

            if not message.text:
                bot.reply_to(message, "Envía ese dato en texto para terminar la verificación manual.")
                return

            if manual_followup['status'] == 'MANUAL_WAIT_AMOUNT':
                amount = normalize_manual_amount(message.text)
                if amount is None:
                    bot.reply_to(message, "Monto inválido. Envía algo como 15.00.")
                    return
                update_receipt_followup(
                    manual_followup['id'],
                    status='MANUAL_WAIT_CONFIRMATION',
                    manual_amount=amount
                )
                bot.reply_to(message, "Ahora envía el código de confirmación (ej: `#ABCDE-12345678` o `ABCDE`).", parse_mode="Markdown")
                return

            if manual_followup['status'] == 'MANUAL_WAIT_CONFIRMATION':
                confirmation, confirmation_full = normalize_manual_confirmation(message.text)
                if not confirmation:
                    attempts = (manual_followup.get('manual_attempts') or 0) + 1
                    if attempts >= 3:
                        complete_receipt_followup(manual_followup['id'], status="IGNORED", release_hash=True)
                        bot.reply_to(message, "Comprobante descartado tras 3 intentos inválidos.")
                    else:
                        update_receipt_followup(manual_followup['id'], manual_attempts=attempts)
                        bot.reply_to(message, f"Código inválido. Envía 5 letras, con o sin el número final. ({attempts}/3)")
                    return

                # If amount and time are already known (from OCR via "Corregir confirmación"),
                # skip the time step and go straight to verification
                if manual_followup.get('manual_amount') and manual_followup.get('manual_receipt_time') is not None:
                    bot.reply_to(message, "Recibido, verificando...")
                    update_receipt_followup(
                        manual_followup['id'],
                        manual_confirmation=confirmation,
                        manual_confirmation_full=confirmation_full
                    )
                    refreshed_followup = get_receipt_followup(manual_followup['id']) or manual_followup
                    process_manual_receipt_submission(
                        refreshed_followup,
                        amount=refreshed_followup.get('manual_amount'),
                        confirmation=confirmation,
                        confirmation_full=confirmation_full,
                        receipt_time=refreshed_followup.get('manual_receipt_time') or None
                    )
                    return

                # Amount missing — ask for it before proceeding
                if not manual_followup.get('manual_amount'):
                    update_receipt_followup(
                        manual_followup['id'],
                        status='MANUAL_WAIT_AMOUNT',
                        manual_confirmation=confirmation,
                        manual_confirmation_full=confirmation_full
                    )
                    bot.reply_to(message, "Ahora envía el monto del comprobante (ej: `15.00`).", parse_mode="Markdown")
                    return

                update_receipt_followup(
                    manual_followup['id'],
                    status='MANUAL_WAIT_TIME',
                    manual_confirmation=confirmation,
                    manual_confirmation_full=confirmation_full
                )
                bot.reply_to(message, "Ahora envía la hora del comprobante (ej: `1:06`) o escribe `saltar`.", parse_mode="Markdown")
                return

            if manual_followup['status'] == 'MANUAL_WAIT_TIME':
                receipt_time = normalize_manual_receipt_time(message.text)
                if receipt_time is None:
                    bot.reply_to(message, "Hora inválida. Envía algo como 1:06 o escribe `saltar`.", parse_mode="Markdown")
                    return

                update_receipt_followup(
                    manual_followup['id'],
                    manual_receipt_time=receipt_time or None
                )
                refreshed_followup = get_receipt_followup(manual_followup['id']) or manual_followup
                process_manual_receipt_submission(
                    refreshed_followup,
                    amount=refreshed_followup.get('manual_amount'),
                    confirmation=refreshed_followup.get('manual_confirmation'),
                    confirmation_full=refreshed_followup.get('manual_confirmation_full'),
                    receipt_time=receipt_time or None
                )
                return

        if is_lista_waiting(message.from_user.id):
            if message.text and message.text.strip().lower() in ('cancelar', 'cancel'):
                clear_lista_waiting_state(message.from_user.id)
                bot.reply_to(message, "Entrada de /lista cancelada.")
                return
            if not message.text:
                bot.reply_to(message, "Envía la lista en texto para continuar con /lista.")
                return
            process_manual_lista_text(message, message.text)
            return

        if is_admin_user(message.from_user.id):
            return

        user_name = format_topic_user_name(
            message.from_user.id,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name
        )

        source_reply_to_id = None
        if message.reply_to_message:
            source_reply_to_id = message.reply_to_message.message_id

        enqueue_mirror_message(
            user_id=message.from_user.id,
            source_chat_id=message.chat.id,
            source_message_id=message.message_id,
            user_name=user_name,
            source_reply_to_id=source_reply_to_id
        )

    except Exception as e:
        print(f"Error forwarding message: {e}")




# 🔥 FIX: CTRL+C SUPPORT 🔥
def check_receipt_followup_reminders():
    """Background thread to remind users about unresolved receipt followups."""
    print("⏳ Receipt Followup Reminder Thread Started")

    while True:
        try:
            time.sleep(RECEIPT_REMINDER_POLL_SECONDS)
            conn = get_yappy_db()
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute(
                '''SELECT * FROM receipt_followups
                   WHERE status = 'OPEN'
                     AND reminder_sent = 0
                     AND remind_at IS NOT NULL
                     AND datetime(remind_at) <= datetime('now')'''
            )
            due_followups = [dict(row) for row in c.fetchall()]
            conn.close()

            for followup in due_followups:
                latest = get_receipt_followup(followup['id'])
                if not latest or latest.get('status') != 'OPEN' or latest.get('reminder_sent'):
                    continue

                reminder_text = "⏳ Por favor verifica este comprobante manualmente o márcalo como 'No es comprobante'."
                try:
                    if latest.get('action_message_id'):
                        try:
                            bot.send_message(
                                latest['chat_id'],
                                reminder_text,
                                reply_to_message_id=latest['action_message_id']
                            )
                        except Exception:
                            bot.send_message(latest['chat_id'], reminder_text)
                    else:
                        bot.send_message(latest['chat_id'], reminder_text)
                    update_receipt_followup(latest['id'], reminder_sent=1)
                except Exception as e:
                    print(f"⚠️ Receipt reminder failed ({latest['id']}): {e}")

        except Exception as e:
            print(f"❌ Receipt reminder loop error: {e}")
            time.sleep(RECEIPT_REMINDER_POLL_SECONDS)


def check_deadlines():
    """Background thread to invalidate unpaid tickets after deadline"""
    import threading
    import datetime
    
    print("⏳ Deadline Enforcement Thread Started")
    
    while True:
        try:
            time.sleep(60) # Check every minute
            
            # Use a new connection for thread safety
            db_path = os.path.join(BASE_DIR, 'tickets_test.db')
            # FIX: Increase timeout to prevent "Database is locked" errors
            conn = sqlite3.connect(db_path, timeout=30.0)
            c = conn.cursor()
            
            # Include notif_stage in query
            c.execute("SELECT id, user_id, date, lottery_type, amount_paid, notif_stage FROM tickets_v3 WHERE status = 'PENDING'")
            rows = c.fetchall()
            
            now_panama = datetime.datetime.now(PANAMA_TZ)
            today_str = now_panama.strftime("%Y-%m-%d")
            
            for t_id, user_id, t_date, t_type, paid, notif_lvl in rows:
                # Admin users never get invalidated or deadline warnings
                if is_admin_user(user_id):
                    continue

                if notif_lvl is None: notif_lvl = 0

                should_invalidate = False
                reason = "Tiempo expirado"
                minutes_remaining = 9999
                
                # ... (Date/Time parsing logic same as before to get deadline_dt) ...
                deadline_dt = None
                
                # 1. Check Date (Past dates are invalid)
                if t_date < today_str:
                    should_invalidate = True
                    reason = "Fecha expirada"
                
                # 2. Check Time (Only if Today)
                elif t_date == today_str:
                    deadline_str = None

                    # A. Try Regex — extract embedded time from lottery_type (e.g., "Nica 1:00 pm")
                    match_time = re.search(r'(\d{1,2}:\d{2})\s?((?:am|pm|AM|PM)?)', t_type)
                    if match_time and (match_time.group(2) or int(match_time.group(1).split(':')[0]) >= 12 or int(match_time.group(1).split(':')[0]) == 0):
                        time_part = match_time.group(1)
                        if match_time.group(2): time_part += " " + match_time.group(2).upper()
                        deadline_str = f"{t_date} {time_part}"

                    # B. Fallback: Use LOTTERY_SCHEDULE to find the nearest upcoming time
                    if not deadline_str:
                        for sched_name, sched_times in LOTTERY_SCHEDULE.items():
                            if sched_name.lower() in t_type.lower():
                                # Pick the nearest upcoming time for today
                                for st in sched_times:
                                    candidate_str = f"{t_date} {st}"
                                    try:
                                        cdt = datetime.datetime.strptime(candidate_str, "%Y-%m-%d %I:%M %p")
                                        cdt = PANAMA_TZ.localize(cdt)
                                        if cdt > now_panama or (now_panama - cdt).total_seconds() < 3600:
                                            deadline_str = candidate_str
                                            break
                                    except ValueError:
                                        continue
                                if deadline_str:
                                    break

                    if deadline_str:
                        try:
                            # Try parsing
                            for fmt in ["%Y-%m-%d %I:%M %p", "%Y-%m-%d %H:%M"]:
                                try:
                                    dt = datetime.datetime.strptime(deadline_str, fmt)
                                    deadline_dt = PANAMA_TZ.localize(dt)
                                    break
                                except ValueError:
                                    continue

                            if deadline_dt:
                                # Calculate time remaining
                                delta = deadline_dt - now_panama
                                mins = delta.total_seconds() / 60

                                # --- NOTIFICATION LOGIC ---
                                warning_msg = "⚠️ Por favor enviar comprobante antes de la hora para que la lista sea válida."
                                new_stage = notif_lvl

                                # Stage 1: ~20 min warning
                                if 0 < mins <= 20 and notif_lvl < 1:
                                    try:
                                        bot.send_message(user_id, f"⏳ **Recordatorio Ticket #{t_id}**\nFaltan {int(mins)} min.\n{warning_msg}", parse_mode="Markdown")
                                    except Exception as e:
                                        print(f"⚠️ Reminder send failed ticket #{t_id} stage 1: {e}")
                                    new_stage = 1

                                # Stage 2: ~15 min warning
                                elif 0 < mins <= 15 and notif_lvl < 2:
                                    try:
                                        bot.send_message(user_id, f"⏳ **URGENTE Ticket #{t_id}**\nFaltan {int(mins)} min.\n{warning_msg}", parse_mode="Markdown")
                                    except Exception as e:
                                        print(f"⚠️ Reminder send failed ticket #{t_id} stage 2: {e}")
                                    new_stage = 2

                                # Stage 3: ~5 min warning
                                elif 0 < mins <= 5 and notif_lvl < 3:
                                    try:
                                        bot.send_message(user_id, f"🚨 **ÚLTIMO AVISO Ticket #{t_id}**\nFaltan {int(mins)} min.\n{warning_msg}", parse_mode="Markdown")
                                    except Exception as e:
                                        print(f"⚠️ Reminder send failed ticket #{t_id} stage 3: {e}")
                                    new_stage = 3

                                # Update DB if stage changed
                                if new_stage != notif_lvl:
                                    c.execute("UPDATE tickets_v3 SET notif_stage = ? WHERE id = ?", (new_stage, t_id))
                                    conn.commit()

                                # Check Expiration (with Grace Period)
                                limit = deadline_dt + datetime.timedelta(minutes=DEADLINE_GRACE_MINUTES)
                                if now_panama > limit:
                                    should_invalidate = True
                                    reason = "Lista no válida por falta de comprobante"

                        except Exception as e:
                            print(f"⚠️ Date parse error for {t_type}: {e}")
                
                if should_invalidate:
                    print(f"🚫 Invalidating Ticket #{t_id}: {reason}")
                    
                    # Refund to Wallet if paid > 0
                    if paid > 0:
                        try:
                            c.execute("INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 0)", (user_id,))
                            c.execute("UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?", (paid, user_id))
                        except Exception as e:
                            print(f"Wallet refund error: {e}")
                    
                    # Mark Invalid
                    c.execute("UPDATE tickets_v3 SET status = 'INVALID' WHERE id = ?", (t_id,))
                    conn.commit()
                    
                    # Notify User and Admin
                    msg = f"🚫 *Ticket #{t_id} Anulado*\n📝 Razón: {reason}\n"
                    if paid > 0:
                        new_bal = get_wallet_balance(user_id)
                        msg += f"💰 Reembolso: +${paid:.2f}\n💰 Fondo: ${new_bal:.2f}"
                    
                    # Mirror to Admin Topic
                    try:
                        sent_inv = bot.send_message(user_id, msg, parse_mode="Markdown")
                        mirror_to_topic(user_id, sent_inv)
                    except Exception as ex:
                        print(f"Failed to notify/mirror invalidation for user {user_id}: {ex}")

            conn.close()
            
        except Exception as e:
            print(f"❌ Deadline Loop Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    import threading
    
    # Start Deadline Thread
    t = threading.Thread(target=check_deadlines, daemon=True)
    t.start()
    
    # Clean up leftover temp files from previous runs
    import glob as _glob
    _stale = _glob.glob(os.path.join(BASE_DIR, "temp_yappy_*.jpg"))
    if _stale:
        for _f in _stale:
            try: os.remove(_f)
            except Exception: pass
        print(f"🧹 Cleaned up {len(_stale)} leftover temp file(s)")

    # Start OCR Worker Thread
    t_ocr = threading.Thread(target=worker_ocr, daemon=True)
    t_ocr.start()

    # Start Receipt Reminder Thread
    t_receipt = threading.Thread(target=check_receipt_followup_reminders, daemon=True)
    t_receipt.start()

    # Start Bridge Consumer Thread
    if YAPPY_BRIDGE_ENABLED:
        t_bridge = threading.Thread(target=bridge_ingest_worker, daemon=True)
        t_bridge.start()

    load_pending_mirror_tasks()
    refresh_open_receipt_followup_markups()

    # NOTE: Bridge reconcile and stuck-monitor threads removed.
    # Lot bot now reacts directly via Telegram API — no reaction tables needed.
    
    print(">>> BOT READY (OPTIMIZED SPEED + REFRESH URL) <<<")
    while True:
        try:
            # timeout=90 keeps connection open
            # allowed_updates=[] gets everything
            # skip_pending=False ensures we get messages sent while bot was restarting
            bot.infinity_polling(timeout=90, long_polling_timeout=5, skip_pending=False, allowed_updates=[])
        except (KeyboardInterrupt, SystemExit):
            print("🛑 Bot stopped by user (Ctrl+C).")
            break
        except (ConnectionError, ReadTimeout, requests.exceptions.ChunkedEncodingError) as e:
            print(f"⚠️ Network blink: {e}")
            time.sleep(5)
        except Exception as e:
            # Handle connection resets gracefully without full traceback
            if "Connection reset by peer" in str(e) or "Connection aborted" in str(e):
                 print(f"⚠️ Connection reset/aborted. Retrying...")
                 time.sleep(2)
                 continue
            
            print(f"❌ Critical Error: {e}")
            time.sleep(5)
