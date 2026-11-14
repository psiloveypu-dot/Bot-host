import os
import sqlite3
import asyncio
import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.request import HTTPXRequest

# ══════════════════════════════════════════════════════
# ⚙️ ADMIN BOT CONFIG
# ══════════════════════════════════════════════════════
ADMIN_BOT_TOKEN = "9240293654:AAEeIziEyXG3zZ-87MaDk5zLH8AmblfPg2c"
OWNER_ID = 8866149676
DATA_DIR = "./data"
DB_PATH = os.path.join(DATA_DIR, "users.db")

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

# ══════════════════════════════════════════════════════
# 🎛 KEYBOARDS
# ══════════════════════════════════════════════════════
def get_admin_kb():
    return ReplyKeyboardMarkup([
        [KeyboardButton("📊 STATS"), KeyboardButton("👥 USERS")],
        [KeyboardButton("➕ GIVE ACCESS"), KeyboardButton("🚫 BAN USER")],
        [KeyboardButton("📢 BROADCAST")]
    ], resize_keyboard=True)

def get_days_kb(target_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("1 DAY", callback_data=f"set_1_{target_id}"), InlineKeyboardButton("7 DAYS", callback_data=f"set_7_{target_id}")],
        [InlineKeyboardButton("30 DAYS", callback_data=f"set_30_{target_id}"), InlineKeyboardButton("LIFETIME", callback_data=f"set_life_{target_id}")]
    ])

# ══════════════════════════════════════════════════════
# 🚀 ADMIN HANDLERS
# ══════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("⛔ Unauthorized")
    await update.message.reply_text("👑 <b>ADMIN CONTROL PANEL</b>", reply_markup=get_admin_kb(), parse_mode="HTML")

async def handle_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID: return
    text = update.message.text
    
    if text == "📊 STATS":
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM users"); total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE banned=1"); banned = c.fetchone()[0]
        conn.close()
        return await update.message.reply_text(f"📊 <b>BOT STATISTICS</b>\n\n👥 Total Users: {total}\n🚫 Banned: {banned}", parse_mode="HTML")

    elif text == "👥 USERS":
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id, name, expiry, banned FROM users LIMIT 30")
        rows = c.fetchall()
        conn.close()
        out = "👥 <b>RECENT USERS</b>\n\n"
        for r in rows:
            st = "🚫" if r[3] else "✅"
            out += f"{st} <a href='tg://user?id={r[0]}'>{r[1]}</a> (<code>{r[0]}</code>) | Exp: {r[2] or 'None'}\n"
        return await update.message.reply_text(out, parse_mode="HTML")

    elif text == "➕ GIVE ACCESS":
        ctx.user_data["action"] = "give_access"
        return await update.message.reply_text("🔍 <b>ENTER USER ID TO GRANT ACCESS:</b>", parse_mode="HTML")
        
    elif text == "🚫 BAN USER":
        ctx.user_data["action"] = "ban"
        return await update.message.reply_text("🚫 <b>ENTER USER ID TO BAN:</b>", parse_mode="HTML")
        
    elif text == "📢 BROADCAST":
        ctx.user_data["action"] = "broadcast"
        return await update.message.reply_text("📢 <b>ENTER BROADCAST MESSAGE:</b>", parse_mode="HTML")

    # Handle Inputs based on Action
    action = ctx.user_data.get("action")
    if action == "give_access":
        try:
            tid = int(text)
            await update.message.reply_text(f"👤 User <code>{tid}</code> found.\n\n⏳ <b>Select Duration:</b>", reply_markup=get_days_kb(tid), parse_mode="HTML")
            ctx.user_data["action"] = None
        except: await update.message.reply_text("❌ <b>INVALID ID!</b>", parse_mode="HTML")
        
    elif action == "ban":
        try:
            tid = int(text)
            conn = get_db()
            conn.execute("UPDATE users SET banned=1, expiry=NULL WHERE user_id=?", (tid,))
            conn.commit()
            conn.close()
            await update.message.reply_text(f"🚫 <b>USER BANNED!</b>\n\nUser {tid} has been banned.", parse_mode="HTML")
            ctx.user_data["action"] = None
        except: await update.message.reply_text("❌ <b>INVALID ID!</b>", parse_mode="HTML")
        
    elif action == "broadcast":
        ctx.user_data["action"] = None
        await broadcast_message(update.message, ctx)

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.from_user.id != OWNER_ID: return
    
    if q.data.startswith("set_"):
        parts = q.data.split("_")
        days = parts[1]
        tid = int(parts[2])
        
        expiry = "lifetime" if days == "life" else (datetime.now() + timedelta(days=int(days))).strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db()
        conn.execute("UPDATE users SET expiry=?, banned=0 WHERE user_id=?", (expiry, tid))
        conn.commit()
        conn.close()
        
        await q.edit_message_text(f"✅ <b>ACCESS GRANTED!</b>\n\nUser: <code>{tid}</code>\nExpiry: {expiry}", parse_mode="HTML")

# ══════════════════════════════════════════════════════
# 🚀 FAST BROADCAST SYSTEM (ANTI-FREEZE)
# ══════════════════════════════════════════════════════
async def broadcast_message(message, ctx):
    conn = get_db()
    users = [row[0] for row in conn.execute("SELECT user_id FROM users WHERE banned=0")]
    conn.close()
    
    if not users: return await message.reply_text("No users found.")
    
    msg = await message.reply_text(f"⏳ <b>Broadcasting to {len(users)} users...</b>", parse_mode="HTML")
    
    success, fail = 0, 0
    sem = asyncio.Semaphore(25) # Ek saath 25 messages bhejega (Super Fast)
    
    async def send(uid):
        nonlocal success, fail
        async with sem:
            try:
                await message.copy_to(chat_id=uid)
                success += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.05) 
            
    await asyncio.gather(*(send(u) for u in users))
    
    await msg.edit_text(f"✅ <b>BROADCAST COMPLETE!</b>\n\n🟢 Success: {success}\n🔴 Failed: {fail}", parse_mode="HTML")

# ══════════════════════════════════════════════════════
# 🚀 MAIN RUNNER
# ══════════════════════════════════════════════════════
async def main():
    app = Application.builder().token(ADMIN_BOT_TOKEN).request(HTTPXRequest(connect_timeout=30)).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin))
    
    await app.bot.delete_webhook(drop_pending_updates=True)
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    logger.info("Admin Bot Started!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
