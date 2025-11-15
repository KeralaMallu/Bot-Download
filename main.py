# main.py (V4.5) - KeralaCaptain File Sender Bot (Fixed start link handling)
"""
Full, standalone bot script (V4.5).
- Robust /start parsing (handles /start <token>, /start=<token>, etc.)
- Atomic single-use token claim at send-time (prevents pre-mark race)
- Always replies on errors (no silent failures)
- Admin panel, broadcast, force-sub, stats, web server, DB fallback
- All user-facing messages use HTML parse mode
"""

import os
import re
import time
import base64
import hmac
import hashlib
import asyncio
import logging
import sys
import signal
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, unquote

from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import (
    FloodWait,
    FileReferenceExpired,
    UserIsBlocked,
    PeerIdInvalid,
    UserNotParticipant,
    MessageNotModified,
    RPCError
)
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

# pymongo helpers for atomic updates
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

# Load environment
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# -----------------------------
# Configuration
# -----------------------------
class Config:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

    raw_admins = os.environ.get("ADMIN_IDS", "").strip()
    if raw_admins:
        # accept comma and/or space separated list
        ADMIN_IDS = [int(x.strip()) for x in re.split(r"[,\s]+", raw_admins) if x.strip()]
    else:
        ADMIN_IDS = []

    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))

    KC_LINK_SECRET = os.environ.get("KC_LINK_SECRET", "KCS3cR3t_v4_d3f6a8b1e9c2a5d4e7f8b1a3c5e")

    TOKEN_EXPIRY_SECONDS = int(os.environ.get("TOKEN_EXPIRY_SECONDS", 1800))  # default 30m
    FILE_DELETE_HOURS = int(os.environ.get("FILE_DELETE_HOURS", 12))

    WEBSITE_URL = os.environ.get("WEBSITE_URL", "https://www.keralacaptain.shop")
    WEBSITE_BUTTON_TEXT = os.environ.get("WEBSITE_BUTTON_TEXT", "Download Movie")

    PORT = int(os.environ.get("PORT", 8080))

# Validate essential config early
if not (Config.API_ID and Config.API_HASH and Config.BOT_TOKEN and Config.MONGO_URI and Config.LOG_CHANNEL_ID and Config.ADMIN_IDS):
    LOGGER.critical("FATAL: Missing required environment variables. Please set API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, ADMIN_IDS")
    if Config.KC_LINK_SECRET == "KCS3cR3t_v4_d3f6a8b1e9c2a5d4e7f8b1a3c5e":
        LOGGER.warning("WARNING: Using default KC_LINK_SECRET. Change it in environment.")
    sys.exit(1)

# -----------------------------
# MongoDB connection and collections
# -----------------------------
try:
    db_client = AsyncIOMotorClient(Config.MONGO_URI)
    try:
        db = db_client.get_default_database()
        if db is None:
            raise Exception("No default DB found in URI")
    except Exception:
        parsed = urlparse(Config.MONGO_URI)
        path = parsed.path or ""
        if path.startswith("/"):
            dbname = unquote(path[1:].split("/", 1)[0].split("?", 1)[0])
            db = db_client[dbname] if dbname else db_client['KeralaCaptainBotDB']
        else:
            db = db_client['KeralaCaptainBotDB']
    LOGGER.info(f"Connected to MongoDB: {db.name}")
except Exception as e:
    LOGGER.critical(f"Could not connect to MongoDB: {e}", exc_info=True)
    sys.exit(1)

# Collections
media_collection = db['media']
users_col = db['users']
sent_files_log_col = db['sent_files_log']
settings_col = db['bot_settings']
conversations_col = db['conversations']
tokens_col = db['issued_tokens']  # used for single-use token enforcement

# In-memory settings
SETTINGS = {}

# Pyrogram bot client
bot = Client(
    name="KeralaCaptainSenderV4_5",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)

start_time = time.time()

# -----------------------------
# Database indices (create on startup)
# -----------------------------
async def create_db_indices():
    try:
        # Index to let Mongo auto-delete expired send logs (if you want TTL)
        # This uses expireAfterSeconds on a datetime field (works if 'delete_at' exists).
        # Other logic also removes records after deletion.
        await sent_files_log_col.create_index("delete_at", expireAfterSeconds=0)
        # Unique index for token hash
        await tokens_col.create_index("hash", unique=True)
        await users_col.create_index("last_count_reset")
        await media_collection.create_index("wp_post_id")
        LOGGER.info("DB indices ensured.")
    except Exception as e:
        LOGGER.error(f"Error creating indices: {e}", exc_info=True)

# -----------------------------
# Settings handling
# -----------------------------
async def load_settings_from_db():
    global SETTINGS
    try:
        doc = await settings_col.find_one({"_id": "bot_config"})
        if doc:
            SETTINGS = doc
        else:
            default_settings = {
                "_id": "bot_config",
                "force_sub_enabled": False,
                "force_sub_channel_id": 0,
                "protect_content_enabled": True,
                "daily_limit": 5,
                "file_delete_hours": Config.FILE_DELETE_HOURS,
                "token_expiry_seconds": Config.TOKEN_EXPIRY_SECONDS,
                "single_use_tokens": True
            }
            await settings_col.insert_one(default_settings)
            SETTINGS = default_settings
        # override with environment-critical values
        SETTINGS["file_delete_hours"] = Config.FILE_DELETE_HOURS
        SETTINGS["token_expiry_seconds"] = Config.TOKEN_EXPIRY_SECONDS
        LOGGER.info("Settings loaded.")
    except Exception as e:
        LOGGER.error(f"Could not load settings: {e}", exc_info=True)
        SETTINGS.update({
            "force_sub_enabled": False,
            "force_sub_channel_id": 0,
            "protect_content_enabled": True,
            "daily_limit": 5,
            "file_delete_hours": Config.FILE_DELETE_HOURS,
            "token_expiry_seconds": Config.TOKEN_EXPIRY_SECONDS,
            "single_use_tokens": True
        })

async def update_db_setting(key: str, value):
    SETTINGS[key] = value
    try:
        await settings_col.update_one({"_id": "bot_config"}, {"$set": {key: value}}, upsert=True)
    except Exception as e:
        LOGGER.error(f"Failed to update setting {key}: {e}", exc_info=True)

# -----------------------------
# User helpers
# -----------------------------
async def get_user_data(user_id: int):
    user_data = await users_col.find_one({"_id": user_id})
    if not user_data:
        new_user = {
            "_id": user_id,
            "join_date": datetime.now(timezone.utc),
            "daily_file_count": 0,
            "last_count_reset": datetime.now(timezone.utc).date().isoformat(),
            "is_banned": False
        }
        try:
            await users_col.insert_one(new_user)
            user_data = new_user
        except Exception:
            user_data = await users_col.find_one({"_id": user_id})
    today = datetime.now(timezone.utc).date()
    if user_data.get("last_count_reset") != today.isoformat():
        user_data["daily_file_count"] = 0
        user_data["last_count_reset"] = today.isoformat()
        try:
            await users_col.update_one({"_id": user_id}, {"$set": {"daily_file_count": 0, "last_count_reset": today.isoformat()}})
        except Exception as e:
            LOGGER.error(f"Failed to reset daily count for {user_id}: {e}", exc_info=True)
    return user_data

async def update_user_data(user_id: int, update_query: dict):
    try:
        await users_col.update_one({"_id": user_id}, update_query, upsert=True)
    except Exception as e:
        LOGGER.error(f"Failed to update user {user_id}: {e}", exc_info=True)

async def get_total_users_count():
    try:
        return await users_col.count_documents({})
    except Exception:
        return 0

async def get_today_users_count():
    try:
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        return await users_col.count_documents({"join_date": {"$gte": today}})
    except Exception:
        return 0

async def get_banned_users_count():
    try:
        return await users_col.count_documents({"is_banned": True})
    except Exception:
        return 0

async def get_all_user_ids():
    cursor = users_col.find({"is_banned": False}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]

# Admin conversation helpers
async def set_admin_conv(chat_id, stage):
    await conversations_col.update_one({"_id": chat_id}, {"$set": {"stage": stage}}, upsert=True)

async def get_admin_conv(chat_id):
    return await conversations_col.find_one({"_id": chat_id})

async def clear_admin_conv(chat_id):
    await conversations_col.delete_one({"_id": chat_id})

# -----------------------------
# Media helpers
# -----------------------------
async def get_media_by_post_id(post_id: int):
    try:
        return await media_collection.find_one({"wp_post_id": post_id})
    except Exception as e:
        LOGGER.error(f"Error fetching media by post_id {post_id}: {e}", exc_info=True)
        return None

async def update_media_links_in_db(post_id: int, new_message_ids, stream_link: str):
    try:
        await media_collection.update_one({"wp_post_id": post_id}, {"$set": {"message_ids": new_message_ids, "stream_link": stream_link}})
        LOGGER.info(f"Updated media links for post {post_id}")
    except Exception as e:
        LOGGER.error(f"Failed to update media links for post {post_id}: {e}", exc_info=True)

async def get_post_id_from_msg_id(msg_id: int):
    try:
        doc = await media_collection.find_one({"message_ids": {"$elemMatch": {"id": msg_id}}})
        if doc:
            return doc.get('wp_post_id')
        cursor = media_collection.find({})
        async for document in cursor:
            message_ids_data = document.get('message_ids', {})
            if isinstance(message_ids_data, dict):
                for v in message_ids_data.values():
                    if isinstance(v, int) and v == msg_id:
                        return document.get('wp_post_id')
                    if isinstance(v, dict) and v.get('id') == msg_id:
                        return document.get('wp_post_id')
        return None
    except Exception as e:
        LOGGER.error(f"Error searching for post_id from msg_id {msg_id}: {e}", exc_info=True)
        return None

# -----------------------------
# Token utilities
# -----------------------------
def _base64url_decode(s: str) -> bytes:
    padding = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + padding)

def token_hash_raw(token_raw: str) -> str:
    return hashlib.sha256(token_raw.encode()).hexdigest()

def compute_expected_hmac(payload: str) -> str:
    return hmac.new(Config.KC_LINK_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()

def parse_token_raw(token_b64: str):
    try:
        token_raw = _base64url_decode(token_b64).decode()
        return token_raw, None
    except Exception as e:
        return None, str(e)

async def verify_token_basic(token_b64: str, expected_bot_username: str):
    """
    Verify token signature, expiry, and bot target.
    Returns tuple (msg_id:int, token_raw:str) if valid, else (None, None)
    NOTE: This function DOES NOT mark token as used. Claiming is done at send-time atomically.
    """
    if not token_b64:
        return None, None
    token_raw, err = parse_token_raw(token_b64)
    if not token_raw:
        LOGGER.warning(f"Token decode error: {err}")
        return None, None
    try:
        # Split signature off
        if ":" not in token_raw:
            LOGGER.warning("Token raw malformed (no ':').")
            return None, None
        payload, sig_from_token = token_raw.rsplit(":", 1)
        expected_sig = compute_expected_hmac(payload)
        if not hmac.compare_digest(expected_sig, sig_from_token):
            LOGGER.warning("Invalid token signature.")
            return None, None
        # payload expected: msg_id:expiry:bot:nonce
        parts = payload.split(":", 3)
        if len(parts) != 4:
            LOGGER.warning("Token payload parts != 4")
            return None, None
        msg_id_s, expiry_s, bot_username, nonce = parts
        if bot_username.lower() != expected_bot_username.lower():
            LOGGER.warning(f"Token intended for different bot: {bot_username} != {expected_bot_username}")
            return None, None
        expiry_ts = int(expiry_s)
        if time.time() > expiry_ts:
            LOGGER.warning(f"Expired token for msg {msg_id_s}.")
            return None, None
        return int(msg_id_s), token_raw
    except Exception as e:
        LOGGER.error(f"Token verification error: {e}", exc_info=True)
        return None, None

# Atomic claim at send-time to enforce single-use tokens reliably
async def claim_token_atomically(token_raw: str, user_id: int) -> bool:
    """
    Try to atomically claim token for single-use. Returns True if claim successful,
    False if token already used by someone else.
    Strategy:
      - compute hash
      - try to find_one_and_update doc with used: False -> set used True
      - if no such doc, try to insert a new doc with used True (if insert fails due to duplicate, check existing doc's used)
    """
    h = token_hash_raw(token_raw)
    now = datetime.now(timezone.utc)
    try:
        # Try to update existing unused doc atomically
        filt = {"hash": h, "$or": [{"used": False}, {"used": {"$exists": False}}]}
        update = {"$set": {"used": True, "used_by": user_id, "used_at": now}}
        doc = await tokens_col.find_one_and_update(filt, update, return_document=ReturnDocument.AFTER)
        if doc:
            LOGGER.info("Token claimed via update for hash %s", h)
            return True
        # No existing doc or couldn't update; try to insert new doc (claim)
        newdoc = {
            "hash": h,
            "issued_raw": token_raw,
            "used": True,
            "used_by": user_id,
            "used_at": now,
            "issued_at": now
        }
        try:
            await tokens_col.insert_one(newdoc)
            LOGGER.info("Token claimed via insert for hash %s", h)
            return True
        except DuplicateKeyError:
            # Race: someone created a doc. Check if used
            doc = await tokens_col.find_one({"hash": h})
            if not doc:
                LOGGER.error("Race: duplicate key but no doc found for hash %s", h)
                return False
            if doc.get("used"):
                LOGGER.warning("Token already used (post-insert) for hash %s", h)
                return False
            # if doc exists and used is False, try to update it now
            updated = await tokens_col.find_one_and_update({"hash": h, "used": False}, {"$set": {"used": True, "used_by": user_id, "used_at": now}}, return_document=ReturnDocument.AFTER)
            if updated and updated.get("used"):
                LOGGER.info("Token claimed after duplicate for hash %s", h)
                return True
            LOGGER.warning("Token could not be claimed after duplicate for hash %s", h)
            return False
    except Exception as e:
        LOGGER.error(f"Error claiming token atomically: {e}", exc_info=True)
        return False

# -----------------------------
# File refresh on FileReferenceExpired
# -----------------------------
async def refresh_file_reference(bot_client: Client, expired_msg_id: int):
    LOGGER.warning(f"Attempting refresh for expired msg {expired_msg_id}")
    try:
        post_id = await get_post_id_from_msg_id(expired_msg_id)
        if not post_id:
            LOGGER.error(f"No post_id found for {expired_msg_id}")
            return
        try:
            original_msg = await bot_client.get_messages(Config.LOG_CHANNEL_ID, expired_msg_id)
        except Exception as e:
            LOGGER.error(f"Could not get original message {expired_msg_id}: {e}", exc_info=True)
            return
        if not original_msg:
            LOGGER.error("Original message missing.")
            return
        try:
            refreshed = await original_msg.forward(Config.LOG_CHANNEL_ID)
        except Exception as e:
            LOGGER.error(f"Forward failed during refresh: {e}", exc_info=True)
            return
        new_msg_id = refreshed.id
        LOGGER.info(f"Refreshed message: {expired_msg_id} -> {new_msg_id}")
        media_doc = await get_media_by_post_id(post_id)
        if not media_doc:
            LOGGER.error(f"No media doc for post {post_id}")
            return
        old_qualities = media_doc.get('message_ids', {})
        found_and_updated = False
        if isinstance(old_qualities, list):
            new_qualities = []
            for item in old_qualities:
                new_item = item.copy()
                if new_item.get('id') == expired_msg_id:
                    new_item['id'] = new_msg_id
                    found_and_updated = True
                new_qualities.append(new_item)
        elif isinstance(old_qualities, dict):
            new_qualities = old_qualities.copy()
            for quality, identifier in old_qualities.items():
                current_id = identifier if isinstance(identifier, int) else identifier.get('id')
                if current_id == expired_msg_id:
                    new_qualities[quality] = new_msg_id if isinstance(identifier, int) else {**identifier, 'id': new_msg_id}
                    found_and_updated = True
                    break
        else:
            new_qualities = old_qualities
        if found_and_updated:
            await update_media_links_in_db(post_id, new_qualities, media_doc.get('stream_link', ''))
    except Exception as e:
        LOGGER.critical(f"Unhandled error during refresh: {e}", exc_info=True)

# -----------------------------
# Logging and helper UI
# -----------------------------
def get_main_admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
        [InlineKeyboardButton("📣 Broadcast", callback_data="admin_broadcast")],
        [InlineKeyboardButton("🚫 Ban User", callback_data="admin_ban"),
         InlineKeyboardButton("✅ Unban User", callback_data="admin_unban")]
    ])

def get_settings_keyboard():
    fsub_status = "✅ ON" if SETTINGS.get("force_sub_enabled") else "❌ OFF"
    protect_status = "✅ ON" if SETTINGS.get("protect_content_enabled") else "❌ OFF"
    single_use_status = "✅ ON" if SETTINGS.get("single_use_tokens", True) else "❌ OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Force Subscribe: {fsub_status}", callback_data="admin_toggle_fsub")],
        [InlineKeyboardButton(f"Protect Content: {protect_status}", callback_data="admin_toggle_protect")],
        [InlineKeyboardButton(f"Single-Use Tokens: {single_use_status}", callback_data="admin_toggle_single_use")],
        [InlineKeyboardButton(f"Daily Limit: {SETTINGS.get('daily_limit', 5)}", callback_data="admin_set_limit")],
        [InlineKeyboardButton(f"Delete Time: {SETTINGS.get('file_delete_hours', Config.FILE_DELETE_HOURS)}h", callback_data="admin_set_delete_time")],
        [InlineKeyboardButton("Set Channel (username or -100..ID)", callback_data="admin_set_fsub_channel")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_main_menu")]
    ])

# -----------------------------
# Admin handlers
# -----------------------------
@bot.on_message(filters.command(["admin", "settings"]) & filters.private & filters.user(Config.ADMIN_IDS))
async def admin_panel_handler(client: Client, message: Message):
    await clear_admin_conv(message.from_user.id)
    me = await client.get_me()
    text = f"<b>👋 Welcome Admin!</b>\n\nThis is the control panel for @{me.username}."
    await message.reply_text(text, reply_markup=get_main_admin_keyboard(), parse_mode=enums.ParseMode.HTML)

@bot.on_callback_query(filters.user(Config.ADMIN_IDS))
async def admin_callback_handler(client: Client, cb: CallbackQuery):
    data = cb.data
    chat_id = cb.message.chat.id
    try:
        if data == "admin_main_menu":
            await clear_admin_conv(chat_id)
            try:
                await cb.message.edit_text(f"<b>👋 Welcome Admin!</b>\n\nThis is the control panel.", reply_markup=get_main_admin_keyboard(), parse_mode=enums.ParseMode.HTML)
            except MessageNotModified:
                pass

        elif data == "admin_stats":
            await cb.answer("Fetching stats...", show_alert=False)
            total_users = await get_total_users_count()
            today_users = await get_today_users_count()
            banned_users = await get_banned_users_count()
            uptime = str(timedelta(seconds=int(time.time() - start_time)))
            text = (f"<b>📊 Bot Statistics</b>\n\n"
                    f"- <b>Total Users:</b> {total_users}\n"
                    f"- <b>New Users Today:</b> {today_users}\n"
                    f"- <b>Banned Users:</b> {banned_users}\n"
                    f"- <b>Uptime:</b> {uptime}\n")
            try:
                await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]]), parse_mode=enums.ParseMode.HTML)
            except MessageNotModified:
                pass

        elif data == "admin_settings":
            await clear_admin_conv(chat_id)
            try:
                await cb.message.edit_text("⚙️ <b>Bot Settings</b>\n\nManage bot configuration here.", reply_markup=get_settings_keyboard(), parse_mode=enums.ParseMode.HTML)
            except MessageNotModified:
                pass

        elif data == "admin_toggle_fsub":
            await update_db_setting("force_sub_enabled", not SETTINGS.get("force_sub_enabled", False))
            await cb.answer(f"Force Subscribe is now {'ON' if SETTINGS.get('force_sub_enabled') else 'OFF'}", show_alert=True)
            try:
                await cb.message.edit_reply_markup(get_settings_keyboard())
            except MessageNotModified:
                pass

        elif data == "admin_toggle_protect":
            await update_db_setting("protect_content_enabled", not SETTINGS.get("protect_content_enabled", True))
            await cb.answer(f"Protect Content is now {'ON' if SETTINGS.get('protect_content_enabled') else 'OFF'}", show_alert=True)
            try:
                await cb.message.edit_reply_markup(get_settings_keyboard())
            except MessageNotModified:
                pass

        elif data == "admin_toggle_single_use":
            newv = not SETTINGS.get("single_use_tokens", True)
            await update_db_setting("single_use_tokens", newv)
            await cb.answer(f"Single-Use Tokens is now {'ON' if newv else 'OFF'}", show_alert=True)
            try:
                await cb.message.edit_reply_markup(get_settings_keyboard())
            except MessageNotModified:
                pass

        elif data in ["admin_set_limit", "admin_set_fsub_channel", "admin_set_delete_time", "admin_broadcast", "admin_ban", "admin_unban"]:
            await set_admin_conv(chat_id, data)
            prompts = {
                "admin_set_limit": "Please send the new daily limit as a number (e.g., <code>5</code>).",
                "admin_set_fsub_channel": "Please send the channel username (e.g., <code>@channelname</code>) or full channel id (e.g., <code>-1001234567890</code>).",
                "admin_set_delete_time": "Please send the new file deletion time in <b>hours</b> (e.g., <code>12</code>).",
                "admin_broadcast": "Please send the message you want to broadcast (text, photo, or video).",
                "admin_ban": "Please send the <b>User ID</b> you want to ban.",
                "admin_unban": "Please send the <b>User ID</b> you want to unban."
            }
            prompt_text = prompts.get(data, "Send input now. Send /cancel to abort.")
            try:
                await cb.message.edit_text(f"{prompt_text}\n\nSend /cancel to abort.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_main_menu")]]), parse_mode=enums.ParseMode.HTML)
                await cb.answer()
            except MessageNotModified:
                pass
        else:
            await cb.answer()
    except Exception as e:
        LOGGER.error(f"Error in admin callback handler: {e}", exc_info=True)
        try:
            await cb.answer("An error occurred.", show_alert=True)
        except Exception:
            pass

@bot.on_message(filters.command("cancel") & filters.private & filters.user(Config.ADMIN_IDS))
async def cancel_conv_handler(client: Client, message: Message):
    await clear_admin_conv(message.from_user.id)
    await message.reply_text("Operation cancelled.", parse_mode=enums.ParseMode.HTML)
    await admin_panel_handler(client, message)

@bot.on_message(filters.private & filters.user(Config.ADMIN_IDS) & ~filters.command(["start", "admin", "settings", "cancel"]))
async def admin_conv_handler(client: Client, message: Message):
    chat_id = message.from_user.id
    conv = await get_admin_conv(chat_id)
    if not conv or not conv.get("stage"):
        await message.reply_text("I don't understand. Use /admin to open the control panel.", parse_mode=enums.ParseMode.HTML)
        return

    stage = conv["stage"]
    try:
        if stage == "admin_set_limit":
            limit = int(message.text.strip())
            if limit < 0:
                raise ValueError("Invalid")
            await update_db_setting("daily_limit", limit)
            await message.reply_text(f"✅ Daily limit set to <b>{limit}</b>.", parse_mode=enums.ParseMode.HTML)

        elif stage == "admin_set_fsub_channel":
            text = message.text.strip()
            channel_ref = None
            if text.startswith("@"):
                channel_ref = text
            else:
                try:
                    numeric = int(text)
                    channel_ref = numeric
                except ValueError:
                    await message.reply_text("Invalid channel. Send @channelusername or -1001234567890.", parse_mode=enums.ParseMode.HTML)
                    return
            await update_db_setting("force_sub_channel_id", channel_ref)
            await message.reply_text(f"✅ Force Subscribe channel set to <b>{channel_ref}</b>.", parse_mode=enums.ParseMode.HTML)

        elif stage == "admin_set_delete_time":
            hours = int(message.text.strip())
            if hours < 1:
                raise ValueError("Must be >= 1")
            await update_db_setting("file_delete_hours", hours)
            await message.reply_text(f"✅ File delete time set to <b>{hours} hours</b>.", parse_mode=enums.ParseMode.HTML)

        elif stage == "admin_ban":
            user_id = int(message.text.strip())
            await update_user_data(user_id, {"$set": {"is_banned": True}})
            await message.reply_text(f"🚫 User <code>{user_id}</code> has been banned.", parse_mode=enums.ParseMode.HTML)

        elif stage == "admin_unban":
            user_id = int(message.text.strip())
            await update_user_data(user_id, {"$set": {"is_banned": False}})
            await message.reply_text(f"✅ User <code>{user_id}</code> has been unbanned.", parse_mode=enums.ParseMode.HTML)

        elif stage == "admin_broadcast":
            all_user_ids = await get_all_user_ids()
            total = len(all_user_ids)
            status_msg = await message.reply_text(f"📣 Starting broadcast to <b>{total}</b> users...", parse_mode=enums.ParseMode.HTML)
            sent_count = 0
            failed_count = 0
            DELAY_BETWEEN_MESSAGES = 0.05
            for idx, user_id in enumerate(all_user_ids):
                if user_id == chat_id:
                    continue
                try:
                    await bot.copy_message(chat_id=user_id, from_chat_id=message.chat.id, message_id=message.id)
                    sent_count += 1
                except FloodWait as e:
                    wait = int(e.x) if hasattr(e, "x") else int(getattr(e, "value", 10))
                    LOGGER.warning(f"FloodWait {wait}s during broadcast. Sleeping.")
                    await asyncio.sleep(wait + 1)
                    try:
                        await bot.copy_message(chat_id=user_id, from_chat_id=message.chat.id, message_id=message.id)
                        sent_count += 1
                    except Exception:
                        failed_count += 1
                except (UserIsBlocked, PeerIdInvalid):
                    failed_count += 1
                except Exception as e:
                    failed_count += 1
                    LOGGER.error(f"Broadcast send error to {user_id}: {e}", exc_info=True)
                if idx % 100 == 0:
                    try:
                        await status_msg.edit_text(f"📣 Broadcasting... \nSent: <b>{sent_count}</b>\nFailed: <b>{failed_count}</b>", parse_mode=enums.ParseMode.HTML)
                    except MessageNotModified:
                        pass
                await asyncio.sleep(DELAY_BETWEEN_MESSAGES)
            try:
                await status_msg.edit_text(f"✅ Broadcast complete.\nSent: <b>{sent_count}</b>\nFailed: <b>{failed_count}</b>", parse_mode=enums.ParseMode.HTML)
            except MessageNotModified:
                pass

    except ValueError:
        await message.reply_text("❌ Invalid input. Please send a valid number.", parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        LOGGER.error(f"Admin conversation error: {e}", exc_info=True)
        await message.reply_text(f"❌ An error occurred: <code>{e}</code>", parse_mode=enums.ParseMode.HTML)
    finally:
        await clear_admin_conv(chat_id)
        if stage != "admin_broadcast":
            await admin_panel_handler(client, message)

# -----------------------------
# Force-sub helper
# -----------------------------
def get_force_sub_button(channel_ref):
    # Improved, more professional text and emoji
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔔 Join our official channel to receive downloads", url=channel_ref)]]
    )

async def check_force_sub(user_id: int):
    fsub_channel = SETTINGS.get("force_sub_channel_id", 0)
    if not SETTINGS.get("force_sub_enabled") or not fsub_channel:
        return True
    try:
        await bot.get_chat_member(fsub_channel, user_id)
        return True
    except UserNotParticipant:
        return False
    except RPCError as e:
        LOGGER.error(f"Error checking force sub: {e}", exc_info=True)
        return False
    except Exception as e:
        LOGGER.error(f"Unexpected error in check_force_sub: {e}", exc_info=True)
        return False

# -----------------------------
# /start handler (robust)
# -----------------------------
# We will parse the token in a tolerant way:
# Accepts:
#  - "/start TOKEN"
#  - "/start=TOKEN"
#  - "/start@BotName TOKEN"
#  - "/start@BotName=TOKEN"
_START_RE = re.compile(r'^/start(?:@[\w_]+)?(?:[ =])?(.+)?$', flags=re.IGNORECASE)

@bot.on_message(filters.command("start") & filters.private)
async def start_command_handler(client: Client, message: Message):
    user_id = message.from_user.id

    # Admin shortcut
    if user_id in Config.ADMIN_IDS:
        await admin_panel_handler(client, message)
        return

    # Basic user checks
    user_data = await get_user_data(user_id)
    if user_data.get("is_banned", False):
        await message.reply_text("<b>❌ You are banned from using this bot.</b>", parse_mode=enums.ParseMode.HTML)
        return

    # Always log incoming start message for debugging
    LOGGER.info(f"[START] from={user_id} text={message.text!r} date={message.date}")

    # Extract token in a tolerant way
    token = None
    text = (message.text or "").strip()
    m = _START_RE.match(text)
    if m:
        token_group = m.group(1)
        if token_group:
            token = token_group.strip()
    # fallback: if message.entities includes a bot command, get the substring after the command span
    if not token:
        try:
            if message.entities:
                for ent in message.entities:
                    if ent.type == "bot_command":
                        # entity offset + length
                        start = ent.offset + ent.length
                        rest = text[start:].strip()
                        if rest:
                            token = rest
                            break
        except Exception:
            pass

    # If no token, treat as plain /start
    if not token:
        welcome_text = (
            "<b>👋 Welcome!</b>\n\n"
            "To receive downloads, please visit our website and click the download button.\n"
            f"Website: <b>{Config.WEBSITE_URL}</b>"
        )
        await message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]), parse_mode=enums.ParseMode.HTML)
        return

    # At this point we have a token string
    bot_info = await bot.get_me()
    bot_username = bot_info.username or ""
    # Verify token payload & signature (but DO NOT mark as used yet)
    msg_id_to_send, token_raw = await verify_token_basic(token, bot_username)
    if not msg_id_to_send:
        await message.reply_text(
            "<b>⏳ Link expired or invalid.</b>\n\n"
            "This download link has expired or is invalid. Please go back to the website to get a new link.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]),
            parse_mode=enums.ParseMode.HTML
        )
        return

    LOGGER.info(f"Token valid for message {msg_id_to_send}. Now enforcing force-sub/daily-limit and attempting send.")

    # Force-sub check
    if SETTINGS.get("force_sub_enabled"):
        is_joined = await check_force_sub(user_id)
        if not is_joined:
            fsub_channel = SETTINGS.get("force_sub_channel_id")
            invite_url = None
            try:
                if isinstance(fsub_channel, str) and fsub_channel.startswith("@"):
                    invite_url = f"https://t.me/{fsub_channel.lstrip('@')}"
                else:
                    channel_obj = await client.get_chat(fsub_channel)
                    if getattr(channel_obj, "invite_link", None):
                        invite_url = channel_obj.invite_link
                    elif getattr(channel_obj, "username", None):
                        invite_url = f"https://t.me/{channel_obj.username}"
                if not invite_url:
                    invite_url = Config.WEBSITE_URL
            except Exception as e:
                LOGGER.error(f"Could not build invite for force-sub: {e}", exc_info=True)
                invite_url = Config.WEBSITE_URL
            await message.reply_text(
                "<b>🔔 Subscription Required</b>\n\nYou must join our channel to receive downloads. Please join and then click the link again.",
                reply_markup=get_force_sub_button(invite_url),
                parse_mode=enums.ParseMode.HTML
            )
            return

    # Daily limit check
    daily_limit = SETTINGS.get("daily_limit", 5)
    if daily_limit > 0 and user_data.get("daily_file_count", 0) >= daily_limit:
        await message.reply_text("<b>⚠️ Daily Limit Reached</b>\n\nYou have reached your daily download limit. Please try again tomorrow.", parse_mode=enums.ParseMode.HTML)
        return

    # Try to send file; claim token atomically AFTER successful send to avoid pre-mark race.
    status_msg = await message.reply_text("<b>✅ Link verified. Sending file, please wait...</b>", parse_mode=enums.ParseMode.HTML)
    try:
        sent_file_msg = await client.copy_message(
            chat_id=user_id,
            from_chat_id=Config.LOG_CHANNEL_ID,
            message_id=msg_id_to_send,
            protect_content=SETTINGS.get("protect_content_enabled", True)
        )
        if not sent_file_msg:
            await status_msg.edit_text("<b>❌ Error: Could not send file.</b>", parse_mode=enums.ParseMode.HTML)
            return

        # Attempt to claim token atomically now that send succeeded
        claimed = True
        if SETTINGS.get("single_use_tokens", True):
            try:
                claimed = await claim_token_atomically(token_raw, user_id)
            except Exception as e:
                LOGGER.error(f"Error claiming token after send: {e}", exc_info=True)
                claimed = False

        if not claimed:
            # Token was already used by someone else — inform user and delete the message we just sent to avoid leakage
            try:
                # try to delete the message we just sent
                await bot.delete_messages(chat_id=user_id, message_ids=sent_file_msg.id)
            except Exception:
                pass
            await status_msg.edit_text("<b>❌ This link has already been used by another user.</b>\n\nPlease get a fresh link from the website.", parse_mode=enums.ParseMode.HTML)
            return

        # Log for auto-deletion & increment daily count
        await log_sent_file_for_deletion(user_id, sent_file_msg, msg_id_to_send)
        await update_user_data(user_id, {"$inc": {"daily_file_count": 1}})
        try:
            await status_msg.delete()
        except Exception:
            pass

        delete_hours = SETTINGS.get("file_delete_hours", Config.FILE_DELETE_HOURS)
        try:
            await sent_file_msg.reply_text(
                f"<b>⚠️ Download quickly</b>\n\nTo avoid copyright issues, this file will be removed in <b>{delete_hours} hours</b>.\nIf the link expires, get a fresh link from our website: {Config.WEBSITE_URL}",
                parse_mode=enums.ParseMode.HTML
            )
        except Exception:
            pass

    except FileReferenceExpired:
        LOGGER.warning(f"FileReferenceExpired for {msg_id_to_send}. Triggering refresh.")
        try:
            await status_msg.edit_text("<b>⏳ The file reference expired. Refreshing now. Please try again in 1 minute.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
        asyncio.create_task(refresh_file_reference(client, msg_id_to_send))
    except (UserIsBlocked, PeerIdInvalid):
        LOGGER.warning(f"User {user_id} has blocked the bot or peer invalid.")
        try:
            await status_msg.delete()
        except Exception:
            pass
    except FloodWait as e:
        wait = int(e.x) if hasattr(e, "x") else int(getattr(e, "value", 10))
        LOGGER.warning(f"FloodWait {wait}s while sending to {user_id}.")
        try:
            await status_msg.edit_text(f"<b>Flood wait: please wait {wait} seconds.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
        await asyncio.sleep(wait + 1)
        try:
            await status_msg.delete()
        except Exception:
            pass
    except Exception as e:
        LOGGER.error(f"Failed to send message {msg_id_to_send} to {user_id}: {e}", exc_info=True)
        try:
            await status_msg.edit_text("<b>❌ A critical error occurred. Please try again later.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass

# -----------------------------
# Log sent file and auto-delete
# -----------------------------
async def log_sent_file_for_deletion(user_id: int, message_obj: Message, original_message_id: int):
    try:
        delete_at = datetime.now(timezone.utc) + timedelta(hours=SETTINGS.get("file_delete_hours", Config.FILE_DELETE_HOURS))
        file_name = None
        if getattr(message_obj, "document", None) and getattr(message_obj.document, "file_name", None):
            file_name = message_obj.document.file_name
        elif getattr(message_obj, "audio", None) and getattr(message_obj.audio, "title", None):
            file_name = message_obj.audio.title
        elif message_obj.caption:
            file_name = message_obj.caption.strip().split("\n", 1)[0][:200]
        if not file_name:
            file_name = "your file"
        await sent_files_log_col.insert_one({
            "user_id": user_id,
            "telegram_message_id": message_obj.id,
            "log_channel_message_id": original_message_id,
            "file_name": file_name,
            "sent_at": datetime.now(timezone.utc),
            "delete_at": delete_at
        })
    except Exception as e:
        LOGGER.error(f"Failed to log sent file: {e}", exc_info=True)

# -----------------------------
# Background tasks
# -----------------------------
async def auto_delete_task():
    await asyncio.sleep(60)
    LOGGER.info("Auto-delete task started.")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            cursor = sent_files_log_col.find({"delete_at": {"$lte": now_utc}})
            async for record in cursor:
                user_id = record.get("user_id")
                tmsg_id = record.get("telegram_message_id")
                file_name = record.get("file_name", "your file")
                try:
                    try:
                        await bot.delete_messages(chat_id=user_id, message_ids=tmsg_id)
                    except (UserIsBlocked, PeerIdInvalid, MessageNotModified):
                        pass
                    except Exception as e:
                        LOGGER.error(f"Error deleting message {tmsg_id} for {user_id}: {e}", exc_info=True)
                    try:
                        notify_text = (f"Hello, your download link for the file <b>{file_name}</b> has expired. "
                                       f"To get a new link, please visit our website: <b>{Config.WEBSITE_URL}</b>")
                        await bot.send_message(user_id, notify_text, parse_mode=enums.ParseMode.HTML)
                    except (UserIsBlocked, PeerIdInvalid):
                        LOGGER.info(f"Could not send expiry notification to {user_id} (blocked/invalid).")
                    except Exception as e:
                        LOGGER.error(f"Failed to send expiry notification to {user_id}: {e}", exc_info=True)
                finally:
                    try:
                        await sent_files_log_col.delete_one({"_id": record["_id"]})
                    except Exception as e:
                        LOGGER.error(f"Failed to delete sent_files_log record: {e}", exc_info=True)
        except Exception as e:
            LOGGER.critical(f"Error in auto_delete_task loop: {e}", exc_info=True)
        await asyncio.sleep(600)

async def daily_limit_reset_task():
    while True:
        now = datetime.now(timezone.utc)
        tomorrow = now.date() + timedelta(days=1)
        next_run = datetime.combine(tomorrow, datetime.min.time(), tzinfo=timezone.utc) + timedelta(minutes=1)
        wait_seconds = (next_run - now).total_seconds()
        LOGGER.info(f"Daily reset will run in {timedelta(seconds=wait_seconds)}")
        await asyncio.sleep(wait_seconds)
        try:
            result = await users_col.update_many({"daily_file_count": {"$gt": 0}}, {"$set": {"daily_file_count": 0, "last_count_reset": datetime.now(timezone.utc).date().isoformat()}})
            LOGGER.info(f"Reset daily limits for {result.modified_count} users.")
        except Exception as e:
            LOGGER.critical(f"Error resetting daily limits: {e}", exc_info=True)

# -----------------------------
# Web server handlers (health)
# -----------------------------
routes = web.RouteTableDef()

@routes.get('/')
async def root_handler(request):
    uptime = timedelta(seconds=int(time.time() - start_time))
    bot_username = "Unknown"
    try:
        if bot.is_connected:
            me = await bot.get_me()
            bot_username = f"@{me.username}"
    except Exception:
        pass
    return web.Response(text=f"Bot is alive!\nBot Username: {bot_username}\nUptime: {uptime}", content_type='text/plain')

@routes.get('/health')
async def health_check_handler(request):
    return web.Response(text="OK", content_type='text/plain')

async def start_web_server():
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    try:
        await site.start()
        LOGGER.info(f"Web server started on port {Config.PORT}.")
    except Exception as e:
        LOGGER.error(f"Failed to start web server: {e}", exc_info=True)
        if bot.is_connected:
            await bot.stop()
        sys.exit(1)

# -----------------------------
# Token inspector admin command (for debugging)
# -----------------------------
@bot.on_message(filters.command("inspect_token") & filters.user(Config.ADMIN_IDS) & filters.private)
async def inspect_token_cmd(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /inspect_token <token>", parse_mode=enums.ParseMode.HTML)
        return
    token = message.text.split(" ", 1)[1].strip()
    try:
        padding = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(token + padding).decode()
    except Exception as e:
        await message.reply_text(f"Decode error: {e}", parse_mode=enums.ParseMode.HTML)
        return
    import json
    h = token_hash_raw(raw)
    doc = await tokens_col.find_one({"hash": h})
    await message.reply_text(f"<pre>{raw}</pre>\n\nhash: {h}\nDB record: {json.dumps(doc, default=str, indent=2)}", parse_mode=enums.ParseMode.HTML)

# -----------------------------
# Startup and lifecycle
# -----------------------------
async def main_startup_logic():
    global start_time
    start_time = time.time()
    LOGGER.info("Starting File Sender Bot V4.5")
    await load_settings_from_db()
    await create_db_indices()
    try:
        await bot.start()
        bot_info = await bot.get_me()
        LOGGER.info(f"Bot @{bot_info.username} started.")
    except Exception as e:
        LOGGER.critical(f"Failed to start bot: {e}", exc_info=True)
        sys.exit(1)
    # Background tasks
    asyncio.create_task(auto_delete_task())
    asyncio.create_task(daily_limit_reset_task())
    # Web server
    await start_web_server()
    # Notify admin
    if Config.ADMIN_IDS:
        try:
            await bot.send_message(Config.ADMIN_IDS[0], f"<b>✅ File Sender Bot (V4.5) started.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
    LOGGER.info("Bot and web server running.")
    await asyncio.Event().wait()

# Graceful shutdown handler
if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"Received exit signal {sig.name}. Shutting down...")
        if bot and bot.is_connected:
            try:
                await bot.stop()
            except Exception:
                pass
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown_handler(s)))
        except NotImplementedError:
            LOGGER.warning(f"Signal handling for {sig.name} not supported on this platform.")

    try:
        loop.run_until_complete(main_startup_logic())
        loop.run_forever()
    except KeyboardInterrupt:
        LOGGER.info("KeyboardInterrupt received. Shutting down...")
        if not loop.is_running():
            asyncio.run(shutdown_handler(signal.SIGINT))
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Cleanup and exit.")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
        LOGGER.info("Shutdown complete.")
