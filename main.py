# main.py (V4.8 patched + fixes for TTL, atomic reservations, atomic notifications)
"""
Patched full bot script with fixes:
- Prevent TTL on sent_files_log_col so bot is authoritative for deletions.
- Atomic pre-delete notifications (no duplicates).
- Atomic reservation for 10-hour rolling limit (prevents race / incorrect denies).
- Guard message.text usage.
- Minor robustness and logging improvements.
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
        ADMIN_IDS = [int(x.strip()) for x in re.split(r"[,\s]+", raw_admins) if x.strip()]
    else:
        ADMIN_IDS = []

    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))

    KC_LINK_SECRET = os.environ.get("KC_LINK_SECRET", "KCS3cR3t_v4_d3f6a8b1e9c2a5d4e7f8b1a3c5e")

    TOKEN_EXPIRY_SECONDS = int(os.environ.get("TOKEN_EXPIRY_SECONDS", 1800))
    FILE_DELETE_HOURS = int(os.environ.get("FILE_DELETE_HOURS", 12))

    WEBSITE_URL = os.environ.get("WEBSITE_URL", "https://www.keralacaptain.shop")
    WEBSITE_BUTTON_TEXT = os.environ.get("WEBSITE_BUTTON_TEXT", "Download Movie")

    PORT = int(os.environ.get("PORT", 8080))

# Validate required envs (allow empty ADMIN_IDS but warn)
required = (Config.API_ID and Config.API_HASH and Config.BOT_TOKEN and Config.MONGO_URI and Config.LOG_CHANNEL_ID)
if not required:
    LOGGER.critical("FATAL: Missing required environment variables.")
    if Config.KC_LINK_SECRET == "KCS3cR3t_v4_d3f6a8b1e9c2a5d4e7f8b1a3c5e":
        LOGGER.warning("WARNING: Using default KC_LINK_SECRET. Change it in environment.")
    sys.exit(1)
if not Config.ADMIN_IDS:
    LOGGER.warning("No ADMIN_IDS configured — admin commands will be disabled.")

# MongoDB
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
issued_tokens_col = db['issued_tokens']  # legacy token hash store
temp_tokens_col = db['temp_tokens']      # short-token collection (created by PHP API or Bot API)

# In-memory settings
SETTINGS = {}

# Pyrogram bot client
bot = Client(
    name="KeralaCaptainSenderV4_8",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)

start_time = time.time()

# -----------------------------
# Indices creation (safe)
# -----------------------------
async def create_db_indices():
    try:
        # issued_tokens_col unique hash index
        existing = [idx["name"] for idx in await issued_tokens_col.list_indexes().to_list(length=None)]
        if "hash_1" not in existing:
            await issued_tokens_col.create_index("hash", unique=True, name="hash_1")

        # users index
        existing = [idx["name"] for idx in await users_col.list_indexes().to_list(length=None)]
        if "limit_window_start_1" not in existing:
            await users_col.create_index("limit_window_start", name="limit_window_start_1")
        if "is_whitelisted_1" not in existing:
            await users_col.create_index("is_whitelisted", name="is_whitelisted_1")

        # media wp_post_id index: check if exists
        existing_media_idx = await media_collection.list_indexes().to_list(length=None)
        names = [idx["name"] for idx in existing_media_idx]
        if "wp_post_id_1" not in names:
            await media_collection.create_index("wp_post_id", name="wp_post_id_1")

        # Ensure sent_files_log_col does NOT have a TTL index on delete_at.
        # Sometimes a TTL was accidentally created — remove it so the bot is authoritative.
        try:
            sent_indexes = await sent_files_log_col.list_indexes().to_list(length=None)
            for idx in sent_indexes:
                # many drivers name TTL index like "delete_at_1", but check expireAfterSeconds
                if idx.get("key") == {"delete_at": 1} or idx.get("name") == "delete_at_1":
                    # if this index has expireAfterSeconds, drop it (we want bot to handle deletion, not Mongo)
                    if "expireAfterSeconds" in idx:
                        await sent_files_log_col.drop_index(idx["name"])
                        LOGGER.warning("Dropped TTL index on sent_files_log_col (bot will manage deletions).")
        except Exception as e:
            LOGGER.warning(f"Could not inspect/drop TTL index on sent_files_log_col: {e}")

        # TTL for temp tokens (ok to keep)
        existing = [idx["name"] for idx in await temp_tokens_col.list_indexes().to_list(length=None)]
        if "expiry_1" not in existing:
            await temp_tokens_col.create_index("expiry", expireAfterSeconds=0, name="expiry_1")

        LOGGER.info("DB indices ensured (safe mode).")
    except Exception as e:
        LOGGER.error(f"Error creating indices (safe): {e}", exc_info=True)

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
            "limit_window_start": None,
            "is_banned": False,
            "is_whitelisted": False  # New Whitelist Field
        }
        try:
            await users_col.insert_one(new_user)
            user_data = new_user
        except Exception:
            user_data = await users_col.find_one({"_id": user_id})
    
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

# -----------------------------
# Atomic reservation helper (10-hour window)
# -----------------------------
async def try_reserve_file_slot(user_id: int, limit: int, window_hours: int = 10):
    """
    Attempt to atomically reserve one slot for the user.
    Returns (True, None) if reserved;
            (False, "<time left string>") if limit reached with time remaining.
    """
    now = datetime.now(timezone.utc)
    window_cutoff = now - timedelta(hours=window_hours)

    # 1) If a window exists and is active and count < limit => increment atomically
    try:
        res = await users_col.find_one_and_update(
            {"_id": user_id, "limit_window_start": {"$gte": window_cutoff}, "daily_file_count": {"$lt": limit}},
            {"$inc": {"daily_file_count": 1}},
            return_document=ReturnDocument.AFTER
        )
        if res:
            # Reserved successfully within active window
            return True, None
    except Exception as e:
        LOGGER.error(f"Error during atomic increment reservation: {e}", exc_info=True)

    # 2) Otherwise, if window doesn't exist or is expired, reset to new window with count=1 (upsert)
    try:
        res2 = await users_col.find_one_and_update(
            {"_id": user_id, "$or": [{"limit_window_start": {"$lt": window_cutoff}}, {"limit_window_start": {"$exists": False}}]},
            {"$set": {"limit_window_start": now, "daily_file_count": 1, "join_date": datetime.now(timezone.utc)}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        if res2:
            return True, None
    except Exception as e:
        LOGGER.error(f"Error resetting user window reservation: {e}", exc_info=True)

    # 3) If both attempts failed, compute time left for active window (if any)
    try:
        user = await users_col.find_one({"_id": user_id})
        if user:
            window_start = user.get("limit_window_start")
            count = user.get("daily_file_count", 0)
            if window_start and isinstance(window_start, datetime):
                ten_hours_since = window_start + timedelta(hours=window_hours)
                if datetime.now(timezone.utc) < ten_hours_since:
                    time_left = ten_hours_since - datetime.now(timezone.utc)
                    hours_left = int(time_left.total_seconds() // 3600)
                    minutes_left = int((time_left.total_seconds() % 3600) // 60)
                    return False, f"{hours_left}h {minutes_left}m"
    except Exception as e:
        LOGGER.error(f"Error computing time left: {e}", exc_info=True)

    # As fallback, say limit reached and not known when
    return False, "10h 0m"

# -----------------------------
# Admin conversation helpers
# -----------------------------
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
            elif isinstance(message_ids_data, list):
                for item in message_ids_data:
                    if isinstance(item, dict) and item.get('id') == msg_id:
                        return document.get('wp_post_id')
        return None
    except Exception as e:
        LOGGER.error(f"Error searching for post_id from msg_id {msg_id}: {e}", exc_info=True)
        return None

# -----------------------------
# Legacy long-token processing
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
    if not token_b64:
        return None, None
    token_raw, err = parse_token_raw(token_b64)
    if not token_raw:
        LOGGER.warning(f"Token decode error: {err}")
        return None, None
    try:
        if ":" not in token_raw:
            LOGGER.warning("Token raw malformed (no ':').")
            return None, None
        payload, sig_from_token = token_raw.rsplit(":", 1)
        expected_sig = compute_expected_hmac(payload)
        if not hmac.compare_digest(expected_sig, sig_from_token):
            LOGGER.warning("Invalid token signature.")
            return None, None
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

# -----------------------------
# Short-token creation helpers (for Bot API)
# -----------------------------
async def kc_generate_short_id(length=9):
    alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    max_val = len(alphabet) - 1
    try:
        b_bytes = os.urandom(length)
    except NotImplementedError:
        b_bytes = str(time.time()).encode() + os.urandom(length)
    result = ''
    for i in range(length):
        char_ord = b_bytes[i] if i < len(b_bytes) else (b_bytes[i % len(b_bytes)] + i) % 256
        result += alphabet[char_ord % (max_val + 1)]
    return result

async def create_short_token_for_msg(message_id: int, bot_username: str, signature: str, post_id: int | None = None) -> (str | None):
    expiry_seconds = SETTINGS.get("token_expiry_seconds", Config.TOKEN_EXPIRY_SECONDS)
    expiry_ts = int(time.time() + expiry_seconds)
    expiry_dt = datetime.fromtimestamp(expiry_ts, timezone.utc)
    nonce = hashlib.sha256(os.urandom(16)).hexdigest()[:16]

    attempts = 0
    short_id = None
    while attempts < 5:
        short_id = await kc_generate_short_id()
        doc = {
            '_id': short_id,
            'message_id': message_id,
            'expiry': expiry_dt,
            'expiry_ts': expiry_ts,
            'bot': bot_username,
            'nonce': nonce,
            'signature': signature,
            'created_at': datetime.now(timezone.utc),
            'post_id': post_id
        }
        try:
            await temp_tokens_col.insert_one(doc)
            return short_id
        except DuplicateKeyError:
            attempts += 1
            await asyncio.sleep(0.01)
        except Exception as e:
            LOGGER.error(f"API Token Insert Failed: {e}", exc_info=True)
            return None
    LOGGER.error("Failed to generate unique short_id after 5 attempts.")
    return None

# -----------------------------
# Short-token lookup and claim
# -----------------------------
async def resolve_short_token(short_id: str, expected_bot_username: str):
    try:
        doc = await temp_tokens_col.find_one({"_id": short_id})
        if not doc:
            return None
        expiry = doc.get("expiry")
        bot_for = doc.get("bot")
        if bot_for and bot_for.lower() != expected_bot_username.lower():
            LOGGER.warning(f"Short token bot mismatch: {bot_for} != {expected_bot_username}")
            return None
        expiry_ts = doc.get("expiry_ts")
        if expiry_ts:
            if time.time() > int(expiry_ts):
                LOGGER.warning("Short token expired (by expiry_ts).")
                return None
        return doc
    except Exception as e:
        LOGGER.error(f"Error resolving short token {short_id}: {e}", exc_info=True)
        return None

async def claim_and_delete_short_token(short_id: str, user_id: int):
    try:
        doc = await temp_tokens_col.find_one_and_delete({"_id": short_id})
        return doc
    except Exception as e:
        LOGGER.error(f"Error claiming short token {short_id}: {e}", exc_info=True)
        return None

# -----------------------------
# Refresh file reference function
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
        new_qualities = None
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
# UI helpers, admin keyboard
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
        [InlineKeyboardButton("Manage Whitelist ➡️", callback_data="admin_whitelist_menu")],
        [InlineKeyboardButton(f"Limit: {SETTINGS.get('daily_limit', 5)} files / 10h", callback_data="admin_set_limit")],
        [InlineKeyboardButton(f"Delete Time: {SETTINGS.get('file_delete_hours', Config.FILE_DELETE_HOURS)}h", callback_data="admin_set_delete_time")],
        [InlineKeyboardButton("Set Channel (username or -100..ID)", callback_data="admin_set_fsub_channel")],
        [InlineKeyboardButton("⬅️ Back to Admin", callback_data="admin_main_menu")]
    ])

# New Whitelist Management Keyboard
def get_whitelist_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add User", callback_data="admin_wl_add"),
         InlineKeyboardButton("❌ Remove User", callback_data="admin_wl_remove")],
        [InlineKeyboardButton("🔍 Check User", callback_data="admin_wl_check"),
         InlineKeyboardButton("📊 Total Users", callback_data="admin_wl_total")],
        [InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]
    ])

# Force-sub button
def get_force_sub_button(channel_ref):
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
# Robust /start handler
# -----------------------------
_START_RE = re.compile(r'^/start(?:@[\w_]+)?(?:[ =])?(.+)?$', flags=re.IGNORECASE)

@bot.on_message(filters.command("start") & filters.private)
async def start_command_handler(client: Client, message: Message):
    user_id = message.from_user.id

    # Admin /start goes to admin panel
    if user_id in Config.ADMIN_IDS:
        text = (message.text or "").strip()
        m = _START_RE.match(text)
        token_group = m.group(1) if m else None
        if not token_group:
            await admin_panel_handler(client, message)
            return
        LOGGER.info(f"Admin {user_id} is testing a start token...")

    user_data = await get_user_data(user_id)
    if user_data.get("is_banned", False):
        await message.reply_text("<b>❌ You are banned from using this bot.</b>", parse_mode=enums.ParseMode.HTML)
        return

    LOGGER.info(f"[START] from={user_id} text={message.text!r} date={message.date}")

    token = None
    text = (message.text or "").strip()
    m = _START_RE.match(text)
    if m:
        token_group = m.group(1)
        if token_group:
            token = token_group.strip()
    if not token:
        try:
            if message.entities:
                for ent in message.entities:
                    if ent.type == "bot_command":
                        start = ent.offset + ent.length
                        rest = text[start:].strip()
                        if rest:
                            token = rest
                            break
        except Exception:
            pass

    if not token:
        welcome_text = (
            "<b>👋 Welcome!</b>\n\n"
            "To receive downloads, please visit our website and click the download button.\n"
            f"Website: <b>{Config.WEBSITE_URL}</b>"
        )
        await message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]), parse_mode=enums.ParseMode.HTML)
        return

    is_short = bool(re.fullmatch(r'[A-Za-z0-9]{4,20}', token))
    bot_info = await bot.get_me()
    bot_username = bot_info.username or ""

    msg_id_to_send = None
    token_raw_for_claim = None
    post_id_for_logging = None

    if is_short:
        doc = await resolve_short_token(token, bot_username)
        if not doc:
            await message.reply_text(
                "<b>⏳ Link expired or invalid.</b>\n\nThis download link has expired or cannot be used. Please go back to the website to get a new link.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]),
                parse_mode=enums.ParseMode.HTML
            )
            return
        msg_id_to_send = int(doc.get("message_id"))
        token_raw_for_claim = token
        post_id_for_logging = doc.get('post_id')
    else:
        msg_id_to_send, legacy_raw = await verify_token_basic(token, bot_username)
        if not msg_id_to_send:
            await message.reply_text(
                "<b>⏳ Link expired or invalid.</b>\n\nThis download link has expired or cannot be used. Please go back to the website to get a new link.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]),
                parse_mode=enums.ParseMode.HTML
            )
            return
        token_raw_for_claim = legacy_raw

    LOGGER.info(f"Resolved msg id to send: {msg_id_to_send} (post_id={post_id_for_logging})")

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

    # --- New 10-Hour Rolling Limit Check (atomic reservation) ---
    if user_id not in Config.ADMIN_IDS:
        daily_limit = SETTINGS.get("daily_limit", 5)
        reserved, time_left_str = await try_reserve_file_slot(user_id, daily_limit, window_hours=10)
        if not reserved:
            await message.reply_text(f"<b>⚠️ Limit Reached</b>\n\nYou have reached your limit of {daily_limit} files for this 10-hour window. Your limit will reset in <b>{time_left_str}</b>.", parse_mode=enums.ParseMode.HTML)
            return
    # --- End of Limit Check ---

    status_msg = await message.reply_text("<b>✅ Link verified. Sending file, please wait...</b>", parse_mode=enums.ParseMode.HTML)
    try:
        # --- NEW WHITELIST LOGIC ---
        # Check the global setting
        protect_default = SETTINGS.get("protect_content_enabled", True)
        
        # Check if the user is special (whitelisted)
        user_is_whitelisted = user_data.get("is_whitelisted", False)

        # Decide the final protection status
        final_protect_status = protect_default
        if user_is_whitelisted:
            final_protect_status = False # Override! Allow forwarding for this user
            LOGGER.info(f"User {user_id} is whitelisted. Bypassing content protection.")
        
        sent_file_msg = await client.copy_message(
            chat_id=user_id,
            from_chat_id=Config.LOG_CHANNEL_ID,
            message_id=msg_id_to_send,
            protect_content=final_protect_status # Use the final status
        )
        if not sent_file_msg:
            await status_msg.edit_text("<b>❌ Error: Could not send file.</b>", parse_mode=enums.ParseMode.HTML)
            return

        claimed = True
        if SETTINGS.get("single_use_tokens", True):
            if is_short:
                doc_claimed = await claim_and_delete_short_token(token_raw_for_claim, user_id)
                if not doc_claimed:
                    try:
                        await bot.delete_messages(chat_id=user_id, message_ids=sent_file_msg.id)
                    except Exception:
                        pass
                    await status_msg.edit_text("<b>❌ This link has already been used by another user.</b>\n\nPlease get a fresh link from the website.", parse_mode=enums.ParseMode.HTML)
                    return
            else:
                try:
                    raw_hash = token_hash_raw(token_raw_for_claim)
                    now_utc = datetime.now(timezone.utc)
                    filt = {"hash": raw_hash, "$or": [{"used": False}, {"used": {"$exists": False}}]}
                    update = {"$set": {"used": True, "used_by": user_id, "used_at": now_utc}}
                    doc = await issued_tokens_col.find_one_and_update(filt, update, return_document=ReturnDocument.AFTER)
                    if doc:
                        claimed = True
                    else:
                        try:
                            await issued_tokens_col.insert_one({"hash": raw_hash, "issued_raw": token_raw_for_claim, "used": True, "used_by": user_id, "used_at": now_utc, "issued_at": now_utc})
                            claimed = True
                        except DuplicateKeyError:
                            doc_existing = await issued_tokens_col.find_one({"hash": raw_hash})
                            if doc_existing and doc_existing.get("used"):
                                claimed = False
                            else:
                                updated = await issued_tokens_col.find_one_and_update({"hash": raw_hash, "used": False}, {"$set": {"used": True, "used_by": user_id, "used_at": now_utc}}, return_document=ReturnDocument.AFTER)
                                claimed = bool(updated and updated.get("used"))
                except Exception as e:
                    LOGGER.error(f"Error claiming legacy token: {e}", exc_info=True)
                    claimed = False

                if not claimed:
                    try:
                        await bot.delete_messages(chat_id=user_id, message_ids=sent_file_msg.id)
                    except Exception:
                        pass
                    await status_msg.edit_text("<b>❌ This link has already been used by another user.</b>\n\nPlease get a fresh link from the website.", parse_mode=enums.ParseMode.HTML)
                    return

        # Log file for deletion (bot is authoritative)
        await log_sent_file_for_deletion(user_id, sent_file_msg, msg_id_to_send, post_id_for_logging)

        # Since we reserved earlier atomically, DO NOT increment again here.
        if user_id in Config.ADMIN_IDS:
            LOGGER.info(f"Admin {user_id} downloaded a file. Limit count not incremented.")

        try:
            await status_msg.delete()
        except Exception:
            pass

        delete_hours = SETTINGS.get("file_delete_hours", Config.FILE_DELETE_HOURS)
        try:
            help_button = InlineKeyboardMarkup([[InlineKeyboardButton("Contact Admin", url="https://t.me/KeralaCaptainHelpBot")]])
            await sent_file_msg.reply_text(
                f"<b>⚠️ Download quickly</b>\n\nTo avoid copyright issues, this file will be removed in <b>{delete_hours} hours</b>.\nIf the link expires, get a fresh link from our website: {Config.WEBSITE_URL}\nIf you face issues (iPhone forwarding or others), contact admin:",
                reply_markup=help_button,
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
# Log sent file & auto-delete (with pre-delete notification)
# -----------------------------
async def log_sent_file_for_deletion(user_id: int, message_obj: Message, original_message_id: int, post_id: int | None = None):
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
            "delete_at": delete_at,
            "post_id": post_id
        })
    except Exception as e:
        LOGGER.error(f"Failed to log sent file: {e}", exc_info=True)

# -----------------------------
# Background tasks (auto-delete + pre-delete notifications)
# -----------------------------
async def auto_delete_task():
    await asyncio.sleep(60)
    LOGGER.info("Auto-delete task started.")
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            # Pre-delete notifications: notify users 1 hour before deletion if not yet notified
            one_hour_from_now = now_utc + timedelta(hours=1)
            cursor_notify = sent_files_log_col.find({"delete_at": {"$lte": one_hour_from_now, "$gt": now_utc}, "notified_before_delete": {"$ne": True}})
            async for record in cursor_notify:
                rec_id = record["_id"]
                # Atomically set notified_before_delete so only one worker/notifier will proceed
                try:
                    prev = await sent_files_log_col.find_one_and_update(
                        {"_id": rec_id, "notified_before_delete": {"$ne": True}},
                        {"$set": {"notified_before_delete": True}},
                        return_document=ReturnDocument.BEFORE
                    )
                except Exception:
                    prev = None
                # If prev is None, someone else updated it just now; skip
                if not prev:
                    continue

                user_id = record.get("user_id")
                file_name = record.get("file_name", "your file")
                try:
                    notify_text = (f"Reminder: the file <b>{file_name}</b> you downloaded will be removed in 1 hour. If you need a new copy, get a fresh link from: <b>{Config.WEBSITE_URL}</b>")
                    await bot.send_message(user_id, notify_text, parse_mode=enums.ParseMode.HTML)
                except Exception:
                    pass

            # Find expired files
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
                        notify_text = (f"Hello, your download link for the file <b>{file_name}</b> has expired. To get a new link, please visit our website: <b>{Config.WEBSITE_URL}</b>")
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
        
        await asyncio.sleep(60) 

# -----------------------------
# Admin callback router and stats handler
# -----------------------------
def _fmt(n):
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)

@bot.on_callback_query()
async def admin_callback_router(client: Client, callback: CallbackQuery):
    data = callback.data or ""
    user_id = callback.from_user.id
    if user_id not in Config.ADMIN_IDS:
        try:
            await callback.answer("Unauthorized", show_alert=True)
        except Exception:
            pass
        return
    
    if data == "admin_stats":
        await handle_admin_stats(callback)
    
    elif data == "admin_settings":
        await callback.answer()
        try:
            await callback.message.edit_text("Settings panel", reply_markup=get_settings_keyboard(), parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
    
    elif data == "admin_broadcast":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_broadcast")
        await callback.message.edit_text("Please send the message to broadcast. (Forwarding not supported yet)")

    elif data == "admin_ban":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_ban_id")
        await callback.message.edit_text("Please send the User ID to **ban**.")
    
    elif data == "admin_unban":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_unban_id")
        await callback.message.edit_text("Please send the User ID to **unban**.")

    elif data.startswith("admin_toggle_"):
        key = data.replace("admin_toggle_", "")
        if key == "fsub":
            new = not SETTINGS.get("force_sub_enabled", False)
            await update_db_setting("force_sub_enabled", new)
            await callback.answer(f"Force sub set to {new}")
            await callback.message.edit_reply_markup(reply_markup=get_settings_keyboard())
        elif key == "protect":
            new = not SETTINGS.get("protect_content_enabled", True)
            await update_db_setting("protect_content_enabled", new)
            await callback.answer(f"Protect content set to {new}")
            await callback.message.edit_reply_markup(reply_markup=get_settings_keyboard())
        elif key == "single_use":
            new = not SETTINGS.get("single_use_tokens", True)
            await update_db_setting("single_use_tokens", new)
            await callback.answer(f"Single-use tokens set to {new}")
            await callback.message.edit_reply_markup(reply_markup=get_settings_keyboard())
        else:
            await callback.answer("Unknown toggle", show_alert=True)
    
    elif data == "admin_set_limit":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_limit")
        await callback.message.edit_text("Please send the new file limit (e.g., 5).")

    elif data == "admin_set_delete_time":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_delete_time")
        await callback.message.edit_text("Please send the new delete time in hours (e.g., 12).")

    elif data == "admin_set_fsub_channel":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_fsub_channel")
        await callback.message.edit_text("Please send the channel ID (e.g., -100...) or username (@...).")

    # --- New Whitelist Routes ---
    elif data == "admin_whitelist_menu":
        await callback.answer()
        text = "<b>Manage Whitelist</b>\n\nUsers on this list will bypass `Protect Content` and be able to forward files."
        await callback.message.edit_text(text, reply_markup=get_whitelist_keyboard(), parse_mode=enums.ParseMode.HTML)
    
    elif data == "admin_wl_add":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_wl_add_id")
        await callback.message.edit_text("Please send the User ID to **add** to the whitelist.", parse_mode=enums.ParseMode.HTML)

    elif data == "admin_wl_remove":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_wl_remove_id")
        await callback.message.edit_text("Please send the User ID to **remove** from the whitelist.", parse_mode=enums.ParseMode.HTML)
    
    elif data == "admin_wl_check":
        await callback.answer()
        await set_admin_conv(user_id, "awaiting_wl_check_id")
        await callback.message.edit_text("Please send the User ID to **check**.", parse_mode=enums.ParseMode.HTML)

    elif data == "admin_wl_total":
        await callback.answer("Checking count...")
        try:
            count = await users_col.count_documents({"is_whitelisted": True})
            await callback.message.edit_text(
                f"📊 There are currently <b>{count}</b> users on the whitelist.",
                reply_markup=get_whitelist_keyboard(),
                parse_mode=enums.ParseMode.HTML
            )
        except Exception as e:
            await callback.message.edit_text(f"Error counting: {e}", reply_markup=get_whitelist_keyboard())
    # --- End Whitelist Routes ---

    elif data == "admin_main_menu":
        await callback.answer()
        await callback.message.edit_text("Admin Panel", reply_markup=get_main_admin_keyboard(), parse_mode=enums.ParseMode.HTML)
    
    else:
        await callback.answer()

async def handle_admin_stats(callback: CallbackQuery):
    await callback.answer()
    total_users = await get_total_users_count()
    today_users = await get_today_users_count()
    banned_count = await get_banned_users_count()
    whitelist_count = await users_col.count_documents({"is_whitelisted": True})
    
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    downloads_today = await sent_files_log_col.count_documents({"sent_at": {"$gte": today_start}})
    downloads_total = await sent_files_log_col.count_documents({})
    
    pipeline = [
        {"$match": {"sent_at": {"$gte": today_start}}},
        {"$group": {"_id": "$log_channel_message_id", "count": {"$sum": 1}, "file_name": {"$first": "$file_name"}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    top_today = await sent_files_log_col.aggregate(pipeline).to_list(length=1)
    top_file_name = top_today[0].get("file_name", "—") if top_today else "—"
    top_file_count = top_today[0].get("count", 0) if top_today else 0

    pipeline_user = [
        {"$match": {"sent_at": {"$gte": today_start}}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    top_user = await sent_files_log_col.aggregate(pipeline_user).to_list(length=1)
    top_user_id = top_user[0].get("_id", "—") if top_user else "—"
    top_user_count = top_user[0].get("count", 0) if top_user else 0
    
    text = (
        f"<b>📊 Admin statistics</b>\n\n"
        f"Total users: <b>{_fmt(total_users)}</b>\n"
        f"New users today: <b>{_fmt(today_users)}</b>\n"
        f"Total downloads (all time): <b>{_fmt(downloads_total)}</b>\n"
        f"Downloads today: <b>{_fmt(downloads_today)}</b>\n\n"
        f"Top file today: <b>{top_file_name}</b> — <b>{_fmt(top_file_count)}</b> downloads\n"
        f"Top user today: <b>{top_user_id}</b> — <b>{_fmt(top_user_count)}</b> downloads\n\n"
        f"Banned users: <b>{_fmt(banned_count)}</b>\n"
        f"Whitelisted users: <b>{_fmt(whitelist_count)}</b>"
    )
    try:
        await callback.message.edit_text(text, reply_markup=get_main_admin_keyboard(), parse_mode=enums.ParseMode.HTML)
    except Exception:
        await callback.message.reply_text(text, reply_markup=get_main_admin_keyboard(), parse_mode=enums.ParseMode.HTML)

# -----------------------------
# Admin commands & Conversation Handler
# -----------------------------
@bot.on_message(filters.command("admin") & filters.user(Config.ADMIN_IDS) & filters.private)
async def admin_panel_handler(client, message: Message):
      await message.reply_text("Admin Panel (Use buttons)", reply_markup=get_main_admin_keyboard())

# THIS IS THE CORRECTED LINE (approx. line 1106)
# This filter explicitly excludes messages that start with "/"
@bot.on_message(filters.private & filters.user(Config.ADMIN_IDS) & ~filters.regex(r"^/") & filters.text)
async def admin_conversation_handler(client: Client, message: Message):
    admin_id = message.from_user.id
    conv = await get_admin_conv(admin_id)
    if not conv:
        return # Not in a conversation

    stage = conv.get("stage")
    text = (message.text or "").strip()
    
    # --- Whitelist Handlers ---
    if stage == "awaiting_wl_add_id":
        try:
            target_user_id = int(text)
            await users_col.update_one({"_id": target_user_id}, {"$set": {"is_whitelisted": True}}, upsert=True)
            await clear_admin_conv(admin_id)
            await message.reply_text(f"✅ Success! User `{target_user_id}` is now on the whitelist.", reply_markup=get_whitelist_keyboard())
            # Notify the user
            try:
                await client.send_message(target_user_id, "🎉 Congratulations! You have been added to our whitelist. You can now forward files from this bot.")
            except (UserIsBlocked, PeerIdInvalid):
                await message.reply_text(f"✅ Success! User `{target_user_id}` is whitelisted, but I could not notify them (they may have blocked the bot).")
        except ValueError:
            await message.reply_text("Invalid User ID. Please send numbers only. Operation cancelled.")
        except Exception as e:
            await message.reply_text(f"Error: {e}")

    elif stage == "awaiting_wl_remove_id":
        try:
            target_user_id = int(text)
            await users_col.update_one({"_id": target_user_id}, {"$set": {"is_whitelisted": False}})
            await clear_admin_conv(admin_id)
            await message.reply_text(f"🗑️ Success! User `{target_user_id}` has been removed from the whitelist.", reply_markup=get_whitelist_keyboard())
            # Notify the user
            try:
                await client.send_message(target_user_id, "You have been removed from the whitelist. File forwarding is now disabled for you (based on global settings).")
            except (UserIsBlocked, PeerIdInvalid):
                await message.reply_text(f"🗑️ Success! User `{target_user_id}` was removed, but I could not notify them.")
        except ValueError:
            await message.reply_text("Invalid User ID. Please send numbers only. Operation cancelled.")
        except Exception as e:
            await message.reply_text(f"Error: {e}")

    elif stage == "awaiting_wl_check_id":
        try:
            target_user_id = int(text)
            user_data = await users_col.find_one({"_id": target_user_id})
            await clear_admin_conv(admin_id)
            if user_data and user_data.get("is_whitelisted"):
                await message.reply_text(f"Yes, User `{target_user_id}` **is** on the whitelist.", reply_markup=get_whitelist_keyboard())
            else:
                await message.reply_text(f"No, User `{target_user_id}` is **not** on the whitelist.", reply_markup=get_whitelist_keyboard())
        except ValueError:
            await message.reply_text("Invalid User ID. Please send numbers only. Operation cancelled.")

    # --- Other Setting Handlers ---
    elif stage == "awaiting_limit":
        try:
            new_limit = int(text)
            if new_limit < 0: raise ValueError
            await update_db_setting("daily_limit", new_limit)
            await clear_admin_conv(admin_id)
            SETTINGS['daily_limit'] = new_limit # Update in-memory
            await message.reply_text(f"✅ Limit set to {new_limit} files per 10 hours.", reply_markup=get_settings_keyboard())
        except ValueError:
            await message.reply_text("Invalid number. Operation cancelled.")

    elif stage == "awaiting_delete_time":
        try:
            new_time = int(text)
            if new_time <= 0: raise ValueError
            await update_db_setting("file_delete_hours", new_time)
            await clear_admin_conv(admin_id)
            SETTINGS['file_delete_hours'] = new_time # Update in-memory
            await message.reply_text(f"✅ File delete time set to {new_time} hours.", reply_markup=get_settings_keyboard())
        except ValueError:
            await message.reply_text("Invalid number. Must be a positive integer. Operation cancelled.")
    
    elif stage == "awaiting_fsub_channel":
        try:
            channel_id = 0
            if text.startswith("@"):
                channel_id = text
            else:
                channel_id = int(text)
            
            await client.get_chat(channel_id) # Test chat exists
            
            await update_db_setting("force_sub_channel_id", channel_id)
            await clear_admin_conv(admin_id)
            SETTINGS['force_sub_channel_id'] = channel_id # Update in-memory
            await message.reply_text(f"✅ ForceSub channel set to `{channel_id}`.", reply_markup=get_settings_keyboard())
        except ValueError:
             await message.reply_text("Invalid ID. Must be an integer (like -100...) or a username (@...).")
        except RPCError as e:
            await message.reply_text(f"❌ Error setting channel: {e}\n\nMake sure the bot is an admin in that channel.")
        except Exception as e:
            await message.reply_text(f"❌ An unexpected error occurred: {e}")

    elif stage == "awaiting_ban_id":
        try:
            target_user_id = int(text)
            await users_col.update_one({"_id": target_user_id}, {"$set": {"is_banned": True}}, upsert=True)
            await clear_admin_conv(admin_id)
            await message.reply_text(f"🚫 User `{target_user_id}` has been banned.", reply_markup=get_main_admin_keyboard())
        except ValueError:
            await message.reply_text("Invalid User ID. Operation cancelled.")

    elif stage == "awaiting_unban_id":
        try:
            target_user_id = int(text)
            await users_col.update_one({"_id": target_user_id}, {"$set": {"is_banned": False}})
            await clear_admin_conv(admin_id)
            await message.reply_text(f"✅ User `{target_user_id}` has been unbanned.", reply_markup=get_main_admin_keyboard())
        except ValueError:
            await message.reply_text("Invalid User ID. Operation cancelled.")

    elif stage == "awaiting_broadcast":
        await clear_admin_conv(admin_id)
        await message.reply_text("Broadcast started... (This may take a while)")
        user_ids = await get_all_user_ids()
        success_count = 0
        fail_count = 0
        for user_id in user_ids:
            try:
                await bot.send_message(user_id, text, parse_mode=enums.ParseMode.HTML)
                success_count += 1
                await asyncio.sleep(0.05) # 20 messages per second
            except (UserIsBlocked, PeerIdInvalid):
                fail_count += 1
            except FloodWait as e:
                await asyncio.sleep(e.x + 1)
            except Exception:
                fail_count += 1
        await message.reply_text(f"Broadcast complete.\n\nSent to: {success_count} users.\nFailed for: {fail_count} users.", reply_markup=get_main_admin_keyboard())


@bot.on_message(filters.command("inspect_token") & filters.user(Config.ADMIN_IDS) & filters.private)
async def inspect_token_cmd(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /inspect_token <shortid_or_longtoken>", parse_mode=enums.ParseMode.HTML)
        return
    token = message.text.split(" ", 1)[1].strip()
    if re.fullmatch(r'[A-Za-z0-9]{4,20}', token):
        doc = await temp_tokens_col.find_one({"_id": token})
        await message.reply_text(f"<pre>{doc}</pre>", parse_mode=enums.ParseMode.HTML)
        return
    raw, err = parse_token_raw(token)
    if not raw:
        await message.reply_text(f"Decode error: {err}", parse_mode=enums.ParseMode.HTML)
        return
    h = token_hash_raw(raw)
    doc = await issued_tokens_col.find_one({"hash": h})
    await message.reply_text(f"<pre>{raw}</pre>\n\nhash: {h}\nDB record: {doc}", parse_mode=enums.ParseMode.HTML)

@bot.on_message(filters.command("revoke_short") & filters.user(Config.ADMIN_IDS) & filters.private)
async def revoke_short_cmd(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /revoke_short <shortid>", parse_mode=enums.ParseMode.HTML)
        return
    short = message.text.split(" ", 1)[1].strip()
    res = await temp_tokens_col.delete_one({"_id": short})
    if res.deleted_count:
        await message.reply_text("Short token revoked.", parse_mode=enums.ParseMode.HTML)
    else:
        await message.reply_text("Token not found.", parse_mode=enums.ParseMode.HTML)

# -----------------------------
# Web server handlers (health + NEW API)
# -----------------------------
routes = web.RouteTableDef()

@routes.get('/')
async def root_handler(request):
    uptime = timedelta(seconds=int(time.time() - start_time))
    bot_username = "Unknown"
    try:
        me = await bot.get_me()
        bot_username = f"@{me.username}"
    except Exception:
        bot_username = "Unknown"
    return web.Response(text=f"Bot is alive!\nBot Username: {bot_username}\nUptime: {uptime}", content_type='text/plain')

@routes.get('/health')
async def health_check_handler(request):
    return web.Response(text="OK", content_type='text/plain')

# CSV export for admin statistics (requires Authorization header with KC_LINK_SECRET)
@routes.get('/admin/export_stats')
async def export_stats_csv(request):
    auth = request.headers.get('Authorization', '')
    if not auth.startswith('Bearer '):
        return web.Response(status=401, text='Unauthorized')
    if not hmac.compare_digest(auth.split(' ',1)[1], Config.KC_LINK_SECRET):
        return web.Response(status=403, text='Forbidden')
    
    params = request.rel_url.query
    try:
        date_from = params.get('date_from')
        date_to = params.get('date_to')
        if date_from:
            dt_from = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        else:
            dt_from = datetime.now(timezone.utc) - timedelta(days=7)
        if date_to:
            dt_to = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) + timedelta(days=1)
        else:
            dt_to = datetime.now(timezone.utc) + timedelta(days=1)
    except Exception:
        return web.Response(status=400, text='Bad date format')
    
    pipeline = [
        {"$match": {"sent_at": {"$gte": dt_from, "$lt": dt_to}}},
        {"$project": {"user_id": 1, "file_name": 1, "sent_at": 1, "telegram_message_id": 1, "post_id": 1}}
    ]
    rows = []
    cursor = sent_files_log_col.aggregate(pipeline)
    async for r in cursor:
        rows.append(r)
    
    import io, csv
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(['user_id','file_name','sent_at','telegram_message_id','post_id'])
    for r in rows:
        writer.writerow([r.get('user_id'), r.get('file_name'), r.get('sent_at').isoformat(), r.get('telegram_message_id'), r.get('post_id')])
    return web.Response(text=buf.getvalue(), content_type='text/csv')

@routes.post('/api/create_token')
async def create_token_handler(request: web.Request):
    LOGGER.info("Received request on /api/create_token")
    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return web.json_response({'error': 'Authorization header missing.'}, status=401)
        provided_secret = auth_header.split(' ', 1)[1]
        if not hmac.compare_digest(provided_secret, Config.KC_LINK_SECRET):
            LOGGER.warning("API call with invalid secret rejected.")
            return web.json_response({'error': 'Invalid secret.'}, status=401)
    except Exception as e:
        LOGGER.error(f"Auth check error: {e}")
        return web.json_response({'error': 'Auth error.'}, status=401)
    try:
        data = await request.json()
        qualities_list = data.get('qualities_list')
        bot_username = data.get('bot_username')
        post_id = data.get('post_id')
        if not all([qualities_list, bot_username]):
            return web.json_response({'error': 'Missing required data: qualities_list or bot_username'}, status=400)
    except Exception as e:
        LOGGER.error(f"API JSON decode error: {e}")
        return web.json_response({'error': 'Invalid JSON body.'}, status=400)
    
    download_links = []
    for item in qualities_list:
        try:
            quality = str(item.get('quality', 'Unknown'))
            message_id = int(item.get('id', 0))
            size = int(item.get('size', 0))
            if not message_id:
                continue
            expiry_ts = int(time.time() + SETTINGS.get("token_expiry_seconds", Config.TOKEN_EXPIRY_SECONDS))
            payload = f"{message_id}:{expiry_ts}:{bot_username}:api_v2"
            signature = compute_expected_hmac(payload)
            short_id = await create_short_token_for_msg(message_id, bot_username, signature, post_id=post_id)
            if short_id:
                link = f"https://t.me/{bot_username}?start={short_id}"
                download_links.append({
                    'quality': quality,
                    'size': size,
                    'link': link
                })
        except Exception as e:
            LOGGER.error(f"Error processing quality {item}: {e}", exc_info=True)
    
    if not download_links:
        return web.json_response({'error': 'No links could be generated.'}, status=500)
    
    LOGGER.info(f"Successfully generated {len(download_links)} links for bot {bot_username}.")
    return web.json_response({'links': download_links}, status=200)

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
# Startup and lifecycle
# -----------------------------
async def main_startup_logic():
    global start_time
    start_time = time.time()
    LOGGER.info("Starting File Sender Bot V4.8 (patched)")
    await load_settings_from_db()
    await create_db_indices()
    try:
        await bot.start()
        bot_info = await bot.get_me()
        LOGGER.info(f"Bot @{bot_info.username} started.")
    except Exception as e:
        LOGGER.critical(f"Failed to start bot: {e}", exc_info=True)
        sys.exit(1)
    
    asyncio.create_task(auto_delete_task())
    
    await start_web_server()
    if Config.ADMIN_IDS:
        try:
            await bot.send_message(Config.ADMIN_IDS[0], f"<b>✅ File Sender Bot (V4.8 patched) started.</b>\n\n- Auto-delete fix applied.\n- 10-hour rolling limit active.\n- Whitelist Management active.", parse_mode=enums.ParseMode.HTML)
        except Exception:
            pass
    LOGGER.info("Bot and web server running.")
    await asyncio.Event().wait()

# Graceful shutdown
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
