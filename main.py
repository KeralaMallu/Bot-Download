# main.py (V4.6) - KeralaCaptain File Sender Bot (Short-token + API support)
"""
Full bot script - V4.6 (with /api/create_token endpoint)
- Accepts short token IDs (stored in MongoDB temp_tokens) via /start
- Looks up temp_tokens, validates expiry and bot, atomically deletes the token (single-use),
  and sends the file to the user.
- Preserves legacy long-token verification for backward compatibility.
- Creates indices (TTL for temp_tokens.expiry), admin panel, broadcast, auto-delete, etc.
- All user-facing messages use HTML parse mode.
- INCLUDES: /api/create_token endpoint to allow PHP to request token creation.
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

# pymongo helpers
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

# Validate
if not (Config.API_ID and Config.API_HASH and Config.BOT_TOKEN and Config.MONGO_URI and Config.LOG_CHANNEL_ID and Config.ADMIN_IDS):
    LOGGER.critical("FATAL: Missing required environment variables.")
    if Config.KC_LINK_SECRET == "KCS3cR3t_v4_d3f6a8b1e9c2a5d4e7f8b1a3c5e":
        LOGGER.warning("WARNING: Using default KC_LINK_SECRET. Change it in environment.")
    sys.exit(1)

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
    name="KeralaCaptainSenderV4_6",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)

start_time = time.time()

# -----------------------------
# Indices creation
# -----------------------------
async def create_db_indices():
    try:
        # TTL index for sent_files_log delete_at (auto cleanup if desired)
        await sent_files_log_col.create_index("delete_at", expireAfterSeconds=0)
        # Unique / helpful indices
        await issued_tokens_col.create_index("hash", unique=True)
        await users_col.create_index("last_count_reset")
        await media_collection.create_index("wp_post_id")
        # TTL for temp tokens: temp_tokens.expiry (should be a MongoDB datetime)
        # Note: expireAfterSeconds=0 will expire exactly at 'expiry'
        await temp_tokens_col.create_index("expiry", expireAfterSeconds=0)
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
# User helpers (same as V4.x)
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
# Media helpers (same as before)
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
            # Handle new list format
            elif isinstance(message_ids_data, list):
                for item in message_ids_data:
                    if isinstance(item, dict) and item.get('id') == msg_id:
                        return document.get('wp_post_id')
        return None
    except Exception as e:
        LOGGER.error(f"Error searching for post_id from msg_id {msg_id}: {e}", exc_info=True)
        return None

# -----------------------------
# Legacy long-token processing (keeps backward compatibility)
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
    Legacy token verification (if token is long/base64). Returns (msg_id:int, token_raw:str) or (None, None)
    """
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
# NEW Short-token creation helpers (for Bot API)
# -----------------------------

async def kc_generate_short_id(length=9):
    """Generate a base62 short token string from random bytes."""
    alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    max_val = len(alphabet) - 1
    try:
        b_bytes = os.urandom(length)
    except NotImplementedError:
        # Fallback for systems without /dev/urandom
        b_bytes = str(time.time()).encode() + os.urandom(length)
    
    result = ''
    for i in range(length):
        char_ord = b_bytes[i] if i < len(b_bytes) else (b_bytes[i % len(b_bytes)] + i) % 256
        result += alphabet[char_ord % (max_val + 1)]
    return result

async def create_short_token_for_msg(message_id: int, bot_username: str, signature: str) -> (str | None):
    """
    Creates and inserts a single short token into temp_tokens_col.
    Handles uniqueness collisions. Returns short_id or None on failure.
    """
    expiry_seconds = SETTINGS.get("token_expiry_seconds", Config.TOKEN_EXPIRY_SECONDS)
    expiry_ts = int(time.time() + expiry_seconds)
    expiry_dt = datetime.fromtimestamp(expiry_ts, timezone.utc)
    nonce = hashlib.sha256(os.urandom(16)).hexdigest()[:16] # 16 char nonce

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
            'signature': signature, # Store the signature from PHP for consistency
            'created_at': datetime.now(timezone.utc),
        }
        try:
            await temp_tokens_col.insert_one(doc)
            # Success
            return short_id
        except DuplicateKeyError:
            # Collision; try again
            attempts += 1
            await asyncio.sleep(0.01) # Small delay
        except Exception as e:
            LOGGER.error(f"API Token Insert Failed: {e}", exc_info=True)
            return None
    
    LOGGER.error("Failed to generate unique short_id after 5 attempts.")
    return None

# -----------------------------
# Short-token lookup (new flow)
# -----------------------------
async def resolve_short_token(short_id: str, expected_bot_username: str):
    """
    Look up temp_tokens collection for short_id.
    - If found and not expired and bot matches: returns dict with message_id and the doc
    - If not found or expired: returns None
    IMPORTANT: This function does NOT delete the token. Deletion is done atomically at claim time.
    """
    try:
        doc = await temp_tokens_col.find_one({"_id": short_id})
        if not doc:
            return None
        # doc expiry stored as datetime in Mongo (expiry)
        expiry = doc.get("expiry")
        bot_for = doc.get("bot")
        # If bot mismatch, refuse
        if bot_for and bot_for.lower() != expected_bot_username.lower():
            LOGGER.warning(f"Short token bot mismatch: {bot_for} != {expected_bot_username}")
            return None
        # Check expiry timestamp if available
        expiry_ts = doc.get("expiry_ts")
        if expiry_ts:
            if time.time() > int(expiry_ts):
                LOGGER.warning("Short token expired (by expiry_ts).")
                return None
        # also check expiry datetime object if present
        # If reached here, consider token valid
        return doc
    except Exception as e:
        LOGGER.error(f"Error resolving short token {short_id}: {e}", exc_info=True)
        return None

async def claim_and_delete_short_token(short_id: str, user_id: int):
    """
    Atomically find and delete token doc. Returns doc if deletion returned doc (claimed), else None.
    This ensures single-use semantics: only the first claim succeeds.
    """
    try:
        doc = await temp_tokens_col.find_one_and_delete({"_id": short_id})
        return doc
    except Exception as e:
        LOGGER.error(f"Error claiming short token {short_id}: {e}", exc_info=True)
        return None

# -----------------------------
# Refresh file reference function (same as before)
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
# UI helpers, admin keyboard (same as before)
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
# Force-sub helper
# -----------------------------
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
# Robust /start handler (handles short tokens and legacy long tokens)
# -----------------------------
_START_RE = re.compile(r'^/start(?:@[\w_]+)?(?:[ =])?(.+)?$', flags=re.IGNORECASE)

@bot.on_message(filters.command("start") & filters.private)
async def start_command_handler(client: Client, message: Message):
    user_id = message.from_user.id

    # Admin /start goes to admin panel
    if user_id in Config.ADMIN_IDS:
        # Check if it's /start admin (for panel) or /start token (for testing)
        text = (message.text or "").strip()
        m = _START_RE.match(text)
        token_group = m.group(1) if m else None
        
        if not token_group:
            await admin_panel_handler(client, message)
            return
        # If admin uses /start with a token, let it pass through to test the token
        LOGGER.info(f"Admin {user_id} is testing a start token...")

    # Basic user checks
    user_data = await get_user_data(user_id)
    if user_data.get("is_banned", False):
        await message.reply_text("<b>❌ You are banned from using this bot.</b>", parse_mode=enums.ParseMode.HTML)
        return

    LOGGER.info(f"[START] from={user_id} text={message.text!r} date={message.date}")

    # Extract token string (tolerant)
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

    # Distinguish short-id tokens (alphanumeric base62 short ids) vs legacy tokens
    is_short = bool(re.fullmatch(r'[A-Za-z0-9]{4,20}', token))  # short tokens (e.g., 6-12 chars)
    bot_info = await bot.get_me()
    bot_username = bot_info.username or ""

    msg_id_to_send = None
    token_raw_for_claim = None

    if is_short:
        # Resolve short token from temp_tokens collection
        doc = await resolve_short_token(token, bot_username)
        if not doc:
            await message.reply_text(
                "<b>⏳ Link expired or invalid.</b>\n\nThis download link has expired or cannot be used. Please go back to the website to get a new link.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]),
                parse_mode=enums.ParseMode.HTML
            )
            return
        # do not delete yet; we will atomically claim it AFTER we successfully send the file
        msg_id_to_send = int(doc.get("message_id"))
        token_raw_for_claim = token  # the short id is used to claim
    else:
        # Try legacy long token verification
        msg_id_to_send, legacy_raw = await verify_token_basic(token, bot_username)
        if not msg_id_to_send:
            await message.reply_text(
                "<b>⏳ Link expired or invalid.</b>\n\nThis download link has expired or cannot be used. Please go back to the website to get a new link.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(Config.WEBSITE_BUTTON_TEXT, url=Config.WEBSITE_URL)]]),
                parse_mode=enums.ParseMode.HTML
            )
            return
        # For legacy tokens we will claim using issued_tokens_col (hash) if single-use enabled
        token_raw_for_claim = legacy_raw

    LOGGER.info(f"Resolved msg id to send: {msg_id_to_send}")

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

    # Daily limit check (skip for admins)
    if user_id not in Config.ADMIN_IDS:
        daily_limit = SETTINGS.get("daily_limit", 5)
        if daily_limit > 0 and user_data.get("daily_file_count", 0) >= daily_limit:
            await message.reply_text("<b>⚠️ Daily Limit Reached</b>\n\nYou have reached your daily download limit. Please try again tomorrow.", parse_mode=enums.ParseMode.HTML)
            return

    # Send file and then atomically claim the token (so we don't pre-mark and cause race)
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

        # Now claim token single-use semantics
        claimed = True
        if SETTINGS.get("single_use_tokens", True):
            if is_short:
                # atomically find_one_and_delete short token
                doc_claimed = await claim_and_delete_short_token(token_raw_for_claim, user_id)
                if not doc_claimed:
                    # someone else consumed it; remove the sent file we just delivered to avoid leakage
                    try:
                        await bot.delete_messages(chat_id=user_id, message_ids=sent_file_msg.id)
                    except Exception:
                        pass
                    await status_msg.edit_text("<b>❌ This link has already been used by another user.</b>\n\nPlease get a fresh link from the website.", parse_mode=enums.ParseMode.HTML)
                    return
            else:
                # legacy: use issued_tokens_col with token hash atomic claim
                # compute hash and try to atomically set used flag
                try:
                    raw_hash = token_hash_raw(token_raw_for_claim)
                    now = datetime.now(timezone.utc)
                    # attempt to update unused doc first
                    filt = {"hash": raw_hash, "$or": [{"used": False}, {"used": {"$exists": False}}]}
                    update = {"$set": {"used": True, "used_by": user_id, "used_at": now}}
                    doc = await issued_tokens_col.find_one_and_update(filt, update, return_document=ReturnDocument.AFTER)
                    if doc:
                        claimed = True
                    else:
                        # attempt to insert new document marking used (claim)
                        try:
                            await issued_tokens_col.insert_one({"hash": raw_hash, "issued_raw": token_raw_for_claim, "used": True, "used_by": user_id, "used_at": now, "issued_at": now})
                            claimed = True
                        except DuplicateKeyError:
                            # someone else claimed
                            doc_existing = await issued_tokens_col.find_one({"hash": raw_hash})
                            if doc_existing and doc_existing.get("used"):
                                claimed = False
                            else:
                                # attempt final update
                                updated = await issued_tokens_col.find_one_and_update({"hash": raw_hash, "used": False}, {"$set": {"used": True, "used_by": user_id, "used_at": now}}, return_document=ReturnDocument.AFTER)
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

        # Log for auto deletion and increment daily count (don't count for admins)
        if user_id not in Config.ADMIN_IDS:
            await log_sent_file_for_deletion(user_id, sent_file_msg, msg_id_to_send)
            await update_user_data(user_id, {"$inc": {"daily_file_count": 1}})
        else:
             await log_sent_file_for_deletion(user_id, sent_file_msg, msg_id_to_send)
             LOGGER.info(f"Admin {user_id} downloaded a file. Daily count not incremented.")
             
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
# Log sent file & auto-delete
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
            result = await users_col.update_many(
                {"daily_file_count": {"$gt": 0}},
                {"$set": {"daily_file_count": 0, "last_count_reset": datetime.now(timezone.utc).date().isoformat()}}
            )
            LOGGER.info(f"Reset daily limits for {result.modified_count} users.")
        except Exception as e:
            LOGGER.critical(f"Error resetting daily limits: {e}", exc_info=True)

# -----------------------------
# Admin commands
# -----------------------------
# (Assuming admin_panel_handler, broadcast handlers etc. are in a separate file or you add them here)
# Placeholder for admin panel
@bot.on_message(filters.command("admin") & filters.user(Config.ADMIN_IDS) & filters.private)
async def admin_panel_handler(client, message: Message):
     await message.reply_text("Admin Panel (To be implemented)", reply_markup=get_main_admin_keyboard())

# (You would add your other admin handlers for stats, settings, broadcast, ban, unban here)
# ...
# ... (all other admin callback handlers go here)
# ...


# -----------------------------
# Web server handlers (health + NEW API)
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


@routes.post('/api/create_token')
async def create_token_handler(request: web.Request):
    """
    NEW API ENDPOINT
    Called by PHP script to create tokens in MongoDB.
    """
    LOGGER.info("Received request on /api/create_token")

    # 1. Check authorization
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

    # 2. Get data from PHP
    try:
        data = await request.json()
        qualities_list = data.get('qualities_list')
        bot_username = data.get('bot_username')
        
        if not all([qualities_list, bot_username]):
            return web.json_response({'error': 'Missing required data: qualities_list or bot_username'}, status=400)
            
    except Exception as e:
        LOGGER.error(f"API JSON decode error: {e}")
        return web.json_response({'error': 'Invalid JSON body.'}, status=400)

    # 3. Process links (this logic is moved from PHP)
    download_links = []
    
    for item in qualities_list:
        try:
            quality = str(item.get('quality', 'Unknown'))
            message_id = int(item.get('id', 0))
            size = int(item.get('size', 0))

            if not message_id:
                continue

            # Create a basic signature (PHP equivalent) for storage
            # Note: This payload is simple, just for consistency. The bot doesn't *use* it.
            expiry_ts = int(time.time() + SETTINGS.get("token_expiry_seconds", Config.TOKEN_EXPIRY_SECONDS))
            payload = f"{message_id}:{expiry_ts}:{bot_username}:api_v2"
            signature = compute_expected_hmac(payload) # Use existing bot hmac function

            # Create the short token in MongoDB
            short_id = await create_short_token_for_msg(message_id, bot_username, signature)

            if short_id:
                link = f"https://t.me/{bot_username}?start={short_id}"
                download_links.append({
                    'quality': quality,
                    'size': size,
                    'link': link
                })
        except Exception as e:
            LOGGER.error(f"Error processing quality {item}: {e}", exc_info=True)

    # 4. Return the links array to PHP
    if not download_links:
        return web.json_response({'error': 'No links could be generated.'}, status=500)

    LOGGER.info(f"Successfully generated {len(download_links)} links for bot {bot_username}.")
    return web.json_response({'links': download_links}, status=200)


async def start_web_server():
    app = web.Application()
    app.add_routes(routes) # This registers all @routes handlers
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
# Admin token inspector (optional)
# -----------------------------
@bot.on_message(filters.command("inspect_token") & filters.user(Config.ADMIN_IDS) & filters.private)
async def inspect_token_cmd(client, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage: /inspect_token <shortid_or_longtoken>", parse_mode=enums.ParseMode.HTML)
        return
    token = message.text.split(" ", 1)[1].strip()
    # If short, check temp_tokens
    if re.fullmatch(r'[A-Za-z0-9]{4,20}', token):
        doc = await temp_tokens_col.find_one({"_id": token})
        await message.reply_text(f"<pre>{doc}</pre>", parse_mode=enums.ParseMode.HTML)
        return
    # else attempt to decode long token
    raw, err = parse_token_raw(token)
    if not raw:
        await message.reply_text(f"Decode error: {err}", parse_mode=enums.ParseMode.HTML)
        return
    h = token_hash_raw(raw)
    doc = await issued_tokens_col.find_one({"hash": h})
    await message.reply_text(f"<pre>{raw}</pre>\n\nhash: {h}\nDB record: {doc}", parse_mode=enums.ParseMode.HTML)

# -----------------------------
# Startup and lifecycle
# -----------------------------
async def main_startup_logic():
    global start_time
    start_time = time.time()
    LOGGER.info("Starting File Sender Bot V4.6 (with API)")
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
            await bot.send_message(Config.ADMIN_IDS[0], f"<b>✅ File Sender Bot (V4.6 + API) started.</b>", parse_mode=enums.ParseMode.HTML)
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
