import logging
import sqlite3
import json
import html
import re
from datetime import datetime, time, timedelta, timezone
from math import ceil
from typing import Optional, Tuple, List

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatMember,
    ChatMemberUpdated,
    User
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    ChatMemberHandler,
    MessageHandler,
    ConversationHandler,
    filters,
)
from telegram.error import TelegramError, BadRequest, Forbidden

# --- CONFIGURATION ---
# IMPORTANT: Replace these values with your actual data.
# Get your Bot Token from BotFather on Telegram.
BOT_TOKEN = "7752800446:AAFyI6lZRXF0BM07O-y0oNYaf7s-hd8PgrA"
# This is your personal Telegram User ID. You can get it from bots like @userinfobot.
ADMIN_ID = 7747045013  # Replace with your Telegram User ID
# The ID of your private channel. Get it from @userinfobot (forward a message from the channel).
CHANNEL_ID = -1003094091326

  # Replace with your Channel ID (must start with -100)
# --- END CONFIGURATION ---

# --- LOGGING SETUP ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- DATABASE SETUP ---
DB_NAME = "subscribers.db"

def setup_database():
    """Initializes the SQLite database and creates the necessary tables."""
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        # Table for bot-managed, "online" subscribers
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                plan_days INTEGER,
                remaining_days INTEGER,
                start_date TEXT,
                payment_info TEXT,
                is_active INTEGER DEFAULT 1,
                no_post_days TEXT DEFAULT '[]'
            )
        """)

        # New table for admin's "offline" manual records
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS offline_subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                identifier TEXT NOT NULL,
                plan_days INTEGER,
                remaining_days INTEGER,
                start_date TEXT,
                payment_info TEXT,
                no_post_days TEXT DEFAULT '[]'
            )
        """)

        # Table to track admin posting activity
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_activity (
                id INTEGER PRIMARY KEY,
                last_post_date TEXT
            )
        """)

        cursor.execute("INSERT OR IGNORE INTO admin_activity (id, last_post_date) VALUES (1, NULL)")
        conn.commit()
        conn.close()
        logger.info("Database setup complete.")

    except Exception as e:
        logger.error(f"Database setup error: {e}")
        raise

# --- UTILITY FUNCTIONS ---

def safe_text(text: str, max_length: int = 4096) -> str:
    """Safely escape text for Telegram and limit length."""
    if not text:
        return "N/A"

    # Remove or replace problematic characters
    text = str(text).strip()

    # Escape HTML entities
    text = html.escape(text)

    # Limit length
    if len(text) > max_length:
        text = text[:max_length-3] + "..."

    return text

def safe_username(username: str) -> str:
    """Safely format username."""
    if not username:
        return "N/A"

    username = str(username).strip()
    if username.startswith('@'):
        return username
    elif username and username != "N/A":
        return f"@{username}"

    return "N/A"

def format_user_info(user: User) -> Tuple[str, str]:
    """Safely format user information."""
    try:
        full_name = safe_text(user.full_name or "Unknown User")
        username = safe_username(user.username)
        return full_name, username
    except Exception as e:
        logger.error(f"Error formatting user info: {e}")
        return "Unknown User", "N/A"

def validate_days(days_text: str) -> Optional[int]:
    """Validate and convert days input."""
    try:
        days = int(days_text.strip())
        if days < 1 or days > 36500:  # Max ~100 years
            return None
        return days
    except (ValueError, AttributeError):
        return None

# --- DATABASE HELPER FUNCTIONS ---

def db_query(query: str, params: tuple = ()) -> List[tuple]:
    """Executes a database query and returns results with error handling."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return []

def safe_json_loads(json_str: str) -> List[str]:
    """Safely load JSON string, return empty list on error."""
    try:
        if not json_str:
            return []
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return []

def safe_json_dumps(data: List[str]) -> str:
    """Safely dump data to JSON string."""
    try:
        return json.dumps(data)
    except (TypeError, ValueError):
        return "[]"

# --- Conversation States ---
(SELECT_PLAN, CUSTOM_DAYS, GET_PAYMENT, GET_BROADCAST_MESSAGE, ADD_OFFLINE_USER_IDENTIFIER, GET_FIRST_NAME, GET_USERNAME) = range(7)

# --- SUBSCRIBER-FACING COMMANDS ---

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Allows a subscriber to check their own subscription status."""
    try:
        user_id = update.effective_user.id
        user_data = db_query("SELECT * FROM subscribers WHERE user_id = ?", (user_id,))

        if not user_data:
            await update.message.reply_text("You are not currently subscribed or you are an offline record.")
            return

        user = user_data[0]
        plan_days = "Lifetime" if user[3] == -1 else f"{user[3]} Days"
        remaining_days = "Infinite" if user[4] == -1 else str(user[4])
        no_post_days = safe_json_loads(user[8])

        message = (
            f"✨ Your Subscription Status ✨\n\n"
            f"▫️ Plan: {plan_days}\n"
            f"▫️ Posting Days Remaining: {remaining_days}\n"
            f"▫️ Start Date: {user[5] or 'N/A'}\n"
        )

        if no_post_days:
            message += "\nYour subscription was extended on these dates (no content posted):\n"
            for day in no_post_days[-5:]:  # Show only last 5 dates
                message += f"- {day}\n"
            if len(no_post_days) > 5:
                message += f"... and {len(no_post_days) - 5} more dates\n"

        await update.message.reply_text(message, parse_mode='HTML')

    except Exception as e:
        logger.error(f"Status command error: {e}")
        await update.message.reply_text("Sorry, there was an error retrieving your status.")

# --- ADMIN-FACING COMMANDS ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /start command."""
    try:
        if update.effective_user.id != ADMIN_ID:
            await update.message.reply_text("This is a private management bot.")
            return

        await update.message.reply_text(
            "Welcome, Admin! This is your Channel Guardian Bot.\n"
            "Use /dashboard to manage your subscribers."
        )
    except Exception as e:
        logger.error(f"Start command error: {e}")

async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the main admin dashboard."""
    try:
        if update.effective_user.id != ADMIN_ID:
            return

        keyboard = [
            [InlineKeyboardButton("📊 View Stats", callback_data="stats")],
            [InlineKeyboardButton("⏳ View Expiring Soon", callback_data="expiring_soon")],
            [InlineKeyboardButton("🗣️ Broadcast Message", callback_data="broadcast")],
            [InlineKeyboardButton("🔍 Check a User/Record", callback_data="check_user")],
            [InlineKeyboardButton("➕ Add Manual Entry", callback_data="add_manual_prompt")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        message_text = "👑 <b>Admin Dashboard</b>"

        if update.callback_query:
            await update.callback_query.edit_message_text(
                message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                message_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Dashboard command error: {e}")

# --- CORE WORKFLOWS & CONVERSATIONS ---

async def handle_first_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the first name input for manual user entry."""
    try:
        first_name = safe_text(update.message.text.strip(), 50)

        if not first_name or first_name == "N/A":
            await update.message.reply_text("Please provide a valid first name.")
            return GET_FIRST_NAME

        context.user_data['manual_first_name'] = first_name
        logger.info(f"Stored first name: {first_name}")

        await update.message.reply_text(
            f"✅ First name saved: <b>{first_name}</b>\n\n"
            f"Now please send me the <b>username</b> of the user (with or without @).\n\n"
            f"Example: @john_doe or john_doe\n\n"
            f"💡 <b>If the user has no username</b>, send the command: <code>/nousername</code>",
            parse_mode='HTML'
        )
        return GET_USERNAME

    except Exception as e:
        logger.error(f"Handle first name input error: {e}")
        await update.message.reply_text("Error processing first name. Please try again.")
        return GET_FIRST_NAME

async def handle_username_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the username input for manual user entry."""
    try:
        message_text = update.message.text.strip()

        if message_text == "/nousername":
            context.user_data['manual_username'] = None
            first_name = context.user_data.get('manual_first_name', 'Unknown')

            await update.message.reply_text(
                f"✅ No username noted for <b>{first_name}</b>\n\n"
                f"🔍 Now checking if this user is in your channel...",
                parse_mode='HTML'
            )

            return await process_manual_user_detection(update, context)
        else:
            username = message_text.replace('@', '').strip()

            if not username:
                await update.message.reply_text(
                    "Please provide a valid username or use <code>/nousername</code> if none.",
                    parse_mode='HTML'
                )
                return GET_USERNAME

            context.user_data['manual_username'] = username
            first_name = context.user_data.get('manual_first_name', 'Unknown')

            await update.message.reply_text(
                f"✅ Username saved: <b>@{username}</b>\n"
                f"✅ First name: <b>{first_name}</b>\n\n"
                f"🔍 Now checking if this user is in your channel...",
                parse_mode='HTML'
            )

            return await process_manual_user_detection(update, context)

    except Exception as e:
        logger.error(f"Handle username input error: {e}")
        await update.message.reply_text("Error processing username. Please try again.")
        return GET_USERNAME

async def process_manual_user_detection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes manual user detection to determine if they're active channel members."""
    try:
        first_name = context.user_data.get('manual_first_name')
        username = context.user_data.get('manual_username')

        logger.info(f"Processing manual user detection - Name: {first_name}, Username: {username}")

        detected_user = await search_channel_member(context, first_name, username)

        if detected_user:
            context.user_data['user_to_add'] = detected_user['user_id']
            context.user_data['detected_as_active'] = True

            await update.message.reply_text(
                f"🎉 <b>User Found in Channel!</b>\n\n"
                f"✅ <b>Name:</b> {detected_user['full_name']}\n"
                f"✅ <b>Username:</b> {detected_user['username']}\n"
                f"✅ <b>User ID:</b> <code>{detected_user['user_id']}</code>\n\n"
                f"This user will be added as an <b>ACTIVE MEMBER</b> (not offline record).\n\n"
                f"Please select a subscription plan:",
                parse_mode='HTML',
                reply_markup=create_user_plan_keyboard()
            )
            return SELECT_PLAN
        else:
            await update.message.reply_text(
                f"❌ <b>User Not Found in Channel</b>\n\n"
                f"The user with:\n"
                f"📝 <b>Name:</b> {first_name}\n"
                f"📝 <b>Username:</b> {'@' + username if username else 'No username'}\n\n"
                f"Could not be found in the channel. Would you like to create an <b>offline record</b> for them instead?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📝 Yes, Create Offline Record", callback_data="create_offline")],
                    [InlineKeyboardButton("🔄 Try Different Name/Username", callback_data="retry_search")],
                    [InlineKeyboardButton("❌ Cancel", callback_data="cancel_manual")]
                ])
            )
            return SELECT_PLAN

    except Exception as e:
        logger.error(f"Process manual user detection error: {e}")
        await update.message.reply_text("Error processing user detection. Please try again.")
        return ConversationHandler.END

# MODIFIED: Rewrote this function for better reliability and logging.
async def search_channel_member(context: ContextTypes.DEFAULT_TYPE, first_name: str, username: str = None) -> Optional[dict]:
    """
    Searches for a channel member.
    NOTE: The most reliable method is searching by a public @username.
    Searching by first_name is limited and may only find channel administrators.
    """
    try:
        logger.info(f"Searching for channel member - Name: {first_name}, Username: {username}")

        # Method 1: Search by username (most reliable)
        if username:
            try:
                logger.info(f"Attempting to resolve username: @{username}")
                user_info = await context.bot.get_chat(f"@{username}")

                if user_info.type != "private":
                    logger.warning(f"Resolved @{username}, but it is not a user (type: {user_info.type})")
                else:
                    try:
                        logger.info(f"Checking membership for user ID: {user_info.id} in channel {CHANNEL_ID}")
                        member = await context.bot.get_chat_member(CHANNEL_ID, user_info.id)

                        if member.status not in ["left", "kicked", "banned"]:
                            logger.info(f"SUCCESS: Found user @{username} in channel.")
                            full_name, username_formatted = format_user_info(user_info)
                            return {
                                'user_id': user_info.id,
                                'full_name': full_name,
                                'username': username_formatted
                            }
                        else:
                             logger.info(f"User @{username} (ID: {user_info.id}) exists but has status '{member.status}' in the channel.")
                    except BadRequest as e:
                        if "user not found" in str(e).lower():
                            logger.info(f"User @{username} (ID: {user_info.id}) is not a member of the channel.")
                        else:
                            logger.error(f"BadRequest when checking membership for {user_info.id}: {e}")
                    except Exception as e:
                        logger.error(f"Error checking channel membership for {user_info.id}: {e}")

            except BadRequest as e:
                if "user not found" in str(e).lower():
                     logger.warning(f"Could not resolve username @{username}. User may not exist or has privacy settings.")
                else:
                    logger.error(f"BadRequest when resolving @{username}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error resolving username @{username}: {e}")

        # Method 2: Fallback search in channel administrators (by name)
        logger.info("Username search failed or not provided. Falling back to searching administrators by name.")
        try:
            admins = await context.bot.get_chat_administrators(CHANNEL_ID)
            for admin in admins:
                user = admin.user
                if user.is_bot:
                    continue

                if user.first_name and re.search(r'\b' + re.escape(first_name) + r'\b', user.first_name, re.IGNORECASE):
                    logger.info(f"SUCCESS: Found matching administrator by name: {user.full_name}")
                    full_name, username_formatted = format_user_info(user)
                    return {
                        'user_id': user.id,
                        'full_name': full_name,
                        'username': username_formatted
                    }
        except Exception as e:
            logger.error(f"Error getting or searching channel administrators: {e}")

        logger.warning(f"User not found using any method - Name: {first_name}, Username: {username}")
        return None

    except Exception as e:
        logger.error(f"General error in search_channel_member: {e}")
        return None

def create_user_plan_keyboard():
    """Creates keyboard for active user plan selection."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("7 Days", callback_data="user_plan:7"),
            InlineKeyboardButton("14 Days", callback_data="user_plan:14"),
        ],
        [
            InlineKeyboardButton("30 Days", callback_data="user_plan:30"),
            InlineKeyboardButton("Lifetime", callback_data="user_plan:-1"),
        ],
        [InlineKeyboardButton("Custom Days", callback_data="user_plan:custom")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_approval")]
    ])

def create_offline_plan_keyboard():
    """Creates keyboard for offline record plan selection."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("7 Days", callback_data="offline_plan:7"),
            InlineKeyboardButton("14 Days", callback_data="offline_plan:14"),
        ],
        [
            InlineKeyboardButton("30 Days", callback_data="offline_plan:30"),
            InlineKeyboardButton("Lifetime", callback_data="offline_plan:-1"),
        ],
        [InlineKeyboardButton("Custom Days", callback_data="offline_plan:custom")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_offline")]
    ])

def extract_status(chat_member_update: ChatMemberUpdated) -> Tuple[Optional[bool], Optional[bool]]:
    """Extracts user and status changes from a ChatMemberUpdated event."""
    try:
        status_change = chat_member_update.difference().get("status")
        if status_change is None:
            return None, None

        old_is_member, new_is_member = (
            (False, True) if status_change in [
                (ChatMember.BANNED, ChatMember.MEMBER),
                (ChatMember.LEFT, ChatMember.MEMBER),
                (ChatMember.RESTRICTED, ChatMember.MEMBER),
            ] else (True, False)
        )
        return old_is_member, new_is_member
    except Exception as e:
        logger.error(f"Extract status error: {e}")
        return None, None

async def track_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tracks new members joining the channel."""
    try:
        was_member, is_member = extract_status(update.chat_member)
        if was_member is None:
            return

        user = update.chat_member.new_chat_member.user
        if not was_member and is_member:
            full_name, username = format_user_info(user)
            logger.info(f"{full_name} ({user.id}) joined the channel.")

            keyboard = [
                [InlineKeyboardButton(f"✅ Approve {full_name[:20]}", callback_data=f"approve:{user.id}")],
                [InlineKeyboardButton("🔍 Check User Info", callback_data=f"info:{user.id}")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=(
                    f"🚨 <b>New Member Alert</b> 🚨\n\n"
                    f"<b>Name:</b> {full_name}\n"
                    f"<b>Username:</b> {username}\n"
                    f"<b>User ID:</b> <code>{user.id}</code>\n\n"
                    f"This user will be actively managed by the bot."
                ),
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Track chats error: {e}")

async def plan_selection_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Sends the plan selection menu for an online user."""
    try:
        query = update.callback_query
        await query.answer()

        callback_parts = query.data.split(':')
        if len(callback_parts) != 2:
            logger.error(f"Invalid callback data format: {query.data}")
            await query.edit_message_text("Error: Invalid user selection.")
            return ConversationHandler.END

        user_id_str = callback_parts[1]
        try:
            user_id = int(user_id_str)
        except ValueError:
            logger.error(f"Invalid user ID in callback: {user_id_str}")
            await query.edit_message_text("Error: Invalid user ID.")
            return ConversationHandler.END

        context.user_data['user_to_add'] = user_id

        try:
            user_info = await context.bot.get_chat(user_id)
            full_name, username = format_user_info(user_info)
        except Exception as e:
            logger.error(f"Could not get user info for {user_id}: {e}")
            full_name = f"User {user_id}"

        reply_markup = create_user_plan_keyboard()

        await query.edit_message_text(
            f"Setting up subscription for: <b>{full_name}</b>\n\nPlease select the subscription plan:",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        return SELECT_PLAN

    except Exception as e:
        logger.error(f"Plan selection prompt error: {e}")
        try:
            await query.edit_message_text("Error processing request. Please try again.")
        except:
            pass
        return ConversationHandler.END

async def select_plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles selection of a predefined plan for regular user and asks for payment info."""
    try:
        query = update.callback_query
        await query.answer()

        plan_days = int(query.data.split(':')[1])
        context.user_data['plan_days'] = plan_days

        user_id = context.user_data.get('user_to_add')
        plan_text = "Lifetime" if plan_days == -1 else f"{plan_days} days"

        try:
            user_info = await context.bot.get_chat(user_id)
            full_name, _ = format_user_info(user_info)
        except:
            full_name = f"User {user_id}"

        await query.edit_message_text(
            f"Plan selected: <b>{plan_text}</b> for <b>{full_name}</b>\n\n"
            f"Please reply with the payment proof details.",
            parse_mode='HTML'
        )
        return GET_PAYMENT
    except Exception as e:
        logger.error(f"Select plan callback error: {e}")
        await query.edit_message_text("Error selecting plan. Please try again.")
        return ConversationHandler.END

async def select_custom_plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles selection of the custom days plan button for regular user."""
    try:
        query = update.callback_query
        await query.answer()

        user_id = context.user_data.get('user_to_add')
        try:
            user_info = await context.bot.get_chat(user_id)
            full_name, _ = format_user_info(user_info)
        except:
            full_name = f"User {user_id}"

        await query.edit_message_text(
            f"Creating custom plan for: <b>{full_name}</b>\n\n"
            f"Please enter the custom number of days (1-36500):",
            parse_mode='HTML'
        )
        return CUSTOM_DAYS
    except Exception as e:
        logger.error(f"Select custom plan callback error: {e}")
        await query.edit_message_text("Error processing request. Please try again.")
        return ConversationHandler.END

async def handle_custom_days_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the user's message containing the number of custom days."""
    try:
        days_input = update.message.text.strip()
        days = validate_days(days_input)

        if days is None:
            await update.message.reply_text(
                "Please enter a valid number between 1 and 36500."
            )
            return CUSTOM_DAYS

        context.user_data['plan_days'] = days
        await update.message.reply_text(
            f"Set custom plan to {days} days. Now, please reply with the payment proof details."
        )
        return GET_PAYMENT
    except Exception as e:
        logger.error(f"Handle custom days input error: {e}")
        await update.message.reply_text("Error processing input. Please try again.")
        return CUSTOM_DAYS

# NEW: This function handles ONLY the new user approval flow, preventing state conflicts.
async def handle_new_user_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Saves payment info and finalizes the subscription for a new, approved user."""
    try:
        payment_info = safe_text(update.message.text, 1000)
        user_id = context.user_data.get('user_to_add')
        plan_days = context.user_data.get('plan_days')

        if not user_id or plan_days is None:
            await update.message.reply_text("Error: Missing user or plan info. Please start over by approving the user again.")
            context.user_data.clear()
            return ConversationHandler.END

        today = datetime.now().strftime("%Y-%m-%d")

        try:
            user_info = await context.bot.get_chat(user_id)
            full_name, username = format_user_info(user_info)

            db_query(
                """INSERT OR REPLACE INTO subscribers
                   (user_id, username, first_name, plan_days, remaining_days, start_date, payment_info, is_active, no_post_days)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                (user_id, username, full_name, plan_days, plan_days, today, payment_info, "[]")
            )

            days_text = "Lifetime" if plan_days == -1 else f"{plan_days} days"
            await update.message.reply_text(
                f"✅ <b>Success!</b>\n"
                f"User {full_name} is now a managed subscriber with a {days_text} plan.",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error adding new subscriber {user_id}: {e}")
            await update.message.reply_text(f"An error occurred while adding the user: {str(e)}. Please check logs.")

        context.user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Handle new user payment error: {e}")
        await update.message.reply_text("An unexpected error occurred. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END


# MODIFIED: This function now only handles manual/offline entries.
async def handle_manual_payment_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Saves payment info and finalizes subscription for manual entries (detected active or offline)."""
    try:
        payment_info = safe_text(update.message.text, 1000)
        plan_days = context.user_data.get('plan_days')

        if plan_days is None:
            await update.message.reply_text("Error: Plan not selected. Please start over.")
            context.user_data.clear()
            return ConversationHandler.END

        today = datetime.now().strftime("%Y-%m-%d")

        detected_as_active = context.user_data.get('detected_as_active', False)
        offline_identifier = context.user_data.get('offline_identifier')
        user_id = context.user_data.get('user_to_add')

        if detected_as_active and user_id:
            try:
                user_info = await context.bot.get_chat(user_id)
                full_name, username = format_user_info(user_info)

                db_query(
                    """INSERT OR REPLACE INTO subscribers
                       (user_id, username, first_name, plan_days, remaining_days, start_date, payment_info, is_active, no_post_days)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)""",
                    (user_id, username, full_name, plan_days, plan_days, today, payment_info, "[]")
                )

                days_text = "Lifetime" if plan_days == -1 else f"{plan_days} days"
                await update.message.reply_text(
                    f"🎉 <b>Success! Active Member Added</b>\n\n"
                    f"✅ <b>User:</b> {full_name}\n"
                    f"✅ <b>Username:</b> {username}\n"
                    f"✅ <b>Plan:</b> {days_text}\n"
                    f"✅ <b>Status:</b> Active Channel Member\n\n"
                    f"This user was detected in your channel and added as an active managed subscriber!",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.error(f"Error adding detected active user {user_id}: {e}")
                await update.message.reply_text(f"An error occurred while adding the detected user: {str(e)}")

        elif offline_identifier:
            db_query(
                """INSERT INTO offline_subscribers
                   (identifier, plan_days, remaining_days, start_date, payment_info, no_post_days)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (offline_identifier, plan_days, plan_days, today, payment_info, "[]")
            )

            days_text = "Lifetime" if plan_days == -1 else f"{plan_days} days"
            await update.message.reply_text(
                f"📝 <b>Offline Record Saved!</b>\n\n"
                f"✅ <b>Identifier:</b> {offline_identifier}\n"
                f"✅ <b>Plan:</b> {days_text}\n"
                f"✅ <b>Status:</b> Offline Record\n\n"
                f"This user was not detected in your channel, so it's saved as an offline record.",
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text("Error: Could not determine user type. Please start over.")

        context.user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Handle manual payment info error: {e}")
        await update.message.reply_text("Error processing payment information. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

# --- DASHBOARD CALLBACK HANDLERS ---

async def display_detailed_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays a detailed breakdown of channel statistics."""
    try:
        query = update.callback_query
        await query.answer()

        managed_active = db_query("SELECT COUNT(*) FROM subscribers WHERE is_active = 1")[0][0]
        offline_active = db_query("SELECT COUNT(*) FROM offline_subscribers")[0][0]
        total_active = managed_active + offline_active

        managed_lifetime = db_query("SELECT COUNT(*) FROM subscribers WHERE is_active = 1 AND plan_days = -1")[0][0]
        offline_lifetime = db_query("SELECT COUNT(*) FROM offline_subscribers WHERE plan_days = -1")[0][0]
        total_lifetime = managed_lifetime + offline_lifetime

        expiring_managed = db_query("SELECT COUNT(*) FROM subscribers WHERE remaining_days BETWEEN 1 AND 3 AND is_active = 1")[0][0]
        expiring_offline = db_query("SELECT COUNT(*) FROM offline_subscribers WHERE remaining_days BETWEEN 1 AND 3")[0][0]
        total_expiring = expiring_managed + expiring_offline

        message = (
            f"📊 <b>Detailed Channel Stats</b>\n\n"
            f"👤 <b>Total Active Subscribers:</b> {total_active}\n"
            f"   - Managed Members: {managed_active}\n"
            f"   - Offline Records: {offline_active}\n\n"
            f"✨ <b>Lifetime Subscribers:</b> {total_lifetime}\n"
            f"   - Managed Members: {managed_lifetime}\n"
            f"   - Offline Records: {offline_lifetime}\n\n"
            f"⏳ <b>Expiring Soon (≤3 days):</b> {total_expiring}"
        )

        keyboard = [[InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="back_to_dashboard")]]
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Display detailed stats error: {e}")
        await query.edit_message_text("Error retrieving statistics.")

async def dashboard_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button clicks from the main dashboard for non-conversation starting actions."""
    try:
        query = update.callback_query
        await query.answer()
        action = query.data

        if action == "expiring_soon":
            managed_expiring = db_query(
                "SELECT first_name, remaining_days FROM subscribers WHERE remaining_days BETWEEN 1 AND 3 AND is_active = 1"
            )
            offline_expiring = db_query(
                "SELECT identifier, remaining_days FROM offline_subscribers WHERE remaining_days BETWEEN 1 AND 3"
            )

            message = "⏳ <b>Expiring Soon (3 days or less)</b>\n"

            if not managed_expiring and not offline_expiring:
                message += "\nNo one is expiring soon."

            if managed_expiring:
                message += "\n<b>Managed Members:</b>\n"
                for user in managed_expiring[:10]:
                    safe_name = safe_text(user[0], 30)
                    message += f"- {safe_name} - {user[1]} days left\n"
                if len(managed_expiring) > 10:
                    message += f"... and {len(managed_expiring) - 10} more\n"

            if offline_expiring:
                message += "\n<b>Offline Records:</b>\n"
                for rec in offline_expiring[:10]:
                    safe_identifier = safe_text(rec[0], 30)
                    message += f"- {safe_identifier} - {rec[1]} days left\n"
                if len(offline_expiring) > 10:
                    message += f"... and {len(offline_expiring) - 10} more\n"

            keyboard = [[InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="back_to_dashboard")]]
            await query.edit_message_text(
                message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Dashboard callbacks error: {e}")
        await query.edit_message_text("Error processing request.")

async def dashboard_conversation_starter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts a conversation from a dashboard button."""
    try:
        query = update.callback_query
        await query.answer()
        action = query.data

        logger.info(f"Dashboard conversation starter called with action: {action}")

        if action == "broadcast":
            await query.edit_message_text(
                "Please send the message you want to broadcast to all MANAGED subscribers."
            )
            return GET_BROADCAST_MESSAGE

        elif action == "add_manual_prompt":
            await query.edit_message_text(
                "🆕 <b>Adding Manual User Entry</b>\n\n"
                "Please send me the <b>first name</b> of the user you want to add.\n\n"
                "Example: John, Sarah, Alex",
                parse_mode='HTML'
            )
            return GET_FIRST_NAME

        else:
            logger.error(f"Unknown action in dashboard conversation starter: {action}")
            await query.edit_message_text("Unknown action. Please try again.")
            return ConversationHandler.END

    except Exception as e:
        logger.error(f"Dashboard conversation starter error: {e}")
        try:
            await query.edit_message_text("Error starting operation. Please try again.")
        except:
            pass
        return ConversationHandler.END

async def back_to_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Returns to the main dashboard view from a sub-menu."""
    try:
        query = update.callback_query
        await query.answer()

        keyboard = [
            [InlineKeyboardButton("📊 View Stats", callback_data="stats")],
            [InlineKeyboardButton("⏳ View Expiring Soon", callback_data="expiring_soon")],
            [InlineKeyboardButton("🗣️ Broadcast Message", callback_data="broadcast")],
            [InlineKeyboardButton("🔍 Check a User/Record", callback_data="check_user")],
            [InlineKeyboardButton("➕ Add Manual Entry", callback_data="add_manual_prompt")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "👑 <b>Admin Dashboard</b>",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Back to dashboard error: {e}")

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Sends the broadcast message to all managed users."""
    try:
        message_to_send = safe_text(update.message.text, 4000)
        active_users = db_query("SELECT user_id FROM subscribers WHERE is_active = 1")

        await update.message.reply_text(
            f"Starting broadcast to {len(active_users)} managed users... This may take a moment."
        )

        sent, failed = 0, 0
        for user in active_users:
            try:
                await context.bot.send_message(chat_id=user[0], text=message_to_send)
                sent += 1
            except (Forbidden, BadRequest) as e:
                logger.warning(f"Failed to send broadcast to {user[0]}: {e}")
                failed += 1
            except Exception as e:
                logger.error(f"Unexpected error sending broadcast to {user[0]}: {e}")
                failed += 1

        await update.message.reply_text(
            f"Broadcast complete.\n- Sent successfully: {sent}\n- Failed: {failed}"
        )
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Handle broadcast message error: {e}")
        await update.message.reply_text("Error processing broadcast message.")
        return ConversationHandler.END

# --- USER LIST FEATURE ---

USERS_PER_PAGE = 8

async def display_user_list(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    """Displays a paginated list of all users and records."""
    try:
        query = update.callback_query
        if query:
            await query.answer()

        managed_users = db_query("SELECT user_id, first_name FROM subscribers ORDER BY first_name")
        offline_users = db_query("SELECT id, identifier FROM offline_subscribers ORDER BY identifier")

        all_users = [('managed', user_id, safe_text(name, 30)) for user_id, name in managed_users] + \
                    [('offline', rec_id, safe_text(identifier, 30)) for rec_id, identifier in offline_users]

        if not all_users:
            keyboard = [[InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="back_to_dashboard")]]
            message = "You have no subscribers or records yet."
            if query:
                await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                await update.message.reply_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
            return

        total_pages = ceil(len(all_users) / USERS_PER_PAGE)
        start_index = page * USERS_PER_PAGE
        end_index = start_index + USERS_PER_PAGE
        users_on_page = all_users[start_index:end_index]

        keyboard = []
        for user_type, user_id, name in users_on_page:
            button_text = f"✅ {name}" if user_type == 'managed' else f"📝 {name}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"show_detail:{user_type}:{user_id}")])

        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"user_page:{page - 1}"))

        nav_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="noop"))

        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_page:{page + 1}"))

        if nav_row:
            keyboard.append(nav_row)

        keyboard.append([InlineKeyboardButton("⬅️ Back to Dashboard", callback_data="back_to_dashboard")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "Select a user or record to view details:"

        if query:
            await query.edit_message_text(message, reply_markup=reply_markup)
        else:
            await update.message.reply_text(message, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Display user list error: {e}")
        if query:
            await query.edit_message_text("Error displaying user list.")
        else:
            await update.message.reply_text("Error displaying user list.")

async def navigate_user_list_pages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the Prev/Next buttons for the user list."""
    try:
        query = update.callback_query
        await query.answer()
        page = int(query.data.split(':')[1])
        await display_user_list(update, context, page=page)
    except Exception as e:
        logger.error(f"Navigate user list pages error: {e}")
        await query.edit_message_text("Error navigating pages.")

async def display_user_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the details for a selected user or record."""
    try:
        query = update.callback_query
        await query.answer()

        parts = query.data.split(':')
        if len(parts) != 3:
            await query.edit_message_text("Invalid user selection.")
            return

        _, user_type, entry_id_str = parts
        entry_id = int(entry_id_str)

        message = "Could not find the specified user or record."
        no_post_days = []

        if user_type == 'managed':
            user_data = db_query("SELECT * FROM subscribers WHERE user_id = ?", (entry_id,))
            if user_data:
                user = user_data[0]
                plan_days = "Lifetime" if user[3] == -1 else f"{user[3]} Days"
                remaining_days = "Infinite" if user[4] == -1 else str(user[4])
                no_post_days = safe_json_loads(user[8])

                safe_name = safe_text(user[2], 50)
                safe_username = safe_text(user[1], 50)
                safe_payment = safe_text(user[6], 100)

                message = (
                    f"<b>Type: Managed Subscriber</b> ✅\n\n"
                    f"<b>Name:</b> {safe_name}\n"
                    f"<b>User ID:</b> <code>{user[0]}</code>\n"
                    f"<b>Username:</b> {safe_username}\n"
                    f"<b>Plan:</b> {plan_days}\n"
                    f"<b>Remaining:</b> {remaining_days} days\n"
                    f"<b>Start Date:</b> {user[5] or 'N/A'}\n"
                    f"<b>Payment:</b> {safe_payment}\n"
                )

        elif user_type == 'offline':
            offline_data = db_query("SELECT * FROM offline_subscribers WHERE id = ?", (entry_id,))
            if offline_data:
                rec = offline_data[0]
                plan_days = "Lifetime" if rec[2] == -1 else f"{rec[2]} Days"
                remaining_days = "Infinite" if rec[3] == -1 else str(rec[3])
                no_post_days = safe_json_loads(rec[6])

                safe_identifier = safe_text(rec[1], 50)
                safe_payment = safe_text(rec[5], 100)

                message = (
                    f"<b>Type: Offline Record</b> 📝\n\n"
                    f"<b>Identifier:</b> {safe_identifier}\n"
                    f"<b>Plan:</b> {plan_days}\n"
                    f"<b>Remaining:</b> {remaining_days} days\n"
                    f"<b>Start Date:</b> {rec[4] or 'N/A'}\n"
                    f"<b>Payment:</b> {safe_payment}\n"
                )

        if no_post_days:
            message += "\n<b>Non-Posting Days:</b>\n"
            display_days = no_post_days[-5:] if len(no_post_days) > 5 else no_post_days
            for day in display_days:
                message += f"- {day}\n"
            if len(no_post_days) > 5:
                message += f"... and {len(no_post_days) - 5} more dates\n"

        keyboard = [[InlineKeyboardButton("⬅️ Back to List", callback_data="check_user")]]
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"Display user details error: {e}")
        await query.edit_message_text("Error displaying user details.")

async def select_offline_plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles selection of a predefined plan for offline user and asks for payment info."""
    try:
        query = update.callback_query
        await query.answer()

        plan_days = int(query.data.split(':')[1])
        context.user_data['plan_days'] = plan_days

        offline_identifier = context.user_data.get('offline_identifier', 'Unknown')
        plan_text = "Lifetime" if plan_days == -1 else f"{plan_days} days"

        logger.info(f"Selected plan {plan_text} for offline user {offline_identifier}")

        await query.edit_message_text(
            f"Plan selected: <b>{plan_text}</b> for <b>{offline_identifier}</b>\n\n"
            f"Please reply with the payment proof details.",
            parse_mode='HTML'
        )
        return GET_PAYMENT
    except Exception as e:
        logger.error(f"Select offline plan callback error: {e}")
        await query.edit_message_text("Error selecting plan. Please try again.")
        return ConversationHandler.END

async def select_offline_custom_plan_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles selection of the custom days plan button for offline user."""
    try:
        query = update.callback_query
        await query.answer()

        offline_identifier = context.user_data.get('offline_identifier', 'Unknown')

        await query.edit_message_text(
            f"Creating custom plan for: <b>{offline_identifier}</b>\n\n"
            f"Please enter the custom number of days (1-36500):",
            parse_mode='HTML'
        )
        return CUSTOM_DAYS
    except Exception as e:
        logger.error(f"Select offline custom plan callback error: {e}")
        await query.edit_message_text("Error processing request. Please try again.")
        return ConversationHandler.END

async def handle_detection_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the choice between offline record, force active, or retry search."""
    try:
        query = update.callback_query
        await query.answer()

        choice = query.data
        first_name = context.user_data.get('manual_first_name')
        username = context.user_data.get('manual_username')

        if choice == "create_offline":
            identifier = f"{first_name}"
            if username:
                identifier += f" (@{username})"

            context.user_data['offline_identifier'] = identifier
            context.user_data['detected_as_active'] = False

            await query.edit_message_text(
                f"📝 <b>Creating Offline Record</b>\n\n"
                f"✅ <b>Identifier:</b> {identifier}\n\n"
                f"Please select a subscription plan:",
                parse_mode='HTML',
                reply_markup=create_offline_plan_keyboard()
            )
            return SELECT_PLAN

        elif choice == "retry_search":
            await query.edit_message_text(
                f"🔄 <b>Let's try again</b>\n\n"
                f"Please send me the <b>first name</b> of the user you want to add.",
                parse_mode='HTML'
            )
            return GET_FIRST_NAME

        elif choice == "cancel_manual":
            await query.edit_message_text("❌ Manual user addition cancelled.")
            context.user_data.clear()
            return ConversationHandler.END

    except Exception as e:
        logger.error(f"Handle detection choice error: {e}")
        await query.edit_message_text("Error processing choice. Please try again.")
        return ConversationHandler.END

async def cancel_offline_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles cancellation of offline user creation."""
    try:
        query = update.callback_query
        await query.answer()

        offline_identifier = context.user_data.get('offline_identifier', 'Unknown')
        await query.edit_message_text(f"❌ Cancelled creating offline record for: {offline_identifier}")

        context.user_data.clear()
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Cancel offline handler error: {e}")
        try:
            await query.edit_message_text("❌ Operation cancelled")
        except:
            pass
        context.user_data.clear()
        return ConversationHandler.END

async def cancel_approval_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles cancellation of user approval process."""
    try:
        query = update.callback_query
        await query.answer()

        user_id = context.user_data.get('user_to_add')
        if user_id:
            try:
                user_info = await context.bot.get_chat(user_id)
                full_name, _ = format_user_info(user_info)
                await query.edit_message_text(f"❌ Approval cancelled for {full_name}")
            except:
                await query.edit_message_text(f"❌ Approval cancelled for User ID {user_id}")
        else:
            await query.edit_message_text("❌ Operation cancelled")

        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Cancel approval handler error: {e}")
        await query.edit_message_text("❌ Operation cancelled")
        return ConversationHandler.END


async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels any active conversation."""
    try:
        await update.message.reply_text("Operation cancelled.")
        context.user_data.clear()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Cancel conversation error: {e}")
        return ConversationHandler.END

# --- DAILY JOB & AUTOMATION ---

async def daily_subscription_check(context: ContextTypes.DEFAULT_TYPE):
    """The daily job to update all subscriptions."""
    try:
        logger.info("Running daily subscription check...")
        today = datetime.now().strftime("%Y-%m-%d")

        last_post_result = db_query("SELECT last_post_date FROM admin_activity WHERE id = 1")
        last_post_date = last_post_result[0][0] if last_post_result else None
        admin_posted_today = last_post_date == today

        # --- Process Managed Subscribers ---
        managed_users = db_query(
            "SELECT user_id, remaining_days, no_post_days FROM subscribers WHERE is_active = 1 AND plan_days != -1"
        )

        for user_id, remaining_days, no_post_days_json in managed_users:
            try:
                no_post_days = safe_json_loads(no_post_days_json)
                new_remaining_days = remaining_days

                if admin_posted_today:
                    new_remaining_days = max(0, remaining_days - 1)
                    db_query(
                        "UPDATE subscribers SET remaining_days = ? WHERE user_id = ?",
                        (new_remaining_days, user_id)
                    )
                else:
                    if today not in no_post_days:
                        no_post_days.append(today)
                        db_query(
                            "UPDATE subscribers SET no_post_days = ? WHERE user_id = ?",
                            (safe_json_dumps(no_post_days), user_id)
                        )

                if new_remaining_days in [1, 2, 3]:
                    try:
                        await context.bot.send_message(
                            user_id,
                            f"👋 You have {new_remaining_days} days left on your subscription."
                        )
                    except Exception as e:
                        logger.warning(f"Could not send reminder to {user_id}: {e}")

                if new_remaining_days <= 0:
                    user_info_result = db_query("SELECT first_name FROM subscribers WHERE user_id = ?", (user_id,))
                    if user_info_result:
                        user_name = safe_text(user_info_result[0][0], 30)
                        keyboard = [[InlineKeyboardButton("📝 Extend Subscription", callback_data=f"extend:{user_id}:7")]]
                        await context.bot.send_message(
                            ADMIN_ID,
                            f"🔔 <b>Subscription Expired</b> 🔔\n\n"
                            f"Managed user <b>{user_name}</b> (<code>{user_id}</code>) subscription has ended.\n\n"
                            f"⚠️ <b>Action Required:</b>\n"
                            f"• Manually remove user from channel if needed\n"
                            f"• Or extend their subscription below.",
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode='HTML'
                        )
                    db_query("UPDATE subscribers SET is_active = 0 WHERE user_id = ?", (user_id,))

            except Exception as e:
                logger.error(f"Error processing managed user {user_id}: {e}")

        # --- Process Offline Records ---
        offline_records = db_query(
            "SELECT id, remaining_days, no_post_days FROM offline_subscribers WHERE plan_days != -1"
        )

        for rec_id, remaining_days, no_post_days_json in offline_records:
            try:
                no_post_days = safe_json_loads(no_post_days_json)

                if admin_posted_today:
                    new_remaining_days = max(0, remaining_days - 1)
                    db_query("UPDATE offline_subscribers SET remaining_days = ? WHERE id = ?", (new_remaining_days, rec_id))
                else:
                    if today not in no_post_days:
                        no_post_days.append(today)
                        db_query("UPDATE offline_subscribers SET no_post_days = ? WHERE id = ?", (safe_json_dumps(no_post_days), rec_id))

            except Exception as e:
                logger.error(f"Error processing offline record {rec_id}: {e}")

        logger.info("Daily subscription check completed successfully.")

    except Exception as e:
        logger.error(f"Daily subscription check failed: {e}")

async def admin_post_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detects when the admin posts in the channel."""
    try:
        if update.effective_user.id == ADMIN_ID:
            today = datetime.now().strftime("%Y-%m-%d")
            db_query("UPDATE admin_activity SET last_post_date = ? WHERE id = 1", (today,))
            logger.info(f"Admin post detected on {today}.")
    except Exception as e:
        logger.error(f"Admin post handler error: {e}")

# --- GENERAL CALLBACK QUERY HANDLER ---

async def general_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for simple, non-conversation button clicks."""
    try:
        query = update.callback_query
        await query.answer()

        parts = query.data.split(':')
        action = parts[0]

        if action == "extend" and len(parts) >= 3:
            user_id, days = int(parts[1]), int(parts[2])

            user_info_result = db_query("SELECT first_name FROM subscribers WHERE user_id = ?", (user_id,))
            user_name = "Unknown User"
            if user_info_result:
                user_name = safe_text(user_info_result[0][0], 30)

            db_query(
                "UPDATE subscribers SET remaining_days = remaining_days + ?, is_active = 1 WHERE user_id = ?",
                (days, user_id)
            )
            await query.edit_message_text(
                f"✅ <b>Subscription Extended</b>\n\n"
                f"User: <b>{user_name}</b> (<code>{user_id}</code>)\n"
                f"Extended by: <b>{days} days</b>",
                parse_mode='HTML'
            )

        elif action == "info" and len(parts) >= 2:
            user_id = int(parts[1])
            try:
                user_info = await context.bot.get_chat(user_id)
                full_name, username = format_user_info(user_info)

                existing_user = db_query("SELECT * FROM subscribers WHERE user_id = ?", (user_id,))

                message = (
                    f"👤 <b>User Information</b>\n\n"
                    f"<b>Name:</b> {full_name}\n"
                    f"<b>Username:</b> {username}\n"
                    f"<b>User ID:</b> <code>{user_id}</code>\n"
                    f"<b>Status:</b> {'Already subscribed' if existing_user else 'Not subscribed'}"
                )

                keyboard = []
                if not existing_user:
                    keyboard.append([InlineKeyboardButton("✅ Approve User", callback_data=f"approve:{user_id}")])
                keyboard.append([InlineKeyboardButton("🔙 Dismiss", callback_data="dismiss_info:0")])

                await query.edit_message_text(
                    message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )

            except Exception as e:
                logger.error(f"Error getting user info for {user_id}: {e}")
                await query.edit_message_text(
                    f"❌ Could not retrieve info for user <code>{user_id}</code>\n\n"
                    f"This user may have blocked the bot or deleted their account.",
                    parse_mode='HTML'
                )

        elif action == "dismiss_info":
            await query.message.delete()

        elif action == "noop":
            pass

    except Exception as e:
        logger.error(f"General button handler error: {e}")
        await query.edit_message_text("Error processing request.")

# --- ERROR HANDLER ---

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a telegram message to notify the developer."""
    logger.error("Exception while handling an update:", exc_info=context.error)

    try:
        if isinstance(update, Update) and update.effective_message:
            error_message = f"An error occurred: {str(context.error)}"
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"🚨 Bot Error:\n<code>{error_message[:500]}</code>",
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"Error sending error notification: {e}")

# --- MAIN APPLICATION SETUP ---

def main():
    """Start the bot."""
    try:
        setup_database()
        application = Application.builder().token(BOT_TOKEN).build()

        if not application.job_queue:
            logger.error("JobQueue is not available. Install `python-telegram-bot[job-queue]`")
            return

        application.job_queue.run_daily(
            daily_subscription_check,
            time=time(hour=0, minute=1, tzinfo=timezone.utc)
        )

        # Conversation handler for manual/offline dashboard operations
        dashboard_conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(dashboard_conversation_starter, pattern="^(broadcast|add_manual_prompt)$")
            ],
            states={
                GET_FIRST_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_first_name_input)],
                GET_USERNAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_username_input),
                    CommandHandler('nousername', handle_username_input)
                ],
                SELECT_PLAN: [
                    CallbackQueryHandler(select_offline_plan_callback, pattern="^offline_plan:(7|14|30|-1)$"),
                    CallbackQueryHandler(select_offline_custom_plan_callback, pattern="^offline_plan:custom$"),
                    CallbackQueryHandler(cancel_offline_handler, pattern="^cancel_offline$"),
                    CallbackQueryHandler(select_plan_callback, pattern="^user_plan:(7|14|30|-1)$"),
                    CallbackQueryHandler(select_custom_plan_callback, pattern="^user_plan:custom$"),
                    CallbackQueryHandler(handle_detection_choice, pattern="^(create_offline|retry_search|cancel_manual)$")
                ],
                CUSTOM_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_days_input)],
                GET_PAYMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manual_payment_info)],
                GET_BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast_message)],
            },
            fallbacks=[
                CommandHandler('cancel', cancel_conversation),
                CallbackQueryHandler(cancel_approval_handler, pattern="^cancel_approval$"),
            ],
            per_message=False, per_user=True, per_chat=True
        )

        # Conversation handler specifically for approving new users
        approve_conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(plan_selection_prompt, pattern="^approve:.*")
            ],
            states={
                SELECT_PLAN: [
                    CallbackQueryHandler(select_plan_callback, pattern="^user_plan:(7|14|30|-1)$"),
                    CallbackQueryHandler(select_custom_plan_callback, pattern="^user_plan:custom$")
                ],
                CUSTOM_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_days_input)],
                GET_PAYMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_new_user_payment)], # MODIFIED
            },
            fallbacks=[
                CommandHandler('cancel', cancel_conversation),
                CallbackQueryHandler(cancel_approval_handler, pattern="^cancel_approval$"),
            ],
            per_message=False, per_user=True, per_chat=True
        )

        # Add handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("status", status_command))
        application.add_handler(CommandHandler("dashboard", dashboard_command))

        application.add_handler(dashboard_conv_handler)
        application.add_handler(approve_conv_handler)

        application.add_handler(CallbackQueryHandler(display_user_list, pattern="^check_user$"))
        application.add_handler(CallbackQueryHandler(navigate_user_list_pages, pattern="^user_page:.*"))
        application.add_handler(CallbackQueryHandler(display_user_details, pattern="^show_detail:.*"))

        application.add_handler(CallbackQueryHandler(display_detailed_stats, pattern="^stats$"))
        application.add_handler(CallbackQueryHandler(back_to_dashboard, pattern="^back_to_dashboard$"))
        application.add_handler(CallbackQueryHandler(dashboard_callbacks, pattern="^expiring_soon$"))

        application.add_handler(CallbackQueryHandler(general_button_handler, pattern="^(extend|info|dismiss_info|noop):.*"))
        application.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.CHAT_MEMBER))
        application.add_handler(
            MessageHandler(filters.Chat(chat_id=CHANNEL_ID) & filters.User(user_id=ADMIN_ID), admin_post_handler)
        )

        application.add_error_handler(error_handler)

        logger.info("Starting bot...")
        application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

if __name__ == "__main__":
    main()
