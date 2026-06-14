#!/usr/bin/env python3
"""
PlayTest Pro — Telegram Channel Auto-Poster v2
Features: APScheduler, Media Support, SQLite Analytics, Bot Commands
"""

import os
import asyncio
import json
import sys
import logging
import sqlite3
import time
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
from functools import wraps

# ── ENV ──
try:
    from dotenv import load_dotenv
    load_dotenv()
    # Fallback: load env.prod if core vars still missing (for deployment)
    if not os.getenv("TELEGRAM_BOT_TOKEN") or not os.getenv("TELEGRAM_CHANNEL_ID"):
        load_dotenv("env.prod")
except ImportError:
    pass

# ── TELEGRAM ──
from telegram import Bot, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# ── SCHEDULER ──
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    AsyncIOScheduler = None
    CronTrigger = None

# ── LOGGING ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ──
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID", "").strip()

# ── RATE LIMITING ──
MIN_DELAY_BETWEEN_POSTS = int(os.getenv("MIN_DELAY_BETWEEN_POSTS", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))

# ── PATHS ──
SCRIPT_DIR = Path(__file__).parent.resolve()
POSTS_JSON_PATH = SCRIPT_DIR / "posts.json"
DB_PATH = SCRIPT_DIR / "analytics.db"
MEDIA_DIR = SCRIPT_DIR / "media"
MEDIA_DIR.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════════
#  DATABASE (SQLite Analytics)
# ═══════════════════════════════════════════════════════════════
class AnalyticsDB:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    day INTEGER NOT NULL,
                    post_number INTEGER NOT NULL,
                    message_id INTEGER,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    success BOOLEAN DEFAULT 0,
                    error TEXT,
                    views INTEGER DEFAULT 0,
                    UNIQUE(day, post_number)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE,
                    total_posts INTEGER DEFAULT 0,
                    successful_posts INTEGER DEFAULT 0,
                    failed_posts INTEGER DEFAULT 0
                )
            """)
            conn.commit()

    def log_post(self, day: int, post_number: int, message_id: int = None,
                 success: bool = False, error: str = None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO posts (day, post_number, message_id, success, error)
                VALUES (?, ?, ?, ?, ?)
            """, (day, post_number, message_id, success, error))
            conn.commit()

    def is_posted(self, day: int, post_number: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT 1 FROM posts WHERE day=? AND post_number=? AND success=1",
                (day, post_number)
            )
            return cursor.fetchone() is not None

    def get_stats(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM posts WHERE success=1").fetchone()[0]
            failed = conn.execute("SELECT COUNT(*) FROM posts WHERE success=0").fetchone()[0]
            last = conn.execute(
                "SELECT MAX(posted_at) FROM posts WHERE success=1"
            ).fetchone()[0]
            return {"total_posted": total, "failed": failed, "last_post": last}

    def get_daily_stats(self, date_str: str = None) -> dict:
        if not date_str:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT total_posts, successful_posts FROM analytics WHERE date=?",
                (date_str,)
            )
            row = cursor.fetchone()
            if row:
                return {"total": row[0], "successful": row[1]}
            return {"total": 0, "successful": 0}


# ═══════════════════════════════════════════════════════════════
#  TELEGRAM POSTER (with Media Support)
# ═══════════════════════════════════════════════════════════════
class TelegramPoster:
    def __init__(self, token: str, channel_id: str, db: AnalyticsDB):
        if not token or not channel_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID are required.")
        self.bot = Bot(token=token)
        self.channel_id = channel_id
        self.db = db

    async def post_message(self, text: str, day: int = 0, post_number: int = 0,
                          media_path: Path = None, retries: int = 0) -> dict:
        """Send message with optional media and retry logic."""
        try:
            if media_path and media_path.exists():
                # Send photo with caption
                with open(media_path, "rb") as photo:
                    message = await self.bot.send_photo(
                        chat_id=self.channel_id,
                        photo=photo,
                        caption=text,
                        parse_mode=ParseMode.HTML,
                    )
            else:
                message = await self.bot.send_message(
                    chat_id=self.channel_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )

            logger.info(f"✅ Posted | Day {day}, Post #{post_number} | ID: {message.message_id}")
            self.db.log_post(day, post_number, message.message_id, success=True)
            return {"success": True, "message_id": message.message_id}

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed | Day {day}, Post #{post_number}: {error_msg}")
            self.db.log_post(day, post_number, success=False, error=error_msg)

            if retries < MAX_RETRIES and ("429" in error_msg or "Too Many Requests" in error_msg):
                wait_time = RETRY_DELAY * (2 ** retries)
                logger.warning(f"🔄 Retrying in {wait_time}s... ({retries + 1}/{MAX_RETRIES})")
                await asyncio.sleep(wait_time)
                return await self.post_message(text, day, post_number, media_path, retries + 1)

            return {"success": False, "error": error_msg}

    async def post_now(self, text: str, **kwargs) -> dict:
        return await self.post_message(text, **kwargs)


# ═══════════════════════════════════════════════════════════════
#  POST LOADING & VALIDATION
# ═══════════════════════════════════════════════════════════════
def load_posts(path: Path = POSTS_JSON_PATH) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Posts file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        posts = json.load(f)
    if not isinstance(posts, list):
        raise ValueError("posts.json must contain a list of post objects.")
    return posts


def get_posts_for_day(posts: list[dict], day: int) -> list[dict]:
    day_posts = [p for p in posts if p.get("day") == day]
    return sorted(day_posts, key=lambda x: x.get("post_number", 0))


def validate_posts(posts: list[dict]) -> bool:
    if not posts:
        logger.error("❌ posts.json is empty.")
        return False
    required_keys = {"day", "post_number", "hour", "text"}
    for i, post in enumerate(posts):
        missing = required_keys - post.keys()
        if missing:
            logger.error(f"❌ Post {i} missing keys: {missing}")
            return False
    sorted_posts = sorted(posts, key=lambda x: (x["day"], x["post_number"]))
    if posts != sorted_posts:
        logger.warning("⚠️  posts.json is not sorted by day and post_number.")
    logger.info(f"✅ Validated {len(posts)} posts.")
    return True


# ═══════════════════════════════════════════════════════════════
#  BOT COMMANDS
# ═══════════════════════════════════════════════════════════════
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = str(update.effective_user.id)
        if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
            await update.message.reply_text("⛔ Unauthorized.")
            return
        return await func(update, context)
    return wrapper


def get_next_unpublished(posts: list) -> dict | None:
    """Return the first post where published is not True."""
    for p in posts:
        if not p.get("published", False):
            return p
    return None


def save_posts(posts: list) -> None:
    """Write posts list back to posts.json."""
    try:
        with open(POSTS_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save posts.json: {e}")


def build_bilingual(post: dict) -> str:
    """Build combined English + Arabic message with hashtags."""
    text_en = post["text"]
    text_ar = post.get("text_ar", "")
    footer_en = "\n\n#PlayTest_Pro #AndroidDev #AppTesting"
    footer_ar = "\n\n#PlayTest_Pro #تطوير_أندرويد #اختبار_التطبيقات"
    separator = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    return f"{text_en}{footer_en}{separator}{text_ar}{footer_ar}" if text_ar else f"{text_en}{footer_en}"


@admin_only
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status — Show bot status"""
    posts = context.application.bot_data.get("posts", [])
    poster = context.application.bot_data.get("poster")
    scheduler = context.application.bot_data.get("scheduler")
    stats = poster.db.get_stats() if poster else {}

    total = len(posts)
    published = sum(1 for p in posts if p.get("published"))
    remaining = total - published
    next_post = get_next_unpublished(posts)

    schedule_info = ""
    if scheduler and scheduler.scheduler and scheduler.scheduler.get_job("daily_posts"):
        schedule_info = "⏰ Schedule: 09:00 / 15:00 daily"

    next_info = ""
    if next_post:
        next_info = f"📌 Next: Day {next_post['day']}, Post #{next_post['post_number']} at {next_post.get('hour', 9)}:00"

    await update.message.reply_text(
        f"📊 <b>PlayTest Pro Status</b>\n\n"
        f"✅ Published: {published} / {total}\n"
        f"📋 Remaining: {remaining}\n"
        f"❌ Failed: {stats.get('failed', 0)}\n"
        f"🕐 Last Post: {stats.get('last_post') or 'Never'}\n\n"
        f"{next_info}\n"
        f"{schedule_info}\n\n"
        f"🤖 Bot is running.",
        parse_mode=ParseMode.HTML
    )


@admin_only
async def cmd_post_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/post_now — Post the next unpublished message immediately"""
    posts = context.application.bot_data.get("posts", [])
    poster = context.application.bot_data.get("poster")
    if not poster:
        await update.message.reply_text("❌ Poster not available.")
        return

    post = get_next_unpublished(posts)
    if not post:
        await update.message.reply_text("✅ All posts have been published!")
        return

    msg = await update.message.reply_text(
        f"🚀 Posting Day {post['day']}, Post #{post['post_number']}..."
    )

    # Build and send
    text = build_bilingual(post)
    media_file = post.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    result = await poster.post_message(
        text,
        day=post["day"],
        post_number=post["post_number"],
        media_path=media_path
    )

    if result.get("success"):
        post["published"] = True
        save_posts(posts)
        await msg.edit_text(
            f"✅ <b>Posted!</b>\n\n"
            f"Day {post['day']}, Post #{post['post_number']}\n"
            f"Message ID: {result['message_id']}",
            parse_mode=ParseMode.HTML
        )
    else:
        await msg.edit_text(
            f"❌ <b>Failed</b>\n\n{result.get('error', 'Unknown error')}",
            parse_mode=ParseMode.HTML
        )


@admin_only
async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/skip — Mark the next unpublished post as skipped"""
    posts = context.application.bot_data.get("posts", [])
    post = get_next_unpublished(posts)
    if not post:
        await update.message.reply_text("✅ All posts have been published or skipped!")
        return

    post["published"] = True
    save_posts(posts)
    await update.message.reply_text(
        f"⏭️ <b>Skipped</b>\n\n"
        f"Day {post['day']}, Post #{post['post_number']} marked as published.",
        parse_mode=ParseMode.HTML
    )


@admin_only
async def cmd_scan_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/scan_media — Scan media/ folder and update post_media in posts.json"""
    posts = context.application.bot_data.get("posts", [])
    msg = await update.message.reply_text("📁 Scanning media folder...")

    result = run_media_scan(posts)

    # Refresh posts in bot_data after potential save
    context.application.bot_data["posts"] = posts

    text = (
        f"📁 <b>Media Scan Complete</b>\n\n"
        f"✅ Updated: {result['updated']}\n"
        f"⏭️  Unchanged: {result['unchanged']}\n"
        f"📁 Invalid names skipped: {result['skipped']}\n"
        f"❌ Orphaned references removed: {result['not_found']}"
    )
    await msg.edit_text(text, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stats — Show detailed analytics"""
    posts = context.application.bot_data.get("posts", [])
    poster = context.application.bot_data.get("poster")
    db = poster.db if poster else AnalyticsDB()

    stats = db.get_stats()
    total = len(posts)
    published = sum(1 for p in posts if p.get("published"))
    remaining = total - published
    next_post = get_next_unpublished(posts)

    days_with_posts = len(set(p.get("day") for p in posts if not p.get("published")))

    next_info = ""
    if next_post:
        next_info = f"\n📌 Next: Day {next_post['day']}, Post #{next_post['post_number']}"

    await update.message.reply_text(
        f"📈 <b>PlayTest Pro Analytics</b>\n\n"
        f"📚 Total Posts: {total}\n"
        f"✅ Published: {published}\n"
        f"⏳ Remaining: {remaining}\n"
        f"📅 Days with pending posts: {days_with_posts}\n"
        f"❌ Failed: {stats.get('failed', 0)}\n"
        f"🕐 Last Post: {stats.get('last_post') or 'N/A'}"
        f"{next_info}",
        parse_mode=ParseMode.HTML
    )


# ═══════════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════════
class PostScheduler:
    def __init__(self, poster: TelegramPoster, posts: list[dict]):
        self.poster = poster
        self.posts = posts
        self.scheduler = None
        if AsyncIOScheduler and CronTrigger:
            self.scheduler = AsyncIOScheduler()

    async def post_scheduled(self):
        """Post messages for the current day."""
        now = datetime.now(timezone.utc)
        current_day = now.day
        today_posts = get_posts_for_day(self.posts, current_day)

        if not today_posts:
            logger.info(f"ℹ️  No posts for day {current_day}.")
            return

        logger.info(f"📅 Posting {len(today_posts)} messages for day {current_day}...")

        for i, post in enumerate(today_posts):
            # Check if already published (from posts.json)
            if post.get("published", False):
                logger.info(f"⏭️  Skipping already published: Day {current_day}, Post #{post['post_number']}")
                continue

            # Optional: wait until scheduled hour
            target_hour = post.get("hour", 9)
            target_time = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
            if target_time > now:
                wait_seconds = (target_time - now).total_seconds()
                logger.info(f"⏳ Waiting {int(wait_seconds/60)} minutes for scheduled hour...")
                await asyncio.sleep(wait_seconds)

            # Build bilingual message
            text_en = post["text"]
            text_ar = post.get("text_ar", "")
            footer_en = "\n\n#PlayTest_Pro #AndroidDev #AppTesting"
            footer_ar = "\n\n#PlayTest_Pro #تطوير_أندرويد #اختبار_التطبيقات"
            separator = "\n\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            combined = f"{text_en}{footer_en}{separator}{text_ar}{footer_ar}" if text_ar else f"{text_en}{footer_en}"

            # Post
            media_file = post.get("post_media", "")
            media_path = MEDIA_DIR / media_file if media_file else None
            result = await self.poster.post_message(
                combined,
                day=current_day,
                post_number=post["post_number"],
                media_path=media_path
            )

            # Mark as published in posts.json if successful
            if result.get("success"):
                post["published"] = True
                try:
                    with open(POSTS_JSON_PATH, "w", encoding="utf-8") as f:
                        json.dump(self.posts, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error(f"Failed to save published state: {e}")

            # Rate limit
            if i < len(today_posts) - 1:
                await asyncio.sleep(MIN_DELAY_BETWEEN_POSTS)

    def start(self):
        if not self.scheduler:
            logger.error("APScheduler not installed. Run: pip install apscheduler")
            return

        # Schedule twice daily: 9:00 and 15:00
        self.scheduler.add_job(
            self.post_scheduled,
            CronTrigger(hour="9,15", minute="0"),
            id="daily_posts",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info("🚀 Scheduler started. Posts at 09:00 and 15:00 daily.")

    def shutdown(self):
        if self.scheduler:
            self.scheduler.shutdown()


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
#  MEDIA SCAN
# ═══════════════════════════════════════════════════════════════
def run_media_scan(posts: list) -> dict:
    """
    Scan media/ folder, match files to posts by pattern day<d>_post<p>.<ext>,
    and update post_media in posts list. Returns summary dict.
    """
    if not MEDIA_DIR.exists():
        logger.warning(f"Media directory not found: {MEDIA_DIR}")
        return {"updated": 0, "unchanged": 0, "skipped": 0, "not_found": 0}

    pattern = re.compile(r"^day(\d+)_post(\d+)\.(jpg|jpeg|png|gif|webp)$", re.IGNORECASE)
    updated = unchanged = skipped = not_found = 0
    media_index = {}

    # Build index: (day, post_number) -> filename
    for f in MEDIA_DIR.iterdir():
        if not f.is_file():
            continue
        match = pattern.match(f.name)
        if not match:
            skipped += 1
            continue
        day = int(match.group(1))
        post_num = int(match.group(2))
        key = (day, post_num)
        # Keep the last file if duplicates exist
        media_index[key] = f.name

    # Match against posts
    for post in posts:
        key = (post["day"], post["post_number"])
        if key in media_index:
            filename = media_index[key]
            if post.get("post_media", "") != filename:
                post["post_media"] = filename
                updated += 1
            else:
                unchanged += 1
        elif post.get("post_media", ""):
            # media file no longer exists
            not_found += 1

    # Save if any changes
    if updated > 0:
        save_posts(posts)

    logger.info(
        f"📁 Scan complete: {updated} updated, {unchanged} unchanged, "
        f"{skipped} invalid names, {not_found} missing"
    )
    return {
        "updated": updated,
        "unchanged": unchanged,
        "skipped": skipped,
        "not_found": not_found,
    }


# ═══════════════════════════════════════════════════════════════
async def main():
    # Init
    db = AnalyticsDB()
    poster = TelegramPoster(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, db)

    # Load posts
    try:
        posts = load_posts()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error loading posts: {e}")
        sys.exit(1)

    if not validate_posts(posts):
        sys.exit(1)

    logger.info(f"📚 Loaded {len(posts)} posts.")

    # Auto-scan media folder on startup
    run_media_scan(posts)

    # Setup bot application for commands
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Share instances with command handlers via bot_data
    app.bot_data["poster"] = poster
    app.bot_data["posts"] = posts
    app.bot_data["db"] = db

    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("post_now", cmd_post_now))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("scan_media", cmd_scan_media))

    # Setup scheduler
    scheduler = PostScheduler(poster, posts)
    app.bot_data["scheduler"] = scheduler
    scheduler.start()

    # Start bot (for commands)
    logger.info("🤖 Starting bot... Use /status, /stats, /post_now, /skip, /scan_media")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
        scheduler.shutdown()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
