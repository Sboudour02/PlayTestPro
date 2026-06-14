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
import requests

# ── ENV ──
SCRIPT_DIR = Path(__file__).parent.resolve()
try:
    from dotenv import load_dotenv
    env_path = SCRIPT_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        # Fallback: load env.prod (Railway deploy) using absolute path
        prod_path = SCRIPT_DIR / "env.prod"
        if prod_path.exists():
            load_dotenv(prod_path)
except ImportError:
    pass

# ── TELEGRAM ──
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

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

# ── X (Twitter) API ──
X_API_KEY = os.getenv("X_API_KEY", "").strip()
X_API_SECRET = os.getenv("X_API_SECRET", "").strip()
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "").strip()
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET", "").strip()
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "").strip()

# ── Facebook Page API ──
FB_PAGE_ID = os.getenv("FB_PAGE_ID", "").strip()
FB_PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN", "").strip()
FB_USER_ACCESS_TOKEN = os.getenv("FB_USER_ACCESS_TOKEN", "").strip()

# ── RATE LIMITING ──
MIN_DELAY_BETWEEN_POSTS = int(os.getenv("MIN_DELAY_BETWEEN_POSTS", "5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))

# ── PATHS ──
SCRIPT_DIR = Path(__file__).parent.resolve()
POSTS_JSON_PATH = SCRIPT_DIR / "posts.json"
XPOSTS_JSON_PATH = SCRIPT_DIR / "xposts.json"
FBPOSTS_JSON_PATH = SCRIPT_DIR / "fbposts.json"
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
#  X (TWITTER) POSTER
# ═══════════════════════════════════════════════════════════════
class XPoster:
    def __init__(self):
        self.client = None
        self.enabled = False
        self.last_tweet_id = None
        if all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET]):
            try:
                import tweepy
                self.client = tweepy.Client(
                    bearer_token=X_BEARER_TOKEN,
                    consumer_key=X_API_KEY,
                    consumer_secret=X_API_SECRET,
                    access_token=X_ACCESS_TOKEN,
                    access_token_secret=X_ACCESS_TOKEN_SECRET,
                )
                self.enabled = True
                logger.info("🐦 X API initialized.")
            except Exception as e:
                logger.warning(f"⚠️ X API init failed: {e}")

    async def post_tweet(self, text: str) -> dict:
        if not self.enabled or not self.client:
            return {"success": False, "error": "X API not configured"}
        try:
            response = await asyncio.to_thread(self.client.create_tweet, text=text)
            tweet_id = response.data["id"]
            self.last_tweet_id = tweet_id
            logger.info(f"✅ Tweeted | ID: {tweet_id}")
            return {"success": True, "tweet_id": tweet_id}
        except Exception as e:
            logger.error(f"❌ Tweet failed: {e}")
            return {"success": False, "error": str(e)}

    async def post_tweet_with_media(self, text: str, media_path: Path = None) -> dict:
        if not self.enabled or not self.client:
            return {"success": False, "error": "X API not configured"}
        try:
            if media_path and media_path.exists():
                auth_v1 = tweepy.OAuthHandler(X_API_KEY, X_API_SECRET)
                auth_v1.set_access_token(X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)
                api_v1 = tweepy.API(auth_v1)
                media = await asyncio.to_thread(api_v1.media_upload, filename=str(media_path))
                response = await asyncio.to_thread(
                    self.client.create_tweet, text=text, media_ids=[media.media_id]
                )
            else:
                response = await asyncio.to_thread(self.client.create_tweet, text=text)
            tweet_id = response.data["id"]
            self.last_tweet_id = tweet_id
            logger.info(f"✅ Tweeted with media | ID: {tweet_id}")
            return {"success": True, "tweet_id": tweet_id}
        except Exception as e:
            logger.error(f"❌ Tweet failed: {e}")
            return {"success": False, "error": str(e)}

    def build_tweet_text(self, xpost: dict) -> str:
        text_en = xpost["text"]
        channel_link = "https://t.me/QannasCore"
        hashtags = "#PlayTest_Pro #AndroidDev #AppTesting"
        return f"{text_en}\n\n{hashtags}\n🔗 {channel_link}"


# ═══════════════════════════════════════════════════════════════
#  FACEBOOK (META) POSTER
# ═══════════════════════════════════════════════════════════════
class FacebookPoster:
    def __init__(self):
        self.page_id = FB_PAGE_ID
        self.access_token = FB_PAGE_ACCESS_TOKEN
        self.enabled = False
        if not self.page_id:
            logger.warning("⚠️ Facebook not configured (missing FB_PAGE_ID).")
            return
        # If a User Token is available, exchange it for a Page Token
        if FB_USER_ACCESS_TOKEN:
            self._fetch_page_token()
        self.enabled = bool(self.access_token)
        if self.enabled:
            logger.info("📘 Facebook API initialized.")
        else:
            logger.warning("⚠️ Facebook not configured (no usable access token).")

    def _fetch_page_token(self):
        """Exchange the long-lived User Token for a Page Access Token."""
        try:
            url = f"https://graph.facebook.com/v21.0/{self.page_id}"
            params = {"fields": "access_token", "access_token": FB_USER_ACCESS_TOKEN}
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
            if "access_token" in data:
                self.access_token = data["access_token"]
                logger.info("📘 Page Access Token obtained from User Token via /me/accounts.")
            else:
                err = data.get("error", {}).get("message", str(data))
                logger.warning(f"⚠️ Could not get Page Token from User Token: {err}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to fetch Page Access Token: {e}")

    async def post_to_feed(self, text: str) -> dict:
        if not self.enabled:
            return {"success": False, "error": "Facebook not configured"}
        try:
            url = f"https://graph.facebook.com/v21.0/{self.page_id}/feed"
            params = {"access_token": self.access_token, "message": text}
            response = await asyncio.to_thread(requests.post, url, params=params)
            data = response.json()
            if "id" in data:
                post_id = data["id"]
                logger.info(f"✅ FB posted | ID: {post_id}")
                return {"success": True, "post_id": post_id}
            error_msg = data.get("error", {}).get("message", str(data))
            logger.error(f"❌ FB post failed: {error_msg}")
            return {"success": False, "error": error_msg}
        except Exception as e:
            logger.error(f"❌ FB post exception: {e}")
            return {"success": False, "error": str(e)}

    async def post_with_photo(self, text: str, media_path: Path = None) -> dict:
        if not self.enabled:
            return {"success": False, "error": "Facebook not configured"}
        try:
            if media_path and media_path.exists():
                url = f"https://graph.facebook.com/v21.0/{self.page_id}/photos"
                with open(media_path, "rb") as f:
                    response = await asyncio.to_thread(
                        requests.post, url,
                        params={"access_token": self.access_token, "message": text},
                        files={"source": f}
                    )
            else:
                return await self.post_to_feed(text)
            data = response.json()
            if "id" in data:
                post_id = data.get("post_id", data["id"])
                logger.info(f"✅ FB posted with photo | ID: {data['id']}")
                return {"success": True, "post_id": data["id"]}
            error_msg = data.get("error", {}).get("message", str(data))
            logger.error(f"❌ FB photo post failed: {error_msg}")
            return {"success": False, "error": error_msg}
        except Exception as e:
            logger.error(f"❌ FB photo post exception: {e}")
            return {"success": False, "error": str(e)}

    def build_post_text(self, fbpost: dict) -> str:
        text_en = fbpost["text"]
        hashtags = "#PlayTest_Pro #AndroidDev #AppTesting"
        channel_link = "https://t.me/QannasCore"
        return f"{text_en}\n\n{hashtags}\n🔗 {channel_link}"


# ═══════════════════════════════════════════════════════════════
#  X POST MANAGER
# ═══════════════════════════════════════════════════════════════
class XPostManager:
    def __init__(self, path: Path = XPOSTS_JSON_PATH):
        self.path = path
        self.posts = self._load_or_generate()

    def _load_or_generate(self) -> list:
        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        logger.info("xposts.json not found, generating from posts.json...")
        return self._generate_from_posts()

    def _generate_from_posts(self) -> list:
        try:
            with open(POSTS_JSON_PATH, "r", encoding="utf-8") as f:
                tg_posts = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Cannot generate xposts.json: {e}")
            return []
        MAX_TEXT = 170
        xposts = []
        for p in tg_posts:
            text = p["text"]
            if len(text) > MAX_TEXT:
                cut = text.rfind(" ", 0, MAX_TEXT - 1)
                if cut < MAX_TEXT // 2:
                    cut = MAX_TEXT - 3
                text = text[:cut].rstrip() + "..."
            xposts.append({
                "day": p["day"],
                "post_number": p["post_number"],
                "hour": p.get("hour", 9),
                "text": text,
                "post_media": p.get("post_media", ""),
                "published": False,
            })
        self.save()
        logger.info(f"✅ Generated {len(xposts)} xposts from posts.json")
        return xposts

    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.posts, f, ensure_ascii=False, indent=2)

    def get_next_unpublished(self) -> dict | None:
        for p in self.posts:
            if not p.get("published", False):
                return p
        return None

    def get_matching(self, day: int, post_number: int) -> dict | None:
        for p in self.posts:
            if p["day"] == day and p["post_number"] == post_number:
                return p
        return None

    def mark_published(self, day: int, post_number: int):
        for p in self.posts:
            if p["day"] == day and p["post_number"] == post_number:
                p["published"] = True
                self.save()
                return


# ═══════════════════════════════════════════════════════════════
#  FB POST MANAGER
# ═══════════════════════════════════════════════════════════════
class FBPostManager:
    def __init__(self, path: Path = FBPOSTS_JSON_PATH):
        self.path = path
        self.posts = self._load_or_generate()

    def _load_or_generate(self) -> list:
        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        logger.info("fbposts.json not found, generating from posts.json...")
        return self._generate_from_posts()

    def _generate_from_posts(self) -> list:
        try:
            with open(POSTS_JSON_PATH, "r", encoding="utf-8") as f:
                tg_posts = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Cannot generate fbposts.json: {e}")
            return []
        MAX_TEXT = 300
        fbposts = []
        for p in tg_posts:
            text = p["text"]
            if len(text) > MAX_TEXT:
                cut = text.rfind(" ", 0, MAX_TEXT - 1)
                if cut < MAX_TEXT // 2:
                    cut = MAX_TEXT - 3
                text = text[:cut].rstrip() + "..."
            fbposts.append({
                "day": p["day"],
                "post_number": p["post_number"],
                "hour": p.get("hour", 9),
                "text": text,
                "post_media": p.get("post_media", ""),
                "published": False,
            })
        self.save()
        logger.info(f"✅ Generated {len(fbposts)} fbposts from posts.json")
        return fbposts

    def save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.posts, f, ensure_ascii=False, indent=2)

    def get_next_unpublished(self) -> dict | None:
        for p in self.posts:
            if not p.get("published", False):
                return p
        return None

    def get_matching(self, day: int, post_number: int) -> dict | None:
        for p in self.posts:
            if p["day"] == day and p["post_number"] == post_number:
                return p
        return None

    def mark_published(self, day: int, post_number: int):
        for p in self.posts:
            if p["day"] == day and p["post_number"] == post_number:
                p["published"] = True
                self.save()
                return


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


async def post_to_telegram(poster, post, msg_to_edit=None) -> dict:
    """Post to Telegram channel, return result."""
    text = build_bilingual(post)
    media_file = post.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    return await poster.post_message(
        text,
        day=post["day"],
        post_number=post["post_number"],
        media_path=media_path
    )


async def post_to_x(x_poster, x_mgr, post, msg_to_edit=None) -> dict:
    """Post matching xpost to X, return result."""
    if not x_poster or not x_poster.enabled:
        return {"success": False, "error": "X not configured"}
    xpost = x_mgr.get_matching(post["day"], post["post_number"])
    if not xpost:
        return {"success": False, "error": "No matching xpost found"}
    if xpost.get("published"):
        return {"success": False, "error": "Already published on X"}
    tweet_text = x_poster.build_tweet_text(xpost)
    media_file = xpost.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    return await x_poster.post_tweet_with_media(tweet_text, media_path)


async def post_to_fb(fb_poster, fb_mgr, post, msg_to_edit=None) -> dict:
    """Post matching fbpost to Facebook, return result."""
    if not fb_poster or not fb_poster.enabled:
        return {"success": False, "error": "Facebook not configured"}
    fbpost = fb_mgr.get_matching(post["day"], post["post_number"])
    if not fbpost:
        return {"success": False, "error": "No matching fbpost found"}
    if fbpost.get("published"):
        return {"success": False, "error": "Already published on Facebook"}
    fb_text = fb_poster.build_post_text(fbpost)
    media_file = fbpost.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    return await fb_poster.post_with_photo(fb_text, media_path)


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
    """/post_now — Choose platform to post on"""
    posts = context.application.bot_data.get("posts", [])
    poster = context.application.bot_data.get("poster")
    if not poster:
        await update.message.reply_text("❌ Poster not available.")
        return

    post = get_next_unpublished(posts)
    if not post:
        await update.message.reply_text("✅ All TG posts have been published!")
        return

    context.bot_data["pending_post"] = {
        "day": post["day"],
        "post_number": post["post_number"],
    }

    keyboard = [
        [
            InlineKeyboardButton("📱 Telegram", callback_data="platform_tg"),
            InlineKeyboardButton("📘 Facebook", callback_data="platform_fb"),
        ],
        [
            InlineKeyboardButton("📱+📘 Both", callback_data="platform_both"),
            InlineKeyboardButton("❌ Cancel", callback_data="platform_cancel"),
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"📢 <b>Day {post['day']}, Post #{post['post_number']}</b>\n\n"
        f"Where to post?",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )


async def cmd_platform_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    choice = query.data
    if choice == "platform_cancel":
        await query.edit_message_text("❌ Cancelled.")
        return

    pending = context.bot_data.get("pending_post", {})
    day = pending.get("day")
    post_number = pending.get("post_number")

    posts = context.application.bot_data.get("posts", [])
    poster = context.application.bot_data.get("poster")
    fb_poster = context.application.bot_data.get("fb_poster")
    fb_mgr = context.application.bot_data.get("fb_mgr")

    # Find the post
    post = None
    for p in posts:
        if p["day"] == day and p["post_number"] == post_number:
            post = p
            break
    if not post:
        await query.edit_message_text("❌ Post not found.")
        return

    await query.edit_message_text(
        f"🚀 Posting Day {day}, Post #{post_number}..."
    )

    results = []

    if choice in ("platform_tg", "platform_both"):
        result = await post_to_telegram(poster, post)
        if result.get("success"):
            post["published"] = True
            save_posts(posts)
            results.append(f"📱 Telegram: ✅ (ID: {result['message_id']})")
        else:
            results.append(f"📱 Telegram: ❌ {result.get('error', '')}")

    if choice in ("platform_fb", "platform_both"):
        result = await post_to_fb(fb_poster, fb_mgr, post)
        if result.get("success"):
            fb_mgr.mark_published(day, post_number)
            results.append(f"📘 Facebook: ✅ (ID: {result['post_id']})")
        else:
            results.append(f"📘 Facebook: ❌ {result.get('error', '')}")

    await query.edit_message_text(
        f"📢 <b>Day {day}, Post #{post_number}</b>\n\n" + "\n".join(results),
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def cmd_x_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/x_now — Post the next unpublished xpost to X"""
    x_poster = context.application.bot_data.get("x_poster")
    x_mgr = context.application.bot_data.get("x_mgr")
    if not x_poster or not x_poster.enabled:
        await update.message.reply_text("🐦 X is not configured.")
        return

    xpost = x_mgr.get_next_unpublished() if x_mgr else None
    if not xpost:
        await update.message.reply_text("✅ All X posts have been published!")
        return

    msg = await update.message.reply_text(
        f"🐦 Posting Day {xpost['day']}, Post #{xpost['post_number']} to X..."
    )

    tweet_text = x_poster.build_tweet_text(xpost)
    media_file = xpost.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    result = await x_poster.post_tweet_with_media(tweet_text, media_path)

    if result.get("success"):
        x_mgr.mark_published(xpost["day"], xpost["post_number"])
        await msg.edit_text(
            f"✅ <b>Posted to X!</b>\n\n"
            f"Day {xpost['day']}, Post #{xpost['post_number']}\n"
            f"Tweet ID: {result['tweet_id']}",
            parse_mode=ParseMode.HTML,
        )
    else:
        await msg.edit_text(
            f"❌ <b>X Post Failed</b>\n\n{result.get('error', 'Unknown error')}",
            parse_mode=ParseMode.HTML,
        )


@admin_only
async def cmd_x_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/x_skip — Mark the next unpublished xpost as published"""
    x_mgr = context.application.bot_data.get("x_mgr")
    xpost = x_mgr.get_next_unpublished() if x_mgr else None
    if not xpost:
        await update.message.reply_text("✅ All X posts have been published or skipped!")
        return

    x_mgr.mark_published(xpost["day"], xpost["post_number"])
    await update.message.reply_text(
        f"⏭️ <b>Skipped on X</b>\n\n"
        f"Day {xpost['day']}, Post #{xpost['post_number']} marked as published.",
        parse_mode=ParseMode.HTML,
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
async def cmd_x_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/x_status — Show X (Twitter) integration status"""
    x_poster = context.application.bot_data.get("x_poster")
    x_mgr = context.application.bot_data.get("x_mgr")
    if not x_poster or not x_poster.enabled:
        await update.message.reply_text(
            "🐦 <b>X Status</b>\n\n❌ Not configured or disabled.",
            parse_mode=ParseMode.HTML,
        )
        return

    total = len(x_mgr.posts) if x_mgr else 0
    published = sum(1 for p in x_mgr.posts if p.get("published")) if x_mgr else 0
    remaining = total - published
    next_x = x_mgr.get_next_unpublished() if x_mgr else None
    next_info = ""
    if next_x:
        next_info = f"\n📌 Next: Day {next_x['day']}, Post #{next_x['post_number']}"

    await update.message.reply_text(
        f"🐦 <b>X Status</b>\n\n"
        f"✅ Enabled\n"
        f"📚 Total: {total}\n"
        f"✅ Published: {published}\n"
        f"⏳ Remaining: {remaining}"
        f"{next_info}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/stats — Show detailed analytics for Telegram"""
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


@admin_only
async def cmd_xstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/xstats — Show detailed analytics for X"""
    x_poster = context.application.bot_data.get("x_poster")
    x_mgr = context.application.bot_data.get("x_mgr")
    if not x_poster or not x_poster.enabled:
        await update.message.reply_text("🐦 <b>X Status</b>\n\n❌ Not configured or disabled.", parse_mode=ParseMode.HTML)
        return

    total = len(x_mgr.posts) if x_mgr else 0
    published = sum(1 for p in x_mgr.posts if p.get("published")) if x_mgr else 0
    remaining = total - published
    next_x = x_mgr.get_next_unpublished() if x_mgr else None
    next_info = ""
    if next_x:
        next_info = f"\n📌 Next: Day {next_x['day']}, Post #{next_x['post_number']}"

    await update.message.reply_text(
        f"🐦 <b>PlayTest Pro — X Analytics</b>\n\n"
        f"📚 Total Posts: {total}\n"
        f"✅ Published: {published}\n"
        f"⏳ Remaining: {remaining}"
        f"{next_info}",
        parse_mode=ParseMode.HTML
    )


@admin_only
async def cmd_fb_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/fb_status — Show Facebook integration status"""
    fb_poster = context.application.bot_data.get("fb_poster")
    fb_mgr = context.application.bot_data.get("fb_mgr")
    if not fb_poster or not fb_poster.enabled:
        await update.message.reply_text(
            "📘 <b>Facebook Status</b>\n\n❌ Not configured or disabled.",
            parse_mode=ParseMode.HTML,
        )
        return

    total = len(fb_mgr.posts) if fb_mgr else 0
    published = sum(1 for p in fb_mgr.posts if p.get("published")) if fb_mgr else 0
    remaining = total - published
    next_fb = fb_mgr.get_next_unpublished() if fb_mgr else None
    next_info = ""
    if next_fb:
        next_info = f"\n📌 Next: Day {next_fb['day']}, Post #{next_fb['post_number']}"

    await update.message.reply_text(
        f"📘 <b>Facebook Status</b>\n\n"
        f"✅ Enabled\n"
        f"📚 Total: {total}\n"
        f"✅ Published: {published}\n"
        f"⏳ Remaining: {remaining}"
        f"{next_info}",
        parse_mode=ParseMode.HTML,
    )


@admin_only
async def cmd_fbstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/fbstats — Show detailed analytics for Facebook"""
    fb_poster = context.application.bot_data.get("fb_poster")
    fb_mgr = context.application.bot_data.get("fb_mgr")
    if not fb_poster or not fb_poster.enabled:
        await update.message.reply_text("📘 <b>Facebook Status</b>\n\n❌ Not configured or disabled.", parse_mode=ParseMode.HTML)
        return

    total = len(fb_mgr.posts) if fb_mgr else 0
    published = sum(1 for p in fb_mgr.posts if p.get("published")) if fb_mgr else 0
    remaining = total - published
    next_fb = fb_mgr.get_next_unpublished() if fb_mgr else None
    next_info = ""
    if next_fb:
        next_info = f"\n📌 Next: Day {next_fb['day']}, Post #{next_fb['post_number']}"

    await update.message.reply_text(
        f"📘 <b>PlayTest Pro — Facebook Analytics</b>\n\n"
        f"📚 Total Posts: {total}\n"
        f"✅ Published: {published}\n"
        f"⏳ Remaining: {remaining}"
        f"{next_info}",
        parse_mode=ParseMode.HTML
    )


@admin_only
async def cmd_fb_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/fb_now — Post the next unpublished fbpost to Facebook"""
    fb_poster = context.application.bot_data.get("fb_poster")
    fb_mgr = context.application.bot_data.get("fb_mgr")
    if not fb_poster or not fb_poster.enabled:
        await update.message.reply_text("📘 Facebook is not configured.")
        return

    fbpost = fb_mgr.get_next_unpublished() if fb_mgr else None
    if not fbpost:
        await update.message.reply_text("✅ All Facebook posts have been published!")
        return

    msg = await update.message.reply_text(
        f"📘 Posting Day {fbpost['day']}, Post #{fbpost['post_number']} to Facebook..."
    )

    fb_text = fb_poster.build_post_text(fbpost)
    media_file = fbpost.get("post_media", "")
    media_path = MEDIA_DIR / media_file if media_file else None
    result = await fb_poster.post_with_photo(fb_text, media_path)

    if result.get("success"):
        fb_mgr.mark_published(fbpost["day"], fbpost["post_number"])
        await msg.edit_text(
            f"✅ <b>Posted to Facebook!</b>\n\n"
            f"Day {fbpost['day']}, Post #{fbpost['post_number']}\n"
            f"Post ID: {result['post_id']}",
            parse_mode=ParseMode.HTML,
        )
    else:
        await msg.edit_text(
            f"❌ <b>Facebook Post Failed</b>\n\n{result.get('error', 'Unknown error')}",
            parse_mode=ParseMode.HTML,
        )


@admin_only
async def cmd_fb_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/fb_skip — Mark the next unpublished fbpost as published"""
    fb_mgr = context.application.bot_data.get("fb_mgr")
    fbpost = fb_mgr.get_next_unpublished() if fb_mgr else None
    if not fbpost:
        await update.message.reply_text("✅ All Facebook posts have been published or skipped!")
        return

    fb_mgr.mark_published(fbpost["day"], fbpost["post_number"])
    await update.message.reply_text(
        f"⏭️ <b>Skipped on Facebook</b>\n\n"
        f"Day {fbpost['day']}, Post #{fbpost['post_number']} marked as published.",
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════════
class PostScheduler:
    def __init__(self, poster: TelegramPoster, posts: list[dict], x_poster: XPoster = None, x_mgr: XPostManager = None, fb_poster: FacebookPoster = None, fb_mgr: FBPostManager = None):
        self.poster = poster
        self.posts = posts
        self.x_poster = x_poster
        self.x_mgr = x_mgr
        self.fb_poster = fb_poster
        self.fb_mgr = fb_mgr
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

                # Also post to X if enabled and not already posted
                if self.x_poster and self.x_poster.enabled and self.x_mgr:
                    xpost = self.x_mgr.get_matching(current_day, post["post_number"])
                    if xpost and not xpost.get("published"):
                        tweet_text = self.x_poster.build_tweet_text(xpost)
                        media_file = xpost.get("post_media", "")
                        media_path = MEDIA_DIR / media_file if media_file else None
                        xr = await self.x_poster.post_tweet_with_media(tweet_text, media_path)
                        if xr.get("success"):
                            self.x_mgr.mark_published(current_day, post["post_number"])

                # Also post to Facebook if enabled and not already posted
                if self.fb_poster and self.fb_poster.enabled and self.fb_mgr:
                    fbpost = self.fb_mgr.get_matching(current_day, post["post_number"])
                    if fbpost and not fbpost.get("published"):
                        fb_text = self.fb_poster.build_post_text(fbpost)
                        media_file = fbpost.get("post_media", "")
                        media_path = MEDIA_DIR / media_file if media_file else None
                        fr = await self.fb_poster.post_with_photo(fb_text, media_path)
                        if fr.get("success"):
                            self.fb_mgr.mark_published(current_day, post["post_number"])

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
    x_poster = XPoster()
    x_mgr = XPostManager()
    fb_poster = FacebookPoster()
    fb_mgr = FBPostManager()

    # Load posts
    try:
        posts = load_posts()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error loading posts: {e}")
        sys.exit(1)

    if not validate_posts(posts):
        sys.exit(1)

    logger.info(f"📚 Loaded {len(posts)} posts. X: {len(x_mgr.posts)}, FB: {len(fb_mgr.posts)}")

    # Auto-scan media folder on startup
    run_media_scan(posts)

    # Setup bot application for commands
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Share instances with command handlers via bot_data
    app.bot_data["poster"] = poster
    app.bot_data["posts"] = posts
    app.bot_data["db"] = db
    app.bot_data["x_poster"] = x_poster
    app.bot_data["x_mgr"] = x_mgr
    app.bot_data["fb_poster"] = fb_poster
    app.bot_data["fb_mgr"] = fb_mgr

    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("post_now", cmd_post_now))
    app.add_handler(CommandHandler("skip", cmd_skip))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("scan_media", cmd_scan_media))
    app.add_handler(CommandHandler("x_status", cmd_x_status))
    app.add_handler(CommandHandler("xstats", cmd_xstats))
    app.add_handler(CommandHandler("x_now", cmd_x_now))
    app.add_handler(CommandHandler("x_skip", cmd_x_skip))
    app.add_handler(CommandHandler("fb_status", cmd_fb_status))
    app.add_handler(CommandHandler("fbstats", cmd_fbstats))
    app.add_handler(CommandHandler("fb_now", cmd_fb_now))
    app.add_handler(CommandHandler("fb_skip", cmd_fb_skip))
    app.add_handler(CallbackQueryHandler(cmd_platform_choice, pattern="^platform_"))

    # Setup scheduler
    scheduler = PostScheduler(poster, posts, x_poster, x_mgr, fb_poster, fb_mgr)
    app.bot_data["scheduler"] = scheduler
    scheduler.start()

    # Start bot (for commands)
    logger.info("🤖 Starting bot... /status, /post_now, /skip, /stats, /x_now, /x_skip, /x_status, /fb_status, /fbstats, /fb_now, /fb_skip")
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
