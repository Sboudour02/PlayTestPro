#!/usr/bin/env python3
"""
PlayTest Pro — Telegram Channel Auto-Poster
Production-ready: loads posts from posts.json and schedules daily delivery.
"""

import os
import asyncio
import json
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Bot
from telegram.constants import ParseMode

# ── LOGGING SETUP ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ──
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()

# ── RATE LIMITING ──
MIN_DELAY_BETWEEN_POSTS = int(os.getenv("MIN_DELAY_BETWEEN_POSTS", "5"))  # seconds
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "10"))  # seconds

# ── PATHS ──
SCRIPT_DIR = Path(__file__).parent.resolve()
POSTS_JSON_PATH = SCRIPT_DIR / "posts.json"

# ── POST LOADING ──
def load_posts(path: Path = POSTS_JSON_PATH) -> list[dict]:
    """Load posts from the JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Posts file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        posts = json.load(f)
    if not isinstance(posts, list):
        raise ValueError("posts.json must contain a list of post objects.")
    return posts


def get_posts_for_day(posts: list[dict], day: int) -> list[dict]:
    """Return all posts for a specific day, sorted by post_number."""
    day_posts = [p for p in posts if p.get("day") == day]
    return sorted(day_posts, key=lambda x: x.get("post_number", 0))


# ── TELEGRAM POSTER ──
class TelegramPoster:
    def __init__(self, token: str, channel_id: str):
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is missing or empty.")
        if not channel_id:
            raise ValueError("TELEGRAM_CHANNEL_ID is missing or empty.")
        self.bot = Bot(token=token)
        self.channel_id = channel_id

    async def post_message(self, text: str, retries: int = 0) -> dict:
        """Send a plain text message to the channel with retry logic."""
        try:
            message = await self.bot.send_message(
                chat_id=self.channel_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            logger.info(f"✅ Posted successfully | Message ID: {message.message_id}")
            return {"success": True, "message_id": message.message_id}
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Failed to post: {error_msg}")
            
            # Retry logic for rate limiting (429) or temporary errors
            if retries < MAX_RETRIES and ("429" in error_msg or "Too Many Requests" in error_msg):
                wait_time = RETRY_DELAY * (2 ** retries)  # Exponential backoff
                logger.warning(f"🔄 Retrying in {wait_time}s... (attempt {retries + 1}/{MAX_RETRIES})")
                await asyncio.sleep(wait_time)
                return await self.post_message(text, retries=retries + 1)
            
            return {"success": False, "error": error_msg}

    async def post_now(self, text: str) -> dict:
        """Immediate post (for manual/testing use)."""
        return await self.post_message(text)

    async def run_scheduler_for_day(self, posts: list[dict], day: int):
        """
        Post all messages scheduled for a specific day.
        Waits until each post's scheduled hour before sending.
        Includes rate limiting between posts.
        """
        today_posts = get_posts_for_day(posts, day)
        if not today_posts:
            logger.info(f"ℹ️  No posts scheduled for day {day}.")
            return

        logger.info(f"📅 Processing {len(today_posts)} posts for day {day}...")

        for i, post in enumerate(today_posts):
            target_hour = post.get("hour", 9)
            now = datetime.now(timezone.utc)
            target_time = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
            if target_time <= now:
                target_time += timedelta(days=1)

            wait_seconds = (target_time - now).total_seconds()
            if wait_seconds > 0:
                mins = int(wait_seconds / 60)
                logger.info(f"⏳ Waiting {mins} minutes for day {day} post #{post['post_number']}...")
                await asyncio.sleep(wait_seconds)

            logger.info(f"📤 Posting day {day}, post #{post['post_number']}...")
            result = await self.post_message(post["text"])
            
            # Rate limiting: wait between posts (except after the last one)
            if i < len(today_posts) - 1 and result.get("success"):
                logger.info(f"⏱️  Rate limit: waiting {MIN_DELAY_BETWEEN_POSTS}s before next post...")
                await asyncio.sleep(MIN_DELAY_BETWEEN_POSTS)


# ── VALIDATION ──
def validate_posts(posts: list[dict]) -> bool:
    """Validate posts structure and ordering."""
    if not posts:
        logger.error("❌ posts.json is empty.")
        return False
    
    required_keys = {"day", "post_number", "hour", "text"}
    for i, post in enumerate(posts):
        missing = required_keys - post.keys()
        if missing:
            logger.error(f"❌ Post {i} missing keys: {missing}")
            return False
    
    # Check ordering
    sorted_posts = sorted(posts, key=lambda x: (x["day"], x["post_number"]))
    if posts != sorted_posts:
        logger.warning("⚠️  posts.json is not sorted by day and post_number.")
        logger.info("💡 Tip: Sort posts by day, then post_number for best results.")
    
    logger.info(f"✅ Validated {len(posts)} posts.")
    return True


# ── SCHEDULER ──
async def run_daily_scheduler(poster: TelegramPoster, posts: list[dict]):
    """
    Run the scheduler continuously, posting content for the current day.
    Designed to run as a long-lived process (e.g., via systemd, Docker, or screen).
    """
    logger.info("🚀 Daily scheduler started. Waiting for scheduled posts...")
    
    while True:
        now = datetime.now(timezone.utc)
        current_day = now.day  # Simple day-based scheduling
        
        # Get posts for today (you may want to use a date-based key instead of day number)
        today_posts = get_posts_for_day(posts, current_day)
        
        if today_posts:
            logger.info(f"📅 Found {len(today_posts)} posts for today (day {current_day}).")
            await poster.run_scheduler_for_day(posts, current_day)
        else:
            logger.info(f"ℹ️  No posts for day {current_day}. Checking again tomorrow.")
        
        # Sleep until next day
        tomorrow = now + timedelta(days=1)
        tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_seconds = (tomorrow - now).total_seconds()
        logger.info(f"😴 Sleeping for {int(sleep_seconds / 3600)} hours until next day...")
        await asyncio.sleep(sleep_seconds)


# ── CLI ENTRY ──
async def main():
    poster = TelegramPoster(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID)

    try:
        posts = load_posts()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error loading posts: {e}")
        sys.exit(1)

    # Validate posts
    if not validate_posts(posts):
        sys.exit(1)

    logger.info(f"📚 Loaded {len(posts)} posts from posts.json")

    # Demo: send the very first post immediately
    first_post = posts[0]
    logger.info(f"🚀 Sending test post (Day {first_post['day']}, Post #{first_post['post_number']})...")
    result = await poster.post_now(first_post["text"])
    if result.get("success"):
        logger.info("🎉 Test post sent successfully!")
    else:
        logger.error("😞 Test post failed. Check the error above.")


if __name__ == "__main__":
    asyncio.run(main())
