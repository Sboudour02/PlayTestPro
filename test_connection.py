#!/usr/bin/env python3
"""
PlayTest Pro — Live Connection Test
Sends a single test message to validate bot + channel wiring.
"""

import os
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import Bot
from telegram.constants import ParseMode

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHANNEL = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()

async def test_post():
    if not TOKEN or not CHANNEL:
        print("❌ Error: TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL_ID not set.")
        print("   Ensure .env exists in the same folder as this script.")
        sys.exit(1)

    bot = Bot(token=TOKEN)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    test_text = (
        f"🤖 <b>PlayTest Pro Bot — Connection Test</b>\n\n"
        f"⏰ Timestamp: <code>{now}</code>\n"
        f"🤖 Bot: @QannasCore_bot\n"
        f"📢 Channel: {CHANNEL}\n\n"
        f"If you see this message, the automation pipeline is live and working correctly. ✅"
    )

    print("Sending test message to channel...")
    try:
        message = await bot.send_message(
            chat_id=CHANNEL,
            text=test_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(f"[OK] SUCCESS | Message ID: {message.message_id}")
        print(f"     Posted to: {CHANNEL}")
        return 0
    except Exception as e:
        print(f"[ERROR] FAILED | {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(test_post())
    sys.exit(exit_code)
