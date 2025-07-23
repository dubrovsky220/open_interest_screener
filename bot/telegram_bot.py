import logging
import os

from aiogram import Bot
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

bot = Bot(token=TELEGRAM_TOKEN)

logger = logging.getLogger("telegram")


async def send_signal_to_telegram(message: str):
    try:
        await bot.send_message(TELEGRAM_CHAT_ID, message, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Telegram send error: {e}")


if __name__ == "__main__":
    import asyncio
    from logic.formatter import format_signal_message

    test_data = {
        "symbol": "BTCUSDT",
        "exchange": "Binance",
        "interval_minutes": 15,
        "oi_growth": 5.05,
        "price_growth": 0.77,
        "volume_growth_ratio": 12.3,
        "signal_number": 2
    }

    message = format_signal_message(**test_data)
    asyncio.run(send_signal_to_telegram(bot, message))