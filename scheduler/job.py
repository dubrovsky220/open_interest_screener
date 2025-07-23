import asyncio
import time
import logging
from asyncio import Semaphore
from datetime import datetime

from bot.telegram_bot import send_signal_to_telegram
from config.config import TIMEFRAME_MINUTES, OI_THRESHOLD_PERCENT, EXCHANGES, MAX_SIGNALS_PER_DAY, \
    PRICE_THRESHOLD_PERCENT, VOLUME_RATIO_THRESHOLD, DEPOSIT, RISK
from data_fetcher.binance import fetch_binance_data, get_binance_symbols
from data_fetcher.bybit import fetch_bybit_data, get_bybit_symbols
from db.crud import save_signal, get_daily_signal_count
from db.engine import async_session
from logic.analyzer import analyze_signal
from logic.formatter import format_signal_message


# Настроим лимиты по биржам
SEMAPHORES = {
    "Binance": Semaphore(10),
    "ByBit": Semaphore(10)
}

logger = logging.getLogger("job")


async def process_symbol(symbol: str, exchange: str):
    semaphore = SEMAPHORES[exchange]

    async with semaphore:
        try:
            # 1. Получение данных с биржи
            if exchange == "Binance":
                symbol_data = await fetch_binance_data(symbol, period="5m", limit=TIMEFRAME_MINUTES // 5 + 1)
            else:
                symbol_data = await fetch_bybit_data(symbol, interval="5", limit=TIMEFRAME_MINUTES // 5 + 1)

            # 2. Анализ на наличие сигнала
            signal_raw = analyze_signal(symbol_data, window=TIMEFRAME_MINUTES, interval=5,
                                        min_growth_oi=OI_THRESHOLD_PERCENT, min_growth_price=PRICE_THRESHOLD_PERCENT,
                                        min_volume_ratio=VOLUME_RATIO_THRESHOLD,
                                        balance=DEPOSIT, risk=RISK)

            if signal_raw is None:
                return

            async with async_session() as session:
                # 3. Сохранение в БД
                await save_signal(session, {
                    "symbol": signal_raw["symbol"],
                    "exchange": exchange,
                    "timestamp": datetime.now(),
                    "oi_growth": signal_raw["oi_growth"],
                    "price_growth": signal_raw["price_growth"],
                    "volume_growth_ratio": signal_raw["volume_growth_ratio"]
                })

                # 4. Получение номера сигнала за сутки
                count = await get_daily_signal_count(session, symbol, exchange)

            # 5. Проверка лимита
            if count > MAX_SIGNALS_PER_DAY:
                return

            # 6. Формирование полной информации
            signal_full = {
                "symbol": symbol,
                "exchange": exchange,
                "interval_minutes": TIMEFRAME_MINUTES,
                "oi_growth": signal_raw["oi_growth"],
                "price_growth": signal_raw["price_growth"],
                "volume_growth_ratio": signal_raw["volume_growth_ratio"],
                "signal_number": count,
                "position_sum": signal_raw["position_sum"],
                "stop_loss": signal_raw["stop_loss"],
            }

            message = format_signal_message(**signal_full)

            # 7. Отправка уведомления
            await send_signal_to_telegram(message)

        except Exception as e:
            logger.exception(f"Ошибка при обработке {symbol} на {exchange}: {e}")



async def process_exchange(exchange: str):
    if exchange == "Binance":
        symbols = await get_binance_symbols()
    else:
        symbols = await get_bybit_symbols()

    tasks = [process_symbol(symbol, exchange) for symbol in symbols]
    await asyncio.gather(*tasks)


async def run_signal_job():
    logger.info("Запуск run_signal_job")
    start_time = time.perf_counter()

    # Запуск обработки обеих бирж одновременно
    await asyncio.gather(*(process_exchange(exchange) for exchange in EXCHANGES))

    duration = time.perf_counter() - start_time
    logger.info(f"Завершено run_signal_job за {duration:.2f} секунд")