import aiohttp
import logging


BINANCE_BASE_URL = "https://fapi.binance.com"
logger = logging.getLogger("binance")


# Получение истории Open Interest
async def fetch_open_interest(symbol: str, period: str = "5m", limit: int = 5) -> list[dict] | None:
    url = f"{BINANCE_BASE_URL}/futures/data/openInterestHist"
    params = {
        "symbol": symbol,
        "period": period,
        "limit": limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if not data:
                logger.warning(f"No OI data for {symbol}")
                return None

            result = [
                {
                    "timestamp": int(item["timestamp"]),
                    "oi": float(item["sumOpenInterestValue"])
                }
                for item in data
            ]
            return result


# Получение истории цены и объёма (расширенной версии)
async def fetch_price_and_volume(symbol: str, interval: str = "5m", limit: int = 5) -> list[dict] | None:
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if not data:
                logger.warning(f"No kline data for {symbol}")
                return None

            result = []

            for i, row in enumerate(data):
                open_price = float(row[1])
                close_price = float(row[4])
                volume = float(row[5])
                buy_volume = float(row[9])  # taker buy volume
                sell_volume = volume - buy_volume
                volume_delta = buy_volume - sell_volume  # простая оценка

                result.append({
                    "timestamp": int(row[0]),
                    "open": open_price,
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": close_price,
                    "price": close_price,  # совместимость
                    "volume": volume,
                    "vd": volume_delta
                })
            return result


# Получение последних N значений OI, цены и объема (объединено по времени)
async def fetch_binance_data(symbol: str, period: str = "5m", limit: int = 5) -> list[dict] | None:
    try:
        oi_data = await fetch_open_interest(symbol, period, limit)
        price_volume_data = await fetch_price_and_volume(symbol, period, limit)

        if not oi_data or not price_volume_data:
            logger.warning(f"Failed to fetch full data for {symbol}")
            return None

        result = []
        for oi_point, pv_point in zip(oi_data, price_volume_data):
            result.append({
                "symbol": symbol,
                "timestamp": max(oi_point["timestamp"], pv_point["timestamp"]),
                "oi": oi_point["oi"],
                "price": pv_point["price"],
                "volume": pv_point["volume"],
                "open": pv_point["open"],
                "high": pv_point["high"],
                "low": pv_point["low"],
                "close": pv_point["close"],
                "vd": pv_point["vd"],
            })

        return result

    except Exception as e:
        logger.error(f"Error fetching Binance data for {symbol}: {e}")
        return None

async def get_binance_symbols():
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                symbols = [
                    item["symbol"] for item in data["symbols"]
                    if item["contractType"] == "PERPETUAL" and item["status"] == "TRADING"
                ]

                return symbols

    except Exception as e:
        logger.error(f"Error fetching Binance symbols: {e}")
        return None


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def test():
        result = await fetch_binance_data("BTCUSDT")
        symbols = await get_binance_symbols()
        print(result)
        print(symbols)

    asyncio.run(test())


