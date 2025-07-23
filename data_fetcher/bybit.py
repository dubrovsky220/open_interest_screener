import aiohttp
import logging

BYBIT_BASE_URL = "https://api.bybit.com"
INTERVAL_MAPPING = {
    "1": "1min",
    "3": "3min",
    "5": "5min",
    "15": "15min",
    "30": "30min",
    "60": "1h",
    "120": "2h",
    "240": "4h",
    "D": "1d"
}

logger = logging.getLogger("bybit")

# Получение истории Open Interest
async def fetch_open_interest(symbol: str, interval: str = "5", limit: int = 5) -> list[dict] | None:
    url = f"{BYBIT_BASE_URL}/v5/market/open-interest"
    interval_api = INTERVAL_MAPPING.get(interval)

    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval_api,
        "limit": limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if data.get("retCode") != 0 or not data["result"]["list"]:
                logger.warning(f"No OI data for {symbol}")
                return None

            result = [
                {
                    "timestamp": int(item["timestamp"]),
                    "oi": float(item["openInterest"])
                }
                for item in reversed(data["result"]["list"])  # по возрастанию времени
            ]
            return result


# Получение истории цены и объема (Klines)
async def fetch_price_and_volume(symbol: str, interval: str = "5", limit: int = 5) -> list[dict] | None:
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if data.get("retCode") != 0 or not data["result"]["list"]:
                logger.warning(f"No kline data for {symbol}")
                return None

            result = [
                {
                    "timestamp": int(row[0]),
                    "price": float(row[4]),     # close
                    "volume": float(row[5])     # volume
                }
                for row in reversed(data["result"]["list"])
            ]
            return result


# Объединённый сборщик
async def fetch_bybit_data(symbol: str, interval: str = "5", limit: int = 5) -> list[dict] | None:
    try:
        oi_data = await fetch_open_interest(symbol, interval, limit)
        price_data = await fetch_price_and_volume(symbol, interval, limit)

        if not oi_data or not price_data:
            logger.warning(f"Failed to fetch full data for {symbol}")
            return None

        result = []
        for oi_point, pv_point in zip(oi_data, price_data):
            result.append({
                "symbol": symbol,
                "timestamp": max(oi_point["timestamp"], pv_point["timestamp"]),
                "oi": oi_point["oi"] * pv_point["price"],
                "price": pv_point["price"],
                "volume": pv_point["volume"]
            })

        return result

    except Exception as e:
        logger.error(f"Error fetching ByBit data for {symbol}: {e}")
        return None

async def get_bybit_symbols():
    try:
        url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                symbols = [
                    item["symbol"] for item in data["result"]["list"]
                    if item["status"] == "Trading"
                ]
                return symbols

    except Exception as e:
        logger.error(f"Error fetching ByBit symbols: {e}")
        return None



if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def test():
        result = await fetch_bybit_data("BTCUSDT")
        symbols = await get_bybit_symbols()
        print(result)
        print(symbols)

    asyncio.run(test())