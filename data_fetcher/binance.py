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

# Получение истории цены и объёма
async def fetch_price_and_volume(symbol: str, interval: str = "5m", limit: int = 5) -> list[dict] | None:
    url = f"{BINANCE_BASE_URL}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            data = await response.json()
            if not data:
                logger.warning(f"No kline data for {symbol}")
                return None

            result = [
                {
                    "timestamp": int(row[0]),
                    "price": float(row[4]),     # close
                    "volume": float(row[5])     # volume
                }
                for row in data
            ]
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
                "volume": pv_point["volume"]
            })

        return result

    except Exception as e:
        logger.error(f"Error fetching Binance data for {symbol}: {e}")
        return None


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)

    async def test():
        result = await fetch_binance_data("BTCUSDT")
        print(result)

    asyncio.run(test())
