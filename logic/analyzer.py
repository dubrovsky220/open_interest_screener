import asyncio


def analyze_signal(
        symbol_data: list[dict],
        min_growth_oi: float = 3.0,
        window: int = 20,
        interval: int = 5
) -> dict | None:
    """
    Анализирует рост открытого интереса за заданное количество минут (window).

    :param symbol_data: Список словарей с полями oi, price, volume, timestamp
    :param min_growth_oi: Минимальный рост OI в процентах для сигнала
    :param window: Временное окно анализа в минутах
    :param interval: Интервал одного бара в минутах (в соответствии с временными метками в symbol_data)
    :return: dict с параметрами сигнала или None
    """
    num_bars = window // interval

    if len(symbol_data) < num_bars + 1:
        return None  # недостаточно данных

    # Сортируем по времени
    sorted_data = sorted(symbol_data, key=lambda x: x["timestamp"])

    first = sorted_data[-(num_bars + 1)]  # старый
    last = sorted_data[-1]  # свежий

    oi_start = first["oi"]
    oi_end = last["oi"]
    price_start = first["price"]
    price_end = last["price"]
    volume_start = first["volume"]
    volume_end = last["volume"]

    # Вычисляем изменения
    oi_growth = ((oi_end - oi_start) / oi_start) * 100
    price_growth = ((price_end - price_start) / price_start) * 100
    volume_growth_ratio = volume_end / volume_start if volume_start != 0 else float("inf")

    if oi_growth >= min_growth_oi:
        return {
            "symbol": last.get("symbol"),
            "oi_growth": round(oi_growth, 2),
            "price_growth": round(price_growth, 2),
            "volume_growth_ratio": round(volume_growth_ratio, 2)
        }

    return None


if __name__ == "__main__":
    from data_fetcher.binance import fetch_binance_data

    async def test():
        # Получаем последние 10 баров по 5 минут
        symbol_data = await fetch_binance_data("BTCUSDT", period="5m", limit=10)

        # Анализируем рост OI за последние 15 минут
        signal = analyze_signal(symbol_data, window=15)

        if signal:
            print("Сигнал:", signal)
        else:
            print("Нет сигнала")

    asyncio.run(test())