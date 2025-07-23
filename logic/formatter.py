from db.crud import get_daily_signal_count

async def build_signal_payload(
    session,
    symbol: str,
    exchange: str,
    interval_minutes: int,
    oi_growth: float,
    price_growth: float,
    volume_growth_ratio: float,
) -> dict:
    signal_number = await get_daily_signal_count(session, symbol, exchange)

    return {
        "symbol": symbol,
        "exchange": exchange,
        "interval": interval_minutes,
        "oi_growth": round(oi_growth, 2),
        "price_growth": round(price_growth, 2),
        "volume_growth_ratio": round(volume_growth_ratio, 2),
        "signal_number": signal_number + 1,  # +1 потому что этот — новый
    }

def format_signal_message(
    symbol: str,
    exchange: str,
    interval_minutes: int,
    oi_growth: float,
    price_growth: float,
    volume_growth_ratio: float,
    signal_number: int,
) -> str:
    message = (
        f"<b>{exchange}</b> – <b>{interval_minutes}м</b> – <a href='https://www.coinglass.com/coin/{symbol}'>{symbol}</a>\n"
        f"ОИ вырос на <b>{oi_growth:.2f}%</b>\n"
        f"Изменение цены: <b>{price_growth:+.2f}%</b>\n"
        f"Изменение объема: <b>x{volume_growth_ratio:.1f}</b>\n"
        f"Сигнал за сутки: <b>{signal_number}</b>\n\n"
    )
    return message