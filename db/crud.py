from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models import SignalData
from datetime import datetime

async def save_signal(session: AsyncSession, signal: dict):
    db_signal = SignalData(**signal)
    session.add(db_signal)
    await session.commit()

async def get_daily_signal_count(session: AsyncSession, symbol: str, exchange: str) -> int:
    now = datetime.now()
    today_start = datetime(now.year, now.month, now.day)

    stmt = select(SignalData).filter(
        SignalData.symbol == symbol,
        SignalData.exchange == exchange,
        SignalData.timestamp >= today_start
    )
    result = await session.execute(stmt)
    return len(result.scalars().all())


if __name__ == "__main__":
    import asyncio
    from engine import async_session

    async def main():
        async with async_session() as session:
            signal = {
                "symbol": "BTCUSDT",
                "exchange": "Binance",
                "timestamp": datetime.now(),
                "oi_growth": 6.2,
                "price_growth": 0.89,
                "volume_growth_ratio": 10.1,
            }

            await save_signal(session, signal)
            signals_count = await get_daily_signal_count(session, "BTCUSDT", "Binance")
            print(signals_count)

    asyncio.run(main())