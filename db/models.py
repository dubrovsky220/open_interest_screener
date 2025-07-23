from sqlalchemy import String, DateTime, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime


class Base(DeclarativeBase):
    pass


class SignalData(Base):
    __tablename__ = "signal_data"

    id: Mapped[int] = mapped_column(primary_key=True)
    symbol: Mapped[str] = mapped_column(String(30))
    exchange: Mapped[str] = mapped_column(String(10))  # 'Binance' or 'ByBit'
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.now())
    oi_growth: Mapped[float] = mapped_column(Float(2))
    price_growth: Mapped[float] = mapped_column(Float(2))
    volume_growth_ratio: Mapped[float] = mapped_column(Float(2))
