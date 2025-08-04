OI_THRESHOLD_PERCENT = 3.0  # минимальный рост OI
PRICE_THRESHOLD_PERCENT = 0.0  # минимальный рост цены
VOLUME_RATIO_THRESHOLD = 5.0  # минимальное отношение объемов
TIMEFRAME_MINUTES = 20     # интервал анализа в минутах
MAX_SIGNALS_PER_DAY = 3     # максимум уведомлений в день по одной монете
RUN_EVERY_SECONDS = 60     # частота запуска
EXCHANGES = ["Binance", "ByBit"]

DATABASE_URL = "postgresql+asyncpg://postgres:79134628@localhost:5432/open_interest_screener"


DEPOSIT = 10.00  # баланс на бирже в USDT
RISK = 10  # потери при срабатывании стоп-лосса в процентах