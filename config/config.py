OI_THRESHOLD_PERCENT = 1.0  # минимальный рост OI
PRICE_THRESHOLD_PERCENT = 10.0  # минимальный рост цены
VOLUME_RATIO_THRESHOLD = 1.0  # минимальное отношение объемов
TIMEFRAME_MINUTES = 20     # интервал анализа в минутах
MAX_SIGNALS_PER_DAY = 10     # максимум уведомлений в день по одной монете
RUN_EVERY_SECONDS = 60     # частота запуска
EXCHANGES = ["Binance", "ByBit"]

DATABASE_URL = "postgresql+asyncpg://postgres:79134628@localhost:5432/open_interest_screener"


DEPOSIT = 100000.00  # баланс на бирже в USDT
RISK = 1  # потери при срабатывании стоп-лосса в процентах