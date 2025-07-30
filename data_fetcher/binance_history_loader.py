import os
import aiohttp
import aiofiles
import asyncio
import pandas as pd
import zipfile
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timedelta
from binance import get_binance_symbols

BASE_URL = "https://data.binance.vision/data/futures/um/daily"
DATA_DIR = "../data/binance_data"
DAYS = 180
MAX_CONCURRENT_DOWNLOADS = 10
BATCH_SIZE = 20

semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)


def generate_urls(symbol: str, days: int) -> list[tuple[str, str, str]]:
    urls = []
    for i in range(days):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")

        interval = "5m"
        kline_url = f"{BASE_URL}/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
        kline_path = f"{DATA_DIR}/{symbol}/klines/{date}.zip"
        kline_csv = f"{symbol}-{interval}-{date}.csv"
        urls.append((kline_url, kline_path, kline_csv))

        metrics_url = f"{BASE_URL}/metrics/{symbol}/{symbol}-metrics-{date}.zip"
        metrics_path = f"{DATA_DIR}/{symbol}/metrics/{date}.zip"
        metrics_csv = f"{symbol}-metrics-{date}.csv"
        urls.append((metrics_url, metrics_path, metrics_csv))

    return urls


async def download_and_extract(session, url, local_path, extract_csv_name):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    csv_path = local_path.replace(".zip", ".csv")

    # Пропускаем, если уже есть CSV
    if os.path.exists(csv_path):
        return

    # Удалим поврежденный ZIP, если он остался от прошлой сессии
    if os.path.exists(local_path):
        os.remove(local_path)

    try:
        async with semaphore:
            async with session.get(url) as resp:
                if resp.status == 429:
                    await asyncio.sleep(5)
                    return
                elif resp.status >= 500:
                    await asyncio.sleep(10)
                    return
                elif resp.status != 200:
                    return

                async with aiofiles.open(local_path, mode='wb') as f:
                    await f.write(await resp.read())

        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extract(extract_csv_name, os.path.dirname(local_path))
            os.rename(
                os.path.join(os.path.dirname(local_path), extract_csv_name),
                csv_path
            )
        os.remove(local_path)

    except Exception as e:
        print(f"Ошибка загрузки {url}: {e}")


async def download_all(symbols: list[str], days: int):
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            tasks = []
            for symbol in batch:
                for url, local_path, csv_name in generate_urls(symbol, days):
                    tasks.append(download_and_extract(session, url, local_path, csv_name))

            await tqdm_asyncio.gather(*tasks)


def load_csvs(symbol: str, days: int) -> pd.DataFrame:
    base_dir = f"{DATA_DIR}/{symbol}"
    kline_dir = os.path.join(base_dir, "klines")
    metrics_dir = os.path.join(base_dir, "metrics")

    kline_files = sorted(os.listdir(kline_dir))[-days:]
    metrics_files = sorted(os.listdir(metrics_dir))[-days:]

    df_all = []

    for kline_file, metrics_file in zip(kline_files, metrics_files):
        kline_path = os.path.join(kline_dir, kline_file)
        df_kline = pd.read_csv(kline_path, header=0)
        df_kline.columns = [
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_volume", "taker_buy_quote_volume", "ignore"
        ]
        df_kline = df_kline[[
            "timestamp", "open", "high", "low", "close", "volume", "taker_buy_volume"
        ]]
        df_kline["timestamp"] = pd.to_datetime(df_kline["timestamp"], unit="ms")

        metrics_path = os.path.join(metrics_dir, metrics_file)
        df_metrics = pd.read_csv(metrics_path, header=0)
        df_metrics["timestamp"] = pd.to_datetime(df_metrics["create_time"])
        df_metrics = df_metrics[[
            "timestamp", "sum_open_interest", "sum_toptrader_long_short_ratio"
        ]]
        df_metrics.rename(columns={
            "sum_open_interest": "open_interest",
            "sum_toptrader_long_short_ratio": "long_short_ratio"
        }, inplace=True)

        df = pd.merge_asof(
            df_kline.sort_values("timestamp"),
            df_metrics.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("2min")
        )

        df["taker_buy_volume"] = pd.to_numeric(df["taker_buy_volume"], errors="coerce").fillna(0)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        df["taker_sell_volume"] = df["volume"] - df["taker_buy_volume"]
        df["volume_delta"] = df["taker_buy_volume"] - df["taker_sell_volume"]

        df_all.append(df)

    if not df_all:
        raise ValueError("Нет доступных данных для объединения.")

    return pd.concat(df_all).sort_values("timestamp").reset_index(drop=True)


async def main():
    symbols = await get_binance_symbols()
    await download_all(symbols, DAYS)
    for symbol in symbols:
        try:
            df = load_csvs(symbol, DAYS)
            df.to_csv(f"{DATA_DIR}/{symbol}/{symbol}_5m_full.csv", index=False)
            print(f"Saved {symbol}: {len(df)} rows")
        except Exception as e:
            print(f"Ошибка при обработке {symbol}: {e}")


if __name__ == "__main__":
    asyncio.run(main())

