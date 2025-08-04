import os
import numpy as np
import pandas as pd
from tqdm import tqdm


def find_pumps(
        df: pd.DataFrame,
        min_growth: float = 0.10,
        window: int = 4,
        cool_down: int = 4,
        full_window: int = 30,
        target_window: int = 12
) -> list[int]:
    """
    Находит индексы пампов в датафрейме.

    Параметры:
    - min_growth: минимальный рост цены (например, 0.10 = 10%)
    - window: количество баров в пампе (например, 4 = 20 минут)
    - cool_down: сколько свечей пропустить после найденного пампа
    - full_window: количество баров, из которых будут формироваться признаки
    - target_window: сколько свечей нужно после пампа для построения целевой переменной

    Возвращает:
    - Список индексов, соответствующих последней свече пампа
    """
    close = df["close"].to_numpy()
    pumps = []
    i = full_window

    while i < len(close) - target_window:
        start_price = close[i - window]
        end_price = close[i]
        growth = (end_price - start_price) / start_price

        if growth >= min_growth:
            pumps.append(i)
            i += cool_down
        else:
            i += 1

    return pumps


def generate_target(
        df: pd.DataFrame,
        pump_idx: int,
        window: int = 4,
        target_window: int = 12,
        threshold: float = 0.5
) -> int | None:
    """
    Генерирует целевую переменную:
    1 — если цена упала на threshold * памп раньше, чем выросла.
    0 — иначе (выросла или флет).
    """

    low = df["low"].to_numpy()
    high = df["high"].to_numpy()
    close = df["close"].to_numpy()

    if pump_idx - window < 0 or pump_idx + target_window >= len(close):
        return None

    pump_start_price = close[pump_idx - window]
    pump_peak_price = close[pump_idx]
    pump_range = pump_peak_price - pump_start_price

    down_target = pump_peak_price - pump_range * threshold
    up_target = pump_peak_price + pump_range * threshold

    future_lows = low[pump_idx + 1: pump_idx + 1 + target_window]
    future_highs = high[pump_idx + 1: pump_idx + 1 + target_window]

    for i in range(len(future_lows)):
        if future_highs[i] >= up_target:
            return 0
        if future_lows[i] <= down_target:
            return 1

    return 0  # если ни падения, ни роста — считаем флэтом


def load_csv(file_path):
    df = pd.read_csv(file_path)
    return df


def create_feature_matrix(df, pump_idx, feature_window=30):
    """
    Возвращает матрицу признаков: 30 баров до и включая памп
    """
    start_idx = pump_idx - (feature_window - 1)
    end_idx = pump_idx + 1  # включительно

    if start_idx < 0 or end_idx > len(df):
        return None

    # Выбираем нужные бары
    df_sub = df.iloc[start_idx:end_idx].copy()

    # Возьмем необходимые признаки: open, high, low, close, volume, open_interest, volume_delta
    features = df_sub[["open", "high", "low", "close", "volume", "open_interest", "volume_delta"]]

    return features  # shape (30, 7)

def generate_dataset(
    data_folder="../data/binance_data",
    save_folder="../data/dataset",
    window=4,
    feature_window=30,
    target_window=12,
    threshold=0.3
):
    os.makedirs(save_folder, exist_ok=True)
    for class_idx in [0,1]:
        os.makedirs(os.path.join(save_folder, str(class_idx)), exist_ok=True)

    symbols = sorted(os.listdir(data_folder))
    print(f"Found {len(symbols)} symbols")

    for symbol in tqdm(symbols):
        symbol_folder = os.path.join(data_folder, symbol)
        csv_path = os.path.join(symbol_folder, f"{symbol}_5m_full.csv")

        if not os.path.isfile(csv_path):
            print(f"CSV not found for {symbol}, skipping")
            continue

        print(f"Processing {symbol}...")
        df = load_csv(csv_path)

        if not {"open", "high", "low", "close", "volume", "open_interest", "volume_delta"}.issubset(df.columns):
            print(f"Missing columns in {symbol}, skipping")
            continue

        pumps = find_pumps(df, min_growth=0.10, window=window)

        for pump_idx in pumps:
            target = generate_target(df, pump_idx, window=window, target_window=target_window, threshold=threshold)
            if target is None:
                continue

            features = create_feature_matrix(df, pump_idx, feature_window=feature_window)
            if features is None:
                continue

            # Сохраняем numpy массив
            filename = f"{symbol}_{pump_idx}.csv"
            save_path = os.path.join(save_folder, str(target), filename)
            features.to_csv(save_path)

    print("Dataset generation completed.")


if __name__ == "__main__":
    generate_dataset()