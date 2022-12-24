import os
import random
import sys
import feather
import pandas as pd
from loguru import logger


def read_catalogue(frequency: str = "1m") -> pd.DataFrame:
    path_main = os.getcwd()
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    if not os.path.exists(path_kline):
        print(f"Error,kline_{frequency} not exist")
        return pd.DataFrame()
    file_name_catalogue_feather = os.path.join(path_kline, f"catalogue.ftr")
    file_name_catalogue_csv = os.path.join(path_main, f"check_catalogue.csv")
    df_catalogue = pd.DataFrame()
    if os.path.exists(file_name_catalogue_feather):
        # 读取腌制数据 catalogue
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_feather)
        df_catalogue.to_csv(path_or_buf=file_name_catalogue_csv)
        print(f"catalogue feather converted to csv and saved as [{file_name_catalogue_feather}]")
    return df_catalogue


def read_data(symbol: str = None, frequency: str = "1m") -> pd.DataFrame:
    path_main = os.getcwd()
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    if not os.path.exists(path_kline):
        print(f"Error,kline_{frequency} not exist")
        return pd.DataFrame()
    if symbol is None:
        file_name_catalogue_feather = os.path.join(path_kline, f"catalogue.ftr")
        if os.path.exists(file_name_catalogue_feather):
            # 读取腌制数据 catalogue
            df_catalogue = feather.read_dataframe(source=file_name_catalogue_feather)
            list_stock = df_catalogue.index.tolist()
            symbol = random.choice(list_stock)
    file_name_data_feather = os.path.join(path_kline, f"{symbol}.ftr")
    file_name_data_csv = os.path.join(path_main, f"check_{symbol}.csv")
    df_data = pd.DataFrame()
    if os.path.exists(file_name_data_feather):
        df_data = feather.read_dataframe(source=file_name_data_feather)  # 读取腌制数据 df_data
        df_data.to_csv(path_or_buf=file_name_data_csv)
        print(f"[{symbol}] feather converted to csv and saved as [{file_name_data_csv}]")
    return df_data


if __name__ == "__main__":
    # 移除import创建的所有handle
    logger.remove()
    # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
    logger.add(sink=sys.stderr, level="INFO")
    df_c = read_catalogue()
    print(df_c)
    df_d = read_data()
    print(df_c)
