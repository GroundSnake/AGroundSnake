import os
import time
import datetime
import random
import numpy as np
import pandas as pd
import feather
from loguru import logger
import analysis.update_data
from analysis.const import (
    path_data,
    dt_trading_last_T0,
    dt_history,
    all_chs_code,
    path_main,
)


def gold_section(days: int = 180, frequency: str = "day") -> bool:
    name: str = "df_gold_grade"
    kline: str = f"update_kline_{frequency}"
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"{name} Begin")
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    filename_df_gold_grade_temp = os.path.join(
        path_data, f"df_gold_grade_temp_{days}_{str_dt_history_path}.ftr"
    )
    if analysis.base.is_latest_version(key=name):
        logger.trace("Gold_Grade Break End")
        return True
    if analysis.base.is_latest_version(key=kline):
        pass
    else:
        logger.trace(f"Update the {frequency}_Kline")
        if analysis.update_data.update_stock_data(frequency=frequency):
            logger.trace(f"{frequency}_Kline Update finish")
        else:
            return False
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    dt_delta = dt_trading_last_T0 - datetime.timedelta(days=days)
    if os.path.exists(filename_df_gold_grade_temp):
        df_gold_grade = feather.read_dataframe(source=filename_df_gold_grade_temp)
    else:
        list_columns = [
            f"date_start_{days}",
            f"date_end_{days}",
            f"days_{days}",
            f"price_start_{days}",
            f"price_end_{days}",
            f"price_max_{days}",
            f"date_max_{days}",
            f"price_min_{days}",
            f"date_min_{days}",
            f"pct_max_min_{days}",
            f"gold_section_{days}",
            f"std_volume_{days}",
        ]
        df_gold_grade = pd.DataFrame(columns=list_columns)
    list_symbol = all_chs_code()
    i = 0
    int_count_df = len(list_symbol)
    for symbol in list_symbol:
        i += 1
        str_msg = f"Gold Section Update: [{i:4d}/{int_count_df:4d}] -- [{symbol}]"
        if symbol in df_gold_grade.index:
            print(f"\r{str_msg} - Exist.\033[K", end="")
            continue
        file_name_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_feather):
            df_delta_gold = feather.read_dataframe(source=file_name_feather)
        else:
            print(f"\r{str_msg} - {frequency}_Kline data is not exist.\033[K")
            continue
        df_delta_gold = df_delta_gold.loc[dt_delta:].copy()
        df_gold_grade.at[
            symbol, f"date_start_{days}"
        ] = date_start = df_delta_gold.index.min()
        df_gold_grade.at[
            symbol, f"date_end_{days}"
        ] = date_end = df_delta_gold.index.max()
        series_max = df_delta_gold.idxmax()
        df_gold_grade.at[symbol, f"date_max_{days}"] = date_max = series_max["high"]
        series_min = df_delta_gold.idxmin()
        df_gold_grade.at[symbol, f"date_min_{days}"] = date_min = series_min["low"]
        df_gold_grade.at[symbol, f"days_{days}"] = (date_end - date_start).days
        df_gold_grade.at[symbol, f"price_start_{days}"] = df_delta_gold.at[
            date_start, "close"
        ].round(2)
        df_gold_grade.at[symbol, f"price_end_{days}"] = price_end = df_delta_gold.at[
            date_end, "close"
        ].round(2)
        df_gold_grade.at[symbol, f"price_max_{days}"] = price_max = df_delta_gold.at[
            date_max, "high"
        ].round(2)
        df_gold_grade.at[symbol, f"price_min_{days}"] = price_min = df_delta_gold.at[
            date_min, "low"
        ].round(2)
        df_gold_grade.at[symbol, f"pct_max_min_{days}"] = (
            (price_max / price_min - 1) * 100
        ).round(2)
        price_diff_max_min = price_max - price_min
        if price_diff_max_min > 0:
            df_gold_grade.at[symbol, f"gold_section_{days}"] = (
                (price_end - price_min) / price_diff_max_min * 100
            ).round(2)
        else:
            df_gold_grade.at[symbol, f"gold_section_{days}"] = 0
        df_gold_grade.at[symbol, f"std_volume_{days}"] = (
            np.std(df_delta_gold["volume"])
        ).round(2)
        if random.randint(0, 10) == 5:
            feather.write_dataframe(df=df_gold_grade, dest=filename_df_gold_grade_temp)
        print(f"\r{str_msg} - Update.\033[K", end="")
    if i >= int_count_df:
        print("\n\r\033[K", end="")  # 格式处理
        logger.trace(f"For loop End")
    print(df_gold_grade)
    df_gold_grade.sort_values(by=[f"gold_section_{days}"], inplace=True)
    df_gold_grade.to_csv("df_gold_grade.csv")
    df_cap = analysis.base.feather_from_file(key="df_cap")
    df_gold_grade = pd.concat(objs=[df_cap, df_gold_grade], axis=1, join="outer")
    days_average = np.average(df_gold_grade[f"days_{days}"].tolist())
    df_gold_grade = df_gold_grade[
        (df_gold_grade[f"pct_max_min_{days}"] >= 50)
        & (df_gold_grade[f"gold_section_{days}"].between(0, 50))
        & (~df_gold_grade["name"].str.contains("ST").fillna(False))
        & (df_gold_grade[f"date_max_{days}"] > df_gold_grade[f"date_min_{days}"])
        & (df_gold_grade[f"price_min_{days}"] < df_gold_grade[f"price_end_{days}"])
        & (df_gold_grade[f"days_{days}"] >= days_average)
    ].copy()
    df_gold_grade.to_csv(f"df_gold_grade_{days}_{str_dt_history_path}.csv")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"{name} analysis takes [{str_gm}]")
