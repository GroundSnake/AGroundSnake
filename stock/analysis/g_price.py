# modified at 2023/05/18 22::25
from __future__ import annotations
import time
import math
import datetime
import random
import feather
import pandas as pd
import numpy as np
from loguru import logger
import analysis.update_data
import analysis.base
from analysis.const import (
    phi,
    path_data,
    path_temp,
    dt_history,
    dt_init,
    dt_pm_end,
    all_chs_code,
    dt_trading_last_T0,
)


def golden_price(
    frequency: str = "1m", days_price: int = 180, days_volume: int = 365
) -> bool:
    """
    :param frequency: choice of {"1m" ,"5m"}
    :param days_price:
    :param days_volume:
    :return: bool
    """
    logger.trace("Golden Price Analysis Begin")
    kline: str = f"update_kline_{frequency}"
    name: str = f"df_golden"
    # 判断Kline是不是最新的
    if analysis.base.is_latest_version(key=kline):
        pass
    else:
        logger.trace("Update the Kline")
        if analysis.update_data.update_stock_data(frequency=frequency):
            logger.trace("{kline} Update finish")
        else:
            return False
    if analysis.base.is_latest_version(key=name):
        logger.trace("Golden Price Analysis Break End")
        return True
    list_code = all_chs_code()
    dt_golden = dt_init
    dt_pass = datetime.datetime(year=1990, month=1, day=1, hour=15)
    start_loop_time = time.perf_counter_ns()
    path_kline = path_data.joinpath(f"kline_{frequency}")
    if not path_kline.exists():
        path_kline.mkdir()
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    filename_df_golden_temp = path_temp.joinpath(
        f"df_golden_temp_{str_dt_history_path}.ftr"
    )
    if filename_df_golden_temp.exists():
        df_golden = feather.read_dataframe(source=filename_df_golden_temp)
        df_golden = df_golden.sample(frac=1)
    else:
        list_columns = [
            "dt",
            "total_volume",
            "gold_section",
            "now_price",
            "gold_section_volume",
            "G_price",
            "gold_date_max",
            "gold_date_min",
            "gold_price_min",
            "gold_pct_max_min",
            "gold_section_price",
        ]
        df_golden = pd.DataFrame(index=list_code, columns=list_columns)
        df_golden["dt"].fillna(value=dt_init, inplace=True)
        df_golden["gold_date_max"].fillna(value=dt_init, inplace=True)
        df_golden["gold_date_min"].fillna(value=dt_init, inplace=True)
        df_golden.fillna(value=0.0, inplace=True)
        feather.write_dataframe(df=df_golden, dest=filename_df_golden_temp)
    if df_golden.empty:
        logger.error("df_golden empty")
        return False
    dt_data_180 = dt_trading_last_T0 - datetime.timedelta(days=days_price)
    dt_data_365 = dt_trading_last_T0 - datetime.timedelta(days=days_volume)
    df_now_price = analysis.ashare.stock_zh_a_spot_em()
    str_msg_bar_basic = f"{name}_{days_price}_{days_volume}"
    i = 0
    all_record = len(df_golden)
    for symbol in df_golden.index:
        i += 1
        str_msg_bar = f"{str_msg_bar_basic}:[{i:4d}/{all_record:4d}] -- [{symbol}]"
        if df_golden.at[symbol, "dt"] != dt_init:
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        file_name_data_feather = path_kline.joinpath(f"{symbol}.ftr")
        if file_name_data_feather.exists():
            df_data = feather.read_dataframe(source=file_name_data_feather)
        else:
            df_golden.at[symbol, "dt"] = dt_pass
            print(f"\r{str_msg_bar} - No data\033[K")
            continue
        if symbol in df_now_price.index:
            df_golden.at[symbol, "now_price"] = now_price = df_now_price.at[
                symbol, "close"
            ]
        else:
            df_golden.at[symbol, "dt"] = dt_pass
            print(f"\r{str_msg_bar} - [{symbol}] Not in df_now_price\033[K")
            continue
        df_data_180 = df_data.loc[dt_data_180:]
        print(f"\r{str_msg_bar} - [{dt_golden}]\033[K", end="")
        series_max = df_data_180.idxmax()
        df_golden.at[symbol, f"gold_date_max"] = gold_date_max = series_max["high"]
        series_min = df_data_180.idxmin()
        df_golden.at[symbol, f"gold_date_min"] = gold_date_min = series_min["low"]
        gold_price_max = df_data_180.at[gold_date_max, "high"]
        gold_price_min = df_golden.at[symbol, f"gold_price_min"] = df_data_180.at[
            gold_date_min, "low"
        ]
        price_diff_max_min = gold_price_max - gold_price_min
        if gold_price_min > 0:
            df_golden.at[symbol, f"gold_pct_max_min"] = (
                (gold_price_max / gold_price_min - 1) * 100
            ).round(2)
        else:
            df_golden.at[symbol, f"gold_pct_max_min"] = 0
        if price_diff_max_min > 0:
            df_golden.at[symbol, "gold_section_price"] = (
                (now_price - gold_price_min) / price_diff_max_min * 100
            ).round(2)
        else:
            df_golden.at[symbol, "gold_section_price"] = 0
        df_data_240 = df_data.loc[dt_data_365:]
        dt_max = df_data_240.index.max()
        if dt_golden < dt_max:
            dt_golden = dt_max
            if dt_pm_end < dt_golden:
                dt_golden = dt_pm_end
        df_pivot = pd.pivot_table(
            df_data_240, index=["close"], aggfunc={"volume": "sum", "close": "count"}
        )
        df_pivot.rename(columns={"close": "count"}, inplace=True)
        df_pivot.sort_values(by=["close"], ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        total_volume = df_pivot["volume"].sum()
        golden_volume = round(total_volume * phi, 2)
        temp_volume = 0
        df_golden.at[symbol, "dt"] = dt_max
        df_golden.at[symbol, "total_volume"] = total_volume
        signal_price = True
        signal_volume = True
        for tup_row in df_pivot.itertuples():
            temp_volume += tup_row.volume
            if tup_row.close <= now_price and signal_price:
                if tup_row.close == now_price:
                    df_golden.at[symbol, "now_price"] = tup_row.close
                else:
                    df_golden.at[symbol, "now_price"] = (now_price + tup_row.close) / 2
                df_golden.at[symbol, "gold_section_volume"] = (
                    (1 - temp_volume / total_volume) * 100
                ).round(2)
                signal_price = False
            if temp_volume >= golden_volume and signal_volume:
                df_golden.at[symbol, "G_price"] = tup_row.close
                signal_volume = False
            if not signal_price and not signal_volume:
                break
        alpha_gs_min = min(
            df_golden.at[symbol, "gold_section_price"],
            df_golden.at[symbol, "gold_section_volume"],
        )
        alpha_gs_max = max(
            df_golden.at[symbol, "gold_section_price"],
            df_golden.at[symbol, "gold_section_volume"],
        )
        if alpha_gs_max > 0 and alpha_gs_min > 0:
            df_golden.at[symbol, "gold_section"] = round(
                pow(alpha_gs_max, 2) / alpha_gs_min, 2
            )
        if random.randint(a=0, b=9) == 5:
            feather.write_dataframe(df=df_golden, dest=filename_df_golden_temp)
    if i >= all_record:
        print("\n", end="")  # 格式处理
        df_golden.index.rename(name="symbol", inplace=True)
        df_golden.sort_values(by=["gold_section_volume"], ascending=False, inplace=True)
        analysis.base.feather_to_file(
            df=df_golden,
            key=name,
        )
        analysis.base.set_version(key=name, dt=dt_golden)
        if filename_df_golden_temp.exists():  # 删除临时文件
            filename_df_golden_temp.unlink()
            logger.trace(f"[{filename_df_golden_temp}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Golden Price Analysis takes [{str_gm}]")
    logger.trace(f"Golden Price Analysis End--[all_record={all_record}]")
    return True
