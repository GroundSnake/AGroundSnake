# modified at 2023/4/12 13:36
from __future__ import annotations
import os
import sys
import datetime
import time
import random
import feather
import numpy as np
import pandas as pd
from loguru import logger
import akshare as ak
import analysis.base
from analysis.const import (
    path_data,
    str_date_path,
    dt_date_trading,
    time_pm_end,
    filename_chip_shelve,
    list_all_stocks,
)


def limit_count(list_symbol: list | str = None) -> bool:
    name: str = "df_limit"
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = list_all_stocks
    elif isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"Limit Count Begin")
    str_date_trading = dt_date_trading.strftime("%Y%m%d")
    file_name_df_limit_temp = os.path.join(
        path_data, f"df_limit_count_temp_{str_date_path}.ftr"
    )
    dt_delta = dt_date_trading - datetime.timedelta(days=366)
    dt_limit = None
    str_delta = dt_delta.strftime("%Y%m%d")
    list_exist = list()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Limit Break End")
        return True
    logger.trace("Update Limit")
    if os.path.exists(file_name_df_limit_temp):
        logger.trace(f"{file_name_df_limit_temp} load feather")
        df_limit = feather.read_dataframe(
            source=file_name_df_limit_temp
        )  # 读取腌制数据 df_limit
        if df_limit.empty:
            logger.trace("df_limit cache is empty")
        else:
            logger.trace("df_limit cache is not empty")
            df_limit = df_limit.sample(frac=1)
            list_exist = df_limit.index.to_list()
    else:
        logger.trace(f"{file_name_df_limit_temp} not exists")
        list_columns = [
            "times",
            "up_times",
            "down_times",
            "up_M_down",
            "up_times_7pct",
            "down_times_7pct",
            "up_A_down_7pct",
            "up_times_5pct",
            "down_times_5pct",
            "up_A_down_5pct",
            "up_times_3pct",
            "down_times_3pct",
            "up_A_down_3pct",
            "T_m_amplitude_grade",
            "T_m_amplitude",
            "T_m_amplitude_std",
            "T5_amplitude",
            "T20_amplitude",
            "T40_amplitude",
            "T60_amplitude",
            "T80_amplitude",
            "T120_amplitude",
            "T_m_pct_grade",
            "T_m_pct",
            "T_m_pct_std",
            "T5_pct",
            "T20_pct",
            "T40_pct",
            "T60_pct",
            "T80_pct",
            "T120_pct",
            "T5_mean",
            "T20_mean",
            "T40_mean",
            "T60_mean",
            "T80_mean",
            "T120_mean",
            "T5_max",
            "T20_max",
            "T40_max",
            "T60_max",
            "T80_max",
            "T120_max",
            "T5_min",
            "T20_min",
            "T40_min",
            "T60_min",
            "T80_min",
            "T120_min",
        ]
        df_limit = pd.DataFrame(columns=list_columns)
    i = 0
    count = len(list_symbol)
    logger.trace(f"For loop Begin")
    for symbol in list_symbol:
        i += 1
        str_msg_bar = f"Limit Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        if symbol in list_exist:  # 己存在，断点继续
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        df_stock = pd.DataFrame()
        i_times = 0
        while i_times <= 2:
            try:
                df_stock = ak.stock_zh_a_hist(
                    symbol=symbol[2:8],
                    period="daily",
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except KeyError as e:
                print(repr(e))
                logger.trace(repr(e))
                break
            except OSError as e:
                print(repr(e))
                logger.trace(repr(e))
                time.sleep(2)
            else:
                break
            if i_times >= 2:
                print(f"[{symbol}] Request TimeoutError")
                sys.exit()
            i_times += 1
        if df_stock.empty:  # 数据接口，没有该[symbol]日K线，跳过本[symbol]处理
            df_limit.at[symbol, "times"] = 0
            df_limit.at[symbol, "up_times"] = -1
            df_limit.at[symbol, "down_times"] = -1
            df_limit.at[symbol, "up_M_down"] = 0
            df_limit.at[symbol, "up_times_7pct"] = -1
            df_limit.at[symbol, "down_times_7pct"] = -1
            df_limit.at[symbol, "up_A_down_7pct"] = 0
            df_limit.at[symbol, "up_times_5pct"] = -1
            df_limit.at[symbol, "down_times_5pct"] = -1
            df_limit.at[symbol, "up_A_down_5pct"] = 0
            df_limit.at[symbol, "up_times_3pct"] = -1
            df_limit.at[symbol, "down_times_3pct"] = -1
            df_limit.at[symbol, "up_A_down_3pct"] = 0
            continue
        df_stock.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct_chg",
                "涨跌额": "change",
                "换手率": "turnover",
            },
            inplace=True,
        )
        df_stock["date"] = pd.to_datetime(df_stock["date"])
        df_stock.set_index(keys="date", inplace=True)
        df_stock.sort_index(ascending=True, inplace=True)
        dt_stock_latest = datetime.datetime.combine(df_stock.index.max(), time_pm_end)
        if dt_limit is None:
            dt_limit = dt_stock_latest
        elif dt_limit < dt_stock_latest:
            dt_limit = dt_stock_latest
        print(f"\r{str_msg_bar} - [{dt_stock_latest}]\033[K", end="")
        df_up = df_stock[df_stock["pct_chg"] > 9.9]
        up_times = len(df_up)
        df_up_7pct = df_stock[df_stock["pct_chg"] > 7]
        up_times_7pct = len(df_up_7pct)
        df_up_5pct = df_stock[df_stock["pct_chg"] > 5]
        up_times_5pct = len(df_up_5pct)
        df_up_3pct = df_stock[df_stock["pct_chg"] > 3]
        up_times_3pct = len(df_up_3pct)
        df_down = df_stock[df_stock["pct_chg"] < -9.9]
        down_times = len(df_down)
        df_down_7pct = df_stock[df_stock["pct_chg"] < -7]
        down_times_7pct = len(df_down_7pct)
        df_down_5pct = df_stock[df_stock["pct_chg"] < -5]
        down_times_5pct = len(df_down_5pct)
        df_down_3pct = df_stock[df_stock["pct_chg"] < -3]
        down_times_3pct = len(df_down_3pct)
        df_stock_t5 = df_stock.iloc[-5:]
        if not df_stock_t5.empty:
            t5_amplitude = df_stock_t5["amplitude"].mean().round(2)
            t5_max = (df_stock_t5["high"].max() * 0.998).round(2)
            t5_min = (df_stock_t5["low"].min() * 1.002).round(2)
            t5_mean = ((t5_max + t5_min) / 2).round(2)
            t5_pct = ((t5_max - t5_min) / t5_mean).round(4) * 100
        else:
            t5_amplitude = 0
            t5_max = 0
            t5_min = 0
            t5_mean = 0
            t5_pct = 0
        df_stock_t20 = df_stock.iloc[-20:-5]
        if not df_stock_t20.empty:
            t20_amplitude = df_stock_t20["amplitude"].mean().round(2)
            t20_max = (df_stock_t20["high"].max() * 0.998).round(2)
            t20_min = (df_stock_t20["low"].min() * 1.002).round(2)
            t20_mean = ((t20_max + t20_min) / 2).round(2)
            t20_pct = ((t20_max - t20_min) / t20_mean).round(4) * 100
        else:
            t20_amplitude = 0
            t20_max = 0
            t20_min = 0
            t20_mean = 0
            t20_pct = 0
        df_stock_t40 = df_stock.iloc[-40:-20]
        if not df_stock_t40.empty:
            t40_amplitude = df_stock_t40["amplitude"].mean().round(2)
            t40_max = (df_stock_t40["high"].max() * 0.998).round(2)
            t40_min = (df_stock_t40["low"].min() * 1.002).round(2)
            t40_mean = ((t40_max + t40_min) / 2).round(2)
            t40_pct = ((t40_max - t40_min) / t40_mean).round(4) * 100
        else:
            t40_amplitude = 0
            t40_max = 0
            t40_min = 0
            t40_mean = 0
            t40_pct = 0
        df_stock_t60 = df_stock.iloc[-60:-40]
        if not df_stock_t60.empty:
            t60_amplitude = df_stock_t60["amplitude"].mean().round(2)
            t60_max = (df_stock_t60["high"].max() * 0.998).round(2)
            t60_min = (df_stock_t60["low"].min() * 1.002).round(2)
            t60_mean = ((t60_max + t60_min) / 2).round(2)
            t60_pct = ((t60_max - t60_min) / t60_mean).round(4) * 100
        else:
            t60_amplitude = 0
            t60_max = 0
            t60_min = 0
            t60_mean = 0
            t60_pct = 0
        df_stock_t80 = df_stock.iloc[-80:-60]
        if not df_stock_t80.empty:
            t80_amplitude = df_stock_t80["amplitude"].mean().round(2)
            t80_max = (df_stock_t80["high"].max() * 0.998).round(2)
            t80_min = (df_stock_t80["low"].min() * 1.002).round(2)
            t80_mean = ((t80_max + t80_min) / 2).round(2)
            t80_pct = ((t80_max - t60_min) / t80_mean).round(4) * 100
        else:
            t80_amplitude = 0
            t80_max = 0
            t80_min = 0
            t80_mean = 0
            t80_pct = 0
        df_stock_t120 = df_stock.iloc[-120:-80]
        if not df_stock_t120.empty:
            t120_amplitude = df_stock_t120["amplitude"].mean().round(2)
            t120_max = (df_stock_t120["high"].max() * 0.998).round(2)
            t120_min = (df_stock_t120["low"].min() * 1.002).round(2)
            t120_mean = ((t120_max + t120_min) / 2).round(2)
            t120_pct = ((t120_max - t120_min) / t120_mean).round(4) * 100
        else:
            t120_amplitude = 0
            t120_max = 0
            t120_min = 0
            t120_mean = 0
            t120_pct = 0
        arr_amplitude = np.array(
            [
                t5_amplitude,
                t20_amplitude,
                t40_amplitude,
                t60_amplitude,
                t80_amplitude,
                t120_amplitude,
            ]
        )
        arr_amplitude = arr_amplitude[arr_amplitude > 0]
        t_m_amplitude = arr_amplitude.mean().round(2)
        t_m_amplitude_std = arr_amplitude.std().round(2)
        t_m_amplitude_grade = (t_m_amplitude_std / t_m_amplitude * 100).round(2)
        arr_pct = np.array([t5_pct, t20_pct, t40_pct, t60_pct, t80_pct, t120_pct])
        arr_pct = arr_pct[arr_pct > 0]
        t_m_pct = arr_pct.mean().round(2)
        t_m_pct_std = arr_pct.std().round(2)
        t_m_pct_grade = (t_m_pct_std / t_m_pct * 100).round(2)
        df_limit.at[symbol, "times"] = len(df_stock)
        df_limit.at[symbol, "up_times"] = up_times
        df_limit.at[symbol, "down_times"] = down_times
        df_limit.at[symbol, "up_M_down"] = up_times - down_times
        df_limit.at[symbol, "up_times_7pct"] = up_times_7pct
        df_limit.at[symbol, "down_times_7pct"] = down_times_7pct
        df_limit.at[symbol, "up_A_down_7pct"] = up_times_7pct + down_times_7pct
        df_limit.at[symbol, "up_times_5pct"] = up_times_5pct
        df_limit.at[symbol, "down_times_5pct"] = down_times_5pct
        df_limit.at[symbol, "up_A_down_5pct"] = up_times_5pct + down_times_5pct
        df_limit.at[symbol, "up_times_3pct"] = up_times_3pct
        df_limit.at[symbol, "down_times_3pct"] = down_times_3pct
        df_limit.at[symbol, "up_A_down_3pct"] = up_times_3pct + down_times_3pct
        df_limit.at[symbol, "T_m_amplitude"] = t_m_amplitude
        df_limit.at[symbol, "T_m_amplitude_std"] = t_m_amplitude_std
        df_limit.at[symbol, "T_m_amplitude_grade"] = t_m_amplitude_grade
        df_limit.at[symbol, "T_m_pct"] = t_m_pct
        df_limit.at[symbol, "T_m_pct_std"] = t_m_pct_std
        df_limit.at[symbol, "T_m_pct_grade"] = t_m_pct_grade
        df_limit.at[symbol, "T5_amplitude"] = t5_amplitude
        df_limit.at[symbol, "T5_pct"] = t5_pct
        df_limit.at[symbol, "T5_max"] = t5_max
        df_limit.at[symbol, "T5_min"] = t5_min
        df_limit.at[symbol, "T5_mean"] = t5_mean
        df_limit.at[symbol, "T20_amplitude"] = t20_amplitude
        df_limit.at[symbol, "T20_pct"] = t20_pct
        df_limit.at[symbol, "T20_max"] = t20_max
        df_limit.at[symbol, "T20_min"] = t20_min
        df_limit.at[symbol, "T20_mean"] = t20_mean
        df_limit.at[symbol, "T40_amplitude"] = t40_amplitude
        df_limit.at[symbol, "T40_pct"] = t40_pct
        df_limit.at[symbol, "T40_max"] = t40_max
        df_limit.at[symbol, "T40_min"] = t40_min
        df_limit.at[symbol, "T40_mean"] = t40_mean
        df_limit.at[symbol, "T60_amplitude"] = t60_amplitude
        df_limit.at[symbol, "T60_pct"] = t60_pct
        df_limit.at[symbol, "T60_max"] = t60_max
        df_limit.at[symbol, "T60_min"] = t60_min
        df_limit.at[symbol, "T60_mean"] = t60_mean
        df_limit.at[symbol, "T80_amplitude"] = t80_amplitude
        df_limit.at[symbol, "T80_pct"] = t80_pct
        df_limit.at[symbol, "T80_max"] = t80_max
        df_limit.at[symbol, "T80_min"] = t80_min
        df_limit.at[symbol, "T80_mean"] = t80_mean
        df_limit.at[symbol, "T120_amplitude"] = t120_amplitude
        df_limit.at[symbol, "T120_pct"] = t120_pct
        df_limit.at[symbol, "T120_max"] = t120_max
        df_limit.at[symbol, "T120_min"] = t120_min
        df_limit.at[symbol, "T120_mean"] = t120_mean
        # 写入腌制数据 df_limit
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_limit, dest=file_name_df_limit_temp)
    if i >= count:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        df_limit.sort_values(
            by=["up_M_down", "up_A_down_7pct", "up_A_down_5pct"],
            ascending=False,
            inplace=True,
        )
        df_limit.index.rename(name="symbol", inplace=True)
        analysis.base.write_obj_to_db(
            obj=df_limit, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_limit)
        if os.path.exists(file_name_df_limit_temp):
            os.remove(path=file_name_df_limit_temp)
            logger.trace(f"[{file_name_df_limit_temp}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Limit Count analysis takes [{str_gm}]")
    logger.trace(f"Limit Count End")
    return True
