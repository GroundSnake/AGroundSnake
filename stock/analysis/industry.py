# modified at 2023/4/12 13:36
from __future__ import annotations
import os
import random
import sys
import time
import datetime
import requests
import pandas as pd
from loguru import logger
import feather
import tushare as ts
import analysis.base
from analysis.const import (
    dt_now,
    list_all_stocks,
    path_data,
    dt_date_trading,
    dt_pm_end,
    str_date_path,
    time_pm_end,
    filename_chip_shelve,
)


str_date_trading = dt_date_trading.strftime("%Y%m%d")
dt_delta = dt_date_trading - datetime.timedelta(days=366)
str_delta = dt_delta.strftime("%Y%m%d")
path_industry = os.path.join(path_data, f"industry_ths")
if not os.path.exists(path_industry):
    os.mkdir(path_industry)


def update_industry_index_ths() -> bool:
    name: str = f"index_kline_industry"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    dt_index_kline_industry = None
    if not os.path.exists(path_industry):
        os.mkdir(path_industry)
    df_industry_index = analysis.base.read_obj_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        logger.error(f"df_industry_index is empty,return None DataFrame")
        return False
    list_industry_index_code = list(set(df_industry_index["industry_code"].tolist()))
    random.shuffle(list_industry_index_code)
    pro = ts.pro_api()
    len_list_index_codes = len(list_industry_index_code)
    i = 0
    for ts_code_index in list_industry_index_code:
        i += 1
        symbol_index = analysis.base.code_ts_to_ths(ts_code_index)
        str_msg_bar = f"\r{name}:[{i:4d}/{len_list_index_codes:4d}] - [{symbol_index}]"
        i_times_ths_daily = 0
        while True:
            try:
                df_ths_daily = pro.ths_daily(
                    ts_code=ts_code_index,
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except requests.exceptions.ConnectionError as e:
                print("--", repr(e))
                logger.trace(repr(e))
                time.sleep(2)
            else:
                df_ths_daily["trade_date"] = pd.to_datetime(df_ths_daily["trade_date"])
                df_ths_daily.set_index(keys=["trade_date"], inplace=True)
                df_ths_daily.sort_index(ascending=True, inplace=True)
                filename_ths_daily = os.path.join(
                    path_industry, f"{symbol_index}.ftr"
                )
                feather.write_dataframe(df=df_ths_daily, dest=filename_ths_daily)
                dt_industry_index_temp = datetime.datetime.combine(df_ths_daily.index.max().date(), time_pm_end)
                if dt_now > dt_pm_end and dt_industry_index_temp != dt_pm_end:
                    print(f'[{name}] - {dt_industry_index_temp} is not new')
                    sys.exit()
                str_msg_bar += f' - {dt_industry_index_temp}'
                if dt_index_kline_industry is None or dt_index_kline_industry < dt_industry_index_temp:
                    dt_index_kline_industry = dt_industry_index_temp
                break
            if i_times_ths_daily >= 2:
                print(f"[{ts_code_index}] Request ConnectionError - [daily] - [{i_times_ths_daily}]times")
                logger.trace(f"[{ts_code_index}] Request ConnectionError - [{i_times_ths_daily}]times")
                sys.exit()
            i_times_ths_daily += 1
        print(str_msg_bar, end='')
    if i >= len_list_index_codes:
        print("\n", end="")  # 格式处理
        analysis.base.set_version(key=name, dt=dt_index_kline_industry)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True

def industry_pct() -> bool:
    name: str = f"df_industry_pct"
    kdata: str = f"index_kline_industry"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    if analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        logger.trace(f"{kdata} is latest")
    else:
        logger.trace(f"Update the {kdata}")
        if update_industry_index_ths():
            logger.trace(f"{kdata} Update finish")
    df_industry_index = analysis.base.read_obj_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    filename_industry_pct = os.path.join(
        path_data, f"industry_pct_temp_{str_date_path}.ftr"
    )
    if os.path.exists(filename_industry_pct):
        df_industry_pct = feather.read_dataframe(source=filename_industry_pct)
        if df_industry_pct.empty:
            logger.trace(f"df_industry_pct cache is empty")
        else:
            logger.trace(f"df_industry_pct cache is not empty")
    else:
        df_industry_pct = pd.DataFrame()
    list_industry_pct_exist = set(df_industry_pct.columns.tolist())
    list_industry_index_code = set(df_industry_index["industry_code"].tolist())
    i = 0
    len_list_index_codes = len(list_industry_index_code)
    for ts_code_index in list_industry_index_code:
        i += 1
        symbol_index = analysis.base.code_ts_to_ths(ts_code_index)
        str_msg_bar = f"\r{name}:[{i:4d}/{len_list_index_codes:4d}] - [{symbol_index}]"
        if ts_code_index in list_industry_pct_exist:
            print(f'{str_msg_bar} - exist', end='')
            continue
        filename_ths_daily = os.path.join(
            path_industry, f"{symbol_index}.ftr"
        )
        df_ths_daily = feather.read_dataframe(source=filename_ths_daily)
        df_ths_daily_pct = df_ths_daily[["pct_change"]].copy()
        df_ths_daily_pct.rename(
            columns={"pct_change": ts_code_index}, inplace=True
        )
        df_industry_pct = pd.concat(
            objs=[df_industry_pct, df_ths_daily_pct],
            axis=1,
            join="outer",
        )
        feather.write_dataframe(
            df=df_industry_pct, dest=filename_industry_pct
        )
        dt_ths_daily = datetime.datetime.combine(
            df_ths_daily.index.max().date(), time_pm_end
        )
        print(f'{str_msg_bar} - {dt_ths_daily}', end='')
    df_industry_pct = df_industry_pct.applymap(func=lambda x: x + 100)
    len_df_industry_pct = len(df_industry_pct)
    i = 0
    while i < len_df_industry_pct:
        mim_pct = df_industry_pct.iloc[i].min()
        df_industry_pct.iloc[i] = df_industry_pct.iloc[i].apply(
            func=lambda x: (x / mim_pct - 1) * 100
        )
        i += 1
    if i >= len_df_industry_pct:
        print("\n", end="")  # 格式处理
        analysis.base.write_obj_to_db(
            obj=df_industry_pct,
            key=name,
            filename=filename_chip_shelve,
        )
        dt_industry_pct = datetime.datetime.combine(
            df_industry_pct.index.max().date(), time_pm_end
        )
        analysis.base.set_version(key=name, dt=dt_industry_pct)
        logger.trace(f"feather df_industry_pct success")
    if os.path.exists(filename_industry_pct):  # 删除临时文件
        os.remove(path=filename_industry_pct)
        logger.trace(f"remove {filename_industry_pct} success")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True
def industry_rank():
    name: str = f"df_industry_rank"
    kdata:str = 'df_industry_pct'
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name} is latest")
        return True
    if analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        logger.trace(f"{kdata} is latest")
    else:
        if industry_pct():
            logger.trace(f"{kdata} is latest")
    df_industry_rank = pd.DataFrame(
        columns=[
            "name",
            "T5",
            "T5_Zeroing_sort",
            "T5_rank",
            "T20",
            "T20_Zeroing_sort",
            "T20_rank",
            "T40",
            "T40_Zeroing_sort",
            "T40_rank",
            "T60",
            "T60_Zeroing_sort",
            "T60_rank",
            "T80",
            "T80_Zeroing_sort",
            "T80_rank",
            "rank",
            "max_min",
        ]
    )
    df_industry_pct = analysis.base.read_obj_from_db(
        key="df_industry_pct", filename=filename_chip_shelve
    )
    dt_industry_rank = datetime.datetime.combine(
        df_industry_pct.index.max().date(), time_pm_end
    )
    df_5_industry_pct = df_industry_pct.iloc[-5:]
    df_20_industry_pct = df_industry_pct.iloc[-20:-5]
    df_40_industry_pct = df_industry_pct.iloc[-40:-20]
    df_60_industry_pct = df_industry_pct.iloc[-60:-40]
    df_80_industry_pct = df_industry_pct.iloc[-80:-60]
    df_industry_rank["T5"] = (df_5_industry_pct.sum(axis=0) / 5 * 20).round(2)
    df_industry_rank["T20"] = (df_20_industry_pct.sum(axis=0) / 15 * 20).round(2)
    df_industry_rank["T40"] = df_40_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T60"] = df_60_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T80"] = df_80_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T5_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T5"]
    )
    df_industry_rank["T5_rank"] = df_industry_rank["T5"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T20_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T20"]
    )
    df_industry_rank["T20_rank"] = df_industry_rank["T20"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T40_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T40"]
    )
    df_industry_rank["T40_rank"] = df_industry_rank["T40"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T60_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T60"]
    )
    df_industry_rank["T60_rank"] = df_industry_rank["T60"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T80_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T80"]
    )
    df_industry_rank["T80_rank"] = df_industry_rank["T80"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["rank"] = (
            df_industry_rank["T5_rank"]
            + df_industry_rank["T20_rank"]
            + df_industry_rank["T40_rank"]
            + df_industry_rank["T60_rank"]
            + df_industry_rank["T80_rank"]
    )
    pro = ts.pro_api()
    df_ths_index = pro.ths_index()
    df_ths_index.set_index(keys="ts_code", inplace=True)
    for ths_index_code in df_industry_rank.index.tolist():
        if ths_index_code in df_ths_index.index.tolist():
            df_industry_rank.at[ths_index_code, "name"] = df_ths_index.at[
                ths_index_code, "name"
            ]
            df_industry_rank.at[ths_index_code, "max_min"] = max(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            ) - min(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            )
    df_industry_rank.sort_values(by=["max_min"], axis=0, ascending=False, inplace=True)
    analysis.base.write_obj_to_db(
        obj=df_industry_rank, key="df_industry_rank", filename=filename_chip_shelve
    )
    analysis.base.set_version(key=name, dt=dt_industry_rank)
    df_industry_rank_pool = df_industry_rank[df_industry_rank["max_min"] >= 56]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T5_rank"] >= 66)
        | (df_industry_rank_pool["T5_rank"] <= 10)
        ]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T20_rank"] >= 66)
        | (df_industry_rank_pool["T20_rank"] <= 10)
        ]
    df_industry_rank_pool = df_industry_rank_pool[
        (df_industry_rank_pool["T40_rank"] >= 56)
        | (df_industry_rank_pool["T40_rank"] <= 20)
        ]
    if  not df_industry_rank_pool.empty:
        df_industry_rank_pool.sort_values(
            by=["T5_rank"], axis=0, ascending=False, inplace=True
        )
        analysis.base.write_obj_to_db(
            obj=df_industry_rank_pool,
            key="df_industry_rank_pool",
            filename=filename_chip_shelve,
        )
        analysis.base.set_version(key='df_industry_rank_pool', dt=dt_industry_rank)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True


def ths_industry(list_symbol: list | str = None) -> bool:
    name: str = f"df_industry"
    kdata: str = f"update_industry_index"
    logger.trace(f"{name} Begin！")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"ths_industry,Break and End")
        return True
    dt_daily_max = None
    dt_mow = datetime.datetime.now()
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = list_all_stocks
    if isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    filename_industry_temp = os.path.join(
        path_data, f"industry_temp_{str_date_path}.ftr"
    )
    list_exist = list()
    if analysis.base.is_latest_version(key=kdata, filename=filename_chip_shelve):
        logger.trace(f"{kdata} is latest")
    else:
        logger.trace(f"Update the {kdata}")
        if update_industry_index_ths():
            logger.trace("{kline} Update finish")
        else:
            sys.exit()
    df_industry_index = analysis.base.read_obj_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        logger.error(f"df_industry_index is empty,return None DataFrame")
        return False
    if os.path.exists(filename_industry_temp):
        df_industry = feather.read_dataframe(source=filename_industry_temp)
        if df_industry.empty:
            logger.trace(f"{name} cache is empty")
        else:
            logger.trace(f"{name} cache is not empty")
            df_industry = df_industry.sample(frac=1)
            list_exist = df_industry.index.tolist()
    else:
        df_industry = pd.DataFrame()
    list_symbol_industry_class = df_industry_index.index.tolist()
    pro = ts.pro_api()
    i = 0
    count_list_symbol = len(list_symbol)
    for symbol in list_symbol:
        i += 1
        str_msg_bar = f"{name}:[{i:4d}/{count_list_symbol:4d}] -- [{symbol}]"
        if symbol in list_exist:  # 己存在，断点继续
            print(f"{str_msg_bar}- exist", end="")
            continue
        if symbol in list_symbol_industry_class:
            ts_code = analysis.base.code_ths_to_ts(symbol)
            ts_code_index = df_industry_index.at[symbol, "industry_code"]
            symbol_class = analysis.base.code_ts_to_ths(ts_code_index)
            i_times_daily = 0
            while True:
                try:
                    df_daily = pro.daily(
                        ts_code=ts_code, start_date=str_delta, end_date=str_date_trading
                    )
                except requests.exceptions.ConnectionError as e:
                    print("--", repr(e))
                    logger.trace(repr(e))
                    time.sleep(2)
                else:
                    if df_daily.empty:
                        print(f"[df_daily] is empty.")
                        logger.trace(f"[df_daily] is empty.")
                        time.sleep(2)
                    else:
                        break
                if i_times_daily >= 2:
                    print(f"[{symbol}] Request ConnectionError - [daily] - [{i_times_daily}]times")
                    logger.trace(f"[{symbol}] Request ConnectionError - [{i_times_daily}]times")
                    sys.exit()
                i_times_daily += 1
            df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
            df_daily.set_index(keys=["trade_date"], inplace=True)
            df_daily.sort_index(ascending=True, inplace=True)
            filename_ths_daily = os.path.join(
                path_industry, f"{symbol_class}.ftr"
            )
            if os.path.exists(filename_ths_daily):
                df_ths_daily = feather.read_dataframe(source=filename_ths_daily)
            else:
                print(f"{symbol_class} is not exist")
                logger.trace(f"{symbol_class} is not exist")
                return False
            list_index_df_data = df_daily.index.tolist()
            list_index_df_ths_daily = df_ths_daily.index.tolist()
            up = 0
            down = 0
            up_keep_days = 0
            down_keep_days = 0
            len_record = len(list_index_df_data)
            str_msg_bar += f" - [{len_record:3d}]"
            dt_daily = datetime.datetime.combine(df_daily.index.max(), time_pm_end)
            dt_ths_daily = datetime.datetime.combine(df_ths_daily.index.max(), time_pm_end)
            if dt_daily_max is None or dt_daily_max < dt_daily:
                dt_daily_max = dt_daily
            if dt_daily == dt_ths_daily:
                str_msg_bar += f" - [{dt_daily}]"
            else:
                str_msg_bar += f" - is not latest - [{dt_daily}] - [{dt_ths_daily}]"
            print(f'\r{str_msg_bar}\033[K', end="")
            for index in list_index_df_data:
                if index in list_index_df_ths_daily:
                    if (
                        df_daily.at[index, "pct_chg"]
                        > df_ths_daily.at[index, "pct_change"]
                    ):
                        up += 1
                        up_keep_days += 1
                        down_keep_days = 0
                    elif (
                        df_daily.at[index, "pct_chg"]
                        < df_ths_daily.at[index, "pct_change"]
                    ):
                        down += 1
                        up_keep_days = 0
                        down_keep_days += 1
                    else:
                        continue
            up_down = up - down
            df_industry.at[symbol, "industry_code"] = df_industry_index.at[
                symbol, "industry_code"
            ]
            df_industry.at[symbol, "industry_name"] = df_industry_index.at[
                symbol, "industry_name"
            ]
            df_industry.at[symbol, "up_industry"] = up
            df_industry.at[symbol, "down_industry"] = down
            df_industry.at[symbol, "up_down_industry"] = up_down
            df_industry.at[symbol, "up_keep_days_industry"] = up_keep_days
            df_industry.at[symbol, "down_keep_days_industry"] = down_keep_days
        feather.write_dataframe(df=df_industry, dest=filename_industry_temp)
    print("\n", end="")  # 格式处理
    if i >= count_list_symbol:
        analysis.base.write_obj_to_db(
            obj=df_industry, key=name, filename=filename_chip_shelve
        )
        if os.path.exists(filename_industry_temp):  # 删除临时文件
            os.remove(path=filename_industry_temp)
            logger.trace(f"remove {filename_industry_temp} success")
    logger.trace(f"remove all ths_daily_industry ftr success")
    if dt_mow > dt_pm_end and dt_daily_max != dt_pm_end:
        print(f"{name} is not latest")
    analysis.base.set_version(key=name, dt=dt_daily_max)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis [{name}] takes {str_gm}")
    logger.trace(f"{name} End")
    return True
"""
    for file in os.listdir(path_industry):  # 删除临时文件
        if file.startswith('ti') and file.endswith('.ftr'):
            filename_ths_daily = os.path.join(path_industry, file)
            os.remove(path=filename_ths_daily)
"""