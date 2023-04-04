# modified at 2023/3/29 15:47
from __future__ import annotations
import os
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
    list_all_stocks,
    path_data,
    dt_date_trading,
    str_date_path,
    time_pm_end,
    filename_chip_shelve,
)


def ths_industry(list_symbol: list | str = None) -> bool:
    name: str = f"df_industry"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    str_date_trading = dt_date_trading.strftime("%Y%m%d")
    dt_delta = dt_date_trading - datetime.timedelta(days=366)
    str_delta = dt_delta.strftime("%Y%m%d")
    path_industry = os.path.join(path_data, f"industry_ths")
    if not os.path.exists(path_industry):
        os.mkdir(path_industry)
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = list_all_stocks
    if isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    filename_industry_temp = os.path.join(
        path_data, f"industry_temp_{str_date_path}.ftr"
    )
    filename_industry_pct_temp = os.path.join(
        path_data, f"industry_pct_temp_{str_date_path}.ftr"
    )
    list_exist = list()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"ths_industry,Break and End")
        return True
    df_industry_class = analysis.base.read_obj_from_db(
        key="df_industry_class", filename=filename_chip_shelve
    )
    if df_industry_class.empty:
        logger.error(f"df_industry_class is empty,return None DataFrame")
        return False
    if os.path.exists(filename_industry_temp):
        df_industry = feather.read_dataframe(source=filename_industry_temp)
        if df_industry.empty:
            logger.trace(f"{name} cache is empty")
        else:
            logger.trace(f"{name} cache is not empty")
            list_exist = df_industry.index.tolist()
    else:
        df_industry = pd.DataFrame()
    list_all_industry_pct_exist = set()
    if os.path.exists(filename_industry_pct_temp):
        df_all_industry_pct = feather.read_dataframe(source=filename_industry_pct_temp)
        if df_all_industry_pct.empty:
            logger.trace(f"df_all_industry_pct cache is empty")
        else:
            list_all_industry_pct_exist = set(df_all_industry_pct.columns.tolist())
            logger.trace(f"df_all_industry_pct cache is not empty")
    else:
        df_all_industry_pct = pd.DataFrame()
    list_symbol_industry_class = df_industry_class.index.tolist()
    pro = ts.pro_api()
    i = 0
    len_list_symbol = len(list_symbol)
    for symbol in list_symbol:
        i += 1
        if symbol in list_exist:  # 己存在，断点继续
            print(f"\rIndustry:[{i:4d}/{len_list_symbol:4d}] -- [{symbol}]", end="")
            continue
        if symbol in list_symbol_industry_class:
            ts_code = analysis.base.code_ths_to_ts(symbol)
            ts_code_class = df_industry_class.at[symbol, "industry_code"]
            symbol_class = analysis.base.code_ts_to_ths(ts_code_class)
            df_daily = pd.DataFrame()
            i_times = 0
            while i_times <= 2:
                try:
                    df_daily = pro.daily(
                        ts_code=ts_code, start_date=str_delta, end_date=str_date_trading
                    )
                except requests.exceptions.ConnectionError as e:
                    print("--", repr(e))
                    logger.error(repr(e))
                    time.sleep(2)
                else:
                    break
                finally:
                    if i_times >= 2:
                        print(f"[{symbol}] Request ConnectionError")
                        sys.exit()
                i_times += 1
            if df_daily.empty:
                print(f"[{df_daily}] is empty.")
                sys.exit()
            df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
            df_daily.set_index(keys=["trade_date"], inplace=True)
            df_daily.sort_index(ascending=True, inplace=True)
            filename_ths_daily = os.path.join(
                path_industry, f"{symbol_class}_{str_date_path}.ftr"
            )
            if os.path.exists(filename_ths_daily):
                df_ths_daily = feather.read_dataframe(source=filename_ths_daily)
            else:
                df_ths_daily = pro.ths_daily(
                    ts_code=ts_code_class,
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
                df_ths_daily["trade_date"] = pd.to_datetime(df_ths_daily["trade_date"])
                df_ths_daily.set_index(keys=["trade_date"], inplace=True)
                df_ths_daily.sort_index(ascending=True, inplace=True)
                feather.write_dataframe(df=df_ths_daily, dest=filename_ths_daily)
            if ts_code_class not in list_all_industry_pct_exist:
                df_ths_daily_pct = df_ths_daily[["pct_change"]].copy()
                df_ths_daily_pct.rename(
                    columns={"pct_change": ts_code_class}, inplace=True
                )
                df_all_industry_pct = pd.concat(
                    objs=[df_all_industry_pct, df_ths_daily_pct],
                    axis=1,
                    join="outer",
                )
                list_all_industry_pct_exist.add(ts_code_class)
                feather.write_dataframe(
                    df=df_all_industry_pct, dest=filename_industry_pct_temp
                )
            list_index_df_data = df_daily.index.tolist()
            list_index_df_ths_daily = df_ths_daily.index.tolist()
            up = 0
            down = 0
            up_keep_days = 0
            down_keep_days = 0
            i_recode = 0
            len_record = len(list_index_df_data)
            dt_ths_daily = df_ths_daily.index.max().date()
            for index in list_index_df_data:
                i_recode += 1
                print(
                    f"\rIndustry:[{i:4d}/{len_list_symbol:4d}] -- [{symbol}] -- [{i_recode:3d}/{len_record:3d}] - {dt_ths_daily}",
                    end="",
                )
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
            df_industry.at[symbol, "industry_code"] = df_industry_class.at[
                symbol, "industry_code"
            ]
            df_industry.at[symbol, "industry_name"] = df_industry_class.at[
                symbol, "industry_name"
            ]
            df_industry.at[symbol, "up_industry"] = up
            df_industry.at[symbol, "down_industry"] = down
            df_industry.at[symbol, "up_down_industry"] = up_down
            df_industry.at[symbol, "up_keep_days_industry"] = up_keep_days
            df_industry.at[symbol, "down_keep_days_industry"] = down_keep_days
        feather.write_dataframe(df=df_industry, dest=filename_industry_temp)
    if i >= len_list_symbol:
        print("\n", end="")  # 格式处理
        analysis.base.write_obj_to_db(
            obj=df_industry, key=name, filename=filename_chip_shelve
        )
        df_all_industry_pct = df_all_industry_pct.applymap(func=lambda x: x + 100)
        len_df_all_industry_pct = len(df_all_industry_pct)
        i = 0
        while i < len_df_all_industry_pct:
            mim_pct = df_all_industry_pct.iloc[i].min()
            df_all_industry_pct.iloc[i] = df_all_industry_pct.iloc[i].apply(
                func=lambda x: (x / mim_pct - 1) * 100
            )
            i += 1
        if i >= len_df_all_industry_pct:
            analysis.base.write_obj_to_db(
                obj=df_all_industry_pct,
                key="df_all_industry_pct",
                filename=filename_chip_shelve,
            )
            dt_all_industry_pct = datetime.datetime.combine(
                df_all_industry_pct.index.max().date(), time_pm_end
            )
            analysis.base.set_version(key="df_all_industry_pct", dt=dt_all_industry_pct)
            logger.trace(f"feather df_all_industry_pct success")
        if os.path.exists(filename_industry_pct_temp):  # 删除临时文件
            os.remove(path=filename_industry_pct_temp)
            logger.trace(f"remove {filename_industry_pct_temp} success")
        if os.path.exists(filename_industry_temp):  # 删除临时文件
            os.remove(path=filename_industry_temp)
            logger.trace(f"remove {filename_industry_temp} success")
        set_industry_class = set(df_industry_class["industry_code"].tolist())
        for ts_code_class in set_industry_class:
            symbol_class = analysis.base.code_ts_to_ths(ts_code_class)
            filename_ths_daily = os.path.join(
                path_industry, f"{symbol_class}_{str_date_path}.ftr"
            )
            if os.path.exists(filename_ths_daily):  # 删除临时文件
                os.remove(path=filename_ths_daily)
        logger.trace(f"remove all ths_daily_industry ftr success")
    dt_industry = datetime.datetime.combine(
        df_all_industry_pct.index.max().date(), time_pm_end
    )
    analysis.base.set_version(key=name, dt=dt_industry)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis takes {str_gm}")
    logger.trace(f"{name} End")
    return True
