# modified at 2023/2/26 13:05
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


def ths_industry(list_symbol: list | str = None) -> pd.DataFrame | bool:
    name: str = f"df_industry"
    start_loop_time = time.perf_counter_ns()
    dt_date_trading = analysis.base.latest_trading_day()
    str_date_trading = dt_date_trading.strftime("%Y%m%d")
    dt_delta = dt_date_trading - datetime.timedelta(days=366)
    str_delta = dt_delta.strftime("%Y%m%d")
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    dt_now = datetime.datetime.now()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    path_industry = os.path.join(path_main, "data", f"industry_ths")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if not os.path.exists(path_industry):
        os.mkdir(path_industry)
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = analysis.base.all_chs_code()
    if isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    file_name_industry_temp = os.path.join(
        path_data, f"industry_temp_{str_date_path}.ftr"
    )
    # file_name_industry = os.path.join(path_data, f"industry.ftr")
    file_name_chip_h5 = os.path.join(path_data, f"chip.h5")
    file_name_industry_csv = os.path.join(path_check, f"industry_{str_date_path}.csv")
    file_name_industry_pct = os.path.join(path_data, f"industry_pct.ftr")
    file_name_industry_pct_temp = os.path.join(path_data, f"industry_pct_temp_{str_date_path}.ftr")
    file_name_industry_pct_csv = os.path.join(path_check, f"industry_pct_{str_date_path}.csv")
    list_exist = list()
    df_config = pd.DataFrame()
    if os.path.exists(file_name_chip_h5):
        try:
            df_config = pd.read_hdf(path_or_buf=file_name_chip_h5, key="df_config")
        except KeyError as e:
            logger.trace(f"df_config not exist KeyError [{e}]")
        if not df_config.empty:
            try:
                logger.trace(
                    f"the latest df_industry at {df_config.at[name, 'date']},The new at {dt_pm_end}"
                )
                if (
                        df_config.at[name, "date"] < dt_now < dt_pm_end
                        or df_config.at[name, "date"] == dt_pm_end
                ):
                    logger.trace(f"{name}-[{file_name_chip_h5}] is latest")
                    df_industry = pd.read_hdf(path_or_buf=file_name_chip_h5, key=name)
                    logger.trace(f"ths_industry,Break and End")
                    return df_industry
            except KeyError as e:
                logger.trace(f"df_config not exist KeyError [{e}]")
                df_config.at[name, "date"] = dt_now
        else:
            logger.trace(f"df_config is empty")
    if os.path.exists(file_name_chip_h5):
        try:
            df_industry_class = pd.read_hdf(path_or_buf=file_name_chip_h5, key="df_industry_class")
        except KeyError as e:
            logger.error(f"df_industry_class not exist,KeyError [{e}]")
            sys.exit()
        else:
            if df_industry_class.empty:
                logger.error(f"df_industry_class is empty")
                return False
    else:
        return False
    if os.path.exists(file_name_industry_temp):
        df_industry = feather.read_dataframe(source=file_name_industry_temp)
        if df_industry.empty:
            logger.trace("df_industry cache is empty")
        else:
            logger.trace("df_industry cache is not empty")
            list_exist = df_industry.index.tolist()
    else:
        df_industry = pd.DataFrame()
    if os.path.exists(file_name_industry_pct_temp):
        df_all_industry_pct = feather.read_dataframe(source=file_name_industry_pct_temp)
    else:
        df_all_industry_pct = pd.DataFrame()
    list_symbol_industry_class = df_industry_class.index.tolist()
    set_industry_class = set(df_industry_class["industry_code"].tolist())
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
            file_name_ths_daily = os.path.join(
                path_industry, f"{symbol_class}_{str_date_path}.ftr"
            )
            if os.path.exists(file_name_ths_daily):
                df_ths_daily = feather.read_dataframe(source=file_name_ths_daily)
            else:
                df_ths_daily = pro.ths_daily(
                    ts_code=ts_code_class,
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
                df_ths_daily["trade_date"] = pd.to_datetime(df_ths_daily["trade_date"])
                df_ths_daily.set_index(keys=["trade_date"], inplace=True)
                df_ths_daily.sort_index(ascending=True, inplace=True)
                feather.write_dataframe(df=df_ths_daily, dest=file_name_ths_daily)
                df_ths_daily_pct = df_ths_daily[['pct_change']].copy()
                df_ths_daily_pct.rename(columns={'pct_change': ts_code_class}, inplace=True)
                df_all_industry_pct = pd.concat(
                    objs=[df_all_industry_pct, df_ths_daily_pct],
                    axis=1,
                    join="outer",
                )
                feather.write_dataframe(df=df_all_industry_pct, dest=file_name_industry_pct_temp)
            list_index_df_data = df_daily.index.tolist()
            list_index_df_ths_daily = df_ths_daily.index.tolist()
            up = 0
            down = 0
            up_keep_days = 0
            down_keep_days = 0
            i_recode = 0
            len_record = len(list_index_df_data)
            for index in list_index_df_data:
                i_recode += 1
                print(
                    f"\rIndustry:[{i:4d}/{len_list_symbol:4d}] -- [{symbol}] -- [{i_recode:3d}/{len_record:3d}]",
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
        feather.write_dataframe(df=df_industry, dest=file_name_industry_temp)
    # print(df_all_industry_pct)
    if i >= len_list_symbol:
        print("\n", end="")  # 格式处理
        df_industry.to_hdf(path_or_buf=file_name_chip_h5, key=name, format='table')
        df_industry.to_csv(path_or_buf=file_name_industry_csv)
        df_all_industry_pct = df_all_industry_pct.applymap(func=lambda x: x + 100)
        len_df_all_industry_pct = len(df_all_industry_pct)
        dt_industry_date = df_all_industry_pct.index.max().date()
        i = 0
        while i < len_df_all_industry_pct:
            mim_pct = df_all_industry_pct.iloc[i].min()
            df_all_industry_pct.iloc[i] = df_all_industry_pct.iloc[i].apply(func=lambda x: (x / mim_pct - 1) * 100)
            i += 1
        if i >= len_df_all_industry_pct:
            df_all_industry_pct.to_csv(path_or_buf=file_name_industry_pct_csv)
            feather.write_dataframe(df=df_all_industry_pct, dest=file_name_industry_pct)
        if os.path.exists(file_name_chip_h5):
            dt_industry = datetime.datetime.combine(dt_industry_date, time_pm_end)
            df_config.at[name, "date"] = dt_industry
            df_config.to_hdf(path_or_buf=file_name_chip_h5, key="df_config", format='table')
        if os.path.exists(file_name_industry_pct_temp):  # 删除临时文件
            os.remove(path=file_name_industry_pct_temp)
        if os.path.exists(file_name_industry_temp):  # 删除临时文件
            os.remove(path=file_name_industry_temp)
        for ts_code_class in set_industry_class:
            symbol_class = analysis.base.code_ts_to_ths(ts_code_class)
            file_name_ths_daily = os.path.join(
                path_industry, f"{symbol_class}_{str_date_path}.ftr"
            )
            if os.path.exists(file_name_ths_daily):  # 删除临时文件
                os.remove(path=file_name_ths_daily)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Industry analysis takes {str_gm}")
    return df_industry
