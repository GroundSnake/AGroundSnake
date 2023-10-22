# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import datetime
import time
import random
import feather
import pandas as pd
from loguru import logger
import akshare as ak
import analysis.base
import analysis.update_data
from analysis.const import (
    dt_init,
    path_data,
    dt_trading_last_T0,
    time_pm_end,
    filename_chip_shelve,
    dt_history,
    all_chs_code,
    all_chs_etf,
    path_main,
)


def limit_count() -> bool:
    name: str = "df_limit"
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"{name} Begin")
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    file_name_df_limit_temp = os.path.join(
        path_data, f"df_limit_count_temp_{str_dt_history_path}.ftr"
    )
    dt_delta = dt_trading_last_T0 - datetime.timedelta(days=366)
    str_date_trading = dt_history().strftime("%Y%m%d")
    str_delta = dt_delta.strftime("%Y%m%d")
    dt_limit = dt_init
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Limit Break End")
        return True
    if os.path.exists(file_name_df_limit_temp):
        df_limit = feather.read_dataframe(source=file_name_df_limit_temp)
    else:
        list_columns = [
            "times_limit",
            "up_7pct_times",
            "down_7pct_times",
            "correct_7pct_times",
            "up_3pct_times",
            "down_3pct_times",
            "correct_3pct_times",
            "alpha_amplitude",
            "alpha_pct",
            "alpha_turnover",
            "T5_amplitude",
            "T240_amplitude",
            "T5_pct",
            "T240_pct",
            "T5_turnover",
            "T240_turnover",
        ]
        list_symbol = all_chs_code()
        df_limit = pd.DataFrame(index=list_symbol, columns=list_columns)
    df_limit = df_limit.sample(frac=1)
    df_limit.fillna(value=0, inplace=True)
    i = 0
    count = len(df_limit)
    logger.trace(f"For loop Begin")
    for symbol in df_limit.index:
        i += 1
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_limit, dest=file_name_df_limit_temp)
        str_msg_bar = f"Limit Update: [{i:4d}/{count:4d}] - [{symbol}]"
        if df_limit.at[symbol, "times_limit"] != 0:
            print(f"\r{str_msg_bar} - Exist\033[K", end="")
            continue
        df_stock = pd.DataFrame()
        i_times = 0
        while i_times < 1:
            i_times += 1
            try:
                df_stock = ak.stock_zh_a_hist(
                    symbol=symbol[2:8],
                    period="daily",
                    start_date=str_delta,
                    end_date=str_date_trading,
                )
            except KeyError as e:
                print(f"\r{str_msg_bar} - Sleep({i_times}) - {repr(e)}\033[K")
                time.sleep(1)
            except OSError as e:
                print(f"\r{str_msg_bar} - Sleep({i_times}) - {repr(e)}\033[K")
                time.sleep(1)
            else:
                if df_stock.empty:
                    print(f"\r{str_msg_bar} - Sleep({i_times}) - empty\033[K")
                    time.sleep(1)
                else:
                    df_stock.rename(
                        columns={
                            "日期": "dt",
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
                    df_stock["dt"] = df_stock["dt"].apply(
                        func=lambda x: datetime.datetime.combine(
                            pd.to_datetime(x).date(), time_pm_end
                        )
                    )
                    df_stock.set_index(keys="dt", inplace=True)
                    df_stock.sort_index(ascending=True, inplace=True)
                    break
        if df_stock.empty:
            print(f"\r{str_msg_bar} - No Data\033[K")
            continue
        dt_stock_latest = df_stock.index.max()
        if dt_limit < dt_stock_latest:
            dt_limit = dt_stock_latest
        df_limit.at[symbol, "times_limit"] = len(df_stock)
        df_up_10pct = df_stock[df_stock["pct_chg"] > 7]
        df_limit.at[symbol, "up_7pct_times"] = len(df_up_10pct)
        df_down_10pct = df_stock[df_stock["pct_chg"] < -7]
        df_limit.at[symbol, "down_7pct_times"] = len(df_down_10pct)
        df_limit.at[symbol, "correct_7pct_times"] = (
            df_limit.at[symbol, "up_7pct_times"]
            - df_limit.at[symbol, "down_7pct_times"]
        )
        df_up_3pct = df_stock[df_stock["pct_chg"] > 3]
        df_limit.at[symbol, "up_3pct_times"] = up_3pct_times = len(df_up_3pct)
        df_down_3pct = df_stock[df_stock["pct_chg"] < -3]
        df_limit.at[symbol, "down_3pct_times"] = down_3pct_times = len(df_down_3pct)
        if up_3pct_times == 0 or down_3pct_times == 0:
            df_limit.at[symbol, "correct_3pct_times"] = 0
        else:
            df_limit.at[symbol, "correct_3pct_times"] = round(
                pow(min(up_3pct_times, down_3pct_times), 2)
                / max(up_3pct_times, down_3pct_times),
                0,
            )
        df_limit.at[symbol, "T240_amplitude"] = df_stock["amplitude"].mean().round(2)
        high_240t = df_stock["high"].max()
        low_240t = df_stock["low"].min()
        df_limit.at[symbol, "T240_pct"] = t240_pct = round(
            ((high_240t - low_240t) / ((high_240t + low_240t) / 2) * 100), 2
        )
        df_limit.at[symbol, "T240_turnover"] = df_stock["turnover"].mean().round(2)
        df_stock_5t = df_stock.iloc[-5:]
        if not df_stock_5t.empty:
            df_limit.at[symbol, "T5_amplitude"] = (
                df_stock_5t["amplitude"].mean().round(2)
            )
            high_5t = df_stock_5t["high"].max()
            low_5t = df_stock_5t["low"].min()
            df_limit.at[symbol, "T5_pct"] = t5_pct = round(
                ((high_5t - low_5t) / ((high_5t + low_5t) / 2) * 100), 2
            )
            df_limit.at[symbol, "T5_turnover"] = df_stock_5t["turnover"].mean().round(2)
        else:
            t5_pct = 0
        df_limit.at[symbol, "alpha_amplitude"] = (
            df_limit.at[symbol, "T5_amplitude"] - df_limit.at[symbol, "T240_amplitude"]
        )
        if t240_pct != 0:
            df_limit.at[symbol, "alpha_pct"] = round((pow(t5_pct, 2) / t240_pct), 2)
        df_limit.at[symbol, "alpha_turnover"] = (
            df_limit.at[symbol, "T5_turnover"] - df_limit.at[symbol, "T240_turnover"]
        )
        print(f"\r{str_msg_bar}\033[K", end="")  # for loop end, progress bar
    if i >= count:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        df_limit.sort_values(
            by=["correct_3pct_times", "correct_7pct_times", "alpha_amplitude"],
            ascending=False,
            inplace=True,
        )
        analysis.base.write_obj_to_db(
            obj=df_limit, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_limit)
        if os.path.exists(file_name_df_limit_temp):
            os.remove(path=file_name_df_limit_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Limit Count analysis takes [{str_gm}]")
    logger.trace(f"Limit Count End")
    return True


def worth_etf(frequency: str = "day") -> bool:
    name: str = f"df_worth_etf_{frequency}"
    logger.trace(f"{name} Begin")
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Worth etf Break End")
        return True
    if analysis.update_data.update_stock_data(frequency=frequency):
        logger.trace("{kline} Update finish")
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    file_name_dt_worth_etf = os.path.join(
        path_data, f"df_limit_etf_{str_dt_history_path}_{frequency}.ftr"
    )
    if os.path.exists(file_name_dt_worth_etf):
        logger.trace(f"{file_name_dt_worth_etf} is not exist.")
        df_worth_etf = feather.read_dataframe(source=file_name_dt_worth_etf)
    else:
        df_worth_etf = pd.DataFrame()
    list_etf = all_chs_etf()
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    dt_now = datetime.datetime.now()
    index_min = datetime.datetime(
        year=dt_trading_last_T0.year - 1, month=1, day=1, hour=15
    )
    trading_days = int((dt_now - index_min).days / 1.47)  # 1.47 ≈ 366 // 244
    int_len_list_etf = len(list_etf)
    i = 0
    for symbol in list_etf:
        i += 1
        str_msg = f"ETF Update: [{i:4d}/{int_len_list_etf:4d}] -- [{symbol}]"
        if symbol in df_worth_etf.columns:
            print(f"\r{str_msg} - Exist\033[K", end="")
            continue
        file_name_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_feather):
            df_delta_etf = feather.read_dataframe(source=file_name_feather)
        else:
            print(f"\r{str_msg} - Kline data is not exist\033[K")
            continue
        if df_delta_etf.shape[0] < trading_days:
            print(f"\r{str_msg} - less then 244\033[K", end="")
            continue
        df_delta_etf.sort_index(ascending=True, inplace=True)
        index_delta_etf_min = df_delta_etf.index.min()
        index_delta_etf_max = df_delta_etf.index.max()
        if index_delta_etf_min > index_min:
            index_min = index_delta_etf_min
        df_delta_temp = df_delta_etf[["close"]].copy()
        df_delta_temp.rename(columns={"close": symbol}, inplace=True)
        df_worth_etf = pd.concat(objs=[df_worth_etf, df_delta_temp], axis=1)
        df_worth_etf.sort_index(ascending=True, inplace=True)
        df_worth_etf = df_worth_etf.loc[index_min:]
        feather.write_dataframe(df=df_worth_etf, dest=file_name_dt_worth_etf)
        print(
            f"\r{str_msg} - [{index_min}] - [{index_delta_etf_max}] - Update\033[K",
            end="",
        )
    df_worth_etf.fillna(method="ffill", inplace=True)
    df_worth_etf.fillna(method="bfill", inplace=True)
    df_statistics_etf = pd.DataFrame(
        index=df_worth_etf.columns, columns=["max", "min", "diff"]
    )
    index_min = df_worth_etf.index.min()
    len_worth_etf_columns = df_worth_etf.shape[1]
    i = 0
    for column in df_worth_etf.columns:
        i += 1
        msg_bar = f"Calculate - [worth] - [{column}]"
        origin_worth = df_worth_etf.at[index_min, column]
        df_worth_etf[column] = round(df_worth_etf[column] / origin_worth, 4)
        df_statistics_etf.at[column, "max"] = df_worth_etf[column].max()
        df_statistics_etf.at[column, "min"] = df_worth_etf[column].min()
        print(f"\r{msg_bar} net value \033[K", end="")
    if i >= len_worth_etf_columns:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        df_statistics_etf["diff"] = df_statistics_etf["max"] - df_statistics_etf["min"]
        df_statistics_etf.sort_values(
            by=["min", "diff"], ascending=[False, False], inplace=True
        )
        analysis.base.write_obj_to_db(
            obj=df_worth_etf, key=name, filename=filename_chip_shelve
        )
        analysis.base.write_obj_to_db(
            obj=df_statistics_etf,
            key=f"df_statistics_etf_{frequency}",
            filename=filename_chip_shelve,
        )
        df_worth_etf.to_csv("df_worth_etf.csv")
        df_statistics_etf.to_csv("df_worth_etf_statistics.csv")
        dt_worth_etf = df_worth_etf.index.max()
        analysis.base.set_version(key=name, dt=dt_worth_etf)
        if os.path.exists(file_name_dt_worth_etf):
            os.remove(path=file_name_dt_worth_etf)
    logger.trace(f"Limit Count End")
    return True
