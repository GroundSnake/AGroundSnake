import os
import time
import datetime
import random
import requests
import numpy as np
import pandas as pd
import feather
from loguru import logger
import analysis.capital
import analysis.base
from analysis.const import (
    client_ts_pro,
    filename_chip_shelve,
    dt_trading_last_T0,
    path_data,
    dt_history,
    dt_init,
)


def top10_float_holders():
    name: str = "df_top10_float_holders"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name} Break and End")
        return True
    dt_period_year = dt_trading_last_T0.year
    dt_date_trading_month = dt_trading_last_T0.month
    if dt_date_trading_month in [1, 2, 3, 4]:  # 预估上年的年报
        dt_period_year -= 1
        dt_period = datetime.datetime(year=dt_period_year, month=9, day=1)
    elif dt_date_trading_month in [5, 6, 7, 8]:  # 预估本年的半年报
        dt_period = datetime.datetime(year=dt_period_year, month=3, day=1)
    elif dt_date_trading_month in [9, 10]:  # 预估本年的3季度报
        dt_period = datetime.datetime(year=dt_period_year, month=6, day=1)
    elif dt_date_trading_month in [11, 12]:  # 预估本年的年报
        dt_period = datetime.datetime(year=dt_period_year, month=9, day=1)
    else:
        logger.error(f"dt_now_month Error.")
        return False
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    filename_df_top10_float_holders_temp = os.path.join(
        path_data, f"df_top10_float_holders_temp_{str_dt_history_path}.ftr"
    )
    list_columns_basic = [
        "hold_float_ratio_0Q",
        "hold_float_ratio_1Q",
        "hold_float_ratio_2Q",
        "hold_float_ratio_3Q",
        "hold_ratio_0Q",
        "hold_ratio_1Q",
        "hold_ratio_2Q",
        "hold_ratio_3Q",
        "hold_amount_E_0Q",
        "hold_amount_E_1Q",
        "hold_amount_E_2Q",
        "hold_amount_E_3Q",
        "hold_change_ratio_0Q",
        "hold_change_ratio_1Q",
        "hold_change_ratio_2Q",
        "hold_change_ratio_3Q",
        "hold_date_0Q",
        "hold_date_1Q",
        "hold_date_2Q",
        "hold_date_3Q",
    ]
    if os.path.exists(filename_df_top10_float_holders_temp):
        df_top10_float_holders = feather.read_dataframe(
            source=filename_df_top10_float_holders_temp
        )
    else:
        if analysis.capital.capital():
            df_top10_float_holders = analysis.base.read_df_from_db(
                key="df_cap", filename=filename_chip_shelve
            )
            logger.trace("load df_cap success")
        else:
            df_top10_float_holders = pd.DataFrame()
            logger.error("load df_cap fail")
        list_columns = df_top10_float_holders.columns.tolist() + list_columns_basic
        df_top10_float_holders = df_top10_float_holders.reindex(columns=list_columns)
        df_top10_float_holders["hold_date_0Q"].fillna(value=dt_init, inplace=True)
        df_top10_float_holders["hold_date_1Q"].fillna(value=dt_init, inplace=True)
        df_top10_float_holders["hold_date_2Q"].fillna(value=dt_init, inplace=True)
        df_top10_float_holders["hold_date_3Q"].fillna(value=dt_init, inplace=True)
        df_top10_float_holders.fillna(value=0.0, inplace=True)
        feather.write_dataframe(
            df=df_top10_float_holders, dest=filename_df_top10_float_holders_temp
        )
    if df_top10_float_holders.empty:
        logger.error("df_golden empty")
        return False
    df_top10_float_holders = df_top10_float_holders.sample(frac=1)
    all_record = df_top10_float_holders.shape[0]
    i = 0
    for symbol in df_top10_float_holders.index:
        i += 1
        str_msg_bar = f"{name}:[{i:4d}/{all_record:4d}] -- [{symbol}]"
        if df_top10_float_holders.at[symbol, "hold_date_0Q"] > dt_period:
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        ts_code = analysis.base.code_ths_to_ts(symbol)
        i_while_holders = 0
        df_symbol_holders = pd.DataFrame()
        while i_while_holders <= 1:
            i_while_holders += 1
            try:
                df_symbol_holders = client_ts_pro.top10_floatholders(
                    ts_code=ts_code,
                )
                time.sleep(0.01)  # 间隔10毫秒
            except requests.exceptions.Timeout as e:
                logger.error(f"\r{str_msg_bar} - [{i_while_holders}] - {repr(e)}\033[K")
                time.sleep(5)
            else:
                if df_symbol_holders.empty:
                    print(
                        f"\r{str_msg_bar} - [Times:{i_while_holders}] - df_symbol_holders empty\033[K"
                    )
                    time.sleep(1)
                else:
                    break
        if df_symbol_holders.empty:
            print(f"\r{str_msg_bar} - No data\033[K")
            continue
        df_pivot = pd.pivot_table(
            df_symbol_holders,
            index=["end_date"],
            aggfunc={
                "hold_amount": np.sum,
                "hold_change": np.sum,
                "end_date": pd.value_counts,
            },
        )
        df_pivot.rename(columns={"end_date": "counts"}, inplace=True)
        df_pivot.index = pd.to_datetime(df_pivot.index)
        dt_max = df_pivot.index.max()
        df_pivot.sort_index(ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        len_df_pivot = df_pivot.shape[0]
        circ_cap = df_top10_float_holders.at[symbol, "circ_cap"]
        total_cap = df_top10_float_holders.at[symbol, "total_cap"]
        if len_df_pivot > 4:
            len_df_pivot = 4
        for i_pivot in df_pivot.index:
            if i_pivot >= len_df_pivot:
                break
            hold_amount = df_pivot.at[i_pivot, "hold_amount"]
            df_top10_float_holders.at[symbol, f"hold_date_{i_pivot}Q"] = df_pivot.at[
                i_pivot, "end_date"
            ]
            df_top10_float_holders.at[symbol, f"hold_amount_E_{i_pivot}Q"] = float(
                hold_amount / 100000000
            ).__round__(2)
            if circ_cap > 0:
                df_top10_float_holders.at[
                    symbol, f"hold_change_ratio_{i_pivot}Q"
                ] = float(
                    df_pivot.at[i_pivot, "hold_change"] / circ_cap * 100
                ).__round__(
                    2
                )
                df_top10_float_holders.at[
                    symbol, f"hold_float_ratio_{i_pivot}Q"
                ] = float(hold_amount / circ_cap * 100).__round__(2)
            if total_cap > 0:
                df_top10_float_holders.at[symbol, f"hold_ratio_{i_pivot}Q"] = float(
                    hold_amount / total_cap * 100
                ).__round__(2)
        if random.randint(a=0, b=9) == 5:
            feather.write_dataframe(
                df=df_top10_float_holders, dest=filename_df_top10_float_holders_temp
            )
        print(f"\r{str_msg_bar} - Update - [{dt_max}].\033[K")
    df_top10_float_holders = df_top10_float_holders[[list_columns_basic]].copy()
    df_top10_float_holders.sort_values(by="hold_date_0Q", ascending=False, inplace=True)
    print(df_top10_float_holders)
    df_top10_float_holders.to_csv(f"df_top10_float_holders{str_dt_history_path}.csv")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"{name} analysis takes [{str_gm}]")
    logger.trace(f"{name} End")
