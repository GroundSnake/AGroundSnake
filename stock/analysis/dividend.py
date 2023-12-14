import datetime
import os
import random
import numpy as np
import pandas as pd
import feather
from loguru import logger
import analysis.base
from analysis.const import (
    dt_init,
    dt_history,
    path_temp,
    all_chs_code,
    dt_pm_end,
    client_ts_pro,
    dt_recent_fiscal_year,
)


def cash_dividend(debug: bool = False):
    name: str = "df_cash_div"
    logger.trace(f"{name} Begin")
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    file_name_df_cash_div = os.path.join(
        path_temp, f"df_cash_div_{str_dt_history_path}.ftr"
    )
    if debug:
        print(f"debug {name}")
    else:
        if analysis.base.is_latest_version(key=name):
            logger.trace("Limit Break End")
            return True
    list_symbol = all_chs_code()
    int_year_3y = dt_pm_end.year - 3
    set_cash_div_year = "None"
    list_columns = [
        "cash_div_tax",
        "cash_div_period",
        "cash_div_excepted_period",
        "cash_div_diff_period",
        "cash_div_end_dt",
        "cash_div_period_list",
    ]
    cash_div_excepted_period_init = -1
    dt_now = datetime.datetime.now()
    dt_cash_div_start_2y = datetime.datetime(
        year=dt_recent_fiscal_year - 1, month=1, day=1
    )
    dt_cash_div_start_1y = datetime.datetime(year=dt_recent_fiscal_year, month=1, day=1)
    dt_cash_div_start_y0 = datetime.datetime(
        year=dt_recent_fiscal_year + 1, month=1, day=1
    )
    int_days_2y = (dt_now - dt_cash_div_start_2y).days
    int_days_1y = (dt_now - dt_cash_div_start_1y).days
    int_days_y0 = (dt_now - dt_cash_div_start_y0).days
    if os.path.exists(file_name_df_cash_div):
        df_cash_div = feather.read_dataframe(source=file_name_df_cash_div)
    else:
        df_cash_div = analysis.base.stock_basic_v2()
        df_cash_div["list_date"] = df_cash_div["list_date"].apply(
            func=lambda x: (dt_now - x).days
        )
        df_cash_div = df_cash_div.reindex(
            columns=df_cash_div.columns.tolist() + list_columns
        )
        df_cash_div = df_cash_div.astype(
            dtype={
                "cash_div_end_dt": "datetime64[ns]",
                "cash_div_period_list": "object",
            }
        )
        df_cash_div["cash_div_end_dt"].fillna(value=dt_init, inplace=True)
        df_cash_div["cash_div_excepted_period"].fillna(
            value=cash_div_excepted_period_init, inplace=True
        )
        df_cash_div["cash_div_period_list"].fillna(
            value=set_cash_div_year, inplace=True
        )
        df_cash_div.fillna(value=0.0, inplace=True)
    df_cash_div = df_cash_div.sample(frac=1)
    df_cash_div = df_cash_div[df_cash_div.index.isin(values=list_symbol)].copy()
    i = 0
    count = len(df_cash_div)
    logger.trace(f"For loop Begin")
    for symbol in list_symbol:
        i += 1
        if random.randint(0, 10) == 5:
            feather.write_dataframe(df=df_cash_div, dest=file_name_df_cash_div)
        str_msg_bar = f"Dividend Update: [{i:4d}/{count:4d}] - [{symbol}]"
        if (
            df_cash_div.at[symbol, "cash_div_excepted_period"]
            > cash_div_excepted_period_init
        ):
            print(f"\r{str_msg_bar} - Exist\033[K", end="")
            continue
        if df_cash_div.at[symbol, "list_date"] > int_days_2y:
            df_cash_div.at[symbol, "cash_div_excepted_period"] = 3
        elif int_days_2y >= df_cash_div.at[symbol, "list_date"] > int_days_1y:
            df_cash_div.at[symbol, "cash_div_excepted_period"] = 2
        elif int_days_1y >= df_cash_div.at[symbol, "list_date"] > int_days_y0:
            df_cash_div.at[symbol, "cash_div_excepted_period"] = 1
        elif int_days_y0 >= df_cash_div.at[symbol, "list_date"] > 0:
            df_cash_div.at[symbol, "cash_div_excepted_period"] = 0
        ts_code = analysis.base.code_ths_to_ts(symbol)
        df_symbol = client_ts_pro.dividend(
            ts_code=ts_code,
            fields="ts_code,end_date,ann_date,div_proc,cash_div_tax,record_date",
        )
        if df_symbol.empty:
            print(f"\r{str_msg_bar} - No data\033[K")
            continue
        df_symbol = df_symbol[
            df_symbol["div_proc"].str.contains("实施").fillna(False)
            & (df_symbol["cash_div_tax"] > 0)
        ]
        df_symbol.drop_duplicates(keep="first", inplace=True)
        if df_symbol.empty:
            print(f"\r{str_msg_bar} - No dividend cash\033[K")
            continue
        df_symbol["end_date"] = pd.to_datetime(df_symbol["end_date"])
        df_symbol = df_symbol[df_symbol["end_date"].dt.year >= int_year_3y]
        if not df_symbol.empty:
            df_cash_div.at[symbol, "cash_div_end_dt"] = df_symbol["end_date"].max()
            list_cash_div_year = df_symbol["end_date"].dt.year.tolist()
            df_cash_div.at[symbol, "cash_div_period"] = int_period = len(
                set(list_cash_div_year)
            )
            df_cash_div.at[symbol, "cash_div_period_list"] = repr(list_cash_div_year)
            if (
                df_cash_div.at[symbol, "cash_div_excepted_period"]
                > df_cash_div.at[symbol, "cash_div_period"]
            ):
                int_period = df_cash_div.at[symbol, "cash_div_excepted_period"]
            df_cash_div.at[symbol, "cash_div_diff_period"] = (
                df_cash_div.at[symbol, "cash_div_period"]
                - df_cash_div.at[symbol, "cash_div_excepted_period"]
            )
            df_cash_div.at[symbol, "cash_div_tax"] = round(
                df_symbol["cash_div_tax"].sum() / 10 / int_period, 3
            )
        print(
            f"\r{str_msg_bar} - {df_cash_div.at[symbol, 'cash_div_tax']}\033[K", end=""
        )
    if i >= count:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        if debug:
            df_cash_div.to_csv("df_cash_div_debug.csv")
        else:
            df_cash_div = df_cash_div[list_columns]
            analysis.base.feather_to_file(
                df=df_cash_div,
                key=name,
            )
            analysis.base.set_version(key=name, dt=dt_pm_end)
            if os.path.exists(file_name_df_cash_div):
                os.remove(path=file_name_df_cash_div)
    logger.trace(f"Limit Count End")
    return True
