import datetime
import random
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    path_chip,
    path_temp,
    path_chip_csv,
    format_dt,
    df_daily_basic,
)
from analysis.log import logger
from analysis.base import feather_from_file, feather_to_file, get_latest_friday
from analysis.api_tushare import pledge_stat


def get_pledge() -> pd.DataFrame:
    name = "Pledge"
    filename_pledge_state = path_chip.joinpath("df_pledge.ftr")
    df_pledge = feather_from_file(filename_df=filename_pledge_state)
    dt_complete = get_latest_friday()
    if not df_pledge.empty:
        dt_now = datetime.datetime.now()
        if dt_balance <= dt_now <= dt_pm_end:
            logger.debug(f"feather from {filename_pledge_state}.--trading time")
            return df_pledge
        try:
            dt_stale = datetime.datetime.strptime(
                df_pledge.index.name,
                format_dt,
            )
        except TypeError:
            dt_stale = dt_init
        if dt_stale >= dt_complete:
            logger.debug(f"feather from {filename_pledge_state}.")
            return df_pledge
    filename_pledge_state_temp = path_temp.joinpath("df_pledge_temp.ftr")
    if filename_pledge_state_temp.exists():
        df_pledge = feather_from_file(filename_df=filename_pledge_state_temp)
    if df_pledge.empty:
        df_pledge = df_daily_basic.copy()
        df_pledge = df_pledge.reindex(
            columns=[
                "pledge_stat_dt_end",
                "pledge_stat_ratio",
            ]
        )
        df_pledge["pledge_stat_dt_end"] = dt_init
        df_pledge["pledge_stat_ratio"] = 0.0
    else:
        df_pledge = df_pledge.reindex(index=df_daily_basic.index)
        df_pledge["pledge_stat_dt_end"] = df_pledge["pledge_stat_dt_end"].fillna(
            value=dt_init
        )
        df_pledge["pledge_stat_ratio"] = df_pledge["pledge_stat_ratio"].fillna(
            value=0.0
        )
    df_pledge = df_pledge.sample(frac=1)
    df_pledge.sort_values(by=["pledge_stat_dt_end"], ascending=False, inplace=True)
    i = 0
    count = df_pledge.shape[0]
    for symbol in df_pledge.index:
        i += 1
        str_msg = f"[{name}] - [{i:4d}/{count}] - [{symbol}]"
        if df_pledge.at[symbol, "pledge_stat_dt_end"] >= dt_complete:
            print(f"{str_msg} - Exist.")
            continue
        if df_pledge.at[symbol, "pledge_stat_ratio"] < 0:
            print(f"{str_msg} - pledge_stat_ratio is zero.")
            continue
        ser_pledge_stat = pledge_stat(symbol=symbol)
        if ser_pledge_stat.empty:
            df_pledge.at[symbol, "pledge_stat_ratio"] = -1
            print(f"{str_msg} - No data")
            continue
        df_pledge.at[symbol, "pledge_stat_dt_end"] = pledge_stat_dt_end = (
            ser_pledge_stat.name
        )
        df_pledge.at[symbol, "pledge_stat_ratio"] = ser_pledge_stat["pledge_ratio"]
        if random.randint(a=0, b=9) == 5:
            feather_to_file(df=df_pledge, filename_df=filename_pledge_state_temp)
        print(f"{str_msg} - [{pledge_stat_dt_end}] - Update.")
    if i >= count:
        df_pledge["pledge_stat_ratio"] = df_pledge["pledge_stat_ratio"].replace(
            to_replace=-1, value=0
        )
        df_pledge_exist = df_pledge[df_pledge["pledge_stat_ratio"] > 0]
        rows_exist = df_pledge_exist.shape[0]
        if rows_exist > 2000:
            dt_max = df_pledge["pledge_stat_dt_end"].max()
        else:
            dt_max = dt_init
        str_dt_max = dt_max.strftime(format_dt)
        df_pledge.index.rename(name=str_dt_max, inplace=True)
        filename_financial_indicator_csv = path_chip_csv.joinpath("df_pledge.csv")
        df_pledge.to_csv(path_or_buf=filename_financial_indicator_csv)
        feather_to_file(df=df_pledge, filename_df=filename_pledge_state)
        logger.debug(f"feather to {filename_pledge_state}.")
        filename_pledge_state_temp.unlink(missing_ok=True)
    return df_pledge
