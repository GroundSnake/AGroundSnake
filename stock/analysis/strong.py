import datetime
import random
import pandas as pd
from console import fg
from analysis.api_tushare_const import daily_basic
from analysis.update_data import kline
from analysis.const_dynamic import (
    dt_init,
    dt_am_start,
    dt_am_0935,
    dt_pm_end,
    dt_pm_end_1t,
    path_temp,
    path_chip,
    path_chip_csv,
    path_check,
    format_dt,
)
from analysis.log import logger
from analysis.base import feather_from_file, feather_to_file, csv_to_file
from analysis.realtime_quotation import realtime_quotation


def strong_stocks(frequency: str = "D", days: int = 2) -> pd.DataFrame:
    name = f"strong_stocks_{frequency}_{days}"
    filename_strong_stocks_pool = path_chip.joinpath(
        f"df_strong_stocks_{frequency}_{days}_pool.ftr",
    )
    filename_strong_stocks_pool_csv = path_check.joinpath(
        f"df_strong_stocks_{frequency}_{days}_pool.csv"
    )
    filename_strong_stocks = path_chip.joinpath(
        f"df_strong_stocks_{frequency}_{days}.ftr",
    )
    filename_strong_stocks_temp = path_temp.joinpath(
        f"df_strong_stocks_{frequency}_{days}_temp.ftr"
    )
    dict_default = {
        "close_add": 0.0,
        "dt_add": dt_init,
        "buy": 0.0,
        "dt_buy": dt_init,
        "now": 0.0,
        "dt_now": dt_init,
        "pct_chg": 0.0,
    }
    dict_default_dtype = {
        "close_add": "float64",
        "dt_add": "datetime64[ns]",
        "buy": "float64",
        "dt_buy": "datetime64[ns]",
        "now": "float64",
        "dt_now": "datetime64[ns]",
        "pct_chg": "float64",
    }
    dt_now = datetime.datetime.now()
    df_strong_stocks_pool = feather_from_file(filename_df=filename_strong_stocks_pool)
    dt_end = dt_pm_end
    if dt_now < dt_pm_end:
        dt_end = dt_pm_end_1t
    if not df_strong_stocks_pool.empty:
        try:
            dt_temp = datetime.datetime.strptime(
                df_strong_stocks_pool.index.name,
                format_dt,
            )
        except TypeError:
            dt_temp = dt_init
        except ValueError:
            dt_temp = dt_init
        if dt_temp >= dt_end:
            logger.debug(f"feather from {filename_strong_stocks_pool}.")
            return df_strong_stocks_pool
    else:
        df_strong_stocks_pool = pd.DataFrame(columns=list(dict_default.keys()))
    df_strong_stocks_pool = df_strong_stocks_pool.reindex(columns=dict_default.keys())
    df_strong_stocks_pool = df_strong_stocks_pool.astype(dtype=dict_default_dtype)
    df_strong_stocks = feather_from_file(filename_df=filename_strong_stocks_temp)
    if df_strong_stocks.empty:
        dict_columns = {
            "strong": "Failed",
            "period": 0,
            "pct_chg_min": 0.0,
            "pct_chg_max": 0.0,
            "pct_chg_min_long": 0.0,
            "pct_chg_max_long": 0.0,
        }
        df_basic = daily_basic()
        df_strong_stocks = df_basic.reindex(columns=dict_columns.keys())
        df_strong_stocks.fillna(value=dict_columns, inplace=True)
        str_dt_init = dt_init.strftime(format_dt)
        df_strong_stocks.index.rename(name=str_dt_init, inplace=True)
        feather_to_file(df=df_strong_stocks, filename_df=filename_strong_stocks_temp)
    short = -days
    long = -(days + 3)
    dt_start = dt_pm_end - datetime.timedelta(days=30)
    df_strong_stocks = df_strong_stocks.sample(frac=1)
    count = df_strong_stocks.shape[0]
    i = 0
    for symbol in df_strong_stocks.index:
        if i % 5 == 1:
            feather_to_file(
                df=df_strong_stocks, filename_df=filename_strong_stocks_temp
            )
        i += 1
        str_msg = f"[{name}] - [{i:4d}/{count}] - [{symbol}] - [{dt_end}]"
        if df_strong_stocks.at[symbol, "period"] > 0:
            print(f"{str_msg} - Exist")
            continue
        df_kline = kline(
            symbol=symbol,
            frequency=frequency,
            adjust="qfq",
            asset="E",
            start_date=dt_start,
            end_date=dt_end,
        )
        if df_kline.empty:
            logger.error(f"{str_msg} - kline is empty")
            continue
        index_df_max = df_kline.index.max()
        if index_df_max < dt_end:
            print(f"{str_msg} - No latest")
            continue
        print(f"{str_msg} - Update")
        df_kline["pct_chg"] = round(
            (df_kline["close"] / df_kline["pre_close"] - 1) * 100, 2
        )
        df_kline_short = df_kline.iloc[short:]
        df_kline_long = df_kline.iloc[long:short]
        pct_chg_min = df_kline_short["pct_chg"].min()
        pct_chg_max = df_kline_short["pct_chg"].max()
        pct_chg_min_long = df_kline_long["pct_chg"].min()
        pct_chg_max_long = df_kline_long["pct_chg"].max()
        df_strong_stocks.at[symbol, "period"] = df_kline_short.shape[0]
        df_strong_stocks.at[symbol, "pct_chg_min"] = pct_chg_min
        df_strong_stocks.at[symbol, "pct_chg_max"] = pct_chg_max
        df_strong_stocks.at[symbol, "pct_chg_min_long"] = pct_chg_min_long
        df_strong_stocks.at[symbol, "pct_chg_max_long"] = pct_chg_max_long
        if pct_chg_min >= 9.9 and 3 >= pct_chg_max_long >= 0 >= pct_chg_min_long >= -3:
            df_strong_stocks.at[symbol, "strong"] = "Pass"
            index_max = df_kline.index.max()
            if symbol not in df_strong_stocks_pool.index:
                df_strong_stocks_pool.at[symbol, "close_add"] = (
                    df_strong_stocks_pool.at[symbol, "buy"]
                ) = df_strong_stocks_pool.at[symbol, "now"] = df_kline.at[
                    index_max, "close"
                ]
                df_strong_stocks_pool.at[symbol, "dt_add"] = df_strong_stocks_pool.at[
                    symbol, "dt_now"
                ] = dt_end
            else:
                df_strong_stocks_pool.at[symbol, "now"] = df_kline.at[
                    index_max, "close"
                ]
                df_strong_stocks_pool.at[symbol, "dt_now"] = dt_end
            df_strong_stocks_pool.at[symbol, "pct_chg"] = round(
                (
                    df_strong_stocks_pool.at[symbol, "now"]
                    / df_strong_stocks_pool.at[symbol, "close_add"]
                    - 1
                )
                * 100,
                2,
            )
    if i >= count:
        df_strong_stocks_pool.fillna(value=dict_default, inplace=True)
        df_strong_stocks_pool = df_strong_stocks_pool[
            (~df_strong_stocks_pool.index.str.contains("688"))
            & (~df_strong_stocks_pool.index.str.contains("bj"))
            & (~df_strong_stocks_pool.index.str.contains("sz3"))
        ]
        str_dt_end = dt_end.strftime(format_dt)
        df_strong_stocks.index.rename(name=str_dt_end, inplace=True)
        filename_strong_stocks_csv = path_chip_csv.joinpath(
            f"df_strong_stocks_{frequency}_{days}.csv"
        )
        csv_to_file(df=df_strong_stocks, filename_df=filename_strong_stocks_csv)
        feather_to_file(df=df_strong_stocks, filename_df=filename_strong_stocks)
        df_strong_stocks_pool.index.rename(name=str_dt_end, inplace=True)
        feather_to_file(
            df=df_strong_stocks_pool, filename_df=filename_strong_stocks_pool
        )
        csv_to_file(
            df=df_strong_stocks_pool, filename_df=filename_strong_stocks_pool_csv
        )
        logger.debug(f"feather to {filename_strong_stocks}.")
        filename_strong_stocks_temp.unlink(missing_ok=True)
    return df_strong_stocks_pool


def get_strong_stocks(frequency: str = "D", days: int = 1, trading: bool = True) -> str:
    str_return_not = "No strong stocks"
    space_line = " " * 43
    filename_strong_stocks_pool = path_chip.joinpath(
        f"df_strong_stocks_{frequency}_{days}_pool.ftr",
    )
    filename_strong_stocks_pool_csv = path_check.joinpath(
        f"df_strong_stocks_{frequency}_{days}_pool.csv"
    )
    if trading:
        df_strong_stocks_pool = feather_from_file(
            filename_df=filename_strong_stocks_pool
        )
    else:
        df_strong_stocks_pool = strong_stocks(frequency=frequency, days=days)
    if df_strong_stocks_pool.empty:
        logger.error("df_strong_stocks_pool empty")
        return str_return_not
    df_realtime = realtime_quotation.get_stocks_a(
        symbols=df_strong_stocks_pool.index.tolist()
    )
    if df_realtime.empty:
        logger.error("df_realtime empty")
        return str_return_not
    dt_now_realtime = datetime.datetime.strptime(df_realtime.index.name, format_dt)
    for symbol in df_strong_stocks_pool.index:
        if symbol in df_realtime.index:
            if (
                dt_am_start <= dt_now_realtime <= dt_am_0935
                and df_strong_stocks_pool.at[symbol, "dt_buy"] == dt_init
                and random.randint(a=0, b=2) == 1
            ):
                df_strong_stocks_pool.at[symbol, "dt_buy"] = dt_now_realtime
                df_strong_stocks_pool.at[symbol, "buy"] = df_realtime.at[
                    symbol, "close"
                ]
            df_strong_stocks_pool.at[symbol, "now"] = now = df_realtime.at[
                symbol, "close"
            ]
            df_strong_stocks_pool.at[symbol, "dt_now"] = dt_now_realtime
            buy = df_strong_stocks_pool.at[symbol, "buy"]
            if buy == 0:
                df_strong_stocks_pool.at[symbol, "pct_chg"] = 0
            else:
                df_strong_stocks_pool.at[symbol, "pct_chg"] = round(
                    (now / buy - 1) * 100,
                    2,
                )
    df_strong_stocks_pool_t = df_strong_stocks_pool[
        df_strong_stocks_pool["dt_add"] >= dt_pm_end_1t
    ]
    if df_strong_stocks_pool_t.empty:
        logger.error("df_strong_stocks_pool_t empty")
        return str_return_not
    df_realtime = df_realtime[
        df_realtime.index.isin(values=df_strong_stocks_pool_t.index.tolist())
    ]
    if df_realtime.empty:
        logger.error("df_realtime empty")
        return str_return_not
    dt_now_realtime = datetime.datetime.strptime(df_realtime.index.name, format_dt)
    dt_now_realtime_time = dt_now_realtime.time()
    df_realtime.sort_values(by=["pct_chg"], ascending=False, inplace=True)
    i = 0
    line_len = 3
    str_return = ""
    for symbol in df_realtime.index:
        i += 1
        pct_chg = df_realtime.at[symbol, "pct_chg"]
        str_symbol = f"[{df_realtime.at[symbol, 'name']:>4}({symbol}) {pct_chg:5.2f}%]"
        if pct_chg > 7:
            str_symbol = fg.purple(str_symbol)
        elif pct_chg > 5:
            str_symbol = fg.red(str_symbol)
        if str_return == "":
            str_return = f"{str_symbol}"
        elif i % line_len == 1:
            str_return += f"\n\r{str_symbol}"
        else:
            str_return += f", {str_symbol}"
    feather_to_file(df=df_strong_stocks_pool, filename_df=filename_strong_stocks_pool)
    csv_to_file(df=df_strong_stocks_pool, filename_df=filename_strong_stocks_pool_csv)
    str_return = f"{space_line}<{dt_now_realtime_time}>\n" + str_return
    return str_return
