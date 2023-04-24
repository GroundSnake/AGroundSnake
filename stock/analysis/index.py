import os
import sys
import datetime
import time
import feather
import pandas as pd
from loguru import logger
import tushare as ts
import analysis.base
from analysis.const import (
    dt_init,
    path_data,
    time_pm_end,
    str_date_path,
    filename_chip_shelve,
    dt_date_trading,
)


def update_index_data() -> bool:
    name: str = f"df_index_data"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    pro = ts.pro_api()
    dt_now = datetime.datetime.now()
    dt_year = dt_now.year
    dt_origin = datetime.datetime(year=dt_year, month=1, day=1)
    str_dt_origin = dt_origin.strftime("%Y%m%d")
    # str_dt_now = dt_now.strftime("%Y%m%d")
    filename_df_index_data = os.path.join(
        path_data, f"df_index_data_temp_{str_date_path}.ftr"
    )
    if os.path.exists(filename_df_index_data):
        df_index_data = feather.read_dataframe(source=filename_df_index_data)
        logger.trace(f"load {name} from [{filename_df_index_data}]")
    else:
        df_index_data = pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,list_date",
        )
        df_index_data['symbol'] = df_index_data['ts_code'].apply(func=analysis.base.code_ts_to_ths)
        df_index_data.set_index(keys=["symbol"], inplace=True)
        df_index_data["list_date"] = pd.to_datetime(df_index_data["list_date"])
        df_index_data.sort_values(by=["list_date"], ascending=True, inplace=True)
        df_index_data["base_mv_date"] = dt_init
        logger.trace(f"load {name} from [API]")
    i = 0
    count = len(df_index_data)
    df_index_data = df_index_data.sample(frac=1)
    for symbol in df_index_data.index:
        i += 1
        srr_msg_bar = f"[{i}/{count}] - [{symbol}]"
        if df_index_data.at[symbol, "base_mv_date"] == dt_init:
            i_times = 0
            while True:
                i_times += 1
                df_daily_basic_symbol = pro.daily_basic(
                    ts_code=df_index_data.at[symbol, 'ts_code'],
                    start_date=str_dt_origin,
                    fields="trade_date,close,total_share,total_mv",
                )
                if not df_daily_basic_symbol.empty:
                    df_daily_basic_symbol["trade_date"] = pd.to_datetime(
                        df_daily_basic_symbol["trade_date"]
                    )
                    df_daily_basic_symbol.sort_values(
                        by=["trade_date"],
                        ascending=True,
                        inplace=True,
                        ignore_index=True,
                    )
                    count_daily_basic_symbol = len(df_daily_basic_symbol)
                    if count_daily_basic_symbol >= 2:
                        max_index = count_daily_basic_symbol - 1
                        max2_index = count_daily_basic_symbol - 2
                    else:
                        max_index = max2_index = 0
                    min_index = 0
                    total_share = df_daily_basic_symbol.at[max_index, "total_share"]
                    df_index_data.at[
                        symbol, "base_mv_date"
                    ] = df_daily_basic_symbol.at[min_index, "trade_date"]
                    """base_mv --- 吸收合并及增减股本，修证初始市值"""
                    df_index_data.at[symbol, "base_mv"] = (
                        df_daily_basic_symbol.at[min_index, "close"]
                        * total_share
                    )
                    df_index_data.at[
                        symbol, "now_mv_date"
                    ] = now_mv_date = df_daily_basic_symbol.at[max_index, "trade_date"]
                    df_index_data.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                        max_index, "total_mv"
                    ]
                    df_index_data.at[symbol, "now_mv_date"] = df_daily_basic_symbol.at[
                        max_index, "trade_date"
                    ]
                    df_index_data.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                        max_index, "total_mv"
                    ]
                    df_index_data.at[symbol, "t1_mv_date"] = df_daily_basic_symbol.at[
                        max2_index, "trade_date"
                    ]
                    df_index_data.at[symbol, "t1_mv"] = (
                        df_daily_basic_symbol.at[max2_index, "close"]
                        * total_share
                    )
                    srr_msg_bar += f" - [{now_mv_date.date()}]"
                    break
                else:
                    srr_msg_bar += "- Error(No data)"
                    time.sleep(2)
                    if i_times > 2:
                        df_index_data.drop(index=symbol, inplace=True)
                        srr_msg_bar += "- break"
                        break
        else:
            srr_msg_bar += " - latest"
        feather.write_dataframe(df=df_index_data, dest=filename_df_index_data)
        print(f'\r{srr_msg_bar}\033[K', end="")
    if i >= count:
        print("\n", end="")  # 格式处理
        base_mv_total = df_index_data["base_mv"].sum()
        df_index_data['contribution_points'] = (
                (df_index_data['now_mv'] - df_index_data['base_mv'])
                / base_mv_total
                * 1000
        )
        df_index_data['t1_contribution_points'] =(
                (df_index_data['t1_mv'] - df_index_data['base_mv'])
                / base_mv_total
                * 1000
        )
        df_index_data['now_t1_contribution_points'] = (
                df_index_data['contribution_points']
                - df_index_data['t1_contribution_points']
        )
        df_index_data.sort_values(by=['now_t1_contribution_points'], ascending=False, inplace=True)
        df_index_data.drop(columns='ts_code', inplace=True)
        analysis.base.write_obj_to_db(
            obj=df_index_data,
            key=name,
            filename=filename_chip_shelve,
        )
        dt_index_data = datetime.datetime.combine(
            df_index_data["now_mv_date"].max().date(), time_pm_end
        )
        analysis.base.set_version(key=name, dt=dt_index_data)
        if os.path.exists(filename_df_index_data):  # 删除临时文件
            os.remove(path=filename_df_index_data)
            logger.trace(f"[{filename_df_index_data}] remove")
    return True


def make_ssb_index() ->bool:
    name: str = "df_ssb_index"
    name_data:str = "df_index_data"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    if analysis.base.is_latest_version(key=name_data, filename=filename_chip_shelve):
        logger.trace(f'{name_data} is latest')
        df_index_data = analysis.base.read_obj_from_db(
            key=name_data, filename=filename_chip_shelve
        )
    else:
        logger.trace(f'{name_data} is not latest and update')
        if update_index_data():
            df_index_data = analysis.base.read_obj_from_db(
                key=name_data, filename=filename_chip_shelve
            )
        else:
            df_index_data = pd.DataFrame()
    if df_index_data.empty:
        print('df_index_data is empty')
        logger.trace('df_index_data is empty')
        sys.exit()
    else:
        df_index_data.sort_values(by=['base_mv'],ascending=False, inplace=True)
    df_ssb_index = analysis.base.read_obj_from_db(
        key=name, filename=filename_chip_shelve
    )

    base_mv_all = df_index_data["base_mv"].sum()
    now_mv_all = df_index_data["now_mv"].sum()
    df_ssb_index.at[dt_date_trading, "base_mv_all"] = base_mv_all
    df_ssb_index.at[dt_date_trading, "now_mv_all"] = now_mv_all

    stocks_index_all = now_mv_all / base_mv_all * 1000
    stocks_index_all = round(stocks_index_all, 2)
    df_ssb_index.at[dt_date_trading, "stocks_all_index"] = stocks_index_all

    df_index_data_50 = df_index_data.iloc[:50]
    df_index_data_300 = df_index_data.iloc[:300]
    df_index_data_500 = df_index_data.iloc[300:800]
    df_index_data_1000 = df_index_data.iloc[800:1800]
    df_index_data_2000 = df_index_data.iloc[1800:3800]
    df_index_data_tail = df_index_data.iloc[3800:]

    base_mv_50 = df_index_data_50["base_mv"].sum()
    now_mv_50 = df_index_data_50["now_mv"].sum()
    stocks_index_50 = now_mv_50 / base_mv_50 * 1000
    stocks_index_50 = round(stocks_index_50, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_50"] = stocks_index_50

    base_mv_300 = df_index_data_300["base_mv"].sum()
    now_mv_300 = df_index_data_300["now_mv"].sum()
    stocks_index_300 = now_mv_300 / base_mv_300 * 1000
    stocks_index_300 = round(stocks_index_300, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_300"] = stocks_index_300

    base_mv_500 = df_index_data_500["base_mv"].sum()
    now_mv_500 = df_index_data_500["now_mv"].sum()
    stocks_index_500 = now_mv_500 / base_mv_500 * 1000
    stocks_index_500 = round(stocks_index_500, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_500"] = stocks_index_500

    base_mv_1000 = df_index_data_1000["base_mv"].sum()
    now_mv_1000 = df_index_data_1000["now_mv"].sum()
    stocks_index_1000 = now_mv_1000 / base_mv_1000 * 1000
    stocks_index_1000 = round(stocks_index_1000, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_1000"] = stocks_index_1000

    base_mv_2000 = df_index_data_2000["base_mv"].sum()
    now_mv_2000 = df_index_data_2000["now_mv"].sum()
    stocks_index_2000 = now_mv_2000 / base_mv_2000 * 1000
    stocks_index_2000 = round(stocks_index_2000, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_2000"] = stocks_index_2000

    base_mv_tail = df_index_data_tail["base_mv"].sum()
    now_mv_tail = df_index_data_tail["now_mv"].sum()
    stocks_index_tail = now_mv_tail / base_mv_tail * 1000
    stocks_index_tail = round(stocks_index_tail, 2)
    df_ssb_index.at[dt_date_trading, "stocks_index_tail"] = stocks_index_tail

    analysis.base.write_obj_to_db(
        obj=df_ssb_index,
        key=name,
        filename=filename_chip_shelve,
    )
    dt_ssb_index = datetime.datetime.combine(
            dt_date_trading, time_pm_end
    )
    analysis.base.set_version(key=name, dt=dt_ssb_index)
    print(df_ssb_index)
    df_ssb_index.to_csv("b.csv")
    df_index_data_50.to_csv("df_index_data_50.csv")
    df_index_data_300.to_csv("df_index_data_300.csv")
    df_index_data_500.to_csv("df_index_data_500.csv")
    df_index_data_1000.to_csv("df_index_data_1000.csv")
    df_index_data_2000.to_csv("df_index_data_2000.csv")
    df_index_data_tail.to_csv("df_index_data_tail.csv")

    logger.trace(f'{name} End')
    return True
