# modified at 2023/5/2 16:03
import os
import sys
import datetime
import time
import feather
import requests
import pandas as pd
from pyecharts.charts import Line
import pyecharts.options as opts
from loguru import logger
import tushare as ts
import analysis.base
from analysis.const import (
    dt_init,
    dt_trading,
    path_data,
    time_pm_end,
    filename_chip_shelve,
    list_all_stocks,
    filename_index_charts,
)


def update_index_data(dt: datetime.datetime = None, dt_origin_year: int = 2023) -> bool:
    """
    :param dt: 指数的日期
    :param dt_origin_year: 指数起点，默认2023年1月1日
    :return: 成功返回Ture，出错返回False
    """
    name: str = f"df_index_data"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    pro = ts.pro_api()
    if dt is None:
        dt_pos = dt_trading
    else:
        dt_pos = dt
    dt_origin = datetime.datetime(year=dt_origin_year, month=1, day=1)
    str_dt_origin = dt_origin.strftime("%Y%m%d")
    str_dt_now = dt_pos.strftime("%Y%m%d")
    str_date_path = dt_pos.strftime("%Y_%m_%d")
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
        df_index_data["symbol"] = df_index_data["ts_code"].apply(
            func=analysis.base.code_ts_to_ths
        )
        df_index_data.set_index(keys=["symbol"], inplace=True)
        df_index_data["list_date"] = pd.to_datetime(df_index_data["list_date"])
        df_index_data.sort_values(by=["list_date"], ascending=True, inplace=True)
        list_index_columns = df_index_data.columns.tolist() + [
            "base_mv",
            "now_mv",
            "t1_mv",
            "contribution_points",
            "t1_contribution_points",
            "now_t1_contribution_points",
            "base_mv_date",
            "now_mv_date",
            "t1_mv_date",
        ]
        df_index_data = df_index_data.reindex(columns=list_index_columns)
        df_index_data["base_mv_date"] = dt_init
        logger.trace(f"load {name} from [API]")
    i = 0
    count = len(df_index_data)
    df_index_data = df_index_data.sample(frac=1)
    for symbol in df_index_data.index:
        i += 1
        srr_msg_bar = f"{name}: [{i:04d}/{count}] - [{symbol}]"
        if df_index_data.at[symbol, "base_mv_date"] == dt_init:
            i_times = 0
            while True:
                i_times += 1
                try:
                    df_daily_basic_symbol = pro.daily_basic(
                        ts_code=df_index_data.at[symbol, "ts_code"],
                        start_date=str_dt_origin,
                        end_date=str_dt_now,
                        fields="trade_date,close,total_share,total_mv",
                    )
                except requests.exceptions.ConnectionError as e:
                    print(f"\r{srr_msg_bar} - {repr(e)}\033[K")
                    logger.trace(repr(e))
                    time.sleep(2)
                    df_daily_basic_symbol = pd.DataFrame()
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
                    df_index_data.at[symbol, "base_mv_date"] = df_daily_basic_symbol.at[
                        min_index, "trade_date"
                    ]
                    """base_mv --- 吸收合并及增减股本，修证初始市值"""
                    if (
                        df_daily_basic_symbol.at[min_index, "total_share"]
                        == total_share
                    ):
                        df_index_data.at[symbol, "base_mv"] = df_daily_basic_symbol.at[
                            min_index, "total_mv"
                        ]
                        df_index_data.at[
                            symbol, "base_mv_date"
                        ] = df_daily_basic_symbol.at[min_index, "trade_date"]
                        df_index_data.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                            max_index, "total_mv"
                        ]
                        df_index_data.at[
                            symbol, "now_mv_date"
                        ] = now_mv_date = df_daily_basic_symbol.at[
                            max_index, "trade_date"
                        ]
                        srr_msg_bar += f" - [{now_mv_date.date()}]"
                        df_index_data.at[symbol, "t1_mv"] = df_daily_basic_symbol.at[
                            max2_index, "total_mv"
                        ]
                        df_index_data.at[
                            symbol, "t1_mv_date"
                        ] = df_daily_basic_symbol.at[max2_index, "trade_date"]
                    else:
                        i_pro_bar = 0
                        while True:
                            i_pro_bar += 1
                            try:
                                df_pro_bar_symbol = ts.pro_bar(
                                    ts_code=df_index_data.at[symbol, "ts_code"],
                                    adj="qfq",
                                    start_date=str_dt_origin,
                                    end_date=str_dt_now,
                                )
                            except requests.exceptions.ConnectionError as e:
                                print(f"\r{srr_msg_bar} - {repr(e)}\033[K")
                                logger.trace(repr(e))
                                time.sleep(2)
                                df_pro_bar_symbol = pd.DataFrame()
                            if not df_pro_bar_symbol.empty:
                                df_pro_bar_symbol["trade_date"] = pd.to_datetime(
                                    df_pro_bar_symbol["trade_date"]
                                )
                                df_pro_bar_symbol.sort_values(
                                    by=["trade_date"],
                                    ascending=True,
                                    inplace=True,
                                    ignore_index=True,
                                )
                                count_pro_bar_symbol = len(df_pro_bar_symbol)
                                if count_pro_bar_symbol >= 2:
                                    max_index_close = count_pro_bar_symbol - 1
                                    max2_index_close = count_pro_bar_symbol - 2
                                else:
                                    max_index_close = max2_index_close = 0
                                min_index_close = 0
                                df_index_data.at[symbol, "base_mv"] = (
                                    df_pro_bar_symbol.at[min_index_close, "close"]
                                    * total_share
                                )
                                df_index_data.at[
                                    symbol, "base_mv_date"
                                ] = df_pro_bar_symbol.at[min_index_close, "trade_date"]
                                df_index_data.at[symbol, "now_mv"] = (
                                    df_pro_bar_symbol.at[max_index_close, "close"]
                                    * total_share
                                )
                                df_index_data.at[
                                    symbol, "now_mv_date"
                                ] = now_mv_date = df_pro_bar_symbol.at[
                                    max_index_close, "trade_date"
                                ]
                                srr_msg_bar += f" - [{now_mv_date.date()}]"
                                df_index_data.at[symbol, "t1_mv"] = (
                                    df_pro_bar_symbol.at[max2_index_close, "close"]
                                    * total_share
                                )
                                df_index_data.at[
                                    symbol, "t1_mv_date"
                                ] = df_pro_bar_symbol.at[max2_index_close, "trade_date"]
                                break
                            else:
                                print(f"\r{srr_msg_bar} - sleep[{i_pro_bar}]......\033[K", end="")
                                time.sleep(2)
                                if i_pro_bar > 2:
                                    break
                    break
                else:
                    print(f"\r{srr_msg_bar} - sleep[{i_times}]......\033[K", end="")
                    time.sleep(2)
                    if i_times > 2:
                        df_index_data.drop(index=symbol, inplace=True)
                        print(' - drop')
                        break
        else:
            srr_msg_bar += " - latest"
        feather.write_dataframe(df=df_index_data, dest=filename_df_index_data)
        print(f"\r{srr_msg_bar}\033[K", end="")
    if i >= count:
        print("\n", end="")  # 格式处理
        base_mv_total = df_index_data["base_mv"].sum()
        df_index_data["contribution_points"] = (
            (df_index_data["now_mv"] - df_index_data["base_mv"]) / base_mv_total * 1000
        )
        df_index_data["t1_contribution_points"] = (
            (df_index_data["t1_mv"] - df_index_data["base_mv"]) / base_mv_total * 1000
        )
        df_index_data["now_t1_contribution_points"] = (
            df_index_data["contribution_points"]
            - df_index_data["t1_contribution_points"]
        )
        df_index_data.sort_values(
            by=["now_t1_contribution_points"], ascending=False, inplace=True
        )
        df_index_data.drop(columns="ts_code", inplace=True)
        df_index_data.applymap(
            func=lambda x: round(x / 10000, 2)
            if (isinstance(x, (int, float)) and x > 100)
            else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
        )
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
    # df_index_data.to_csv("df_index_data.csv")
    return True


def make_ssb_index(dt: datetime.datetime = None) -> bool:
    name: str = "df_ssb_index"
    name_data: str = "df_index_data"
    if dt is None:
        dt_pos = dt_trading
    else:
        dt_pos = dt
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    if analysis.base.is_latest_version(key=name_data, filename=filename_chip_shelve):
        logger.trace(f"{name_data} is latest")
        df_index_data = analysis.base.read_df_from_db(
            key=name_data, filename=filename_chip_shelve
        )
    else:
        logger.trace(f"{name_data} is not latest and update")
        if update_index_data(dt = dt_pos):
            df_index_data = analysis.base.read_df_from_db(
                key=name_data, filename=filename_chip_shelve
            )
        else:
            df_index_data = pd.DataFrame()
    if df_index_data.empty:
        print("df_index_data is empty")
        logger.trace("df_index_data is empty")
        sys.exit()
    else:
        df_index_data.sort_values(by=["base_mv"], ascending=False, inplace=True)
    df_ssb_index = analysis.base.read_df_from_db(
        key=name, filename=filename_chip_shelve
    )
    if df_ssb_index.empty:
        df_ssb_index = df_ssb_index.reindex(
            columns=[
                "base_mv_all",
                "now_mv_all",
                "stocks_all_index",
                "stocks_index_50",
                "stocks_index_300",
                "stocks_index_500",
                "stocks_index_1000",
                "stocks_index_2000",
                "stocks_index_tail",
                "base_mv_50",
                "base_mv_300",
                "base_mv_500",
                "base_mv_1000",
                "base_mv_2000",
                "base_mv_tail",
                "now_mv_50",
                "now_mv_300",
                "now_mv_500",
                "now_mv_1000",
                "now_mv_2000",
                "now_mv_tail",
            ]
        )
        print("df_ssb_index is empty")
        logger.trace("df_ssb_index is empty")
    dt_date_index = dt_pos.date()
    base_mv_all = df_index_data["base_mv"].sum()
    now_mv_all = df_index_data["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_all"] = round(base_mv_all / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_all"] = round(now_mv_all / 100000000, 2)
    stocks_index_all = now_mv_all / base_mv_all * 1000
    stocks_index_all = round(stocks_index_all, 2)
    df_ssb_index.at[dt_date_index, "stocks_all_index"] = stocks_index_all
    df_index_data_50 = df_index_data.iloc[:50].copy()
    df_index_data_300 = df_index_data.iloc[:300].copy()
    df_index_data_500 = df_index_data.iloc[300:800].copy()
    df_index_data_1000 = df_index_data.iloc[800:1800].copy()
    df_index_data_2000 = df_index_data.iloc[1800:3800].copy()
    df_index_data_tail = df_index_data.iloc[3800:].copy()
    ########----50---########
    base_mv_50 = df_index_data_50["base_mv"].sum()
    now_mv_50 = df_index_data_50["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_50"] = round(base_mv_50 / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_50"] = round(now_mv_50 / 100000000, 2)
    stocks_index_50 = now_mv_50 / base_mv_50 * 1000
    stocks_index_50 = round(stocks_index_50, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_50"] = stocks_index_50
    df_index_data_50["contribution_points_index_50"] = (
        (df_index_data_50["now_mv"] - df_index_data_50["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_50["t1_contribution_points_index_50"] = (
        (df_index_data_50["t1_mv"] - df_index_data_50["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_50["now_t1_contribution_points_index_50"] = (
        df_index_data_50["contribution_points_index_50"]
        - df_index_data_50["t1_contribution_points_index_50"]
    )
    df_index_data_50.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_50 = df_index_data_50.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
    )
    ########----300---########
    base_mv_300 = df_index_data_300["base_mv"].sum()
    now_mv_300 = df_index_data_300["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_300"] = round(base_mv_300 / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_300"] = round(now_mv_300 / 100000000, 2)
    stocks_index_300 = now_mv_300 / base_mv_300 * 1000
    stocks_index_300 = round(stocks_index_300, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_300"] = stocks_index_300
    df_index_data_300["contribution_points_index_300"] = (
        (df_index_data_300["now_mv"] - df_index_data_300["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_300["t1_contribution_points_index_300"] = (
        (df_index_data_300["t1_mv"] - df_index_data_300["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_300["now_t1_contribution_points_index_300"] = (
        df_index_data_300["contribution_points_index_300"]
        - df_index_data_300["t1_contribution_points_index_300"]
    )
    df_index_data_300.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_300 = df_index_data_300.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
    )
    ########----500---########
    base_mv_500 = df_index_data_500["base_mv"].sum()
    now_mv_500 = df_index_data_500["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_500"] = round(base_mv_500 / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_500"] = round(now_mv_500 / 100000000, 2)
    stocks_index_500 = now_mv_500 / base_mv_500 * 1000
    stocks_index_500 = round(stocks_index_500, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_500"] = stocks_index_500
    df_index_data_500["contribution_points_index_500"] = (
        (df_index_data_500["now_mv"] - df_index_data_500["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_500["t1_contribution_points_index_500"] = (
        (df_index_data_500["t1_mv"] - df_index_data_500["base_mv"]) / base_mv_50 * 1000
    )
    df_index_data_500["now_t1_contribution_points_index_500"] = (
        df_index_data_500["contribution_points_index_500"]
        - df_index_data_500["t1_contribution_points_index_500"]
    )
    df_index_data_500.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_500 = df_index_data_500.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
    )
    ########----1000---########
    base_mv_1000 = df_index_data_1000["base_mv"].sum()
    now_mv_1000 = df_index_data_1000["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_1000"] = round(base_mv_1000 / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_1000"] = round(now_mv_1000 / 100000000, 2)
    stocks_index_1000 = now_mv_1000 / base_mv_1000 * 1000
    stocks_index_1000 = round(stocks_index_1000, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_1000"] = stocks_index_1000
    df_index_data_1000["contribution_points_index_1000"] = (
        (df_index_data_1000["now_mv"] - df_index_data_1000["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_1000["t1_contribution_points_index_1000"] = (
        (df_index_data_1000["t1_mv"] - df_index_data_1000["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_1000["now_t1_contribution_points_index_1000"] = (
        df_index_data_1000["contribution_points_index_1000"]
        - df_index_data_1000["t1_contribution_points_index_1000"]
    )
    df_index_data_1000.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_1000 = df_index_data_1000.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
    )
    ########----2000---########
    base_mv_2000 = df_index_data_2000["base_mv"].sum()
    now_mv_2000 = df_index_data_2000["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_2000"] = round(base_mv_2000 / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_2000"] = round(now_mv_2000 / 100000000, 2)
    stocks_index_2000 = now_mv_2000 / base_mv_2000 * 1000
    stocks_index_2000 = round(stocks_index_2000, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_2000"] = stocks_index_2000
    df_index_data_2000["contribution_points_index_2000"] = (
        (df_index_data_2000["now_mv"] - df_index_data_2000["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_2000["t1_contribution_points_index_2000"] = (
        (df_index_data_2000["t1_mv"] - df_index_data_2000["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_2000["now_t1_contribution_points_index_2000"] = (
        df_index_data_2000["contribution_points_index_2000"]
        - df_index_data_2000["t1_contribution_points_index_2000"]
    )
    df_index_data_2000.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_2000 = df_index_data_2000.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
    )
    ########----tail---########
    base_mv_tail = df_index_data_tail["base_mv"].sum()
    now_mv_tail = df_index_data_tail["now_mv"].sum()
    df_ssb_index.at[dt_date_index, "base_mv_tail"] = round(base_mv_tail / 100000000, 2)
    df_ssb_index.at[dt_date_index, "now_mv_tail"] = round(now_mv_tail / 100000000, 2)
    stocks_index_tail = now_mv_tail / base_mv_tail * 1000
    stocks_index_tail = round(stocks_index_tail, 2)
    df_ssb_index.at[dt_date_index, "stocks_index_tail"] = stocks_index_tail
    df_index_data_tail["contribution_points_index_tail"] = (
        (df_index_data_tail["now_mv"] - df_index_data_tail["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_tail["t1_contribution_points_index_tail"] = (
        (df_index_data_tail["t1_mv"] - df_index_data_tail["base_mv"])
        / base_mv_50
        * 1000
    )
    df_index_data_tail["now_t1_contribution_points_index_tail"] = (
        df_index_data_tail["contribution_points_index_tail"]
        - df_index_data_tail["t1_contribution_points_index_tail"]
    )
    df_index_data_tail.sort_values(
        by=["now_t1_contribution_points"], ascending=False, inplace=True
    )
    df_index_data_tail = df_index_data_tail.applymap(
        func=lambda x: round(x / 10000, 2)
        if (isinstance(x, (int, float)) and x > 100)
        else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x),
        na_action="ignore",
    )
    ###################################################################
    df_ssb_index.sort_index(ascending=True, inplace=True)
    x_axis = df_ssb_index.index.tolist()
    v_all = df_ssb_index["stocks_all_index"].tolist()
    v_50 = df_ssb_index["stocks_index_50"].tolist()
    v_300 = df_ssb_index["stocks_index_300"].tolist()
    v_500 = df_ssb_index["stocks_index_500"].tolist()
    v_1000 = df_ssb_index["stocks_index_1000"].tolist()
    v_2000 = df_ssb_index["stocks_index_2000"].tolist()
    v_tail = df_ssb_index["stocks_index_tail"].tolist()
    list_values = v_all + v_50 + v_300 + v_500 + v_1000 + v_2000 + v_tail
    y_min = min(list_values)
    y_max = max(list_values)
    line_index = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
            page_title='SSB index',
        )
    )
    line_index.add_xaxis(xaxis_data=x_axis)
    line_index.add_yaxis(
        series_name="index_all",
        y_axis=v_all,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index.add_yaxis(
        series_name="index_50",
        y_axis=v_50,
        is_symbol_show=False,
    )
    line_index.add_yaxis(
        series_name="index_300",
        y_axis=v_300,
        is_symbol_show=False,
    )
    line_index.add_yaxis(
        series_name="index_500",
        y_axis=v_500,
        is_symbol_show=False,
    )
    line_index.add_yaxis(
        series_name="index_1000",
        y_axis=v_1000,
        is_symbol_show=False,
    )
    line_index.add_yaxis(
        series_name="index_2000",
        y_axis=v_2000,
        is_symbol_show=False,
    )
    line_index.add_yaxis(
        series_name="index_tail",
        y_axis=v_tail,
        is_symbol_show=False,
    )
    line_index.set_global_opts(
        title_opts=opts.TitleOpts(title="SSB Index", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(
            orient='vertical',
            pos_right=0,
            pos_top=60),
        yaxis_opts=opts.AxisOpts(
            min_=y_min,
            max_=y_max,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,

        ),
    )
    line_index.set_colors(colors=['red', 'orange', 'yellow', 'green', 'blue', 'purple', 'black', 'pink', 'brown'])
    line_index.render(path=filename_index_charts)
    analysis.base.write_obj_to_db(
        obj=df_ssb_index,
        key=name,
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_50,
        key="df_index_data_50",
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_300,
        key="df_index_data_300",
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_500,
        key="df_index_data_500",
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_1000,
        key="df_index_data_1000",
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_2000,
        key="df_index_data_2000",
        filename=filename_chip_shelve,
    )
    analysis.base.write_obj_to_db(
        obj=df_index_data_tail,
        key="df_index_data_tail",
        filename=filename_chip_shelve,
    )
    dt_ssb_index = datetime.datetime.combine(dt_date_index, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_ssb_index)
    logger.trace(f"{name} End")
    # df_ssb_index.to_csv('df_ssb_index.csv')
    return True


def stocks_in_ssb(dt: datetime.datetime = None):
    name: str = "df_stocks_in_ssb"
    name_data: str = "df_ssb_index"
    if dt is None:
        dt_pos = dt_trading
    else:
        dt_pos = dt
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    if analysis.base.is_latest_version(key=name_data, filename=filename_chip_shelve):
        logger.trace(f"{name_data} is latest")
    else:
        logger.trace(f"{name_data} is not latest and update")
        if make_ssb_index(dt=dt_pos):
            logger.trace(f"{name_data} update and the version is the latest")
        else:
            logger.trace(f"make_ssb_index return False,program exit")
            sys.exit()
    df_index_data_50 = analysis.base.read_df_from_db(
        key="df_index_data_50", filename=filename_chip_shelve
    )
    df_index_data_300 = analysis.base.read_df_from_db(
        key="df_index_data_300", filename=filename_chip_shelve
    )
    df_index_data_500 = analysis.base.read_df_from_db(
        key="df_index_data_500", filename=filename_chip_shelve
    )
    df_index_data_1000 = analysis.base.read_df_from_db(
        key="df_index_data_1000", filename=filename_chip_shelve
    )
    df_index_data_2000 = analysis.base.read_df_from_db(
        key="df_index_data_2000", filename=filename_chip_shelve
    )
    df_index_data_tail = analysis.base.read_df_from_db(
        key="df_index_data_tail", filename=filename_chip_shelve
    )
    df_stocks_in_ssb = pd.DataFrame(index=list_all_stocks, columns=["ssb_index"])
    for symbol in df_stocks_in_ssb.index:
        if symbol in df_index_data_300.index:
            if symbol in df_index_data_50.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_50"
            else:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_300"
        elif symbol in df_index_data_500.index:
            df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_500"
        elif symbol in df_index_data_1000.index:
            df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_1000"
        elif symbol in df_index_data_2000.index:
            df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_2000"
        elif symbol in df_index_data_tail.index:
            df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_tail"
        else:
            df_stocks_in_ssb.at[symbol, "ssb_index"] = "NA"
        print(f"\r{name}:[{symbol}]\033[K", end="")
    analysis.base.write_obj_to_db(
        obj=df_stocks_in_ssb,
        key="df_stocks_in_ssb",
        filename=filename_chip_shelve,
    )
    dt_date_stocks_in_ssb = df_index_data_2000["now_mv_date"].max().date()
    dt_stocks_in_ssb = datetime.datetime.combine(dt_date_stocks_in_ssb, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_stocks_in_ssb)
    return True


"""
    dt_origin = datetime.datetime(year=2023, month=1, day=1)
    pro = ts.pro_api()
    dt_now = datetime.datetime.now()
    df_trader_day = pro.trade_cal(exchange='', start_date='20230101', end_date='20231231')
    df_trader_day.sort_values(by=["cal_date"], ascending=True, inplace=True, ignore_index=True)
    filename_date = 'date.ftr'
    if os.path.exists(filename_date):
        df_date = feather.read_dataframe(source=filename_date)
        list_date = df_date['date'].tolist()
    else:
        df_date = pd.DataFrame()
        list_date = list()
    print(df_date)
    for i in df_trader_day.index:
        if df_trader_day.at[i, 'is_open'] == 1:
            dt = pd.to_datetime(df_trader_day.at[i, 'cal_date'])
            if dt in list_date:
                continue
        else:
            continue
        if dt < dt_now:
            start_loop_time = time.perf_counter_ns()
            print(f'Current Date:{dt.date()}')
            analysis.index.make_ssb_index(dt=dt)
            df_date.at[i, 'date'] = dt
            feather.write_dataframe(df=df_date, dest=filename_date)
            end_loop_time = time.perf_counter_ns()
            interval_time = (end_loop_time - start_loop_time) / 1000000000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            print(f'{dt.date()} tacks {str_gm}')
        else:
            break
    print(df_date)
"""