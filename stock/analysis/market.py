import datetime
import pandas as pd
import numpy as np
from console import fg
from pyecharts.charts import Line, Page
import pyecharts.options as opts
from analysis.log import logger
from analysis.realtime_quotation import realtime_quotation
from analysis.const_dynamic import (
    dt_am_start,
    dt_am_0931,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    dt_trading_t0,
    filename_market_activity_charts_ftr,
    filename_market_activity_charts_html,
    filename_etfs_percentile_ftr,
    filename_grid_json,
    phi_s,
    pct_stock_q1_const,
    pct_stock_q3_const,
    pct_etf_q1_const,
    pct_etf_q3_const,
)
from analysis.base import feather_from_file, feather_to_file
from pyecharts.globals import CurrentConfig
from analysis.util import json_grid_from_file


CurrentConfig.ONLINE_HOST = ""
list_series_columns = [
    "all",
    "lt_abs_1",
    "ups1",
    "ups3",
    "ups5",
    "ups7",
    "downs1",
    "downs3",
    "downs5",
    "downs7",
    "stock_q1",
    "stock_q2",
    "stock_q3",
    "index_ups",
    "index_downs",
]
list_df_columns = list_series_columns + [
    "index_ups_moving_average",
    "index_downs_moving_average",
]
list_etfs_percentile_columns = [
    "etf_q1",
    "etf_q2",
    "etf_q3",
]


def get_grid_stock() -> list:
    df_stocks = realtime_quotation.get_stocks_a()  # 调用实时数据接口
    array_pct_etf_chg = df_stocks["pct_chg"].to_numpy()
    pct_stock_up = json_grid_from_file(
        item="pct_stock_up", filename_json=filename_grid_json
    )
    pct_stock_downs = json_grid_from_file(
        item="pct_stock_down", filename_json=filename_grid_json
    )
    pct_stock_quantile = np.percentile(
        a=array_pct_etf_chg, q=[25, 50, 75], method="nearest"
    )
    pct_stock_q1 = float(pct_stock_quantile[0])
    pct_stock_q2 = float(pct_stock_quantile[1])
    pct_stock_q3 = float(pct_stock_quantile[2])
    q_rate = max(abs(pct_stock_up), abs(pct_stock_downs)) / max(
        abs(pct_stock_q3_const), abs(pct_stock_q1_const)
    )
    if 0 < pct_stock_q3_const < pct_stock_q3:
        pct_stock_up = min(round(pct_stock_q3 * q_rate, 2), 7)
        pct_stock_downs = pct_stock_q1
    if pct_stock_q1 < pct_stock_q1_const < 0:
        pct_stock_downs = max(round(pct_stock_q1 * q_rate, 2), -7)
        pct_stock_up = pct_stock_q3
    grid_stock_ups = max(pct_stock_up, 1.5)
    grid_stock_downs = min(pct_stock_downs, -1.5)
    return [grid_stock_ups, grid_stock_downs, pct_stock_q2]


def get_etfs_percentile(type: str = "Series") -> pd.Series | pd.DataFrame | None:
    if type not in ["Series", "DataFrame"]:
        raise TypeError("tpye = ['Series' or 'DataFrame']")
    df_etfs = realtime_quotation.get_etfs_a()
    ser_etfs_percentile = pd.Series(index=list_etfs_percentile_columns)
    ser_etfs_percentile.index.name = df_etfs.index.name
    if df_etfs.shape[0] > 0:
        array_pct_etf_chg = df_etfs["pct_chg"].to_numpy()
        array_pct_etf_chg = np.unique(array_pct_etf_chg)
        pct_etf_quantile = np.percentile(
            a=array_pct_etf_chg, q=[25, 50, 75], method="nearest"
        )
        ser_etfs_percentile["etf_q1"] = pct_etf_quantile[0]
        ser_etfs_percentile["etf_q2"] = pct_etf_quantile[1]
        ser_etfs_percentile["etf_q3"] = pct_etf_quantile[2]
    ser_etfs_percentile.fillna(value=0.0, inplace=True)
    if type == "Series":
        return ser_etfs_percentile
    df_etfs_percentile = feather_from_file(
        filename_df=filename_etfs_percentile_ftr,
    )
    if df_etfs_percentile.empty:
        df_etfs_percentile = pd.DataFrame(columns=list_etfs_percentile_columns)
    if ser_etfs_percentile.empty and type == "DataFrame":
        return df_etfs_percentile
    str_dt_stale = df_etfs_percentile.index.name = ser_etfs_percentile.index.name
    dt_stale = datetime.datetime.strptime(str_dt_stale, "%Y%m%d_%H%M%S")
    df_etfs_percentile.loc[dt_stale] = ser_etfs_percentile
    if dt_am_0931 < dt_stale < dt_am_end or dt_pm_start < dt_stale < dt_pm_end:
        feather_to_file(df=df_etfs_percentile, filename_df=filename_etfs_percentile_ftr)
    if type == "DataFrame":
        return df_etfs_percentile
    else:
        return None


def get_grid_etf() -> list:
    pct_etf_up = json_grid_from_file(
        item="pct_etf_up", filename_json=filename_grid_json
    )
    pct_etf_down = json_grid_from_file(
        item="pct_etf_down", filename_json=filename_grid_json
    )
    ser_etfs_percentile = get_etfs_percentile(type="Series")
    pct_etf_q1 = float(ser_etfs_percentile["etf_q1"])
    pct_etf_q2 = float(ser_etfs_percentile["etf_q2"])
    pct_etf_q3 = float(ser_etfs_percentile["etf_q3"])
    q_rate = max(abs(pct_etf_up), abs(pct_etf_down)) / max(
        abs(pct_etf_q3_const), abs(pct_etf_q3_const)
    )
    if 0 < pct_etf_q3_const < pct_etf_q3:
        pct_etf_up = min(round(pct_etf_q3 * q_rate, 2), 3.5)
        pct_etf_down = pct_etf_q1
    if pct_etf_q1 < pct_etf_q1_const < 0:
        pct_etf_down = max(round(pct_etf_q1 * q_rate, 2), -3.5)
        pct_etf_up = pct_etf_q3
    grid_etf_up = max(pct_etf_up, 1.5)
    grid_etf_down = min(pct_etf_down, -1.5)
    return [grid_etf_up, grid_etf_down, pct_etf_q2]


def get_kline_distribution_ups_downs() -> pd.Series:
    df_stocks = realtime_quotation.get_stocks_a()  # 调用实时数据接口
    df_lt_1 = df_stocks[(df_stocks["pct_chg"] > -1) & (df_stocks["pct_chg"] < 1)]
    df_ups1 = df_stocks[df_stocks["pct_chg"] >= 1]
    df_ups3 = df_stocks[df_stocks["pct_chg"] >= 3]
    df_ups5 = df_stocks[df_stocks["pct_chg"] >= 5]
    df_ups7 = df_stocks[df_stocks["pct_chg"] >= 7]
    df_downs1 = df_stocks[df_stocks["pct_chg"] <= -1]
    df_downs3 = df_stocks[df_stocks["pct_chg"] <= -3]
    df_downs5 = df_stocks[df_stocks["pct_chg"] <= -5]
    df_downs7 = df_stocks[df_stocks["pct_chg"] <= -7]
    df_all_lt_30 = df_stocks[df_stocks["pct_chg"] < 31]
    array_pct_etf_chg = df_all_lt_30["pct_chg"].to_numpy()
    series_return = pd.Series(index=list_series_columns)
    series_return["all"] = df_stocks.shape[0]
    series_return.index.name = df_stocks.index.name
    if series_return["all"] > 0:
        series_return["lt_abs_1"] = df_lt_1.shape[0]
        series_return["ups1"] = df_ups1.shape[0]
        series_return["ups3"] = df_ups3.shape[0]
        series_return["ups5"] = df_ups5.shape[0]
        series_return["ups7"] = df_ups7.shape[0]
        series_return["downs1"] = df_downs1.shape[0]
        series_return["downs3"] = df_downs3.shape[0]
        series_return["downs5"] = df_downs5.shape[0]
        series_return["downs7"] = df_downs7.shape[0]
        pct_stock_quantile = np.percentile(
            a=array_pct_etf_chg, q=[25, 50, 75], method="nearest"
        )
        series_return["stock_q1"] = float(pct_stock_quantile[0])
        series_return["stock_q2"] = float(pct_stock_quantile[1])
        series_return["stock_q3"] = float(pct_stock_quantile[2])
        series_return["index_ups"] = round(
            (
                series_return["ups1"]
                + series_return["ups3"]
                + series_return["ups5"]
                + series_return["ups7"]
            )
            / series_return["all"]
            * 100,
            2,
        )
        series_return["index_downs"] = round(
            (
                series_return["downs1"]
                + series_return["downs3"]
                + series_return["downs5"]
                + series_return["downs7"]
            )
            / series_return["all"]
            * 100,
            2,
        )
    series_return.fillna(value=0.0, inplace=True)
    return series_return


def get_update_distribution_ups_downs(
    days: int = 5, reset: bool = False
) -> pd.DataFrame:
    df_market_activity_charts = feather_from_file(
        filename_df=filename_market_activity_charts_ftr,
    )
    # df_market_activity_charts['stock_q2'] = df_market_activity_charts['pct_median']
    series_dist = get_kline_distribution_ups_downs()
    if series_dist.empty:
        return df_market_activity_charts
    windows = days * 600
    if reset:
        df_market_activity_charts["index_ups_moving_average"] = (
            df_market_activity_charts["index_ups"]
            .rolling(window=windows, min_periods=1)
            .mean()
            .round(2)
        )
        df_market_activity_charts["index_downs_moving_average"] = (
            df_market_activity_charts["index_downs"]
            .rolling(window=windows, min_periods=1)
            .mean()
            .round(2)
        )
    if df_market_activity_charts.empty:

        df_market_activity_charts = df_market_activity_charts.reindex(
            columns=list_df_columns
        )
    df_market_activity_charts = df_market_activity_charts.reindex(
        columns=list_df_columns
    )
    str_dt_stale = series_dist.index.name
    dt_stale = datetime.datetime.strptime(str_dt_stale, "%Y%m%d_%H%M%S")
    df_market_activity_charts.loc[dt_stale] = series_dist
    df_market_activity_charts.index.name = str_dt_stale
    df_market_activity_charts = df_market_activity_charts[
        df_market_activity_charts["all"] > 0
    ]
    df_market_activity_charts.sort_index(ascending=True, inplace=True)
    df_index_moving_average = df_market_activity_charts.iloc[-windows:]
    df_market_activity_charts.at[dt_stale, "index_ups_moving_average"] = (
        df_index_moving_average["index_ups"].mean().round(2)
    )
    df_market_activity_charts.at[dt_stale, "index_downs_moving_average"] = (
        df_index_moving_average["index_downs"].mean().round(2)
    )
    df_market_activity_charts.index.rename(name=str_dt_stale, inplace=True)
    df_market_activity_charts.fillna(value=0.0, inplace=True)
    if dt_am_0931 < dt_stale < dt_am_end or dt_pm_start < dt_stale < dt_pm_end:
        feather_to_file(
            df=df_market_activity_charts,
            filename_df=filename_market_activity_charts_ftr,
        )
    return df_market_activity_charts


def market_activity() -> list:
    rate = 25
    df_market_activity_charts = get_update_distribution_ups_downs()
    ser_stocks_m_a_c = df_market_activity_charts.iloc[-1]
    df_etfs_percentile = get_etfs_percentile(type="DataFrame")
    ser_etfs_percentile = df_etfs_percentile.iloc[-1]
    str_return_empty = "No Market Activity"
    bool_not_crash = True
    bool_not_warning = True
    bool_not_long_warning = True
    list_default = [
        str_return_empty,
        bool_not_crash,
        bool_not_warning,
        bool_not_long_warning,
    ]
    if ser_stocks_m_a_c.empty:
        return list_default
    dt_stale = datetime.datetime.strptime(
        df_market_activity_charts.index.name, "%Y%m%d_%H%M%S"
    )
    if not (dt_am_start <= dt_stale <= dt_pm_end):
        return list_default
    all_stocks = ser_stocks_m_a_c["all"]
    pct_stock_q1 = ser_stocks_m_a_c["stock_q1"]
    pct_stock_q2 = ser_stocks_m_a_c["stock_q2"]
    pct_stock_q3 = ser_stocks_m_a_c["stock_q3"]
    lt_abs_1 = ser_stocks_m_a_c["lt_abs_1"]
    ups1 = ser_stocks_m_a_c["ups1"]
    ups3 = ser_stocks_m_a_c["ups3"]
    ups5 = ser_stocks_m_a_c["ups5"]
    ups7 = ser_stocks_m_a_c["ups7"]
    downs1 = ser_stocks_m_a_c["downs1"]
    downs3 = ser_stocks_m_a_c["downs3"]
    downs5 = ser_stocks_m_a_c["downs5"]
    downs7 = ser_stocks_m_a_c["downs7"]
    index_ups = ser_stocks_m_a_c["index_ups"]
    index_downs = ser_stocks_m_a_c["index_downs"]
    rate_lt_abs_1 = round(lt_abs_1 / all_stocks * 100, 2)
    rate_ups1 = round(ups1 / all_stocks * 100, 2)
    rate_ups3 = round(ups3 / all_stocks * 100, 2)
    rate_downs1 = round(downs1 / all_stocks * 100, 2)
    rate_downs3 = round(downs3 / all_stocks * 100, 2)
    rate_downs5 = round(downs5 / all_stocks * 100, 2)
    rate_downs7 = round(downs7 / all_stocks * 100, 2)
    index_ups_moving_average = ser_stocks_m_a_c["index_ups_moving_average"]
    index_downs_moving_average = ser_stocks_m_a_c["index_downs_moving_average"]
    if rate_ups3 >= 75:
        str_add = fg.blue("<Reduce Position>")
        logger.debug(f"<Reduce Position> - [rate_ups3({rate_ups3}) >= 75]")
    elif rate_downs3 >= 75:
        str_add = fg.purple("<Increase Position>")
        logger.debug(f"<Increase Position> - [rate_downs3({rate_downs3}) >= 75]")
    elif rate_lt_abs_1 >= phi_s:
        str_add = fg.white("<Keep Position>")
        logger.debug(f"<Keep Position> - [rate_lt_abs_1({rate_lt_abs_1}) >= {phi_s}]")
    elif rate_ups1 >= 50:
        str_add = fg.green("<Reduce Position Ready...>")
        logger.debug(f"<Reduce Position Ready...> - [rate_ups1({rate_ups1}) >= 50]")
    elif rate_downs1 >= 50:
        str_add = fg.red("<Increase Position Ready...>")
        logger.debug(
            f"<Increase Position Ready...> - [rate_downs1({rate_downs1}) >= 50]"
        )
    else:
        logger.debug(
            f"rate_ups3={rate_ups3}, "
            f"rate_downs3={rate_downs3}, "
            f"rate_lt_abs_1={rate_lt_abs_1}, "
            f"rate_ups1={rate_ups1}, "
            f"rate_downs1={rate_downs1}, "
        )
        str_add = fg.white("Else")
    if pct_stock_q2 <= -3.0 or rate_downs5 > rate or rate_downs7 > 10:
        str_add += " - " + fg.green("<Stock Market Crash>")
        bool_not_crash = False
        bool_not_warning = False
        bool_not_long_warning = False
    elif -3.0 < pct_stock_q2 < -1.5 or rate_downs3 > rate:
        str_add += " - " + fg.green("<Stock Market Crash Warning...>")
        bool_not_warning = False
        if pct_stock_q2 < -2.0:
            bool_not_long_warning = False
    pct_etf_q1 = ser_etfs_percentile["etf_q1"]
    pct_etf_q2 = ser_etfs_percentile["etf_q2"]
    pct_etf_q3 = ser_etfs_percentile["etf_q3"]
    str_pct_etf_q1 = f"[Q1:{ser_etfs_percentile["etf_q1"]:5.2f}]"
    str_pct_etf_q2 = f"[Q2:{ser_etfs_percentile["etf_q2"]:5.2f}]"
    str_pct_etf_q3 = f"[Q3:{ser_etfs_percentile["etf_q3"]:5.2f}]"
    if pct_etf_q1 < pct_etf_q1_const:
        str_pct_etf_q1 = fg.green(str_pct_etf_q1)
    elif pct_etf_q1 > 0.0:
        str_pct_etf_q1 = fg.red(str_pct_etf_q1)
    if pct_etf_q3 > pct_etf_q3_const:
        str_pct_etf_q3 = fg.red(str_pct_etf_q3)
    elif pct_etf_q3 < 0.0:
        str_pct_etf_q3 = fg.green(str_pct_etf_q3)
    if pct_etf_q2 > 0.7:
        str_pct_etf_q2 = fg.red(str_pct_etf_q2)
    elif pct_etf_q2 < -0.7:
        str_pct_etf_q2 = fg.green(str_pct_etf_q2)
    str_pct_stock_q1 = f"[Q1:{pct_stock_q1:5.2f}]"
    str_pct_stock_q2 = f"[Q2:{pct_stock_q2:5.2f}]"
    str_pct_stock_q3 = f"[Q3:{pct_stock_q3:5.2f}]"
    if pct_stock_q1 < pct_stock_q1_const:  # pct_stock_q1_const < 0
        str_pct_stock_q1 = fg.green(str_pct_stock_q1)
    elif pct_stock_q1 > 0.0:
        str_pct_stock_q1 = fg.red(str_pct_stock_q1)
    if pct_stock_q3 > pct_stock_q3_const:  # pct_stock_q3_const > 0
        str_pct_stock_q3 = fg.red(str_pct_stock_q3)
    elif pct_stock_q3 < 0.0:
        str_pct_stock_q3 = fg.green(str_pct_stock_q3)
    if 1.0 < pct_stock_q2 < 2.0:
        str_pct_stock_q2 = fg.red(str_pct_stock_q2)
    elif -1.0 < pct_stock_q2 < -2.0:
        str_pct_stock_q2 = fg.green(str_pct_stock_q2)
    elif pct_stock_q2 > 2.0:
        str_pct_stock_q2 = fg.purple(str_pct_stock_q2)
    elif pct_stock_q2 < -2.0:
        str_pct_stock_q2 = fg.blue(str_pct_stock_q2)
    space_line1 = " " * 43
    space_line2 = " " * 29
    space_line3 = " " * 30
    space_line4 = " " * 18
    space_line5 = " "
    space_line6 = " " * 24
    space_line7 = " " * 23
    space_line8 = " " * 24
    str_return = (
        f"{space_line1}<{dt_stale.time()}>\n"
        f"{space_line2}[{str_add}]\n"
        f"{space_line3}{fg.purple(f'[↑3%_Rate]:{rate_ups3}%')} - "
        f"{fg.blue(f'[↓3%_Rate]:{rate_downs3}%')}]\n"
        f"{space_line4}{fg.red(f'[Index-ups]:{index_ups}({index_ups_moving_average})')} - "
        f"{fg.green(f'[Index-Downs]:{index_downs}({index_downs_moving_average})')} - "
        f"[All]:{int(all_stocks)}\n"
        f"{space_line5}{fg.red(f'[↑7%]:{int(ups7)}')} - "
        f"{fg.purple(f'[↑5%]:{int(ups5)}')} - "
        f"{fg.lightred(f'[↑3%]:{int(ups3)}')} - "
        f"{fg.red(f'[↑1%]:{int(ups1)}')} - "
        f"{fg.white(f'[-1% >|< 1%]:{int(lt_abs_1)}')} - "
        f"{fg.lightgreen(f'[↓1%]:{int(downs1)}')} - "
        f"{fg.green(f'[↓3%]:{int(downs3)}')} - "
        f"{fg.blue(f'[↓5%]:{int(downs5)}')} - "
        f"{fg.lightgreen(f'[↓7%]:{int(downs7)}')}\n"
        f"{space_line6}ETF_Percentile - {str_pct_etf_q1} - "
        f"{str_pct_etf_q2} - {str_pct_etf_q3}\n"
        f"{space_line7}Stock_Percentile - {str_pct_stock_q1} - "
        f"{str_pct_stock_q2} - {str_pct_stock_q3}\n"
        f"{space_line8}[Bool_Not_Crash: {bool_not_crash}] - "
        f"[bool_not_long_warning: {bool_not_long_warning}]"
    )
    return [str_return, bool_not_crash, bool_not_warning, bool_not_long_warning]


def market_activity_charts(days: int = 20) -> bool:
    logger.trace(f"market_activity_charts Begin")
    df_market_activity_charts = get_update_distribution_ups_downs()
    df_etfs_percentile = get_etfs_percentile(type="DataFrame")
    if df_market_activity_charts.empty or df_etfs_percentile.empty:
        return False
    dt_index_start_365_t = dt_trading_t0 - datetime.timedelta(days=365)
    df_market_activity_365_t = df_market_activity_charts[
        df_market_activity_charts.index > dt_index_start_365_t
    ]
    df_etfs_percentile_365_t = df_etfs_percentile[
        df_etfs_percentile.index > dt_index_start_365_t
    ]
    x_dt_365_t = df_market_activity_365_t.index.tolist()
    y_index_ups_365_t = df_market_activity_365_t["index_ups"].tolist()
    y_index_ups_moving_average_365_t = df_market_activity_365_t[
        "index_ups_moving_average"
    ].tolist()
    y_index_downs_365_t = df_market_activity_365_t["index_downs"].tolist()
    y_index_downs_moving_average_365_t = df_market_activity_365_t[
        "index_downs_moving_average"
    ].tolist()
    y_index_365_t = set(
        y_index_ups_365_t
        + y_index_ups_moving_average_365_t
        + y_index_downs_365_t
        + y_index_downs_moving_average_365_t
    )
    y_index_min_365_t = min(y_index_365_t)
    y_index_max_365_t = max(y_index_365_t)
    dt_index_start_n_t = dt_trading_t0 - datetime.timedelta(days=days)
    df_market_activity_n_t = df_market_activity_charts[
        df_market_activity_charts.index > dt_index_start_n_t
    ]
    x_dt_n_t = df_market_activity_n_t.index.tolist()
    y_lt_abs_1_n_t = df_market_activity_n_t["lt_abs_1"].tolist()
    y_lt_abs_1__max_n_t = max(y_lt_abs_1_n_t)
    y_lt_abs_1__min_n_t = min(y_lt_abs_1_n_t)
    y_index_ups_n_t = df_market_activity_n_t["index_ups"].tolist()
    y_index_ups_moving_average_n_t = df_market_activity_n_t[
        "index_ups_moving_average"
    ].tolist()
    y_index_downs_n_t = df_market_activity_n_t["index_downs"].tolist()
    y_index_downs_moving_average_n_t = df_market_activity_n_t[
        "index_downs_moving_average"
    ].tolist()
    y_index_n_t = set(
        y_index_ups_n_t
        + y_index_ups_moving_average_n_t
        + y_index_downs_n_t
        + y_index_downs_moving_average_n_t
    )
    y_index_min_n_t = min(y_index_n_t)
    y_index_max_n_t = max(y_index_n_t)
    df_market_activity_t0 = df_market_activity_charts[
        df_market_activity_charts.index > dt_trading_t0
    ]
    y_pct_stock_q1_n_t = df_market_activity_n_t["stock_q1"].tolist()
    y_pct_stock_q2_n_t = df_market_activity_n_t["stock_q2"].tolist()
    y_pct_stock_q3_n_t = df_market_activity_n_t["stock_q3"].tolist()
    y_pct_stock_q_n_t = set(
        y_pct_stock_q1_n_t + y_pct_stock_q2_n_t + y_pct_stock_q3_n_t
    )
    y_pct_median_max_n_t = max(max(y_pct_stock_q_n_t), abs(min(y_pct_stock_q_n_t)))
    y_pct_median_min_n_t = -y_pct_median_max_n_t
    if df_market_activity_t0.empty:
        return False
    x_dt_t0 = df_market_activity_t0.index.tolist()
    y_index_ups_t0 = df_market_activity_t0["index_ups"].tolist()
    y_index_ups_moving_average_t0 = df_market_activity_t0[
        "index_ups_moving_average"
    ].tolist()
    y_index_downs_t0 = df_market_activity_t0["index_downs"].tolist()
    y_index_downs_moving_average_t0 = df_market_activity_t0[
        "index_downs_moving_average"
    ].tolist()
    y_ups1_t0 = df_market_activity_t0["ups1"].tolist()
    y_ups3_t0 = df_market_activity_t0["ups3"].tolist()
    y_ups5_t0 = df_market_activity_t0["ups5"].tolist()
    y_ups7_t0 = df_market_activity_t0["ups7"].tolist()
    y_downs1_t0 = df_market_activity_t0["downs1"].tolist()
    y_downs3_t0 = df_market_activity_t0["downs3"].tolist()
    y_downs5_t0 = df_market_activity_t0["downs5"].tolist()
    y_downs7_t0 = df_market_activity_t0["downs7"].tolist()
    y_index_t0 = set(
        y_index_ups_t0
        + y_index_ups_moving_average_t0
        + y_index_downs_t0
        + y_index_downs_moving_average_t0
    )
    y_index_min_t0 = min(y_index_t0)
    y_index_max_t0 = max(y_index_t0)
    y_all_t0 = set(y_ups1_t0 + y_downs1_t0)
    y_charts_min_t0 = min(y_all_t0)
    y_charts_max_t0 = max(y_all_t0)
    y_pct3_t0 = set(y_ups3_t0 + y_downs3_t0)
    y_charts_min3_t0 = min(y_pct3_t0)
    y_charts_max3_t0 = max(y_pct3_t0)
    y_pct5_t0 = set(y_ups5_t0 + y_downs5_t0)
    y_charts_min5_t0 = min(y_pct5_t0)
    y_charts_max5_t0 = max(y_pct5_t0)
    y_pct7_t0 = set(y_ups7_t0 + y_downs7_t0)
    y_charts_min7_t0 = min(y_pct7_t0)
    y_charts_max7_t0 = max(y_pct7_t0)
    # ETFs
    x_etf_dt_365_t = df_etfs_percentile_365_t.index.tolist()
    y_etf_q1_356_t = df_etfs_percentile_365_t["etf_q1"].tolist()
    y_etf_q2_356_t = df_etfs_percentile_365_t["etf_q2"].tolist()
    y_etf_q3_356_t = df_etfs_percentile_365_t["etf_q3"].tolist()
    y_etf_q_365_t = set(y_etf_q1_356_t + y_etf_q2_356_t + y_etf_q3_356_t)
    y_etf_q_max_365_t = max(y_etf_q_365_t)
    y_etf_q_min_365_t = min(y_etf_q_365_t)

    line_index_n_t = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_index_n_t.add_xaxis(xaxis_data=x_dt_n_t)
    line_index_n_t.add_yaxis(
        series_name="Index Ups",
        y_axis=y_index_ups_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_n_t.add_yaxis(
        series_name="Index Ups Average",
        y_axis=y_index_ups_moving_average_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_n_t.add_yaxis(
        series_name="Index Downs",
        y_axis=y_index_downs_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_n_t.add_yaxis(
        series_name="Index Downs Average",
        y_axis=y_index_downs_moving_average_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_n_t.set_colors(
        colors=[
            "red",
            "purple",
            "green",
            "blue",
        ]
    )
    line_index_n_t.set_global_opts(
        title_opts=opts.TitleOpts(
            title=f"[{days}D] Index Ups and Downs", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_index_min_n_t,
            max_=y_index_max_n_t,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_lt_abs_1_365_t = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_lt_abs_1_365_t.add_xaxis(xaxis_data=x_dt_n_t)
    line_lt_abs_1_365_t.add_yaxis(
        series_name="Lt_Abs_1",
        y_axis=y_lt_abs_1_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_lt_abs_1_365_t.set_colors(
        colors=[
            "red",
            "purple",
            "green",
            "blue",
        ]
    )
    line_lt_abs_1_365_t.set_global_opts(
        title_opts=opts.TitleOpts(title="[365D] Lt_Abs_1", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_lt_abs_1__min_n_t,
            max_=y_lt_abs_1__max_n_t,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_percentile_n_t = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_percentile_n_t.add_xaxis(xaxis_data=x_dt_n_t)
    line_percentile_n_t.add_yaxis(
        series_name="Stock_Q1",
        y_axis=y_pct_stock_q1_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_percentile_n_t.add_yaxis(
        series_name="Stock_Q2",
        y_axis=y_pct_stock_q2_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_percentile_n_t.add_yaxis(
        series_name="Stock_Q3",
        y_axis=y_pct_stock_q3_n_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_percentile_n_t.set_colors(
        colors=[
            "blue",
            "purple",
            "red",
        ]
    )
    line_percentile_n_t.set_global_opts(
        title_opts=opts.TitleOpts(title=f"[{days}D] Pct Qercentile", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_pct_median_min_n_t,
            max_=y_pct_median_max_n_t,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_index_365_t = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_index_365_t.add_xaxis(xaxis_data=x_dt_365_t)
    line_index_365_t.add_yaxis(
        series_name="Index Ups days",
        y_axis=y_index_ups_365_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_365_t.add_yaxis(
        series_name="Index Ups Average",
        y_axis=y_index_ups_moving_average_365_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_365_t.add_yaxis(
        series_name="Index Downs",
        y_axis=y_index_downs_365_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_365_t.add_yaxis(
        series_name="Index Downs Average",
        y_axis=y_index_downs_moving_average_365_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_365_t.set_colors(
        colors=[
            "red",
            "purple",
            "green",
            "blue",
        ]
    )
    line_index_365_t.set_global_opts(
        title_opts=opts.TitleOpts(
            title="[365D] Index Ups and Downs", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_index_min_365_t,
            max_=y_index_max_365_t,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )

    line_index_t0 = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_index_t0.add_xaxis(xaxis_data=x_dt_t0)
    line_index_t0.add_yaxis(
        series_name="Index Ups",
        y_axis=y_index_ups_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_t0.add_yaxis(
        series_name="Index Ups Average",
        y_axis=y_index_ups_moving_average_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_t0.add_yaxis(
        series_name="Index Downs",
        y_axis=y_index_downs_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_t0.add_yaxis(
        series_name="Index Downs Average",
        y_axis=y_index_downs_moving_average_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_index_t0.set_colors(
        colors=[
            "red",
            "purple",
            "green",
            "blue",
        ]
    )
    line_index_t0.set_global_opts(
        title_opts=opts.TitleOpts(
            title="[Today]    Index Ups and Downs", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_index_min_t0,
            max_=y_index_max_t0,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )

    line_ups_downs_t0 = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_ups_downs_t0.add_xaxis(xaxis_data=x_dt_t0)
    line_ups_downs_t0.add_yaxis(
        series_name="涨超1%",
        y_axis=y_ups1_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs_t0.add_yaxis(
        series_name="跌超1%",
        y_axis=y_downs1_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs_t0.set_colors(
        colors=[
            "red",
            "green",
        ]
    )
    line_ups_downs_t0.set_global_opts(
        title_opts=opts.TitleOpts(
            title="Number of Stock Ups and Downs", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_charts_min_t0,
            max_=y_charts_max_t0,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_ups_downs3_t0 = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_ups_downs3_t0.add_xaxis(xaxis_data=x_dt_t0)
    line_ups_downs3_t0.add_yaxis(
        series_name="涨超3%",
        y_axis=y_ups3_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs3_t0.add_yaxis(
        series_name="跌超3%",
        y_axis=y_downs3_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs3_t0.set_colors(
        colors=[
            "red",
            "green",
        ]
    )
    line_ups_downs3_t0.set_global_opts(
        title_opts=opts.TitleOpts(
            title="More than 3 of stocks have risen or fallen", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_charts_min3_t0,
            max_=y_charts_max3_t0,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_ups_downs5_t0 = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_ups_downs5_t0.add_xaxis(xaxis_data=x_dt_t0)
    line_ups_downs5_t0.add_yaxis(
        series_name="涨超5%",
        y_axis=y_ups5_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs5_t0.add_yaxis(
        series_name="跌超5%",
        y_axis=y_downs5_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs5_t0.set_colors(
        colors=[
            "red",
            "green",
        ]
    )
    line_ups_downs5_t0.set_global_opts(
        title_opts=opts.TitleOpts(
            title="More than 5 of stocks have risen or fallen", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_charts_min5_t0,
            max_=y_charts_max5_t0,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    line_ups_downs7_t0 = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_ups_downs7_t0.add_xaxis(xaxis_data=x_dt_t0)
    line_ups_downs7_t0.add_yaxis(
        series_name="涨超7%",
        y_axis=y_ups7_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs7_t0.add_yaxis(
        series_name="跌超7%",
        y_axis=y_downs7_t0,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_ups_downs7_t0.set_colors(
        colors=[
            "red",
            "green",
        ]
    )
    line_ups_downs7_t0.set_global_opts(
        title_opts=opts.TitleOpts(
            title="More than 7 of stocks have risen or fallen", pos_left="center"
        ),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_charts_min7_t0,
            max_=y_charts_max7_t0,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    # ETF Qercentile
    line_etf_q_365_t = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
        )
    )
    line_etf_q_365_t.add_xaxis(xaxis_data=x_etf_dt_365_t)
    line_etf_q_365_t.add_yaxis(
        series_name="ETF_Q1",
        y_axis=y_etf_q1_356_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_etf_q_365_t.add_yaxis(
        series_name="ETF_Q2",
        y_axis=y_etf_q2_356_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_etf_q_365_t.add_yaxis(
        series_name="ETF_Q3",
        y_axis=y_etf_q3_356_t,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_etf_q_365_t.set_colors(
        colors=[
            "green",
            "purple",
            "red",
            "blue",
        ]
    )
    line_etf_q_365_t.set_global_opts(
        title_opts=opts.TitleOpts(title=f"[365D] ETF Qercentile", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_etf_q_min_365_t,
            max_=y_etf_q_max_365_t,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    dt_stale = datetime.datetime.strptime(
        df_market_activity_charts.index.name, "%Y%m%d_%H%M%S"
    )
    str_dt_stale = dt_stale.strftime("%H:%M:%S")
    page = Page(
        page_title=f"Market Activity {str_dt_stale}",
    )
    page.add(
        line_percentile_n_t,
        line_etf_q_365_t,
        line_index_n_t,
        line_lt_abs_1_365_t,
        line_index_365_t,
        line_index_t0,
        line_ups_downs_t0,
        line_ups_downs3_t0,
        line_ups_downs5_t0,
        line_ups_downs7_t0,
    )
    page.render(path=filename_market_activity_charts_html)
    return True
