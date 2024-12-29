import time
import datetime
import requests
import pandas as pd
import akshare as ak
import json
from analysis.const_dynamic import (
    dt_init,
    dt_pm_end,
    path_user_home,
    path_temp,
    format_dt,
)
from analysis.log import logger
from analysis.base import (
    feather_to_file,
    feather_from_file,
    get_stock_type,
    code_ths_to_code,
)


def ak_stock_zh_index_spot_sina() -> pd.DataFrame:
    """
    实时指数行情
    """
    dt_now = datetime.datetime.now()
    filename_realtime_index = path_user_home.joinpath("ak_stock_zh_index_spot_sina.ftr")
    df_index_realtime = feather_from_file(filename_df=filename_realtime_index)
    if not df_index_realtime.empty:
        try:
            dt_stale = datetime.datetime.strptime(
                df_index_realtime.index.name, format_dt
            )
        except ValueError:
            dt_stale = dt_init
        if dt_stale >= dt_pm_end:

            return df_index_realtime
        timedelta_now = (dt_now - dt_stale).seconds
        if timedelta_now < 60:

            return df_index_realtime
    try:
        df_index_realtime = ak.stock_zh_index_spot_sina()
    except json.JSONDecodeError:
        return pd.DataFrame()
    df_index_realtime.rename(
        columns={
            "代码": "symbol",
            "名称": "name",
            "最新价": "close",
            "涨跌额": "change",
            "涨跌幅": "pct_chg",
            "昨收": "pre_close",
            "今开": "open",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        },
        inplace=True,
    )
    df_index_realtime.set_index(keys=["symbol"], inplace=True)
    df_index_realtime["close"] = df_index_realtime["close"].apply(
        func=lambda x: round(x, 3)
    )
    df_index_realtime["pct_chg"] = df_index_realtime["pct_chg"].apply(
        func=lambda x: round(x, 2)
    )
    df_index_realtime["change"] = df_index_realtime["change"].apply(
        func=lambda x: round(x, 2)
    )
    str_dt_now = dt_now.strftime(format_dt)
    df_index_realtime.index.rename(name=str_dt_now, inplace=True)
    feather_to_file(df=df_index_realtime, filename_df=filename_realtime_index)
    return df_index_realtime


def ak_stock_hk_spot_em() -> pd.DataFrame:
    """
    实时港股行情
    """
    dict_columns = {
        "代码": "symbol",
        "名称": "name",
        "最新价": "close",
        "涨跌额": "change",
        "涨跌幅": "pct_chg",
        "今开": "open",
        "最高": "high",
        "最低": "low",
        "昨收": "pre_close",
        "成交量": "volume",
        "成交额": "amount",
    }
    df_em_hk = ak.stock_hk_spot_em()
    df_em_hk = df_em_hk.rename(columns=dict_columns)
    df_em_hk["symbol"] = df_em_hk["symbol"].apply(func=lambda x: "hk" + x)
    df_em_hk["close"] = df_em_hk["close"].apply(func=lambda x: round(x, 3))
    df_em_hk["pct_chg"] = df_em_hk["pct_chg"].apply(func=lambda x: round(x, 2))
    df_em_hk["change"] = df_em_hk["change"].apply(func=lambda x: round(x, 2))
    df_em_hk = df_em_hk[list(dict_columns.values())]
    df_em_hk.set_index(keys=["symbol"], inplace=True)
    dt_max = datetime.datetime.now()
    if dt_max > dt_pm_end:
        dt_max = dt_pm_end
    df_em_hk["datetime"] = dt_max
    str_dt_max = dt_max.strftime(format_dt)
    df_em_hk.index.rename(name=str_dt_max, inplace=True)
    return df_em_hk


def ak_stock_zh_a_spot_em() -> pd.DataFrame:
    """
    实时A股行情_东方财富
    """
    dt_now = datetime.datetime.now().replace(microsecond=0)
    dict_columns_stocks = {
        "代码": "code",
        "名称": "name",
        "最新价": "close",
        "涨跌幅": "pct_chg",
        "涨跌额": "change",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "最高": "high",
        "最低": "low",
        "今开": "open",
        "昨收": "pre_close",
        "量比": "volume_ratio",
        "换手率": "turnover",
        "市盈率-动态": "pe_ttm",
        "市净率": "PB",
        "总市值": "total_mv",
        "流通市值": "circ_mv",
    }
    i_while = 0
    df_em_stocks = pd.DataFrame()
    while i_while < 3:
        i_while += 1
        try:
            df_em_stocks = ak.stock_zh_a_spot_em()
            break
        except requests.exceptions.ConnectTimeout as e:
            logger.error(f"Error-({e})")
    if df_em_stocks.empty:
        logger.error(f"ak_stock_zh_a_spot_em empty")
        time.sleep(20)
        return df_em_stocks
    df_em_stocks = df_em_stocks.rename(
        columns=dict_columns_stocks,
    )
    df_em_stocks = df_em_stocks[list(dict_columns_stocks.values())]
    df_em_stocks["close"] = df_em_stocks["close"].apply(func=lambda x: round(x, 3))
    df_em_stocks["pct_chg"] = df_em_stocks["pct_chg"].apply(func=lambda x: round(x, 2))
    df_em_stocks["change"] = df_em_stocks["change"].apply(func=lambda x: round(x, 2))
    if dt_now >= dt_pm_end:
        dt_max = dt_pm_end
    else:
        dt_max = dt_now
    df_em_stocks["datetime"] = dt_max
    df_em_stocks["code"] = df_em_stocks["code"].apply(
        func=lambda x: get_stock_type(x) + x
    )
    df_em_stocks["volume"] = df_em_stocks["volume"].apply(func=lambda x: x * 100)
    df_em_stocks["total_mv"] = df_em_stocks["total_mv"].apply(
        func=lambda x: round(x / 100000000, 2)
    )
    df_em_stocks["circ_mv"] = df_em_stocks["circ_mv"].apply(
        func=lambda x: round(x / 100000000, 2)
    )
    df_em_stocks.set_index(keys="code", inplace=True)
    str_dt_max = dt_max.strftime(format_dt)
    df_em_stocks.index.rename(name=str_dt_max, inplace=True)
    df_em_stocks = df_em_stocks[df_em_stocks["volume"] > 0]
    return df_em_stocks


def ak_fund_etf_spot_em() -> pd.DataFrame:
    """
    实时ETF行情_东方财富
    """
    dt_now = datetime.datetime.now().replace(microsecond=0)
    dict_columns_etfs = {
        "代码": "code",
        "名称": "name",
        "最新价": "close",
        "涨跌幅": "pct_chg",
        "涨跌额": "change",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "最高价": "high",
        "最低价": "low",
        "开盘价": "open",
        "昨收": "pre_close",
        "量比": "volume_ratio",
        "换手率": "turnover",
        "总市值": "total_mv",
        "流通市值": "circ_mv",
    }
    df_em_etfs = ak.fund_etf_spot_em()
    if dt_now >= dt_pm_end:
        dt_max = dt_pm_end
    else:
        dt_max = dt_now
    df_em_etfs = df_em_etfs.rename(columns=dict_columns_etfs)
    df_em_etfs = df_em_etfs[list(dict_columns_etfs.values())]
    dict_columns_etfs_values = {
        "close": 0.0,
        "pct_chg": 0.0,
        "change": 0.0,
        "volume": 0.0,
        "amount": 0.0,
        "amplitude": 0.0,
        "high": 0.0,
        "low": 0.0,
        "open": 0.0,
        "pre_close": 0.0,
        "volume_ratio": 0.0,
        "turnover": 0.0,
        "total_mv": 0.0,
        "circ_mv": 0.0,
    }
    df_em_etfs.fillna(value=dict_columns_etfs_values, inplace=True)
    df_em_etfs = df_em_etfs[df_em_etfs["volume"] > 0]
    df_em_etfs["close"] = df_em_etfs["close"].apply(func=lambda x: round(x, 3))
    df_em_etfs["pct_chg"] = df_em_etfs["pct_chg"].apply(func=lambda x: round(x, 2))
    df_em_etfs["change"] = df_em_etfs["change"].apply(func=lambda x: round(x, 2))
    df_em_etfs["code"] = df_em_etfs["code"].apply(func=lambda x: get_stock_type(x) + x)
    df_em_etfs["datetime"] = dt_max
    df_em_etfs.set_index(keys="code", inplace=True)
    str_dt_max = dt_max.strftime(format_dt)
    df_em_etfs.index.rename(name=str_dt_max, inplace=True)
    return df_em_etfs


def ak_index_zh_a_hist_min_em(
    symbol: str = "sz399006",
    period: int = 1,
) -> pd.DataFrame:
    filename_index_min = path_temp.joinpath(
        f"df_index_a_hist_min_{period}_{symbol}.ftr"
    )
    df_em_index = feather_from_file(filename_df=filename_index_min)
    dt_end = dt_pm_end
    if not df_em_index.empty:
        dt_stale = datetime.datetime.strptime(df_em_index.index.name, format_dt)
        if dt_stale >= dt_end:
            logger.debug(f"pickle from [{filename_index_min}] - [{dt_stale}]")
            return df_em_index
    format_dt_space = "%Y-%m-%d %H:%M:%S"
    if period == 1:
        dict_columns_index = {
            "时间": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "最新价": "new",
        }
    else:
        dict_columns_index = {
            "时间": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "涨跌幅": "pct_chg",
            "涨跌额": "change",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "换手率": "turnover",
        }
    str_dt_init = dt_init.strftime(format=format_dt_space)
    str_dt_pm_end = dt_pm_end.strftime(format=format_dt_space)
    code = code_ths_to_code(symbol)
    i_while = 0
    while i_while < 2:
        i_while += 1
        try:
            df_em_index = ak.index_zh_a_hist_min_em(
                symbol=code,
                period=str(period),
                start_date=str_dt_init,
                end_date=str_dt_pm_end,
            )
        except TypeError as e:
            logger.error(f"{symbol} error. - ({e})")
            df_em_index = pd.DataFrame()
    if not df_em_index.empty:
        df_em_index = df_em_index.rename(columns=dict_columns_index)
        df_em_index["close"] = df_em_index["close"].apply(func=lambda x: round(x, 3))
        df_em_index["datetime"] = pd.to_datetime(
            df_em_index["datetime"], errors="coerce"
        )
        df_em_index.set_index(keys=["datetime"], inplace=True)
        dt_max = df_em_index.index.max()
        str_dt_max = dt_max.strftime(format_dt)
        df_em_index.index.rename(name=str_dt_max, inplace=True)
        feather_to_file(df=df_em_index, filename_df=filename_index_min)
        logger.debug(f"feather to [{filename_index_min}]")
    return df_em_index
