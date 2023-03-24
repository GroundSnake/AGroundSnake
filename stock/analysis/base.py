from __future__ import annotations
import datetime

import pandas as pd
import tushare as ts


pro = ts.pro_api()


def all_ts_code() -> list | None:
    df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    if len(df_basic) == 0:
        return
    else:
        list_ts_code = df_basic["ts_code"].tolist()
        return list_ts_code


def all_chs_code() -> list | None:
    list_ts_code = all_ts_code()
    if list_ts_code:
        list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
        return list_chs_code
    else:
        return


def code_ths_to_ts(symbol: str):
    return symbol[2:] + "." + symbol[0:2].upper()


def code_ts_to_ths(ts_code: str):
    return ts_code[-2:].lower() + ts_code[:6]


def get_stock_type_in(code_in: str):
    if code_in[:2].lower() == "sh":
        return "sh"
    elif code_in[:2].lower() == "sz":
        return "sh"
    elif code_in[:2].lower() == "bj":
        return "bj"
    elif code_in[-2:].lower() == "sh":
        return "sh"
    elif code_in[-2:].lower() == "sz":
        return "sz"
    elif code_in[-2:].lower() == "bj":
        return "bj"


def latest_trading_day() -> datetime.date:
    dt_now = datetime.datetime.now()
    str_date_now = dt_now.strftime("%Y%m%d")
    df_trade = pro.trade_cal(exchange='', start_date='20230101', end_date=str_date_now)
    df_trade.set_index(keys=["cal_date"], inplace=True)
    if df_trade.at[str_date_now, "is_open"] == 1:
        str_dt_out = str_date_now
    else:
        str_dt_out = df_trade.at[str_date_now, "pretrade_date"]
    dt_out = datetime.datetime.strptime(str_dt_out, "%Y%m%d").date()
    return dt_out


def transaction_unit(price: float, amount: float = 1000) -> int:
    if price * 100 > amount:
        return 100
    unit_temp = amount / price
    unit_small = int(unit_temp // 100 * 100)
    unit_big = unit_small + 100
    differ_big = unit_big * price - amount
    differ_small = amount - unit_small * price
    if differ_big < differ_small:
        return unit_big
    else:
        return unit_small


def zeroing_sort(pd_series: pd.Series) -> pd.Series:  # 归零化排序
    min_unit = pd_series.min()
    pd_series_out = pd_series.apply(func=lambda x: (x / min_unit - 1).round(4))
    return pd_series_out


