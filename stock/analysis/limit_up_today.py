# modified at 2023/08/05 12:12
import decimal
import random
import pandas as pd
from console import fg
from loguru import logger
import pywencai
import analysis


def limit_up_today(df_trader: pd.DataFrame, df_stocks_pool: pd.DataFrame = None) -> str:
    logger.trace("limit_up_today Begin")
    str_return = ""
    if df_trader.empty:
        return str_return
    set_trader = set(df_trader.index.tolist())
    try:
        df_limit_up = pywencai.get(loop=True, query="今日涨停")
    except AttributeError:
        df_limit_up = pd.DataFrame()
    try:
        df_limit_up_7 = pywencai.get(loop=True, query="今日涨幅大于等于5%")
    except AttributeError:
        df_limit_up_7 = pd.DataFrame()
    if df_limit_up is None:
        set_limit_up = set()
    else:
        try:
            set_limit_up = set(df_limit_up["股票代码"].tolist())
        except KeyError:
            set_limit_up = set()
    if df_limit_up_7 is None:
        set_limit_up_7 = set()
    else:
        try:
            set_limit_up_7 = set(df_limit_up_7["股票代码"].tolist())
        except KeyError:
            set_limit_up_7 = set()
    set_limit_up_all = set_limit_up | set_limit_up_7
    if not set_limit_up_all:
        return str_return
    set_limit_up_all = {item[-2:].lower() + item[:6] for item in set_limit_up_all}
    list_union = list(set_trader & set_limit_up_all)
    random.shuffle(list_union)
    df_realtime = pd.DataFrame()
    i_realtime = 0
    while i_realtime <= 2:
        i_realtime += 1
        df_realtime = analysis.realtime_quotations(
            stock_codes=df_trader.index.to_list()
        )
        if not df_realtime.empty:
            break
        else:
            logger.trace("df_realtime is empty")
    count = len(list_union)
    if count == 0:
        return str_return
    if df_stocks_pool is None:
        df_stocks_pool = pd.DataFrame()
    i = 0
    line_len = 4
    while i < count:
        symbol = list_union[i]
        i += 1
        pct_chg = decimal.Decimal(df_realtime.at[symbol, "pct_chg"]).quantize(
            decimal.Decimal("0.00"), rounding=decimal.ROUND_DOWN
        )
        str_symbol = f"[{df_realtime.at[symbol, 'name']}({symbol})_{pct_chg:.2f}%]"
        if df_trader.at[symbol, "position"] > 0:
            if pct_chg > 7:
                str_symbol = fg.purple(str_symbol)
            else:
                str_symbol = fg.red(str_symbol)
        if symbol in df_stocks_pool.index:
            str_symbol = fg.yellow(str_symbol)
        if str_return == "":
            str_return = f"{str_symbol}"
        elif i % line_len == 1:
            str_return += f"\n\r{str_symbol}"
        else:
            str_return += f", {str_symbol}"
    logger.trace("limit_up_today End")
    return str_return
