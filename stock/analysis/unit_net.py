# modified at 2023/05/18 22::25
import datetime
import pandas as pd
from loguru import logger
import analysis.base
from analysis.const import (
    dt_date_trading_last_1T,
    dt_date_trading_last_T0,
    dt_am_start,
)


def unit_net(sort: bool = False):
    name: str = f"df_unit_net"
    total_market_value = 0
    df_unit_net = analysis.base.feather_from_file(key=name)
    df_trader = analysis.base.feather_from_file(key="df_trader")
    if df_trader.empty:
        logger.trace("df_trader empty")
    else:
        for code in df_trader.index:
            if pd.notnull(df_trader.at[code, "position"]):
                total_market_value += (
                    df_trader.at[code, "position"] * df_trader.at[code, "now_price"]
                )
        dt_now = datetime.datetime.now()
        if dt_now < dt_am_start:
            dt_date_trading = dt_date_trading_last_1T
        else:
            dt_date_trading = dt_date_trading_last_T0
        df_unit_net.at[dt_date_trading, "total_market_value"] = total_market_value
        if sort:
            df_unit_net.sort_index(inplace=True)
        print(df_unit_net.tail(5))
        analysis.base.feather_to_file(
            df=df_unit_net,
            key=name,
        )
