import pandas as pd
import datetime
from analysis.const_dynamic import (
    dt_init,
    dt_am_start,
    client_mootdx,
    MARKET_SZ,
    MARKET_SH,
    format_dt,
    path_user_home,
)
from analysis.base import feather_to_file, feather_from_file, code_code_to_ths


def mootdx_stocks() -> pd.DataFrame:
    filename_realtime_index = path_user_home.joinpath("mootdx_stocks.ftr")
    df_index_realtime = feather_from_file(filename_df=filename_realtime_index)
    if not df_index_realtime.empty:
        try:
            dt_stale = datetime.datetime.strptime(
                df_index_realtime.index.name, format_dt
            )
        except ValueError:
            dt_stale = dt_init
        if dt_stale >= dt_am_start:
            return df_index_realtime
    df_sh = client_mootdx.stocks(market=MARKET_SH)
    df_sh["symbol"] = df_sh["code"].apply(func=lambda x: "sh" + x)
    df_sz = client_mootdx.stocks(market=MARKET_SZ)
    df_sz["symbol"] = df_sz["code"].apply(func=lambda x: "sz" + x)
    df_stocks = pd.concat(objs=[df_sh, df_sz], axis=0)
    df_stocks.set_index(keys=["symbol"], inplace=True)
    df_stocks = df_stocks.reindex(columns=["name"])
    dt_now = datetime.datetime.now()
    str_str_dt_now_opened = dt_now.strftime(format_dt)
    df_stocks.index.rename(name=str_str_dt_now_opened, inplace=True)
    feather_to_file(df=df_stocks, filename_df=filename_realtime_index)
    return df_stocks


def mootdx_quotes(symbols=None) -> pd.DataFrame:
    if symbols is None:
        return pd.DataFrame()
    df_quotes = client_mootdx.quotes(symbol=symbols)
    df_stocks = mootdx_stocks()
    if not df_quotes.empty:
        df_quotes["symbol"] = df_quotes["code"].apply(func=code_code_to_ths)
        df_quotes.set_index(keys=["symbol"], inplace=True)
        df_quotes = df_quotes.reindex(
            columns=[
                "name",
                "price",
                "last_close",
                "open",
                "high",
                "low",
                "amount",
                "volume",
                "cur_vol",
                "s_vol",
                "b_vol",
            ]
        )
        df_quotes = df_quotes.reindex(index=symbols)
        df_quotes["name"] = df_quotes["name"].fillna(value="No_name")
        dt_now = datetime.datetime.now()
        df_quotes["servertime"] = dt_now
        df_quotes.fillna(value=0, inplace=True)
        for symbol in df_quotes.index:
            if symbol in df_stocks.index:
                df_quotes.at[symbol, "name"] = df_stocks.at[symbol, "name"]
    return df_quotes
