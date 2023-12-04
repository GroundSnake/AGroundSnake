import datetime
import os
import pandas as pd
from loguru import logger
from console import fg
import analysis.base
from analysis.const import (
    client_ts_pro,
    filename_chip_shelve,
    dt_pm_end,
    dt_pm_end_last_1T,
    path_check,
    str_trading_path,
)


def update_convertible_bonds_basic() -> bool:
    name: str = f"df_cb_basic"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Worth etf Break End")
        return True
    df_cb_basic = client_ts_pro.cb_basic()
    df_cb_basic["conv_start_date"] = pd.to_datetime(df_cb_basic["conv_start_date"])
    df_cb_basic["conv_end_date"] = pd.to_datetime(df_cb_basic["conv_end_date"])
    df_cb_basic = df_cb_basic[
        (df_cb_basic["bond_short_name"].str.contains("转").fillna(False))
        & (df_cb_basic["delist_date"].isnull())
        & (df_cb_basic["list_date"].notnull())
        & (df_cb_basic["conv_start_date"] < dt_pm_end)
        & (df_cb_basic["conv_end_date"] > dt_pm_end)
    ]
    df_cb_basic["symbol"] = df_cb_basic["stk_code"].apply(func=analysis.code_ts_to_ths)
    df_cb_basic["bond_symbol"] = df_cb_basic["ts_code"].apply(
        func=lambda x: x[-2:].lower() + x[:6]
    )
    df_cb_basic.set_index(keys=["bond_symbol"], inplace=True)
    df_cb_basic = df_cb_basic.reindex(
        columns=[
            "symbol",
            "conv_price",
            "conv_start_date",
            "conv_end_date",
            "bond_short_name",
            "stk_short_name",
        ]
    )
    analysis.base.write_obj_to_db(
        obj=df_cb_basic, key=name, filename=filename_chip_shelve
    )
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_cd_basic = dt_pm_end
    else:
        dt_cd_basic = dt_pm_end_last_1T
    analysis.base.set_version(key=name, dt=dt_cd_basic)
    return True


def realtime_cb() -> str:
    df_cb = analysis.read_df_from_db(
        key="df_cb_basic",
        filename=filename_chip_shelve,
    )
    df_cb = df_cb.reindex(
        columns=[
            "symbol",
            "pnl",
            "conv_price",
            "diff_value",
            "stock_price",
            "bonds_price",
            "conversion_value",
            "conv_start_date",
            "conv_end_date",
            "bond_short_name",
            "stk_short_name",
        ]
    )
    df_stock_realtime = analysis.realtime_quotations(
        stock_codes=df_cb["symbol"].to_list()
    )
    df_bonds_realtime = analysis.realtime_tdx(stock_codes=df_cb.index.to_list())
    for bond_symbol in df_cb.index:
        symbol = df_cb.at[bond_symbol, "symbol"]
        conv_price = df_cb.at[bond_symbol, "conv_price"]
        if symbol in df_stock_realtime.index:
            df_cb.at[bond_symbol, "stock_price"] = stock_price = df_stock_realtime.at[
                symbol, "close"
            ]
        else:
            df_cb.at[bond_symbol, "stock_price"] = stock_price = 0
        if bond_symbol in df_bonds_realtime.index:
            df_cb.at[bond_symbol, "bonds_price"] = bonds_price = df_bonds_realtime.at[
                bond_symbol, "price"
            ]
        else:
            df_cb.at[bond_symbol, "bonds_price"] = bonds_price = 0
        if conv_price > 0 and stock_price > 0:
            df_cb.at[bond_symbol, "conversion_value"] = (
                100 / conv_price * stock_price
            ).round(3)
            df_cb.at[bond_symbol, "diff_value"] = (
                df_cb.at[bond_symbol, "conversion_value"] - bonds_price
            )
        else:
            df_cb.at[bond_symbol, "conversion_value"] = 0
            df_cb.at[bond_symbol, "diff_value"] = 0
        if df_cb.at[bond_symbol, "bonds_price"] > 0:
            df_cb.at[bond_symbol, "pnl"] = (
                df_cb.at[bond_symbol, "diff_value"]
                / df_cb.at[bond_symbol, "bonds_price"]
                * 100
            ).round(2)
        else:
            df_cb.at[bond_symbol, "pnl"] = 0
    filename_cb_csv = os.path.join(
        path_check, f"convertible_bonds_{str_trading_path()}.csv"
    )
    df_cb.index.name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
    df_cb.sort_values(by=["pnl"], ascending=False, inplace=True)
    df_cb.to_csv(path_or_buf=filename_cb_csv)
    df_cb = df_cb[df_cb["pnl"] >= 0.5]
    str_return = ""
    df_trader = analysis.read_df_from_db(key="df_trader", filename=filename_chip_shelve)
    len_cb = df_cb.shape[0]
    i = 0
    for bond_symbol in df_cb.index:
        i += 1
        str_symbol = (
            f"[{df_cb.at[bond_symbol, 'bond_short_name']}({bond_symbol})"
            f"_{df_cb.at[bond_symbol, 'bonds_price']:.3f}] - "
            f"[{df_cb.at[bond_symbol, 'stk_short_name']}({df_cb.at[bond_symbol, 'symbol']})"
            f"_{df_cb.at[bond_symbol, 'stock_price']}] - "
            f"[{df_cb.at[bond_symbol, 'diff_value']:.2f}] - "
            f"[{df_cb.at[bond_symbol, 'conversion_value']:.3f}] - "
            f"[C_P:{df_cb.at[bond_symbol, 'conv_price']}] - "
            f"[PNL:{df_cb.at[bond_symbol, 'pnl']:.2f}%]"
        )
        if df_cb.at[bond_symbol, "symbol"] in df_trader.index:
            str_symbol = fg.red(str_symbol)
        elif df_cb.at[bond_symbol, "pnl"] >= 5:
            str_symbol = fg.red(str_symbol)
        elif df_cb.at[bond_symbol, "pnl"] >= 7:
            str_symbol = fg.purple(str_symbol)
        elif df_cb.at[bond_symbol, "pnl"] >= 9:
            str_symbol = fg.purple(str_symbol)
            str_symbol += "\a"
        if i < len_cb:
            str_symbol += "\n"
        str_return += str_symbol
    if str_return == "":
        return "No match convertible bonds."
    else:
        return str_return
