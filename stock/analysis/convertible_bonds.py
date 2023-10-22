import datetime

import feather
import pandas as pd
from loguru import logger
import analysis.base
from analysis.const import client_ts_pro, filename_chip_shelve, dt_pm_end, client_mootdx


def update_convertible_bonds_basic() -> bool:
    name: str = f"df_convertible_bonds_basic"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Worth etf Break End")
        return True
    df_cb = client_ts_pro.cb_basic()
    df_cb["conv_start_date"] = pd.to_datetime(df_cb["conv_start_date"])
    df_cb["conv_end_date"] = pd.to_datetime(df_cb["conv_end_date"])
    df_cb.to_csv("df_cb_98.csv")
    df_cb = df_cb[
        df_cb["delist_date"].isnull()
        & df_cb["list_date"].notnull()
        & (df_cb["conv_start_date"] < dt_pm_end)
        & (df_cb["conv_end_date"] > dt_pm_end)
        & (~df_cb["bond_short_name"].str.contains("定").fillna(False))
    ]
    df_cb["symbol"] = df_cb["stk_code"].apply(func=analysis.code_ts_to_ths)
    df_cb["bond_symbol"] = df_cb["ts_code"].apply(func=lambda x: x[-2:].lower() + x[:6])
    df_cb.set_index(keys=["bond_symbol"], inplace=True)
    df_cb = df_cb.reindex(
        columns=[
            "symbol",
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
            df_cb.at[bond_symbol, "conversion_value"] = round(
                100 / conv_price * stock_price, 3
            )
            df_cb.at[bond_symbol, "diff_value"] = (
                df_cb.at[bond_symbol, "conversion_value"] - bonds_price
            )
        else:
            df_cb.at[bond_symbol, "conversion_value"] = 0
            df_cb.at[bond_symbol, "diff_value"] = 0

    print(df_cb)
    df_cb.to_csv("df_cb.csv")
    # df_bonds_realtime.to_csv("df_bonds_realtime.csv")
    """
    analysis.base.write_obj_to_db(
        obj=df_cb, key=name, filename=filename_chip_shelve
    )
    analysis.base.set_version(key=name, dt=dt_pm_end)
    """
    return True


def convertible_bonds():
    pass
