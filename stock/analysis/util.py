import datetime
import json
import random
import pandas as pd
from pathlib import Path
from analysis.const_dynamic import (
    dt_am_start,
    dt_pm_end,
    time_balance,
    dict_grid,
)
from analysis.log import logger
from analysis.base import code_ts_to_ths
from analysis.api_tushare import (
    cb_basic,
    fund_basic,
)
from analysis.api_tushare_const import (
    trade_cal,
    daily_basic,
)


def get_stock_code_chs(r: bool = True) -> list | None:
    df_basic = daily_basic()
    if len(df_basic) == 0:
        return
    else:
        list_chs_code = df_basic.index.tolist()
        if r:
            random.shuffle(list_chs_code)
        return list_chs_code


def get_etf_code_chs(r: bool = True) -> list | None:
    df_etf = fund_basic(market="E")
    df_etf = df_etf[
        (df_etf["name"].str.contains("ETF").fillna(False))
        & (df_etf["invest_type"].str.contains("被动指数型"))
        & (df_etf["fund_type"].str.contains("股票型"))
        & (df_etf["status"].str.contains("L"))
        & (df_etf["market"].str.contains("E"))
        & (df_etf["due_date"].isnull())
    ]
    list_ts_code = df_etf["ts_code"].tolist()
    list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
    if len(df_etf) == 0:
        return
    else:
        if r:
            random.shuffle(list_chs_code)
        return list_chs_code


def get_convertible_bonds() -> pd.DataFrame:
    name: str = f"df_cb_basic"
    logger.trace(f"update_convertible_bonds_basic [{name}] Begin!")
    df_cb_basic = cb_basic()
    df_cb_basic["conv_start_date"] = pd.to_datetime(df_cb_basic["conv_start_date"])
    df_cb_basic["conv_end_date"] = pd.to_datetime(df_cb_basic["conv_end_date"])
    df_cb_basic = df_cb_basic[
        (df_cb_basic["bond_short_name"].str.contains("转").fillna(False))
        & (~df_cb_basic["bond_short_name"].str.contains("定转").fillna(False))
        & (df_cb_basic["delist_date"].isnull())
        & (df_cb_basic["list_date"].notnull())
        & (df_cb_basic["conv_start_date"] < dt_pm_end)
        & (df_cb_basic["conv_end_date"] > dt_pm_end)
    ]
    df_cb_basic["symbol"] = df_cb_basic["stk_code"].apply(func=code_ts_to_ths)
    df_cb_basic = df_cb_basic.reindex(
        columns=[
            "symbol",
            "conv_price",
            "conv_start_date",
            "conv_end_date",
            "bond_short_name",
            "stk_short_name",
        ],
    )
    logger.trace(f"update_convertible_bonds_basic [{name}] End!")
    return df_cb_basic


def is_trading_day(dt: datetime.datetime = None) -> bool:
    if dt is None:
        dt = datetime.datetime.now().replace(microsecond=0)
    dt = datetime.datetime.combine(dt.date(), time_balance)
    df_trade_cal = trade_cal()
    if df_trade_cal.at[dt, "is_open"] == 1:
        return True
    else:
        return False


def json_grid_from_file(item: str, filename_json: Path) -> float:
    def json_to_file(obj: dict, filename: Path) -> bool:
        with open(file=filename, mode="w") as f_i:
            json.dump(obj=obj, fp=f_i, indent=1)
        return True

    json_obj_item = dict_grid[item]
    bool_json_reset = False
    dt_now = datetime.datetime.now()
    if dt_am_start < dt_now < dt_pm_end:
        if filename_json.exists():
            with open(file=filename_json, mode="r") as f:
                json_obj = json.load(fp=f)
                try:
                    json_obj_item = json_obj[item]
                except KeyError:
                    json_obj[item] = dict_grid[item]
                    json_obj_item = json_obj[item]
                    json_to_file(obj=json_obj, filename=filename_json)
        else:
            bool_json_reset = True
    else:
        bool_json_reset = True
    if bool_json_reset:
        json_obj = dict_grid
        json_obj_item = json_obj[item]
        json_to_file(obj=json_obj, filename=filename_json)
    return json_obj_item
