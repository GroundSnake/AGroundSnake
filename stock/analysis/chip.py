# modified at 2023/3/29 15:47
from __future__ import annotations
import datetime
import os
import sys
import tushare as ts
import pandas as pd
from pandas import DataFrame
from loguru import logger
import analysis.base
import analysis.golden
import analysis.limit
import analysis.update_data
import analysis.capital
import analysis.st
import analysis.industry


def chip() -> object | DataFrame:
    name: str = "df_chip"
    logger.trace(f"{name} Begin")
    dt_date_trading = analysis.base.latest_trading_day()
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    dt_init = datetime.datetime(year=1989, month=1, day=1)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    filename_chip_shelve = os.path.join(path_data, f"chip")
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    filename_excel = os.path.join(path_check, f"chip_{str_date_path}.xlsx")
    if analysis.base.is_latest_version(key=name):
        df_chip = analysis.base.read_obj_from_db(key=name, filename=filename_chip_shelve)
        logger.trace(f"{name} Break End")
        return df_chip
    logger.trace(f"Update {name}")
    analysis.update_data.update_stock_data()
    analysis.update_data.update_index_data(symbol="sh000001")
    analysis.update_data.update_index_data(symbol="sh000852")
    if analysis.golden.golden_price():
        df_golden = analysis.base.read_obj_from_db(key="df_golden", filename=filename_chip_shelve)
        logger.trace("load df_golden success")
    else:
        df_golden = pd.DataFrame()
        logger.trace("load df_golden fail")
    if analysis.limit.limit_count():
        df_limit = analysis.base.read_obj_from_db(key="df_limit", filename=filename_chip_shelve)
        logger.trace("load df_limit success")
    else:
        df_limit = pd.DataFrame()
        logger.trace("load df_limit fail")
    if analysis.capital.capital():
        df_cap = analysis.base.read_obj_from_db(key="df_cap", filename=filename_chip_shelve)
        logger.trace("load df_cap success")
    else:
        df_cap = pd.DataFrame()
        logger.trace("load df_cap fail")
    if analysis.st.st_income():
        df_st = analysis.base.read_obj_from_db(key="df_st", filename=filename_chip_shelve)
        logger.trace("load df_st success")
    else:
        df_st = pd.DataFrame()
        logger.trace("load df_st fail")
    if analysis.industry.ths_industry():
        df_industry = analysis.base.read_obj_from_db(key="df_industry", filename=filename_chip_shelve)
        logger.trace("load df_industry success")
    else:
        df_industry = pd.DataFrame()
        logger.trace("load df_industry fail")
    df_chip = pd.concat(
        objs=[df_cap, df_industry, df_golden, df_limit, df_st],
        axis=1,
        join="outer",
    )
    df_chip["list_date"] = df_chip["list_date"].apply(
        func=lambda x: (dt_pm_end - x).days
    )
    df_chip.rename(columns={"list_date": "list_days"}, inplace=True)
    df_chip["turnover"] = df_chip["total_volume"] / (df_chip["circ_cap"] / 100)
    df_chip["turnover"] = df_chip["turnover"].apply(func=lambda x: round(x, 2))
    df_chip["turnover"].fillna(value=0, inplace=True)
    df_chip.sort_values(
        by=["up_M_down", "now_price_ratio"], ascending=False, inplace=True
    )
    df_chip['dt'].fillna(value=dt_init, inplace=True)
    analysis.base.write_obj_to_db(obj=df_chip, key=name, filename=filename_chip_shelve)
    logger.trace(f"{name} save as [db_chip]")
    df_g_price_1 = df_chip[
        (df_chip["now_price_ratio"] <= 71.8) & (df_chip["now_price_ratio"] >= 51.8)
    ]
    # df_g_price_1 = df_g_price_1.copy()
    df_up_a_down_5pct_2 = df_chip[(df_chip["up_A_down_5pct"] >= 48)]
    df_tm_grade_3 = df_chip[
        (df_chip["T_m_pct_grade"] <= 38.2) & (df_chip["T_m_amplitude_grade"] <= 38.2)
    ]
    df_t5_pct_4 = df_chip[
        (df_chip["T_m_pct"] >= 10)
        & (df_chip["T5_pct"] >= 10)
        & (df_chip["T20_pct"] >= 10)
    ]
    df_t5_amplitude_5 = df_chip[
        (df_chip["T_m_amplitude"] >= 5)
        & (df_chip["T5_amplitude"] >= 5)
        & (df_chip["T20_amplitude"] >= 5)
    ]
    df_turnover_6 = df_chip[(df_chip["turnover"] >= 10)]
    df_stocks_pool = pd.concat(
        objs=[
            df_g_price_1,
            df_up_a_down_5pct_2,
            df_tm_grade_3,
            df_t5_pct_4,
            df_t5_amplitude_5,
            df_turnover_6,
        ],
        axis=0,
        join="outer",
    )
    df_stocks_pool = df_stocks_pool[~df_stocks_pool.index.duplicated(keep="first")]
    df_stocks_pool = df_stocks_pool[
        (df_stocks_pool["list_days"] > 540)
        & (df_stocks_pool["up_times"] >= 4)
        & (df_stocks_pool["now_price"] <= 25)
        & (df_stocks_pool["up_A_down_7pct"] >= 12)
        & (df_stocks_pool["up_A_down_5pct"] >= 24)
        & (df_stocks_pool["up_A_down_3pct"] >= 48)
        & (df_stocks_pool["turnover"] <= 40)
        & (~df_stocks_pool["name"].str.contains("ST").fillna(False))
        & (~df_stocks_pool["ST"].str.contains("ST").fillna(False))
    ]
    df_stocks_pool["factor_count"] = 1
    df_stocks_pool["factor"] = None
    list_game_over = df_stocks_pool.index.tolist()
    list_1 = df_g_price_1.index.tolist()
    list_2 = df_up_a_down_5pct_2.index.tolist()
    list_3 = df_tm_grade_3.index.tolist()
    list_4 = df_t5_pct_4.index.tolist()
    list_5 = df_t5_amplitude_5.index.tolist()
    list_6 = df_turnover_6.index.tolist()
    for symbol in list_game_over:
        if symbol in list_1:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[G_price]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[G_price]"
        if symbol in list_2:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[up_A_down_5pct]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[up_A_down_5pct]"
        if symbol in list_3:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[tm_grade]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[tm_grade]"
        if symbol in list_4:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[t5_pct]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[t5_pct]"
        if symbol in list_5:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[t5_amplitude]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[t5_amplitude]"
        if symbol in list_6:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[turnover]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[turnover]"
    df_stocks_pool = df_stocks_pool[df_stocks_pool["factor_count"] > 2]
    df_stocks_pool.sort_values(
        by=["factor_count", "factor"], ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(obj=df_stocks_pool, key="df_stocks_pool", filename=filename_chip_shelve)
    df_industry_rank = pd.DataFrame(
        columns=[
            "name",
            "T5",
            "T5_Zeroing_sort",
            "T5_rank",
            "T20",
            "T20_Zeroing_sort",
            "T20_rank",
            "T40",
            "T40_Zeroing_sort",
            "T40_rank",
            "T60",
            "T60_Zeroing_sort",
            "T60_rank",
            "T80",
            "T80_Zeroing_sort",
            "T80_rank",
            "rank",
            "max_min",
        ]
    )
    df_all_industry_pct = analysis.base.read_obj_from_db(key="df_all_industry_pct", filename=filename_chip_shelve)
    df_5_industry_pct = df_all_industry_pct.iloc[-5:]
    df_20_industry_pct = df_all_industry_pct.iloc[-20:-5]
    df_40_industry_pct = df_all_industry_pct.iloc[-40:-20]
    df_60_industry_pct = df_all_industry_pct.iloc[-60:-40]
    df_80_industry_pct = df_all_industry_pct.iloc[-80:-60]
    df_industry_rank["T5"] = (df_5_industry_pct.sum(axis=0) / 5 * 20).round(2)
    df_industry_rank["T20"] = (df_20_industry_pct.sum(axis=0) / 15 * 20).round(2)
    df_industry_rank["T40"] = df_40_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T60"] = df_60_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T80"] = df_80_industry_pct.sum(axis=0).round(2)
    df_industry_rank["T5_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T5"]
    )
    df_industry_rank["T5_rank"] = df_industry_rank["T5"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T20_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T20"]
    )
    df_industry_rank["T20_rank"] = df_industry_rank["T20"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T40_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T40"]
    )
    df_industry_rank["T40_rank"] = df_industry_rank["T40"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T60_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T60"]
    )
    df_industry_rank["T60_rank"] = df_industry_rank["T60"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["T80_Zeroing_sort"] = analysis.base.zeroing_sort(
        pd_series=df_industry_rank["T80"]
    )
    df_industry_rank["T80_rank"] = df_industry_rank["T80"].rank(
        axis=0, method="min", ascending=False
    )
    df_industry_rank["rank"] = (
            df_industry_rank["T5_rank"]
            + df_industry_rank["T20_rank"]
            + df_industry_rank["T40_rank"]
            + df_industry_rank["T60_rank"]
            + df_industry_rank["T80_rank"]
    )
    pro = ts.pro_api()
    df_ths_index = pro.ths_index()
    df_ths_index.set_index(keys="ts_code", inplace=True)
    for ths_index_code in df_industry_rank.index.tolist():
        if ths_index_code in df_ths_index.index.tolist():
            df_industry_rank.at[ths_index_code, "name"] = df_ths_index.at[
                ths_index_code, "name"
            ]
            df_industry_rank.at[ths_index_code, "max_min"] = max(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            ) - min(
                df_industry_rank.at[ths_index_code, "T5_rank"],
                df_industry_rank.at[ths_index_code, "T20_rank"],
                df_industry_rank.at[ths_index_code, "T40_rank"],
                df_industry_rank.at[ths_index_code, "T60_rank"],
                df_industry_rank.at[ths_index_code, "T80_rank"],
            )
    df_industry_rank.sort_values(
        by=["max_min"], axis=0, ascending=False, inplace=True
    )
    df_industry_rank = df_industry_rank[
        (df_industry_rank["T5_rank"] >= 66)
        | (df_industry_rank["T20_rank"] >= 66)
        | (df_industry_rank["T40_rank"] >= 66)
        | (df_industry_rank["T60_rank"] >= 66)
        | (df_industry_rank["T80_rank"] >= 66)
        ]
    df_industry_rank = df_industry_rank[
        (df_industry_rank["T5_rank"] <= 20)
        | (df_industry_rank["T20_rank"] <= 10)
        | (df_industry_rank["T40_rank"] <= 10)
        | (df_industry_rank["T60_rank"] <= 10)
        | (df_industry_rank["T80_rank"] <= 10)
        ]
    df_industry_rank.sort_values(
        by=["T5_rank"], axis=0, ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(obj=df_industry_rank, key="df_industry_rank", filename=filename_chip_shelve)
    analysis.base.shelve_to_excel(path_shelve=filename_chip_shelve, path_excel=filename_excel)
    analysis.base.set_version(key=name, dt=dt_pm_end)
    logger.trace("chip End")
    return df_chip


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sink=sys.stderr, level="INFO"
    )  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    chip()
