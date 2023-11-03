# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import datetime
import sys
import time
import pandas as pd
from pandas import DataFrame
from loguru import logger
import analysis.base
import analysis.unit_net
import analysis.g_price
import analysis.limit
import analysis.update_data
import analysis.capital
import analysis.st
import analysis.industry
import analysis.index
import analysis.concentration
import analysis.dividend
from analysis.const import (
    filename_chip_shelve,
    path_check,
    dt_history,
    phi_b_neg,
    dt_recent_fiscal_start,
    INDUSTRY_MAX_MIN,
    G_PRICE_MAX,
    lIST_DAYS_MAX,
    TOTAL_MV_E_MAX,
)


def chip() -> object | DataFrame:
    name: str = "df_chip"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        df_chip = analysis.base.read_df_from_db(key=name, filename=filename_chip_shelve)
        logger.trace(f"{name} Break End")
        return df_chip
    logger.trace(f"Update {name}")
    analysis.unit_net.unit_net()
    if analysis.g_price.golden_price():
        df_golden = analysis.base.read_df_from_db(
            key="df_golden", filename=filename_chip_shelve
        )
        logger.trace("load df_golden success")
    else:
        df_golden = pd.DataFrame()
        logger.error("load df_golden fail")
    if analysis.limit.limit_count():
        df_limit = analysis.base.read_df_from_db(
            key="df_limit", filename=filename_chip_shelve
        )
        logger.trace("load df_limit success")
    else:
        df_limit = pd.DataFrame()
        logger.error("load df_limit fail")
    if analysis.st.st_income():
        df_st = analysis.base.read_df_from_db(
            key="df_st", filename=filename_chip_shelve
        )
        logger.trace("load df_st success")
    else:
        df_st = pd.DataFrame()
        logger.error("load df_st fail")
    if analysis.dividend.cash_dividend():
        df_cash_div = analysis.base.read_df_from_db(
            key="df_cash_div", filename=filename_chip_shelve
        )
        logger.trace("load df_cash_div success")
    else:
        df_cash_div = pd.DataFrame()
        logger.error("load df_cash_div fail")
    while True:
        if analysis.industry.industry_rank():
            df_industry_rank = analysis.base.read_df_from_db(
                key="df_industry_rank", filename=filename_chip_shelve
            )
            df_industry_rank_deviation = df_industry_rank[
                df_industry_rank["max_min"] >= INDUSTRY_MAX_MIN
            ]
            list_industry_code_deviation = df_industry_rank_deviation.index.tolist()
            if analysis.industry.ths_industry():
                df_industry = analysis.base.read_df_from_db(
                    key="df_industry", filename=filename_chip_shelve
                )
                logger.trace("load df_industry success")
            else:
                df_industry = pd.DataFrame()
                logger.error("load df_industry fail")
            break
        else:
            print("Sleep 1 hour")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=3600)
            analysis.base.sleep_to_time(dt_time=dt_now_delta, seconds=10)
    if analysis.capital.capital():
        df_cap = analysis.base.read_df_from_db(
            key="df_cap", filename=filename_chip_shelve
        )
        logger.trace("load df_cap success")
    else:
        df_cap = pd.DataFrame()
        logger.error("load df_cap fail")
    if analysis.base.is_latest_version(
        key="df_stocks_in_ssb", filename=filename_chip_shelve
    ):
        index_ssb = analysis.index.IndexSSB(update=False)
    else:
        index_ssb = analysis.index.IndexSSB(update=True)
        dt_stocks_in_ssb = index_ssb.version()
        analysis.base.set_version(key="df_stocks_in_ssb", dt=dt_stocks_in_ssb)
    df_stocks_in_ssb = index_ssb.stocks_in_ssb()
    if analysis.concentration.concentration():
        df_concentration = analysis.base.read_df_from_db(
            key="df_concentration", filename=filename_chip_shelve
        )
    else:
        df_concentration = pd.DataFrame()
        logger.error("load df_concentration fail")
    if analysis.limit.worth_etf():
        pass
    else:
        logger.error("load df_cap fail")
    analysis.update_data.update_index_data(symbol="000001")
    analysis.update_data.update_index_data(symbol="000852")
    str_pos_ctl_zh = analysis.position(index="sh000001")
    print(str_pos_ctl_zh)
    str_pos_ctl_csi1000 = analysis.position(index="sh000852")
    print(str_pos_ctl_csi1000)
    if (
        df_cap.empty
        or df_stocks_in_ssb.empty
        or df_st.empty
        or df_concentration.empty
        or df_golden.empty
        or df_limit.empty
        or df_cash_div.empty
    ):
        logger.error("df_chip missing Table entry")
        sys.exit()
    df_chip = pd.concat(
        objs=[
            df_cap,
            df_stocks_in_ssb,
            df_st,
            df_cash_div,
            df_concentration,
            df_industry,
            df_golden,
            df_limit,
        ],
        axis=1,
        join="outer",
    )
    df_chip.dropna(subset=["name"], inplace=True)
    df_chip["profit_rate"].fillna(value=0, inplace=True)
    df_chip["cash_div_tax"].fillna(value=0, inplace=True)
    df_chip["now_price"].fillna(value=0, inplace=True)
    for symbol in df_chip.index:
        if (
            df_chip.at[symbol, "cash_div_tax"] > 0
            and df_chip.at[symbol, "now_price"] > 0
        ):
            df_chip.at[symbol, "dividend_rate"] = round(
                df_chip.at[symbol, "cash_div_tax"]
                / df_chip.at[symbol, "now_price"]
                * 100,
                2,
            )
        else:
            df_chip.at[symbol, "dividend_rate"] = 0
    analysis.base.write_obj_to_db(obj=df_chip, key=name, filename=filename_chip_shelve)
    df_g_price_1 = df_chip[
        (df_chip["now_price_ratio"] <= 71.8) & (df_chip["now_price_ratio"] >= 51.8)
    ]
    df_limit_2 = df_chip[
        (df_chip["correct_3pct_times"] >= 30)
        & (df_chip["alpha_pct"] >= 3)
        & (df_chip["alpha_amplitude"] >= 1)
        & (df_chip["alpha_turnover"] >= 3)
    ]
    df_exceed_industry_3 = df_chip[
        (df_chip["times_exceed_correct_industry"] >= 60)
        & (df_chip["mean_exceed_correct_industry"] >= 1.3)
    ]
    df_concentration_4 = df_chip[
        (df_chip["rate_concentration"] >= 60)
        & (df_chip["days_latest_concentration"] <= 7)
    ]
    df_stocks_pool = pd.concat(
        objs=[
            df_g_price_1,
            df_limit_2,
            df_exceed_industry_3,
            df_concentration_4,
        ],
        axis=0,
        join="outer",
    )
    df_stocks_pool = df_stocks_pool[~df_stocks_pool.index.duplicated(keep="first")]
    list_st = ["A+", "A-", "B+", "C+"]
    # list_ssb_index = ["ssb_tail", "ssb_2000"]
    df_stocks_pool = df_stocks_pool[
        (df_stocks_pool["list_days"] > lIST_DAYS_MAX)
        & (df_stocks_pool["correct_7pct_times"] > 1)
        & (df_stocks_pool["now_price_ratio"].between(36.8, 86.8))
        & (df_stocks_pool["G_price"] <= G_PRICE_MAX)
        & (df_stocks_pool["total_mv_E"] <= TOTAL_MV_E_MAX)
        & (df_stocks_pool["industry_code"].isin(values=list_industry_code_deviation))
        & (~df_stocks_pool["name"].str.contains("ST").fillna(False))
        & (df_stocks_pool["ST"].isin(values=list_st))
        & (~df_stocks_pool.index.str.contains("sh68"))
        & (~df_stocks_pool.index.str.contains("bj"))
        & (df_stocks_pool["times_exceed_correct_industry"] >= 35)
        & (df_stocks_pool["mean_exceed_correct_industry"] >= 0.7)
        & (df_stocks_pool["profit_rate"] > phi_b_neg)
        & (df_stocks_pool["dividend_rate"] > 0)
        & (df_stocks_pool["cash_div_end_dt"] >= dt_recent_fiscal_start)
    ]
    df_stocks_pool["factor_count"] = 1
    df_stocks_pool["factor"] = None
    for symbol in df_stocks_pool.index:
        if symbol in df_g_price_1.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[G_price]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[G_price]"
        if symbol in df_limit_2.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[limit]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[limit]"
        if symbol in df_exceed_industry_3.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[exceed_industry]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[exceed_industry]"
        if symbol in df_concentration_4.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[concentration]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[concentration]"
    # df_stocks_pool = df_stocks_pool[df_stocks_pool["factor_count"] > 2]
    df_stocks_pool.sort_values(
        by=["factor_count", "factor"], ascending=False, inplace=True
    )
    df_stocks_pool = df_stocks_pool[
        [
            "name",
            "list_days",
            "correct_7pct_times",
            "now_price_ratio",
            "G_price",
            "total_mv_E",
            "industry_code",
            "industry_name",
            "ST",
            "times_exceed_correct_industry",
            "mean_exceed_correct_industry",
            "profit_rate",
            "dividend_rate",
            "cash_div_end_dt",
            "factor_count",
            "factor",
            "now_price",
        ]
    ]
    analysis.base.write_obj_to_db(
        obj=df_stocks_pool, key="df_stocks_pool", filename=filename_chip_shelve
    )
    df_config = analysis.base.read_df_from_db(
        key="df_config", filename=filename_chip_shelve
    )
    try:
        df_config_temp = df_config.drop(index=[name])
    except KeyError as e:
        print(f"[{name}] is not found in df_config - Error[{repr(e)}]")
        logger.trace(f"[{name}] is not found in df_config - Error[{repr(e)}]")
        df_config_temp = df_config.copy()
    dt_chip = df_config_temp["date"].min()
    analysis.base.set_version(key=name, dt=dt_chip)
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    filename_chip_excel = os.path.join(path_check, f"chip_{str_dt_history_path}.xlsx")
    if not analysis.base.shelve_to_excel(
        filename_shelve=filename_chip_shelve, filename_excel=filename_chip_excel
    ):
        logger.error(f"{name} Save Error")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Chip analysis takes [{str_gm}]")
    logger.trace(f"{name} End")
    return df_chip
