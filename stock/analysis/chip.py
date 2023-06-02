# modified at 2023/05/18 22::25
from __future__ import annotations
import datetime
import time
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
import analysis.index
import analysis.concentration
from analysis.const import filename_chip_shelve, filename_chip_excel, dt_date_trading


def chip() -> object | DataFrame:
    name: str = "df_chip"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    dt_init = datetime.datetime(year=1989, month=1, day=1)
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        df_chip = analysis.base.read_df_from_db(key=name, filename=filename_chip_shelve)
        logger.trace(f"{name} Break End")
        return df_chip
    logger.trace(f"Update {name}")
    analysis.update_data.update_index_data(symbol="sh000001")
    analysis.update_data.update_index_data(symbol="sh000852")
    if analysis.golden.golden_price():
        df_golden = analysis.base.read_df_from_db(
            key="df_golden", filename=filename_chip_shelve
        )
        logger.trace("load df_golden success")
    else:
        df_golden = pd.DataFrame()
        logger.trace("load df_golden fail")
    if analysis.limit.limit_count():
        df_limit = analysis.base.read_df_from_db(
            key="df_limit", filename=filename_chip_shelve
        )
        logger.trace("load df_limit success")
    else:
        df_limit = pd.DataFrame()
        logger.trace("load df_limit fail")
    if analysis.capital.capital():
        df_cap = analysis.base.read_df_from_db(
            key="df_cap", filename=filename_chip_shelve
        )
        logger.trace("load df_cap success")
    else:
        df_cap = pd.DataFrame()
        logger.error("load df_cap fail")
    if analysis.st.st_income():
        df_st = analysis.base.read_df_from_db(
            key="df_st", filename=filename_chip_shelve
        )
        logger.trace("load df_st success")
    else:
        df_st = pd.DataFrame()
        logger.trace("load df_st fail")
    while True:
        if analysis.industry.industry_rank():
            df_industry_rank = analysis.base.read_df_from_db(
                key="df_industry_rank", filename=filename_chip_shelve
            )
            df_industry_rank_deviation = df_industry_rank[
                df_industry_rank["max_min"] >= 60
            ]
            list_industry_code_deviation = df_industry_rank_deviation.index.tolist()
            break
        else:
            print("Sleep 1 hour")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=3600)
            analysis.base.sleep_to_time(dt_time=dt_now_delta, seconds=10)
    if analysis.base.is_latest_version(
        key="df_stocks_in_ssb", filename=filename_chip_shelve
    ):
        index_ssb = analysis.index.IndexSSB(update=False)
    else:
        index_ssb = analysis.index.IndexSSB(update=True)
        dt_stocks_in_ssb = index_ssb.version()
        analysis.base.set_version(key="df_stocks_in_ssb", dt=dt_stocks_in_ssb)
    if analysis.industry.reset_industry_member():
        pass
    df_stocks_in_ssb = index_ssb.stocks_in_ssb()
    if analysis.concentration():
        df_concentration = analysis.base.read_df_from_db(
            key="df_concentration", filename=filename_chip_shelve
        )
    else:
        df_concentration = pd.DataFrame()
        logger.error("load df_concentration fail")
    if analysis.industry.ths_industry():
        df_industry = analysis.base.read_df_from_db(
            key="df_industry", filename=filename_chip_shelve
        )
        logger.trace("load df_industry success")
    else:
        df_industry = pd.DataFrame()
        logger.trace("load df_industry fail")
    df_chip = pd.concat(
        objs=[
            df_cap,
            df_stocks_in_ssb,
            df_st,
            df_concentration,
            df_industry,
            df_golden,
            df_limit,
        ],
        axis=1,
        join="outer",
    )
    df_chip["list_date"].fillna(value=dt_date_trading, inplace=True)
    df_chip["list_date"] = df_chip["list_date"].apply(
        func=lambda x: (dt_date_trading - x).days
    )
    df_chip.rename(columns={"list_date": "list_days"}, inplace=True)
    df_chip["turnover"] = df_chip["total_volume"] / (df_chip["circ_cap"] / 100)
    df_chip["turnover"] = df_chip["turnover"].apply(func=lambda x: round(x, 2))
    df_chip["turnover"].fillna(value=0, inplace=True)
    df_chip.sort_values(by=["T5_pct"], ascending=False, inplace=True)
    df_chip["dt"].fillna(value=dt_init, inplace=True)
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
    df_alpha_7 = df_chip[
        (df_chip["alpha_times"] >= 70) & (df_chip["alpha_mean"] >= 1.5)
    ]
    df_stocks_pool = pd.concat(
        objs=[
            df_g_price_1,
            df_up_a_down_5pct_2,
            df_tm_grade_3,
            df_t5_pct_4,
            df_t5_amplitude_5,
            df_turnover_6,
            df_alpha_7,
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
        & (df_stocks_pool["industry_code"].isin(values=list_industry_code_deviation))
        & (~df_stocks_pool["name"].str.contains("ST").fillna(False))
        & (~df_stocks_pool["ST"].str.contains("ST").fillna(False))
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
        if symbol in df_up_a_down_5pct_2.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[up_A_down_5pct]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[up_A_down_5pct]"
        if symbol in df_tm_grade_3.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[tm_grade]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[tm_grade]"
        if symbol in df_t5_pct_4.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[t5_pct]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[t5_pct]"
        if symbol in df_t5_amplitude_5.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[t5_amplitude]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[t5_amplitude]"
        if symbol in df_turnover_6.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[turnover]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[turnover]"
        if symbol in df_alpha_7.index:
            if pd.notnull(df_stocks_pool.at[symbol, "factor"]):
                df_stocks_pool.at[symbol, "factor"] += ",[alpha]"
                df_stocks_pool.at[symbol, "factor_count"] += 1
            else:
                df_stocks_pool.at[symbol, "factor"] = "[turnover]"
    df_stocks_pool = df_stocks_pool[df_stocks_pool["factor_count"] > 2]
    df_stocks_pool.sort_values(
        by=["factor_count", "factor"], ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(
        obj=df_stocks_pool, key="df_stocks_pool", filename=filename_chip_shelve
    )
    df_config = analysis.base.read_df_from_db(
        key="df_config", filename=filename_chip_shelve
    )
    try:
        df_config_temp = df_config.drop(index=[name])
    except KeyError as e:
        print(f"[{name}] is not found in df_config -Error[{repr(e)}]")
        logger.trace(f"[{name}] is not found in df_config -Error[{repr(e)}]")
        df_config_temp = df_config.copy()
    analysis.base.shelve_to_excel(
        filename_shelve=filename_chip_shelve, filename_excel=filename_chip_excel
    )
    dt_chip = df_config_temp["date"].min()
    analysis.base.set_version(key=name, dt=dt_chip)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Chip analysis takes [{str_gm}]")
    logger.trace("Chip End")
    return df_chip
