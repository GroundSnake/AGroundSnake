import datetime
import pandas as pd
from analysis.log import logger
from analysis.const_dynamic import (
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    filename_pool_history,
    filename_analysis,
    filename_analysis_csv,
    filename_pool,
    filename_pool_csv,
    filename_pool_history_csv,
    format_dt,
    amplitude_c,
    pct_c,
    turnover_c,
    stock_top_c,
    close_price_max,
    df_daily_basic,
)
from analysis.base import feather_from_file, feather_to_file, get_ratio_value
from analysis.risk import get_risk
from analysis.chip import get_chip
from analysis.update_data import update_data


def get_analysis(
    symbols: list | str | None = None,
    fields: str | None = None,
    trading: bool = False,
) -> pd.DataFrame:
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    df_analysis = feather_from_file(filename_df=filename_analysis)
    bool_reset_analysis = True
    if not df_analysis.empty:
        if dt_balance < dt_now < dt_pm_end or trading:
            logger.debug(f"feather from [{filename_analysis}]. --trading time.")
            bool_reset_analysis = False
        dt_stale = datetime.datetime.strptime(df_analysis.index.name, format_dt)
        if dt_stale >= dt_end:
            logger.debug(f"feather from [{filename_analysis}].")
            bool_reset_analysis = False
    if bool_reset_analysis:
        update_data()
        df_analysis = df_daily_basic[["name", "list_days", "total_mv_E"]]
        df_risk = get_risk()
        df_chip = get_chip()
        df_analysis = pd.concat(
            objs=[df_analysis, df_chip, df_risk], axis=1, join="outer"
        )
        df_analysis["name"] = df_analysis["name"].fillna(value="None")
        df_analysis["index_code"] = df_analysis["index_code"].fillna(value="None")
        df_analysis["industry_name"] = df_analysis["industry_name"].fillna(value="None")
        df_analysis["chip"] = df_analysis["chip"].fillna(value="Failed")
        df_analysis["risk"] = df_analysis["risk"].fillna(value="ST")
        df_analysis.fillna(value=0.0, inplace=True)
        str_end = dt_end.strftime(format_dt)
        df_analysis.index.rename(name=str_end, inplace=True)
        df_analysis.to_csv(path_or_buf=filename_analysis_csv)
        feather_to_file(df=df_analysis, filename_df=filename_analysis)
        logger.debug(f"feather to {filename_analysis}.")
    if isinstance(symbols, str):
        symbols = [symbols]
    if fields == "simple":
        df_analysis = df_analysis.reindex(
            columns=[
                "total_mv_E",
                "industry_name",
                "rv_10000",
                "rate_gt_price",
                "natr",
                "bbands_width",
                "cash_div_tax",
                "cash_div_latest_year",
                "cash_div_rate_period",
                f"rate_daily_up_{pct_c}pct",
                f"rate_daily_down_{pct_c}pct",
                f"rate_daily_gt{amplitude_c}_amplitude",
                f"rate_daily_gt{turnover_c}_turnover",
                f"rate_concentration_top{stock_top_c}",
                "amplitude_N_days",
                "chip",
                "risk",
            ]
        )
    if symbols is not None:
        df_analysis = df_analysis.reindex(index=symbols)
    df_analysis.dropna(inplace=True)
    return df_analysis


def get_pool(treading: bool = False) -> pd.DataFrame:
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    df_pool = feather_from_file(filename_df=filename_pool)
    if treading:
        return df_pool
    if not df_pool.empty:
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from [{filename_pool}]. --trading time.")
            return df_pool
        dt_stale = datetime.datetime.strptime(df_pool.index.name, format_dt)
        if dt_stale >= dt_end:
            logger.debug(f"feather from [{filename_pool}].")
            return df_pool
    df_analysis = get_analysis()
    mv_mim_e = get_ratio_value(data=df_analysis["total_mv_E"].tolist())
    rate_up_n_pct_median = df_analysis[f"rate_daily_up_{pct_c}pct"].median()
    rate_down_n_pct_median = df_analysis[f"rate_daily_down_{pct_c}pct"].median()
    rate_gt_n_amplitude_median = df_analysis[
        f"rate_daily_gt{amplitude_c}_amplitude"
    ].median()
    rate_gt_n_turnover_median = df_analysis[
        f"rate_daily_gt{turnover_c}_turnover"
    ].median()
    df_pool = df_analysis[
        (~df_analysis.index.str.contains("688"))
        & (~df_analysis.index.str.contains("bj"))
        & (~df_analysis["risk"].str.contains("ST"))
        & (~df_analysis["chip"].str.contains("Failed"))
        & (~df_analysis["name"].str.contains("ST"))
        & (df_analysis["close"] <= close_price_max)
        & (df_analysis["total_mv_E"] > mv_mim_e)
        & (df_analysis["list_days"] > 360)
        & (df_analysis[f"rate_daily_up_{pct_c}pct"] > rate_up_n_pct_median)
        & (df_analysis[f"rate_daily_down_{pct_c}pct"] > rate_down_n_pct_median)
        & (
            df_analysis[f"rate_daily_gt{amplitude_c}_amplitude"]
            > rate_gt_n_amplitude_median
        )
        & (
            df_analysis[f"rate_daily_gt{turnover_c}_turnover"]
            > rate_gt_n_turnover_median
        )
    ].copy()
    dt_add_pool = dt_end
    df_pool_history = feather_from_file(filename_df=filename_pool_history)
    if not df_pool.empty:
        feather_to_file(df=df_pool, filename_df=filename_pool)
        df_pool.to_csv(path_or_buf=filename_pool_csv)
        if df_pool_history.empty:
            df_pool_history = pd.DataFrame(columns=df_analysis.index.tolist())
        else:
            df_pool_history.reindex(columns=df_analysis.index.tolist())
            df_pool_history.fillna(value=0.0, inplace=True)
        for index in df_pool_history.columns:
            if index in df_pool.index:
                df_pool_history.at[dt_add_pool, index] = 1
            else:
                df_pool_history.at[dt_add_pool, index] = 0
        df_pool_history.sort_index(
            axis=0,
            inplace=True,
            ascending=False,
        )
    dt_max = df_pool_history.index.max()
    str_max = dt_max.strftime(format_dt)
    df_pool_history.index.rename(name=str_max, inplace=True)
    df_pool_history.to_csv(path_or_buf=filename_pool_history_csv)
    feather_to_file(df=df_pool_history, filename_df=filename_pool_history)
    logger.debug(f"feather to {filename_pool}.")
    return df_pool
