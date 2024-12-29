import datetime
import random
from analysis.log import logger
from scipy.spatial.distance import cosine
from scipy.stats import pearsonr
import pandas as pd
import pandas_ta as ta
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    path_chip,
    path_temp,
    path_chip_csv,
    format_dt,
    pct_c,
    amplitude_c,
    turnover_c,
    stock_top_c,
    df_daily_basic,
)
from analysis.base import feather_from_file, feather_to_file
from analysis.update_data import kline_industry_index, kline
from analysis.industry import get_industry_member
from analysis.concentration import get_concentration_stocks
from analysis.rv import volatility_yz_rv_mean


def get_kline_analysis_low_Frequency(frequency: str = "D", days: int = 180):
    name = f"Kline_Analysis_Low_{frequency}"
    filename_kline_analysis = path_chip.joinpath(
        f"df_kline_analysis_low_{frequency}.ftr",
    )
    filename_kline_analysis_csv = path_chip_csv.joinpath(
        f"df_kline_analysis_low_{frequency}.csv"
    )
    filename_kline_analysis_temp = path_temp.joinpath(
        f"df_kline_analysis_low_{frequency}_temp.ftr"
    )
    df_kline_analysis_low = feather_from_file(filename_df=filename_kline_analysis)
    if not df_kline_analysis_low.empty:
        dt_now = datetime.datetime.now()
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from {filename_kline_analysis}.--trading time")
            return df_kline_analysis_low
        try:
            dt_temp = datetime.datetime.strptime(
                df_kline_analysis_low.index.name,
                format_dt,
            )
        except TypeError:
            dt_temp = dt_init
        if dt_temp >= dt_pm_end:
            logger.debug(f"feather from {filename_kline_analysis}.")
            return df_kline_analysis_low
    df_kline_analysis_low = feather_from_file(filename_df=filename_kline_analysis_temp)
    if df_kline_analysis_low.empty:
        df_industry_member = get_industry_member()
        df_kline_analysis_low = df_industry_member
        df_kline_analysis_low = df_kline_analysis_low.reindex(
            columns=[
                "index_code",
                "industry_name",
                "index_code_is_pub",
                "pearson_corr",
                "pearson_corr_abs",
                "pearson_pvalue",
                "cosine_sim",
                f"rate_daily_up_{pct_c}pct",
                f"rate_daily_down_{pct_c}pct",
                f"rate_daily_gt{amplitude_c}_amplitude",
                f"rate_daily_gt{turnover_c}_turnover",
                f"rate_concentration_top{stock_top_c}",
                "amplitude_N_days",
                "natr",
                "bbands_width",
            ],
        )
        df_kline_analysis_low = df_kline_analysis_low[
            df_kline_analysis_low.index.isin(values=df_daily_basic.index.tolist())
        ]
        df_kline_analysis_low.fillna(value=0.0, inplace=True)
        str_dt_init = dt_init.strftime(format_dt)
        df_kline_analysis_low.index.rename(name=str_dt_init, inplace=True)
        feather_to_file(
            df=df_kline_analysis_low, filename_df=filename_kline_analysis_temp
        )
    df_concentration = get_concentration_stocks(top=stock_top_c)
    all_concentration_days = df_concentration.shape[0]
    dt_start = dt_pm_end - datetime.timedelta(days=days)
    dt_end = dt_pm_end
    df_kline_analysis_low = df_kline_analysis_low.sample(frac=1)
    df_kline_analysis_low.sort_values(by=["cosine_sim"], ascending=False, inplace=True)
    count = df_kline_analysis_low.shape[0]
    i = 0
    for symbol in df_kline_analysis_low.index:
        i += 1
        index_code = df_kline_analysis_low.at[symbol, "index_code"]
        str_msg = f"[{name}]- [{i:4d}/{count}] - [{symbol} - {index_code}]"
        if df_kline_analysis_low.at[symbol, "cosine_sim"] > 0:
            print(f"{str_msg} - Exist")
            continue
        df_kline = kline(
            symbol=symbol,
            frequency=frequency,
            adjust="qfq",
            asset="E",
            start_date=dt_start,
            end_date=dt_end,
        )
        if df_kline.empty:
            logger.error(f"{str_msg} - kline is empty")
            continue
        if df_kline_analysis_low.at[symbol, "index_code_is_pub"] == 1:
            df_kline_index = kline_industry_index(
                ts_code=index_code,
                frequency=frequency,
                dt_start=dt_start,
                dt_end=dt_end,
            )
        else:
            logger.error(f"{index_code} index is not publish.")
            continue
        if df_kline_index.empty:
            logger.error(f"{str_msg} - kline_index is empty")
            continue
        dt_max_kline = df_kline.index.max()
        df_kline_index = df_kline_index[["close"]]
        df_kline_index.rename(
            columns={
                "close": "industry_close",
            },
            inplace=True,
        )
        dt_max_kline_index = df_kline_index.index.max()
        df_kline = pd.concat(
            objs=[df_kline, df_kline_index],
            axis=1,
            join="outer",
        )
        df_kline.dropna(subset=["close"], inplace=True)
        df_kline = df_kline.ffill(axis=0)
        df_kline = df_kline.bfill(axis=0)
        # dt_max = max(dt_max_kline, dt_max_kline_index)
        close_max = df_kline["close"].max()
        close_min = df_kline["close"].min()
        close_median = df_kline["close"].median()
        df_kline["pct_chg"] = (
            (df_kline["close"] - df_kline["pre_close"]) / df_kline["pre_close"] * 100
        )
        df_kline["amplitude"] = (
            (df_kline["high"] - df_kline["low"]) / df_kline["pre_close"] * 100
        )
        df_kline["turnover"] = (
            df_kline["volume"] * 10000 / df_daily_basic.at[symbol, "circ_cap"] * 100
        )
        all_count_kline = df_kline.shape[0]
        close_kline = df_kline["close"]
        close_kline_industry = df_kline["industry_close"]
        """
        pearson_corr: 为0表示没有相关性,为1正相关,为-1负相关.
        pearson_pvalue: 值表示对x和y不相关的零假设的检验(即真实总体相关系数为零)。
        因此,样本相关系数接近零(即弱相关)将趋向于为您提供较大的p值,
        而系数接近1或-1(即强正/负相关性)将为您提供较小的p值.
        """
        pearson_corr, pearson_p_value = pearsonr(close_kline, close_kline_industry)
        pearson_corr_abs = abs(pearson_corr)
        """ 
        cosine_sim greater than 0.985, cosine_sim >= 0.985
        """
        cosine_sim = 1 - cosine(close_kline, close_kline_industry)
        df_kline_analysis_low.at[symbol, "pearson_corr"] = pearson_corr
        df_kline_analysis_low.at[symbol, "pearson_corr_abs"] = pearson_corr_abs
        df_kline_analysis_low.at[symbol, "pearson_pvalue"] = pearson_p_value
        df_kline_analysis_low.at[symbol, "cosine_sim"] = cosine_sim
        df_up_n_pct = df_kline[df_kline["pct_chg"] > pct_c]
        df_kline_analysis_low.at[symbol, f"rate_daily_up_{pct_c}pct"] = round(
            df_up_n_pct.shape[0] / all_count_kline * 100, 2
        )
        df_daily_down_n_pct = df_kline[df_kline["pct_chg"] < -pct_c]
        df_kline_analysis_low.at[symbol, f"rate_daily_down_{pct_c}pct"] = round(
            df_daily_down_n_pct.shape[0] / all_count_kline * 100, 2
        )
        df_daily_gt_n_amplitude = df_kline[df_kline["amplitude"] > amplitude_c]
        df_kline_analysis_low.at[symbol, f"rate_daily_gt{amplitude_c}_amplitude"] = (
            round(df_daily_gt_n_amplitude.shape[0] / all_count_kline * 100, 2)
        )
        df_daily_gt_n_turnover = df_kline[df_kline["turnover"] > turnover_c]
        df_kline_analysis_low.at[symbol, f"rate_daily_gt{turnover_c}_turnover"] = round(
            df_daily_gt_n_turnover.shape[0] / all_count_kline * 100, 2
        )
        if symbol in df_concentration.columns:
            df_kline_analysis_low.at[symbol, f"rate_concentration_top{stock_top_c}"] = (
                round(df_concentration[symbol].sum() / all_concentration_days * 100, 2)
            )
        df_kline_analysis_low.at[symbol, "amplitude_N_days"] = round(
            (close_max - close_min) / close_median * 100, 2
        )
        if df_kline.shape[0] > 14:
            ser_n_atr = df_kline.ta.natr(length=14)
            ser_n_atr.dropna(inplace=True)
            df_kline_analysis_low.at[symbol, "natr"] = round(
                (
                    ser_n_atr.max()
                    + ser_n_atr.min()
                    + ser_n_atr.median()
                    + ser_n_atr.mean()
                )
                / 4,
                2,
            )
        else:
            df_kline_analysis_low.at[symbol, "natr"] = 0.0
        if df_kline.shape[0] > 5:
            df_b_bands = df_kline.ta.bbands(length=5, std=1.65)
            df_b_bands = df_b_bands.rename(columns=lambda x: x[:3])
            df_b_bands.dropna(subset=["BBB"], inplace=True)
            df_kline_analysis_low.at[symbol, "bbands_width"] = round(
                (
                    df_b_bands["BBB"].max()
                    + df_b_bands["BBB"].min()
                    + df_b_bands["BBB"].median()
                    + df_b_bands["BBB"].mean()
                )
                / 4,
                2,
            )
        else:
            df_kline_analysis_low.at[symbol, "bbands_width"] = 0.0
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_kline_analysis_low, filename_df=filename_kline_analysis_temp
            )
        print(f"{str_msg} - [{dt_max_kline}] - [{dt_max_kline_index}] - " f"Update")
    if i >= count:
        str_dt_end = dt_end.strftime(format_dt)
        df_kline_analysis_low.index.rename(name=str_dt_end, inplace=True)
        df_kline_analysis_low.to_csv(path_or_buf=filename_kline_analysis_csv)
        feather_to_file(df=df_kline_analysis_low, filename_df=filename_kline_analysis)
        logger.debug(f"feather to {filename_kline_analysis}.")
        filename_kline_analysis_temp.unlink(missing_ok=True)
    return df_kline_analysis_low


def get_kline_analysis_high_Frequency(frequency: str = "1m", days: int = 20):
    name = f"Kline_Analysis_high_{frequency}"
    filename_kline_analysis_high = path_chip.joinpath(
        f"df_kline_analysis_high_{frequency}.ftr",
    )
    filename_kline_analysis_high_csv = path_chip_csv.joinpath(
        f"df_kline_analysis_high_{frequency}.csv"
    )
    filename_kline_analysisHigh_temp = path_temp.joinpath(
        f"df_kline_analysis_high_{frequency}_temp.ftr"
    )
    rv_days = days
    days = rv_days + 15
    df_kline_analysis_high = feather_from_file(filename_df=filename_kline_analysis_high)
    dt_now = datetime.datetime.now()
    if not df_kline_analysis_high.empty:
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from {filename_kline_analysis_high}.--trading time")
            return df_kline_analysis_high
        try:
            dt_temp = datetime.datetime.strptime(
                df_kline_analysis_high.index.name,
                format_dt,
            )
        except TypeError:
            dt_temp = dt_init
        if dt_temp >= dt_pm_end:
            logger.debug(f"feather from {filename_kline_analysis_high}.")
            return df_kline_analysis_high
    df_kline_analysis_high = feather_from_file(
        filename_df=filename_kline_analysisHigh_temp
    )
    if df_kline_analysis_high.empty:
        df_kline_analysis_high = df_daily_basic.copy()
        df_kline_analysis_high = df_kline_analysis_high.reindex(
            columns=[
                "rv_10000",
                "rv_10000_count",
            ],
        )
        df_kline_analysis_high = df_kline_analysis_high[
            df_kline_analysis_high.index.isin(values=df_daily_basic.index.tolist())
        ]
        df_kline_analysis_high.fillna(value=0.0, inplace=True)
        str_dt_init = dt_init.strftime(format_dt)
        df_kline_analysis_high.index.rename(name=str_dt_init, inplace=True)
        feather_to_file(
            df=df_kline_analysis_high, filename_df=filename_kline_analysisHigh_temp
        )
    dt_start = dt_pm_end - datetime.timedelta(days=days)
    if dt_now > dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    df_kline_analysis_high = df_kline_analysis_high.sample(frac=1)
    df_kline_analysis_high.sort_values(by=["rv_10000"], ascending=False, inplace=True)
    count = df_kline_analysis_high.shape[0]
    i = 0
    for symbol in df_kline_analysis_high.index:
        i += 1
        str_msg = f"[{name}]- [{i:4d}/{count}] - [{symbol}]"
        if df_kline_analysis_high.at[symbol, "rv_10000_count"] > 0:
            print(f"{str_msg} - Exist")
            continue
        df_kline = kline(
            symbol=symbol,
            frequency=frequency,
            adjust="qfq",
            asset="E",
            start_date=dt_start,
            end_date=dt_end,
        )
        if df_kline.empty:
            logger.error(f"{str_msg} - kline is empty")
            continue
        dt_max_kline = df_kline.index.max()
        list_rv_mean = volatility_yz_rv_mean(data=df_kline, days=rv_days)
        df_kline_analysis_high.at[symbol, "rv_10000"] = list_rv_mean[0]
        df_kline_analysis_high.at[symbol, "rv_10000_count"] = list_rv_mean[1]
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_kline_analysis_high, filename_df=filename_kline_analysisHigh_temp
            )
        print(f"{str_msg} - [{dt_max_kline}] - " f"Update")
    if i >= count:
        str_dt_end = dt_end.strftime(format_dt)
        df_kline_analysis_high.index.rename(name=str_dt_end, inplace=True)
        df_kline_analysis_high.to_csv(path_or_buf=filename_kline_analysis_high_csv)
        feather_to_file(
            df=df_kline_analysis_high, filename_df=filename_kline_analysis_high
        )
        logger.debug(f"feather to {filename_kline_analysis_high}.")
        filename_kline_analysisHigh_temp.unlink(missing_ok=True)
    return df_kline_analysis_high
