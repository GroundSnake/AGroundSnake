import datetime
from analysis.log import logger
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    path_chip,
    path_temp,
    path_chip_csv,
    phi_s_net,
    phi_s,
    phi_b,
    pct_c,
    amplitude_c,
    turnover_c,
    stock_top_c,
    format_dt,
)
from analysis.base import feather_from_file, feather_to_file, get_ratio_value
from analysis.kline_analysis import (
    get_kline_analysis_low_Frequency,
    get_kline_analysis_high_Frequency,
)
from analysis.rate_gt_price import get_rate_gt_price


def get_chip() -> pd.DataFrame:
    def std_golden_sort(data: list, reverse=False) -> tuple:
        phi_s_half = phi_s / 2
        return_value = (0, 0)
        if len(data) > 0:
            data = [x for x in data if 0 < x < 100]
            data = sorted(data, reverse=reverse)
        else:
            return return_value
        c = len(data)
        phi_min = 0.1
        phi_max = phi_s_net + phi_min
        index_phi_max = round(c * phi_max - 1)
        index_phi_min = round(c * phi_min - 1)
        golden_max = data[index_phi_max]
        golden_min = data[index_phi_min]
        if golden_min > phi_s_half:
            golden_min = round(phi_s_half, 2)
        elif golden_min < 10:
            golden_min = 10
        if golden_max < phi_s:
            golden_max = round(phi_s, 2)
        elif golden_max > phi_b:
            golden_max = round(phi_b, 2)
        return_value = (golden_min, golden_max)
        return return_value

    name = "Chip"
    filename_chip = path_chip.joinpath("df_chip.ftr")
    df_chip = feather_from_file(filename_df=filename_chip)
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    if not df_chip.empty:
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from {filename_chip}.--trading time")
            return df_chip
        try:
            dt_stale = datetime.datetime.strptime(
                df_chip.index.name,
                format_dt,
            )
        except TypeError:
            dt_stale = dt_init
        if dt_stale >= dt_end:
            logger.debug(f"feather from {filename_chip}.")
            return df_chip
    filename_chip_temp = path_temp.joinpath("df_chip_temp.ftr")
    df_chip = feather_from_file(filename_df=filename_chip_temp)
    if df_chip.empty:
        df_rate_gt_price = get_rate_gt_price()
        df_kline_analysis_low = get_kline_analysis_low_Frequency()
        df_kline_analysis_high = get_kline_analysis_high_Frequency()
        df_chip = pd.concat(
            objs=[df_rate_gt_price, df_kline_analysis_low, df_kline_analysis_high],
            axis=1,
            join="outer",
        )
        df_chip["rate_gt_price_dt_start"] = df_chip["rate_gt_price_dt_start"].fillna(
            value=dt_init
        )
        df_chip["rate_gt_price_dt_end"] = df_chip["rate_gt_price_dt_end"].fillna(
            value=dt_init
        )
        df_chip["index_code"] = df_chip["index_code"].fillna(value="index_code")
        df_chip["industry_name"] = df_chip["industry_name"].fillna(
            value="industry_name"
        )
        df_chip.fillna(value=0.0, inplace=True)
        df_chip["chip"] = "chip"
        feather_to_file(df=df_chip, filename_df=filename_chip_temp)
    i = 0
    count = df_chip.shape[0]
    dt_rate_end_max = df_chip["rate_gt_price_dt_end"].median()
    days_mean = df_chip["rate_gt_price_days"].mean()
    natr = get_ratio_value(df_chip["natr"].tolist(), frac=0.5)
    rv_10000 = get_ratio_value(df_chip["rv_10000"].tolist(), frac=0.5)
    if natr < 4.5:
        natr = 4.5
    tuple_std = std_golden_sort(data=df_chip["rate_gt_price"].tolist())
    rate_gt_price_min = tuple_std[0]
    rate_gt_price_max = tuple_std[1]
    str_rate_gt_price = f"({rate_gt_price_max}-{rate_gt_price_min})"
    df_chip = df_chip.sample(frac=1)
    df_chip.sort_values(by=["chip"], ascending=True, inplace=True)
    """
    pearson_corr: 为0表示没有相关性,为1正相关,为-1负相关.
    pearson_pvalue: 值表示对x和y不相关的零假设的检验(即真实总体相关系数为零)。
    因此,样本相关系数接近零(即弱相关)将趋向于为您提供较大的p值,
    而系数接近1或-1(即强正/负相关性)将为您提供较小的p值.

    cosine_sim greater than 0.985, cosine_sim >= 0.985
    """
    for symbol in df_chip.index:
        i += 1
        str_msg = f"[{name}] - [{i:4d}/{count}] - [{symbol}]"
        if df_chip.at[symbol, "chip"] != "chip":
            print(f"{str_msg} - Latest.")
            continue
        str_chip = ""
        bool_g_price = False
        bool_rv_10000 = False
        if (
            df_chip.at[symbol, "rate_gt_price_dt_end"] >= dt_rate_end_max
            and df_chip.at[symbol, "rate_gt_price_days"] >= days_mean
            and rate_gt_price_min
            < df_chip.at[symbol, "rate_gt_price"]
            < rate_gt_price_max
        ):
            bool_g_price = True
        if df_chip.at[symbol, "rv_10000"] > rv_10000:
            bool_rv_10000 = True
        if df_chip.at[symbol, "natr"] > natr:
            str_chip += f"_NATR_{natr}"
        if (
            (
                df_chip.at[symbol, "pearson_corr"] > 0.9
                or df_chip.at[symbol, "pearson_corr"] < -0.3
            )
            and df_chip.at[symbol, "pearson_pvalue"] < 0.005
            and df_chip.at[symbol, "cosine_sim"] > 0.985
        ):
            str_chip += "_Industry"
        if (
            df_chip.at[symbol, f"rate_daily_up_{pct_c}pct"] > 8
            and df_chip.at[symbol, f"rate_daily_down_{pct_c}pct"] > 8
            and df_chip.at[symbol, f"rate_daily_gt{amplitude_c}_amplitude"] > 15
        ):
            str_chip += "_Limit"
        if df_chip.at[symbol, f"rate_concentration_top{stock_top_c}"] > 0:
            str_chip += "_Concentration"
        if str_chip != "":
            if bool_g_price and bool_rv_10000:
                str_chip = f"Hit__G_price_{str_rate_gt_price}_RV_{rv_10000}{str_chip}"
            elif bool_g_price:
                str_chip = f"Failed__G_price_{str_rate_gt_price}{str_chip}"
            elif bool_rv_10000:
                str_chip = f"Failed__RV_{rv_10000}{str_chip}"
            else:
                str_chip = f"Failed__{str_chip}"
        else:
            str_chip = "Failed"
        df_chip.at[symbol, "chip"] = str_chip
        feather_to_file(df=df_chip, filename_df=filename_chip_temp)
        print(f"{str_msg} - Update - [{str_chip}].")
    if i >= count:
        dt_max = df_chip["rate_gt_price_dt_end"].max()
        str_dt_max = dt_max.strftime(format_dt)
        df_chip.index.rename(name=str_dt_max, inplace=True)
        filename_chip_csv = path_chip_csv.joinpath("df_chip.csv")
        df_chip.to_csv(path_or_buf=filename_chip_csv)
        df_chip = df_chip[
            [
                "index_code",
                "industry_name",
                "close",
                "rv_10000",
                "rate_gt_price",
                "natr",
                "bbands_width",
                f"rate_daily_up_{pct_c}pct",
                f"rate_daily_down_{pct_c}pct",
                f"rate_daily_gt{amplitude_c}_amplitude",
                f"rate_daily_gt{turnover_c}_turnover",
                f"rate_concentration_top{stock_top_c}",
                "amplitude_N_days",
                "chip",
            ]
        ]
        feather_to_file(df=df_chip, filename_df=filename_chip)
        logger.debug(f"feather to {filename_chip}.")
        filename_chip_temp.unlink(missing_ok=True)
    return df_chip
