import random
import datetime
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    path_temp,
    path_chip,
    path_chip_csv,
    format_dt,
)
from analysis.log import logger
from analysis.base import feather_from_file, feather_to_file, get_financial_period
from analysis.finance import (
    get_all_fina_audit,
    get_cash_dividend,
    get_financial_indicator,
)
from analysis.pledge_state import get_pledge


def get_risk():
    name = "Risk"
    filename_risk = path_chip.joinpath("df_risk.ftr")
    df_risk = feather_from_file(filename_df=filename_risk)
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    if not df_risk.empty:
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from {filename_risk}.--trading time")
            return df_risk
        try:
            dt_stale = datetime.datetime.strptime(
                df_risk.index.name,
                format_dt,
            )
        except TypeError:
            dt_stale = dt_init
        if dt_stale >= dt_end:
            logger.debug(f"feather from {filename_risk}.")
            return df_risk
    filename_risk_temp = path_temp.joinpath("df_risk_temp.ftr")
    df_risk = feather_from_file(filename_df=filename_risk_temp)
    if df_risk.empty:
        df_all_fina_audit = get_all_fina_audit()
        df_cash_dividend = get_cash_dividend()
        df_financial_indicator = get_financial_indicator()
        df_pledge = get_pledge()
        df_risk = pd.concat(
            objs=[
                df_all_fina_audit,
                df_cash_dividend,
                df_financial_indicator,
                df_pledge,
            ],
            axis=1,
            join="outer",
        )
        df_risk["fina_audit_dt_end"] = df_risk["fina_audit_dt_end"].fillna(
            value=dt_init
        )
        df_risk["fina_indicator_dt_end"] = df_risk["fina_indicator_dt_end"].fillna(
            value=dt_init
        )
        df_risk["pledge_stat_dt_end"] = df_risk["pledge_stat_dt_end"].fillna(
            value=dt_init
        )
        df_risk["fina_audit_result"] = df_risk["fina_audit_result"].fillna(value="None")
        df_risk["risk"] = "None"
        df_risk.fillna(value=0.0, inplace=True)
        feather_to_file(df=df_risk, filename_df=filename_risk_temp)
    dt_now_1m1d = datetime.datetime(year=dt_end.year, month=1, day=1)
    days_1m1d = (dt_end - dt_now_1m1d).days
    if days_1m1d > 120:
        int_year0 = dt_end.year - 1
    else:
        int_year0 = dt_end.year - 2
    dt_financial_period = get_financial_period(dt=dt_now, pub=True)
    fina_audit_result_std = "标准无保留意见"
    i = 0
    count = df_risk.shape[0]
    for symbol in df_risk.index:
        i += 1
        str_msg = f"[{name}] - [{i:4d}/{count}] - [{symbol}]"
        if df_risk.at[symbol, "risk"] != "None":
            print(f"{str_msg} - Latest.")
            continue
        if df_risk.at[symbol, "fina_audit_dt_end"].year < int_year0:
            df_risk.at[symbol, "risk"] = "ST_audit_dt_end"
        elif df_risk.at[symbol, "fina_audit_result"] != fina_audit_result_std:
            df_risk.at[symbol, "risk"] = "ST_audit_result"
        elif df_risk.at[symbol, "fina_indicator_dt_end"] < dt_financial_period:
            df_risk.at[symbol, "risk"] = "ST_lt_financial_period"
        elif df_risk.at[symbol, "fina_indicator_roe_waa"] < -5:
            df_risk.at[symbol, "risk"] = "ST_Roe_lt_-5"
        elif df_risk.at[symbol, "pledge_stat_ratio"] > 50:
            df_risk.at[symbol, "risk"] = "ST_pledge_stat_ratio_gt_50"
        elif df_risk.at[symbol, "fina_indicator_debt_to_assets"] > 90:
            df_risk.at[symbol, "risk"] = "ST_Debt_to_assets_gt_90"
        elif df_risk.at[symbol, "cash_div_latest_year"] <= 0:
            df_risk.at[symbol, "risk"] = "ST_no_div_forever"
        elif df_risk.at[symbol, "cash_div_rate_period"] < 55:
            df_risk.at[symbol, "risk"] = "ST_div_rate_period_lt_55"
        elif df_risk.at[symbol, "fina_indicator_assets_turn"] <= 0:
            df_risk.at[symbol, "risk"] = "ST_assets_turn_lt_0"
        else:
            df_risk.at[symbol, "risk"] = "Pass"
        if random.randint(a=0, b=9) == 5:
            feather_to_file(df=df_risk, filename_df=filename_risk_temp)
        print(f"{str_msg} - [{df_risk.at[symbol, "risk"]}] - Update.")
    if i >= count:
        str_dt_end = dt_end.strftime(format_dt)
        df_risk.index.rename(name=str_dt_end, inplace=True)
        filename_risk_csv = path_chip_csv.joinpath("df_risk.csv")
        df_risk.to_csv(path_or_buf=filename_risk_csv)
        df_risk = df_risk[
            [
                "cash_div_tax",
                "cash_div_period",
                "cash_div_expected_period",
                "cash_div_latest_year",
                "cash_div_rate_period",
                "fina_indicator_assets_turn",
                "risk",
            ]
        ]
        feather_to_file(df=df_risk, filename_df=filename_risk)
        logger.debug(f"feather to {filename_risk}.")
        filename_risk_temp.unlink(missing_ok=True)
    return df_risk
