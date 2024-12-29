import datetime
import random
import pandas as pd
from analysis.log import logger
from console import fg
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    time_balance,
    path_chip,
    path_temp,
    path_chip_csv,
    format_dt,
    df_daily_basic,
)
from analysis.base import feather_from_file, feather_to_file, get_financial_period
from analysis.api_tushare import fina_audit, dividend, fina_indicator


def get_all_fina_audit(year: int = None):  # get_financial_indicator
    """
    获取上市公司定期财务审计意见数据
    """
    if year is None:
        year = dt_pm_end.year - 1
    name: str = f"df_all_fina_audit_{year}"
    filename_df_all_fina_audit = path_chip.joinpath(
        f"df_fina_audit_{year}.ftr",
    )
    df_all_fina_audit = feather_from_file(filename_df=filename_df_all_fina_audit)
    if not df_all_fina_audit.empty:
        dt_now = datetime.datetime.now()
        if dt_balance < dt_now < dt_pm_end:
            logger.debug("feather from file.----trading time")
            return df_all_fina_audit
        df_all_fina_audit = df_all_fina_audit.reindex(
            index=df_daily_basic.index.tolist(),
        )
        df_all_fina_audit["fina_audit_dt_end"] = df_all_fina_audit[
            "fina_audit_dt_end"
        ].fillna(value=dt_init)
        df_all_fina_audit["fina_audit_result"] = df_all_fina_audit[
            "fina_audit_result"
        ].fillna(value="None")
        dt_stale = df_all_fina_audit["fina_audit_dt_end"].min().year
        if dt_stale >= year:
            logger.debug("feather from file")
            return df_all_fina_audit
    else:
        df_all_fina_audit = df_daily_basic.copy()
        df_all_fina_audit = df_all_fina_audit.reindex(
            columns=[
                "fina_audit_dt_end",
                "fina_audit_result",
            ],
        )
        df_all_fina_audit["fina_audit_dt_end"] = dt_init
        df_all_fina_audit["fina_audit_result"] = "None"
    df_all_fina_audit = df_all_fina_audit.reindex(
        index=df_daily_basic.index.tolist(),
    )
    df_all_fina_audit["fina_audit_dt_end"] = df_all_fina_audit[
        "fina_audit_dt_end"
    ].fillna(value=dt_init)
    df_all_fina_audit["fina_audit_result"] = df_all_fina_audit[
        "fina_audit_result"
    ].fillna(value="None")
    df_all_fina_audit = df_all_fina_audit.sample(frac=1)
    df_all_fina_audit.sort_values(
        by=["fina_audit_dt_end"],
        ascending=False,
        inplace=True,
    )
    dt_period = datetime.datetime(year=year, month=12, day=31)
    i = 0
    count = df_all_fina_audit.shape[0]
    for symbol in df_all_fina_audit.index:
        i += 1
        str_msg_bar = f"[{name}] - [{symbol}] - [{i:4d}/{count:4d}]"
        if df_all_fina_audit.at[symbol, "fina_audit_dt_end"].year >= year:
            print(
                f"{str_msg_bar} - Latest\033[K",
            )
            continue
        df_fina_audit = fina_audit(symbol=symbol, dt_period=dt_period)
        if df_fina_audit.empty:
            logger.error(f"{str_msg_bar} - Empty.")
            continue
        index_max = df_fina_audit["ann_date"].idxmax()
        df_all_fina_audit.at[symbol, "fina_audit_result"] = df_fina_audit.at[
            index_max, "audit_result"
        ]
        df_all_fina_audit.at[symbol, "fina_audit_dt_end"] = datetime.datetime.combine(
            df_fina_audit.at[index_max, "end_date"].date(), time_balance
        )
        if random.randint(0, 19) == 10:
            feather_to_file(
                df=df_all_fina_audit,
                filename_df=filename_df_all_fina_audit,
            )
        print(
            f"{str_msg_bar} - Update\033[K",
        )
    if i >= count:
        str_min_year = str(df_all_fina_audit["fina_audit_dt_end"].min().year)
        df_all_fina_audit.index.rename(name=str_min_year, inplace=True)
        filename_df_all_fina_audit_csv = path_chip_csv.joinpath(
            f"df_fina_audit_{year}.csv",
        )
        df_all_fina_audit.to_csv(path_or_buf=filename_df_all_fina_audit_csv)
        feather_to_file(df=df_all_fina_audit, filename_df=filename_df_all_fina_audit)
        logger.debug(f"feather to {filename_df_all_fina_audit}.")
    return df_all_fina_audit


def get_cash_dividend():
    name = "Cash_dividend"
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    filename_df_cash_dividend = path_chip.joinpath(
        "df_cash_dividend.ftr",
    )
    df_cash_dividend = feather_from_file(filename_df=filename_df_cash_dividend)
    if not df_cash_dividend.empty:
        if dt_balance <= dt_now <= dt_pm_end:
            logger.debug(f"feather from {filename_df_cash_dividend}.--trading time")
            return df_cash_dividend
        try:
            dt_temp = datetime.datetime.strptime(
                df_cash_dividend.index.name,
                format_dt,
            )
        except TypeError:
            dt_temp = dt_init
        if dt_temp >= dt_end:
            logger.debug(f"feather from {filename_df_cash_dividend}.")
            return df_cash_dividend
    filename_df_cash_dividend_temp = path_temp.joinpath(
        "df_cash_dividend_temp.ftr",
    )
    df_cash_dividend = feather_from_file(filename_df=filename_df_cash_dividend_temp)
    if df_cash_dividend.empty:
        df_cash_dividend = df_daily_basic.copy()
        df_cash_dividend = df_cash_dividend.reindex(
            columns=[
                "list_days",
                "cash_div_tax",
                "cash_div_period",
                "cash_div_expected_period",
                "cash_div_rate_period",
                "cash_div_latest_year",
                "cash_div_period_set",
            ],
        )
        df_cash_dividend["cash_div_tax"] = -1.0
        df_cash_dividend["cash_div_period_set"] = "None"
        df_cash_dividend.fillna(value=0.0, inplace=True)
        str_dt_init = dt_init.strftime(format_dt)
        df_cash_dividend.index.rename(name=str_dt_init, inplace=True)
        feather_to_file(df=df_cash_dividend, filename_df=filename_df_cash_dividend_temp)
    df_cash_dividend = df_cash_dividend.sample(frac=1)
    df_cash_dividend.sort_values(
        by=["cash_div_tax"],
        ascending=False,
        inplace=True,
    )
    int_year_3y = dt_end.year - 3
    int_year_y0 = dt_end.year - 1
    dt_now_1m1d = datetime.datetime(year=dt_end.year, month=1, day=1)
    days_1m1d = (dt_end - dt_now_1m1d).days
    i = 0
    count = df_cash_dividend.shape[0]
    for symbol in df_cash_dividend.index:
        i += 1
        str_msg = f"[{name}] - [{i}/{count}] - [{symbol}]"
        if df_cash_dividend.at[symbol, "cash_div_tax"] >= 0:
            print(f"{str_msg} Exist.")
            continue
        df_dividend = dividend(symbol=symbol)
        if df_dividend.empty:
            df_cash_dividend.at[symbol, "cash_div_tax"] = cash_div_tax = 0
            logger.error(f"{str_msg} - [{cash_div_tax}] - No dividend")
            continue
        df_dividend["end_date_year"] = df_dividend["end_date"].dt.year
        df_dividend = df_dividend[df_dividend["end_date_year"] >= int_year_3y]
        df_dividend_year = pd.pivot_table(
            data=df_dividend,
            index=["end_date_year"],
            aggfunc={
                "cash_div_tax": "sum",
            },
        )
        if df_dividend_year.empty:
            df_cash_dividend.at[symbol, "cash_div_tax"] = cash_div_tax = 0
            logger.error(f"{str_msg} - [{cash_div_tax}] - No latest dividend")
            continue
        df_cash_dividend.at[symbol, "cash_div_latest_year"] = index_max = (
            df_dividend_year.index.max()
        )
        df_cash_dividend.at[symbol, "cash_div_tax"] = cash_div_tax = round(
            df_dividend_year.at[index_max, "cash_div_tax"],
            3,
        )
        df_cash_dividend.at[symbol, "cash_div_period"] = df_dividend_year.shape[0]
        cash_div_expected_period = df_cash_dividend.at[symbol, "list_days"] // 365
        if days_1m1d >= 120:
            cash_div_expected_period += 1
        if cash_div_expected_period > 3:
            df_cash_dividend.at[symbol, "cash_div_expected_period"] = 3
        else:
            df_cash_dividend.at[symbol, "cash_div_expected_period"] = (
                cash_div_expected_period
            )
        if df_cash_dividend.at[symbol, "cash_div_expected_period"] > 0:
            df_cash_dividend.at[symbol, "cash_div_rate_period"] = round(
                df_cash_dividend.at[symbol, "cash_div_period"]
                / df_cash_dividend.at[symbol, "cash_div_expected_period"]
                * 100,
                2,
            )
        df_cash_dividend.at[symbol, "cash_div_period_set"] = repr(
            df_dividend_year.index.tolist()
        )
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_cash_dividend, filename_df=filename_df_cash_dividend_temp
            )
        str_msg += f" - [{cash_div_tax:5.3f}] - [{index_max}] - Update."
        if index_max >= int_year_y0:
            str_msg = fg.green(str_msg)
        print(str_msg)
    if i >= count:
        str_dt_catalogue_max = dt_end.strftime(format_dt)
        df_cash_dividend.index.rename(name=str_dt_catalogue_max, inplace=True)
        df_cash_dividend.sort_values(by=["cash_div_tax"], ascending=False, inplace=True)
        filename_df_cash_dividend_csv = path_chip_csv.joinpath(
            "df_cash_dividend.csv",
        )
        df_cash_dividend.to_csv(
            path_or_buf=filename_df_cash_dividend_csv,
        )
        df_cash_dividend = df_cash_dividend[
            [
                "cash_div_tax",
                "cash_div_period",
                "cash_div_expected_period",
                "cash_div_rate_period",
                "cash_div_latest_year",
            ]
        ]
        feather_to_file(df=df_cash_dividend, filename_df=filename_df_cash_dividend)
        logger.debug(f"feather to {filename_df_cash_dividend}.")
        filename_df_cash_dividend_temp.unlink(missing_ok=True)
    return df_cash_dividend


def get_financial_indicator() -> pd.DataFrame:
    name = "Financial_indicator"
    dt_now = datetime.datetime.now()
    dt_financial_period = get_financial_period(dt=dt_pm_end)
    filename_financial_indicator = path_chip.joinpath(
        "df_financial_indicator.ftr",
    )
    df_indicator = feather_from_file(
        filename_df=filename_financial_indicator,
    )
    if not df_indicator.empty:
        if dt_balance <= dt_now <= dt_pm_end:
            logger.debug(f"feather from {filename_financial_indicator}.--trading time")
            df_indicator = df_indicator[
                [
                    "fina_indicator_dt_end",
                    "fina_indicator_roe_waa",
                    "fina_indicator_debt_to_assets",
                    "fina_indicator_assets_turn",
                ]
            ]
            return df_indicator
        try:
            dt_stale = datetime.datetime.strptime(
                df_indicator.index.name,
                format_dt,
            )
        except TypeError:
            dt_stale = dt_init
        if dt_stale >= dt_financial_period:
            logger.debug(f"feather from [{filename_financial_indicator}].")
            df_indicator = df_indicator[
                [
                    "fina_indicator_dt_end",
                    "fina_indicator_roe_waa",
                    "fina_indicator_debt_to_assets",
                    "fina_indicator_assets_turn",
                ]
            ]
            return df_indicator
    else:
        df_indicator = df_daily_basic.copy()
        df_indicator = df_indicator.reindex(
            columns=[
                "fina_indicator_dt_end",
                "fina_indicator_roe_waa",
                "fina_indicator_debt_to_assets",
                "fina_indicator_assets_turn",
                "fina_indicator_dt_update",
            ]
        )
        df_indicator["fina_indicator_dt_end"] = dt_init
        df_indicator["fina_indicator_dt_update"] = dt_init
        str_dt_init = dt_init.strftime(format_dt)
        df_indicator.index.rename(name=str_dt_init, inplace=True)
        feather_to_file(df=df_indicator, filename_df=filename_financial_indicator)
    df_indicator = df_indicator.reindex(index=df_daily_basic.index.tolist())
    df_indicator["fina_indicator_dt_end"] = df_indicator[
        "fina_indicator_dt_end"
    ].fillna(value=dt_init)
    df_indicator["fina_indicator_dt_update"] = df_indicator[
        "fina_indicator_dt_update"
    ].fillna(value=dt_init)
    df_indicator.fillna(value=0.0, inplace=True)
    df_indicator = df_indicator.sample(frac=1)
    df_indicator.sort_values(
        by=["fina_indicator_dt_end"], ascending=False, inplace=True
    )
    dt_end = dt_pm_end
    dt_start = dt_end - datetime.timedelta(days=130)
    i = 0
    count = df_indicator.shape[0]
    for symbol in df_indicator.index:
        i += 1
        str_msg = f"[{name}] - [{i:4d}/{count}] - [{symbol}]"
        if df_indicator.at[symbol, "fina_indicator_dt_end"] >= dt_financial_period:
            print(
                f"{str_msg} - [{df_indicator.at[symbol, "fina_indicator_dt_end"]}] - Exist."
            )
            continue
        if df_indicator.at[symbol, "fina_indicator_dt_update"] >= dt_pm_end:
            print(
                f"{str_msg} - [{df_indicator.at[symbol, "fina_indicator_dt_end"]}] - Check today."
            )
            continue
        else:
            df_indicator.at[symbol, "fina_indicator_dt_update"] = dt_pm_end
        ser_fina_indicator = fina_indicator(
            symbol=symbol, dt_start=dt_start, dt_end=dt_end
        )
        if ser_fina_indicator.empty:
            logger.error(f"{str_msg} - No data")
            continue
        df_indicator.at[symbol, "fina_indicator_dt_end"] = fina_indicator_dt_end = (
            ser_fina_indicator.name
        )
        df_indicator.at[symbol, "fina_indicator_roe_waa"] = ser_fina_indicator[
            "roe_waa"
        ]
        df_indicator.at[symbol, "fina_indicator_debt_to_assets"] = ser_fina_indicator[
            "debt_to_assets"
        ]
        df_indicator.at[symbol, "fina_indicator_assets_turn"] = ser_fina_indicator[
            "assets_turn"
        ]
        if random.randint(a=0, b=9) == 5:
            feather_to_file(df=df_indicator, filename_df=filename_financial_indicator)
        print(f"{str_msg} - [{fina_indicator_dt_end}] - Update.")
    if i >= count:
        dt_min = df_indicator["fina_indicator_dt_end"].min()
        str_dt_min = dt_min.strftime(format_dt)
        df_indicator.index.rename(name=str_dt_min, inplace=True)
        filename_financial_indicator_csv = path_chip_csv.joinpath(
            "df_financial_indicator.csv",
        )
        df_indicator.to_csv(path_or_buf=filename_financial_indicator_csv)
        feather_to_file(
            df=df_indicator,
            filename_df=filename_financial_indicator,
        )
        logger.debug(f"feather to [{filename_financial_indicator}].")
    df_indicator = df_indicator[
        [
            "fina_indicator_dt_end",
            "fina_indicator_roe_waa",
            "fina_indicator_debt_to_assets",
            "fina_indicator_assets_turn",
        ]
    ]
    return df_indicator
