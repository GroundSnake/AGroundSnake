import time
import datetime
from requests import exceptions
import pandas as pd
from analysis.log import log_json, logger
from analysis.const_dynamic import (
    dt_init,
    dt_am_start,
    dt_pm_end,
    client_ts_pro,
    time_pm_end,
    time_balance,
    path_temp,
    format_dt,
    format_date,
    float_time_sleep,
)
from analysis.base import (
    feather_from_file,
    feather_to_file,
    code_ts_to_ths,
    code_ths_to_ts,
    get_financial_period,
    pickle_from_file,
    pickle_to_file,
    get_latest_friday,
)

dt_now = datetime.datetime.now()


def fund_basic(market="E"):
    filename_fund_basic_temp = path_temp.joinpath(
        f"df_fund_basic_{market}.ftr",
    )
    df_fund_basic = feather_from_file(filename_df=filename_fund_basic_temp)
    if not df_fund_basic.empty:
        dt_stale_fund_basic = datetime.datetime.strptime(
            df_fund_basic.index.name, format_dt
        )
        if dt_stale_fund_basic >= dt_pm_end:
            logger.debug(f"feather from [{filename_fund_basic_temp}]")
            return df_fund_basic
    log_json(item="fund_basic_TS")
    df_fund_basic = client_ts_pro.fund_basic(market=market)
    if not df_fund_basic.empty:
        df_fund_basic.index.name = dt_pm_end.strftime(format_dt)
        feather_to_file(df=df_fund_basic, filename_df=filename_fund_basic_temp)
        logger.debug(f"feather to [{filename_fund_basic_temp}]")
    return df_fund_basic


def fina_audit(symbol: str, dt_period: datetime.datetime) -> pd.DataFrame:
    year = dt_period.year
    filename_fina_audit_temp = path_temp.joinpath(
        f"df_fina_audit_{symbol}_{year}.ftr",
    )
    df_fina_audit = feather_from_file(filename_df=filename_fina_audit_temp)
    if not df_fina_audit.empty:
        dt_stale_fina_audit = int(df_fina_audit.index.name)
        if dt_stale_fina_audit >= year:
            logger.debug(f"feather from [{filename_fina_audit_temp}].")
            return df_fina_audit
    ts_code = code_ths_to_ts(symbol)
    str_dt_period = dt_period.strftime(format_date)
    i_while = 0
    while i_while < 2:
        i_while += 1
        try:
            log_json(item="fina_audit_TS")
            df_fina_audit = client_ts_pro.fina_audit(
                ts_code=ts_code, period=str_dt_period
            )
        except Exception as e:
            df_fina_audit = pd.DataFrame()
            logger.error(f"[{ts_code}] - fina_audit empty.--{repr(e)}")
        if not df_fina_audit.empty:
            break
        time.sleep(float_time_sleep)
    if not df_fina_audit.empty:
        df_fina_audit.drop_duplicates(
            subset=["ann_date", "end_date", "audit_result"],
            keep="first",
            inplace=True,
            ignore_index=True,
        )
        df_fina_audit["end_date"] = df_fina_audit["end_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_balance
            )
        )
        df_fina_audit["ann_date"] = df_fina_audit["ann_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_balance
            )
        )
        dt_max = df_fina_audit["end_date"].max()
        df_fina_audit.index.name = dt_max.strftime("%Y")
        feather_to_file(df=df_fina_audit, filename_df=filename_fina_audit_temp)
        logger.debug(f"feather to [{filename_fina_audit_temp}] - [{dt_max}]")
    return df_fina_audit


def dividend(symbol: str) -> pd.DataFrame:
    filename_dividend_temp = path_temp.joinpath(
        f"df_dividend_{symbol}.ftr",
    )
    df_dividend = feather_from_file(filename_df=filename_dividend_temp)
    if not df_dividend.empty:
        try:
            dt_stale_dividend = datetime.datetime.strptime(
                df_dividend.index.name, format_dt
            )
        except ValueError:
            dt_stale_dividend = dt_init
        if dt_stale_dividend >= dt_pm_end:
            logger.debug(
                f"feather from [{filename_dividend_temp}] - [{dt_stale_dividend}]",
            )
            return df_dividend
    ts_code = code_ths_to_ts(symbol)
    i_while = 0
    while i_while < 2:
        i_while += 1
        try:
            log_json(item="dividend_TS")
            df_dividend = client_ts_pro.dividend(
                ts_code=ts_code,
            )
        except exceptions as e:
            logger.error(f"dividend - [{symbol}] - {repr(e)}")
            df_dividend = pd.DataFrame()
        except TypeError as e:
            logger.error(f"dividend - [{symbol}] - {repr(e)}")
            df_dividend = pd.DataFrame()
        if df_dividend.empty:
            logger.error(f"dividend - [{symbol}] - empty.")
        else:
            logger.trace(f"dividend - [{symbol}] - update.")
            break
        time.sleep(float_time_sleep)
    if not df_dividend.empty:
        df_dividend = df_dividend[
            df_dividend["div_proc"].str.contains("实施").fillna(False)
            & (df_dividend["cash_div_tax"] > 0)
        ]
        df_dividend.drop_duplicates(keep="first", inplace=True)
        df_dividend.dropna(subset=["end_date", "ann_date"], axis=0, inplace=True)
        df_dividend["end_date"] = df_dividend["end_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_pm_end,
            )
        )
        df_dividend["ann_date"] = df_dividend["ann_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_pm_end,
            )
        )
        str_dt_now_closed = dt_pm_end.strftime(format_dt)
        df_dividend.index.rename(name=str_dt_now_closed, inplace=True)
        feather_to_file(df=df_dividend, filename_df=filename_dividend_temp)
        logger.debug(f"feather from [{filename_dividend_temp}] - [{dt_pm_end}]")
    return df_dividend


def cb_basic(fields: str = None) -> pd.DataFrame:
    filename_cb_basic_temp = path_temp.joinpath(f"cb_basic.ftr")
    df_cb_basic = feather_from_file(filename_df=filename_cb_basic_temp)
    if not df_cb_basic.empty:
        dt_stale = datetime.datetime.strptime(df_cb_basic.index.name, format_dt)
        if dt_stale >= dt_am_start:
            logger.debug(f"feather to [{filename_cb_basic_temp}] - [{dt_stale}]")
            return df_cb_basic
    i_while = 0
    while i_while < 2:
        i_while += 1
        log_json(item="cb_basic_TS")
        df_cb_basic = client_ts_pro.cb_basic(fields=fields)
        if not df_cb_basic.empty:
            break
        time.sleep(float_time_sleep)
    df_cb_basic.dropna(
        subset=[
            "ts_code",
            "stk_code",
            "value_date",
            "list_date",
        ],
        inplace=True,
    )
    df_cb_basic["bond_symbol"] = df_cb_basic["ts_code"].apply(
        func=code_ts_to_ths,
    )
    if not df_cb_basic.empty:
        df_cb_basic.set_index(keys=["bond_symbol"], inplace=True)
        str_dt_now_opened = dt_am_start.strftime(format_dt)
        df_cb_basic.index.rename(name=str_dt_now_opened, inplace=True)
        feather_to_file(df=df_cb_basic, filename_df=filename_cb_basic_temp)
        logger.debug(f"feather to [{filename_cb_basic_temp}] - [{dt_am_start}]")
    return df_cb_basic


def fina_indicator(
    symbol: str, dt_start: datetime.datetime, dt_end: datetime.datetime
) -> pd.Series:
    dt_financial_period = get_financial_period(dt=dt_end)
    filename_fina_indicator_temp = path_temp.joinpath(
        f"ser_fina_indicator_{symbol}.pkl"
    )
    ser_fina_indicator = pickle_from_file(filename_ser=filename_fina_indicator_temp)
    if not ser_fina_indicator.empty:
        dt_stale = pd.to_datetime(ser_fina_indicator.name)
        if dt_stale >= dt_financial_period:
            logger.debug(
                f"pickle from [{filename_fina_indicator_temp}] - [{dt_stale}]",
            )
            return ser_fina_indicator
    str_start = dt_start.strftime(format_date)
    str_end = dt_end.strftime(format_date)
    ts_code = code_ths_to_ts(symbol=symbol)
    df_fina_indicator = pd.DataFrame()
    i_while = 0
    while i_while < 2:
        i_while += 1
        try:
            log_json(item="fina_indicator_TS")
            df_fina_indicator = client_ts_pro.fina_indicator(
                ts_code=ts_code,
                start_date=str_start,
                end_date=str_end,
            )
        except exceptions.ReadTimeout:
            logger.error(f"[{symbol}] - ReadTimeout sleep 3 seconds.")
        if not df_fina_indicator.empty:
            break
        time.sleep(float_time_sleep)
    if not df_fina_indicator.empty:
        df_fina_indicator.drop_duplicates(subset=["end_date"], inplace=True)
        df_fina_indicator["end_date"] = df_fina_indicator["end_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_balance
            )
        )
        df_fina_indicator.set_index(keys=["end_date"], inplace=True)
        ser_fina_indicator = df_fina_indicator.loc[df_fina_indicator.index.max()]
        pickle_to_file(
            ser=ser_fina_indicator, filename_ser=filename_fina_indicator_temp
        )
        logger.debug(f"pickle to [{filename_fina_indicator_temp}]")
    else:
        ser_fina_indicator = pd.Series()
    return ser_fina_indicator


def pledge_stat(symbol: str) -> pd.Series:
    filename_pledge_stat_temp = path_temp.joinpath(f"ser_pledge_stat_{symbol}.pkl")
    ser_pledge_stat = pickle_from_file(filename_ser=filename_pledge_stat_temp)
    if not ser_pledge_stat.empty:
        dt_stale = ser_pledge_stat.name
        dt_complete = get_latest_friday()
        if dt_stale >= dt_complete:
            logger.debug(f"pickle from [{filename_pledge_stat_temp}] - [{dt_stale}]")
            return ser_pledge_stat
    ts_code = code_ths_to_ts(symbol)
    df_pledge_stat = pd.DataFrame()
    i_while = 0
    while i_while < 2:
        i_while += 1
        log_json(item="pledge_stat_TS")
        try:
            df_pledge_stat = client_ts_pro.pledge_stat(ts_code=ts_code)
        except exceptions.ReadTimeout as e:
            logger.error(f"[{symbol}] - {repr(e)}.")
        if df_pledge_stat.empty:
            logger.error(f"{symbol} - empty.")
            time.sleep(float_time_sleep)
        else:
            break
    if not df_pledge_stat.empty:
        df_pledge_stat["end_date"] = df_pledge_stat["end_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_pm_end,
            )
        )
        df_pledge_stat.drop_duplicates(
            subset=["end_date"],
            keep="first",
            inplace=True,
        )
        df_pledge_stat.set_index(keys="end_date", inplace=True)
        ser_pledge_stat = df_pledge_stat.loc[df_pledge_stat.index.max()]
        pickle_to_file(ser=ser_pledge_stat, filename_ser=filename_pledge_stat_temp)
        logger.debug(f"pickle to [{filename_pledge_stat_temp}].")
    return ser_pledge_stat


def index_classify(level: str = "L2", src: str = "SW2021") -> pd.DataFrame:
    filename_index_classify = path_temp.joinpath(f"df_index_classify_{level}_{src}.ftr")
    df_index_classify = feather_from_file(filename_df=filename_index_classify)
    if not df_index_classify.empty:
        dt_stale = datetime.datetime.strptime(df_index_classify.index.name, format_dt)
        if dt_stale >= dt_am_start:
            logger.debug(f"pickle from [{filename_index_classify}] - [{dt_stale}]")
            return df_index_classify
    i_while = 0
    while i_while < 2:
        i_while += 1
        log_json(item="index_classify_TS")
        df_index_classify = client_ts_pro.index_classify(level=level, src=src)
        if not df_index_classify.empty:
            break
        time.sleep(float_time_sleep)
    if not df_index_classify.empty:
        df_index_classify["is_pub"] = df_index_classify["is_pub"].astype("float64")
        df_index_classify.set_index(keys=["index_code"], inplace=True)
        str_dt_now_opened = dt_am_start.strftime(format_dt)
        df_index_classify.index.rename(name=str_dt_now_opened, inplace=True)
        feather_to_file(df=df_index_classify, filename_df=filename_index_classify)
        logger.debug(f"pickle to [{filename_index_classify}]")
    return df_index_classify


def index_member(index_code: str = "850531.SI") -> pd.DataFrame:
    symbol = code_ts_to_ths(index_code)
    filename_index_member_temp = path_temp.joinpath(
        f"df_index_member_{symbol}.ftr",
    )
    df_index_member = feather_from_file(filename_df=filename_index_member_temp)
    if not df_index_member.empty:
        try:
            dt_stale = datetime.datetime.strptime(
                df_index_member.index.name,
                format_dt,
            )
        except ValueError:
            dt_stale = dt_init
        if dt_stale >= dt_am_start:
            logger.debug(
                f"Feather from [{filename_index_member_temp}] - [{dt_stale}].",
            )
            return df_index_member
    i_while = 0
    while i_while < 2:
        i_while += 1
        log_json(item="index_member_TS")
        df_index_member = client_ts_pro.index_member(index_code=index_code, is_new="Y")
        if not df_index_member.empty:
            break
        time.sleep(float_time_sleep)
    if not df_index_member.empty:
        df_index_member["con_code"] = df_index_member["con_code"].apply(
            func=code_ts_to_ths,
        )
        df_index_member.set_index(keys=["con_code"], inplace=True)
        df_index_member = df_index_member[
            [
                "index_code",
            ]
        ]
        str_dt_now_opened = dt_am_start.strftime(format_dt)
        df_index_member.index.rename(name=str_dt_now_opened, inplace=True)
        feather_to_file(df=df_index_member, filename_df=filename_index_member_temp)
        logger.debug(f"Feather to [{filename_index_member_temp}] - [{dt_pm_end}].")
    return df_index_member


def ts_sw_daily(ts_code: str, dt_start: datetime.datetime, dt_end: datetime.datetime):
    global dt_now
    symbol = code_ts_to_ths(ts_code)
    filename_sw_daily_temp = path_temp.joinpath(
        f"df_sw_daily_{symbol}.ftr",
    )
    df_sw_daily = feather_from_file(filename_df=filename_sw_daily_temp)
    if not df_sw_daily.empty:
        dt_now = datetime.datetime.now()
        try:
            dt_stale = datetime.datetime.strptime(
                df_sw_daily.index.name,
                format_dt,
            )
        except ValueError:
            dt_stale = dt_init
        if dt_am_start < dt_now < dt_pm_end:
            logger.debug(
                f"feather from [{filename_sw_daily_temp}] - [{dt_stale}].--trading time"
            )
            return df_sw_daily
        if dt_stale >= dt_pm_end:
            logger.debug(f"feather from [{filename_sw_daily_temp}] - [{dt_stale}].")
            return df_sw_daily
    str_dt_start = dt_start.strftime("%Y%m%d")
    str_df_end = dt_end.strftime("%Y%m%d")
    i_while = 0
    while i_while < 2:
        i_while += 1
        log_json(item="sw_daily_TS")
        try:
            df_sw_daily = client_ts_pro.sw_daily(
                ts_code=ts_code, start_date=str_dt_start, end_date=str_df_end
            )
        except Exception as e:
            logger.error(f"({repr(e)})")
            df_sw_daily = pd.DataFrame()
        if not df_sw_daily.empty:
            break
        time.sleep(float_time_sleep)
    if not df_sw_daily.empty:
        df_sw_daily["trade_date"] = df_sw_daily["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_pm_end
            )
        )
        df_sw_daily.set_index(keys=["trade_date"], inplace=True)
        df_sw_daily.sort_index(ascending=True, inplace=True)
        dt_max = df_sw_daily.index.max()
        str_dt_max = dt_max.strftime(format_dt)
        df_sw_daily.index.rename(name=str_dt_max, inplace=True)
        feather_to_file(df=df_sw_daily, filename_df=filename_sw_daily_temp)
        logger.debug(
            f"feather to [{filename_sw_daily_temp}] - [{dt_max}]",
        )
    return df_sw_daily


def index_weight(index_code: str):
    symbol = code_ts_to_ths(index_code)
    filename_index_weight_temp = path_temp.joinpath(
        f"df_index_weight_{symbol}.ftr",
    )
    df_index_weight = feather_from_file(filename_df=filename_index_weight_temp)
    if not df_index_weight.empty:
        dt_stale_index_weight = datetime.datetime.strptime(
            df_index_weight.index.name,
            format_dt,
        )
        if dt_stale_index_weight >= dt_pm_end:
            return df_index_weight
    log_json(item="index_weight_TS")
    df_index_weight = client_ts_pro.index_weight(index_code=index_code)
    if not df_index_weight.empty:
        df_index_weight.index.name = dt_pm_end.strftime(format_dt)
        feather_to_file(df=df_index_weight, filename_df=filename_index_weight_temp)
    return df_index_weight


def index_daily(symbol: str, dt_start: datetime.datetime, dt_end: datetime.datetime):
    filename_index_daily_temp = path_temp.joinpath(
        f"df_index_daily_{symbol}.ftr",
    )
    df_index_daily = feather_from_file(filename_df=filename_index_daily_temp)
    if not df_index_daily.empty:
        try:
            dt_stale = datetime.datetime.strptime(
                df_index_daily.index.name,
                format_dt,
            )
        except ValueError:
            dt_stale = dt_init
        if dt_stale >= dt_pm_end:
            return df_index_daily
    ts_code = code_ths_to_ts(symbol)
    str_dt_start = dt_start.strftime(format_date)
    str_dt_end = dt_end.strftime(format_date)
    log_json(item="index_daily_TS")
    df_index_daily = client_ts_pro.index_daily(
        ts_code=ts_code, start_date=str_dt_start, end_date=str_dt_end
    )
    if not df_index_daily.empty:
        df_index_daily["date"] = df_index_daily["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_pm_end
            )
        )
        df_index_daily.set_index(keys=["date"], inplace=True)
        df_index_daily.sort_index(inplace=True)
        dt_max = df_index_daily.index.max()
        str_dt_max = dt_max.strftime(format_dt)
        df_index_daily.index.rename(name=str_dt_max, inplace=True)
        feather_to_file(df=df_index_daily, filename_df=filename_index_daily_temp)
        logger.debug(
            f"feather to [{filename_index_daily_temp}] - [{dt_max}]",
        )
    else:
        logger.error(
            f"df_index_daily_[{symbol}] is empty.",
        )
    return df_index_daily
