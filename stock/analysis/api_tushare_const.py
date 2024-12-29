import time
import datetime
import pandas as pd
from analysis.log import log_json, logger
from analysis.const import (
    dt_init,
    dt_now_am_start,
    time_pm_end,
    time_balance,
    format_dt,
    format_date,
    float_time_sleep,
    path_temp,
    client_ts_pro,
)
from analysis.base import (
    feather_from_file,
    feather_to_file,
)

dt_now = datetime.datetime.now()


def trade_cal() -> pd.DataFrame:
    filename_trade_cal_temp = path_temp.joinpath(
        f"df_trade_cal.ftr",
    )
    df_trade_cal = feather_from_file(filename_df=filename_trade_cal_temp)
    if not df_trade_cal.empty:
        dt_stale_trade_cal = datetime.datetime.strptime(
            df_trade_cal.index.name,
            format_dt,
        )
        if dt_stale_trade_cal >= dt_now_am_start:
            logger.debug(f"feather from [{filename_trade_cal_temp}].")
            return df_trade_cal
    global dt_now
    dt_now = datetime.datetime.now().replace(microsecond=0)
    df_trade_cal = pd.DataFrame()
    dt_start = dt_now - datetime.timedelta(days=14)
    dt_end = dt_now + datetime.timedelta(days=14)
    str_date_start = dt_start.strftime(format_date)
    str_date_end = dt_end.strftime(format_date)
    i_while_trade_cal = 0
    while i_while_trade_cal < 2:
        i_while_trade_cal += 1
        try:
            log_json(item="trade_cal_TS")
            df_trade_cal = client_ts_pro.trade_cal(
                exchange="", start_date=str_date_start, end_date=str_date_end
            )
        except Exception as e:
            logger.error(f"Tushare api Error! - 【{i_while_trade_cal}】 - ({repr(e)})")
        if df_trade_cal.empty:
            logger.error(f"trade_cal is empty!")
        else:
            logger.trace(f"trade_cal - update.")
            break
        time.sleep(float_time_sleep)
    if not df_trade_cal.empty:
        df_trade_cal["cal_date"] = df_trade_cal["cal_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_balance,
            )
        )
        df_trade_cal["pretrade_date"] = df_trade_cal["pretrade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_balance,
            )
        )
        df_trade_cal.set_index(keys=["cal_date"], inplace=True)
        df_trade_cal.index.name = dt_now_am_start.strftime(format_dt)
        feather_to_file(df=df_trade_cal, filename_df=filename_trade_cal_temp)
        logger.debug(f"feather to [{filename_trade_cal_temp}].")
    return df_trade_cal


def trading_t0() -> datetime.datetime:
    global dt_now
    dt_now = datetime.datetime.now()
    dt_now_balance = datetime.datetime.combine(dt_now.date(), time_balance)
    df_trade_cal = trade_cal()
    if df_trade_cal.at[dt_now_balance, "is_open"] != 1:
        dt_t0 = df_trade_cal.at[dt_now_balance, "pretrade_date"]
    else:
        dt_t0 = dt_now_balance
    dt_1t = df_trade_cal.at[dt_t0, "pretrade_date"]
    if dt_now < dt_t0:
        dt_return = dt_1t
    else:
        dt_return = dt_t0
    return dt_return


def trading_1t() -> datetime.datetime:
    df_trade_cal = trade_cal()
    dt_t0 = trading_t0()
    dt_1t = df_trade_cal.at[dt_t0, "pretrade_date"]
    return dt_1t


def trading_2t() -> datetime.datetime:
    df_trade_cal = trade_cal()
    dt_t0 = trading_t0()
    dt_1t = df_trade_cal.at[dt_t0, "pretrade_date"]
    dt_2t = df_trade_cal.at[dt_1t, "pretrade_date"]
    return dt_2t


def trading_t1() -> datetime.datetime:
    df_trade_cal = trade_cal()
    dt_t0 = trading_t0()
    i_while_t = 0
    dt_t1 = dt_t0
    while i_while_t < 14:
        i_while_t += 1
        dt_t1 = dt_t0 + datetime.timedelta(days=i_while_t)
        if df_trade_cal.at[dt_t1, "is_open"] == 1:
            break
    return dt_t1


def daily_basic() -> pd.DataFrame:
    """
    接口:daily_basic,可以通过数据工具调试和查看数据.
    #########################################################
    df = pro.daily_basic(
        ts_code='',
        trade_date='20180726',
        fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb'
        )
    #########################################################

    """

    def stock_basic() -> pd.DataFrame:
        """
        可以用 <daily_basic> 代替本接口 <stock_basic>
        tushare接口:daily_basic,可以通过数据工具调试和查看数据.
        #########################################################
        data = pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date'
            )
        #########################################################
        """
        log_json(item="stock_basic_TS")
        df_return = client_ts_pro.stock_basic(
            exchange="", list_status="L", fields="ts_code,name,list_date"
        )
        if df_return.empty:
            return pd.DataFrame()
        df_return["symbol"] = df_return["ts_code"].apply(
            func=lambda x: x[7:].lower() + x[:6]
        )
        df_return["list_date"] = df_return["list_date"].apply(
            func=lambda x: pd.to_datetime(x)
        )
        df_return.set_index(keys="symbol", inplace=True)
        df_return = df_return[["name", "list_date"]]
        return df_return

    name: str = "df_daily_basic"
    filename_daily_basic_temp = path_temp.joinpath(f"{name}.ftr")
    df_daily_basic = feather_from_file(filename_df=filename_daily_basic_temp)
    if not df_daily_basic.empty:
        try:
            dt_daily_basic = datetime.datetime.strptime(
                df_daily_basic.index.name,
                format_dt,
            )
        except ValueError:
            dt_daily_basic = dt_init
        if dt_daily_basic >= dt_now_am_start:  # this is opened
            logger.debug(f"feather from [{filename_daily_basic_temp}]")
            return df_daily_basic
    df_stock_basic = stock_basic()
    dt_date_trading_1t = trading_1t().date()
    trade_date_dt = datetime.datetime.combine(dt_date_trading_1t, time_pm_end)
    str_trade_date_dt = trade_date_dt.strftime(format_date)
    log_json(item="daily_basic_TS")
    df_daily_basic = client_ts_pro.daily_basic(
        trade_date=str_trade_date_dt,
        fields="trade_date,ts_code,float_share,total_share,total_mv",
    )
    if df_daily_basic.empty and df_stock_basic.empty:
        logger.error("df_daily_basic and df_stock_basic is empty!")
        return pd.DataFrame()
    elif not df_daily_basic.empty and df_stock_basic.empty:
        logger.error("df_stock_basic is empty!")
        return df_daily_basic
    elif df_daily_basic.empty and not df_stock_basic.empty:
        logger.error("df_daily_basic is empty!")
        return df_stock_basic
    df_daily_basic["symbol"] = df_daily_basic["ts_code"].apply(
        func=lambda x: x[7:].lower() + x[:6]
    )
    df_daily_basic.drop(columns="ts_code", inplace=True)
    df_daily_basic["trade_date"] = df_daily_basic["trade_date"].apply(
        func=lambda x: datetime.datetime.combine(pd.to_datetime(x).date(), time_pm_end)
    )
    df_daily_basic.set_index(keys="symbol", inplace=True)
    df_daily_basic = pd.concat(
        objs=[
            df_stock_basic,
            df_daily_basic,
        ],
        axis=1,
        join="outer",
    )
    df_daily_basic.rename(
        columns={
            "total_share": "total_cap",
            "float_share": "circ_cap",
            "total_mv": "total_mv_E",
        },
        inplace=True,
    )
    df_daily_basic["trade_date"] = df_daily_basic["trade_date"].fillna(value=dt_init)
    df_daily_basic["total_cap"] = df_daily_basic["total_cap"].fillna(value=0.0)
    df_daily_basic["circ_cap"] = df_daily_basic["circ_cap"].fillna(value=0.0)
    df_daily_basic["total_mv_E"] = df_daily_basic["total_mv_E"].fillna(value=0.0)
    df_daily_basic["list_days"] = (
        df_daily_basic["trade_date"] - df_daily_basic["list_date"]
    )
    df_daily_basic["list_days"] = df_daily_basic["list_days"].apply(
        func=lambda x: x.days
    )
    df_daily_basic["total_cap"] = (df_daily_basic["total_cap"] * 10000).round(0)
    df_daily_basic["circ_cap"] = (df_daily_basic["circ_cap"] * 10000).round(0)
    df_daily_basic["total_mv_E"] = round(df_daily_basic["total_mv_E"] / 10000, 2)
    df_daily_basic = df_daily_basic[df_daily_basic["list_days"] > 0]
    df_daily_basic = df_daily_basic[
        [
            "name",
            "list_date",
            "list_days",
            "circ_cap",
            "total_cap",
            "total_mv_E",
        ]
    ]
    str_dt_now_opened = dt_now_am_start.strftime(format_dt)
    df_daily_basic.index.rename(name=str_dt_now_opened, inplace=True)
    feather_to_file(df_daily_basic, filename_df=filename_daily_basic_temp)
    logger.debug(f"feather to [{filename_daily_basic_temp}] - [{str_dt_now_opened}]")
    return df_daily_basic
