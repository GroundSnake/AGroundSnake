import os
import time
import datetime
import random
import feather
import pandas as pd
from loguru import logger
import analysis
from analysis.const import (
    time_pm_end,
    dt_init,
    path_data,
    dt_trading_last_1T,
    dt_trading_last_T0,
    filename_chip_shelve,
    dt_pm_end,
    client_ts_pro,
)


def capital() -> bool:
    name: str = "df_cap"
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"capital Break End")
        return True
    dt_now = datetime.datetime.now()
    if dt_now > dt_pm_end:
        dt_history = dt_trading_last_T0
    else:
        dt_history = dt_trading_last_1T
    str_dt_history = dt_history.strftime("%Y%m%d")
    str_history_path = dt_history.strftime("%Y_%m_%d")
    filename_cap_feather_temp = os.path.join(
        path_data, f"capital_temp_{str_history_path}.ftr"
    )
    if os.path.exists(filename_cap_feather_temp):
        df_cap = feather.read_dataframe(source=filename_cap_feather_temp)
    else:
        df_stock_basic = analysis.base.stock_basic_v2()
        df_daily_basic = client_ts_pro.daily_basic(
            trade_date=str_dt_history,
            fields="trade_date,ts_code,float_share,total_share,total_mv",
        )
        df_daily_basic["symbol"] = df_daily_basic["ts_code"].apply(
            func=lambda x: x[7:].lower() + x[:6]
        )
        df_daily_basic.drop(columns="ts_code", inplace=True)
        df_daily_basic["trade_date"] = df_daily_basic["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_pm_end
            )
        )
        df_daily_basic.set_index(keys="symbol", inplace=True)
        df_cap = pd.concat(
            objs=[
                df_stock_basic,
                df_daily_basic,
            ],
            axis=1,
            join="outer",
        )
        df_cap.index.name = ""
        df_cap.rename(
            columns={
                "total_share": "total_cap",
                "float_share": "circ_cap",
                "total_mv": "total_mv_E",
            },
            inplace=True,
        )
        df_cap["trade_date"].fillna(value=dt_init, inplace=True)
        df_cap["total_cap"].fillna(value=0.0, inplace=True)
        df_cap["circ_cap"].fillna(value=0.0, inplace=True)
        df_cap["total_mv_E"].fillna(value=0.0, inplace=True)
        df_cap["list_days"] = df_cap["trade_date"] - df_cap["list_date"]
        df_cap["list_days"] = df_cap["list_days"].apply(func=lambda x: x.days)
        df_cap["total_cap"] = df_cap["total_cap"] * 10000
        df_cap["circ_cap"] = df_cap["circ_cap"] * 10000
        df_cap["total_mv_E"] = round(df_cap["total_mv_E"] / 10000, 2)
    if df_cap.empty:
        return False
    df_cap = df_cap.sample(frac=1)
    dt_delta = dt_history - datetime.timedelta(days=366)
    str_delta = dt_delta.strftime("%Y%m%d")
    i = 0
    count = len(df_cap)
    for symbol in df_cap.index:
        i += 1
        str_msg_bar = f"Capital Update: [{i:4d}/{count:4d}] - [{symbol}]"
        if df_cap.at[symbol, "trade_date"] != dt_init:
            print(f"\r{str_msg_bar} - Exist\033[K", end="")
            continue
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_cap, dest=filename_cap_feather_temp)
        ts_code = symbol[2:] + "." + symbol[:2].upper()
        df_daily_basic = pd.DataFrame()
        i_times = 0
        while i_times < 2:
            i_times += 1
            try:
                df_daily_basic = client_ts_pro.daily_basic(
                    ts_code=ts_code,
                    start_date=str_delta,
                    end_date=str_dt_history,
                    fields="trade_date,ts_code,float_share,total_share,total_mv",
                )
            except KeyError as e:
                print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                time.sleep(1)
            except ConnectionError as e:
                print(f"\r{str_msg_bar} - [Error={i_times}] - {repr(e)}\033[K")
                time.sleep(1)
            else:
                if df_daily_basic.empty:
                    print(f"\r{str_msg_bar} - [Empty={i_times}]\033[K")
                    time.sleep(1)
                else:
                    break
        if df_daily_basic.empty:
            df_cap.at[symbol, "total_cap"] = 0
            df_cap.at[symbol, "circ_cap"] = 0
            df_cap.at[symbol, "total_mv_E"] = 0
            print(f"{str_msg_bar} - Zero\033[K")
            continue
        df_daily_basic["trade_date"] = df_daily_basic["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(), time_pm_end
            )
        )
        df_daily_basic.set_index(keys="trade_date", inplace=True)
        dt_max = df_daily_basic.index.max()
        df_cap.at[symbol, "trade_date"] = dt_max
        df_cap.at[symbol, "total_cap"] = df_daily_basic.at[dt_max, "total_share"]
        df_cap.at[symbol, "circ_cap"] = df_daily_basic.at[dt_max, "float_share"]
        df_cap.at[symbol, "total_mv_E"] = round(
            df_daily_basic.at[dt_max, "total_mv"] / 10000, 2
        )
        df_cap.at[symbol, "list_days"] = (
            dt_pm_end - df_cap.at[symbol, "list_date"]
        ).days
        print(f"\r{str_msg_bar} - {dt_max.date()}\033[K", end="")
    dt_trader = df_cap["trade_date"].max()
    if i >= count:
        print("\n", end="")  # 格式处理
        df_cap = df_cap.reindex(
            columns=["name", "list_days", "total_cap", "circ_cap", "total_mv_E"]
        )
        analysis.base.write_obj_to_db(
            obj=df_cap, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_trader)
        if os.path.exists(filename_cap_feather_temp):  # 删除临时文件
            os.remove(path=filename_cap_feather_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"capital analysis takes [{str_gm}]")
    logger.trace(f"capital End")
    return True
