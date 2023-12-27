# modified at 2023/05/18 22::25
from __future__ import annotations
import datetime
import time
import random
import pandas as pd
import feather
from loguru import logger
import analysis.base
from analysis.const import (
    path_temp,
    dt_init,
    dt_trading_last_T0,
    dt_pm_end,
    all_chs_code,
    client_ts_pro,
)


def fina_audit_vip(year: int):
    name: str = f"df_fina_audit_{year}"
    logger.trace(f"{name} Begin")
    df_fina_audit = analysis.base.feather_from_file(
        key=name,
    )
    if not df_fina_audit.empty:
        logger.trace(f"{name} Break and End")
        return df_fina_audit
    dt_period = datetime.datetime(year=year, month=12, day=31)
    str_dt_period = dt_period.strftime("%Y%m%d")
    str_date_path = dt_period.strftime("%Y_%m_%d")
    audit_result_init = "NON"
    filename_df_fina_audit = path_temp.joinpath(f"df_fina_audit_{str_date_path}.ftr")
    if filename_df_fina_audit.exists():
        df_fina_audit = feather.read_dataframe(source=filename_df_fina_audit)
        df_fina_audit = df_fina_audit.sample(frac=1)
    else:
        list_symbol = all_chs_code()
        df_fina_audit = pd.DataFrame(
            index=list_symbol,
            columns=[
                "dt_ann",
                "audit_result",
            ],
        )
        df_fina_audit["dt_ann"].fillna(value=dt_init, inplace=True)
        df_fina_audit["audit_result"].fillna(value=audit_result_init, inplace=True)
    audit_result_std = "标准无保留意见"
    i = 0
    count = len(df_fina_audit)
    for symbol in df_fina_audit.index:
        i += 1
        str_msg_bar = f"[{name}] - [{symbol}] - [{i:4d}/{count:4d}]"
        if df_fina_audit.at[symbol, "dt_ann"] != dt_init:
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        if random.randint(0, 19) == 10:
            feather.write_dataframe(df=df_fina_audit, dest=filename_df_fina_audit)
        ts_code = analysis.code_ths_to_ts(symbol)  # sz002564
        df_symbol = pd.DataFrame()
        i_while = 0
        while i_while < 2:
            i_while += 1
            df_symbol = client_ts_pro.fina_audit(ts_code=ts_code, period=str_dt_period)
            time.sleep(0.02)
            if df_symbol.empty:  # sz002564
                time.sleep(0.5)
            else:
                break
        if df_symbol.empty:
            print(f"\r{str_msg_bar} - NON\033[K")
            continue
        df_symbol.drop_duplicates(
            subset=["ann_date", "end_date", "audit_result"],
            keep="first",
            inplace=True,
            ignore_index=True,
        )
        df_symbol["dt_fina_audit"] = pd.to_datetime(df_symbol["end_date"])
        dt_symbol_fina_audit_max = df_symbol["dt_fina_audit"].max()
        if dt_symbol_fina_audit_max != dt_period:
            print(f"\r{str_msg_bar} - ({dt_symbol_fina_audit_max.year})\033[K")
            continue
        else:
            print(f"\r{str_msg_bar} - Update\033[K", end="")
        df_symbol["dt_ann"] = pd.to_datetime(df_symbol["ann_date"])
        dt_symbol_ann_max = df_symbol["dt_ann"].max()
        df_symbol = df_symbol[df_symbol["dt_ann"] == dt_symbol_ann_max]
        df_fina_audit.at[symbol, "dt_ann"] = dt_symbol_ann_max
        df_symbol.reset_index(drop=True, inplace=True)
        df_symbol_count = len(df_symbol)
        if df_symbol_count == 1:
            df_fina_audit.at[symbol, "audit_result"] = df_symbol.at[0, "audit_result"]
        else:
            set_audit_result = set(df_symbol["audit_result"].tolist())
            if audit_result_std in set_audit_result:
                set_audit_result.remove(audit_result_std)
            df_fina_audit.at[symbol, "audit_result"] = ",".join(set_audit_result)
    if i >= count:
        print("\n", end="")  # 格式处理
        analysis.base.feather_to_file(
            df=df_fina_audit,
            key=name,
        )
        if filename_df_fina_audit.exists():
            filename_df_fina_audit.unlink()
    return df_fina_audit


def st_income() -> bool:
    name: str = "df_st"
    logger.trace(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    if analysis.base.is_latest_version(key=name):
        logger.trace(f"{name} Break and End")
        return True
    list_symbol = all_chs_code()
    dt_period_year = dt_trading_last_T0.year
    dt_date_trading_month = dt_trading_last_T0.month
    if dt_date_trading_month in [1, 2, 3, 4]:  # 预估上年的年报
        dt_period_year -= 1
        dt_period_previous = datetime.date(year=dt_period_year, month=9, day=30)
        dt_period_next = datetime.date(year=dt_period_year, month=12, day=31)
    elif dt_date_trading_month in [5, 6, 7, 8]:  # 预估本年的半年报
        dt_period_previous = datetime.date(year=dt_period_year, month=3, day=31)
        dt_period_next = datetime.date(year=dt_period_year, month=6, day=30)
    elif dt_date_trading_month in [9, 10]:  # 预估本年的3季度报
        dt_period_previous = datetime.date(year=dt_period_year, month=6, day=30)
        dt_period_next = datetime.date(year=dt_period_year, month=9, day=30)
    elif dt_date_trading_month in [11, 12]:  # 预估本年的年报
        dt_period_previous = datetime.date(year=dt_period_year, month=9, day=30)
        dt_period_next = datetime.date(year=dt_period_year, month=12, day=31)
    else:
        logger.trace(f"dt_now_month Error.")
        return False
    dt_period_previous_month = dt_period_previous.month
    if dt_period_previous_month == 3:
        arg_revenue = 0.25
    elif dt_period_previous_month == 6:
        arg_revenue = 0.5
    elif dt_period_previous_month == 9:
        arg_revenue = 0.75
    else:
        arg_revenue = 1
    str_dt_period_previous = dt_period_previous.strftime("%Y%m%d")
    str_dt_period_next = dt_period_next.strftime("%Y%m%d")
    str_date_path = dt_trading_last_T0.strftime("%Y_%m_%d")
    filename_df_st = path_temp.joinpath(f"df_st_temp_{str_date_path}.ftr")
    audit_result_init = "NON"
    st_init = "NON"
    audit_result_std = "标准无保留意见"
    if filename_df_st.exists():
        df_st = feather.read_dataframe(source=filename_df_st)
        df_st = df_st.sample(frac=1)
    else:
        df_income = client_ts_pro.income_vip(period=str_dt_period_previous)
        df_income = df_income[["ts_code", "end_date", "revenue", "n_income"]]
        df_income["symbol"] = df_income["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_income.drop_duplicates(subset="symbol", keep="first", inplace=True)
        df_income.set_index(keys=["symbol"], inplace=True)
        df_income["dt_income"] = pd.to_datetime(df_income["end_date"])
        df_income = df_income.reindex(
            columns=[
                "dt_income",
                "revenue",
                "n_income",
            ]
        )
        df_income_next = client_ts_pro.income_vip(period=str_dt_period_next)
        df_income_next = df_income_next[["ts_code", "end_date", "revenue", "n_income"]]
        df_income_next["symbol"] = df_income_next["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_income_next.drop_duplicates(subset="symbol", keep="first", inplace=True)
        df_income_next.set_index(keys=["symbol"], inplace=True)
        df_income_next["dt_income"] = pd.to_datetime(df_income_next["end_date"])
        df_income_next = df_income_next.reindex(
            columns=[
                "dt_income",
                "revenue",
                "n_income",
            ]
        )
        for symbol in df_income.index:
            if symbol in df_income_next.index:
                df_income.loc[symbol] = df_income_next.loc[symbol]
        df_balance_sheet = client_ts_pro.balancesheet_vip(
            period=str_dt_period_previous,
            fields="ts_code,end_date,total_hldr_eqy_exc_min_int",
        )
        df_balance_sheet["symbol"] = df_balance_sheet["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet.drop_duplicates(subset="ts_code", keep="first", inplace=True)
        df_balance_sheet["symbol"] = df_balance_sheet["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet.set_index(keys=["symbol"], inplace=True)
        df_balance_sheet["dt_balance"] = pd.to_datetime(df_balance_sheet["end_date"])
        df_balance_sheet = df_balance_sheet.reindex(
            columns=[
                "dt_balance",
                "total_hldr_eqy_exc_min_int",
            ]
        )
        df_balance_sheet_next = client_ts_pro.balancesheet_vip(
            period=str_dt_period_next,
            fields="ts_code,end_date,total_hldr_eqy_exc_min_int",
        )
        df_balance_sheet_next["symbol"] = df_balance_sheet_next["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet_next.drop_duplicates(
            subset="ts_code", keep="first", inplace=True
        )
        df_balance_sheet_next["symbol"] = df_balance_sheet_next["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet_next.set_index(keys=["symbol"], inplace=True)
        df_balance_sheet_next["dt_balance"] = pd.to_datetime(
            df_balance_sheet_next["end_date"]
        )
        df_balance_sheet_next = df_balance_sheet_next.reindex(
            columns=[
                "dt_balance",
                "total_hldr_eqy_exc_min_int",
            ]
        )
        for symbol in df_balance_sheet.index:
            if symbol in df_balance_sheet_next.index:
                df_balance_sheet.loc[symbol] = df_balance_sheet_next.loc[symbol]
        df_forecast = client_ts_pro.forecast_vip(period=str_dt_period_next)
        df_forecast = df_forecast[
            ["ts_code", "ann_date", "end_date", "net_profit_min", "net_profit_max"]
        ]
        df_forecast["ts_code"] = df_forecast["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_forecast["ann_date"] = pd.to_datetime(df_forecast["ann_date"])
        df_forecast["dt_forecast"] = pd.to_datetime(df_forecast["end_date"])
        df_forecast.sort_values(by=["ann_date"], ascending=False, inplace=True)
        df_forecast.drop_duplicates(subset="ts_code", keep="first", inplace=True)
        df_forecast.set_index(keys=["ts_code"], inplace=True)
        df_forecast = df_forecast.reindex(
            columns=[
                "dt_forecast",
                "net_profit_min",
                "net_profit_max",
                "profit_rate",
            ]
        )
        df_forecast["net_profit"] = (
            df_forecast["net_profit_min"] + df_forecast["net_profit_max"]
        ) / 2
        dt_period_year -= 1
        df_fina_audit = fina_audit_vip(year=dt_period_year)
        df_basic = client_ts_pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name",
        )
        df_basic["ts_code"] = df_basic["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_basic.set_index(keys=["ts_code"], inplace=True)
        df_st = pd.concat(
            objs=[
                df_basic,
                df_income,
                df_balance_sheet,
                df_forecast,
                df_fina_audit,
            ],
            axis=1,
            join="outer",
        )
        df_st = df_st[df_st.index.isin(values=list_symbol)]
        df_st["dt_income"].fillna(value=dt_init, inplace=True)
        df_st["dt_balance"].fillna(value=dt_init, inplace=True)
        df_st["dt_forecast"].fillna(value=dt_init, inplace=True)
        df_st["dt_ann"].fillna(value=dt_init, inplace=True)
        df_st["audit_result"].fillna(value=audit_result_init, inplace=True)
        df_st["ST"] = st_init
        df_st.fillna(value=0.0, inplace=True)
        df_st["revenue"] = df_st["revenue"] / 100000000
        df_st["n_income"] = df_st["n_income"] / 100000000
        df_st["total_hldr_eqy_exc_min_int"] = (
            df_st["total_hldr_eqy_exc_min_int"] / 100000000
        )
        df_st["net_profit_min"] = df_st["net_profit_min"] / 10000
        df_st["net_profit_max"] = df_st["net_profit_max"] / 10000
        df_st["net_profit"] = df_st["net_profit"] / 10000
    count = len(df_st)
    i = 0
    for symbol in df_st.index:
        i += 1
        if random.randint(0, 19) == 10:
            feather.write_dataframe(df=df_st, dest=filename_df_st)
        str_msg_bar = f"{name} - [{symbol}] - [{i:4d}/{count:4d}]"
        if df_st.at[symbol, "ST"] != st_init:
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        print(f"\r{str_msg_bar}\033[K", end="")
        if df_st.at[symbol, "dt_income"] != dt_init:
            revenue = df_st.at[symbol, "revenue"]
            revenue_year = df_st.at[symbol, "revenue"] / arg_revenue
            if (
                df_st.at[symbol, "dt_forecast"] != dt_init
                and df_st.at[symbol, "net_profit"] != 0
            ):
                n_income = df_st.at[symbol, "net_profit"]
            else:
                n_income = df_st.at[symbol, "n_income"]
        else:
            revenue = 0
            revenue_year = 0
            n_income = 0
        if revenue > 0:
            df_st.at[symbol, "profit_rate"] = (n_income / revenue) * 100
        if (
            isinstance(df_st.at[symbol, "name"], str)
            and "ST" in df_st.at[symbol, "name"]
        ):
            df_st.at[symbol, "ST"] = "ST"
        elif (
            df_st.at[symbol, "dt_ann"] != dt_init
            and df_st.at[symbol, "audit_result"] != audit_result_std
        ):
            df_st.at[symbol, "ST"] = "ST"
        elif (
            df_st.at[symbol, "dt_balance"] != dt_init
            and df_st.at[symbol, "total_hldr_eqy_exc_min_int"] < 0
        ):
            df_st.at[symbol, "ST"] = "ST"
        elif revenue > 1:
            if n_income > 0:
                df_st.at[symbol, "ST"] = "A+"
            else:
                df_st.at[symbol, "ST"] = "A-"
        elif revenue_year > 1:
            if n_income > 0:
                df_st.at[symbol, "ST"] = "B+"
            else:
                df_st.at[symbol, "ST"] = "B-"
        elif 0 < revenue_year < 1:
            if n_income > 0:
                df_st.at[symbol, "ST"] = "C+"
            else:
                df_st.at[symbol, "ST"] = "C-"
        else:
            df_st.at[symbol, "ST"] = "NULL"
    df_st = df_st[
        [
            "ST",
            "profit_rate",
            "dt_income",
            "revenue",
            "n_income",
            "total_hldr_eqy_exc_min_int",
            "dt_forecast",
            "net_profit_min",
            "net_profit_max",
            "net_profit",
            "dt_ann",
            "audit_result",
        ]
    ]
    df_st["revenue"] = round(df_st["revenue"], 4)
    df_st["n_income"] = round(df_st["n_income"], 4)
    df_st["total_hldr_eqy_exc_min_int"] = round(df_st["total_hldr_eqy_exc_min_int"], 4)
    df_st["profit_rate"] = round(df_st["profit_rate"], 2)
    if i >= count:
        print("\n", end="")  # 格式处理
        df_st.sort_values(by=["ST"], ascending=False, inplace=True)
        analysis.base.feather_to_file(
            df=df_st,
            key=name,
        )
        analysis.base.set_version(key=name, dt=dt_pm_end)
        if filename_df_st.exists():
            filename_df_st.unlink()
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"ST analysis takes {str_gm}")
    logger.trace(f"ST End")
    return True
