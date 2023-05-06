# modified at 2023/4/28 13:44
from __future__ import annotations
import os
import datetime
import time
import random
import pandas as pd
import feather
import tushare as ts
from loguru import logger
import analysis.base
from analysis.const import (
    path_data,
    filename_chip_shelve,
    dt_init,
    dt_trading,
    dt_date_trading,
    dt_pm_end,
    list_all_stocks,
)


def non_standard_opinions():
    dt_end = dt_trading
    dt_start = dt_trading - datetime.timedelta(days=6 * 365)
    stt_dt_end = dt_end.strftime("%Y%m%d")
    stt_dt_start = dt_start.strftime("%Y%m%d")
    print(stt_dt_end)
    print(stt_dt_start)
    filename_df_balance_sheet = os.path.join(
        path_data, f"df_balance_sheet_{stt_dt_end}.ftr"
    )
    pro = ts.pro_api()
    if os.path.exists(filename_df_balance_sheet):
        df_balance_sheet_vip = feather.read_dataframe(source=filename_df_balance_sheet)
    else:
        df_balance_sheet_vip = pro.balancesheet_vip(
            period="20230331",
            fields="ts_code,ann_date,f_ann_date,end_date,total_hldr_eqy_exc_min_int",
        )
        df_balance_sheet_vip.drop_duplicates(
            subset="ts_code", keep="first", inplace=True
        )
        df_balance_sheet_vip["symbol"] = df_balance_sheet_vip["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet_vip.set_index(keys=["symbol"], inplace=True)
        df_balance_sheet_vip["end_date"] = pd.to_datetime(
            df_balance_sheet_vip["end_date"]
        )
        # df_balance_sheet_vip["end_date"].fillna(method="ffill", inplace=True)
        # df_balance_sheet_vip["end_type"].fillna(method="ffill", inplace=True)
        # df_balance_sheet_vip["update_flag"].fillna(method="ffill", inplace=True)
        # df_balance_sheet_vip.fillna(value=0, inplace=True)
        feather.write_dataframe(df=df_balance_sheet_vip, dest=filename_df_balance_sheet)
    df_balance_sheet_vip.to_csv("a.csv")
    return df_balance_sheet_vip


def st_income(list_symbol: str | list = None) -> bool:
    name: str = "df_st"
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"ST income Begin")
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = list_all_stocks
    if isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    pro = ts.pro_api()
    dt_date_trading_year = dt_date_trading.year
    dt_date_trading_month = dt_date_trading.month
    if dt_date_trading_month in [1, 2, 3, 4]:  # 预估上年的年报
        dt_period_income = datetime.date(year=dt_date_trading_year - 1, month=9, day=30)
        dt_period_forecast = datetime.date(
            year=dt_date_trading_year - 1, month=12, day=31
        )
    elif dt_date_trading_month in [5, 6, 7, 8]:  # 预估本年的半年报
        dt_period_income = datetime.date(year=dt_date_trading_year, month=3, day=31)
        dt_period_forecast = datetime.date(year=dt_date_trading_year, month=6, day=30)
    elif dt_date_trading_month in [9, 10]:  # 预估本年的3季度报
        dt_period_income = datetime.date(year=dt_date_trading_year, month=6, day=30)
        dt_period_forecast = datetime.date(year=dt_date_trading_year, month=9, day=30)
    elif dt_date_trading_month in [11, 12]:  # 预估本年的年报
        dt_period_income = datetime.date(year=dt_date_trading_year, month=9, day=30)
        dt_period_forecast = datetime.date(year=dt_date_trading_year, month=12, day=31)
    else:
        logger.trace(f"dt_now_month Error.")
        return False
    dt_period_income_next = dt_period_forecast
    str_dt_period_income = dt_period_income.strftime("%Y%m%d")
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    str_date_path_income = dt_period_income.strftime("%Y_%m_%d")
    str_dt_period_forecast = dt_period_forecast.strftime("%Y%m%d")
    str_dt_period_income_next = dt_period_income_next.strftime("%Y%m%d")
    filename_df_income = os.path.join(
        path_data, f"df_income_{str_date_path_income}.ftr"
    )
    filename_df_balance_sheet = os.path.join(
        path_data, f"df_balance_sheet_{str_date_path_income}.ftr"
    )
    filename_df_income_next_temp = os.path.join(
        path_data, f"df_income_next_temp_{str_date_path}.ftr"
    )
    filename_df_forecast_temp = os.path.join(
        path_data, f"df_forecast_temp_{str_date_path}.ftr"
    )
    filename_df_st_temp = os.path.join(path_data, f"df_st_temp_{str_date_path}.ftr")
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"ST Break End")
        return True
    if os.path.exists(filename_df_income):
        df_income = feather.read_dataframe(source=filename_df_income)
    else:
        df_income = pro.income_vip(period=str_dt_period_income)
        df_income["symbol"] = df_income["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_income.drop_duplicates(subset="symbol", keep="first", inplace=True)
        df_income.set_index(keys=["symbol"], inplace=True)
        df_income["end_date"] = pd.to_datetime(df_income["end_date"])
        df_income["end_date"].fillna(method="ffill", inplace=True)
        df_income["end_type"].fillna(method="ffill", inplace=True)
        df_income["update_flag"].fillna(method="ffill", inplace=True)
        df_income.fillna(value=0, inplace=True)
        feather.write_dataframe(df=df_income, dest=filename_df_income)
    if os.path.exists(filename_df_balance_sheet):
        df_balance_sheet_vip = feather.read_dataframe(source=filename_df_balance_sheet)
    else:
        df_balance_sheet_vip = pro.balancesheet_vip(
            period=str_dt_period_income,
            fields="ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_hldr_eqy_exc_min_int",
        )
        df_balance_sheet_vip["symbol"] = df_balance_sheet_vip["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet_vip.drop_duplicates(
            subset="ts_code", keep="first", inplace=True
        )
        df_balance_sheet_vip["symbol"] = df_balance_sheet_vip["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_balance_sheet_vip.set_index(keys=["symbol"], inplace=True)
        df_balance_sheet_vip["end_date"] = pd.to_datetime(
            df_balance_sheet_vip["end_date"]
        )
        feather.write_dataframe(df=df_balance_sheet_vip, dest=filename_df_balance_sheet)
    if os.path.exists(filename_df_income_next_temp):
        df_income_next = feather.read_dataframe(source=filename_df_income_next_temp)
    else:
        df_income_next = pro.income_vip(period=str_dt_period_income_next)
        df_income_next["symbol"] = df_income_next["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_income_next.drop_duplicates(subset="symbol", keep="first", inplace=True)
        df_income_next.set_index(keys=["symbol"], inplace=True)
        df_income_next["end_date"] = pd.to_datetime(df_income_next["end_date"])
        df_income_next["end_date"].fillna(method="ffill", inplace=True)
        df_income_next["end_type"].fillna(method="ffill", inplace=True)
        df_income_next["update_flag"].fillna(method="ffill", inplace=True)
        df_income_next.fillna(value=0, inplace=True)
        feather.write_dataframe(df=df_income_next, dest=filename_df_income_next_temp)
    if os.path.exists(filename_df_st_temp):
        df_st = feather.read_dataframe(source=filename_df_st_temp)
    else:
        df_st = pd.DataFrame()
    if os.path.exists(filename_df_forecast_temp):
        df_forecast = feather.read_dataframe(source=filename_df_forecast_temp)
    else:
        df_forecast = pro.forecast_vip(period=str_dt_period_forecast)
        df_forecast["ts_code"] = df_forecast["ts_code"].apply(
            func=lambda x: x[-2:].lower() + x[:6]
        )
        df_forecast["ann_date"] = pd.to_datetime(df_forecast["ann_date"])
        df_forecast["end_date"] = pd.to_datetime(df_forecast["end_date"])
        df_forecast.sort_values(by=["ann_date"], ascending=False, inplace=True)
        df_forecast.drop_duplicates(subset="ts_code", keep="first", inplace=True)
        df_forecast.set_index(keys=["ts_code"], inplace=True)
        df_forecast["ann_date"].fillna(method="ffill", inplace=True)
        df_forecast["type"].fillna(value="No statement", inplace=True)
        df_forecast["first_ann_date"].fillna(method="ffill", inplace=True)
        df_forecast["summary"].fillna(value="No statement", inplace=True)
        df_forecast["change_reason"].fillna(value="No statement", inplace=True)
        df_forecast.fillna(value=0, inplace=True)
        feather.write_dataframe(df=df_forecast, dest=filename_df_forecast_temp)
    list_df_income = df_income.index.tolist()
    list_df_income_next = df_income_next.index.tolist()
    list_df_forecast = df_forecast.index.tolist()
    list_df_st = df_st.index.tolist()
    i = 0
    all_record = len(list_symbol)
    for symbol in list_symbol:
        i += 1
        str_msg_bar = f"ST Update:[{i:4d}/{all_record:4d}]--[{symbol}]"
        if symbol in list_df_st:
            print(f"\r{str_msg_bar} - exist\033[K", end="")
            continue
        print(f"\r{str_msg_bar} - {dt_date_trading}\033[K", end="")
        zero_time = datetime.time()
        period_forecast = datetime.datetime.combine(dt_period_forecast, zero_time)
        period_income = datetime.datetime.combine(dt_period_income, zero_time)
        revenue = 0
        revenue_forecast = 0
        n_income = 0
        net_profit = 0
        net_profit_min = 0
        net_profit_max = 0
        grade = ""
        if symbol in df_balance_sheet_vip.index:
            df_st.at[symbol, "end_date"] = df_balance_sheet_vip.at[symbol, "end_date"]
            df_st.at[symbol, "total_hldr_eqy_exc_min_int"] = df_balance_sheet_vip.at[
                symbol, "total_hldr_eqy_exc_min_int"
            ]
        else:
            df_st.at[symbol, "end_date"] = dt_init
            df_st.at[symbol, "total_hldr_eqy_exc_min_int"] = 0
        if symbol in list_df_income_next:
            period_income = df_income_next.at[symbol, "end_date"]
            revenue = df_income_next.at[symbol, "revenue"] / 100000000
            n_income = df_income_next.at[symbol, "n_income"] / 100000000
            dt_period_income_next_month = dt_period_income_next.month
            if dt_period_income_next_month == 3:
                revenue_forecast = revenue / 0.25
            elif dt_period_income_next_month == 6:
                revenue_forecast = revenue / 0.5
            elif dt_period_income_next_month == 9:
                revenue_forecast = revenue / 0.75
            elif dt_period_income_next_month == 12:
                revenue_forecast = revenue
            else:
                revenue_forecast = revenue
            period_forecast = period_income
            net_profit_min = 0
            net_profit_max = 0
            net_profit = n_income
            net_profit_forecast = n_income
            grade = f"C_{dt_period_income_next_month}_"
        else:
            if symbol in list_df_income:
                period_income = df_income.at[symbol, "end_date"]
                revenue = df_income.at[symbol, "revenue"] / 100000000
                n_income = df_income.at[symbol, "n_income"] / 100000000
                if dt_period_income.month == 3:
                    revenue_forecast = revenue / 0.25
                elif dt_period_income.month == 6:
                    revenue_forecast = revenue / 0.5
                elif dt_period_income.month == 9:
                    revenue_forecast = revenue / 0.75
                else:
                    revenue_forecast = revenue
            if symbol in list_df_forecast:
                period_forecast = df_forecast.at[symbol, "end_date"]
                net_profit_min = df_forecast.at[symbol, "net_profit_min"] / 10000
                net_profit_max = df_forecast.at[symbol, "net_profit_max"] / 10000
                net_profit = (net_profit_min + net_profit_max) / 2
            if net_profit != 0:
                net_profit_forecast = net_profit
            else:
                net_profit_forecast = n_income
        if revenue > 1:
            grade_revenue = "A"
        elif revenue < 1 < revenue_forecast:
            grade_revenue = "B"
        elif revenue > 0:
            grade_revenue = "Z"
        else:
            grade_revenue = "#"
        if net_profit_forecast > 0:
            grade_net_profit_forecast = "A"
        elif net_profit_forecast < 0:
            grade_net_profit_forecast = "Z"
        else:
            grade_net_profit_forecast = "#"
        grade_combine = grade_revenue + grade_net_profit_forecast
        if grade_combine in ["AA", "AZ"]:
            grade += "A++"
        elif grade_combine in ["BA", "ZA"]:
            grade += "A--"
        elif grade_combine == "BZ":
            grade += "ST--"
        elif grade_combine == "ZZ":
            grade += "ST++"
        else:
            grade += "#"
        df_st.at[symbol, "period_forecast"] = period_forecast
        df_st.at[symbol, "period_income"] = period_income
        df_st.at[symbol, "revenue"] = revenue
        df_st.at[symbol, "n_income"] = n_income
        df_st.at[symbol, "net_profit"] = net_profit
        df_st.at[symbol, "net_profit_min"] = net_profit_min
        df_st.at[symbol, "net_profit_max"] = net_profit_max
        if df_st.at[symbol, "total_hldr_eqy_exc_min_int"] < 0:
            df_st.at[symbol, "ST"] = "ST++"
        else:
            df_st.at[symbol, "ST"] = grade
        if random.randint(0, 2) == 1:
            feather.write_dataframe(df=df_st, dest=filename_df_st_temp)
    if i >= all_record:
        print("\n", end="")  # 格式处理
        logger.trace(f"For loop End")
        analysis.base.write_obj_to_db(
            obj=df_st, key=name, filename=filename_chip_shelve
        )
        df_st.sort_values(by=["ST"], ascending=False, inplace=True)
        analysis.base.set_version(key=name, dt=dt_pm_end)
        if os.path.exists(filename_df_st_temp):
            os.remove(path=filename_df_st_temp)
            logger.trace(f"[{filename_df_st_temp}] remove")
        if os.path.exists(filename_df_forecast_temp):
            os.remove(path=filename_df_forecast_temp)
            logger.trace(f"[{filename_df_forecast_temp}] remove")
        if os.path.exists(filename_df_income_next_temp):
            os.remove(path=filename_df_income_next_temp)
            logger.trace(f"[{filename_df_income_next_temp}] remove")
        if os.path.exists(filename_df_income):
            os.remove(path=filename_df_income)
            logger.trace(f"[{filename_df_income}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"ST analysis takes {str_gm}")
    logger.trace(f"ST End")
    return True
