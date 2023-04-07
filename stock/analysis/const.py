import os
import datetime
import tushare as ts


def latest_trading_day() -> datetime.date:
    dt_now = datetime.datetime.now()
    str_date_now = dt_now.strftime("%Y%m%d")
    pro = ts.pro_api()
    df_trade = pro.trade_cal(exchange="", start_date="20230301", end_date=str_date_now)
    df_trade.set_index(keys=["cal_date"], inplace=True)
    if df_trade.at[str_date_now, "is_open"] == 1:
        str_dt_out = str_date_now
    else:
        str_dt_out = df_trade.at[str_date_now, "pretrade_date"]
    dt_out = datetime.datetime.strptime(str_dt_out, "%Y%m%d").date()
    return dt_out


def all_ts_code() -> list | None:
    pro = ts.pro_api()
    df_basic = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date",
    )
    if len(df_basic) == 0:
        return
    else:
        list_ts_code = df_basic["ts_code"].tolist()
        return list_ts_code


def all_chs_code() -> list | None:
    list_ts_code = all_ts_code()
    if list_ts_code:
        list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
        return list_chs_code
    else:
        return


path_main = os.getcwd()
path_data = os.path.join(path_main, "data")
if not os.path.exists(path_data):
    os.mkdir(path_data)
path_history = os.path.join(path_main, "history")
if not os.path.exists(path_history):
    os.mkdir(path_history)
path_check = os.path.join(path_main, "check")
if not os.path.exists(path_check):
    os.mkdir(path_check)
path_index = os.path.join(path_data, f"index")
if not os.path.exists(path_index):
    os.mkdir(path_index)
path_log = os.path.join(path_data, f"log")
if not os.path.exists(path_log):
    os.mkdir(path_log)
dt_date_trading = latest_trading_day()
str_date_path = dt_date_trading.strftime("%Y_%m_%d")
time_am_0100 = datetime.time(hour=1, minute=0, second=0, microsecond=0)
time_am_0910 = datetime.time(hour=9, minute=10, second=0, microsecond=0)
time_am_start = datetime.time(hour=9, minute=28, second=0, microsecond=0)
time_am_end = datetime.time(hour=11, minute=30, second=0, microsecond=0)
time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
time_pm_1457 = datetime.time(hour=14, minute=57, second=0, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
dt_init = datetime.datetime(year=1989, month=1, day=1)
dt_am_0100 = datetime.datetime.combine(dt_date_trading, time_am_0100)
dt_am_0910 = datetime.datetime.combine(dt_date_trading, time_am_0910)
dt_am_start = datetime.datetime.combine(dt_date_trading, time_am_start)
dt_am_end = datetime.datetime.combine(dt_date_trading, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date_trading, time_pm_start)
dt_pm_1457 = datetime.datetime.combine(dt_date_trading, time_pm_1457)
dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
filename_log = os.path.join(path_log, "log{time}.log")
filename_input = os.path.join(path_main, f"input.xlsx")
filename_trader_template = os.path.join(path_main, f"trader.xlsx")
filename_chip_shelve = os.path.join(path_data, f"chip")
filename_chip_excel = os.path.join(path_check, f"chip_{str_date_path}.xlsx")
filename_signal = os.path.join(path_check, f"signal_{str_date_path}.xlsx")
filename_data_csv = os.path.join(path_check, f"trader_{str_date_path}.csv")
list_all_stocks = all_chs_code()
