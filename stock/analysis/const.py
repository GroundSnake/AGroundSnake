import os
import datetime
import analysis.base


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
dt_date_trading = analysis.base.latest_trading_day()
str_date_path = dt_date_trading.strftime("%Y_%m_%d")
time_program_start = datetime.time(hour=1, minute=0, second=0, microsecond=0)
time_am_start = datetime.time(hour=9, minute=28, second=0, microsecond=0)
time_am_end = datetime.time(hour=12, minute=30, second=0, microsecond=0)
time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
time_pm_1457 = datetime.time(hour=14, minute=57, second=0, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
time_program_end = datetime.time(hour=23, minute=0, second=0, microsecond=0)
dt_program_start = datetime.datetime.combine(dt_date_trading, time_program_start)
dt_am_start = datetime.datetime.combine(dt_date_trading, time_am_start)
dt_am_end = datetime.datetime.combine(dt_date_trading, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date_trading, time_pm_start)
dt_pm_1457 = datetime.datetime.combine(dt_date_trading, time_pm_1457)
dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
dt_program_end = datetime.datetime.combine(dt_date_trading, time_program_end)
filename_log = os.path.join(path_data, "program_log.log")
filename_input = os.path.join(path_main, f"input.xlsx")
filename_trader_template = os.path.join(path_main, f"trader.xlsx")
filename_chip_shelve = os.path.join(path_data, f"chip")
filename_chip_excel = os.path.join(path_check, f"chip_{str_date_path}.xlsx")
filename_signal = os.path.join(path_check, f"signal_{str_date_path}.xlsx")
filename_data_csv = os.path.join(path_check, f"trader_{str_date_path}.csv")
list_all_stocks = analysis.base.all_chs_code()
