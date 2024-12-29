# modified at 2023/06/29 22::25
import datetime
import pathlib
import tushare as client_ts
from mootdx.quotes import Quotes
from scipy.constants import golden


token_ts = "c92d583aa4d571cd19c20620bbb53c616b37b9a17d8da2d4e022a325"
client_ts_pro = client_ts.pro_api(token_ts)
client_mootdx = Quotes.factory(market="std")
log_level = "INFO"  # "INFO", "DEBUG"

# 股价
now_price_max = 35
close_price_max = 30
pct_etf_c = 3  # etf pct
days_delete = 30  # 30 days later delete
grid_ups_min = 1.5
# extreme and mean ratio 黄金分割常数, golden = 1.618033988749895
phi_b_net = golden - 1  # 0.618033988749895
phi_s_net = 1 - phi_b_net  # 0.38196601125010524
phi_s_net_pow = pow(phi_s_net, 2)  # 0.1458980337503154
phi_b = phi_b_net * 100  # 61.8033988749895
phi_s = phi_s_net * 100  # 38.196601125010524
phi_s_pow = phi_s_net_pow * 100  # 14.58980337503154
format_dt = "%Y%m%d_%H%M%S"
format_date = "%Y%m%d"
format_path = "%Y_%m_%d"
float_time_sleep = 0.5
dict_grid = {
    "pct_stock_up": 5.0,
    "pct_stock_down": -5.0,
    "grid_stock": 5.0,
    "pct_etf_up": 2.0,
    "pct_etf_down": -2.0,
    "grid_etf": 2.0,
}
# grid stocks
pct_stock_q1_const = -3.0
pct_stock_q3_const = 3.0
# grid etfs
pct_etf_q1_const = -1.7
pct_etf_q3_const = 1.7
#
pct_c = 5
amplitude_c = 7
turnover_c = 2
stock_top_c = 5

dt_init = datetime.datetime(year=1989, month=1, day=1, hour=5)
time_balance = datetime.time(hour=5, minute=0, second=0, microsecond=0)
time_am_start = datetime.time(hour=9, minute=30, second=0, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
time_am_0910 = datetime.time(hour=9, minute=10, second=0, microsecond=0)
time_am_0929 = datetime.time(hour=9, minute=29, second=30, microsecond=0)
time_am_0931 = datetime.time(hour=9, minute=31, second=0, microsecond=0)
time_am_0935 = datetime.time(hour=9, minute=35, second=0, microsecond=0)
time_am_1015 = datetime.time(hour=10, minute=15, second=0, microsecond=0)
time_am_1129 = datetime.time(hour=11, minute=29, second=0, microsecond=0)
time_am_end = datetime.time(hour=11, minute=30, second=0, microsecond=0)
time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
time_pm_1457 = datetime.time(hour=14, minute=57, second=0, microsecond=0)
dt_now_date = datetime.datetime.now().date()
dt_now_balance = datetime.datetime.combine(dt_now_date, time_balance)
dt_now_am_start = datetime.datetime.combine(dt_now_date, time_am_start)
dt_now_am_end = datetime.datetime.combine(dt_now_date, time_am_end)
dt_now_pm_start = datetime.datetime.combine(dt_now_date, time_pm_start)
dt_now_pm_end = datetime.datetime.combine(dt_now_date, time_pm_end)

path_user_home = pathlib.Path.home()
path_main = pathlib.Path.cwd()
path_check = path_main.joinpath("check")
path_check.mkdir(exist_ok=True)
path_history = path_main.joinpath("history")
path_history.mkdir(exist_ok=True)
path_data = path_main.joinpath("data")
path_data.mkdir(exist_ok=True)
path_chip = path_data.joinpath("chip")
path_chip.mkdir(exist_ok=True)
path_chip_csv = path_data.joinpath("chip_csv")
path_chip_csv.mkdir(exist_ok=True)
path_kline_index = path_data.joinpath("kline_index")
path_kline_index.mkdir(exist_ok=True)
path_log = path_data.joinpath("log")
path_log.mkdir(exist_ok=True)
path_temp = path_data.joinpath("temp")
path_temp.mkdir(exist_ok=True)
path_json = path_data.joinpath("json")
path_json.mkdir(exist_ok=True)


filename_log = path_log.joinpath("log{time}.log")
filename_grid_json = path_check.joinpath("grid.json")
filename_market_activity_charts_ftr = path_chip.joinpath(
    "df_market_activity_charts.ftr"
)
filename_etfs_percentile_ftr = path_chip.joinpath("df_etfs_percentile.ftr")
filename_market_activity_charts_html = path_check.joinpath(
    "market_activity_charts.html"
)
filename_concentration_rate_charts_html = path_check.joinpath(
    "concentration_rate_charts.html"
)
filename_scan_ftr = path_data.joinpath("df_scan.ftr")
filename_scan_csv = path_check.joinpath(f"df_scan.csv")
filename_signal_xlsx = path_check.joinpath(f"signal.xlsx")
filename_auto_scan_ftr = path_data.joinpath("df_auto_scan.ftr")
filename_auto_scan_csv = path_check.joinpath(f"df_auto_scan.csv")
filename_auto_signal_xlsx = path_check.joinpath(f"auto_signal.xlsx")
filename_scan_modified_xlsx = path_main.joinpath("df_scan_modified.xlsx")
filename_concentration_rate_ftr = path_chip.joinpath("df_concentration_rate.ftr")
filename_concentration_rate_csv = path_check.joinpath(f"df_concentration_rate.csv")
filename_analysis = path_chip.joinpath("df_analysis.ftr")
filename_analysis_csv = path_chip_csv.joinpath("df_analysis.csv")
filename_pool = path_chip.joinpath("df_pool.ftr")
filename_pool_csv = path_check.joinpath("df_pool.csv")
filename_pool_history = path_chip.joinpath("df_pool_history.ftr")
filename_pool_history_csv = path_check.joinpath("df_pool_history.csv")
filename_check_etf_t0 = path_data.joinpath("df_check_etf_t0.ftr")
filename_check_etf_t0_csv = path_check.joinpath("df_check_etf_t0.csv")

# 市场
MARKET_SZ = 0  # 深市
MARKET_SH = 1  # 沪市
MARKET_BJ = 2  # 北交

dict_scan_dtype = {
    "name": "object",
    "attention": "float64",
    "price": "float64",
    "position": "float64",
    "pct_chg": "float64",
    "now": "float64",
    "position_unit": "float64",
    "dt_pool": "datetime64[ns]",
    "add_pool_rate": "float64",
    "display": "float64",
    "rid": "object",
    "remark": "object",
}
dict_scan_value_default = {
    "name": "name",
    "attention": 50.0,
    "price": 0.0,
    "position": 0.0,
    "pct_chg": 0.0,
    "now": 0.0,
    "position_unit": 0.0,
    "dt_pool": dt_now_pm_end,
    "add_pool_rate": 0.0,
    "display": 1.0,
    "rid": "rid",
    "remark": "remark",
}


"""
dict_business_dtype = {
    "account_id": "object",
    "symbol": "object",
    "name": "object",
    "volume": "float64",
    "price_buy": "float64",
    "price_sell": "float64",
    "pct": "float64",
    "profit": "float64",
    "dt_buy": "datetime64[ns]",
    "dt_sell": "datetime64[ns]",
}
# account_id = "gs66086043349"
dict_value_default = {
    "account_id": "gs66086043349",
    "symbol": "symbol",
    "name": "name",
    "volume": 100.0,
    "price_buy": 0.0,
    "price_sell": 0.0,
    "pct": 0.0,
    "profit": 0.0,
    "dt_buy": dt_init,
    "dt_sell": dt_init,
}
"""
