# modified at 2023/06/29 22::25
import os
import datetime
import random
import pandas as pd
import analysis.tushare as ts
from loguru import logger
from analysis.mootdx.quotes import Quotes
from scipy.constants import golden


client_ts_pro = ts.pro_api("fac4c0a5680be5aba00200a1f2c2fe3edc8c808bea9d51a510a734c5")
client_mootdx = Quotes.factory(market="std")


def latest_trading_day(days: int = None) -> datetime.datetime:
    dt = datetime.datetime.now().replace(microsecond=0)
    if days is None:
        dt_pos = dt
    else:
        dt_pos = dt + datetime.timedelta(days=days)
    if dt_pos > dt:
        str_dt_end = dt_pos.strftime("%Y%m%d")
    else:
        str_dt_end = dt.strftime("%Y%m%d")
    try:
        df_trade = client_ts_pro.trade_cal(
            exchange="", start_date="20230401", end_date=str_dt_end
        )
    except Exception as e:
        print(f"Tushare api Error!({repr(e)})")
        df_trade = pd.DataFrame()
        import sys

        sys.exit()
    if not df_trade.empty:
        df_trade.set_index(keys=["cal_date"], inplace=True)
        str_dt_pos = dt_pos.strftime("%Y%m%d")
        if df_trade.at[str_dt_pos, "is_open"] == 1:
            str_dt_return = str_dt_pos
        else:
            str_dt_return = df_trade.at[str_dt_pos, "pretrade_date"]
        dt_return = datetime.datetime.strptime(str_dt_return, "%Y%m%d")
        return dt_return


def all_chs_code(r: bool = True) -> list | None:
    df_basic = client_ts_pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date",
    )
    if len(df_basic) == 0:
        return
    else:
        list_ts_code = df_basic["ts_code"].tolist()
        list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
        if r:
            random.shuffle(list_chs_code)
        return list_chs_code


def all_stock_etf(r: bool = True) -> list | None:
    df_etf = client_ts_pro.fund_basic(market="E")
    df_etf = df_etf[
        df_etf["name"].str.contains("ETF").fillna(False)
        & (df_etf["invest_type"].str.contains("被动指数型").fillna(False))
        & (df_etf["fund_type"].str.contains("股票型").fillna(False))
        & (df_etf["status"].str.contains("L").fillna(False))
        & (df_etf["market"].str.contains("E").fillna(False))
        & (df_etf["due_date"].isnull())
    ]
    list_ts_code = df_etf["ts_code"].tolist()
    list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
    if len(df_etf) == 0:
        return
    else:
        if r:
            random.shuffle(list_chs_code)
        return list_chs_code


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
path_mv = os.path.join(path_data, f"mv")
if not os.path.exists(path_mv):
    os.mkdir(path_mv)
path_temp = os.path.join(path_data, f"temp")
if not os.path.exists(path_temp):
    os.mkdir(path_temp)
path_chip = os.path.join(path_data, f"chip")
if not os.path.exists(path_chip):
    os.mkdir(path_chip)
path_config = os.path.join(path_data, f"config")
if not os.path.exists(path_config):
    os.mkdir(path_config)
dt_trading_last_1T = latest_trading_day(days=-1)
dt_trading_last_T0 = latest_trading_day()
dt_trading_last_T1 = latest_trading_day(days=1)
dt_date_trading_last_1T = dt_trading_last_1T.date()
dt_date_trading_last_T0 = dt_trading_last_T0.date()
dt_date_trading_last_T1 = dt_trading_last_T1.date()
time_am_0500 = datetime.time(hour=5, minute=0, second=0, microsecond=0)
time_am_0910 = datetime.time(hour=9, minute=10, second=0, microsecond=0)
time_am_1015 = datetime.time(hour=10, minute=15, second=0, microsecond=0)
time_am_start = datetime.time(hour=9, minute=29, second=30, microsecond=0)
time_am_end = datetime.time(hour=11, minute=30, second=30, microsecond=0)
time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
time_pm_1457 = datetime.time(hour=14, minute=57, second=0, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
time_pm_2215 = datetime.time(hour=22, minute=15, second=0, microsecond=0)
dt_init = datetime.datetime(year=1989, month=1, day=1, hour=15)
dt_recent_fiscal_year = dt_trading_last_T0.year - 1
dt_recent_fiscal_start = datetime.datetime(year=dt_recent_fiscal_year, month=1, day=1)
dt_recent_fiscal_end = datetime.datetime(year=dt_recent_fiscal_year, month=12, day=31)
dt_am_0500 = datetime.datetime.combine(dt_date_trading_last_T0, time_am_0500)
dt_am_0910 = datetime.datetime.combine(dt_date_trading_last_T0, time_am_0910)
dt_am_1015 = datetime.datetime.combine(dt_date_trading_last_T0, time_am_1015)
dt_am_start = datetime.datetime.combine(dt_date_trading_last_T0, time_am_start)
dt_am_end = datetime.datetime.combine(dt_date_trading_last_T0, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date_trading_last_T0, time_pm_start)
dt_pm_1457 = datetime.datetime.combine(dt_date_trading_last_T0, time_pm_1457)
dt_pm_end = datetime.datetime.combine(dt_date_trading_last_T0, time_pm_end)
dt_pm_2215 = datetime.datetime.combine(dt_date_trading_last_T0, time_pm_2215)
dt_pm_end_last_1T = datetime.datetime.combine(dt_date_trading_last_1T, time_pm_end)


def dt_trading() -> datetime.datetime:
    dt_now = datetime.datetime.now()
    if dt_now < dt_am_start:
        return dt_trading_last_1T
    elif dt_now > dt_pm_end:
        return dt_trading_last_T1
    else:
        return dt_trading_last_T0


def dt_history() -> datetime.datetime:
    dt_now = datetime.datetime.now()
    if dt_now > dt_pm_end:
        return dt_trading_last_T0
    else:
        return dt_trading_last_1T


def str_trading_path() -> str:
    dt_now = datetime.datetime.now()
    if dt_now < dt_pm_end:
        return dt_date_trading_last_T0.strftime("%Y_%m_%d")
    else:
        return dt_date_trading_last_T1.strftime("%Y_%m_%d")


filename_log = os.path.join(path_log, "log{time}.log")
filename_input = os.path.join(path_main, f"input.xlsx")
filename_trader_template = os.path.join(path_main, f"trader.xlsx")
filename_market_values_shelve = os.path.join(path_mv, f"mv")
filename_index_charts = os.path.join(path_check, f"index_charts.html")
filename_concentration_rate_charts = os.path.join(
    path_check, f"concentration_rate_charts.html"
)
filename_config = os.path.join(path_config, f"config.ftr")
fall = -5
rise = 10000 / (100 + fall) - 100  # rise = 5.26315789473683
phi = 1 / golden  # extreme and mean ratio 黄金分割常数
phi_a = phi * 100
phi_b = 100 - phi_a
phi_b_neg = -(100 - phi_a)
INDUSTRY_MAX_MIN = 45
G_PRICE_MAX = 30  # g_price 最大价格 30
NOW_PRICE_MAX = G_PRICE_MAX
lIST_DAYS_MAX = 365
TOTAL_MV_E_MAX = 120


def get_trader_columns(data_type=None) -> list | dict | None:
    """
    :param data_type: e.g. "list","dict","dtype"
    :return:
    """
    dict_trader_default = {
        "name": "股票简称",
        "recent_price": 0.0,
        "position": 0,
        "now_price": 0.0,
        "pct_chg": 0.0,
        "position_unit": 0.0,
        "trx_unit_share": 0.0,
        "position_unit_max": 0.0,
        "industry_code": "000000.TI",
        "industry_name": "行业",
        "max_min": -1.0,
        "times_exceed_correct_industry": 0.0,
        "mean_exceed_correct_industry": 0.0,
        "total_mv_E": 0.0,
        "ssb_index": "index_none",
        "7Pct_T": 0.0,
        "T5_pct": 0.0,
        "T5_amplitude": 0.0,
        "G_price": 0.0,
        "gold_section": 0.0,
        "gold_section_volume": 0.0,
        "gold_section_price": 0.0,
        "gold_pct_max_min": 0.0,
        "gold_date_max": dt_init,
        "gold_date_min": dt_init,
        "gold_price_min": 0.0,
        "times_concentration": 0.0,
        "rate_concentration": 0.0,
        "recent_trading": dt_init,
        "ST": "ST_none",
        "profit_rate": 0.0,
        "dividend_rate": 0.0,
        "cash_div_period": 0.0,
        "cash_div_excepted_period": 0.0,
        "date_of_inclusion_first": dt_init,
        "date_of_inclusion_latest": dt_init,
        "times_of_inclusion": 0.0,
        "rate_of_inclusion": 0.0,
        "price_of_inclusion": 0.0,
        "pct_of_inclusion": 0.0,
        "rise": rise,
        "fall": fall,
        "factor_count": 0.0,
        "factor": "factor",
        "news": "news",
        "remark": "remark",
    }
    dict_trader_dtype = {
        "name": "object",
        "recent_price": "float64",
        "position": "float64",
        "now_price": "float64",
        "pct_chg": "float64",
        "position_unit": "float64",
        "trx_unit_share": "float64",
        "position_unit_max": "float64",
        "industry_code": "object",
        "industry_name": "object",
        "max_min": "float64",
        "times_exceed_correct_industry": "float64",
        "mean_exceed_correct_industry": "float64",
        "total_mv_E": "float64",
        "ssb_index": "object",
        "7Pct_T": "float64",
        "T5_pct": "float64",
        "T5_amplitude": "float64",
        "G_price": "float64",
        "gold_section": "float64",
        "gold_section_volume": "float64",
        "gold_section_price": "float64",
        "gold_pct_max_min": "float64",
        "gold_date_max": "datetime64[ns]",
        "gold_date_min": "datetime64[ns]",
        "gold_price_min": "float64",
        "times_concentration": "float64",
        "rate_concentration": "float64",
        "recent_trading": "datetime64[ns]",
        "ST": "object",
        "profit_rate": "float64",
        "dividend_rate": "float64",
        "cash_div_period": "float64",
        "cash_div_excepted_period": "float64",
        "date_of_inclusion_first": "datetime64[ns]",
        "date_of_inclusion_latest": "datetime64[ns]",
        "times_of_inclusion": "float64",
        "rate_of_inclusion": "float64",
        "price_of_inclusion": "float64",
        "pct_of_inclusion": "float64",
        "rise": "float64",
        "fall": "float64",
        "factor_count": "float64",
        "factor": "object",
        "news": "object",
        "remark": "object",
    }
    if data_type == "list":
        return list(dict_trader_default.keys())
    elif data_type == "dict":
        return dict_trader_default
    elif data_type == "dtype":
        return dict_trader_dtype
    else:
        return None
