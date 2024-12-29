from analysis.const import *
from analysis.api_tushare_const import (
    trading_t0,
    trading_1t,
    trading_t1,
    trading_2t,
    daily_basic,
)


##########################################################################
dt_trading_t0 = trading_t0()
dt_date_trading_t0 = dt_trading_t0.date()
dt_balance = datetime.datetime.combine(dt_date_trading_t0, time_balance)
dt_am_0910 = datetime.datetime.combine(dt_date_trading_t0, time_am_0910)
dt_am_0929 = datetime.datetime.combine(dt_date_trading_t0, time_am_0929)
dt_am_start = datetime.datetime.combine(dt_date_trading_t0, time_am_start)
dt_am_0931 = datetime.datetime.combine(dt_date_trading_t0, time_am_0931)
dt_am_0935 = datetime.datetime.combine(dt_date_trading_t0, time_am_0935)
dt_am_1015 = datetime.datetime.combine(dt_date_trading_t0, time_am_1015)
dt_am_1129 = datetime.datetime.combine(dt_date_trading_t0, time_am_1129)
dt_am_end = datetime.datetime.combine(dt_date_trading_t0, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date_trading_t0, time_pm_start)
dt_pm_1457 = datetime.datetime.combine(dt_date_trading_t0, time_pm_1457)
dt_pm_end = datetime.datetime.combine(dt_date_trading_t0, time_pm_end)
dt_pm_end_1t = datetime.datetime.combine(trading_1t().date(), time_pm_end)
dt_pm_end_2t = datetime.datetime.combine(trading_2t().date(), time_pm_end)
dt_pm_end_t1 = datetime.datetime.combine(trading_t1().date(), time_pm_end)
##########################################################################

df_daily_basic = daily_basic()
