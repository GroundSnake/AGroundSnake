# coding=utf-8
# gm - strategy_id：d5498384-3f97-11ed-a875-b42e99f64ba9
# gm - token='3dcdec79cad272ab751050c0654ed4711a2fbdc2'
from base.gm_callable import *

os.environ['strategy_id'] = 'd5498384-3f97-11ed-a875-b42e99f64ba9'
os.environ['token'] = '3dcdec79cad272ab751050c0654ed4711a2fbdc2'
"""
os.environ['path_main'] = ''  # 设置工作目录，未设置默认为初始化进程所在目录
strategy_id策略ID, 由系统生成
filename文件名, 请与本文件名保持一致
mode运行模式, 实时模式:MODE_LIVE回测模式:MODE_BACKTEST
token绑定计算机的ID, 可在系统设置-密钥管理中生成
backtest_start_time回测开始时间
backtest_end_time回测结束时间
backtest_adjust股票复权方式, 不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
backtest_initial_cash回测初始资金
backtest_commission_ratio回测佣金比例
backtest_slippage_ratio回测滑点比例
"""


