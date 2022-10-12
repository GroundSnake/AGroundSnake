# coding=utf-8
import os
import dill
import datetime
from gm.api import *
from base.gm_utils import *
from base.object import *


def init(context):
    msg = f"GM Program Start.(version={get_version()})"
    log(level='info', msg=msg, source='init')  # gm终端的日志
    stocks_price = {
        "SHSE.600698": 5.01,
        "SZSE.002658": 8.00,
        "SZSE.002374": 5.30,
        "SZSE.002657": 14.30,
    }
    if os.environ.get("path_main", False):
        path_main = os.environ.get("path_main", False)
    else:
        path_main = os.getcwd()
    if context.mode == MODE_BACKTEST:
        context.path_data = os.path.join(
            path_main,
            f"data",
            r"backtest",
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
        )
    else:
        context.path_data = os.path.join(path_main, r"data", r"realtime")
    os.makedirs(context.path_data, exist_ok=True)
    file_name_log = os.path.join(context.path_data, "grid_trader.log")
    handler_stream = logging.StreamHandler(stream=None)
    handler_stream.setLevel(level=logging.INFO)  # set Console level [logging.INFO]
    handler_file = logging.FileHandler(filename=file_name_log, mode="a", encoding=None)
    handler_file.setLevel(level=logging.DEBUG)  # set file level [logging.DEBUG]
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
        handlers=[handler_stream, handler_file],
    )

    symbols = stocks_price.keys()
    subscribe(symbols=symbols, frequency="tick")  # 订阅行情数据

    schedule(schedule_func=report_status, date_rule="1d", time_rule="09:30:30")  # 定时任务
    schedule(schedule_func=report_status, date_rule="1d", time_rule="10:30:00")  # 定时任务
    schedule(schedule_func=report_status, date_rule="1d", time_rule="11:30:00")  # 定时任务
    schedule(schedule_func=report_status, date_rule="1d", time_rule="13:00:00")  # 定时任务
    schedule(schedule_func=report_status, date_rule="1d", time_rule="14:00:00")  # 定时任务
    schedule(schedule_func=report_status, date_rule="1d", time_rule="15:00:00")  # 定时任务

    context.grid_traders = {}
    context.file_path_trader = {}
    df_attributes = pandas.DataFrame()
    for symbol in symbols:
        file_name_trader = os.path.join(context.path_data, f"{symbol}.cat")
        context.file_path_trader[symbol] = file_name_trader
        if os.path.exists(file_name_trader) and context.mode != MODE_BACKTEST:
            with open(file_name_trader, "rb") as file_trader:
                context.grid_traders[symbol] = dill.load(file_trader)
                logging.info(
                    f"[{context.grid_traders[symbol].symbol}] - init:Loaded Trader from {file_name_trader}"
                )
        else:
            with open(file_name_trader, "wb") as file_trader:
                price = stocks_price[symbol]
                context.grid_traders[symbol] = GridTrader(symbol=symbol, price=price)
                dill.dump(context.grid_traders[symbol], file_trader)
        logging.info(
            f"[{context.grid_traders[symbol].symbol}]- init: Trader 构建成功，最新时间：{context.now}"
        )
        df_temp_attributes = context.grid_traders[symbol].get_status()
        if df_temp_attributes.empty:
            logging.error(f"{symbol}] - init：<Attribute> is empty")
        else:
            df_attributes = pandas.concat(
                objs=[df_attributes, df_temp_attributes], ignore_index=True
            )
    logging.info(f"\n{df_attributes}")
    file_nane_attributes = os.path.join(context.path_data, "GridTrader_attributes.xlsx")
    df_attributes.to_excel(
        excel_writer=file_nane_attributes, sheet_name="GridTrader_attributes"
    )


def on_tick(context, tick):
    print(f"\r<{context.now}> - [{tick.symbol}] - {tick.price:.2f} - Tick Received", end="")
    side = context.grid_traders[tick.symbol].get_signal(price=tick.price)
    if side == OrderSide_Buy:
        order_volume(
            symbol=tick.symbol,
            volume=context.grid_traders[tick.symbol].volume,
            side=side,
            order_type=OrderType_Market,  # 市价委托，price形参无效
            position_effect=PositionEffect_Open,
            price=tick.price,
            order_duration=OrderDuration_Unknown,
            order_qualifier=OrderQualifier_BOC,
        )
        msg = f"[{tick.symbol}] - on_tick：方向：{order_side_map[side]} - 数量：{context.grid_traders[tick.symbol].volume} -价格：{tick.price:.2f} - Time：{context.now}"
        logging.info(msg=msg)
        log(level='info', msg=msg, source='on_tick')  # gm终端的日志
    elif side == OrderSide_Sell:
        order_volume(
            symbol=tick.symbol,
            volume=context.grid_traders[tick.symbol].volume,
            side=side,
            order_type=OrderType_Market,  # 市价委托，price形参无效
            position_effect=PositionEffect_Close,
            price=tick.price,
            order_duration=OrderDuration_Unknown,
            order_qualifier=OrderQualifier_BOC,
        )
        msg = f"[{tick.symbol}] - on_tick：方向：{order_side_map[side]} - 数量：{context.grid_traders[tick.symbol].volume} -价格：{tick.price:.2f} - Time：{context.now}"
        logging.info(msg=msg)
        log(level='info', msg=msg, source='on_tick')  # gm终端的日志


def on_execution_report(context, execrpt):
    symbol = execrpt.symbol
    side = execrpt.side
    price = execrpt.price
    volume = execrpt.volume
    msg = f"{context.now} - {symbol} - {order_side_map[side]} - {volume}股 - 己成交"
    log(level='info', msg=msg, source='on_execution_report')  # gm终端的日志
    signal_switch = context.grid_traders[symbol].signal_switch
    if not signal_switch:
        context.grid_traders[symbol].record(side=side, price=price)  # 记录交易数据
        file_path_trader = context.file_path_trader[symbol]
        with open(file_path_trader, "wb") as file_trader:
            dill.dump(context.grid_traders[symbol], file_trader)  # 将交易员当前状保存至硬盘
        logging.info(
            f"[{symbol}] - on_execution_report：price:[{price}] - Time：{context.now}"
        )
        if volume != context.grid_traders[symbol].volume:
            logging.info(
                f"[{symbol}] -  on_execution_report：成交数量[{volume}]与委托数量[{context.grid_traders[symbol].volume}]不一致 - Time：{context.now}"
            )


def on_error(context, code, info):
    """
    https://www.myquant.cn/docs/python/python_other_event#e9025f677a44a3ab
    :param context:
    :param code:
    :param info:
    :return:
    """
    msg = f"[on_error] -- {context.now} - code:{code}, info:{info}"
    logging.error(msg=msg)
    log(level='error', msg=msg, source='on_error')  # gm终端的日志
    # stop()


def on_market_data_connected(context):
    """实时行情网络连接成功事件
    https://www.myquant.cn/docs/python/python_other_event#e9025f677a44a3ab
    :param context:
    :return:
    """
    msg = f'实时行情网络连接成功'
    log(level='info', msg=msg, source='on_market_data_connected')  # gm终端的日志
    msg = f"{context.now}" + msg
    logging.info(msg=msg)


def on_trade_data_connected(context):
    """交易通道网络连接成功事件
    https://www.myquant.cn/docs/python/python_other_event#e9025f677a44a3ab
    :param context:
    :return:
    """
    msg = f'交易通道网络连接'
    log(level='error', msg=msg, source='on_trade_data_connected')  # gm终端的日志
    msg = f"{context.now}" + msg
    logging.error(msg=msg)


def on_market_data_disconnected(context):
    """实时行情网络连接断开事件
    https://www.myquant.cn/docs/python/python_other_event#e9025f677a44a3ab
    :param context:
    :return:
    """
    msg = f'实时行情网络连接断开'
    log(level='error', msg=msg, source='on_market_data_disconnected')  # gm终端的日志
    msg = f"{context.now}" + msg
    logging.error(msg=msg)


def on_trade_data_disconnected(context):
    """交易通道网络连接断开事件
    https://www.myquant.cn/docs/python/python_other_event#e9025f677a44a3ab
    :param context:
    :return:
    """
    msg = f'交易通道网络连接断开'
    log(level='error', msg=msg, source='on_trade_data_disconnected')  # gm终端的日志
    msg = f"{context.now}" + msg
    logging.error(msg=msg)
