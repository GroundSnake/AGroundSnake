# -*- coding:utf-8 -*-
from __future__ import annotations
import re
import random
import requests
import datetime
import pandas as pd


headers = {
        "Accept-Encoding": "gzip, deflate, sdch",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.100 Safari/537.36",
    }


def get_history_n_sina(symbol, frequency="1d", count=10):
    """
    http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=sh600519&scale=5&ma=5&datalen=1
    :param symbol: eg."sh600519"
    :param count: default "1d",options["5m", "15m", "30m", "60m", "1d", "1w", "1M"]
    :param frequency: default 10
    :return:
    """
    if frequency not in ["5m", "15m", "30m", "60m", "1d", "1w", "1M"]:
        frequency = "1d"
    frequency = (
        frequency.replace("1d", "240m").replace("1w", "1200m").replace("1M", "7200m")
    )
    frequency = frequency[:-1]
    if frequency.isdigit():
        frequency = int(frequency)
    else:
        frequency = 240
    url_sina = f"http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol={symbol}&scale={frequency}&ma=5&datalen={count}"
    rs = requests.get(url_sina)
    data = rs.json()
    df_sina = pd.DataFrame(
        data, columns=["day", "open", "high", "low", "close", "volume"]
    )
    df_sina.rename(columns={"day": "datetime"}, inplace=True)
    df_sina.datetime = pd.to_datetime(df_sina.datetime)
    df_sina.set_index(keys=["datetime"], inplace=True)
    # df_sina.index.rename(name="datetime", inplace=True)  # 索引改名
    # df_sina.index.name = ""  # 索引改名
    df_sina = df_sina.applymap(func=float)
    df_sina = df_sina.reindex(labels=["open", "close", "high", "low", "volume"], axis=1)  # 重新排序所有列
    df_sina["volume"] = df_sina["volume"].apply(func=lambda x: round(x, -2)//100)
    return df_sina


def get_history_n_tx(symbol, frequency="1d", count=10):
    """
    https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600519,day,2022-09-01,2022-10-10,10,qfq
    :param symbol:
    :param count:
    :param frequency: default "1d",options["1d", "1w", "1M"]
    :return:
    """
    if frequency not in ["1d", "1w", "1M"]:
        frequency = "1d"
    frq_map = {"1d": "day",
               "1w": "week",
               "1M": "month",
               }
    frequency = frq_map.get(frequency, "day")
    url_qq = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},{frequency},,,{count},qfq"
    rs = requests.get(url=url_qq, headers=headers)
    data = rs.json()
    data = data["data"][symbol]
    adjust = "qfq" + frequency
    data = data[adjust] if adjust in data else data[frequency]  # 指数返回不是qfqday,是day
    df_qq = pd.DataFrame(data)  # ["time", "open", "close", "high", "low", "volume"]
    df_qq = df_qq.iloc[:, 0:6]  # 用序号取第0到第5列的切片
    df_qq.rename(columns={0: "datetime", 1: "open", 2: "close", 3: "high", 4: "low", 5: "volume"}, inplace=True)
    # df_qq.columns = ["time", "open", "close", "high", "low", "volume"]
    df_qq.datetime = pd.to_datetime(df_qq.datetime)
    df_qq.set_index(keys=["datetime"], inplace=True)
    # df_qq.index.rename(name="datetime", inplace=True)  # 索引改名
    # df_qq.index.name = ""  # 索引改名
    df_qq = df_qq.round(2)
    return df_qq


def get_history_n_min_tx(symbol, frequency="5m", count=10):  # 分钟线获取
    """
    :param symbol:
    :param count:
    :param frequency: default "5m",options["1m", "5m", "15m","30m", "60m"]
    :return:
    """
    if frequency not in ["1m", "5m", "15m", "30m", "60m"]:
        frequency = "5m"
    frq_map = {"1m": "m1",
               "5m": "m5",
               "15m": "m15",
               "30m": "m30",
               "60m": "m60",
               }
    frequency = frq_map.get(frequency, "5m")
    url_qq = f"http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={symbol},{frequency},,{count}"
    rs = requests.get(url=url_qq, headers=headers)
    data = rs.json()
    data = data["data"][symbol][frequency]
    df_qq = pd.DataFrame(data)  # , columns=["time", "open", "close", "high", "low", "volume", "n1", "n2"]
    df_qq = df_qq.iloc[:, 0:6]  # 用序号取第0到第5列的切片
    df_qq.rename(columns={0: "datetime", 1: "open", 2: "close", 3: "high", 4: "low", 5: "volume"}, inplace=True)
    df_qq["datetime"] = pd.to_datetime(df_qq["datetime"])
    df_qq.set_index(["datetime"], inplace=True)
    df_qq = df_qq.applymap(func=float)
    return df_qq


def history_n(symbol, frequency="1d", count=10):
    source = random.choice(["sina", "tencent"])  # 随机选择数据源，防ban
    if frequency in ["1d", "1w", "1M"]:  # 1d日线  1w周线  1M月线
        if source == "sina":
            return get_history_n_sina(symbol=symbol, frequency=frequency, count=count)
        elif source == "tencent":
            return get_history_n_tx(symbol=symbol, frequency=frequency, count=count)
        else:
            return None
    elif frequency in ["5m", "15m", "30m", "60m"]:
        if source == "sina":
            return get_history_n_sina(symbol=symbol, frequency=frequency, count=count)
        elif source == "tencent":
            return get_history_n_min_tx(symbol=symbol, frequency=frequency, count=count)
    elif frequency in ["1m"]:
        return get_history_n_min_tx(symbol=symbol, frequency=frequency, count=count)
    else:
        return None


def realtime_quotations(stock_codes: str | list):
    """
    http://qt.gtimg.cn/q=sh600519,sz002621
    :return:
    """
    grep_stock_code = re.compile(r"(?<=_)\w+")
    if not isinstance(stock_codes, list):
        stock_codes = [stock_codes]
    str_stocks = ",".join(stock_codes)
    url = "http://qt.gtimg.cn/q="
    url = url + str_stocks
    rs = requests.get(url=url, headers=headers)
    rep_data = rs.text
    stocks_detail = "".join(rep_data)
    stock_details = stocks_detail.split(";")
    stock_dict = dict()
    for stock_detail in stock_details:
        stock = stock_detail.split("~")
        if len(stock) <= 49:
            continue
        stock_code = grep_stock_code.search(stock[0]).group()
        stock_dict[stock_code] = {
            "name": stock[1],
            "code": stock_code,
            "close": float(stock[3]),
            "pre_close": float(stock[4]),
            "open": float(stock[5]),
            "volume": float(stock[6]) * 100,
            "bid_volume": int(stock[7]) * 100,
            "ask_volume": float(stock[8]) * 100,
            "bid1": float(stock[9]),
            "bid1_volume": int(stock[10]) * 100,
            "bid2": float(stock[11]),
            "bid2_volume": int(stock[12]) * 100,
            "bid3": float(stock[13]),
            "bid3_volume": int(stock[14]) * 100,
            "bid4": float(stock[15]),
            "bid4_volume": int(stock[16]) * 100,
            "bid5": float(stock[17]),
            "bid5_volume": int(stock[18]) * 100,
            "ask1": float(stock[19]),
            "ask1_volume": int(stock[20]) * 100,
            "ask2": float(stock[21]),
            "ask2_volume": int(stock[22]) * 100,
            "ask3": float(stock[23]),
            "ask3_volume": int(stock[24]) * 100,
            "ask4": float(stock[25]),
            "ask4_volume": int(stock[26]) * 100,
            "ask5": float(stock[27]),
            "ask5_volume": int(stock[28]) * 100,
            "tick": stock[29],
            "datetime": datetime.datetime.strptime(stock[30], "%Y%m%d%H%M%S"),
            "change": float(stock[31]),
            "pct_chg": float(stock[32]),
            "high": float(stock[33]),
            "low": float(stock[34]),
            "price/volume/amount": stock[35],
            "volume": int(stock[36]) * 100,
            "amount": float(stock[37]) * 10000,
            "turnover": float(stock[38]),
            "pe_ttm": float(stock[39]),
            "unknown": stock[40],
            "high_2": float(stock[41]),  # 意义不明
            "low_2": float(stock[42]),  # 意义不明
            "amplitude": float(stock[43]),
            "circ_mv": float(stock[44]),
            "total_mv": float(stock[45]),
            "PB": float(stock[46]),
            "up_limit": float(stock[47]),
            "down_limit": float(stock[48]),
            "volume_ratio": float(stock[49]),
            "weicha": float(stock[50]),
            "ma": float(stock[51]),
            "unknown": float(stock[52]),
            "pe": float(stock[53]),
        }
    return stock_dict


if __name__ == "__main__":
    import sys
    from loguru import logger
    logger.remove()
    logger.add(sink=sys.stderr, level="TRACE")  # "TRACE","DEBUG","INFO"
    list_stock = ["sh600519", "sz002621"]
    str_stock = "sh600519"
    test = realtime_quotations(stock_codes=list_stock)
    test = pd.DataFrame(test).T
    logger.trace(f"realtime_quotations\n{test}")
    test = history_n(symbol=str_stock, frequency="1m")
    logger.trace(f"history_n\n{test}")
    test = get_history_n_tx(symbol=str_stock, frequency="1d", count=10)
    logger.trace(f"get_history_n_tx\n{test}")
    test = get_history_n_sina(symbol=str_stock, frequency="1d", count=10)
    logger.trace(f"get_history_n_sina\n{test}")
    test = get_history_n_min_tx(symbol=str_stock, frequency="5m", count=10)
    logger.trace(f"get_history_n_min_tx\n{test}")


