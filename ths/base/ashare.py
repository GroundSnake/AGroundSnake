# -*- coding:utf-8 -*-
import random
import requests
import pandas as pd


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
    rs = requests.get(url_qq)
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
    rs = requests.get(url_qq)
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


if __name__ == "__main__":
    import sys
    from loguru import logger
    logger.remove()
    logger.add(sink=sys.stderr, level="TRACE")  # "TRACE","DEBUG","INFO"
    test = history_n(symbol="sz002621", frequency="5m")
    logger.trace(f"002621\n{test}")
