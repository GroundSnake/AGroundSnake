# -*- coding:utf-8 -*-
import os
import sys
import dill
import pandas as pd
import numpy as np
from loguru import logger
import stockstats
import scipy.stats as stats

from base.ashare import get_price

logger.remove()
logger.add(sink=sys.stderr, level="TRACE")  # "TRACE","DEBUG","INFO"


if __name__ == "__main__":
    symbol = "sz002621"
    file_name_temp = f"{symbol}_1645.cat"
    file_name_csv = f"{symbol}_stockstats.csv"

    if os.path.exists(file_name_temp):
        with open(file=file_name_temp, mode="rb") as file_temp:
            df_temp = dill.load(file=file_temp)
            logger.trace("load")
    else:
        with open(file=file_name_temp, mode="wb") as file_temp:
            df_temp = get_price(code=symbol, count=1645, frequency="1d")
            dill.dump(obj=df_temp, file=file_temp)
            logger.trace("dump")
    df_temp.index.rename(name="date", inplace=True)  # 索引改名
    logger.debug(f"{symbol}\n{df_temp}")
    df_stock_temp = stockstats.wrap(df_temp)
    logger.debug(f"{symbol}\n{df_stock_temp}")
    df = df_stock_temp[['high', 'high_2_ema', 'high_2_sma']]
    df.to_csv(path_or_buf=file_name_csv)
    logger.debug(f"{symbol}\n{df_stock_temp}")
    logger.debug(f"{symbol}\n{df}")

    """
    x = np.random.normal(0, 1, 100)
    logger.info(f"sh000001\n{x}")
    tuple_temp = stats.normaltest(a=x)
    logger.info(f"normaltest\n{tuple_temp}")
    tuple_temp = stats.kurtosistest(a=x)
    logger.info(f"kurtosistest\n{tuple_temp}")
    logger.info(f"sh000001\n{df_temp}")
    # list_temp = df_temp["close"].apply(func=lambda a: a//10*10).to_list()
    df_temp["close"] = df_temp["close"].apply(func=lambda a: a//10*10)
    df_temp["index"] = df_temp["close"]
    logger.info(f"sh000001\n{df_temp}")
    table = pd.pivot_table(df_temp, index=['index'], values=['close'], aggfunc="count")
    logger.info(f"table\n{table}")
    table.to_csv(path_or_buf="123.csv")

    list_temp = df_temp["close"].to_list()
    # logger.info(f"sh000001\n{list_temp}")
    tuple_temp = stats.normaltest(a=list_temp)
    logger.info(f"normaltest\n{tuple_temp}")
    tuple_temp = stats.kurtosistest(a=list_temp)
    logger.info(f"kurtosistest\n{tuple_temp}")
    """