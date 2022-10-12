# -*- coding:utf-8 -*-
import os
import sys
import dill
import pandas as pd
import numpy as np
from loguru import logger
import scipy.stats as stats

from base.ashare import get_price

logger.remove()
logger.add(sink=sys.stderr, level="TRACE")


if __name__ == "__main__":
    """
    x = np.random.normal(0, 1, 100)
    logger.info(f"sh000001\n{x}")
    tuple_temp = stats.normaltest(a=x)
    logger.info(f"normaltest\n{tuple_temp}")
    tuple_temp = stats.kurtosistest(a=x)
    logger.info(f"kurtosistest\n{tuple_temp}")
    """
    file_name_temp = "sh00001_1645.cat"
    if os.path.exists(file_name_temp):
        with open(file=file_name_temp, mode="rb") as file_temp:
            df_temp = dill.load(file=file_temp)
            logger.trace("load")
    else:
        with open(file=file_name_temp, mode="wb") as file_temp:
            df_temp = get_price(code="sh000001", count=1645, frequency="1d")
            dill.dump(obj=df_temp, file=file_temp)
            logger.trace("dump")
    logger.info(f"sh000001\n{df_temp}")
    # list_temp = df_temp["close"].apply(func=lambda a: a//10*10).to_list()
    df_temp["close"] = df_temp["close"].apply(func=lambda a: a//10*10)
    df_temp["index"] = df_temp["close"]
    logger.info(f"sh000001\n{df_temp}")
    table = pd.pivot_table(df_temp, index=['index'], values=['close'], aggfunc="count")
    logger.info(f"table\n{table}")
    table.to_csv(path_or_buf="123.csv")
    """
    list_temp = df_temp["close"].to_list()
    # logger.info(f"sh000001\n{list_temp}")
    tuple_temp = stats.normaltest(a=list_temp)
    logger.info(f"normaltest\n{tuple_temp}")
    tuple_temp = stats.kurtosistest(a=list_temp)
    logger.info(f"kurtosistest\n{tuple_temp}")
    """