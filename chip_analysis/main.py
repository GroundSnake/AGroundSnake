from __future__ import annotations
import os
import sys
import ashare
import pandas as pd
from loguru import logger
import cost
import limit

# 移除import创建的所有handle
logger.remove()
# 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
logger.add(sink=sys.stderr, level="INFO")

source_choose = "ak"
# "ak"：全市场标的，"excel"：文件中的标的。注意：选该参数“excel”，文件"specified.xlsx"必须存在。
file_name_in = "specified.xlsx"  # 指定标的说文件读取
path_main = os.getcwd()


if __name__ == "__main__":
    dt_now = ashare.latest_trading_day()
    str_now = dt_now.strftime("%Y%m%d")
    df1 = cost.cost()
    df1.sort_index(inplace=True)
    df2 = limit.limit()
    df2.sort_index(inplace=True)
    print(df1)
    print(df2)
    df3 = pd.concat([df1, df2], axis=1, join="outer")
    df3.dropna(axis=0, how="any", inplace=True)
    df3.sort_values(by=["pct_chg", "up_down", "up_down"], ascending=True, inplace=True)
    # df3 = df3[df3.up_down >= 12 & df3.signal == "Buy"]
    print(df3)
    df3.to_csv(path_or_buf=f"temp_{str_now}.csv")
    df4 = pd.concat([df1, df2], axis=0, join="outer")
    print(df4)
    df4.to_csv(path_or_buf=f"temp2_{str_now}.csv")
