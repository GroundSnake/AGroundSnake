# coding=utf-8
import datetime
import time

from base.ashare import realtime_quotations, history_n
import pandas as pd
import os
file_name_stock = "sh600519.xlsx"

if __name__ == "__main__":
    a = os.path.getmtime(file_name_stock)
    a = time.localtime(a)
    a = time.strftime("%Y-%m-%d %H:%M:%S", a)
    print(a)

"""
    list_stock = ["sh600519", "sz002621"]
    stock = "sh600519"
    # df = history_n(symbol=stock, frequency="1m")
    con = "mysql+pymysql://root:20270426@localhost:3306/astock_1m"
    sql = f"select * from {stock}"
    df_temp = pd.read_sql_query(sql=sql, con=con)
    print(df_temp)
    print(df_temp.iloc[0, 0])
    print(df_temp.info())
"""