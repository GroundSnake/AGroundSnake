from __future__ import annotations
import os
import sys
import datetime
import time
import akshare as ak
import ashare
import feather
import pandas as pd
from loguru import logger

# 移除import创建的所有handle
logger.remove()
# 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
logger.add(sink=sys.stderr, level="INFO")

# "ak"：全市场标的，"excel"：文件中的标的。注意：选该参数“excel”，文件"specified.xlsx"必须存在。
source_choose = "ak"
# 指定标的说文件读取
file_name_in = "specified.xlsx"
path_main = os.getcwd()


def limit() -> pd.DataFrame:
    logger.info(f"Limit Begin")
    start_loop_time = time.perf_counter_ns()
    list_symbol = ashare.stock_list_all()
    # 转化为力sh，sz格式的股票代码
    list_symbol = [ashare.get_stock_type(x) + x for x in list_symbol]
    dt_now = ashare.latest_trading_day()
    str_now = dt_now.strftime("%Y%m%d")
    dt_delta = dt_now - datetime.timedelta(days=366)
    # 转化为akshare可接受的字符串日期格式
    str_delta = dt_delta.strftime("%Y%m%d")
    file_name_data_feather = os.path.join(path_main, f"Limit_count_{str_now}.ftr")
    file_name_out_csv = os.path.join(path_main, f"Limit_count_{str_now}.csv")
    if os.path.exists(file_name_out_csv):
        df_out = pd.read_csv(filepath_or_buffer=file_name_out_csv, index_col="symbol")
        logger.info(f"{file_name_out_csv} exists and limit End")
        return df_out
    list_exist = list()
    # 读取腌制数据 df_data
    if os.path.exists(file_name_data_feather):
        logger.info(f"{file_name_data_feather} load feather")
        df_out = feather.read_dataframe(source=file_name_data_feather)
        if df_out.empty:
            logger.trace("df_out cache is empty")
        else:
            logger.trace("df_out cache is not empty")
            list_exist = df_out.index.to_list()
    else:
        logger.trace(f"{file_name_data_feather} not exists")
        list_columns = [
            "up_times",
            "down_times",
            "up_down",
        ]
        df_out = pd.DataFrame(columns=list_columns)
    i = 0
    count = len(list_symbol)
    # print(list_symbol)
    # print(list_exist)
    # 包括sleep的时间，统计挂钟时间
    for symbol in list_symbol:
        logger.trace(f"[{symbol}] loop Begin")
        i += 1
        # 以下1条print代码, 当logger设为TRACE时关闭
        print(f"\r[{i:4d}/{count:4d}] -- [{symbol}]", end="")
        # 测试10条记录中断，正式版删除以下代码
        """
        if i > 10:
            logger.trace(f"[i = {i}] break")
            break
        """
        # 测试10条记录中断，正式版删除以上代码
        if symbol in list_exist:
            logger.trace(f"[{symbol}] in list_exist")
            continue
        else:
            logger.trace(f"[{symbol}] not in list_exist")
        df_stock = ak.stock_zh_a_hist(
            symbol=symbol[2:8],
            period="daily",
            start_date=str_delta,
            end_date=str_now,
            adjust="qfq",
        )
        if df_stock.empty:
            logger.trace(f"[{symbol}] df_stock is empty")
            continue
        df_stock.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "pct_chg",
                "涨跌额": "change",
                "换手率": "turnover",

            },
            inplace=True,
        )
        # print(df_stock)
        df_up = df_stock[df_stock["pct_chg"] > 9.9]
        up_times = len(df_up)
        df_down = df_stock[df_stock["pct_chg"] < -9.9]
        down_times = len(df_down)
        df_out.loc[symbol, "up_times"] = len(df_up)
        df_out.loc[symbol, "down_times"] = len(df_down)
        df_out.loc[symbol, "up_down"] = up_times - down_times
        # 写入腌制数据 df_out
        feather.write_dataframe(df=df_out, dest=file_name_data_feather)
        logger.trace(f"{file_name_data_feather} save -- [{symbol}]")
        if i >= count:
            if os.path.exists(file_name_data_feather):
                os.remove(file_name_data_feather)
                logger.trace(f"{file_name_data_feather} remove")
    print("\n", end="")  # ##
    df_out.sort_values(by=["up_down"], ascending=False, inplace=True)
    df_out.index.rename(name="symbol", inplace=True)
    df_out.to_csv(path_or_buf=file_name_out_csv)
    logger.info(f"[{file_name_out_csv}] save")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    logger.info(f"This analysis takes {str_gm}")
    logger.info(f"Limit Begin")
    return df_out


if __name__ == "__main__":
    limit()
