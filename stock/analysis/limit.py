from __future__ import annotations
import os
import pickle
import sys
import datetime
import time
import ashare
import feather
import pandas as pd
from loguru import logger
import akshare as ak


def limit_count(list_symbol: list | str = None) -> pd.DataFrame:
    start_loop_time = time.perf_counter_ns()
    logger.trace(f"Limit Count Begin")
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if list_symbol is None:
        logger.trace("list_code is None")
        list_symbol = ashare.stock_list_all()
        list_symbol = [ashare.get_stock_type(x) + x for x in list_symbol]
    if isinstance(list_symbol, str):
        list_symbol = [list_symbol]
    list_symbol.sort()
    dt_date_trading = ashare.latest_trading_day()
    str_date_trading = dt_date_trading.strftime("%Y%m%d")
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    dt_delta = dt_date_trading - datetime.timedelta(days=366)
    str_delta = dt_delta.strftime("%Y%m%d")
    dt_now = datetime.datetime.now()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    # 转化为akshare可接受的字符串日期格式
    file_name_config = os.path.join(path_data, f"config.pkl")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_limit_feather_finish = os.path.join(path_data, f"Limit_count.ftr")
    file_name_limit_feather_temp = os.path.join(path_data, f"Limit_count_temp_{str_date_path}.ftr")
    file_name_limit_csv = os.path.join(path_check, f"Limit_count_{str_date_path}.csv")
    list_exist = list()
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        if "limit_count" in dict_config:
            logger.trace(f"the latest df_limit at {dict_config['limit_count']},The new at {dt_pm_end}")
            if dict_config["limit_count"] < dt_now < dt_pm_end or dict_config["limit_count"] == dt_pm_end:
                logger.trace(f"df_limit-[{file_name_limit_feather_finish}] is latest")
                df_limit = feather.read_dataframe(source=file_name_limit_feather_finish)
                logger.trace(f"Limit Count Break Begin")
                return df_limit
    # 读取腌制数据 df_data
    if os.path.exists(file_name_limit_feather_temp):
        logger.trace(f"{file_name_limit_feather_temp} load feather")
        df_limit = feather.read_dataframe(source=file_name_limit_feather_temp)
        if df_limit.empty:
            logger.trace("df_limit cache is empty")
        else:
            logger.trace("df_limit cache is not empty")
            list_exist = df_limit.index.to_list()
    else:
        logger.trace(f"{file_name_limit_feather_temp} not exists")
        list_columns = [
            "up_times",
            "down_times",
            "up_down",
        ]
        df_limit = pd.DataFrame(columns=list_columns)
    i = 0
    count = len(list_symbol)
    logger.trace(f"For loop Begin")
    for symbol in list_symbol:
        i += 1
        print(f"\rLimit Update [{i:4d}/{count:4d}] -- [{symbol}]", end="")
        # 测试10条记录中断，正式版删除以下代码
        """
        if i > 10:
            logger.trace(f"[i = {i}] break")
            break
        """
        # 测试10条记录中断，正式版删除以上代码
        if symbol in list_exist:
            # 己存在，断点继续
            continue
        else:
            # 未计算
            pass
        df_stock = ak.stock_zh_a_hist(
            symbol=symbol[2:8],
            period="daily",
            start_date=str_delta,
            end_date=str_date_trading,
            adjust="qfq",
        )
        if df_stock.empty:
            # 数据接口，没有该[symbol]日K线，跳过本[symbol]处理
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
        df_up = df_stock[df_stock["pct_chg"] > 9.9]
        up_times = len(df_up)
        df_down = df_stock[df_stock["pct_chg"] < -9.9]
        down_times = len(df_down)
        df_limit.loc[symbol, "up_times"] = len(df_up)
        df_limit.loc[symbol, "down_times"] = len(df_down)
        df_limit.loc[symbol, "up_down"] = up_times - down_times
        # 写入腌制数据 df_limit
        feather.write_dataframe(df=df_limit, dest=file_name_limit_feather_temp)
    if i >= count:
        print("\n", end="")  # 格式处理
    logger.trace(f"For loop End")
    if os.path.exists(file_name_limit_feather_temp):
        os.remove(path=file_name_limit_feather_temp)
        logger.trace(f"[{file_name_limit_feather_temp}] remove")
    df_limit.sort_values(by=["up_down"], ascending=False, inplace=True)
    df_limit.index.rename(name="symbol", inplace=True)
    feather.write_dataframe(df=df_limit, dest=file_name_limit_feather_finish)
    df_limit.to_csv(path_or_buf=file_name_limit_csv)
    logger.trace(f"[{file_name_limit_csv}] save")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config["limit_count"] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - Limit_Count --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    logger.trace(f"This analysis takes {str_gm}")
    logger.trace(f"Limit Count End")
    return df_limit


if __name__ == "__main__":
    # 移除import创建的所有handle
    logger.remove()
    # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
    logger.add(sink=sys.stderr, level="INFO")
    limit_count(list_symbol=["sh600519", "sz002621", "sz000422"])
