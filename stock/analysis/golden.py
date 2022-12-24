from __future__ import annotations
import os
import pickle
import sys
import time
import datetime
import feather
import pandas as pd
import numpy as np
from scipy.constants import golden
from loguru import logger
import ashare
import analysis.update_data


def golden_price(list_code: list | str = None, frequency: str = "1m") -> pd.DataFrame:
    """分析挂仓成本
    :param list_code: e.g.sh600519
    :param frequency: choice of {"1m" ,"5m"}
    :return: pd.DataFrame
    """
    logger.trace("Golden Price Analysis Begin")
    start_loop_time = time.perf_counter_ns()
    phi = 1 / golden  # extreme and mean ratio 黄金分割常数
    all_record = 0
    if list_code is None:
        logger.trace("list_code is None")
        list_code = ashare.stock_list_all()
        list_code = [ashare.get_stock_type(x) + x for x in list_code]
    if isinstance(list_code, str):
        list_code = [list_code]
    list_code.sort()
    dt_now = datetime.datetime.now()
    dt_date_trading = ashare.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    path_check = os.path.join(path_main, "check")
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    file_name_config = os.path.join(path_data, f"config.pkl")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_golden_csv = os.path.join(path_check, f"golden_price_{str_date_path}.csv")
    file_name_golden_feather_finish = os.path.join(path_data, f"golden_price.ftr")
    file_name_golden_feather_temp = os.path.join(path_data, f"golden_price_temp_{str_date_path}.ftr")
    list_golden_exist = list()
    # 判断Kline是不是最新的
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
        if "update_data" in dict_config:
            logger.trace(f"the latest Kline at {dict_config['update_data']},The new at {dt_pm_end}")
            if dict_config["update_data"] < dt_now < dt_pm_end or dt_pm_end == dict_config["update_data"]:
                logger.trace("The Kline is latest")
            else:
                logger.trace("Update the Kline")
                analysis.update_data.update_data()
        else:
            logger.trace("Update the Kline")
            analysis.update_data.update_data()
        if 'golden_price' in dict_config:
            logger.trace(
                f"the latest df_golden at {dict_config['golden_price']},The new at {dict_config['golden_price']}")
            if dict_config['golden_price'] < dt_pm_end < dt_pm_end or dt_pm_end == dict_config['golden_price']:
                logger.trace(f"df_golden-[{file_name_golden_feather_finish}] is latest")
                df_golden = feather.read_dataframe(source=file_name_golden_feather_finish)
                logger.trace("Golden Price Analysis Break End")
                return df_golden
    if os.path.exists(file_name_golden_feather_temp):
        logger.trace(f"{file_name_golden_feather_temp} load feather")
        df_golden = feather.read_dataframe(source=file_name_golden_feather_temp)
        if df_golden.empty:
            logger.trace("df_golden cache is empty")
        else:
            logger.trace("df_golden cache is not empty")
            list_golden_exist = df_golden.index.to_list()
    else:
        logger.trace(f"{file_name_golden_feather_temp} not exists")
        list_columns = [
            "dt",
            "now_price",
            "price_ratio",
            "G_price",
            "total_volume",
        ]
        df_golden = pd.DataFrame(columns=list_columns)
    df_now_price = ashare.stock_zh_a_spot_em()
    i = 0
    count = len(list_code)
    all_record += count
    logger.trace(f"for loop Begin")
    for symbol in list_code:
        i += 1
        print(f"\rGolden Price Update [{i:4d}/{count:4d}] -- [{symbol}]", end="")
        if symbol in list_golden_exist:
            continue
        file_name_data_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_data_feather):
            # 找到kline，读取腌制数据 df_data
            df_data = feather.read_dataframe(source=file_name_data_feather)
        else:
            # 无Kline，跳过本次[symbol]处理
            continue
        df_data = df_data.iloc[-57600:]  # 取得最近1个整年的交易记录，240x240=57600算头不算尾
        dt_max = df_data.index.max()
        df_pivot = pd.pivot_table(
            df_data, index=["close"], aggfunc={"volume": np.sum, "close": len}
        )
        df_pivot.rename(columns={"close": "count"}, inplace=True)
        df_pivot.sort_values(by=["close"], ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        now_price = df_now_price.loc[symbol, "close"]
        total_volume = df_pivot["volume"].sum()
        golden_volume = round(total_volume * phi, 2)
        now_volume = 0
        df_golden.loc[symbol, "dt"] = dt_max
        df_golden.loc[symbol, "total_volume"] = total_volume
        signal_price = True
        signal_volume = True
        for tup_row in df_pivot.itertuples():
            now_volume += tup_row.volume
            if tup_row.close <= now_price and signal_price:
                if tup_row.close == now_price:
                    df_golden.loc[symbol, "now_price"] = tup_row.close
                else:
                    df_golden.loc[symbol, "now_price"] = (now_price + tup_row.close) / 2
                price_ratio = now_volume / total_volume
                df_golden.loc[symbol, "price_ratio"] = round(price_ratio, 4) * 100
                signal_price = False
                feather.write_dataframe(df=df_golden, dest=file_name_golden_feather_temp)

            if now_volume >= golden_volume and signal_volume:
                df_golden.loc[symbol, "G_price"] = tup_row.close
                signal_volume = False
                feather.write_dataframe(df=df_golden, dest=file_name_golden_feather_temp)
            if not signal_price and not signal_volume:
                break
    if i >= count:
        print("\n", end="")  # 格式处理
    logger.trace(f"for loop End")
    if os.path.exists(file_name_golden_feather_temp):
        os.remove(path=file_name_golden_feather_temp)
        logger.trace(f"[{file_name_golden_feather_temp}] remove")
    df_golden.sort_values(by=["price_ratio"], ascending=False, inplace=True)
    df_golden.index.rename(name="symbol", inplace=True)
    feather.write_dataframe(df=df_golden, dest=file_name_golden_feather_finish)
    df_golden.to_csv(path_or_buf=file_name_golden_csv)
    logger.trace(f"[{file_name_golden_csv}] save")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config["golden_price"] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - Golden_Price --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    logger.trace(f"This analysis takes {str_gm}")
    logger.trace(f"Golden Price Analysis End--{all_record}")
    return df_golden


if __name__ == "__main__":
    logger.remove()
    logger.add(sink=sys.stderr, level="INFO")  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    golden_price(list_code=["sh600519", "sz002621", "sz000422"])
