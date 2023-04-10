# modified at 2023/3/29 15:47
from __future__ import annotations
import os
import time
import random
import feather
import pandas as pd
import numpy as np
from scipy.constants import golden
from loguru import logger
import ashare
import analysis.update_data
import analysis.base
from analysis.const import (
    path_main,
    path_data,
    str_date_path,
    filename_chip_shelve,
    dt_pm_end,
    list_all_stocks,
)


def golden_price(list_code: list | str = None, frequency: str = "1m") -> bool:
    """
    :param list_code: e.g.sh600519
    :param frequency: choice of {"1m" ,"5m"}
    :return: bool
    """
    logger.trace("Golden Price Analysis Begin")
    kline: str = f"update_kline_{frequency}"
    name: str = f"df_golden"
    dt_golden = None
    start_loop_time = time.perf_counter_ns()
    phi = 1 / golden  # extreme and mean ratio 黄金分割常数
    if list_code is None:
        logger.trace("list_code is None")
        list_code = list_all_stocks
    if isinstance(list_code, str):
        list_code = [list_code]
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    filename_df_golden_temp = os.path.join(
        path_data, f"df_golden_temp_{str_date_path}.ftr"
    )
    # 判断Kline是不是最新的
    if analysis.base.is_latest_version(key=kline, filename=filename_chip_shelve):
        pass
    else:
        logger.trace("Update the Kline")
        if analysis.update_data.update_stock_data():
            logger.trace("{kline} Update finish")
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace("Golden Price Analysis Break End")
        return True  # df_golden is object
    list_golden_exist = list()
    if os.path.exists(filename_df_golden_temp):
        logger.trace(f"[{filename_df_golden_temp}] load feather")
        df_golden = feather.read_dataframe(source=filename_df_golden_temp)
        if df_golden.empty:
            logger.trace("df_golden cache is empty")
        else:
            logger.trace("df_golden cache is not empty")
            list_golden_exist = df_golden.index.to_list()
    else:
        logger.trace(f"[{filename_df_golden_temp}] not exists")
        list_columns = [
            "dt",
            "total_volume",
            "now_price",
            "now_price_ratio",
            "now_price_volume",
            "G_price",
            "G_price_volume",
        ]
        df_golden = pd.DataFrame(columns=list_columns)
    df_now_price = ashare.stock_zh_a_spot_em()
    i = 0
    all_record = len(list_code)
    logger.trace(f"for loop Begin")
    for symbol in list_code:
        i += 1
        print(f"\rGolden Price Update:[{i:4d}/{all_record:4d}] -- [{symbol}]", end="")
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
        if dt_golden is None:
            dt_golden = dt_max
        elif dt_golden < dt_max:
            dt_golden = dt_max
        else:
            dt_golden = dt_pm_end
        df_pivot = pd.pivot_table(
            df_data, index=["close"], aggfunc={"volume": np.sum, "close": len}
        )
        df_pivot.rename(columns={"close": "count"}, inplace=True)
        df_pivot.sort_values(by=["close"], ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        now_price = df_now_price.at[symbol, "close"]
        total_volume = df_pivot["volume"].sum()
        golden_volume = round(total_volume * phi, 2)
        temp_volume = 0
        df_golden.at[symbol, "dt"] = dt_max
        df_golden.at[symbol, "total_volume"] = total_volume
        signal_price = True
        signal_volume = True
        for tup_row in df_pivot.itertuples():
            temp_volume += tup_row.volume
            if tup_row.close <= now_price and signal_price:
                if tup_row.close == now_price:
                    df_golden.at[symbol, "now_price"] = tup_row.close
                else:
                    df_golden.at[symbol, "now_price"] = (now_price + tup_row.close) / 2
                price_ratio = temp_volume / total_volume
                df_golden.at[symbol, "now_price_ratio"] = round(price_ratio, 4) * 100
                df_golden.at[symbol, "now_price_volume"] = temp_volume
                signal_price = False
            if temp_volume >= golden_volume and signal_volume:
                df_golden.at[symbol, "G_price"] = tup_row.close
                df_golden.at[symbol, "G_price_volume"] = temp_volume
                signal_volume = False
            if not signal_price and not signal_volume:
                break
        if random.randint(0, 5) == 3:
            feather.write_dataframe(df=df_golden, dest=filename_df_golden_temp)
    if i >= all_record:
        print("\n", end="")  # 格式处理
        df_golden.index.rename(name="symbol", inplace=True)
        df_golden.sort_values(by=["now_price_ratio"], ascending=False, inplace=True)
        analysis.base.write_obj_to_db(
            obj=df_golden, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_golden)
        if os.path.exists(filename_df_golden_temp):  # 删除临时文件
            os.remove(path=filename_df_golden_temp)
            logger.trace(f"[{filename_df_golden_temp}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"Golden Price Analysis takes [{str_gm}]")
    logger.trace(f"Golden Price Analysis End--[all_record={all_record}]")
    return True
