from __future__ import annotations
import os
import sys
from scipy.constants import golden
import time
import ashare
import feather
import pandas as pd
import numpy as np
from loguru import logger

logger.remove()  # 移除import创建的所有handle
logger.add(
    sink=sys.stderr, level="INFO"
)  # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"

source_choose = "ak"
# "ak"：全市场标的，"excel"：文件中的标的。注意：选该参数“excel”，文件"specified.xlsx"必须存在。
file_name_in = "specified.xlsx"  # 指定标的说文件读取


def cost(list_code: list | str = None, frequency: str = "1m") -> pd.DataFrame:
    """分析挂仓成本
    :param list_code: e.g.sh600519
    :param frequency: choice of {"1m" ,"5m"}
    :return: pd.DataFrame
    """
    logger.info("analysis Begin")
    phi = 1 / golden  # extreme and mean ratio 黄金分割常数
    start_loop_time = time.perf_counter_ns()
    if list_code is None:
        logger.trace("list_code is None")
        list_code = ashare.stock_list_all()
        list_code = [ashare.get_stock_type(x) + x for x in list_code]
    if isinstance(list_code, str):
        list_code = [list_code]
    dt_now = ashare.latest_trading_day()
    str_now = dt_now.strftime("%Y%m%d")
    path_main = os.getcwd()
    path_folder = os.path.join(path_main, f"data_{frequency}")
    file_name_out_csv = os.path.join(path_main, f"cost_{str_now}.csv")
    file_name_out_feather = os.path.join(path_main, f"cost_{str_now}.ftr")
    list_exist = list()
    if os.path.exists(file_name_out_feather):
        logger.info(f"{file_name_out_feather} load feather")
        df_out = feather.read_dataframe(source=file_name_out_feather)
        if df_out.empty:
            logger.trace("df_out cache is empty")
        else:
            logger.trace("df_out cache is not empty")
            list_exist = df_out.index.to_list()
    else:
        logger.trace(f"{file_name_out_feather} not exists")
        list_columns = [
            "price_half",
            "sum_volume",
            "half_volume",
            "now",
            "pct_chg",
            "signal",
            "dt",
        ]
        df_out = pd.DataFrame(columns=list_columns)
    if os.path.exists(file_name_out_csv):
        df_out = pd.read_csv(filepath_or_buffer=file_name_out_csv, index_col="symbol")
        logger.info(f"{file_name_out_csv} exists and analysis End")
        return df_out
    df_now_price = ashare.stock_zh_a_spot_em()
    i = 0
    count = len(list_code)
    for symbol in list_code:
        logger.trace(f"[{symbol}] loop Begin")
        i += 1
        # 以下1条print代码, 当logger设为TRACE时关闭
        print(f"\r[{i:4d}/{count:4d}] -- [{symbol}]", end="")
        # 测试10条记录中断，正式版删除以下代码
        """
        if i > 13:
            logger.trace(f"[i = {i}] break")
            break
        """
        # 测试10条记录中断，正式版删除以上代码
        if symbol in list_exist:
            logger.trace(f"[{symbol}] in list_exist")
            continue
        else:
            logger.trace(f"[{symbol}] not in list_exist")
        file_name_data_feather = os.path.join(path_folder, f"{symbol}.ftr")
        # df_data = None
        if os.path.exists(file_name_data_feather):
            logger.trace(f"[{file_name_data_feather}] is exists")
            # 读取腌制数据 df_data
            df_data = feather.read_dataframe(source=file_name_data_feather)
        else:
            logger.trace(f"[{file_name_data_feather}] is not exists")
            logger.trace(f"[{symbol}] continue")
            continue
        dt_max = df_data.index.max()
        df_pivot = pd.pivot_table(
            df_data, index=["close"], aggfunc={"volume": np.sum, "close": len}
        )
        df_pivot.rename(columns={"close": "count"}, inplace=True)
        df_pivot.sort_values(by=["close"], ascending=False, inplace=True)
        df_pivot.reset_index(inplace=True)
        number_half_sum = round(df_pivot["volume"].sum() * phi, 2)
        number_count = 0
        for tup_row in df_pivot.itertuples():
            number_count += tup_row.volume
            if number_count > number_half_sum:
                df_out.loc[symbol, "price_half"] = tup_row.close
                df_out.loc[symbol, "sum_volume"] = df_pivot["volume"].sum()
                df_out.loc[symbol, "half_volume"] = number_count
                now_price = df_now_price.loc[symbol, "close"]
                df_out.loc[symbol, "now"] = now_price
                df_out.loc[symbol, "pct_chg"] = round(
                    (now_price / tup_row.close - 1) * 100, 2
                )
                df_out.loc[symbol, "dt"] = dt_max
                if now_price < tup_row.close:
                    df_out.loc[symbol, "signal"] = "Buy"
                else:
                    df_out.loc[symbol, "signal"] = "Sell"
                feather.write_dataframe(df=df_out, dest=file_name_out_feather)
                logger.trace(f"{file_name_out_feather} save -- [{symbol}]")
                break
    if i == count:
        logger.trace(f"i >= count")
        if os.path.exists(file_name_out_feather):
            os.remove(file_name_out_feather)
            logger.info(f"{file_name_out_feather} remove")
    print("\n", end="")  # ##
    df_out.sort_values(by=["pct_chg"], ascending=True, inplace=True)
    df_out.index.rename(name="symbol", inplace=True)
    df_out.to_csv(path_or_buf=file_name_out_csv)
    logger.info(f"[{file_name_out_csv}] save")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    logger.info(f"This analysis takes {str_gm}")
    logger.info("analysis End")
    return df_out


if __name__ == "__main__":
    cost()
