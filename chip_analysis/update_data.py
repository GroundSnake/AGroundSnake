import datetime
import os
import sys
import pandas as pd
import ashare
from loguru import logger


logger.remove()  # 移除import创建的所有handle
logger.add(sink=sys.stderr, level="TRACE")  # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"


logger.info(f"Begin")

if __name__ == "__main__":
    path_main = os.getcwd()
    path_folder = os.path.join(path_main, "data")
    if not os.path.exists(path_folder):
        os.mkdir(path_folder)
    file_name_catalogue = os.path.join(path_folder, f"catalogue.cat")
    if os.path.exists(file_name_catalogue):
        df_catalogue = pd.read_pickle(filepath_or_buffer=file_name_catalogue)  # 读取腌制断点数据
        logger.info(f"Load Catalogue Pickles")
    else:
        # list_stock = ashare.stock_list_all()
        list_stock = ["600519", "002188"]  # 测试用
        list_stock = [ashare.get_stock_type(x) + x for x in list_stock]
        df_catalogue = pd.DataFrame(index=list_stock, columns=["latest"])
        dt_delta = datetime.datetime(year=1990, month=1, day=1)
        df_catalogue.fillna(value=dt_delta, inplace=True)
        df_catalogue.to_pickle(path=file_name_catalogue)
        logger.info(f"Create and Pickle Catalogue")
    print(df_catalogue)
    time_pm = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_date = ashare.latest_trading_day()
    dt_pm = datetime.datetime.combine(dt_date, time_pm)
    frequency = "1m"  # choice of ["1m", "5m", "15m", "30m", "60m"]
    quantity = 80000
    for symbol in df_catalogue.index:
        file_name_stock = os.path.join(path_folder, f"{frequency}_{symbol}.cat")
        str_msg = f"[{symbol}]---[{dt_date}]"
        dt_max = df_catalogue.loc[symbol, "latest"]
        # logger.trace(f"dt_max={dt_max}")
        if dt_max == dt_pm:
            str_msg = str_msg + f"----Latest"
            print(str_msg)
            logger.trace(f"dt_max == dt_pm")
            continue
        elif dt_max < dt_pm:
            df_data = pd.read_pickle(filepath_or_buffer=file_name_stock)  # 读取腌制断点数据
            df_delta = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
            df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
            df_data = df_data[~df_data.index.duplicated(keep="last")]
            df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
            df_data.to_pickle(path=file_name_stock)
            df_catalogue.loc[symbol, "latest"] = dt_pm
            str_msg = str_msg + f"----update"
            logger.trace(f"dt_max < dt_pm")
        else:
            df_data = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
            logger.trace(f"dt_max else dt_pm")
            if df_data.empty:
                logger.trace(f"[{symbol}] No data")
                continue
            else:
                df_data.to_pickle(path=file_name_stock)
                df_catalogue.loc[symbol, "latest"] = dt_pm
                logger.trace(f"[{symbol}] Load Ashare")
        print(str_msg)
        df_catalogue.to_pickle(path=file_name_catalogue)
        logger.trace(f"Pickle Catalogue")
    print(df_catalogue)
