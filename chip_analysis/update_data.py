import datetime
import os
import sys
import pandas as pd
<<<<<<< HEAD
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
=======
import feather
from loguru import logger
import ashare


logger.remove()  # 移除import创建的所有handle
logger.add(sink=sys.stderr, level="INFO")  # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"


def update_data(frequency: str = "1m"):
    """
    :param frequency: frequency choice of ["1m", "5m", "15m", "30m", "60m"]
    :return:
    """
    logger.info(f"[{frequency}] A Share Data Update Begin")
    dt_init = datetime.datetime(year=1980, month=1, day=1)
    dt_no_data = datetime.datetime(year=1990, month=1, day=1)
    path_main = os.getcwd()
    path_folder = os.path.join(path_main, f"data_{frequency}")
    if not os.path.exists(path_folder):
        os.mkdir(path_folder)
    file_name_catalogue_feather = os.path.join(path_folder, f"catalogue.ftr")
    file_name_catalogue_csv = os.path.join(path_folder, f"catalogue.csv")
    if os.path.exists(file_name_catalogue_feather):
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_feather)  # 读取腌制数据 catalogue
        logger.trace(f"Load Catalogue Pickles")
    else:
        list_stock = ashare.stock_list_all()
        # list_stock = ["600519", "002188"]  # 测试用
        list_stock = [ashare.get_stock_type(x) + x for x in list_stock]
        list_stock.sort()  # list 排序
        df_catalogue = pd.DataFrame(index=list_stock, columns=["latest"])
        df_catalogue.fillna(value=dt_init, inplace=True)
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_feather)  # 写入腌制数据 catalogue
        logger.trace(f"Create and Pickle Catalogue")
    # print(df_catalogue)
    # df_catalogue.sort_values(by=["latest"], ascending=False, inplace=True)  # 排序 catalogue
    time_pm = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_date = ashare.latest_trading_day()
    dt_pm = datetime.datetime.combine(dt_date, time_pm)
    quantity = 80000
    i = 0
    count = len(df_catalogue)
    for symbol in df_catalogue.index:
        logger.trace(f"for loop Begin")
        i += 1
        file_name_feather = os.path.join(path_folder, f"{symbol}.ftr")
        str_msg = f"\r[{i:4d}/{count:4d}] -- [{symbol}]---[{dt_date}]"
        dt_max = df_catalogue.loc[symbol, "latest"]
        if dt_max == dt_init:
            logger.trace(f"dt_max == dt_init")
            if os.path.exists(file_name_feather):
                logger.trace(f"dt_max == dt_init and {file_name_feather} exists")
                df_data = feather.read_dataframe(source=file_name_feather)  # 读取腌制数据 df_data
                dt_data_max = df_data.index.max()
                if dt_data_max == dt_pm:
                    logger.trace(f"dt_max == dt_init and {file_name_feather} exists and dt_data_max == dt_pm")
                    df_catalogue.loc[symbol, "latest"] = df_data.index.max()
                    str_msg = str_msg + f"----------------latest"
                else:
                    logger.trace(f"dt_max == dt_init and {file_name_feather} exists and dt_data_max != dt_pm")
                    df_delta = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
                    df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
                    df_data = df_data[~df_data.index.duplicated(keep="last")]
                    df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
                    feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
                    df_catalogue.loc[symbol, "latest"] = df_data.index.max()
                    str_msg = str_msg + f"----------------update"
            else:
                logger.trace(f"dt_max == dt_init and {file_name_feather} not exists")
                df_data = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
                if df_data.empty:
                    logger.trace(f"[{symbol}] is empty")
                    df_catalogue.loc[symbol, "latest"] = dt_no_data
                    str_msg = str_msg + f"----unable to get data"
                else:
                    logger.trace(f"[{symbol}] is not empty")
                    feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
                    str_msg = str_msg + f"----------------Create"
                    df_catalogue.loc[symbol, "latest"] = df_data.index.max()
                    logger.trace(f"[{symbol}] Load Ashare")
        elif dt_max == dt_no_data:
            logger.trace(f"dt_max == dt_no_data")
            str_msg = str_msg + f"---------------No data"
        elif dt_max == dt_pm:
            logger.trace(f"dt_max == dt_pm")
            str_msg = str_msg + f"----------------latest"
        elif dt_no_data < dt_max < dt_pm:
            logger.trace(f"dt_no_data < dt_max < dt_pm")
            df_data = feather.read_dataframe(source=file_name_feather)  # 读取腌制数据 df_data
>>>>>>> 7c065a67d3d4ecf0679b21ea4653b6af9d30b946
            df_delta = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
            df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
            df_data = df_data[~df_data.index.duplicated(keep="last")]
            df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
<<<<<<< HEAD
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
=======
            feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
            df_catalogue.loc[symbol, "latest"] = df_data.index.max()
            str_msg = str_msg + f"----------------update"
        print(str_msg, end="")
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_feather)  # 写入腌制数据 catalogue
        logger.trace(f"Pickle Catalogue")
        logger.trace(f"for loop End")
    print("\n", end="")
    df_catalogue.to_csv(path_or_buf=file_name_catalogue_csv)
    logger.info(f"Catalogue csv Save")
    logger.info(f"End")
    return


if __name__ == "__main__":
    update_data(frequency="1m")
>>>>>>> 7c065a67d3d4ecf0679b21ea4653b6af9d30b946
