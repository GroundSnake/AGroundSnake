import datetime
import os
import pickle
import sys
import pandas as pd
import feather
from loguru import logger
import ashare


def update_data(frequency: str = "1m") -> datetime.datetime:
    """
    :param frequency: frequency choice of ["1m", "5m", "15m", "30m", "60m"]
    :return:
    """
    logger.trace(f"[{frequency}] A Share Data Update Begin")
    dt_now = datetime.datetime.now()
    dt_date_trading = ashare.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    dt_init = datetime.datetime(year=1989, month=1, day=1)
    dt_no_data = datetime.datetime(year=1990, month=1, day=1)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    path_kline = os.path.join(path_main, "data", f"kline_{frequency}")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    file_name_config = os.path.join(path_data, f"config.pkl")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_catalogue_feather = os.path.join(path_kline, f"catalogue.ftr")
    file_name_catalogue_csv = os.path.join(path_check, f"catalogue_{str_date_path}.csv")
    time_pm = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_date = ashare.latest_trading_day()
    dt_pm = datetime.datetime.combine(dt_date, time_pm)
    quantity = 80000
    list_stock = ashare.stock_list_all()
    list_stock = [ashare.get_stock_type(x) + x for x in list_stock]
    list_stock.sort()  # list 排序
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load dict_config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
        if "update_data" in dict_config:
            logger.trace(f"the latest Kline at {dict_config['update_data']},The new at {dt_pm_end}")
            if dict_config["update_data"] < dt_now < dt_pm_end or dt_pm_end == dict_config["update_data"]:
                logger.trace(f"[{frequency}] A Share Data is latest,Break and End")
                return dict_config["update_data"]
    if os.path.exists(file_name_catalogue_feather):
        # 读取腌制数据 catalogue
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_feather)
        logger.trace(f"Load Catalogue Pickles from [{file_name_catalogue_feather}]")
    else:
        df_catalogue = pd.DataFrame(columns=["start", "end", "count"])
        logger.trace(f"Create and Pickle Catalogue")
    count = len(list_stock)
    i = 0
    logger.trace(f"for loop Begin")
    for symbol in list_stock:
        i += 1
        str_msg = f"\rKline Update[{i:4d}/{count:4d}] -- [{symbol}]---[{dt_date}]"
        if symbol not in df_catalogue.index:
            df_catalogue.at[symbol, "count"] = 0
            df_catalogue.at[symbol, "start"] = dt_init
            df_catalogue.at[symbol, "end"] = dt_init
        dt_max = df_catalogue.loc[symbol, "end"]
        file_name_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if dt_max == dt_init:
            if os.path.exists(file_name_feather):
                # 读取腌制数据 df_data
                df_data = feather.read_dataframe(source=file_name_feather)
                dt_data_max = df_data.index.max()
                if dt_data_max == dt_pm:
                    df_catalogue.loc[symbol, "end"] = df_data.index.max()
                    df_catalogue.loc[symbol, "start"] = df_data.index.min()
                    df_catalogue.loc[symbol, "count"] = len(df_data)
                    str_msg = str_msg + f"-------------latest"
                else:
                    df_delta = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
                    df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
                    df_data = df_data[~df_data.index.duplicated(keep="last")]  # 删除重复记录
                    df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
                    feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
                    df_catalogue.loc[symbol, "end"] = df_data.index.max()
                    df_catalogue.loc[symbol, "start"] = df_data.index.min()
                    df_catalogue.loc[symbol, "count"] = len(df_data)
                    str_msg = str_msg + f"-------------update"
            else:
                df_data = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
                if df_data.empty:
                    df_catalogue.loc[symbol, "end"] = dt_no_data
                    df_catalogue.loc[symbol, "start"] = dt_init
                    df_catalogue.loc[symbol, "count"] = 0
                    str_msg = str_msg + f"-unable to get data"
                else:
                    feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
                    str_msg = str_msg + f"-------------Create"
                    df_catalogue.loc[symbol, "end"] = df_data.index.max()
                    df_catalogue.loc[symbol, "start"] = df_data.index.min()
                    df_catalogue.loc[symbol, "count"] = len(df_data)
        elif dt_max == dt_no_data:
            str_msg = str_msg + f"------------No data"
        elif dt_max == dt_pm:
            str_msg = str_msg + f"-------------latest"
        elif dt_no_data < dt_max < dt_pm:
            df_data = feather.read_dataframe(source=file_name_feather)  # 读取腌制数据 df_data
            df_delta = ashare.get_history_n_min_tx(symbol=symbol, frequency=frequency, count=quantity)
            df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
            df_data = df_data[~df_data.index.duplicated(keep="last")]
            df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
            feather.write_dataframe(df=df_data, dest=file_name_feather)  # 写入腌制数据 df_data
            df_catalogue.loc[symbol, "end"] = df_data.index.max()
            df_catalogue.loc[symbol, "start"] = df_data.index.min()
            df_catalogue.loc[symbol, "count"] = len(df_data)
            str_msg = str_msg + f"----------------update"
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_feather)  # 写入腌制数据 catalogue
        print(str_msg, end="")
    if i >= count:
        print("\n", end="")
        df_catalogue['end'] = df_catalogue['end'].apply(func=lambda x: dt_init if x == dt_no_data else x)
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_feather)  # 处理初始化数据
    logger.trace(f"for loop End")
    df_catalogue.to_csv(path_or_buf=file_name_catalogue_csv)
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config["update_data"] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - Update_Data --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    logger.trace(f"Catalogue csv Save at [{file_name_catalogue_csv}]")
    logger.trace(f"[{frequency}] A Share Data Update End")
    return dt_pm_end


if __name__ == "__main__":
    # 移除import创建的所有handle
    logger.remove()
    # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
    logger.add(sink=sys.stderr, level="INFO")
    update_data(frequency="1m")
