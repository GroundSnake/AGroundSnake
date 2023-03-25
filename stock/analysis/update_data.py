# modified at 2023/3/24 15:00
import datetime
import os
import time
import sys
import pandas as pd
import feather
from loguru import logger
import ashare
import akshare as ak
import analysis.base


def update_stock_data(frequency: str = "1m") -> None:
    """
    :param frequency: frequency choice of ["1m", "5m", "15m", "30m", "60m"]
    :return:
    """
    name: str = f"update_kline_{frequency}"
    logger.trace(f"[{frequency}] A Share Data Update Begin")
    start_loop_time = time.perf_counter_ns()
    dt_init = datetime.datetime(year=1989, month=1, day=1)
    dt_no_data = datetime.datetime(year=1990, month=1, day=1)
    dt_date_trading = analysis.base.latest_trading_day()
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
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
    # file_name_chip_h5 = os.path.join(path_data, f"chip.h5")
    file_name_catalogue_temp = os.path.join(
        path_data, f"catalogue_temp_{str_date_path}.h5"
    )
    time_pm = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_date = analysis.base.latest_trading_day()
    dt_pm = datetime.datetime.combine(dt_date, time_pm)
    quantity = 80000
    list_stock = analysis.base.all_chs_code()
    if analysis.base.is_latest_version(key=name):
        logger.trace(f"update stock Kline Break")
        return
    if os.path.exists(file_name_catalogue_temp):
        # 读取腌制数据 catalogue
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_temp)
        logger.trace(f"Load Catalogue from [{file_name_catalogue_temp}]")
    else:
        df_catalogue = pd.DataFrame(columns=["start", "end", "count"])
        logger.trace(f"Create and Pickle Catalogue")
    count = len(list_stock)
    i = 0
    logger.trace(f"for loop Begin")
    for symbol in list_stock:
        i += 1
        str_msg = f"\rKline Update: [{i:4d}/{count:4d}] -- [{symbol}]---[{dt_date}]"
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
                    df_delta = ashare.get_history_n_min_tx(
                        symbol=symbol, frequency=frequency, count=quantity
                    )
                    df_data = pd.concat([df_data, df_delta], axis=0, join="outer")
                    df_data = df_data[~df_data.index.duplicated(keep="last")]  # 删除重复记录
                    df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
                    feather.write_dataframe(
                        df=df_data, dest=file_name_feather
                    )  # 写入腌制数据 df_data
                    df_catalogue.loc[symbol, "end"] = df_data.index.max()
                    df_catalogue.loc[symbol, "start"] = df_data.index.min()
                    df_catalogue.loc[symbol, "count"] = len(df_data)
                    str_msg = str_msg + f"-------------update"
            else:
                df_data = ashare.get_history_n_min_tx(
                    symbol=symbol, frequency=frequency, count=quantity
                )
                if df_data.empty:
                    df_catalogue.loc[symbol, "end"] = dt_no_data
                    df_catalogue.loc[symbol, "start"] = dt_init
                    df_catalogue.loc[symbol, "count"] = 0
                    str_msg = str_msg + f"-unable to get data"
                else:
                    feather.write_dataframe(
                        df=df_data, dest=file_name_feather
                    )  # 写入腌制数据 df_data
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
            df_delta = pd.DataFrame()
            i_times = 0
            while i_times <= 2:
                try:
                    df_delta = ashare.get_history_n_min_tx(
                        symbol=symbol, frequency=frequency, count=quantity
                    )
                    # logger.trace(f"[{symbol}] get_history_n_min_tx success")
                    break
                except ConnectionError as e:
                    print("--", repr(e))
                    time.sleep(3)
                if i_times >= 2:
                    print(f"[{symbol}] Request TimeoutError")
                    sys.exit()
                i_times += 1
            if not df_delta.empty:
                df_data = pd.concat(objs=[df_data, df_delta], axis=0, join="outer")
            df_data = df_data[~df_data.index.duplicated(keep="last")]
            df_data.sort_values(by=["datetime"], ascending=True, inplace=True)
            feather.write_dataframe(
                df=df_data, dest=file_name_feather
            )  # 写入腌制数据 df_data
            df_catalogue.loc[symbol, "end"] = df_data.index.max()
            df_catalogue.loc[symbol, "start"] = df_data.index.min()
            df_catalogue.loc[symbol, "count"] = len(df_data)
            str_msg = str_msg + f"-------------update"
        feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_temp)
        print(str_msg, end="")
    if i >= count:
        print("\n", end="")
        df_catalogue = df_catalogue[df_catalogue["count"] != 0]  # 删除无K线记录的股票
        df_catalogue["end"] = df_catalogue["end"].apply(
            func=lambda x: dt_init if x == dt_no_data else x
        )

        analysis.base.write_df_to_db(obj=df_catalogue, key="df_catalogue")
        logger.trace(f"Catalogue pickle at [pydb_chip]")
        df_catalogue.sort_values(by=["end"], ascending=False, inplace=True)
        analysis.base.add_chip_excel(df=df_catalogue, key="df_catalogue")
        analysis.base.set_version(key=name, dt=df_catalogue["end"].max())
        if os.path.exists(file_name_catalogue_temp):
            os.remove(path=file_name_catalogue_temp)
    logger.trace(f"for loop End")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"[{frequency}] A Share Data Update takes {str_gm}")
    logger.trace(f"[{frequency}] A Share Data Update End")
    return


def update_index_data(
    symbol: str = "sh000001", period: str = "1", adjust: str = ""
) -> pd.DataFrame:
    if symbol == "sh000001":
        name = "index_1kline_sh"
    elif symbol == "sh000852":
        name = "index_1kline_csi1000"
    else:
        name = "index_1kline_other"
    logger.trace(f"[{symbol}] update_index_data Begin")
    dt_date_trading = analysis.base.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_index = os.path.join(path_main, "data", f"index")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_index):
        os.mkdir(path_index)
    file_name_index_feather = os.path.join(path_index, f"{symbol}.ftr")
    if analysis.base.is_latest_version(key=name):
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"[{symbol}] update_index_data Break and End")
        return df_index
    if os.path.exists(file_name_index_feather):
        # 读取腌制数据 catalogue
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"Load index[{symbol}] feather from [{file_name_index_feather}]")
        df_index_delta = ak.stock_zh_a_minute(
            symbol=symbol, period=period, adjust=adjust
        )
        df_index_delta["day"] = pd.to_datetime(df_index_delta["day"])
        df_index_delta.set_index(keys=["day"], inplace=True)
        df_index_delta = df_index_delta.astype(float)
        df_index = pd.concat(objs=[df_index, df_index_delta], axis=0, join="outer")
        df_index = df_index[~df_index.index.duplicated(keep="last")]
    else:
        df_index = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust=adjust)
        df_index["day"] = pd.to_datetime(df_index["day"])
        df_index.set_index(keys=["day"], inplace=True)
        df_index = df_index.astype(float)
        logger.trace(f"Create and feather index[{symbol}]")
    df_index.sort_index(inplace=True)
    if not df_index.empty:
        feather.write_dataframe(df=df_index, dest=file_name_index_feather)
    analysis.base.set_version(key=name, dt=dt_pm_end)
    logger.trace(f"[{symbol}] update_index_data End")
    return df_index


if __name__ == "__main__":
    # 移除import创建的所有handle
    logger.remove()
    # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
    logger.add(sink=sys.stderr, level="INFO")
    update_stock_data(frequency="1m")
