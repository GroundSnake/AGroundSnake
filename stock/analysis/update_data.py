# modified at 2023/05/18 22::25
import datetime
import os
import time
import httpx
import random
import pandas as pd
import feather
from loguru import logger
import analysis.base
import analysis.ashare
from analysis.const import (
    dt_history,
    path_data,
    path_index,
    path_temp,
    dt_init,
    dt_pm_end,
    all_chs_code,
    all_stock_etf,
    client_mootdx,
)


def update_stock_data(
    frequency: str = "1m",
    stock: bool = True,
    etf: bool = True,
    reset_catalogue: bool = False,
) -> bool:
    name: str = f"update_kline_{frequency}"
    logger.trace(f"[{frequency}] A Share Data Update Begin")
    if analysis.base.is_latest_version(key=name):
        logger.trace("update stock Kline Break and End")
        return True
    start_loop_time = time.perf_counter_ns()
    path_kline = os.path.join(path_data, f"kline_{frequency}")
    if not os.path.exists(path_kline):
        os.mkdir(path_kline)
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    file_name_catalogue_temp = os.path.join(
        path_temp, f"df_catalogue_temp_{str_dt_history_path}_{frequency}.ftr"
    )
    if reset_catalogue:
        analysis.base.delete_feather(key=f"df_catalogue_{frequency}")
        logger.trace("reset df_catalogue")
    list_all_code = list()
    if stock:
        list_all_stock = all_chs_code()
        list_all_code += list_all_stock
    else:
        list_all_stock = list()
    if etf:
        list_all_etf = all_stock_etf()
        list_all_code += list_all_etf
    else:
        list_all_etf = list()
    if os.path.exists(file_name_catalogue_temp):
        # 读取腌制数据 catalogue
        df_catalogue = feather.read_dataframe(source=file_name_catalogue_temp)
    else:
        print("stock =", len(list_all_stock), ",    ETF =", len(list_all_etf))
        df_catalogue = pd.DataFrame(
            index=list_all_code, columns=["start", "end", "count"]
        )
        df_catalogue["start"].fillna(value=dt_init, inplace=True)
        df_catalogue["end"].fillna(value=dt_init, inplace=True)
        df_catalogue["count"].fillna(value=0, inplace=True)
    df_catalogue = df_catalogue.sample(frac=1)
    count = len(df_catalogue)
    # quantity = 80000
    i = 0
    for symbol in df_catalogue.index:
        i += 1
        str_msg = f"Kline_{frequency} Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        if random.randint(a=0, b=9) == 5:
            feather.write_dataframe(df=df_catalogue, dest=file_name_catalogue_temp)
        if df_catalogue.at[symbol, "end"] == dt_pm_end:
            print(
                f"\r{str_msg} - [{df_catalogue.at[symbol, 'end']}] - Latest.\033[K",
                end="",
            )
            continue
        file_name_feather = os.path.join(path_kline, f"{symbol}.ftr")
        if os.path.exists(file_name_feather):
            df_data = feather.read_dataframe(source=file_name_feather)
        else:
            # print(f"\r{str_msg} - Kline data is not exist\033[K", end="")
            df_data = pd.DataFrame()
        if not df_data.empty:
            df_catalogue.loc[symbol, "end"] = df_data.index.max()
            df_catalogue.loc[symbol, "start"] = df_data.index.min()
            df_catalogue.loc[symbol, "count"] = len(df_data)
            if df_catalogue.at[symbol, "end"] == dt_pm_end:
                print(
                    f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - Latest - [Reset].\033[K",
                    end="",
                )
                continue
        df_delta = pd.DataFrame()
        i_while_delta = 0
        stock_code = analysis.base.get_stock_code(symbol)
        while i_while_delta < 1:
            i_while_delta += 1
            try:
                """
                df_delta = analysis.ashare.get_history_n_min_tx(
                    symbol=symbol, frequency=frequency, count=quantity
                )
                """
                if symbol in list_all_stock:
                    df_delta = client_mootdx.bars(
                        symbol=stock_code, frequency=frequency, adjust="qfq"
                    )
                elif symbol in list_all_etf:
                    df_delta = client_mootdx.bars(
                        symbol=stock_code, frequency=frequency
                    )
            except httpx.ReadTimeout as e:
                logger.error(f"\r{str_msg} - [{i_while_delta}] - {repr(e)}\033[K")
                time.sleep(3)
            except ValueError as e:
                logger.error(f"\r{str_msg} - [{i_while_delta}] - {repr(e)}\033[K")
                time.sleep(3)
            else:
                if df_delta.empty:
                    print(
                        f"\r{str_msg} - [Times:{i_while_delta}] - df_delta empty\033[K",
                    )
                    time.sleep(0.5)
                else:
                    break
        if df_delta.empty:
            if df_data.empty:
                print(
                    f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - No data\033[K",
                )
            else:
                print(
                    f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - Suspension\033[K",
                )
            continue
        else:
            df_delta["volume"] = round(df_delta["vol"] / 100, 2)
            df_delta = df_delta[["open", "close", "high", "low", "volume", "amount"]]
            if not isinstance(df_delta.index.max(), datetime.datetime):
                df_delta.index = pd.to_datetime(df_delta.index)
            if df_data.empty:
                df_data = df_delta.copy()
            else:
                df_data = pd.concat(objs=[df_data, df_delta], axis=0, join="outer")
                df_data = df_data[~df_data.index.duplicated(keep="last")]
                df_data.sort_index(ascending=True, inplace=True)
        df_data.map(func=lambda x: round(x, 2))
        feather.write_dataframe(df=df_data, dest=file_name_feather)
        dt_data_max = df_data.index.max()
        if dt_data_max > dt_pm_end:
            dt_data_max = dt_pm_end
        elif dt_data_max < dt_pm_end:
            dt_now = datetime.datetime.now().replace(microsecond=0)
            if dt_now >= dt_pm_end:
                dt_data_max = dt_pm_end
        else:
            dt_now = datetime.datetime.now().replace(microsecond=0)
            if dt_now < dt_pm_end:
                dt_data_max = dt_now
                df_data.rename(
                    index={
                        dt_pm_end: dt_now,
                    },
                    inplace=True,
                )
        df_catalogue.loc[symbol, "end"] = dt_data_max
        df_catalogue.loc[symbol, "start"] = df_data.index.min()
        df_catalogue.loc[symbol, "count"] = len(df_data)
        print(
            f"\r{str_msg} - [{df_catalogue.loc[symbol, 'end']}] - Update.\033[K", end=""
        )
    if i >= count:
        print("\n", end="")
        df_catalogue.sort_values(by=["count"], ascending=False, inplace=True)
        analysis.base.feather_to_file(
            df=df_catalogue,
            key=f"df_catalogue_{frequency}",
        )
        analysis.base.set_version(key=name, dt=df_catalogue["end"].max())
        if os.path.exists(file_name_catalogue_temp):
            os.remove(path=file_name_catalogue_temp)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"[{frequency}] A Share Data Update takes {str_gm}")
    logger.trace(f"[{frequency}] A Share Data Update End")
    return True


def update_index_data(symbol: str = "000001") -> pd.DataFrame:
    frequency = "1m"
    if symbol == "000001":
        name = f"index_{frequency}_kline_sh"
    elif symbol == "000852":
        name = f"index_{frequency}_kline_csi1000"
    else:
        name = f"index_{frequency}_kline_other"
    logger.trace(f"[{symbol}] update_index_data Begin")
    file_name_index_feather = os.path.join(path_index, f"sh{symbol}.ftr")
    if analysis.base.is_latest_version(key=name):
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"[{symbol}] update_index_data Break and End")
        return df_index
    if os.path.exists(file_name_index_feather):
        # 读取腌制数据 catalogue
        df_index = feather.read_dataframe(source=file_name_index_feather)
        logger.trace(f"Load index[{symbol}] feather from [{file_name_index_feather}]")
        df_index_delta = analysis.ashare.index_zh_a_hist_min_em(
            symbol=symbol, today=True
        )
        if df_index_delta.empty:
            df_index_delta = analysis.ashare.index_zh_a_hist_min_em(
                symbol=symbol, today=False
            )
        df_index = pd.concat(objs=[df_index, df_index_delta], axis=0, join="outer")
        df_index = df_index[~df_index.index.duplicated(keep="last")]
        df_index.fillna(value=0.0, inplace=True)
    else:
        df_index = analysis.ashare.index_zh_a_hist_min_em(symbol=symbol, today=False)
    if df_index.empty:
        logger.error(f"df_index {symbol} is empty")
    else:
        df_index.sort_index(inplace=True)
        feather.write_dataframe(df=df_index, dest=file_name_index_feather)
        if not os.path.exists(file_name_index_feather):
            logger.trace(f"Create and feather index_[{symbol}]")
        dt_update_index_data = df_index.index.max()
        analysis.base.set_version(key=name, dt=dt_update_index_data)
    logger.trace(f"[{symbol}] update_index_data End")
    return df_index
