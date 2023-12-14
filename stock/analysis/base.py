# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import time
import re
import random
import sys
import math
import datetime
import shelve
import dbm
import feather
import win32file
import pandas as pd
from pathlib import Path
from console import fg
from pandas import DataFrame
from loguru import logger
from analysis.mootdx.consts import MARKET_SZ, MARKET_SH, MARKET_BJ
from analysis.const import (
    dt_am_0910,
    dt_pm_end,
    dt_pm_end_last_1T,
    path_chip,
    client_ts_pro,
    filename_config,
    path_check,
    dt_history,
)


def is_trading_day(dt: datetime.datetime = None) -> bool:
    if dt is None:
        dt = datetime.datetime.now()
    dt_start = dt - datetime.timedelta(days=14)
    str_date_start = dt_start.strftime("%Y%m%d")
    str_date_now = dt.strftime("%Y%m%d")
    try:
        df_trade = client_ts_pro.trade_cal(
            exchange="", start_date=str_date_start, end_date=str_date_now
        )
    except Exception as e:
        print(
            f"The token is invalid. Please apply for a token at tushare - Error-[{e}]"
        )
        sys.exit()
    df_trade.set_index(keys=["cal_date"], inplace=True)
    try:
        if df_trade.at[str_date_now, "is_open"] == 1:
            return True
        else:
            return False
    except KeyError as e:
        print(repr(e))
        sys.exit()


def code_ths_to_ts(symbol: str):
    """
    :param symbol: "sh600519"
    :return: "600519.sh"
    """
    if isinstance(symbol, str):
        return symbol[2:] + "." + symbol[0:2].upper()
    else:
        return


def code_ts_to_ths(ts_code: str):
    """
    :param ts_code: "600519.sh"
    :return: "sh600519"
    """
    if isinstance(ts_code, str):
        return ts_code[-2:].lower() + ts_code[:6]
    else:
        return


def get_stock_type(code: str):
    """
    :param code: e.g. "600519"
    :return: ["sz", "sh", "bj"]
    """
    if re.match(r"30\d{4}|00\d{4}|12\d{4}", code):
        return "sz"
    elif re.match(r"60\d{4}|68\d{4}|11\d{4}", code):
        return "sh"
    elif re.match(r"430\d{3}|83\d{4}|87\d{4}", code):
        return "bj"


def code_to_ths(code: str):
    return f"{get_stock_type(code=code)}{code}"
    pass


def get_market_code(symbol: str):
    """
    :param symbol:  e.g. "sh600519" or "600519.sh"
    :return: [MARKET_SZ, MARKET_SH, MARKET_BJ, -1]
    """
    if re.fullmatch(r"sz\d{6}", symbol, re.I) or re.fullmatch(
        r"\d{6}\.sz", symbol, re.I
    ):
        return MARKET_SZ
    elif re.fullmatch(r"sh\d{6}", symbol, re.I) or re.fullmatch(
        r"\d{6}\.sh", symbol, re.I
    ):
        return MARKET_SH
    elif re.fullmatch(r"bj\d{6}", symbol, re.I) or re.fullmatch(
        r"\d{6}\.bj", symbol, re.I
    ):
        return MARKET_BJ
    else:
        return -1


def get_stock_code(symbol: str):
    """
    :param symbol: e.g."sh600519" or "600519.sh"
    :return: "600519"
    """
    int_pytdx_market_code = get_market_code(symbol)
    result = re.search(r"\d{6}", symbol)
    if int_pytdx_market_code >= 0 and result:
        return result.group()
    else:
        return None


def transaction_unit(price: float, amount: float = 1000) -> int:
    if price * 100 > amount:
        return 100
    amount_max = amount * 1.5
    volume = math.ceil(amount / price / 100) * 100
    actual_amount = volume * price
    if actual_amount > amount_max:
        return volume - 100
    else:
        return volume


def zeroing_sort(pd_series: pd.Series) -> pd.Series:  # 归零化排序
    min_unit = pd_series.min()
    pd_series_out = pd_series.apply(func=lambda x: (x / min_unit - 1).round(4))
    return pd_series_out


def feather_to_file(df: DataFrame, key: str):
    filename_df = os.path.join(path_chip, f"{key}.ftr")
    feather.write_dataframe(df=df, dest=filename_df)
    return True


def feather_from_file(key: str) -> DataFrame:
    filename_df = os.path.join(path_chip, f"{key}.ftr")
    if os.path.exists(filename_df):
        df = feather.read_dataframe(source=filename_df)
        if not isinstance(df, DataFrame):
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    return df


def delete_feather(key: str, path_folder: str = path_chip) -> bool:
    file_name = os.path.join(path_chip, f"{key}.ftr")
    if os.path.exists(file_name):
        os.remove(path=file_name)
        logger.trace(f"[{file_name}] delete success.")
        return True
    else:
        logger.error(f"[{file_name}] is not exist.")
        return False


def sleep_to_time(dt_time: datetime.datetime, seconds: int = 1):
    dt_now_sleep = datetime.datetime.now()
    while dt_now_sleep <= dt_time:
        int_delay = int((dt_time - dt_now_sleep).total_seconds())
        str_sleep_gm = time.strftime("%H:%M:%S", time.gmtime(int_delay))
        str_sleep_msg = f"Waiting: {str_sleep_gm}"
        str_sleep_msg = fg.cyan(str_sleep_msg)
        str_dt_now_sleep = dt_now_sleep.strftime("<%H:%M:%S>")
        str_sleep_msg = f"{str_dt_now_sleep}----" + str_sleep_msg
        print(f"\r{str_sleep_msg}\033[K", end="")  # 进度条
        time.sleep(seconds)
        dt_now_sleep = datetime.datetime.now()
    print("\n", end="")
    return True


def is_latest_version(key: str) -> bool:
    dt_now = datetime.datetime.now()
    # df_config = read_df_from_db(key="df_config", filename=filename)
    if os.path.exists(filename_config):
        df_config = feather.read_dataframe(source=filename_config)
    else:
        logger.error(f"[{filename_config}] is not exist.")
        return False
    if df_config.empty:
        return False
    if key not in df_config.index:
        return False
    dt_latest = df_config.at[key, "date"]
    if not isinstance(dt_latest, datetime.date):
        return False
    if dt_latest == dt_pm_end:
        return True
    else:
        if dt_am_0910 < dt_now < dt_pm_end:
            return True
        elif dt_pm_end_last_1T < dt_now < dt_am_0910:
            if dt_latest == dt_pm_end_last_1T:
                return True
            else:
                return False


def set_version(key: str, dt: datetime.datetime) -> bool:
    if os.path.exists(filename_config):
        df_config = feather.read_dataframe(source=filename_config)
    else:
        df_config = pd.DataFrame(columns=["date"])
    df_config.at[key, "date"] = dt
    df_config.sort_values(by="date", ascending=False, inplace=True)
    feather.write_dataframe(df=df_config, dest=filename_config)
    return True


def is_exist(date_index: datetime.date, columns: str) -> bool:
    df_date_exist = feather_from_file(key="df_index_exist")
    try:
        if df_date_exist.at[date_index, columns] == 1:
            return True
        else:
            return False
    except KeyError:
        return False


def set_exist(date_index: datetime.date, columns: str) -> bool:
    df_date_exist = feather_from_file(key="df_index_exist")
    df_date_exist.at[date_index, columns] = 1
    feather_to_file(df=df_date_exist, key="df_index_exist")
    return True


def feather_to_excel(path_folder: str = path_chip):
    str_dt_history_path = dt_history().strftime("%Y_%m_%d")
    filename_excel = os.path.join(path_check, f"chip_{str_dt_history_path}.xlsx")

    def is_open(filename) -> bool:
        if not os.access(path=filename, mode=os.F_OK):
            logger.trace(f"[{filename}] is not exist")
            return False
        else:
            logger.trace(f"[{filename}] is exist")
        try:
            v_handle = win32file.CreateFile(
                filename,
                win32file.GENERIC_READ,
                0,
                None,
                win32file.OPEN_EXISTING,
                win32file.FILE_ATTRIBUTE_NORMAL,
                None,
            )
        except Exception as e_in:
            print(f"{filename} - {repr(e_in)}")
            logger.trace(f"{filename} - {repr(e_in)}")
            return True
        else:
            v_handle.close()
            logger.trace("close Handle")
            logger.trace(f"[{filename}] not in use")
            return False

    i_file = 0
    filename_excel_old = filename_excel
    while i_file <= 5:
        i_file += 1
        if is_open(filename=filename_excel):
            logger.trace(f"[{filename_excel}] is open")
        else:
            logger.trace(f"[{filename_excel}] is not open")
            break
        path, ext = os.path.splitext(filename_excel_old)
        path += f"_{i_file}"
        filename_excel = path + ext
    if is_open(filename=filename_excel):
        logger.error(f"Loop Times out - ({i_file})")
        return False
    path = Path(path_folder)
    files = [p.name for p in path.iterdir() if p.is_file()]
    try:
        writer = pd.ExcelWriter(
            path=filename_excel, mode="a", if_sheet_exists="replace"
        )
    except FileNotFoundError:
        writer = pd.ExcelWriter(path=filename_excel, mode="w")
    for file in files:
        file_name = os.path.join(path_folder, file)
        postfix = os.path.splitext(file_name)[1]
        key = os.path.splitext(file)[0]
        if postfix == ".ftr":
            df = feather.read_dataframe(source=file_name)
            df.to_excel(excel_writer=writer, sheet_name=key)
            print(f"\r[{key}]\033[K", end="")
    writer.close()
    return True


def stock_basic_v2() -> pd.DataFrame:
    df_stock_basic = client_ts_pro.stock_basic(
        exchange="", list_status="L", fields="ts_code,name,list_date"
    )
    if df_stock_basic.empty:
        return pd.DataFrame()
    df_stock_basic["symbol"] = df_stock_basic["ts_code"].apply(
        func=lambda x: x[7:].lower() + x[:6]
    )
    df_stock_basic["list_date"] = df_stock_basic["list_date"].apply(
        func=lambda x: pd.to_datetime(x)
    )
    df_stock_basic.set_index(keys="symbol", inplace=True)
    df_stock_basic = df_stock_basic[["name", "list_date"]]
    return df_stock_basic
