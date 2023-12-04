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
import win32file
import pandas as pd
from console import fg
from pandas import DataFrame
from loguru import logger
from mootdx.consts import MARKET_SZ, MARKET_SH, MARKET_BJ
from analysis.const import (
    dt_am_0910,
    dt_pm_end,
    dt_pm_end_last_1T,
    filename_chip_shelve,
    dt_init,
    client_ts_pro,
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


def write_obj_to_db(obj: object, key: str, filename: str):
    with shelve.open(filename=filename, flag="c") as py_dbm_chip:
        py_dbm_chip[key] = obj
        logger.trace(f"{key} save as pydb_chip-[{filename}]")
    return True


def delete_obj_from_db(key: str, filename: str):
    with shelve.open(filename=filename, flag="c") as py_dbm_chip:
        if key in py_dbm_chip.keys():
            del py_dbm_chip[key]
            logger.trace(f"del {key} from pydb_chip-[{filename}]")
    return True


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


def read_df_from_db(key: str, filename: str) -> DataFrame:
    try:
        with shelve.open(filename=filename, flag="r") as py_dbm_chip:
            logger.trace(f"loading {key} from [{filename}]....")
            try:
                df = py_dbm_chip[key]
                isinstance(df, DataFrame)
                return df
            except KeyError as e:
                print(f"[{key}] is not exist -Error[{repr(e)}]")
                logger.trace(f"[{key}] is not exist - Error[{repr(e)}]")
                return pd.DataFrame()
    except dbm.error as e:
        print(f"[{filename}-{key}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename}] - [{key}] is not exist - Error[{repr(e)}]")
        return pd.DataFrame()


def is_latest_version(key: str, filename: str) -> bool:
    dt_now = datetime.datetime.now()
    df_config = read_df_from_db(key="df_config", filename=filename)
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


def get_config(key: str):
    df_config = read_df_from_db(key="df_config", filename=filename_chip_shelve)
    if df_config.empty:
        return dt_init
    else:
        try:
            return df_config.at[key, "date"]
        except KeyError:
            return dt_init


def set_version(key: str, dt: datetime.datetime) -> bool:
    df_config = read_df_from_db(key="df_config", filename=filename_chip_shelve)
    df_config.at[key, "date"] = dt
    df_config.sort_values(by="date", ascending=False, inplace=True)
    write_obj_to_db(obj=df_config, key="df_config", filename=filename_chip_shelve)
    return True


def is_exist(date_index: datetime.date, columns: str, filename: str) -> bool:
    df_date_exist = read_df_from_db(key="df_index_exist", filename=filename)
    try:
        if df_date_exist.at[date_index, columns] == 1:
            return True
        else:
            return False
    except KeyError:
        return False


def set_exist(date_index: datetime.date, columns: str, filename: str) -> bool:
    df_date_exist = read_df_from_db(key="df_index_exist", filename=filename)
    df_date_exist.at[date_index, columns] = 1
    write_obj_to_db(obj=df_date_exist, key="df_index_exist", filename=filename)
    return True


def shelve_to_excel(filename_shelve: str, filename_excel: str):
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
    try:
        logger.trace(f"try open [{filename_shelve}]")
        with shelve.open(filename=filename_shelve, flag="r") as py_dbm_chip:
            key_random = ""
            try:
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
            except FileNotFoundError:
                with pd.ExcelWriter(path=filename_excel, mode="w") as writer_e:
                    key_random = random.choice(list(py_dbm_chip.keys()))
                    if isinstance(py_dbm_chip[key_random], DataFrame):
                        py_dbm_chip[key_random].to_excel(
                            excel_writer=writer_e, sheet_name=key_random
                        )
                    else:
                        logger.trace(f"{key_random} is not DataFrame")
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
                logger.trace(f"create file-[{filename_excel}]")
            count = len(py_dbm_chip)
            i = 0
            for key in py_dbm_chip:
                i += 1
                str_shelve_to_excel = f"[{i}/{count}] - {key}"
                print(f"\r{str_shelve_to_excel}\033[K", end="")
                if key != key_random:
                    if isinstance(py_dbm_chip[key], DataFrame):
                        py_dbm_chip[key].to_excel(excel_writer=writer, sheet_name=key)
                    else:
                        logger.trace(f"{key} is not DataFrame")
                        continue
                else:
                    logger.trace(f"{key} is exist")
                    continue
            writer.close()
            if i >= count:
                print("\n", end="")  # 格式处理
                return True
    except dbm.error as e:
        print(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        return False


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
