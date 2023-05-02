# modified at 2023/5/2 16:03
from __future__ import annotations
import os
import random
import sys
import datetime
import shelve
import dbm
import time

import win32file
import pywintypes
import pandas as pd
from pandas import DataFrame
import tushare as ts
from loguru import logger
from analysis.const import (
    dt_am_0100,
    dt_am_0910,
    dt_pm_end,
    str_date_path,
    path_check,
    filename_chip_shelve,
)


def is_trading_day(dt: datetime.datetime = None) -> bool:
    ts.set_token("77f61903681b936f371c34d8abf7603a324ed90d070e4eb6992d0832")
    pro = ts.pro_api()
    if dt is None:
        dt = datetime.datetime.now()
    dt_start = dt - datetime.timedelta(days=14)
    str_date_start = dt_start.strftime("%Y%m%d")
    str_date_now = dt.strftime("%Y%m%d")
    try:
        df_trade = pro.trade_cal(
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
    return symbol[2:] + "." + symbol[0:2].upper()


def code_ts_to_ths(ts_code: str):
    return ts_code[-2:].lower() + ts_code[:6]


def get_stock_type_in(code_in: str):
    if code_in[:2].lower() == "sh":
        return "sh"
    elif code_in[:2].lower() == "sz":
        return "sh"
    elif code_in[:2].lower() == "bj":
        return "bj"
    elif code_in[-2:].lower() == "sh":
        return "sh"
    elif code_in[-2:].lower() == "sz":
        return "sz"
    elif code_in[-2:].lower() == "bj":
        return "bj"


def transaction_unit(price: float, amount: float = 1000) -> int:
    if price * 100 > amount:
        return 100
    unit_temp = amount / price
    unit_small = int(unit_temp // 100 * 100)
    unit_big = unit_small + 100
    differ_big = unit_big * price - amount
    differ_small = amount - unit_small * price
    if differ_big < differ_small:
        return unit_big
    else:
        return unit_small


def zeroing_sort(pd_series: pd.Series) -> pd.Series:  # 归零化排序
    min_unit = pd_series.min()
    pd_series_out = pd_series.apply(func=lambda x: (x / min_unit - 1).round(4))
    return pd_series_out


def write_obj_to_db(obj: object, key: str, filename: str):
    with shelve.open(filename=filename, flag="c") as py_dbm_chip:
        py_dbm_chip[key] = obj
        logger.trace(f"{key} save as pydb_chip-[{filename}]")
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
        logger.trace(f"[{filename}-{key}] is not exist - Error[{repr(e)}]")
        return pd.DataFrame()


def is_latest_version(key: str, filename: str) -> bool:
    dt_now = datetime.datetime.now()
    df_config = read_df_from_db(key="df_config", filename=filename)
    if df_config.empty:
        logger.trace(f"df_config is empty")
        return False
    else:
        if key not in df_config.index:
            logger.trace(f"df_config-[{key}] is not exist")
            return False
        else:
            if dt_am_0910 < dt_now < dt_pm_end:
                logger.trace(
                    f"df_config-[{key}]-[{df_config.at[key, 'date']}] less than [{dt_pm_end}],but update df_config-[{key}] will at [{dt_pm_end}]"
                )
                return True
            elif df_config.at[key, "date"] == dt_pm_end:
                logger.trace(
                    f"df_config-[{key}]-[{df_config.at[key, 'date']}] is latest"
                )
                return True
            elif dt_am_0100 < dt_now < dt_am_0910:
                dt_latest_trading = dt_pm_end - datetime.timedelta(days=1)
                i = 1
                while not is_trading_day(dt_latest_trading):
                    i += 1
                    dt_latest_trading = dt_pm_end - datetime.timedelta(days=i)
                if df_config.at[key, "date"] == dt_latest_trading:
                    logger.trace(
                        f"df_config-[{key}]-[{df_config.at[key, 'date']}] is latest"
                    )
                    return True
                else:
                    logger.trace(
                        f"df_config-[{key}]-[{df_config.at[key, 'date']}] is not latest"
                    )
                    return False
            else:
                logger.trace(f"df_config-[{key}] update")
                return False


def set_version(key: str, dt: datetime.datetime) -> bool:
    df_config = read_df_from_db(key="df_config", filename=filename_chip_shelve)
    df_config.at[key, "date"] = dt
    write_obj_to_db(obj=df_config, key="df_config", filename=filename_chip_shelve)
    logger.trace(f"{key} update - [{df_config.at[key, 'date']}]")
    return True


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
    except Exception as e:
        print(f"{filename} - {repr(e)}")
        logger.trace(f"{filename} - {repr(e)}")
        return True
    else:
        v_handle.close()
        logger.trace("close Handle")
        logger.trace(f"[{filename}] not in use")
        return False


def shelve_to_excel(filename_shelve: str, filename_excel: str):
    i_file = 0
    while True:
        if is_open(filename=filename_excel):
            logger.trace(f"[{filename_excel}] is open")
        else:
            logger.trace(f"[{filename_excel}] is not open")
            break
        i_file += 1
        filename_excel = os.path.join(path_check, f"chip_{str_date_path}_{i_file}.xlsx")
    try:
        logger.trace(f"try open [{filename_shelve}]")
        with shelve.open(filename=filename_shelve, flag="r") as py_dbm_chip:
            key_random = ""
            try:
                writer = pd.ExcelWriter(
                    path=filename_excel, mode="a", if_sheet_exists="replace"
                )
            except FileNotFoundError as e:
                logger.trace(f"{filename_excel} is not exist -Error[{repr(e)}]")
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
                return
    except dbm.error as e:
        print(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
