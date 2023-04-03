# modified at 2023/3/31 15:19
from __future__ import annotations
import os
import sys
import datetime
import shelve
import dbm
import win32file
import pywintypes
import pandas as pd
from pandas import DataFrame
import tushare as ts
from loguru import logger
from analysis.const import dt_init, dt_pm_end, str_date_path, path_check, filename_chip_shelve


def is_trading_day() -> bool:
    """无法使用调用"""
    ts.set_token("77f61903681b936f371c34d8abf7603a324ed90d070e4eb6992d0832")
    pro = ts.pro_api()
    dt_now = datetime.datetime.now()
    str_date_now = dt_now.strftime("%Y%m%d")
    try:
        df_trade = pro.trade_cal(
            exchange="", start_date="20230301", end_date=str_date_now
        )
    except Exception as e:
        print(f"The token is invalid. Please apply for a token at tushare - Error-[{e}]")
        sys.exit()
    df_trade.set_index(keys=["cal_date"], inplace=True)
    if df_trade.at[str_date_now, "is_open"] == 1:
        return True
    else:
        return False


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
    with shelve.open(filename=filename, flag="c") as pydbm_chip:
        pydbm_chip[key] = obj
        logger.trace(f"{key} save as pydb_chip-[{filename}]")
    return True


def read_obj_from_db(key: str, filename: str) -> object:
    try:
        with shelve.open(filename=filename, flag="r") as pydbm_chip:
            logger.trace(f"loading {key} from [{filename}]....")
            try:
                return pydbm_chip[key]
            except KeyError as e:
                print(f"[{key}] is not exist -Error[{repr(e)}]")
                logger.trace(f"[{key}] is not exist -Error[{repr(e)}]")
                return pd.DataFrame()
    except dbm.error as e:
        print(f"[{filename}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename}] is not exist - Error[{repr(e)}]")
        return pd.DataFrame()


def is_latest_version(key: str, filename: str) -> bool:
    dt_now = datetime.datetime.now()
    df_config = read_obj_from_db(key="df_config", filename=filename)
    if df_config.empty:
        logger.trace(f"df_config-[{key}] is empty")
        df_config.at[key, "date"] = dt_init
    if key not in df_config.index:
        logger.trace(f"{key} not in df_config")
        df_config.at[key, "date"] = dt_init
    if df_config.at[key, "date"] == dt_init:
        logger.trace(
            f"Update key-({key}-[{dt_init}]) to [{dt_pm_end}]"
        )
        return False
    elif df_config.at[key, "date"] < dt_now < dt_pm_end:
        logger.trace(f"{key}-[{df_config.at[key, 'date']}] less than [{dt_pm_end}],but update {key} at [{dt_pm_end}]")
        return True
    elif df_config.at[key, "date"] ==dt_pm_end:
        logger.trace(f"{key}-[{df_config.at[key, 'date']}]is latest")
        return True
    else:
        logger.trace(f"{key}-Error")
        return False


def set_version(key: str, dt: datetime.datetime) -> bool:
    df_config = read_obj_from_db(key="df_config", filename=filename_chip_shelve)
    df_config.at[key, "date"] = dt
    write_obj_to_db(obj=df_config, key="df_config", filename=filename_chip_shelve)
    logger.trace(f"{key} update - [{df_config.at[key, 'date']}]")
    return True


def is_open(filename) -> bool:
    if not os.access(path=filename, mode=os.F_OK):
        logger.trace(f"[{filename}] is exist")
        return False
    else:
        logger.trace(f"[{filename}] is not exist")
    v_handle = pywintypes.HANDLE()
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
        logger.trace(f"[{filename}] not in use")
        return False  # int(v_handle) == win32file.INVALID_HANDLE_VALUE
    except pywintypes.error as e:
        print(f'{filename} - {e.args[2]}')
        logger.trace(f'{filename} - {e.args[2]}')
        return True
    finally:
        try:
            v_handle.close()
            logger.trace("v_handle close")
        except pywintypes.error as e:
            logger.error(f'v_handle - {repr(e)}')


def add_chip_excel(df: pd.DataFrame, key: str, filename: str):
    i = 0
    while True:
        if is_open(filename=filename):
            logger.trace(f"[{filename}] is open")
        else:
            logger.trace(f"[{filename}] is not open")
            break
        i += 1
        filename = os.path.join(path_check, f"chip_{str_date_path}_{i}.xlsx")
    try:
        with pd.ExcelWriter(
            path=filename, mode="a", if_sheet_exists="replace"
        ) as writer:
            df.to_excel(excel_writer=writer, sheet_name=key)
    except FileNotFoundError as e:
        logger.trace(f'{filename} is not exist -Error[{repr(e)}]')
        with pd.ExcelWriter(path=filename, mode="w") as writer:
            df.to_excel(excel_writer=writer, sheet_name=key)
    finally:
        logger.trace(f"save {key} at Excel-[{filename}]")


def shelve_to_excel(filename_shelve: str, filename_excel: str):
    try:
        with shelve.open(filename=filename_shelve, flag="r") as pydbm_chip:
            count = len(pydbm_chip)
            i = 0
            for key in pydbm_chip:
                i += 1
                str_shelve_to_excel = f"\r[{i}/{count}] - {key}"
                print(str_shelve_to_excel, end="")
                if isinstance(pydbm_chip[key], DataFrame):
                    add_chip_excel(
                        df=pydbm_chip[key], key=f"{key}", filename=filename_excel
                    )
                else:
                    logger.trace(f"{key} is not DataFrame")
                    continue
            if i >= count:
                print("\n", end="")  # 格式处理
    except dbm.error as e:
        print(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        logger.trace(f"[{filename_shelve}] is not exist - Error[{repr(e)}]")
        return pd.DataFrame()
