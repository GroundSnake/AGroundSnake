# modified at 2023/3/27 09:16
from __future__ import annotations
import os
import datetime
import shelve
import dbm
import win32file
import pywintypes
import pandas as pd
import tushare as ts
from loguru import logger


def all_ts_code() -> list | None:
    pro = ts.pro_api()
    df_basic = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date",
    )
    if len(df_basic) == 0:
        return
    else:
        list_ts_code = df_basic["ts_code"].tolist()
        return list_ts_code


def all_chs_code() -> list | None:
    list_ts_code = all_ts_code()
    if list_ts_code:
        list_chs_code = [item[-2:].lower() + item[:6] for item in list_ts_code]
        return list_chs_code
    else:
        return


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


def latest_trading_day() -> datetime.date:
    dt_now = datetime.datetime.now()
    str_date_now = dt_now.strftime("%Y%m%d")
    pro = ts.pro_api()
    df_trade = pro.trade_cal(exchange="", start_date="20230301", end_date=str_date_now)
    df_trade.set_index(keys=["cal_date"], inplace=True)
    if df_trade.at[str_date_now, "is_open"] == 1:
        str_dt_out = str_date_now
    else:
        str_dt_out = df_trade.at[str_date_now, "pretrade_date"]
    dt_out = datetime.datetime.strptime(str_dt_out, "%Y%m%d").date()
    return dt_out


def is_trading_day() -> bool:
    pro = ts.pro_api()
    dt_now = datetime.datetime.now()
    str_date_now = dt_now.strftime("%Y%m%d")
    df_trade = pro.trade_cal(exchange="", start_date="20230301", end_date=str_date_now)
    df_trade.set_index(keys=["cal_date"], inplace=True)
    if df_trade.at[str_date_now, "is_open"] == 1:
        return True
    else:
        return False


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


def write_obj_to_db(obj: object, key: str):
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    file_name_chip_shelve = os.path.join(path_data, f"chip")
    with shelve.open(filename=file_name_chip_shelve, flag='c') as pydbm_chip:
        pydbm_chip[key] = obj
        logger.trace(f"{key} save as pydb_chip-[{file_name_chip_shelve}]")
    return True


def read_obj_from_db(key: str) -> object:
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    file_name_chip_shelve = os.path.join(path_data, f"chip")
    try:
        with shelve.open(filename=file_name_chip_shelve, flag='r') as pydbm_chip:
            logger.trace(f"loading {key} from [{file_name_chip_shelve}]....")
            try:
                return pydbm_chip[key]
            except KeyError as e:
                print(repr(e))
                logger.trace(f"[{key}] is not exist")
                return pd.DataFrame()
    except dbm.error as e:
        print(repr(e))
        logger.trace(f"[{file_name_chip_shelve}] is not exist")
        return pd.DataFrame()


def is_latest_version(key: str) -> bool:
    dt_now = datetime.datetime.now()
    dt_date_trading = latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    df_config = read_obj_from_db(key="df_config")
    logger.trace(
        f"the latest {key} at {df_config.at[key, 'date']},The new at {dt_pm_end}"
    )
    if (
        df_config.at[key, "date"] < dt_now < dt_pm_end
        or df_config.at[key, "date"] == dt_pm_end
    ):
        logger.trace(f"{key}-[{df_config.at[key, 'date']}]is latest")
        return True
    else:
        return False


def set_version(key: str, dt: datetime.datetime) -> bool:
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    df_config = read_obj_from_db(key="df_config")
    df_config.at[key, "date"] = dt
    write_obj_to_db(obj=df_config, key="df_config")
    logger.trace(f"{key} update - [{df_config.at[key, 'date']}]")
    return True


def is_open(filename) -> bool:
    if not os.access(path=filename, mode=os.F_OK):
        logger.trace(f'[{filename}] is exist')
        return False
    else:
        logger.trace(f'[{filename}] is not exist')
    v_handle = pywintypes.HANDLE()
    try:
        v_handle = win32file.CreateFile(filename, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                        win32file.FILE_ATTRIBUTE_NORMAL, None)
        logger.trace(f'[{filename}] not in use')
        return False  # int(v_handle) == win32file.INVALID_HANDLE_VALUE
    except pywintypes.error as e:
        logger.error(e.args[2])
        logger.trace(f'[{filename}] in use')
        return True
    finally:
        try:
            v_handle.close()
            logger.trace("v_handle close")
        except pywintypes.error as e:
            logger.error(repr(e))


def add_chip_excel(df: pd.DataFrame, key: str):
    dt_date_trading = latest_trading_day()
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_check = os.path.join(path_main, "check")
    i= 0
    file_name_chip_excel = os.path.join(path_check, f"chip_{str_date_path}.xlsx")
    while True:
        if is_open(filename=file_name_chip_excel):
            logger.trace(f'{file_name_chip_excel} is open')
        else:
            logger.trace(f'{file_name_chip_excel} is not open')
            break
        i += 1
        file_name_chip_excel = os.path.join(path_check, f"chip_{str_date_path}_{i}.xlsx")
    try:
        with pd.ExcelWriter(
                path=file_name_chip_excel, mode="a", if_sheet_exists="replace"
        ) as writer:
            df.to_excel(excel_writer=writer, sheet_name=key)
            logger.trace(f"save {key} at Excel-[{file_name_chip_excel}]")
    except FileNotFoundError as e:
        logger.error(repr(e))
        with pd.ExcelWriter(path=file_name_chip_excel, mode="w") as writer:
            df.to_excel(excel_writer=writer, sheet_name=key)
