# modified at 2023/05/18 22::25

import time
import re
import math
import datetime
from pyarrow.feather import read_feather, write_feather
from pathlib import Path
import pandas as pd
from pandas import DataFrame, Series
from console import fg
from analysis.log import logger
from analysis.const import (
    MARKET_SZ,
    MARKET_SH,
    MARKET_BJ,
    time_pm_end,
    float_time_sleep,
)


def code_ths_to_ts(symbol: str):
    """
    :param symbol: "sh600519"
    :return: "600519.sh"
    """
    return symbol[2:] + "." + symbol[0:2].upper()


def code_ts_to_ths(ts_code: str):
    """
    :param ts_code: "600519.sh"
    :return: "sh600519"
    """
    return ts_code[-2:].lower() + ts_code[:6]


def get_stock_type(code: str) -> str:
    """
    :param code: e.g. "600519"
    :return: ["sz", "sh", "bj", "no"]
    """
    if re.match(r"^30\d{4}$|^00\d{4}$|^12\d{4}$|^159\d{3}$", code):
        return "sz"
    elif re.match(r"^60\d{4}$|^68\d{4}$|^11\d{4}$|^5\d{5}$", code):
        return "sh"
    elif re.match(r"^430\d{3}$|^83\d{4}$|^87\d{4}$", code):
        return "bj"
    else:
        return "no"


def code_code_to_ths(code: str):
    """
    :param code: e.g. "600519"
    :return: "sh600519"
    """
    return f"{get_stock_type(code=code)}{code}"


def code_ths_to_code(symbol: str):
    """
    :param symbol: e.g. "sh600519"
    :return: "600519"
    """
    return symbol[2:]


def check_chs_code(symbols: list) -> list:
    list_return = list()
    if not isinstance(symbols, list):
        return list_return
    for symbol in symbols:
        if re.match(r"^sz\d{6}$|^sh\d{6}$|^bj\d{6}$", symbol):
            list_return.append(symbol)
    return list_return


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


def feather_to_file(df: DataFrame, filename_df: Path | str) -> bool:
    if not isinstance(filename_df, Path):
        filename_df = Path(filename_df)
    if not isinstance(df, DataFrame):
        return False
    if df.empty:
        return False
    else:
        write_feather(df=df, dest=str(filename_df))
        return True


def feather_from_file(filename_df: Path | str) -> DataFrame:
    if not isinstance(filename_df, Path):
        filename_df = Path(filename_df)
    if not filename_df.exists():
        return pd.DataFrame()
    i_while = 0
    df = pd.DataFrame()
    while i_while < 5:
        i_while += 1
        try:
            df = read_feather(source=str(filename_df))
        except OSError as e:
            logger.error(f"{filename_df} - Error -{e}")
            time.sleep(float_time_sleep)
            if i_while >= 3:
                filename_df.unlink(missing_ok=True)
        else:
            break
    return df


def csv_to_file(df: DataFrame, filename_df: Path) -> bool:
    filename_df_temp = filename_df
    i_while = 1
    while i_while < 3:
        try:
            df.to_csv(path_or_buf=filename_df_temp)
        except PermissionError as e:
            filename_name = filename_df.stem + f"_{i_while}" + filename_df.suffix
            filename_df_temp = filename_df.parent.joinpath(filename_name)
            logger.error(f"{repr(e)}")
            time.sleep(float_time_sleep)
        else:
            break
        i_while += 1
    return True


def pickle_from_file(filename_ser: Path) -> Series:
    if filename_ser.exists():
        try:
            ser = pd.read_pickle(filepath_or_buffer=filename_ser)
        except EOFError:
            ser = pd.Series()
        if not isinstance(ser, Series):
            ser = pd.Series()
    else:
        ser = pd.Series()
    return ser


def pickle_to_file(ser: Series, filename_ser: Path) -> bool:
    if isinstance(ser, Series):
        ser.to_pickle(path=filename_ser)
        return True
    else:
        logger.error(f"item is not Series")
        return False


def feather_delete(filename_df: Path) -> bool:
    if filename_df.exists():
        filename_df.unlink(missing_ok=True)
        logger.trace(f"[{filename_df}] delete success.")
        return True
    else:
        logger.error(f"[{filename_df}] is not exist.")
        return False


def sleep_to_time(dt_time: datetime.datetime, seconds: int | None = None) -> bool:
    dt_now_sleep = datetime.datetime.now().replace(microsecond=0)
    if seconds is None:
        seconds = 2
    str_sleep_msg_head = f"Waiting - {dt_time.strftime("<%H:%M:%S>")}: "
    while dt_now_sleep <= dt_time:
        int_delay = int((dt_time - dt_now_sleep).total_seconds())
        str_sleep_msg = f"{str_sleep_msg_head} {int_delay} seconds"
        str_sleep_msg = fg.cyan(str_sleep_msg)
        str_dt_now_sleep = dt_now_sleep.strftime("<%H:%M:%S>")
        str_sleep_msg = f"{str_dt_now_sleep}----" + str_sleep_msg
        print(f"\r{str_sleep_msg}\033[K", end="")  # 进度条
        if int_delay > seconds:
            time.sleep(seconds)
        else:
            time.sleep(int_delay)
        dt_now_sleep = datetime.datetime.now().replace(microsecond=0)
    print()
    return True


def get_financial_period(dt: datetime.datetime, pub: bool = False) -> datetime.datetime:
    dt_0 = datetime.datetime(year=dt.year, month=1, day=1, hour=5)
    dt_1 = datetime.datetime(year=dt.year, month=4, day=1, hour=5)
    dt_2 = datetime.datetime(year=dt.year, month=9, day=1, hour=5)
    dt_3 = datetime.datetime(year=dt.year, month=10, day=1, hour=5)
    dt_4y = datetime.datetime(year=dt.year + 1, month=1, day=1, hour=5)
    dt_return_y1 = datetime.datetime(year=dt.year - 1, month=9, day=30, hour=5)
    dt_return_y0 = datetime.datetime(year=dt.year - 1, month=12, day=31, hour=5)
    dt_return_1 = datetime.datetime(year=dt.year, month=3, day=31, hour=5)
    dt_return_2 = datetime.datetime(year=dt.year, month=6, day=30, hour=5)
    dt_return_3 = datetime.datetime(year=dt.year, month=9, day=30, hour=5)
    dt_return_4 = datetime.datetime(year=dt.year, month=12, day=31, hour=5)
    if dt_0 < dt < dt_1:
        dt_return = dt_return_y0
    elif dt_1 < dt < dt_2:
        dt_return = dt_return_1
    elif dt_2 < dt < dt_3:
        dt_return = dt_return_2
    elif dt_3 < dt < dt_4y:
        dt_return = dt_return_3
    else:
        dt_return = dt_return_4
    if pub:
        if dt_return == dt_return_y0:
            dt_return = dt_return_y1
        elif dt_return == dt_return_1:
            dt_return = dt_return_1
        elif dt_return == dt_return_2:
            dt_return = dt_return_1
        elif dt_return == dt_return_3:
            dt_return = dt_return_2
        else:
            dt_return = dt_return_3
    return dt_return


def get_latest_friday():
    date_now = datetime.date.today()
    weekday_now = date_now.weekday()
    offset = (weekday_now - 4) % 7
    date_friday = date_now - datetime.timedelta(days=offset)
    dt_friday = datetime.datetime.combine(date_friday, time_pm_end)
    if weekday_now > 4:
        dt_friday = dt_friday - datetime.timedelta(days=7)
    return dt_friday


def transaction_unit(price: float, amount: float = 2000) -> int:
    if price <= 0:
        return 100
    if price * 100 > amount:
        return 100
    amount_max = int(amount * 1.5)
    volume = math.ceil(amount / price / 100) * 100
    actual_amount = volume * price
    if actual_amount > amount_max:
        return volume - 100
    else:
        return volume


def get_ratio_value(data: list, frac: float = 0.1, reverse: bool = False) -> float:
    if len(data) > 0:
        data = [i for i in data if i > 0]
        data = sorted(data, reverse=reverse)
    else:
        return 0.0
    count = len(data)
    index_phi = round(count * frac - 1)
    golden_value = data[index_phi]
    return golden_value
