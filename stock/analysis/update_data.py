# modified at 2023/05/18 22::25
import datetime
import time
import httpx
import random
import requests
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_am_start,
    dt_pm_end,
    dt_pm_end_1t,
    time_pm_end,
    path_data,
    path_temp,
    path_chip,
    path_chip_csv,
    format_dt,
    df_daily_basic,
    float_time_sleep,
    client_ts_pro,
    client_mootdx,
    client_ts,
)
from analysis.base import (
    get_stock_code,
    code_ths_to_ts,
    feather_to_file,
    feather_from_file,
    feather_delete,
    code_ts_to_ths,
    sleep_to_time,
)
from analysis.log import log_json, logger
from analysis.api_tushare import ts_sw_daily, index_classify, index_daily
from analysis.api_akshare import ak_index_zh_a_hist_min_em, ak_stock_zh_index_spot_sina
from analysis.util import get_stock_code_chs, get_etf_code_chs


list_all_stock = get_stock_code_chs()
list_all_etf = get_etf_code_chs()


def kline_tdx(symbol="sh600519", frequency="1m", adjust="qfq") -> pd.DataFrame:
    """
    :param symbol: ["sh600519"]
    :param frequency: ['5m', '15m', '30m', '1h', 'day', 'week', 'mon', '1m', '1m', 'day', 'mon', 'year']
    :param adjust: ["qfq", "hfq"]
    :return: DataFrame [["open", "close", "high", "low", "volume", "amount"]]
    """
    dt_now = datetime.datetime.now()
    df_symbol_kline_tdx = pd.DataFrame()
    list_frequency_tdx = [
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
        "day",
        "week",
        "mon",
        "year",
    ]
    if frequency not in list_frequency_tdx:
        logger.error(f"[{symbol}] - [{frequency}] - [{adjust}] - frequency is Error.")
        return df_symbol_kline_tdx
    filename_df_symbol_kline_tdx_temp = path_temp.joinpath(
        f"{symbol}_{frequency}_{adjust}.ftr"
    )
    df_symbol_kline_tdx_temp = feather_from_file(filename_df_symbol_kline_tdx_temp)
    if not df_symbol_kline_tdx_temp.empty:
        dt_max = df_symbol_kline_tdx_temp.index.max()
        logger.debug(
            f"[{symbol}] - [{dt_max}] - feather from {filename_df_symbol_kline_tdx_temp}."
        )
        if dt_am_start < dt_now < dt_pm_end:
            logger.debug(f"[{symbol}] - [{dt_max}] --- trading time")
            return df_symbol_kline_tdx_temp
        if dt_max >= dt_pm_end:
            logger.debug(f"[{symbol}] - [{dt_max}] is latest")
            return df_symbol_kline_tdx_temp
    stock_code = get_stock_code(symbol)
    i_while_kline = 0
    while i_while_kline < 2:
        i_while_kline += 1
        try:
            log_json(item="bars_TDX")
            if adjust == "None":
                df_symbol_kline_tdx = client_mootdx.bars(
                    symbol=stock_code,
                    frequency=frequency,
                )
            else:
                df_symbol_kline_tdx = client_mootdx.bars(
                    symbol=stock_code,
                    frequency=frequency,
                    adjust=adjust,
                )
        except httpx.ReadTimeout as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        except httpx.ConnectTimeout as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        except ValueError as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        except IndexError as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        except TimeoutError as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        except ConnectionResetError as e:
            df_symbol_kline_tdx = pd.DataFrame()
            logger.error(f"{symbol} - Error - {e}")
        if not df_symbol_kline_tdx.empty:
            break
        logger.error(f"Kline - [{symbol}] - Empty - Sleep({i_while_kline}).")
        time.sleep(float_time_sleep)
    if not df_symbol_kline_tdx.empty:
        index_random = random.choice(df_symbol_kline_tdx.index)
        if not isinstance(index_random, datetime.datetime):
            df_symbol_kline_tdx.index = pd.to_datetime(df_symbol_kline_tdx.index)
        df_symbol_kline_tdx = df_symbol_kline_tdx[
            ~df_symbol_kline_tdx.index.duplicated(keep="last")
        ]
        df_symbol_kline_tdx["volume"] = round(df_symbol_kline_tdx["vol"] / 100, 2)
        df_symbol_kline_tdx.sort_index(ascending=True, inplace=True)
        df_symbol_kline_tdx.dropna(inplace=True)
        df_symbol_kline_tdx = df_symbol_kline_tdx[
            ["open", "close", "high", "low", "volume", "amount"]
        ]
        df_symbol_kline_tdx.drop_duplicates(keep="first", inplace=True)
        feather_to_file(
            df=df_symbol_kline_tdx, filename_df=filename_df_symbol_kline_tdx_temp
        )
        dr_max = df_symbol_kline_tdx.index.max()
        logger.debug(
            f"[{symbol}] - [{dr_max}] - feather to [{filename_df_symbol_kline_tdx_temp}]",
        )
    return df_symbol_kline_tdx


def kline_ts(
    symbol="sh600519",
    frequency="D",
    adjust="qfq",
    asset="E",
    start_date=dt_init,
    end_date=dt_pm_end,
) -> pd.DataFrame:
    """
    :param symbol: ["sh600519"]
    :param adjust: [None, "qfq", "hfq"]
    :param frequency: ['D', 'W', 'M']
    :param asset: ['E', 'FD',]
    :param start_date:
    :param end_date:
    :return: DataFrame [["open", "close", "high", "low", "volume", "amount"]]
    """
    dt_now = datetime.datetime.now()
    df_symbol_kline_ts = pd.DataFrame()
    list_frequency_ts = [
        "D",
        "W",
        "M",
    ]
    if frequency not in list_frequency_ts:
        logger.error(
            f"[{symbol}] - [{frequency}] - "
            f"[{adjust}] - [{asset}] - frequency is Error."
        )
        return df_symbol_kline_ts
    filename_df_symbol_kline_ts_temp = path_temp.joinpath(
        f"{symbol}_{frequency}_{adjust}_{asset}.ftr"
    )
    df_symbol_kline_ts_temp = feather_from_file(filename_df_symbol_kline_ts_temp)
    if not df_symbol_kline_ts_temp.empty:
        dt_max = df_symbol_kline_ts_temp.index.max()
        if dt_am_start < dt_now < dt_pm_end:
            logger.debug(
                f"[{symbol}] - [{dt_max}] - feather from file.----trading time",
            )
            return df_symbol_kline_ts_temp[start_date:end_date]
        dt_mim = df_symbol_kline_ts_temp.index.min()
        if dt_mim <= start_date < end_date <= dt_pm_end <= dt_max:
            logger.debug(f"[{symbol}] - [{dt_max}] - feather from file.")
            return df_symbol_kline_ts_temp[start_date:end_date]
    ts_code = code_ths_to_ts(symbol)
    str_start_date = start_date.strftime("%Y%m%d")
    str_end_date = end_date.strftime("%Y%m%d")
    i_while_kline = 0
    while i_while_kline < 2:
        i_while_kline += 1
        try:
            log_json(item="pro_bar_TS")
            df_symbol_kline_ts = client_ts.pro_bar(
                ts_code=ts_code,
                api=client_ts_pro,
                start_date=str_start_date,
                end_date=str_end_date,
                asset=asset,
                adj=adjust,
                freq=frequency,
            )
        except requests.exceptions as e:
            logger.error(f"{symbol} - Error - {e}")
            time.sleep(float_time_sleep)
        except TypeError as e:
            logger.error(f"{symbol} - Error - {e}")
            time.sleep(float_time_sleep)
        if isinstance(df_symbol_kline_ts, pd.DataFrame):
            if not df_symbol_kline_ts.empty:
                break
            else:
                logger.error(f"Kline - [{symbol}] - Empty - Sleep-{i_while_kline}.")
        logger.error(
            f"Kline - [{symbol}] - type[{type(df_symbol_kline_ts)}] - Sleep({i_while_kline})."
        )
        df_symbol_kline_ts = pd.DataFrame()
    if not df_symbol_kline_ts.empty:
        df_symbol_kline_ts["trade_date"] = df_symbol_kline_ts["trade_date"].apply(
            func=lambda x: datetime.datetime.combine(
                pd.to_datetime(x).date(),
                time_pm_end,
            )
        )
        df_symbol_kline_ts.set_index(keys=["trade_date"], inplace=True)
        df_symbol_kline_ts["volume"] = round(df_symbol_kline_ts["vol"] / 100, 2)
        df_symbol_kline_ts["amount"] = round(df_symbol_kline_ts["amount"] * 1000, 2)
        df_symbol_kline_ts.sort_index(ascending=True, inplace=True)
        df_symbol_kline_ts = df_symbol_kline_ts[
            ["open", "close", "high", "low", "pre_close", "volume", "amount"]
        ]
        df_symbol_kline_ts.dropna(inplace=True)
        df_symbol_kline_ts.drop_duplicates(keep="first", inplace=True)
        feather_to_file(
            df=df_symbol_kline_ts, filename_df=filename_df_symbol_kline_ts_temp
        )
        dt_max = df_symbol_kline_ts.index.max()
        logger.debug(
            f"[{symbol}] - [{dt_max}] -  feather to [{filename_df_symbol_kline_ts_temp}]",
        )
    return df_symbol_kline_ts


def kline_base(
    symbol="sh600519",
    frequency="D",
    adjust="qfq",
    asset="E",
    start_date=dt_init,
    end_date=dt_pm_end,
) -> pd.DataFrame:
    df_symbol_kline = pd.DataFrame()
    list_frequency_tdx = [
        "1m",
        "5m",
        "15m",
        "30m",
        "1h",
    ]
    list_frequency_ts = [
        "D",
        "W",
        "M",
    ]
    if asset == "FD":
        df_symbol_kline = kline_tdx(symbol=symbol, frequency=frequency, adjust="None")
    elif asset == "E":
        if frequency == "day":
            frequency = "D"
        elif frequency == "mon":
            frequency = "M"
        if frequency in list_frequency_tdx:
            df_symbol_kline = kline_tdx(
                symbol=symbol, frequency=frequency, adjust=adjust
            )
        elif frequency in list_frequency_ts:
            df_symbol_kline = kline_ts(
                symbol=symbol,
                frequency=frequency,
                adjust=adjust,
                asset=asset,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            logger.error(f"[{symbol}] - frequency = {frequency} is Error.")
    else:
        logger.error(f"[{symbol}] - asset = {asset} is Error.")
    if not isinstance(df_symbol_kline, pd.DataFrame):
        df_symbol_kline = pd.DataFrame()
    df_symbol_kline.dropna(inplace=True)
    return df_symbol_kline


def kline(
    symbol="sh600519",
    frequency="D",
    adjust="qfq",
    asset=None,
    start_date=dt_init,
    end_date=dt_pm_end,
    reset=False,
) -> pd.DataFrame:
    dt_now = datetime.datetime.now()
    dt_end = dt_pm_end
    if dt_now < dt_pm_end:
        dt_end = dt_pm_end_1t
    if asset is None:
        if symbol in list_all_etf:
            asset = "FD"
        elif symbol in list_all_stock:
            asset = "E"
        else:
            logger.error(f"{symbol} is No stock or etf.")
            return pd.DataFrame()
    list_frequency_ts = [
        "D",
        "W",
        "M",
    ]
    path_kline = path_data.joinpath(f"kline_{frequency}")
    path_kline.mkdir(exist_ok=True)
    file_name_feather = path_kline.joinpath(f"{symbol}.ftr")
    if asset == "FD" and frequency in list_frequency_ts:
        if frequency == "D":
            frequency = "day"
        elif frequency == "M":
            frequency = "mon"
        else:
            logger.error(f"asset={asset}, frequency={frequency}.")
            return pd.DataFrame()
    df_symbol_kline = feather_from_file(filename_df=file_name_feather)
    if df_symbol_kline.empty:
        dt_list_date = dt_start_stale = dt_end_stale = dt_init
    else:
        if dt_am_start < dt_now < dt_pm_end:
            logger.debug(f"[{symbol}]- feather from file.----trading time")
            df_symbol_kline = df_symbol_kline.loc[start_date:end_date]
            return df_symbol_kline
        dt_start_stale = dt_list_date = df_symbol_kline.index.min()
        dt_end_stale = df_symbol_kline.index.max()
    if symbol in df_daily_basic.index:
        dt_list_date = datetime.datetime.combine(
            df_daily_basic.at[symbol, "list_date"].date(), time_pm_end
        )
    if df_symbol_kline.empty:
        df_symbol_kline = kline_base(
            symbol=symbol,
            frequency=frequency,
            asset=asset,
            adjust=adjust,
            start_date=dt_list_date,
            end_date=dt_end,
        )
    else:
        if dt_end_stale < dt_end or dt_start_stale < dt_list_date:
            if dt_list_date <= dt_start_stale:
                dt_update_start = dt_end_stale - datetime.timedelta(days=14)
            else:
                dt_update_start = dt_list_date
            if reset:
                dt_update_start = dt_list_date
            df_symbol_kline_update = kline_base(
                symbol=symbol,
                frequency=frequency,
                asset=asset,
                adjust=adjust,
                start_date=dt_update_start,
                end_date=dt_end,
            )
            if not df_symbol_kline_update.empty:
                df_symbol_kline = pd.concat(
                    objs=[df_symbol_kline, df_symbol_kline_update],
                    axis=0,
                    join="outer",
                )
                df_symbol_kline = df_symbol_kline[
                    ~df_symbol_kline.index.duplicated(keep="last")
                ]
        else:
            df_symbol_kline = df_symbol_kline[start_date:end_date]
            logger.debug(f"[{symbol}] - feather from [{file_name_feather}]")
            return df_symbol_kline
    if not df_symbol_kline.empty:
        dt_start_min = df_symbol_kline.index.min().replace(microsecond=0)
        dt_end_max = df_symbol_kline.index.max().replace(microsecond=0)
        if dt_start_stale <= dt_start_min < dt_end <= dt_end_max <= dt_end_stale:
            logger.debug(f"[{symbol}] - [{dt_end_max}] - [{dt_end}] is Latest")
        df_symbol_kline.sort_index(ascending=True, inplace=True)
        df_symbol_kline.dropna(inplace=True)
        dt_start_min = df_symbol_kline.index.min().replace(microsecond=0)
        if dt_start_min > dt_list_date:
            dt_list_date = dt_start_min
        df_symbol_kline.index.name = dt_list_date.strftime(format_dt)
        feather_to_file(df=df_symbol_kline, filename_df=file_name_feather)
        logger.debug(
            f"[{symbol}] - [{dt_end_max}] - feather to - [{file_name_feather}]"
        )
        df_symbol_kline = df_symbol_kline.loc[start_date:end_date]
    return df_symbol_kline


def index_zh_a_hist_min_em(
    symbol: str = "sz399006", today: bool = True
) -> pd.DataFrame:
    temp_df = ak_index_zh_a_hist_min_em(symbol=symbol, period=1)
    if today:
        dt_now_date = datetime.datetime.now().date()
        time_start = datetime.time(hour=9, minute=30)
        time_end = datetime.time(hour=15)
        dt_start = datetime.datetime.combine(dt_now_date, time_start)
        dt_end = datetime.datetime.combine(dt_now_date, time_end)
        temp_df = temp_df.loc[dt_start:dt_end]
    return temp_df


def kline_index(
    symbol: str = "sh000001",
    frequency: str = "D",
    dt_start: datetime.datetime = dt_init,
    dt_end: datetime.datetime = dt_pm_end,
) -> pd.DataFrame:
    dt_now = datetime.datetime.now()
    path_kline = path_data.joinpath(f"kline_index_{frequency}")
    path_kline.mkdir(exist_ok=True)
    file_name_index_feather = path_kline.joinpath(f"{symbol}.ftr")
    df_index = feather_from_file(filename_df=file_name_index_feather)
    dt_start_update = dt_start
    dt_end_update = dt_end
    if not df_index.empty:
        if dt_am_start < dt_now < dt_pm_end and frequency == "D":
            logger.debug(f"feather from {file_name_index_feather}.----trading time")
            df_index = df_index.loc[dt_start:dt_end]
            return df_index
        dt_max = df_index.index.max()
        if dt_max >= dt_pm_end:
            logger.debug(f"feather from [{file_name_index_feather}]")
            df_index = df_index.loc[dt_start:dt_end]
            return df_index
        dt_min = df_index.index.min()
        if dt_start > dt_min:
            dt_start_update = dt_end - datetime.timedelta(days=30)
        if dt_max < dt_end:
            dt_end_update = dt_pm_end
    if frequency == "D":
        df_index_update = index_daily(
            symbol=symbol, dt_start=dt_start_update, dt_end=dt_end_update
        )
        df_index_update["volume"] = df_index_update["vol"]
    elif frequency == "1m":
        df_index_update = index_zh_a_hist_min_em(symbol=symbol, today=False)
    else:
        df_index_update = pd.DataFrame()
    if df_index.empty:
        df_index = df_index_update
    else:
        df_index = pd.concat(objs=[df_index, df_index_update], axis=0, join="outer")
        df_index = df_index[~df_index.index.duplicated(keep="last")]
        df_index.fillna(value=0.0, inplace=True)
    if df_index.empty:
        logger.error(f"df_index {symbol} is empty")
    else:
        df_index = df_index[(df_index["close"] > 0)]
        str_dt_max = df_index.index.max().strftime(format_dt)
        df_index.index.rename(name=str_dt_max, inplace=True)
        feather_to_file(df=df_index, filename_df=file_name_index_feather)
        logger.debug(f"feather to [{file_name_index_feather}]")
        df_index = df_index.loc[dt_start:dt_end]
    return df_index


def kline_industry_index(
    ts_code: str,
    frequency="D",
    dt_start=dt_init,
    dt_end=dt_pm_end,
    all_data=False,
) -> pd.DataFrame:
    symbol_index = code_ts_to_ths(ts_code)
    path_kline_industry = path_data.joinpath(f"kline_industry_{frequency}")
    path_kline_industry.mkdir(exist_ok=True)
    filename_kline_industry_index = path_kline_industry.joinpath(f"{symbol_index}.ftr")
    df_kline_industry_index = feather_from_file(
        filename_df=filename_kline_industry_index
    )
    dt_update_start = dt_start
    if not df_kline_industry_index.empty:
        dt_min = df_kline_industry_index.index.min()
        dt_max = df_kline_industry_index.index.max()
        logger.debug(
            f"[{ts_code}] - [{dt_max}] - feather from [{filename_kline_industry_index}]"
        )
        if dt_min <= dt_start < dt_pm_end <= dt_max:
            if not all_data:
                df_kline_industry_index = df_kline_industry_index.loc[dt_start:dt_end]
            return df_kline_industry_index
        if dt_min > dt_start:
            dt_update_start = dt_end - datetime.timedelta(days=15)
    df_kline_industry_index_update = ts_sw_daily(
        ts_code=ts_code,
        dt_start=dt_update_start,
        dt_end=dt_pm_end,
    )
    if not df_kline_industry_index_update.empty:
        if not df_kline_industry_index.empty:
            df_kline_industry_index = pd.concat(
                objs=[df_kline_industry_index, df_kline_industry_index_update],
                axis=0,
                join="outer",
            )
            df_kline_industry_index = df_kline_industry_index[
                ~df_kline_industry_index.index.duplicated(keep="last")
            ]
        else:
            df_kline_industry_index = df_kline_industry_index_update
    else:
        logger.error(f"[{ts_code}] - Update Error")
    if not df_kline_industry_index.empty:
        df_kline_industry_index.sort_index(ascending=True, inplace=True)
        dt_max = df_kline_industry_index.index.max()
        str_dt_max = dt_max.strftime(format_dt)
        df_kline_industry_index.index.rename(name=str_dt_max, inplace=True)
        df_kline_industry_index.sort_index(ascending=True, inplace=True)
        feather_to_file(
            df=df_kline_industry_index, filename_df=filename_kline_industry_index
        )
        logger.debug(
            f"feather to [{filename_kline_industry_index}] - [{dt_max}]",
        )
        if not all_data:
            df_kline_industry_index = df_kline_industry_index.loc[dt_start:dt_end]
    return df_kline_industry_index


def update_kline_stocks(
    frequency: str = "1m",
    stock: bool = True,
    etf: bool = True,
    reset_catalogue: bool = False,
    reset_kline: bool = False,
) -> bool:
    name: str = f"update_kline_{frequency}"
    start_loop_time = time.perf_counter_ns()
    file_name_catalogue = path_chip.joinpath(f"df_catalogue_stock_{frequency}.ftr")
    if reset_catalogue:
        feather_delete(filename_df=file_name_catalogue)
        logger.debug("reset df_catalogue")
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    df_catalogue = feather_from_file(filename_df=file_name_catalogue)
    if not df_catalogue.empty:
        dt_catalogue = datetime.datetime.strptime(
            df_catalogue.index.name,
            format_dt,
        )
        if dt_catalogue >= dt_end:
            return True
    list_all_code = list()
    if stock:
        list_all_code += list_all_stock
    if etf:
        list_all_code += list_all_etf
    file_name_catalogue_temp = path_temp.joinpath(
        f"df_catalogue_stock_{frequency}_temp.ftr",
    )
    df_catalogue = feather_from_file(filename_df=file_name_catalogue_temp)
    if df_catalogue.empty:
        logger.info(f"stock ={len(list_all_stock)},     ETF = {len(list_all_etf)}")
        df_catalogue = pd.DataFrame(
            index=list_all_code, columns=["start", "end", "count"]
        )
        df_catalogue["start"] = dt_init
        df_catalogue["end"] = dt_init
        df_catalogue["count"] = 0
        feather_to_file(df=df_catalogue, filename_df=file_name_catalogue_temp)
    df_catalogue = df_catalogue.sample(frac=1)
    df_catalogue.sort_values(by=["end"], ascending=False, inplace=True)
    i_lack = 0
    if frequency == "1m":
        i_lack_max = 550
    else:
        i_lack_max = 30
    dt_finish = dt_end
    logger.info(f"{name} - [{dt_finish}]")
    time.sleep(float_time_sleep)
    count = len(df_catalogue)
    i = 0
    for symbol in df_catalogue.index:
        i += 1
        str_msg = f"Kline_{frequency} Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        if random.randint(a=0, b=9) == 5:
            feather_to_file(df=df_catalogue, filename_df=file_name_catalogue_temp)
        if df_catalogue.at[symbol, "end"] >= dt_finish:
            print(
                f"{str_msg} - [{df_catalogue.at[symbol, 'end']}] - "
                f"[{df_catalogue.at[symbol, 'count']}] - Latest\033[K",
            )
            continue
        if symbol in list_all_etf:
            df_symbol_kline = kline(
                symbol=symbol, frequency=frequency, asset="FD", reset=reset_kline
            )
            str_msg += " - FD"
            logger.debug(str_msg)
        elif symbol in list_all_stock:
            df_symbol_kline = kline(
                symbol=symbol, frequency=frequency, asset="E", reset=reset_kline
            )
            logger.debug(str_msg)
        else:
            print(f"{str_msg} - is not A stock code!")
            logger.debug(f"{str_msg} - is not A stock code!")
            continue
        if df_symbol_kline.empty:
            print(
                f"{str_msg} - No data\033[K",
            )
            continue
        df_catalogue.at[symbol, "start"] = df_symbol_kline.index.min().replace(
            microsecond=0
        )
        df_catalogue.at[symbol, "end"] = df_symbol_kline.index.max().replace(
            microsecond=0
        )
        df_catalogue.at[symbol, "count"] = len(df_symbol_kline)
        str_msg += (
            f" - [{df_catalogue.at[symbol, "count"]:6d}] - "
            f"[{df_catalogue.at[symbol, "end"]}]"
        )
        if df_catalogue.at[symbol, "end"] >= dt_finish:
            print(f"{str_msg} - Update")
        else:
            i_lack += 1
            print(f"{str_msg} - Less[{i_lack}]")
        if i_lack > i_lack_max:
            logger.error(f"Lack_count = {i_lack}")
            return False
    if i >= count:
        print("\n", end="")
        str_dt_end = dt_finish.strftime(format_dt)
        df_catalogue.index.rename(name=str_dt_end, inplace=True)
        df_catalogue.sort_values(by=["count"], ascending=False, inplace=True)
        feather_to_file(
            df=df_catalogue,
            filename_df=file_name_catalogue,
        )
        file_name_catalogue_csv = path_chip_csv.joinpath(
            f"df_catalogue_{frequency}.csv"
        )
        df_catalogue.to_csv(path_or_buf=file_name_catalogue_csv)
        file_name_catalogue_temp.unlink(missing_ok=True)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    logger.debug(f"[{frequency}] A Share Data Update takes {str_gm}")
    return True


def update_kline_industry_index(reset_catalogue: bool = False) -> bool:
    name: str = f"industry_index_kline_industry"
    logger.debug(f"{name} Begin")
    start_loop_time = time.perf_counter_ns()
    dt_now = datetime.datetime.now()
    if dt_now > dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    filename_catalogue_industry = path_chip.joinpath(
        f"df_catalogue_industry_D.ftr",
    )
    if reset_catalogue:
        feather_delete(filename_df=filename_catalogue_industry)
        logger.debug("reset df_catalogue_industry_D")
    df_catalogue_industry = feather_from_file(
        filename_df=filename_catalogue_industry,
    )
    if not df_catalogue_industry.empty:
        dt_stale = datetime.datetime.strptime(
            df_catalogue_industry.index.name,
            format_dt,
        )
        if dt_stale >= dt_end:
            logger.debug("Update success")
            return True
    filename_catalogue_industry_temp = path_temp.joinpath(
        f"df_catalogue_industry_D_temp.ftr",
    )
    df_catalogue_industry = feather_from_file(
        filename_df=filename_catalogue_industry_temp,
    )
    if df_catalogue_industry.empty:
        df_catalogue_industry = index_classify()
        df_catalogue_industry["start"] = dt_end
        df_catalogue_industry["end"] = dt_init
        df_catalogue_industry["count"] = 0
        feather_to_file(
            df=df_catalogue_industry,
            filename_df=filename_catalogue_industry_temp,
        )
    dt_finish = dt_end
    dt_start = dt_now - datetime.timedelta(days=365)
    i_lack = 0
    i_lack_max = 3
    df_catalogue_industry = df_catalogue_industry.sample(frac=1)
    df_catalogue_industry.sort_values(by=["end"], ascending=False, inplace=True)
    count_industry_index = df_catalogue_industry.shape[0]
    i = 0
    for industry_code in df_catalogue_industry.index:
        i += 1
        symbol_index = code_ts_to_ths(industry_code)
        str_msg_bar = (
            f"{name}:[{i:3d}/{count_industry_index:3d}] - " f"[{symbol_index}]"
        )
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_catalogue_industry,
                filename_df=filename_catalogue_industry_temp,
            )
        if df_catalogue_industry.at[industry_code, "end"] >= dt_finish:
            print(
                f"{str_msg_bar} - "
                f"[{df_catalogue_industry.at[industry_code, 'end']}] - "
                f"[{df_catalogue_industry.at[industry_code, 'count']}] - "
                "Latest\033[K",
            )
            continue
        if df_catalogue_industry.at[industry_code, "is_pub"] == 0:
            logger.error(f"kline_industry_index No pub")
            continue
        df_kline_ths_daily = kline_industry_index(
            ts_code=industry_code, dt_start=dt_start, all_data=True
        )
        df_catalogue_industry.at[industry_code, "start"] = (
            df_kline_ths_daily.index.min()
        )
        df_catalogue_industry.at[industry_code, "end"] = df_kline_ths_daily.index.max()
        df_catalogue_industry.at[industry_code, "count"] = df_kline_ths_daily.shape[0]
        str_msg_bar += (
            f" - [{df_catalogue_industry.at[industry_code, "count"]:6d}] - "
            f"[{df_catalogue_industry.at[industry_code, "start"]}] - "
            f"[{df_catalogue_industry.at[industry_code, "end"]}]"
        )
        if df_catalogue_industry.at[industry_code, "end"] >= dt_finish:
            print(f"{str_msg_bar} - Update")
        else:
            i_lack += 1
            print(f"{str_msg_bar} - Less[{i_lack}]")
        if i_lack > i_lack_max:
            logger.error(f"Lack_count = {i_lack}")
            return False
    if i >= count_industry_index:
        print("\n", end="")
        dt_catalogue_max = df_catalogue_industry["end"].max()
        str_dt_catalogue_max = dt_catalogue_max.strftime(format_dt)
        df_catalogue_industry.index.rename(name=str_dt_catalogue_max, inplace=True)
        df_catalogue_industry.sort_values(by=["count"], ascending=False, inplace=True)
        feather_to_file(
            df=df_catalogue_industry,
            filename_df=filename_catalogue_industry,
        )
        filename_catalogue_industry_csv = path_chip_csv.joinpath(
            "df_catalogue_industry_D.csv"
        )
        df_catalogue_industry.to_csv(
            path_or_buf=filename_catalogue_industry_csv,
        )
        filename_catalogue_industry_temp.unlink(missing_ok=True)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    logger.info(f"[{name}] takes {str_gm}")
    return True


def update_kline_index(
    frequency: str = "1m",
    reset_catalogue: bool = False,
) -> bool:
    name: str = f"update_kline_index_{frequency}"
    start_loop_time = time.perf_counter_ns()
    file_name_catalogue = path_chip.joinpath(f"df_catalogue_index_{frequency}.ftr")
    if reset_catalogue:
        feather_delete(filename_df=file_name_catalogue)
        logger.debug("reset df_catalogue_index")
    df_catalogue = feather_from_file(filename_df=file_name_catalogue)
    dt_now = datetime.datetime.now()
    if dt_now >= dt_pm_end:
        dt_end = dt_pm_end
    else:
        dt_end = dt_pm_end_1t
    if not df_catalogue.empty:
        dt_catalogue = datetime.datetime.strptime(
            df_catalogue.index.name,
            format_dt,
        )
        if dt_catalogue >= dt_end:
            return True
    df_index_realtime = ak_stock_zh_index_spot_sina()
    list_index = df_index_realtime.index.tolist()
    filename_catalogue_temp = path_temp.joinpath(
        f"df_catalogue_index_{frequency}_temp.ftr",
    )
    df_catalogue = feather_from_file(filename_df=filename_catalogue_temp)
    if df_catalogue.empty:
        df_catalogue = pd.DataFrame(index=list_index, columns=["start", "end", "count"])
        df_catalogue["start"] = dt_init
        df_catalogue["end"] = dt_init
        df_catalogue["count"] = 0
        feather_to_file(df=df_catalogue, filename_df=filename_catalogue_temp)
    df_catalogue = df_catalogue.sample(frac=1)
    df_catalogue.sort_values(by=["end"], ascending=False, inplace=True)
    i_lack = 0
    if frequency == "1m":
        i_lack_max = 550
    else:
        i_lack_max = 30
    dt_finish = dt_end
    logger.info(f"{name} - [{dt_finish}]")
    time.sleep(float_time_sleep)
    count = len(df_catalogue)
    i = 0
    for symbol in df_catalogue.index:
        i += 1
        str_msg = f"Kline_index_{frequency}: [{i:4d}/{count:4d}] -- [{symbol}]"
        feather_to_file(df=df_catalogue, filename_df=filename_catalogue_temp)
        if random.randint(a=0, b=9) == 5:
            feather_to_file(df=df_catalogue, filename_df=filename_catalogue_temp)
        if df_catalogue.at[symbol, "end"] >= dt_finish:
            print(
                f"{str_msg} - [{df_catalogue.at[symbol, 'end']}] - "
                f"[{df_catalogue.at[symbol, 'count']}] - Latest\033[K",
            )
            continue
        df_kline_index = kline_index(
            symbol=symbol,
            frequency=frequency,
        )
        if df_kline_index.empty:
            print(
                f"{str_msg} - No data\033[K",
            )
            continue
        df_catalogue.at[symbol, "start"] = df_kline_index.index.min().replace(
            microsecond=0
        )
        df_catalogue.at[symbol, "end"] = df_kline_index.index.max().replace(
            microsecond=0
        )
        df_catalogue.at[symbol, "count"] = df_kline_index.shape[0]
        str_msg += (
            f" - [{df_catalogue.at[symbol, "count"]:6d}] - "
            f"[{df_catalogue.at[symbol, "end"]}]"
        )
        if df_catalogue.at[symbol, "end"] >= dt_finish:
            print(f"{str_msg} - Update")
        else:
            i_lack += 1
            print(f"{str_msg} - Less[{i_lack}]")
        if i_lack > i_lack_max:
            logger.error(f"Lack_count = {i_lack}")
            return False
    if i >= count:
        print("\n", end="")
        str_dt_end = dt_finish.strftime(format_dt)
        df_catalogue.index.rename(name=str_dt_end, inplace=True)
        df_catalogue.sort_values(by=["count"], ascending=False, inplace=True)
        feather_to_file(
            df=df_catalogue,
            filename_df=file_name_catalogue,
        )
        file_name_catalogue_csv = path_chip_csv.joinpath(
            f"df_catalogue_index_{frequency}.csv"
        )
        df_catalogue.to_csv(path_or_buf=file_name_catalogue_csv)
        filename_catalogue_temp.unlink(missing_ok=True)
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    logger.debug(f"[{frequency}] A Index Data Update takes {str_gm}")
    return True


def update_data() -> bool:
    while True:
        if update_kline_stocks(frequency="1m"):
            logger.trace(f"Kline_Stocks_1m Update finish")
            break
        else:
            logger.error("Sleep 30 minutes")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            sleep_to_time(dt_time=dt_now_delta, seconds=10)
    while True:
        if update_kline_index(frequency="1m"):
            logger.trace(f"Kline_Index_1m Update finish")
            break
        else:
            logger.error("Sleep 30 minutes")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            sleep_to_time(dt_time=dt_now_delta, seconds=10)
    while True:
        if update_kline_stocks(frequency="D"):
            logger.trace(f"Kline_Stocks_D Update finish")
            break
        else:
            logger.error("Sleep 30 minutes")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            sleep_to_time(dt_time=dt_now_delta, seconds=10)
    while True:
        if update_kline_index(frequency="D"):
            logger.trace(f"Kline_Index_1m Update finish")
            break
        else:
            logger.error("Sleep 30 minutes")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            sleep_to_time(dt_time=dt_now_delta, seconds=10)
    while True:
        if update_kline_industry_index():
            logger.trace(f"Kline_industry_index_D Update finish")
            break
        else:
            logger.error("Sleep 30 minutes")
            dt_now_delta = datetime.datetime.now() + datetime.timedelta(seconds=1800)
            sleep_to_time(dt_time=dt_now_delta, seconds=10)
    return True
