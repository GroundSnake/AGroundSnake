import datetime
import pandas as pd
import pywencai
import feather
from console import fg
from loguru import logger
import analysis.base
from analysis.const import path_check, path_data, str_trading_path


# Rising on Both Volume and Price
def volume_price_rise(df: pd.DataFrame) -> str:
    logger.trace("volume_price_rise Begin")
    str_return = ""
    path_kline = path_data.joinpath("kline_1m")
    try:
        df_price = pywencai.get(loop=True, query="主升浪量化分析系统早盘竞价选强势妖股")
    except AttributeError:
        df_price = pd.DataFrame()
    if df_price is None:
        return str_return
    else:
        if df_price.empty:
            return str_return
        else:
            set_price = set(df_price["股票代码"].tolist())
    try:
        df_volume = pywencai.get(loop=True, query="今日同期成交量超过昨日同期成交量20%")
    except AttributeError:
        df_volume = pd.DataFrame()
    if df_volume is None:
        return str_return
    else:
        if df_volume.empty:
            return str_return
        else:
            set_volume = set(df_volume["股票代码"].tolist())
    set_vp = set_price & set_volume
    list_vp = [item[-2:].lower() + item[:6] for item in set_vp]
    filename_vp_ftr = path_data.join("volume_price_rise.ftr")
    if filename_vp_ftr.exists():
        df_vp = feather.read_dataframe(source=filename_vp_ftr)
    else:
        df_vp = pd.DataFrame()
    dt_now = datetime.datetime.now().replace(microsecond=0)
    df_current = pd.DataFrame(index=list_vp)
    df_current["chosen"] = 0
    df_current["dt"] = dt_now
    if df_vp.empty:
        df_vp = df_current
    else:
        for index in df_current.index:
            if index not in df_vp.index:
                df_vp.loc[index] = df_current.loc[index]
    df_realtime = analysis.realtime_quotations(stock_codes=df_vp.index.tolist())
    for index in df_vp.index:
        if index in df_realtime.index:
            df_vp.at[index, "name"] = df_realtime.at[index, "name"]
            df_vp.at[index, "now"] = now = df_realtime.at[index, "close"]
            dt_start = df_vp.at[index, "dt"]
            if df_vp.at[index, "chosen"] == 0:
                df_vp.at[index, "chosen"] = chosen = now
            else:
                chosen = df_vp.at[index, "chosen"]
            df_vp.at[index, "pct_chg"] = round((now / chosen - 1) * 100, 2)
            file_name_data_feather = path_kline.joinpath(f"{index}.ftr")
            if file_name_data_feather.exists():
                df_data = feather.read_dataframe(source=file_name_data_feather)
                df_data = df_data[dt_start:]
                df_vp.at[index, "low"] = df_data["low"].min()
                df_vp.at[index, "pct_low"] = round(
                    (df_vp.at[index, "low"] / chosen - 1) * 100, 2
                )
                df_vp.at[index, "high"] = df_data["high"].max()
                df_vp.at[index, "pct_high"] = round(
                    (df_vp.at[index, "high"] / chosen - 1) * 100, 2
                )
                df_vp.at[index, "minutes"] = len(df_data)
        else:
            df_vp.at[index, "name"] = "NON"
    df_vp.sort_values(by=["pct_chg"], ascending=False, inplace=True)
    feather.write_dataframe(df=df_vp, dest=filename_vp_ftr)
    filename_vp_csv = path_check.joinpath(f"wc_use_{str_trading_path()}.csv")
    df_vp.to_csv(path_or_buf=filename_vp_csv)
    count = len(list_vp)
    if count == 0:
        return str_return
    line_len = 5
    i = 0
    while i < count:
        symbol = list_vp[i]
        i += 1
        str_symbol = f"[{df_vp.at[symbol, 'name']}{symbol}]"
        if symbol in df.index:
            str_symbol = fg.red(str_symbol)
        if str_return == "":
            str_return = f"{str_symbol}"
        elif i % line_len == 1:
            str_return += f"\n\r{str_symbol}"
        else:
            str_return += f", {str_symbol}"
    logger.trace("volume_price_rise End")
    return str_return
