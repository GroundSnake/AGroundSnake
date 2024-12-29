import json
import datetime
from console import fg
import pandas as pd
from analysis.const_dynamic import (
    path_history,
    path_main,
    path_check,
    filename_check_etf_t0_csv,
    filename_check_etf_t0,
    pct_etf_c,
)
from analysis.list_etf_t0 import list_etfs_t0
from analysis.realtime_quotation import realtime_quotation
from analysis.base import (
    feather_from_file,
    feather_to_file,
    check_chs_code,
    csv_to_file,
)
from analysis.win32_speak import say


def check_etf_t0(etfs: list | None = None) -> list:
    ups_etf = pct_etf_c
    downs_etf = -pct_etf_c
    dict_etf_t0_dtype = {
        "name": "object",
        "price": "float64",
        "pct_chg": "float64",
        "now": "float64",
    }
    dict_etf_t0_default = {
        "name": "name",
        "price": 0.0,
        "pct_chg": 0.0,
        "now": 0.0,
    }
    filename_etfs_reset = path_main.joinpath("reset_etf_t0.json")
    list_modified = list()
    if filename_etfs_reset.exists():
        with open(file=filename_etfs_reset, mode="r") as f:
            list_modified = json.load(fp=f)
        str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename_etfs_reset_remove = path_history.joinpath(
            f"reset_etf_t0_{str_now_input}.json"
        )
        with open(file=filename_etfs_reset_remove, mode="w") as fp:
            json.dump(obj=list_modified, fp=fp, indent=1)
        filename_etfs_reset.unlink(missing_ok=True)
    if len(list_modified) > 0:
        list_modified = check_chs_code(symbols=list_modified)
    df_realtime_etf = realtime_quotation.get_etfs_a()
    df_realtime_etf.to_csv("b.csv")
    df_realtime_etf = df_realtime_etf[df_realtime_etf["close"] < 90]
    list_realtime_etf = df_realtime_etf.index.tolist()
    df_check_etf_t0 = feather_from_file(filename_df=filename_check_etf_t0)
    if df_check_etf_t0.empty:
        df_check_etf_t0 = pd.DataFrame(
            index=list_etfs_t0, columns=["name", "price", "pct_chg", "now"]
        )
        df_check_etf_t0 = df_check_etf_t0.astype(dtype=dict_etf_t0_dtype)
        df_check_etf_t0.fillna(value=dict_etf_t0_default, inplace=True)
    list_etfs = list(set(list_etfs_t0) & set(list_realtime_etf))
    if etfs is not None:
        list_etfs = list(set(list_etfs + etfs))
    if len(list_modified) > 0:
        list_etfs = list(set(list_etfs + list_modified))
        df_check_etf_t0 = df_check_etf_t0.reindex(index=list_etfs)
        df_check_etf_t0 = df_check_etf_t0.astype(dtype=dict_etf_t0_dtype)
        df_check_etf_t0.fillna(value=dict_etf_t0_default, inplace=True)
        dt_now_time = datetime.datetime.now().time().replace(microsecond=0)
        str_reset_etfs = f"\n<{dt_now_time}>----[Reset ETFs] - {list_modified}"
        print(str_reset_etfs)
        say(text="reset ETFs")
    list_signal = list()
    for symbol in df_check_etf_t0.index:
        if symbol not in df_realtime_etf.index:
            continue
        df_check_etf_t0.at[symbol, "name"] = df_realtime_etf.at[symbol, "name"]
        df_check_etf_t0.at[symbol, "now"] = now = df_realtime_etf.at[symbol, "close"]
        if df_check_etf_t0.at[symbol, "price"] <= 0 or symbol in list_modified:
            df_check_etf_t0.at[symbol, "price"] = df_realtime_etf.at[symbol, "close"]
        df_check_etf_t0.at[symbol, "pct_chg"] = pct_chg = round(
            now / df_check_etf_t0.at[symbol, "price"] * 100 - 100, 2
        )
        if pct_chg < downs_etf or pct_chg > ups_etf:
            list_signal.append(symbol)
    df_check_etf_t0 = df_check_etf_t0[df_check_etf_t0["price"] > 0]
    df_check_etf_t0.sort_values(by=["pct_chg"], ascending=[True], inplace=True)
    feather_to_file(df=df_check_etf_t0, filename_df=filename_check_etf_t0)
    if len(list_signal) > 0:
        filename_signal_etfs_t0 = path_check.joinpath("signal_etf_t0.json")
        with open(file=filename_signal_etfs_t0, mode="w") as fp:
            json.dump(obj=list_signal, fp=fp, indent=1)
    csv_to_file(df=df_check_etf_t0, filename_df=filename_check_etf_t0_csv)
    if etfs is None:
        df_check_etf_t0_ctrl = df_check_etf_t0.reindex(index=list_signal)
    else:
        list_signal_and_etfs = list(set(list_signal + etfs))
        df_check_etf_t0_ctrl = df_check_etf_t0.reindex(index=list_signal_and_etfs)
    i_ups = 0
    i_downs = 0
    line_len = 3
    str_signal_ups = ""
    str_signal_downs = ""
    df_check_etf_t0_ctrl_ups = df_check_etf_t0_ctrl[df_check_etf_t0_ctrl["pct_chg"] > 0]
    df_check_etf_t0_ctrl_downs = df_check_etf_t0_ctrl[
        df_check_etf_t0_ctrl["pct_chg"] < 0
    ]
    df_check_etf_t0_ctrl_ups = df_check_etf_t0_ctrl_ups.sort_values(
        by=["pct_chg"], ascending=[False]
    )
    df_check_etf_t0_ctrl_downs = df_check_etf_t0_ctrl_downs.sort_values(
        by=["pct_chg"], ascending=[True]
    )
    for symbol in df_check_etf_t0_ctrl_ups.index:
        i_ups += 1
        pct_chg = round(df_check_etf_t0_ctrl_ups.at[symbol, "pct_chg"], 2)
        str_symbol = f"[{df_check_etf_t0_ctrl_ups.at[symbol, 'name']:>8}({symbol}) {pct_chg:5.2f}%]"
        if pct_chg >= 5:
            str_symbol = fg.purple(str_symbol)
        elif 3 <= pct_chg < 5:
            str_symbol = fg.red(str_symbol)
        elif -3 < pct_chg <= -3:
            str_symbol = fg.green(str_symbol)
        elif pct_chg <= -5:
            str_symbol = fg.blue(str_symbol)
        if str_signal_ups == "":
            str_signal_ups = f"{str_symbol}"
        elif i_ups % line_len == 1:
            str_signal_ups += f"\n{str_symbol}"
        else:
            str_signal_ups += f", {str_symbol}"
    for symbol in df_check_etf_t0_ctrl_downs.index:
        i_downs += 1
        pct_chg = round(df_check_etf_t0_ctrl_downs.at[symbol, "pct_chg"], 2)
        str_symbol = f"[{df_check_etf_t0_ctrl_downs.at[symbol, 'name']:>8}({symbol}) {pct_chg:5.2f}%]"
        if pct_chg >= 5:
            str_symbol = fg.purple(str_symbol)
        elif 3 <= pct_chg < 5:
            str_symbol = fg.red(str_symbol)
        elif -5 < pct_chg <= -3:
            str_symbol = fg.green(str_symbol)
        elif pct_chg <= -5:
            str_symbol = fg.blue(str_symbol)
        if str_signal_downs == "":
            str_signal_downs = f"{str_symbol}"
        elif i_downs % line_len == 1:
            str_signal_downs += f"\n\r{str_symbol}"
        else:
            str_signal_downs += f", {str_symbol}"
    dt_now = datetime.datetime.now()
    str_dt_time = dt_now.strftime("<%H:%M:%S>")
    space_line = " " * 43
    if str_signal_ups:
        str_signal_ups = (
            f"{space_line}"
            f"{fg.red(f"{str_dt_time} - ETF_T+0 - <Ups>")}"
            f"\n{str_signal_ups}"
        )
    if str_signal_downs:
        str_signal_downs = (
            f"{space_line}"
            f"{fg.green(f"{str_dt_time} - ETF_T+0 - <Downs>")}"
            f"\n{str_signal_downs}"
        )
    set_return = [str_signal_ups, str_signal_downs]
    return set_return
