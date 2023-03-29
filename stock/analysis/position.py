# modified at 2023/3/25 16：59
import datetime
import os
import numpy as np
import pandas as pd
from loguru import logger
import akshare as ak
import analysis.base
import analysis.update_data


def position(index: str = "sh000001") -> str:
    if index == "sh000001":
        name = "df_pos_ctl_sh"
    elif index == "sh000852":
        name = "df_pos_ctl_csi1000"
    else:
        name = "df_pos_ctl_other"
    logger.trace(f"position_control-[{name}] Begin")
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    path_main = os.getcwd()
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    # 取得实时上证指数
    df_index_realtime = ak.stock_zh_index_spot()
    df_index_realtime.set_index(keys="代码", inplace=True)
    if analysis.base.is_latest_version(key=name):
        df_pos_ctl = analysis.base.read_obj_from_db(key=name)
        stock_close = df_index_realtime.at[index, "最新价"]
        stock_name = df_index_realtime.at[index, "名称"]
        stock_close = int(stock_close) // 10 * 10
        len_df_pos_ctl = len(df_pos_ctl)
        rank = int(df_pos_ctl.at[stock_close, "rank"])
        str_pos_ctl = (
            f"[{stock_name}] - [{stock_close} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
            f"[{df_pos_ctl.at[stock_close, 'sun_descending']:5.2f}]"
        )
        logger.trace(f"position_control-[{name}] Break End")
        return str_pos_ctl
    logger.trace(f"Update df_pos_ctl-[pydb_chip] Begin")
    if index == "sh000001":
        df_sh_index = analysis.update_data.update_index_data(symbol=index)
        dt_date = df_sh_index.index.max().date()
    else:
        df_sh_index = ak.stock_zh_index_daily(symbol=index)
        df_sh_index["date"] = pd.to_datetime(df_sh_index["date"])
        dt_begin = datetime.datetime(year=2015, month=9, day=1)
        df_sh_index.set_index(keys=["date"], inplace=True)
        df_sh_index = df_sh_index.loc[dt_begin:]
        dt_date = analysis.base.latest_trading_day()
    df_sh_index["close"] = df_sh_index["close"].apply(
        func=lambda x: int(round(x, 0)) // 10 * 10
    )
    df_pos_ctl = pd.pivot_table(
        data=df_sh_index, index=["close"], aggfunc={"volume": np.sum, "close": len}
    )
    df_pos_ctl.rename(columns={"close": "count"}, inplace=True)  # _descending
    df_pos_ctl.sort_index(ascending=False, inplace=True)
    df_pos_ctl.reset_index(inplace=True)
    descending_volume = 0
    total_volume = df_pos_ctl["volume"].sum()
    line_number = len(df_pos_ctl)
    for i in range(line_number):
        descending_volume += df_pos_ctl.at[i, "volume"]
        df_pos_ctl.at[i, "sun_descending"] = (
            int(descending_volume / total_volume * 10000) / 100
        )
        if i == 0:
            df_pos_ctl.at[i, "weight_volume"] = (
                df_pos_ctl.at[i, "volume"] + df_pos_ctl.at[i + 1, "volume"]
            )
        elif i == line_number - 1:
            df_pos_ctl.at[i, "weight_volume"] = (
                df_pos_ctl.at[i - 1, "volume"] + df_pos_ctl.at[i, "volume"]
            )
        else:
            df_pos_ctl.at[i, "weight_volume"] = (
                df_pos_ctl.at[i - 1, "volume"]
                + df_pos_ctl.at[i, "volume"]
                + df_pos_ctl.at[i + 1, "volume"]
            )
    df_pos_ctl.set_index(keys=["close"], inplace=True)
    df_pos_ctl["volume_rank"] = df_pos_ctl["volume"].rank(
        axis=0, method="min", ascending=False
    )
    df_pos_ctl["rank"] = df_pos_ctl["weight_volume"].rank(
        axis=0, method="min", ascending=False
    )
    analysis.base.write_obj_to_db(obj=df_pos_ctl, key=name)
    analysis.base.add_chip_excel(df=df_pos_ctl, key=name)
    close_sh = df_index_realtime.at[index, "最新价"]
    nane = df_index_realtime.at[index, "名称"]
    close_sh = int(close_sh) // 10 * 10
    len_df_pos_ctl = len(df_pos_ctl)
    rank = int(df_pos_ctl.at[close_sh, "rank"])
    str_pos_ctl = (
        f"[{nane}] - [{close_sh} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
        f"[{df_pos_ctl.at[close_sh, 'sun_descending']:5.2f}] -- {dt_date}"
    )
    dt_sh_index_latest = datetime.datetime.combine(dt_date, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_sh_index_latest)
    logger.trace(f"position_control-[{name}] End")
    return str_pos_ctl
