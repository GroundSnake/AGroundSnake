# modified at 2023/05/18 22::25
import datetime
import pandas as pd
from loguru import logger
import akshare as ak
from console import fg
import analysis.base
import analysis.update_data
from analysis.const import time_pm_end, dt_pm_end, client_ts_pro


def position(index: str = "sh000001") -> str:
    if index == "sh000001":
        name = "df_pos_ctl_sh"
    elif index == "sh000852":
        name = "df_pos_ctl_csi1000"
    else:
        name = "df_pos_ctl_other"
    logger.trace(f"position_control-[{name}] Begin")
    rank = 0
    i_rank = 0
    position_rate = 0.7
    # 取得实时上证指数
    df_index_realtime = ak.stock_zh_index_spot()
    df_index_realtime.set_index(keys="代码", inplace=True)
    close_ssb = close_ssb_gap = int(df_index_realtime.at[index, "最新价"]) // 10 * 10
    nane_ssb = df_index_realtime.at[index, "名称"]
    if analysis.base.is_latest_version(key=name):
        df_pos_ctl = analysis.base.feather_from_file(key=name)
        len_df_pos_ctl = len(df_pos_ctl)
        while i_rank < 5:
            try:
                rank = int(df_pos_ctl.at[close_ssb, "rank"])
                break
            except KeyError:
                close_ssb += 10
                i_rank += 1
        if rank == 0:
            logger.error("Rank ERROR")
            return f"{index} ERROR"
        str_pos_ctl = (
            f"[{nane_ssb}] - [{close_ssb} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
            f"[{df_pos_ctl.at[close_ssb, 'sun_descending'] * position_rate:5.2f}]"
        )
        if i_rank != 0:
            str_pos_ctl += fg.red(f" * Gap[{close_ssb_gap}]")
        logger.trace(f"position_control-[{name}] Break End")
        return str_pos_ctl
    logger.trace(f"Update [{name}] Begin")
    dt_begin = datetime.datetime(year=2015, month=9, day=1)
    str_dt = dt_begin.strftime("%Y%m%d")
    str_now = datetime.datetime.now().strftime("%Y%m%d")
    if index == "sh000001":
        df_index = analysis.update_data.update_index_data(symbol=index[2:])
    else:
        ts_code = analysis.base.code_ths_to_ts(index)
        # df_index = ak.stock_zh_index_daily(symbol=index)
        df_index = client_ts_pro.index_daily(
            ts_code=ts_code, start_date=str_dt, end_date=str_now
        )
        df_index["date"] = pd.to_datetime(df_index["trade_date"])
        df_index.set_index(keys=["date"], inplace=True)
        df_index.sort_index(inplace=True)
        df_index["volume"] = df_index["vol"]
    df_index = df_index.loc[dt_begin:].copy()
    df_index["close"] = df_index["close"].apply(
        func=lambda x: int(round(x, 0)) // 10 * 10
    )
    df_pos_ctl = pd.pivot_table(
        data=df_index, index=["close"], aggfunc={"volume": "sum", "close": "count"}
    )
    dt_index_max = df_index.index.max()
    dt_index_max_date = dt_index_max.date()
    if dt_index_max < dt_pm_end:
        logger.error(f"{name} is not update - [{dt_index_max}]")
    df_pos_ctl.rename(columns={"close": "count"}, inplace=True)  # _descending
    df_pos_ctl.sort_index(ascending=False, inplace=True)
    count_all = df_pos_ctl["count"].sum()
    df_pos_ctl["count_rate"] = round(df_pos_ctl["count"] / count_all * 100, 2)
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
            if line_number > 0:
                df_pos_ctl.at[i, "weight_volume"] = (
                    df_pos_ctl.at[i, "volume"] + df_pos_ctl.at[i + 1, "volume"]
                )
            else:
                df_pos_ctl.at[i, "weight_volume"] *= 2
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
    analysis.base.feather_to_file(
        df=df_pos_ctl,
        key=name,
    )
    len_df_pos_ctl = len(df_pos_ctl)
    while i_rank < 5:
        try:
            rank = int(df_pos_ctl.at[close_ssb, "rank"])
            break
        except KeyError:
            close_ssb += 10
            i_rank += 1
    if rank == 0:
        logger.error("Rank ERROR")
        return f"{index} ERROR"
    str_pos_ctl = (
        f"[{nane_ssb}] - [{close_ssb} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
        f"[{df_pos_ctl.at[close_ssb, 'sun_descending'] * position_rate:5.2f}] -- {dt_index_max_date}"
    )
    if i_rank != 0:
        str_pos_ctl += fg.red(f" * Gap[{close_ssb_gap}]")
    dt_sh_index_latest = datetime.datetime.combine(dt_index_max_date, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_sh_index_latest)
    logger.trace(f"position_control-[{name}] End")
    return str_pos_ctl
