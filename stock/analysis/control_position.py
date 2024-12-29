import datetime
import math
import pandas as pd
from console import fg
from analysis.log import logger
from analysis.const_dynamic import (
    dt_init,
    dt_pm_end,
    dt_pm_end_1t,
    path_chip,
    path_chip_csv,
    format_dt,
)
from analysis.base import (
    feather_from_file,
    feather_to_file,
)
from analysis.api_akshare import ak_stock_zh_index_spot_sina
from analysis.update_data import kline_index


def control_position_index(index: str = "sh000001") -> str:
    name = f"df_pos_ctl_{index}"
    filename_pos_ctl_x = path_chip.joinpath(f"{name}.ftr")
    filename_pos_ctl_x_csv = path_chip_csv.joinpath(f"{name}.csv")
    df_pos_ctl = feather_from_file(filename_df=filename_pos_ctl_x)
    dt_now = datetime.datetime.now()
    bool_update_pos_ctl_x = True
    if dt_now > dt_pm_end:
        dt_update = dt_pm_end
    else:
        dt_update = dt_pm_end_1t
    dt_start = datetime.datetime(year=2015, month=9, day=1)
    if index == "sh000001":
        df_index = kline_index(
            symbol=index, frequency="1m", dt_start=dt_start, dt_end=dt_update
        )
    else:
        df_index = kline_index(
            symbol=index, frequency="D", dt_start=dt_start, dt_end=dt_update
        )
    dt_index_max = df_index.index.max()
    dt_index_min = df_index.index.min()
    if not df_pos_ctl.empty:
        try:
            dt_stale = datetime.datetime.strptime(df_pos_ctl.index.name, format_dt)
        except ValueError:
            dt_stale = dt_init
        if dt_stale >= dt_update:
            bool_update_pos_ctl_x = False
    if bool_update_pos_ctl_x:
        # df_index = df_index.loc[dt_start:].copy()
        df_index["close"] = df_index["close"].apply(
            func=lambda x: round(math.floor(x), -1)
        )
        df_pos_ctl = pd.pivot_table(
            data=df_index, index=["close"], aggfunc={"volume": "sum", "close": "count"}
        )
        if dt_index_max < dt_update:
            logger.error(f"{name} is not update - [{dt_index_max}]")
        df_pos_ctl.rename(columns={"close": "count"}, inplace=True)  # _descending
        df_pos_ctl.sort_index(ascending=False, inplace=True)
        count_all = df_pos_ctl["count"].sum()
        df_pos_ctl["count_rate"] = round(df_pos_ctl["count"] / count_all * 100, 2)
        volume_all = df_pos_ctl["volume"].sum()
        df_pos_ctl["volume_rate"] = round(df_pos_ctl["volume"] / volume_all * 100, 2)
        str_dt_index_max = dt_index_max.strftime(format_dt)
        df_pos_ctl.index.rename(name=str_dt_index_max, inplace=True)
        feather_to_file(df=df_pos_ctl, filename_df=filename_pos_ctl_x)
        logger.debug(f"feather to [{filename_pos_ctl_x}]")
        df_pos_ctl.to_csv(path_or_buf=filename_pos_ctl_x_csv)
    if df_pos_ctl.empty:
        logger.error(f"{name} is empty.")
        return f"{name} is empty."
    df_index_realtime = ak_stock_zh_index_spot_sina()
    if df_index_realtime.empty:
        logger.error(f"df_index_realtime[{index}] is empty.")
        return f"df_index_realtime[{index}] is empty."
    if index not in df_index_realtime.index:
        return f"[{index}] not is df_index_realtime."
    close_ssb = int(df_index_realtime.at[index, "close"]) // 10 * 10
    nane_ssb = df_index_realtime.at[index, "name"]
    df_index_min = df_pos_ctl[df_pos_ctl.index < close_ssb]
    count_rate_min = df_index_min["count_rate"].sum()
    df_index_max = df_pos_ctl[df_pos_ctl.index > close_ssb]
    volume_rate_max = df_index_max["volume_rate"].sum()
    count_all = df_pos_ctl["count"].sum()
    str_pos_ctl = fg.red(
        f"[{nane_ssb} - {close_ssb:5d}] - "
        f"<{count_all:6d} Items> - "
        f"<Duration-↓:{count_rate_min:5.2f}%> - "
        f"[{volume_rate_max:5.2f}] - "
        f"[{dt_index_min.date()} - "
        f"{dt_index_max.date()}]"
    )
    str_pos_ctl = (
        f"<{datetime.datetime.now().time().replace(microsecond=0)}> - " f"{str_pos_ctl}"
    )
    return str_pos_ctl
