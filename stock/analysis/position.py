# modified at 2023/3/24 15:00
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
    dt_now = datetime.datetime.now()
    dt_date_trading = analysis.base.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    file_name_chip_h5 = os.path.join(path_data, f"chip.h5")
    # file_name_pos_ctl_feather = os.path.join(path_data, f"{name}.ftr")
    file_name_pos_ctl_csv = os.path.join(path_check, f"{name}_{str_date_path}.csv")
    # 取得实时上证指数
    df_index_realtime = ak.stock_zh_index_spot()
    df_index_realtime.set_index(keys="代码", inplace=True)
    df_config = pd.DataFrame()
    if os.path.exists(file_name_chip_h5):
        try:
            df_config = pd.read_hdf(path_or_buf=file_name_chip_h5, key="df_config")
        except KeyError as e:
            logger.trace(f"df_config not exist KeyError [{e}]")
        if not df_config.empty:
            try:
                logger.trace(
                    f"the latest {name} at {df_config.at[name, 'date']},The new at {dt_pm_end}"
                )
                if (df_config.at[name, "date"] < dt_now < dt_pm_end
                    or df_config.at[name, "date"] == dt_pm_end
                ):
                    logger.trace(f"df_pos_ctl-[{file_name_chip_h5}] is latest")
                    # df_pos_ctl = feather.read_dataframe(source=file_name_pos_ctl_feather)
                    df_pos_ctl = pd.read_hdf(path_or_buf=file_name_chip_h5, key=name)
                    stock_close = df_index_realtime.at[index, "最新价"]
                    stock_name = df_index_realtime.at[index, "名称"]
                    stock_close = int(stock_close) // 10 * 10
                    len_df_pos_ctl = len(df_pos_ctl)
                    rank = int(df_pos_ctl.at[stock_close, "rank"])
                    str_pos_ctl = (
                        f"[{stock_name}] - [{stock_close} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
                        f"[{df_pos_ctl.at[stock_close, 'sun_descending']:5.2f}] -- {df_config.at[name, 'date'].date()}"
                    )
                    logger.trace(f"position_control-{name} Break End")
                    return str_pos_ctl
                else:
                    logger.trace(
                        f"df_pos_ctl-{name}-[{file_name_chip_h5}] is not latest"
                    )
            except KeyError as e:
                logger.trace(f"df_config not exist KeyError [{e}]")
    logger.trace(f"Update df_pos_ctl-[{file_name_chip_h5}] Begin")
    if index == "sh000001":
        df_sh_index = analysis.update_data.update_index_data(symbol=index)
    else:
        df_sh_index = ak.stock_zh_index_daily(symbol=index)
        df_sh_index["date"] = pd.to_datetime(df_sh_index["date"])
        dt_begin = datetime.datetime(year=2015, month=9, day=1)
        df_sh_index.set_index(keys=["date"], inplace=True)
        df_sh_index = df_sh_index.loc[dt_begin:]
    dt_date = df_sh_index.index.max().date()
    # df_sh_index["close"] = df_sh_index["close"].round(0)
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
    # for tup_item in df_pos_ctl.itertuples():
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
    # feather.write_dataframe(df=df_pos_ctl, dest=file_name_pos_ctl_feather)
    df_pos_ctl.to_hdf(path_or_buf=file_name_chip_h5, key=name, format='table')
    logger.trace(f"HDF5 df_pos_ctl-[{file_name_chip_h5}]")
    df_pos_ctl.to_csv(path_or_buf=file_name_pos_ctl_csv)
    logger.trace(f"save df_pos_ctl at csv-[{file_name_pos_ctl_csv}]")
    logger.trace(f"Update df_sh_index-[{file_name_chip_h5}] End")
    close_sh = df_index_realtime.at[index, "最新价"]
    nane = df_index_realtime.at[index, "名称"]
    close_sh = int(close_sh) // 10 * 10
    len_df_pos_ctl = len(df_pos_ctl)
    rank = int(df_pos_ctl.at[close_sh, "rank"])
    str_pos_ctl = (
        f"[{nane}] - [{close_sh} - {rank:3.0f}/{len_df_pos_ctl:3.0f}] --- "
        f"[{df_pos_ctl.at[close_sh, 'sun_descending']:5.2f}] -- {dt_date}"
    )
    if os.path.exists(file_name_chip_h5):
        dt_sh_index_latest = datetime.datetime.combine(dt_date, time_pm_end)
        df_config.at[name, "date"] = dt_sh_index_latest
        df_config.to_hdf(path_or_buf=file_name_chip_h5, key="df_config", format='table')
    logger.trace(f"position_control-[{name}] End")
    return str_pos_ctl
