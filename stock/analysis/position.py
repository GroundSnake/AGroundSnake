import datetime
import os
import pickle
import feather
import numpy as np
import pandas as pd
from loguru import logger
import akshare as ak
import ashare


def position(index: str = "sh000001") -> str:
    if index == "sh000001":
        pos_ctl = "pos_ctl_sh"
    elif index == "sh000852":
        pos_ctl = "pos_ctl_csi1000"
    else:
        pos_ctl = "pos_ctl_other"
    logger.trace(f"position_control-{pos_ctl} Begin")
    dt_now = datetime.datetime.now()
    dt_date_trading = ashare.latest_trading_day()
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
    file_name_config = os.path.join(path_data, f"config.pkl")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_pos_ctl_feather = os.path.join(path_data, f"{pos_ctl}.ftr")
    file_name_pos_ctl_csv = os.path.join(path_check, f"{pos_ctl}_{str_date_path}.csv")
    # 取得实时上证指数
    df_index_realtime = ak.stock_zh_index_spot()
    df_index_realtime.set_index(keys="代码", inplace=True)
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load dict_config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
        if pos_ctl in dict_config:
            logger.trace(f"the latest df_pos_ctl at {dict_config[pos_ctl]},The new at {dt_pm_end}")
            if dict_config[pos_ctl] < dt_now < dt_pm_end or dict_config[pos_ctl] == dt_pm_end:
                logger.trace(f"df_pos_ctl-[{file_name_pos_ctl_feather}] is latest")
                df_pos_ctl = feather.read_dataframe(source=file_name_pos_ctl_feather)
                close_sh = df_index_realtime.at[index, "最新价"]
                nane = df_index_realtime.at[index, "名称"]
                close_sh = int(close_sh) // 10 * 10
                str_pos_ctl = f"[{nane}] - [{close_sh}] --- [{df_pos_ctl.at[close_sh, 'sun_descending']}]"
                logger.trace(f"position_control-{pos_ctl} Break End")
                return str_pos_ctl
            else:
                logger.trace(f"df_pos_ctl-{pos_ctl}-[{file_name_pos_ctl_feather}] is not latest")
    logger.trace(f"Update df_pos_ctl-[{file_name_pos_ctl_feather}] Begin")
    df_sh_index = ak.stock_zh_index_daily(symbol=index)
    df_sh_index["date"] = pd.to_datetime(df_sh_index["date"])
    dt_begin = datetime.datetime(year=2015, month=9, day=1)
    df_sh_index.set_index(keys=["date"], inplace=True)
    df_sh_index = df_sh_index.loc[dt_begin:dt_date_trading]
    df_sh_index["close"] = df_sh_index["close"].round(0)
    df_sh_index["close"] = df_sh_index["close"].apply(func=lambda x: int(x) // 10 * 10)
    df_pos_ctl = pd.pivot_table(data=df_sh_index, index=["close"], aggfunc={"volume": np.sum, "close": len})
    df_pos_ctl.rename(columns={"close": "count"}, inplace=True)  # _descending
    df_pos_ctl.sort_index(ascending=False, inplace=True)
    descending_volume = 0
    total_volume = df_pos_ctl["volume"].sum()
    for tup_item in df_pos_ctl.itertuples():
        descending_volume += tup_item.volume
        df_pos_ctl.at[tup_item.Index, "sun_descending"] = int(descending_volume / total_volume * 10000) / 100
    feather.write_dataframe(df=df_pos_ctl, dest=file_name_pos_ctl_feather)
    logger.trace(f"feather df_pos_ctl-[{file_name_pos_ctl_feather}]")
    df_pos_ctl.to_csv(path_or_buf=file_name_pos_ctl_csv)
    logger.trace(f"save df_pos_ctl at csv-[{file_name_pos_ctl_csv}]")
    logger.trace(f"Update df_sh_index-[{file_name_pos_ctl_feather}] End")
    close_sh = df_index_realtime.at[index, "最新价"]
    nane = df_index_realtime.at[index, "名称"]
    close_sh = int(close_sh) // 10 * 10
    str_pos_ctl = f"[{nane}] - [{close_sh}] --- [{df_pos_ctl.at[close_sh, 'sun_descending']}]"
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config[pos_ctl] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - position_control --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    logger.trace(f"position_control-{pos_ctl} End")
    return str_pos_ctl
