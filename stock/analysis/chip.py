from __future__ import annotations
import datetime
import os
import sys
import pandas as pd
from loguru import logger
import feather
import pickle
import ashare
import analysis.golden
import analysis.limit
import analysis.update_data
import analysis.capital


def chip() -> pd.DataFrame:
    logger.trace("chip Begin")
    dt_now = datetime.datetime.now()
    dt_date_trading = ashare.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_check = os.path.join(path_main, "check")
    path_data = os.path.join(path_main, "data")
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    file_name_config = os.path.join(path_data, f"config.pkl")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_chip_csv = os.path.join(path_check, f"chip_{str_date_path}.csv")
    file_name_chip_feather = os.path.join(path_data, f"chip.ftr")
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
        if "chip" in dict_config:
            logger.trace("dict_config key-[chip_analysis] exist")
            logger.trace(f"the latest df_chip at {dict_config['chip']},The new at {dt_pm_end}")
            if dict_config["chip"] < dt_now < dt_pm_end or dt_pm_end == dict_config["chip"]:
                logger.trace(f"df_chip-[{file_name_chip_feather}] is latest")
                df_chip = feather.read_dataframe(source=file_name_chip_feather)
                logger.trace("chip Break End")
                return df_chip
    logger.trace("Update df_chip")

    analysis.update_data.update_data()
    df_golden = analysis.golden.golden_price()
    df_limit = analysis.limit.limit_count()
    df_cap = analysis.capital.capital()
    df_chip = pd.concat([df_cap, df_golden, df_limit], axis=1, join="outer")
    df_chip.dropna(subset='dt', inplace=True)
    df_chip["list_date"] = df_chip["list_date"].apply(func=lambda x: (dt_pm_end - x).days)
    df_chip.rename(columns={'list_date': "list_days"}, inplace=True)
    df_chip['turnover'] = df_chip['total_volume'] / (df_chip['circ_cap'] / 100)
    df_chip['turnover'] = df_chip['turnover'].round(2)
    df_chip['turnover'].fillna(value=0, inplace=True)
    df_chip.sort_values(by=["up_down", "price_ratio"], ascending=False, inplace=True)
    feather.write_dataframe(df=df_chip, dest=file_name_chip_feather)
    logger.trace(f"feather df_chip at [{file_name_chip_feather}]")
    df_chip.to_csv(path_or_buf=file_name_chip_csv)
    logger.trace(f"save df_chip at csv-[{file_name_chip_csv}]")
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config["chip"] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - chip --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    logger.trace("chip End")
    return df_chip


if __name__ == "__main__":
    logger.remove()
    logger.add(sink=sys.stderr, level="INFO")  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    chip()
