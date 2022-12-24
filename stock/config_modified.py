import datetime
import os
import pickle
import sys
from loguru import logger


def modified_dict_config():
    # dt_date_trading = ashare.latest_trading_day()
    dt_date_trading = datetime.date(year=2022, month=12, day=23)
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_config = os.path.join(path_data, f"config.pkl")
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
    else:
        logger.trace(f"Create config data-[dict_config]")
        dict_config = dict()
    # dict_config["update_data"] = dt_pm_end
    # dict_config['golden_price'] = dt_pm_end
    # dict_config["limit_count"] = dt_pm_end
    # dict_config["pos_ctl_sh"] = dt_pm_end
    # dict_config["pos_ctl_csi1000"] = dt_pm_end
    # del dict_config["golden_price"]
    # del dict_config["capital"]
    del dict_config["chip"]
    with open(file=file_name_config, mode="wb") as f:
        pickle.dump(obj=dict_config, file=f)
        logger.trace(f"dump config-[{file_name_config}]")
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - modified --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)


def check_dict_config():
    path_main = os.getcwd()
    str_config = ""
    path_data = os.path.join(path_main, "data")
    file_name_config = os.path.join(path_data, f"config.pkl")
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            logger.trace(f"load config from [{file_name_config}]")
            dict_config = pickle.load(file=f)
    else:
        logger.trace(f"Create config data-[dict_config]")
        dict_config = dict()
    for key in dict_config.keys():
        str_config += f"{key} : {dict_config[key].strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    print(str_config)


if __name__ == "__main__":
    logger.remove()
    # 创建一个Console输出handle,eg："TRACE","DEBUG","INFO"，"ERROR"
    logger.add(sink=sys.stderr, level="TRACE")
    # modified_dict_config()
    check_dict_config()
