import json
from loguru import logger
from analysis.const import path_config


def log_josn(item: str == None) -> bool:
    if item is None:
        logger.error("item is None")
        return False
    filename_access_log = path_config.joinpath("access_log.json")
    if filename_access_log.exists():
        with open(file=filename_access_log, mode="r") as f:
            json_access = json.load(fp=f)
    else:
        json_access = dict()
    try:
        json_access[item] += 1
    except KeyError:
        json_access[item] = 1
    with open(file=filename_access_log, mode="w") as f:
        json.dump(obj=json_access, fp=f, indent=1)
    return True
