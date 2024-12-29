import sys
import json
import datetime
from loguru import logger
from analysis.const import path_json, filename_log, log_level

logger.remove()
logger.add(sink=sys.stderr, level=log_level)  # "INFO", "DEBUG"
logger.add(sink=filename_log, level="TRACE", encoding="utf-8")


def log_json(item: str = None) -> bool:
    if item is None:
        logger.error("item is None")
        return False
    filename_access_log = path_json.joinpath("access_log.json")
    if filename_access_log.exists():
        with open(file=filename_access_log, mode="r") as f:
            json_access = json.load(fp=f)
    else:
        json_access = dict()
        json_access["create_datetime"] = datetime.datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )
    try:
        json_access[f"{item}_datetime"] = datetime.datetime.now().strftime(
            "%Y%m%d_%H%M%S_%f"
        )
        json_access[item] += 1
    except KeyError:
        json_access[item] = 1
    with open(file=filename_access_log, mode="w") as f:
        json.dump(obj=json_access, fp=f, indent=1)
    return True
