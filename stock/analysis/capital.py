# modified at 2023/3/29 15:47
import os
import sys
import time
import datetime
import feather
import requests
import pandas as pd
from loguru import logger
from requests import RequestException
import analysis.base
from analysis.const import (
    path_data,
    str_date_path,
    filename_chip_shelve,
    dt_pm_end,
    list_all_stocks,
)


def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    dict_code_id = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["sz_id"] = 0
    dict_code_id.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12",
        "_": "1623833739532",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["bj_id"] = 0
    dict_code_id.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return dict_code_id


code_id_dict = code_id_map_em()


def stock_individual_info(code: str = "603777") -> pd.DataFrame:
    """
    东方财富-个股-股票信息
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fields=f120%2Cf121%2Cf122%2Cf174%2Cf175%2Cf59%2Cf163%2Cf43%2Cf57%2Cf58%2Cf169%2Cf170%2Cf46%2Cf44%2Cf51%2Cf168%2Cf47%2Cf164%2Cf116%2Cf60%2Cf45%2Cf52%2Cf50%2Cf48%2Cf167%2Cf117%2Cf71%2Cf161%2Cf49%2Cf530%2Cf135%2Cf136%2Cf137%2Cf138%2Cf139%2Cf141%2Cf142%2Cf144%2Cf145%2Cf147%2Cf148%2Cf140%2Cf143%2Cf146%2Cf149%2Cf55%2Cf62%2Cf162%2Cf92%2Cf173%2Cf104%2Cf105%2Cf84%2Cf85%2Cf183%2Cf184%2Cf185%2Cf186%2Cf187%2Cf188%2Cf189%2Cf190%2Cf191%2Cf192%2Cf107%2Cf111%2Cf86%2Cf177%2Cf78%2Cf110%2Cf262%2Cf263%2Cf264%2Cf267%2Cf268%2Cf255%2Cf256%2Cf257%2Cf258%2Cf127%2Cf199%2Cf128%2Cf198%2Cf259%2Cf260%2Cf261%2Cf171%2Cf277%2Cf278%2Cf279%2Cf288%2Cf152%2Cf250%2Cf251%2Cf252%2Cf253%2Cf254%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf265%2Cf266%2Cf289%2Cf290%2Cf286%2Cf285%2Cf292%2Cf293%2Cf294%2Cf295&secid=1.603777&_=1640157544804
    :param code: 股票代码
    :type code: str
    :return: 股票信息
    :rtype: pandas.DataFrame
    """
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fltt": "2",
        "invt": "2",
        "fields": "f57,f58,f84,f85,f189",
        "secid": f"{code_id_dict[code]}.{code}",
        "_": "1640157544804",
    }
    while True:
        try:
            r = requests.get(url, params=params)
        except RequestException as e:
            logger.error(repr(e))
            time.sleep(2)
        else:
            break
    data_json = r.json()
    temp_df = pd.DataFrame(data_json)
    temp_df.reset_index(inplace=True)
    del temp_df["rc"]
    del temp_df["rt"]
    del temp_df["svr"]
    del temp_df["lt"]
    del temp_df["full"]
    if "dlmkts" in temp_df.columns:
        del temp_df["dlmkts"]
    code_name_map = {
        "f57": "code",
        "f58": "name",
        "f84": "total_cap",
        "f85": "circ_cap",
        "f189": "list_date",
    }
    temp_df["index"] = temp_df["index"].map(code_name_map)
    temp_df = temp_df[pd.notna(temp_df["index"])]
    list_columns = temp_df["index"].to_list()
    list_data = temp_df["data"].to_list()
    df_return = pd.DataFrame(columns=list_columns)
    count = len(list_data)
    for i in range(count):
        if list_data[i] == "-":
            list_data[i] = None
    df_return.loc[0] = list_data
    if df_return.at[0, "list_date"] is not None:
        str_list_date = str(df_return.at[0, "list_date"])
        df_return.at[0, "list_date"] = datetime.datetime.strptime(
            str_list_date, "%Y%m%d"
        )
    df_return.set_index(keys="code", inplace=True)
    return df_return


def capital() -> bool:
    name: str = "df_cap"
    start_loop_time = time.perf_counter_ns()
    filename_cap_feather_temp = os.path.join(
        path_data, f"capital_temp_{str_date_path}.ftr"
    )
    list_cap_exist = list()
    df_cap = pd.DataFrame()
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"capital Break End")
        return True
    if os.path.exists(filename_cap_feather_temp):
        logger.trace(f"[{filename_cap_feather_temp}] exists")
        df_cap = feather.read_dataframe(source=filename_cap_feather_temp)
        if df_cap.empty:
            logger.trace(f"{name} cache is empty")
        else:
            logger.trace(f"{name} cache is not empty")
            list_cap_exist = df_cap.index.to_list()
    i = 0
    count = len(list_all_stocks)
    for symbol in list_all_stocks:
        i += 1
        str_msg = f"\rCapital Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        print(str_msg, end="")
        if symbol in list_cap_exist:
            continue
        if symbol in ["bj834770"]:  # 删除无法识别的股票, 可能是新股
            continue
        code = symbol[2:]
        df_cap_temp = pd.DataFrame()
        i_times = 0
        while i_times <= 2:
            try:
                df_cap_temp = stock_individual_info(code=code)
            except KeyError as e:
                logger.error(repr(e))
                print("--", repr(e))
                break
            except ConnectionError as e:
                logger.error(repr(e))
                print("--", repr(e))
            else:
                break
            if i_times >= 2:
                print(f"[{symbol}] Request Error")
                sys.exit()
            i_times += 1
        if not df_cap_temp.empty:
            if df_cap.empty:
                df_cap = pd.DataFrame(columns=df_cap_temp.columns)
            df_cap.loc[symbol] = df_cap_temp.loc[code]
        feather.write_dataframe(df=df_cap, dest=filename_cap_feather_temp)
    if i >= count:
        print("\n", end="")  # 格式处理
        analysis.base.write_obj_to_db(
            obj=df_cap, key=name, filename=filename_chip_shelve
        )
        analysis.base.set_version(key=name, dt=dt_pm_end)
        logger.trace(f"Update df_config-[{name}]")
        if os.path.exists(filename_cap_feather_temp):  # 删除临时文件
            os.remove(path=filename_cap_feather_temp)
            logger.trace(f"[{filename_cap_feather_temp}] remove")
    end_loop_time = time.perf_counter_ns()
    interval_time = (end_loop_time - start_loop_time) / 1000000000
    str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
    print(f"capital analysis takes [{str_gm}]")
    logger.trace(f"capital End")
    return True
