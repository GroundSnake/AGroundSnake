import os
import datetime
import feather
import requests
import pickle
import pandas as pd
import ashare
from loguru import logger


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
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
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
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
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
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return code_id_dict


def stock_list_all() -> list:
    dk_stocks = code_id_map_em().keys()
    if dk_stocks:
        list_stocks_all = list(dk_stocks)
        list_stocks_all = [ashare.get_stock_type(x) + x for x in list_stocks_all]
    else:
        list_stocks_all = list()
    return list_stocks_all


def stock_individual_info(symbol: str = "sh603777") -> pd.DataFrame:
    """
    东方财富-个股-股票信息
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&fltt=2&invt=2&fields=f120%2Cf121%2Cf122%2Cf174%2Cf175%2Cf59%2Cf163%2Cf43%2Cf57%2Cf58%2Cf169%2Cf170%2Cf46%2Cf44%2Cf51%2Cf168%2Cf47%2Cf164%2Cf116%2Cf60%2Cf45%2Cf52%2Cf50%2Cf48%2Cf167%2Cf117%2Cf71%2Cf161%2Cf49%2Cf530%2Cf135%2Cf136%2Cf137%2Cf138%2Cf139%2Cf141%2Cf142%2Cf144%2Cf145%2Cf147%2Cf148%2Cf140%2Cf143%2Cf146%2Cf149%2Cf55%2Cf62%2Cf162%2Cf92%2Cf173%2Cf104%2Cf105%2Cf84%2Cf85%2Cf183%2Cf184%2Cf185%2Cf186%2Cf187%2Cf188%2Cf189%2Cf190%2Cf191%2Cf192%2Cf107%2Cf111%2Cf86%2Cf177%2Cf78%2Cf110%2Cf262%2Cf263%2Cf264%2Cf267%2Cf268%2Cf255%2Cf256%2Cf257%2Cf258%2Cf127%2Cf199%2Cf128%2Cf198%2Cf259%2Cf260%2Cf261%2Cf171%2Cf277%2Cf278%2Cf279%2Cf288%2Cf152%2Cf250%2Cf251%2Cf252%2Cf253%2Cf254%2Cf269%2Cf270%2Cf271%2Cf272%2Cf273%2Cf274%2Cf275%2Cf276%2Cf265%2Cf266%2Cf289%2Cf290%2Cf286%2Cf285%2Cf292%2Cf293%2Cf294%2Cf295&secid=1.603777&_=1640157544804
    :param symbol: 股票代码
    :type symbol: str
    :return: 股票信息
    :rtype: pandas.DataFrame
    """
    symbol = symbol[2:8]
    code_id_dict = code_id_map_em()
    url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fltt": "2",
        "invt": "2",
        "fields": "f57,f58,f84,f85,f189",
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "_": "1640157544804",
    }
    r = requests.get(url, params=params)
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
        "f57": "symbol",
        "f58": "name",
        "f84": "total_cap",
        "f85": "circ_cap",
        "f189": "list_date",
    }
    temp_df["index"] = temp_df["index"].map(code_name_map)
    temp_df = temp_df[pd.notna(temp_df["index"])]
    list_columns = temp_df['index'].to_list()
    list_data = temp_df['data'].to_list()
    df_return = pd.DataFrame(columns=list_columns)
    count = len(list_data)
    for i in range(count):
        if list_data[i] == "-":
            list_data[i] = None
    df_return.loc[0] = list_data
    df_return.at[0, "symbol"] = ashare.get_stock_type(df_return.at[0, "symbol"]) + df_return.at[0, "symbol"]
    if df_return.at[0, "list_date"] is not None:
        str_list_date = str(df_return.at[0, "list_date"])
        df_return.at[0, "list_date"] = datetime.datetime.strptime(str_list_date, "%Y%m%d")
    df_return.set_index(keys="symbol", drop=True, inplace=True)
    return df_return


def capital() -> pd.DataFrame:
    dt_date_trading = ashare.latest_trading_day()
    time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
    dt_pm_end = datetime.datetime.combine(dt_date_trading, time_pm_end)
    path_main = os.getcwd()
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    file_name_config_txt = os.path.join(path_check, f"config.txt")
    file_name_cap_feather = os.path.join(path_data, f"capital.ftr")
    file_name_cap_feather_temp = os.path.join(path_data, f"capital_temp.ftr")
    file_name_config = os.path.join(path_data, f"config.pkl")
    list_stocks = stock_list_all()
    list_stocks.sort()
    list_cap_exist = list()
    df_cap = pd.DataFrame()
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        if "capital" in dict_config:
            dt_delta = dt_pm_end - dict_config["capital"]
            days = dt_delta.days
            if days < 365:
                df_cap = feather.read_dataframe(source=file_name_cap_feather)
                logger.trace(f"capital Break Begin")
                return df_cap
    if os.path.exists(file_name_cap_feather_temp):
        logger.trace(f'{file_name_cap_feather_temp}')
        df_cap = feather.read_dataframe(source=file_name_cap_feather_temp)
        if df_cap.empty:
            logger.trace("df_cap cache is empty")
        else:
            logger.trace("df_cap cache is not empty")
            list_cap_exist = df_cap.index.to_list()
    i = 0
    count = len(list_stocks)
    for symbol in list_stocks:
        i += 1
        str_msg = f"\rCapital Update: [{i:4d}/{count:4d}] -- [{symbol}]"
        print(str_msg, end="")
        if symbol in list_cap_exist:
            continue
        df_cap_temp = stock_individual_info(symbol=symbol)
        if df_cap.empty:
            df_cap = df_cap_temp
        else:
            df_cap.loc[symbol] = df_cap_temp.loc[symbol]
        feather.write_dataframe(df=df_cap, dest=file_name_cap_feather_temp)
    if i >= count:
        if os.path.exists(file_name_cap_feather_temp):
            os.remove(path=file_name_cap_feather_temp)
            logger.trace(f"[{file_name_cap_feather_temp}] remove")
    df_cap.dropna(inplace=True)
    feather.write_dataframe(df=df_cap, dest=file_name_cap_feather)
    if os.path.exists(file_name_config):
        with open(file=file_name_config, mode="rb") as f:
            dict_config = pickle.load(file=f)
        dict_config["capital"] = dt_pm_end
        with open(file=file_name_config, mode="wb") as f:
            pickle.dump(obj=dict_config, file=f)
            logger.trace("dump")
    dt_temp = datetime.datetime.now()
    str_check_dict_config = f"[{dt_temp}] - capital --- " + str(dict_config) + "\n"
    with open(file=file_name_config_txt, mode="a") as f:
        f.write(str_check_dict_config)
    return df_cap


if __name__ == "__main__":
    pass

"""
circulation_capital circ_cap
total_capital total_cap
"""
