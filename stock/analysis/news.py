# modified at 2023/09/09 08:23
import os
import time
import requests
import hashlib
import datetime
import re
import pandas as pd
from console import fg
import analysis.base
from analysis.const import (
    path_check,
    str_trading_path,
)


def news_cls() -> pd.DataFrame:
    """
    财联社-电报 https://www.cls.cn/telegraph
    :return: 财联社-电报
    :rtype: pandas.DataFrame
    """
    current_time = int(time.time())
    url = "https://www.cls.cn/nodeapi/telegraphList"
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
    }
    text = requests.get(url, params=params).url.split("?")[1]
    if not isinstance(text, bytes):
        text = bytes(text, "utf-8")
    sha1 = hashlib.sha1(text).hexdigest()
    code = hashlib.md5(sha1.encode()).hexdigest()
    params = {
        "app": "CailianpressWeb",
        "category": "",
        "lastTime": current_time,
        "last_time": current_time,
        "os": "web",
        "refresh_type": "1",
        "rn": "2000",
        "sv": "7.7.5",
        "sign": code,
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=utf-8",
        "Host": "www.cls.cn",
        "Pragma": "no-cache",
        "Referer": "https://www.cls.cn/telegraph",
        "sec-ch-ua": '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    }
    data = requests.get(url, headers=headers, params=params).json()
    df_news = pd.DataFrame(data["data"]["roll_data"])
    df_news = df_news[["id", "ctime", "level", "content"]]
    df_news["ctime"] = pd.to_datetime(
        arg=df_news["ctime"], unit="s"
    ) + datetime.timedelta(hours=8)
    df_news.set_index(keys=["id"], inplace=True)
    df_news.sort_index(ascending=True, inplace=False)
    return df_news


def update_news(start_id: int = 0, hours: int = 8) -> int:
    name: str = f"df_news"
    try:
        df_news = news_cls()
    except requests.exceptions.SSLError:
        return start_id
    dt_start = datetime.datetime.now() - datetime.timedelta(hours=hours)
    df_news = df_news[df_news["ctime"] > dt_start]
    df_news = df_news[df_news.index > start_id]
    if df_news.empty:
        end_id = start_id
    else:
        end_id = int(df_news.index.max())
        analysis.base.feather_to_file(
            df=df_news,
            key=name,
        )
    filename_news_csv = os.path.join(path_check, f"news_{str_trading_path()}.csv")
    df_news.to_csv(path_or_buf=filename_news_csv)
    return end_id


def get_news(df_news: pd.DataFrame, stock: str = "开心汽车") -> str | None:
    text_news = None
    for index in df_news.index:
        if stock in df_news.at[index, "content"]:
            if text_news is None:
                text_news = (
                    f"[{df_news.at[index, 'ctime']}]--{df_news.at[index, 'content']}"
                )
            else:
                text_news += (
                    f"[{df_news.at[index, 'ctime']}]--{df_news.at[index, 'content']}"
                )
    return text_news


def get_stock_news(df_news: pd.DataFrame, stock: str = "开心汽车") -> str:
    stock_keyword = [
        r".*?免.*?职务?.*?】",
        r".*?因?.*?逮捕.*?】",
    ]
    text_stock = get_news(df_news=df_news, stock=stock)
    text_return = ""
    if text_stock is not None:
        for pattern in stock_keyword:
            pattern = f"【{stock}：" + pattern
            pattern_msg = re.compile(pattern)
            match_msg = pattern_msg.search(string=text_stock)
            if match_msg:
                if text_return is None:
                    text_return = match_msg.group()
                else:
                    text_return += match_msg.group()
    return text_return


def scan_all_stock_news(df_news: pd.DataFrame) -> str:
    df_cap = analysis.base.feather_from_file(key="df_cap")
    df_trader = analysis.base.feather_from_file(
        key="df_trader",
    )
    stock_keyword = r".*?】"
    i = 0
    for symbol in df_cap.index:
        i += 1
        pattern = f".*?--【{df_cap.at[symbol, 'name']}" + stock_keyword
        pattern_msg = re.compile(pattern)
        text_stock = get_news(df_news=df_news, stock=df_cap.at[symbol, "name"])
        if text_stock is None:
            continue
        match_msg = pattern_msg.search(string=text_stock)
        if match_msg:
            if symbol in df_trader.index:
                str_stock_news = fg.red(match_msg.group())
            else:
                str_stock_news = match_msg.group()
            print(str_stock_news)
    return ""
