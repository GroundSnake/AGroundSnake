import akshare as ak
import pandas as pd
import numpy as np
import stockstats
import datetime
from sqlalchemy import create_engine
from loguru import logger
import time


def save_mini_to_db():
    engine = create_engine("mysql+pymysql://root:20270426@localhost:3306/akshareChinaAdata")
    logger.debug(f"\n{engine}")
    nowdate = datetime.datetime.now()
    newdate = nowdate.strftime("%Y%m%d")
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")
    process = 0  # 进度计数
    # usstockcode=pd.DataFrame()
    # usstockcode= ak.get_us_stock_name() #取所有美国的代码
    # print(usstockcode)
    stockcode = pd.read_csv("akshareChinaAstockcode.csv")
    stockcode.rename(columns={"代码": "code"}, inplace=True)
    stockcode["code"].astype("string")
    logger.debug(f"stockcode\n{stockcode}")
    unliststockdf = pd.DataFrame(columns=["code"])
    with open("akshareChinaAdatalastcode.txt", "r") as fi:
        f1 = fi.readline()
        logger.debug(f1)
    if f1 != "301039":
        lastcode = int(f1)  # 最后一个股票代码
        stockcode.set_index(["code"], inplace=True)
        stockcode = stockcode.loc[lastcode:]
        stockcode = stockcode.reset_index()
        logger.debug(f"断点同步：{str(len(stockcode))}")

    else:
        logger.debug(f"数据更新从头开始")

    for code in stockcode["code"]:
        if code < 10:
            scode = "00000" + str(code)
        if 10 < code < 100:
            scode = "0000" + str(code)
        if 100 < code < 1000:
            scode = "000" + str(code)
        if 1000 < code < 10000:
            scode = "00" + str(code)
        if 10000 < code < 100000:
            scode = "0" + str(code)
        if code > 100000:
            scode = str(code)
            logger.debug(f"读akshare接口{scode}")

        process = process + 1
        stockdailydf = ak.stock_zh_a_hist(symbol=scode, adjust="", start_date="20210301")
        logger.debug(f"\n{stockdailydf}")
        stockdailydf.rename(
            columns={"开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "日期": "date"},
            inplace=True,
        )

        stockdailydf = stockdailydf.reset_index()
        if stockdailydf.empty == True:
            continue
        d = str(stockdailydf.loc[len(stockdailydf) - 1, "date"])
        tempd = d.split("-", 2)[0]
        if int(tempd) < 2021:
            unliststockdf.loc[len(unliststockdf), "code"] = code
            continue

        logger.debug(f"\n{stockdailydf}")
        stock = stockstats.StockDataFrame.retype(stockdailydf)
        logger.debug(f"\n{stock}")

        stockdailydf[np.isinf(stockdailydf)] = np.nan  # 判断有inf替换nan！！！

        stockdailydf = stockdailydf.reset_index(drop=False)

        tablename = str(scode)
        has_table = engine.dialect.has_table(engine.connect(), tablename)
        if not has_table:
            # 这里要判断表不存在创建新表 create table if not exists
            # 获得行情数据 ts_code, dataframe清洗数据  str(format(process / len(stockcode), ".4%"))
            # stockdailydf.to_sql(name=str(scode), con=engine)  # df大表存入数据库akshareChinaA
            stockdailydf.to_excel(excel_writer=f"{str(scode)}.xlsx")
            logger.debug(f"新建{str(scode)}数据至{str(newdate)}===>进度 {str(format(process / len(stockcode), '.4%'))}")

        else:  # 表存在，判断是否需要更新
            SQLSelectExist = (
                    "SELECT * FROM akshareChinaAdata.`" + tablename + "`"
            )  # 取到已存在的表里的数据
            dfExist = pd.read_sql_query(SQLSelectExist, engine)
            if stockdailydf.loc[len(stockdailydf) - 1, "date"] in (
                    dfExist["date"].values
            ):  # 如果接口取到df表的最后一行在数据库的Date列里
                print(
                    str(scode)
                    + "已经是最新数据===>进度 "
                    + str(format(process / len(stockcode), ".4%"))
                )
                # stockdailydf.to_sql(name=code, con=engine, if_exists='replace')  # df大表存入数据库ACSQuant
            else:
                logger.debug(
                    f"{str(scode)}添加新数据{newdate}数据--最新日期{newdate}===>进度 {str(format(process / len(stockcode), '.4%'))}")
                stockdailydf.to_sql(
                    name=str(scode), con=engine, if_exists="replace"
                )  # df大表存入数据库ACSQuant
        logger.debug(f"{code}\n{stockdailydf}")
        with open("akshareChinaAdatalastcode.txt", "w+") as f:
            f.write(str(scode))
    unliststockdf.to_csv("akshareChinaAunlist.csv")
