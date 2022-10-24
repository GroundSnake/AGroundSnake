import sys
import numpy
import datetime
import time
import pandas as pd
from ashare import realtime_quotations
from loguru import logger
from tqdm import tqdm

__version__ = "1.1.4"  # 2022-10-24

# 设定全局参数-----start--------------------------
dt_date = datetime.datetime.now().date()
time_am_start = datetime.time(hour=8, minute=25, second=30, microsecond=0)
time_am_end = datetime.time(hour=11, minute=30, second=30, microsecond=0)
time_pm_start = datetime.time(hour=12, minute=59, second=30, microsecond=0)
time_pm_end = datetime.time(hour=15, minute=1, second=0, microsecond=0)
dt_am_start = datetime.datetime.combine(dt_date, time_am_start)
dt_am_end = datetime.datetime.combine(dt_date, time_am_end)
dt_pm_start = datetime.datetime.combine(dt_date, time_pm_start)
dt_pm_end = datetime.datetime.combine(dt_date, time_pm_end)
scan_interval = 60  # 标准设置为30(秒)，扫描间隔时间
BeepChange = 1.0  # 快速变动1.0%提醒
frq = 0
increase = 5.0
decrease = -5.0
str_stock_path = "stock.xlsx"  # 标准设置为"stock.xlsx"
file_name_log = "program_log.log"
file_name_change_log = "StockChangeLog.txt"
file_name_suggest_log = "StockSuggest.txt"
# str_out_stock_path = "writer.xlsx" # 调试程序使用的输出文件
str_out_stock_path = str_stock_path
list_stock_code_col = [
    "name",
    "last_price",
    "last_datetime",
    "price",
    "datetime",
    "pct_chg",
]
list_rp_code_col = [
    "name",
    "reminder_price",
    "quantity",
    "decrease",
    "buy_flag",
    "increase",
    "sell_flay",
    "price",
    "pct_chg",
    "datetime",
    "remark",
]
num_type = (int, float, bool, numpy.int64, numpy.float64, numpy.bool_)
str_key = "code"  # stock.xlsx唯一不能少的股票代码表头
global dfExcel_change, dfExcel_rp, df_temp
# 设定全局参数-----end--------------------------

logger.remove()  # 移除import创建的所有handle
logger.add(sink=sys.stderr, level="ERROR")  # 创建一个Console输出handle,eg："INFO"，"ERROR","DEBUG"
logger.add(sink=file_name_log, level="DEBUG")


def sleep_interval(scan_time):
    i = 0
    while i < scan_time:
        i = i + 1
        str_datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\rWaiting: {i:2d}/{scan_time:2d} seconds -- {str_datetime_now}", end="")  # 进度条
        time.sleep(1)
    print("\n", end="")
    return True


# warn_suggest()------start-------------------
def warn_suggest():
    # 声明全局变量------start-----------------
    global dfExcel_change, dfExcel_rp, df_temp
    # 声明全局变量--------end-----------------
    logger.info("[Warn and Suggest]Update start......")
    # 局部变量初始化------start-----------------
    log_up = ""
    log_down = ""
    log_sell = ""
    log_buy = ""
    dt_today = datetime.datetime.now()
    # 局部变量初始化-------end-----------------
    # [dfExcel_change],[dfExcel_rp] 初始化-------start-----------------
    dfExcel_change = pd.read_excel(
        io=str_stock_path, sheet_name="stock_code", index_col="ID"
    )
    dfExcel_rp = pd.read_excel(
        io=str_stock_path, sheet_name="right_price", index_col="ID"
    )
    if str_key not in dfExcel_change.columns:
        logger.info("[code] key not found")
        return False
    if str_key not in dfExcel_rp.columns:
        logger.info("[code] key not found")
        return False
    for i, v in dfExcel_change["code"].items():
        dfExcel_change.loc[i, "code"] = v.lower()
        pass
    for i, v in dfExcel_rp["code"].items():
        dfExcel_rp.loc[i, "code"] = v.lower()
        pass
    dfExcel_change = pd.concat(
        [dfExcel_change, dfExcel_rp[["code"]]], axis=0, join="outer"
    )
    dfExcel_change.drop_duplicates(
        subset="code", keep="first", inplace=True, ignore_index=True
    )
    dfExcel_change.rename_axis(index="ID", inplace=True)
    dfExcel_rp.drop_duplicates(
        subset="code", keep="first", inplace=True, ignore_index=True
    )
    dfExcel_rp.rename_axis(index="ID", inplace=True)
    for col in list_stock_code_col:
        if col not in dfExcel_change.columns:
            dfExcel_change[col] = None
            pass
        pass
    for col in list_rp_code_col:
        if col not in dfExcel_rp.columns:
            dfExcel_rp[col] = None
    dfExcel_change["name"].fillna("NAME", inplace=True)
    dfExcel_change["last_price"].fillna(0.0, inplace=True)
    dfExcel_change["last_datetime"].fillna(dt_today, inplace=True)
    dfExcel_change["price"].fillna(0.0, inplace=True)
    dfExcel_change["datetime"].fillna(dt_today, inplace=True)
    dfExcel_change["pct_chg"].fillna(0.0, inplace=True)
    dfExcel_rp["name"].fillna("NAME", inplace=True)
    dfExcel_rp["reminder_price"].fillna(0.0, inplace=True)
    dfExcel_rp["quantity"].fillna(0.0, inplace=True)
    dfExcel_rp["decrease"].fillna(decrease, inplace=True)
    dfExcel_rp["buy_flag"].fillna(True, inplace=True)
    dfExcel_rp["increase"].fillna(increase, inplace=True)
    dfExcel_rp["sell_flay"].fillna(True, inplace=True)
    dfExcel_rp["price"].fillna(0.0, inplace=True)
    dfExcel_rp["pct_chg"].fillna(0.0, inplace=True)
    dfExcel_rp["datetime"].fillna(dt_today, inplace=True)
    # [dfExcel_change],[dfExcel_rp] 初始化-------end-----------------
    # [dfExcel_change],[dfExcel_rp] BUG处理-------start-----------------
    for index, data in dfExcel_rp.iterrows():
        dfExcel_rp.loc[index, "buy_flag"] = bool(data["buy_flag"])
        dfExcel_rp.loc[index, "sell_flay"] = bool(data["sell_flay"])
        if not isinstance(dfExcel_rp.loc[index, "reminder_price"], num_type):
            dfExcel_rp.loc[index, "reminder_price"] = 0.0
        if not isinstance(dfExcel_rp.loc[index, "quantity"], num_type):
            dfExcel_rp.loc[index, "quantity"] = 0.0
        if not isinstance(dfExcel_rp.loc[index, "decrease"], num_type):
            dfExcel_rp.loc[index, "decrease"] = decrease
            logger.debug(type(dfExcel_rp.loc[index, "decrease"]))
        if not isinstance(dfExcel_rp.loc[index, "buy_flag"], num_type):
            dfExcel_rp.loc[index, "buy_flag"] = True
        if not isinstance(dfExcel_rp.loc[index, "increase"], num_type):
            dfExcel_rp.loc[index, "increase"] = increase
        if not isinstance(dfExcel_rp.loc[index, "sell_flay"], num_type):
            dfExcel_rp.loc[index, "sell_flay"] = True
        if not isinstance(dfExcel_rp.loc[index, "price"], num_type):
            dfExcel_rp.loc[index, "price"] = 0.0
        if not isinstance(dfExcel_rp.loc[index, "pct_chg"], num_type):
            dfExcel_rp.loc[index, "pct_chg"] = 0.0
        if not data["sell_flay"] and not data["buy_flag"]:
            dfExcel_rp.drop(labels=index, axis=0, inplace=True)  # 删除flag都是False的记录
            logger.info(f"{data['code']} -- {data['name']} -- Drop")
    for index, data in dfExcel_change.iterrows():
        if not isinstance(dfExcel_change.at[index, "last_price"], num_type):
            dfExcel_change.at[index, "last_price"] = 0.0
        if not isinstance(dfExcel_change.at[index, "price"], num_type):
            dfExcel_change.at[index, "price"] = 0.0
        if not isinstance(dfExcel_change.at[index, "pct_chg"], num_type):
            dfExcel_change.at[index, "pct_chg"] = 0.0
    # [dfExcel_change],[dfExcel_rp] BUG处理-------end-----------------
    # [df_temp] update------Start-----------------
    list_stock = dfExcel_change["code"].to_list()
    df_temp = realtime_quotations(stock_codes=list_stock)  # 调用实时数据接口
    # [df_temp] update------End-----------------

    pbar = tqdm(iterable=dfExcel_change.iterrows())
    for index, data in pbar:
        index_temp = dfExcel_change.loc[index, "code"]
        if index_temp in df_temp.index:
            dfExcel_change.loc[index, "name"] = df_temp.loc[index_temp, "name"]
            if dfExcel_change.loc[index, "last_price"] == 0.0:
                dfExcel_change.loc[index, "last_price"] = df_temp.loc[index_temp, "close"]
                dfExcel_change.loc[index, "last_datetime"] = "昨天的价"
                dfExcel_change.loc[index, "price"] = df_temp.loc[index_temp, "close"]
                dfExcel_change.loc[index, "datetime"] = df_temp.loc[index_temp, "datetime"]
            else:
                dfExcel_change.loc[index, "last_price"] = dfExcel_change.loc[index, "price"]
                dfExcel_change.loc[index, "last_datetime"] = dfExcel_change.loc[index, "datetime"]
                dfExcel_change.loc[index, "price"] = df_temp.loc[index_temp, "close"]
                dfExcel_change.loc[index, "datetime"] = df_temp.loc[index_temp, "datetime"]

            if dfExcel_change.loc[index, "price"] != 0 and dfExcel_change.loc[index, "last_price"] != 0:
                pct_chg = (dfExcel_change.loc[index, "price"] / dfExcel_change.loc[index, "last_price"] - 1) * 100
                dfExcel_change.loc[index, "pct_chg"] = round(pct_chg, 2)
            else:
                dfExcel_change.loc[index, "pct_chg"] = 0
        else:
            logger.error(f"[dfExcel_change] {index_temp} not in df_temp")
        pbar.set_description(f"[Change] Stock {data['code']} Update")

    pbar = tqdm(iterable=dfExcel_rp.iterrows())
    for index, data in pbar:
        index_temp = dfExcel_rp.loc[index, "code"]
        if index_temp in df_temp.index:
            dfExcel_rp.loc[index, "name"] = df_temp.loc[index_temp, "name"]
            if dfExcel_rp.loc[index, "reminder_price"] == 0.0:
                dfExcel_rp.loc[index, "reminder_price"] = df_temp.loc[
                    index_temp, "close"
                ]
            dfExcel_rp.loc[index, "price"] = df_temp.loc[index_temp, "close"]
            dfExcel_rp.loc[index, "datetime"] = df_temp.loc[index_temp, "datetime"]
            if (
                dfExcel_rp.loc[index, "reminder_price"] != 0
                and dfExcel_rp.loc[index, "price"] != 0.0
            ):
                pct_chg = (
                    dfExcel_rp.loc[index, "price"]
                    / dfExcel_rp.loc[index, "reminder_price"]
                    - 1
                ) * 100
                dfExcel_rp.loc[index, "pct_chg"] = round(pct_chg, 2)
            else:
                dfExcel_rp.loc[index, "pct_chg"] = 0.0
        else:
            logger.debug(f"[dfExcel_rp] {index_temp} not in df_temp")
        if data["quantity"] > 0:
            dfExcel_rp.at[index, "sell_flay"] = True
        else:
            dfExcel_rp.at[index, "sell_flay"] = False
        pbar.set_description(f"[Right Price] Stock {data['code']} Update")

    # 按涨幅重新排序dfExcel_change-----start--------
    dfExcel_change.sort_values("pct_chg", ascending=False, inplace=True)
    dfExcel_change.reset_index(drop=True, inplace=True)
    dfExcel_change.rename_axis(index="ID", inplace=True)
    # 按涨幅重新排序dfExcel_change-----end--------
    # 按涨幅重新排序dfExcel_RP-----start--------
    dfExcel_rp.sort_values(by="pct_chg", ascending=False, inplace=True)
    dfExcel_rp.reset_index(drop=True, inplace=True)
    dfExcel_rp.rename_axis(index="ID", inplace=True)
    # 按涨幅重新排序dfExcel_RP-----end--------

    # dfExcel_rp.at[45, 'pct_chg'] = 20  # 异动测试点
    # dfExcel_rp.at[10, 'pct_chg'] = -20  # 异动测试点

    # [dfExcel_rp]Update-----start--------
    for index, data_rp in dfExcel_rp.iterrows():
        if data_rp["pct_chg"] > data_rp["increase"] and data_rp["sell_flay"]:
            str_message = (
                "\n<T0 Sell>-["
                + data_rp["datetime"].strftime("%m-%d %H:%M:%S")
                + "]---["
                + data_rp["name"]
                + "_"
                + data_rp["code"]
                + "]：↑<"
                + str(data_rp["price"])
                + ">---["
                + str(data_rp["reminder_price"])
                + "]---↑<"
                + str(data_rp["pct_chg"])
                + "%>--("
                + str(data_rp["quantity"])
                + ")"
            )
            if pd.notnull(data_rp["remark"]):
                str_message += "--" + data_rp["remark"] + "\n"
            else:
                str_message += "\n"
            log_sell = log_sell + str_message
            pass
        elif data_rp["pct_chg"] <= data_rp["decrease"] and data_rp["buy_flag"]:
            str_message = (
                "\n<T0 Buy>-["
                + data_rp["datetime"].strftime("%m-%d %H:%M:%S")
                + "]---["
                + data_rp["name"]
                + "_"
                + data_rp["code"]
                + "]：↓<"
                + str(data_rp["price"])
                + ">---["
                + str(data_rp["reminder_price"])
                + "]---↓<"
                + str(data_rp["pct_chg"])
                + "%>--("
                + str(data_rp["quantity"])
                + ")"
            )
            if pd.notnull(data_rp["remark"]):
                str_message += "--" + data_rp["remark"] + "\n"
            else:
                str_message += "\n"
            log_buy = log_buy + str_message
            pass
        else:
            pass
        pass
    # [dfExcel_rp]Update------End-------------------

    # dfExcel_change.loc[5, 'pct_chg'] = 3  # 异动测试点
    # dfExcel_change.loc[6, 'pct_chg'] = -3  # 异动测试点

    # [dfExcel_change]Update-----start - -------
    for index, data_change in dfExcel_change.iterrows():
        if data_change["pct_chg"] > BeepChange:
            log_temp = (
                "\n["
                + data_change["datetime"].strftime("%m-%d %H:%M:%S")
                + "]<↑>"
                + "<"
                + data_change["name"]
                + "_"
                + data_change["code"]
                + ">价格：<"
                + str(data_change["price"])
                + ">--["
                + str(data_change["last_price"])
                + "]-["
                + data_change["last_datetime"].strftime("%m-%d %H:%M:%S")
                + "] -- Change："
                + str(data_change["pct_chg"])
                + "%"
                + "↑\n"
            )
            log_up = log_up + log_temp
            with open(file=file_name_change_log, mode="w") as filelog:
                filelog.write(log_temp)
                pass
            pass
        elif data_change["pct_chg"] < -BeepChange:
            log_temp = (
                "\n["
                + data_change["datetime"].strftime("%m-%d %H:%M:%S")
                + "]<↓>"
                + "<"
                + data_change["name"]
                + "_"
                + data_change["code"]
                + ">价格：<"
                + str(data_change["price"])
                + ">--["
                + str(data_change["last_price"])
                + "]-["
                + data_change["last_datetime"].strftime("%m-%d %H:%M:%S")
                + "] -- Change："
                + str(data_change["pct_chg"])
                + "%"
                + "↓\n"
            )
            log_down = log_down + log_temp
            with open(file=file_name_change_log, mode="w") as filelog:
                filelog.write(log_temp)
                pass
            pass
        else:
            pass
    # [dfExcel_change]Update------End-------------------
    # [dfExcel_rp]print-----start--------
    log_file = ""
    if log_sell != "":
        print(
            "\a====<T0 Suggest selling>==========================================================="
        )
        print(log_sell)
        log_file = "+++++++++++\n" + log_sell
    if log_buy != "":
        print(
            "====<T0 Suggest buying>============================================================"
        )
        print(log_buy)
        log_file = log_file + "-------------\n" + log_buy
    if log_file != "":
        with open(file=file_name_suggest_log, mode="w") as filelog:
            filelog.write(log_file)
    if log_sell != "" or log_buy != "":
        print(
            "====<T0 END>======================================================================="
        )
        # winsound.Beep(frequency=900, duration=1200)  # 调用主板声音
    # [dfExcel_rp]print-----end--------
    # [dfExcel_change]print------Start-------------------
    if log_up != "":
        print(
            "****<Change Up>************************************************************************************"
        )
        print(log_up)
    if log_down != "":
        print(
            "****<Change Down>***********************************************************************************"
        )
        print(log_down)
    if log_up != "" or log_down != "":
        print(
            "****<Change END>***********************************************************************************"
        )
        # winsound.Beep(frequency=1800, duration=600)  # 调用主板声音
    # [dfExcel_change]print------End-------------------
    writer = pd.ExcelWriter(path=str_out_stock_path, mode="w")
    dfExcel_rp.to_excel(excel_writer=writer, sheet_name="right_price")
    dfExcel_change.to_excel(excel_writer=writer, sheet_name="stock_code")
    df_temp.to_excel(excel_writer=writer, sheet_name="df_temp")
    writer.close()
    logger.info(f"[{str_stock_path}] save! ---[OK]---<Save>")
    logger.info(f"[Warn and Suggest]Update End......")
    return True
# warn_suggest()------end-------------------


if __name__ == "__main__":
    logger.info(f"Robot working......")
    while True:
        dt_now = datetime.datetime.now()
        if (dt_am_start < dt_now < dt_am_end) or (dt_pm_start < dt_now < dt_pm_end):
            logger.info(f"Start of this cycle.---[{frq:3d}]---<Start>")
            start_loop_time = time.perf_counter_ns()
            logger.debug(f"start_loop_time = {start_loop_time}")
            # 全局变量处理区---------Start---------------------

            flag_change = warn_suggest()
            if flag_change:
                pass
            else:
                logger.error(f"[warn_suggest]Error")

            # 全局变量处理区---------End---------------------
            end_loop_time = time.perf_counter_ns()
            interval_time = end_loop_time - start_loop_time
            logger.debug(f"end_loop_time = {end_loop_time}")
            logger.info(f"This cycle takes {interval_time} ns.---[{frq:2d}]---<End>")
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            delay = int((dt_pm_start - dt_now).total_seconds())
            logger.info(f"The exchange is closed at noon and will open in {delay} seconds")
            sleep_interval(delay)
            # -----当前时间与当日指定时间的间隔时间计算-----
        else:
            logger.info(f"The exchange has closed.")
            logger.info(f"Robot off duty...")
            sys.exit()
        frq += 1
        logger.info(f"The [No{frq:3d}] cycle will start in {scan_interval:2d} seconds.")
        sleep_interval(scan_interval)
