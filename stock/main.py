from __future__ import annotations
import os
import random
import sys
import datetime
import time
import feather
import pandas as pd
from ashare import realtime_quotations, latest_trading_day
from loguru import logger
from console import fg
import analysis.position
import analysis.chip


__version__ = "2.1.1"  # 2022-12-21 14:39

logger_console_level = "TRACE"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}


def sleep_interval(scan_time: int):
    scan_time -= 1
    while scan_time >= 0:
        str_sleep_gm = time.strftime("%H:%M:%S", time.gmtime(scan_time))
        dt_sleep = datetime.datetime.now()
        str_sleep_msg = f"\r{dt_sleep} - Waiting: {str_sleep_gm}"
        str_sleep_msg = fg.cyan(str_sleep_msg)
        print(str_sleep_msg, end="")  # 进度条
        time.sleep(1)
        scan_time -= 1
    print("\n", end="")
    return True


if __name__ == "__main__":
    """ "init Begin"""
    rise = 5.2631578
    fall = -5
    frq = 0
    scan_interval = 20
    dt_date_trading = latest_trading_day()
    str_date_path = dt_date_trading.strftime("%Y_%m_%d")
    path_main = os.getcwd()
    path_history = os.path.join(path_main, "history")
    path_data = os.path.join(path_main, "data")
    path_check = os.path.join(path_main, "check")
    if not os.path.exists(path_history):
        os.mkdir(path_history)
    if not os.path.exists(path_data):
        os.mkdir(path_data)
    if not os.path.exists(path_check):
        os.mkdir(path_check)
    file_name_input = os.path.join(path_main, f"input.xlsx")
    file_name_trader = os.path.join(path_main, f"trader.xlsx")
    file_name_log = os.path.join(path_data, "program_log.log")
    file_name_data_pickle = os.path.join(path_data, f"data.pkl")
    file_name_signal = os.path.join(path_check, f"signal_{str_date_path}.xlsx")
    file_name_data_csv = os.path.join(path_check, f"position_{str_date_path}.csv")
    file_name_chip_feather = os.path.join(path_data, f"chip.ftr")
    logger.remove()
    logger.add(sink=sys.stderr, level=logger_console_level)
    # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    logger.add(sink=file_name_log, level="TRACE")
    logger.trace(f"initialization Begin")
    #  设定交易时间 Begin
    dt_date = datetime.datetime.now().date()
    time_program_start = datetime.time(hour=1, minute=0, second=0, microsecond=0)
    time_am_start = datetime.time(hour=9, minute=30, second=0, microsecond=0)
    time_am_end = datetime.time(hour=11, minute=30, second=0, microsecond=0)
    time_pm_start = datetime.time(hour=13, minute=0, second=0, microsecond=0)
    time_pm_end = datetime.time(hour=15, minute=0, second=scan_interval, microsecond=0)
    time_program_end = datetime.time(hour=23, minute=0, second=0, microsecond=0)
    dt_program_start = datetime.datetime.combine(dt_date, time_program_start)
    dt_am_start = datetime.datetime.combine(dt_date, time_am_start)
    dt_am_end = datetime.datetime.combine(dt_date, time_am_end)
    dt_pm_start = datetime.datetime.combine(dt_date, time_pm_start)
    dt_pm_end = datetime.datetime.combine(dt_date, time_pm_end)
    dt_program_end = datetime.datetime.combine(dt_date, time_program_end)
    #  设定交易时间 End
    # 加载df_data Begin
    logger.trace("Create df_data Begin")
    if os.path.exists(file_name_data_pickle):
        logger.trace(f"loading df_data from [{file_name_data_pickle}]")
        df_data = pd.read_pickle(filepath_or_buffer=file_name_data_pickle)
    else:
        logger.trace(f"Create a new df_data")
        list_data_columns = [
            "name",
            "recent_price",
            "position",
            "now_price",
            "pct_chg",
            "rise",
            "fall",
            "stock_index",
            "remark",
        ]
        list_symbol = ["sh600519", "sz300750"]
        df_data = pd.DataFrame(index=list_symbol, columns=list_data_columns)
        df_data.index.rename(name="code", inplace=True)
    df_data["recent_price"].fillna(0, inplace=True)
    df_data["position"].fillna(0, inplace=True)
    df_data["rise"].fillna(rise, inplace=True)
    df_data["fall"].fillna(fall, inplace=True)
    logger.trace("Create df_data End")
    # 加载df_data End
    # 加载df_chip Begin
    logger.trace("Create df_chip Begin")
    if os.path.exists(file_name_chip_feather):
        df_chip = feather.read_dataframe(source=file_name_chip_feather)
    else:
        df_chip = pd.DataFrame()
    if df_chip.empty:
        dt_chip_max = None
        print(f"No df_chip")
    else:
        dt_chip_max = df_chip["dt"].max()
        str_chip_msg = f"The latest chip analysis is on [{dt_chip_max}]"
        str_chip_msg = fg.red(str_chip_msg)
        print(str_chip_msg)
    logger.trace("Create df_chip End")
    # 加载df_chip End
    # 用df_chip初始化df_data----Begin
    logger.trace("initialization df_index")
    list_data = df_data.index.to_list()
    for code in list_data:
        if code in df_chip.index:
            price_ratio = round(df_chip.at[code, "price_ratio"], 2)
            G_price = df_chip.at[code, "G_price"]
            up_times = int(df_chip.at[code, "up_times"])
            down_times = int(df_chip.at[code, "down_times"])
            up_down = int(df_chip.at[code, "up_down"])
            df_data.at[code, "stock_index"] = (
                f"({price_ratio:5.2f}% / "
                f"{G_price:5.2f}$):"
                f"[{up_times:2.0f}/"
                f"{down_times:2.0f}/"
                f"{up_down:2.0f}]"
            )
            if 0 <= up_down <= 3:
                remark_1 = "1"
            elif 4 <= up_down <= 6:
                remark_1 = "2"
            elif 7 <= up_down <= 9:
                remark_1 = "3"
            elif 10 <= up_down <= 12:
                remark_1 = "4"
            elif up_down >= 13:
                remark_1 = "5"
            else:
                remark_1 = "0"
            if price_ratio > 61.80:
                remark_2 = "A"
            else:
                remark_2 = "B"
            if df_data.at[code, "recent_price"] < G_price:
                remark_3 = "A"
            else:
                remark_3 = "B"
            remark = remark_1 + remark_2 + remark_3
            if remark == "5AA":
                remark = "*****"
            df_data.at[code, "remark"] = remark
    # 用df_chip初始化df_data-----End
    # 创建df_trader Begin
    logger.trace("Create df_signal")
    if os.path.exists(file_name_signal):
        logger.trace(f"load df_signal from [{file_name_signal}]")
        df_signal_buy = pd.read_excel(
            io=file_name_signal, sheet_name="buy", index_col="code"
        )
        df_signal_sell = pd.read_excel(
            io=file_name_signal, sheet_name="sell", index_col="code"
        )
    else:
        list_signal_columns = ["name", "recent_price", "now_price", "pct_chg", "dt"]
        df_signal_buy = pd.DataFrame(columns=list_signal_columns)
        df_signal_buy.index.rename(name="code", inplace=True)
        df_signal_sell = pd.DataFrame(columns=list_signal_columns)
        df_signal_sell.index.rename(name="code", inplace=True)
    list_signal_buy = df_signal_buy.index.to_list()
    list_signal_sell = df_signal_sell.index.to_list()
    # 创建空的交易员模板 file_name_trader End
    df_modified = pd.DataFrame(columns=df_data.columns)
    df_modified.index.rename(name="code", inplace=True)
    df_add = pd.DataFrame(columns=df_data.columns)
    df_add.index.rename(name="code", inplace=True)
    df_delete = pd.DataFrame(columns=df_data.columns)
    df_delete.index.rename(name="code", inplace=True)
    with pd.ExcelWriter(path=file_name_trader, mode="w") as writer:
        df_modified.to_excel(excel_writer=writer, sheet_name="modified")
        df_add.to_excel(excel_writer=writer, sheet_name="add")
        df_delete.to_excel(excel_writer=writer, sheet_name="delete")
    # 创建空的交易员模板 file_name_trader End
    # 取得仓位控制提示
    str_pos_ctl_zh = analysis.position.position(index="sh000001")
    str_pos_ctl_csi100 = analysis.position.position(index="sh000852")
    logger.trace(f"initialization End")
    """"init End"""

    """loop Begin"""
    while True:
        if frq > 2:
            os.system("cls")
            logger.trace(f"clear screen")
        logger.trace(f"loop Begin")
        dt_now = datetime.datetime.now()
        # 盘中 9:30 -- 11:30 and 13:00 -- 15:00
        if (dt_am_start < dt_now < dt_am_end) or (dt_pm_start < dt_now < dt_pm_end):
            logger.trace(f"Start of this cycle.---[{frq:3d}]---<Start>")
            start_loop_time = time.perf_counter_ns()
            logger.trace(f"start_loop_time = {start_loop_time}")
            # 主循环块---------Start------Start-----Start-----Start----Start-------Start----Start------Start---------

            # 增加修改删除df_data中的项目 Begin
            str_msg_modified = ""
            str_msg_add = ""
            str_msg_del = ""
            if os.path.exists(file_name_input):
                df_in_modified = pd.read_excel(
                    io=file_name_input, sheet_name="modified", index_col="code"
                )
                df_in_add = pd.read_excel(
                    io=file_name_input, sheet_name="add", index_col="code"
                )
                df_in_del = pd.read_excel(
                    io=file_name_input, sheet_name="delete", index_col="code"
                )
                logger.trace(f"load [{file_name_input}] success")
                # 索引转为小写字母 Begin
                df_in_modified.index = df_in_modified.index.str.lower()
                df_in_add.index = df_in_add.index.str.lower()
                df_in_del.index = df_in_del.index.str.lower()
                # 索引转为小写字母 End
                list_data = df_data.index.to_list()
                list_in_modified = df_in_modified.index.to_list()
                list_in_add = df_in_add.index.to_list()
                list_in_del = df_in_del.index.to_list()
                if len(list_in_modified) > 0:
                    df_in_modified["recent_trading"] = dt_now
                    str_msg_modified = f"{list_in_modified} modified"
                if len(list_in_add) > 0:
                    str_msg_add = f"\n{list_in_add} remove"
                if len(list_in_del) > 0:
                    str_msg_del = f"\n{list_in_del} remove"
                list_remove = list_in_modified + list_in_del
                list_remove = set(list_remove)  # 去重
                if len(list_remove) > 0:
                    for code in list_remove:
                        if code in list_data:
                            df_data.drop(index=code, inplace=True)
                if not df_in_modified.empty:
                    df_data = pd.concat(
                        objs=[df_data, df_in_modified], axis=0, join="outer"
                    )
                if not df_in_add.empty:
                    df_data = pd.concat(objs=[df_data, df_in_add], axis=0, join="outer")
                    df_data = df_data[~df_data.index.duplicated(keep="first")]
                    logger.trace("add stock success")
                logger.trace("concat df_data and df_in_modified success")
                str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                file_name_input_rename = os.path.join(
                    path_history, f"input_{str_now_input}.xlsx"
                )
                try:
                    os.rename(src=file_name_input, dst=file_name_input_rename)
                except Exception as e:
                    logger.error(f"[{file_name_input}] rename file fail")
                    logger.error(e)
                else:
                    logger.trace(f"[{file_name_input_rename}] rename file success")
                df_data["recent_price"].fillna(0, inplace=True)
                df_data["position"].fillna(0, inplace=True)
                df_data["rise"].fillna(rise, inplace=True)
                df_data["fall"].fillna(fall, inplace=True)
            else:
                logger.trace(f"[{file_name_input}] is not exist")
            # 增加修改删除df_data中的项目 End
            # 调用实时数据接口，更新df_realtime Begin
            list_data = df_data.index.to_list()
            df_realtime = realtime_quotations(stock_codes=list_data)  # 调用实时数据接口
            if df_realtime.empty:
                logger.trace(f"[df_realtime] is empty, the program ends")
                sys.exit()
            # 调用实时数据接口，更新df_realtime End
            # 更新df_data，str_msg_rise，str_msg_fall------Begin
            str_msg_rise = ""
            str_msg_fall = ""
            i = 0
            count = len(list_data)
            # 清空df_trader
            for code in list_data:
                i += 1
                dt_now = datetime.datetime.now()
                str_msg = f"\r{dt_now}----"
                str_msg += fg.blue(f"[{i:3d}/{count:3d}]")
                print(str_msg, end="")
                if i >= count:
                    print("\n", end="")  # 调整输出console格式
                if code not in df_realtime.index:
                    logger.trace("code not in df_realtime")
                    continue
                df_data.at[code, "name"] = df_realtime.at[code, "name"]
                now_price = df_realtime.at[code, "close"]
                df_data.at[code, "now_price"] = now_price
                if df_data.at[code, "recent_price"] == 0:
                    df_data.at[code, "recent_price"] = recent_price = now_price
                else:
                    recent_price = df_data.at[code, "recent_price"]
                pct_chg = (now_price / recent_price - 1) * 100
                pct_chg = round(pct_chg, 2)
                df_data.at[code, "pct_chg"] = pct_chg
                # df_trader Begin
                if (
                    pct_chg >= df_data.at[code, "rise"]
                    and df_data.at[code, "position"] > 0
                ):
                    if (
                        code in list_signal_buy
                        and df_signal_buy.at[code, "now_price"]
                        < df_data.at[code, "now_price"]
                    ):
                        df_signal_buy.at[code, "now_price"] = df_data.at[
                            code, "now_price"
                        ]
                        df_signal_buy.at[code, "pct_chg"] = df_data.at[code, "pct_chg"]
                        df_signal_buy.at[code, "dt"] = dt_now
                    else:  # ["name", "recent_price", "now_price", "pct_chg", "dt"]
                        df_signal_buy.at[code, "name"] = df_data.at[code, "name"]
                        df_signal_buy.at[code, "recent_price"] = df_data.at[
                            code, "recent_price"
                        ]
                        df_signal_buy.at[code, "now_price"] = df_data.at[
                            code, "now_price"
                        ]
                        df_signal_buy.at[code, "pct_chg"] = df_data.at[code, "pct_chg"]
                        df_signal_buy.at[code, "dt"] = dt_now
                    str_msg_rise += (
                        f"<Sell>-[{code}_{df_data.at[code, 'name']}]-"
                        f"<{now_price:5.2f}_↓_{pct_chg:5.2f}%> - "
                        f"[{df_data.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_data.at[code, 'position']):4d}]"
                    )
                    if pd.notnull(df_data.at[code, "stock_index"]):
                        str_msg_rise += f" - {df_data.at[code, 'stock_index']}"
                    if pd.notnull(df_data.at[code, "remark"]):
                        str_msg_rise += f" - [{df_data.at[code, 'remark']}]"
                    if pd.notnull(df_data.at[code, "recent_trading"]):
                        if isinstance(
                            df_data.at[code, "recent_trading"], datetime.datetime
                        ):
                            dt_trading = df_data.at[code, "recent_trading"].date()
                            str_msg_rise += f" - [{dt_trading}]"
                    str_msg_rise += "\n\n"
                elif pct_chg <= df_data.at[code, "fall"]:
                    if (
                        code in list_signal_sell
                        and df_signal_sell.at[code, "now_price"]
                        > df_data.at[code, "now_price"]
                    ):
                        df_signal_sell.at[code, "now_price"] = df_data.at[
                            code, "now_price"
                        ]
                        df_signal_sell.at[code, "pct_chg"] = df_data.at[code, "pct_chg"]
                        df_signal_sell.at[code, "dt"] = dt_now
                        pass
                    else:  # ["name", "recent_price", "now_price", "pct_chg", "dt"]
                        df_signal_sell.at[code, "name"] = df_data.at[code, "name"]
                        df_signal_sell.at[code, "recent_price"] = df_data.at[
                            code, "recent_price"
                        ]
                        df_signal_sell.at[code, "now_price"] = df_data.at[
                            code, "now_price"
                        ]
                        df_signal_sell.at[code, "pct_chg"] = df_data.at[code, "pct_chg"]
                        df_signal_sell.at[code, "dt"] = dt_now
                    str_msg_fall += (
                        f"<Buy>-[{code}_{df_data.at[code, 'name']}]-"
                        f"<{now_price:5.2f}_↓_{pct_chg:5.2f}%> - "
                        f"[{df_data.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_data.at[code, 'position']):4d}]"
                    )
                    if pd.notnull(df_data.at[code, "stock_index"]):
                        str_msg_fall += f" - {df_data.at[code, 'stock_index']}"
                    if pd.notnull(df_data.at[code, "remark"]):
                        str_msg_fall += f" - [{df_data.at[code, 'remark']}]"
                    if pd.notnull(df_data.at[code, "recent_trading"]):
                        if isinstance(
                            df_data.at[code, "recent_trading"], datetime.datetime
                        ):
                            dt_trading = df_data.at[code, "recent_trading"].date()
                            str_msg_fall += f" - [{dt_trading}]"
                    str_msg_fall += "\n\n"
                else:
                    pass
                # df_trader End
            # 更新df_data，str_msg_rise，str_msg_fall------End
            df_data.sort_values(by=["pct_chg"], ascending=False, inplace=True)
            df_data.to_pickle(path=file_name_data_pickle)
            logger.trace(f"df_data pickle at [{file_name_data_pickle}]")
            if random.randint(0, 2) == 1:
                df_data.to_csv(path_or_buf=file_name_data_csv)
                logger.trace(f"df_data csv at [{file_name_data_csv}]")
            list_signal_buy_temp = df_signal_buy.index.to_list()
            list_signal_sell_temp = df_signal_sell.index.to_list()
            list_signal_chg = list()
            if (
                list_signal_buy != list_signal_buy_temp
                or list_signal_sell != list_signal_sell_temp
            ):
                list_signal = list_signal_buy + list_signal_sell
                list_signal_temp = list_signal_buy_temp + list_signal_sell_temp
                list_signal = set(list_signal)
                list_signal_temp = set(list_signal_temp)
                for code in list_signal_temp:
                    if code not in list_signal:
                        list_signal_chg.append(code)
                df_signal_buy.sort_values(by=["dt"], ascending=False, inplace=True)
                df_signal_sell.sort_values(by=["dt"], ascending=False, inplace=True)
                with pd.ExcelWriter(path=file_name_signal, mode="w") as writer:
                    df_signal_buy.to_excel(excel_writer=writer, sheet_name="buy")
                    df_signal_sell.to_excel(excel_writer=writer, sheet_name="sell")
            if list_signal_buy != list_signal_buy_temp:
                list_signal_buy = list_signal_buy_temp.copy()
            if list_signal_sell != list_signal_sell_temp:
                list_signal_sell = list_signal_sell_temp.copy()
            if str_msg_fall != "":
                print(
                    f"===={fg.green('<Suggest Buying>')}===========================================\n"
                )
                str_msg_fall = fg.green(str_msg_fall)
                print(str_msg_fall)
            if str_msg_rise != "":
                print(
                    f"===={fg.red('<Suggest Selling>')}==========================================\n"
                )
                str_msg_rise = fg.red(str_msg_rise) + "\a"  # 加上“\a”，铃声提醒
                print(str_msg_rise)
            if str_msg_fall != "" or str_msg_rise != "":
                print(
                    f"****{fg.yellow('<Suggest END>')}**********************************************"
                )
            str_msg_temp = str_msg_modified + str_msg_add + str_msg_del
            if str_msg_temp != "":
                str_msg_temp = fg.red(str_msg_temp)
                print(dt_now, str_msg_temp)
            if len(list_signal_chg) > 0:
                print("\a\r", dt_now, ":", list_signal_chg, " --- New Signal")

            # 主循环块---------End----End-----End----End------End----End------End------End-------End------
            end_loop_time = time.perf_counter_ns()
            logger.trace(f"end_loop_time = {end_loop_time}")
            interval_time = (end_loop_time - start_loop_time) / 1000000000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            logger.trace(f"This cycle takes {str_gm}---[{frq:2d}]")
            dt_now = datetime.datetime.now()
            str_msg_loop_end = f"{dt_now}----[{str_gm}] --- {fg.red(str_pos_ctl_zh)} --- {fg.red(str_pos_ctl_csi100)}"
            print(str_msg_loop_end)
        # 中午体息时间： 11:30 -- 13:00
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            delay = int((dt_pm_start - dt_now).total_seconds())
            logger.trace(
                f"The exchange is closed at noon and will open in {delay} seconds"
            )
            logger.trace(f"loop End")
            sleep_interval(delay)
            # -----当前时间与当日指定时间的间隔时间计算-----
        # 开盘前：1:00 至 9:30
        elif dt_program_start < dt_now < dt_am_start:
            delay = int((dt_am_start - dt_now).total_seconds())
            logger.trace(f"The exchange will open in {delay} seconds")
            sleep_interval(delay)
            pass
        # 收盘后：15:00 -- 23:00
        elif dt_pm_end < dt_now < dt_program_end:
            logger.trace(f"loop End")
            print("\a", end="")
            str_pos_ctl_zh = analysis.position.position(index="sh000001")
            str_pos_ctl_csi100 = analysis.position.position(index="sh000852")
            print(str_pos_ctl_zh)
            print(str_pos_ctl_csi100)
            df_chip = analysis.chip.chip()
            print(df_chip)
            logger.trace(f"Program End")
            sys.exit()
        # 休息： 23:00 -- +1:00(次日)
        else:
            logger.trace(f"loop End")
            logger.trace("Program OFF")
            sys.exit()
        frq += 1
        logger.trace(
            f"The [No{frq:3d}] cycle will start in {scan_interval:2d} seconds."
        )
        logger.trace(f"loop End")
        sleep_interval(scan_interval)
