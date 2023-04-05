# modified at 2023/3/29 15:47
from __future__ import annotations
import os
import random
import sys
import datetime
import time
import pandas as pd
from ashare import realtime_quotations
from loguru import logger
from console import fg
import analysis
from analysis import (
    dt_date_trading,
    dt_program_start,
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_1457,
    dt_pm_end,
    dt_program_end,
    filename_trader_template,
    path_history,
    filename_log,
    filename_input,
    filename_signal,
    filename_data_csv,
    filename_chip_shelve,
)


__version__ = "3.0.0"
logger_console_level = "INFO"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}


def sleep_to_time(dt_time: datetime.datetime):
    dt_now_sleep = datetime.datetime.now()
    while dt_now_sleep <= dt_time:
        int_delay = int((dt_time - dt_now_sleep).total_seconds())
        str_sleep_gm = time.strftime("%H:%M:%S", time.gmtime(int_delay))
        str_sleep_msg = f"Waiting: {str_sleep_gm}"
        str_sleep_msg = fg.cyan(str_sleep_msg)
        str_sleep_msg = f"\r{dt_now_sleep}----" + str_sleep_msg
        print(str_sleep_msg, end="")  # 进度条
        time.sleep(1)
        dt_now_sleep = datetime.datetime.now()
    print("\n", end="")
    return True


if __name__ == "__main__":
    logger.remove()
    logger.add(sink=sys.stderr, level=logger_console_level)
    logger.add(sink=filename_log, level='TRACE', encoding='utf-8')
    # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    if analysis.is_trading_day():
        logger.trace("Betting day")
        print("Betting day")
    else:
        logger.trace("Non betting day")
        print("Non betting day")
        logger.trace("Program OFF")
        sys.exit()
    """init Begin"""
    fall = -5
    rise = 10000 / (100 + fall) - 100  # rise = 5.26315789473683
    frq = 0
    scan_interval = 20
    logger.trace(f"initialization Begin")
    # 加载df_industry_class Begin
    logger.trace("load df_industry_class...")
    df_industry_class = analysis.read_obj_from_db(
        key="df_industry_class", filename=filename_chip_shelve
    )
    if df_industry_class.empty:
        logger.trace(f"df_industry_class from filename_chip_shelve is empty")
        try:
            df_industry_class = pd.read_excel(
                io="df_industry_class.xlsx", index_col="code"
            )
        except FileNotFoundError as e:
            print(f"[df_industry_class.xlsx] - {e.args[1]}")
            logger.error(f"[df_industry_class.xlsx] - {e.args[1]}")
            sys.exit()
        else:
            if df_industry_class.empty:
                logger.error(
                    f"df_industry_class from [df_industry_class.xlsx] is empty"
                )
                sys.exit()
            else:
                analysis.write_obj_to_db(
                    obj=df_industry_class,
                    key="df_industry_class",
                    filename=filename_chip_shelve,
                )
    logger.trace(f"df_industry_class create success")
    # 加载df_industry_class End
    # 加载df_chip Begin
    logger.trace("load df_chip...")
    df_chip = analysis.read_obj_from_db(
        key="df_chip", filename=filename_chip_shelve
    )
    if df_chip.empty:
        logger.trace(f"df_chip from filename_chip_shelve is empty")
        df_chip = analysis.chip()
        logger.trace(f"df_chip create success")
        if df_chip.empty:
            sys.exit()
    dt_chip_max = df_chip["dt"].max()
    str_chip_msg = f"The latest chip analysis is on [{dt_chip_max}]"
    str_chip_msg = fg.red(str_chip_msg)
    print(str_chip_msg)
    logger.trace(f"load df_chip success")
    # 加载df_chip End
    # 加载df_industry_rank_pool Begin
    logger.trace("load df_industry_rank_pool...")
    df_industry_rank_pool = analysis.read_obj_from_db(
        key="df_industry_rank_pool", filename=filename_chip_shelve
    )
    if df_industry_rank_pool.empty:
        logger.error(f"df_industry_rank_pool is empty")
        print('Please rerun df_ Chip function')
        sys.exit()
    df_industry_rank_pool_buying = df_industry_rank_pool[
        df_industry_rank_pool["T5_rank"] >= 66
        ]
    df_industry_rank_pool_selling = df_industry_rank_pool[
        df_industry_rank_pool["T5_rank"] <= 10
        ]
    list_industry_buying = df_industry_rank_pool_buying["name"].tolist()
    list_industry_buying_code = df_industry_rank_pool_buying.index.tolist()
    list_industry_selling = df_industry_rank_pool_selling["name"].tolist()
    list_industry_selling_code = df_industry_rank_pool_selling.index.tolist()
    # 加载df_industry_rank_pool End
    # 加载df_trader Begin
    logger.trace("Create df_trader Begin")
    df_trader = analysis.read_obj_from_db(
        key="df_trader", filename=filename_chip_shelve
    )
    if df_trader.empty:
        logger.trace(f"Create a new df_trader")
        time.sleep(20)
        list_trader_columns = [
            "name",
            "recent_price",
            "position",
            "position_unit",
            "trx_unit_share",
            "now_price",
            "pct_chg",
            "rise",
            "fall",
            "stock_index",
            "grade",
            "recent_trading",
            "ST",
            "industry_code",
            "industry_name",
            "date_of_inclusion_first",
            "date_of_inclusion_latest",
            "times_of_inclusion",
            "price_of_inclusion",
            "remark",
        ]
        list_trader_symbol = ["sh600519", "sz300750"]
        df_trader = pd.DataFrame(index=list_trader_symbol, columns=list_trader_columns)
        df_trader.index.rename(name="code", inplace=True)
    df_stocks_pool = analysis.read_obj_from_db(
        key="df_stocks_pool", filename=filename_chip_shelve
    )
    list_trader = df_trader.index.to_list()
    list_stocks_pool = df_stocks_pool.index.to_list()
    for code in list_stocks_pool:
        if code not in list_trader:
            df_trader.loc[code] = pd.Series(index=df_trader.columns, dtype="object")
        if pd.isnull(df_trader.at[code, "date_of_inclusion_first"]):
            df_trader.at[code, "date_of_inclusion_first"] = dt_date_trading
            df_trader.at[code, "times_of_inclusion"] = 1
            df_trader.at[code, "price_of_inclusion"] = df_chip.at[code, "now_price"]
            df_trader.at[code, "date_of_inclusion_latest"] = dt_date_trading
        if pd.isnull(df_trader.at[code, "date_of_inclusion_latest"]):
            df_trader.at[code, "date_of_inclusion_latest"] = dt_date_trading
        elif df_trader.at[code, "date_of_inclusion_latest"] != dt_date_trading:
            df_trader.at[code, "date_of_inclusion_latest"] = dt_date_trading
            df_trader.at[code, "times_of_inclusion"] += 1
    df_trader["date_of_inclusion_first"].fillna(dt_date_trading, inplace=True)
    df_trader["times_of_inclusion"].fillna(1, inplace=True)
    df_trader["recent_price"].fillna(0, inplace=True)
    df_trader["position"].fillna(0, inplace=True)
    df_trader["now_price"].fillna(0, inplace=True)
    df_trader["pct_chg"].fillna(0, inplace=True)
    df_trader["rise"].fillna(rise, inplace=True)
    df_trader["fall"].fillna(fall, inplace=True)
    dt_now = datetime.datetime.now()
    df_trader["recent_trading"].fillna(dt_now, inplace=True)
    logger.trace("Create df_trader End")
    # 加载df_trader End
    # 用df_chip初始化df_trader----Begin
    logger.trace("initialization df_index")
    list_trader = df_trader.index.to_list()
    for code in list_trader:
        if code in df_industry_class.index:
            df_trader.at[code, "industry_code"] = df_industry_class.at[
                code, "industry_code"
            ]
            df_trader.at[code, "industry_name"] = df_industry_class.at[
                code, "industry_name"
            ]
        if code in df_chip.index:
            now_price = df_trader.at[code, "now_price"]
            now_price_ratio = round(df_chip.at[code, "now_price_ratio"], 1)
            G_price = df_chip.at[code, "G_price"]
            if (
                pd.isnull(df_trader.at[code, "recent_price"])
                or df_trader.at[code, "recent_price"] == 0
            ):
                df_trader.at[code, "recent_price"] = G_price
            t5_amplitude = df_chip.at[code, "T5_amplitude"]
            t5_pct = df_chip.at[code, "T5_pct"]
            up_times = int(df_chip.at[code, "up_times"])
            up_A_down_7pct = int(df_chip.at[code, "up_A_down_7pct"])
            up_A_down_5pct = int(df_chip.at[code, "up_A_down_5pct"])
            up_A_down_3pct = int(df_chip.at[code, "up_A_down_3pct"])
            turnover = round(df_chip.at[code, "turnover"], 1)
            df_trader.at[code, "trx_unit_share"] = analysis.transaction_unit(
                price=df_chip.at[code, "G_price"]
            )
            df_trader.at[code, "position_unit"] = (
                df_trader.at[code, "position"] / df_trader.at[code, "trx_unit_share"]
            ).round(2)
            df_trader.at[code, "stock_index"] = (
                f"({up_times:2.0f}U /"
                f"{turnover:2.0f}T /"
                f"{now_price_ratio:6.2f}% -"
                f"{G_price:6.2f}$)--"
                f"[T5_amp:{t5_amplitude:5.2f}]-"
                f"[T5_pct:{t5_pct:5.2f}]"
            )
            if up_A_down_7pct >= 12:
                grade_ud_7 = "A"
            elif 4 <= up_A_down_7pct < 12:
                grade_ud_7 = "B"
            else:
                grade_ud_7 = "Z"
            if up_A_down_5pct >= 24:
                grade_ud_5 = "A"
            elif 12 <= up_A_down_5pct < 24:
                grade_ud_5 = "B"
            else:
                grade_ud_5 = "Z"
            if up_A_down_3pct >= 48:
                grade_ud_3 = "A"
            elif 24 <= up_A_down_3pct < 48:
                grade_ud_3 = "B"
            else:
                grade_ud_3 = "Z"
            if up_times >= 4:
                grade_ud_limit = "A"
            elif 2 <= up_times < 4:
                grade_ud_limit = "B"
            else:
                grade_ud_limit = "Z"
            if 15 <= turnover <= 40:
                grade_to = "A"
            elif 5 <= turnover < 15:
                grade_to = "B"
            else:
                grade_to = "Z"
            if 51.80 <= now_price_ratio <= 71.8:  # 61.8 上下10%
                grade_pr = "A"
            elif 71.8 < now_price_ratio <= 81.8 or 41.8 <= now_price_ratio < 51.8:
                grade_pr = "B"
            else:
                grade_pr = "Z"
            if 0 < now_price < G_price:
                grade_G = "Under"
            elif G_price <= now_price:
                grade_G = "Over"
            else:
                grade_G = "#"
            grade = (
                grade_ud_7
                + grade_ud_5
                + grade_ud_3
                + "-"
                + grade_ud_limit
                + grade_to
                + grade_pr
                + "-"
                + grade_G
            )
            df_trader.at[code, "grade"] = grade
            df_trader.at[code, "ST"] = df_chip.at[code, "ST"]
    # 用df_chip初始化df_trader-----End
    # 保存df_trader Begin
    analysis.write_obj_to_db(
        obj=df_trader, key="df_trader", filename=filename_chip_shelve
    )
    # 保存df_trader End
    # 创建df_signal Begin
    logger.trace("Create df_signal")
    if os.access(path=filename_signal, mode=os.F_OK):
        logger.trace(f"load df_signal from [{filename_signal}]")
        df_signal_sell = pd.read_excel(
            io=filename_signal, sheet_name="sell", index_col="code"
        )
        df_signal_buy = pd.read_excel(
            io=filename_signal, sheet_name="buy", index_col="code"
        )
        df_signal_sell.sort_values(
            by=["position", "pct_chg", "dt"], ascending=False, inplace=True
        )
        df_signal_buy.sort_values(
            by=["position", "pct_chg", "dt"], ascending=False, inplace=True
        )
    else:
        list_signal_columns = [
            "name",
            "recent_price",
            "position",
            "now_price",
            "pct_chg",
            "stock_index",
            "grade",
            "dt",
        ]
        df_signal_sell = pd.DataFrame(columns=list_signal_columns)
        df_signal_sell.index.rename(name="code", inplace=True)
        df_signal_buy = pd.DataFrame(columns=list_signal_columns)
        df_signal_buy.index.rename(name="code", inplace=True)
    list_signal_buy = df_signal_sell.index.to_list()
    list_signal_sell = df_signal_buy.index.to_list()
    # 创建空的交易员模板 file_name_trader_template End
    if not os.access(path=filename_trader_template, mode=os.F_OK):
        df_modified = pd.DataFrame(columns=df_trader.columns)
        df_modified.index.rename(name="code", inplace=True)
        df_add = pd.DataFrame(columns=df_trader.columns)
        df_add.index.rename(name="code", inplace=True)
        df_delete = pd.DataFrame(columns=df_trader.columns)
        df_delete.index.rename(name="code", inplace=True)
        analysis.add_chip_excel(
            df=df_modified, key="modified", filename=filename_trader_template
        )
        analysis.add_chip_excel(
            df=df_add, key="add", filename=filename_trader_template
        )
        analysis.add_chip_excel(
            df=df_delete, key="delete", filename=filename_trader_template
        )
    else:
        logger.trace(f"[{filename_trader_template}] can be access")
    # 创建空的交易员模板 file_name_trader End
    # 取得仓位控制提示
    str_pos_ctl_zh = analysis.position(index="sh000001")
    str_pos_ctl_csi1000 = analysis.position(index="sh000852")
    logger.trace(f"initialization End")
    """init End"""
    """loop Begin"""
    while True:
        if frq > 2:
            os.system("cls")
            logger.trace(f"clear screen")
        logger.trace(f"loop Begin")
        dt_now = datetime.datetime.now()
        # 盘中 9:30 -- 11:30 and 13:00 -- 15:00
        if (dt_am_start <= dt_now <= dt_am_end) or (dt_pm_start <= dt_now <= dt_pm_end):
            logger.trace(f"Start of this cycle.---[{frq:3d}]---<Start>")
            start_loop_time = time.perf_counter_ns()
            logger.trace(f"start_loop_time = {start_loop_time}")

            # 主循环块---------Start------Start-----Start-----Start----Start-------Start----Start------
            # 增加修改删除df_data中的项目 Begin
            str_msg_modified = ""
            str_msg_add = ""
            str_msg_del = ""
            if os.path.exists(filename_input):
                df_in_modified = pd.read_excel(
                    io=filename_input, sheet_name="modified", index_col="code"
                )
                df_in_add = pd.read_excel(
                    io=filename_input, sheet_name="add", index_col="code"
                )
                df_in_del = pd.read_excel(
                    io=filename_input, sheet_name="delete", index_col="code"
                )
                logger.trace(f"load [{filename_input}] success")
                # 索引转为小写字母 Begin
                df_in_modified.index = df_in_modified.index.str.lower()
                df_in_add.index = df_in_add.index.str.lower()
                df_in_del.index = df_in_del.index.str.lower()
                # 索引转为小写字母 End
                list_trader = df_trader.index.to_list()
                df_in_modified = df_in_modified[
                    ~df_in_modified.index.duplicated(keep="first")
                ]  # 去重
                list_in_modified = df_in_modified.index.to_list()
                df_in_add = df_in_add[~df_in_add.index.duplicated(keep="first")]  # 去重
                list_in_add = df_in_add.index.to_list()
                df_in_del = df_in_del[~df_in_del.index.duplicated(keep="first")]  # 去重
                list_in_del = df_in_del.index.to_list()
                if len(list_in_modified) > 0:
                    df_in_modified["recent_trading"] = dt_now
                    for code in list_in_modified:
                        if code in list_trader:
                            series_add_index = df_in_modified.loc[code].index
                            for item in series_add_index:
                                if pd.notnull(df_in_modified.at[code, item]):
                                    df_trader.at[code, item] = df_in_modified.at[
                                        code, item
                                    ]
                        else:
                            list_in_modified.remove(code)
                            logger.trace(f"[{code}] does not exist,cannot be modified")
                    if list_in_modified:
                        str_msg_modified = f"{list_in_modified} modified"
                    else:
                        str_msg_modified = ""
                    logger.trace("modified stock success")
                if len(list_in_add) > 0:
                    df_in_add["recent_trading"] = dt_now
                    df_trader = pd.concat(
                        objs=[df_trader, df_in_add], axis=0, join="outer"
                    )
                    df_trader = df_trader[~df_trader.index.duplicated(keep="first")]
                    for code in df_trader.index:
                        if code in list_in_add:
                            if pd.isnull(df_trader.at[code, "date_of_inclusion_first"]):
                                df_trader.at[
                                    code, "date_of_inclusion_first"
                                ] = dt_date_trading
                            else:
                                df_trader.at[
                                    code, "date_of_inclusion_latest"
                                ] = dt_date_trading
                            if pd.isnull(df_trader.at[code, "times_of_inclusion"]):
                                df_trader.at[code, "times_of_inclusion"] = 1
                            if pd.isnull(df_trader.at[code, "price_of_inclusion"]):
                                df_trader.at[code, "price_of_inclusion"] = df_chip.at[
                                    code, "now_price"
                                ]
                    df_trader["recent_price"].fillna(0, inplace=True)
                    df_trader["position"].fillna(0, inplace=True)
                    df_trader["now_price"].fillna(0, inplace=True)
                    df_trader["pct_chg"].fillna(0, inplace=True)
                    df_trader["rise"].fillna(rise, inplace=True)
                    df_trader["fall"].fillna(fall, inplace=True)
                    df_trader["recent_trading"].fillna(dt_now, inplace=True)
                    str_msg_add = f"\n{list_in_add} add"
                    logger.trace("add stock success")
                if len(list_in_del) > 0:
                    df_in_del["recent_trading"] = dt_now
                    for code in list_in_del:
                        if code in list_trader:
                            if df_trader.at[code, "position"] <= 0:
                                df_trader.drop(index=code, inplace=True)
                            else:
                                list_in_del.remove(code)
                    str_msg_del = f"\n{list_in_del} remove"
                    logger.trace("del stock success")
                str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                file_name_input_rename = os.path.join(
                    path_history, f"input_{str_now_input}.xlsx"
                )
                try:
                    os.rename(src=filename_input, dst=file_name_input_rename)
                except Exception as e:
                    logger.error(repr(e))
                    logger.error(f"[{filename_input}] rename file fail")
                else:
                    logger.trace(f"[{file_name_input_rename}] rename file success")
                # df_trader["recent_price"].fillna(0, inplace=True)
                df_trader["position"].fillna(0, inplace=True)
                df_trader["rise"].fillna(rise, inplace=True)
                df_trader["fall"].fillna(fall, inplace=True)
            else:
                logger.trace(f"[{filename_input}] is not exist")
            # 增加修改删除df_data中的项目 End
            # 调用实时数据接口，更新df_realtime Begin
            list_trader = df_trader.index.to_list()
            df_realtime = realtime_quotations(stock_codes=list_trader)  # 调用实时数据接口
            if df_realtime.empty:
                logger.trace(f"[df_realtime] is empty, the program ends")
                sys.exit()
            # 调用实时数据接口，更新df_realtime End
            # 更新df_data，str_msg_rise，str_msg_fall------Begin
            str_msg_rise = ""
            str_msg_fall = ""
            i = 0
            count_trader = len(list_trader)
            # 清空df_trader
            for code in list_trader:
                i += 1
                dt_now = datetime.datetime.now()
                str_msg = f"\r{dt_now}----"
                str_msg += fg.blue(f"[{i:3d}/{count_trader:3d}]")
                print(str_msg, end="")
                if i >= count_trader:
                    print("\n", end="")  # 调整输出console格式
                try:
                    if not pd.notnull(df_trader.at[code, "industry_code"]):
                        df_trader.at[code, "industry_code"] = df_industry_class.at[
                            code, "industry_code"
                        ]
                        logger.trace(f"{code} update industry_code")
                    if not pd.notnull(df_trader.at[code, "industry_name"]):
                        df_trader.at[code, "industry_name"] = df_industry_class.at[
                            code, "industry_name"
                        ]
                        logger.trace(f"{code} update industry_name")
                except KeyError as e:
                    logger.trace(f"df_industry_class KeyError [{repr(e)}]")
                    time.sleep(5)
                industry_code = df_trader.at[code, "industry_code"]
                if code not in df_realtime.index:
                    logger.trace("code not in df_realtime")
                    continue
                df_trader.at[code, "name"] = df_realtime.at[code, "name"]
                now_price = df_realtime.at[code, "close"]
                df_trader.at[code, "now_price"] = now_price
                if pd.isnull(df_trader.at[code, "recent_price"]):
                    df_trader.at[code, "recent_price"] = recent_price = now_price
                else:
                    recent_price = df_trader.at[code, "recent_price"]
                pct_chg = (now_price / recent_price - 1) * 100
                pct_chg = round(pct_chg, 2)
                df_trader.at[code, "pct_chg"] = pct_chg
                if not pd.notnull(df_trader.at[code, "position_unit"]):
                    df_trader.at[
                        code, "trx_unit_share"
                    ] = analysis.transaction_unit(
                        price=df_chip.at[code, "G_price"]
                    )
                    df_trader.at[code, "position_unit"] = (
                        df_trader.at[code, "position"]
                        / df_trader.at[code, "trx_unit_share"]
                    ).round(2)
                # df_trader Begin
                if (
                    pct_chg >= df_trader.at[code, "rise"]
                    and df_trader.at[code, "position"] > 0
                ):
                    if (
                        code in list_signal_buy
                        and df_signal_sell.at[code, "now_price"]
                        < df_trader.at[code, "now_price"]
                    ):
                        df_signal_sell.at[code, "now_price"] = df_trader.at[
                            code, "now_price"
                        ]
                        df_signal_sell.at[code, "pct_chg"] = df_trader.at[
                            code, "pct_chg"
                        ]
                        df_signal_sell.at[code, "dt"] = dt_now
                    else:
                        df_signal_sell.at[code, "name"] = df_trader.at[code, "name"]
                        df_signal_sell.at[code, "recent_price"] = df_trader.at[
                            code, "recent_price"
                        ]
                        df_signal_sell.at[code, "now_price"] = df_trader.at[
                            code, "now_price"
                        ]
                        df_signal_sell.at[code, "pct_chg"] = df_trader.at[
                            code, "pct_chg"
                        ]
                        df_signal_sell.at[code, "dt"] = dt_now
                    df_signal_sell.at[code, "position"] = df_trader.at[code, "position"]
                    df_signal_sell.at[code, "stock_index"] = df_trader.at[
                        code, "stock_index"
                    ]
                    df_signal_sell.at[code, "grade"] = df_trader.at[code, "grade"]
                    str_msg_rise += fg.lightred(
                        f"\n<Sell>-[{code}_{df_trader.at[code, 'name']}]-"
                        f"<{now_price:5.2f}_↑_{pct_chg:5.2f}%> - "
                        f"[{df_trader.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_trader.at[code, 'position']):4d}:( "
                        f"{df_trader.at[code, 'position_unit']:3.1f}*"
                        f"{int(df_trader.at[code, 'trx_unit_share']):3d})]"
                    )
                    if pd.notnull(df_trader.at[code, "ST"]):
                        str_msg_rise += fg.white(f" - [{df_trader.at[code, 'ST']}]")
                    str_msg_rise_industry = (
                        f"\n ---- [{df_trader.at[code, 'name']}]"
                        f" - [{industry_code}]"
                        f"-[{df_trader.at[code, 'times_of_inclusion']:2.0f}]"
                        f"-[{df_trader.at[code, 'date_of_inclusion_latest']}]"
                    )
                    if industry_code in list_industry_buying_code:
                        str_msg_rise_industry = fg.lightred(str_msg_rise_industry)
                        str_msg_rise += str_msg_rise_industry + fg.blue(
                            f"\n ---- [{df_industry_rank_pool.at[industry_code, 'name']}]"
                            f" - [{industry_code}]"
                            f" - [{df_industry_rank_pool.at[industry_code, 'T5_rank']:2.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T20_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T40_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T60_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T80_rank']:02.0f}] - "
                            f"[{df_industry_rank_pool.at[industry_code, 'max_min']:02.0f}]"
                        )
                    else:
                        str_msg_rise_industry = fg.yellow(str_msg_rise_industry)
                        str_msg_rise += str_msg_rise_industry
                    if pd.notnull(df_trader.at[code, "grade"]):
                        str_msg_rise += fg.lightyellow(
                            f"\n ---- [{df_trader.at[code, 'industry_name']}]"
                        )
                    if pd.notnull(df_trader.at[code, "stock_index"]):
                        str_msg_rise += fg.purple(
                            f" - {df_trader.at[code, 'stock_index']}"
                        )
                    if pd.notnull(df_trader.at[code, "recent_trading"]):
                        if isinstance(
                            df_trader.at[code, "recent_trading"], datetime.datetime
                        ):
                            dt_trading = df_trader.at[code, "recent_trading"].date()
                            str_msg_rise += f"\n ---- [{dt_trading}]"
                    if pd.notnull(df_trader.at[code, "remark"]):
                        str_msg_rise += f" - {df_trader.at[code, 'remark']}"
                    str_msg_rise += "\n\n"
                elif pct_chg <= df_trader.at[code, "fall"]:
                    if (
                        code in list_signal_sell
                        and df_signal_buy.at[code, "now_price"]
                        > df_trader.at[code, "now_price"]
                    ):
                        df_signal_buy.at[code, "now_price"] = df_trader.at[
                            code, "now_price"
                        ]
                        df_signal_buy.at[code, "pct_chg"] = df_trader.at[
                            code, "pct_chg"
                        ]
                        df_signal_buy.at[code, "dt"] = dt_now
                        pass
                    else:
                        df_signal_buy.at[code, "name"] = df_trader.at[code, "name"]
                        df_signal_buy.at[code, "recent_price"] = df_trader.at[
                            code, "recent_price"
                        ]
                        df_signal_buy.at[code, "now_price"] = df_trader.at[
                            code, "now_price"
                        ]
                        df_signal_buy.at[code, "pct_chg"] = df_trader.at[
                            code, "pct_chg"
                        ]
                        df_signal_buy.at[code, "dt"] = dt_now
                    df_signal_buy.at[code, "position"] = df_trader.at[code, "position"]
                    df_signal_buy.at[code, "stock_index"] = df_trader.at[
                        code, "stock_index"
                    ]
                    df_signal_buy.at[code, "grade"] = df_trader.at[code, "grade"]
                    str_msg_fall += fg.lightgreen(
                        f"\n<Buy>-[{code}_{df_trader.at[code, 'name']}]-"
                        f"<{now_price:5.2f}_↓_{pct_chg:5.2f}%> - "
                        f"[{df_trader.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_trader.at[code, 'position']):4d}:( "
                        f"{df_trader.at[code, 'position_unit']:3.1f}*"
                        f"{int(df_trader.at[code, 'trx_unit_share']):3d})]"
                    )
                    if pd.notnull(df_trader.at[code, "ST"]):
                        str_msg_fall += fg.white(f" - [{df_trader.at[code, 'ST']}]")
                    str_msg_fall_industry = (
                        f"\n ---- [{df_trader.at[code, 'industry_name']}]"
                        f" - [{industry_code}]"
                        f"-[{df_trader.at[code, 'times_of_inclusion']:2.0f}]"
                        f"-[{df_trader.at[code, 'date_of_inclusion_latest']}]"
                    )
                    if industry_code in list_industry_selling_code:
                        str_msg_fall_industry = fg.lightred(str_msg_fall_industry)
                        str_msg_fall += str_msg_fall_industry + fg.blue(
                            f" - [{df_industry_rank_pool.at[industry_code, 'T5_rank']:2.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T20_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T40_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T60_rank']:02.0f} - "
                            f"{df_industry_rank_pool.at[industry_code, 'T80_rank']:02.0f}] - "
                            f"[{df_industry_rank_pool.at[industry_code, 'max_min']:02.0f}]"
                        )
                    else:
                        str_msg_fall_industry = fg.yellow(str_msg_fall_industry)
                        str_msg_fall += str_msg_fall_industry
                    if pd.notnull(df_trader.at[code, "grade"]):
                        str_msg_fall += fg.lightyellow(
                            f"\n ---- [{df_trader.at[code, 'grade']}]"
                        )
                    if pd.notnull(df_trader.at[code, "stock_index"]):
                        str_msg_fall += fg.purple(
                            f" - {df_trader.at[code, 'stock_index']}"
                        )
                    if pd.notnull(df_trader.at[code, "recent_trading"]):
                        if isinstance(
                            df_trader.at[code, "recent_trading"], datetime.datetime
                        ):
                            dt_trading = df_trader.at[code, "recent_trading"].date()
                            str_msg_fall += f"\n ---- [{dt_trading}]"
                    if pd.notnull(df_trader.at[code, "remark"]):
                        str_msg_rise += f" - {df_trader.at[code, 'remark']}"
                    str_msg_fall += "\n\n"
                else:
                    pass
                # df_trader End
            # 更新df_data，str_msg_rise，str_msg_fall------End
            df_trader.sort_values(by=["pct_chg"], ascending=False, inplace=True)
            analysis.write_obj_to_db(
                obj=df_trader, key="df_trader", filename=filename_chip_shelve
            )
            if random.randint(0, 2) == 1:
                df_trader.to_csv(path_or_buf=filename_data_csv)
                logger.trace(f"df_trader csv at [{filename_data_csv}]")
            list_signal_buy_temp = df_signal_sell.index.to_list()
            list_signal_sell_temp = df_signal_buy.index.to_list()
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
                with pd.ExcelWriter(path=filename_signal, mode="w") as writer:
                    df_signal_sell.to_excel(excel_writer=writer, sheet_name="sell")
                    df_signal_buy.to_excel(excel_writer=writer, sheet_name="buy")
            if list_signal_buy != list_signal_buy_temp:
                list_signal_buy = list_signal_buy_temp.copy()
            if list_signal_sell != list_signal_sell_temp:
                list_signal_sell = list_signal_sell_temp.copy()
            print(
                f"===={fg.green('<Suggest Buying>')}=================================================="
                f"\n{fg.lightred(f'{list_industry_buying}')}"
            )
            if str_msg_fall != "":
                print(str_msg_fall)
            print(
                f"===={fg.red('<Suggest Selling>')}================================================="
                f"\n{fg.lightred(f'{list_industry_selling}')}"
            )
            if str_msg_rise != "":
                print(str_msg_rise,'\a')  # 加上“\a”，铃声提醒
            print(
                f"****{fg.yellow('<Suggest END>')}*****************************************************"
            )
            str_msg_temp = str_msg_modified + str_msg_add + str_msg_del
            if str_msg_temp != "":
                str_msg_temp = fg.red(str_msg_temp)
                print(dt_now, str_msg_temp)
            if len(list_signal_chg) > 0:
                print(dt_now, ":", list_signal_chg, " --- New Signal\a")
            # 主循环块---------End----End-----End----End------End----End------End------End-------End------

            end_loop_time = time.perf_counter_ns()
            logger.trace(f"end_loop_time = {end_loop_time}")
            interval_time = (end_loop_time - start_loop_time) / 100000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            logger.trace(f"This cycle takes {str_gm}---[{frq:2d}]")
            dt_now = datetime.datetime.now()
            str_msg_loop_end = f"{dt_now}----[{str_gm}]"
            str_msg_loop_ctl_zh = f"{dt_now}----{fg.red(str_pos_ctl_zh)}"
            str_msg_loop_ctl_csi1000 = f"{dt_now}----{fg.red(str_pos_ctl_csi1000)}"
            print(str_msg_loop_end)
            print(str_msg_loop_ctl_zh)
            print(str_msg_loop_ctl_csi1000)
            # 收盘前集合竟价：14:57 -- 15:00 响玲
            if dt_pm_1457 < dt_now <= dt_pm_end:
                print("\a", end="")
                scan_interval = 60
        # 中午体息时间： 11:30 -- 13:00
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            logger.trace(
                f"The exchange is closed at noon and will open at {dt_pm_start}"
            )
            logger.trace(f"loop End")
            sleep_to_time(dt_pm_start)
            # -----当前时间与当日指定时间的间隔时间计算-----
        # 开盘前：1:00 至 9:30
        elif dt_program_start < dt_now < dt_am_start:
            logger.trace(f"The exchange will open ar {dt_am_start}")
            print(f"The exchange will open ar {dt_am_start}")
            sleep_to_time(dt_am_start)
        # 收盘后：15:00 -- 23:00
        elif dt_pm_end < dt_now < dt_program_end:
            logger.trace(f"loop End")
            print("\a\r", end="")
            analysis.unit_net()
            str_pos_ctl_zh = analysis.position(index="sh000001")
            str_pos_ctl_csi1000 = analysis.position(index="sh000852")
            print(str_pos_ctl_zh)
            print(str_pos_ctl_csi1000)
            df_chip = analysis.chip()
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
        dt_now = datetime.datetime.now()
        dt_now_delta = dt_now + datetime.timedelta(seconds=scan_interval)
        sleep_to_time(dt_now_delta)
