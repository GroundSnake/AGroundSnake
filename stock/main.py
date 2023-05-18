# modified at 2023/05/18 22::25
from __future__ import annotations
import os
import sys
import datetime
import time
import pandas as pd
import akshare as ak
from ashare import realtime_quotations
from loguru import logger
from console import fg
import analysis
from analysis import (
    dt_date_trading,
    dt_am_0910,
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_1457,
    dt_pm_end,
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
        str_dt_now_sleep = dt_now_sleep.strftime("<%H:%M:%S>")
        str_sleep_msg = f"{str_dt_now_sleep}----" + str_sleep_msg
        print(f"\r{str_sleep_msg}\033[K", end="")  # 进度条
        time.sleep(1)
        dt_now_sleep = datetime.datetime.now()
    print("\n", end="")
    return True


if __name__ == "__main__":
    logger.remove()
    logger.add(sink=sys.stderr, level=logger_console_level)
    logger.add(sink=filename_log, level="TRACE", encoding="utf-8")
    # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    """
    if analysis.is_trading_day():
        logger.trace("Betting day")
        print("Betting day")
    else:
        logger.trace("Non betting day")
        print("Non betting day")
        logger.trace("Program OFF")
        sys.exit()
    """
    """init Begin"""
    fall = -5
    rise = 10000 / (100 + fall) - 100  # rise = 5.26315789473683
    frq = 0
    scan_interval = 20
    amount_all = 0
    amount_top5 = 0
    concentration_rate_amount = 0
    str_msg_concentration_rate = ""
    logger.trace(f"initialization Begin")
    # 加载df_industry_class Begin
    df_industry_index = analysis.read_df_from_db(
        key="df_industry_index", filename=filename_chip_shelve
    )
    if df_industry_index.empty:
        try:
            df_industry_index = pd.read_excel(io="df_industry_index.xlsx", index_col=0)
        except FileNotFoundError as e:
            print(f"[df_industry_index.xlsx] - {e.args[1]}")
            sys.exit()
        else:
            if df_industry_index.empty:
                logger.error(
                    f"df_industry_index from [df_industry_index.xlsx] is empty"
                )
                sys.exit()
            else:
                analysis.write_obj_to_db(
                    obj=df_industry_index,
                    key="df_industry_index",
                    filename=filename_chip_shelve,
                )
    # 加载df_industry_class End
    # 加载df_chip Begin
    df_chip = analysis.read_df_from_db(key="df_chip", filename=filename_chip_shelve)
    if df_chip.empty:
        df_chip = analysis.chip()
        if df_chip.empty:
            sys.exit()
    dt_chip_max = df_chip["dt"].max()
    str_chip_msg = f"The latest chip analysis is on [{dt_chip_max}]"
    str_chip_msg = fg.red(str_chip_msg)
    print(str_chip_msg)
    # 加载df_chip End
    # 加载df_industry_rank_pool Begin
    df_industry_rank_pool = analysis.read_df_from_db(
        key="df_industry_rank_pool", filename=filename_chip_shelve
    )
    if df_industry_rank_pool.empty:
        print("Please rerun df_ Chip function")
        sys.exit()
    df_industry_rank_pool_buying = df_industry_rank_pool[
        df_industry_rank_pool["T5_rank"] >= 66
    ]
    df_industry_rank_pool_selling = df_industry_rank_pool[
        df_industry_rank_pool["T5_rank"] <= 10
    ]
    list_industry_buying = df_industry_rank_pool_buying["name"].tolist()
    list_industry_selling = df_industry_rank_pool_selling["name"].tolist()
    # 加载df_industry_rank_pool End
    # 加载df_trader Begin
    df_trader = analysis.read_df_from_db(key="df_trader", filename=filename_chip_shelve)
    if df_trader.empty:
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
            "total_mv_E",
            "ssb_index",
            "stock_index",
            "grade",
            "recent_trading",
            "ST",
            "industry_code",
            "industry_name",
            "date_of_inclusion_first",
            "date_of_inclusion_latest",
            "times_of_inclusion",
            "rate_of_inclusion",
            "price_of_inclusion",
            "pct_of_inclusion",
            "remark",
        ]
        list_trader_symbol = ["sh600519", "sz300750"]
        df_trader = pd.DataFrame(index=list_trader_symbol, columns=list_trader_columns)
        df_trader.index.rename(name="code", inplace=True)
    df_stocks_pool = analysis.read_df_from_db(
        key="df_stocks_pool", filename=filename_chip_shelve
    )
    list_stocks_pool = df_stocks_pool.index.to_list()
    for code in list_stocks_pool:
        if code not in df_trader.index:
            df_trader.loc[code] = pd.Series(index=df_trader.columns, dtype="object")
        if pd.isnull(df_trader.at[code, "date_of_inclusion_first"]):
            df_trader.at[code, "date_of_inclusion_first"] = dt_date_trading
            df_trader.at[code, "times_of_inclusion"] = 1
            df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                code, "recent_price"
            ] = df_trader.at[code, "now_price"] = df_chip.at[code, "now_price"]
            df_trader.at[code, "date_of_inclusion_latest"] = dt_date_trading
            df_trader.at[code, "name"] = df_chip.at[code, "name"]
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
    # 加载df_trader End
    for code in df_trader.index:
        if code in df_industry_index.index:
            df_trader.at[code, "industry_code"] = df_industry_index.at[
                code, "industry_code"
            ]
            df_trader.at[code, "industry_name"] = df_industry_index.at[
                code, "industry_name"
            ]
        # 删除df_trader中标的----Begin
        if df_trader.at[code, "position"] == 0:
            dt_inclusion_latest = df_trader.at[
                code, "date_of_inclusion_latest"
            ] - datetime.timedelta(days=30)
            days_inclusion = (
                dt_inclusion_latest - df_trader.at[code, "date_of_inclusion_first"]
            ).days
            if days_inclusion > 30 and df_trader.at[code, "times_of_inclusion"] < 10:
                df_trader.drop(index=code, inplace=True)
        # 删除df_trader中标的----End
        # 用df_chip初始化df_trader----Begin
        if code in df_chip.index and code in df_trader.index:
            df_trader.at[code, "total_mv_E"] = df_chip.at[code, "total_mv_E"]
            df_trader.at[code, "ssb_index"] = df_chip.at[code, "ssb_index"]
            now_price = df_trader.at[code, "now_price"]
            now_price_ratio = round(df_chip.at[code, "now_price_ratio"], 1)
            G_price = df_chip.at[code, "G_price"]
            if (
                pd.isnull(df_trader.at[code, "recent_price"])
                or df_trader.at[code, "recent_price"] == 0
            ):
                df_trader.at[code, "recent_price"] = now_price
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
            if pd.isnull(df_trader.at[code, "date_of_inclusion_latest"]):
                df_trader.at[code, "date_of_inclusion_latest"] = df_trader.at[
                    code, "date_of_inclusion_first"
                ]
            if pd.notnull(df_trader.at[code, "date_of_inclusion_first"]):
                days_inclusion = (
                    dt_date_trading - df_trader.at[code, "date_of_inclusion_first"]
                )
                days_inclusion = days_inclusion.days + 1
                days_inclusion = (
                    days_inclusion // 7 * 5 + days_inclusion % 7
                )  # 修正除数，尽可能趋近交易日
                df_trader.at[code, "rate_of_inclusion"] = round(
                    df_trader.at[code, "times_of_inclusion"] / days_inclusion * 100, 2
                )
            if pd.isnull(df_trader.at[code, "price_of_inclusion"]):
                df_trader.at[code, "price_of_inclusion"] = G_price
        # 用df_chip初始化df_trader-----End
    # 保存df_trader----Begin
    """
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
        "total_mv_E",
        "ssb_index",
        "stock_index",
        "grade",
        "recent_trading",
        "ST",
        "industry_code",
        "industry_name",
        "date_of_inclusion_first",
        "date_of_inclusion_latest",
        "times_of_inclusion",
        "rate_of_inclusion",
        "price_of_inclusion",
        "pct_of_inclusion",
        "remark",
    ]
    df_trader = df_trader.reindex(columns = list_trader_columns)
    """
    analysis.write_obj_to_db(
        obj=df_trader, key="df_trader", filename=filename_chip_shelve
    )
    # 保存df_trader----End
    # 创建df_signal----Begin
    if os.access(path=filename_signal, mode=os.F_OK):
        df_signal_sell = pd.read_excel(
            io=filename_signal, sheet_name="sell", index_col=0
        )
        df_signal_buy = pd.read_excel(io=filename_signal, sheet_name="buy", index_col=0)
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
        with pd.ExcelWriter(path=filename_trader_template, mode="w") as writer:
            df_modified.to_excel(excel_writer=writer, sheet_name="modified")
            df_add.to_excel(excel_writer=writer, sheet_name="add")
            df_delete.to_excel(excel_writer=writer, sheet_name="delete")
    else:
        pass
    # 创建空的交易员模板 file_name_trader End
    # 取得仓位控制提示
    str_pos_ctl_zh = analysis.position(index="sh000001")
    str_pos_ctl_csi1000 = analysis.position(index="sh000852")
    """init End"""
    """loop Begin"""
    while True:
        if frq > 2:
            os.system("cls")
        dt_now = datetime.datetime.now()
        if frq % 3 == 0:
            str_msg_concentration_rate = analysis.concentration_rate()
        # 开盘前：9:10 至 9:30
        if dt_am_0910 < dt_now < dt_am_start:
            print(f"The exchange will open at {dt_am_start}")
            sleep_to_time(dt_am_start)
        # 盘中 9:30 -- 11:30 and 13:00 -- 15:00
        elif (dt_am_start <= dt_now <= dt_am_end) or (
            dt_pm_start <= dt_now <= dt_pm_end
        ):
            start_loop_time = time.perf_counter_ns()
            logger.trace(f"start_loop_time = {start_loop_time}")

            # 主循环块---------Start------Start-----Start-----Start----Start-------Start----Start------
            # 增加修改删除df_data中的项目 Begin
            str_msg_modified = ""
            str_msg_add = ""
            str_msg_del = ""
            if os.path.exists(filename_input):
                df_in_modified = pd.read_excel(
                    io=filename_input, sheet_name="modified", index_col=0
                )
                df_in_add = pd.read_excel(
                    io=filename_input, sheet_name="add", index_col=0
                )
                df_in_del = pd.read_excel(
                    io=filename_input, sheet_name="delete", index_col=0
                )
                # 索引转为小写字母 Begin
                df_in_modified.index = df_in_modified.index.str.lower()
                df_in_add.index = df_in_add.index.str.lower()
                df_in_del.index = df_in_del.index.str.lower()
                # 索引转为小写字母 End
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
                        if code in df_trader.index:
                            series_add_index = df_in_modified.loc[code].index
                            for item in series_add_index:
                                if pd.notnull(df_in_modified.at[code, item]):
                                    df_trader.at[code, item] = df_in_modified.at[
                                        code, item
                                    ]
                        else:
                            list_in_modified.remove(code)
                    if list_in_modified:
                        str_msg_modified = f" {list_in_modified}"
                    else:
                        str_msg_modified = ""
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
                    str_msg_add = f"{list_in_add}"
                if len(list_in_del) > 0:
                    df_in_del["recent_trading"] = dt_now
                    for code in list_in_del:
                        if code in df_trader.index:
                            if df_trader.at[code, "position"] <= 0:
                                df_trader.drop(index=code, inplace=True)
                            else:
                                list_in_del.remove(code)
                    str_msg_del = f"{list_in_del}"
                str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                file_name_input_rename = os.path.join(
                    path_history, f"input_{str_now_input}.xlsx"
                )
                try:
                    os.rename(src=filename_input, dst=file_name_input_rename)
                except Exception as e:
                    logger.error(f"[{filename_input}] rename file fail - {repr(e)}")
                # df_trader["recent_price"].fillna(0, inplace=True)
                df_trader["position"].fillna(0, inplace=True)
                df_trader["rise"].fillna(rise, inplace=True)
                df_trader["fall"].fillna(fall, inplace=True)
            else:
                pass
            # 增加修改删除df_data中的项目 End
            # 调用实时数据接口，更新df_realtime Begin
            df_realtime = realtime_quotations(
                stock_codes=df_trader.index.to_list()
            )  # 调用实时数据接口
            if df_realtime.empty:
                sys.exit()
            # 调用实时数据接口，更新df_realtime End
            df_stock_market_activity_legu = ak.stock_market_activity_legu()
            df_stock_market_activity_legu.at[10, "value"] = (
                df_stock_market_activity_legu.at[10, "value"]
                .replace("\n", "")
                .replace("\t", "")
            )
            df_stock_market_activity_legu.at[11, "value"] = pd.to_datetime(
                df_stock_market_activity_legu.at[11, "value"]
            )
            str_stock_market_activity_items = (
                fg.red(f"{df_stock_market_activity_legu.at[0, 'item']}   ")
                + fg.green(f"{df_stock_market_activity_legu.at[4, 'item']}   ")
                + f"{df_stock_market_activity_legu.at[1, 'item']}   "
                f"{df_stock_market_activity_legu.at[5, 'item']}   "
                f"{df_stock_market_activity_legu.at[2, 'item']}   "
                f"{df_stock_market_activity_legu.at[6, 'item']}   "
                f"{df_stock_market_activity_legu.at[3, 'item']}   "
                f"{df_stock_market_activity_legu.at[7, 'item']}   "
                f"{df_stock_market_activity_legu.at[8, 'item']}   "
                f"{df_stock_market_activity_legu.at[9, 'item']}   "
                f"{df_stock_market_activity_legu.at[10, 'item']}   "
                f"{df_stock_market_activity_legu.at[11, 'value'].date()}"
            )
            str_stock_market_activity_value = (
                fg.red(f"{df_stock_market_activity_legu.at[0, 'value']:4.0f}   ")
                + fg.green(f"{df_stock_market_activity_legu.at[4, 'value']:4.0f}  ")
                + f"{df_stock_market_activity_legu.at[1, 'value']:4.0f}   "
                f"{df_stock_market_activity_legu.at[5, 'value']:4.0f}     "
                f"{df_stock_market_activity_legu.at[2, 'value']:4.0f}       "
                f"{df_stock_market_activity_legu.at[6, 'value']:4.0f}         "
                f"{df_stock_market_activity_legu.at[3, 'value']:4.0f}        "
                f"{df_stock_market_activity_legu.at[7, 'value']:4.0f}      "
                f"{df_stock_market_activity_legu.at[8, 'value']:4.0f}  "
                f"{df_stock_market_activity_legu.at[9, 'value']:4.0f}     "
                f"{df_stock_market_activity_legu.at[10, 'value']}   "
                f"{df_stock_market_activity_legu.at[11, 'value'].time()}"
            )
            # 更新df_data，str_msg_rise，str_msg_fall------Begin
            str_msg_rise = ""
            count_rise = 0
            str_msg_fall = ""
            count_fall = 0
            i = 0
            count_trader = len(df_trader.index)
            for code in df_trader.index:
                i += 1
                dt_now = datetime.datetime.now()
                str_dt_now_time = dt_now.strftime("<%H:%M:%S>")
                str_msg = f"\r{str_dt_now_time}----"
                str_msg += fg.blue(f"[{i:3d}/{count_trader:3d}]")
                print(f"\r{str_msg}\033[K", end="")
                if i >= count_trader:
                    print("\n", end="")  # 调整输出console格式
                try:
                    if not pd.notnull(df_trader.at[code, "industry_code"]):
                        df_trader.at[code, "industry_code"] = df_industry_index.at[
                            code, "industry_code"
                        ]
                    if not pd.notnull(df_trader.at[code, "industry_name"]):
                        df_trader.at[code, "industry_name"] = df_industry_index.at[
                            code, "industry_name"
                        ]
                except KeyError as e:
                    logger.trace(f"df_industry_index KeyError [{repr(e)}]")
                    time.sleep(5)
                industry_code = df_trader.at[code, "industry_code"]
                if code not in df_realtime.index:
                    continue
                df_trader.at[code, "name"] = df_realtime.at[code, "name"]
                now_price = df_realtime.at[code, "close"]
                df_trader.at[code, "now_price"] = now_price
                if (
                    pd.isnull(df_trader.at[code, "recent_price"])
                    or df_trader.at[code, "recent_price"] == 0
                ):
                    df_trader.at[code, "recent_price"] = recent_price = now_price
                else:
                    recent_price = df_trader.at[code, "recent_price"]
                pct_chg = (now_price / recent_price - 1) * 100
                pct_chg = round(pct_chg, 2)
                df_trader.at[code, "pct_chg"] = pct_chg
                if pd.notnull(df_trader.at[code, "price_of_inclusion"]):
                    pct_of_inclusion = (
                        now_price / df_trader.at[code, "price_of_inclusion"] - 1
                    ) * 100
                    pct_of_inclusion = round(pct_of_inclusion, 2)
                else:
                    pct_of_inclusion = 0
                df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
                if not pd.notnull(df_trader.at[code, "position_unit"]):
                    df_trader.at[code, "trx_unit_share"] = analysis.transaction_unit(
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
                    count_rise += 1
                    str_msg_rise += fg.red(
                        f"\n<Sell-{count_rise}>-[{code}_{df_trader.at[code, 'name']}]-"
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
                    )
                    if industry_code in df_industry_rank_pool_selling.index:
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
                            str_msg_rise += (
                                f"\n ---- [Trading: {dt_trading}]"
                                f" - [{df_trader.at[code, 'times_of_inclusion']:3.0f}/{df_trader.at[code, 'rate_of_inclusion']:6.2f}]"
                                f" - [{df_trader.at[code, 'date_of_inclusion_first']}] - [{df_trader.at[code, 'date_of_inclusion_latest']}]"
                            )
                    if pd.notnull(df_trader.at[code, "remark"]):
                        str_msg_rise += f" - {df_trader.at[code, 'remark']}"
                    str_msg_rise += "\n"
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
                    count_fall += 1
                    str_msg_fall += fg.lightgreen(
                        f"\n<Buy-{count_fall}>-[{code}_{df_trader.at[code, 'name']}]-"
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
                    )
                    if industry_code in df_industry_rank_pool_buying.index:
                        str_msg_fall_industry = fg.red(str_msg_fall_industry)
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
                            str_msg_fall += (
                                f"\n ---- [Trading: {dt_trading}]"
                                f" - [{df_trader.at[code, 'times_of_inclusion']:3.0f}/{df_trader.at[code, 'rate_of_inclusion']:6.2f}]"
                                f" - [{df_trader.at[code, 'date_of_inclusion_first']}] - [{df_trader.at[code, 'date_of_inclusion_latest']}]"
                            )
                    if pd.notnull(df_trader.at[code, "remark"]):
                        str_msg_rise += f" - {df_trader.at[code, 'remark']}"
                    str_msg_fall += "\n"
                else:
                    pass
                # df_trader End
            # 更新df_data，str_msg_rise，str_msg_fall------End
            df_trader.sort_values(
                by=["date_of_inclusion_latest", "rate_of_inclusion", "pct_chg"],
                ascending=[True, True, False],
                inplace=True,
            )
            analysis.write_obj_to_db(
                obj=df_trader, key="df_trader", filename=filename_chip_shelve
            )
            if frq % 3 == 0:
                df_trader.to_csv(path_or_buf=filename_data_csv)
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
            dt_now = datetime.datetime.now()
            str_dt_now_time = dt_now.strftime("<%H:%M:%S>")
            if str_msg_fall != "":
                print(
                    f"===={fg.green('<Suggest Buying>')}=================================================="
                )
                print(str_msg_fall)
            if str_msg_rise != "":
                print(
                    f"===={fg.red('<Suggest Selling>')}================================================="
                )
                print(str_msg_rise, "\a")  # 加上“\a”，铃声提醒
            if str_msg_fall != "" or str_msg_rise != "":
                print(
                    f"****{fg.yellow('<Suggest END>')}*****************************************************"
                )
            if str_msg_modified != "":
                str_msg_modified = (
                    f"{str_dt_now_time}----modified: {fg.blue(str_msg_modified)}"
                )
                print(str_msg_modified)
            if str_msg_add != "":
                str_msg_add = f"{str_dt_now_time}----add: {fg.red(str_msg_add)}"
                print(str_msg_add)
            if str_msg_del != "":
                str_msg_del = f"{str_dt_now_time}----remove: {fg.green(str_msg_del)}"
                print(str_msg_del)
            if len(list_signal_chg) > 0:
                print(f"{str_dt_now_time}----New Signal: {list_signal_chg}\a")
            if list_industry_buying:
                print(f"{fg.green(f'Buying: {list_industry_buying}')}")
                print("*" * 108)
            if list_industry_selling:
                print(f"{fg.red(f'Selling: {list_industry_selling}')}")
                print("*" * 108)
            print(str_stock_market_activity_items)
            print(str_stock_market_activity_value)
            print("*" * 108)
            # 主循环块---------End----End-----End----End------End----End------End------End-------End------

            end_loop_time = time.perf_counter_ns()
            interval_time = (end_loop_time - start_loop_time) / 1000000000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            str_msg_loop_end = f"{dt_now}----[{str_gm}]"
            str_msg_loop_ctl_zh = f"{str_dt_now_time}----{fg.red(str_pos_ctl_zh)}"
            str_msg_loop_ctl_csi1000 = (
                f"{str_dt_now_time}----{fg.red(str_pos_ctl_csi1000)}"
            )
            print(f"{str_dt_now_time}----{str_msg_concentration_rate}")
            print(str_msg_loop_ctl_zh)
            print(str_msg_loop_ctl_csi1000)
            # 收盘前集合竟价：14:57 -- 15:00 响玲
            if dt_pm_1457 < dt_now <= dt_pm_end:
                print("\a", end="")
                scan_interval = 60
            dt_now = datetime.datetime.now()
            dt_now_delta = dt_now + datetime.timedelta(seconds=scan_interval)
            sleep_to_time(dt_now_delta)
        # 中午体息时间： 11:30 -- 13:00
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            sleep_to_time(dt_pm_start)
            # -----当前时间与当日指定时间的间隔时间计算-----
        else:
            print("\a\r", end="")
            analysis.unit_net()
            str_pos_ctl_zh = analysis.position(index="sh000001")
            str_pos_ctl_csi1000 = analysis.position(index="sh000852")
            print(str_pos_ctl_zh)
            print(str_pos_ctl_csi1000)
            df_chip = analysis.chip()
            print(df_chip)
            if dt_now < dt_am_0910:
                df_trader.to_csv(path_or_buf=filename_data_csv)
                sleep_to_time(dt_am_start)
            elif dt_now > dt_pm_end:
                logger.trace(f"Program End")
                sys.exit()
        frq += 1
