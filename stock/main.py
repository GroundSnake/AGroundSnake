# modified at 2023/05/18 22::25
import os
import sys
import datetime
import time
import numpy as np
import pyttsx3
import pandas as pd
import akshare as ak
from loguru import logger
from console import fg
import analysis
from analysis import (
    dt_trading,
    dt_init,
    dt_am_0910,
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_1457,
    dt_pm_end,
    dt_pm_2215,
    filename_trader_template,
    path_history,
    filename_log,
    filename_input,
    path_check,
    sleep_to_time,
    str_trading_path,
    phi_a,
    all_chs_code,
    get_trader_columns,
)

__version__ = "3.0.0"
running_state = "NORMAL"  # NORMAL ,DEBUG


def main() -> None:
    if running_state == "NORMAL":
        logger_console_level = "INFO"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    elif running_state == "DEBUG":
        logger_console_level = "TRACE"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    else:
        logger_console_level = "INFO"  # choice of {"TRACE","DEBUG","INFO"，"ERROR"}
    logger.remove()
    logger.add(sink=sys.stderr, level=logger_console_level)
    logger.add(sink=filename_log, level="TRACE", encoding="utf-8")
    logger.trace(f"{__name__} Begin")
    if analysis.is_trading_day() or (running_state == "DEBUG"):
        logger.trace("Betting day")
        print("Betting day")
    else:
        logger.trace("Non betting day")
        print("Non betting day")
        logger.trace("Program OFF")
        sys.exit()
    """init Begin"""
    frq = 0
    scan_interval = 20
    str_msg_concentration_rate = ""
    str_msg_concentration_additional = ""
    str_limit_up = ""
    str_index_ssb_now = dict()
    int_news_id = 0
    int_news_id_latest = 0
    news_update_frq = 4  # 4 Hours
    list_all_code = all_chs_code()
    dict_trader_dtype = get_trader_columns(data_type="dtype")
    dict_trader_default = get_trader_columns(data_type="dict")
    str_stock_market_activity_items = ""
    str_stock_market_activity_value = ""
    # 加载df_trader Begin
    df_trader = analysis.feather_from_file(
        key="df_trader",
    )
    if df_trader.empty:
        list_trader_columns = get_trader_columns(data_type="list")
        list_trader_symbol = ["sh600519", "sz300750"]
        df_trader = pd.DataFrame(index=list_trader_symbol, columns=list_trader_columns)
        df_trader.index.rename(name="code", inplace=True)
        df_trader["recent_trading"] = datetime.datetime.now().replace(microsecond=0)
        df_trader = analysis.init_trader(df_trader=df_trader, sort=True)
        analysis.feather_to_file(
            df=df_trader,
            key="df_trader",
        )
        logger.error("create df_trader and save.")
    df_trader["news"] = ""
    df_trader["remark"] = ""
    df_trader["factor"] = ""
    # 创建空的交易员模板 file_name_trader_template Begin
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
    # 创建空的交易员模板 file_name_trader End
    # 加载df_industry_class Begin
    df_industry_member = analysis.feather_from_file(
        key="df_industry_member",
    )
    if df_industry_member.empty:
        try:
            df_industry_member = pd.read_excel(
                io="df_industry_member.xlsx", index_col=0
            )
        except FileNotFoundError as e:
            print(f"[df_industry_member.xlsx] - {e.args[1]}")
            sys.exit()
        else:
            if df_industry_member.empty:
                logger.error(
                    f"df_industry_member from [df_industry_member.xlsx] is empty"
                )
                sys.exit()
            else:
                analysis.feather_to_file(
                    df=df_industry_member,
                    key="df_industry_member",
                )
    # 加载df_industry_class End
    index_ssb = analysis.IndexSSB(update=False)
    # 加载df_industry_rank_pool Begin
    df_industry_rank_pool = analysis.feather_from_file(
        key="df_industry_rank_pool",
    )
    # 加载df_industry_rank_pool End
    df_industry_rank = analysis.feather_from_file(
        key="df_industry_rank",
    )
    # 加载df_industry_rank_pool End
    df_stocks_pool = analysis.feather_from_file(
        key="df_stocks_pool",
    )
    str_add_stocks = ""
    dt_now = datetime.datetime.now().replace(microsecond=0)
    if not df_stocks_pool.empty:
        line_len = 5
        if dt_now < dt_pm_end:
            dt_inclusion = dt_pm_end
        else:
            i_while = 0
            while i_while <= 15:
                i_while += 1
                dt_inclusion = dt_pm_end + datetime.timedelta(days=i_while)
                if analysis.is_trading_day(dt=dt_inclusion):
                    break
        i = 0
        for code in df_stocks_pool.index:
            i += 1
            str_add_stock = f"[{df_stocks_pool.at[code, 'name']}({code})]"
            if code not in df_trader.index:
                str_add_stock = fg.yellow(str_add_stock)
                df_trader.at[code, "date_of_inclusion_first"] = dt_inclusion
                df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                    code, "recent_price"
                ] = df_stocks_pool.at[code, "now_price"]
                df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                df_trader.at[code, "times_of_inclusion"] = 1
                df_trader.at[code, "factor"] = df_stocks_pool.at[code, "factor"]
                df_trader.at[code, "factor_count"] = df_stocks_pool.at[
                    code, "factor_count"
                ]
            else:
                if (
                    df_trader.at[code, "date_of_inclusion_first"] == dt_init
                    or df_trader.at[code, "date_of_inclusion_latest"] == dt_init
                ):
                    df_trader.at[code, "date_of_inclusion_first"] = dt_inclusion
                    df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                    df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                        code, "recent_price"
                    ] = df_stocks_pool.at[code, "now_price"]
                    df_trader.at[code, "times_of_inclusion"] = 1
                    str_add_stock = fg.yellow(str_add_stock)
                else:
                    if df_trader.at[code, "date_of_inclusion_latest"] != dt_inclusion:
                        df_trader.at[code, "date_of_inclusion_latest"] = dt_inclusion
                        df_trader.at[code, "times_of_inclusion"] += 1
                df_trader.at[code, "factor"] = df_stocks_pool.at[code, "factor"]
                df_trader.at[code, "factor_count"] = df_stocks_pool.at[
                    code, "factor_count"
                ]
                if df_trader.at[code, "position"] > 0:
                    str_add_stock = fg.purple(str_add_stock)    
            if str_add_stocks == "":
                str_add_stocks = f"{str_add_stock}"
            elif i % line_len == 1:
                str_add_stocks += f"\n\r{str_add_stock}"
            else:
                str_add_stocks += f", {str_add_stock}"
    df_trader = analysis.init_trader(df_trader=df_trader, sort=True)
    # 保存df_trader----Begin
    analysis.feather_to_file(
        df=df_trader,
        key="df_trader",
    )
    filename_data_csv = path_check.joinpath(f"trader_{str_trading_path()}.csv")
    df_trader = df_trader.sort_values(by=["position_unit", "pct_chg"], ascending=False)
    df_trader.to_csv(path_or_buf=filename_data_csv)
    # 保存df_trader----End
    # 创建df_signal----Begin
    filename_signal = path_check.joinpath(f"signal_{str_trading_path()}.xlsx")
    if filename_signal.exists():
        df_signal_sell = pd.read_excel(
            io=filename_signal, sheet_name="sell", index_col=0
        )
        df_signal_buy = pd.read_excel(io=filename_signal, sheet_name="buy", index_col=0)
        df_signal_sell.sort_values(
            by=["position", "pct_chg"], ascending=False, inplace=True
        )
        df_signal_buy.sort_values(
            by=["position", "pct_chg"], ascending=False, inplace=True
        )
    else:
        df_signal_sell = pd.DataFrame(columns=df_trader.columns)
        df_signal_buy = pd.DataFrame(columns=df_trader.columns)
    list_signal_buy_before = df_signal_buy.index.tolist()
    list_signal_sell_before = df_signal_sell.index.tolist()
    for column_buy in df_signal_buy.columns:
        df_signal_buy[column_buy] = df_signal_buy[column_buy].astype(
            dtype=dict_trader_dtype[column_buy]
        )
    for column_sell in df_signal_sell.columns:
        df_signal_sell[column_sell] = df_signal_sell[column_sell].astype(
            dtype=dict_trader_dtype[column_sell]
        )
    # 创建df_signal----End
    # 取得仓位控制提示
    str_pos_ctl_zh = analysis.position(index="sh000001")
    str_pos_ctl_csi1000 = analysis.position(index="sh000852")
    """init End"""
    """loop Begin"""
    while True:
        dt_now = datetime.datetime.now().replace(microsecond=0)
        # 开盘前：9:10 至 9:30
        if dt_am_0910 < dt_now < dt_am_start:
            print(f"The exchange will open at 9:30")
            sleep_to_time(dt_time=dt_am_start, seconds=2)
        # 盘中 9:30 -- 11:30 and 13:00 -- 15:00
        elif (dt_am_start <= dt_now <= dt_am_end) or (
            dt_pm_start <= dt_now <= dt_pm_end
        ):
            if frq > 2:
                os.system("cls")
            start_loop_time = time.perf_counter_ns()
            logger.trace(f"start_loop_time = {start_loop_time}")

            # 主循环块---------Start------Start-----Start-----Start----Start-------Start----Start------
            # 增加修改删除df_data中的项目 Begin
            str_msg_modified = ""
            str_msg_add = ""
            str_msg_del = ""
            if filename_input.exists():
                df_in_modified = pd.read_excel(
                    io=filename_input, sheet_name="modified", index_col=0
                )
                df_in_add = pd.read_excel(
                    io=filename_input, sheet_name="add", index_col=0, header=0
                )
                df_in_del = pd.read_excel(
                    io=filename_input, sheet_name="delete", index_col=0
                )
                # 索引转为小写字母 Begin
                try:
                    df_in_modified.index = df_in_modified.index.str.lower()
                    df_in_add.index = df_in_add.index.str.lower()
                    df_in_del.index = df_in_del.index.str.lower()
                except AttributeError:
                    filename_input.unlink()
                # 索引转为小写字母 End
                df_in_modified = df_in_modified[
                    ~df_in_modified.index.duplicated(keep="first")
                ]
                df_in_add = df_in_add[~df_in_add.index.duplicated(keep="first")]
                df_in_del = df_in_del[~df_in_del.index.duplicated(keep="first")]
                list_in_modified = df_in_modified.index.to_list()
                list_in_add = df_in_add.index.to_list()
                list_in_del = df_in_del.index.to_list()
                if len(list_in_modified) > 0:
                    df_in_modified["recent_trading"] = dt_now
                    for code in df_in_modified.index:
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
                    for column_add in df_in_add.columns:
                        if column_add not in dict_trader_default:
                            df_in_add.drop(columns=column_add, inplace=True)
                    for column_add in dict_trader_default:
                        if column_add in df_trader.columns:
                            df_in_add[column_add] = df_in_add[column_add].astype(
                                dtype=dict_trader_dtype[column_add]
                            )
                            df_in_add[column_add].fillna(
                                value=dict_trader_default[column_add], inplace=True
                            )
                        else:
                            df_in_add[column_add] = dict_trader_default[column_add]
                    df_trader = pd.concat(
                        objs=[df_trader, df_in_add.astype(df_trader.dtypes)],
                        axis=0,
                        join="outer",
                    )
                    df_trader = df_trader[~df_trader.index.duplicated(keep="first")]
                    for code in df_trader.index:
                        if code in df_in_add.index:
                            if pd.isnull(df_trader.at[code, "date_of_inclusion_first"]):
                                df_trader.at[
                                    code, "date_of_inclusion_first"
                                ] = dt_trading()
                            else:
                                df_trader.at[
                                    code, "date_of_inclusion_latest"
                                ] = dt_trading()
                            if pd.isnull(df_trader.at[code, "times_of_inclusion"]):
                                df_trader.at[code, "times_of_inclusion"] = 1
                            if pd.isnull(df_trader.at[code, "price_of_inclusion"]):
                                df_trader.at[code, "price_of_inclusion"] = df_trader.at[
                                    code, "now_price"
                                ]
                    str_msg_add = f"{list_in_add}"
                if len(list_in_del) > 0:
                    df_in_del["recent_trading"] = dt_now
                    for code in df_in_del.index:
                        if code in df_trader.index:
                            if df_trader.at[code, "position"] <= 0:
                                df_trader.drop(index=code, inplace=True)
                            else:
                                list_in_del.remove(code)
                    str_msg_del = f"{list_in_del}"
                str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                file_name_input_rename = path_history.joinpath(
                    f"input_{str_now_input}.xlsx"
                )
                df_trader = analysis.init_trader(df_trader=df_trader, sort=False)
                try:
                    filename_input.rename(target=file_name_input_rename)
                except Exception as e:
                    logger.error(f"[{filename_input}] rename file fail - {repr(e)}")
                    time.sleep(2)
            # 增加修改删除df_data中的项目 End
            try:
                df_stock_market_activity_legu = ak.stock_market_activity_legu()
            except TypeError as e:
                print(f"{e}")
            else:
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
            # 调用实时数据接口，更新df_realtime Begin
            i_realtime = 0
            df_realtime = pd.DataFrame()
            while i_realtime <= 2:
                i_realtime += 1
                # list_trader = [item[2:] for item in list_trader]
                # df_realtime = client_mootdx.quotes(symbol=list_trader)
                df_realtime = analysis.realtime_quotations(
                    stock_codes=df_trader.index.to_list()
                )  # 调用实时数据接口
                if not df_realtime.empty:
                    break
            if df_realtime.empty:
                logger.error("df_realtime is empty, sys exit")
                sys.exit()
            # 调用实时数据接口，更新df_realtime End
            if frq % 3 == 0:
                int_news_id = analysis.update_news(
                    start_id=int_news_id_latest, hours=news_update_frq
                )
                str_index_ssb_now = index_ssb.realtime_index()
            df_news = analysis.base.feather_from_file(
                key="df_news",
            )
            list_signal_on_sell = list()
            list_signal_on_buy = list()
            dt_now = datetime.datetime.now().replace(microsecond=0)
            str_dt_now_time = dt_now.strftime("<%H:%M:%S>")
            i = 0
            count_trader = df_trader.shape[0]
            for code in df_trader.index:
                i += 1
                print(f"\r[{i}/{count_trader}] - [{code}]\033[K", end="")
                if i >= count_trader:
                    time.sleep(1)
                    print("\r\033[K", end="")
                if code in df_realtime.index:
                    now_price = df_realtime.at[code, "close"]
                else:
                    if code not in list_all_code:
                        df_trader.drop(index=code, inplace=True)
                        print(f"[{code}] not in df_realtime and drop [{code}]")
                        time.sleep(10)
                        continue
                    now_price = 0
                # now_price = df_realtime.at[code, "price"]
                pct_chg = (now_price / df_trader.at[code, "recent_price"] - 1) * 100
                pct_chg = round(pct_chg, 2)
                if df_trader.at[code, "price_of_inclusion"] != 0:
                    pct_of_inclusion = (
                        now_price / df_trader.at[code, "price_of_inclusion"] - 1
                    ) * 100
                    pct_of_inclusion = round(pct_of_inclusion, 2)
                else:
                    pct_of_inclusion = 0
                df_trader.at[code, "now_price"] = now_price
                df_trader.at[code, "pct_chg"] = pct_chg
                df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
                if int_news_id > int_news_id_latest:
                    df_trader.at[code, "news"] = analysis.get_stock_news(
                        df_news=df_news,
                        stock=df_trader.at[code, "name"],
                    )
                if (
                    pct_chg >= df_trader.at[code, "rise"]
                    and df_trader.at[code, "position"] > 0
                ):
                    df_signal_sell.loc[code] = df_trader.loc[code]
                    list_signal_on_sell.append(code)
                elif pct_chg <= df_trader.at[code, "fall"]:
                    df_signal_buy.loc[code] = df_trader.loc[code]
                    list_signal_on_buy.append(code)
                elif (
                    df_trader.at[code, "recent_trading"] <= dt_init
                    and 19.1 <= df_trader.at[code, "gold_section_price"] <= 38.2
                    and 19.1 <= df_trader.at[code, "gold_section_volume"] <= 38.2
                    and df_trader.at[code, "gold_pct_max_min"] >= 50
                    and df_trader.at[code, "gold_date_max"]
                    > df_trader.at[code, "gold_date_min"]
                    and df_trader.at[code, "gold_price_min"]
                    < now_price
                    < df_trader.at[code, "G_price"]
                ):
                    df_trader.at[code, "remark"] = "Gold Section"
                    df_signal_buy.loc[code] = df_trader.loc[code]
                    list_signal_on_buy.append(code)
                if code in df_signal_sell.index:
                    df_signal_sell.loc[code] = df_trader.loc[code]
                if code in df_signal_buy.index:
                    df_signal_buy.loc[code] = df_trader.loc[code]
            if int_news_id > int_news_id_latest:
                print(f"{str_dt_now_time}----{fg.red('News Update.')}")
                int_news_id_latest = int_news_id
            analysis.feather_to_file(
                df=df_trader,
                key="df_trader",
            )
            list_signal_buy_after = df_signal_buy.index.tolist()
            list_signal_sell_after = df_signal_sell.index.tolist()
            list_signal_chg = list()
            for code in list_signal_buy_after:
                if code not in list_signal_buy_before:
                    list_signal_chg.append(code)
            for code in list_signal_sell_after:
                if code not in list_signal_sell_before:
                    list_signal_chg.append(code)
            if list_signal_chg:
                filename_signal = path_check.joinpath(f"signal_{str_trading_path()}.xlsx")
                with pd.ExcelWriter(path=filename_signal, mode="w") as writer:
                    df_signal_sell.to_excel(excel_writer=writer, sheet_name="sell")
                    df_signal_buy.to_excel(excel_writer=writer, sheet_name="buy")
            list_signal_sell_before = list_signal_sell_after.copy()
            list_signal_buy_before = list_signal_buy_after.copy()
            if not df_signal_buy.empty:
                df_signal_buy.sort_values(
                    by=[
                        "position",
                        "recent_trading",
                        "gold_section",
                        "gold_section_volume",
                        "gold_pct_max_min",
                        "dividend_rate",
                        "max_min",
                        "factor_count",
                        "profit_rate",
                        "rate_of_inclusion",
                        "times_of_inclusion",
                        "pct_chg",
                    ],
                    ascending=[
                        True,
                        True,
                        False,
                        False,
                        False,
                        True,
                        True,
                        True,
                        True,
                        True,
                        True,
                        False,
                    ],
                    inplace=True,
                )
            if not df_signal_sell.empty:
                df_signal_sell.sort_values(
                    by=[
                        "factor_count",
                        "profit_rate",
                        "rate_of_inclusion",
                        "times_of_inclusion",
                        "pct_chg",
                    ],
                    ascending=[True, True, True, True, False],
                    inplace=True,
                )
            list_signal_t0 = list()
            for code in df_signal_buy.index:
                if code in df_signal_sell.index:
                    list_signal_t0.append(code)
            dict_list_signal_on = {
                "Buy": list_signal_on_buy,
                "Sell": list_signal_on_sell,
            }
            dict_df_signal = {
                "Buy": df_signal_buy,
                "Sell": df_signal_sell,
            }
            msg_signal_chg = ""
            msg_signal_t0 = ""
            for item in dict_df_signal:
                msg_signal = ""
                if item in "Buy":
                    str_arrow = "↓"
                elif item in "Sell":
                    str_arrow = "↑"
                else:
                    str_arrow = "|"
                i_records = 0
                df_item = dict_df_signal[item]
                for code in df_item.index:
                    i_records += 1
                    if code not in dict_list_signal_on[item]:
                        continue
                    msg_signal_code_1 = (
                        f"<{item}-{i_records}>-[{code}_{df_item.at[code, 'name']}]-"
                        f"<{df_item.at[code, 'now_price']:5.2f}_{str_arrow}_{df_item.at[code, 'pct_chg']:5.2f}%>"
                    )
                    msg_signal_code_2 = (
                        f"[{df_item.at[code, 'recent_price']:5.2f} * "
                        f"{int(df_item.at[code, 'position']):4d}:( "
                        f"{df_item.at[code, 'position_unit']:3.1f}*"
                        f"{int(df_item.at[code, 'trx_unit_share']):3d})]"
                    )
                    if df_item.at[code, "position"] > 0:
                        msg_signal_code_2 = fg.red(msg_signal_code_2)
                    msg_signal_code_3 = (
                        f"[{df_item.at[code, 'ST']}]"
                        f"_({df_item.at[code, 'profit_rate']}%PR"
                        f" / {df_item.at[code, 'dividend_rate']}%DR)"
                        f"_({int(df_item.at[code, 'cash_div_period'])}"
                        f"/{int(df_item.at[code, 'cash_div_excepted_period'])})Y"
                    )
                    if (
                        df_item.at[code, "profit_rate"] >= 5
                        and df_item.at[code, "dividend_rate"] >= 0.05
                        and df_item.at[code, "cash_div_period"]
                        >= df_item.at[code, "cash_div_excepted_period"]
                    ):
                        msg_signal_code_3 = fg.purple(msg_signal_code_3)
                    elif "ST" in msg_signal_code_3:
                        msg_signal_code_3 = fg.green(msg_signal_code_3)
                    elif "A+" in msg_signal_code_3:
                        msg_signal_code_3 = fg.yellow(msg_signal_code_3)
                    industry_code = df_item.at[code, "industry_code"]
                    msg_signal_code_4 = (
                        f"[{df_item.at[code, 'industry_name']}_({industry_code})]"
                    )
                    if industry_code in df_industry_rank_pool.index:
                        if (
                            df_industry_rank_pool.at[industry_code, "T5_rank"] >= 56
                            and df_industry_rank.at[industry_code, "T1_rank"] >= 66
                        ):
                            msg_signal_code_4 = fg.purple(msg_signal_code_4)
                            msg_signal_code_4 = fg.purple(msg_signal_code_4)
                    msg_signal_code_5 = (
                        f"[Diff={df_industry_rank.at[industry_code, 'max_min']:02.0f}]"
                    )
                    if df_industry_rank.at[industry_code, "max_min"] >= 60:
                        msg_signal_code_5 = fg.purple(msg_signal_code_5)
                    msg_signal_code_6 = (
                        f"[{df_industry_rank.at[industry_code, 'T1_rank']:2.0f} - "
                        f"{df_industry_rank.at[industry_code, 'T5_rank']:02.0f} - "
                        f"{df_industry_rank.at[industry_code, 'T20_rank']:02.0f} - "
                        f"{df_industry_rank.at[industry_code, 'T40_rank']:02.0f} - "
                        f"{df_industry_rank.at[industry_code, 'T60_rank']:02.0f} - "
                        f"{df_industry_rank.at[industry_code, 'T80_rank']:02.0f}]"
                    )
                    if df_item.at[code, "max_min"] >= 45:
                        msg_signal_code_5 = fg.yellow(msg_signal_code_5)
                        if df_industry_rank.at[industry_code, "T5_rank"] >= 60:
                            msg_signal_code_6 = fg.purple(msg_signal_code_6)
                        elif df_industry_rank.at[industry_code, "T5_rank"] <= 16:
                            msg_signal_code_6 = fg.green(msg_signal_code_6)
                    msg_signal_code_7 = (
                        f"[EX_ts:{df_item.at[code, 'times_exceed_correct_industry']}"
                        f" - EX_avg:{df_item.at[code, 'mean_exceed_correct_industry']:5.2f}]"
                    )
                    if (
                        df_item.at[code, "times_exceed_correct_industry"] >= 60
                        and df_item.at[code, "mean_exceed_correct_industry"] >= 1.3
                    ):
                        msg_signal_code_7 = fg.purple(msg_signal_code_7)
                    msg_signal_code_amplitude = (
                        f"[7pct_Ts:{int(df_item.at[code, '7Pct_T']):2d} - "
                        f"T5_pct:{df_item.at[code, 'T5_pct']:.2f} - "
                        f"T5_amp:{df_item.at[code, 'T5_amplitude']:.2f}]"
                    )
                    if (
                        df_item.at[code, "7Pct_T"] > 1
                        and df_item.at[code, "T5_pct"] > 5
                        and df_item.at[code, "T5_amplitude"] > 3
                    ):
                        msg_signal_code_amplitude = fg.purple(msg_signal_code_amplitude)
                    msg_signal_code_10 = (
                        f"[Rate:{df_item.at[code, 'rate_of_inclusion']:6.2f}%"
                        f" - Inclusion:{int(df_item.at[code, 'times_of_inclusion'])}]"
                    )
                    if (
                        df_item.at[code, "times_of_inclusion"] >= 5
                        and df_item.at[code, "rate_of_inclusion"] > phi_a
                    ):
                        msg_signal_code_10 = fg.purple(msg_signal_code_10)
                    msg_signal_code_11 = (
                        f"[RT: {df_item.at[code, 'recent_trading'].date()}]"
                        f" - [FD: {df_item.at[code, 'date_of_inclusion_first'].date()}]"
                        f" - [LD: {df_item.at[code, 'date_of_inclusion_latest'].date()}]"
                    )
                    if df_item.at[code, "recent_trading"] != dt_init:
                        msg_signal_code_11 = fg.purple(msg_signal_code_11)
                    msg_signal_code_12 = (
                        f"CON:[{df_item.at[code, 'rate_concentration']:5.2f}%"
                        f" - {df_item.at[code, 'times_concentration']:3.0f}]"
                        f" - [SSB:{df_item.at[code, 'ssb_index']}"
                        f" - {df_item.at[code, 'total_mv_E']}E]"
                    )
                    msg_signal_code_gold = (
                        f"[GS:{df_item.at[code, 'gold_section']:.2f}%-"
                        f"(GS_p:{df_item.at[code, 'gold_section_price']:.2f}% - "
                        f"GS_v:{df_item.at[code, 'gold_section_volume']:.2f}%) - "
                        f"PCT_MAX:{df_item.at[code, 'gold_pct_max_min']:.2f}%"
                    )
                    if (
                        df_item.at[code, "gold_date_max"]
                        > df_item.at[code, "gold_date_min"]
                    ):
                        msg_signal_code_gold += " - O]"
                    else:
                        msg_signal_code_gold += " - X]"
                    if (
                        19.1 <= df_item.at[code, "gold_section"] <= 38.2
                        and 19.1 <= df_item.at[code, "gold_section_price"] <= 38.2
                        and 19.1 <= df_item.at[code, "gold_section_volume"] <= 38.2
                        and df_item.at[code, "gold_pct_max_min"] >= 50
                        and df_item.at[code, "gold_date_max"]
                        > df_item.at[code, "gold_date_min"]
                        and df_item.at[code, "gold_price_min"]
                        < df_item.at[code, "now_price"]
                        < df_item.at[code, "G_price"]
                    ):
                        msg_signal_code_gold = fg.yellow(msg_signal_code_gold)
                        if (
                            df_item.at[code, "gold_section"] <= 28.65
                            and df_item.at[code, "gold_section_price"] <= 28.65
                            and df_item.at[code, "gold_section_volume"] <= 28.65
                        ):
                            msg_signal_code_gold = fg.purple(msg_signal_code_gold)
                    if df_item.at[code, "remark"] != "":
                        msg_signal_code_remark = fg.red(
                            f" - Remark:{df_item.at[code, 'remark']}"
                        )
                    else:
                        msg_signal_code_remark = ""

                    if df_item.at[code, "factor"] != "":
                        msg_signal_code_14 = fg.red(
                            f"\n**** {df_item.at[code, 'factor']}"
                        )
                    else:
                        msg_signal_code_14 = ""
                    if df_item.at[code, "news"] != "":
                        msg_signal_code_15 = f"\nNews: {df_item.at[code, 'news']}"
                    else:
                        msg_signal_code_15 = ""
                    if item in "Buy":
                        msg_signal_code_1 = fg.lightgreen(msg_signal_code_1)
                    elif item in "Sell":
                        msg_signal_code_1 = fg.red(msg_signal_code_1)
                    msg_signal_code = (
                        "\n"
                        + msg_signal_code_1
                        + " - "
                        + msg_signal_code_2
                        + " - "
                        + msg_signal_code_3
                        + "\n---- "
                        + msg_signal_code_4
                        + " - "
                        + msg_signal_code_5
                        + " - "
                        + msg_signal_code_6
                        + " - "
                        + msg_signal_code_7
                        + "\n---- "
                        + msg_signal_code_amplitude
                        + " - "
                        + msg_signal_code_gold
                        + "\n---- "
                        + msg_signal_code_10
                        + " - "
                        + msg_signal_code_11
                        + "\n---- "
                        + msg_signal_code_12
                        + msg_signal_code_14
                        + msg_signal_code_remark
                        + msg_signal_code_15
                        + "\n"
                    )
                    if code in list_signal_chg:
                        msg_signal_chg += msg_signal_code
                    elif code in list_signal_t0:
                        msg_signal_t0 += msg_signal_code
                    else:
                        msg_signal += msg_signal_code
                if msg_signal:
                    if item in "Sell":
                        print(f"====<Suggest {item}>====\a", "=" * 60)
                        print(msg_signal)
                        pyttsx3.speak("Sell signal")
                    else:
                        print(f"====<Suggest {item}>====", "=" * 61)
                        print(msg_signal)
            if msg_signal_t0:
                print(f"====<T0>====", "=" * 74)
                print(msg_signal_t0)
                pyttsx3.speak("Reverse trading signal")
            if msg_signal_chg:
                print(f"====<Change>====", "=" * 70)
                print(msg_signal_chg)
                pyttsx3.speak("Changing trading signals")
            # 更新df_data，str_msg_rise，str_msg_fall------End
            if str_msg_modified:
                str_msg_modified = (
                    f"{str_dt_now_time}----modified: {fg.blue(str_msg_modified)}"
                )
                print("=" * 86)
                print(str_msg_modified)
                pyttsx3.speak("Record modified")
            if str_msg_add:
                str_msg_add = f"{str_dt_now_time}----add: {fg.red(str_msg_add)}"
                print("=" * 86)
                print(str_msg_add)
                pyttsx3.speak("Record add")
            if str_msg_del:
                str_msg_del = f"{str_dt_now_time}----remove: {fg.green(str_msg_del)}"
                print("=" * 86)
                print(str_msg_del)
                pyttsx3.speak("Record deleted")
            if str_index_ssb_now:
                print("=" * 108)
                print(f"------{dt_now}------")
                print(str_index_ssb_now)
            if str_stock_market_activity_items or str_stock_market_activity_value:
                print("#" * 108)
                print(str_stock_market_activity_items)
                print(str_stock_market_activity_value)
                print("#" * 108)
            str_cb = analysis.realtime_cb()
            if str_cb:
                print(f"===Convertible Bonds==={dt_now}", "=" * 58)
                print(str_cb)
                print("=" * 108)
            if str_add_stocks:
                print(f"===Add Stocks pool==={dt_now}", "=" * 60)
                print(str_add_stocks)
                print("=" * 108)
            if frq % 3 == 0:
                filename_data_csv = path_check.joinpath(
                    f"trader_{str_trading_path()}.csv"
                )
                df_trader_csv = df_trader.sort_values(by=["pct_chg"], ascending=False)
                df_trader_csv.to_csv(path_or_buf=filename_data_csv)
                (
                    str_msg_concentration_rate,
                    str_msg_concentration_additional,
                ) = analysis.concentration_rate()
            if frq % 6 == 0:  # 3 = 1 minutes, 6 = 2 minutes, 15 = 5 minutes
                str_limit_up = analysis.limit_up_today(
                    df_trader=df_trader, df_stocks_pool=df_stocks_pool
                )
            """
            if frq % 15 == 0:  # 3 = 1 minutes, 6 = 2 minutes, 15 = 5 minutes
                str_wc_use = analysis.volume_price_rise(df_trader)
            """
            if str_msg_concentration_rate and str_msg_concentration_additional:
                print(f"{str_dt_now_time}----{str_msg_concentration_rate}")
                print(f"{str_dt_now_time}----{str_msg_concentration_additional}")
            str_msg_loop_ctl_zh = f"{str_dt_now_time}----{fg.red(str_pos_ctl_zh)}"
            str_msg_loop_ctl_csi1000 = (
                f"{str_dt_now_time}----{fg.red(str_pos_ctl_csi1000)}"
            )
            print(str_msg_loop_ctl_zh)
            print(str_msg_loop_ctl_csi1000)
            if str_limit_up:
                print(f"===Limit Up=== {dt_now}", "=" * 66)
                print(str_limit_up)
                print("=" * 108)
            """
            if str_wc_use:
                print("===Convertible_Bonds===", "=" * 65)
                print(str_wc_use)
                print("=" * 108)
            """
            # 主循环块---------End----End-----End----End------End----End------End------End-------End------
            end_loop_time = time.perf_counter_ns()
            interval_time = (end_loop_time - start_loop_time) / 1000000000
            str_gm = time.strftime("%H:%M:%S", time.gmtime(interval_time))
            str_msg_loop_end = f"{str_dt_now_time}----[{str_gm}]"
            print(str_msg_loop_end)
            # 收盘前集合竟价：14:57 -- 15:00 响玲
            if dt_pm_1457 < dt_now <= dt_pm_end:
                pyttsx3.speak("Collective bidding time.")
                scan_interval = 60
            dt_now = datetime.datetime.now().replace(microsecond=0)
            dt_now_delta = dt_now + datetime.timedelta(seconds=scan_interval)
            sleep_to_time(dt_time=dt_now_delta, seconds=2)
        # 中午休息时间： 11:30 -- 13:00
        elif dt_am_end < dt_now < dt_pm_start:
            # -----当前时间与当日指定时间的间隔时间计算-----
            sleep_to_time(dt_time=dt_pm_start, seconds=4)
            # -----当前时间与当日指定时间的间隔时间计算-----
        else:
            pyttsx3.speak("Outside of trading hours and Update chip.")
            df_chip = analysis.chip()
            print("\n", df_chip)
            dt_now = datetime.datetime.now().replace(microsecond=0)
            if dt_now < dt_am_0910:
                sleep_to_time(dt_time=dt_am_start, seconds=2)
            elif dt_pm_end < dt_now < dt_pm_2215:
                print(f"sleep to [{dt_pm_2215}]")
                sleep_to_time(dt_time=dt_pm_2215, seconds=2)
                logger.trace(f"Program End")
                print(f"Program End")
                if running_state == "NORMAL":
                    print(f"The program will shut down the computer.")
                    time.sleep(30)
                    os.system("shutdown -s -t 15")
                else:
                    sys.exit()
            elif dt_now > dt_pm_2215:
                logger.trace(f"The Program End")
                print(f"The Program End")
                if running_state == "NORMAL":
                    print(f"The program will shut down the computer.")
                    time.sleep(30)
                    os.system("shutdown -s -t 15")
                else:
                    sys.exit()
        frq += 1


if __name__ == "__main__":
    main()
