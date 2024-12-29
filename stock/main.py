import os
import sys
import time
import datetime
import pandas as pd
from console import fg
from analysis.win32_speak import say
import threading
from pathlib import Path
from analysis.log import log_json, logger
from analysis.list_etf_t0 import list_etfs_t0
from analysis.const_dynamic import (
    path_history,
    dt_am_0929,
    dt_am_start,
    dt_pm_start,
    dt_am_end,
    dt_pm_1457,
    dt_am_1129,
    dt_pm_end,
    filename_scan_modified_xlsx,
    filename_scan_ftr,
    filename_scan_csv,
    filename_signal_xlsx,
    dict_scan_dtype,
    dict_scan_value_default,
    format_dt,
    now_price_max,
    phi_s,
    days_delete,
)
from analysis.global_variable import context
from analysis.base import feather_from_file, feather_to_file, sleep_to_time, csv_to_file
from analysis.analysis import get_pool, get_analysis
from analysis.market import (
    market_activity,
    market_activity_charts,
    get_grid_stock,
    get_grid_etf,
)
from analysis.concentration import (
    grt_str_realtime_concentration_rate,
    concentration_rate_chart,
    get_concentration_rate,
)
from analysis.limit_today import get_limit_up_today, get_limit_down_today
from analysis.control_position import control_position_index
from analysis.etf import check_etf_t0
from analysis.strong import get_strong_stocks, strong_stocks
from analysis.realtime_quotation import realtime_quotation


class Main(object):
    def __init__(self) -> None:
        self.position_unit_max = 7
        self.unit = 2000
        self.loop = 0
        self.scan_delta = 2
        self.scan_states = 10
        self.scan_print = 11
        self.scan_signal = 20
        self.scan_min = 20
        self.scan_median = 30
        self.scan_max = 180
        self.i_thread = 1
        self.str_etf = fg.yellow("ETF")
        self.str_stock = fg.blue("STOCK")
        self.str_c_r = fg.red("CONCENTRATION")
        self.str_pos_r = fg.yellow("POSITION")
        self.str_m_r = fg.blue("MARKET")
        self.df_pool = get_pool(treading=True)
        self.list_sub_threads = list()
        self.trading = True
        self.trading_exit = False
        self.trading_pause = False
        self.str_concentration_rate = grt_str_realtime_concentration_rate()
        self.list_check_etf_t0 = check_etf_t0()
        list_market_activity = market_activity()
        self.str_market_activity = list_market_activity[0]
        self.bool_not_long_crash = list_market_activity[1]
        self.bool_not_warning = list_market_activity[2]
        self.bool_not_long_warning = list_market_activity[3]
        self.str_pos_ctl_zh = control_position_index(index="sh000001")
        self.str_pos_ctl_sz399001 = control_position_index(index="sz399001")
        self.str_pos_ctl_csi1000 = control_position_index(index="sh000852")
        self.str_pos_ctl_sz399303 = control_position_index(index="sz399303")
        self.str_pos_ctl_sz399006 = control_position_index(index="sz399006")
        self.str_strong_stocks_1 = get_strong_stocks(days=1)
        self.str_strong_stocks_2 = get_strong_stocks(days=2)
        self.grid_stock = get_grid_stock()
        self.grid_etf = get_grid_etf()
        self.df_scan = self.create_scan(filename=filename_scan_ftr)
        self.add_stock_to_scan()
        self.init_scan()
        self.list_position = self.df_scan[self.df_scan["position"] > 0].index.tolist()
        self.str_limit_up_today = get_limit_up_today(stocks=self.list_position)
        self.str_limit_down_today = get_limit_down_today(stocks=self.list_position)
        self.str_position_gt_max = self.get_gt_position_max()
        dt_now = datetime.datetime.now()
        str_initialization = (
            f"<{dt_now.time().replace(microsecond=0)}>----"
            f"[Initialization Main Finish!]"
        )
        self.str_msg_signals = ""
        self.set_states()
        print(str_initialization)
        return

    def get_gt_position_max(self) -> str:
        str_msg = ""
        list_position_gt_max = self.df_scan[
            self.df_scan["position_unit"] > self.position_unit_max
        ].index.tolist()
        if len(list_position_gt_max) <= 0:
            return str_msg
        df_stocks_a = realtime_quotation.get_stocks_a()
        list_symbol_stocks = df_stocks_a.index.tolist()
        line_len = 3
        i_msg = 0
        for symbol in list_position_gt_max:
            i_msg += 1
            str_symbol = (
                f"[{self.df_scan.at[symbol, 'name']}({symbol}) "
                f"Unit:{self.df_scan.at[symbol, "position_unit"]}]"
            )
            if symbol in list_symbol_stocks:
                str_symbol = fg.red(str_symbol)
            if str_msg == "":
                str_msg = f"{str_symbol}"
            elif i_msg % line_len == 1:
                str_msg += f"\n{str_symbol}"
            else:
                str_msg += f", {str_symbol}"
        return str_msg

    def run(self) -> None:
        logger.debug("Run Start")
        dt_now = datetime.datetime.now()
        if dt_now < dt_am_0929:
            print(f"The exchange will open at 9:30")
            sleep_to_time(dt_time=dt_am_0929, seconds=5)
        thread1 = threading.Thread(
            target=realtime_quotation.update_while_trading, daemon=True
        )
        thread2 = threading.Thread(target=self.update_else_min, daemon=True)
        thread3 = threading.Thread(target=self.update_else_median, daemon=True)
        thread4 = threading.Thread(target=self.update_else_max, daemon=True)
        thread5 = threading.Thread(target=self.update_str_msg_signals, daemon=True)
        thread6 = threading.Thread(target=self.update_print_msg, daemon=True)
        thread7 = threading.Thread(target=self.update_states, daemon=True)
        logger.debug("update_while_trading Start")
        thread1.start()
        time.sleep(0.1)
        logger.debug("update_else_min Start")
        thread2.start()
        time.sleep(0.1)
        logger.debug("update_else_median Start")
        thread3.start()
        time.sleep(0.1)
        logger.debug("update_else_max Start")
        thread4.start()
        time.sleep(0.1)
        logger.debug("update_str_msg_signals Start")
        thread5.start()
        time.sleep(1)
        logger.debug("update_print_msg Start")
        thread6.start()
        logger.debug("update_states Start")
        thread7.start()
        self.list_sub_threads = [
            thread1,
            thread2,
            thread3,
            thread4,
            thread5,
            thread6,
            thread7,
        ]
        self.i_thread = threading.active_count()
        while True:
            i_thread = threading.active_count()
            if i_thread < self.i_thread:
                count_sub_threads = count_sub_threads_alive = len(self.list_sub_threads)
                for thread_i in self.list_sub_threads:
                    if not thread_i.is_alive():
                        count_sub_threads_alive -= 1
                        logger.error(
                            f"[{count_sub_threads_alive}/{count_sub_threads}] - "
                            f"{thread_i.name} - Break"
                        )
                if count_sub_threads_alive == 0:
                    logger.debug(f"list_sub_threads{self.list_sub_threads} END")
                    break
            time.sleep(self.scan_states)
        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()
        thread6.join()
        thread7.join()
        print("Outside of trading hours and Update chip.")
        say(text="Outside of trading hours and Update chip.")
        dt_now_delta = dt_pm_end + datetime.timedelta(seconds=1800)
        sleep_to_time(dt_time=dt_now_delta, seconds=10)
        logger.trace("get_pool Begin")
        self.df_pool = get_pool()
        if self.df_pool.empty:
            print("NO Stock Pool!")
        else:
            print(self.df_pool)
        df_strong_stocks_pool_1 = strong_stocks(days=1)
        print(df_strong_stocks_pool_1)
        df_strong_stocks_pool_2 = strong_stocks(days=2)
        print(df_strong_stocks_pool_2)
        logger.trace("The program will shut down the computer.")
        print(f"The program will shut down the computer.")
        say(text="The program will shut down the computer.")
        time.sleep(self.scan_median)
        say(text="The program will shut down the computer.")
        time.sleep(self.scan_min)
        os.system("shutdown -s -t 15")
        logger.error("The program exit.")
        print(f"The program exit.")
        time.sleep(self.scan_delta)
        sys.exit()

    @staticmethod
    def create_scan(filename: Path) -> pd.DataFrame:
        df_scan = feather_from_file(filename_df=filename)
        if df_scan.empty:
            df_scan = pd.DataFrame(
                columns=dict_scan_dtype.keys(), index=["sh600519", "sz300750"]
            )
            df_scan = df_scan.astype(dtype=dict_scan_dtype)
        return df_scan

    def get_position_unit(self, price: float, position: float) -> float:
        unit_100 = price * 100
        unit_symbol = max(self.unit, unit_100)
        position_unit = round(price * position / unit_symbol, 2)
        return position_unit

    def set_states(self) -> None:
        dt_now = datetime.datetime.now()
        if dt_am_start <= dt_now <= dt_am_end or dt_pm_start <= dt_now <= dt_pm_end:
            self.trading = True
        else:
            self.trading = False
        if dt_now > dt_pm_end:
            self.trading_exit = True
        else:
            self.trading_exit = False
        if dt_am_end < dt_now < dt_pm_start:
            self.trading_pause = True
        else:
            self.trading_pause = False
        if dt_am_1129 <= dt_now <= dt_am_end:
            print(f"\nComing to a close soon")
            say(text="Coming to a closed soon")
        if dt_pm_1457 <= dt_now <= dt_pm_end:
            print(f"\nCall Auction Time.")
            say(text="Call Auction Time.")
        return

    def update_states(self) -> None:
        while True:
            if self.trading_exit:
                logger.debug("update_states END---#1")
                return
            else:
                self.set_states()
                if self.trading_exit:
                    logger.debug("update_states END--#2")
                time.sleep(self.scan_states)

    def update_else_max(self) -> None:
        while True:
            if self.trading:
                self.str_pos_ctl_sz399001 = control_position_index(index="sz399001")
                self.str_pos_ctl_csi1000 = control_position_index(index="sh000852")
                self.str_pos_ctl_sz399303 = control_position_index(index="sz399303")
                self.str_pos_ctl_sz399006 = control_position_index(index="sz399006")
                market_activity_charts()
                concentration_rate_chart()
            if self.trading_pause:
                logger.debug("update_else_max Pause")
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
            if self.trading_exit:
                logger.debug("update_else_max END")
                return
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----" f"[update_else_max]"
            )
            time.sleep(self.scan_max)

    def update_else_median(self) -> None:
        while True:
            if self.trading:
                if get_concentration_rate():
                    logger.error("get_concentration_rate Error!")
            if self.trading_pause:
                logger.debug("update_else_median Pause")
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
            if self.trading_exit:
                logger.debug("update_else_median END")
                return
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----"
                f"[update_else_median]"
            )
            time.sleep(self.scan_median)

    def update_else_min(self) -> None:
        while True:
            if self.trading:
                list_market_activity = market_activity()
                self.grid_stock = get_grid_stock()
                self.grid_etf = get_grid_etf()
                self.str_market_activity = list_market_activity[0]
                self.bool_not_long_crash = list_market_activity[1]
                self.bool_not_warning = list_market_activity[2]
                self.bool_not_long_warning = list_market_activity[3]
                if self.bool_not_warning is False:
                    print("\a", end="")
                self.list_check_etf_t0 = check_etf_t0()
                self.str_concentration_rate = grt_str_realtime_concentration_rate()
                self.str_limit_up_today = get_limit_up_today(stocks=self.list_position)
                self.str_limit_down_today = get_limit_down_today(
                    stocks=self.list_position
                )
                self.str_strong_stocks_1 = get_strong_stocks(days=1)
                self.str_strong_stocks_2 = get_strong_stocks(days=2)

            if self.trading_pause:
                logger.debug("update_else_min Pause")
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
            if self.trading_exit:
                logger.debug("update_else_min END")
                return
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----" f"[update_else_min]",
            )
            time.sleep(self.scan_min)

    def update_str_msg_signals(self) -> None:
        while True:
            if self.trading:
                self.str_msg_signals = self.get_str_msg_signals()
            if self.trading_pause:
                logger.debug("update_str_msg_signals Pause")
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
            if self.trading_exit:
                logger.debug("update_str_msg_signals END")
                return
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----"
                f"[update_str_msg_signals]",
            )
            time.sleep(self.scan_signal)

    def update_print_msg(self) -> None:
        while True:
            if self.trading:
                self.print_msg()
            if self.trading_pause:
                logger.debug("update_print_msg Pause")
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
            if self.trading_exit:
                logger.debug("update_print_msg END")
                return
            dt_now = datetime.datetime.now()
            dt_now_delta = dt_now + datetime.timedelta(seconds=self.scan_print)
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----" f"[update_print_msg]",
            )
            sleep_to_time(dt_time=dt_now_delta, seconds=1)

    def add_stock_to_scan(self) -> None:
        if self.df_pool.empty:
            logger.error("stock pool empty.")
            say(text="stock pool empty.")
            return
        dt_add = datetime.datetime.strptime(self.df_pool.index.name, format_dt)
        print(f"Add pool [{dt_add.date()}]")
        say(text=f"Add pool {dt_add.date()}")
        str_scan_add = ""
        str_scan_reset = ""
        for symbol in self.df_pool.index:
            if symbol in self.df_scan.index:
                self.df_scan.at[symbol, "dt_pool"] = dt_add
                if self.df_scan.at[symbol, "position"] == 0:
                    self.df_scan.at[symbol, "price"] = price_add = self.df_pool.at[
                        symbol, "close"
                    ]
                else:
                    price_add = self.df_scan.at[symbol, "price"]
                str_scan_reset += (
                    f"[{symbol}-{self.df_scan.at[symbol, "name"]}] - [{price_add}] - "
                    f"scan - [reset dt_pool].\n"
                )
            else:
                self.df_scan.at[symbol, "dt_pool"] = dt_add
                self.df_scan.at[symbol, "name"] = name = self.df_pool.at[symbol, "name"]
                self.df_scan.at[symbol, "price"] = self.df_pool.at[symbol, "close"]
                str_scan_add += fg.red(f"[{symbol}-{name}] - scan - [add].")
                str_scan_add += "\n"
        print(str_scan_reset, end="")
        print(str_scan_add, end="")
        return

    def init_scan(self) -> None:
        self.df_scan = self.df_scan.reindex(columns=dict_scan_dtype.keys())
        self.df_scan = self.df_scan.astype(dtype=dict_scan_dtype)
        self.df_scan.fillna(value=dict_scan_value_default, inplace=True)
        dt_delete = dt_pm_end - datetime.timedelta(days=days_delete)
        # print(f"Delete date{dt_delete}, rete(0, {phi_s:5.2f})")
        for symbol in self.df_scan.index:
            self.df_scan.at[symbol, "add_pool_rate"] = add_pool_rate = (
                context.get_pool_rate(symbol=symbol)
            )
            if self.df_scan.at[symbol, "position"] > 0:
                continue
            if (
                self.df_scan.at[symbol, "dt_pool"] < dt_delete
                and 0 < add_pool_rate < phi_s
            ):
                print(
                    f"[{symbol}-{self.df_scan.at[symbol, 'name']}] - "
                    f"[{self.df_scan.at[symbol, "dt_pool"]} - {add_pool_rate}%] - "
                    f"delete. - Pool Rate"
                )
                self.df_scan.drop(labels=symbol, inplace=True)
            elif self.df_scan.at[symbol, "price"] > now_price_max:
                print(
                    f"[{symbol}-{self.df_scan.at[symbol, 'name']}] - delete. - Now Price"
                )
                self.df_scan.drop(labels=symbol, inplace=True)
        feather_to_file(df=self.df_scan, filename_df=filename_scan_ftr)
        self.df_scan["display"] = 1.0
        self.df_scan["rid"] = "rid"
        csv_to_file(df=self.df_scan, filename_df=filename_scan_csv)

    def get_str_msg_signals(self) -> str:
        str_msg_signals = ""
        self.loop += 1
        log_json("get_signal")
        dt_now = datetime.datetime.now()
        if filename_scan_modified_xlsx.exists():
            logger.debug("filename_scan_modified_xlsx exist.")
            df_scan_modified = pd.read_excel(
                io=filename_scan_modified_xlsx, sheet_name="signal", index_col=0
            )
            try:
                df_scan_modified = df_scan_modified.astype(dtype=dict_scan_dtype)
            except ValueError as e:
                str_msg_signals = f"{e}"
                logger.error(f"{e}")
                say(text="Record modified error")
                return str_msg_signals
            df_scan_modified.fillna(value=dict_scan_value_default, inplace=True)
            for symbol in df_scan_modified.index:
                if symbol in self.df_scan.index:
                    df_scan_modified.at[symbol, "dt_pool"] = self.df_scan.at[
                        symbol, "dt_pool"
                    ]
                    if df_scan_modified.at[symbol, "price"] == 0.0:
                        df_scan_modified.at[symbol, "price"] = self.df_scan.at[
                            symbol, "price"
                        ]
                        df_scan_modified.at[symbol, "position"] = self.df_scan.at[
                            symbol, "position"
                        ]
                        df_scan_modified.at[symbol, "attention"] = self.df_scan.at[
                            symbol, "attention"
                        ]
                self.df_scan.loc[symbol] = df_scan_modified.loc[symbol]
            self.df_scan = self.df_scan[self.df_scan["attention"] > 0]
            str_now_input = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename_scan_modified_csv = path_history.joinpath(
                f"df_scan_modified_{str_now_input}.csv"
            )
            csv_to_file(df=df_scan_modified, filename_df=filename_scan_modified_csv)
            list_scan_modified = df_scan_modified.index.tolist()
            filename_scan_modified_xlsx.unlink(missing_ok=True)
            print(
                f"\r<{dt_now.time().replace(microsecond=0)}>----"
                f"Record modified: {list_scan_modified}"
            )
            self.str_position_gt_max = self.get_gt_position_max()
            say(text="Record modified")
        # df_realtime = realtime_quotations(stock_codes=self.df_scan.index.to_list())
        df_signal = pd.DataFrame(columns=dict_scan_dtype.keys())
        df_signal = df_signal.astype(dtype=dict_scan_dtype)
        pct_ups_stock = self.grid_stock[0]
        pct_downs_stock = self.grid_stock[1]
        pct_ups_etf = self.grid_etf[0]
        pct_downs_etf = self.grid_etf[1]
        i_for_scan = 0
        count_scan = self.df_scan.shape[0]
        for symbol in self.df_scan.index:
            i_for_scan += 1
            str_msg = f"Scan - [{i_for_scan}/{count_scan}] - [{symbol}]"
            now = realtime_quotation.get_close(symbol=symbol)
            if now == 0:
                self.df_scan.at[symbol, "remark"] = "Stock Closed"
                logger.error(f"{str_msg} has not now price and Next")
                continue
            self.df_scan.at[symbol, "now"] = now
            self.df_scan.at[symbol, "name"] = realtime_quotation.get_name(symbol=symbol)
            if self.df_scan.at[symbol, "price"] == 0:
                self.df_scan.at[symbol, "price"] = now
                logger.error(f"{str_msg} - Price error")
            self.df_scan.at[symbol, "pct_chg"] = pct_chg = round(
                (now / self.df_scan.at[symbol, "price"] - 1) * 100, 2
            )
            position = self.df_scan.at[symbol, "position"]
            self.df_scan.at[symbol, "position_unit"] = position_unit = (
                self.get_position_unit(price=now, position=position)
            )
            bbl = context.realtime_bbands.get_item(symbol=symbol, items="BBL")
            bbu = context.realtime_bbands.get_item(symbol=symbol, items="BBU")
            bbm = context.realtime_bbands.get_item(symbol=symbol, items="BBM")
            level = context.realtime_level.get_level(symbol=symbol)
            self.df_scan.at[symbol, "remark"] = (
                f"___{level}_BBL_{bbl}_BBM_{bbm}_BBU_{bbu}"
            )
            if bbl == 0 or bbu == 0 or bbm == 0:
                if pct_chg <= -7.0 and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_PCT_Const_-7_0_&_No_bbands"
                elif -7.0 < pct_chg <= -5.0 and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_PCT_Const_-5_0_&_No_bbands"
                elif 5.0 <= pct_chg < 7.0 and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"SELL_PCT_Const_5_0_&_No_bbands"
                elif pct_chg >= 7.0 and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"SELL_PCT_Const_7_0_&_No_bbands"
            elif symbol in list_etfs_t0:
                if pct_chg < pct_downs_etf and now < bbl and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_PCT_{pct_downs_etf}_&_ETF_T0"
                elif pct_chg > pct_ups_etf and now > bbu and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = (
                        f"SELL_PCT_{pct_downs_etf}_&_ETF_T0"
                    )
            elif position <= 0:
                if now < bbl and pct_chg < -0.2 and self.bool_not_long_warning:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = (
                        f"BUY_BBL_{bbl}_&_PCT_-0_2_&_position_0"
                    )
                elif now < bbu and pct_chg < -7.0 and self.bool_not_long_warning:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = (
                        f"BUY_BBU_{bbu}_&_PcCT_Const_-7_0_&_position_0"
                    )
            elif 0 < position_unit < self.position_unit_max:
                if (0 < position <= 100 or 0 < position_unit <= 1) and pct_chg > 0:
                    if now > bbm and pct_chg > 5.0:
                        df_signal.loc[symbol] = self.df_scan.loc[symbol]
                        df_signal.at[symbol, "remark"] = (
                            f"SELL_BBL_{bbl}_&_PCT_Const_5_0_&_position_unit_1"
                        )
                    elif pct_chg > 7.0:
                        df_signal.loc[symbol] = self.df_scan.loc[symbol]
                        df_signal.at[symbol, "remark"] = (
                            f"SELL_PCT_Const_7_0_&_position_unit_1"
                        )
                else:
                    if pct_chg > 0:
                        if now > bbm and pct_chg > 5.0:
                            df_signal.loc[symbol] = self.df_scan.loc[symbol]
                            df_signal.at[symbol, "remark"] = (
                                f"SELL_BBU_{bbu}_&_PCT_Const_5_0"
                            )
                        elif now > bbu and pct_chg > pct_ups_stock:
                            df_signal.loc[symbol] = self.df_scan.loc[symbol]
                            df_signal.at[symbol, "remark"] = (
                                f"SELL_BBU_{bbu}_&_PCT_{pct_ups_stock}"
                            )
                        elif pct_chg > 7.0:
                            df_signal.loc[symbol] = self.df_scan.loc[symbol]
                            df_signal.at[symbol, "remark"] = f"SELL_PCT_{pct_chg}"
                    elif pct_chg < 0:
                        if (
                            now < bbl
                            and pct_chg < pct_downs_stock
                            and self.bool_not_long_crash
                        ):
                            df_signal.loc[symbol] = self.df_scan.loc[symbol]
                            df_signal.at[symbol, "remark"] = (
                                f"BUY_BBL_{bbl}_&_PCT_{pct_downs_stock}"
                            )
                        elif now < bbm and pct_chg < -5.0 and self.bool_not_long_crash:
                            df_signal.loc[symbol] = self.df_scan.loc[symbol]
                            df_signal.at[symbol, "remark"] = (
                                f"BUY_BBM_{bbm}_&_PCT_Const_-5_0"
                            )
            elif position_unit >= self.position_unit_max:
                if pct_chg <= -7.0 and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = (
                        f"BUY_pct_Const_-7_0_&_osition_unit_{self.position_unit_max}"
                    )
                elif pct_chg >= 7.0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = (
                        f"SELL_pct_Const_7_0_&_osition_unit_{self.position_unit_max}"
                    )
            else:
                if pct_chg <= -7.0 and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_pct_Const_-7_0"
                elif -7.0 < pct_chg <= -5.0 and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_pct_Const_-5_0"
                elif -5.0 < pct_chg <= -3.0 and now < bbl and self.bool_not_long_crash:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"BUY_pct_Const_-3_0"
                elif 3.0 <= pct_chg < 5.0 and now > bbu and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"SELL_pct_Const_3_0"
                elif 5.0 <= pct_chg < 7.0 and now > bbm and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"SELL_pct_Const_5_0"
                elif pct_chg >= 7.0 and position > 0:
                    df_signal.loc[symbol] = self.df_scan.loc[symbol]
                    df_signal.at[symbol, "remark"] = f"SELL_pct_Const_7_0"
            print(f"{str_msg} - Update\033[K", end="\r")
        print("\033[K", end="")
        if i_for_scan != count_scan:
            logger.error(f"{i_for_scan} != {count_scan}")
        self.df_scan.sort_values(by=["pct_chg"], ascending=True, inplace=True)
        feather_to_file(df=self.df_scan, filename_df=filename_scan_ftr)
        csv_to_file(df=self.df_scan, filename_df=filename_scan_csv)
        self.list_position = self.df_scan[self.df_scan["position"] > 0].index.tolist()
        if df_signal.empty:
            logger.debug(f"No signal - #1")
            return str_msg_signals
        df_signal.sort_values(
            by=["pct_chg"], key=lambda x: x.abs(), ascending=True, inplace=True
        )
        filename_signal_xlsx_temp = filename_signal_xlsx
        i_while_signal_xlsx = 1
        while i_while_signal_xlsx < 3:
            try:
                with pd.ExcelWriter(path=filename_signal_xlsx_temp, mode="w") as writer:
                    df_signal.to_excel(excel_writer=writer, sheet_name="signal")
            except PermissionError as e:
                filename_signal_name = (
                    filename_signal_xlsx.stem
                    + f"_{i_while_signal_xlsx}"
                    + filename_signal_xlsx.suffix
                )
                filename_signal_xlsx_temp = filename_signal_xlsx.parent.joinpath(
                    filename_signal_name
                )
                logger.error(f"{repr(e)}")
            else:
                break
            i_while_signal_xlsx += 1
        df_signal = df_signal[df_signal["display"] == 1]
        if df_signal.empty:
            logger.debug(f"No signal - #2")
            return str_msg_signals
        df_analysis = get_analysis(
            symbols=df_signal.index.tolist(), fields="simple", trading=True
        )
        i_for_signal = 0
        count_signal = df_signal.shape[0]
        for symbol in df_signal.index:
            if symbol not in self.df_scan.index:
                continue
            i_for_signal += 1
            str_msg = f"Signal - [{i_for_signal}/{count_signal}] - [{symbol}]"
            pct_chg = df_signal.at[symbol, "pct_chg"]
            remark = df_signal.at[symbol, "remark"]
            str_msg_signal_now = (
                f"[{i_for_signal}]-[{symbol}-{df_signal.at[symbol, 'name']:>4}] - "
                f"<{df_signal.at[symbol, 'now']:6.3f} - {pct_chg:5.2f}%> - "
            )
            if "SELL" in remark:
                str_msg_signal_now = fg.red(str_msg_signal_now)
            else:
                str_msg_signal_now = fg.green(str_msg_signal_now)
            str_msg_signal_position = (
                f"[{df_signal.at[symbol, 'price']:6.3f} * "
                f"{df_signal.at[symbol, 'position']:3.0f}-"
                f"({df_signal.at[symbol, 'position_unit']:2.0f})]"
            )
            str_msg_index_name = f" - [{context.get_index_name(symbol=symbol)}]"
            str_msg_signal = (
                str_msg_signal_now + str_msg_signal_position + str_msg_index_name + "\n"
            )
            if remark != "remark":
                str_msg_signal_remark = fg.yellow(f"Remark:[{remark}]")
                str_msg_signal += f"--------{str_msg_signal_remark}\n"
            if df_analysis.empty:
                if str_msg_signal != "":
                    str_msg_signals += str_msg_signal
                continue
            i_analysis_columns = 0
            line_len = 3
            str_msg_signal_plus = ""
            for column in df_analysis.columns:
                if symbol not in df_analysis.index:
                    continue
                int_line_print = i_analysis_columns % line_len
                if int_line_print == 0:
                    str_msg_signal_plus += (
                        f"[{column}: {df_analysis.at[symbol, column]}]"
                    )
                elif int_line_print == line_len - 1:
                    str_msg_signal_plus += (
                        f" - [{column}: {df_analysis.at[symbol, column]}]\n"
                    )
                else:
                    str_msg_signal_plus += (
                        f" - [{column}: {df_analysis.at[symbol, column]}]"
                    )
                i_analysis_columns += 1
            str_msg_signal += f"{str_msg_signal_plus}\n"
            str_msg_signals += f"{str_msg_signal}\n"
            print(f"{str_msg} - Update\033[K", end="\r")
        if str_msg_signals != "":
            say(text="new signal.")
        return str_msg_signals

    def print_msg(self) -> None:
        start_loop_time = time.perf_counter_ns()
        if self.loop < 1:
            return
        elif self.loop > 1:
            os.system("cls")
        dt_now = datetime.datetime.now()
        dt_realtime_time = (
            realtime_quotation.get_datetime().time().replace(microsecond=0)
        )
        if not self.bool_not_long_crash:
            space_line1 = " " * 43
            str_msg_crash = fg.red("Stock Market Crash !!!")
            print(f"{space_line1}{str_msg_crash}")
            print(f"{space_line1}{str_msg_crash}")
            print(f"{space_line1}{str_msg_crash}")
        if self.str_msg_signals != "":
            print("#" * 108)
            print(f"{self.str_msg_signals}", end="")
            print(
                f"### <{fg.red(dt_realtime_time)}>",
                "#" * 70,
            )
        if self.str_limit_up_today != "" or self.str_limit_down_today != "":
            print(f"##{self.str_stock}##" * 12)
        if self.str_limit_up_today != "":
            print(self.str_limit_up_today)
            if self.str_limit_up_today != "" and self.str_limit_down_today != "":
                print(fg.lightblack("=" * 108))
        if self.str_limit_down_today != "":
            print(self.str_limit_down_today)
        if self.str_limit_up_today != "" or self.str_limit_down_today != "":
            print(f"##{self.str_stock}##" * 12)
        if self.list_check_etf_t0[0] != "" or self.list_check_etf_t0[1] != "":
            print(f"##{self.str_etf}##" * 16)
        if self.list_check_etf_t0[0] != "":
            print(self.list_check_etf_t0[0])
            if self.list_check_etf_t0[0] != "" and self.list_check_etf_t0[1] != "":
                print(fg.lightblack("=" * 108))
        if self.list_check_etf_t0[1] != "":
            print(self.list_check_etf_t0[1])
        if self.list_check_etf_t0[0] != "" or self.list_check_etf_t0[1] != "":
            print(f"##{self.str_etf}##" * 16)
        if self.str_concentration_rate != "":
            print(f"##{self.str_c_r}##" * 6)
            print(self.str_concentration_rate)
            print(f"##{self.str_c_r}##" * 6)
        if self.str_market_activity != "":
            print(f"##{self.str_m_r}##" * 11)
            print(self.str_market_activity)
            print(f"##{self.str_m_r}##" * 11)
        print(f"#{self.str_pos_r}" * 11)
        print(self.str_pos_ctl_zh)
        print(self.str_pos_ctl_sz399001)
        print(self.str_pos_ctl_sz399303)
        print(self.str_pos_ctl_csi1000)
        print(self.str_pos_ctl_sz399006)
        print(f"#{self.str_pos_r}" * 11)
        if self.str_position_gt_max != "":
            print("#" * 108)
            print(self.str_position_gt_max)
            print("#" * 108)
        print("#1" * 54)
        print(self.str_strong_stocks_1)
        print("#1" * 54)
        print("#2" * 54)
        print(self.str_strong_stocks_2)
        print("#2" * 54)
        print("#END" * 27)
        print(
            f"<{dt_realtime_time}>----<STOCK> -- "
            f"[Ups={self.grid_stock[0]:5.2f}] - [Downs={self.grid_stock[1]:5.2f}] - "
            f"[Median={self.grid_stock[2]:5.2f}]"
        )
        print(
            f"<{dt_realtime_time}>----<ETF> -- "
            f"[Ups={self.grid_etf[0]:5.2f}] - [Downs={self.grid_etf[1]:5.2f}] - "
            f"[Median={self.grid_etf[2]:5.2f}]"
        )
        if not self.bool_not_long_crash:
            say(text="Stock Market Crash")
        end_loop_time = time.perf_counter_ns()  # 1毫秒 = 1,000,000纳秒
        interval_time_ms = round((end_loop_time - start_loop_time) / 1000000, 2)
        str_msg_loop_end = (
            f"<{dt_now.time().replace(microsecond=0)}>----"
            f"[{interval_time_ms:9.2f}ms]----[{self.loop}]"
        )
        print(str_msg_loop_end, end="")
        return


if __name__ == "__main__":
    myapp = Main()
    myapp.run()
