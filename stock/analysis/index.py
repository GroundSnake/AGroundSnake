# modified at 2023/05/18 22::25
import os
import sys
import time
import feather
import numpy as np
import requests
import datetime
import random
import win32file
import shelve
import pandas as pd
from loguru import logger
from pyecharts.charts import Line, Page
import pyecharts.options as opts
import tushare as ts
import analysis.ashare
from analysis.const import (
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
)


class IndexSSB(object):
    def __init__(self, update: bool = True, origin: int = 2023):
        self.__name = "IndexSSB"
        logger.trace(f"__init__ Begin")
        self.__pro = ts.pro_api()
        self.date_origin = datetime.date(year=origin, month=1, day=1)
        self.str_date_origin = self.date_origin.strftime("%Y%m%d")
        self.date_now = datetime.datetime.now().date()
        self.str_date_now = self.date_now.strftime("%Y%m%d")
        path_main = os.getcwd()
        self.path_mv = os.path.join(path_main, "data", "mv")
        self.path_check = os.path.join(path_main, "check")
        self.filename_shelve = os.path.join(self.path_mv, f"mv")
        self.filename_index_charts = os.path.join(self.path_check, "index_charts.html")
        self.filename_index_charts_min = os.path.join(
            self.path_check, "index_charts_min.html"
        )
        self.py_dbm = shelve.open(filename=self.filename_shelve, flag="c")
        df_trade_cal = self.__pro.trade_cal(
            exchange="", start_date=self.str_date_origin, end_date=self.str_date_now
        )
        df_trade_cal.sort_values(
            by=["cal_date"], ascending=True, inplace=True, ignore_index=True
        )
        df_trade_cal = df_trade_cal[["cal_date", "is_open"]]
        df_trade_cal["cal_date"] = df_trade_cal["cal_date"].apply(
            func=lambda x: pd.to_datetime(x).date()
        )
        df_trade_cal.set_index(keys=["cal_date"], inplace=True)
        try:
            self.df_index_exist = self.py_dbm["df_index_exist"]
            self.df_index_exist = self.df_index_exist.reindex(index=df_trade_cal.index)
            self.df_index_exist["is_open"] = df_trade_cal["is_open"]
        except KeyError as e:
            key_df_x = e.args[0].decode()
            if "df_index_exist" in key_df_x:
                self.df_index_exist = df_trade_cal
                self.df_index_exist = self.df_index_exist.reindex(
                    columns=["is_open", "market_value", "index_ssb"]
                )
        self.df_index_exist.fillna(value=0, inplace=True)
        self.py_dbm["df_index_exist"] = self.df_index_exist
        df_mv = self.__pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,list_date",
        )
        df_mv["symbol"] = df_mv["ts_code"].apply(
            func=lambda ts_code: ts_code[-2:].lower() + ts_code[:6]
        )
        df_mv.drop(columns="ts_code", inplace=True)
        df_mv.set_index(keys=["symbol"], inplace=True)
        df_mv["list_date"] = pd.to_datetime(df_mv["list_date"])
        df_mv.sort_values(by=["list_date"], ascending=True, inplace=True)
        list_index_columns = df_mv.columns.tolist() + [
            "base_mv",
            "now_mv",
            "t1_mv",
            "base_mv_date",
            "now_mv_date",
            "t1_mv_date",
        ]
        df_mv = df_mv.reindex(columns=list_index_columns)
        self.dt_init = datetime.date(year=1989, month=1, day=1)
        df_mv["base_mv_date"] = self.dt_init
        df_mv["now_mv_date"] = self.dt_init
        df_mv["t1_mv_date"] = self.dt_init
        # print(type(df_mv.at['sh600519', "now_mv_date"]))
        self.df_mv = df_mv
        # print(self.df_mv)
        self.list_all_stocks = self.df_mv.index.tolist()
        random.shuffle(self.list_all_stocks)
        if update is True:
            self.make()  # 更新数据调用
            self.stocks_in_ssb()
            self.shelve_to_excel()
        logger.trace(f"__init__ End")

    def __get_market_values(self, date_pos: datetime.date) -> bool:
        name = "market_value"
        data = "is_open"
        logger.trace(f"{name} Begin")
        if self.df_index_exist.at[date_pos, data] == 1:
            if self.df_index_exist.at[date_pos, name] == 1:
                logger.trace(f"{name} Break, and End")
                return True
        else:
            print(f"get_market_values: {date_pos} is not trading day")
            return False
        str_date_pos_ul = date_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        str_date_pos = date_pos.strftime("%Y%m%d")
        filename_df_mv_temp = os.path.join(
            self.path_mv, f"df_mv_temp_{str_date_pos_ul}.ftr"
        )
        try:
            df_mv = self.py_dbm[str_df_mv]
        except KeyError:
            if os.path.exists(filename_df_mv_temp):
                df_mv = feather.read_dataframe(source=filename_df_mv_temp)
            else:
                df_mv = self.df_mv
        diff_date_pos = 0
        same_date_pos = 0
        share_change = 0
        count = len(df_mv)
        df_mv.sort_values(by=["base_mv"], inplace=True)
        df_mv = df_mv.sample(frac=1)
        i = 0
        times_try = 2
        for symbol in df_mv.index:
            ts_code = symbol[2:] + "." + symbol[:2]
            feather.write_dataframe(df=df_mv, dest=filename_df_mv_temp)
            i += 1
            str_msg_bar = f"{name} - {str_df_mv}: [{i:04d}/{count}] - [{symbol}]"
            now_mv_date = df_mv.at[symbol, "now_mv_date"]
            if now_mv_date != date_pos:
                i_times = 0
                while True:
                    i_times += 1
                    try:
                        df_daily_basic_symbol = self.__pro.daily_basic(
                            ts_code=ts_code,
                            start_date=self.str_date_origin,
                            end_date=str_date_pos,
                            fields="trade_date,close,total_share,total_mv",
                        )
                    except requests.exceptions.ConnectionError as e:
                        print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                        logger.error(repr(e))
                        time.sleep(2)
                        if i_times > times_try:
                            df_daily_basic_symbol = pd.DataFrame()
                            break
                    else:
                        if df_daily_basic_symbol.empty:
                            time.sleep(2)
                            if i_times > times_try:
                                break
                        else:
                            break
                if df_daily_basic_symbol.empty:
                    df_mv.drop(index=symbol, inplace=True)
                    str_msg_bar += " - drop#1"
                else:
                    df_daily_basic_symbol["trade_date"] = pd.to_datetime(
                        df_daily_basic_symbol["trade_date"]
                    )
                    df_daily_basic_symbol["trade_date"] = df_daily_basic_symbol[
                        "trade_date"
                    ].apply(func=lambda x: x.date())
                    df_daily_basic_symbol.sort_values(
                        by=["trade_date"],
                        ascending=True,
                        inplace=True,
                        ignore_index=True,
                    )
                    count_daily_basic_symbol = len(df_daily_basic_symbol)
                    if count_daily_basic_symbol >= 2:
                        max_index = count_daily_basic_symbol - 1
                        max2_index = count_daily_basic_symbol - 2
                    else:
                        max_index = max2_index = 0
                    min_index = 0
                    now_total_share = df_daily_basic_symbol.at[max_index, "total_share"]
                    base_total_share = df_daily_basic_symbol.at[
                        min_index, "total_share"
                    ]
                    if now_total_share == base_total_share:
                        base_mv_date = df_daily_basic_symbol.at[min_index, "trade_date"]
                        now_mv_date = df_daily_basic_symbol.at[max_index, "trade_date"]
                        df_mv.at[symbol, "base_mv"] = df_daily_basic_symbol.at[
                            min_index, "total_mv"
                        ]
                        df_mv.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                            max_index, "total_mv"
                        ]
                        df_mv.at[symbol, "t1_mv"] = df_daily_basic_symbol.at[
                            max2_index, "total_mv"
                        ]
                        df_mv.at[symbol, "base_mv_date"] = base_mv_date
                        df_mv.at[symbol, "now_mv_date"] = now_mv_date
                        df_mv.at[symbol, "t1_mv_date"] = df_daily_basic_symbol.at[
                            max2_index, "trade_date"
                        ]
                        if now_mv_date != date_pos:
                            diff_date_pos += 1
                            print(
                                f"\r{str_msg_bar} - [diff = {diff_date_pos}/{same_date_pos}]"
                                f" - [<{now_mv_date}> / <{date_pos}>]\033[K"
                            )
                        else:
                            same_date_pos += 1
                        str_msg_bar += f" - [{now_mv_date}] - [{date_pos}]"
                    elif now_total_share != base_total_share:
                        share_change += 1
                        print(
                            f"\r{str_msg_bar} - [Change = {share_change:2d}]"
                            f" - [{base_total_share.round(2)} - {now_total_share.round(2)}]\033[K"
                        )
                        i_pro_bar = 0
                        while True:
                            i_pro_bar += 1
                            try:
                                df_pro_bar_symbol = ts.pro_bar(
                                    ts_code=ts_code,
                                    adj="qfq",
                                    start_date=self.str_date_origin,
                                    end_date=str_date_pos,
                                )
                            except requests.exceptions.ConnectionError as e:
                                print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                                logger.error(repr(e))
                                time.sleep(2)
                                if i_pro_bar > times_try:
                                    df_pro_bar_symbol = pd.DataFrame()
                                    break
                            else:
                                if df_pro_bar_symbol.empty:
                                    time.sleep(2)
                                    if i_pro_bar > times_try:
                                        break
                                else:
                                    break
                        if df_pro_bar_symbol.empty:
                            df_mv.drop(index=symbol, inplace=True)
                            str_msg_bar += " - drop#2"
                        else:
                            df_pro_bar_symbol["trade_date"] = pd.to_datetime(
                                df_pro_bar_symbol["trade_date"]
                            )
                            df_pro_bar_symbol["trade_date"] = df_pro_bar_symbol[
                                "trade_date"
                            ].apply(func=lambda x: x.date())
                            df_pro_bar_symbol.sort_values(
                                by=["trade_date"],
                                ascending=True,
                                inplace=True,
                                ignore_index=True,
                            )
                            count_pro_bar_symbol = len(df_pro_bar_symbol)
                            if count_pro_bar_symbol >= 2:
                                max_index_close = count_pro_bar_symbol - 1
                                max2_index_close = count_pro_bar_symbol - 2
                            else:
                                max_index_close = max2_index_close = 0
                            min_index_close = 0
                            base_mv_date = df_pro_bar_symbol.at[
                                min_index_close, "trade_date"
                            ]
                            now_mv_date = df_pro_bar_symbol.at[
                                max_index_close, "trade_date"
                            ]
                            df_mv.at[symbol, "base_mv"] = (
                                df_pro_bar_symbol.at[min_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "now_mv"] = (
                                df_pro_bar_symbol.at[max_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "t1_mv"] = (
                                df_pro_bar_symbol.at[max2_index_close, "close"]
                                * now_total_share
                            )
                            df_mv.at[symbol, "base_mv_date"] = base_mv_date
                            df_mv.at[symbol, "now_mv_date"] = now_mv_date
                            df_mv.at[symbol, "t1_mv_date"] = df_pro_bar_symbol.at[
                                max2_index_close, "trade_date"
                            ]
                            if now_mv_date != date_pos:
                                diff_date_pos += 1
                                print(
                                    f"\r{str_msg_bar} - [diff = {diff_date_pos}/{same_date_pos}]"
                                    f" - [<{now_mv_date}> / <{date_pos}>]\033[K"
                                )
                            else:
                                same_date_pos += 1
                            str_msg_bar += f" - [{now_mv_date}] - [{date_pos}]"
                    else:
                        logger.error(
                            f"{symbol} - Unknow Error"
                            f" - now_total_share={now_total_share}, base_total_share={base_total_share}"
                        )
            elif now_mv_date == date_pos:
                same_date_pos += 1
                print(
                    f"\r{str_msg_bar} - [{now_mv_date}] - [{date_pos}] - latest\033[K",
                    end="",
                )
            else:
                diff_date_pos += 1
                print(f"\r{str_msg_bar} - [{now_mv_date}] - [{date_pos}] - None\033[K")
            if diff_date_pos >= 50:
                print("\n", end="")
                logger.error("diff_date_pos >= 50")
                sys.exit()
        if i >= count:
            print("\n", end="")  # 格式处理
            df_mv.applymap(
                func=lambda x: round(x / 10000, 2)
                if (isinstance(x, (int, float)) and x > 100)
                else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
            )
            date_mv_max = df_mv["now_mv_date"].max()
            if date_mv_max == date_pos:
                self.df_index_exist.at[date_pos, name] = 1
                self.py_dbm["df_index_exist"] = self.df_index_exist
                self.py_dbm[str_df_mv] = df_mv
            if os.path.exists(filename_df_mv_temp):
                os.remove(path=filename_df_mv_temp)
        logger.trace(f"{name} End")
        return True

    def __make_index(self, date_pos: datetime.date):
        name = "index_ssb"
        data = "market_value"
        logger.trace(f"{name} Begin")
        str_date_pos_ul = date_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        if (
            self.df_index_exist.at[date_pos, data] == 1
            and self.df_index_exist.at[date_pos, name] == 1
        ):
            logger.trace(f"{name} Break, and End")
            return True
        bool_get_mv = self.__get_market_values(date_pos=date_pos)
        if bool_get_mv:
            try:
                df_mv = self.py_dbm[str_df_mv]
            except KeyError as e:
                logger.error(f"{date_pos} - {repr(e)}, sys exit")
                sys.exit()
        else:
            print(f"make_index: {date_pos} is not trading day")
            return False
        df_mv_non_st = df_mv[~df_mv["name"].str.contains("ST").fillna(False)].copy()
        df_mv_st = df_mv[df_mv["name"].str.contains("ST").fillna(False)].copy()
        try:
            df_index_ssb = self.py_dbm["df_index_ssb"]
        except KeyError as e:
            logger.error(f"load df_index_ssb from py_dbm fail - Error[{repr(e)}]")
            df_index_ssb = pd.DataFrame(
                columns=[
                    "base_mv_all",
                    "now_mv_all",
                    "ssb_all",
                    "ssb_non_st",
                    "ssb_50",
                    "ssb_300",
                    "ssb_500",
                    "ssb_1000",
                    "ssb_2000",
                    "ssb_tail",
                    "ssb_st",
                    "base_mv_non_st",
                    "base_mv_50",
                    "base_mv_300",
                    "base_mv_500",
                    "base_mv_1000",
                    "base_mv_2000",
                    "base_mv_tail",
                    "base_mv_st",
                    "now_mv_non_st",
                    "now_mv_50",
                    "now_mv_300",
                    "now_mv_500",
                    "now_mv_1000",
                    "now_mv_2000",
                    "now_mv_tail",
                    "now_mv_st",
                ]
            )
        df_mv_non_st.sort_values(by=["base_mv"], ascending=False, inplace=True)
        df_mv_50 = df_mv_non_st.iloc[:50].copy()
        df_mv_300 = df_mv_non_st.iloc[:300].copy()
        df_mv_500 = df_mv_non_st.iloc[300:800].copy()
        df_mv_1000 = df_mv_non_st.iloc[800:1800].copy()
        df_mv_2000 = df_mv_non_st.iloc[1800:3800].copy()
        df_mv_tail = df_mv_non_st.iloc[3800:].copy()
        dict_df_index_n = {
            "all": df_mv,
            "non_st": df_mv_non_st,
            "st": df_mv_st,
            "50": df_mv_50,
            "300": df_mv_300,
            "500": df_mv_500,
            "1000": df_mv_1000,
            "2000": df_mv_2000,
            "tail": df_mv_tail,
        }
        i = 0
        count = len(dict_df_index_n)
        for key in dict_df_index_n:
            i += 1
            df_mv_n_name = f"df_mv_{key}"
            df_mv_n = dict_df_index_n[key]
            base_mv_n = df_mv_n["base_mv"].sum()
            now_mv_n = df_mv_n["now_mv"].sum()
            df_index_ssb.at[date_pos, f"base_mv_{key}"] = round(
                base_mv_n / 100000000, 2
            )
            df_index_ssb.at[date_pos, f"now_mv_{key}"] = round(now_mv_n / 100000000, 2)
            stocks_index_n = now_mv_n / base_mv_n * 1000
            stocks_index_n = round(stocks_index_n, 2)
            df_index_ssb.at[date_pos, f"ssb_{key}"] = stocks_index_n
            df_mv_n[f"contribution_points_index_{key}"] = (
                (df_mv_n["now_mv"] - df_mv_n["base_mv"]) / base_mv_n * 1000
            )
            df_mv_n[f"t1_contribution_points_index_{key}"] = (
                (df_mv_n["t1_mv"] - df_mv_n["base_mv"]) / base_mv_n * 1000
            )
            df_mv_n[f"now_t1_contribution_points_index_{key}"] = (
                df_mv_n[f"contribution_points_index_{key}"]
                - df_mv_n[f"t1_contribution_points_index_{key}"]
            )
            df_mv_n.sort_values(
                by=[f"now_t1_contribution_points_index_{key}"],
                ascending=False,
                inplace=True,
            )
            df_mv_n = df_mv_n.applymap(
                func=lambda x: round(x / 10000, 2)
                if (isinstance(x, (int, float)) and x > 100)
                else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
            )
            self.py_dbm[df_mv_n_name] = df_mv_n
            print(
                f"\r{name} - {date_pos} - [{i:2d}/{count:2d}] - {df_mv_n_name} - save\033[K",
                end="",
            )
        if i >= count:
            print("\n", end="")
        self.df_index_exist.at[date_pos, name] = 1
        self.py_dbm["df_index_exist"] = self.df_index_exist
        self.py_dbm["df_index_ssb"] = df_index_ssb
        logger.trace(f"{name} End")
        return True

    def __make_index_line(self):
        logger.trace("make_index_line Start")
        i = 0
        count = len(self.df_index_exist.index)
        for date in self.df_index_exist.index:
            i += 1
            if i == count:
                time_now = datetime.datetime.now().time()
                time_pm_end = datetime.time(hour=15, minute=0, second=0, microsecond=0)
                if time_now < time_pm_end:
                    break
            str_msg_bar = f"Current Date: {date} - [{i:03d}/{count:03d}]"
            if self.df_index_exist.at[date, "is_open"] == 1:
                if not self.__make_index(date):
                    logger.error(f"make_index_line: {date} is not trading day")
                    str_msg_bar += " - [non trading day]"
                else:
                    str_msg_bar += " - [trading day]"
            else:
                str_msg_bar += " - [non trading day]"
            print(f"\r{str_msg_bar}\033[K", end="")
        self.py_dbm["df_index_exist"] = self.df_index_exist
        logger.trace("make_index_line End")

    def __make_charts(self):
        name = "make_charts"
        logger.trace(f"{name} Begin")
        try:
            df_index_ssb = self.py_dbm["df_index_ssb"]
        except KeyError as e:
            logger.error(f"df_index_ssb is not exist - {repr(e)}")
            raise KeyError("df_index_ssb is not exist")
        df_index_ssb.sort_index(inplace=True)
        x_axis = df_index_ssb.index.tolist()
        dict_list_index_n = {
            "all": df_index_ssb["ssb_all"].tolist(),
            "non_st": df_index_ssb["ssb_non_st"].tolist(),
            "50": df_index_ssb["ssb_50"].tolist(),
            "300": df_index_ssb["ssb_300"].tolist(),
            "500": df_index_ssb["ssb_500"].tolist(),
            "1000": df_index_ssb["ssb_1000"].tolist(),
            "2000": df_index_ssb["ssb_2000"].tolist(),
            "tail": df_index_ssb["ssb_tail"].tolist(),
        }
        list_values = list()
        for key in dict_list_index_n:
            list_values += dict_list_index_n[key]
        list_values = [x for x in list_values if ~np.isnan(x)]
        y_min = min(list_values)
        y_max = max(list_values)
        line_index = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index.add_xaxis(xaxis_data=x_axis)
        for key in dict_list_index_n:
            line_index.add_yaxis(
                series_name=f"ssb_{key}",
                y_axis=dict_list_index_n[key],
                is_symbol_show=False,
            )
        line_index.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_top="0%", pos_left="50%"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min, max_=y_max),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index.set_colors(
            colors=[
                "red",
                "orange",
                "olive",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "deeppink",
            ]
        )
        # st
        line_index_st = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index_st.add_xaxis(xaxis_data=x_axis)
        y_axis_st = df_index_ssb["ssb_st"].tolist()
        y_min_st = min(y_axis_st)
        y_max_st = max(y_axis_st)
        line_index_st.add_yaxis(
            series_name=f"ssb_st",
            y_axis=y_axis_st,
            is_symbol_show=False,
        )
        line_index_st.set_global_opts(
            title_opts=opts.TitleOpts(
                title="SSB Index ST", pos_top="0%", pos_left="50%"
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min_st, max_=y_max_st),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index_st.set_colors(
            colors=[
                "red",
            ]
        )
        page = Page(
            page_title="Stock Index",
        )
        page.add(line_index, line_index_st)
        page.render(path=self.filename_index_charts)
        logger.trace(f"{name} End")

    def make(self):
        self.__make_index_line()
        self.__make_charts()

    def stocks_in_ssb(self) -> pd.DataFrame:
        name = "stocks_in_ssb"
        logger.trace(f"{name} Begin")
        try:
            df_mv_50 = self.py_dbm["df_mv_50"]
            df_mv_300 = self.py_dbm["df_mv_300"]
            df_mv_500 = self.py_dbm["df_mv_500"]
            df_mv_1000 = self.py_dbm["df_mv_1000"]
            df_mv_2000 = self.py_dbm["df_mv_2000"]
            df_mv_tail = self.py_dbm["df_mv_tail"]
            df_mv_st = self.py_dbm["df_mv_st"]
        except KeyError as e:
            logger.error(f"df_mv_n is not exist - {repr(e)}")
            raise KeyError(f"{e.args[0]} is not exist")
        df_stocks_in_ssb = pd.DataFrame(
            index=self.list_all_stocks, columns=["ssb_index"]
        )
        for symbol in df_stocks_in_ssb.index:
            if symbol in df_mv_300.index:
                if symbol in df_mv_50.index:
                    df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_50"
                else:
                    df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_300"
            elif symbol in df_mv_500.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_500"
            elif symbol in df_mv_1000.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_1000"
            elif symbol in df_mv_2000.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_2000"
            elif symbol in df_mv_tail.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_tail"
            elif symbol in df_mv_st.index:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "ssb_st"
            else:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "NON"
        self.py_dbm["df_stocks_in_ssb"] = df_stocks_in_ssb
        logger.trace(f"{name} End")
        return self.py_dbm["df_stocks_in_ssb"]

    def realtime_index(self):
        df_stocks_in_ssb = self.py_dbm["df_stocks_in_ssb"]
        df_realtime = analysis.ashare.stock_zh_a_spot_em()[["total_mv"]]  # 调用实时数据接口
        df_mv_all = self.py_dbm["df_mv_all"][["base_mv", "now_mv"]]
        df_mv_now = pd.concat(
            objs=[
                df_stocks_in_ssb,
                df_realtime,
                df_mv_all,
            ],
            axis=1,
            join="outer",
        )
        df_mv_now.dropna(subset=["ssb_index"], inplace=True)
        df_mv_now.rename(
            columns={
                "total_mv": "now_mv",
                "now_mv": "t1_mv",
            },
            inplace=True,
        )
        df_index_now = pd.pivot_table(
            data=df_mv_now,
            index=["ssb_index"],
            aggfunc=np.sum,
        )
        if "NON" in df_index_now.index:
            df_index_now.drop(index=["NON"], inplace=True)
        df_index_now.at["ssb_all", "base_mv"] = df_index_now["base_mv"].sum()
        df_index_now.at["ssb_all", "now_mv"] = df_index_now["now_mv"].sum()
        df_index_now.at["ssb_all", "t1_mv"] = df_index_now["t1_mv"].sum()
        df_index_now.at["ssb_300", "base_mv"] += df_index_now.at["ssb_50", "base_mv"]
        df_index_now.at["ssb_300", "now_mv"] += df_index_now.at["ssb_50", "now_mv"]
        df_index_now.at["ssb_300", "t1_mv"] += df_index_now.at["ssb_50", "t1_mv"]
        df_index_now.at["ssb_non_st", "base_mv"] = (
            df_index_now.at["ssb_all", "base_mv"] - df_index_now.at["ssb_st", "base_mv"]
        )
        df_index_now.at["ssb_non_st", "now_mv"] = (
            df_index_now.at["ssb_all", "now_mv"] - df_index_now.at["ssb_st", "now_mv"]
        )
        df_index_now.at["ssb_non_st", "t1_mv"] = (
            df_index_now.at["ssb_all", "t1_mv"] - df_index_now.at["ssb_st", "t1_mv"]
        )
        df_index_now["index_now"] = (
            df_index_now["now_mv"] / df_index_now["base_mv"] * 1000
        )
        df_index_now["index_1t"] = (
            df_index_now["t1_mv"] / df_index_now["base_mv"] * 1000
        )
        df_index_now["index_now"] = df_index_now["index_now"].apply(
            func=lambda x: round(x, 2)
        )
        # df_index_now["index_1t"] = df_index_now["index_1t"].apply(func=lambda x: round(x, 2))
        try:
            df_index_ssb_min = self.py_dbm["df_index_ssb_min"]
        except KeyError as e:
            logger.error(f"df_index_ssb_min is not exist - {repr(e)}")
            df_index_ssb_min = pd.DataFrame(columns=df_index_now.index)
        dt_now = datetime.datetime.now()
        df_index_ssb_min.loc[dt_now] = df_index_now["index_now"]
        if len(df_index_ssb_min) < 2:
            df_index_ssb_min = df_index_ssb_min.reindex(
                columns=[
                    "ssb_all",
                    "ssb_non_st",
                    "ssb_50",
                    "ssb_300",
                    "ssb_500",
                    "ssb_1000",
                    "ssb_2000",
                    "ssb_tail",
                    "ssb_st",
                ]
            )
        if dt_am_start < dt_now < dt_am_end or dt_pm_start < dt_now < dt_pm_end:
            self.py_dbm["df_index_ssb_min"] = df_index_ssb_min
        if not self.py_dbm["df_index_ssb_min"].empty:
            self.__make_charts_min()
        dict_return = df_index_ssb_min.iloc[-1].to_dict()
        return dict_return

    def __make_charts_min(self):
        name = "make_charts"
        logger.trace(f"{name} Begin")
        try:
            df_index_ssb_min = self.py_dbm["df_index_ssb_min"]
        except KeyError as e:
            logger.error(f"df_index_ssb_min is not exist - {repr(e)}")
            raise KeyError("df_index_ssb_min is not exist")
        df_index_ssb_min.sort_index(inplace=True)
        x_axis = df_index_ssb_min.index.tolist()
        dict_list_index_n = {
            "all": df_index_ssb_min["ssb_all"].tolist(),
            "non_st": df_index_ssb_min["ssb_non_st"].tolist(),
            "50": df_index_ssb_min["ssb_50"].tolist(),
            "300": df_index_ssb_min["ssb_300"].tolist(),
            "500": df_index_ssb_min["ssb_500"].tolist(),
            "1000": df_index_ssb_min["ssb_1000"].tolist(),
            "2000": df_index_ssb_min["ssb_2000"].tolist(),
            "tail": df_index_ssb_min["ssb_tail"].tolist(),
        }
        list_values = list()
        for key in dict_list_index_n:
            list_values += dict_list_index_n[key]
        list_values = [x for x in list_values if ~np.isnan(x)]
        y_min = min(list_values)
        y_max = max(list_values)
        line_index = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index.add_xaxis(xaxis_data=x_axis)
        for key in dict_list_index_n:
            line_index.add_yaxis(
                series_name=f"ssb_{key}",
                y_axis=dict_list_index_n[key],
                is_symbol_show=False,
            )
        line_index.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_top="0%", pos_left="50%"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min, max_=y_max),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index.set_colors(
            colors=[
                "red",
                "orange",
                "olive",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "deeppink",
            ]
        )
        # st
        line_index_st = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
            ),
        )
        line_index_st.add_xaxis(xaxis_data=x_axis)
        y_axis_st = df_index_ssb_min["ssb_st"].tolist()
        y_min_st = min(y_axis_st)
        y_max_st = max(y_axis_st)
        line_index_st.add_yaxis(
            series_name=f"ssb_st",
            y_axis=y_axis_st,
            is_symbol_show=False,
        )
        line_index_st.set_global_opts(
            title_opts=opts.TitleOpts(
                title="SSB Index ST", pos_top="0%", pos_left="50%"
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(
                orient="vertical", pos_right="0px", pos_top="60px"
            ),
            yaxis_opts=opts.AxisOpts(min_=y_min_st, max_=y_max_st),
            datazoom_opts=opts.DataZoomOpts(range_start=0, range_end=100),
        )
        line_index_st.set_colors(
            colors=[
                "red",
            ]
        )
        page = Page(
            page_title="Stock Index",
        )
        page.add(line_index, line_index_st)
        page.render(path=self.filename_index_charts_min)
        logger.trace(f"{name} End")

    def shelve_to_excel(self):
        logger.trace("shelve_to_excel Begin")

        def is_open(filename) -> bool:
            if not os.access(path=filename, mode=os.F_OK):
                logger.error(f"[{filename}] is not exist")
                return False
            else:
                logger.trace(f"[{filename}] is exist")
            try:
                v_handle = win32file.CreateFile(
                    filename,
                    win32file.GENERIC_READ,
                    0,
                    None,
                    win32file.OPEN_EXISTING,
                    win32file.FILE_ATTRIBUTE_NORMAL,
                    None,
                )
            except Exception as e_in:
                print(f"{filename} - {repr(e_in)}")
                logger.trace(f"{filename} - {repr(e_in)}")
                return True
            else:
                v_handle.close()
                logger.trace("close Handle")
                logger.trace(f"[{filename}] not in use")
                return False

        filename_excel = os.path.join(self.path_check, f"chip_SSB.xlsx")
        i_file = 0
        while True:
            if is_open(filename=filename_excel):
                logger.trace(f"[{filename_excel}] is open")
            else:
                logger.trace(f"[{filename_excel}] is not open")
                break
            i_file += 1
            filename_excel = os.path.join(self.path_check, f"chip_SSB_{i_file}.xlsx")
        keys_df = list(self.py_dbm.keys())
        keys_df = [x for x in keys_df if "df" in x]
        key_random = random.choice(keys_df)
        try:
            writer = pd.ExcelWriter(
                path=filename_excel, mode="a", if_sheet_exists="replace"
            )
        except FileNotFoundError as e:
            logger.error(f"{repr(e)}")
            with pd.ExcelWriter(path=filename_excel, mode="w") as writer_e:
                if isinstance(self.py_dbm[key_random], pd.DataFrame):
                    self.py_dbm[key_random].to_excel(
                        excel_writer=writer_e, sheet_name=key_random
                    )
            writer = pd.ExcelWriter(
                path=filename_excel, mode="a", if_sheet_exists="replace"
            )
        count = len(keys_df)
        i = 0
        for key in keys_df:
            i += 1
            str_shelve_to_excel = f"[{i}/{count}] - {key}"
            print(f"\r{str_shelve_to_excel}\033[K", end="")
            if key != key_random:
                if isinstance(self.py_dbm[key], pd.DataFrame):
                    self.py_dbm[key].to_excel(excel_writer=writer, sheet_name=key)
        writer.close()
        if i >= count:
            print("\n", end="")  # 格式处理
        logger.trace("shelve_to_excel End")

    def reset_index_ssb(self):
        self.df_index_exist["index_ssb"] = 0
        del self.py_dbm["df_index_ssb"]
        self.make()
        self.stocks_in_ssb()
        self.shelve_to_excel()

    def test(self):
        pass

    def __del__(self):
        self.py_dbm.close()
        print("del py_dbm")
