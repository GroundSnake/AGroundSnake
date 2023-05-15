# modified at 2023/5/14 10:08
import os
import time
import numpy as np
import requests
import datetime
import shelve
import pandas as pd
from loguru import logger
from pyecharts.charts import Line
import pyecharts.options as opts
import tushare as ts


class IndexSSB(object):
    def __init__(self, origin: int = 2023):
        self.__name = "IndexSSB"
        logger.trace(f"__init__ Begin")
        self.__pro = ts.pro_api()
        self.date_origin = datetime.date(year=origin, month=1, day=1)
        self.str_date_origin = self.date_origin.strftime("%Y%m%d")
        self.date_now = datetime.datetime.now().date()
        self.str_date_now = self.date_now.strftime("%Y%m%d")
        path_mv = os.path.join(os.getcwd(), "data", f"mv")
        self.filename_shelve = os.path.join(path_mv, f"mv")
        path_charts = os.path.join(os.getcwd(), "check")
        self.filename_index_charts = os.path.join(path_charts, "index_charts.html")
        self.py_dbm = shelve.open(filename=self.filename_shelve, flag="c")
        pro = ts.pro_api()
        df_trade_cal = pro.trade_cal(
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
            logger.trace(f"load self.df_index_exist from py_dbm")
            self.df_index_exist = self.df_index_exist.reindex(index=df_trade_cal.index)
            self.df_index_exist["is_open"] = df_trade_cal["is_open"]
        except KeyError as e:
            logger.trace(f"self.df_index_exist - Error[{repr(e)}]")
            self.df_index_exist = df_trade_cal
            logger.trace(f"load self.df_index_exist from [API]")
            self.df_index_exist = self.df_index_exist.reindex(
                columns=["is_open", "market_value", "index_ssb"]
            )
        self.df_index_exist.fillna(value=0, inplace=True)
        self.py_dbm["df_index_exist"] = self.df_index_exist
        self.df_mv = self.__pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,list_date",
        )
        self.list_all_stocks = self.df_mv["ts_code"].tolist()
        self.list_all_stocks = [item[-2:].lower() + item[:6] for item in self.list_all_stocks]
        self.make()
        logger.trace(f"__init__ End")

    def __get_market_values(self, date_pos: datetime.date) -> bool:
        name = "market_value"
        data = "is_open"
        logger.trace(f'{name} Begin')
        if self.df_index_exist.at[date_pos, data] == 1:
            if self.df_index_exist.at[date_pos, name] == 1:
                logger.trace(f'{name} Break, and End')
                return True
        else:
            print(f"get_market_values: {date_pos} is not trading day")
            logger.trace(f"{date_pos} is not trading day")
            return False
        str_date_pos_ul = date_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        str_date_pos = date_pos.strftime("%Y%m%d")
        dt_init = datetime.datetime(year=1989, month=1, day=1)
        try:
            df_mv = self.py_dbm[str_df_mv]
        except KeyError as e:
            logger.trace(f"{str_df_mv} is not exist - {repr(e)}")
            df_mv = self.df_mv
            df_mv["symbol"] = df_mv["ts_code"].apply(func=lambda ts_code: ts_code[-2:].lower() + ts_code[:6])
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
            df_mv["base_mv_date"] = dt_init
            logger.trace(f"load {str_df_mv} from [API]")
        i = 0
        count = len(df_mv)
        df_mv.sort_values(by=["base_mv"], inplace=True)
        df_mv = df_mv.sample(frac=1)
        for symbol in df_mv.index:
            self.py_dbm[str_df_mv] = df_mv
            i += 1
            str_msg_bar = f"{str_df_mv}: [{i:04d}/{count}] - [{symbol}]"
            if df_mv.at[symbol, "base_mv_date"] == dt_init:
                i_times = 0
                while True:
                    i_times += 1
                    try:
                        df_daily_basic_symbol = self.__pro.daily_basic(
                            ts_code=df_mv.at[symbol, "ts_code"],
                            start_date=self.str_date_origin,
                            end_date=str_date_pos,
                            fields="trade_date,close,total_share,total_mv",
                        )
                    except requests.exceptions.ConnectionError as e:
                        print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                        logger.trace(repr(e))
                        time.sleep(2)
                        df_daily_basic_symbol = pd.DataFrame()
                    if not df_daily_basic_symbol.empty:
                        df_daily_basic_symbol["trade_date"] = pd.to_datetime(
                            df_daily_basic_symbol["trade_date"]
                        )
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
                        total_share = df_daily_basic_symbol.at[max_index, "total_share"]
                        df_mv.at[symbol, "base_mv_date"] = df_daily_basic_symbol.at[
                            min_index, "trade_date"
                        ]
                        """base_mv --- 吸收合并及增减股本，修证初始市值"""
                        if (
                            df_daily_basic_symbol.at[min_index, "total_share"]
                            == total_share
                        ):
                            df_mv.at[symbol, "base_mv"] = df_daily_basic_symbol.at[
                                min_index, "total_mv"
                            ]
                            df_mv.at[symbol, "base_mv_date"] = df_daily_basic_symbol.at[
                                min_index, "trade_date"
                            ]
                            df_mv.at[symbol, "now_mv"] = df_daily_basic_symbol.at[
                                max_index, "total_mv"
                            ]
                            df_mv.at[
                                symbol, "now_mv_date"
                            ] = now_mv_date = df_daily_basic_symbol.at[
                                max_index, "trade_date"
                            ]
                            str_msg_bar += f" - [{now_mv_date.date()}]"
                            df_mv.at[symbol, "t1_mv"] = df_daily_basic_symbol.at[
                                max2_index, "total_mv"
                            ]
                            df_mv.at[symbol, "t1_mv_date"] = df_daily_basic_symbol.at[
                                max2_index, "trade_date"
                            ]
                        else:
                            i_pro_bar = 0
                            while True:
                                i_pro_bar += 1
                                try:
                                    df_pro_bar_symbol = ts.pro_bar(
                                        ts_code=df_mv.at[symbol, "ts_code"],
                                        adj="qfq",
                                        start_date=self.str_date_origin,
                                        end_date=str_date_pos,
                                    )
                                except requests.exceptions.ConnectionError as e:
                                    print(f"\r{str_msg_bar} - {repr(e)}\033[K")
                                    logger.trace(repr(e))
                                    time.sleep(2)
                                    df_pro_bar_symbol = pd.DataFrame()
                                if not df_pro_bar_symbol.empty:
                                    df_pro_bar_symbol["trade_date"] = pd.to_datetime(
                                        df_pro_bar_symbol["trade_date"]
                                    )
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
                                    df_mv.at[symbol, "base_mv"] = (
                                        df_pro_bar_symbol.at[min_index_close, "close"]
                                        * total_share
                                    )
                                    df_mv.at[
                                        symbol, "base_mv_date"
                                    ] = df_pro_bar_symbol.at[
                                        min_index_close, "trade_date"
                                    ]
                                    df_mv.at[symbol, "now_mv"] = (
                                        df_pro_bar_symbol.at[max_index_close, "close"]
                                        * total_share
                                    )
                                    df_mv.at[
                                        symbol, "now_mv_date"
                                    ] = now_mv_date = df_pro_bar_symbol.at[
                                        max_index_close, "trade_date"
                                    ]
                                    str_msg_bar += f" - [{now_mv_date.date()}]"
                                    df_mv.at[symbol, "t1_mv"] = (
                                        df_pro_bar_symbol.at[max2_index_close, "close"]
                                        * total_share
                                    )
                                    df_mv.at[
                                        symbol, "t1_mv_date"
                                    ] = df_pro_bar_symbol.at[
                                        max2_index_close, "trade_date"
                                    ]
                                    break
                                else:
                                    print(
                                        f"\r{str_msg_bar} - sleep[{i_pro_bar}]......\033[K",
                                        end="",
                                    )
                                    time.sleep(2)
                                    if i_pro_bar > 2:
                                        break
                        break
                    else:
                        print(f"\r{str_msg_bar} - sleep[{i_times}]......\033[K", end="")
                        time.sleep(1)
                        if i_times > 1:
                            df_mv.drop(index=symbol, inplace=True)
                            print(" - drop")
                            break
            else:
                str_msg_bar += " - latest"
            print(f"\r{str_msg_bar}\033[K", end="")
        if i >= count:
            print("\n", end="")  # 格式处理
            if "ts_code" in df_mv.columns:
                df_mv.drop(columns="ts_code", inplace=True)
            df_mv.applymap(
                func=lambda x: round(x / 10000, 2)
                if (isinstance(x, (int, float)) and x > 100)
                else (round(x, 4) if (isinstance(x, (int, float)) and x < 100) else x)
            )
            self.df_index_exist.at[date_pos, name] = 1
            self.py_dbm["df_index_exist"] = self.df_index_exist
            self.py_dbm[str_df_mv] = df_mv
        logger.trace(f'{name} End')
        return True

    def __make_index(self, date_pos: datetime.date):
        name = "index_ssb"
        data = "market_value"
        logger.trace(f'{name} Begin')
        str_date_pos_ul = date_pos.strftime("%Y_%m_%d")
        str_df_mv = f"mv_{str_date_pos_ul}"
        if (
            self.df_index_exist.at[date_pos, data] == 1
            and self.df_index_exist.at[date_pos, name] == 1
        ):
            logger.trace(f"{name} Break, and End")
            return True
        if self.__get_market_values(date_pos=date_pos):
            df_mv = self.py_dbm[str_df_mv]
        else:
            print(f"make_index: {date_pos} is not trading day")
            logger.trace(f"{date_pos} is not trading day")
            return False
        df_mv_not_st = df_mv[~df_mv["name"].str.contains("ST").fillna(False)].copy()
        df_mv_st = df_mv[df_mv["name"].str.contains("ST").fillna(False)].copy()
        try:
            df_index_ssb = self.py_dbm["df_index_ssb"]
            logger.trace(f"load df_index_ssb from py_dbm")
        except KeyError as e:
            logger.trace(f"load df_index_ssb from py_dbm fail - Error[{repr(e)}]")
            df_index_ssb = pd.DataFrame(
                columns=[
                    "base_mv_all",
                    "now_mv_all",
                    "stocks_index_not_st",
                    "stocks_index_50",
                    "stocks_index_300",
                    "stocks_index_500",
                    "stocks_index_1000",
                    "stocks_index_2000",
                    "stocks_index_tail",
                    "stocks_index_st",
                    "base_mv_not_st",
                    "base_mv_50",
                    "base_mv_300",
                    "base_mv_500",
                    "base_mv_1000",
                    "base_mv_2000",
                    "base_mv_tail",
                    "base_mv_st",
                    "now_mv_not_st",
                    "now_mv_50",
                    "now_mv_300",
                    "now_mv_500",
                    "now_mv_1000",
                    "now_mv_2000",
                    "now_mv_tail",
                    "now_mv_st",
                ]
            )
        # print(df_index_ssb)
        df_mv_50 = df_mv_not_st.iloc[:50].copy()
        df_mv_300 = df_mv_not_st.iloc[:300].copy()
        df_mv_500 = df_mv_not_st.iloc[300:800].copy()
        df_mv_1000 = df_mv_not_st.iloc[800:1800].copy()
        df_mv_2000 = df_mv_not_st.iloc[1800:3800].copy()
        df_mv_tail = df_mv_not_st.iloc[3800:].copy()
        dict_df_index_n = {
            "all": df_mv,
            "not_st": df_mv_not_st,
            "st": df_mv_st,
            "50": df_mv_50,
            "300": df_mv_300,
            "500": df_mv_500,
            "1000": df_mv_1000,
            "2000": df_mv_2000,
            "tail": df_mv_tail,
        }
        for key in dict_df_index_n:
            df_mv_n_name = f"df_mv_{key}"
            df_mv_n = dict_df_index_n[key]
            base_mv_n = df_mv_n["base_mv"].sum()
            now_mv_n = df_mv_n["now_mv"].sum()
            df_index_ssb.at[date_pos, f"base_mv_{key}"] = round(base_mv_n / 100000000, 2)
            df_index_ssb.at[date_pos, f"now_mv_{key}"] = round(now_mv_n / 100000000, 2)
            stocks_index_n = now_mv_n / base_mv_n * 1000
            stocks_index_n = round(stocks_index_n, 2)
            df_index_ssb.at[date_pos, f"stocks_index_{key}"] = stocks_index_n
            df_mv_n[f"contribution_points_index_{key}"] = (
                    (df_mv_n["now_mv"] - df_mv_n["base_mv"])
                    / base_mv_n
                    * 1000
            )
            df_mv_n[f"t1_contribution_points_index_{key}"] = (
                    (df_mv_n["t1_mv"] - df_mv_n["base_mv"])
                    / base_mv_n
                    * 1000
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
            self.py_dbm[df_mv_n_name] = dict_df_index_n[key] = df_mv_n
        self.df_index_exist.at[date_pos, name] = 1
        self.py_dbm["df_index_exist"] = self.df_index_exist
        self.py_dbm['df_index_ssb'] = df_index_ssb
        df_mv_not_st.sort_values(by=["base_mv"], ascending=False, inplace=True)
        logger.trace(f"{name} End")
        return True

    def __make_index_line(self):
        logger.trace("make_index_line Start")
        i = 0
        count = len(self.df_index_exist.index)
        for date in self.df_index_exist.index:
            i += 1
            str_msg_bar = f'Current Date: {date} - [{i:03d}/{count:03d}]'
            if self.df_index_exist.at[date, "is_open"] == 1:
                if not self.__make_index(date):
                    print(f"make_index_line: {date} is not trading day")
                    logger.trace(f"{date} is not trading day")
                str_msg_bar += ' - [trading day]'
            else:
                str_msg_bar += ' - [non trading day]'
                print(f"\r{str_msg_bar}\033[K")
                continue
            print(f"\r{str_msg_bar}\033[K")
        self.py_dbm["df_index_exist"] = self.df_index_exist
        logger.trace("make_index_line End")

    def __make_charts(self):
        name = 'make_charts'
        logger.trace(f"{name} Begin")
        try:
            df_index_ssb = self.py_dbm['df_index_ssb']
        except KeyError as e:
            logger.trace(f"df_index_ssb is not exist - {repr(e)}")
            raise KeyError('df_index_ssb is not exist')
        x_axis = df_index_ssb.index.tolist()
        dict_list_index_n = {
            'all': df_index_ssb["stocks_index_all"].tolist(),
            'not_st': df_index_ssb["stocks_index_not_st"].tolist(),
            'st': df_index_ssb["stocks_index_st"].tolist(),
            '50': df_index_ssb["stocks_index_50"].tolist(),
            '300': df_index_ssb["stocks_index_300"].tolist(),
            '500': df_index_ssb["stocks_index_500"].tolist(),
            '1000': df_index_ssb["stocks_index_1000"].tolist(),
            '2000': df_index_ssb["stocks_index_2000"].tolist(),
            'tail': df_index_ssb["stocks_index_tail"].tolist(),
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
                page_title="SSB index",
            )
        )
        line_index.add_xaxis(xaxis_data=x_axis)
        for key in dict_list_index_n:
            line_index.add_yaxis(
                series_name=f"index_{key}",
                y_axis=dict_list_index_n[key],
                is_symbol_show=False,
                markpoint_opts=opts.MarkPointOpts(
                    data=[
                        opts.MarkPointItem(name="最大值", type_="max"),
                        opts.MarkPointItem(name="最小值", type_="min"),
                    ]
                ),
            )
        line_index.set_global_opts(
            title_opts=opts.TitleOpts(title="SSB Index", pos_left="center"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top=60),
            yaxis_opts=opts.AxisOpts(
                min_=y_min,
                max_=y_max,
            ),
            datazoom_opts=opts.DataZoomOpts(
                range_start=0,
                range_end=100,
            ),
        )
        line_index.set_colors(
            colors=[
                "red",
                "orange",
                "yellow",
                "green",
                "blue",
                "purple",
                "black",
                "brown",
                "pink",
            ]
        )
        line_index.render(path=self.filename_index_charts)
        logger.trace(f"{name} End")

    def make(self):
        self.__make_index_line()
        self.__make_charts()

    def stocks_in_ssb(self) -> pd.DataFrame:
        name = 'stocks_in_ssb'
        logger.trace(f"{name} Begin")
        try:
            df_mv_50 = self.py_dbm["df_mv_50"]
            df_mv_300 = self.py_dbm["df_mv_300"]
            df_mv_500 = self.py_dbm["df_mv_500"]
            df_mv_1000 = self.py_dbm["df_mv_1000"]
            df_mv_2000 = self.py_dbm["df_mv_2000"]
            df_mv_tail = self.py_dbm["df_mv_tail"]
        except KeyError as e:
            logger.trace(f"df_mv_n is not exist - {repr(e)}")
            return pd.DataFrame()
        df_stocks_in_ssb = pd.DataFrame(index=self.list_all_stocks, columns=["ssb_index"])
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
            else:
                df_stocks_in_ssb.at[symbol, "ssb_index"] = "NA"
            print(f"\r{name}:[{symbol}]\033[K", end="")
        self.py_dbm["df_stocks_in_ssb"] = df_stocks_in_ssb
        logger.trace(f"{name} End")
        return df_stocks_in_ssb

    def test(self):
        pass
        print(list(self.py_dbm.keys()))
        self.py_dbm['df_mv_500'].to_csv('a.csv')
        self.py_dbm['df_index_ssb'].to_csv('b.csv')
        # print(self.py_dbm["df_index_exist"].head(10))
        # df = self.py_dbm["df_index_exist"]
        # df['market_value'] = 0
        # self.py_dbm["df_index_exist"] = df
        # print(self.py_dbm["df_index_exist"].head(10))
        # print(self.py_dbm["mv_2023_01_03"])
        # self.py_dbm["mv_2023_01_03"].to_csv('a.csv')
        # del self.py_dbm["df_index_exist"]
        # print(list(self.py_dbm.keys()))

    def __del__(self):
        self.py_dbm.close()
        print("del py_dbm")

