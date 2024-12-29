import datetime
import pandas as pd
from console import fg
import pandas_ta as ta
from analysis.base import code_ts_to_ths, feather_from_file
from analysis.api_tushare import index_weight
from analysis.const_dynamic import (
    dt_pm_end,
    dt_pm_end_1t,
    filename_pool_history,
    format_dt,
    days_delete,
)
from analysis.update_data import kline


class RealTimeBBands(object):
    def __init__(self, symbols: list | None = None) -> None:
        if isinstance(symbols, list):
            self.symbols = symbols
        elif symbols is None:
            self.symbols = list()
        else:
            raise ValueError("symbols is not list")
        self.df_real_time_bbands = pd.DataFrame()
        self.__make_bbands()
        return

    def get_item(self, symbol: str, items: str) -> float:
        if symbol in self.symbols:
            return round(self.df_real_time_bbands.at[symbol, items], 2)
        else:
            self.add_symbol(symbols=symbol)
            if symbol in self.symbols:
                return round(self.df_real_time_bbands.at[symbol, items], 2)
            else:
                return 0.0

    def add_symbol(self, symbols: str | list) -> None:
        if isinstance(symbols, str):
            symbols = [symbols]
        self.__make_bbands(symbols=symbols)
        return

    def __make_bbands(self, symbols: list | None = None) -> None:
        if symbols is None:
            symbols = self.symbols.copy()
        dt_start = dt_pm_end - datetime.timedelta(days=14)
        dt_end = dt_pm_end
        if symbols is None or len(symbols) == 0:
            return
        for symbol in symbols:
            df_kline = kline(
                symbol=symbol,
                frequency="D",
                adjust="qfq",
                start_date=dt_start,
                end_date=dt_end,
            )
            if df_kline.empty:
                df_bbands = pd.DataFrame(
                    index=[dt_pm_end_1t],
                    columns=["BBL", "BBM", "BBU", "BBB", "BBP"],
                    dtype="float",
                )
                df_bbands.fillna(
                    value={
                        "BBL": 0.0,
                        "BBM": 0.0,
                        "BBU": 0.0,
                        "BBB": 0.0,
                        "BBP": 0.0,
                    },
                    inplace=True,
                )
            else:
                df_bbands = df_kline.ta.bbands(length=5, std=1.65)
                df_bbands.bfill(inplace=True)
                df_bbands = df_bbands.rename(columns=lambda x: x[:3])
            if self.df_real_time_bbands.empty:
                self.df_real_time_bbands = pd.DataFrame(columns=df_bbands.columns)
            index_max = df_bbands.index.max()
            self.df_real_time_bbands.loc[symbol] = df_bbands.loc[index_max]
            self.symbols.append(symbol)
        return


class RealtimeLevel(object):
    def __init__(self, symbols: list | None = None) -> None:
        if isinstance(symbols, list):
            self.symbols = symbols
        elif symbols is None:
            self.symbols = list()
        else:
            raise ValueError("symbols is not list")
        self.df_real_time_level = pd.DataFrame()
        self.days = -3
        self.__make_level()
        return

    def get_level(self, symbol: str) -> bool:
        if symbol not in self.symbols:
            self.add_symbol(symbols=symbol)
        pct_chg_min = self.df_real_time_level.at[symbol, "pct_chg_min"]
        pct_chg_max = self.df_real_time_level.at[symbol, "pct_chg_max"]
        if 3 > pct_chg_max > 0 > pct_chg_min > -3:
            return True
        else:
            return False

    def add_symbol(self, symbols: str | list) -> None:
        if isinstance(symbols, str):
            symbols = [symbols]
        self.__make_level(symbols=symbols)
        return

    def __make_level(self, symbols: list | None = None) -> None:
        if symbols is None:
            symbols = self.symbols.copy()
        if symbols is None or len(symbols) == 0:
            return
        dt_start = dt_pm_end - datetime.timedelta(days=14)
        dt_end = dt_pm_end
        for symbol in symbols:
            self.symbols.append(symbol)
            df_kline = kline(
                symbol=symbol,
                frequency="D",
                adjust="qfq",
                start_date=dt_start,
                end_date=dt_end,
            )
            if df_kline.empty:
                self.df_real_time_level.at[symbol, "pct_chg_min"] = 0.0
                self.df_real_time_level.at[symbol, "pct_chg_max"] = 0.0
                continue
            try:
                df_kline["pct_chg"] = round(
                    (df_kline["close"] / df_kline["pre_close"] - 1) * 100, 2
                )
            except KeyError:
                close_first = df_kline.iloc[0]["close"]
                df_kline["pre_close"] = df_kline["close"].shift(
                    periods=1, fill_value=close_first
                )
                df_kline["pct_chg"] = round(
                    (df_kline["close"] / df_kline["pre_close"] - 1) * 100, 2
                )
            df_kline_N = df_kline.iloc[self.days :]
            if df_kline_N.empty:
                self.df_real_time_level.at[symbol, "pct_chg_min"] = 0.0
                self.df_real_time_level.at[symbol, "pct_chg_max"] = 0.0
            else:
                self.df_real_time_level.at[symbol, "pct_chg_min"] = df_kline_N[
                    "pct_chg"
                ].min()
                self.df_real_time_level.at[symbol, "pct_chg_max"] = df_kline_N[
                    "pct_chg"
                ].max()
        return


class _Context(object):
    def __init__(self) -> None:
        self.df_index_weight = self.__get_index_weight_custom()
        self.dict_pool_history_rate = dict()
        self.realtime_bbands = RealTimeBBands()
        self.realtime_level = RealtimeLevel()
        self.__make_pool_rate()
        return

    @staticmethod
    def __get_index_weight_custom() -> pd.DataFrame:
        def get_index_weight_new(index_code: str = "000016.SH"):
            df = index_weight(index_code=index_code)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df[df["trade_date"] == df["trade_date"].max()]
            df["symbol"] = df["con_code"].apply(func=code_ts_to_ths)
            df.set_index(keys=["symbol"], inplace=True)
            df = df.reindex(columns=["index_code", "weight"])
            return df

        df_index_300 = get_index_weight_new(index_code="000300.SH")
        df_index_500 = get_index_weight_new(index_code="000905.SH")
        df_index_1000 = get_index_weight_new(index_code="000852.SH")
        df_index_2000 = get_index_weight_new(index_code="932000.CSI")
        df_index_weight = pd.concat(
            objs=[df_index_300, df_index_500, df_index_1000, df_index_2000],
            axis=0,
            join="outer",
        )
        df_index_weight = df_index_weight[
            ~df_index_weight.index.duplicated(keep="first")
        ]
        return df_index_weight

    def get_index_name(self, symbol: str) -> str:
        index_name = "或是微盘股"
        if symbol in self.df_index_weight.index:
            if self.df_index_weight.at[symbol, "index_code"] == "000300.SH":
                index_name = "沪深300"
            elif self.df_index_weight.at[symbol, "index_code"] == "000905.SH":
                index_name = "中证500"
            elif self.df_index_weight.at[symbol, "index_code"] == "000852.SH":
                index_name = fg.purple("中证1000")
            elif self.df_index_weight.at[symbol, "index_code"] == "932000.CSI":
                index_name = fg.purple("中证2000")
        return index_name

    def __make_pool_rate(self) -> None:
        df_pool_history = feather_from_file(filename_df=filename_pool_history)
        if df_pool_history.empty:
            return
        days = days_delete
        count = df_pool_history.shape[0]
        count_weight = min(days, count)
        dt_stale = datetime.datetime.strptime(df_pool_history.index.name, format_dt)
        if dt_pm_end_1t <= dt_stale <= dt_pm_end:
            df_pool_history.loc[dt_stale] = df_pool_history.loc[dt_stale].apply(
                func=lambda x: x + count_weight if x > 0 else x
            )
        df_pool_history = df_pool_history.iloc[:days]
        for column in df_pool_history.columns:
            self.dict_pool_history_rate[column] = round(
                df_pool_history[column].sum() / count * 100, 2
            )
        return

    def get_pool_rate(self, symbol: str) -> float:
        try:
            r = self.dict_pool_history_rate[symbol]
        except KeyError:
            r = 0.0
        return r


context = _Context()
