import time
import random
import datetime
import pandas as pd
from analysis.const_dynamic import (
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    path_user_home,
    format_dt,
)
from analysis.log import logger
from analysis.api_akshare import (
    ak_stock_zh_a_spot_em,
    ak_stock_hk_spot_em,
    ak_fund_etf_spot_em,
)
from analysis.base import sleep_to_time, feather_to_file, feather_from_file


class _Realtime_Quotation(object):
    def __init__(self) -> None:
        self.filename_em_stocks_a = path_user_home.joinpath("df_em_stocks_a.ftr")
        self.filename_em_etfs_a = path_user_home.joinpath("df_em_etfs_a.ftr")
        self.filename_em_stocks_hk = path_user_home.joinpath("df_em_stocks_hk.ftr")
        self.df_em_stocks_a = feather_from_file(filename_df=self.filename_em_stocks_a)
        if self.df_em_stocks_a.empty:
            self.df_em_stocks_a = ak_stock_zh_a_spot_em()
        self.df_em_etfs_a = feather_from_file(filename_df=self.filename_em_etfs_a)
        if self.df_em_etfs_a.empty:
            self.df_em_etfs_a = ak_fund_etf_spot_em()
        self.df_em_stocks_hk = feather_from_file(filename_df=self.filename_em_stocks_hk)
        if self.df_em_stocks_hk.empty:
            self.df_em_stocks_hk = ak_stock_hk_spot_em()
        self.list_stocks_a = self.df_em_stocks_a.index.tolist()
        self.list_etfs_a = self.df_em_etfs_a.index.tolist()
        self.list_stocks_hk = self.df_em_stocks_hk.index.tolist()
        if self.df_em_stocks_a.empty:
            self.dt_now = datetime.datetime.now()
        else:
            self.dt_now = datetime.datetime.strptime(
                self.df_em_stocks_a.index.name, format_dt
            )
        str_initialization = (
            f"<{self.dt_now.time().replace(microsecond=0)}>----"
            f"[Initialization Realtime_Quotation Finish!]"
        )
        print(str_initialization)

    def upgrade_symbols(self) -> None:
        self.list_stocks_a = self.df_em_stocks_a.index.tolist()
        self.list_etfs_a = self.df_em_etfs_a.index.tolist()
        self.list_stocks_hk = self.df_em_stocks_hk.index.tolist()
        return

    def get_close(self, symbol: str) -> float:
        close = 0.0
        bool_not_exist = True
        self.upgrade_symbols()
        try:
            if symbol in self.list_stocks_a:
                close = self.df_em_stocks_a.at[symbol, "close"]
                bool_not_exist = False
            if symbol in self.list_etfs_a:
                close = self.df_em_etfs_a.at[symbol, "close"]
                bool_not_exist = False
            if symbol in self.list_stocks_hk:
                close = self.df_em_stocks_hk.at[symbol, "close"]
                bool_not_exist = False
        except KeyError as e:
            logger.error(f"{symbol} - Error - [{e}]")
            self.update_from_network()
        if bool_not_exist:
            logger.error(f"{symbol} not Exist")
        return close

    def get_name(self, symbol: str) -> str:
        name = "Closed"
        bool_not_exist = True
        self.upgrade_symbols()
        try:
            if symbol in self.list_stocks_a:
                name = self.df_em_stocks_a.at[symbol, "name"]
                bool_not_exist = False
            if symbol in self.list_etfs_a:
                name = self.df_em_etfs_a.at[symbol, "name"]
                bool_not_exist = False
            if symbol in self.list_stocks_hk:
                name = self.df_em_stocks_hk.at[symbol, "name"]
                bool_not_exist = False
        except KeyError as e:
            logger.error(f"{symbol} - Error - [{e}]")
            self.update_from_network()
        if bool_not_exist:
            logger.error(f"{symbol} not Exist")
        return name

    def get_stocks_a(self, symbols: list | None = None) -> pd.DataFrame:
        if self.df_em_stocks_a.empty:
            self.df_em_stocks_a = feather_from_file(
                filename_df=self.filename_em_stocks_a
            )
        if self.df_em_stocks_a.empty:
            self.df_em_stocks_a = ak_stock_zh_a_spot_em()
        if symbols is None or self.df_em_stocks_a.empty or len(symbols) == 0:
            return self.df_em_stocks_a
        self.list_stocks_a = self.df_em_stocks_a.index.tolist()
        set_symbols = set(symbols)
        symbols = list(set_symbols & set(self.list_stocks_a))
        df = self.df_em_stocks_a.loc[symbols]
        return df

    def get_etfs_a(self, symbols: list | None = None) -> pd.DataFrame:
        if self.df_em_etfs_a.empty:
            self.df_em_etfs_a = feather_from_file(filename_df=self.filename_em_etfs_a)
        if self.df_em_etfs_a.empty:
            self.df_em_etfs_a = ak_fund_etf_spot_em()
        if symbols is None or self.df_em_etfs_a.empty or len(symbols) == 0:
            return self.df_em_etfs_a
        self.list_etfs_a = self.df_em_etfs_a.index.tolist()
        set_symbols = set(symbols)
        symbols = list(set_symbols & set(self.list_etfs_a))
        df = self.df_em_etfs_a.loc[symbols]
        return df

    def get_stocks_hk(self, symbols: list | None = None) -> pd.DataFrame:
        if self.df_em_stocks_hk.empty:
            self.df_em_stocks_hk = feather_from_file(
                filename_df=self.filename_em_stocks_hk
            )
        if self.df_em_stocks_hk.empty:
            self.df_em_stocks_hk = ak_stock_hk_spot_em()
        if symbols is None or self.df_em_stocks_hk.empty or len(symbols) == 0:
            return self.df_em_stocks_hk
        self.list_stocks_hk = self.df_em_stocks_hk.index.tolist()
        set_symbols = set(symbols)
        symbols = list(set_symbols & set(self.list_etfs_a))
        df = self.df_em_stocks_hk.loc[symbols]
        return df

    def get_datetime(self) -> datetime.datetime:
        return self.dt_now

    def get_from_file(self) -> None:
        self.df_em_stocks_a = feather_from_file(filename_df=self.filename_em_stocks_a)
        self.df_em_etfs_a = feather_from_file(filename_df=self.filename_em_etfs_a)
        self.df_em_stocks_hk = feather_from_file(filename_df=self.filename_em_stocks_hk)
        return

    def update_from_network(self) -> None:
        self.df_em_stocks_hk = ak_stock_hk_spot_em()
        self.df_em_etfs_a = ak_fund_etf_spot_em()
        self.df_em_stocks_a = ak_stock_zh_a_spot_em()
        return

    def cache_to_file(self) -> None:
        feather_to_file(df=self.df_em_stocks_a, filename_df=self.filename_em_stocks_a)
        feather_to_file(df=self.df_em_etfs_a, filename_df=self.filename_em_etfs_a)
        feather_to_file(df=self.df_em_stocks_hk, filename_df=self.filename_em_stocks_hk)
        return

    def update_while_trading(self) -> None:
        i_while = 0
        while True:
            i_while += 1
            dt_now = datetime.datetime.now()
            print(
                f"\n<{dt_now.time().replace(microsecond=0)}>----"
                f"[update_realtime_quotation]"
            )
            if dt_am_start <= dt_now <= dt_am_end or dt_pm_start <= dt_now <= dt_pm_end:
                self.update_from_network()
                self.dt_now = datetime.datetime.strptime(
                    self.df_em_stocks_a.index.name, format_dt
                )
            elif dt_am_end < dt_now < dt_pm_start:
                logger.debug("update_realtime_quotation Pause")
                self.cache_to_file()
                sleep_to_time(dt_time=dt_pm_start, seconds=5)
                continue
            elif dt_now < dt_am_start:
                logger.debug("update_realtime_quotation waiting the exchange open")
                sleep_to_time(dt_time=dt_am_start, seconds=5)
                continue
            elif dt_now > dt_pm_end:
                self.cache_to_file()
                logger.debug("update_realtime_quotation END")
                break
            if i_while % 20 == 1:
                self.cache_to_file()
            sceonds = random.randint(8, 11)
            time.sleep(sceonds)
        return


realtime_quotation = _Realtime_Quotation()
