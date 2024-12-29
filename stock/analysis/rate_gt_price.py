import datetime
import random
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_balance,
    dt_pm_end,
    dt_pm_end_1t,
    path_chip,
    path_chip_csv,
    path_temp,
    format_dt,
    format_date,
    client_ts_pro,
)
from analysis.log import log_json, logger
from analysis.base import feather_from_file, feather_to_file, code_ts_to_ths
from analysis.realtime_quotation import realtime_quotation
from analysis.util import get_stock_code_chs
from analysis.update_data import kline


def get_rate_gt_price(days: int = 365) -> pd.DataFrame:
    def daily_latest_day(dt: datetime.datetime) -> pd.DataFrame:
        if dt < dt_pm_end:
            dt_trade_date = dt_pm_end_1t
        else:
            dt_trade_date = dt_pm_end
        str_trade_date = dt_trade_date.strftime(format_date)
        log_json(item="daily")
        df_daily = client_ts_pro.daily(trade_date=str_trade_date)
        df_daily["ts_code"] = df_daily["ts_code"].apply(func=code_ts_to_ths)
        df_daily.set_index(keys=["ts_code"], inplace=True)
        df_daily = df_daily[["close"]]
        return df_daily

    frequency: str = "1m"
    dt_now = datetime.datetime.now()
    filename_df_rate_gt_price = path_chip.joinpath("df_rate_gt_price.ftr")
    df_rate_gt_price = feather_from_file(filename_df=filename_df_rate_gt_price)
    if not df_rate_gt_price.empty:
        if dt_balance <= dt_now <= dt_pm_end:
            logger.debug(f"feather from {filename_df_rate_gt_price}.--trading time")
            return df_rate_gt_price
        dt_stale = datetime.datetime.strptime(
            df_rate_gt_price.index.name,
            format_dt,
        )
        if dt_stale >= dt_pm_end:
            logger.debug(f"feather from {filename_df_rate_gt_price}.")
            return df_rate_gt_price
    filename_df_rate_gt_price_temp = path_temp.joinpath(f"df_rate_gt_price_temp.ftr")
    df_rate_gt_price = feather_from_file(filename_df=filename_df_rate_gt_price_temp)
    if df_rate_gt_price.empty:
        df_rate_gt_price = daily_latest_day(dt=dt_now)
        if df_rate_gt_price.empty:
            df_rate_gt_price = realtime_quotation.get_stocks_a()[["close"]]
            list_all_stocks = get_stock_code_chs()
            df_rate_gt_price = df_rate_gt_price[
                df_rate_gt_price.index.isin(list_all_stocks)
            ]
        if df_rate_gt_price.empty:
            logger.error("df_rate_gt_price is empty")
            import sys

            sys.exit()
        dict_columns = {
            "close": "float64",
            "rate_gt_price": "float64",
            "rate_gt_price_count": "float64",
            "rate_gt_price_volume": "float64",
            "rate_gt_price_days": "float64",
            "rate_gt_price_dt_start": "datetime64[ns]",
            "rate_gt_price_dt_end": "datetime64[ns]",
        }
        df_rate_gt_price = df_rate_gt_price.reindex(columns=dict_columns.keys())
        df_rate_gt_price = df_rate_gt_price.astype(dtype=dict_columns)
        df_rate_gt_price["rate_gt_price_dt_start"] = dt_init
        df_rate_gt_price["rate_gt_price_dt_end"] = dt_init
        df_rate_gt_price.fillna(value=0.0, inplace=True)
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_rate_gt_price,
                filename_df=filename_df_rate_gt_price_temp,
            )
    df_rate_gt_price = df_rate_gt_price.sample(frac=1)
    df_rate_gt_price.sort_values(
        by=["rate_gt_price_dt_end"], ascending=False, inplace=True
    )
    dt_data_365 = dt_pm_end - datetime.timedelta(days=days)
    str_msg_bar_basic = f"rate_gt_price_{frequency}_{days}"
    i = 0
    all_record = df_rate_gt_price.shape[0]
    for symbol in df_rate_gt_price.index:
        i += 1
        str_msg_bar = f"{str_msg_bar_basic}:[{i:4d}/{all_record:4d}] -- [{symbol}]"
        if df_rate_gt_price.at[symbol, "rate_gt_price_dt_end"] != dt_init:
            print(f"{str_msg_bar} - exist\033[K")
            continue
        df_kline_1m = kline(
            symbol=symbol,
            frequency=frequency,
            adjust="qfq",
            asset="E",
        )
        if df_kline_1m.empty:
            logger.error(f"{str_msg_bar} - No data\033[K")
            continue
        df_kline_365 = df_kline_1m.loc[dt_data_365:]
        df_kline_pivot_365 = pd.pivot_table(
            df_kline_365, index=["close"], aggfunc={"volume": "sum", "close": "count"}
        )
        df_kline_pivot_365.rename(
            columns={"close": "count"},
            inplace=True,
        )
        close = df_rate_gt_price.at[symbol, "close"]
        df_kline_pivot_now = df_kline_pivot_365[df_kline_pivot_365.index <= close]
        count_gt_price_365 = df_kline_pivot_365["count"].sum()
        volume_gt_price_365 = df_kline_pivot_365["volume"].sum()
        count_gt_price_now = df_kline_pivot_now["count"].sum()
        volume_gt_price_now = df_kline_pivot_now["volume"].sum()
        rate_gt_price_count = round(count_gt_price_now / count_gt_price_365 * 100, 2)
        rate_gt_price_volume = round(volume_gt_price_now / volume_gt_price_365 * 100, 2)
        rate_gt_price = max(rate_gt_price_count, rate_gt_price_volume)
        df_rate_gt_price.at[symbol, "rate_gt_price"] = rate_gt_price
        df_rate_gt_price.at[symbol, "rate_gt_price_count"] = rate_gt_price_count
        df_rate_gt_price.at[symbol, "rate_gt_price_volume"] = rate_gt_price_volume
        df_rate_gt_price.at[symbol, "rate_gt_price_dt_start"] = df_kline_1m.index.min()
        df_rate_gt_price.at[symbol, "rate_gt_price_dt_end"] = df_kline_1m.index.max()
        df_rate_gt_price.at[symbol, "rate_gt_price_days"] = (
            df_rate_gt_price.at[symbol, "rate_gt_price_dt_end"]
            - df_rate_gt_price.at[symbol, "rate_gt_price_dt_start"]
        ).days
        if random.randint(a=0, b=9) == 5:
            feather_to_file(
                df=df_rate_gt_price,
                filename_df=filename_df_rate_gt_price_temp,
            )
        print(f"{str_msg_bar} - Update\033[K")
    if i >= all_record:
        dt_max = df_rate_gt_price["rate_gt_price_dt_end"].max()
        str_dt_rate_gt_price = dt_max.strftime(format_dt)
        df_rate_gt_price.index.rename(name=str_dt_rate_gt_price, inplace=True)
        filename_rate_gt_price_csv = path_chip_csv.joinpath(
            "df_rate_gt_price.csv",
        )
        df_rate_gt_price.to_csv(
            path_or_buf=filename_rate_gt_price_csv,
        )
        feather_to_file(df=df_rate_gt_price, filename_df=filename_df_rate_gt_price)
        logger.debug(f"feather to {filename_df_rate_gt_price}.")
        filename_df_rate_gt_price_temp.unlink(missing_ok=True)
    return df_rate_gt_price
