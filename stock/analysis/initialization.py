# modified at 2023/05/18 22::25
import os
import datetime
from loguru import logger
import pandas as pd
import analysis.base
import analysis.chip
from analysis.const import (
    path_check,
    str_trading_path,
    rise,
    filename_chip_shelve,
    dt_init,
    dt_date_trading_last_T0,
    phi_a,
    phi_b_neg,
    INDUSTRY_MAX_MIN,
    NOW_PRICE_MAX,
    G_PRICE_MAX,
)


def init_trader(
    df_trader: pd.DataFrame, sort: bool = False, drop_count: int = 15
) -> pd.DataFrame:
    logger.trace("init_trader Begin")
    df_chip = analysis.base.read_df_from_db(
        key="df_chip", filename=filename_chip_shelve
    )
    if df_chip.empty:
        df_chip = analysis.chip()
    if df_chip.empty:
        logger.error("df_chip is empty.")
        import sys

        sys.exit()
    i_realtime = 0
    df_realtime = pd.DataFrame()
    filename_drop_stock = os.path.join(
        path_check, f"drop_stock_{str_trading_path()}.csv"
    )
    """
    if os.access(path=filename_drop_stock, mode=os.F_OK):
        df_drop_stock = pd.read_csv(filepath_or_buffer=filename_drop_stock, index_col=0)
    else:
        df_drop_stock = pd.DataFrame(columns=df_trader.columns)
    """
    while i_realtime <= 2:
        i_realtime += 1
        df_realtime = analysis.realtime_quotations(
            stock_codes=df_trader.index.to_list()
        )
        if df_realtime.empty:
            logger.trace("df_realtime is empty")
        else:
            break
    dict_trader_default = analysis.const.get_trader_columns(data_type="dict")
    for column in df_trader.columns:
        if column not in dict_trader_default:
            df_trader.drop(columns=column, inplace=True)
    for column in dict_trader_default:
        if column in df_trader.columns:
            df_trader[column].fillna(value=dict_trader_default[column], inplace=True)
        else:
            df_trader[column] = dict_trader_default[column]
    dt_now = datetime.datetime.now()
    for code in df_trader.index:
        if (
            df_trader.at[code, "date_of_inclusion_latest"] == dt_init
            and df_trader.at[code, "date_of_inclusion_first"] != dt_init
        ):
            df_trader.at[code, "date_of_inclusion_latest"] = df_trader.at[
                code, "date_of_inclusion_first"
            ]
        if df_trader.at[code, "position_unit"] <= 0:
            df_trader.at[code, "position_unit_max"] = 0
            df_trader.at[code, "rise"] = rise
        else:
            if (
                df_trader.at[code, "position_unit"]
                > df_trader.at[code, "position_unit_max"]
            ):
                df_trader.at[code, "position_unit_max"] = df_trader.at[
                    code, "position_unit"
                ]
            if (df_trader.at[code, "position_unit"] < 2) and (
                df_trader.at[code, "position_unit_max"] >= 2
            ):
                df_trader.at[code, "rise"] = rise * (
                    1 + df_trader.at[code, "position_unit_max"] * 0.25
                )
            else:
                df_trader.at[code, "rise"] = rise
        if code in df_chip.index:
            df_trader.at[code, "T5_amplitude"] = df_chip.at[code, "T5_amplitude"]
            df_trader.at[code, "T5_pct"] = df_chip.at[code, "T5_pct"]
            df_trader.at[code, "7Pct_T"] = int(df_chip.at[code, "correct_7pct_times"])
            df_trader.at[code, "G_price"] = g_price = df_chip.at[code, "G_price"]
            df_trader.at[code, "gold_section_volume"] = df_chip.at[
                code, "gold_section_volume"
            ]
            df_trader.at[code, "gold_pct_max_min"] = df_chip.at[
                code, "gold_pct_max_min"
            ]
            df_trader.at[code, "gold_date_max"] = df_chip.at[code, "gold_date_max"]
            df_trader.at[code, "gold_date_min"] = df_chip.at[code, "gold_date_min"]
            df_trader.at[code, "gold_price_min"] = df_chip.at[code, "gold_price_min"]
            df_trader.at[code, "gold_section_volume"] = round(
                df_chip.at[code, "gold_section_volume"], 1
            )
            df_trader.at[code, "gold_section_price"] = df_chip.at[
                code, "gold_section_price"
            ]
            if df_realtime.empty:
                name = df_chip.at[code, "name"]
                now_price = df_chip.at[code, "now_price"]
            else:
                name = df_realtime.at[code, "name"]
                now_price = df_realtime.at[code, "close"]
            df_trader.at[code, "name"] = name
            df_trader.at[code, "now_price"] = now_price
            if df_trader.at[code, "recent_price"] == 0:
                df_trader.at[code, "recent_price"] = now_price
            if df_trader.at[code, "price_of_inclusion"] == 0:
                df_trader.at[code, "price_of_inclusion"] = now_price
            df_trader.at[code, "total_mv_E"] = df_chip.at[code, "total_mv_E"]
            df_trader.at[code, "times_exceed_correct_industry"] = df_chip.at[
                code, "times_exceed_correct_industry"
            ]
            df_trader.at[code, "mean_exceed_correct_industry"] = df_chip.at[
                code, "mean_exceed_correct_industry"
            ]
            df_trader.at[code, "times_concentration"] = df_chip.at[
                code, "times_concentration"
            ]
            df_trader.at[code, "rate_concentration"] = df_chip.at[
                code, "rate_concentration"
            ]
            df_trader.at[code, "ssb_index"] = df_chip.at[code, "ssb_index"]
            df_trader.at[code, "ST"] = df_chip.at[code, "ST"]
            df_trader.at[code, "profit_rate"] = df_chip.at[code, "profit_rate"]
            df_trader.at[code, "dividend_rate"] = df_chip.at[code, "dividend_rate"]
            df_trader.at[code, "cash_div_period"] = df_chip.at[code, "cash_div_period"]
            df_trader.at[code, "cash_div_excepted_period"] = df_chip.at[
                code, "cash_div_excepted_period"
            ]
            df_trader.at[code, "industry_code"] = df_chip.at[code, "industry_code"]
            df_trader.at[code, "industry_name"] = df_chip.at[code, "industry_name"]
            df_trader.at[code, "max_min"] = df_chip.at[code, "max_min"]
            df_trader.at[code, "trx_unit_share"] = analysis.transaction_unit(
                price=g_price, amount=2000
            )
            df_trader.at[code, "position_unit"] = (
                df_trader.at[code, "position"] / df_trader.at[code, "trx_unit_share"]
            ).round(2)
            if (
                df_trader.at[code, "position"] == 0
                and df_trader.at[code, "times_of_inclusion"] > 5
                and df_trader.at[code, "rate_of_inclusion"] > phi_a
                and df_trader.at[code, "recent_price"] < g_price
            ):
                df_trader.at[code, "recent_price"] = g_price
                df_trader.at[
                    code, "remark"
                ] = f"Reset <recent_price> at {dt_now.date()}"
            pct_chg = (now_price / df_trader.at[code, "recent_price"] - 1) * 100
            pct_chg = round(pct_chg, 2)
            df_trader.at[code, "pct_chg"] = pct_chg
            days_of_inclusion = (
                dt_date_trading_last_T0
                - df_trader.at[code, "date_of_inclusion_first"].date()
            ).days + 1
            days_of_inclusion = (
                days_of_inclusion // 7 * 5 + days_of_inclusion % 7
            )  # 修正除数，尽可能趋近交易日
            if days_of_inclusion >= 1:
                df_trader.at[code, "rate_of_inclusion"] = round(
                    df_trader.at[code, "times_of_inclusion"] / days_of_inclusion * 100,
                    2,
                )
            else:
                df_trader.at[code, "rate_of_inclusion"] = 0
            pct_of_inclusion = (
                now_price / df_trader.at[code, "price_of_inclusion"] - 1
            ) * 100
            pct_of_inclusion = round(pct_of_inclusion, 2)
            df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
    # 删除df_trader过期的标的----Begin
    len_trader = df_trader.shape[0]
    df_drop_stock = pd.DataFrame(columns=df_trader.columns)
    if len_trader > drop_count:
        for code in df_trader.index:
            days_recent_trading = (dt_now - df_trader.at[code, "recent_trading"]).days
            days_of_inclusion_latest = (
                dt_now - df_trader.at[code, "date_of_inclusion_latest"]
            ).days
            if (
                df_trader.at[code, "position"] == 0
                and days_recent_trading > 60
                and days_of_inclusion_latest > 30
            ):
                if (
                    "ST" in df_trader.at[code, "ST"]
                    or "ST" in df_trader.at[code, "name"]
                ):
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_trader.drop(index=code, inplace=True)
                elif df_trader.at[code, "rate_of_inclusion"] < phi_a:
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "rate_of_inclusion"
                    df_trader.drop(index=code, inplace=True)
                elif df_trader.at[code, "G_price"] > G_PRICE_MAX:
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "G_price"
                    df_trader.drop(index=code, inplace=True)
                elif df_trader.at[code, "now_price"] > NOW_PRICE_MAX:
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "now_price"
                    df_trader.drop(index=code, inplace=True)
                elif df_trader.at[code, "profit_rate"] <= phi_b_neg:
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "profit_rate"
                    df_trader.drop(index=code, inplace=True)
                elif (
                    df_trader.at[code, "times_exceed_correct_industry"] < 35
                    or df_trader.at[code, "mean_exceed_correct_industry"] < 0.7
                ):
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "times_exceed_correct"
                    df_trader.drop(index=code, inplace=True)
                elif df_trader.at[code, "max_min"] < INDUSTRY_MAX_MIN:
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "max_min"
                    df_trader.drop(index=code, inplace=True)
                elif pd.isnull(df_trader.at[code, "factor"]):
                    # Temporary conditions 2023-12-18 30days
                    df_drop_stock.loc[code] = df_trader.loc[code]
                    df_drop_stock.at[code, "drop"] = "factor_null"
                    df_trader.drop(index=code, inplace=True)
    if not df_drop_stock.empty:
        df_drop_stock.to_csv(path_or_buf=filename_drop_stock)
    # 删除df_trader过期的标的----End
    if sort:
        df_trader = df_trader.reindex(columns=dict_trader_default.keys())
    logger.trace("init_trader End")
    return df_trader
