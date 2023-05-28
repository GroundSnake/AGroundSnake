# modified at 2023/05/18 22::25
from loguru import logger
import pandas as pd
import analysis.base
from analysis.const import rise, fall, filename_chip_shelve, dt_date_trading, dt_init


def init_trader(df_trader: pd.DataFrame) -> pd.DataFrame:
    df_chip = analysis.base.read_df_from_db(
        key="df_chip", filename=filename_chip_shelve
    )
    i_realtime = 0
    df_realtime = pd.DataFrame()
    while i_realtime <= 2:
        i_realtime += 1
        df_realtime = analysis.realtime_quotations(
            stock_codes=df_trader.index.to_list()
        )  # 调用实时数据接口
        if not df_realtime.empty:
            break
        else:
            logger.trace("df_realtime is empty")
    dict_trader_default = {
        "name": "股票简称",
        "recent_price": 0,
        "position": 0,
        "position_unit": 0,
        "trx_unit_share": 0,
        "now_price": 0,
        "pct_chg": 0,
        "rise": rise,
        "fall": fall,
        "total_mv_E": 0,
        "ssb_index": "index_none",
        "stock_index": "stock_index_none",
        "grade": "grade_none",
        "recent_trading": dt_init,
        "ST": "ST_none",
        "industry_code": "000000.TI",
        "industry_name": "行业",
        "date_of_inclusion_first": dt_init,
        "date_of_inclusion_latest": dt_init,
        "times_of_inclusion": 0,
        "rate_of_inclusion": 0,
        "price_of_inclusion": 0,
        "pct_of_inclusion": 0,
    }
    for columns in dict_trader_default:
        df_trader[columns].fillna(value=dict_trader_default[columns], inplace=True)
    for code in df_trader.index:
        if code in df_chip.index:
            t5_amplitude = df_chip.at[code, "T5_amplitude"]
            t5_pct = df_chip.at[code, "T5_pct"]
            up_times = int(df_chip.at[code, "up_times"])
            up_a_down_7pct = int(df_chip.at[code, "up_A_down_7pct"])
            up_a_down_5pct = int(df_chip.at[code, "up_A_down_5pct"])
            up_a_down_3pct = int(df_chip.at[code, "up_A_down_3pct"])
            turnover = round(df_chip.at[code, "turnover"], 1)
            g_price = df_chip.at[code, "G_price"]
            now_price_ratio = round(df_chip.at[code, "now_price_ratio"], 1)
            if df_realtime.empty:
                df_trader.at[code, "name"] = df_chip.at[code, "name"]
                df_trader.at[code, "now_price"] = now_price = df_chip.at[
                    code, "now_price"
                ]
            else:
                df_trader.at[code, "name"] = df_realtime.at[code, "name"]
                df_trader.at[code, "now_price"] = now_price = df_realtime.at[
                    code, "close"
                ]
            df_trader.at[code, "total_mv_E"] = df_chip.at[code, "total_mv_E"]
            df_trader.at[code, "ssb_index"] = df_chip.at[code, "ssb_index"]
            df_trader.at[code, "ST"] = df_chip.at[code, "ST"]
            df_trader.at[code, "industry_code"] = df_chip.at[code, "industry_code"]
            df_trader.at[code, "industry_name"] = df_chip.at[code, "industry_name"]
            df_trader.at[code, "trx_unit_share"] = analysis.transaction_unit(
                price=df_chip.at[code, "G_price"]
            )
            df_trader.at[code, "position_unit"] = (
                df_trader.at[code, "position"] / df_trader.at[code, "trx_unit_share"]
            ).round(2)
            pct_chg = (
                df_trader.at[code, "now_price"] / df_trader.at[code, "recent_price"] - 1
            ) * 100
            pct_chg = round(pct_chg, 2)
            df_trader.at[code, "pct_chg"] = pct_chg
            days_of_inclusion = (
                dt_date_trading - df_trader.at[code, "date_of_inclusion_first"]
            ).days + 1
            days_of_inclusion = (
                days_of_inclusion // 7 * 5 + days_of_inclusion % 7
            )  # 修正除数，尽可能趋近交易日
            df_trader.at[code, "rate_of_inclusion"] = round(
                df_trader.at[code, "times_of_inclusion"] / days_of_inclusion * 100,
                2,
            )
            pct_of_inclusion = (
                df_trader.at[code, "now_price"]
                / df_trader.at[code, "price_of_inclusion"]
                - 1
            ) * 100
            pct_of_inclusion = round(pct_of_inclusion, 2)
            df_trader.at[code, "pct_of_inclusion"] = pct_of_inclusion
            df_trader.at[code, "stock_index"] = (
                f"({up_times:2.0f}U /"
                f"{turnover:2.0f}T /"
                f"{now_price_ratio:6.2f}% -"
                f"{g_price:6.2f}$)--"
                f"[T5_amp:{t5_amplitude:5.2f}]-"
                f"[T5_pct:{t5_pct:5.2f}]"
            )
            if up_a_down_7pct >= 12:
                grade_ud_7 = "A"
            elif 4 <= up_a_down_7pct < 12:
                grade_ud_7 = "B"
            else:
                grade_ud_7 = "Z"
            if up_a_down_5pct >= 24:
                grade_ud_5 = "A"
            elif 12 <= up_a_down_5pct < 24:
                grade_ud_5 = "B"
            else:
                grade_ud_5 = "Z"
            if up_a_down_3pct >= 48:
                grade_ud_3 = "A"
            elif 24 <= up_a_down_3pct < 48:
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
            if 0 < now_price < g_price:
                grade_g = "Under"
            elif g_price <= now_price:
                grade_g = "Over"
            else:
                grade_g = "#"
            grade = (
                grade_ud_7
                + grade_ud_5
                + grade_ud_3
                + "-"
                + grade_ud_limit
                + grade_to
                + grade_pr
                + "-"
                + grade_g
            )
            df_trader.at[code, "grade"] = grade
    return df_trader


# df_trader = df_trader.reindex(columns = list_trader_columns)
