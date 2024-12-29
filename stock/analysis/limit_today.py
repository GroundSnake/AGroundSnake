# modified at 2023/08/05 12:12
import datetime
from console import fg
from analysis.realtime_quotation import realtime_quotation
from analysis.log import logger

dt_now = datetime.datetime.now()
line_len = 3


def get_limit_up_today(stocks: list) -> str:
    if not isinstance(stocks, list):
        logger.error("stocks is not list.")
        return "stocks is not list."
    count = len(stocks)
    if count == 0:
        return "Stocks is empty"
    df_realtime = realtime_quotation.get_stocks_a(symbols=stocks)
    if df_realtime.empty:
        logger.error("df_realtime Error")
        return "df_realtime Error"
    df_realtime = df_realtime[["name", "pct_chg"]]
    df_realtime = df_realtime[df_realtime["pct_chg"] >= 3]
    df_realtime.sort_values(by=["pct_chg"], ascending=False, inplace=True)
    count_gt3 = df_realtime.shape[0]
    i = 0
    str_return = ""
    for symbol in df_realtime.index:
        i += 1
        """
        pct_chg = decimal.Decimal(df_realtime.at[symbol, "pct_chg"]).quantize(
            decimal.Decimal("0.00"), rounding=decimal.ROUND_DOWN
        )
        """
        pct_chg = round(df_realtime.at[symbol, "pct_chg"], 2)
        str_symbol = f"[{df_realtime.at[symbol, 'name']:>4}({symbol}) {pct_chg:5.2f}%]"
        if pct_chg > 7:
            str_symbol = fg.purple(str_symbol)
        elif pct_chg > 5:
            str_symbol = fg.red(str_symbol)
        if str_return == "":
            str_return = f"{str_symbol}"
        elif i % line_len == 1:
            str_return += f"\n\r{str_symbol}"
        else:
            str_return += f", {str_symbol}"
    global dt_now
    dt_now = datetime.datetime.now()
    str_dt_time = dt_now.strftime("<%H:%M:%S>")
    rate_ups3 = round(count_gt3 / count * 100, 2)
    if str_return != "":
        space_line1 = " " * 43
        space_line2 = " " * 35
        str_return = (
            fg.red(f"{space_line1}{str_dt_time} - Ups")
            + "\n"
            + f"{space_line2}({count_gt3:2d}/{count:2d}) - "
            + fg.purple(f"[↑3%_Rate]:{rate_ups3}%")
            + f"\n{str_return}"
        )
    return str_return


def get_limit_down_today(stocks: list) -> str:
    if not isinstance(stocks, list):
        logger.error("stocks is not list.")
        return "Stocks is not list."
    count = len(stocks)
    if count == 0:
        return "stocks is empty"
    df_realtime = realtime_quotation.get_stocks_a(symbols=stocks)
    if df_realtime.empty:
        logger.error("df_realtime Error")
        return "df_realtime Error"
    df_realtime = df_realtime[["name", "pct_chg"]]
    df_realtime = df_realtime[df_realtime["pct_chg"] <= -3]
    df_realtime.sort_values(by=["pct_chg"], ascending=True, inplace=True)
    count_lt3 = df_realtime.shape[0]
    i = 0
    str_return = ""
    for symbol in df_realtime.index:
        i += 1
        """
        pct_chg = decimal.Decimal(df_realtime.at[symbol, "pct_chg"]).quantize(
            decimal.Decimal("0.00"), rounding=decimal.ROUND_DOWN
        )
        """
        pct_chg = round(df_realtime.at[symbol, "pct_chg"], 2)
        str_symbol = f"[{df_realtime.at[symbol, 'name']:>4}({symbol}) {pct_chg:5.2f}%]"
        if pct_chg < -7:
            str_symbol = fg.blue(str_symbol)
        elif pct_chg < -5:
            str_symbol = fg.green(str_symbol)
        if str_return == "":
            str_return = f"{str_symbol}"
        elif i % line_len == 1:
            str_return += f"\n\r{str_symbol}"
        else:
            str_return += f", {str_symbol}"
    global dt_now
    dt_now = datetime.datetime.now()
    str_dt_time = dt_now.strftime("<%H:%M:%S>")
    rate_downs3 = round(count_lt3 / count * 100, 2)
    if str_return != "":
        space_line1 = " " * 43
        space_line2 = " " * 35
        str_return = (
            fg.green(f"{space_line1}{str_dt_time} - Downs")
            + "\n"
            + f"{space_line2}({count_lt3:2d}/{count:2d}) - "
            + fg.blue(f"[↓3%%_Rate]:{rate_downs3}%")
            + f"\n{str_return}"
        )
    return str_return
