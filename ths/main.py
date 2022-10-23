# -*- coding:utf-8 -*-
import pandas as pd
from base.object import GridTrader
from base.ashare import history_n
import logging

if __name__ == "__main__":
    handler_stream = logging.StreamHandler(stream=None)
    handler_stream.setLevel(level=logging.INFO)  # set Console level [logging.INFO]
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
        handlers=[handler_stream],
    )
    symbols = ["sz002621"]
    traders = {}
    for symbol in symbols:
        traders[symbol] = GridTrader(symbol=symbol, price=4.00, volume=300)
        # df_history = history_n(symbol=symbol, frequency="5m", count=500)
        df_history = pd.read_pickle(filepath_or_buffer="002621.SZ.cat")
        df_history.sort_values(by="trade_time", ascending=True, inplace=True)
        prices = df_history["close"].to_list()
        print(df_history)
        for price in prices:
            signal = traders[symbol].get_signal(price=price)
            traders[symbol].record(side=signal, price=price)
    print(traders["sz002621"].tick_position)
    print(traders["sz002621"].transaction)
    print(traders["sz002621"].get_status())
    file_name_stock = "writer.xlsx"
    with pd.ExcelWriter(path=file_name_stock, mode="w") as writer:
        traders["sz002621"].tick_position.to_excel(excel_writer=writer, sheet_name="tick_position")
        traders["sz002621"].transaction.to_excel(excel_writer=writer, sheet_name="transaction")
        traders["sz002621"].get_status().to_excel(excel_writer=writer, sheet_name="get_status")
