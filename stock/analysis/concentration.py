# modified at 2023/4/28 13:44
from ashare import stock_zh_a_spot_em
from analysis.const import list_all_stocks


def concentration_rate() -> str:
    df_realtime = stock_zh_a_spot_em()  # 调用实时数据接口
    df_realtime.sort_values(by=["amount"], ascending=False, inplace=True)
    top5_stocks = int(round(len(list_all_stocks) * 0.05, 0))
    df_realtime_top5 = df_realtime.iloc[:top5_stocks]
    df_realtime_tail95 = df_realtime.iloc[top5_stocks:]
    amount_all = df_realtime["amount"].sum() / 100000000
    amount_top5 = df_realtime_top5["amount"].sum() / 100000000
    turnover_top5 = df_realtime_top5["turnover"].mean().round(2)
    turnover_tail95 = df_realtime_tail95["turnover"].mean().round(2)
    amplitude_top5 = df_realtime_top5["amplitude"].mean().round(2)
    amplitude_tail95 = df_realtime_tail95["amplitude"].mean().round(2)
    if amount_all != 0:
        amount_rate_top5 = (amount_top5 / amount_all * 100).round(2)
    else:
        amount_rate_top5 = 0
    amount_rate_tail95 = 100 - amount_rate_top5
    str_msg = (
        f'amount[{amount_rate_top5:.2f}//{amount_rate_tail95:.2f}] - '
        f'turnover[{turnover_top5:.2f}/{turnover_tail95:.2f}] - '
        f'amplitude[{amplitude_top5:.2f}/{amplitude_tail95:.2f}]'
    )
    return str_msg

