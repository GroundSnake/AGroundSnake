import pandas as pd
import analysis.ashare


def distribution_ups_downs():
    df_all = analysis.ashare.stock_zh_a_spot_em()  # 调用实时数据接口
    df_all = df_all[(df_all["volume"] > 0)]
    df_zero = df_all[df_all["pct_chg"] == 0]
    df_ups = df_all[df_all["pct_chg"] > 0]
    df_ups5 = df_all[df_all["pct_chg"] >= 5]
    df_downs = df_all[df_all["pct_chg"] < 0]
    df_downs5 = df_all[df_all["pct_chg"] <= -5]
    series_return = pd.Series()
    series_return["all"] = df_all.shape[0]
    series_return["zero"] = df_zero.shape[0]
    series_return["ups5"] = df_ups5.shape[0]
    series_return["downs5"] = df_downs5.shape[0]
    series_return["ups"] = df_ups.shape[0]
    series_return["downs"] = df_downs.shape[0]
    return series_return


def correct_gird():
    series_dist = distribution_ups_downs()
    correct_ups = round(series_dist["ups"] / series_dist["all"], 2)
    correct_downs = round(series_dist["downs"] / series_dist["all"], 2)
    correct_gird = correct_ups - correct_downs
    # correct_fall = fall*(1-correct_gird)
    # correct_rise = rise*(1+correct_gird)
    # return_tuple = (correct_rise, correct_fall)
    return correct_gird
