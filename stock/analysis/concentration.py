import datetime
from analysis.log import logger
from console import fg
from pyecharts.charts import Line, Page
import pyecharts.options as opts
import pandas as pd
from analysis.const_dynamic import (
    dt_init,
    dt_am_start,
    dt_am_1015,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    path_chip,
    path_chip_csv,
    filename_concentration_rate_charts_html,
    filename_concentration_rate_ftr,
    filename_concentration_rate_csv,
    format_dt,
)
from analysis.base import feather_from_file, feather_to_file
from analysis.realtime_quotation import realtime_quotation

# from pyecharts.globals import CurrentConfig

# CurrentConfig.ONLINE_HOST = ''
dt_now = datetime.datetime.now()


def get_concentration_stocks(top: int = 5) -> pd.DataFrame:
    filename_concentration = path_chip.joinpath(
        f"df_concentration_top{top}.ftr",
    )
    df_concentration_stocks = feather_from_file(filename_df=filename_concentration)
    if not df_concentration_stocks.empty:
        global dt_now
        dt_now = datetime.datetime.now()
        if dt_am_start <= dt_now <= dt_pm_end:
            logger.debug(f"feather from {filename_concentration}.----trading time")
            return df_concentration_stocks
        try:
            dt_temp = datetime.datetime.strptime(
                df_concentration_stocks.index.name, format_dt
            )
        except TypeError:
            dt_temp = dt_init
        if dt_temp >= dt_pm_end:
            logger.debug(f"feather from {filename_concentration}.")
            return df_concentration_stocks
    df_all = realtime_quotation.get_stocks_a()
    if df_all.empty:
        logger.error("df_all is empty")
        if df_concentration_stocks.empty:
            return pd.DataFrame()
        else:
            return df_concentration_stocks
    df_all.sort_values(by=["amount"], ascending=False, inplace=True)
    list_all_stocks = df_all.index.tolist()
    top_pct = top * 0.01
    top_n_stocks = int(round(df_all.shape[0] * top_pct, 0))
    if df_concentration_stocks.empty:
        df_concentration_stocks = pd.DataFrame(columns=list_all_stocks)
    df_all["rank_amount"] = df_all["amount"].rank(
        axis=0,
        method="min",
        ascending=False,
    )
    df_all["concentration_topN"] = df_all["rank_amount"].apply(
        func=lambda x: 1 if x <= top_n_stocks else 0
    )
    df_concentration_stocks.loc[dt_pm_end] = df_all["concentration_topN"]
    df_concentration_stocks.fillna(value=0, inplace=True)
    str_dt_pm_end = dt_pm_end.strftime(format_dt)
    df_concentration_stocks.index.rename(name=str_dt_pm_end, inplace=True)
    df_concentration_stocks.sort_index(ascending=True, inplace=True)
    filename_concentration_csv = path_chip_csv.joinpath(
        f"df_concentration_top{top}.csv",
    )
    df_concentration_stocks.to_csv(path_or_buf=filename_concentration_csv)
    feather_to_file(df=df_concentration_stocks, filename_df=filename_concentration)
    logger.debug(f"feather to {filename_concentration}.")
    return df_concentration_stocks


def _grt_realtime_concentration_rate() -> pd.Series:
    df_all_stocks = realtime_quotation.get_stocks_a()  # 调用实时数据接口
    ser_realtime_concentration_rate = pd.Series()
    amount_all = df_all_stocks["amount"].sum()
    if amount_all <= 0:
        return ser_realtime_concentration_rate
    count_all_stocks = df_all_stocks.shape[0]
    count_top5_stocks = int(round(count_all_stocks * 0.05, 0))
    dt_temp = datetime.datetime.strptime(
        df_all_stocks.index.name, "%Y%m%d_%H%M%S"
    ).replace(second=0, microsecond=0)
    ser_realtime_concentration_rate.rename(index=dt_temp, inplace=True)
    df_amount_sort = df_all_stocks.sort_values(by=["amount"], ascending=False)
    df_amount_sort_top5 = df_amount_sort.iloc[:count_top5_stocks]
    ser_realtime_concentration_rate["count_all_stocks"] = count_all_stocks
    ser_realtime_concentration_rate["count_top5_stocks"] = count_top5_stocks
    ser_realtime_concentration_rate["mv_all"] = round(
        df_all_stocks["total_mv"].sum(), 3
    )
    ser_realtime_concentration_rate["mv_top5_sort_by_amount"] = round(
        df_amount_sort_top5["total_mv"].sum(),
        3,
    )
    ser_realtime_concentration_rate["amount_all"] = round(amount_all / 100000000, 3)
    ser_realtime_concentration_rate["amount_top5_sort_by_amount"] = round(
        df_amount_sort_top5["amount"].sum() / 100000000, 3
    )
    ser_realtime_concentration_rate["mean_all"] = round(
        df_all_stocks["total_mv"].mean(), 3
    )
    ser_realtime_concentration_rate["mean_top5_sort_by_amount"] = round(
        df_amount_sort_top5["total_mv"].mean(), 3
    )
    ser_realtime_concentration_rate["rate_mv_top5_sort_by_amount"] = round(
        ser_realtime_concentration_rate["mv_top5_sort_by_amount"]
        / ser_realtime_concentration_rate["mv_all"]
        * 100,
        3,
    )
    ser_realtime_concentration_rate["rate_amount_top5_sort_by_amount"] = round(
        ser_realtime_concentration_rate["amount_top5_sort_by_amount"]
        / ser_realtime_concentration_rate["amount_all"]
        * 100,
        3,
    )
    ser_realtime_concentration_rate["rate_mean_top5_sort_by_amount"] = round(
        ser_realtime_concentration_rate["mean_all"]
        / ser_realtime_concentration_rate["mean_top5_sort_by_amount"]
        * 100,
        3,
    )
    return ser_realtime_concentration_rate


def get_concentration_rate() -> None:
    ser_realtime_concentration_rate = _grt_realtime_concentration_rate()
    df_concentration_rate = feather_from_file(
        filename_df=filename_concentration_rate_ftr,
    )
    if df_concentration_rate.empty:
        df_concentration_rate = df_concentration_rate.reindex(
            columns=ser_realtime_concentration_rate.index
        )
        dt_max = dt_am_1015
    else:
        dt_max = df_concentration_rate.index.max()
    dt_add = pd.to_datetime(ser_realtime_concentration_rate.name)
    if dt_am_end < dt_add < dt_pm_start:
        dt_add = dt_am_end
    elif dt_add > dt_pm_end:
        dt_add = dt_pm_end
    df_concentration_rate.loc[dt_add] = ser_realtime_concentration_rate
    dt_max = dt_max + datetime.timedelta(seconds=30)
    if (
        dt_am_1015 < dt_add < dt_am_end or dt_pm_start < dt_add < dt_pm_end
    ) and dt_max < dt_add:
        str_dt_add = dt_add.strftime(format_dt)
        df_concentration_rate.index.rename(name=str_dt_add, inplace=True)
        feather_to_file(
            df=df_concentration_rate, filename_df=filename_concentration_rate_ftr
        )
        logger.debug(f"feather to {filename_concentration_rate_ftr}")
    filename_concentration_rate_csv_temp = filename_concentration_rate_csv
    i_while_csv = 0
    while i_while_csv < 3:
        i_while_csv += 1
        try:
            df_concentration_rate.to_csv(
                path_or_buf=filename_concentration_rate_csv_temp
            )
            break
        except PermissionError:
            filename_scan_csv_name = (
                filename_concentration_rate_csv.stem
                + f"_{i_while_csv}"
                + filename_concentration_rate_csv.suffix
            )
            filename_concentration_rate_csv_temp = (
                filename_concentration_rate_csv.parent.joinpath(filename_scan_csv_name)
            )
    return


def grt_str_realtime_concentration_rate() -> str:
    ser_realtime_c_r = _grt_realtime_concentration_rate()
    if ser_realtime_c_r.empty:
        logger.error(f"str_realtime_concentration_rate is empty")
        return ""
    dt_add = pd.to_datetime(ser_realtime_c_r.name)
    str_msg_concentration = (
        f"[{fg.red(f'CONC:{ser_realtime_c_r['rate_amount_top5_sort_by_amount']:.2f}%')} - "
        f"{fg.blue(f"MV_R:{ser_realtime_c_r['rate_mv_top5_sort_by_amount']:.2f}%")} - "
        f"{fg.green(f"Mean_R:{ser_realtime_c_r['rate_mean_top5_sort_by_amount']:.2f}%")}]"
    )
    str_msg_all = (
        f"Amount:[{round(ser_realtime_c_r['amount_top5_sort_by_amount'], 2)} / "
        f"{round(ser_realtime_c_r['amount_all'], 2)}](E) - "
        f"MV:[{round(ser_realtime_c_r['mv_top5_sort_by_amount']/10000, 2)} / "
        f"{round(ser_realtime_c_r['mv_all']/10000, 2)}](T) - "
        f"Mean:[{ser_realtime_c_r['mean_top5_sort_by_amount']} / "
        f"{ser_realtime_c_r['mean_all']}](E)"
    )
    str_msg_head = f"{dt_add.strftime("<%H:%M:%S>")}"
    str_msg_count = (
        f"({ser_realtime_c_r['count_top5_stocks']:4.0f} / "
        f"{ser_realtime_c_r['count_all_stocks']:4.0f})"
    )
    space_line1 = " " * 43
    space_line2 = " " * 40
    space_line3 = " " * 25
    space_line4 = " " * 5
    str_return = (
        f"{space_line1}{str_msg_head}\n "
        f"{space_line2}{str_msg_count}\n "
        f"{space_line3}{str_msg_concentration}\n"
        f"{space_line4}{str_msg_all}"
    )
    return str_return


def concentration_rate_chart() -> bool:
    if get_concentration_rate():
        logger.error("get_concentration_rate Error!")
    df_concentration_rate = feather_from_file(
        filename_df=filename_concentration_rate_ftr,
    )
    if df_concentration_rate.empty:
        return False
    else:
        try:
            dt_stale = datetime.datetime.strptime(
                df_concentration_rate.index.name, format_dt
            )
        except ValueError:
            logger.error("df_concentration_rate.index.name is empty.")
            dt_stale = datetime.datetime.now()
    str_dt_stale = f"<{dt_stale.time()}>"
    x_dt = df_concentration_rate.index.tolist()
    y_rate_mv_top5_sort_by_amount = df_concentration_rate[
        "rate_mv_top5_sort_by_amount"
    ].tolist()
    y_rate_amount_top5_sort_by_amount = df_concentration_rate[
        "rate_amount_top5_sort_by_amount"
    ].tolist()
    y_rate_min = min(y_rate_mv_top5_sort_by_amount + y_rate_amount_top5_sort_by_amount)
    y_rate_max = max(y_rate_mv_top5_sort_by_amount + y_rate_amount_top5_sort_by_amount)
    line_rate = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
            page_title="Concentration Rate",
        )
    )
    line_rate.add_xaxis(xaxis_data=x_dt)
    line_rate.add_yaxis(
        series_name="CONC",
        y_axis=y_rate_amount_top5_sort_by_amount,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_rate.add_yaxis(
        series_name="MV_R",
        y_axis=y_rate_mv_top5_sort_by_amount,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_rate.set_colors(
        colors=[
            "red",
            "blue",
            "green",
        ]
    )
    line_rate.set_global_opts(
        title_opts=opts.TitleOpts(title="Concentration Rate", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_rate_min,
            max_=y_rate_max,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    y_rate_mean_top5_sort_by_amount = df_concentration_rate[
        "rate_mean_top5_sort_by_amount"
    ].tolist()
    y_rate_mean_min = min(y_rate_mean_top5_sort_by_amount)
    y_rate_mean_max = max(y_rate_mean_top5_sort_by_amount)
    line_rate_mean = Line(
        init_opts=opts.InitOpts(
            width="1800px",
            height="860px",
            page_title="Mean Rate",
        )
    )
    line_rate_mean.add_xaxis(xaxis_data=x_dt)
    line_rate_mean.add_yaxis(
        series_name="Mean_R",
        y_axis=y_rate_mean_top5_sort_by_amount,
        is_symbol_show=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(name="最大值", type_="max"),
                opts.MarkPointItem(name="最小值", type_="min"),
            ]
        ),
    )
    line_rate_mean.set_colors(
        colors=[
            "red",
            "blue",
            "green",
        ]
    )
    line_rate_mean.set_global_opts(
        title_opts=opts.TitleOpts(title="Mean Rate", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(),
        legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
        yaxis_opts=opts.AxisOpts(
            min_=y_rate_mean_min,
            max_=y_rate_mean_max,
        ),
        datazoom_opts=opts.DataZoomOpts(
            range_start=0,
            range_end=100,
        ),
    )
    page = Page(
        page_title=f"Concentration {str_dt_stale}",
    )
    page.add(line_rate, line_rate_mean)
    page.render(path=filename_concentration_rate_charts_html)
    return True
