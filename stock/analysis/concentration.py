# modified at 2023/05/18 22::25
import datetime
import pandas as pd
from pyecharts.charts import Line, Page
import pyecharts.options as opts
from loguru import logger
import analysis.ashare
import analysis.base
from analysis.const import (
    dt_date_init,
    time_pm_end,
    dt_am_start,
    dt_am_end,
    dt_pm_start,
    dt_pm_end,
    dt_pm_end_last_T1,
    dt_date_trading,
    dt_date_trading_last_T1,
    list_all_stocks,
    filename_chip_shelve,
    filename_concentration_rate_charts,
)


def concentration_rate() -> str:
    name: str = f"df_concentration_rate"
    df_realtime = analysis.ashare.stock_zh_a_spot_em()  # 调用实时数据接口
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
    total_mv_top5 = df_realtime_top5["total_mv"].sum()
    total_mv_tail95 = df_realtime_tail95["total_mv"].sum()
    total_mv_all = total_mv_top5 + total_mv_tail95
    if amount_all != 0:
        rate_amount_top5 = (amount_top5 / amount_all * 100).round(2)
    else:
        rate_amount_top5 = 0
    rate_amount_tail95 = 100 - rate_amount_top5
    rate_total_mv_top5 = (total_mv_top5 / total_mv_all * 100).round(2)
    rate_total_mv_tail95 = 100 - rate_total_mv_top5
    index_concentration = (rate_amount_top5 - rate_total_mv_top5).round(2)
    str_msg = (
        f"{round(amount_all, 2)} - Index[{index_concentration:6.2f}]"
        f" - Amount[{rate_amount_top5:.2f}({rate_total_mv_top5:.2f})]"
        f" - Turnover[{turnover_top5:.2f}/{turnover_tail95:.2f}]"
        f" - Amplitude[{amplitude_top5:.2f}/{amplitude_tail95:.2f}]"
    )
    df_concentration_rate = analysis.base.read_df_from_db(
        key=name, filename=filename_chip_shelve
    )
    dt_now = datetime.datetime.now()
    if dt_am_start < dt_now < dt_am_end or dt_pm_start < dt_now < dt_pm_end:
        df_concentration_rate.at[dt_now, "rate_amount_top5"] = rate_amount_top5
        df_concentration_rate.at[dt_now, "rate_amount_tail95"] = rate_amount_tail95
        df_concentration_rate.at[dt_now, "rate_total_mv_top5"] = rate_total_mv_top5
        df_concentration_rate.at[dt_now, "rate_total_mv_tail95"] = rate_total_mv_tail95
        df_concentration_rate.at[dt_now, "turnover_top5"] = turnover_top5
        df_concentration_rate.at[dt_now, "turnover_tail95"] = turnover_tail95
        df_concentration_rate.at[dt_now, "amplitude_top5"] = amplitude_top5
        df_concentration_rate.at[dt_now, "amplitude_tail95"] = amplitude_tail95
        df_concentration_rate.at[dt_now, "index_concentration"] = index_concentration
        analysis.base.write_obj_to_db(
            obj=df_concentration_rate,
            key=name,
            filename=filename_chip_shelve,
        )
    if not df_concentration_rate.empty:
        x_axis = df_concentration_rate.index.tolist()
        y_axis_rate_amount_top5 = df_concentration_rate["rate_amount_top5"].tolist()
        y_axis_rate_total_mv_top5 = df_concentration_rate["rate_total_mv_top5"].tolist()
        y_axis_index_concentration = df_concentration_rate[
            "index_concentration"
        ].tolist()
        y_min = min(
            y_axis_rate_amount_top5
            + y_axis_rate_total_mv_top5
            + y_axis_index_concentration
        )
        y_max = max(
            y_axis_rate_amount_top5
            + y_axis_rate_total_mv_top5
            + y_axis_index_concentration
        )
        line_concentration_rate = Line(
            init_opts=opts.InitOpts(
                width="1800px",
                height="860px",
                page_title="Concentration Rate",
            )
        )
        line_concentration_rate.add_xaxis(xaxis_data=x_axis)
        line_concentration_rate.add_yaxis(
            series_name="rate_amount_top5",
            y_axis=y_axis_rate_amount_top5,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.add_yaxis(
            series_name="rate_total_mv_top5",
            y_axis=y_axis_rate_total_mv_top5,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.add_yaxis(
            series_name="index_concentration",
            y_axis=y_axis_index_concentration,
            is_symbol_show=False,
            markpoint_opts=opts.MarkPointOpts(
                data=[
                    opts.MarkPointItem(name="最大值", type_="max"),
                    opts.MarkPointItem(name="最小值", type_="min"),
                ]
            ),
        )
        line_concentration_rate.set_global_opts(
            title_opts=opts.TitleOpts(title="Concentration Rate", pos_left="center"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            toolbox_opts=opts.ToolboxOpts(),
            legend_opts=opts.LegendOpts(orient="vertical", pos_right=0, pos_top="48%"),
            yaxis_opts=opts.AxisOpts(
                min_=y_min,
                max_=y_max,
            ),
            datazoom_opts=opts.DataZoomOpts(
                range_start=0,
                range_end=100,
            ),
        )
        page = Page(
            page_title="concentration",
        )
        page.add(line_concentration_rate)
        page.render(path=filename_concentration_rate_charts)
        logger.trace(f"{name} End")
    return str_msg


def concentration() -> bool:
    name: str = "df_concentration"
    if analysis.base.is_latest_version(key=name, filename=filename_chip_shelve):
        logger.trace(f"{name},Break and End")
        return True
    df_realtime = analysis.ashare.stock_zh_a_spot_em()  # 调用实时数据接口
    df_realtime.sort_values(by=["amount"], ascending=False, inplace=True)
    top5_stocks = int(round(len(list_all_stocks) * 0.05, 0))
    df_realtime_top5 = df_realtime.iloc[:top5_stocks]
    df_concentration_old = analysis.base.read_df_from_db(
        key="df_concentration", filename=filename_chip_shelve
    )
    if df_concentration_old.empty:
        df_concentration = pd.DataFrame(
            index=list_all_stocks,
            columns=[
                "first_concentration",
                "latest_concentration",
                "days_concentration",
                "times_concentration",
            ],
        )
    else:
        df_concentration_empty = pd.DataFrame(index=list_all_stocks)
        df_concentration = pd.concat(
            objs=[df_concentration_old, df_concentration_empty],
            axis=1,
            join="outer",
        )
    df_concentration["first_concentration"].fillna(value=dt_date_init, inplace=True)
    df_concentration["latest_concentration"].fillna(value=dt_date_init, inplace=True)
    df_concentration["days_concentration"].fillna(value=0, inplace=True)
    df_concentration["times_concentration"].fillna(value=0, inplace=True)
    if dt_pm_end_last_T1 < datetime.datetime.now() <= dt_pm_end:
        date_latest = dt_date_trading_last_T1
    else:
        date_latest = dt_date_trading
    for symbol in df_concentration.index:
        if symbol in df_realtime_top5.index:
            if df_concentration.at[symbol, "first_concentration"] == dt_date_init:
                df_concentration.at[
                    symbol, "first_concentration"
                ] = df_concentration.at[symbol, "latest_concentration"] = date_latest
                df_concentration.at[symbol, "days_concentration"] = 1
                df_concentration.at[symbol, "times_concentration"] = 1
            else:
                if df_concentration.at[symbol, "latest_concentration"] != date_latest:
                    df_concentration.at[symbol, "latest_concentration"] = date_latest
                    df_concentration.at[symbol, "times_concentration"] += 1
                df_concentration.at[symbol, "days_concentration"] = (
                    df_concentration.at[symbol, "latest_concentration"]
                    - df_concentration.at[symbol, "first_concentration"]
                ).days + 1
    df_concentration.sort_values(
        by=["times_concentration"], ascending=False, inplace=True
    )
    analysis.base.write_obj_to_db(
        obj=df_concentration,
        key="df_concentration",
        filename=filename_chip_shelve,
    )
    dt_concentration_date = df_concentration["latest_concentration"].max(skipna=True)
    if dt_concentration_date > date_latest:
        logger.error(
            f"Error - dt_concentration_date[{dt_concentration_date}] greater then date_latest"
        )
        dt_concentration_date = date_latest
    dt_concentration = datetime.datetime.combine(dt_concentration_date, time_pm_end)
    analysis.base.set_version(key=name, dt=dt_concentration)
    print(df_concentration)
    return True
