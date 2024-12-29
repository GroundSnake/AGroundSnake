import datetime
import pandas as pd
from analysis.log import logger
from analysis.const_dynamic import (
    dt_init,
    dt_pm_end,
    dt_balance,
    path_chip,
    path_chip_csv,
    path_temp,
    format_dt,
)
from analysis.base import (
    feather_from_file,
    feather_to_file,
    feather_delete,
)
from analysis.util import get_stock_code_chs
from analysis.api_tushare import index_classify, index_member


def get_industry_member(reset: bool = False) -> pd.DataFrame:
    name = "Industry_member"
    list_all_stocks = get_stock_code_chs()
    filename_industry_member = path_chip.joinpath("df_industry_member.ftr")
    if reset:
        feather_delete(filename_df=filename_industry_member)
        logger.debug("reset industry_member")
    df_industry_member = feather_from_file(
        filename_df=filename_industry_member,
    )
    if not df_industry_member.empty:
        dt_now = datetime.datetime.now()
        if dt_balance < dt_now < dt_pm_end:
            logger.debug(f"feather from {filename_industry_member}.--trading time")
            return df_industry_member
        try:
            dt_stale = datetime.datetime.strptime(
                df_industry_member.index.name,
                format_dt,
            )
        except ValueError:
            dt_stale = dt_init
        except TypeError:
            dt_stale = dt_init
        if dt_stale >= dt_pm_end:
            logger.debug(f"feather from {filename_industry_member}")
            return df_industry_member
    filename_industry_member_temp = path_temp.joinpath(f"df_industry_member_temp.ftr")
    df_industry_member = feather_from_file(filename_df=filename_industry_member_temp)
    filename_index_classify_exist = path_temp.joinpath(f"df_index_classify_exist.ftr")
    df_index_classify = feather_from_file(filename_df=filename_index_classify_exist)
    if df_index_classify.empty or df_industry_member.empty:
        df_index_classify = index_classify()
        df_index_classify = df_index_classify.reindex(
            columns=["industry_name", "is_pub", "exist"]
        )
        df_index_classify["exist"] = "N"
        feather_to_file(df=df_index_classify, filename_df=filename_index_classify_exist)
    i = 0
    count = df_index_classify.shape[0]
    for index_code in df_index_classify.index:
        i += 1
        str_msg = f"[{name}] - [{i}/{count}] - [{index_code}]"
        if df_index_classify.at[index_code, "exist"] == "Y":
            print(f"{str_msg} - Exist.")
            continue
        if df_index_classify.at[index_code, "is_pub"] == "0":
            print(f"{str_msg} - No publish index.")
        df_index_member = index_member(index_code=index_code)[["index_code"]]
        if df_index_member.empty:
            logger.error(f"{str_msg} - df_index_member is empty.")
            continue
        df_index_member["industry_name"] = df_index_classify.at[
            index_code, "industry_name"
        ]
        df_index_member["index_code_is_pub"] = int(
            df_index_classify.at[index_code, "is_pub"]
        )
        if df_industry_member.empty:
            df_industry_member = df_index_member
        else:
            df_industry_member = pd.concat(
                objs=[df_industry_member, df_index_member], axis=0, join="outer"
            )
        print(f"{str_msg} - [{df_industry_member.shape[0]}] - Update.")
        df_index_classify.at[index_code, "exist"] = "Y"
        feather_to_file(df=df_index_classify, filename_df=filename_index_classify_exist)
        feather_to_file(
            df=df_industry_member, filename_df=filename_industry_member_temp
        )
    if i >= count:
        str_dt_pm_end = dt_pm_end.strftime(format_dt)
        df_industry_member.index.rename(name=str_dt_pm_end, inplace=True)
        filename_industry_member_csv = path_chip_csv.joinpath(
            "df_industry_member.csv",
        )
        df_industry_member.to_csv(
            path_or_buf=filename_industry_member_csv,
        )
        feather_to_file(df=df_industry_member, filename_df=filename_industry_member)
        logger.debug(f"feather to {filename_industry_member}.")
        filename_index_classify_exist.unlink(missing_ok=True)
        filename_industry_member_temp.unlink(missing_ok=True)
    df_industry_member = df_industry_member[
        df_industry_member.index.isin(list_all_stocks)
    ]
    return df_industry_member
