import datetime
import os
import re
import uuid
import pandas as pd
from loguru import logger
from analysis.const import path_main


class TraderHc(object):
    def __init__(self, account):
        self.name = "TraderHc"
        self.account = account
        self.position_type = "普通头寸"
        list_columns = [
            "交易单元",
            "代码",
            "市场",
            "方向",
            "委托数量",
            "报价方式",
            "委托价格",
            "头寸类型",
            "算法名称",
            "开始时间",
            "结束时间",
            "参与率",
            "松紧度",
            "是否参与开盘竞价",
            "是否参与收盘竞价",
            "限价内参与率",
            "错误信息",
        ]
        self.df_file_order = pd.DataFrame(columns=list_columns)
        self.df_file_order.index.name = "ID"

    @staticmethod
    def get_market_type(code: str):
        """
        :param code: e.g. "600519"
        :return: ["上海A股", "深圳A股", "北京A股"]
        """
        if re.match(r"30\d{4}|00\d{4}|12\d{4}", code):
            return "深圳A股"
        elif re.match(r"60\d{4}|68\d{4}|11\d{4}", code):
            return "上海A股"
        elif re.match(r"430\d{3}|83\d{4}|87\d{4}", code):
            return "北京A股"

    def add_order(self, symbol, volume, order_type, order_business, price) -> bool:
        col_id = uuid.uuid4()
        self.df_file_order.at[col_id, "交易单元"] = self.account
        self.df_file_order.at[col_id, "代码"] = symbol
        self.df_file_order.at[col_id, "市场"] = self.get_market_type(code=symbol)
        self.df_file_order.at[col_id, "方向"] = order_business
        self.df_file_order.at[col_id, "委托数量"] = volume
        self.df_file_order.at[col_id, "报价方式"] = order_type
        self.df_file_order.at[col_id, "委托价格"] = price
        self.df_file_order.at[col_id, "头寸类型"] = self.position_type
        return True

    def send_order(self) -> bool:
        if self.df_file_order.empty:
            logger.error("Order queue is empty.")
            return False
        else:
            str_now = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
            filename_order = os.path.join(path_main, "order", f"order_{str_now}.csv")
            self.df_file_order.to_csv(path_or_buf=filename_order)
            # self.df_file_order.to_dbf(path_or_buf=filename_order)
            print(self.df_file_order)
            self.df_file_order.drop(self.df_file_order.index, inplace=True)
            print("清空")
            print(self.df_file_order)
            return True
