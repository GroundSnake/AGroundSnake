# coding=utf8
"""变量开头备注

file_nane_ : 文件名，可以包括路径
file_      : 文件对象
path_      : 单纯路径，不包括文件名
str_       : 字符串
"""
OrderSide_Unknown = 0
OrderSide_Buy = 1             # 买入
OrderSide_Sell = 2            # 卖出

order_side_map = {OrderSide_Buy: "买入", OrderSide_Sell: "卖出"}
