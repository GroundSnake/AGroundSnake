import pandas as pd
import numpy as np
import warnings


def volatility_yz_rv_mean(data: pd.DataFrame, days: int = 20) -> list:
    """
    波动率-已实现波动率-Yang-Zhang 已实现波动率(Yang-Zhang Realized Volatility)
    https://github.com/hugogobato/Yang-Zhang-s-Realized-Volatility-Automated-Estimation-in-Python
    论文地址：https://www.jstor.org/stable/10.1086/209650
    基于以下公式计算:
    RV^2 = Vo + k*Vc + (1-k)*Vrs
    其中:
    - Vo: 隔夜波动率, Vo = 1/(n-1)*sum(Oi-Obar)^2
        Oi为标准化开盘价, Obar为标准化开盘价均值
    - Vc: 收盘波动率, Vc = 1/(n-1)*sum(ci-Cbar)^2
        ci为标准化收盘价, Cbar为标准化收盘价均值
    - k: 权重系数, k = 0.34/(1.34+(n+1)/(n-1))
        n为样本数量
    - Vrs: Rogers-Satchell波动率代理, Vrs = ui(ui-ci)+di(di-ci)
        ui = ln(Hi/Oi), ci = ln(Ci/Oi), di = ln(Li/Oi), oi = ln(Oi/Ci-1)
        Hi/Li/Ci/Oi分别为最高价/最低价/收盘价/开盘价

    :param days: 
    :param data: 包含 OHLC(开高低收) 价格的 pandas.DataFrame
    :type data: pandas.DataFrame
    :return: 包含 Yang-Zhang 实现波动率的 pandas.DataFrame
    :rtype: pandas.DataFrame

    要求输入数据包含以下列:
    - Open: 开盘价
    - High: 最高价
    - Low: 最低价
    - Close: 收盘价
    # yang_zhang_rv formula is give as:
    # RV^2 = Vo + k*Vc + (1-k)*Vrs
    # where Vo = 1/(n-1)*sum(Oi-Obar)^2
    # with oi = normalized opening price at time t and Obar = mean of normalized opening prices
    # Vc = = 1/(n-1)*sum(ci-Cbar)^2
    # with ci = normalized close price at time t and Cbar = mean of normalized close prices
    # k = 0.34/(1.34+(n+1)/(n-1))
    # with n = total number of days or time periods considered
    # Vrs (Rogers & Satchell RV proxy) = ui(ui-ci)+di(di-ci)
    # with ui = ln(Hi/Oi), ci = ln(Ci/Oi), di=(Li/Oi), oi = ln(Oi/Ci-1)
    # where Hi = high price at time t and Li = low price at time t
    """ ""
    warnings.filterwarnings("ignore")
    try:
        data["ui"] = np.log(np.divide(data["High"][1:], data["Open"][1:]))
        data["ci"] = np.log(np.divide(data["Close"][1:], data["Open"][1:]))
        data["di"] = np.log(np.divide(data["Low"][1:], data["Open"][1:]))
        data["oi"] = np.log(np.divide(data["Open"][1:], data["Close"][: len(data) - 1]))
    except KeyError:
        data.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
            },
            inplace=True,
        )
        data["ui"] = np.log(np.divide(data["High"][1:], data["Open"][1:]))
        data["ci"] = np.log(np.divide(data["Close"][1:], data["Open"][1:]))
        data["di"] = np.log(np.divide(data["Low"][1:], data["Open"][1:]))
        data["oi"] = np.log(np.divide(data["Open"][1:], data["Close"][: len(data) - 1]))
    data = data[1:]
    data["RS"] = data["ui"] * (data["ui"] - data["ci"]) + data["di"] * (
        data["di"] - data["ci"]
    )
    rs_var = data["RS"].groupby(pd.Grouper(freq="1D")).mean().dropna()
    vc_and_vo = data[["oi", "ci"]].groupby(pd.Grouper(freq="1D")).var().dropna()
    n = int(len(data) / len(rs_var))
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    yang_zhang_rv = np.sqrt((1 - k) * rs_var + vc_and_vo["oi"] + vc_and_vo["ci"] * k)
    yang_zhang_rv_df = pd.DataFrame(yang_zhang_rv)
    yang_zhang_rv_df.rename(columns={0: "rv"}, inplace=True)
    yang_zhang_rv_df["rv_10000"] = yang_zhang_rv_df["rv"] * 10000
    yang_zhang_rv_df.sort_index(ascending=True, inplace=True)
    count = yang_zhang_rv_df.shape[0]
    rv_10000_mean = round(yang_zhang_rv_df["rv_10000"][-days:].mean(), 2)
    return [rv_10000_mean, count]
