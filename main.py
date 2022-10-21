import requests


if __name__ == "__main__":
    r = requests.session()
    url ="https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600519,day,2022-09-01,2022-10-10,10,qfq"
    r = r.get(url)
    print(type(r))
    print(r.text)

