import os
from datetime import datetime, timedelta
import pandas as pd
import pandas_datareader
import pandas_datareader.data as web
import requests

os.environ['http_proxy'] = 'http://127.0.0.1:1082'
os.environ['https_proxy'] = 'https://127.0.0.1:1082'


def get_hist_dfs(days, stock_symbol, start, end):
    """
    获取美股历史数据

    :param days:
    :param stock_symbol
    :param start
    :param end
    """

    df = web.DataReader(stock_symbol, 'yahoo', start=start, end=end)

    return df.iloc[-(days+1): -1], df.iloc[-1]


def get_stocks_increasing_volume():
    """
    获取成交量对比前几个工作日均值明显增加

    """
    days = 30

    df_complany_nasdaq = pd.read_csv('files/companylist_NASDAQ.csv')
    df_complany_amex = pd.read_csv('files/companylist_AMEX.csv')
    df_complany_nyse = pd.read_csv('files/companylist_NYSE.csv')

    df_companies = pd.concat([df_complany_nasdaq, df_complany_amex, df_complany_nyse], ignore_index=True)

    end = datetime.today()
    start = end - timedelta(100)

    for stock_symbol in df_companies.Symbol:
        try:
            hist, last = get_hist_dfs(days, stock_symbol, start, end)
        except KeyError as e:
            print(f'{stock_symbol} request failed, {e}')
            continue
        except pandas_datareader._utils.RemoteDataError as e:
            print(f'{stock_symbol} request failed, {e}')
            continue
        except requests.exceptions.ConnectionError as e:
            print(f'{stock_symbol} request failed, {e}')
            continue

        if last.Volume > hist.Volume.sum()*2/days and \
                hist[:-3].Volume.max() < last.Volume and \
                hist.Volume.sum()/days > 100000:
            print(stock_symbol)


if __name__ == '__main__':
    get_stocks_increasing_volume()
