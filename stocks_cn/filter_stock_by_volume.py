import tushare as ts
from datetime import datetime, timedelta
import pandas as pd


def get_hist_dfs(days):
    """
    获取沪深股市历史数据

    :param days:
    """
    pro = ts.pro_api(token='a30d82221b0e8430ff8392f9b793dd9a35b57917a8a2c38ca7c901b6')

    now = datetime.now()
    i = 1
    flag = days
    dfs = []
    while flag:
        trade_date = now - timedelta(i)
        df = pro.daily(trade_date=trade_date.strftime('%Y%m%d'))
        if len(df) != 0:
            df['ts_code'] = df['ts_code'].apply(lambda x: x.split('.')[0])
            df.set_index('ts_code', inplace=True)
            dfs.append(df)
            flag -= 1
        i += 1

    return dfs


def get_stocks_increasing_volume():
    """
    获取成交量对比前几个工作日均值明显增加

    """
    days = 5

    dfs = get_hist_dfs(days)

    # 历史数据成交量均值，万为计量单位
    df_result = None
    for df in dfs:
        if not isinstance(df_result, pd.DataFrame):
            df_result = df.loc[:, ['vol']]
        else:
            df_result += df.loc[:, ['vol']]
    df_result['vol'] = df_result['vol'].apply(lambda x: x / days*100)

    # 今天数据
    df_now = ts.get_today_all()
    df_now = df_now.loc[(df_now.changepercent > -1) & (df_now.changepercent < 3)]
    df_now.rename(columns={'code': 'ts_code'}, inplace='True')
    df_now.set_index('ts_code', inplace=True)
    df_now['volume'] = df_now['volume'].apply(lambda x: x / 10000)

    df_result = df_result.join(df_now['volume'], lsuffix='hist', rsuffix='now')

    df_filtered = df_result.loc[df_result['volume'] > 2 * df_result['vol']]

    df_filtered.to_csv('filtered_stocks.csv')


if __name__ == '__main__':
    get_stocks_increasing_volume()
