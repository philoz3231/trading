import pandas as pd
import pandas_datareader.data as web
import datetime

def download_stock_data(file_name, company_code, year1, month1, date1, year2, month2, date2):
    start = datetime.datetime(year1, month1, date1)
    end = datetime.datetime(year2, month2, date2)
    df = web.DataReader(company_code, "google", start, end)

    df.to_pickle(file_name)

    return df

download_stock_data('apple.data', 'AAPL', 2016,7,1,2017,7,1)

def load_stock_data(file_name):
    df = pd.read_pickle(file_name)
    return df

df = load_stock_data('apple.data')
print(df.describe())
