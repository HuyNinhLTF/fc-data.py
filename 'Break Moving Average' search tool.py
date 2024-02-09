import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from ssi_fc_data import fc_md_client, model
import config
import datetime

client = fc_md_client.MarketDataClient(config)

def get_securities_list():
    req = model.securities('HOSE', 1, 1000)
    data = client.securities(config, req)
    df = pd.DataFrame(data['data'], columns=['Symbol', 'StockName', 'StockEnName'])
    list_name_companies = [symbol for symbol in df['Symbol'] if len(symbol) == 3]
    return list_name_companies

def fetch_data(symbol):
    try:
        daily_stock_price = get_daily_price(symbol)
        return symbol, daily_stock_price
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def parallel_fetch_data(symbols):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(fetch_data, symbols))
    return [result for result in results if result is not None]

def candle_type(ohlc_price):
    HC_index = ohlc_price['High'] / ohlc_price['Close']
    CL_index = ohlc_price['Close'] / ohlc_price['Low']
    return 'doji or red body' if HC_index >= 1.015 else ('hammer or green body' if CL_index >= 1.015 else 'normal')

def get_daily_price(name, start_date='01/10/2023', end_date = None):
    if end_date is None:
        end_date = datetime.datetime.now().strftime('%d/%m/%Y')
    data = client.daily_ohlc(config, model.daily_ohlc(name, start_date, datetime.datetime.now().strftime('%d/%m/%Y'), 1, 1000, True))
    daily_price_df = pd.DataFrame(data['data'], columns=['Symbol', 'TradingDate', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value'])
    # convert columns type
    daily_price_df['TradingDate'] = pd.to_datetime(daily_price_df['TradingDate'], format='%d/%m/%Y')
    cols = ['Open', 'High', 'Low', 'Close', 'Volume', 'Value']
    daily_price_df[cols] = daily_price_df[cols].apply(pd.to_numeric, errors='coerce')
    # add new column volume 20 average
    daily_price_df['Volume 20 Average'] = daily_price_df['Volume'].rolling(window=20, min_periods=0).mean().astype(float)
    # add new column MA 20
    daily_price_df['MA 20'] = daily_price_df['Close'].rolling(window=20, min_periods=0).mean().astype(float)
    # add new column MA 50
    daily_price_df['MA 50'] = daily_price_df['Close'].rolling(window= 50, min_periods=0).mean().astype(float)
    # add new column MA 200
    daily_price_df['MA 200'] = daily_price_df['Close'].rolling(window=200, min_periods=0).mean().astype(float)
    # add new column candle type
    daily_price_df['Candle type'] = daily_price_df.apply(candle_type, axis=1)
    # add new column break MA 20
    daily_price_df['Breakout'] = daily_price_df.apply(break_MA, df=daily_price_df, axis=1)
    return daily_price_df

def break_MA(row,df):
    pre_row = df.iloc[row.name-1] if row.name -1 >= 0 else None
    if pre_row is not None and pre_row['Close'] < pre_row['MA 20'] and row['Close'] > row['MA 20']:
        if row['Candle type'] == 'hammer or green body' and row['Volume'] > row['Volume 20 Average']:
            return 'Break MA 20'
    if pre_row is not None and pre_row['Close'] < pre_row['MA 50'] and row['Close'] > row['MA 50']:
        if row['Candle type'] == 'hammer or green body' and row['Volume'] > row['Volume 20 Average']:
            return 'Break MA 50'
    if pre_row is not None and pre_row['Close'] < pre_row['MA 200'] and row['Close'] > row['MA 200']:
        if row['Candle type'] == 'hammer or green body' and row['Volume'] > row['Volume 20 Average']:
            return 'Break MA 200'
    else:
        return None

def get_close_value(dataframe):
    if not dataframe.empty and len(dataframe) > 0:
        return dataframe['Value'].iloc[-1]
    else:
        return None  # Return an appropriate value when the DataFrame is empty





if __name__ == "__main__":
    list_name_companies = get_securities_list()
    data_companies = parallel_fetch_data(list_name_companies)
    for symbol, daily_stock_price_df in data_companies:
        result = daily_stock_price_df.loc[~daily_stock_price_df['Breakout'].isnull()]
        fifteen_day_ago = datetime.datetime.now() - datetime.timedelta(days=15)
        result = result[result['TradingDate'] > fifteen_day_ago]
        #result = result[result['TradingDate'] > datetime.datetime(2024, 1, 1)]
        if not result.empty and get_close_value(result)> 5000000000:
            result['TradingDate'] = result['TradingDate'].dt.strftime('%d/%m/%Y')
            print(result[['Symbol','TradingDate','Breakout']])





