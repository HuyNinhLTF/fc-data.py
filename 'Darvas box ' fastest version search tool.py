import pandas as pd
import mplfinance as mpl
import matplotlib.pyplot as plt
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
        daily_stock_price.set_index('TradingDate', inplace=True)
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
    # add new column candle type
    daily_price_df['Candle type'] = daily_price_df.apply(candle_type, axis=1)
    # add new column pivotpoint

    return daily_price_df

def get_flatbase_value(dataframe):
    fifteen_closest_price = dataframe['Close'].iloc[-15:]
    sort_fifteen_closest_price = sorted(fifteen_closest_price)
    flatbase_value = (sort_fifteen_closest_price[-1] - sort_fifteen_closest_price[0]) / sort_fifteen_closest_price[0]
    return flatbase_value

def count_bad_candles(dataframe):
    return (dataframe['Candle type'].iloc[-15:] == 'doji or red body').sum()

def get_close_value(dataframe):
    if not dataframe.empty and len(dataframe) > 0:
        return dataframe['Value'].iloc[-1]
    else:
        return None  # Return an appropriate value when the DataFrame is empty


list_name_companies = get_securities_list()

# Fetch data in parallel
result_data = parallel_fetch_data(list_name_companies)

# Filter and print the result
result = [symbol for symbol, daily_stock_price in result_data
          if count_bad_candles(daily_stock_price) < 3
          and get_close_value(daily_stock_price) is not None  # Check for None
          and get_close_value(daily_stock_price) > 2000000000
          and get_flatbase_value(daily_stock_price) < 0.08]
print(result)

# Plot all charts at once
#for symbol in result:
#    daily_price_df = get_daily_price(symbol)
#    daily_price_df.set_index('TradingDate',inplace=True)
#    mpl.plot(daily_price_df, type='candle', mav=(10, 20, 50), volume=True, title=symbol)

#plt.show()

