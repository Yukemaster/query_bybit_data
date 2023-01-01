import os
import pandas as pd
import time
import ccxt
from datetime import datetime
import openpyxl

import config

'''DISCLAIMER

    For this to run you need to have a working Bybit API key that is stored as a string in a config.py file. See lines 107 and 108. Querying data from bybit using the testnet can give spotty data.

    Possible issues:
    - timezone confusion (still works fine though)
    - cannot query data from before the first entry in the file (start time jumps to the last entry)
    

    For a list of available symbols run these 3 lines:
    
    raw = session_unauth.query_symbol()['result']
    availableSymbols = [symbol['name'] for symbol in raw]
    print(availableSymbols)

    Variables:
    - dataSymbol: the symbol to query
    - data_mode: can be 'csv' or 'excel'. Creates either a csv or excel for the queried data. 'excel' is broken :(.
    - dataInterval allowed: 1m 3m 5m 15m 30m 1h 2h 4h 12h "1D" "1W" "1M" and perhaps more.
    - day: YYYY-MM-DD HH:MM:SS
    - dataStartTime: converts day to a unix timestamp in ms
    - intervalTimestamp: ms, match the timeframe you're querying (change first number - format seconds)
    - dataLimit: specify how many bars of data you want to query. Bybits limit is 200.
'''

dataSymbol = 'BTCUSD'
data_mode = 'csv'
dataInterval = "3m"
day = '2022-12-31 01:00:00'
dataStartTime = round(datetime.strptime(str(day), '%Y-%m-%d %H:%M:%S').timestamp()*1000)
intervalTimestamp = 180 * 1000
dataLimit = 200

print('starting at: ' + day)


def excel_data():
    global dataStartTime, filename, excel_sheet_name
    #set the filename for the excel based on the selected timeframe
    if 'm' in dataInterval:
        excel_sheet_name = dataInterval + "in_data"
    else:
        excel_sheet_name = dataInterval + "_data"
    filename = excel_sheet_name + '.xlsx'

    #check if an excel for specified timeframe already exists
    if not os.path.isfile(filename):
        wb = openpyxl.Workbook()
        wb.save(filename)
        print('Created new sheet: ' + filename)

    #check if excel is empty or not
    existing_data = pd.read_excel(filename)
    if not existing_data.empty:
        last_existing_data = existing_data['open_time'].iloc[-1]
        last_existing_data_int = round(datetime.strptime(str(last_existing_data), '%Y-%m-%d %H:%M:%S').timestamp()*1000)
        print(filename + ' is not empty. Last existing data: ' + str(last_existing_data))

        #changing the start time according to the data thats already available (so no gaps occur)
        dataStartTime = last_existing_data_int + intervalTimestamp
        dataStartTime_readable = str(datetime.fromtimestamp(dataStartTime/1000))
        print('New starting point: ' + str(dataStartTime_readable))

    else:
        print(filename + ' is empty. Starting point remains the same.')


def csv_data():
    global dataStartTime, csv_name
    # filename for csv file
    if 'm' in dataInterval:
        csv_name = dataInterval + "in_data.csv"
    else:
        csv_name = dataInterval + '_data.csv'

    # check if a csv file for specified timeframe already exists
    if not os.path.isfile(csv_name):
        open(csv_name, 'w').close()
        print('Created new .csv file: ' + csv_name)

    #check if csv is empty or not
    size = os.stat(csv_name).st_size
    if size != 0:
        existing_csv_data = pd.read_csv(csv_name)
        last_existing_csv_data = existing_csv_data['open_time'].iloc[-1]
        last_existing_csv_data_int = round(datetime.strptime(str(last_existing_csv_data), '%Y-%m-%d %H:%M:%S').timestamp()*1000)
        print(csv_name + ' is not empty. Last existing data: ' + str(last_existing_csv_data))

        #changing the start time according to the data thats already available (so no gaps occur)
        dataStartTime = last_existing_csv_data_int + intervalTimestamp
        dataStartTime_readable = str(datetime.fromtimestamp(dataStartTime/1000))
        print('New starting point: ' + str(dataStartTime_readable))

    else:
        print(csv_name + ' is empty. Starting point remains the same.')

#function to query data from bybit and return it
def getData(dataSymbol = "BTCUSD", dataInterval = "15m", dataStartTime = 1653408000, dataLimit=200):
    # Set the exchange and the trading pair
    exchange = ccxt.bybit({
        'apiKey': config.bybit_api_key, 
        'secret': config.bybit_api_secret
    })
    # Get the ohlcv data from Bybit
    ohlcv = exchange.fetch_ohlcv(symbol=dataSymbol, since=dataStartTime, timeframe=dataInterval, limit=dataLimit)

    # Create a pandas DataFrame from the ohlcv data
    df = pd.DataFrame(ohlcv, columns=['open_time', 'Open', 'High', 'Low', 'Close', 'Volume'])

    return df


#check data mode
fail = False
if data_mode == 'csv':
    csv_data()
elif data_mode == 'excel':
    excel_data()
else:
    print('Enter a valid format: csv or excel')
    fail = True


#repeatedly calls the getData() function from the starting point dataStartTime until the current time is reached
df_list = []
while True and not fail:
    if data_mode == 'csv':
        now = round(time.time(),2) * 1000
        if int(dataStartTime) <= int(now):
            #print('inside if')
            new_df = getData(dataSymbol, dataInterval, dataStartTime, dataLimit)
        else:
            print('current time reached: ' + str(now))
            break
    elif data_mode == 'excel':
        now = round(time.time(),2) * 1000
        if int(dataStartTime) <= int(now):
            #print('inside if')
            new_df = getData(dataSymbol, dataInterval, dataStartTime, dataLimit)

        else:
            print('current time reached: ' + str(now))
            break
    
    #append results to the list
    df_list.append(new_df)

    #set the start time to get the next 200 rows
    dataStartTime = max(new_df['open_time'], default=0) + intervalTimestamp
    print('Current starting time: ' + str(datetime.fromtimestamp(dataStartTime/1000)))

# clean up the dataframe, yes there are now two open_time columns: don't question it (or just drop the other one)
df = pd.concat(df_list).reset_index().drop('index', axis=1)
df['open_time']= pd.to_datetime(df['open_time'], unit='ms')   
df.index = df['open_time']
df[['Open', 'High', 'Low', 'Close', 'Volume']] = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)


print(df)

# writing the queried data to created/existing .csv file.
if data_mode == 'csv':
    size = os.stat(csv_name).st_size
    if size == 0:
        df.to_csv(csv_name, index=False)
        print('File was empty. Wrote everything to it.')
    else:
        df.to_csv(csv_name, header=False, index=False, mode='a')
        print('File was not empty. Appended data.')

# dont use, use csv mode instead. this mf doesn't wanna append ReallyMad
elif data_mode == 'excel':
    with pd.ExcelWriter(filename, engine='openpyxl', mode='a', if_sheet_exists='error') as writer:
        df.to_excel(writer, sheet_name=excel_sheet_name)

      
  


