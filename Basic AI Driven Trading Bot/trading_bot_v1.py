import yfinance as yf
import pandas as pd
import numpy as np

## We can setup below code in VM of cloud as well.

## Fetch Market Data
def fetch_data(symbol, interval='1h', period='60d'):
    data = yf.download(tickers=symbol, interval=interval, period=period)
    return data

symbol = 'BTC-USD'
data = fetch_data(symbol)
print(data.tail())

## Signal Generation (Simple AI/ML Placeholder)
## For demonstration, we will use a simple moving average crossover. We can update later with an ML model.

def generate_signals(data):
    data['SMA_20'] = data['Close'].rolling(window=20).mean()
    data['SMA_50'] = data['Close'].rolling(window=50).mean()
    data['Signal'] = 0.0
    data['Signal'][20:] = np.where(data['SMA_20'][20:] > data['SMA_50'][20:], 1, -1)
    data['Position'] = data['Signal'].diff()
    return data

data = generate_signals(data)
print(data[['Close', 'SMA_20', 'SMA_50', 'Signal', 'Position']].tail())


## We'll simulate trades based on signals. For live or paper trading with real brokers, 
# use APIs like Alpaca, interactive Brokers,or Binance 

initial_balance = 10000
balance = initial_balance
position = 0
trade_log = []

for i in range(1, len(data)):
    if data['Position'].iloc[i] == 1: #Buy Signal
        position = balance / data['Close'].iloc[i]
        balance = 0
        trade_log.append(('BUY', data.index[i], data['Close'].iloc[i]))

    elif data['Position'].iloc[i] == -1 and position > 0: #Sell Signal
        balance = position * data['Close'].iloc[i]
        position = 0    
        trade_log.append(('SELL', data.index[i], data['Close'].iloc[i]))

# Final Balance
if position > 0:
    balance = position * data['Close'].iloc[-1]

print(f"Final Balance: {balance:.2f}")
print("Trade Log: ", trade_log)




