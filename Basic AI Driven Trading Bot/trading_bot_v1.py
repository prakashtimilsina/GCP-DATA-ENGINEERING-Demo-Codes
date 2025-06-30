import yfinance as yf
import pandas as pd
import numpy as np

## Multi-Asset Trading Bot Template
'''
    .   Multiple tickers (AAPL, TSLA, BTC-USD, ETH-USD)
	•	Moving average crossover strategy
	•	Position sizing (risk per trade)
	•	Stop-loss and take-profit
	•	Portfolio drawdown control
	•	Trade logging and summary
'''

## We can setup below code in VM of cloud as well.

# Configuration
# List of tickers (can be stocks or cryptos)
tickers = ['BTC-USD', 'ETH-USD', 'LTC-USD', 'AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
# Parameters
interval = '1h'
period = '60d'
initial_balance = 100000
stop_loss_pct = 0.03 # 5% stop loss
take_profit_pct = 0.05 # 10% take profit 
risk_per_trade = 0.02 # 5% risk per trade 
max_drawdown = 0.15 # 15% max drawdown
         

## Fetch Market Data
def fetch_data(symbol, interval='1h', period='60d'):
    data = yf.download(tickers=symbol, interval=interval, period=period, progress=False, auto_adjust=True)
    data.dropna(inplace=True)
    return data

## Signal Generation (Simple AI/ML Placeholder)
## For demonstration, we will use a simple moving average crossover. We can update later with an ML model.

def generate_signals(data):
    data['SMA_20'] = data['Close'].rolling(window=20).mean()
    data['SMA_50'] = data['Close'].rolling(window=50).mean()
    data['Signal'] = 0
    data.loc[data.index[20:], 'Signal'] = np.where(data['SMA_20'][20:] > data['SMA_50'][20:], 1, -1)
    data['Position'] = data['Signal'].diff()
    return data

## We'll simulate trades based on signals. For live or paper trading with real brokers, 
# use APIs like Alpaca, interactive Brokers,or Binance 
def simulate_trades_with_risk(data, initial_balance, risk_per_trade, stop_loss_pct, take_profit_pct, max_drawdown):
    balance = initial_balance
    position = 0
    entry_price = 0
    trade_log = []
    peak_balance = initial_balance   

    for i in range(1, len(data)):
        # Calculate current portfolio value
        current_price = data['Close'].iloc[i]
        portfolio_value = balance + (position * current_price if position > 0 else 0)

        # Update peak balance for drawdown tracking
        if portfolio_value > peak_balance:
            peak_balance = portfolio_value
        # Calculate drawdown
        drawdown = (peak_balance - portfolio_value) / peak_balance
        if drawdown > max_drawdown:
            print("Max drawdown reached. Halting trading ...")
            break 

        # Entry signal ( Buy Signal )
        if data['Position'].iloc[i] == 1 and position == 0:
            # Risk-based position sizing
            risk_amount = balance * risk_per_trade  
            entry_price = current_price
            stop_loss = entry_price * (1 - stop_loss_pct)
            take_profit = entry_price * (1 + take_profit_pct)   
            position = risk_amount / entry_price 
            balance -= risk_amount
            trade_log.append(('BUY', data.index[i], entry_price, position))
            
        # Exit signal - Monitor for exit
        if position > 0:
            if current_price <= stop_loss or current_price >= take_profit or data['Position'].iloc[i] == -1:
                exit_price = current_price
                balance += position * exit_price
                trade_log.append(('SELL', data.index[i], exit_price, position))
                position = 0
                entry_price = 0      
            
    # Liquidate at the end if still holding
    if position > 0:
        final_price = data['Close'].iloc[-1]
        balance += position * final_price
        trade_log.append(('FINAL SELL', data.index[-1], final_price, position))
        position 
   
    return balance, trade_log

##  ===== Main Loop Starts =====

results = {} 

for ticker in tickers:
    print(f"\nProcessing {ticker}..")    
    data = fetch_data(ticker, interval, period)
    if data.empty or len(data) < 60:
        print(f"No or insufficient data found for {ticker}....")
        continue    
    data = generate_signals(data)
    print(f"generate signal: {data}")
    final_balance, trade_log = simulate_trades_with_risk(data, initial_balance, risk_per_trade, stop_loss_pct, take_profit_pct, max_drawdown)
    results[ticker] = {'final_balance': final_balance, 'trade_log': trade_log}
    print(f"Final Balance for {ticker}: {final_balance:.2f}")
    print(f"Trade Log: {trade_log}")

# Summary
print("\n------ Summary ------")
for ticker, result in results.items():
    print(f"{ticker}: Final Balance = {result['final_balance']:.2f}, Trades = {len(result['trade_log'])}")









