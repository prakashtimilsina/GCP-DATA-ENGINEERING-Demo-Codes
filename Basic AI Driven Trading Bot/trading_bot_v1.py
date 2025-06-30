import yfinance as yf
import pandas as pd
import numpy as np
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone
import pytz


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
LOG_FILE = "trading_bot.log"
         
## Email Settings
GMAIL_USER = 'thaneshwortimalsina43@gmail.com'
GMAIL_APP_PASSWORD = 'yjmz dlrx mnpx ohkx'
TO_EMAIL = 'thaneshwortimalsina43@gmail.com'

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

def get_cst_time():
    utc_now = datetime.now(timezone.utc)
    cst = pytz.timezone('America/Chicago')  # CST/CDT timezone
    cst_now = utc_now.astimezone(cst)
    return cst_now.strftime("%Y-%m-%d %I:%M %p %Z")

## Log and Summarize
def log_and_summarize(results):
    now = get_cst_time()
    lines = [f"=== Trading Bot Run at {now} ===\n"]
    for ticker, info in results.items():
        lines.append(f"{ticker}: Final balance = ${info['final_balance']:.2f}, Trades = {len(info['trade_log'])}")
        for trade in info['trade_log']:
            lines.append(f"  {trade}")
        lines.append("")
    summary = "\n".join(lines)
    with open(LOG_FILE, "w") as f:
        f.write(summary)
    return summary

def send_email(subject, body, gmail_user, gmail_app_password, to_email):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = to_email

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_app_password)
            server.sendmail(gmail_user, to_email, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

##  ===== Main Loop Starts =====
if __name__ == "__main__":
    results = {} 

    for ticker in tickers:
        print(f"\nProcessing {ticker}..")    
        data = fetch_data(ticker, interval, period)
        if data.empty or len(data) < 60:
            print(f"No or insufficient data found for {ticker}....")
            continue    
        data = generate_signals(data)
        #print(f"generate signal: {data}")
        final_balance, trade_log = simulate_trades_with_risk(data, initial_balance, risk_per_trade, stop_loss_pct, take_profit_pct, max_drawdown)
        results[ticker] = {'final_balance': final_balance, 'trade_log': trade_log}
        print(f"Final Balance for {ticker}: {final_balance:.2f}")
        print(f"Trade Log: {trade_log}")

    # Log and summarize
    summary = log_and_summarize(results)

    # Send email with log
    send_email(
        subject="Trading Bot Report",
        body=summary,
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        to_email=TO_EMAIL
    )

    # # Summary
    # print("\n------ Summary ------")
    # for ticker, result in results.items():
    #     print(f"{ticker}: Final Balance = {result['final_balance']:.2f}, Trades = {len(result['trade_log'])}")


## Setup GCP VM and SSH it.

'''
create and activate a virtual environment
    python3 -m venv /home/thaneshwortimalsina43/python_trading_bot/py_envs/tradingbot
    source /home/thaneshwortimalsina43/python_trading_bot/py_envs/tradingbot/bin/activate

install your python package inside the virtual environment
    pip install yfinance pandas numpy

run the script inside the virtual environment
    python ~/trading_bot.py

Optional - schedule with cron using Virtual Environment
    crontab -e

Add a line like with path:
    0 * * * * source /home/thaneshwortimalsina43/python_trading_bot/py_envs/tradingbot/bin/activate && python /home/thaneshwortimalsina43/python_trading_bot/trading_bot.py >> /home/thaneshwortimalsina43/python_trading_bot/logs/trading_bot.log 2>&1

'''