import yfinance as yf
import pandas as pd
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone
import pytz
import os
from dotenv import load_dotenv 
load_dotenv() # loads .env file in current dir or parents.

# ==== CONFIGURATION ====
TICKERS = ['AAPL', 'TSLA', 'BTC-USD', 'ETH-USD']
INTERVAL = '1h'
PERIOD = '60d'
INITIAL_BALANCE = 10000
RISK_PER_TRADE = 0.02      # 2% of balance per trade
STOP_LOSS_PCT = 0.03       # 3% stop loss
TAKE_PROFIT_PCT = 0.06     # 6% take profit
MAX_DRAWDOWN = 0.15        # 15% portfolio drawdown

# Email settings
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
TO_EMAIL = os.getenv('TO_EMAIL')

# ==== TIMEZONE HANDLING ====
def get_cst_time():
    utc_now = datetime.now(timezone.utc)
    cst = pytz.timezone('America/Chicago')
    cst_now = utc_now.astimezone(cst)
    return cst_now.strftime("%Y-%m-%d %I:%M %p %Z")

# ==== TRADING LOGIC ====
def fetch_data(symbol, interval='1h', period='60d'):
    data = yf.download(tickers=symbol, interval=interval, period=period, progress=False, auto_adjust=True)
    return data

def generate_signals(data):
    data['SMA20'] = data['Close'].rolling(window=20).mean()
    data['SMA50'] = data['Close'].rolling(window=50).mean()
    data['Signal'] = 0
    data.loc[data.index[20:], 'Signal'] = np.where(
        data['SMA20'][20:] > data['SMA50'][20:], 1, -1
    )
    data['Position'] = data['Signal'].diff()
    return data

def simulate_trading_with_risk(data, initial_balance, risk_per_trade, stop_loss_pct, take_profit_pct, max_drawdown):
    balance = initial_balance
    position = 0
    entry_price = 0
    trade_log = []
    peak_balance = initial_balance

    for i in range(1, len(data)):
        current_price = data['Close'].iloc[i]
        portfolio_value = balance + (position * current_price if position > 0 else 0)
        if portfolio_value > peak_balance:
            peak_balance = portfolio_value
        drawdown = (peak_balance - portfolio_value) / peak_balance
        if drawdown > max_drawdown:
            trade_log.append(("HALT", data.index[i], "Max drawdown reached. Trading halted.", ""))
            break

        # Entry condition (buy signal)
        if data['Position'].iloc[i] == 1 and position == 0:
            risk_amount = balance * risk_per_trade
            entry_price = current_price
            stop_loss = entry_price * (1 - stop_loss_pct)
            take_profit = entry_price * (1 + take_profit_pct)
            position = risk_amount / entry_price
            balance -= risk_amount
            trade_log.append(('BUY', data.index[i], round(entry_price, 2), round(position, 4)))

        # Exit conditions (stop-loss, take-profit, or sell signal)
        if position > 0:
            if current_price <= stop_loss or current_price >= take_profit or data['Position'].iloc[i] == -1:
                exit_price = current_price
                balance += position * exit_price
                trade_log.append(('SELL', data.index[i], round(exit_price, 2), round(position, 4)))
                position = 0
                entry_price = 0

    # Liquidate at the end if still holding
    if position > 0:
        final_price = data['Close'].iloc[-1]
        balance += position * final_price
        trade_log.append(('FINAL SELL', data.index[-1], round(final_price, 2), round(position, 4)))
        position = 0

    return balance, trade_log

# ==== EMAIL FORMATTING ====
def log_and_summarize_html(results):
    now = get_cst_time()
    html = [f"<h2>Trading Bot Run at {now}</h2>"]
    for ticker, info in results.items():
        html.append(f"<h3>{ticker}: Final balance = ${info['final_balance']:.2f}, Trades = {len(info['trade_log'])}</h3>")
        if info['trade_log']:
            html.append("""
            <table border="1" cellpadding="4" cellspacing="0">
                <tr>
                    <th>Action</th>
                    <th>Time</th>
                    <th>Price</th>
                    <th>Position</th>
                </tr>
            """)
            for trade in info['trade_log']:
                action = trade[0]
                time = trade[1]
                price = trade[2]
                position = trade[3] if len(trade) > 3 else ""
                html.append(f"<tr><td>{action}</td><td>{time}</td><td>{price:.2f}</td><td>{position:.2f}</td></tr>")
            html.append("</table><br>")
        else:
            html.append("<p>No trades executed.</p>")
    return "\n".join(html)

def send_email(subject, html_body, gmail_user, gmail_app_password, to_email):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = gmail_user
    msg['To'] = to_email

    part = MIMEText(html_body, 'html')
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_app_password)
            server.sendmail(gmail_user, to_email, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

# ==== MAIN EXECUTION ====
if __name__ == "__main__":
    # --- For testing HTML email, use hardcoded results. Set to True to test, False for real trading ---
    TEST_EMAIL_TABLE = True

    if TEST_EMAIL_TABLE:
        results = {
            "AAPL": {
                "final_balance": 10500,
                "trade_log": [
                    ("BUY", "2025-06-30 09:00", 180.25, 55.43),
                    ("SELL", "2025-06-30 13:00", 182.10, 55.43),
                    ("BUY", "2025-06-30 15:00", 181.00, 55.24),
                    ("SELL", "2025-06-30 19:00", 183.50, 55.24),
                ]
            },
            "BTC-USD": {
                "final_balance": 11200,
                "trade_log": [
                    ("BUY", "2025-06-30 10:00", 65000.00, 0.154),
                    ("SELL", "2025-06-30 18:00", 66000.00, 0.154),
                ]
            },
            "TSLA": {
                "final_balance": 9800,
                "trade_log": []
            }
        }
    else:
        results = {}
        for ticker in TICKERS:
            print(f"\nProcessing {ticker} ...")
            data = fetch_data(ticker, INTERVAL, PERIOD)
            if data.empty or len(data) < 60:
                print(f"No or insufficient data for {ticker}")
                results[ticker] = {
                    'final_balance': INITIAL_BALANCE,
                    'trade_log': []
                }
                continue
            data = generate_signals(data)
            final_balance, trade_log = simulate_trading_with_risk(
                data, INITIAL_BALANCE, RISK_PER_TRADE, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_DRAWDOWN
            )
            results[ticker] = {
                'final_balance': final_balance,
                'trade_log': trade_log
            }
            print(f"Final balance for {ticker}: ${final_balance:.2f}")
            print(f"Trade log: {trade_log}")

    # Log and summarize in HTML
    html_summary = log_and_summarize_html(results)

    # Send email with HTML table
    send_email(
        subject="Trading Bot Report",
        html_body=html_summary,
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        to_email=TO_EMAIL
    )
