import yfinance as yf
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Load environment variables
load_dotenv()

# Email settings
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
TO_EMAIL = os.getenv('TO_EMAIL')

# Trading parameters
YF_INTERVAL = '1h'
PERIOD = '60d'
STOP_LOSS_PCT = 0.03
TAKE_PROFIT_PCT = 0.06

def fetch_data(symbol, yf_interval='1h', period='60d'):
    try:
        data = yf.download(tickers=symbol, interval=yf_interval, period=period, progress=False, auto_adjust=True)
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def generate_signal(data):
    # Check for required columns and enough data
    if 'SMA20' not in data.columns or 'SMA50' not in data.columns:
        return 'HOLD'
    if len(data) < 52:  # Need at least 50+ rows for SMA50 to be valid
        return 'HOLD'
    # Ensure last two rows have no NaN in SMA columns
    if data[['SMA20', 'SMA50']].iloc[-2:].isnull().values.any():
        return 'HOLD'
    prev = data.iloc[-2]
    latest = data.iloc[-1]
    try:
        # Use .item() to extract scalar from Series (future-proof)
        prev_sma20 = prev['SMA20'].item() if hasattr(prev['SMA20'], 'item') else float(prev['SMA20'])
        prev_sma50 = prev['SMA50'].item() if hasattr(prev['SMA50'], 'item') else float(prev['SMA50'])
        latest_sma20 = latest['SMA20'].item() if hasattr(latest['SMA20'], 'item') else float(latest['SMA20'])
        latest_sma50 = latest['SMA50'].item() if hasattr(latest['SMA50'], 'item') else float(latest['SMA50'])
    except Exception as e:
        return 'HOLD'
    if prev_sma20 < prev_sma50 and latest_sma20 > latest_sma50:
        return 'BUY'
    elif prev_sma20 > prev_sma50 and latest_sma20 < latest_sma50:
        return 'SELL'
    else:
        return 'HOLD'

def calculate_entry_exit(data, signal):
    price = data['Close'].iloc[-1]
    if isinstance(price, pd.Series):
        price = price.iloc[0]
    price = float(price)
    if signal == 'BUY':
        entry = price
        stop_loss = entry * (1 - STOP_LOSS_PCT)
        take_profit = entry * (1 + TAKE_PROFIT_PCT)
        return entry, stop_loss, take_profit
    elif signal == 'SELL':
        entry = price
        stop_loss = entry * (1 + STOP_LOSS_PCT)
        take_profit = entry * (1 - TAKE_PROFIT_PCT)
        return entry, stop_loss, take_profit
    else:
        return price, None, None

def analyze_tickers(ticker_list):
    summary = []
    for symbol in ticker_list:
        data = fetch_data(symbol, yf_interval=YF_INTERVAL, period=PERIOD)
        if data is None or data.empty or len(data) < 60:
            summary.append({
                "symbol": symbol,
                "signal": "NO DATA",
                "entry": None,
                "stop_loss": None,
                "take_profit": None
            })
            continue
        # Calculate SMAs
        data['SMA20'] = data['Close'].rolling(window=20).mean()
        data['SMA50'] = data['Close'].rolling(window=50).mean()
        signal = generate_signal(data)
        entry, stop_loss, take_profit = calculate_entry_exit(data, signal)
        summary.append({
            "symbol": symbol,
            "signal": signal,
            "entry": round(entry, 2) if entry is not None else '-',
            "stop_loss": round(stop_loss, 2) if stop_loss is not None else '-',
            "take_profit": round(take_profit, 2) if take_profit is not None else '-'
        })
    return summary

def build_email_html(summary):
    html = """
    <h2>Stock/Crypto Analysis Report</h2>
    <table border="1" cellpadding="5" cellspacing="0">
        <tr>
            <th>Symbol</th>
            <th>Signal</th>
            <th>Entry Price</th>
            <th>Stop Loss</th>
            <th>Take Profit</th>
        </tr>
    """
    for s in summary:
        html += f"""
        <tr>
            <td>{s['symbol']}</td>
            <td>{s['signal']}</td>
            <td>{s['entry']}</td>
            <td>{s['stop_loss']}</td>
            <td>{s['take_profit']}</td>
        </tr>
        """
    html += "</table>"
    return html

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

if __name__ == "__main__":
    # Example: Pass your list here
    ticker_list = ["AAPL", "TSLA", "BTC-USD", "ETH-USD"]
    summary = analyze_tickers(ticker_list)
    html_report = build_email_html(summary)
    send_email(
        subject="Stock/Crypto Buy/Sell Analysis",
        html_body=html_report,
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        to_email=TO_EMAIL
    )
