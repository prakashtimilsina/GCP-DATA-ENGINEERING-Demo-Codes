import yfinance as yf
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timezone
import alpaca_trade_api as tradeapi
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv 
load_dotenv() # loads .env file in current dir or parents.


'''
Below is a full working script that:
	•	Uses your live Alpaca paper trading cash balance for position sizing and risk management.
	•	Places real paper trades (buy/sell) via Alpaca for your tickers.
	•	At the end, fetches your actual account status (cash, buying power, open positions, recent orders).
	•	Sends a single HTML email report with both the trading log and the real account status.
'''

# ==== CONFIGURATION ====
TICKERS = [
    {'data': 'AAPL',     'order': 'AAPL',    'type': 'stock'},
    {'data': 'TSLA',     'order': 'TSLA',    'type': 'stock'},
    {'data': 'BTC-USD',  'order': 'BTCUSD',  'type': 'crypto'},
    {'data': 'ETH-USD',  'order': 'ETHUSD',  'type': 'crypto'},
]
YF_INTERVAL = '1h'
PERIOD = '60d'
RISK_PER_TRADE = 0.02
STOP_LOSS_PCT = 0.03
TAKE_PROFIT_PCT = 0.06
MAX_DRAWDOWN = 0.15

# Alpaca API credentials
ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
ALPACA_BASE_URL = os.getenv('ALPACA_BASE_URL')

# Email settings
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
TO_EMAIL = os.getenv('TO_EMAIL')

def get_cst_time():
    utc_now = datetime.now(timezone.utc)
    cst = pytz.timezone('America/Chicago')
    cst_now = utc_now.astimezone(cst)
    return cst_now.strftime("%Y-%m-%d %I:%M %p %Z")

# ==== ALPACA CONNECTION ====
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, api_version='v2')

# ==== TRADING LOGIC ====
def fetch_data(yf_symbol, yf_interval='1h', period='60d'):
    try:
        data = yf.download(tickers=yf_symbol, interval=yf_interval, period=period, progress=False, auto_adjust=True)
        return data
    except Exception as e:
        print(f"Error fetching data for {yf_symbol}: {e}")
        return pd.DataFrame()

def generate_signals(data):
    data['SMA20'] = data['Close'].rolling(window=20).mean()
    data['SMA50'] = data['Close'].rolling(window=50).mean()
    data['Signal'] = 0
    data.loc[data.index[20:], 'Signal'] = np.where(
        data['SMA20'][20:] > data['SMA50'][20:], 1, -1
    )
    data['Position'] = data['Signal'].diff()
    return data

def place_order(order_symbol, qty, side, asset_type, type='market', time_in_force='gtc'):
    try:
        if asset_type == 'crypto':
            order = api.submit_order(
                symbol=order_symbol,
                qty=qty,
                side=side,
                type=type,
                time_in_force=time_in_force
            )
        else:
            order = api.submit_order(
                symbol=order_symbol,
                qty=int(qty),
                side=side,
                type=type,
                time_in_force=time_in_force
            )
        return f"OrderID: {order.id} Status: {order.status}"
    except Exception as e:
        return f"Order Error: {e}"

def get_live_cash():
    try:
        account = api.get_account()
        return float(account.cash)
    except Exception as e:
        print(f"Error fetching live cash: {e}")
        return 0.0

def simulate_and_trade(data, order_symbol, asset_type, risk_per_trade, stop_loss_pct, take_profit_pct, max_drawdown):
    # Use live cash from Alpaca for position sizing
    live_cash = get_live_cash()
    if live_cash <= 0:
        print("Warning: No available cash in Alpaca account.")
        return live_cash, []

    position = 0
    entry_price = 0
    trade_log = []
    peak_balance = live_cash
    balance = live_cash

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
            # Fetch latest cash before sizing
            live_cash = get_live_cash()
            risk_amount = live_cash * risk_per_trade
            entry_price = current_price
            stop_loss = entry_price * (1 - stop_loss_pct)
            take_profit = entry_price * (1 + take_profit_pct)
            qty = round(risk_amount / entry_price, 6 if asset_type == 'crypto' else 2)
            # Only trade if you have enough cash for at least 1 share/coin
            if qty > 0:
                order_resp = place_order(order_symbol, qty, 'buy', asset_type)
                trade_log.append(('BUY', data.index[i], round(entry_price, 2), qty, str(order_resp)))
                position = qty
                balance -= risk_amount

        # Exit conditions (stop-loss, take-profit, or sell signal)
        if position > 0:
            if current_price <= stop_loss or current_price >= take_profit or data['Position'].iloc[i] == -1:
                exit_price = current_price
                order_resp = place_order(order_symbol, position, 'sell', asset_type)
                balance += position * exit_price
                trade_log.append(('SELL', data.index[i], round(exit_price, 2), position, str(order_resp)))
                position = 0
                entry_price = 0

    # Liquidate at the end if still holding
    if position > 0:
        final_price = data['Close'].iloc[-1]
        order_resp = place_order(order_symbol, position, 'sell', asset_type)
        balance += position * final_price
        trade_log.append(('FINAL SELL', data.index[-1], round(final_price, 2), position, str(order_resp)))
        position = 0

    return balance, trade_log

# ==== LIVE ACCOUNT STATUS ====
def fetch_alpaca_status():
    account = api.get_account()
    cash = float(account.cash)
    portfolio_value = float(account.portfolio_value)
    buying_power = float(account.buying_power)

    positions = api.list_positions()
    positions_html = ""
    if positions:
        positions_html += "<table border='1' cellpadding='4' cellspacing='0'><tr><th>Symbol</th><th>Qty</th><th>Avg Price</th><th>Market Value</th><th>Unrealized P/L</th></tr>"
        for pos in positions:
            unrealized = float(pos.unrealized_pl)
            color = "green" if unrealized >= 0 else "red"
            unrealized_html = f"<span style='color:{color};'>${unrealized:,.2f}</span>"
            positions_html += (
                f"<tr>"
                f"<td>{pos.symbol}</td>"
                f"<td>{pos.qty}</td>"
                f"<td>${float(pos.avg_entry_price):,.2f}</td>"
                f"<td>${float(pos.market_value):,.2f}</td>"
                f"<td>{unrealized_html}</td>"
                f"</tr>"
            )
        positions_html += "</table>"
    else:
        positions_html = "<p>No open positions.</p>"

    orders = api.list_orders(status='all', limit=10, nested=True)
    orders_html = ""
    if orders:
        orders_html += "<table border='1' cellpadding='4' cellspacing='0'><tr><th>Symbol</th><th>Qty</th><th>Side</th><th>Status</th><th>Filled Avg Price</th><th>Submitted At</th></tr>"
        for order in orders:
            filled_avg_price = order.filled_avg_price if order.filled_avg_price else "-"
            orders_html += (
                f"<tr>"
                f"<td>{order.symbol}</td>"
                f"<td>{order.qty}</td>"
                f"<td>{order.side}</td>"
                f"<td>{order.status}</td>"
                f"<td>{filled_avg_price}</td>"
                f"<td>{order.submitted_at.strftime('%Y-%m-%d %H:%M:%S') if order.submitted_at else '-'}</td>"
                f"</tr>"
            )
        orders_html += "</table>"
    else:
        orders_html = "<p>No recent orders.</p>"

    return cash, portfolio_value, buying_power, positions_html, orders_html

# ==== EMAIL REPORTING ====
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
                    <th>Order Response</th>
                </tr>
            """)
            for trade in info['trade_log']:
                action = trade[0]
                time = trade[1]
                price = trade[2]
                position = trade[3] if len(trade) > 3 else ""
                order_resp = trade[4] if len(trade) > 4 else ""
                html.append(f"<tr><td>{action}</td><td>{time}</td><td>{price}</td><td>{position}</td><td>{order_resp}</td></tr>")
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
    # 1. Run trading bot logic and collect trade logs (using real Alpaca cash for sizing)
    results = {}
    for t in TICKERS:
        print(f"\nProcessing {t['data']} ...")
        data = fetch_data(t['data'], yf_interval=YF_INTERVAL, period=PERIOD)
        if data is None or data.empty or len(data) < 60:
            print(f"No or insufficient data for {t['data']}")
            results[t['data']] = {
                'final_balance': get_live_cash(),
                'trade_log': []
            }
            continue
        data = generate_signals(data)
        final_balance, trade_log = simulate_and_trade(
            data, t['order'], t['type'], RISK_PER_TRADE, STOP_LOSS_PCT, TAKE_PROFIT_PCT, MAX_DRAWDOWN
        )
        results[t['data']] = {
            'final_balance': final_balance,
            'trade_log': trade_log
        }
        print(f"Final balance for {t['data']}: ${final_balance:.2f}")
        print(f"Trade log: {trade_log}")

    # 2. Fetch live account status
    cash, portfolio_value, buying_power, positions_html, orders_html = fetch_alpaca_status()

    # 3. Build combined HTML report
    now = get_cst_time()
    html_report = f"""
    <h1>Alpaca Trading Bot Report - {now}</h1>
    <h2>Trading Bot Simulation (using live cash for sizing)</h2>
    {log_and_summarize_html(results)}
    <hr>
    <h2>Live Alpaca Paper Account Status</h2>
    <h3>Account Balance</h3>
    <ul>
        <li>Cash: <b>${cash:,.2f}</b></li>
        <li>Portfolio Value: <b>${portfolio_value:,.2f}</b></li>
        <li>Buying Power: <b>${buying_power:,.2f}</b></li>
    </ul>
    <h3>Open Positions</h3>
    {positions_html}
    <h3>Recent Orders (last 10)</h3>
    {orders_html}
    """

    # 4. Send email
    send_email(
        subject="Alpaca Trading Bot & Account Status Report",
        html_body=html_report,
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        to_email=TO_EMAIL
    )

'''
How this script works:
	•	For each ticker, it fetches your live cash from Alpaca before every trade sizing.
	•	It places real paper trades (visible in your Alpaca dashboard).
	•	At the end, it fetches and reports your real account status.
	•	You get a full HTML email with both the trading log and your live account info.
'''