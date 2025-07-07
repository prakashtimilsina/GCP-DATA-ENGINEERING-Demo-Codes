import os
import requests
import pandas as pd
from datetime import datetime, timezone
import openai

# Load API keys from environment
ALPACA_KEY = os.getenv('APCA_API_KEY_ID')
ALPACA_SECRET = os.getenv('APCA_API_SECRET_KEY')
OPENAI_KEY = os.getenv('OPENAI_API_KEY')

ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET
}

# 1. Check if US market is open
def is_market_open():
    resp = requests.get(f"{ALPACA_BASE_URL}/v2/clock", headers=HEADERS)
    return resp.json().get("is_open", False)

# 2. Fetch recent price bars for a symbol
def fetch_bars(symbol, timeframe="5Min", limit=30):
    url = f"{ALPACA_BASE_URL}/v2/stocks/{symbol}/bars"
    params = {"timeframe": timeframe, "limit": limit}
    resp = requests.get(url, headers=HEADERS, params=params)
    bars = resp.json().get("bars", [])
    if not bars:
        return None
    df = pd.DataFrame(bars)
    df['t'] = pd.to_datetime(df['t'])
    df.set_index('t', inplace=True)
    return df

# 3. Calculate RSI and MACD
def calculate_indicators(df):
    close = df['c']
    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    rsi_value = rsi.iloc[-1]

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_value = macd.iloc[-1]
    return float(rsi_value), float(macd_value)

# 4. Use OpenAI to interpret indicators
def get_ai_signal(symbol, rsi, macd):
    openai.api_key = OPENAI_KEY
    prompt = (
        f"Stock: {symbol}\n"
        f"RSI: {rsi:.2f}\n"
        f"MACD: {macd:.2f}\n"
        "If RSI < 30 and MACD > 0, recommend BUY.\n"
        "If RSI > 70 and MACD < 0, recommend SELL.\n"
        "Otherwise, HOLD.\n"
        "Respond with only one word: BUY, SELL, or HOLD."
    )
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1
    )
    signal = response.choices[0].message.content.strip().upper()
    return signal

# 5. Place order via Alpaca
def place_order(symbol, qty, side):
    order = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": "market",
        "time_in_force": "day"
    }
    resp = requests.post(f"{ALPACA_BASE_URL}/v2/orders", headers=HEADERS, json=order)
    return resp.json()

# 6. Send notification (print or integrate with email/Slack/Telegram as needed)
def notify(symbol, rsi, macd, signal, order_resp):
    print(f"--- Trade Alert ---")
    print(f"Time: {datetime.now(timezone.utc)}")
    print(f"Symbol: {symbol}")
    print(f"RSI: {rsi:.2f}, MACD: {macd:.2f}")
    print(f"AI Signal: {signal}")
    print(f"Order Response: {order_resp}")

# 7. Main workflow
def main(symbol="AAPL", qty=5):
    if not is_market_open():
        print("Market is closed. Exiting.")
        return

    df = fetch_bars(symbol)
    if df is None or len(df) < 26:
        print("Not enough data to compute indicators.")
        return

    rsi, macd = calculate_indicators(df)
    signal = get_ai_signal(symbol, rsi, macd)

    # Only trade if signal is BUY or SELL
    if signal in ("BUY", "SELL"):
        # Optional: add price/quantity/risk checks here
        order_resp = place_order(symbol, qty, signal.lower())
    else:
        order_resp = "No trade executed."

    notify(symbol, rsi, macd, signal, order_resp)

# --- Run the workflow for AAPL and TSLA ---
if __name__ == "__main__":
    for ticker in ["AAPL", "TSLA"]:
        main(symbol=ticker, qty=5)
