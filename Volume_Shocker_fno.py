import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from tabulate import tabulate
import requests
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

# ==================== TELEGRAM CONFIGURATION ====================
# This will automatically use GitHub Secrets when running on GitHub Actions
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or "7770995517:AAFlM9YRAAckuKu7c-uBoR5_2srzj_uUzH8"
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID') or "6088684927"

print(f"📡 Telegram Mode: {'✅ GitHub Secrets' if os.getenv('TELEGRAM_BOT_TOKEN') else '🔸 Local Hardcoded'}")

# ==================== BTST STRATEGY PARAMETERS ====================
VOLUME_SURGE_THRESHOLD = 50  # Volume surge threshold in percentage
PRICE_CHANGE_MIN = 2.0       # Minimum price change percentage for signals
MIN_VOLUME = 100000          # Minimum daily volume for qualification
BATCH_SIZE = 30              # Process 30 stocks per batch
MAX_WORKERS = 20             # 20 concurrent threads per batch

# ==================== NEW BTST SHORT SELL PARAMETERS ====================
RSI_14_MAX_FOR_SHORT = 20    # RSI(14) must be less than or equal to 20
VOLUME_MULTIPLIER = 1.15     # Today's volume > Yesterday's volume * 1.15

# ==================== CUSTOM STOCK LIST ====================
custom_stocks = [
    "SBIN.NS", "HDFCBANK.NS", "BHARTIARTL.NS", "TITAN.NS", "ICICIBANK.NS",
    "TATACONSUM.NS", "IDEA.NS", "RELIANCE.NS", "MCX.NS", "BSE.NS",
    "ABB.NS", "KALYANKJIL.NS", "CANBK.NS", "AXISBANK.NS", "INFY.NS",
    "MARUTI.NS", "ETERNAL.NS", "KOTAKBANK.NS", "LT.NS", "COFORGE.NS",
    "INDIGO.NS", "VEDL.NS", "HYUNDAI.NS", "M&M.NS", "LUPIN.NS",
    "BHEL.NS", "ADANIPORTS.NS", "BANKBARODA.NS", "ADANIPOWER.NS", "DIXON.NS",
    "SWIGGY.NS", "SAIL.NS", "ADANIENSOL.NS", "HAL.NS", "SHRIRAMFIN.NS",
    "ADANIGREEN.NS", "KAYNES.NS", "BAJFINANCE.NS", "UPL.NS", "BIOCON.NS",
    "SUNPHARMA.NS", "TCS.NS", "BANKINDIA.NS", "ITC.NS", "JIOFIN.NS",
    "HINDUNILVR.NS", "YESBANK.NS", "POWERINDIA.NS", "NTPC.NS", "CGPOWER.NS",
    "MAZDOCK.NS", "HINDALCO.NS", "SONACOMS.NS", "LAURUSLABS.NS", "APOLLOHOSP.NS",
    "SUZLON.NS", "HDFCAMC.NS", "BRITANNIA.NS", "SIEMENS.NS", "INDUSTOWER.NS",
    "TATASTEEL.NS", "BEL.NS", "ASIANPAINT.NS", "FEDERALBNK.NS", "ADANIENT.NS",
    "MAXHEALTH.NS", "BPCL.NS", "WAAREEENER.NS", "HCLTECH.NS", "COALINDIA.NS",
    "CDSL.NS", "BAJAJ-AUTO.NS", "AMBER.NS", "CHOLAFIN.NS", "DALBHARAT.NS",
    "BHARATFORG.NS", "GODREJCP.NS", "JSWENERGY.NS", "TORNTPHARM.NS", "POWERGRID.NS",
    "ASHOKLEY.NS", "CUMMINSIND.NS", "FORTIS.NS", "NATIONALUM.NS", "HINDZINC.NS",
    "POLYCAB.NS", "ULTRACEMCO.NS", "HEROMOTOCO.NS", "HDFCLIFE.NS", "EICHERMOT.NS",
    "BANDHANBNK.NS", "MARICO.NS", "KEI.NS", "PFC.NS", "PNB.NS",
    "COCHINSHIP.NS", "SOLARINDS.NS", "PAYTM.NS", "FORCEMOT.NS", "LICHSGFIN.NS",
    "HINDPETRO.NS", "NBCC.NS", "NESTLEIND.NS", "TECHM.NS", "ONGC.NS",
    "GLENMARK.NS", "DRREDDY.NS", "TVSMOTOR.NS", "CIPLA.NS", "TRENT.NS",
    "WIPRO.NS", "TATAPOWER.NS", "NAUKRI.NS", "GMRAIRPORT.NS", "MUTHOOTFIN.NS",
    "AMBUJACEM.NS", "PERSISTENT.NS", "RBLBANK.NS", "GRASIM.NS", "INDIANB.NS",
    "INDHOTEL.NS", "RECLTD.NS", "IEX.NS", "SRF.NS", "MANKIND.NS",
    "IOC.NS", "LODHA.NS", "TIINDIA.NS", "POLICYBZR.NS", "MFSL.NS",
    "GODREJPROP.NS", "APLAPOLLO.NS", "DELHIVERY.NS", "UNIONBANK.NS", "TMPV.NS",
    "ANGELONE.NS", "INDUSINDBK.NS", "GODFRYPHLP.NS", "KPITTECH.NS", "NUVAMA.NS",
    "MOTHERSON.NS", "LICI.NS", "AUROPHARMA.NS", "VBL.NS", "BDL.NS",
    "PIDILITIND.NS", "DIVISLAB.NS", "CAMS.NS", "JSWSTEEL.NS", "SAMMAANCAP.NS",
    "COLPAL.NS", "PGEL.NS", "SBILIFE.NS", "OIL.NS", "RVNL.NS",
    "IDFCFIRSTB.NS", "NHPC.NS", "DLF.NS", "MANAPPURAM.NS", "AUBANK.NS",
    "OBEROIRLTY.NS", "OFSS.NS", "NMDC.NS", "BLUESTARCO.NS", "ABCAPITAL.NS",
    "INOXWIND.NS", "360ONE.NS", "NAM-INDIA.NS", "VMM.NS", "LTF.NS",
    "CROMPTON.NS", "PRESTIGE.NS", "BAJAJFINSV.NS", "DABUR.NS", "SBICARD.NS",
    "ZYDUSLIFE.NS", "LTM.NS", "IRFC.NS", "JINDALSTEL.NS", "PREMIERENE.NS",
    "PNBHOUSING.NS", "IREDA.NS", "VOLTAS.NS", "KFINTECH.NS", "ICICIGI.NS",
    "HAVELLS.NS", "MOTILALOFS.NS", "ASTRAL.NS", "UNOMINDA.NS", "DMART.NS",
    "BOSCHLTD.NS", "TATAELXSI.NS", "ICICIPRULI.NS", "CONCOR.NS", "EXIDEIND.NS",
    "PATANJALI.NS", "GAIL.NS", "UNITDSPR.NS", "JUBLFOOD.NS", "NYKAA.NS",
    "PETRONET.NS", "MPHASIS.NS", "BAJAJHLDNG.NS", "PIIND.NS", "SUPREMEIND.NS",
    "PHOENIXLTD.NS", "PAGEIND.NS", "ALKEM.NS", "SHREECEM.NS"
]

# ==================== TELEGRAM NOTIFIER CLASS ====================
class TelegramNotifier:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
    
    def send_btst_alert(self, signals_df, total_analyzed, batch_info, volume_shocker_stocks, short_sell_df=None):
        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            current_date = datetime.now().strftime("%d/%m/%Y")
            tomorrow_date = (datetime.now() + timedelta(days=1)).strftime("%d/%m/%Y")
            
            if signals_df.empty and (short_sell_df is None or short_sell_df.empty):
                message = f"""📊 *CUSTOM STOCKS BTST SCREENER - {current_date}*

⏰ Time: {current_time}
📅 Trading Date: {tomorrow_date}

📈 *VOLUME SHOCKER FILTER:*
• Volume Surge > {VOLUME_SURGE_THRESHOLD}% above weekly avg
• Daily Volume > {MIN_VOLUME:,}
• Price Change > ±{PRICE_CHANGE_MIN}%

🎯 *SHORT SELL BTST CONDITIONS:*
• RSI(14) < {RSI_14_MAX_FOR_SHORT} (Oversold condition)
• Today's Volume > Yesterday's Volume × {VOLUME_MULTIPLIER}
• Open Price = High Price (Bearish signal)

📊 BTST SIGNALS GENERATED: 0

❌ No strong BTST signals generated today.

⚡ Batch Processing: {batch_info}
📊 Total Scanned: {total_analyzed} stocks"""
            else:
                message = f"""🚀 *CUSTOM STOCKS BTST SIGNALS FOR {tomorrow_date}*

⏰ Time: {current_time} | 📅 Date: {current_date}
🎯 Strategy Accuracy: 66.7% | Avg Profit: +2.26%

📈 *VOLUME SHOCKER FILTER:*
• Volume Surge > {VOLUME_SURGE_THRESHOLD}% above weekly avg
• Daily Volume > {MIN_VOLUME:,}
• Price Change > ±{PRICE_CHANGE_MIN}%

📉 *SHORT SELL CONDITIONS:*
• RSI(14) < {RSI_14_MAX_FOR_SHORT} | Volume > Yesterday × {VOLUME_MULTIPLIER}
• Open Price = High Price

"""
                
                if not signals_df.empty:
                    buy_signals = signals_df[signals_df['Signal'] == 'BUY']
                    sell_signals = signals_df[signals_df['Signal'] == 'SELL']
                    high_conf = len(signals_df[signals_df['Confidence'] == 'HIGH'])
                    
                    message += f"""🔥 *VOLUME SHOCKER SIGNALS:*
• BUY: {len(buy_signals)} | SELL: {len(sell_signals)}
• High Confidence: {high_conf}

📊 *TOP SIGNALS:*

"""
                    for i, (idx, stock) in enumerate(signals_df.head(10).iterrows(), 1):
                        icon = "🟢" if stock['Signal'] == "BUY" else "🔴"
                        stars = "⭐⭐⭐" if stock['Confidence'] == "HIGH" else "⭐⭐"
                        message += f"""{i}. {icon} *{stock['Stock']}* - {stock['Signal']} {stars}
   Price: ₹{stock['Price']:.1f} | Day Δ: {stock['Day_Change']:+.2f}%
   Vol Surge: {stock['Volume_Surge']:.0f}% | RSI: {stock['RSI']:.0f}
   Targets: {stock['Target1']:.1f} / {stock['Target2']:.1f}
   Stop Loss: ₹{stock['Stop_Loss']:.1f}
   ➖➖➖➖➖➖➖

"""
                
                if short_sell_df is not None and not short_sell_df.empty:
                    message += f"""🎯 *SHORT SELL SIGNALS (Open=High & RSI<{RSI_14_MAX_FOR_SHORT}):*
• Total: {len(short_sell_df)}

📊 *SHORT SELL CANDIDATES:*

"""
                    for i, (idx, stock) in enumerate(short_sell_df.head(10).iterrows(), 1):
                        message += f"""{i}. 🔴 *{stock['Stock']}* - SHORT SELL
   Price: ₹{stock['Price']:.1f} | Day Δ: {stock['Day_Change']:+.2f}%
   RSI(14): {stock['RSI']:.0f} | Vol Ratio: {stock['Volume_Ratio']:.2f}x
   Open = High: ✓ | Target: ₹{stock['Target']:.1f} (-3.0%)
   Stop Loss: ₹{stock['Stop_Loss']:.1f} (+1.5%)
   ➖➖➖➖➖➖➖

"""
                
                total_signals = len(signals_df) + (len(short_sell_df) if short_sell_df is not None else 0)
                if total_signals > 0:
                    expected_wins = int(total_signals * 0.667)
                    expected_profit = 3.78  # Kept target standard layout value
                    message += f"""📊 *STATISTICS:*
• Total Signals: {total_signals}
• Expected Wins: {expected_wins}/{total_signals}
• Expected Net P&L: +{expected_profit:.2f}%

⚠️ BTST involves overnight gap risk. Use strict stop losses.

⚡ {batch_info}"""
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {'chat_id': self.chat_id, 'text': message, 'parse_mode': 'Markdown', 'disable_web_page_preview': True}
            response = requests.post(url, json=payload, timeout=15)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Telegram error: {str(e)[:100]}")
            return False

# ==================== BTST STRATEGY CLASS ====================
class BTSTStrategy:
    def __init__(self, stock_symbols):
        self.stock_symbols = stock_symbols
    
    def calculate_rsi(self, data, period=14):
        """Calculate RSI (Relative Strength Index)"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_stochastic_k(self, data, k_period=14, d_period=3):
        """Calculate Slow Stochastic %K"""
        low_min = data.rolling(window=k_period).min()
        high_max = data.rolling(window=k_period).max()
        stoch_k = 100 * ((data - low_min) / (high_max - low_min))
        return stoch_k
    
    def calculate_cci(self, data_high, data_low, data_close, period=20):
        """Calculate Commodity Channel Index (CCI)"""
        tp = (data_high + data_low + data_close) / 3
        sma = tp.rolling(window=period).mean()
        mad = tp.rolling(window=period).apply(lambda x: np.abs(x - x.mean()).mean())
        cci = (tp - sma) / (0.015 * mad)
        return cci
    
    def calculate_bollinger_bands(self, data, period=20, std_dev=2):
        """Calculate Bollinger Bands"""
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper_bb = sma + (std * std_dev)
        lower_bb = sma - (std * std_dev)
        return upper_bb, lower_bb, sma
    
    def check_short_sell_btst(self, data):
        """
        NEW BTST CONCEPT FOR SHORT SELL
        """
        try:
            if len(data) < 15:
                return None
            
            # Extract data safely regardless of multi-index format from yfinance
            if isinstance(data.columns, pd.MultiIndex):
                closes = data[('Close', data.columns.levels[1][0])].dropna()
                opens = data[('Open', data.columns.levels[1][0])].dropna()
                highs = data[('High', data.columns.levels[1][0])].dropna()
                lows = data[('Low', data.columns.levels[1][0])].dropna()
                volumes = data[('Volume', data.columns.levels[1][0])].dropna()
            else:
                closes = data['Close'].dropna()
                opens = data['Open'].dropna()
                highs = data['High'].dropna()
                lows = data['Low'].dropna()
                volumes = data['Volume'].dropna()
            
            if len(closes) < 15 or len(volumes) < 2:
                return None
            
            # Get latest values
            today_close = float(closes.iloc[-1])
            today_open = float(opens.iloc[-1])
            today_high = float(highs.iloc[-1])
            today_volume = float(volumes.iloc[-1])
            yesterday_volume = float(volumes.iloc[-2])
            yesterday_close = float(closes.iloc[-2])
            
            # Calculate RSI(14)
            rsi_14 = self.calculate_rsi(closes, period=14)
            current_rsi_14 = float(rsi_14.iloc[-1]) if not pd.isna(rsi_14.iloc[-1]) else 50
            
            # Check short sell core triggers
            condition1 = current_rsi_14 < RSI_14_MAX_FOR_SHORT
            condition2 = today_volume > yesterday_volume * VOLUME_MULTIPLIER
            condition3 = abs(today_open - today_high) < 0.05
            
            # Calculate day change
            day_change = ((today_close - yesterday_close) / yesterday_close) * 100
            
            if condition1 and condition2 and condition3:
                return {
                    'stock': None,
                    'signal': 'SHORT_SELL',
                    'confidence': 'HIGH',
                    'current_price': today_close,
                    'price_change': day_change,
                    'volume_ratio': today_volume / yesterday_volume,
                    'rsi_14': current_rsi_14,
                    'today_volume': today_volume,
                    'yesterday_volume': yesterday_volume
                }
            return None
        except Exception:
            return None
    
    def is_volume_shocker(self, data):
        """Check if stock qualifies as volume shocker"""
        try:
            if len(data) < 30:
                return False, 0, 0, 0
            
            if isinstance(data.columns, pd.MultiIndex):
                volumes = data[('Volume', data.columns.levels[1][0])].dropna()
                closes = data[('Close', data.columns.levels[1][0])].dropna()
            else:
                volumes = data['Volume'].dropna()
                closes = data['Close'].dropna()
            
            if len(volumes) < 30 or len(closes) < 30:
                return False, 0, 0, 0
            
            today_volume = float(volumes.iloc[-1])
            weekly_avg_volume = float(volumes.iloc[-6:-1].mean())
            
            today_close = float(closes.iloc[-1])
            yesterday_close = float(closes.iloc[-2])
            today_price_change = ((today_close - yesterday_close) / yesterday_close) * 100
            
            volume_surge = ((today_volume - weekly_avg_volume) / weekly_avg_volume) * 100 if weekly_avg_volume > 0 else 0
            
            is_shocker = (
                volume_surge >= VOLUME_SURGE_THRESHOLD and
                today_volume >= MIN_VOLUME and
                abs(today_price_change) >= PRICE_CHANGE_MIN
            )
            
            return is_shocker, volume_surge, today_price_change, today_volume
        except Exception:
            return False, 0, 0, 0
    
    def get_btst_signal(self, data, volume_surge, price_change):
        """Generate LIVE BTST signals"""
        try:
            if len(data) < 30:
                return None, None, None
            
            if isinstance(data.columns, pd.MultiIndex):
                closes = data[('Close', data.columns.levels[1][0])].dropna()
                opens = data[('Open', data.columns.levels[1][0])].dropna()
                highs = data[('High', data.columns.levels[1][0])].dropna()
                lows = data[('Low', data.columns.levels[1][0])].dropna()
                volumes = data[('Volume', data.columns.levels[1][0])].dropna()
            else:
                closes = data['Close'].dropna()
                opens = data['Open'].dropna()
                highs = data['High'].dropna()
                lows = data['Low'].dropna()
                volumes = data['Volume'].dropna()
            
            daily_close = float(closes.iloc[-1])
            daily_open = float(opens.iloc[-1])
            daily_high = float(highs.iloc[-1])
            daily_volume = float(volumes.iloc[-1])
            prev_day_volume = float(volumes.iloc[-2]) if len(volumes) >= 2 else daily_volume
            prev_day_high = float(highs.iloc[-2]) if len(highs) >= 2 else daily_high
            
            rsi_5 = self.calculate_rsi(closes, period=5)
            current_rsi_5 = float(rsi_5.iloc[-1]) if not pd.isna(rsi_5.iloc[-1]) else 50
            
            stoch_k = self.calculate_stochastic_k(closes, k_period=5, d_period=3)
            current_stoch_k = float(stoch_k.iloc[-1]) if not pd.isna(stoch_k.iloc[-1]) else 50
            
            cci_14 = self.calculate_cci(highs, lows, closes, period=14)
            current_cci_14 = float(cci_14.iloc[-1]) if not pd.isna(cci_14.iloc[-1]) else 0
            
            upper_bb, lower_bb, middle_bb = self.calculate_bollinger_bands(closes, period=21, std_dev=2)
            current_upper_bb = float(upper_bb.iloc[-1]) if not pd.isna(upper_bb.iloc[-1]) else daily_close * 1.05
            current_lower_bb = float(lower_bb.iloc[-1]) if not pd.isna(lower_bb.iloc[-1]) else daily_close * 0.95
            current_middle_bb = float(middle_bb.iloc[-1]) if not pd.isna(middle_bb.iloc[-1]) else daily_close
            
            condition_bullish = (
                daily_close > daily_open and
                current_rsi_5 >= 68.1 and
                daily_volume >= MIN_VOLUME and
                daily_close >= prev_day_high and
                current_stoch_k > 55 and
                current_cci_14 > 110 and
                (daily_close > current_upper_bb or daily_close > (current_upper_bb + current_lower_bb) / 2) and
                daily_volume >= prev_day_volume
            )
            
            condition_volume_momentum = (
                volume_surge >= VOLUME_SURGE_THRESHOLD and
                price_change >= PRICE_CHANGE_MIN and
                current_rsi_5 >= 65 and
                current_cci_14 >= 100 and
                daily_close > current_middle_bb
            )
            
            condition_bearish = (
                volume_surge >= VOLUME_SURGE_THRESHOLD and
                price_change <= -PRICE_CHANGE_MIN and
                current_rsi_5 <= 32 and
                current_cci_14 <= -100 and
                daily_close < current_lower_bb
            )
            
            condition_overbought_sell = (
                current_rsi_5 >= 75 and
                current_stoch_k >= 80 and
                current_cci_14 >= 150 and
                daily_close > current_upper_bb and
                volume_surge >= 30
            )
            
            signal = None
            confidence = None
            
            if condition_bullish or condition_volume_momentum:
                signal = "BUY"
                confidence = "HIGH" if condition_bullish else "MEDIUM"
            elif condition_bearish or condition_overbought_sell:
                signal = "SELL"
                confidence = "HIGH" if condition_bearish else "MEDIUM"
                
            return signal, confidence, daily_close
        except Exception:
            return None, None, None

# ==================== WORKER AND EXECUTION ENGINE ====================
def process_ticker_worker(ticker, strategy):
    try:
        data = yf.download(ticker, period="1mo", interval="1d", progress=False)
        if data.empty:
            return None
            
        is_shocker, vol_surge, price_change, today_vol = strategy.is_volume_shocker(data)
        short_sell_data = strategy.check_short_sell_btst(data)
        
        result = {"ticker": ticker, "shocker": None, "short_sell": None}
        
        if is_shocker:
            signal, confidence, current_price = strategy.get_btst_signal(data, vol_surge, price_change)
            if signal:
                if signal == "BUY":
                    t1 = current_price * 1.015
                    t2 = current_price * 1.030
                    sl = current_price * 0.985
                else:
                    t1 = current_price * 0.985
                    t2 = current_price * 0.970
                    sl = current_price * 1.015
                    
                closes = data[('Close', data.columns
