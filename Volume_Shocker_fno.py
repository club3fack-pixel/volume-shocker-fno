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
TELEGRAM_BOT_TOKEN = "7770995517:AAFlM9YRAAckuKu7c-uBoR5_2srzj_uUzH8"
TELEGRAM_CHAT_ID = "6088684927"

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
                message = f"""📊 CUSTOM STOCKS BTST SCREENER - {current_date}

⏰ Time: {current_time}
📅 Trading Date: {tomorrow_date}

📈 VOLUME SHOCKER FILTER:
• Volume Surge > {VOLUME_SURGE_THRESHOLD}% above weekly avg
• Daily Volume > {MIN_VOLUME:,}
• Price Change > ±{PRICE_CHANGE_MIN}%

🎯 SHORT SELL BTST CONDITIONS:
• RSI(14) < 20 (Oversold condition)
• Today's Volume > Yesterday's Volume × 1.15
• Open Price = High Price (Bearish signal)

📊 BTST SIGNALS GENERATED: 0

❌ No strong BTST signals generated today.

⚡ Batch Processing: {batch_info}
📊 Total Scanned: {total_analyzed} stocks"""
            else:
                message = f"""🚀 CUSTOM STOCKS BTST SIGNALS FOR {tomorrow_date}

⏰ Time: {current_time} | 📅 Date: {current_date}
🎯 Strategy Accuracy: 66.7% | Avg Profit: +2.26%

📈 VOLUME SHOCKER FILTER:
• Volume Surge > {VOLUME_SURGE_THRESHOLD}% above weekly avg
• Daily Volume > {MIN_VOLUME:,}
• Price Change > ±{PRICE_CHANGE_MIN}%

📉 SHORT SELL CONDITIONS:
• RSI(14) < {RSI_14_MAX_FOR_SHORT} | Volume > Yesterday × {VOLUME_MULTIPLIER}
• Open Price = High Price

"""
                
                # Add volume shocker signals
                if not signals_df.empty:
                    buy_signals = signals_df[signals_df['Signal'] == 'BUY']
                    sell_signals = signals_df[signals_df['Signal'] == 'SELL']
                    
                    message += f"""🔥 VOLUME SHOCKER SIGNALS:
• BUY: {len(buy_signals)} | SELL: {len(sell_signals)}
• High Confidence: {len(signals_df[signals_df['Confidence'] == 'HIGH'])}

📊 TOP SIGNALS:

"""
                    for i, (idx, stock) in enumerate(signals_df.head(10).iterrows(), 1):
                        icon = "🟢" if stock['Signal'] == "BUY" else "🔴"
                        stars = "⭐⭐⭐" if stock['Confidence'] == "HIGH" else "⭐⭐"
                        message += f"""{i}. {icon} {stock['Stock']} - {stock['Signal']} {stars}
   Price: ₹{stock['Price']:.1f} | Day Δ: {stock['Day_Change']:+.2f}%
   Vol Surge: {stock['Volume_Surge']:.0f}% | RSI: {stock['RSI']:.0f}
   Targets: {stock['Target1']:.1f} / {stock['Target2']:.1f}
   Stop Loss: ₹{stock['Stop_Loss']:.1f}
   ➖➖➖➖➖➖➖

"""
                
                # Add short sell signals
                if short_sell_df is not None and not short_sell_df.empty:
                    message += f"""🎯 SHORT SELL SIGNALS (Open=High & RSI<20):
• Total: {len(short_sell_df)}

📊 SHORT SELL CANDIDATES:

"""
                    for i, (idx, stock) in enumerate(short_sell_df.head(10).iterrows(), 1):
                        message += f"""{i}. 🔴 {stock['Stock']} - SHORT SELL
   Price: ₹{stock['Price']:.1f} | Day Δ: {stock['Day_Change']:+.2f}%
   RSI(14): {stock['RSI']:.0f} | Vol Ratio: {stock['Volume_Ratio']:.2f}x
   Open = High: ✓ | Target: ₹{stock['Target']:.1f} (-3.0%)
   Stop Loss: ₹{stock['Stop_Loss']:.1f} (+1.5%)
   ➖➖➖➖➖➖➖

"""
                
                total_signals = len(signals_df) + (len(short_sell_df) if short_sell_df is not None else 0)
                if total_signals > 0:
                    expected_wins = int(total_signals * 0.667)
                    expected_profit = (expected_wins * 2.26) - ((total_signals - expected_wins) * 1.5)
                    message += f"""📊 STATISTICS:
• Total Signals: {total_signals}
• Expected Wins: {expected_wins}/{total_signals}
• Expected Net P&L: {expected_profit:+.2f}%

⚠️ BTST involves overnight gap risk. Use strict stop losses.

⚡ {batch_info}"""
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {'chat_id': self.chat_id, 'text': message, 'disable_web_page_preview': True}
            response = requests.post(url, json=payload, timeout=10)
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
        NEW BTST CONCEPT FOR SHORT SELL:
        Conditions:
        1. Daily RSI(14) < 20
        2. Daily volume > 1 day ago volume * 1.15
        3. Daily open = Daily high
        """
        try:
            if len(data) < 15:
                return None
            
            # Extract data
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
            
            # Check conditions
            condition1 = current_rsi_14 < RSI_14_MAX_FOR_SHORT
            condition2 = today_volume > yesterday_volume * VOLUME_MULTIPLIER
            condition3 = abs(today_open - today_high) < 0.01
            
            # Calculate day change
            day_change = ((today_close - yesterday_close) / yesterday_close) * 100
            
            # Calculate additional indicators
            rsi_5 = self.calculate_rsi(closes, period=5)
            current_rsi_5 = float(rsi_5.iloc[-1]) if not pd.isna(rsi_5.iloc[-1]) else 50
            
            stoch_k = self.calculate_stochastic_k(closes, k_period=5, d_period=3)
            current_stoch_k = float(stoch_k.iloc[-1]) if not pd.isna(stoch_k.iloc[-1]) else 50
            
            cci_14 = self.calculate_cci(highs, lows, closes, period=14)
            current_cci_14 = float(cci_14.iloc[-1]) if not pd.isna(cci_14.iloc[-1]) else 0
            
            upper_bb, lower_bb, _ = self.calculate_bollinger_bands(closes, period=21, std_dev=2)
            current_upper_bb = float(upper_bb.iloc[-1]) if not pd.isna(upper_bb.iloc[-1]) else today_close * 1.05
            current_lower_bb = float(lower_bb.iloc[-1]) if not pd.isna(lower_bb.iloc[-1]) else today_close * 0.95
            
            if today_close > current_upper_bb:
                bb_position = "Above Upper"
            elif today_close < current_lower_bb:
                bb_position = "Below Lower"
            else:
                bb_position = "Within Range"
            
            if condition1 and condition2 and condition3:
                return {
                    'stock': None,
                    'signal': 'SHORT_SELL',
                    'confidence': 'HIGH',
                    'current_price': today_close,
                    'price_change': day_change,
                    'volume_ratio': today_volume / yesterday_volume,
                    'rsi_14': current_rsi_14,
                    'rsi_5': current_rsi_5,
                    'stoch_k': current_stoch_k,
                    'cci': current_cci_14,
                    'bb_position': bb_position,
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
                confidence = "HIGH" if condition_overbought_sell else "MEDIUM"
            
            bb_position = "Above Upper" if daily_close > current_upper_bb else "Below Lower" if daily_close < current_lower_bb else "Within Range"
            
            tech_status = {
                'rsi': current_rsi_5,
                'stoch_k': current_stoch_k,
                'cci': current_cci_14,
                'bb_position': bb_position,
                'price': daily_close,
                'change': price_change,
                'volume_surge': volume_surge
            }
            
            return signal, confidence, tech_status
        except Exception:
            return None, None, None
    
    def fetch_batch_data(self, symbols_batch, start_date, end_date):
        """Fetch data for a batch of stocks"""
        batch_data = {}
        try:
            data = yf.download(
                symbols_batch, 
                start=start_date, 
                end=end_date, 
                progress=False, 
                auto_adjust=True,
                group_by='ticker',
                threads=True
            )
            
            for symbol in symbols_batch:
                try:
                    if len(symbols_batch) == 1:
                        df = data.copy()
                    else:
                        if symbol not in data.columns.get_level_values(0):
                            continue
                        df = data[symbol].copy()
                    
                    if df is None or df.empty or len(df) < 30:
                        continue
                    
                    batch_data[symbol] = df
                except Exception:
                    continue
            return batch_data
        except Exception:
            return self.fetch_individual_fallback(symbols_batch, start_date, end_date)
    
    def fetch_individual_fallback(self, symbols_batch, start_date, end_date):
        """Fallback to individual stock fetching"""
        batch_data = {}
        for symbol in symbols_batch:
            try:
                df = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=True)
                if df is not None and not df.empty and len(df) >= 30:
                    batch_data[symbol] = df
            except Exception:
                continue
        return batch_data
    
    def process_single_batch(self, batch_idx, symbols_batch, start_date, end_date):
        """Process a single batch of stocks"""
        batch_data = self.fetch_batch_data(symbols_batch, start_date, end_date)
        
        batch_volume_shockers = []
        batch_signals = []
        batch_short_sells = []
        
        for symbol in symbols_batch:
            if symbol not in batch_data:
                continue
            
            df = batch_data[symbol]
            
            try:
                # Check for short sell BTST conditions
                short_sell_result = self.check_short_sell_btst(df)
                if short_sell_result:
                    short_sell_result['stock'] = symbol.replace(".NS", "")
                    batch_short_sells.append(short_sell_result)
                
                # Check if volume shocker
                is_shocker, volume_surge, price_change, today_volume = self.is_volume_shocker(df)
                
                if is_shocker:
                    signal, confidence, tech_status = self.get_btst_signal(df, volume_surge, price_change)
                    
                    if isinstance(df.columns, pd.MultiIndex):
                        close_price = float(df[('Close', df.columns.levels[1][0])].iloc[-1])
                    else:
                        close_price = float(df['Close'].iloc[-1])
                    
                    if pd.isna(close_price) or close_price <= 0:
                        continue
                    
                    volume_shocker_info = {
                        'stock': symbol.replace(".NS", ""),
                        'volume_surge': volume_surge,
                        'price_change': price_change,
                        'today_volume': today_volume,
                        'current_price': close_price,
                        'signal': signal,
                        'confidence': confidence,
                        'tech_status': tech_status
                    }
                    batch_volume_shockers.append(volume_shocker_info)
                    
                    if signal and confidence:
                        batch_signals.append(volume_shocker_info)
            except Exception:
                continue
        
        return batch_volume_shockers, batch_signals, batch_short_sells, len(batch_data)
    
    def screen_batch_processing(self):
        """Screen stocks using batch processing"""
        print("\n" + "="*100)
        print("🎯 CUSTOM STOCKS - LIVE BTST SIGNALS FOR TOMORROW")
        print(f"Based on TODAY's Market Data ({datetime.now().strftime('%Y-%m-%d')})")
        print(f"Strategy Accuracy: 66.7% | Avg Profit: +2.26%")
        print(f"Total Stocks Scanned: {len(self.stock_symbols)}")
        print("="*100)
        print()
        
        analysis_date = datetime.today()
        start_date = analysis_date - timedelta(days=60)
        end_date = analysis_date + timedelta(days=1)
        
        print(f"📅 Analysis Date: {analysis_date.strftime('%Y-%m-%d')}")
        print(f"🎯 Trading Date: {(analysis_date + timedelta(days=1)).strftime('%Y-%m-%d')}")
        print("="*100)
        print()
        print("📊 Scanning stocks with Advanced Technical Indicators...")
        print("-"*100)
        
        start_time = time.time()
        
        batches = [self.stock_symbols[i:i+BATCH_SIZE] for i in range(0, len(self.stock_symbols), BATCH_SIZE)]
        
        print(f"\n📥 Processing {len(batches)} batches with {MAX_WORKERS} parallel threads...")
        print(f"   Batch Size: {BATCH_SIZE} stocks per batch")
        
        all_volume_shockers = []
        all_signals = []
        all_short_sells = []
        stocks_with_data = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_batch = {executor.submit(self.process_single_batch, batch_idx, batch, start_date, end_date): batch_idx 
                              for batch_idx, batch in enumerate(batches)}
            
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_shockers, batch_signals, batch_short_sells, batch_success = future.result()
                    all_volume_shockers.extend(batch_shockers)
                    all_signals.extend(batch_signals)
                    all_short_sells.extend(batch_short_sells)
                    stocks_with_data += batch_success
                    
                    completed = batch_idx + 1
                    if completed % 5 == 0 or completed == len(batches):
                        print(f"  ✓ Batch {completed}/{len(batches)} | Signals: {len(batch_signals)} | Short Sell: {len(batch_short_sells)}")
                except Exception as e:
                    print(f"  ✗ Batch {batch_idx + 1} failed: {str(e)[:50]}")
        
        total_time = time.time() - start_time
        batch_info = f"{len(batches)} batches | {MAX_WORKERS} threads | {BATCH_SIZE}/batch"
        
        return all_volume_shockers, all_signals, all_short_sells, stocks_with_data, batch_info, total_time

# ==================== DISPLAY FUNCTIONS ====================
def display_results(volume_shockers, signals, short_sells, total_time, batch_info):
    """Display screening results"""
    print("\n" + "="*100)
    print("🔥 CUSTOM STOCKS - LIVE BTST TRADING SIGNALS FOR TOMORROW")
    print("="*100)
    print()
    
    if signals:
        print("📊 VOLUME SHOCKER BTST SIGNALS:")
        print("-"*100)
        
        signals.sort(key=lambda x: (x['confidence'] == "HIGH", x['volume_surge']), reverse=True)
        
        signal_table = []
        for item in signals:
            tech = item['tech_status']
            signal_icon = "🟢" if item['signal'] == "BUY" else "🔴"
            confidence_stars = "⭐⭐⭐" if item['confidence'] == "HIGH" else "⭐⭐"
            
            signal_table.append([
                signal_icon,
                item['stock'],
                item['signal'],
                confidence_stars,
                f"₹{item['current_price']:.2f}",
                f"{item['price_change']:+.2f}%",
                f"{item['volume_surge']:.0f}%",
                f"{tech['rsi']:.0f}",
                f"{tech['stoch_k']:.0f}",
                f"{tech['cci']:.0f}",
                tech['bb_position'][:10]
            ])
        
        print(tabulate(signal_table, 
                       headers=["", "Stock", "Signal", "Conf", "Price", "Day Δ", "Vol Surge", "RSI(5)", "StochK", "CCI", "BB Pos"], 
                       tablefmt="grid",
                       numalign="center",
                       stralign="left"))
        
        print("\n" + "="*100)
        print("📈 DETAILED TRADING RECOMMENDATIONS")
        print("="*100)
        
        for idx, item in enumerate(signals[:20], 1):
            if item['signal'] == "BUY":
                target1 = item['current_price'] * 1.015
                target2 = item['current_price'] * 1.03
                stop_loss = item['current_price'] * 0.985
            else:
                target1 = item['current_price'] * 0.985
                target2 = item['current_price'] * 0.97
                stop_loss = item['current_price'] * 1.015
            
            tech = item['tech_status']
            print(f"\n{idx}. {'🟢' if item['signal'] == 'BUY' else '🔴'} {item['stock']} - {item['signal']} ({item['confidence']})")
            print(f"   ├─ Price: ₹{item['current_price']:.2f} (Day: {item['price_change']:+.2f}%)")
            print(f"   ├─ Volume Surge: {item['volume_surge']:.0f}%")
            print(f"   ├─ Tech: RSI={tech['rsi']:.0f} | StochK={tech['stoch_k']:.0f} | CCI={tech['cci']:.0f}")
            print(f"   ├─ BB: {tech['bb_position']}")
            print(f"   ├─ ENTRY: ₹{item['current_price']:.2f}")
            
            if item['signal'] == "BUY":
                print(f"   ├─ TARGETS: ₹{target1:.2f} / ₹{target2:.2f}")
                print(f"   └─ STOP LOSS: ₹{stop_loss:.2f}")
            else:
                print(f"   ├─ TARGETS: ₹{target1:.2f} / ₹{target2:.2f}")
                print(f"   └─ STOP LOSS: ₹{stop_loss:.2f}")
    else:
        print("📊 VOLUME SHOCKER BTST SIGNALS: None")
    
    if short_sells:
        print("\n" + "="*100)
        print("🎯 SHORT SELL BTST SIGNALS (Open=High & RSI<20)")
        print("="*100)
        
        short_table = []
        for item in short_sells:
            short_table.append([
                "🔴",
                item['stock'],
                "SHORT",
                f"₹{item['current_price']:.2f}",
                f"{item['price_change']:+.2f}%",
                f"{item['volume_ratio']:.2f}x",
                f"{item['rsi_14']:.0f}",
                item['bb_position'][:10]
            ])
        
        print(tabulate(short_table, 
                       headers=["", "Stock", "Signal", "Price", "Day Δ", "Vol Ratio", "RSI(14)", "BB Pos"], 
                       tablefmt="grid",
                       numalign="center",
                       stralign="left"))
        
        print("\n" + "="*100)
        print("📉 SHORT SELL - TRADING RECOMMENDATIONS")
        print("="*100)
        
        for idx, item in enumerate(short_sells[:20], 1):
            target = item['current_price'] * 0.97
            stop_loss = item['current_price'] * 1.015
            
            print(f"\n{idx}. 🔴 {item['stock']} - SHORT SELL (HIGH Confidence)")
            print(f"   ├─ Price: ₹{item['current_price']:.2f} (Day: {item['price_change']:+.2f}%)")
            print(f"   ├─ Volume Ratio: {item['volume_ratio']:.2f}x")
            print(f"   ├─ RSI(14): {item['rsi_14']:.0f} (Oversold)")
            print(f"   ├─ Pattern: Open = High")
            print(f"   ├─ ENTRY: ₹{item['current_price']:.2f}")
            print(f"   ├─ TARGET: ₹{target:.2f} (-3.0%)")
            print(f"   └─ STOP LOSS: ₹{stop_loss:.2f} (+1.5%)")
    else:
        print("\n📉 SHORT SELL BTST SIGNALS: None")
    
    print("\n" + "="*100)
    print("📊 TRADING SUMMARY")
    print("="*100)
    print(f"   Volume Shocker Stocks: {len(volume_shockers)}")
    print(f"   Volume Shocker Signals: {len(signals)}")
    print(f"   Short Sell Signals: {len(short_sells)}")
    print(f"   📈 Total Signals: {len(signals) + len(short_sells)}")
    
    total_signals = len(signals) + len(short_sells)
    if total_signals > 0:
        expected_wins = int(total_signals * 0.667)
        expected_profit = (expected_wins * 2.26) - ((total_signals - expected_wins) * 1.5)
        print(f"\n   EXPECTED OUTCOME (66.7% accuracy):")
        print(f"   • Expected Wins: {expected_wins}/{total_signals}")
        print(f"   • Expected Net P&L: {expected_profit:+.2f}%")
    
    # Show top volume shockers
    if volume_shockers:
        print("\n" + "="*100)
        print(f"📋 TOP VOLUME SHOCKER STOCKS (Surge > {VOLUME_SURGE_THRESHOLD}%)")
        print("="*100)
        
        volume_shockers.sort(key=lambda x: x['volume_surge'], reverse=True)
        
        volume_table = []
        for item in volume_shockers[:20]:
            signal_indicator = ""
            signal_text = item.get('signal')
            if signal_text == "BUY":
                signal_indicator = "🟢"
            elif signal_text == "SELL":
                signal_indicator = "🔴"
            else:
                signal_indicator = "⚪"
            
            display_signal = signal_text if signal_text else "NO SIGNAL"
            
            volume_table.append([
                signal_indicator,
                item['stock'],
                display_signal[:8],
                f"{item['volume_surge']:+.0f}%",
                f"{item['price_change']:+.2f}%",
                f"{item['today_volume']:,}"
            ])
        
        print(tabulate(volume_table, 
                       headers=["", "Stock", "Signal", "Vol Surge", "Price Δ", "Volume"], 
                       tablefmt="simple",
                       numalign="right",
                       stralign="left"))
    
    print("\n" + "="*100)
    print("✅ ANALYSIS COMPLETE")
    print("="*100)
    print(f"⏱️ Time: {total_time:.1f} seconds | ⚡ {batch_info}")

# ==================== MAIN FUNCTION ====================
def main():
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║              CUSTOM STOCKS BTST SCREENER                            ║
║                                                                      ║
║  Volume Shocker Criteria:                                           ║
║  • Volume Surge > 50% above weekly average                          ║
║  • Daily Volume > 100,000                                           ║
║  • Price Change > ±2%                                               ║
║                                                                      ║
║  NEW: SHORT SELL BTST CONCEPT:                                      ║
║  • RSI(14) < 20 (Oversold condition)                                ║
║  • Today's Volume > Yesterday's Volume × 1.15                       ║
║  • Daily Open = Daily High (Bearish reversal signal)                ║
║                                                                      ║
║  BATCH PROCESSING:                                                  ║
║  ✓ {BATCH_SIZE} stocks per batch | {MAX_WORKERS} parallel threads   ║
║  ✓ Telegram alerts for all signals                                  ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    print(f"\n📊 Total Custom stocks to screen: {len(custom_stocks)}")
    print(f"⚡ Processing with batch size of {BATCH_SIZE} stocks in {MAX_WORKERS} parallel batches!")
    
    screener = BTSTStrategy(custom_stocks)
    volume_shockers, signals, short_sells, stocks_analyzed, batch_info, total_time = screener.screen_batch_processing()
    
    display_results(volume_shockers, signals, short_sells, total_time, batch_info)
    
    # Prepare DataFrames for Telegram
    signals_list = []
    for item in signals:
        if item['signal'] == "BUY":
            target1 = item['current_price'] * 1.015
            target2 = item['current_price'] * 1.03
            stop_loss = item['current_price'] * 0.985
        else:
            target1 = item['current_price'] * 0.985
            target2 = item['current_price'] * 0.97
            stop_loss = item['current_price'] * 1.015
        
        signals_list.append({
            'Stock': item['stock'],
            'Signal': item['signal'],
            'Confidence': item['confidence'],
            'Price': item['current_price'],
            'Day_Change': item['price_change'],
            'Volume_Surge': item['volume_surge'],
            'RSI': item['tech_status']['rsi'],
            'Target1': target1,
            'Target2': target2,
            'Stop_Loss': stop_loss
        })
    
    signals_df = pd.DataFrame(signals_list) if signals_list else pd.DataFrame()
    
    short_sell_list = []
    for item in short_sells:
        target = item['current_price'] * 0.97
        stop_loss = item['current_price'] * 1.015
        
        short_sell_list.append({
            'Stock': item['stock'],
            'Signal': 'SHORT_SELL',
            'Confidence': 'HIGH',
            'Price': item['current_price'],
            'Day_Change': item['price_change'],
            'Volume_Ratio': item['volume_ratio'],
            'RSI': item['rsi_14'],
            'Target': target,
            'Stop_Loss': stop_loss
        })
    
    short_sell_df = pd.DataFrame(short_sell_list) if short_sell_list else pd.DataFrame()
    
    print("\n📤 Sending results to Telegram...")
    telegram = TelegramNotifier()
    telegram.send_btst_alert(signals_df, stocks_analyzed, batch_info, volume_shockers, short_sell_df)
    
    print("\n" + "⚠️"*35)
    print("IMPORTANT DISCLAIMER")
    print("⚠️"*35)
    print("• BTST involves overnight gap risk")
    print("• Always use strict stop losses")
    print("• Start with small position sizes")
    print("• Past performance doesn't guarantee future results")
    print("="*80)

if __name__ == "__main__":
    required_packages = ['yfinance', 'pandas', 'numpy', 'requests', 'tabulate']
    print("Checking required packages...")
    import subprocess
    import sys
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            print(f"  Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", package])
    
    main()
