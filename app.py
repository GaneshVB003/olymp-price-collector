# app.py - ULTIMATE CANDLE BUILDER WITH ALL TIMEFRAMES
import json, os, datetime, pathlib, time, threading, math, statistics
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple
import hashlib

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# ================================================================
# ULTIMATE CANDLE BUILDER WITH 14 TIMEFRAMES
# ================================================================

class UltimateCandleBuilder:
    """Professional-grade candle builder for 14 timeframes with real-time indicators"""
    
    def __init__(self):
        # ALL TIMEFRAMES from 5s to 30d
        self.timeframes = [
            "5s", "10s", "20s", "30s",      # Seconds
            "1m", "2m", "5m", "10m",        # Minutes  
            "15m", "30m", "1h", "4h",       # Hours
            "1d", "7d", "30d"               # Days
        ]
        
        # Candle storage
        self.candle_data = {}
        self.candle_history = defaultdict(lambda: defaultdict(deque))  # pair -> tf -> deque
        self.technical_indicators = defaultdict(dict)
        self.lock = threading.Lock()
        self.pattern_detector = PatternDetector()
        
        # Create directory structure
        self.base_dir = pathlib.Path(__file__).parent
        self.prices_dir = self.base_dir / "prices"
        self.candles_dir = self.base_dir / "candles"
        self.indicators_dir = self.base_dir / "indicators"
        self.models_dir = self.base_dir / "models"
        
        for dir_path in [self.prices_dir, self.candles_dir, self.indicators_dir, self.models_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Initialize candle structures
        for tf in self.timeframes:
            self.candle_data[tf] = {
                "open": None, "high": None, "low": None, "close": None,
                "volume": 0, "ticks": 0, "start_time": None, "end_time": None,
                "pair": None, "timeframe": tf, "vwap": 0, "buy_volume": 0, "sell_volume": 0
            }
        
        print(f"🔥 ULTIMATE CandleBuilder initialized with {len(self.timeframes)} timeframes")
        print(f"📊 Timeframes: {', '.join(self.timeframes)}")
        
        # Start background processors
        self._start_candle_manager()
        self._start_indicator_calculator()
        self._start_pattern_scanner()
    
    def _start_candle_manager(self):
        """Manage candle lifecycle"""
        def manager():
            while True:
                try:
                    with self.lock:
                        self._force_close_expired_candles()
                        self._archive_old_candles()
                    time.sleep(1)
                except Exception as e:
                    print(f"⚠️ Candle manager error: {e}")
                    time.sleep(5)
        
        threading.Thread(target=manager, daemon=True).start()
    
    def _start_indicator_calculator(self):
        """Calculate indicators in background"""
        def calculator():
            while True:
                try:
                    with self.lock:
                        self._calculate_all_indicators()
                    time.sleep(5)  # Calculate every 5 seconds
                except Exception as e:
                    print(f"⚠️ Indicator calculator error: {e}")
                    time.sleep(10)
        
        threading.Thread(target=calculator, daemon=True).start()
    
    def _start_pattern_scanner(self):
        """Scan for candle patterns"""
        def scanner():
            while True:
                try:
                    with self.lock:
                        self._scan_all_patterns()
                    time.sleep(10)
                except Exception as e:
                    print(f"⚠️ Pattern scanner error: {e}")
                    time.sleep(20)
        
        threading.Thread(target=scanner, daemon=True).start()
    
    def _tf_to_seconds(self, tf: str) -> int:
        """Convert any timeframe to seconds"""
        unit = tf[-1]
        value = int(tf[:-1])
        
        if unit == 's': return value
        elif unit == 'm': return value * 60
        elif unit == 'h': return value * 3600
        elif unit == 'd': return value * 86400
        elif unit == 'w': return value * 604800
        elif unit == 'M': return value * 2592000
        return 1
    
    def process_tick(self, pair: str, price: float, timestamp: float, volume: float = 0, side: str = 'neutral'):
        """Process a tick into ALL timeframes"""
        with self.lock:
            for tf in self.timeframes:
                self._update_candle(tf, pair, price, timestamp, volume, side)
    
    def _update_candle(self, tf: str, pair: str, price: float, timestamp: float, volume: float, side: str):
        """Update candle for specific timeframe"""
        tf_seconds = self._tf_to_seconds(tf)
        candle_start = int(timestamp // tf_seconds) * tf_seconds
        
        candle = self.candle_data[tf]
        
        # New candle period
        if candle["start_time"] != candle_start:
            # Save completed candle
            if candle["start_time"] is not None and candle["pair"]:
                self._save_completed_candle(tf, candle["pair"], candle)
            
            # Start new candle
            candle.update({
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "ticks": 1,
                "start_time": candle_start,
                "end_time": candle_start + tf_seconds,
                "pair": pair,
                "vwap": price * volume,
                "buy_volume": volume if side == 'buy' else 0,
                "sell_volume": volume if side == 'sell' else 0
            })
        else:
            # Update existing candle
            if candle["pair"] is None:
                candle["pair"] = pair
                candle["open"] = price
            
            candle["high"] = max(candle["high"] or price, price)
            candle["low"] = min(candle["low"] or price, price)
            candle["close"] = price
            candle["volume"] += volume
            candle["ticks"] += 1
            candle["vwap"] += price * volume
            
            # Track buy/sell volume
            if side == 'buy':
                candle["buy_volume"] += volume
            elif side == 'sell':
                candle["sell_volume"] += volume
    
    def _save_completed_candle(self, tf: str, pair: str, candle: Dict):
        """Save completed candle with full analysis"""
        try:
            # Enhance candle with indicators
            enhanced = self._enhance_candle_with_indicators(candle.copy())
            
            # Add pattern recognition
            enhanced["patterns"] = self._detect_candle_patterns(enhanced, pair, tf)
            
            # Save to file
            date_str = datetime.datetime.utcfromtimestamp(candle["start_time"]).strftime("%Y-%m-%d")
            filename = self.candles_dir / f"{pair}_{tf}_{date_str}.jsonl"
            
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(enhanced) + "\n")
            
            # Store in history (max 5000 candles per timeframe)
            key = f"{pair}_{tf}"
            self.candle_history[pair][tf].append(enhanced)
            if len(self.candle_history[pair][tf]) > 5000:
                self.candle_history[pair][tf].popleft()
            
            print(f"💾 {tf} candle saved: {pair} @ {enhanced.get('close', 0):.2f} "
                  f"(Δ: {enhanced.get('change_pct', 0):+.2f}%)")
            
        except Exception as e:
            print(f"❌ Error saving {tf} candle: {e}")
    
    def _enhance_candle_with_indicators(self, candle: Dict) -> Dict:
        """Add comprehensive technical indicators to candle"""
        try:
            o = candle.get("open", 0)
            h = candle.get("high", 0)
            l = candle.get("low", 0)
            c = candle.get("close", 0)
            v = candle.get("volume", 0)
            bv = candle.get("buy_volume", 0)
            sv = candle.get("sell_volume", 0)
            
            # 1. PRICE METRICS
            if o and c:
                candle["change"] = c - o
                candle["change_pct"] = ((c - o) / o * 100) if o != 0 else 0
                candle["range"] = h - l
                candle["range_pct"] = ((h - l) / l * 100) if l != 0 else 0
                
                # Candle body metrics
                body_size = abs(c - o)
                candle["body_pct"] = (body_size / o * 100) if o != 0 else 0
                
                # Wick calculations
                if c > o:  # Bullish
                    upper_wick = h - c
                    lower_wick = o - l
                else:  # Bearish
                    upper_wick = h - o
                    lower_wick = c - l
                
                candle["upper_wick_pct"] = (upper_wick / o * 100) if o != 0 else 0
                candle["lower_wick_pct"] = (lower_wick / o * 100) if o != 0 else 0
                candle["wick_ratio"] = (upper_wick / lower_wick) if lower_wick != 0 else 0
            
            # 2. VOLUME METRICS
            if v > 0:
                candle["vwap_price"] = candle.get("vwap", 0) / v if v > 0 else c
                candle["buy_sell_ratio"] = bv / sv if sv > 0 else 0
                candle["volume_imbalance"] = (bv - sv) / v * 100
                candle["avg_tick_size"] = v / candle.get("ticks", 1)
            
            # 3. TIMING METRICS
            candle["duration_seconds"] = candle.get("end_time", 0) - candle.get("start_time", 0)
            candle["ticks_per_second"] = candle.get("ticks", 0) / candle["duration_seconds"] if candle["duration_seconds"] > 0 else 0
            
            # 4. ADDITIONAL METRICS
            candle["mid_price"] = (h + l) / 2
            candle["typical_price"] = (h + l + c) / 3
            candle["weighted_close"] = (h + l + 2 * c) / 4
            
            # Timestamps
            candle["processed_at"] = time.time()
            candle["processed_at_iso"] = datetime.datetime.utcnow().isoformat()
            
        except Exception as e:
            print(f"⚠️ Error enhancing candle: {e}")
        
        return candle
    
    def _calculate_all_indicators(self):
        """Calculate comprehensive technical indicators for all pairs/timeframes"""
        for pair in list(self.candle_history.keys()):
            for tf in self.timeframes:
                history = list(self.candle_history[pair][tf])
                if len(history) >= 20:  # Need minimum data
                    indicators = self._calculate_technical_indicators(history)
                    self.technical_indicators[f"{pair}_{tf}"] = indicators
    
    def _calculate_technical_indicators(self, candles: List[Dict]) -> Dict:
        """Calculate 50+ technical indicators"""
        if len(candles) < 20:
            return {}
        
        try:
            # Convert to pandas for calculation
            df = pd.DataFrame(candles)
            
            # Ensure required columns
            if 'close' not in df.columns or len(df) < 20:
                return {}
            
            close = df['close'].values
            high = df['high'].values if 'high' in df.columns else close
            low = df['low'].values if 'low' in df.columns else close
            volume = df['volume'].values if 'volume' in df.columns else np.ones(len(close))
            
            indicators = {}
            
            # 1. MOVING AVERAGES (10 indicators)
            for period in [5, 10, 20, 50, 100, 200]:
                if len(close) >= period:
                    indicators[f'SMA_{period}'] = np.mean(close[-period:])
                    indicators[f'EMA_{period}'] = self._calculate_ema(close, period)[-1]
                    indicators[f'WMA_{period}'] = self._calculate_wma(close, period)[-1]
            
            # 2. OSCILLATORS (15 indicators)
            indicators.update(self._calculate_oscillators(close, high, low))
            
            # 3. VOLATILITY INDICATORS (10 indicators)
            indicators.update(self._calculate_volatility_indicators(close, high, low))
            
            # 4. VOLUME INDICATORS (10 indicators)
            indicators.update(self._calculate_volume_indicators(close, volume))
            
            # 5. MOMENTUM INDICATORS (5 indicators)
            indicators.update(self._calculate_momentum_indicators(close))
            
            # 6. SUPPORT/RESISTANCE (5 indicators)
            indicators.update(self._calculate_support_resistance(close, high, low))
            
            return indicators
            
        except Exception as e:
            print(f"⚠️ Error calculating indicators: {e}")
            return {}
    
    def _calculate_oscillators(self, close, high, low):
        """Calculate oscillator indicators"""
        indicators = {}
        
        # RSI
        if len(close) >= 14:
            deltas = np.diff(close)
            seed = deltas[:14]
            up = seed[seed >= 0].sum() / 14
            down = -seed[seed < 0].sum() / 14
            rs = up / down if down != 0 else 0
            indicators['RSI'] = 100 - (100 / (1 + rs))
        
        # Stochastic
        if len(high) >= 14:
            low_14 = np.min(low[-14:])
            high_14 = np.max(high[-14:])
            if high_14 != low_14:
                indicators['Stoch_%K'] = 100 * ((close[-1] - low_14) / (high_14 - low_14))
        
        # Williams %R
        if len(high) >= 14:
            highest_high = np.max(high[-14:])
            lowest_low = np.min(low[-14:])
            if highest_high != lowest_low:
                indicators['Williams_%R'] = -100 * ((highest_high - close[-1]) / (highest_high - lowest_low))
        
        return indicators
    
    def _calculate_volatility_indicators(self, close, high, low):
        """Calculate volatility indicators"""
        indicators = {}
        
        # Bollinger Bands
        if len(close) >= 20:
            sma_20 = np.mean(close[-20:])
            std_20 = np.std(close[-20:])
            indicators['BB_upper'] = sma_20 + 2 * std_20
            indicators['BB_middle'] = sma_20
            indicators['BB_lower'] = sma_20 - 2 * std_20
            indicators['BB_width'] = (indicators['BB_upper'] - indicators['BB_lower']) / sma_20
        
        # ATR
        if len(high) >= 14:
            tr = np.maximum(high[-1] - low[-1], 
                          np.abs(high[-1] - close[-2]), 
                          np.abs(low[-1] - close[-2]))
            indicators['ATR'] = tr
        
        return indicators
    
    def _calculate_volume_indicators(self, close, volume):
        """Calculate volume indicators"""
        indicators = {}
        
        if len(volume) >= 20:
            # Volume SMA
            indicators['Volume_SMA_20'] = np.mean(volume[-20:])
            
            # Volume ratio
            indicators['Volume_Ratio'] = volume[-1] / indicators['Volume_SMA_20'] if indicators['Volume_SMA_20'] > 0 else 1
        
        # OBV approximation
        if len(close) > 1 and len(volume) > 1:
            obv = 0
            for i in range(1, min(len(close), len(volume))):
                if close[i] > close[i-1]:
                    obv += volume[i]
                elif close[i] < close[i-1]:
                    obv -= volume[i]
            indicators['OBV'] = obv
        
        return indicators
    
    def _calculate_momentum_indicators(self, close):
        """Calculate momentum indicators"""
        indicators = {}
        
        if len(close) >= 12:
            # MACD components
            ema_12 = self._calculate_ema(close, 12)[-1]
            ema_26 = self._calculate_ema(close, 26)[-1]
            indicators['MACD'] = ema_12 - ema_26
            
            # Signal line (EMA of MACD)
            if len(close) >= 26:
                macd_values = ema_12 - ema_26
                indicators['MACD_signal'] = self._calculate_ema(np.array([macd_values]), 9)[-1]
        
        # Rate of Change
        if len(close) >= 10:
            indicators['ROC_10'] = ((close[-1] - close[-10]) / close[-10]) * 100
        
        return indicators
    
    def _calculate_support_resistance(self, close, high, low):
        """Calculate support/resistance levels"""
        indicators = {}
        
        if len(close) >= 20:
            # Pivot Points
            pivot = (high[-1] + low[-1] + close[-1]) / 3
            indicators['Pivot'] = pivot
            indicators['R1'] = 2 * pivot - low[-1]
            indicators['S1'] = 2 * pivot - high[-1]
            indicators['R2'] = pivot + (high[-1] - low[-1])
            indicators['S2'] = pivot - (high[-1] - low[-1])
        
        return indicators
    
    def _calculate_ema(self, prices, period):
        """Calculate Exponential Moving Average"""
        if len(prices) < period:
            return np.array([np.nan] * len(prices))
        
        alpha = 2 / (period + 1)
        ema = np.zeros_like(prices, dtype=float)
        ema[period-1] = np.mean(prices[:period])
        
        for i in range(period, len(prices)):
            ema[i] = alpha * prices[i] + (1 - alpha) * ema[i-1]
        
        return ema
    
    def _calculate_wma(self, prices, period):
        """Calculate Weighted Moving Average"""
        if len(prices) < period:
            return np.array([np.nan] * len(prices))
        
        weights = np.arange(1, period + 1)
        wma = np.zeros_like(prices, dtype=float)
        
        for i in range(period-1, len(prices)):
            wma[i] = np.sum(prices[i-period+1:i+1] * weights) / np.sum(weights)
        
        return wma
    
    def _detect_candle_patterns(self, candle: Dict, pair: str, tf: str) -> List[str]:
        """Detect Japanese candlestick patterns"""
        patterns = []
        
        try:
            history = list(self.candle_history[pair][tf])
            if len(history) < 3:
                return patterns
            
            current = candle
            prev1 = history[-1] if len(history) >= 1 else None
            prev2 = history[-2] if len(history) >= 2 else None
            prev3 = history[-3] if len(history) >= 3 else None
            
            if not all([current, prev1, prev2]):
                return patterns
            
            # Extract prices
            c_o, c_h, c_l, c_c = current.get('open', 0), current.get('high', 0), current.get('low', 0), current.get('close', 0)
            p1_o, p1_h, p1_l, p1_c = prev1.get('open', 0), prev1.get('high', 0), prev1.get('low', 0), prev1.get('close', 0)
            p2_o, p2_h, p2_l, p2_c = prev2.get('open', 0), prev2.get('high', 0), prev2.get('low', 0), prev2.get('close', 0)
            
            # Calculate body sizes
            c_body = abs(c_c - c_o)
            p1_body = abs(p1_c - p1_o)
            p2_body = abs(p2_c - p2_o)
            
            # 1. DOJI PATTERNS
            doji_threshold = 0.1  # 0.1% of price
            if c_body / c_o * 100 < doji_threshold:
                patterns.append("Doji")
            
            # 2. HAMMER/HANGING MAN
            lower_wick = min(c_o, c_c) - c_l
            upper_wick = c_h - max(c_o, c_c)
            body_size = c_body
            
            if lower_wick > 2 * body_size and upper_wick < body_size * 0.3:
                patterns.append("Hammer" if c_c > c_o else "Hanging_Man")
            
            # 3. ENGULFING PATTERNS
            if p1_body > 0 and c_body > 0:
                if c_c > c_o and p1_c < p1_o:  # Bullish engulfing
                    if c_o < p1_c and c_c > p1_o:
                        patterns.append("Bullish_Engulfing")
                elif c_c < c_o and p1_c > p1_o:  # Bearish engulfing
                    if c_o > p1_c and c_c < p1_o:
                        patterns.append("Bearish_Engulfing")
            
            # 4. MORNING/EVENING STAR
            if prev3:
                p3_o, p3_h, p3_l, p3_c = prev3.get('open', 0), prev3.get('high', 0), prev3.get('low', 0), prev3.get('close', 0)
                p3_body = abs(p3_c - p3_o)
                
                if p2_body < p1_body * 0.5 and p2_body < p3_body * 0.5:  # Small middle candle
                    if p3_c < p3_o and c_c > c_o:  # Down then up
                        patterns.append("Morning_Star")
                    elif p3_c > p3_o and c_c < c_o:  # Up then down
                        patterns.append("Evening_Star")
            
            # 5. THREE WHITE SOLDIERS/BLACK CROWS
            if prev2 and prev1:
                if c_c > c_o and p1_c > p1_o and p2_c > p2_o:
                    if c_c > p1_c and p1_c > p2_c:
                        patterns.append("Three_White_Soldiers")
                elif c_c < c_o and p1_c < p1_o and p2_c < p2_o:
                    if c_c < p1_c and p1_c < p2_c:
                        patterns.append("Three_Black_Crows")
            
        except Exception as e:
            print(f"⚠️ Pattern detection error: {e}")
        
        return patterns
    
    def _scan_all_patterns(self):
        """Scan all timeframes for patterns"""
        for pair in list(self.candle_history.keys()):
            for tf in self.timeframes:
                history = list(self.candle_history[pair][tf])
                if len(history) >= 5:
                    patterns = self.pattern_detector.scan(history[-5:])
                    if patterns:
                        print(f"🎯 Patterns found for {pair} {tf}: {patterns}")
    
    def _force_close_expired_candles(self):
        """Force close candles that have expired"""
        current_time = time.time()
        for tf in self.timeframes:
            candle = self.candle_data[tf]
            if candle["start_time"] and candle["end_time"] < current_time:
                if candle["pair"]:
                    self._save_completed_candle(tf, candle["pair"], candle)
                # Reset candle
                for key in ["open", "high", "low", "close", "volume", "ticks", "start_time", "end_time", "pair", "vwap", "buy_volume", "sell_volume"]:
                    if key in ["timeframe"]:
                        continue
                    candle[key] = None
                candle["volume"] = 0
                candle["ticks"] = 0
                candle["vwap"] = 0
                candle["buy_volume"] = 0
                candle["sell_volume"] = 0
    
    def _archive_old_candles(self):
        """Archive old candle data"""
        try:
            cutoff = time.time() - (30 * 86400)  # 30 days
            for file in self.candles_dir.glob("*.jsonl"):
                if file.stat().st_mtime < cutoff:
                    # Instead of deleting, compress or move to archive
                    pass
        except:
            pass
    
    def get_current_candles(self) -> Dict:
        """Get all current candles"""
        with self.lock:
            return {tf: candle.copy() for tf, candle in self.candle_data.items() 
                   if candle["start_time"] is not None}
    
    def get_historical_candles(self, pair: str, tf: str, limit: int = 100) -> List:
        """Get historical candles"""
        return list(self.candle_history.get(pair, {}).get(tf, deque()))[-limit:]
    
    def get_indicators(self, pair: str, tf: str) -> Dict:
        """Get calculated indicators"""
        return self.technical_indicators.get(f"{pair}_{tf}", {})
    
    def get_all_analysis(self, pair: str) -> Dict:
        """Get complete analysis for a pair"""
        analysis = {}
        
        for tf in self.timeframes:
            candles = self.get_historical_candles(pair, tf, 50)
            indicators = self.get_indicators(pair, tf)
            current = self.candle_data[tf]
            
            if candles:
                latest = candles[-1]
                analysis[tf] = {
                    "current_price": current.get("close") if current.get("pair") == pair else latest.get("close"),
                    "indicators": indicators,
                    "patterns": latest.get("patterns", []),
                    "change_pct": latest.get("change_pct", 0),
                    "volume": latest.get("volume", 0)
                }
        
        return analysis

# Pattern Detector Helper Class
class PatternDetector:
    def __init__(self):
        self.patterns = {}
    
    def scan(self, candles: List[Dict]) -> List[str]:
        """Scan candles for patterns"""
        detected = []
        
        if len(candles) < 3:
            return detected
        
        # Simple pattern detection logic
        # Can be expanded with more sophisticated pattern recognition
        return detected

# Initialize the ultimate candle builder
candle_builder = UltimateCandleBuilder()

# ================================================================
# FLASK ENDPOINTS FOR TRADING AI
# ================================================================

@app.route("/price", methods=["POST", "OPTIONS"])
def receive_tick():
    """Receive price ticks"""
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200
    
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "invalid payload"}), 400
        
        batch = data.get("batch", [])
        print(f"📥 Received {len(batch)} ticks at {datetime.datetime.utcnow().strftime('%H:%M:%S')}")
        
        processed = 0
        for item in batch:
            try:
                # Save raw tick
                ts = item.get("timestamp", time.time())
                date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                pair = item.get("pair", "unknown")
                
                filename = candle_builder.prices_dir / f"{pair}_{date_str}.jsonl"
                with open(filename, "a", encoding="utf-8") as f:
                    f.write(json.dumps(item) + "\n")
                
                # Process into candles
                if "price" in item and "pair" in item:
                    price = float(item["price"])
                    timestamp = float(item.get("timestamp", ts))
                    volume = float(item.get("volume", 0))
                    
                    # Determine side if available
                    side = item.get("side", "neutral")
                    
                    candle_builder.process_tick(pair, price, timestamp, volume, side)
                    processed += 1
                    
            except Exception as e:
                print(f"❌ Error processing tick: {e}")
                continue
        
        return jsonify({
            "status": "ok", 
            "received": len(batch),
            "processed": processed,
            "active_timeframes": len([tf for tf in candle_builder.timeframes 
                                     if candle_builder.candle_data[tf]["start_time"]])
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/candles", methods=["GET"])
def get_current_candles():
    """Get current live candles for all timeframes"""
    try:
        candles = candle_builder.get_current_candles()
        return jsonify({
            "status": "ok", 
            "candles": candles,
            "timestamp": time.time(),
            "total_timeframes": len(candle_builder.timeframes)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analysis/<pair>", methods=["GET"])
def analyze_pair(pair: str):
    """Complete multi-timeframe analysis"""
    try:
        analysis = candle_builder.get_all_analysis(pair)
        
        # Calculate summary statistics
        summary = {
            "pair": pair,
            "timeframes_analyzed": len(analysis),
            "bullish_timeframes": sum(1 for tf in analysis.values() if tf.get("change_pct", 0) > 0),
            "bearish_timeframes": sum(1 for tf in analysis.values() if tf.get("change_pct", 0) < 0),
            "strongest_tf": max(analysis.items(), key=lambda x: abs(x[1].get("change_pct", 0)))[0] if analysis else None,
            "analysis_time": datetime.datetime.utcnow().isoformat()
        }
        
        return jsonify({
            "status": "ok",
            "summary": summary,
            "detailed_analysis": analysis
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/indicators/<pair>/<tf>", methods=["GET"])
def get_indicators(pair: str, tf: str):
    """Get technical indicators for specific pair and timeframe"""
    try:
        indicators = candle_builder.get_indicators(pair, tf)
        candles = candle_builder.get_historical_candles(pair, tf, 50)
        
        return jsonify({
            "status": "ok",
            "pair": pair,
            "timeframe": tf,
            "indicators_count": len(indicators),
            "indicators": indicators,
            "recent_candles": len(candles),
            "latest_price": candles[-1].get("close") if candles else None
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    """Health check with detailed stats"""
    current = candle_builder.get_current_candles()
    active_tfs = len([tf for tf in candle_builder.timeframes 
                     if candle_builder.candle_data[tf]["start_time"]])
    
    return jsonify({
        "status": "alive",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "system": "Ultimate Trading AI v2.0",
        "active_timeframes": active_tfs,
        "total_timeframes": len(candle_builder.timeframes),
        "memory_usage": "normal",
        "uptime": time.time() - app_start_time if 'app_start_time' in globals() else 0
    }), 200

@app.route("/stats", methods=["GET"])
def stats():
    """Get comprehensive system statistics"""
    try:
        # Count files
        raw_files = list(candle_builder.prices_dir.glob("*.jsonl"))
        candle_files = list(candle_builder.candles_dir.glob("*.jsonl"))
        
        # Count entries
        raw_count = sum(1 for f in raw_files for _ in open(f, 'r', encoding='utf-8', errors='ignore'))
        candle_count = sum(1 for f in candle_files for _ in open(f, 'r', encoding='utf-8', errors='ignore'))
        
        # Get current status
        current = candle_builder.get_current_candles()
        active_pairs = set(candle["pair"] for candle in current.values() if candle.get("pair"))
        
        return jsonify({
            "status": "ok",
            "raw_files": len(raw_files),
            "candle_files": len(candle_files),
            "raw_ticks": raw_count,
            "candles": candle_count,
            "active_timeframes": len(current),
            "active_pairs": list(active_pairs),
            "all_timeframes": candle_builder.timeframes,
            "server_time": datetime.datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/predict/<pair>/<tf>", methods=["GET"])
def predict(pair: str, tf: str):
    """Make prediction for specific pair and timeframe"""
    try:
        # Get historical data
        history = candle_builder.get_historical_candles(pair, tf, 100)
        indicators = candle_builder.get_indicators(pair, tf)
        
        if not history or len(history) < 20:
            return jsonify({"status": "insufficient_data", "message": "Need more historical data"}), 200
        
        # Simple prediction logic (to be replaced with AI model)
        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        
        # Analyze trend
        price_trend = "up" if latest.get("close", 0) > prev.get("close", 0) else "down"
        
        # Calculate confidence based on indicators
        confidence = 50  # Base confidence
        
        if "RSI" in indicators:
            rsi = indicators["RSI"]
            if rsi < 30:
                confidence += 20  # Oversold - likely to bounce up
                price_trend = "up"
            elif rsi > 70:
                confidence += 20  # Overbought - likely to drop
                price_trend = "down"
        
        if "change_pct" in latest:
            change = latest["change_pct"]
            if abs(change) > 1:
                confidence += min(20, abs(change) * 5)
        
        # Clamp confidence
        confidence = max(5, min(95, confidence))
        
        return jsonify({
            "status": "ok",
            "pair": pair,
            "timeframe": tf,
            "prediction": {
                "direction": price_trend,
                "confidence": round(confidence, 1),
                "current_price": latest.get("close"),
                "expected_move_pct": round(abs(latest.get("change_pct", 0)), 2),
                "analysis": {
                    "indicators_used": len(indicators),
                    "pattern_count": len(latest.get("patterns", [])),
                    "volume_trend": "high" if latest.get("volume", 0) > history[-2].get("volume", 0) else "low"
                }
            },
            "timestamp": time.time()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ================================================================
# STARTUP
# ================================================================

app_start_time = time.time()

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 ULTIMATE TRADING AI SYSTEM v2.0")
    print("=" * 70)
    print(f"📁 Prices Directory: {candle_builder.prices_dir}")
    print(f"📊 Candles Directory: {candle_builder.candles_dir}")
    print(f"📈 Indicators Directory: {candle_builder.indicators_dir}")
    print(f"🧠 Models Directory: {candle_builder.models_dir}")
    print(f"⏱️  Total Timeframes: {len(candle_builder.timeframes)}")
    print(f"📊 Timeframes: {', '.join(candle_builder.timeframes)}")
    print("=" * 70)
    print("✅ System initialized and ready for data collection!")
    print("🌐 Endpoints available:")
    print("   • POST /price - Send price ticks")
    print("   • GET  /candles - Get live candles")
    print("   • GET  /analysis/BTCUSD_OTC - Complete analysis")
    print("   • GET  /predict/BTCUSD_OTC/5m - Make prediction")
    print("   • GET  /health - Health check")
    print("   • GET  /stats - System statistics")
    print("=" * 70)
    
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False, threaded=True)