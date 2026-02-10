# app.py - COMPLETE with integrated Candle Builder
import json, os, datetime, pathlib, time, threading
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS
from collections import defaultdict
from typing import Dict, List, Optional

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# ================================================================
# INTEGRATED CANDLE BUILDER
# ================================================================

class IntegratedCandleBuilder:
    """Built-in candle builder that processes ticks into multiple timeframes"""
    
    def __init__(self, timeframes: List[str] = ["1s", "5s", "1m", "5m", "15m", "1h"]):
        self.timeframes = timeframes
        self.candle_data = {}
        self.lock = threading.Lock()
        self.candle_history = defaultdict(list)  # Store historical candles
        
        # Create directories
        self.base_dir = pathlib.Path(__file__).parent
        self.prices_dir = self.base_dir / "prices"
        self.candles_dir = self.base_dir / "candles"
        self.prices_dir.mkdir(exist_ok=True)
        self.candles_dir.mkdir(exist_ok=True)
        
        # Initialize candle structure for each timeframe
        for tf in timeframes:
            self.candle_data[tf] = {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": 0,
                "ticks": 0,
                "start_time": None,
                "end_time": None,
                "pair": None,
                "timeframe": tf
            }
        
        print(f"🔥 IntegratedCandleBuilder initialized with {len(timeframes)} timeframes")
        
        # Start background processing
        self._start_processor()
    
    def _start_processor(self):
        """Start background thread for candle management"""
        def processor():
            while True:
                try:
                    with self.lock:
                        self._check_stale_candles()
                        self._cleanup_old_data()
                    time.sleep(2)
                except Exception as e:
                    print(f"⚠️ Processor error: {e}")
                    time.sleep(5)
        
        thread = threading.Thread(target=processor, daemon=True)
        thread.start()
    
    def _tf_to_seconds(self, tf: str) -> int:
        """Convert timeframe to seconds"""
        unit = tf[-1]
        value = int(tf[:-1])
        if unit == 's': return value
        if unit == 'm': return value * 60
        if unit == 'h': return value * 3600
        if unit == 'd': return value * 86400
        return 1
    
    def process_tick(self, pair: str, price: float, timestamp: float, volume: float = 0):
        """Process a tick into all timeframes"""
        with self.lock:
            for tf in self.timeframes:
                self._update_candle(tf, pair, price, timestamp, volume)
    
    def _update_candle(self, tf: str, pair: str, price: float, timestamp: float, volume: float):
        """Update or create a candle for given timeframe"""
        tf_seconds = self._tf_to_seconds(tf)
        candle_start = int(timestamp // tf_seconds) * tf_seconds
        
        candle = self.candle_data[tf]
        
        # Check if we need to start new candle
        if candle["start_time"] != candle_start:
            # Save old candle if exists
            if candle["start_time"] is not None and candle["pair"]:
                self._save_candle(tf, candle["pair"], candle)
                # Store in history (keep last 1000 candles)
                key = f"{candle['pair']}_{tf}"
                self.candle_history[key].append(candle.copy())
                if len(self.candle_history[key]) > 1000:
                    self.candle_history[key].pop(0)
            
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
                "pair": pair
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
    
    def _save_candle(self, tf: str, pair: str, candle: Dict):
        """Save completed candle to file"""
        try:
            # Add indicators
            enhanced = self._enhance_candle(candle.copy())
            
            # Save to file
            date_str = datetime.datetime.utcfromtimestamp(candle["start_time"]).strftime("%Y-%m-%d")
            filename = self.candles_dir / f"{pair}_{tf}_{date_str}.jsonl"
            
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(enhanced) + "\n")
            
            print(f"💾 Saved {tf} candle: {pair} @ {enhanced.get('close', 0):.2f}")
            
        except Exception as e:
            print(f"❌ Error saving candle: {e}")
    
    def _enhance_candle(self, candle: Dict) -> Dict:
        """Add basic technical indicators to candle"""
        try:
            # Calculate basic metrics
            if candle.get("open") and candle.get("close"):
                open_price = candle["open"]
                close_price = candle["close"]
                high_price = candle.get("high", close_price)
                low_price = candle.get("low", close_price)
                
                # Price change
                candle["change_pct"] = ((close_price - open_price) / open_price * 100) if open_price != 0 else 0
                
                # Body size
                candle["body_pct"] = (abs(close_price - open_price) / open_price * 100) if open_price != 0 else 0
                
                # Upper/Lower wick
                if close_price > open_price:  # Bullish
                    candle["upper_wick"] = (high_price - close_price) / open_price * 100 if open_price != 0 else 0
                    candle["lower_wick"] = (open_price - low_price) / open_price * 100 if open_price != 0 else 0
                else:  # Bearish
                    candle["upper_wick"] = (high_price - open_price) / open_price * 100 if open_price != 0 else 0
                    candle["lower_wick"] = (close_price - low_price) / open_price * 100 if open_price != 0 else 0
                
                # Is doji?
                candle["is_doji"] = candle["body_pct"] < 0.1  # Less than 0.1% body
                
                # Volume metrics
                volume = candle.get("volume", 0)
                ticks = candle.get("ticks", 1)
                candle["avg_tick_volume"] = volume / ticks if ticks > 0 else 0
                
                # Add timestamp
                candle["saved_at"] = time.time()
                candle["saved_at_iso"] = datetime.datetime.utcnow().isoformat()
                
        except Exception as e:
            print(f"⚠️ Error enhancing candle: {e}")
        
        return candle
    
    def _check_stale_candles(self):
        """Force close candles that should have ended"""
        current_time = time.time()
        for tf in self.timeframes:
            candle = self.candle_data[tf]
            if candle["start_time"] and candle["end_time"] < current_time:
                if candle["pair"]:
                    self._save_candle(tf, candle["pair"], candle)
                # Reset candle
                for key in ["open", "high", "low", "close", "volume", "ticks", "start_time", "end_time", "pair"]:
                    if key in ["timeframe", "pair"]:
                        continue
                    candle[key] = None
                candle["volume"] = 0
                candle["ticks"] = 0
    
    def _cleanup_old_data(self):
        """Clean up old data files (keep last 7 days)"""
        try:
            cutoff = time.time() - (7 * 86400)  # 7 days
            for file in self.candles_dir.glob("*.jsonl"):
                if file.stat().st_mtime < cutoff:
                    file.unlink()
                    print(f"🗑️  Cleaned up old file: {file.name}")
        except:
            pass
    
    def get_current_candles(self) -> Dict:
        """Get all current (in-progress) candles"""
        with self.lock:
            current = {}
            for tf, candle in self.candle_data.items():
                if candle["start_time"] is not None:
                    current[tf] = candle.copy()
            return current
    
    def get_historical_candles(self, pair: str, tf: str, limit: int = 100) -> List:
        """Get historical candles for analysis"""
        key = f"{pair}_{tf}"
        return self.candle_history.get(key, [])[-limit:]
    
    def calculate_indicators(self, candles: List[Dict]) -> Dict:
        """Calculate technical indicators from candle history"""
        if len(candles) < 10:
            return {"status": "insufficient_data"}
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            
            # Ensure we have required columns
            if 'close' not in df.columns:
                return {"error": "no_close_data"}
            
            # Calculate basic indicators
            indicators = {}
            
            # 1. Moving Averages
            for period in [5, 10, 20, 50]:
                if len(df) >= period:
                    indicators[f'sma_{period}'] = df['close'].tail(period).mean()
                    indicators[f'ema_{period}'] = df['close'].tail(period).ewm(span=period, adjust=False).mean().iloc[-1]
            
            # 2. Price momentum
            if len(df) >= 2:
                indicators['momentum'] = (df['close'].iloc[-1] - df['close'].iloc[-2]) / df['close'].iloc[-2] * 100
            
            # 3. Volume trend
            if 'volume' in df.columns and len(df) >= 5:
                indicators['volume_trend'] = df['volume'].tail(5).pct_change().mean() * 100
            
            # 4. Volatility (ATR approximation)
            if len(df) >= 14:
                high_low = (df['high'] - df['low']).tail(14)
                indicators['avg_true_range'] = high_low.mean()
                indicators['volatility_pct'] = (high_low.mean() / df['close'].iloc[-1]) * 100
            
            return indicators
            
        except Exception as e:
            return {"error": str(e)}

# Initialize candle builder
candle_builder = IntegratedCandleBuilder()

# ================================================================
# FLASK ENDPOINTS
# ================================================================

@app.route("/price", methods=["POST", "OPTIONS"])
def receive():
    """Receive price ticks from browser"""
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200
    
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "invalid payload"}), 400
        
        batch = data.get("batch", [])
        print(f"📥 Received {len(batch)} ticks")
        
        # Process each tick
        for item in batch:
            # Save raw tick
            try:
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
                    
                    candle_builder.process_tick(pair, price, timestamp, volume)
                    
            except Exception as e:
                print(f"❌ Error processing tick: {e}")
                continue
        
        return jsonify({
            "status": "ok", 
            "received": len(batch),
            "candles_updated": True
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/candles", methods=["GET"])
def get_candles():
    """Get current live candles"""
    try:
        candles = candle_builder.get_current_candles()
        return jsonify({
            "status": "ok", 
            "candles": candles,
            "timestamp": time.time()
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/indicators/<pair>/<tf>", methods=["GET"])
def get_indicators(pair: str, tf: str):
    """Calculate indicators for a specific pair and timeframe"""
    try:
        # Get historical candles
        history = candle_builder.get_historical_candles(pair, tf, limit=100)
        
        if not history:
            return jsonify({"status": "no_data", "message": "Insufficient historical data"}), 200
        
        # Calculate indicators
        indicators = candle_builder.calculate_indicators(history)
        
        # Get current candle
        current_candles = candle_builder.get_current_candles()
        current = current_candles.get(tf, {})
        
        return jsonify({
            "status": "ok",
            "pair": pair,
            "timeframe": tf,
            "history_count": len(history),
            "current_price": current.get("close") if current else None,
            "indicators": indicators,
            "last_candle": history[-1] if history else None
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/health")
def health():
    """Health check endpoint"""
    current = candle_builder.get_current_candles()
    return jsonify({
        "status": "alive",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "candle_builder": "active",
        "active_candles": len(current),
        "memory_usage": f"{len(candle_builder.candle_history)} historical sets"
    }), 200

@app.get("/stats")
def stats():
    """Get system statistics"""
    try:
        # Count raw price files
        raw_files = list(candle_builder.prices_dir.glob("*.jsonl"))
        raw_count = 0
        for f in raw_files:
            try:
                with open(f, 'r') as file:
                    raw_count += len(file.readlines())
            except:
                pass
        
        # Count candle files
        candle_files = list(candle_builder.candles_dir.glob("*.jsonl"))
        candle_count = 0
        for f in candle_files:
            try:
                with open(f, 'r') as file:
                    candle_count += len(file.readlines())
            except:
                pass
        
        # Get current candle status
        current = candle_builder.get_current_candles()
        
        return jsonify({
            "status": "ok",
            "raw_files": len(raw_files),
            "candle_files": len(candle_files),
            "raw_ticks": raw_count,
            "candles": candle_count,
            "active_candles": len(current),
            "timeframes": candle_builder.timeframes,
            "server_time": datetime.datetime.utcnow().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/analyze/<pair>", methods=["GET"])
def analyze_pair(pair: str):
    """Complete analysis of a trading pair"""
    try:
        results = {}
        
        # Analyze each timeframe
        for tf in candle_builder.timeframes:
            history = candle_builder.get_historical_candles(pair, tf, limit=50)
            if history:
                indicators = candle_builder.calculate_indicators(history)
                results[tf] = {
                    "history_count": len(history),
                    "indicators": indicators,
                    "latest": history[-1] if history else None
                }
        
        # Get current prices from all timeframes
        current = candle_builder.get_current_candles()
        current_prices = {}
        for tf, candle in current.items():
            if candle.get("pair") == pair:
                current_prices[tf] = {
                    "price": candle.get("close"),
                    "change_pct": candle.get("change_pct", 0)
                }
        
        return jsonify({
            "status": "ok",
            "pair": pair,
            "analysis": results,
            "current": current_prices,
            "analysis_time": time.time()
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ================================================================
# STARTUP
# ================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 AI Trading System - Candle Builder v2.0")
    print("=" * 60)
    print(f"📁 Prices dir: {candle_builder.prices_dir}")
    print(f"📊 Candles dir: {candle_builder.candles_dir}")
    print(f"⏱️  Timeframes: {candle_builder.timeframes}")
    print("=" * 60)
    
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)