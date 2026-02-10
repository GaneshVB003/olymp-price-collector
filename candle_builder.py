# candle_builder.py
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading
import time
import os
import pathlib

class CandleBuilder:
    def __init__(self, timeframes: List[str] = ["1s", "5s", "1m", "5m", "15m"]):
        self.timeframes = timeframes
        self.candle_data = {}
        self.indicators = {}
        self.lock = threading.Lock()
        
        # Create candles directory
        self.candle_dir = pathlib.Path(__file__).parent / "candles"
        self.candle_dir.mkdir(exist_ok=True)
        
        # Initialize candle storage for each timeframe
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
                "pair": None
            }
        
        # Start background thread to close stale candles
        self._start_background_processor()
        print(f"✅ CandleBuilder initialized with timeframes: {timeframes}")
    
    def _start_background_processor(self):
        """Start thread to process and close candles"""
        def processor():
            while True:
                try:
                    with self.lock:
                        self._check_stale_candles()
                    time.sleep(1)  # Check every second
                except Exception as e:
                    print(f"Background processor error: {e}")
                    time.sleep(5)
        
        thread = threading.Thread(target=processor, daemon=True)
        thread.start()
    
    def process_tick(self, pair: str, price: float, timestamp: float, volume: float = 0):
        """Process a single price tick into candles"""
        with self.lock:
            for tf in self.timeframes:
                self._update_candle(tf, pair, price, timestamp, volume)
    
    def _get_timeframe_seconds(self, tf: str) -> int:
        """Convert timeframe string to seconds"""
        if tf.endswith('s'):
            return int(tf[:-1])
        elif tf.endswith('m'):
            return int(tf[:-1]) * 60
        elif tf.endswith('h'):
            return int(tf[:-1]) * 3600
        elif tf.endswith('d'):
            return int(tf[:-1]) * 86400
        return 1
    
    def _update_candle(self, tf: str, pair: str, price: float, timestamp: float, volume: float):
        """Update candle for specific timeframe"""
        tf_seconds = self._get_timeframe_seconds(tf)
        candle_start = int(timestamp // tf_seconds) * tf_seconds
        
        candle = self.candle_data[tf]
        
        # New candle period
        if candle["start_time"] != candle_start:
            # Save completed candle
            if candle["start_time"] is not None and candle["pair"]:
                self._save_candle(tf, candle["pair"], candle)
            
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
                "timeframe": tf
            })
        else:
            # Update existing candle
            if candle["pair"] is None:
                candle["pair"] = pair
                candle["open"] = price
                candle["high"] = price
                candle["low"] = price
                candle["close"] = price
            else:
                candle["high"] = max(candle["high"], price)
                candle["low"] = min(candle["low"], price)
                candle["close"] = price
            candle["volume"] += volume
            candle["ticks"] += 1
    
    def _save_candle(self, tf: str, pair: str, candle: Dict):
        """Save completed candle to file"""
        try:
            # Create filename with date and pair
            date_str = datetime.utcfromtimestamp(candle["start_time"]).strftime("%Y-%m-%d")
            filename = self.candle_dir / f"{pair}_{tf}_{date_str}.jsonl"
            
            # Add calculated indicators
            candle_with_indicators = self._add_indicators(candle.copy())
            
            # Remove None values
            clean_candle = {k: v for k, v in candle_with_indicators.items() if v is not None}
            
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(clean_candle) + "\n")
            
            print(f"💾 Saved {tf} candle for {pair} at {candle['close']:.2f}")
            
        except Exception as e:
            print(f"Error saving candle: {e}")
    
    def _check_stale_candles(self):
        """Close candles that haven't been updated recently"""
        current_time = time.time()
        for tf in self.timeframes:
            candle = self.candle_data[tf]
            if candle["start_time"] and candle["end_time"] < current_time:
                # Candle period ended, force close
                if candle["pair"]:
                    self._save_candle(tf, candle["pair"], candle)
                candle["start_time"] = None
                candle["pair"] = None
    
    def _add_indicators(self, candle: Dict) -> Dict:
        """Add basic indicators to candle"""
        try:
            # Simple moving average placeholder (will be expanded)
            close_price = candle.get("close", 0)
            open_price = candle.get("open", close_price)
            
            if open_price and close_price and open_price != 0:
                candle["price_change_pct"] = (close_price - open_price) / open_price * 100
            else:
                candle["price_change_pct"] = 0
            
            # Volume indicators
            volume = candle.get("volume", 0)
            candle["volume"] = volume
            
            # Basic volatility
            high = candle.get("high", close_price)
            low = candle.get("low", close_price)
            if high and low:
                candle["range_pct"] = (high - low) / low * 100
            else:
                candle["range_pct"] = 0
            
            # Timestamps
            candle["saved_at"] = time.time()
            
        except Exception as e:
            print(f"Error adding indicators: {e}")
        
        return candle
    
    def get_current_candles(self) -> Dict:
        """Get current state of all candles"""
        with self.lock:
            current_candles = {}
            for tf, candle in self.candle_data.items():
                if candle["start_time"] is not None:
                    # Create a clean copy without the lock
                    current_candles[tf] = {
                        "open": candle["open"],
                        "high": candle["high"],
                        "low": candle["low"],
                        "close": candle["close"],
                        "volume": candle["volume"],
                        "ticks": candle["ticks"],
                        "start_time": candle["start_time"],
                        "end_time": candle["end_time"],
                        "pair": candle["pair"],
                        "timeframe": tf
                    }
            return current_candles

# Global instance
candle_builder = CandleBuilder()