# candle_builder.py - Converts ticks to candles and calculates indicators
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import threading
import time

class CandleBuilder:
    def __init__(self, timeframes: List[str] = ["1s", "5s", "1m", "5m", "15m"]):
        self.timeframes = timeframes
        self.candle_data = {}
        self.indicators = {}
        self.lock = threading.Lock()
        
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
                "end_time": None
            }
        
        # Start background thread to close stale candles
        self._start_background_processor()
    
    def _start_background_processor(self):
        """Start thread to process and close candles"""
        def processor():
            while True:
                try:
                    with self.lock:
                        self._check_stale_candles()
                        self._calculate_indicators()
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
            if candle["start_time"] is not None:
                self._save_candle(tf, pair, candle)
            
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
            filename = f"candles/{pair}_{tf}_{date_str}.jsonl"
            
            with open(filename, "a", encoding="utf-8") as f:
                # Add calculated indicators
                candle_with_indicators = self._add_indicators(candle.copy())
                f.write(json.dumps(candle_with_indicators) + "\n")
            
            print(f"💾 Saved {tf} candle for {pair} at {candle['close']}")
            
        except Exception as e:
            print(f"Error saving candle: {e}")
    
    def _check_stale_candles(self):
        """Close candles that haven't been updated recently"""
        current_time = time.time()
        for tf in self.timeframes:
            candle = self.candle_data[tf]
            if candle["start_time"] and candle["end_time"] < current_time:
                # Candle period ended, force close
                self._save_candle(tf, "BTCUSD_OTC", candle)
                candle["start_time"] = None
    
    def _add_indicators(self, candle: Dict) -> Dict:
        """Add basic indicators to candle"""
        # Simple moving average placeholder
        candle["sma_5"] = candle["close"]
        candle["sma_10"] = candle["close"]
        candle["price_change"] = (candle["close"] - candle["open"]) / candle["open"] * 100
        
        # Volume indicators
        candle["volume_avg"] = candle["volume"]
        candle["volume_ratio"] = 1.0
        
        return candle
    
    def _calculate_indicators(self):
        """Calculate real-time indicators"""
        # This will be expanded with 50+ indicators
        pass
    
    def get_current_candles(self) -> Dict:
        """Get current state of all candles"""
        with self.lock:
            return {
                tf: {
                    k: v for k, v in candle.items() 
                    if k not in ["pair", "timeframe"]
                }
                for tf, candle in self.candle_data.items()
            }
    
    def get_indicators(self) -> Dict:
        """Get current indicator values"""
        with self.lock:
            return self.indicators.copy()

# Global instance
candle_builder = CandleBuilder()