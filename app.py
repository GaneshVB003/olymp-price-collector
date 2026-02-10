# Update app.py to include candle processing
from candle_builder import candle_builder
from flask import Flask, request, jsonify
from flask_cors import CORS
import json, os, datetime, pathlib

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Create directories
DATA_ROOT = pathlib.Path(__file__).parent / "prices"
CANDLE_ROOT = pathlib.Path(__file__).parent / "candles"
DATA_ROOT.mkdir(exist_ok=True)
CANDLE_ROOT.mkdir(exist_ok=True)

def _store_batch(payload: dict):
    """Store batch data and process into candles"""
    try:
        batch = payload.get("batch", [])
        if not batch:
            return
        
        for item in batch:
            # Store raw data
            ts = item.get("timestamp", datetime.datetime.utcnow().timestamp())
            date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            pair = item.get("pair", "unknown")
            
            raw_filename = DATA_ROOT / f"{pair}_{date_str}.jsonl"
            with open(raw_filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(item) + "\n")
            
            # Process into candles if it's a price tick
            if "price" in item and "pair" in item:
                price = float(item["price"])
                timestamp = float(item.get("timestamp", ts))
                volume = float(item.get("volume", 0))
                
                candle_builder.process_tick(pair, price, timestamp, volume)
                
    except Exception as e:
        print(f"Error storing batch: {e}")

@app.route("/price", methods=["POST", "OPTIONS"])
def receive():
    if request.method == "OPTIONS":
        return jsonify({"status": "ok"}), 200
    
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "invalid payload"}), 400
        
        _store_batch(data)
        return jsonify({
            "status": "ok", 
            "received": len(data.get("batch", [])),
            "message": "Processing into candles"
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# New endpoint to get current candle status
@app.route("/candles", methods=["GET"])
def get_candles():
    try:
        candles = candle_builder.get_current_candles()
        return jsonify({"status": "ok", "candles": candles}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/health")
def health():
    return jsonify({"status": "alive"}), 200

@app.get("/stats")
def stats():
    try:
        raw_files = list(DATA_ROOT.glob("*.jsonl"))
        candle_files = list(CANDLE_ROOT.glob("*.jsonl"))
        
        raw_count = sum(1 for f in raw_files for _ in open(f))
        candle_count = sum(1 for f in candle_files for _ in open(f))
        
        return jsonify({
            "status": "ok",
            "raw_files": len(raw_files),
            "candle_files": len(candle_files),
            "raw_entries": raw_count,
            "candle_entries": candle_count,
            "current_candles": len(candle_builder.get_current_candles())
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))