# app.py – Updated with CORS support
import json, os, datetime, pathlib
from flask import Flask, request, jsonify
from flask_cors import CORS  # Add this import

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for now

# Folder where we store daily JSON‑Lines files
DATA_ROOT = pathlib.Path(__file__).parent / "prices"
DATA_ROOT.mkdir(exist_ok=True)

def _store_batch(payload: dict):
    """Store batch data with candle aggregation"""
    try:
        batch = payload.get("batch", [])
        if not batch:
            return
        
        for item in batch:
            # Add server timestamp if missing
            if "server_timestamp" not in item:
                item["server_timestamp"] = datetime.datetime.utcnow().timestamp()
            
            # Save each item with date and pair in filename
            ts = item.get("timestamp", item["server_timestamp"])
            date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            pair = item.get("pair", "unknown")
            
            # Create filename with pair and date
            filename = DATA_ROOT / f"{pair}_{date_str}.jsonl"
            
            # Append JSON line
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(item) + "\n")
                
    except Exception as e:
        print(f"Error storing batch: {e}")

@app.route("/price", methods=["POST", "OPTIONS"])
def receive():
    if request.method == "OPTIONS":
        # Handle preflight request
        return jsonify({"status": "ok"}), 200
    
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "invalid payload"}), 400
        
        _store_batch(data)
        return jsonify({"status": "ok", "received": len(data.get("batch", []))}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/health")
def health():
    return jsonify({"status": "alive"}), 200

# New endpoint to check collected data
@app.get("/stats")
def stats():
    try:
        files = list(DATA_ROOT.glob("*.jsonl"))
        total_lines = 0
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                total_lines += len(f.readlines())
        
        return jsonify({
            "status": "ok",
            "files": len(files),
            "total_entries": total_lines,
            "data_root": str(DATA_ROOT)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))