# ──────────────────────────────────────────────────────────────
# app.py – Flask collector for Olymp‑Trade live ticks
# ──────────────────────────────────────────────────────────────
import json, os, datetime, pathlib
from flask import Flask, request, jsonify

app = Flask(__name__)

# Folder where we store daily JSON‑Lines files
DATA_ROOT = pathlib.Path(__file__).parent / "prices"
DATA_ROOT.mkdir(exist_ok=True)

def _store(payload: dict):
    """Append one JSON line to a daily file."""
    ts = payload.get("timestamp", datetime.datetime.utcnow().timestamp())
    date = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    (DATA_ROOT / f"{date}.jsonl").open("a", encoding="utf-8").write(json.dumps(payload) + "\n")

@app.route("/price", methods=["POST"])
def receive():
    try:
        data = request.get_json(force=True)
        if not data or "price" not in data:
            return jsonify({"error": "invalid payload"}), 400
        _store(data)
        return jsonify({"status": "ok"}), 200
    except Exception as e:               # pragma: no cover
        return jsonify({"error": str(e)}), 500

# -----------------------------------------------------------------
# Optional health‑check endpoint (useful for Render)
# -----------------------------------------------------------------
@app.get("/health")
def health():
    return jsonify({"status": "alive"}), 200

# -----------------------------------------------------------------
if __name__ == "__main__":
    # Local test:  http://127.0.0.1:5000/price
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
