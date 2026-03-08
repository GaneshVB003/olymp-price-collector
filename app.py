"""
╔══════════════════════════════════════════════════════════════╗
║      QUANTUM OLYMPTRADE AI — RENDER SERVER  v2.0             ║
║  Receives all WebSocket data from Tampermonkey               ║
║  Builds 15-timeframe candles per instrument                  ║
║  Serves Colab AI + Dashboard + Notification system           ║
╚══════════════════════════════════════════════════════════════╝
Deploy on Render.com (free tier).  Start command: python app.py
"""

import os, json, time, threading, pathlib, datetime, math
from collections import defaultdict, deque
from typing import Dict, List, Optional, Any
from flask import Flask, request, jsonify, Response
from flask_cors import CORS

# ═══════════════════════════════════════════════════════════════
# 0.  BOOTSTRAP — auto-install missing packages on Render
# ═══════════════════════════════════════════════════════════════
import subprocess, sys

def _ensure(pkg, imp=None):
    imp = imp or pkg
    try:
        __import__(imp)
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', pkg])

_ensure('flask');  _ensure('flask-cors', 'flask_cors')

# ═══════════════════════════════════════════════════════════════
# 1.  CONFIG
# ═══════════════════════════════════════════════════════════════
class CFG:
    PORT              = int(os.getenv('PORT', 5000))
    DATA_DIR          = pathlib.Path('./data')
    MAX_TICKS_RAM     = 3000    # ticks kept in RAM per instrument
    MAX_CANDLES_RAM   = 2000    # candles per timeframe per instrument (RAM)
    MAX_NOTIFS        = 200     # notifications kept in RAM
    CANDLE_FLUSH_SEC  = 10      # write candles to disk every N seconds
    SESSION_TTL_SEC   = 86400   # 24-hour session lifetime
    MAX_BATCH_SIZE    = 500     # max items accepted per POST /tick

    # All 15 timeframes in seconds
    TIMEFRAMES: Dict[str, int] = {
        '1s':  1,   '5s':  5,   '10s': 10,  '15s': 15,  '30s': 30,
        '1m':  60,  '2m':  120, '3m':  180, '5m':  300,
        '10m': 600, '15m': 900, '30m': 1800,
        '1h':  3600,'4h':  14400,'1d': 86400
    }

# ═══════════════════════════════════════════════════════════════
# 2.  DIRECTORY SETUP
# ═══════════════════════════════════════════════════════════════
for sub in ['ticks', 'candles', 'orderbook', 'sentiment', 'predictions', 'sessions']:
    (CFG.DATA_DIR / sub).mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# 3.  THREAD-SAFE HELPERS
# ═══════════════════════════════════════════════════════════════
def now_ts() -> float:
    return time.time()

def utc_str() -> str:
    return datetime.datetime.utcnow().isoformat()

def append_jsonl(path: pathlib.Path, obj: Any):
    """Append one JSON line to a file (thread-safe via GIL on CPython)."""
    try:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f'[DISK] write error {path}: {e}')

def read_jsonl_tail(path: pathlib.Path, n: int) -> List[dict]:
    """Read last N lines from a JSONL file efficiently."""
    if not path.exists():
        return []
    lines = []
    try:
        with open(path, 'rb') as f:
            # seek from end for large files
            try:
                f.seek(0, 2)
                size = f.tell()
                chunk = min(size, n * 300)   # estimate 300 bytes/line
                f.seek(max(0, size - chunk))
                raw = f.read().decode('utf-8', errors='ignore').splitlines()
                for line in raw[-n:]:
                    line = line.strip()
                    if line:
                        try:
                            lines.append(json.loads(line))
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass
    return lines

# ═══════════════════════════════════════════════════════════════
# 4.  CANDLE BUILDER  — 15 timeframes, per instrument
# ═══════════════════════════════════════════════════════════════
class CandleBuilder:
    """
    Receives price ticks, maintains live OHLCV candles for all 15 timeframes.
    Stores completed candles in RAM deques AND flushes to disk JSONL files.
    """

    def __init__(self):
        self._lock    = threading.RLock()
        # live open candle per instrument per timeframe
        # { pair: { tf: { open, high, low, close, volume, ticks, period_start, period_end } } }
        self._live: Dict[str, Dict[str, dict]] = defaultdict(dict)
        # completed candles in RAM
        # { pair: { tf: deque([candle, ...]) } }
        self._history: Dict[str, Dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=CFG.MAX_CANDLES_RAM))
        )
        self._pending_flush: List[tuple] = []   # (pair, tf, candle)
        self._start_flush_thread()

    # ── public: feed one tick ──────────────────────────────────
    def tick(self, pair: str, price: float, ts: float):
        with self._lock:
            for tf, sec in CFG.TIMEFRAMES.items():
                period = int(ts // sec) * sec
                live   = self._live[pair].get(tf)

                if live is None or live['period_start'] != period:
                    # close old candle
                    if live is not None:
                        closed = self._close(live)
                        self._history[pair][tf].append(closed)
                        self._pending_flush.append((pair, tf, closed))
                    # open new candle
                    self._live[pair][tf] = {
                        'pair': pair, 'tf': tf,
                        'period_start': period,
                        'period_end':   period + sec,
                        'open':  price, 'high':  price,
                        'low':   price, 'close': price,
                        'volume': 0,    'ticks':  1
                    }
                else:
                    live['high']   = max(live['high'], price)
                    live['low']    = min(live['low'],  price)
                    live['close']  = price
                    live['ticks'] += 1

    # ── public: feed a pre-built candle from e:1003 ───────────
    def ingest_candle(self, pair: str, o: float, h: float,
                      l: float, c: float, ts: float):
        """Accept OHLCV candles that OlympTrade sends directly (e:1003)."""
        with self._lock:
            # treat as a 1-second candle; also update live price
            self.tick(pair, c, ts)
            # store the richer e:1003 candle into 1s history directly
            candle = {
                'pair': pair, 'tf': '1s',
                'period_start': int(ts),
                'period_end':   int(ts) + 1,
                'open': o, 'high': h, 'low': l, 'close': c,
                'volume': 0, 'ticks': 1,
                'source': 'e1003'
            }
            self._history[pair]['1s'].append(candle)

    # ── public: get candles ────────────────────────────────────
    def get(self, pair: str, tf: str, n: int = 500) -> List[dict]:
        with self._lock:
            hist = list(self._history[pair][tf])[-n:]
            # append live candle snapshot if exists
            live = self._live[pair].get(tf)
            if live:
                snap = live.copy()
                snap['live'] = True
                hist = hist + [snap]
            return hist

    def get_live_price(self, pair: str) -> Optional[float]:
        with self._lock:
            live = self._live[pair].get('1s') or self._live[pair].get('5s')
            return live['close'] if live else None

    def instruments(self) -> List[str]:
        with self._lock:
            return sorted(self._live.keys())

    def candle_counts(self, pair: str) -> Dict[str, int]:
        with self._lock:
            return {tf: len(self._history[pair][tf]) for tf in CFG.TIMEFRAMES}

    # ── internal ───────────────────────────────────────────────
    @staticmethod
    def _close(live: dict) -> dict:
        c = live.copy()
        c['closed'] = True
        c['closed_at'] = now_ts()
        return c

    def _start_flush_thread(self):
        def worker():
            while True:
                time.sleep(CFG.CANDLE_FLUSH_SEC)
                with self._lock:
                    items = self._pending_flush[:]
                    self._pending_flush.clear()
                for pair, tf, candle in items:
                    date = datetime.datetime.utcfromtimestamp(
                        candle['period_start']).strftime('%Y-%m-%d')
                    path = CFG.DATA_DIR / 'candles' / f'{pair}_{tf}_{date}.jsonl'
                    append_jsonl(path, candle)
        threading.Thread(target=worker, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# 5.  ORDER BOOK STORE  — payout / trader positioning
# ═══════════════════════════════════════════════════════════════
class OrderBookStore:
    """
    Stores the payout/weight data from e:80.
    This is our contrarian alpha: when 90% of traders bet UP, AI should lean DOWN.
    """
    def __init__(self):
        self._lock    = threading.RLock()
        self._latest: Dict[str, dict] = {}         # pair → latest snapshot
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

    def update(self, pair: str, durations: List[dict], ts: float):
        rec = {'pair': pair, 'durations': durations, 'ts': ts}
        with self._lock:
            self._latest[pair] = rec
            self._history[pair].append(rec)
        # persist
        date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        path = CFG.DATA_DIR / 'orderbook' / f'{pair}_{date}.jsonl'
        append_jsonl(path, rec)

    def get_latest(self, pair: str) -> Optional[dict]:
        with self._lock:
            return self._latest.get(pair)

    def get_history(self, pair: str, n: int = 100) -> List[dict]:
        with self._lock:
            return list(self._history[pair])[-n:]

    def get_contrarian_signal(self, pair: str, duration_sec: int) -> dict:
        """
        Returns the contrarian signal for a specific trade duration.
        If 80%+ of traders bet UP → platform likely moves DOWN → signal DOWN.
        """
        snap = self.get_latest(pair)
        if not snap:
            return {'signal': 'NONE', 'weight_up': 50, 'weight_down': 50,
                    'payout_up': 85, 'payout_down': 85, 'strength': 0}
        # find matching duration bucket
        bucket = None
        for d in snap.get('durations', []):
            if d['min_sec'] <= duration_sec <= d['max_sec']:
                bucket = d
                break
        if not bucket:
            return {'signal': 'NONE', 'weight_up': 50, 'weight_down': 50,
                    'payout_up': 85, 'payout_down': 85, 'strength': 0}
        wu = bucket['weight_up']
        wd = bucket['weight_down']
        strength = max(wu, wd) - 50  # 0-50 scale
        if wu > 75:
            signal = 'DOWN'   # majority up → fade them
        elif wd > 75:
            signal = 'UP'
        else:
            signal = 'NONE'
        return {
            'signal':      signal,
            'weight_up':   wu,
            'weight_down': wd,
            'payout_up':   bucket['payout_up'],
            'payout_down': bucket['payout_down'],
            'strength':    round(strength, 1)
        }

# ═══════════════════════════════════════════════════════════════
# 6.  SENTIMENT STORE
# ═══════════════════════════════════════════════════════════════
class SentimentStore:
    def __init__(self):
        self._lock    = threading.RLock()
        self._latest: Dict[str, dict]  = {}
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

    def update(self, pair: str, value: float, ts: float):
        rec = {'pair': pair, 'sentiment': value, 'ts': ts}
        with self._lock:
            self._latest[pair] = rec
            self._history[pair].append(rec)

    def get_latest(self, pair: str) -> Optional[dict]:
        with self._lock:
            return self._latest.get(pair)

    def get_history(self, pair: str, n: int = 200) -> List[dict]:
        with self._lock:
            return list(self._history[pair])[-n:]

# ═══════════════════════════════════════════════════════════════
# 7.  RAW TICK STORE  — per-second prices, kept in RAM + disk
# ═══════════════════════════════════════════════════════════════
class TickStore:
    def __init__(self):
        self._lock    = threading.RLock()
        self._ticks: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=CFG.MAX_TICKS_RAM))
        self._pending: List[tuple] = []
        self._start_flush_thread()

    def add(self, pair: str, price: float, ts: float):
        rec = {'pair': pair, 'price': price, 'ts': ts}
        with self._lock:
            self._ticks[pair].append(rec)
            self._pending.append((pair, rec))

    def get(self, pair: str, n: int = 1000) -> List[dict]:
        with self._lock:
            return list(self._ticks[pair])[-n:]

    def _start_flush_thread(self):
        def worker():
            while True:
                time.sleep(5)
                with self._lock:
                    items = self._pending[:]
                    self._pending.clear()
                grouped: Dict[str, List] = defaultdict(list)
                for pair, rec in items:
                    grouped[pair].append(rec)
                for pair, recs in grouped.items():
                    date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
                    path = CFG.DATA_DIR / 'ticks' / f'{pair}_{date}.jsonl'
                    for rec in recs:
                        append_jsonl(path, rec)
        threading.Thread(target=worker, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# 8.  PREDICTION STORE  — AI pushes here; browser polls here
# ═══════════════════════════════════════════════════════════════
class PredictionStore:
    """
    Colab AI POSTs predictions here.
    Tampermonkey polls /notifications every 2s.
    Dashboard reads /predictions/{pair}.
    """
    def __init__(self):
        self._lock   = threading.RLock()
        # latest per pair+timeframe
        self._latest: Dict[str, dict] = {}          # key = f"{pair}_{tf}"
        # notification queue (all high-confidence predictions)
        self._notifs: deque = deque(maxlen=CFG.MAX_NOTIFS)
        # all predictions ever (for dashboard history)
        self._all: deque = deque(maxlen=5000)
        # accuracy tracking
        self._pending_resolution: List[dict] = []   # predictions awaiting expiry
        self._accuracy: Dict[str, dict] = defaultdict(
            lambda: {'correct': 0, 'total': 0, 'winrate': 0.0})

    def save(self, pred: dict):
        """Called by Colab AI to store a new prediction."""
        key = f"{pred['pair']}_{pred['timeframe']}"
        with self._lock:
            self._latest[key]  = pred
            self._all.append(pred)
            # queue for accuracy tracking
            if pred.get('confidence', 0) >= pred.get('min_confidence', 0):
                self._notifs.append(pred)
                self._pending_resolution.append({
                    **pred,
                    'resolve_at': now_ts() + pred.get('duration_sec', 60)
                })
        # persist
        date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
        path = CFG.DATA_DIR / 'predictions' / f"{pred['pair']}_{date}.jsonl"
        append_jsonl(path, pred)

    def resolve(self, pair: str, price: float):
        """
        Called periodically when new price arrives.
        Checks expired predictions, marks WIN/LOSS, updates accuracy.
        """
        ts = now_ts()
        with self._lock:
            remaining = []
            for p in self._pending_resolution:
                if p['pair'] != pair or ts < p['resolve_at']:
                    remaining.append(p)
                    continue
                # resolve
                entry_price = p.get('entry_price', 0)
                if entry_price == 0:
                    remaining.append(p)
                    continue
                diff = price - entry_price
                won  = (p['direction'] == 'UP' and diff > 0) or \
                       (p['direction'] == 'DOWN' and diff < 0)
                key  = f"{pair}_{p['timeframe']}"
                self._accuracy[key]['total']   += 1
                self._accuracy[key]['correct'] += int(won)
                t = self._accuracy[key]['total']
                c = self._accuracy[key]['correct']
                self._accuracy[key]['winrate'] = round(c / t * 100, 1) if t else 0
                outcome = {**p, 'won': won, 'exit_price': price,
                           'resolved_at': ts}
                date = datetime.datetime.utcnow().strftime('%Y-%m-%d')
                path = CFG.DATA_DIR / 'predictions' / \
                       f"{pair}_outcomes_{date}.jsonl"
                append_jsonl(path, outcome)
            self._pending_resolution = remaining

    def get_latest(self, pair: str, tf: str) -> Optional[dict]:
        with self._lock:
            return self._latest.get(f'{pair}_{tf}')

    def get_for_pair(self, pair: str) -> List[dict]:
        with self._lock:
            return [p for p in self._all if p.get('pair') == pair][-50:]

    def get_notifications(self, since_ts: float, min_conf: float = 60) -> List[dict]:
        with self._lock:
            return [n for n in self._notifs
                    if n.get('ts', 0) > since_ts
                    and n.get('confidence', 0) >= min_conf]

    def get_accuracy(self, pair: str, tf: str) -> dict:
        with self._lock:
            return self._accuracy.get(f'{pair}_{tf}',
                   {'correct': 0, 'total': 0, 'winrate': 0.0})

    def get_all_accuracy(self) -> dict:
        with self._lock:
            return dict(self._accuracy)

# ═══════════════════════════════════════════════════════════════
# 9.  SESSION MANAGER  — multi-user, per-session state
# ═══════════════════════════════════════════════════════════════
class SessionManager:
    def __init__(self):
        self._lock     = threading.RLock()
        self._sessions: Dict[str, dict] = {}

    def touch(self, sid: str, active_pair: Optional[str] = None,
              min_confidence: float = 60.0) -> dict:
        with self._lock:
            if sid not in self._sessions:
                self._sessions[sid] = {
                    'id':              sid,
                    'created':         now_ts(),
                    'last_seen':       now_ts(),
                    'active_pair':     active_pair,
                    'min_confidence':  min_confidence,
                    'last_notif_ts':   0.0,
                    'prediction_count': 0
                }
            else:
                self._sessions[sid]['last_seen'] = now_ts()
                if active_pair:
                    self._sessions[sid]['active_pair'] = active_pair
            return self._sessions[sid]

    def get(self, sid: str) -> Optional[dict]:
        with self._lock:
            return self._sessions.get(sid)

    def update_notif_ts(self, sid: str, ts: float):
        with self._lock:
            if sid in self._sessions:
                self._sessions[sid]['last_notif_ts'] = ts

    def update_min_confidence(self, sid: str, mc: float):
        with self._lock:
            if sid in self._sessions:
                self._sessions[sid]['min_confidence'] = mc

    def purge_old(self):
        cutoff = now_ts() - CFG.SESSION_TTL_SEC
        with self._lock:
            dead = [s for s, v in self._sessions.items()
                    if v['last_seen'] < cutoff]
            for s in dead:
                del self._sessions[s]

    def count(self) -> int:
        with self._lock:
            return len(self._sessions)

# ═══════════════════════════════════════════════════════════════
# 10.  GLOBAL INSTANCES
# ═══════════════════════════════════════════════════════════════
candles   = CandleBuilder()
orderbook = OrderBookStore()
sentiment = SentimentStore()
ticks     = TickStore()
preds     = PredictionStore()
sessions  = SessionManager()

# server stats
_stats = {
    'requests':       0,
    'ticks_received': 0,
    'errors':         0,
    'start_time':     now_ts()
}

# ── background: purge old sessions every hour ─────────────────
def _session_purge_loop():
    while True:
        time.sleep(3600)
        sessions.purge_old()
threading.Thread(target=_session_purge_loop, daemon=True).start()

# ═══════════════════════════════════════════════════════════════
# 11.  FLASK APP
# ═══════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': '*'}})

# ── middleware: count every request ───────────────────────────
@app.before_request
def _count():
    _stats['requests'] += 1

# ─────────────────────────────────────────────────────────────
#  POST /tick  — Tampermonkey sends all captured WS data here
# ─────────────────────────────────────────────────────────────
@app.route('/tick', methods=['POST', 'OPTIONS'])
def recv_tick():
    if request.method == 'OPTIONS':
        return jsonify({'ok': True})

    try:
        body = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({'error': 'bad json'}), 400

    sid         = request.headers.get('X-Session-ID') or \
                  body.get('session_id', 'anonymous')
    active_pair = body.get('active_pair')
    client_ts   = body.get('client_ts', now_ts())
    batch_items = body.get('batch', [])

    if len(batch_items) > CFG.MAX_BATCH_SIZE:
        batch_items = batch_items[:CFG.MAX_BATCH_SIZE]

    # update session
    sess = sessions.touch(sid, active_pair)

    processed = 0
    for item in batch_items:
        t     = item.get('type', '')
        pair  = item.get('pair', '')
        if not pair:
            continue

        try:
            if t == 'tick':
                price = float(item['price'])
                ts_   = float(item.get('ts', client_ts))
                ticks.add(pair, price, ts_)
                candles.tick(pair, price, ts_)
                preds.resolve(pair, price)     # check expired predictions
                _stats['ticks_received'] += 1

            elif t == 'candle':
                candles.ingest_candle(
                    pair,
                    float(item.get('open',  0)),
                    float(item.get('high',  0)),
                    float(item.get('low',   0)),
                    float(item.get('close', 0)),
                    float(item.get('ts', client_ts))
                )

            elif t == 'orderbook':
                orderbook.update(pair, item.get('durations', []),
                                 float(item.get('ts', client_ts)))

            elif t == 'sentiment':
                sentiment.update(pair,
                                 float(item.get('sentiment', 50)),
                                 float(item.get('ts', client_ts)))

            elif t in ('volume', 'period_start'):
                pass   # candle builder already gets close price from ticks

            processed += 1
        except Exception as ex:
            _stats['errors'] += 1
            print(f'[TICK] error on {t}/{pair}: {ex}')

    return jsonify({
        'ok':              True,
        'processed':       processed,
        'total_received':  _stats['ticks_received'],
        'instruments':     len(candles.instruments()),
        'session':         sid,
        'active_pair':     active_pair
    })

# ─────────────────────────────────────────────────────────────
#  GET /candles/<pair>/<tf>?n=500
#  Colab AI pulls candle history for training/inference
# ─────────────────────────────────────────────────────────────
@app.route('/candles/<pair>/<tf>')
def get_candles(pair, tf):
    if tf not in CFG.TIMEFRAMES:
        return jsonify({'error': f'unknown timeframe {tf}',
                        'valid': list(CFG.TIMEFRAMES.keys())}), 400
    n    = min(int(request.args.get('n', 500)), 5000)
    data = candles.get(pair, tf, n)
    return jsonify({
        'pair':   pair,
        'tf':     tf,
        'count':  len(data),
        'candles': data,
        'live_price': candles.get_live_price(pair),
        'ts':     now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /ticks/<pair>?n=500
#  Raw price ticks per second for Colab feature extraction
# ─────────────────────────────────────────────────────────────
@app.route('/ticks/<pair>')
def get_ticks(pair):
    n    = min(int(request.args.get('n', 500)), 3000)
    data = ticks.get(pair, n)
    return jsonify({'pair': pair, 'count': len(data), 'ticks': data})

# ─────────────────────────────────────────────────────────────
#  GET /orderbook/<pair>?duration_sec=60
#  Payout + contrarian signal for Colab feature extraction
# ─────────────────────────────────────────────────────────────
@app.route('/orderbook/<pair>')
def get_orderbook(pair):
    dur = int(request.args.get('duration_sec', 60))
    return jsonify({
        'pair':             pair,
        'latest':           orderbook.get_latest(pair),
        'contrarian':       orderbook.get_contrarian_signal(pair, dur),
        'history_count':    len(orderbook.get_history(pair, 1)),
        'ts':               now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /sentiment/<pair>
# ─────────────────────────────────────────────────────────────
@app.route('/sentiment/<pair>')
def get_sentiment(pair):
    return jsonify({
        'pair':    pair,
        'latest':  sentiment.get_latest(pair),
        'history': sentiment.get_history(pair, 100),
        'ts':      now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /state/<pair>
#  Single endpoint: Colab pulls everything it needs in one call
# ─────────────────────────────────────────────────────────────
@app.route('/state/<pair>')
def get_state(pair):
    """
    Colab AI polls this every 2 seconds.
    Returns enough data to compute all 150+ features and run inference.
    """
    n_ticks   = int(request.args.get('ticks',   500))
    n_candles = int(request.args.get('candles', 200))

    result = {
        'pair':      pair,
        'ts':        now_ts(),
        'live_price': candles.get_live_price(pair),
        'candle_counts': candles.candle_counts(pair),
        'candles':   {},
        'ticks':     ticks.get(pair, n_ticks),
        'orderbook': {
            'latest':     orderbook.get_latest(pair),
            'contrarian': {dur_label: orderbook.get_contrarian_signal(
                               pair, sec)
                           for dur_label, sec in {
                               '15s': 15, '30s': 30, '1m': 60, '5m': 300
                           }.items()}
        },
        'sentiment':   sentiment.get_latest(pair),
        'instruments': candles.instruments()
    }

    # include candles for key timeframes (Colab can request all TFs separately)
    for tf in ['1s', '5s', '10s', '30s', '1m', '5m', '15m', '1h']:
        data = candles.get(pair, tf, n_candles)
        result['candles'][tf] = data

    return jsonify(result)

# ─────────────────────────────────────────────────────────────
#  POST /prediction
#  Colab AI pushes predictions here
# ─────────────────────────────────────────────────────────────
@app.route('/prediction', methods=['POST'])
def save_prediction():
    try:
        pred = request.get_json(force=True)
        if not pred or 'pair' not in pred or 'direction' not in pred:
            return jsonify({'error': 'missing required fields'}), 400

        required = ['pair', 'direction', 'confidence', 'timeframe',
                    'duration_sec', 'reason', 'ts']
        for r in required:
            if r not in pred:
                pred.setdefault(r, None)

        pred['received_at'] = now_ts()
        preds.save(pred)

        return jsonify({'ok': True, 'id': pred.get('id', 'unknown')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─────────────────────────────────────────────────────────────
#  GET /predictions/<pair>
#  Dashboard fetches recent predictions for a pair
# ─────────────────────────────────────────────────────────────
@app.route('/predictions/<pair>')
def get_predictions(pair):
    tf = request.args.get('tf')
    if tf:
        p = preds.get_latest(pair, tf)
        return jsonify({'pair': pair, 'tf': tf,
                        'prediction': p,
                        'accuracy': preds.get_accuracy(pair, tf)})
    all_p = preds.get_for_pair(pair)
    return jsonify({
        'pair':        pair,
        'predictions': all_p,
        'accuracy':    {tf: preds.get_accuracy(pair, tf)
                        for tf in CFG.TIMEFRAMES}
    })

# ─────────────────────────────────────────────────────────────
#  GET /notifications?session=<id>
#  Tampermonkey polls this every 2s — returns unseen predictions
# ─────────────────────────────────────────────────────────────
@app.route('/notifications')
def get_notifications():
    sid  = request.args.get('session', 'anonymous')
    sess = sessions.touch(sid)
    mc   = float(request.args.get('min_confidence',
                  sess.get('min_confidence', 60)))

    since = sess.get('last_notif_ts', 0.0)
    notifs = preds.get_notifications(since, mc)

    if notifs:
        latest_ts = max(n.get('ts', 0) for n in notifs)
        sessions.update_notif_ts(sid, latest_ts)

    return jsonify({
        'notifications': notifs,
        'count':         len(notifs),
        'since':         since,
        'ts':            now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /instruments
#  List all instruments currently receiving data
# ─────────────────────────────────────────────────────────────
@app.route('/instruments')
def get_instruments():
    pairs = candles.instruments()
    result = []
    for p in pairs:
        counts = candles.candle_counts(p)
        ob     = orderbook.get_contrarian_signal(p, 60)
        sent   = sentiment.get_latest(p)
        result.append({
            'pair':             p,
            'live_price':       candles.get_live_price(p),
            'candle_counts':    counts,
            'total_1s_candles': counts.get('1s', 0),
            'contrarian_60s':   ob,
            'sentiment':        sent.get('sentiment') if sent else None
        })
    return jsonify({
        'instruments': result,
        'count':       len(result),
        'ts':          now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /accuracy
#  Overall AI accuracy across all pairs and timeframes
# ─────────────────────────────────────────────────────────────
@app.route('/accuracy')
def get_accuracy():
    return jsonify({
        'accuracy': preds.get_all_accuracy(),
        'ts':       now_ts()
    })

# ─────────────────────────────────────────────────────────────
#  GET /health  — keep-alive ping + system status
# ─────────────────────────────────────────────────────────────
@app.route('/health')
def health():
    uptime = now_ts() - _stats['start_time']
    return jsonify({
        'status':          'alive',
        'uptime_sec':      round(uptime, 1),
        'uptime_fmt':      _fmt_uptime(uptime),
        'ticks_received':  _stats['ticks_received'],
        'requests':        _stats['requests'],
        'errors':          _stats['errors'],
        'instruments':     len(candles.instruments()),
        'active_sessions': sessions.count(),
        'ts':              now_ts(),
        'utc':             utc_str()
    })

# ─────────────────────────────────────────────────────────────
#  GET /dashboard  — simple status page (full dashboard is in Colab)
# ─────────────────────────────────────────────────────────────
@app.route('/dashboard')
def dashboard():
    sid = request.args.get('session', '')
    html = _build_status_html(sid)
    return Response(html, mimetype='text/html')

# ─────────────────────────────────────────────────────────────
#  PUT /session/confidence?session=<id>&value=<pct>
#  Dashboard sets min confidence for a session
# ─────────────────────────────────────────────────────────────
@app.route('/session/confidence', methods=['PUT', 'POST'])
def set_confidence():
    sid = request.args.get('session') or \
          (request.get_json(silent=True) or {}).get('session', '')
    val = float(request.args.get('value', 0) or
                (request.get_json(silent=True) or {}).get('value', 60))
    val = max(50.0, min(95.0, val))
    sessions.update_min_confidence(sid, val)
    return jsonify({'ok': True, 'min_confidence': val})

# ═══════════════════════════════════════════════════════════════
# 12.  SIMPLE STATUS HTML  (redirect point when users open /dashboard)
# ═══════════════════════════════════════════════════════════════
def _build_status_html(sid: str) -> str:
    insts = candles.instruments()
    rows  = ''
    for p in insts[:30]:
        price  = candles.get_live_price(p)
        cnt    = candles.candle_counts(p).get('1s', 0)
        ob     = orderbook.get_contrarian_signal(p, 60)
        sent   = sentiment.get_latest(p)
        sv     = sent['sentiment'] if sent else '—'
        csig   = ob['signal']
        csig_c = '#0f0' if csig == 'UP' else '#f44' if csig == 'DOWN' else '#888'
        rows  += f"""
          <tr>
            <td><b>{p}</b></td>
            <td>{f'{price:.5f}' if price else '—'}</td>
            <td>{cnt}</td>
            <td style="color:{csig_c}">{csig} ({ob['weight_up']}%↑)</td>
            <td>{sv}</td>
          </tr>"""
    return f"""<!DOCTYPE html><html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<title>Quantum AI Server</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box;}}
  body{{background:#0a0f0a;color:#0f0;font-family:monospace;padding:24px;}}
  h1{{font-size:1.5em;text-shadow:0 0 12px #0f0;margin-bottom:16px;}}
  .pill{{display:inline-block;background:#0f01;border:1px solid #0f0;
         border-radius:20px;padding:4px 14px;margin:0 6px 12px 0;font-size:.85em;}}
  table{{width:100%;border-collapse:collapse;margin-top:16px;}}
  th{{text-align:left;border-bottom:1px solid #0f0;padding:6px 10px;color:#0a0;font-size:.8em;}}
  td{{padding:6px 10px;font-size:.85em;border-bottom:1px solid #111;}}
  tr:hover td{{background:#011;}}
  .note{{color:#555;font-size:.75em;margin-top:16px;}}
</style></head><body>
<h1>🧠 Quantum OlympTrade AI — Data Server</h1>
<div>
  <span class="pill">✅ LIVE</span>
  <span class="pill">Instruments: {len(insts)}</span>
  <span class="pill">Session: {sid[:12] + '…' if sid else '—'}</span>
</div>
<table>
  <tr><th>Instrument</th><th>Live Price</th>
      <th>1s Candles</th><th>Contrarian</th><th>Sentiment</th></tr>
  {rows if rows else '<tr><td colspan="5" style="color:#555;padding:20px;">Waiting for data from OlympTrade…</td></tr>'}
</table>
<p class="note">Auto-refreshes every 3 seconds. Full dashboard is in Google Colab.</p>
</body></html>"""

# ═══════════════════════════════════════════════════════════════
# 13.  UTILS
# ═══════════════════════════════════════════════════════════════
def _fmt_uptime(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = int(sec % 60)
    return f'{h:02d}h {m:02d}m {s:02d}s'

# ═══════════════════════════════════════════════════════════════
# 14.  ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print('═' * 60)
    print('  🧠 QUANTUM OLYMPTRADE AI — RENDER SERVER v2.0')
    print('═' * 60)
    print(f'  Data dir : {CFG.DATA_DIR.resolve()}')
    print(f'  Port     : {CFG.PORT}')
    print(f'  Timeframes: {len(CFG.TIMEFRAMES)}')
    print()
    print('  Endpoints:')
    print('   POST /tick              ← Tampermonkey sends data here')
    print('   GET  /state/<pair>      ← Colab AI pulls all features')
    print('   GET  /candles/<pair>/<tf>  ← Colab pulls candle history')
    print('   GET  /orderbook/<pair>  ← Colab pulls contrarian signal')
    print('   POST /prediction        ← Colab pushes predictions')
    print('   GET  /notifications     ← Tampermonkey polls for alerts')
    print('   GET  /instruments       ← List all active instruments')
    print('   GET  /accuracy          ← Overall AI accuracy')
    print('   GET  /health            ← Keep-alive ping')
    print('   GET  /dashboard         ← Status page')
    print('═' * 60)
    app.run(host='0.0.0.0', port=CFG.PORT,
            debug=False, threaded=True, use_reloader=False)
