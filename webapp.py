import json
import time
import threading
import requests
import websocket

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

DELTA_WS = "wss://public-socket.india.delta.exchange"
BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_INFO_URL = "https://fapi.binance.com/fapi/v1/fundingInfo"

delta_data = {}
binance_data = {}
binance_intervals = {}

lock = threading.Lock()

engine_started = False
engine_lock = threading.Lock()

last_delta_error = ""
last_binance_error = ""
last_delta_update = 0
last_binance_update = 0


# =========================
# DELTA WEBSOCKET
# =========================

def delta_message(ws, message):
    global last_delta_update

    try:
        msg = json.loads(message)

        data = msg.get("d", msg)

        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            return

        changed = False

        with lock:
            for item in items:
                if not isinstance(item, dict):
                    continue

                if item.get("type") != "funding_rate":
                    continue

                symbol = item.get("sy")
                rate = item.get("fr")
                interval = item.get("fi")

                if not symbol or rate is None:
                    continue

                symbol = str(symbol).upper()

                if symbol.endswith("USD"):
                    coin = symbol[:-3]
                else:
                    coin = symbol

                try:
                    rate = float(rate)
                except Exception:
                    continue

                try:
                    interval = int(interval) if interval else 28800
                except Exception:
                    interval = 28800

                delta_data[coin] = {
                    "symbol": symbol,
                    "rate": rate,
                    "interval": interval,
                    "updated": time.time()
                }

                changed = True

        if changed:
            last_delta_update = time.time()

    except Exception as e:
        last_delta_error = str(e)


def delta_open(ws):
    print("[DELTA] WebSocket connected", flush=True)

    payload = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": "funding_rate",
                    "symbols": ["perpetual_futures"]
                }
            ]
        }
    }

    try:
        ws.send(json.dumps(payload))
        print("[DELTA] Funding channel subscribed", flush=True)
    except Exception as e:
        print("[DELTA] Subscribe error:", e, flush=True)


def delta_error(ws, error):
    global last_delta_error
    last_delta_error = str(error)
    print("[DELTA] ERROR:", error, flush=True)


def delta_close(ws, close_status_code, close_msg):
    print(
        "[DELTA] Disconnected:",
        close_status_code,
        close_msg,
        flush=True
    )


def delta_loop():
    global last_delta_error

    print("[DELTA] Thread started", flush=True)

    while True:
        try:
            ws = websocket.WebSocketApp(
                DELTA_WS,
                on_open=delta_open,
                on_message=delta_message,
                on_error=delta_error,
                on_close=delta_close
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
                http_proxy_host=None,
                http_proxy_port=None,
                proxy_type=None
            )

        except Exception as e:
            last_delta_error = str(e)
            print("[DELTA] Connection exception:", e, flush=True)

        print("[DELTA] Reconnecting in 5 seconds...", flush=True)
        time.sleep(5)


# =========================
# BINANCE
# =========================

def update_binance():
    global last_binance_error
    global last_binance_update

    print("[BINANCE] Thread started", flush=True)

    headers = {
        "User-Agent": "Mozilla/5.0 funding-scanner"
    }

    while True:

        try:
            response = requests.get(
                BINANCE_URL,
                headers=headers,
                timeout=15
            )

            response.raise_for_status()

            data = response.json()

            new_data = {}

            for item in data:

                symbol = item.get("symbol", "")

                if not symbol.endswith("USDT"):
                    continue

                coin = symbol[:-4]

                try:
                    rate = float(item.get("lastFundingRate", 0))
                except Exception:
                    continue

                try:
                    next_time = int(item.get("nextFundingTime", 0))
                except Exception:
                    next_time = 0

                new_data[coin] = {
                    "symbol": symbol,
                    "rate": rate,
                    "nextFundingTime": next_time,
                    "updated": time.time()
                }

            with lock:
                binance_data.clear()
                binance_data.update(new_data)

            last_binance_update = time.time()

            print(
                "[BINANCE] Funding coins:",
                len(new_data),
                flush=True
            )

        except Exception as e:
            last_binance_error = str(e)
            print("[BINANCE] ERROR:", e, flush=True)

        # Funding interval information
        try:
            response = requests.get(
                BINANCE_INFO_URL,
                headers=headers,
                timeout=15
            )

            response.raise_for_status()

            info = response.json()

            new_intervals = {}

            for item in info:

                symbol = item.get("symbol", "")

                if not symbol.endswith("USDT"):
                    continue

                coin = symbol[:-4]

                hours = item.get("fundingIntervalHours")

                if hours is None:
                    continue

                try:
                    hours = float(hours)
                except Exception:
                    continue

                if hours > 0:
                    new_intervals[coin] = hours * 3600

            with lock:
                binance_intervals.clear()
                binance_intervals.update(new_intervals)

        except Exception as e:
            last_binance_error = str(e)
            print(
                "[BINANCE] Interval ERROR:",
                e,
                flush=True
            )

        time.sleep(15)


# =========================
# ENGINE START
# =========================

def start_engine():
    global engine_started

    with engine_lock:

        if engine_started:
            return

        engine_started = True

        print("[ENGINE] Starting data engine...", flush=True)

        delta_thread = threading.Thread(
            target=delta_loop,
            name="delta-thread",
            daemon=True
        )

        binance_thread = threading.Thread(
            target=update_binance,
            name="binance-thread",
            daemon=True
        )

        delta_thread.start()
        binance_thread.start()

        print("[ENGINE] Threads started", flush=True)


# =========================
# API
# =========================

@app.route("/api/data")
def api_data():

    # Important:
    # Start only after Gunicorn worker receives a request.
    # This makes Render/Gunicorn much more reliable.
    start_engine()

    with lock:
        delta = dict(delta_data)
        binance = dict(binance_data)
        intervals = dict(binance_intervals)

    common = sorted(
        set(delta.keys()) & set(binance.keys())
    )

    rows = []

    for coin in common:

        try:
            d = delta[coin]
            b = binance[coin]

            delta_rate = float(d["rate"])
            delta_interval = int(d.get("interval") or 28800)

            binance_rate = float(b["rate"])

            binance_interval = intervals.get(
                coin,
                28800
            )

            if delta_interval <= 0:
                delta_interval = 28800

            if binance_interval <= 0:
                binance_interval = 28800

            # Delta FR is already percentage.
            delta_pct = delta_rate

            # Binance FR is decimal.
            binance_pct = binance_rate * 100

            # Convert both to hourly percentage.
            delta_hourly_pct = (
                delta_pct *
                3600 /
                delta_interval
            )

            binance_hourly_pct = (
                binance_pct *
                3600 /
                binance_interval
            )

            gap_hourly_pct = (
                delta_hourly_pct -
                binance_hourly_pct
            )

            rows.append({
                "coin": coin,

                "delta_rate": delta_pct,
                "delta_interval": delta_interval,

                "binance_rate": binance_pct,
                "binance_interval": binance_interval,

                "delta_hourly": delta_hourly_pct,
                "binance_hourly": binance_hourly_pct,

                "gap_hourly": gap_hourly_pct,

                "delta_symbol": d.get("symbol"),
                "binance_symbol": b.get("symbol")
            })

        except Exception:
            continue

    # Default: biggest absolute gap first
    rows.sort(
        key=lambda x: abs(x["gap_hourly"]),
        reverse=True
    )

    return jsonify({
        "success": True,
        "count": len(rows),
        "updated": time.time(),

        "debug": {
            "delta_count": len(delta),
            "binance_count": len(binance),
            "common_count": len(common),

            "engine_started": engine_started,

            "delta_updated": last_delta_update,
            "binance_updated": last_binance_update,

            "last_delta_error": last_delta_error,
            "last_binance_error": last_binance_error
        },

        "data": rows
    })


# =========================
# STATUS
# =========================

@app.route("/status")
def status():

    with lock:
        dc = len(delta_data)
        bc = len(binance_data)

    return jsonify({
        "status": "running",
        "engine_started": engine_started,
        "delta_count": dc,
        "binance_count": bc,
        "delta_last_update": last_delta_update,
        "binance_last_update": last_binance_update,
        "delta_error": last_delta_error,
        "binance_error": last_binance_error
    })


# =========================
# WEB PAGE
# =========================

HTML = """
<!DOCTYPE html>
<html>
<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>Funding Rate Scanner</title>

<style>

body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 12px;
    background: #f5f5f5;
}

h2 {
    margin: 5px 0 10px;
}

#status {
    background: white;
    padding: 10px;
    border-radius: 8px;
    margin-bottom: 10px;
    font-size: 13px;
}

.table-wrap {
    overflow-x: auto;
    background: white;
    border-radius: 8px;
}

table {
    border-collapse: collapse;
    width: 100%;
    min-width: 900px;
}

th {
    background: #222;
    color: white;
    padding: 10px 7px;
    position: sticky;
    top: 0;
    cursor: pointer;
}

td {
    padding: 8px 7px;
    border-bottom: 1px solid #ddd;
    text-align: right;
    white-space: nowrap;
}

td:first-child,
th:first-child {
    text-align: left;
}

.gap-positive {
    font-weight: bold;
}

.gap-negative {
    font-weight: bold;
}

.small {
    font-size: 11px;
    color: #666;
}

button {
    padding: 8px 12px;
    border: 0;
    border-radius: 6px;
    background: #222;
    color: white;
}

</style>

</head>

<body>

<h2>Funding Rate Gap Scanner</h2>

<div id="status">
    Loading...
</div>

<div class="table-wrap">

<table>

<thead>

<tr>

<th>Coin</th>

<th>Delta FR</th>

<th>Delta Interval</th>

<th>Binance FR</th>

<th>Binance Interval</th>

<th>Delta / H</th>

<th>Binance / H</th>

<th id="gapHeader"
    onclick="toggleGapSort()">
    Gap / H ↕
</th>

</tr>

</thead>

<tbody id="rows">
</tbody>

</table>

</div>


<script>

let allRows = [];

let gapSortMode = "none";


function formatRate(value) {

    if (value === null || value === undefined) {
        return "-";
    }

    return Number(value).toFixed(6) + "%";
}


function formatInterval(seconds) {

    let hours = Number(seconds) / 3600;

    if (hours === 1) {
        return "1h";
    }

    if (hours === 2) {
        return "2h";
    }

    if (hours === 4) {
        return "4h";
    }

    if (hours === 8) {
        return "8h";
    }

    return hours.toFixed(2) + "h";
}


function renderRows(rows) {

    let sorted = [...rows];

    if (gapSortMode === "desc") {

        sorted.sort(
            (a, b) =>
                Math.abs(b.gap_hourly) -
                Math.abs(a.gap_hourly)
        );

    } else if (gapSortMode === "asc") {

        sorted.sort(
            (a, b) =>
                Math.abs(a.gap_hourly) -
                Math.abs(b.gap_hourly)
        );
    }

    const body = document.getElementById("rows");

    body.innerHTML = "";

    for (const r of sorted) {

        const tr = document.createElement("tr");

        let gapClass =
            r.gap_hourly >= 0
                ? "gap-positive"
                : "gap-negative";

        tr.innerHTML = `

<td>
    <b>${r.coin}</b>
</td>

<td>
    ${formatRate(r.delta_rate)}
</td>

<td>
    ${formatInterval(r.delta_interval)}
</td>

<td>
    ${formatRate(r.binance_rate)}
</td>

<td>
    ${formatInterval(r.binance_interval)}
</td>

<td>
    ${formatRate(r.delta_hourly)}
</td>

<td>
    ${formatRate(r.binance_hourly)}
</td>

<td class="${gapClass}">
    ${formatRate(r.gap_hourly)}
</td>

`;

        body.appendChild(tr);
    }
}


function toggleGapSort() {

    if (gapSortMode === "none") {

        gapSortMode = "desc";

    } else if (gapSortMode === "desc") {

        gapSortMode = "asc";

    } else {

        gapSortMode = "desc";
    }

    renderRows(allRows);

    const header =
        document.getElementById("gapHeader");

    if (gapSortMode === "desc") {

        header.innerText = "Gap / H ↓";

    } else {

        header.innerText = "Gap / H ↑";
    }
}


async function loadData() {

    try {

        const response =
            await fetch("/api/data?t=" + Date.now());

        const result =
            await response.json();

        if (!result.success) {
            throw new Error("API error");
        }

        allRows = result.data || [];

        renderRows(allRows);

        const d =
            result.debug || {};

        document.getElementById("status").innerHTML =
            "Coins: <b>" + result.count + "</b>" +
            " | Delta: <b>" + (d.delta_count || 0) + "</b>" +
            " | Binance: <b>" + (d.binance_count || 0) + "</b>" +
            " | Common: <b>" + (d.common_count || 0) + "</b>" +
            "<br><span class='small'>" +
            "Auto refresh: 10 sec" +
            "</span>";

    } catch (error) {

        document.getElementById("status").innerHTML =
            "<b>API Error:</b> " +
            error.message;
    }
}


loadData();

setInterval(
    loadData,
    10000
);

</script>

</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


# =========================
# LOCAL RUN
# =========================

if __name__ == "__main__":

    import os

    port = int(
        os.environ.get(
            "PORT",
            "5000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
