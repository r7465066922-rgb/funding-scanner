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


# =========================================================
# DELTA LIVE FUNDING
# =========================================================

def delta_message(ws, message):
    try:
        msg = json.loads(message)

        data = msg.get("d", msg)

        if isinstance(data, list):
            items = data
        else:
            items = [data]

        for x in items:
            if not isinstance(x, dict):
                continue

            if x.get("type") != "funding_rate":
                continue

            symbol = x.get("sy")
            rate = x.get("fr")
            interval = x.get("fi")

            if not symbol or rate is None:
                continue

            # Example: BTCUSD -> BTC
            coin = symbol.upper()

            if coin.endswith("USD"):
                coin = coin[:-3]

            try:
                rate = float(rate)
                interval = int(interval or 28800)
            except Exception:
                continue

            with lock:
                delta_data[coin] = {
                    "symbol": symbol,
                    "rate": rate,
                    "interval": interval,
                    "updated": time.time()
                }

    except Exception as e:
        print("Delta message error:", e)


def delta_open(ws):
    print("Delta WebSocket connected")

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

    ws.send(json.dumps(payload))


def delta_error(ws, error):
    print("Delta error:", error)


def delta_close(ws, code, msg):
    print("Delta disconnected:", code, msg)


def delta_loop():
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
                ping_timeout=10
            )

        except Exception as e:
            print("Delta reconnect:", e)

        time.sleep(5)


# =========================================================
# BINANCE LIVE FUNDING
# =========================================================

def update_binance():
    while True:
        try:
            # Current funding rates
            r = requests.get(
                BINANCE_URL,
                timeout=10
            )

            r.raise_for_status()
            data = r.json()

            new_data = {}

            for x in data:
                symbol = x.get("symbol", "")

                if not symbol.endswith("USDT"):
                    continue

                rate = x.get("lastFundingRate")

                if rate is None or rate == "":
                    continue

                try:
                    rate = float(rate)
                except Exception:
                    continue

                coin = symbol[:-4]

                new_data[coin] = {
                    "symbol": symbol,
                    "rate": rate,
                    "nextFunding": x.get("nextFundingTime"),
                    "updated": time.time()
                }

            with lock:
                binance_data.clear()
                binance_data.update(new_data)

        except Exception as e:
            print("Binance error:", e)

        # Funding interval information
        try:
            r2 = requests.get(
                BINANCE_INFO_URL,
                timeout=10
            )

            if r2.ok:
                info = r2.json()

                with lock:
                    for x in info:
                        symbol = x.get("symbol", "")

                        if not symbol.endswith("USDT"):
                            continue

                        coin = symbol[:-4]

                        hours = x.get("fundingIntervalHours")

                        if hours:
                            try:
                                binance_intervals[coin] = float(hours)
                            except Exception:
                                pass

        except Exception as e:
            print("Binance interval info:", e)

        time.sleep(10)


# =========================================================
# API
# =========================================================

@app.route("/api/data")
def api_data():

    rows = []

    with lock:
        delta = dict(delta_data)
        binance = dict(binance_data)
        intervals = dict(binance_intervals)

    common = set(delta.keys()) & set(binance.keys())

    for coin in common:

        d = delta[coin]
        b = binance[coin]

        delta_rate = d["rate"]
        delta_interval = d["interval"]

        binance_rate = b["rate"]

        binance_hours = intervals.get(coin, 8.0)
        binance_interval = binance_hours * 3600

        # -------------------------------------------------
        # IMPORTANT:
        # Delta "fr" is already percentage units.
        # Binance funding rate is decimal, so *100.
        # -------------------------------------------------

        delta_pct = delta_rate
        binance_pct = binance_rate * 100

        # Hourly equivalent in percentage units
        delta_hourly_pct = (
            delta_rate * 3600 / delta_interval
        )

        binance_hourly_pct = (
            binance_rate * 100 * 3600 / binance_interval
        )

        gap_hourly_pct = (
            delta_hourly_pct - binance_hourly_pct
        )

        rows.append({
            "coin": coin,

            "delta": delta_pct,
            "delta_interval": round(
                delta_interval / 3600, 2
            ),

            "binance": binance_pct,
            "binance_interval": round(
                binance_interval / 3600, 2
            ),

            "delta_hourly": delta_hourly_pct,
            "binance_hourly": binance_hourly_pct,

            "gap": gap_hourly_pct,
            "abs_gap": abs(gap_hourly_pct)
        })

    # Default API order = biggest absolute gap first
    rows.sort(
        key=lambda x: x["abs_gap"],
        reverse=True
    )

    return jsonify({
        "success": True,
        "count": len(rows),
        "updated": time.time(),
        "data": rows
    })


# =========================================================
# WEB PAGE
# =========================================================

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
    background: #111;
    color: #eee;
    margin: 0;
    padding: 15px;
}

h1 {
    font-size: 22px;
}

.status {
    margin-bottom: 12px;
    color: #aaa;
}

.controls {
    margin-bottom: 12px;
}

button {
    padding: 8px 14px;
    border: 0;
    border-radius: 6px;
    cursor: pointer;
}

.table-wrap {
    overflow-x: auto;
}

table {
    border-collapse: collapse;
    width: 100%;
    min-width: 850px;
}

th, td {
    padding: 9px;
    border-bottom: 1px solid #333;
    text-align: right;
}

th {
    background: #222;
    position: sticky;
    top: 0;
}

th:first-child,
td:first-child {
    text-align: left;
}

#gapHeader {
    cursor: pointer;
    user-select: none;
}

#gapHeader:active {
    background: #444;
}

.positive {
    color: #00e676;
}

.negative {
    color: #ff5252;
}

.big {
    font-weight: bold;
}

</style>

</head>

<body>

<h1>⚡ Delta India ↔ Binance Funding Scanner</h1>

<div class="status">
    Coins:
    <span id="count">0</span>
    |
    Last update:
    <span id="time">-</span>
</div>

<div class="controls">
    <button onclick="loadData()">Refresh</button>
</div>

<div class="table-wrap">

<table>

<thead>

<tr>

<th>Coin</th>

<th>Delta %</th>
<th>Delta H</th>

<th>Binance %</th>
<th>Binance H</th>

<th>Delta / H</th>
<th>Binance / H</th>

<th id="gapHeader"
    onclick="toggleGapSort()"
    aria-sort="none">
    Gap / H ↕
</th>

</tr>

</thead>

<tbody id="rows"></tbody>

</table>

</div>


<script>

let gapSortMode = "none";


function fmt(x) {

    if (x === null || x === undefined) {
        return "-";
    }

    return Number(x).toFixed(5) + "%";
}


function renderRows(data) {

    const tbody = document.getElementById("rows");

    let rows = [...data];

    // Sort according to user's selected mode
    if (gapSortMode === "desc") {

        rows.sort(
            (a, b) => b.abs_gap - a.abs_gap
        );

    } else if (gapSortMode === "asc") {

        rows.sort(
            (a, b) => a.abs_gap - b.abs_gap
        );

    } else {

        // Default: biggest gap first
        rows.sort(
            (a, b) => b.abs_gap - a.abs_gap
        );
    }

    tbody.innerHTML = "";

    for (const x of rows) {

        const tr = document.createElement("tr");

        const gapClass =
            x.gap >= 0
            ? "positive"
            : "negative";

        tr.dataset.gap = x.abs_gap;

        tr.innerHTML = `

            <td><b>${x.coin}</b></td>

            <td>${fmt(x.delta)}</td>
            <td>${x.delta_interval}h</td>

            <td>${fmt(x.binance)}</td>
            <td>${x.binance_interval}h</td>

            <td>${fmt(x.delta_hourly)}</td>
            <td>${fmt(x.binance_hourly)}</td>

            <td class="${gapClass} big">
                ${fmt(x.gap)}
            </td>

        `;

        tbody.appendChild(tr);
    }
}


function toggleGapSort() {

    if (gapSortMode === "none" ||
        gapSortMode === "asc") {

        // First tap = biggest
        gapSortMode = "desc";

    } else {

        // Second tap = smallest
        gapSortMode = "asc";
    }

    const header =
        document.getElementById("gapHeader");

    if (gapSortMode === "desc") {

        header.innerText = "Gap / H ↓";
        header.setAttribute(
            "aria-sort",
            "descending"
        );

    } else {

        header.innerText = "Gap / H ↑";
        header.setAttribute(
            "aria-sort",
            "ascending"
        );
    }

    // Re-sort currently displayed rows
    const tbody =
        document.getElementById("rows");

    const rows =
        Array.from(
            tbody.querySelectorAll("tr")
        );

    rows.sort((a, b) => {

        const aGap =
            parseFloat(a.dataset.gap);

        const bGap =
            parseFloat(b.dataset.gap);

        return gapSortMode === "desc"
            ? bGap - aGap
            : aGap - bGap;
    });

    rows.forEach(row => {
        tbody.appendChild(row);
    });
}


async function loadData() {

    try {

        const response =
            await fetch("/api/data");

        const result =
            await response.json();

        document.getElementById("count")
            .innerText = result.count;

        document.getElementById("time")
            .innerText =
            new Date(
                result.updated * 1000
            ).toLocaleTimeString();

        renderRows(result.data);

    } catch (e) {

        console.log(e);

        document.getElementById("time")
            .innerText = "Error";
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


# =========================================================
# START BACKGROUND THREADS
# =========================================================

def start_background_threads():

    if not any(
        t.name == "delta-thread"
        for t in threading.enumerate()
    ):
        threading.Thread(
            target=delta_loop,
            name="delta-thread",
            daemon=True
        ).start()

    if not any(
        t.name == "binance-thread"
        for t in threading.enumerate()
    ):
        threading.Thread(
            target=update_binance,
            name="binance-thread",
            daemon=True
        ).start()


start_background_threads()


# =========================================================
# LOCAL RUN
# =========================================================

if __name__ == "__main__":

    print("")
    print("=================================")
    print(" Funding Rate Scanner")
    print(" http://127.0.0.1:5000")
    print("=================================")
    print("")

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )
