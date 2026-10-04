import json
import time
import threading
from flask import Flask, jsonify, render_template_string
import websocket

app = Flask(__name__)

# ============================================================
# DATA
# ============================================================

delta_data = {}
delta_lock = threading.Lock()
engine_started = False
engine_lock = threading.Lock()

# ============================================================
# DELTA WEBSOCKET
# ============================================================

DELTA_WS = "wss://public-socket.india.delta.exchange"


def delta_on_open(ws):
    print("[DELTA] Connected")

    payload = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": "funding_rate",
                    "symbols": ["all"]
                }
            ]
        }
    }

    ws.send(json.dumps(payload))
    print("[DELTA] Subscribed to funding_rate / all")


def delta_on_message(ws, message):
    try:
        data = json.loads(message)

        # Funding message
        if data.get("type") == "funding_rate":
            symbol = data.get("sy") or data.get("symbol")

            if not symbol:
                return

            fr = data.get("fr")
            fi = data.get("fi")
            next_time = data.get("nfr")

            if fr is None:
                fr = data.get("funding_rate")

            if fi is None:
                fi = data.get("funding_interval")

            if fr is None:
                return

            try:
                fr = float(fr)
            except:
                return

            try:
                fi = int(fi) if fi else 28800
            except:
                fi = 28800

            # Delta fr is already percentage.
            # Example: 0.00638 means 0.00638%
            with delta_lock:
                delta_data[symbol] = {
                    "symbol": symbol,
                    "rate_pct": fr,
                    "interval": fi,
                    "next_funding": next_time,
                    "updated": time.time()
                }

    except Exception as e:
        print("[DELTA] MESSAGE ERROR:", e)


def delta_on_error(ws, error):
    print("[DELTA] ERROR:", error)


def delta_on_close(ws, code, msg):
    print("[DELTA] CLOSED:", code, msg)


def delta_worker():
    while True:
        try:
            print("[DELTA] Connecting...")

            ws = websocket.WebSocketApp(
                DELTA_WS,
                on_open=delta_on_open,
                on_message=delta_on_message,
                on_error=delta_on_error,
                on_close=delta_on_close
            )

            ws.run_forever(
                ping_interval=25,
                ping_timeout=10
            )

        except Exception as e:
            print("[DELTA] CONNECTION ERROR:", e)

        print("[DELTA] Reconnecting in 5 seconds...")
        time.sleep(5)


# ============================================================
# ENGINE
# ============================================================

def start_engine():
    global engine_started

    with engine_lock:
        if engine_started:
            return

        engine_started = True

        t = threading.Thread(
            target=delta_worker,
            daemon=True
        )
        t.start()

        print("[ENGINE] Started")


# ============================================================
# API
# ============================================================

@app.route("/")
def home():
    start_engine()
    return render_template_string(HTML)


@app.route("/api/data")
def api_data():
    start_engine()

    with delta_lock:
        data = list(delta_data.values())

    return jsonify({
        "delta": data,
        "delta_count": len(data),
        "server_time": int(time.time() * 1000)
    })


@app.route("/status")
def status():
    with delta_lock:
        count = len(delta_data)

    return jsonify({
        "status": "ok",
        "delta_count": count,
        "engine_started": engine_started
    })


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>Funding Rate Gap Scanner</title>

<style>

body {
    font-family: Arial, sans-serif;
    background: #111;
    color: white;
    margin: 0;
    padding: 12px;
}

h2 {
    margin: 5px 0 10px 0;
}

.status {
    background: #1d1d1d;
    padding: 10px;
    border-radius: 8px;
    margin-bottom: 10px;
    font-size: 14px;
}

.connected {
    color: #00e676;
}

.disconnected {
    color: #ff5252;
}

.loading {
    color: #ffca28;
}

.table-wrap {
    overflow-x: auto;
    border-radius: 8px;
}

table {
    width: 100%;
    min-width: 850px;
    border-collapse: collapse;
    background: #1a1a1a;
}

th {
    background: #252525;
    position: sticky;
    top: 0;
    padding: 10px 6px;
    font-size: 13px;
}

td {
    padding: 9px 6px;
    border-bottom: 1px solid #333;
    text-align: center;
    font-size: 13px;
}

.coin {
    font-weight: bold;
    text-align: left;
}

.gap {
    font-weight: bold;
    font-size: 14px;
}

button {
    background: #333;
    color: white;
    border: 0;
    padding: 8px 12px;
    border-radius: 6px;
}

.small {
    font-size: 11px;
    color: #aaa;
}

</style>
</head>

<body>

<h2>Funding Rate Gap Scanner</h2>

<div class="status">

<div>
Delta:
<span id="deltaStatus" class="loading">Connecting...</span>
</div>

<div>
Binance:
<span id="binanceStatus" class="loading">Connecting...</span>
</div>

<div>
Common:
<span id="commonStatus">0</span>
</div>

<div class="small">
Binance data: browser WebSocket
</div>

</div>

<div class="table-wrap">

<table>

<thead>

<tr>

<th>Coin</th>

<th>Delta<br>Funding</th>

<th>Delta<br>Interval</th>

<th>Binance<br>Funding</th>

<th>Binance<br>Interval</th>

<th id="gapHeader" onclick="toggleGapSort()">
Gap / H ↕
</th>

<th>Next Funding</th>

</tr>

</thead>

<tbody id="rows">

<tr>
<td colspan="7">Connecting...</td>
</tr>

</tbody>

</table>

</div>


<script>

let deltaData = {};

let binanceData = {};

let gapSortMode = "none";

let binanceWS = null;

let reconnectTimer = null;

let binanceIntervalMap = {};


// ============================================================
// BINANCE INTERVAL
// ============================================================

async function loadBinanceIntervals() {

    try {

        const response = await fetch(
            "https://fapi.binance.com/fapi/v1/fundingInfo",
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error("HTTP " + response.status);
        }

        const data = await response.json();

        if (Array.isArray(data)) {

            for (const x of data) {

                if (x.symbol && x.fundingIntervalHours) {

                    binanceIntervalMap[
                        x.symbol
                    ] = Number(x.fundingIntervalHours);

                }

            }

        }

        console.log(
            "Binance funding intervals loaded:",
            Object.keys(binanceIntervalMap).length
        );

    } catch (e) {

        console.log(
            "Binance fundingInfo unavailable. Using 8h default.",
            e
        );

    }

}


// ============================================================
// BINANCE WEBSOCKET
// ============================================================

function connectBinance() {

    if (binanceWS) {

        try {
            binanceWS.close();
        } catch(e) {}

    }

    document.getElementById(
        "binanceStatus"
    ).textContent = "Connecting...";

    document.getElementById(
        "binanceStatus"
    ).className = "loading";


    const url =
        "wss://fstream.binance.com/market/ws/!markPrice@arr@1s";


    try {

        binanceWS = new WebSocket(url);

    } catch (e) {

        console.log("Binance WS create error:", e);

        scheduleBinanceReconnect();

        return;
    }


    binanceWS.onopen = function() {

        console.log("Binance WebSocket connected");

        document.getElementById(
            "binanceStatus"
        ).textContent = "Connected";

        document.getElementById(
            "binanceStatus"
        ).className = "connected";
    };


    binanceWS.onmessage = function(event) {

        try {

            const payload = JSON.parse(event.data);

            let arr = payload;

            // Combined wrapper support
            if (payload && payload.data) {
                arr = payload.data;
            }

            if (!Array.isArray(arr)) {
                arr = [arr];
            }


            for (const item of arr) {

                if (!item) continue;

                const symbol = item.s;

                if (!symbol) continue;

                // Only USDT-M symbols
                if (!symbol.endsWith("USDT")) {
                    continue;
                }

                const rate = Number(item.r);

                if (!Number.isFinite(rate)) {
                    continue;
                }

                const intervalHours =
                    Number(
                        binanceIntervalMap[symbol]
                    ) || 8;

                binanceData[symbol] = {

                    symbol: symbol,

                    rateDecimal: rate,

                    ratePct: rate * 100,

                    intervalHours: intervalHours,

                    intervalSeconds:
                        intervalHours * 3600,

                    nextFunding: item.T || null,

                    updated: Date.now()
                };

            }


            updateBinanceStatus();

            renderRows();

        } catch (e) {

            console.log(
                "Binance message error:",
                e
            );

        }

    };


    binanceWS.onerror = function(error) {

        console.log(
            "Binance WebSocket error",
            error
        );

        document.getElementById(
            "binanceStatus"
        ).textContent = "Error";

        document.getElementById(
            "binanceStatus"
        ).className = "disconnected";
    };


    binanceWS.onclose = function() {

        console.log(
            "Binance WebSocket closed"
        );

        document.getElementById(
            "binanceStatus"
        ).textContent = "Disconnected";

        document.getElementById(
            "binanceStatus"
        ).className = "disconnected";

        scheduleBinanceReconnect();
    };

}


function scheduleBinanceReconnect() {

    if (reconnectTimer) {
        return;
    }

    reconnectTimer = setTimeout(
        function() {

            reconnectTimer = null;

            connectBinance();

        },
        5000
    );

}


// ============================================================
// BINANCE STATUS
// ============================================================

function updateBinanceStatus() {

    const count =
        Object.keys(binanceData).length;

    document.getElementById(
        "binanceStatus"
    ).textContent =
        "Connected (" + count + ")";

    document.getElementById(
        "binanceStatus"
    ).className = "connected";

}


// ============================================================
// DELTA DATA
// ============================================================

async function loadDelta() {

    try {

        const response =
            await fetch(
                "/api/data?t=" + Date.now(),
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();

        deltaData = {};

        if (Array.isArray(data.delta)) {

            for (const x of data.delta) {

                if (!x.symbol) continue;

                const coin =
                    x.symbol
                    .replace(/USD$/, "");

                deltaData[coin] = {

                    symbol: x.symbol,

                    ratePct:
                        Number(x.rate_pct),

                    intervalSeconds:
                        Number(x.interval) || 28800,

                    nextFunding:
                        x.next_funding || null
                };

            }

        }

        document.getElementById(
            "deltaStatus"
        ).textContent =
            "Connected (" +
            Object.keys(deltaData).length +
            ")";

        document.getElementById(
            "deltaStatus"
        ).className = "connected";


        renderRows();

    } catch (e) {

        console.log(
            "Delta API error:",
            e
        );

        document.getElementById(
            "deltaStatus"
        ).textContent = "Error";

        document.getElementById(
            "deltaStatus"
        ).className = "disconnected";

    }

}


// ============================================================
// FORMAT
// ============================================================

function formatRate(x) {

    if (!Number.isFinite(x)) {
        return "-";
    }

    return x.toFixed(5) + "%";

}


function formatInterval(seconds) {

    if (!seconds) {
        return "-";
    }

    const h =
        seconds / 3600;

    if (Number.isInteger(h)) {
        return h + "h";
    }

    return h.toFixed(2) + "h";

}


function formatTime(timestamp) {

    if (!timestamp) {
        return "-";
    }

    let ms =
        Number(timestamp);

    // Delta nfr can be microseconds
    if (ms > 100000000000000) {
        ms = ms / 1000;
    }

    if (ms < 10000000000) {
        ms = ms * 1000;
    }

    const d =
        new Date(ms);

    if (isNaN(d.getTime())) {
        return "-";
    }

    return d.toLocaleTimeString(
        [],
        {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit"
        }
    );

}


// ============================================================
// RENDER
// ============================================================

function renderRows() {

    const tbody =
        document.getElementById("rows");

    const rows = [];


    for (const coin in deltaData) {

        const d =
            deltaData[coin];

        const b =
            binanceData[coin + "USDT"];

        if (!b) {
            continue;
        }


        const deltaHourly =
            d.ratePct *
            3600 /
            d.intervalSeconds;


        const binanceHourly =
            b.ratePct *
            3600 /
            b.intervalSeconds;


        const gap =
            deltaHourly -
            binanceHourly;


        rows.push({

            coin: coin,

            deltaRate:
                d.ratePct,

            deltaInterval:
                d.intervalSeconds,

            binanceRate:
                b.ratePct,

            binanceInterval:
                b.intervalSeconds,

            gap: gap,

            nextFunding:
                b.nextFunding ||
                d.nextFunding
        });

    }


    // Sort
    if (gapSortMode === "desc") {

        rows.sort(
            (a,b) =>
                Math.abs(b.gap) -
                Math.abs(a.gap)
        );

    }

    else if (gapSortMode === "asc") {

        rows.sort(
            (a,b) =>
                Math.abs(a.gap) -
                Math.abs(b.gap)
        );

    }

    else {

        rows.sort(
            (a,b) =>
                Math.abs(b.gap) -
                Math.abs(a.gap)
        );

    }


    document.getElementById(
        "commonStatus"
    ).textContent =
        rows.length;


    if (rows.length === 0) {

        tbody.innerHTML =
            '<tr><td colspan="7">' +
            'Waiting for Delta + Binance data...' +
            '</td></tr>';

        return;
    }


    let html = "";


    for (const r of rows) {

        const gapText =
            (r.gap >= 0 ? "+" : "") +
            r.gap.toFixed(5) +
            "%";


        html +=
            "<tr>" +

            "<td class='coin'>" +
            r.coin +
            "</td>" +

            "<td>" +
            formatRate(r.deltaRate) +
            "</td>" +

            "<td>" +
            formatInterval(r.deltaInterval) +
            "</td>" +

            "<td>" +
            formatRate(r.binanceRate) +
            "</td>" +

            "<td>" +
            formatInterval(r.binanceInterval) +
            "</td>" +

            "<td class='gap'>" +
            gapText +
            "</td>" +

            "<td>" +
            formatTime(r.nextFunding) +
            "</td>" +

            "</tr>";
    }


    tbody.innerHTML = html;

}


// ============================================================
// GAP SORT
// ============================================================

function toggleGapSort() {

    if (gapSortMode === "none") {

        gapSortMode = "desc";

        document.getElementById(
            "gapHeader"
        ).textContent =
            "Gap / H ↓";

    }

    else if (gapSortMode === "desc") {

        gapSortMode = "asc";

        document.getElementById(
            "gapHeader"
        ).textContent =
            "Gap / H ↑";

    }

    else {

        gapSortMode = "none";

        document.getElementById(
            "gapHeader"
        ).textContent =
            "Gap / H ↕";

    }

    renderRows();

}


// ============================================================
// START
// ============================================================

loadBinanceIntervals();

connectBinance();

loadDelta();

setInterval(
    loadDelta,
    10000
);

</script>

</body>
</html>
"""


# ============================================================
# START APP
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=10000
    )
