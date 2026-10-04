import json
import threading
import time
from flask import Flask, jsonify, render_template_string

try:
    import websocket
except Exception:
    websocket = None


app = Flask(__name__)

# ============================================================
# DATA STORAGE
# ============================================================

delta_data = {}
binance_data = {}

delta_connected = False
binance_browser_seen = False

delta_lock = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def clean_number(value):
    try:
        return float(value)
    except Exception:
        return None


def base_from_delta(symbol):
    """
    Delta:
      BTCUSD -> BTC
      ETHUSD -> ETH
      SOLUSD -> SOL
    """
    if not symbol:
        return None

    s = str(symbol).upper()

    if s.endswith("USD"):
        return s[:-3]

    return s


def base_from_binance(symbol):
    """
    Binance:
      BTCUSDT -> BTC
      ETHUSDT -> ETH
      SOLUSDT -> SOL
    """
    if not symbol:
        return None

    s = str(symbol).upper()

    if s.endswith("USDT"):
        return s[:-4]

    return None


# ============================================================
# DELTA WEBSOCKET
# ============================================================

DELTA_WS_URL = "wss://public-socket.india.delta.exchange"


def delta_on_message(ws, message):
    global delta_connected

    try:
        data = json.loads(message)

        # Subscription confirmation
        if data.get("type") == "subscriptions":
            delta_connected = True
            return

        # Funding rate message
        if data.get("type") != "funding_rate":
            return

        symbol = data.get("sy")

        if not symbol:
            return

        symbol = str(symbol).upper()

        # Delta fr is already in percentage units.
        # Example:
        # 0.00638 = 0.00638%
        fr = clean_number(data.get("fr"))

        # fi = funding interval in seconds
        fi = clean_number(data.get("fi"))

        # nfr = next funding realization in microseconds
        nfr = clean_number(data.get("nfr"))

        if fr is None:
            return

        if fi is None or fi <= 0:
            fi = 28800

        base = base_from_delta(symbol)

        if not base:
            return

        next_funding_ms = None

        if nfr:
            next_funding_ms = int(nfr / 1000)

        item = {
            "symbol": symbol,
            "base": base,
            "rate_pct": fr,
            "interval_sec": int(fi),
            "next_funding_ms": next_funding_ms,
            "updated_ms": now_ms(),
        }

        with delta_lock:
            delta_data[base] = item

        delta_connected = True

    except Exception:
        pass


def delta_on_error(ws, error):
    global delta_connected
    delta_connected = False


def delta_on_close(ws, close_status_code, close_msg):
    global delta_connected
    delta_connected = False


def delta_on_open(ws):
    global delta_connected

    delta_connected = True

    subscribe_message = {
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

    ws.send(json.dumps(subscribe_message))


def delta_loop():
    global delta_connected

    if websocket is None:
        return

    while True:
        try:
            ws = websocket.WebSocketApp(
                DELTA_WS_URL,
                on_open=delta_on_open,
                on_message=delta_on_message,
                on_error=delta_on_error,
                on_close=delta_on_close,
            )

            ws.run_forever(
                ping_interval=25,
                ping_timeout=10,
            )

        except Exception:
            delta_connected = False

        time.sleep(5)


# ============================================================
# START DELTA THREAD
# ============================================================

delta_thread_started = False


def start_delta_thread():
    global delta_thread_started

    if delta_thread_started:
        return

    delta_thread_started = True

    thread = threading.Thread(
        target=delta_loop,
        daemon=True
    )

    thread.start()


start_delta_thread()


# ============================================================
# API
# ============================================================

@app.route("/api/data")
def api_data():

    with delta_lock:
        dcopy = dict(delta_data)

    bcopy = dict(binance_data)

    all_bases = sorted(
        set(dcopy.keys()) | set(bcopy.keys())
    )

    rows = []

    for base in all_bases:

        d = dcopy.get(base)
        b = bcopy.get(base)

        row = {
            "base": base,
            "delta": d,
            "binance": b,
            "status": "common" if d and b else (
                "delta_only" if d else "binance_only"
            ),
            "gap_hourly_pct": None,
        }

        # ----------------------------------------------------
        # GAP CALCULATION
        # ----------------------------------------------------
        if d and b:

            delta_rate = d.get("rate_pct")
            delta_interval = d.get("interval_sec") or 28800

            binance_rate = b.get("rate_pct")
            binance_interval = b.get("interval_sec") or 28800

            try:
                delta_hourly = (
                    float(delta_rate)
                    * 3600.0
                    / float(delta_interval)
                )

                binance_hourly = (
                    float(binance_rate)
                    * 3600.0
                    / float(binance_interval)
                )

                row["gap_hourly_pct"] = (
                    delta_hourly - binance_hourly
                )

            except Exception:
                row["gap_hourly_pct"] = None

        rows.append(row)

    common_count = sum(
        1 for x in rows
        if x["status"] == "common"
    )

    delta_only_count = sum(
        1 for x in rows
        if x["status"] == "delta_only"
    )

    binance_only_count = sum(
        1 for x in rows
        if x["status"] == "binance_only"
    )

    result = {
        "rows": rows,

        "delta_count": len(dcopy),
        "binance_count": len(bcopy),
        "total_count": len(all_bases),

        "common_count": common_count,
        "delta_only_count": delta_only_count,
        "binance_only_count": binance_only_count,

        "delta_connected": delta_connected,

        "server_time": now_ms(),
    }

    return jsonify(result)


@app.route("/status")
def status():

    with delta_lock:
        dc = len(delta_data)

    bc = len(binance_data)

    return jsonify({
        "delta": dc,
        "binance": bc,
        "total": len(set(delta_data.keys()) | set(binance_data.keys())),
        "delta_connected": delta_connected,
        "binance_browser": binance_browser_seen,
    })


# ============================================================
# HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Funding Gap Scanner</title>

<style>

body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 12px;
    background: #f4f4f4;
}

h2 {
    margin-top: 0;
}

#statusBox {
    background: white;
    padding: 12px;
    border-radius: 10px;
    margin-bottom: 10px;
    box-shadow: 0 1px 5px rgba(0,0,0,0.12);
}

.statusLine {
    margin: 4px 0;
    font-size: 14px;
}

.connected {
    font-weight: bold;
}

table {
    width: 100%;
    border-collapse: collapse;
    background: white;
    font-size: 12px;
}

th {
    background: #222;
    color: white;
    padding: 8px 5px;
    position: sticky;
    top: 0;
    cursor: pointer;
}

td {
    padding: 7px 5px;
    border-bottom: 1px solid #ddd;
    text-align: center;
}

td:first-child {
    text-align: left;
    font-weight: bold;
}

.common {
    background: #ffffff;
}

.deltaOnly {
    background: #fff8dc;
}

.binanceOnly {
    background: #eaf4ff;
}

.positive {
    font-weight: bold;
}

.negative {
    font-weight: bold;
}

.small {
    font-size: 11px;
}

#loading {
    padding: 20px;
    text-align: center;
}

button {
    padding: 8px 12px;
    margin-bottom: 10px;
    border: 0;
    border-radius: 7px;
    background: #222;
    color: white;
}

</style>

</head>

<body>

<h2>Crypto Funding Gap Scanner</h2>

<div id="statusBox">

    <div class="statusLine">
        Delta:
        <span id="deltaStatus">0</span>
    </div>

    <div class="statusLine">
        Binance:
        <span id="binanceStatus">0</span>
    </div>

    <div class="statusLine">
        Total:
        <span id="totalStatus">0</span>
    </div>

    <div class="statusLine">
        Common:
        <span id="commonStatus">0</span>
    </div>

    <div class="statusLine">
        Delta only:
        <span id="deltaOnlyStatus">0</span>
    </div>

    <div class="statusLine">
        Binance only:
        <span id="binanceOnlyStatus">0</span>
    </div>

    <div class="statusLine">
        Delta connection:
        <span id="deltaConnection">Connecting...</span>
    </div>

    <div class="statusLine">
        Binance connection:
        <span id="binanceConnection">Connecting...</span>
    </div>

</div>

<button onclick="refreshData()">
    Refresh
</button>

<div id="loading">
    Loading data...
</div>

<table id="dataTable" style="display:none;">

<thead>

<tr>

<th>Coin</th>

<th>
Delta Funding
</th>

<th>
Delta Interval
</th>

<th>
Binance Funding
</th>

<th>
Binance Interval
</th>

<th id="gapHeader" onclick="toggleGapSort()">
Gap / H ↕
</th>

<th>
Next Funding
</th>

<th>
Status
</th>

</tr>

</thead>

<tbody id="tableBody">
</tbody>

</table>


<script>

/* ==========================================================
   BINANCE DATA
   ========================================================== */

let binanceData = {};

let binanceSocket = null;

let binanceReconnectTimer = null;

let binanceConnected = false;


/* ==========================================================
   SORT
   ========================================================== */

let gapSortMode = "none";


function toggleGapSort() {

    if (gapSortMode === "none") {

        gapSortMode = "desc";

    } else if (gapSortMode === "desc") {

        gapSortMode = "asc";

    } else {

        gapSortMode = "none";
    }

    refreshData();
}


/* ==========================================================
   NUMBER FORMAT
   ========================================================== */

function fmtRate(value) {

    if (
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ) {
        return "-";
    }

    return Number(value).toFixed(6) + "%";
}


function fmtInterval(seconds) {

    if (
        seconds === null ||
        seconds === undefined
    ) {
        return "-";
    }

    let s = Number(seconds);

    if (!Number.isFinite(s)) {
        return "-";
    }

    let hours = s / 3600;

    if (hours >= 1) {
        return hours.toFixed(2) + "h";
    }

    return Math.round(s / 60) + "m";
}


function fmtGap(value) {

    if (
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ) {
        return "-";
    }

    let n = Number(value);

    return n.toFixed(6) + "%";
}


function fmtNext(ms) {

    if (
        ms === null ||
        ms === undefined ||
        !Number.isFinite(Number(ms))
    ) {
        return "-";
    }

    let d = new Date(Number(ms));

    return d.toLocaleTimeString();
}


/* ==========================================================
   BINANCE WEBSOCKET
   ========================================================== */

function connectBinance() {

    try {

        if (binanceSocket) {

            try {
                binanceSocket.close();
            } catch(e) {}

        }

        /*
         * Binance USD-M Futures all-symbol mark price stream.
         *
         * It gives:
         * s = symbol
         * r = funding rate
         * i = next funding time
         */

        let url =
            "wss://fstream.binance.com/ws/!markPrice@arr@1s";

        binanceSocket = new WebSocket(url);

        binanceSocket.onopen = function() {

            binanceConnected = true;

            document.getElementById(
                "binanceConnection"
            ).innerText = "Connected";

        };


        binanceSocket.onmessage = function(event) {

            try {

                let message =
                    JSON.parse(event.data);

                if (!Array.isArray(message)) {
                    return;
                }

                for (let x of message) {

                    if (!x) {
                        continue;
                    }

                    let symbol =
                        String(x.s || "").toUpperCase();

                    if (!symbol.endsWith("USDT")) {
                        continue;
                    }

                    let base =
                        symbol.substring(
                            0,
                            symbol.length - 4
                        );

                    if (!base) {
                        continue;
                    }

                    /*
                     * Binance r is decimal.
                     *
                     * Example:
                     * 0.0001 = 0.01%
                     *
                     * Therefore multiply by 100.
                     */

                    let funding =
                        Number(x.r);

                    if (!Number.isFinite(funding)) {
                        continue;
                    }

                    let ratePct =
                        funding * 100;

                    let nextFunding =
                        Number(x.T);

                    if (
                        !Number.isFinite(nextFunding) ||
                        nextFunding <= 0
                    ) {
                        nextFunding = null;
                    }

                    /*
                     * Default Binance funding interval
                     * is 8 hours.
                     *
                     * It can be replaced by fundingInfo
                     * if browser REST is available.
                     */

                    let intervalSec = 28800;

                    binanceData[base] = {

                        symbol: symbol,

                        base: base,

                        rate_pct: ratePct,

                        interval_sec:
                            intervalSec,

                        next_funding_ms:
                            nextFunding,

                        updated_ms:
                            Date.now()
                    };
                }

                binanceConnected = true;

            } catch(e) {

            }

        };


        binanceSocket.onerror = function() {

            binanceConnected = false;

            document.getElementById(
                "binanceConnection"
            ).innerText = "Error";

        };


        binanceSocket.onclose = function() {

            binanceConnected = false;

            document.getElementById(
                "binanceConnection"
            ).innerText = "Reconnecting...";

            if (binanceReconnectTimer) {
                clearTimeout(binanceReconnectTimer);
            }

            binanceReconnectTimer =
                setTimeout(
                    connectBinance,
                    5000
                );
        };


    } catch(e) {

        binanceConnected = false;

        setTimeout(
            connectBinance,
            5000
        );
    }
}


/* ==========================================================
   LOAD BINANCE FUNDING INTERVALS
   ========================================================== */

async function loadBinanceIntervals() {

    try {

        /*
         * This is browser-side.
         * Therefore Render server does not need
         * to call Binance REST.
         */

        let response =
            await fetch(
                "https://fapi.binance.com/fapi/v1/fundingInfo",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            return;
        }

        let data =
            await response.json();

        if (!Array.isArray(data)) {
            return;
        }

        for (let x of data) {

            let symbol =
                String(
                    x.symbol || ""
                ).toUpperCase();

            if (!symbol.endsWith("USDT")) {
                continue;
            }

            let base =
                symbol.substring(
                    0,
                    symbol.length - 4
                );

            if (!base) {
                continue;
            }

            let hours =
                Number(
                    x.fundingIntervalHours
                );

            if (
                Number.isFinite(hours) &&
                hours > 0 &&
                binanceData[base]
            ) {

                binanceData[base].interval_sec =
                    hours * 3600;
            }
        }

    } catch(e) {

        /*
         * If this fails, 8-hour fallback remains.
         */

    }
}


/* ==========================================================
   GET SERVER DATA
   ========================================================== */

async function refreshData() {

    try {

        let response =
            await fetch(
                "/api/data?t=" + Date.now(),
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            throw new Error("API error");
        }

        let data =
            await response.json();

        renderStatus(data);

        renderRows(data.rows || []);

    } catch(e) {

        document.getElementById(
            "loading"
        ).innerText =
            "Waiting for data...";

    }
}


/* ==========================================================
   STATUS
   ========================================================== */

function renderStatus(data) {

    document.getElementById(
        "deltaStatus"
    ).innerText =
        data.delta_count ?? 0;

    document.getElementById(
        "binanceStatus"
    ).innerText =
        data.binance_count ?? 0;

    document.getElementById(
        "totalStatus"
    ).innerText =
        data.total_count ?? 0;

    document.getElementById(
        "commonStatus"
    ).innerText =
        data.common_count ?? 0;

    document.getElementById(
        "deltaOnlyStatus"
    ).innerText =
        data.delta_only_count ?? 0;

    document.getElementById(
        "binanceOnlyStatus"
    ).innerText =
        data.binance_only_count ?? 0;

    document.getElementById(
        "deltaConnection"
    ).innerText =
        data.delta_connected
            ? "Connected"
            : "Disconnected";

    document.getElementById(
        "binanceConnection"
    ).innerText =
        binanceConnected
            ? "Connected"
            : "Connecting...";

}


/* ==========================================================
   RENDER TABLE
   ========================================================== */

function renderRows(rows) {

    /*
     * Replace server Binance data with
     * browser Binance data where available.
     *
     * This makes the table use the latest
     * Binance WebSocket values.
     */

    let merged = [];

    for (let row of rows) {

        let base = row.base;

        let b =
            binanceData[base];

        if (b) {

            row.binance = b;

            if (row.delta) {

                let deltaRate =
                    Number(
                        row.delta.rate_pct
                    );

                let deltaInterval =
                    Number(
                        row.delta.interval_sec ||
                        28800
                    );

                let binanceRate =
                    Number(
                        b.rate_pct
                    );

                let binanceInterval =
                    Number(
                        b.interval_sec ||
                        28800
                    );

                if (
                    Number.isFinite(deltaRate) &&
                    Number.isFinite(deltaInterval) &&
                    Number.isFinite(binanceRate) &&
                    Number.isFinite(binanceInterval)
                ) {

                    let deltaHourly =
                        deltaRate *
                        3600 /
                        deltaInterval;

                    let binanceHourly =
                        binanceRate *
                        3600 /
                        binanceInterval;

                    row.gap_hourly_pct =
                        deltaHourly -
                        binanceHourly;
                }

                row.status = "common";
            }
        }

        merged.push(row);
    }


    /*
     * Add Binance-only coins that may not yet
     * have appeared in server response.
     */

    for (
        let base in binanceData
    ) {

        let exists =
            merged.some(
                x => x.base === base
            );

        if (exists) {
            continue;
        }

        merged.push({

            base: base,

            delta: null,

            binance:
                binanceData[base],

            status: "binance_only",

            gap_hourly_pct: null
        });
    }


    /*
     * SORTING
     */

    if (gapSortMode === "desc") {

        merged.sort(
            function(a, b) {

                let av =
                    a.gap_hourly_pct;

                let bv =
                    b.gap_hourly_pct;

                if (
                    av === null ||
                    av === undefined
                ) {
                    return 1;
                }

                if (
                    bv === null ||
                    bv === undefined
                ) {
                    return -1;
                }

                return (
                    Math.abs(Number(bv)) -
                    Math.abs(Number(av))
                );
            }
        );

    } else if (gapSortMode === "asc") {

        merged.sort(
            function(a, b) {

                let av =
                    a.gap_hourly_pct;

                let bv =
                    b.gap_hourly_pct;

                if (
                    av === null ||
                    av === undefined
                ) {
                    return 1;
                }

                if (
                    bv === null ||
                    bv === undefined
                ) {
                    return -1;
                }

                return (
                    Math.abs(Number(av)) -
                    Math.abs(Number(bv))
                );
            }
        );

    } else {

        merged.sort(
            function(a, b) {

                return String(a.base)
                    .localeCompare(
                        String(b.base)
                    );
            }
        );
    }


    let tbody =
        document.getElementById(
            "tableBody"
        );

    tbody.innerHTML = "";


    for (let row of merged) {

        let tr =
            document.createElement("tr");


        if (row.status === "delta_only") {

            tr.className = "deltaOnly";

        } else if (
            row.status === "binance_only"
        ) {

            tr.className =
                "binanceOnly";

        } else {

            tr.className =
                "common";
        }


        let d =
            row.delta;

        let b =
            row.binance;


        let deltaRate =
            d
                ? fmtRate(d.rate_pct)
                : "-";

        let deltaInterval =
            d
                ? fmtInterval(
                    d.interval_sec
                )
                : "-";


        let binanceRate =
            b
                ? fmtRate(b.rate_pct)
                : "-";

        let binanceInterval =
            b
                ? fmtInterval(
                    b.interval_sec
                )
                : "-";


        let gap =
            row.gap_hourly_pct;


        let gapText =
            fmtGap(gap);


        if (
            gap !== null &&
            gap !== undefined &&
            Number.isFinite(Number(gap))
        ) {

            if (Number(gap) >= 0) {

                gapText =
                    "+" + gapText;

            }
        }


        let nextFunding =
            "-";

        if (d && d.next_funding_ms) {

            nextFunding =
                fmtNext(
                    d.next_funding_ms
                );

        } else if (
            b &&
            b.next_funding_ms
        ) {

            nextFunding =
                fmtNext(
                    b.next_funding_ms
                );
        }


        let statusText = "";

        if (
            row.status ===
            "delta_only"
        ) {

            statusText =
                "Delta only";

        } else if (
            row.status ===
            "binance_only"
        ) {

            statusText =
                "Binance only";

        } else {

            statusText =
                "Common";
        }


        tr.innerHTML = `

            <td>
                ${escapeHtml(row.base)}
            </td>

            <td>
                ${deltaRate}
            </td>

            <td>
                ${deltaInterval}
            </td>

            <td>
                ${binanceRate}
            </td>

            <td>
                ${binanceInterval}
            </td>

            <td>
                ${gapText}
            </td>

            <td>
                ${nextFunding}
            </td>

            <td>
                ${statusText}
            </td>

        `;

        tbody.appendChild(tr);
    }


    document.getElementById(
        "loading"
    ).style.display = "none";

    document.getElementById(
        "dataTable"
    ).style.display = "table";
}


/* ==========================================================
   HTML ESCAPE
   ========================================================== */

function escapeHtml(value) {

    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}


/* ==========================================================
   START
   ========================================================== */

connectBinance();

loadBinanceIntervals();

refreshData();


/*
 * Refresh table every 3 seconds.
 */

setInterval(
    refreshData,
    3000
);


/*
 * Try funding interval refresh every 60 seconds.
 */

setInterval(
    loadBinanceIntervals,
    60000
);

</script>

</body>
</html>
"""


# ============================================================
# HOME
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000,
        debug=False
    )
