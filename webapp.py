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
# GLOBAL DATA
# ============================================================

delta_data = {}

delta_lock = threading.Lock()

delta_connected = False

delta_thread_started = False


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def number(value):
    try:
        return float(value)
    except Exception:
        return None


def delta_base(symbol):
    if not symbol:
        return None

    s = str(symbol).upper()

    if s.endswith("USD"):
        return s[:-3]

    return s


# ============================================================
# DELTA WEBSOCKET
# ============================================================

DELTA_WS = "wss://public-socket.india.delta.exchange"


def delta_open(ws):

    global delta_connected

    delta_connected = True

    message = {
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

    try:
        ws.send(json.dumps(message))
    except Exception:
        pass


def delta_message(ws, message):

    global delta_connected

    try:

        data = json.loads(message)

        if data.get("type") == "subscriptions":
            delta_connected = True
            return

        if data.get("type") != "funding_rate":
            return

        symbol = data.get("sy")

        if not symbol:
            return

        symbol = str(symbol).upper()

        rate = number(data.get("fr"))

        interval = number(data.get("fi"))

        next_funding = number(data.get("nfr"))

        if rate is None:
            return

        if interval is None or interval <= 0:
            interval = 28800

        base = delta_base(symbol)

        if not base:
            return

        next_ms = None

        if next_funding:
            next_ms = int(next_funding / 1000)

        item = {
            "symbol": symbol,
            "base": base,
            "rate_pct": rate,
            "interval_sec": int(interval),
            "next_funding_ms": next_ms,
            "updated_ms": now_ms()
        }

        with delta_lock:
            delta_data[base] = item

        delta_connected = True

    except Exception:
        pass


def delta_error(ws, error):

    global delta_connected

    delta_connected = False


def delta_close(ws, code, message):

    global delta_connected

    delta_connected = False


def delta_loop():

    global delta_connected

    if websocket is None:
        return

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
                ping_interval=25,
                ping_timeout=10
            )

        except Exception:

            delta_connected = False

        time.sleep(5)


def start_delta():

    global delta_thread_started

    if delta_thread_started:
        return

    delta_thread_started = True

    thread = threading.Thread(
        target=delta_loop,
        daemon=True
    )

    thread.start()


start_delta()


# ============================================================
# SERVER API
# ============================================================

@app.route("/api/data")
def api_data():

    with delta_lock:
        d = dict(delta_data)

    coins = sorted(d.keys())

    rows = []

    for coin in coins:

        rows.append({
            "base": coin,
            "delta": d.get(coin)
        })

    return jsonify({
        "rows": rows,
        "delta_count": len(d),
        "delta_connected": delta_connected,
        "server_time": now_ms()
    })


@app.route("/status")
def status():

    with delta_lock:
        count = len(delta_data)

    return jsonify({
        "delta": count,
        "delta_connected": delta_connected,
        "time": now_ms()
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

<title>Crypto Funding Gap Scanner</title>


<style>

body {
    font-family: Arial, sans-serif;
    margin: 0;
    padding: 12px;
    background: #f4f4f4;
}

h1 {
    font-size: 28px;
    margin: 10px 0 20px;
}

#statusBox {
    background: white;
    border-radius: 12px;
    padding: 18px;
    margin-bottom: 12px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.12);
}

.status {
    font-size: 16px;
    margin: 7px 0;
}

button {
    background: #222;
    color: white;
    border: 0;
    border-radius: 8px;
    padding: 12px 18px;
    font-size: 16px;
    margin-bottom: 15px;
}

#message {
    background: white;
    padding: 15px;
    border-radius: 8px;
    margin-bottom: 10px;
}

.tableWrap {
    width: 100%;
    overflow-x: auto;
    background: white;
}

table {
    width: 100%;
    min-width: 900px;
    border-collapse: collapse;
    background: white;
}

th {
    background: #222;
    color: white;
    padding: 10px 7px;
    cursor: pointer;
    position: sticky;
    top: 0;
    z-index: 2;
}

td {
    padding: 9px 7px;
    border-bottom: 1px solid #ddd;
    text-align: center;
}

td:first-child {
    text-align: left;
    font-weight: bold;
}

.common {
    background: white;
}

.deltaOnly {
    background: #fff8d9;
}

.binanceOnly {
    background: #e8f3ff;
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

</style>

</head>


<body>


<h1>
Crypto Funding Gap Scanner
</h1>


<div id="statusBox">

<div class="status">
Delta:
<span id="deltaCount">0</span>
</div>

<div class="status">
Binance:
<span id="binanceCount">0</span>
</div>

<div class="status">
Total:
<span id="totalCount">0</span>
</div>

<div class="status">
Common:
<span id="commonCount">0</span>
</div>

<div class="status">
Delta only:
<span id="deltaOnlyCount">0</span>
</div>

<div class="status">
Binance only:
<span id="binanceOnlyCount">0</span>
</div>

<div class="status">
Delta connection:
<span id="deltaConnection">
Connecting...
</span>
</div>

<div class="status">
Binance connection:
<span id="binanceConnection">
Connecting...
</span>
</div>

</div>


<button onclick="loadEverything()">
Refresh
</button>


<div id="message">
Loading...
</div>


<div class="tableWrap">

<table id="scannerTable">

<thead>

<tr>

<th>
Coin
</th>

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

<th onclick="toggleSort()">
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

</div>


<script>


// ==========================================================
// BINANCE DATA
// ==========================================================

let binanceData = {};

let binanceSocket = null;

let reconnectTimer = null;

let binanceConnected = false;


// ==========================================================
// SORT
// ==========================================================

let sortMode = "none";


// ==========================================================
// FORMAT
// ==========================================================

function rateText(value) {

    if (
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ) {
        return "-";
    }

    return Number(value).toFixed(6) + "%";
}


function intervalText(seconds) {

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

    if (s >= 3600) {

        return (s / 3600).toFixed(2) + "h";

    }

    return Math.round(s / 60) + "m";
}


function gapText(value) {

    if (
        value === null ||
        value === undefined ||
        !Number.isFinite(Number(value))
    ) {
        return "-";
    }

    let n = Number(value);

    if (n >= 0) {
        return "+" + n.toFixed(6) + "%";
    }

    return n.toFixed(6) + "%";
}


function timeText(ms) {

    if (
        ms === null ||
        ms === undefined ||
        !Number.isFinite(Number(ms))
    ) {
        return "-";
    }

    try {

        return new Date(
            Number(ms)
        ).toLocaleTimeString();

    } catch(e) {

        return "-";
    }
}


// ==========================================================
// BINANCE MESSAGE PROCESSOR
// ==========================================================

function processBinanceMessage(message) {

    try {

        let payload = message;

        /*
         * Combined Binance stream:
         *
         * {
         *   "stream": "...",
         *   "data": [...]
         * }
         */

        if (
            message &&
            message.data !== undefined
        ) {

            payload = message.data;
        }


        /*
         * Some streams may provide a single object.
         */

        if (!Array.isArray(payload)) {

            payload = [payload];
        }


        let received = 0;


        for (let x of payload) {

            if (!x) {
                continue;
            }


            let symbol =
                String(
                    x.s || ""
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


            /*
             * Binance funding rate is decimal.
             *
             * Example:
             *
             * 0.0001
             *
             * means:
             *
             * 0.01%
             */

            let funding =
                Number(x.r);


            if (!Number.isFinite(funding)) {
                continue;
            }


            let ratePct =
                funding * 100;


            /*
             * Next funding timestamp.
             */

            let nextFunding =
                Number(x.T);


            if (
                !Number.isFinite(nextFunding) ||
                nextFunding <= 0
            ) {

                nextFunding = null;
            }


            /*
             * Default interval:
             *
             * 8 hours.
             *
             * fundingInfo can update this later.
             */

            let interval =
                28800;


            binanceData[base] = {

                symbol: symbol,

                base: base,

                rate_pct: ratePct,

                interval_sec: interval,

                next_funding_ms:
                    nextFunding,

                updated_ms:
                    Date.now()
            };


            received++;
        }


        if (received > 0) {

            binanceConnected = true;

            updateConnectionText();

        }


    } catch(e) {

        console.log(
            "Binance parse error",
            e
        );
    }
}


// ==========================================================
// BINANCE WEBSOCKET
// ==========================================================

function connectBinance() {

    try {

        if (binanceSocket) {

            try {
                binanceSocket.close();
            } catch(e) {}

        }


        /*
         * Combined Binance Futures stream.
         *
         * This is deliberately used instead of
         * the raw /ws endpoint so we can handle
         * the {stream,data} format reliably.
         */

        let url =
            "wss://fstream.binance.com/stream?streams=!markPrice@arr@1s";


        binanceSocket =
            new WebSocket(url);


        binanceSocket.onopen =
            function() {

                binanceConnected = true;

                updateConnectionText();

                console.log(
                    "Binance WebSocket connected"
                );
            };


        binanceSocket.onmessage =
            function(event) {

                try {

                    let message =
                        JSON.parse(
                            event.data
                        );

                    processBinanceMessage(
                        message
                    );

                    renderStatus();

                } catch(e) {

                    console.log(
                        "Binance message error",
                        e
                    );
                }
            };


        binanceSocket.onerror =
            function(error) {

                console.log(
                    "Binance WebSocket error",
                    error
                );

                binanceConnected = false;

                updateConnectionText();
            };


        binanceSocket.onclose =
            function() {

                console.log(
                    "Binance WebSocket closed"
                );

                binanceConnected = false;

                updateConnectionText();


                if (reconnectTimer) {

                    clearTimeout(
                        reconnectTimer
                    );
                }


                reconnectTimer =
                    setTimeout(
                        function() {

                            connectBinance();

                        },
                        5000
                    );
            };


    } catch(e) {

        console.log(
            "Binance connection error",
            e
        );

        binanceConnected = false;

        updateConnectionText();

        setTimeout(
            connectBinance,
            5000
        );
    }
}


// ==========================================================
// BINANCE REST FALLBACK
// ==========================================================

async function binanceRestFallback() {

    try {

        /*
         * Browser-side request.
         *
         * Render server does NOT make this request.
         */

        let response =
            await fetch(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                {
                    cache: "no-store"
                }
            );


        if (!response.ok) {

            console.log(
                "Binance REST status:",
                response.status
            );

            return;
        }


        let data =
            await response.json();


        if (!Array.isArray(data)) {
            return;
        }


        let received = 0;


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


            let funding =
                Number(
                    x.lastFundingRate
                );


            if (!Number.isFinite(funding)) {
                continue;
            }


            let next =
                Number(
                    x.nextFundingTime
                );


            if (
                !Number.isFinite(next) ||
                next <= 0
            ) {

                next = null;
            }


            binanceData[base] = {

                symbol: symbol,

                base: base,

                rate_pct:
                    funding * 100,

                interval_sec:
                    28800,

                next_funding_ms:
                    next,

                updated_ms:
                    Date.now()
            };


            received++;
        }


        if (received > 0) {

            binanceConnected = true;

            updateConnectionText();

            renderStatus();

            renderTable();

        }


    } catch(e) {

        console.log(
            "Binance REST fallback failed:",
            e
        );
    }
}


// ==========================================================
// FUNDING INTERVAL
// ==========================================================

async function loadFundingIntervals() {

    try {

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


            if (!binanceData[base]) {
                continue;
            }


            let hours =
                Number(
                    x.fundingIntervalHours
                );


            if (
                Number.isFinite(hours) &&
                hours > 0
            ) {

                binanceData[
                    base
                ].interval_sec =
                    hours * 3600;
            }
        }


        renderTable();


    } catch(e) {

        console.log(
            "Funding interval error",
            e
        );
    }
}


// ==========================================================
// LOAD DELTA
// ==========================================================

let deltaRows = [];


async function loadDelta() {

    try {

        let response =
            await fetch(
                "/api/data?t=" +
                Date.now(),
                {
                    cache: "no-store"
                }
            );


        if (!response.ok) {
            throw new Error(
                "Server API error"
            );
        }


        let data =
            await response.json();


        deltaRows =
            data.rows || [];


        document.getElementById(
            "deltaConnection"
        ).innerText =
            data.delta_connected
                ? "Connected"
                : "Disconnected";


        renderStatus();

        renderTable();


        document.getElementById(
            "message"
        ).innerText =
            "Live data";


    } catch(e) {

        console.log(
            "Delta API error",
            e
        );


        document.getElementById(
            "message"
        ).innerText =
            "Waiting for server data...";
    }
}


// ==========================================================
// STATUS
// ==========================================================

function renderStatus() {

    let deltaCount =
        deltaRows.length;


    let binanceCount =
        Object.keys(
            binanceData
        ).length;


    let deltaSet =
        new Set(
            deltaRows.map(
                x => x.base
            )
        );


    let common = 0;

    for (
        let coin of deltaSet
    ) {

        if (
            binanceData[coin]
        ) {

            common++;
        }
    }


    let deltaOnly =
        deltaCount -
        common;


    let binanceOnly =
        binanceCount -
        common;


    let total =
        deltaCount +
        binanceOnly;


    document.getElementById(
        "deltaCount"
    ).innerText =
        deltaCount;


    document.getElementById(
        "binanceCount"
    ).innerText =
        binanceCount;


    document.getElementById(
        "totalCount"
    ).innerText =
        total;


    document.getElementById(
        "commonCount"
    ).innerText =
        common;


    document.getElementById(
        "deltaOnlyCount"
    ).innerText =
        deltaOnly;


    document.getElementById(
        "binanceOnlyCount"
    ).innerText =
        binanceOnly;


    updateConnectionText();
}


// ==========================================================
// CONNECTION TEXT
// ==========================================================

function updateConnectionText() {

    document.getElementById(
        "binanceConnection"
    ).innerText =
        binanceConnected
            ? "Connected"
            : "Disconnected";
}


// ==========================================================
// BUILD ROWS
// ==========================================================

function buildRows() {

    let map = {};


    /*
     * Add Delta coins.
     */

    for (let row of deltaRows) {

        let coin =
            row.base;

        map[coin] = {

            base: coin,

            delta:
                row.delta || null,

            binance:
                null
        };
    }


    /*
     * Add Binance coins.
     */

    for (
        let coin in binanceData
    ) {

        if (!map[coin]) {

            map[coin] = {

                base: coin,

                delta: null,

                binance:
                    binanceData[coin]
            };

        } else {

            map[coin].binance =
                binanceData[coin];
        }
    }


    let rows =
        Object.values(map);


    /*
     * Calculate gap.
     */

    for (let row of rows) {

        row.gap = null;


        if (
            row.delta &&
            row.binance
        ) {

            let dr =
                Number(
                    row.delta.rate_pct
                );


            let di =
                Number(
                    row.delta.interval_sec ||
                    28800
                );


            let br =
                Number(
                    row.binance.rate_pct
                );


            let bi =
                Number(
                    row.binance.interval_sec ||
                    28800
                );


            if (
                Number.isFinite(dr) &&
                Number.isFinite(di) &&
                Number.isFinite(br) &&
                Number.isFinite(bi)
            ) {

                let deltaHourly =
                    dr *
                    3600 /
                    di;


                let binanceHourly =
                    br *
                    3600 /
                    bi;


                row.gap =
                    deltaHourly -
                    binanceHourly;
            }
        }
    }


    /*
     * Status.
     */

    for (let row of rows) {

        if (
            row.delta &&
            row.binance
        ) {

            row.status =
                "common";

        } else if (
            row.delta
        ) {

            row.status =
                "deltaOnly";

        } else {

            row.status =
                "binanceOnly";
        }
    }


    return rows;
}


// ==========================================================
// SORT
// ==========================================================

function toggleSort() {

    if (sortMode === "none") {

        sortMode = "desc";

    } else if (
        sortMode === "desc"
    ) {

        sortMode = "asc";

    } else {

        sortMode = "none";
    }


    renderTable();
}


function sortRows(rows) {

    if (sortMode === "none") {

        rows.sort(
            function(a, b) {

                return String(
                    a.base
                ).localeCompare(
                    String(b.base)
                );
            }
        );

        return rows;
    }


    rows.sort(
        function(a, b) {

            let av =
                a.gap;

            let bv =
                b.gap;


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


            av =
                Math.abs(
                    Number(av)
                );


            bv =
                Math.abs(
                    Number(bv)
                );


            if (
                sortMode === "desc"
            ) {

                return bv - av;

            } else {

                return av - bv;
            }
        }
    );


    return rows;
}


// ==========================================================
// TABLE
// ==========================================================

function renderTable() {

    let rows =
        buildRows();


    rows =
        sortRows(rows);


    let tbody =
        document.getElementById(
            "tableBody"
        );


    tbody.innerHTML = "";


    for (let row of rows) {

        let tr =
            document.createElement(
                "tr"
            );


        if (
            row.status ===
            "deltaOnly"
        ) {

            tr.className =
                "deltaOnly";

        } else if (
            row.status ===
            "binanceOnly"
        ) {

            tr.className =
                "binanceOnly";

        } else {

            tr.className =
                "common";
        }


        let deltaRate =
            row.delta
                ? rateText(
                    row.delta.rate_pct
                )
                : "-";


        let deltaInterval =
            row.delta
                ? intervalText(
                    row.delta.interval_sec
                )
                : "-";


        let binanceRate =
            row.binance
                ? rateText(
                    row.binance.rate_pct
                )
                : "-";


        let binanceInterval =
            row.binance
                ? intervalText(
                    row.binance.interval_sec
                )
                : "-";


        let gap =
            gapText(
                row.gap
            );


        let next =
            "-";


        if (
            row.binance &&
            row.binance.next_funding_ms
        ) {

            next =
                timeText(
                    row.binance.next_funding_ms
                );

        } else if (
            row.delta &&
            row.delta.next_funding_ms
        ) {

            next =
                timeText(
                    row.delta.next_funding_ms
                );
        }


        let status =
            row.status ===
            "common"
                ? "Common"
                : (
                    row.status ===
                    "deltaOnly"
                        ? "Delta only"
                        : "Binance only"
                );


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
${gap}
</td>

<td>
${next}
</td>

<td>
${status}
</td>

`;


        tbody.appendChild(tr);
    }


    renderStatus();
}


// ==========================================================
// ESCAPE
// ==========================================================

function escapeHtml(value) {

    return String(value)
        .replace(
            /&/g,
            "&amp;"
        )
        .replace(
            /</g,
            "&lt;"
        )
        .replace(
            />/g,
            "&gt;"
        )
        .replace(
            /"/g,
            "&quot;"
        )
        .replace(
            /'/g,
            "&#039;"
        );
}


// ==========================================================
// EVERYTHING
// ==========================================================

async function loadEverything() {

    await loadDelta();

    renderStatus();

    renderTable();
}


// ==========================================================
// START BINANCE
// ==========================================================

connectBinance();


// ==========================================================
// START DELTA
// ==========================================================

loadDelta();


// ==========================================================
// BINANCE INTERVALS
// ==========================================================

loadFundingIntervals();


// ==========================================================
// REST FALLBACK
// ==========================================================

setTimeout(
    function() {

        if (
            Object.keys(
                binanceData
            ).length === 0
        ) {

            binanceRestFallback();
        }

    },
    5000
);


// ==========================================================
// AUTO REFRESH
// ==========================================================

setInterval(
    function() {

        loadDelta();

    },
    3000
);


setInterval(
    function() {

        renderStatus();

        renderTable();

    },
    3000
);


setInterval(
    function() {

        loadFundingIntervals();

    },
    60000
);


setInterval(
    function() {

        if (
            Object.keys(
                binanceData
            ).length === 0
        ) {

            binanceRestFallback();
        }

    },
    10000
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

    return render_template_string(
        HTML
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=10000,
        debug=False
    )
