# ckb-monitor

A live CKB (Nervos) on-chain monitor in a single file. It subscribes to the public
WebSocket (no node, no polling), classifies every transaction by type, tracks the
pending to committed lifecycle, and the point of the tool, fires **actions** when a
transaction matches rules you care about.

Zero dependencies on Node 22+ (built-in `WebSocket`). On older Node: `npm i ws`.

## Install

```bash
git clone <this-repo> ckb-monitor
cd ckb-monitor
# nothing to install on Node 22+
node ckb-monitor.mjs --help   # see all flags
```

Or run it directly:

```bash
node ckb-monitor.mjs
```

## Quick start

```bash
# just stream everything (mainnet)
node ckb-monitor.mjs

# testnet (Pudge)
node ckb-monitor.mjs --net testnet

# alert on moves of at least 1,000,000 CKB
node ckb-monitor.mjs --min-ckb 1000000

# deposit watch on an address, push to a webhook
node ckb-monitor.mjs --address ckb1q... --webhook https://hooks.example/...

# only show token (xUDT) activity, quietly
node ckb-monitor.mjs --type xudt --quiet

# run a shell command on every match
node ckb-monitor.mjs --type-args 0x... --exec "notify-send 'CKB' \"$CKB_TX_HASH\""
```

## Watch rules

Combine freely. A transaction must satisfy **all** given rules; commas mean **OR**
within one rule.

| Flag | Matches |
| --- | --- |
| `--address <ckb1.../ckt1...>[,...]` | outputs paying to these address(es) (incoming funds) |
| `--min-ckb <N>` | txs whose total output capacity is at least N CKB (whale watch) |
| `--type <alias/0xhash>[,...]` | an asset type: `dao`, `sudt`, `xudt`, `cota`, `unique`, `typeid`, or a raw code hash |
| `--type-args <0x...>[,...]` | a specific token (xUDT/sUDT args = token id) |
| `--lock-args <0x...>[,...]` | outputs by raw lock args (alternative to `--address`) |

## Actions on a match

- (always) prints a highlighted ALERT line
- `--webhook <url>` HTTP POST a JSON payload (Slack/Discord/your backend)
- `--exec "<cmd>"` run a shell command. Env vars passed in:
  `CKB_TX_HASH`, `CKB_TX_TYPE`, `CKB_TX_CKB`, `CKB_TX_EVENT`, `CKB_MATCHED`, `CKB_EXPLORER`
- `--quiet` show only alerts plus the periodic summary (good for background monitoring)

## Live dashboard

Add `--serve` to also run a tiny local web dashboard. It serves a single embedded
page (no framework, no build) and pushes every pool/block/commit/alert event to the
browser over Server-Sent Events. Works alongside all the watch rules and actions.

```bash
# stream testnet activity to a browser
node ckb-monitor.mjs --serve --net testnet           # open http://localhost:8080

# pick a port, and combine with rules
node ckb-monitor.mjs --serve 9000 --min-ckb 1000000  # dashboard + whale alerts
```

The page shows a live-updating, **paginated** table (newest first), per-type colors,
running stat cards (blocks / committed / alerts / in-pool), per-kind filters, and
pause / clear controls. Matching transactions are highlighted as alert rows and hashes
link to the explorer. The connection auto-reconnects, and a freshly opened tab immediately
sees the last ~250 events.

It also includes:

- **Network switch** — toggle testnet / mainnet from the header; the server reconnects
  its upstream and resets the view, no restart needed.
- **Live charts** (pure canvas): throughput (events/sec), tx per block, and type mix.
- **Node pulse** — an ambient visualization that emits a colored ripple per transaction.
- **Chain bar** — epoch progress, block interval, mempool depth (pending + proposed),
  and the median fee rate, plus a banner if the node reports network alerts.
- **Richer rows** — per-transaction **fee** and **cycles** (read for free from the
  mempool entry) and decoded **xUDT / sUDT token amounts** (raw, no decimals applied).

The list is paged (default 50 rows; selectable 25 / 50 / 100 / 200) with newer / older /
live controls, so it never grows without bound.

| Flag | Default |
| --- | --- |
| `--serve [port]` | off; default port `8080` |
| `--port <n>` or `PORT=<n>` | `8080` (alternative to `--serve <port>`) |

## Other flags

| Flag | Default |
| --- | --- |
| `--net testnet` or `CKB_NET=testnet` | `mainnet` |
| `--ws <url>` or `CKB_WS=<url>` | public endpoint for the chosen net |
| `--summary-ms <ms>` | `30000` |
| `-h`, `--help` | print usage and exit |

## Webhook payload

On a match, the JSON posted to `--webhook` looks like:

```json
{
  "network": "mainnet",
  "event": "commit",
  "txHash": "0x...",
  "type": "xUDT token",
  "capacityCKB": "12345.6",
  "matched": ["type=xUDT token"],
  "explorer": "https://explorer.nervos.org/transaction/0x...",
  "time": "2026-06-18T00:00:00.000Z"
}
```

## How it works

- Subscribes to `new_tip_header` and `new_transaction` over the JSON-RPC WebSocket.
- New mempool transactions are classified and (optionally) alerted as `pool` events.
- On each new tip it pulls the block, marks its transactions `commit`, and reports the
  time each spent in the pool.
- Addresses are decoded locally with a dependency-free bech32m implementation
  (full CKB2021 addresses only; use `--lock-args` for short/legacy forms).

## Development

No build step. Sanity-check the single file before committing:

```bash
npm run check   # node --check (syntax)
npm test        # syntax check + --help smoke test
```

## License

MIT
