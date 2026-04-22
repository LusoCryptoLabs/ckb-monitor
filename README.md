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
node ckb-monitor.mjs --help-ish   # see the flags below
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

## Other flags

| Flag | Default |
| --- | --- |
| `--net testnet` or `CKB_NET=testnet` | `mainnet` |
| `--ws <url>` or `CKB_WS=<url>` | public endpoint for the chosen net |
| `--summary-ms <ms>` | `30000` |

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

## License

MIT
