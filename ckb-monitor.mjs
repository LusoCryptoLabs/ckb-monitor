#!/usr/bin/env node
// ckb-monitor.mjs — a live CKB on-chain monitor.
//
// It subscribes to the PUBLIC WebSocket (no node, no polling), classifies every
// transaction by type, tracks the pending->committed lifecycle, and — the point of
// the tool — fires ACTIONS when a transaction matches rules you care about.
//
// Zero dependencies on Node 22+ (built-in WebSocket). On older Node: `npm i ws`.
//
// ── Watch rules (combine freely; a tx must satisfy ALL given rules; commas = OR within one) ──
//   --address <ckb1…|ckt1…>[,…]   match outputs paying to these address(es)   (incoming funds)
//   --min-ckb <N>                 match txs whose total output capacity ≥ N CKB (whale watch)
//   --type <alias|0xhash>[,…]     match an asset type: dao,sudt,xudt,cota,unique,typeid, or a raw code hash
//   --type-args <0x…>[,…]         match a specific token (xUDT/sUDT args = token id)
//   --lock-args <0x…>[,…]         match outputs by raw lock args (alternative to --address)
//
// ── Actions on a match ──
//   (always) prints a highlighted 🔔 ALERT line
//   --webhook <url>               HTTP POST a JSON payload (Slack/Discord/your backend)
//   --exec "<cmd>"                run a shell command; env: CKB_TX_HASH, CKB_TX_TYPE, CKB_TX_CKB, CKB_TX_EVENT, CKB_MATCHED, CKB_EXPLORER
//   --quiet                       show only alerts + the periodic summary (good for background monitoring)
//
// ── Live dashboard ──
//   --serve [port]                browser dashboard (live table over SSE); default port 8080
//   --port <n>                    dashboard port (alt to --serve <port>)
//
// ── Other ──
//   --net testnet | CKB_NET=testnet      (default mainnet)
//   --ws <url> | CKB_WS=<url>            custom endpoint
//   --summary-ms <ms>                    summary cadence (default 30000)
//
// Examples:
//   node ckb-monitor.mjs --min-ckb 1000000                       # alert on moves ≥ 1M CKB
//   node ckb-monitor.mjs --address ckb1q… --webhook https://…   # deposit watch -> webhook
//   node ckb-monitor.mjs --type xudt --quiet                     # only show token activity
//   node ckb-monitor.mjs --type-args 0x… --exec "notify-send 'CKB' \"$CKB_TX_HASH\""

import { spawn } from "node:child_process";
import http from "node:http";

// ---------------- args ----------------
const argv = process.argv.slice(2);
const opt = (name, def = null) => {
  const i = argv.indexOf(name);
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : def;
};
const flag = (name) => argv.includes(name);
const csv = (name) => {
  const v = opt(name);
  return v ? v.split(",").map((s) => s.trim()).filter(Boolean) : [];
};

// ---------------- help ----------------
if (flag("--help") || flag("-h")) {
  console.log(`ckb-monitor  a live CKB on-chain monitor

USAGE
  node ckb-monitor.mjs [watch rules] [actions] [options]

  With no watch rules it just streams pool/block/commit activity. Add a rule
  to get ALERTs. A tx must satisfy ALL given rules; commas = OR within a rule.

WATCH RULES
  --address <ckb1.../ckt1...>[,...]   outputs paying to these address(es)
  --min-ckb <N>                       total output capacity >= N CKB (whale watch)
  --type <alias|0xhash>[,...]         dao, sudt, xudt, cota, unique, typeid, or a code hash
  --type-args <0x...>[,...]           a specific token (xUDT/sUDT args = token id)
  --lock-args <0x...>[,...]           outputs by raw lock args (alternative to --address)

ACTIONS ON A MATCH
  (always)                            print a highlighted ALERT line
  --webhook <url>                     HTTP POST a JSON payload (Slack/Discord/backend)
  --exec "<cmd>"                      run a shell command; env: CKB_TX_HASH, CKB_TX_TYPE,
                                      CKB_TX_CKB, CKB_TX_EVENT, CKB_MATCHED, CKB_EXPLORER
  --quiet                             only alerts + the periodic summary

DASHBOARD
  --serve [port]                      live browser dashboard over SSE (default port 8080)
  --port <n>                          dashboard port (alt to --serve <port>, or PORT env)

OPTIONS
  --net testnet|mainnet               (or CKB_NET=...)            default mainnet
  --ws <url>                          custom endpoint (or CKB_WS=...)
  --summary-ms <ms>                   summary cadence (or SUMMARY_MS=...)  default 30000
  -h, --help                          show this help

EXAMPLES
  node ckb-monitor.mjs --min-ckb 1000000
  node ckb-monitor.mjs --address ckb1q... --webhook https://...
  node ckb-monitor.mjs --type xudt --quiet
  node ckb-monitor.mjs --serve --net testnet            # open http://localhost:8080
  node ckb-monitor.mjs --serve 9000 --min-ckb 1000000   # dashboard + whale alerts
  node ckb-monitor.mjs --net testnet --type-args 0x... --exec "notify-send 'CKB' \\"$CKB_TX_HASH\\""`);
  process.exit(0);
}

// ---------------- endpoint ----------------
const NET = (opt("--net") || process.env.CKB_NET || "mainnet").toLowerCase();
const WS_URL =
  opt("--ws") ||
  process.env.CKB_WS ||
  (NET === "testnet" ? "wss://testnet.ckb.dev/ws" : "wss://mainnet.ckb.dev/ws");
const SUMMARY_EVERY_MS = Number(opt("--summary-ms") || process.env.SUMMARY_MS || 30000);
const EXPLORER = NET === "testnet"
  ? "https://pudge.explorer.nervos.org/transaction/"
  : "https://explorer.nervos.org/transaction/";

// ---------------- WebSocket impl ----------------
let WSImpl = globalThis.WebSocket;
if (!WSImpl) {
  try {
    WSImpl = (await import("ws")).default;
  } catch {
    console.error("Needs Node 22+ (built-in WebSocket), or run:  npm i ws");
    process.exit(1);
  }
}

// ---------------- known type scripts (RFC0024 + CCC), per network ----------------
const TYPE_DEFS = {
  mainnet: [
    { alias: "dao", label: "Nervos DAO", hash: "0x82d76d1b75fe2fd9a27dfbaa65a039221a380d76c926f378d3f81cf3e7e13f2e" },
    { alias: "sudt", label: "sUDT token", hash: "0x5e7a36a77e68eecc013dfa2fe6a23f3b6c344b04005808694ae6dd45eea4cfd5" },
    { alias: "xudt", label: "xUDT token", hash: "0x50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95" },
    { alias: "cota", label: "CoTA NFT", hash: "0x1122a4fb54697cf2e6e3a96c9d80fd398a936559b90954c6e88eb7ba0cf652df" },
    { alias: "unique", label: "Unique cell", hash: "0x2c8c11c985da60b0a330c61a85507416d6382c130ba67f0c47ab071e00aec628" },
    { alias: "typeid", label: "Type ID", hash: "0x00000000000000000000000000000000000000000000000000545950455f4944" },
  ],
  testnet: [
    { alias: "dao", label: "Nervos DAO", hash: "0x82d76d1b75fe2fd9a27dfbaa65a039221a380d76c926f378d3f81cf3e7e13f2e" },
    { alias: "sudt", label: "sUDT token", hash: "0xc5e5dcf215925f7ef4dfaf5f4b4f105bc321c02776d6e7d52a1db3fcd9d011a4" },
    { alias: "xudt", label: "xUDT token", hash: "0x25c29dc317811a6f6f3985a7a9ebc4838bd388d19d0feeecf0bcd60f6c0975bb" },
    { alias: "cota", label: "CoTA NFT", hash: "0x89cd8003a0eaf8e65e0c31525b7d1d5c1becefd2ea75bb4cff87810ae37764d8" },
    { alias: "unique", label: "Unique cell", hash: "0x8e341bcfec6393dcd41e635733ff2dca00a6af546949f70c57a706c0f344df8b" },
    { alias: "typeid", label: "Type ID", hash: "0x00000000000000000000000000000000000000000000000000545950455f4944" },
  ],
};
const DEFS = TYPE_DEFS[NET] || TYPE_DEFS.mainnet;
const LABEL_OF = Object.fromEntries(DEFS.map((d) => [d.hash, d.label]));
const ALIAS_OF = Object.fromEntries(DEFS.map((d) => [d.alias, d.hash]));

// ---------------- colors ----------------
const useColor = !process.env.NO_COLOR && process.stdout.isTTY;
const C = (n, s) => (useColor ? `\x1b[${n}m${s}\x1b[0m` : s);
const dim = (s) => C("2", s);
const cyan = (s) => C("36", s);
const green = (s) => C("32", s);
const yellow = (s) => C("33", s);
const magenta = (s) => C("35", s);
const red = (s) => C("31", s);
const bold = (s) => C("1", s);
const bgAlert = (s) => C("1;30;43", s); // bold black on yellow

// ---------------- bech32m address decode (dependency-free) ----------------
const BCHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BGEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
function bPolymod(values) {
  let chk = 1;
  for (const v of values) {
    const top = chk >> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ v;
    for (let i = 0; i < 5; i++) if ((top >> i) & 1) chk ^= BGEN[i];
  }
  return chk;
}
function bHrpExpand(hrp) {
  const o = [];
  for (let i = 0; i < hrp.length; i++) o.push(hrp.charCodeAt(i) >> 5);
  o.push(0);
  for (let i = 0; i < hrp.length; i++) o.push(hrp.charCodeAt(i) & 31);
  return o;
}
function convertBits(data, from, to, pad) {
  let acc = 0, bits = 0;
  const ret = [];
  const maxv = (1 << to) - 1;
  for (const v of data) {
    acc = (acc << from) | v;
    bits += from;
    while (bits >= to) { bits -= to; ret.push((acc >> bits) & maxv); }
  }
  if (pad && bits > 0) ret.push((acc << (to - bits)) & maxv);
  return ret;
}
function decodeAddress(addr) {
  const pos = addr.lastIndexOf("1");
  if (pos < 1) throw new Error("not an address");
  const hrp = addr.slice(0, pos);
  const data = [...addr.slice(pos + 1)].map((c) => BCHARSET.indexOf(c));
  if (data.some((d) => d < 0)) throw new Error("bad character");
  const spec = bPolymod([...bHrpExpand(hrp), ...data]);
  if (spec !== 0x2bc830a3 && spec !== 1) throw new Error("bad checksum");
  const bytes = convertBits(data.slice(0, -6), 5, 8, false);
  const hex = (b) => "0x" + b.map((x) => x.toString(16).padStart(2, "0")).join("");
  if (bytes[0] !== 0x00) {
    throw new Error("only full (CKB2021) addresses are supported here — use --lock-args instead");
  }
  return {
    name: addr,
    code_hash: hex(bytes.slice(1, 33)).toLowerCase(),
    hash_type: { 0: "data", 1: "type", 2: "data1", 4: "data2" }[bytes[33]] ?? String(bytes[33]),
    args: hex(bytes.slice(34)).toLowerCase(),
  };
}

// ---------------- rules ----------------
function ckbToShannons(s) {
  const [w, f = ""] = String(s).split(".");
  const frac = (f + "00000000").slice(0, 8);
  return BigInt(w || "0") * 100000000n + BigInt(frac || "0");
}
const rules = {
  minShannons: opt("--min-ckb") !== null ? ckbToShannons(opt("--min-ckb")) : null,
  types: csv("--type").map((t) => (t.startsWith("0x") ? t.toLowerCase() : ALIAS_OF[t.toLowerCase()])).filter(Boolean),
  typeArgs: csv("--type-args").map((s) => s.toLowerCase()),
  lockArgs: csv("--lock-args").map((s) => s.toLowerCase()),
  locks: csv("--address").map(decodeAddress),
};
const HAS_RULES =
  rules.minShannons !== null ||
  rules.types.length ||
  rules.typeArgs.length ||
  rules.lockArgs.length ||
  rules.locks.length;
const QUIET = flag("--quiet");
const WEBHOOK = opt("--webhook");
const EXEC = opt("--exec");

// dashboard: --serve [port]  (or --port <n>, or PORT env)
const SERVE = flag("--serve");
const serveIdx = argv.indexOf("--serve");
const inlinePort = serveIdx >= 0 && /^\d+$/.test(argv[serveIdx + 1] || "") ? argv[serveIdx + 1] : null;
const PORT = Number(opt("--port") || inlinePort || process.env.PORT || 8080);

// ---------------- helpers ----------------
const fromHex = (h) => BigInt(h);
const ts = () => new Date().toLocaleTimeString();
const short = (h) => `${h.slice(0, 10)}\u2026${h.slice(-6)}`;
const ckb = (sh) => {
  const w = sh / 100000000n;
  const f = (sh % 100000000n).toString().padStart(8, "0").replace(/0+$/, "");
  return f ? `${w}.${f}` : `${w}`;
};
const capOf = (tx) => (tx.outputs || []).reduce((a, o) => a + fromHex(o.capacity), 0n);
function classify(tx, isCellbase) {
  if (isCellbase) return { label: "cellbase", color: dim };
  const hashes = (tx.outputs || []).map((o) => o.type && o.type.code_hash.toLowerCase()).filter(Boolean);
  for (const h of hashes) {
    const name = LABEL_OF[h];
    if (name) {
      const color = name.includes("DAO") ? magenta : name.includes("token") ? yellow : name.includes("NFT") ? cyan : dim;
      return { label: name, color };
    }
  }
  if (hashes.length) return { label: "typed cell", color: dim };
  return { label: "CKB transfer", color: green };
}

// returns matched-reason strings if the tx satisfies every given rule, else null
function matchRules(tx) {
  if (!HAS_RULES) return null;
  const outs = tx.outputs || [];
  const reasons = [];
  if (rules.minShannons !== null) {
    const cap = capOf(tx);
    if (cap < rules.minShannons) return null;
    reasons.push(`\u2265${ckb(rules.minShannons)} CKB (=${ckb(cap)})`);
  }
  if (rules.types.length) {
    const hit = outs.find((o) => o.type && rules.types.includes(o.type.code_hash.toLowerCase()));
    if (!hit) return null;
    reasons.push(`type=${LABEL_OF[hit.type.code_hash.toLowerCase()] || short(hit.type.code_hash)}`);
  }
  if (rules.typeArgs.length) {
    const hit = outs.find((o) => o.type && rules.typeArgs.includes((o.type.args || "").toLowerCase()));
    if (!hit) return null;
    reasons.push(`token=${short(hit.type.args)}`);
  }
  if (rules.lockArgs.length) {
    const hit = outs.find((o) => rules.lockArgs.includes((o.lock.args || "").toLowerCase()));
    if (!hit) return null;
    reasons.push(`lock-args=${short(hit.lock.args)}`);
  }
  if (rules.locks.length) {
    const hit = outs.find((o) =>
      rules.locks.some(
        (L) =>
          o.lock.code_hash.toLowerCase() === L.code_hash &&
          o.lock.hash_type === L.hash_type &&
          (o.lock.args || "").toLowerCase() === L.args
      )
    );
    if (!hit) return null;
    reasons.push(`address`);
  }
  return reasons;
}

// ---------------- actions ----------------
function fireWebhook(payload) {
  if (!WEBHOOK) return;
  fetch(WEBHOOK, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  }).catch((e) => console.error(red(`webhook error: ${e.message}`)));
}
function fireExec(payload) {
  if (!EXEC) return;
  const child = spawn(EXEC, {
    shell: true,
    env: {
      ...process.env,
      CKB_TX_HASH: payload.txHash,
      CKB_TX_TYPE: payload.type,
      CKB_TX_CKB: payload.capacityCKB,
      CKB_TX_EVENT: payload.event,
      CKB_MATCHED: payload.matched.join("; "),
      CKB_EXPLORER: payload.explorer,
    },
    stdio: "ignore",
  });
  child.on("error", (e) => console.error(red(`exec error: ${e.message}`)));
}
function alert(event, tx, cls, reasons, extra = "") {
  const payload = {
    network: NET,
    event,
    txHash: tx.hash,
    type: cls.label,
    capacityCKB: ckb(capOf(tx)),
    matched: reasons,
    explorer: EXPLORER + tx.hash,
    time: new Date().toISOString(),
  };
  console.log(
    `${dim(ts())} ${bgAlert(" \ud83d\udd14 ALERT ")} ${event.padEnd(6)} ${cls.color(cls.label)} ${bold(short(tx.hash))} ` +
      `${dim(`\u00b7 ${payload.capacityCKB} CKB \u00b7 ${reasons.join(", ")}`)}${extra}`
  );
  console.log(dim(`            ${payload.explorer}`));
  fireWebhook(payload);
  fireExec(payload);
  pushEvent({
    kind: "alert", t: Date.now(), event,
    hash: tx.hash, type: cls.label, ckb: payload.capacityCKB,
    matched: reasons, explorer: payload.explorer,
  });
}

// ---------------- stats ----------------
const counts = new Map();
const bump = (k) => counts.set(k, (counts.get(k) || 0) + 1);
const poolSeen = new Map();
let blocksSeen = 0, txCommitted = 0, alerts = 0;
function printSummary() {
  const parts = [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}:${v}`).join("  ");
  console.log(
    dim(`\u2500\u2500 ${ts()} \u00b7 ${blocksSeen} blocks \u00b7 ${txCommitted} committed \u00b7 alerts ${alerts} \u00b7 in-pool ${poolSeen.size} \u00b7 ${parts || "no activity yet"}`)
  );
  pushEvent(statsPayload());
}

// ---------------- dashboard page (embedded, no framework) ----------------
const DASHBOARD_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CKB monitor</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; background: #0d1117; color: #c9d1d9;
         font: 13px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  header { display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
           padding: 10px 16px; border-bottom: 1px solid #21262d; position: sticky; top: 0;
           background: #0d1117; z-index: 2; }
  h1 { font-size: 15px; margin: 0; font-weight: 700; letter-spacing: .3px; }
  .pill { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid #30363d; color: #8b949e; }
  .pill.net { color: #58a6ff; border-color: #1f6feb; }
  .dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; background: #f85149; }
  .dot.live { background: #3fb950; box-shadow: 0 0 6px #3fb950; }
  .stats { margin-left: auto; display: flex; gap: 16px; flex-wrap: wrap; }
  .stat b { color: #e6edf3; font-weight: 700; } .stat span { color: #8b949e; }
  #controls { display: flex; align-items: center; gap: 14px; flex-wrap: wrap;
              padding: 8px 16px; border-bottom: 1px solid #21262d; color: #8b949e; }
  #controls label { cursor: pointer; user-select: none; }
  #controls input[type=checkbox] { vertical-align: -1px; }
  button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
           padding: 4px 10px; cursor: pointer; font: inherit; }
  button:hover { border-color: #58a6ff; }
  button.on { background: #1f6feb; border-color: #1f6feb; color: #fff; }
  #wrap { overflow: auto; height: calc(100vh - 96px); }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 4px 10px; white-space: nowrap; border-bottom: 1px solid #161b22; }
  th { position: sticky; top: 0; background: #0d1117; color: #8b949e; font-weight: 600; z-index: 1; }
  td.detail { white-space: normal; color: #8b949e; }
  a { color: #58a6ff; text-decoration: none; } a:hover { text-decoration: underline; }
  .badge { font-size: 11px; padding: 1px 7px; border-radius: 4px; font-weight: 700; }
  .badge.pool   { background: #3a2f00; color: #e3b341; }
  .badge.commit { background: #0f2e1a; color: #3fb950; }
  .badge.block  { background: #0c2233; color: #58a6ff; }
  .badge.alert  { background: #e3b341; color: #1a1500; }
  tr.alert td { background: rgba(227,179,65,.08); }
  tr.block td { background: rgba(88,166,255,.05); }
  .t-token { color: #e3b341; } .t-nft { color: #56d4dd; } .t-dao { color: #d2a8ff; }
  .t-transfer { color: #3fb950; } .t-dim { color: #8b949e; }
  .hide-pool tr[data-k=pool], .hide-commit tr[data-k=commit],
  .hide-block tr[data-k=block], .hide-alert tr[data-k=alert] { display: none; }
  .muted { color: #6e7681; }
</style>
</head>
<body>
<header>
  <span class="dot" id="dot"></span>
  <h1>CKB monitor</h1>
  <span class="pill net" id="net">…</span>
  <span class="pill" id="conn">connecting…</span>
  <div class="stats">
    <span class="stat"><b id="s-blocks">0</b> <span>blocks</span></span>
    <span class="stat"><b id="s-committed">0</b> <span>committed</span></span>
    <span class="stat"><b id="s-alerts">0</b> <span>alerts</span></span>
    <span class="stat"><b id="s-pool">0</b> <span>in-pool</span></span>
  </div>
</header>
<section id="controls">
  <span>show:</span>
  <label><input type="checkbox" data-f="pool" checked> pool</label>
  <label><input type="checkbox" data-f="commit" checked> commit</label>
  <label><input type="checkbox" data-f="block" checked> block</label>
  <label><input type="checkbox" data-f="alert" checked> alert</label>
  <button id="pause">pause</button>
  <button id="clear">clear</button>
  <span class="muted" id="hint"></span>
</section>
<div id="wrap">
  <table>
    <thead><tr><th>time</th><th>event</th><th>type</th><th>hash</th><th>CKB</th><th>detail</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
</div>
<script>
(function () {
  var MAXROWS = 600;
  var explorer = "", paused = false;
  var rows = document.getElementById("rows");
  var wrap = document.getElementById("wrap");
  var dot = document.getElementById("dot"), conn = document.getElementById("conn");
  function $(id) { return document.getElementById(id); }
  function shorten(h) { return h.length > 20 ? h.slice(0, 10) + "\\u2026" + h.slice(-6) : h; }
  function fmtTime(t) { try { return new Date(t).toLocaleTimeString(); } catch (e) { return ""; } }
  function typeClass(t) {
    if (!t) return "t-dim";
    if (t.indexOf("token") >= 0) return "t-token";
    if (t.indexOf("NFT") >= 0) return "t-nft";
    if (t.indexOf("DAO") >= 0) return "t-dao";
    if (t.indexOf("transfer") >= 0) return "t-transfer";
    return "t-dim";
  }
  function cell(html, cls) { var td = document.createElement("td"); if (cls) td.className = cls; td.innerHTML = html; return td; }
  function hashLink(h) {
    if (!h) return "";
    return '<a href="' + explorer + h + '" target="_blank" rel="noopener">' + shorten(h) + '</a>';
  }
  function addRow(o) {
    if (paused) return;
    var tr = document.createElement("tr");
    tr.setAttribute("data-k", o.kind);
    tr.className = o.kind;
    var type = "", hash = "", ckb = "", detail = "", badge = o.kind;
    if (o.kind === "block") {
      type = '<span class="t-dim">block ' + o.number + "</span>";
      hash = ""; ckb = ""; detail = o.txCount + " tx";
    } else if (o.kind === "alert") {
      badge = "alert " + (o.event || "");
      type = '<span class="' + typeClass(o.type) + '">' + (o.type || "") + "</span>";
      hash = hashLink(o.hash); ckb = o.ckb || "";
      detail = (o.matched || []).join(", ");
    } else if (o.kind === "pool") {
      type = '<span class="' + typeClass(o.type) + '">' + (o.type || "") + "</span>";
      hash = hashLink(o.hash); ckb = o.ckb || "";
      detail = '<span class="muted">' + o.ins + " \\u2192 " + o.outs + "</span>";
    } else if (o.kind === "commit") {
      type = '<span class="' + typeClass(o.type) + '">' + (o.type || "") + "</span>";
      hash = hashLink(o.hash); ckb = o.ckb || "";
      var d = [];
      if (o.latencyMs != null) d.push('<span class="muted">\\u23f1 ' + o.latencyMs + "ms in pool</span>");
      if (o.matched) d.push('<span class="t-token">matched: ' + o.matched.join(", ") + "</span>");
      detail = d.join(" \\u00b7 ");
    } else { return; }
    tr.appendChild(cell(fmtTime(o.t), "muted"));
    tr.appendChild(cell('<span class="badge ' + o.kind + '">' + badge + "</span>"));
    tr.appendChild(cell(type));
    tr.appendChild(cell(hash));
    tr.appendChild(cell(ckb));
    tr.appendChild(cell(detail, "detail"));
    rows.insertBefore(tr, rows.firstChild);
    while (rows.childElementCount > MAXROWS) rows.removeChild(rows.lastChild);
  }
  function applyStats(o) {
    $("s-blocks").textContent = o.blocks;
    $("s-committed").textContent = o.committed;
    $("s-alerts").textContent = o.alerts;
    $("s-pool").textContent = o.inPool;
  }
  function applyMeta(o) {
    explorer = o.explorer || "";
    $("net").textContent = o.network;
    document.title = "CKB monitor \\u00b7 " + o.network;
    $("hint").textContent = o.hasRules ? ("watching: " + o.rules.join("  \\u00b7  ")) : "no rules set \\u2014 streaming all activity";
  }
  // filter checkboxes -> toggle a class on #wrap
  var boxes = document.querySelectorAll("#controls input[data-f]");
  for (var i = 0; i < boxes.length; i++) {
    boxes[i].addEventListener("change", function (e) {
      wrap.classList.toggle("hide-" + e.target.getAttribute("data-f"), !e.target.checked);
    });
  }
  $("pause").addEventListener("click", function () {
    paused = !paused;
    this.classList.toggle("on", paused);
    this.textContent = paused ? "resume" : "pause";
  });
  $("clear").addEventListener("click", function () { rows.innerHTML = ""; });

  function connect() {
    var es = new EventSource("/events");
    es.onopen = function () { dot.classList.add("live"); conn.textContent = "live"; };
    es.onerror = function () { dot.classList.remove("live"); conn.textContent = "reconnecting…"; };
    es.onmessage = function (e) {
      var o; try { o = JSON.parse(e.data); } catch (err) { return; }
      if (o.kind === "stats") applyStats(o);
      else if (o.kind === "meta") applyMeta(o);
      else addRow(o);
    };
  }
  connect();
})();
</script>
</body>
</html>`;

// ---------------- live dashboard (SSE + embedded page) ----------------
const sseClients = new Set();
const recent = [];                       // ring buffer so a freshly opened tab sees recent activity
const RECENT_MAX = 250;
function pushEvent(obj) {
  if (!SERVE) return;
  recent.push(obj);
  if (recent.length > RECENT_MAX) recent.shift();
  const line = "data: " + JSON.stringify(obj) + "\n\n";
  for (const res of sseClients) { try { res.write(line); } catch {} }
}
function metaPayload() {
  const desc = [];
  if (rules.minShannons !== null) desc.push("min " + ckb(rules.minShannons) + " CKB");
  if (rules.types.length) desc.push("types [" + rules.types.map((h) => LABEL_OF[h] || short(h)).join(", ") + "]");
  if (rules.typeArgs.length) desc.push("token-args [" + rules.typeArgs.map(short).join(", ") + "]");
  if (rules.lockArgs.length) desc.push("lock-args [" + rules.lockArgs.map(short).join(", ") + "]");
  if (rules.locks.length) desc.push("address(es) [" + rules.locks.length + "]");
  return { kind: "meta", network: NET, ws: WS_URL, explorer: EXPLORER, hasRules: !!HAS_RULES, rules: desc };
}
function statsPayload() {
  return {
    kind: "stats",
    blocks: blocksSeen,
    committed: txCommitted,
    alerts,
    inPool: poolSeen.size,
    counts: Object.fromEntries([...counts.entries()].sort((a, b) => b[1] - a[1])),
  };
}
function startServer() {
  const server = http.createServer((req, res) => {
    const url = (req.url || "/").split("?")[0];
    if (url === "/" || url === "/index.html") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(DASHBOARD_HTML);
      return;
    }
    if (url === "/events") {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
        "x-accel-buffering": "no",
      });
      res.write("retry: 3000\n\n");
      res.write("data: " + JSON.stringify(metaPayload()) + "\n\n");
      for (const obj of recent) res.write("data: " + JSON.stringify(obj) + "\n\n");
      res.write("data: " + JSON.stringify(statsPayload()) + "\n\n");
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });
  server.on("error", (e) => {
    if (e.code === "EADDRINUSE") console.error(red(`dashboard: port ${PORT} is in use \u2014 pass --port <n>`));
    else console.error(red(`dashboard error: ${e.message}`));
  });
  server.listen(PORT, () => console.log(green(`dashboard \u2192 http://localhost:${PORT}`)));
  // heartbeat so SSE connections survive idle proxies / sleeping tabs
  setInterval(() => { for (const res of sseClients) { try { res.write(": ping\n\n"); } catch {} } }, 25000);
}

// ---------------- request/response over the socket ----------------
let ws, reqId = 1000;
const pending = new Map();
function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = ++reqId;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, jsonrpc: "2.0", method, params }));
    setTimeout(() => { if (pending.delete(id)) reject(new Error(`${method} timeout`)); }, 15000);
  });
}

function handleNewTx(inner) {
  const tx = inner.transaction;
  if (!tx) return;
  const cls = classify(tx, false);
  poolSeen.set(tx.hash, Date.now());
  bump(cls.label);
  const reasons = matchRules(tx);
  if (reasons) { alerts++; alert("pool", tx, cls, reasons); }
  if (!QUIET) {
    console.log(
      `${dim(ts())} ${yellow("\u27f3 pool  ")} ${cls.color(cls.label.padEnd(12))} ${short(tx.hash)} ${dim(`\u00b7 ${tx.inputs.length}\u2192${tx.outputs.length} \u00b7 ${ckb(capOf(tx))} CKB`)}`
    );
  }
  pushEvent({
    kind: "pool", t: Date.now(),
    hash: tx.hash, type: cls.label, ckb: ckb(capOf(tx)),
    ins: tx.inputs.length, outs: tx.outputs.length,
  });
}

async function handleNewTip(inner) {
  blocksSeen++;
  let block;
  try { block = await call("get_block_by_number", [inner.number, "0x2"]); } catch { return; }
  if (!block) return;
  const txs = block.transactions || [];
  if (!QUIET) console.log(cyan(`${dim(ts())} \u25a3 block ${fromHex(inner.number).toString()} \u00b7 ${txs.length} tx`));
  pushEvent({ kind: "block", t: Date.now(), number: Number(fromHex(inner.number)), txCount: txs.length });
  txs.forEach((tx, i) => {
    txCommitted++;
    const cls = classify(tx, i === 0);
    const seen = poolSeen.get(tx.hash);
    let lat = "", latencyMs = null;
    if (seen) { latencyMs = Date.now() - seen; lat = dim(` \u00b7 \u23f1 ${latencyMs}ms in pool`); poolSeen.delete(tx.hash); }
    const reasons = matchRules(tx);
    if (reasons) { alerts++; alert("commit", tx, cls, reasons, lat); }
    if (!QUIET) {
      console.log(
        `${dim(ts())} ${green("\u2713 commit")} ${cls.color(cls.label.padEnd(12))} ${short(tx.hash)} ${dim(`\u00b7 ${ckb(capOf(tx))} CKB`)}${lat}`
      );
    }
    pushEvent({
      kind: "commit", t: Date.now(),
      hash: tx.hash, type: cls.label, ckb: ckb(capOf(tx)),
      latencyMs, matched: reasons || null,
    });
  });
  pushEvent(statsPayload());
  if (poolSeen.size > 5000) {
    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [h, t] of poolSeen) if (t < cutoff) poolSeen.delete(h);
  }
}

// ---------------- connect (auto-reconnect) ----------------
function connect() {
  ws = new WSImpl(WS_URL);
  ws.addEventListener("open", () => {
    console.log(green(`connected \u2192 ${WS_URL}`));
    ws.send(JSON.stringify({ id: 1, jsonrpc: "2.0", method: "subscribe", params: ["new_tip_header"] }));
    ws.send(JSON.stringify({ id: 2, jsonrpc: "2.0", method: "subscribe", params: ["new_transaction"] }));
  });
  ws.addEventListener("message", (ev) => {
    let m;
    try { m = JSON.parse(typeof ev.data === "string" ? ev.data : ev.data.toString()); } catch { return; }
    if (m.method === "subscribe") {
      let inner = null;
      try { inner = JSON.parse(m.params.result); } catch { return; }
      if (inner.number && !inner.transaction) handleNewTip(inner);
      else if (inner.transaction) handleNewTx(inner);
      return;
    }
    if (m.id !== undefined && pending.has(m.id)) {
      const r = pending.get(m.id);
      pending.delete(m.id);
      r(m.result);
    }
  });
  ws.addEventListener("close", () => {
    console.log(red("disconnected \u2014 reconnecting in 3s"));
    setTimeout(connect, 3000);
  });
  ws.addEventListener("error", () => {});
}

// ---------------- go ----------------
console.log(bold("CKB monitor") + dim(`  \u00b7  ${NET}  \u00b7  ${WS_URL}`));
if (HAS_RULES) {
  const desc = [];
  if (rules.minShannons !== null) desc.push(`min ${ckb(rules.minShannons)} CKB`);
  if (rules.types.length) desc.push(`types [${rules.types.map((h) => LABEL_OF[h] || short(h)).join(", ")}]`);
  if (rules.typeArgs.length) desc.push(`token-args [${rules.typeArgs.map(short).join(", ")}]`);
  if (rules.lockArgs.length) desc.push(`lock-args [${rules.lockArgs.map(short).join(", ")}]`);
  if (rules.locks.length) desc.push(`address(es) [${rules.locks.length}]`);
  console.log(dim(`watching: ${desc.join("  \u00b7  ")}`));
  console.log(dim(`actions: alert${WEBHOOK ? " + webhook" : ""}${EXEC ? " + exec" : ""}${SERVE ? ` + dashboard:${PORT}` : ""}${QUIET ? "  (quiet)" : ""}\n`));
} else {
  console.log(dim(`no --rules set, so just streaming. add e.g. --min-ckb 100000  or  --address ckb1…  to get alerts.`));
  console.log(dim(`${yellow("\u27f3")} pool   ${cyan("\u25a3")} block   ${green("\u2713")} committed (\u23f1 = time in pool)\n`));
}
if (SERVE) startServer();
connect();
setInterval(printSummary, SUMMARY_EVERY_MS);
process.on("SIGINT", () => { console.log(); printSummary(); console.log(dim("stopped.")); process.exit(0); });
