#!/usr/bin/env node
// ckb-monitor.mjs: a live CKB on-chain monitor.
//
// It subscribes to the PUBLIC WebSocket (no node, no polling), classifies every
// transaction by type, tracks the pending->committed lifecycle, and (the point of
// the tool) fires ACTIONS when a transaction matches rules you care about.
//
// Zero dependencies on Node 22+ (built-in WebSocket). On older Node: `npm i ws`.
//
// === Watch rules (combine freely; a tx must satisfy ALL given rules; commas = OR within one) ===
//   --address <ckb1…|ckt1…>[,…]   match outputs paying to these address(es)   (incoming funds)
//   --min-ckb <N>                 match txs whose total output capacity ≥ N CKB (whale watch)
//   --type <alias|0xhash>[,…]     match an asset type: dao,sudt,xudt,cota,unique,typeid, or a raw code hash
//   --type-args <0x…>[,…]         match a specific token (xUDT/sUDT args = token id)
//   --lock-args <0x…>[,…]         match outputs by raw lock args (alternative to --address)
//
// === Actions on a match ===
//   (always) prints a highlighted 🔔 ALERT line
//   --webhook <url>               HTTP POST a JSON payload (Slack/Discord/your backend)
//   --exec "<cmd>"                run a shell command; env: CKB_TX_HASH, CKB_TX_TYPE, CKB_TX_CKB, CKB_TX_EVENT, CKB_MATCHED, CKB_EXPLORER
//   --quiet                       show only alerts + the periodic summary (good for background monitoring)
//
// === Live dashboard ===
//   --serve [port]                browser dashboard (live table over SSE); default port 8080
//   --port <n>                    dashboard port (alt to --serve <port>)
//
// === Other ===
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
import fs from "node:fs";

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

WATCHTOWER
  --watch <file.json>                 watch your own contracts; alarm when one goes silent
                                      (each entry: name/desc + type/typeArgs/lockArgs/address
                                      /minCkb rules + optional idleMin liveness threshold)

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

// ---------------- endpoint (NET is switchable at runtime via the dashboard) ----------------
const wsForNet = (n) => (n === "testnet" ? "wss://testnet.ckb.dev/ws" : "wss://mainnet.ckb.dev/ws");
const explorerForNet = (n) => (n === "testnet" ? "https://pudge.explorer.nervos.org/transaction/" : "https://explorer.nervos.org/transaction/");
let NET = (opt("--net") || process.env.CKB_NET || "mainnet").toLowerCase();
let WS_URL = opt("--ws") || process.env.CKB_WS || wsForNet(NET);
let EXPLORER = explorerForNet(NET);
const SUMMARY_EVERY_MS = Number(opt("--summary-ms") || process.env.SUMMARY_MS || 30000);

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
    { alias: "xudt", label: "xUDT token", hash: "0x50bd8d6680b8b9cf98b73f3c08faf8b2a21914311954118ad6609be6e78a1b95" },
    { alias: "cota", label: "CoTA NFT", hash: "0x89cd8003a0eaf8e65e0c31525b7d1d5c1becefd2ea75bb4cff87810ae37764d8" },
    { alias: "unique", label: "Unique cell", hash: "0x8e341bcfec6393dcd41e635733ff2dca00a6af546949f70c57a706c0f344df8b" },
    { alias: "typeid", label: "Type ID", hash: "0x00000000000000000000000000000000000000000000000000545950455f4944" },
  ],
};
let DEFS, LABEL_OF, ALIAS_OF, TOKEN_HASHES;
function rebuildDefs() {
  DEFS = TYPE_DEFS[NET] || TYPE_DEFS.mainnet;
  LABEL_OF = Object.fromEntries(DEFS.map((d) => [d.hash, d.label]));
  ALIAS_OF = {};
  for (const d of DEFS) (ALIAS_OF[d.alias] = ALIAS_OF[d.alias] || []).push(d.hash.toLowerCase());
  TOKEN_HASHES = new Set([...(ALIAS_OF.sudt || []), ...(ALIAS_OF.xudt || [])]);
}
rebuildDefs();

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
    throw new Error("only full (CKB2021) addresses are supported here; use --lock-args instead");
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
const rawTypeTokens = csv("--type");
const resolveTypes = () => rawTypeTokens.flatMap((t) => (t.startsWith("0x") ? [t.toLowerCase()] : (ALIAS_OF[t.toLowerCase()] || [])));
const rules = {
  minShannons: opt("--min-ckb") !== null ? ckbToShannons(opt("--min-ckb")) : null,
  types: resolveTypes(),
  typeArgs: csv("--type-args").map((s) => s.toLowerCase()),
  lockArgs: csv("--lock-args").map((s) => s.toLowerCase()),
  locks: csv("--address").map(decodeAddress),
  outPoints: csv("--out-point").map((s) => s.toLowerCase()),
};
const HAS_RULES =
  rules.minShannons !== null ||
  rules.types.length ||
  rules.typeArgs.length ||
  rules.lockArgs.length ||
  rules.locks.length ||
  rules.outPoints.length;
const QUIET = flag("--quiet");
const WEBHOOK = opt("--webhook");
const EXEC = opt("--exec");

// watchtower: --watch <file.json>  (named watches over your own contracts, with liveness alarms)
const WATCH_FILE = opt("--watch");

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

// little-endian u128 from a hex string (xUDT/sUDT amount lives in the first 16 bytes of cell data)
function leU128(hex) {
  if (!hex || hex.length < 4) return null;
  const b = hex.slice(2);
  let v = 0n;
  for (let i = 0; i < 16 && i * 2 + 2 <= b.length; i++) v += BigInt(parseInt(b.slice(i * 2, i * 2 + 2), 16)) << BigInt(8 * i);
  return v;
}
// first token output of a tx, with its decoded amount + token id (raw, no decimals applied)
function tokenInfo(tx) {
  const outs = tx.outputs || [], data = tx.outputs_data || [];
  for (let i = 0; i < outs.length; i++) {
    const o = outs[i];
    if (o.type && TOKEN_HASHES.has(o.type.code_hash.toLowerCase())) {
      const amt = leU128(data[i] || "0x");
      return { amount: amt == null ? null : amt.toString(), id: (o.type.args || "").toLowerCase(), label: LABEL_OF[o.type.code_hash.toLowerCase()] || "token" };
    }
  }
  return null;
}
// CKB epoch field packs number/index/length into one uint64
function decodeEpoch(hex) {
  const e = fromHex(hex);
  return { number: Number(e & 0xffffffn), index: Number((e >> 24n) & 0xffffn), length: Number((e >> 40n) & 0xffffn) };
}

// returns matched-reason strings if the tx satisfies every dimension of `r`, else null
function matchAgainst(tx, r) {
  const outs = tx.outputs || [];
  const reasons = [];
  if (r.minShannons != null) {
    const cap = capOf(tx);
    if (cap < r.minShannons) return null;
    reasons.push(`\u2265${ckb(r.minShannons)} CKB (=${ckb(cap)})`);
  }
  if (r.types && r.types.length) {
    const hit = outs.find((o) => o.type && r.types.includes(o.type.code_hash.toLowerCase()));
    if (!hit) return null;
    reasons.push(`type=${LABEL_OF[hit.type.code_hash.toLowerCase()] || short(hit.type.code_hash)}`);
  }
  if (r.typeArgs && r.typeArgs.length) {
    // prefix match: an xUDT's args is the 32-byte owner-lock hash (the token id) plus optional extension flags
    const hit = outs.find((o) => o.type && r.typeArgs.some((a) => (o.type.args || "").toLowerCase().startsWith(a)));
    if (!hit) return null;
    reasons.push(`token=${short(hit.type.args)}`);
  }
  if (r.lockArgs && r.lockArgs.length) {
    const hit = outs.find((o) => r.lockArgs.includes((o.lock.args || "").toLowerCase()));
    if (!hit) return null;
    reasons.push(`lock-args=${short(hit.lock.args)}`);
  }
  if (r.outPoints && r.outPoints.length) {
    // The only rule that reads INPUTS: it fires when the named cell is spent,
    // whatever the spender does with it afterwards.
    const hit = (tx.inputs || []).find((i) => {
      const p = i.previous_output || i.previousOutput;
      if (!p) return false;
      const idx = typeof p.index === "string" ? parseInt(p.index, 16) : p.index;
      return r.outPoints.includes(`${String(p.tx_hash || p.txHash).toLowerCase()}:${idx}`);
    });
    if (!hit) return null;
    const p = hit.previous_output || hit.previousOutput;
    reasons.push(`spent ${short(p.tx_hash || p.txHash)}:${typeof p.index === "string" ? parseInt(p.index, 16) : p.index}`);
  }
  if (r.locks && r.locks.length) {
    const hit = outs.find((o) =>
      r.locks.some(
        (L) =>
          o.lock.code_hash.toLowerCase() === L.code_hash &&
          o.lock.hash_type === L.hash_type &&
          (o.lock.args || "").toLowerCase() === L.args
      )
    );
    if (!hit) return null;
    reasons.push(`address`);
  }
  return reasons.length ? reasons : null;
}
function matchRules(tx) {
  if (!HAS_RULES) return null;
  return matchAgainst(tx, rules);
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
      CKB_TX_FEE: payload.feeCKB || "",
      CKB_TOKEN: payload.token ? (payload.token.amount || "") : "",
    },
    stdio: "ignore",
  });
  child.on("error", (e) => console.error(red(`exec error: ${e.message}`)));
}
function alert(event, tx, cls, reasons, extra = "", info = {}) {
  const feeCKB = info.fee != null ? ckb(info.fee) : null;
  const token = info.token || tokenInfo(tx);
  const payload = {
    network: NET,
    event,
    txHash: tx.hash,
    type: cls.label,
    capacityCKB: ckb(capOf(tx)),
    feeCKB,
    token,
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
    fee: feeCKB, token, matched: reasons, explorer: payload.explorer,
  });
}

// ---------------- stats ----------------
const counts = new Map();
const bump = (k) => counts.set(k, (counts.get(k) || 0) + 1);
const poolSeen = new Map();
let blocksSeen = 0, txCommitted = 0, alerts = 0, lastTipTs = 0;
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
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%230a0e14'/><path d='M16 4l10 12-10 12L6 16z' fill='%232fe6c5'/></svg>">
<style>
  :root {
    --bg:#0a0e14; --border:#1b2433; --text:#d8e0ea; --muted:#6b7785;
    --teal:#2fe6c5; --green:#3fd07f; --amber:#f5b342; --blue:#4aa3ff; --purple:#c08bff; --nft:#45d6e0;
    color-scheme: dark;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body { margin: 0; color: var(--text); display: flex; flex-direction: column; overflow: hidden;
         background: radial-gradient(1100px 520px at 82% -12%, rgba(47,230,197,.10), transparent 60%), var(--bg);
         font: 13px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .accentbar { height: 3px; flex: none; background: linear-gradient(90deg, var(--teal), var(--blue), var(--purple)); }
  header { display: flex; align-items: center; gap: 13px; padding: 14px 20px; flex: none;
           border-bottom: 1px solid var(--border); background: linear-gradient(180deg, #0e1420, #0a0e14); }
  .logo { width: 28px; height: 28px; border-radius: 8px; font-size: 15px; color: #04141a; font-weight: 800;
          display: flex; align-items: center; justify-content: center;
          background: linear-gradient(135deg, var(--teal), var(--blue)); box-shadow: 0 0 18px rgba(47,230,197,.35); }
  .brand h1 { font-size: 15px; margin: 0; font-weight: 700; letter-spacing: .2px; }
  .brand .sub { font-size: 11px; color: var(--muted); }
  .spacer { margin-left: auto; }
  .pill { font-size: 11px; padding: 3px 10px; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); }
  .pill.net { color: var(--teal); border-color: #1c4a44; background: #0c1a18; text-transform: uppercase; letter-spacing: .6px; font-weight: 600; }
  .status { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--muted);
            padding: 5px 12px; border: 1px solid var(--border); border-radius: 999px; background: #0c121c; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #f85149; }
  .dot.live { background: var(--green); animation: pulse 1.8s infinite; }
  @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(63,208,127,.5); } 70% { box-shadow: 0 0 0 7px rgba(63,208,127,0); } 100% { box-shadow: 0 0 0 0 rgba(63,208,127,0); } }
  .cards { display: flex; gap: 12px; padding: 14px 20px 6px; flex: none; flex-wrap: wrap; }
  .card { flex: 1; min-width: 120px; padding: 12px 15px; border-radius: 12px; border: 1px solid var(--border);
          background: linear-gradient(180deg, #0f1622, #0b1019); }
  .card .num { font-size: 26px; font-weight: 800; line-height: 1; letter-spacing: -.5px; font-family: ui-monospace, monospace; }
  .card .lbl { font-size: 10px; color: var(--muted); margin-top: 7px; text-transform: uppercase; letter-spacing: .7px; }
  .card.blocks .num { color: var(--blue); } .card.committed .num { color: var(--green); }
  .card.alerts .num { color: var(--amber); } .card.pool .num { color: var(--teal); }
  .num.bump { animation: bump .6s ease; }
  @keyframes bump { 0% { transform: scale(1.18); filter: brightness(1.6); } 100% { transform: scale(1); } }
  #controls { display: flex; align-items: center; gap: 9px; padding: 10px 20px 12px; flex: none; flex-wrap: wrap; }
  .chips { display: flex; gap: 7px; }
  .chip { font: inherit; font-size: 12px; cursor: pointer; color: var(--text); background: #0c121c;
          border: 1px solid var(--border); padding: 5px 12px; border-radius: 999px; display: flex; align-items: center; gap: 7px; }
  .chip::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--muted); }
  .chip.pool::before { background: var(--amber); } .chip.commit::before { background: var(--green); }
  .chip.block::before { background: var(--blue); } .chip.alert::before { background: var(--amber); }
  .chip.off { opacity: .35; }
  .btn { font: inherit; font-size: 12px; cursor: pointer; color: var(--text); background: #0c121c;
         border: 1px solid var(--border); padding: 5px 13px; border-radius: 8px; }
  .btn:hover { border-color: var(--teal); }
  .btn.on { background: var(--amber); border-color: var(--amber); color: #1a1200; font-weight: 700; }
  .hint { font-size: 11px; color: var(--muted); margin-left: auto; }
  #wrap { flex: 1; overflow: auto; margin: 0 14px 14px; border: 1px solid var(--border); border-radius: 12px; background: #0b1018; position: relative; }
  table { width: 100%; border-collapse: separate; border-spacing: 0; }
  thead th { position: sticky; top: 0; z-index: 1; text-align: left; padding: 9px 13px; background: #0c1320;
             color: var(--muted); font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: .7px;
             border-bottom: 1px solid var(--border); }
  tbody td { padding: 7px 13px; border-bottom: 1px solid #0f1622; white-space: nowrap; vertical-align: top; }
  tbody tr.justin { animation: rowin .28s ease; }
  @keyframes rowin { from { opacity: 0; transform: translateY(-5px); } to { opacity: 1; transform: none; } }
  tbody tr:hover td { background: rgba(255,255,255,.025); }
  td:first-child { border-left: 3px solid transparent; }
  td.c-time { color: var(--muted); font-family: ui-monospace, monospace; font-size: 12px; }
  .c-ckb { text-align: right; }
  td.c-ckb { font-family: ui-monospace, monospace; color: #aeb9c6; }
  td.c-hash { font-family: ui-monospace, monospace; }
  td.c-detail { white-space: normal; color: var(--muted); max-width: 440px; }
  tr.commit td:first-child { border-left-color: rgba(63,208,127,.5); }
  tr.pool   td:first-child { border-left-color: rgba(245,179,66,.4); }
  tr.block  td:first-child { border-left-color: rgba(74,163,255,.6); }
  tr.alert  td:first-child { border-left-color: var(--amber); }
  tr.block td { background: rgba(74,163,255,.05); color: var(--blue); }
  tr.alert td { background: rgba(245,179,66,.09); }
  tr.alert { animation: rowin .25s ease, apulse 1.5s ease-in-out 2; }
  @keyframes apulse { 0%,100% { filter: none; } 50% { filter: brightness(1.18); } }
  a { color: var(--blue); text-decoration: none; } a:hover { color: var(--teal); text-decoration: underline; }
  .badge { font-size: 10px; padding: 2px 8px; border-radius: 5px; font-weight: 700; text-transform: uppercase; letter-spacing: .4px; }
  .badge.pool   { background: #2a2200; color: var(--amber); }
  .badge.commit { background: #0c2317; color: var(--green); }
  .badge.block  { background: #0a1c2e; color: var(--blue); }
  .badge.alert  { background: var(--amber); color: #1a1200; }
  .t-token { color: var(--amber); } .t-nft { color: var(--nft); } .t-dao { color: var(--purple); }
  .t-transfer { color: var(--green); } .t-dim { color: var(--muted); }
  .hide-pool tr[data-k=pool], .hide-commit tr[data-k=commit],
  .hide-block tr[data-k=block], .hide-alert tr[data-k=alert] { display: none; }
  #empty { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: var(--muted); pointer-events: none; }
  .muted { color: var(--muted); }
  .charts { display: flex; gap: 12px; padding: 4px 20px 2px; flex: none; flex-wrap: wrap; }
  .panel { flex: 1; min-width: 210px; padding: 9px 13px 10px; border-radius: 12px; border: 1px solid var(--border);
           background: linear-gradient(180deg, #0f1622, #0b1019); }
  .ptitle { display: flex; justify-content: space-between; font-size: 10px; color: var(--muted);
            text-transform: uppercase; letter-spacing: .7px; margin-bottom: 6px; }
  .psub { color: var(--text); font-family: ui-monospace, monospace; text-transform: none; letter-spacing: 0; }
  .panel canvas { width: 100%; height: 58px; display: block; }
  #mix { display: flex; flex-direction: column; gap: 4px; min-height: 58px; justify-content: center; }
  .mixrow { display: flex; align-items: center; gap: 8px; font-size: 11px; }
  .mixlbl { width: 86px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .mixbar { flex: 1; height: 7px; background: #0c121c; border-radius: 4px; overflow: hidden; }
  .mixbar i { display: block; height: 100%; background: currentColor; opacity: .85; border-radius: 4px; }
  .mixn { width: 32px; text-align: right; color: var(--muted); font-family: ui-monospace, monospace; }
  #pulsewrap { position: relative; flex: none; height: 100px; margin: 14px 14px 0; overflow: hidden;
               border: 1px solid var(--border); border-radius: 12px;
               background: radial-gradient(120% 150% at 50% 130%, #0f1826, #0a0e14); }
  #pulse { width: 100%; height: 100%; display: block; }
  .plabel { position: absolute; top: 8px; left: 12px; z-index: 1; font-size: 10px; letter-spacing: .7px;
            text-transform: uppercase; color: var(--muted); pointer-events: none; }
  .netsw { display: flex; gap: 4px; }
  .nbtn { font: inherit; font-size: 11px; cursor: pointer; color: var(--muted); background: #0c121c;
          border: 1px solid var(--border); padding: 4px 11px; border-radius: 7px; }
  .nbtn:hover { border-color: var(--teal); }
  .nbtn.on { background: linear-gradient(135deg, var(--teal), var(--blue)); color: #04141a; font-weight: 700; border-color: transparent; }
  #chainbar { display: flex; gap: 20px; align-items: center; flex-wrap: wrap; padding: 10px 20px 0; flex: none; font-size: 12px; color: var(--muted); }
  .cstat b { color: var(--text); font-family: ui-monospace, monospace; }
  .ebar { display: inline-block; width: 84px; height: 6px; background: #0c121c; border-radius: 4px; overflow: hidden; vertical-align: middle; }
  .ebar i { display: block; height: 100%; width: 0; background: linear-gradient(90deg, var(--teal), var(--blue)); transition: width .4s ease; }
  .alertbanner { color: #ff7b72; font-weight: 700; }
  #pager { display: flex; align-items: center; gap: 10px; padding: 10px 20px 14px; flex: none; font-size: 12px; color: var(--muted); }
  #pager .btn[disabled] { opacity: .35; cursor: default; }
  .pgl.on { background: var(--green); border-color: var(--green); color: #04140a; font-weight: 700; }
  .pginfo b { color: var(--text); font-family: ui-monospace, monospace; }
  #pg-size { font: inherit; font-size: 12px; background: #0c121c; color: var(--text); border: 1px solid var(--border); border-radius: 7px; padding: 4px 6px; }
  #watchtower { display: none; flex-direction: column; gap: 6px; margin: 12px 14px 0; padding: 11px 14px; flex: none;
                border: 1px solid var(--border); border-radius: 12px; background: linear-gradient(180deg, #11182a, #0b1019); }
  .wt-title { font-size: 10px; text-transform: uppercase; letter-spacing: .7px; color: var(--muted); margin-bottom: 2px; }
  .wt-row { display: flex; align-items: center; gap: 10px; font-size: 12px; }
  .wt-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); flex: none; }
  .wt-dot.healthy { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .wt-dot.stale { background: var(--amber); }
  .wt-dot.alarm { background: #ff5147; box-shadow: 0 0 8px #ff5147; animation: pulse 1.2s infinite; }
  .wt-dot.idle { background: var(--blue); }
  .wt-name { font-weight: 700; color: var(--text); min-width: 160px; }
  .wt-desc { color: var(--muted); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .wt-meta { color: var(--muted); font-family: ui-monospace, monospace; white-space: nowrap; }
  .wt-meta b { color: var(--text); }
  tr.watch td { background: rgba(47,230,197,.07); }
  tr.watch td:first-child { border-left-color: var(--teal); }
  .badge.watch { background: #0c2a26; color: var(--teal); }
</style>
</head>
<body>
<div class="accentbar"></div>
<header>
  <div class="logo">◈</div>
  <div class="brand">
    <h1>CKB monitor</h1>
    <div class="sub mono" id="ws">…</div>
  </div>
  <span class="pill net" id="net">…</span>
  <div class="netsw">
    <button class="nbtn" data-net="testnet">testnet</button>
    <button class="nbtn" data-net="mainnet">mainnet</button>
  </div>
  <div class="spacer"></div>
  <div class="status"><span class="dot" id="dot"></span><span id="conn">connecting…</span></div>
</header>
<div id="pulsewrap"><span class="plabel">live network pulse</span><canvas id="pulse"></canvas></div>
<div id="chainbar">
  <span class="cstat">epoch <b id="ch-epoch">-</b> <span class="ebar"><i id="ch-ebar"></i></span> <span class="muted" id="ch-eidx"></span></span>
  <span class="cstat">block <b id="ch-int">-</b></span>
  <span class="cstat">mempool <b id="ch-pool">-</b></span>
  <span class="cstat">fee <b id="ch-fee">-</b> <span class="muted">sh/kB</span></span>
  <span class="cstat alertbanner" id="ch-alert" style="display:none"></span>
</div>
<div id="watchtower"></div>
<div class="cards">
  <div class="card blocks"><div class="num" id="s-blocks">0</div><div class="lbl">blocks</div></div>
  <div class="card committed"><div class="num" id="s-committed">0</div><div class="lbl">committed</div></div>
  <div class="card alerts"><div class="num" id="s-alerts">0</div><div class="lbl">alerts</div></div>
  <div class="card pool"><div class="num" id="s-pool">0</div><div class="lbl">in pool</div></div>
</div>
<div class="charts">
  <div class="panel">
    <div class="ptitle"><span>throughput</span><span class="psub" id="tp-now">0/s</span></div>
    <canvas id="c-tp"></canvas>
  </div>
  <div class="panel">
    <div class="ptitle"><span>tx / block</span><span class="psub" id="bk-now"></span></div>
    <canvas id="c-bk"></canvas>
  </div>
  <div class="panel">
    <div class="ptitle"><span>type mix</span></div>
    <div id="mix"></div>
  </div>
</div>
<section id="controls">
  <div class="chips">
    <button class="chip pool" data-f="pool">pool</button>
    <button class="chip commit" data-f="commit">commit</button>
    <button class="chip block" data-f="block">block</button>
    <button class="chip alert" data-f="alert">alert</button>
  </div>
  <button class="btn" id="pause">pause</button>
  <button class="btn" id="clear">clear</button>
  <span class="hint" id="hint"></span>
</section>
<div id="wrap">
  <div id="empty">waiting for activity…</div>
  <table>
    <thead><tr><th>time</th><th>event</th><th>type</th><th>hash</th><th class="c-ckb">CKB</th><th>detail</th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
</div>
<div id="pager">
  <button class="btn pgl" id="pg-live">● live</button>
  <button class="btn" id="pg-newer">◂ newer</button>
  <span class="pginfo"><b id="pg-info">0 / 1</b> <span class="muted" id="pg-count">0 rows</span></span>
  <button class="btn" id="pg-older">older ▸</button>
  <span class="muted">per page</span>
  <select id="pg-size"><option>25</option><option selected>50</option><option>100</option><option>200</option></select>
</div>
<script>
(function () {
  var BUFMAX = 2000;
  var buf = [];                 // newest-first event objects backing the paged table
  var page = 0, pageSize = 50, pendingNew = 0, paused = false, prev = {};
  var filters = { pool: true, commit: true, block: true, alert: true, watch: true };
  var explorer = "";
  var rows = document.getElementById("rows");
  var wrap = document.getElementById("wrap");
  var empty = document.getElementById("empty");
  var dot = document.getElementById("dot"), conn = document.getElementById("conn");
  function $(id) { return document.getElementById(id); }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>]/g, function (c) { return c === "&" ? "&amp;" : c === "<" ? "&lt;" : "&gt;"; }); }
  function shorten(h) { return h.length > 20 ? h.slice(0, 10) + "\\u2026" + h.slice(-6) : h; }
  function fmtTime(t) { try { return new Date(t).toLocaleTimeString(); } catch (e) { return ""; } }
  function fmtCkb(s) {
    if (s == null || s === "") return "";
    var p = String(s).split(".");
    var i = p[0].replace(/\\B(?=(\\d{3})+(?!\\d))/g, ",");
    return p.length > 1 ? i + '<span class="muted">.' + p[1] + "</span>" : i;
  }
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
    return '<a href="' + explorer + esc(h) + '" target="_blank" rel="noopener" title="' + esc(h) + '">' + shorten(h) + "</a>";
  }
  function typeCell(o) { return '<span class="' + typeClass(o.type) + '">' + esc(o.type || "") + "</span>"; }
  function tokenName(t) { return (t.label || "token") + (t.id ? " " + t.id.slice(0, 8) : ""); }
  function rowHtml(o) {
    var badge = o.kind, type = "", hash = "", ckb = "", detail = "";
    if (o.kind === "watch") {
      badge = o.status === "idle" ? "silent" : o.status === "recovered" ? "back" : "watch";
      type = '<span class="t-token">\\u25c9 ' + esc(o.watch) + "</span>";
      hash = o.hash ? hashLink(o.hash) : "";
      detail = o.status === "idle"
        ? '<span class="alertbanner">no activity in ' + o.idleMin + "m</span>"
        : '<span class="t-token">' + esc((o.matched || []).join(", ")) + "</span>";
      return '<tr data-k="watch" class="watch">'
        + '<td class="c-time">' + fmtTime(o.t) + "</td>"
        + '<td class="c-badge"><span class="badge watch">' + badge + "</span></td>"
        + '<td class="c-type">' + type + "</td>"
        + '<td class="c-hash">' + hash + "</td>"
        + '<td class="c-ckb"></td>'
        + '<td class="c-detail">' + detail + "</td></tr>";
    }
    if (o.kind === "block") {
      type = '<span class="t-dim">block ' + o.number + "</span>";
      var bp = ['<span class="muted">' + o.txCount + " tx</span>"];
      if (o.intervalMs != null) bp.push('<span class="muted">+' + (o.intervalMs / 1000).toFixed(1) + "s</span>");
      if (o.epoch) bp.push('<span class="muted">epoch ' + o.epoch.number + "</span>");
      detail = bp.join(" \\u00b7 ");
    } else {
      if (o.kind === "alert") badge = "alert " + (o.event || "");
      type = typeCell(o); hash = hashLink(o.hash); ckb = fmtCkb(o.ckb);
      var d = [];
      if (o.token) d.push('<span class="t-token">\\u2b21 ' + (o.token.amount != null ? fmtCkb(o.token.amount) : "?") + " " + esc(tokenName(o.token)) + "</span>");
      if (o.kind === "alert") d.push('<span class="t-token">' + esc((o.matched || []).join(", ")) + "</span>");
      if (o.kind === "commit" && o.matched) d.push('<span class="t-token">matched: ' + esc(o.matched.join(", ")) + "</span>");
      if (o.fee) d.push('<span class="muted">fee ' + o.fee + "</span>");
      if (o.kind === "pool") d.push('<span class="muted">' + o.ins + " \\u2192 " + o.outs + " cells</span>");
      if (o.kind === "commit" && o.latencyMs != null) d.push('<span class="muted">\\u23f1 ' + o.latencyMs + "ms</span>");
      detail = d.join(" \\u00b7 ");
    }
    return '<tr data-k="' + o.kind + '" class="' + o.kind + '">'
      + '<td class="c-time">' + fmtTime(o.t) + "</td>"
      + '<td class="c-badge"><span class="badge ' + o.kind + '">' + badge + "</span></td>"
      + '<td class="c-type">' + type + "</td>"
      + '<td class="c-hash">' + hash + "</td>"
      + '<td class="c-ckb">' + ckb + "</td>"
      + '<td class="c-detail">' + detail + "</td></tr>";
  }
  function visible() { var r = []; for (var i = 0; i < buf.length; i++) if (filters[buf[i].kind]) r.push(buf[i]); return r; }
  function totalPages(n) { return Math.max(1, Math.ceil(n / pageSize)); }
  function pagerCounts() {
    var vis = visible(), tpg = totalPages(vis.length);
    if (page >= tpg) page = tpg - 1; if (page < 0) page = 0;
    $("pg-info").textContent = (vis.length ? (page + 1) : 0) + " / " + tpg;
    $("pg-count").textContent = vis.length + " rows";
    $("pg-newer").disabled = page <= 0;
    $("pg-older").disabled = page >= tpg - 1;
    var live = $("pg-live"); live.classList.toggle("on", page === 0);
    live.textContent = page === 0 ? "\\u25cf live" : ("\\u2191 " + pendingNew + " new");
    return vis;
  }
  function render() {
    var vis = pagerCounts();
    var slice = vis.slice(page * pageSize, page * pageSize + pageSize);
    var html = ""; for (var i = 0; i < slice.length; i++) html += rowHtml(slice[i]);
    rows.innerHTML = html;
    empty.style.display = vis.length ? "none" : "";
  }
  function addEvent(o) {
    if (paused) return;
    buf.unshift(o); if (buf.length > BUFMAX) buf.pop();
    if (page !== 0) { pendingNew++; pagerCounts(); return; }
    if (!filters[o.kind]) { pagerCounts(); return; }
    var tmp = document.createElement("tbody"); tmp.innerHTML = rowHtml(o);
    var tr = tmp.firstChild; if (tr) { tr.className += " justin"; rows.insertBefore(tr, rows.firstChild); }
    while (rows.childElementCount > pageSize) rows.removeChild(rows.lastChild);
    empty.style.display = "none";
    pagerCounts();
  }
  function setNum(id, val) {
    var el = $(id);
    var s = (typeof val === "number") ? val.toLocaleString() : String(val);
    if (el.textContent === s) return;
    el.textContent = s;
    if (prev[id] !== undefined) { el.classList.remove("bump"); void el.offsetWidth; el.classList.add("bump"); }
    prev[id] = s;
  }
  function applyStats(o) { setNum("s-blocks", o.blocks); setNum("s-committed", o.committed); setNum("s-alerts", o.alerts); setNum("s-pool", o.inPool); }
  function applyMeta(o) {
    explorer = o.explorer || "";
    $("net").textContent = o.network;
    $("ws").textContent = o.ws || "";
    document.title = "CKB monitor \\u00b7 " + o.network;
    $("hint").textContent = o.hasRules ? ("watching: " + o.rules.join("  \\u00b7  ")) : "no rules \\u2014 streaming all activity";
    var nb = document.querySelectorAll(".nbtn");
    for (var i = 0; i < nb.length; i++) nb[i].classList.toggle("on", nb[i].getAttribute("data-net") === o.network);
  }
  function applyChain(o) {
    if (o.pending != null) $("ch-pool").textContent = o.pending + "+" + o.proposed;
    if (o.feeMedian != null) $("ch-fee").textContent = Math.round(o.feeMedian).toLocaleString();
    var ab = $("ch-alert");
    if (o.alerts && o.alerts.length) { ab.style.display = ""; ab.textContent = "\\u26a0 " + o.alerts.join(" \\u00b7 "); }
    else { ab.style.display = "none"; ab.textContent = ""; }
  }
  function relAge(ms) {
    var s = Math.floor(ms / 1000);
    if (s < 60) return s + "s";
    var m = Math.floor(s / 60);
    if (m < 60) return m + "m";
    return Math.floor(m / 60) + "h";
  }
  function applyWatchStatus(o) {
    var wt = $("watchtower"); if (!wt) return;
    if (!o.watches || !o.watches.length) { wt.style.display = "none"; return; }
    wt.style.display = "flex";
    var html = '<div class="wt-title">watchtower \\u2014 your contracts</div>';
    for (var i = 0; i < o.watches.length; i++) {
      var w = o.watches[i];
      var tail = w.idleMin
        ? (w.health === "alarm" ? ' \\u00b7 <span class="alertbanner">SILENT &gt;' + w.idleMin + "m</span>" : ' \\u00b7 alarm @ ' + w.idleMin + "m")
        : "";
      html += '<div class="wt-row"><span class="wt-dot ' + w.health + '"></span>'
        + '<span class="wt-name">' + esc(w.name) + "</span>"
        + '<span class="wt-desc">' + esc(w.desc || "") + "</span>"
        + '<span class="wt-meta"><b>' + w.matchCount + "</b> hits \\u00b7 last " + relAge(w.ageMs) + " ago" + tail + "</span></div>";
    }
    wt.innerHTML = html;
  }
  function applyBlockMeta(o) {
    if (o.epoch) {
      $("ch-epoch").textContent = o.epoch.number;
      var pct = o.epoch.length ? Math.round(o.epoch.index / o.epoch.length * 100) : 0;
      $("ch-ebar").style.width = pct + "%";
      $("ch-eidx").textContent = o.epoch.index + "/" + o.epoch.length;
    }
    if (o.intervalMs != null) $("ch-int").textContent = (o.intervalMs / 1000).toFixed(1) + "s";
  }
  function resetAll() {
    buf = []; page = 0; pendingNew = 0;
    tp = new Array(60).fill(0); blocks = []; typeCounts = {}; ripples = []; parts = [];
    render(); drawTp(); drawBlocks(); drawMix();
  }
  // filter chips drive the buffer view
  var chips = document.querySelectorAll(".chip[data-f]");
  for (var ci = 0; ci < chips.length; ci++) {
    chips[ci].addEventListener("click", function () {
      filters[this.getAttribute("data-f")] = !this.classList.toggle("off");
      page = 0; render();
    });
  }
  // network switch buttons
  var nbtns = document.querySelectorAll(".nbtn");
  for (var ni = 0; ni < nbtns.length; ni++) {
    nbtns[ni].addEventListener("click", function () {
      fetch("/net?to=" + this.getAttribute("data-net")).catch(function () {});
    });
  }
  // pager
  $("pg-live").addEventListener("click", function () { page = 0; pendingNew = 0; render(); });
  $("pg-newer").addEventListener("click", function () { if (page > 0) { page--; render(); } });
  $("pg-older").addEventListener("click", function () { page++; render(); });
  $("pg-size").addEventListener("change", function () { pageSize = parseInt(this.value, 10) || 50; page = 0; render(); });
  $("pause").addEventListener("click", function () {
    paused = !paused; this.classList.toggle("on", paused); this.textContent = paused ? "resume" : "pause";
  });
  $("clear").addEventListener("click", function () { buf = []; page = 0; pendingNew = 0; render(); });
  render();

  // ---- live charts (canvas, zero-dependency) ----
  var tp = new Array(60).fill(0);   // events per second (ring; last slot = current second)
  var blocks = [];                  // recent blocks: { n: txCount }
  var typeCounts = {};
  function track(o) {
    tp[tp.length - 1]++;
    emit(o);
    if (o.kind === "block") { blocks.push({ n: o.txCount || 0 }); if (blocks.length > 48) blocks.shift(); drawBlocks(); }
    if (o.kind === "pool" || o.kind === "commit" || o.kind === "alert") {
      var t = o.type || "other"; typeCounts[t] = (typeCounts[t] || 0) + 1;
    }
  }
  function ctxFor(cv, h) {
    var dpr = window.devicePixelRatio || 1;
    var w = cv.clientWidth || cv.parentNode.clientWidth || 200;
    cv.width = Math.max(1, Math.floor(w * dpr)); cv.height = Math.floor(h * dpr);
    var c = cv.getContext("2d"); c.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { c: c, w: w, h: h };
  }
  function drawTp() {
    var cv = $("c-tp"); if (!cv) return; var g = ctxFor(cv, 58), c = g.c, w = g.w, h = g.h;
    c.clearRect(0, 0, w, h);
    var max = 1; for (var i = 0; i < tp.length; i++) if (tp[i] > max) max = tp[i];
    var n = tp.length, step = w / (n - 1);
    function y(v) { return h - (v / max) * (h - 6) - 2; }
    c.beginPath(); c.moveTo(0, h);
    for (var i = 0; i < n; i++) c.lineTo(i * step, y(tp[i]));
    c.lineTo(w, h); c.closePath();
    var grad = c.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, "rgba(47,230,197,.35)"); grad.addColorStop(1, "rgba(47,230,197,0)");
    c.fillStyle = grad; c.fill();
    c.beginPath();
    for (var j = 0; j < n; j++) { var x = j * step, yy = y(tp[j]); if (j === 0) c.moveTo(x, yy); else c.lineTo(x, yy); }
    c.strokeStyle = "#2fe6c5"; c.lineWidth = 1.5; c.stroke();
  }
  function drawBlocks() {
    var cv = $("c-bk"); if (!cv) return; var g = ctxFor(cv, 58), c = g.c, w = g.w, h = g.h;
    c.clearRect(0, 0, w, h);
    if (!blocks.length) return;
    var max = 1; for (var i = 0; i < blocks.length; i++) if (blocks[i].n > max) max = blocks[i].n;
    var slots = Math.max(blocks.length, 24), step = w / slots, bw = Math.max(2, step - 2);
    var off = blocks.length < slots ? (slots - blocks.length) : 0;
    for (var k = 0; k < blocks.length; k++) {
      var x = (k + off) * step, bh = (blocks[k].n / max) * (h - 4), yv = h - bh;
      c.fillStyle = blocks[k].n >= max ? "#4aa3ff" : "rgba(74,163,255,.45)";
      c.fillRect(x, yv, bw, Math.max(1, bh));
    }
    var bn = $("bk-now"); if (bn) bn.textContent = blocks[blocks.length - 1].n + " tx";
  }
  function drawMix() {
    var el = $("mix"); if (!el) return;
    var arr = []; for (var k in typeCounts) arr.push([k, typeCounts[k]]);
    arr.sort(function (a, b) { return b[1] - a[1]; }); arr = arr.slice(0, 6);
    var tot = 0; for (var i = 0; i < arr.length; i++) tot += arr[i][1];
    var html = "";
    for (var m = 0; m < arr.length; m++) {
      var name = arr[m][0], cnt = arr[m][1], pct = tot ? Math.round(cnt / tot * 100) : 0, tc = typeClass(name);
      html += '<div class="mixrow"><span class="mixlbl ' + tc + '">' + esc(name) + "</span>"
            + '<span class="mixbar"><i class="' + tc + '" style="width:' + pct + '%"></i></span>'
            + '<span class="mixn">' + cnt + "</span></div>";
    }
    el.innerHTML = html || '<div class="muted">no activity yet</div>';
  }
  setInterval(function () {
    var nowsec = tp[tp.length - 1]; tp.push(0); tp.shift();
    var lbl = $("tp-now"); if (lbl) lbl.textContent = nowsec + "/s";
    drawTp(); drawMix();
  }, 1000);
  window.addEventListener("resize", function () { drawTp(); drawBlocks(); });
  drawTp(); drawBlocks(); drawMix();

  // ---- ambient node-pulse (decorative, driven by the live event stream) ----
  var pulseCv = $("pulse"), ripples = [], parts = [], pt = 0;
  var PCOL = { token: "#f5b342", transfer: "#3fd07f", block: "#4aa3ff", nft: "#45d6e0", dao: "#c08bff", dim: "#6b7785" };
  function hexA(hex, a) {
    var n = parseInt(hex.slice(1), 16);
    return "rgba(" + ((n >> 16) & 255) + "," + ((n >> 8) & 255) + "," + (n & 255) + "," + (a < 0 ? 0 : a).toFixed(3) + ")";
  }
  function colorFor(o) {
    if (o.kind === "block") return PCOL.block;
    var tc = typeClass(o.type);
    if (tc === "t-token") return PCOL.token;
    if (tc === "t-transfer") return PCOL.transfer;
    if (tc === "t-nft") return PCOL.nft;
    if (tc === "t-dao") return PCOL.dao;
    return o.kind === "alert" ? PCOL.token : PCOL.dim;
  }
  function emit(o) {
    var col = colorFor(o), big = (o.kind === "block" || o.kind === "alert");
    ripples.push({ r: 7, max: big ? 175 : 120, col: col, a: big ? 0.85 : 0.55, w: big ? 2.4 : 1.4 });
    for (var i = 0; i < (big ? 7 : 2); i++) {
      var ang = Math.random() * Math.PI * 2, sp = 0.6 + Math.random() * 1.7;
      parts.push({ x: 0, y: 0, vx: Math.cos(ang) * sp, vy: Math.sin(ang) * sp, life: 1, col: col, sz: big ? 2.6 : 1.7 });
    }
    if (ripples.length > 44) ripples.splice(0, ripples.length - 44);
    if (parts.length > 280) parts.splice(0, parts.length - 280);
  }
  function pulseFrame() {
    if (!pulseCv) return;
    var dpr = window.devicePixelRatio || 1;
    var w = pulseCv.clientWidth || 600, h = pulseCv.clientHeight || 100;
    if (pulseCv.width !== Math.floor(w * dpr) || pulseCv.height !== Math.floor(h * dpr)) { pulseCv.width = Math.floor(w * dpr); pulseCv.height = Math.floor(h * dpr); }
    var c = pulseCv.getContext("2d"); c.setTransform(dpr, 0, 0, dpr, 0, 0);
    c.clearRect(0, 0, w, h);
    var cx = w / 2, cy = h / 2, i;
    c.strokeStyle = "rgba(255,255,255,.035)"; c.lineWidth = 1;
    for (var gr = 28; gr < w; gr += 36) { c.beginPath(); c.arc(cx, cy, gr, 0, Math.PI * 2); c.stroke(); }
    for (i = ripples.length - 1; i >= 0; i--) {
      var rp = ripples[i]; rp.r += 1.7; var prog = rp.r / rp.max;
      if (prog >= 1) { ripples.splice(i, 1); continue; }
      c.beginPath(); c.arc(cx, cy, rp.r, 0, Math.PI * 2);
      c.strokeStyle = hexA(rp.col, rp.a * (1 - prog)); c.lineWidth = rp.w; c.stroke();
    }
    for (i = parts.length - 1; i >= 0; i--) {
      var p = parts[i]; p.x += p.vx; p.y += p.vy; p.life -= 0.012;
      if (p.life <= 0) { parts.splice(i, 1); continue; }
      c.beginPath(); c.arc(cx + p.x, cy + p.y, p.sz, 0, Math.PI * 2);
      c.fillStyle = hexA(p.col, p.life * 0.9); c.fill();
    }
    pt += 0.05; var br = 6.5 + Math.sin(pt) * 1.3;
    var grd = c.createRadialGradient(cx, cy, 0, cx, cy, 28);
    grd.addColorStop(0, "rgba(47,230,197,.5)"); grd.addColorStop(1, "rgba(47,230,197,0)");
    c.fillStyle = grd; c.beginPath(); c.arc(cx, cy, 28, 0, Math.PI * 2); c.fill();
    c.fillStyle = "#2fe6c5"; c.beginPath(); c.arc(cx, cy, br, 0, Math.PI * 2); c.fill();
    requestAnimationFrame(pulseFrame);
  }
  if (pulseCv) requestAnimationFrame(pulseFrame);

  function connect() {
    var es = new EventSource("/events");
    es.onopen = function () { dot.classList.add("live"); conn.textContent = "live"; };
    es.onerror = function () { dot.classList.remove("live"); conn.textContent = "reconnecting…"; };
    es.onmessage = function (e) {
      var o; try { o = JSON.parse(e.data); } catch (err) { return; }
      if (o.kind === "stats") { applyStats(o); return; }
      if (o.kind === "meta") { applyMeta(o); return; }
      if (o.kind === "chain") { applyChain(o); return; }
      if (o.kind === "watchstatus") { applyWatchStatus(o); return; }
      if (o.kind === "reset") { resetAll(); return; }
      if (o.kind === "watch") { addEvent(o); return; }
      if (o.kind === "block") applyBlockMeta(o);
      track(o); addEvent(o);
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
      if (watches.length) res.write("data: " + JSON.stringify(watchStatusPayload()) + "\n\n");
      sseClients.add(res);
      req.on("close", () => sseClients.delete(res));
      return;
    }
    if (url === "/net") {
      const to = decodeURIComponent(((req.url.split("to=")[1] || "").split("&")[0]) || "");
      const ok = switchNet(to);
      res.writeHead(ok ? 200 : 400, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok, net: NET }));
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
let ws, reqId = 1000, switching = false;
const pending = new Map();
function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = ++reqId;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, jsonrpc: "2.0", method, params }));
    setTimeout(() => { if (pending.delete(id)) reject(new Error(`${method} timeout`)); }, 15000);
  });
}

// ---------------- watchtower: watch your own contracts, alarm on silence ----------------
let watches = [];
function loadWatches() {
  if (!WATCH_FILE) return;
  let cfg;
  try { cfg = JSON.parse(fs.readFileSync(WATCH_FILE, "utf8")); }
  catch (e) { console.error(red(`--watch: cannot read ${WATCH_FILE}: ${e.message}`)); process.exit(1); }
  const list = Array.isArray(cfg) ? cfg : (cfg.watches || []);
  const arr = (v) => (v == null ? [] : [].concat(v));
  watches = list.filter((w) => w && !w.disabled).map((w) => ({
    name: w.name || "watch",
    desc: w.desc || "",
    idleMin: Number(w.idleMin || 0),
    webhook: w.webhook || null,
    exec: w.exec || null,
    rules: {
      minShannons: w.minCkb != null ? ckbToShannons(w.minCkb) : null,
      types: arr(w.type).flatMap((t) => (String(t).startsWith("0x") ? [String(t).toLowerCase()] : (ALIAS_OF[String(t).toLowerCase()] || []))),
      typeArgs: arr(w.typeArgs).map((s) => String(s).toLowerCase()),
      lockArgs: arr(w.lockArgs).map((s) => String(s).toLowerCase()),
      locks: arr(w.address).map(decodeAddress),
      // "0xhash:index" — matches when that exact cell is SPENT. Every other rule
      // reads outputs, so a cell that is consumed and never recreated slips past
      // them; for a contract's own code cell that is the difference between
      // spotting an upgrade and missing a deliberate destruction.
      outPoints: arr(w.outPoint).map((s) => String(s).toLowerCase()),
    },
    lastSeen: Date.now(), matchCount: 0, alarmed: false,
  }));
  if (watches.length) console.log(green(`watchtower: ${watches.length} watch(es) from ${WATCH_FILE}`));
}
loadWatches();

function watchFire(w, status, info = {}) {
  const tx = info.tx || null, reasons = info.reasons || [];
  const explorer = tx ? EXPLORER + tx.hash : null;
  const payload = {
    watch: w.name, status, network: NET,
    txHash: tx ? tx.hash : null, explorer, matched: reasons,
    token: info.token || null, idleMin: w.idleMin || null, lastSeen: w.lastSeen,
    time: new Date().toISOString(),
  };
  const tag = status === "idle" ? bgAlert(" ⏰ SILENT ") : status === "recovered" ? green(" ✓ BACK ") : bgAlert(" 🔔 WATCH ");
  if (status === "idle")
    console.log(`${dim(ts())} ${tag} ${bold(w.name)} ${red(`no activity in ${w.idleMin}m`)} ${dim(w.desc)}`);
  else
    console.log(`${dim(ts())} ${tag} ${bold(w.name)} ${dim(reasons.join(", "))} ${tx ? short(tx.hash) : ""}`);
  if (explorer) console.log(dim(`            ${explorer}`));
  const hook = w.webhook || WEBHOOK;
  if (hook) fetch(hook, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload) }).catch((e) => console.error(red(`watch webhook error: ${e.message}`)));
  const ex = w.exec || EXEC;
  if (ex) {
    const child = spawn(ex, { shell: true, env: { ...process.env, CKB_WATCH: w.name, CKB_WATCH_STATUS: status, CKB_TX_HASH: tx ? tx.hash : "", CKB_EXPLORER: explorer || "", CKB_MATCHED: reasons.join("; ") }, stdio: "ignore" });
    child.on("error", (e) => console.error(red(`watch exec error: ${e.message}`)));
  }
  pushEvent({ kind: "watch", t: Date.now(), watch: w.name, status, hash: tx ? tx.hash : null, matched: reasons, explorer, idleMin: w.idleMin || null });
}
function checkWatches(tx, event) {
  if (!watches.length) return;
  let tok;
  for (const w of watches) {
    const reasons = matchAgainst(tx, w.rules);
    if (!reasons) continue;
    w.lastSeen = Date.now(); w.matchCount++;
    const recovered = w.alarmed; w.alarmed = false;
    if (tok === undefined) tok = tokenInfo(tx);
    watchFire(w, recovered ? "recovered" : "hit", { tx, reasons, event, token: tok });
  }
}
function checkIdle() {
  const now = Date.now();
  for (const w of watches) {
    if (!w.idleMin) continue;
    if ((now - w.lastSeen) / 60000 >= w.idleMin && !w.alarmed) { w.alarmed = true; watchFire(w, "idle"); }
  }
  if (watches.length) pushEvent(watchStatusPayload());
}
function watchStatusPayload() {
  const now = Date.now();
  return {
    kind: "watchstatus",
    watches: watches.map((w) => ({
      name: w.name, desc: w.desc, idleMin: w.idleMin || null, matchCount: w.matchCount,
      lastSeen: w.lastSeen, ageMs: now - w.lastSeen, alarmed: w.alarmed,
      health: w.idleMin ? (w.alarmed ? "alarm" : ((now - w.lastSeen) / 60000 < w.idleMin ? "healthy" : "stale")) : "idle",
    })),
  };
}

function handleNewTx(inner) {
  const tx = inner.transaction;
  if (!tx) return;
  const cls = classify(tx, false);
  // the mempool entry carries fee/cycles/size for free; stash them for the commit row
  const meta = {
    t: Date.now(),
    fee: inner.fee != null ? fromHex(inner.fee) : null,
    cycles: inner.cycles != null ? Number(fromHex(inner.cycles)) : null,
    size: inner.size != null ? Number(fromHex(inner.size)) : null,
  };
  poolSeen.set(tx.hash, meta);
  bump(cls.label);
  const tok = tokenInfo(tx);
  const reasons = matchRules(tx);
  if (reasons) { alerts++; alert("pool", tx, cls, reasons, "", { fee: meta.fee, token: tok }); }
  if (!QUIET) {
    console.log(
      `${dim(ts())} ${yellow("\u27f3 pool  ")} ${cls.color(cls.label.padEnd(12))} ${short(tx.hash)} ${dim(`\u00b7 ${tx.inputs.length}\u2192${tx.outputs.length} \u00b7 ${ckb(capOf(tx))} CKB`)}`
    );
  }
  pushEvent({
    kind: "pool", t: Date.now(),
    hash: tx.hash, type: cls.label, ckb: ckb(capOf(tx)),
    ins: tx.inputs.length, outs: tx.outputs.length,
    fee: meta.fee != null ? ckb(meta.fee) : null, cycles: meta.cycles, token: tok,
  });
  checkWatches(tx, "pool");
}

async function handleNewTip(inner) {
  blocksSeen++;
  let block;
  try { block = await call("get_block_by_number", [inner.number, "0x2"]); } catch { return; }
  if (!block) return;
  const txs = block.transactions || [];
  if (!QUIET) console.log(cyan(`${dim(ts())} \u25a3 block ${fromHex(inner.number).toString()} \u00b7 ${txs.length} tx`));
  const tipTs = inner.timestamp ? Number(fromHex(inner.timestamp)) : Date.now();
  const intervalMs = lastTipTs ? tipTs - lastTipTs : null;
  lastTipTs = tipTs;
  const epoch = inner.epoch ? decodeEpoch(inner.epoch) : null;
  pushEvent({ kind: "block", t: Date.now(), number: Number(fromHex(inner.number)), txCount: txs.length, ts: tipTs, intervalMs, epoch });
  txs.forEach((tx, i) => {
    txCommitted++;
    const cls = classify(tx, i === 0);
    const seen = poolSeen.get(tx.hash);
    let lat = "", latencyMs = null, fee = seen ? seen.fee : null;
    if (seen) { latencyMs = Date.now() - seen.t; lat = dim(` \u00b7 \u23f1 ${latencyMs}ms in pool`); poolSeen.delete(tx.hash); }
    const tok = tokenInfo(tx);
    const reasons = matchRules(tx);
    if (reasons) { alerts++; alert("commit", tx, cls, reasons, lat, { fee, token: tok }); }
    if (!QUIET) {
      console.log(
        `${dim(ts())} ${green("\u2713 commit")} ${cls.color(cls.label.padEnd(12))} ${short(tx.hash)} ${dim(`\u00b7 ${ckb(capOf(tx))} CKB`)}${lat}`
      );
    }
    pushEvent({
      kind: "commit", t: Date.now(),
      hash: tx.hash, type: cls.label, ckb: ckb(capOf(tx)),
      latencyMs, fee: fee != null ? ckb(fee) : null, token: tok, matched: reasons || null,
    });
    checkWatches(tx, "commit");
  });
  pushEvent(statsPayload());
  if (poolSeen.size > 5000) {
    const cutoff = Date.now() - 10 * 60 * 1000;
    for (const [h, m] of poolSeen) if (m.t < cutoff) poolSeen.delete(h);
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
    if (switching) { switching = false; connect(); return; }
    console.log(red("disconnected \u2014 reconnecting in 3s"));
    setTimeout(connect, 3000);
  });
  ws.addEventListener("error", () => {});
}

// switch the upstream network at runtime (driven by the dashboard /net endpoint)
function switchNet(to) {
  to = to === "mainnet" ? "mainnet" : to === "testnet" ? "testnet" : null;
  if (!to || to === NET) return false;
  NET = to;
  WS_URL = wsForNet(NET);
  EXPLORER = explorerForNet(NET);
  rebuildDefs();
  rules.types = resolveTypes();
  counts.clear(); poolSeen.clear();
  blocksSeen = 0; txCommitted = 0; alerts = 0; lastTipTs = 0;
  recent.length = 0;
  console.log(green(`switching network \u2192 ${NET}`));
  pushEvent({ kind: "reset", network: NET });
  pushEvent(metaPayload());
  pushEvent(statsPayload());
  switching = true;
  try { ws && ws.close(); } catch {}
  return true;
}

// poll chain-wide stats (mempool depth, fee market, network alerts) for the dashboard
async function pollChain() {
  if (!SERVE || !ws || ws.readyState !== 1) return;
  try {
    const [info, pool, fee] = await Promise.all([
      call("get_blockchain_info", []).catch(() => null),
      call("tx_pool_info", []).catch(() => null),
      call("get_fee_rate_statistics", []).catch(() => null),
    ]);
    pushEvent({
      kind: "chain",
      alerts: info && info.alerts ? info.alerts.map((a) => a.message || String(a)) : [],
      pending: pool ? Number(fromHex(pool.pending)) : null,
      proposed: pool ? Number(fromHex(pool.proposed)) : null,
      minFee: pool ? Number(fromHex(pool.min_fee_rate)) : null,
      feeMedian: fee && fee.median ? Number(fromHex(fee.median)) : null,
    });
  } catch {}
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
if (watches.length) {
  console.log(dim(`watchtower: ${watches.map((w) => w.name + (w.idleMin ? `(silent>${w.idleMin}m)` : "")).join("  ·  ")}\n`));
  setInterval(checkIdle, 30000);
}
if (SERVE) { startServer(); setInterval(pollChain, 8000); }
connect();
setInterval(printSummary, SUMMARY_EVERY_MS);
process.on("SIGINT", () => { console.log(); printSummary(); console.log(dim("stopped.")); process.exit(0); });
