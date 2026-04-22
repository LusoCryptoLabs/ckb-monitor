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
}

async function handleNewTip(inner) {
  blocksSeen++;
  let block;
  try { block = await call("get_block_by_number", [inner.number, "0x2"]); } catch { return; }
  if (!block) return;
  const txs = block.transactions || [];
  if (!QUIET) console.log(cyan(`${dim(ts())} \u25a3 block ${fromHex(inner.number).toString()} \u00b7 ${txs.length} tx`));
  txs.forEach((tx, i) => {
    txCommitted++;
    const cls = classify(tx, i === 0);
    const seen = poolSeen.get(tx.hash);
    let lat = "";
    if (seen) { lat = dim(` \u00b7 \u23f1 ${Date.now() - seen}ms in pool`); poolSeen.delete(tx.hash); }
    const reasons = matchRules(tx);
    if (reasons) { alerts++; alert("commit", tx, cls, reasons, lat); }
    if (!QUIET) {
      console.log(
        `${dim(ts())} ${green("\u2713 commit")} ${cls.color(cls.label.padEnd(12))} ${short(tx.hash)} ${dim(`\u00b7 ${ckb(capOf(tx))} CKB`)}${lat}`
      );
    }
  });
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
  console.log(dim(`actions: alert${WEBHOOK ? " + webhook" : ""}${EXEC ? " + exec" : ""}${QUIET ? "  (quiet)" : ""}\n`));
} else {
  console.log(dim(`no --rules set, so just streaming. add e.g. --min-ckb 100000  or  --address ckb1…  to get alerts.`));
  console.log(dim(`${yellow("\u27f3")} pool   ${cyan("\u25a3")} block   ${green("\u2713")} committed (\u23f1 = time in pool)\n`));
}
connect();
setInterval(printSummary, SUMMARY_EVERY_MS);
process.on("SIGINT", () => { console.log(); printSummary(); console.log(dim("stopped.")); process.exit(0); });
