/**
 * test-harness.js - timer-threshold
 *
 * Standalone test harness for the timer-threshold node. Stubs enough of
 * the Node-RED runtime to instantiate the node for real, then exercises
 * every scenario in the design document with PASS/FAIL assertions:
 *
 *   Section 1 - Synchronous routing & commands (outputs, gating,
 *               ignored tagging, set commands, envelope shape)
 *   Section 2 - Asynchronous timer behavior (fixed expiry, sliding
 *               decay, cooldown lifecycle, heartbeat)
 *   Section 3 - Persistence restore paths (sliding prune, fixed
 *               recalculation, 3-8s randomization, cooldown restore,
 *               restore-never-triggers, idle context restore)
 *
 * Run from the repo root:  node test-scripts/test-harness.js  (or npm test)
 * Exit code 0 = all pass, 1 = failures (suitable for CI).
 *
 * Uses real timers with short durations - a full run takes ~10 seconds.
 * All persistence files are written to a throwaway temp directory, never
 * to a real Node-RED userDir.
 **/

"use strict";

const fs = require("fs");
const path = require("path");
const os = require("os");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

let failures = 0;
let passes = 0;

function check(label, cond, detail) {
  if (cond) {
    passes++;
    console.log("  PASS  " + label);
  } else {
    failures++;
    console.log(
      "  FAIL  " + label + (detail !== undefined ? "  ->  " + detail : ""),
    );
  }
}

function section(title) {
  console.log("\n== " + title + " ==");
}

// ---------------------------------------------------------------------------
// Module loading
// ---------------------------------------------------------------------------
// Repo layout: this harness lives in <root>/test-scripts/, the node source
// lives in <root>/timer-threshold/ (timer-threshold.js + cycle.js +
// timer-threshold.html). If cycle.js is missing from the source folder,
// copy the node into a temp directory with an identity stub so the
// harness can still run.

const sourceDir = path.join(__dirname, "..", "timer-threshold");
let modulePath = path.join(sourceDir, "timer-threshold.js");
if (!fs.existsSync(path.join(sourceDir, "cycle.js"))) {
  console.log(
    "NOTE: cycle.js not found in " +
      sourceDir +
      " - using identity stub in temp dir",
  );
  const stubDir = fs.mkdtempSync(path.join(os.tmpdir(), "tt-module-"));
  fs.copyFileSync(modulePath, path.join(stubDir, "timer-threshold.js"));
  fs.writeFileSync(
    path.join(stubDir, "cycle.js"),
    'if (typeof JSON.decycle !== "function") { JSON.decycle = function(o) { return o; }; }\n' +
      'if (typeof JSON.retrocycle !== "function") { JSON.retrocycle = function(o) { return o; }; }\n',
  );
  modulePath = path.join(stubDir, "timer-threshold.js");
}

// ---------------------------------------------------------------------------
// RED runtime stub
// ---------------------------------------------------------------------------

const userDir = fs.mkdtempSync(path.join(os.tmpdir(), "tt-userdir-"));

const RED = {
  _ctor: null,
  nodes: {
    createNode: function (node, n) {
      node.id = n.id;
      node._handlers = {};
      node._sent = []; // every node.send([o1,o2,o3]) call, in order
      node._statuses = []; // every node.status(...) call, in order
      node.on = function (evt, fn) {
        node._handlers[evt] = fn;
      };
      node.status = function (s) {
        node._statuses.push(s);
      };
      node.error = function (e) {
        console.error("  NODE ERROR: " + e);
      };
      node.warn = function (w) {
        console.warn("  NODE WARN: " + w);
      };
      node.send = function (arr) {
        node._sent.push(arr);
      };
    },
    registerType: function (name, ctor) {
      RED._ctor = ctor;
    },
  },
  util: {
    cloneMessage: function (m) {
      return JSON.parse(JSON.stringify(m));
    },
    evaluateNodeProperty: function (v) {
      return v;
    },
  },
  settings: { userDir: userDir },
};

require(modulePath)(RED);

// ---------------------------------------------------------------------------
// Node construction + inspection helpers
// ---------------------------------------------------------------------------

const DEFAULT_CFG = {
  countlimit: "3",
  window: "10",
  windowType: "num",
  windowunits: "Second",
  windowmode: "fixed",
  reporting: "none",
  reportingformat: "human",
  persist: false,
  cooldownduration: "0",
  cooldownunits: "Second",
  heartbeatinterval: "0",
  heartbeatintervalunits: "Second",
};

let nodeSeq = 0;
const openNodes = [];

function makeNode(overrides) {
  const cfg = Object.assign({}, DEFAULT_CFG, overrides || {});
  if (!cfg.id) cfg.id = "tnode" + ++nodeSeq;
  const node = {};
  RED.nodes.createNode(node, cfg);
  RED._ctor.call(node, cfg);
  node._receive = function (msg) {
    node._handlers["input"](msg);
  };
  node._close = function (removed) {
    return new Promise((res) => node._handlers["close"](removed || false, res));
  };
  openNodes.push(node);
  return node;
}

// Flattened view of everything a node has sent: [{port, msg}, ...]
function allEvents(node) {
  const out = [];
  node._sent.forEach(function (arr) {
    arr.forEach(function (m, i) {
      if (m !== null) out.push({ port: i + 1, msg: m });
    });
  });
  return out;
}

// All messages that appeared on a given port (1=Trigger, 2=Query, 3=Events)
function portMsgs(node, port) {
  return allEvents(node)
    .filter((e) => e.port === port)
    .map((e) => e.msg);
}

// Compact "port:event(ignored)" trace for failure diagnostics
function trace(node) {
  return allEvents(node)
    .map((e) => e.port + ":" + e.msg.timerEvent + (e.msg.ignored ? "(i)" : ""))
    .join(" ");
}

function lastOn(node, port) {
  const msgs = portMsgs(node, port);
  return msgs.length ? msgs[msgs.length - 1] : null;
}

function persistFile(id) {
  return path.join(userDir, "timerthreshold-timers", id);
}

function writePersist(id, obj) {
  fs.mkdirSync(path.dirname(persistFile(id)), { recursive: true });
  fs.writeFileSync(persistFile(id), JSON.stringify(obj));
}

const ENVELOPE_KEYS = [
  "timerEvent",
  "timerState",
  "count",
  "countLimit",
  "windowMode",
  "windowDuration",
  "windowRemaining",
  "cooldownRemaining",
  "ignoredCount",
  "lastIgnoredTime",
  "disabled",
  "ignored",
  "source",
];

function hasFullEnvelope(msg) {
  return ENVELOPE_KEYS.every((k) =>
    Object.prototype.hasOwnProperty.call(msg, k),
  );
}

// ---------------------------------------------------------------------------
// Test sections
// ---------------------------------------------------------------------------

async function section1_syncRoutingAndCommands() {
  section("1.1 Count / Trigger routing and envelope");
  {
    const n = makeNode({});
    n._receive({ payload: "s1", topic: "hum" });
    n._receive({ payload: "s2", topic: "hum" });

    const ev = allEvents(n);
    check(
      "two counted events, Events output only",
      ev.length === 2 &&
        ev.every(
          (e) =>
            e.port === 3 && e.msg.timerEvent === "counted" && !e.msg.ignored,
        ),
      trace(n),
    );
    check(
      "count increments 1 then 2",
      ev[0].msg.count === 1 && ev[1].msg.count === 2,
    );
    check("state is counting", ev[1].msg.timerState === "counting");
    check("full envelope on counted", hasFullEnvelope(ev[0].msg));
    check(
      "windowRemaining populated while counting",
      ev[1].msg.windowRemaining > 9000 && ev[1].msg.windowRemaining <= 10000,
      "got " + ev[1].msg.windowRemaining,
    );

    n._receive({ payload: "s3", topic: "hum" });
    const trig1 = portMsgs(n, 1);
    const trig3 = portMsgs(n, 3).filter((m) => m.timerEvent === "triggered");
    check(
      "trigger fires on output 1 exactly once",
      trig1.length === 1,
      trace(n),
    );
    check("trigger duplicated on output 3", trig3.length === 1);
    check(
      "no additional counted event for the completing message",
      portMsgs(n, 3).filter((m) => m.timerEvent === "counted").length === 2,
    );
    check(
      "trigger clones final counted message (payload/topic carry through)",
      trig1[0].payload === "s3" && trig1[0].topic === "hum",
    );
    check(
      "trigger reports settled post-trigger state (count 0, idle, window 0)",
      trig1[0].count === 0 &&
        trig1[0].timerState === "idle" &&
        trig1[0].windowRemaining === 0,
    );
    check("trigger never ignored", trig1[0].ignored === false);
    await n._close();
  }

  section("1.2 Query isolation");
  {
    const n = makeNode({});
    n._receive({ payload: "x" });
    n._receive({ payload: "QUERY" }); // case-insensitivity check too
    const q = portMsgs(n, 2);
    check(
      "query on output 2 only, once",
      q.length === 1 && allEvents(n).filter((e) => e.port === 2).length === 1,
    );
    check(
      "query never on output 3",
      portMsgs(n, 3).filter((m) => m.timerEvent === "query").length === 0,
    );
    check(
      "query snapshot correct (count 1, counting, external)",
      q[0].count === 1 &&
        q[0].timerState === "counting" &&
        q[0].source === "external",
    );
    check("query did not disturb the count", true); // verified by next message
    n._receive({ payload: "y" });
    check(
      "count continued to 2 after query",
      lastOn(n, 3).count === 2,
      trace(n),
    );
    await n._close();
  }

  section("1.3 Commands are never counted");
  {
    const n = makeNode({});
    [
      "stop",
      "reset",
      "query",
      "disable",
      "enable",
      "setcountlimit",
      "setwindow",
    ].forEach((c) => n._receive({ payload: c }));
    check(
      "no counted events from command payloads",
      portMsgs(n, 3).filter((m) => m.timerEvent === "counted").length === 0,
      trace(n),
    );
    check("nothing on Trigger output", portMsgs(n, 1).length === 0);
    await n._close();
  }

  section("1.4 Stop / Reset");
  {
    const n = makeNode({});
    n._receive({ payload: "stop" });
    check(
      "stop while idle is ignored:true",
      lastOn(n, 3).timerEvent === "stopped" && lastOn(n, 3).ignored === true,
    );
    n._receive({ payload: "reset" });
    check(
      "reset while idle is ignored:true",
      lastOn(n, 3).timerEvent === "reset" && lastOn(n, 3).ignored === true,
    );

    n._receive({ payload: "m1" });
    n._receive({ payload: "stop" });
    let s = lastOn(n, 3);
    check(
      "stop while counting: genuine stopped, back to idle, count wiped",
      s.timerEvent === "stopped" &&
        s.ignored === false &&
        s.timerState === "idle" &&
        s.count === 0,
    );

    n._receive({ payload: "m1" });
    n._receive({ payload: "m2" });
    n._receive({ payload: "reset" });
    s = lastOn(n, 3);
    check(
      "reset while counting: genuine reset, back to idle, count wiped",
      s.timerEvent === "reset" &&
        s.ignored === false &&
        s.timerState === "idle" &&
        s.count === 0,
    );
    n._receive({ payload: "m1" });
    check(
      "node re-armed after reset (next message is count 1)",
      lastOn(n, 3).count === 1,
    );
    await n._close();
  }

  section("1.5 Disable / Enable gating");
  {
    const n = makeNode({ window: "1", windowunits: "Second" }); // short window for the expiry check below
    n._receive({ payload: "enable" });
    check(
      "redundant enable is ignored:true",
      lastOn(n, 3).timerEvent === "enabled" && lastOn(n, 3).ignored === true,
    );
    n._receive({ payload: "m1" }); // in-flight cycle
    n._receive({ payload: "disable" });
    check(
      "genuine disable",
      lastOn(n, 3).timerEvent === "disabled" && lastOn(n, 3).ignored === false,
    );
    n._receive({ payload: "disable" });
    check("redundant disable is ignored:true", lastOn(n, 3).ignored === true);
    n._receive({ payload: "m2" });
    const blocked = lastOn(n, 3);
    check(
      "candidate while disabled: counted/ignored:true, count unchanged",
      blocked.timerEvent === "counted" &&
        blocked.ignored === true &&
        blocked.count === 1,
    );
    check(
      "blocked candidate tracked in ignoredCount with timestamp",
      blocked.ignoredCount === 1 && blocked.lastIgnoredTime !== null,
    );

    // Disable does NOT wipe the in-flight window - it expires naturally.
    await sleep(1200);
    const exp = portMsgs(n, 3).filter((m) => m.timerEvent === "windowexpired");
    check(
      "in-flight window expired naturally while disabled",
      exp.length === 1 &&
        exp[0].disabled === true &&
        exp[0].source === "internal",
      trace(n),
    );
    n._receive({ payload: "enable" });
    check(
      "genuine enable",
      lastOn(n, 3).timerEvent === "enabled" && lastOn(n, 3).ignored === false,
    );
    n._receive({ payload: "m3" });
    check(
      "counting resumes after enable, ignoredCount reset on new cycle",
      lastOn(n, 3).timerEvent === "counted" &&
        lastOn(n, 3).count === 1 &&
        lastOn(n, 3).ignoredCount === 0,
    );
    await n._close();
  }

  section("1.6 setcountlimit");
  {
    const n = makeNode({});
    n._receive({ payload: "setcountlimit", setcountlimit: 0 });
    check(
      "limit 0 rejected, attempted value included",
      lastOn(n, 3).timerEvent === "countlimitset" &&
        lastOn(n, 3).ignored === true &&
        lastOn(n, 3).countLimitSet === 0,
    );
    n._receive({ payload: "setcountlimit", setcountlimit: 2.5 });
    check("non-integer rejected", lastOn(n, 3).ignored === true);
    n._receive({ payload: "setcountlimit", setcountlimit: "abc" });
    check("non-numeric rejected", lastOn(n, 3).ignored === true);

    n._receive({ payload: "setcountlimit", setcountlimit: 5 });
    check(
      "valid limit applied",
      lastOn(n, 3).ignored === false && lastOn(n, 3).countLimit === 5,
    );

    n._receive({ payload: "p1" });
    n._receive({ payload: "p2" });
    n._receive({ payload: "setcountlimit", setcountlimit: 2 });
    const trig = portMsgs(n, 1);
    check(
      "lowering limit to live count fires Trigger immediately",
      trig.length === 1,
      trace(n),
    );
    check(
      "that trigger clones the LAST COUNTED message (originalMsg lineage)",
      trig[0].payload === "p2",
    );
    const evs = portMsgs(n, 3).map((m) => m.timerEvent);
    check(
      "countlimitset dispatched before the trigger",
      evs.indexOf("countlimitset", 3) < evs.indexOf("triggered"),
      evs.join(","),
    );
    await n._close();
  }

  section("1.7 setwindow");
  {
    // Invalid values
    const n = makeNode({});
    n._receive({ payload: "setwindow", setwindow: -5 });
    check(
      "negative window rejected with attempted value",
      lastOn(n, 3).timerEvent === "windowset" &&
        lastOn(n, 3).ignored === true &&
        lastOn(n, 3).windowSet === -5,
    );
    n._receive({
      payload: "setwindow",
      setwindow: 2,
      setwindowunits: "Seconds",
    });
    check(
      "valid window with units applied (2s -> 2000ms)",
      lastOn(n, 3).ignored === false && lastOn(n, 3).windowDuration === 2000,
    );
    await n._close();

    // Fixed re-anchor causing immediate expiry
    const nf = makeNode({ windowmode: "fixed" });
    nf._receive({ payload: "m1" });
    await sleep(100); // 100ms elapsed in the cycle
    nf._receive({ payload: "setwindow", setwindow: 50 }); // ms - re-anchored expiry is now in the past
    const evf = portMsgs(nf, 3).map((m) => m.timerEvent);
    check(
      "fixed: shrinking window past elapsed time expires the cycle immediately",
      evf.indexOf("windowset") >= 0 &&
        evf.indexOf("windowexpired") > evf.indexOf("windowset"),
      evf.join(","),
    );
    check("no trigger from a setwindow expiry", portMsgs(nf, 1).length === 0);
    await nf._close();

    // Sliding partial re-prune
    const ns = makeNode({ windowmode: "sliding" });
    ns._receive({ payload: "old" });
    await sleep(300);
    ns._receive({ payload: "new" });
    ns._receive({ payload: "setwindow", setwindow: 200 }); // ms - "old" (age ~300) prunes, "new" survives
    const q = (ns._receive({ payload: "query" }), lastOn(ns, 2));
    check(
      "sliding: re-prune under new window drops aged message only (count 2 -> 1)",
      q.count === 1 && q.timerState === "counting",
      "count=" + q.count + " state=" + q.timerState,
    );
    await ns._close();
  }
}

async function section2_asyncTimerBehavior() {
  section("2.1 Fixed window natural expiry");
  {
    const n = makeNode({
      windowmode: "fixed",
      window: "300",
      windowunits: "Millisecond",
    });
    n._receive({ payload: "only-one" });
    await sleep(500);
    const exp = portMsgs(n, 3).filter((m) => m.timerEvent === "windowexpired");
    check("windowexpired fired once", exp.length === 1, trace(n));
    check(
      "windowexpired is internal, count 0, idle",
      exp.length === 1 &&
        exp[0].source === "internal" &&
        exp[0].count === 0 &&
        exp[0].timerState === "idle",
    );
    check(
      "windowexpired clones originalMsg (last counted message)",
      exp.length === 1 && exp[0].payload === "only-one",
    );
    check("nothing on Trigger output", portMsgs(n, 1).length === 0);
    await n._close();
  }

  section("2.2 Sliding decay: intermediate silent, decay-to-zero emits");
  {
    const n = makeNode({
      windowmode: "sliding",
      window: "400",
      windowunits: "Millisecond",
    });
    n._receive({ payload: "a" }); // decays at ~400
    await sleep(150);
    n._receive({ payload: "b" }); // decays at ~550
    await sleep(300); // now ~450: "a" has decayed, "b" still live
    const midEvents = portMsgs(n, 3).filter(
      (m) => m.timerEvent === "windowexpired",
    );
    check(
      "intermediate decay (2 -> 1) emitted NO event",
      midEvents.length === 0,
      trace(n),
    );
    n._receive({ payload: "query" });
    check(
      "decayed count observable via query",
      lastOn(n, 2).count === 1 && lastOn(n, 2).timerState === "counting",
    );
    await sleep(300); // now ~750: "b" decayed too - zero
    const exp = portMsgs(n, 3).filter((m) => m.timerEvent === "windowexpired");
    check(
      "decay to ZERO emitted windowexpired once",
      exp.length === 1,
      trace(n),
    );
    check(
      "decay-to-zero is internal and settles to idle",
      exp.length === 1 &&
        exp[0].source === "internal" &&
        exp[0].timerState === "idle",
    );
    await n._close();
  }

  section("2.3 Cooldown lifecycle");
  {
    const n = makeNode({
      countlimit: "2",
      cooldownduration: "400",
      cooldownunits: "Millisecond",
    });
    n._receive({ payload: "m1" });
    n._receive({ payload: "m2" }); // trigger -> cooldown
    const cs = portMsgs(n, 3).filter((m) => m.timerEvent === "cooldownstarted");
    check(
      "cooldownstarted after trigger (internal)",
      cs.length === 1 && cs[0].source === "internal",
      trace(n),
    );
    check(
      "cooldownstarted reports cooldown state with remaining",
      cs[0].timerState === "cooldown" && cs[0].cooldownRemaining === 400,
    );

    n._receive({ payload: "blocked" });
    const b = lastOn(n, 3);
    check(
      "candidate during cooldown: counted/ignored:true",
      b.timerEvent === "counted" && b.ignored === true,
    );

    n._receive({ payload: "reset" });
    check(
      "reset during cooldown is ignored:true (does not cancel it)",
      lastOn(n, 3).timerEvent === "reset" &&
        lastOn(n, 3).ignored === true &&
        lastOn(n, 3).timerState === "cooldown",
    );

    await sleep(600);
    const ce = portMsgs(n, 3).filter((m) => m.timerEvent === "cooldownended");
    check(
      "cooldownended fired naturally (internal, idle)",
      ce.length === 1 &&
        ce[0].source === "internal" &&
        ce[0].timerState === "idle",
      trace(n),
    );
    check(
      "cooldownended did NOT re-fire the Trigger output",
      portMsgs(n, 1).length === 1,
    );
    n._receive({ payload: "fresh" });
    check(
      "ignoredCount reset when the new cycle begins",
      lastOn(n, 3).count === 1 && lastOn(n, 3).ignoredCount === 0,
    );
    await n._close();
  }

  section("2.4 Stop cancels cooldown early");
  {
    const n = makeNode({
      countlimit: "1",
      cooldownduration: "5",
      cooldownunits: "Second",
    });
    n._receive({ payload: "go" }); // limit 1: immediate trigger -> long cooldown
    n._receive({ payload: "stop" });
    const s = lastOn(n, 3);
    check(
      "stop during cooldown: genuine stopped, idle immediately",
      s.timerEvent === "stopped" &&
        s.ignored === false &&
        s.timerState === "idle",
      trace(n),
    );
    n._receive({ payload: "again" });
    // limit is 1, so an accepted message doesn't emit "counted" - it
    // triggers again immediately. A second trigger IS the proof the
    // cancelled cooldown released the gate.
    check(
      "counting available immediately after the cancelled cooldown (second trigger fires)",
      portMsgs(n, 1).length === 2 && lastOn(n, 1).payload === "again",
      trace(n),
    );
    n._receive({ payload: "stop" }); // cancel the second cooldown too, so no 5s timer lingers
    await sleep(100);
    check(
      "no stray cooldownended after cancellation",
      portMsgs(n, 3).filter((m) => m.timerEvent === "cooldownended").length ===
        0,
    );
    await n._close();
  }

  section("2.5 Heartbeat");
  {
    const n = makeNode({
      heartbeatinterval: "100",
      heartbeatintervalunits: "Millisecond",
    });
    n._receive({ payload: "query" });
    await sleep(250);
    check(
      "no heartbeat while idle",
      portMsgs(n, 2).length === 1,
      "got " + portMsgs(n, 2).length,
    );

    n._receive({ payload: "m1" }); // cycle begins -> heartbeat starts
    await sleep(350);
    const hb = portMsgs(n, 2).filter((m) => m.source === "internal");
    check(
      "heartbeat ticking while counting (~3 ticks in 350ms)",
      hb.length >= 2 && hb.length <= 4,
      "got " + hb.length,
    );
    check(
      "heartbeat ticks are query events cloning originalMsg",
      hb.length > 0 && hb[0].timerEvent === "query" && hb[0].payload === "m1",
    );

    n._receive({ payload: "stop" });
    const countAtStop = portMsgs(n, 2).length;
    await sleep(300);
    check(
      "heartbeat stops when node returns to idle",
      portMsgs(n, 2).length === countAtStop,
    );
    await n._close();
  }

  section("2.6 Heartbeat continues through cooldown");
  {
    const n = makeNode({
      countlimit: "1",
      cooldownduration: "350",
      cooldownunits: "Millisecond",
      heartbeatinterval: "100",
      heartbeatintervalunits: "Millisecond",
    });
    n._receive({ payload: "go" }); // immediate trigger -> cooldown
    await sleep(250); // mid-cooldown
    const during = portMsgs(n, 2).filter((m) => m.timerState === "cooldown");
    check(
      "heartbeat ticks during cooldown report cooldown state",
      during.length >= 1,
      trace(n),
    );
    await sleep(400); // cooldown ended at ~350
    const after = portMsgs(n, 2).length;
    await sleep(250);
    check(
      "heartbeat stops after cooldownended",
      portMsgs(n, 2).length === after,
    );
    await n._close();
  }
}

async function section3_persistenceRestores() {
  // Each restore test crafts a persist file directly, then constructs a
  // node with that id and persist:true - the constructor runs the restore.

  section("3.1 Sliding restore: prune against wall clock");
  {
    const id = "restore-sliding";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 10000, now - 200], // first long-dead under a 1s window, second live
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 1000,
    });
    const n = makeNode({ id: id, persist: true, windowmode: "sliding" });
    check("restore emitted no events", allEvents(n).length === 0, trace(n));
    n._receive({ payload: "query" });
    const q = lastOn(n, 2);
    check(
      "aged message fell away, live one survived (count 1, counting)",
      q.count === 1 && q.timerState === "counting",
      "count=" + q.count + " state=" + q.timerState,
    );
    await sleep(1100); // survivor decays -> zero
    check(
      "restored cycle decays to zero normally (windowexpired)",
      portMsgs(n, 3).filter((m) => m.timerEvent === "windowexpired").length ===
        1,
      trace(n),
    );
    await n._close();
  }

  section("3.2 Sliding restore: everything aged out -> idle, silent");
  {
    const id = "restore-sliding-empty";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 9000, now - 8000],
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 1000,
    });
    const n = makeNode({ id: id, persist: true, windowmode: "sliding" });
    check(
      "no events (no retroactive windowexpired)",
      allEvents(n).length === 0,
      trace(n),
    );
    n._receive({ payload: "query" });
    check(
      "settled to idle, count 0",
      lastOn(n, 2).timerState === "idle" && lastOn(n, 2).count === 0,
    );
    await n._close();
  }

  section("3.3 Fixed restore: continues and expires on schedule");
  {
    const id = "restore-fixed";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 3200, now - 3100], // anchor 3.2s ago, 3.5s window -> ~300ms left (>3s guard not hit... see 3.4)
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 5,
      effectiveWindowMS: 6500,
    });
    // 6.5s window, anchor 3.2s ago -> ~3.3s remaining (just above the 3s randomization guard)
    const n = makeNode({ id: id, persist: true, windowmode: "fixed" });
    check("restore emitted no events", allEvents(n).length === 0, trace(n));
    n._receive({ payload: "query" });
    const q = lastOn(n, 2);
    check(
      "resumed counting at persisted count with recalculated remaining",
      q.count === 2 &&
        q.timerState === "counting" &&
        q.windowRemaining > 3000 &&
        q.windowRemaining <= 3400,
      "count=" + q.count + " rem=" + q.windowRemaining,
    );
    await sleep(3600);
    check(
      "restored window expired on the recalculated schedule",
      portMsgs(n, 3).filter((m) => m.timerEvent === "windowexpired").length ===
        1,
      trace(n),
    );
    await n._close();
  }

  section("3.4 Fixed restore: <3s remaining randomizes to 3-8s");
  {
    const id = "restore-fixed-rand";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 4900], // 5s window -> ~100ms left -> randomize
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 5000,
    });
    const n = makeNode({ id: id, persist: true, windowmode: "fixed" });
    n._receive({ payload: "query" });
    const rem = lastOn(n, 2).windowRemaining;
    check(
      "nearly-expired window randomized into the 3-8s band",
      rem >= 2800 && rem <= 8200,
      "remaining=" + rem,
    );
    await n._close();
  }

  section("3.5 Fixed restore: already expired during downtime -> idle, silent");
  {
    const id = "restore-fixed-expired";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 60000],
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 5000,
    });
    const n = makeNode({ id: id, persist: true, windowmode: "fixed" });
    check(
      "no events (no retroactive windowexpired)",
      allEvents(n).length === 0,
      trace(n),
    );
    n._receive({ payload: "query" });
    check(
      "settled to idle",
      lastOn(n, 2).timerState === "idle" && lastOn(n, 2).count === 0,
    );
    await n._close();
  }

  section("3.6 Cooldown restore");
  {
    const id = "restore-cooldown";
    const now = Date.now();
    writePersist(id, {
      timerState: "cooldown",
      timestamps: [],
      cooldownActive: true,
      cooldownTarget: new Date(now + 400).toISOString(),
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 5000,
    });
    const n = makeNode({ id: id, persist: true });
    n._receive({ payload: "blocked" });
    check(
      "restored straight into cooldown (candidate blocked)",
      lastOn(n, 3).timerEvent === "counted" &&
        lastOn(n, 3).ignored === true &&
        lastOn(n, 3).timerState === "cooldown",
      trace(n),
    );
    await sleep(700);
    check(
      "restored cooldown completed naturally (cooldownended)",
      portMsgs(n, 3).filter((m) => m.timerEvent === "cooldownended").length ===
        1,
      trace(n),
    );
    check("nothing ever fired on Trigger output", portMsgs(n, 1).length === 0);
    await n._close();
  }

  section("3.7 Restore never fires the Trigger");
  {
    const id = "restore-no-trigger";
    const now = Date.now();
    writePersist(id, {
      timerState: "counting",
      timestamps: [now - 300, now - 200, now - 100], // 3 live, limit 3 - already "met"
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: false,
      ignoredCount: 0,
      lastIgnoredTime: null,
      effectiveCountLimit: 3,
      effectiveWindowMS: 60000,
    });
    const n = makeNode({ id: id, persist: true, windowmode: "sliding" });
    check(
      "restore with count == limit did NOT trigger",
      portMsgs(n, 1).length === 0,
      trace(n),
    );
    n._receive({ payload: "live-one" });
    check(
      "next LIVE message fires the trigger",
      portMsgs(n, 1).length === 1,
      trace(n),
    );
    await n._close();
  }

  section("3.8 Idle restore carries disabled + runtime overrides");
  {
    const id = "restore-idle-context";
    writePersist(id, {
      timerState: "idle",
      timestamps: [],
      cooldownActive: false,
      cooldownTarget: null,
      origmsg: { payload: "persisted" },
      disabled: true,
      ignoredCount: 4,
      lastIgnoredTime: new Date().toISOString(),
      effectiveCountLimit: 7,
      effectiveWindowMS: 2500,
    });
    const n = makeNode({ id: id, persist: true });
    n._receive({ payload: "should-block" });
    const b = lastOn(n, 3);
    check(
      "disabled state survived the restore (candidate blocked)",
      b.timerEvent === "counted" && b.ignored === true && b.disabled === true,
      trace(n),
    );
    check(
      "runtime overrides survived (limit 7, window 2500ms)",
      b.countLimit === 7 && b.windowDuration === 2500,
      "limit=" + b.countLimit + " win=" + b.windowDuration,
    );
    await n._close();
  }

  section("3.9 Live round-trip: node writes state a fresh instance restores");
  {
    const id = "roundtrip";
    const n1 = makeNode({
      id: id,
      persist: true,
      windowmode: "sliding",
      window: "5",
      windowunits: "Second",
    });
    n1._receive({ payload: "rt1" });
    n1._receive({ payload: "rt2" });
    n1._receive({ payload: "setcountlimit", setcountlimit: 4 });
    await n1._close(); // simulate redeploy (not removal)

    const n2 = makeNode({
      id: id,
      persist: true,
      windowmode: "sliding",
      window: "5",
      windowunits: "Second",
    });
    n2._receive({ payload: "query" });
    const q = lastOn(n2, 2);
    check(
      "round-trip restored count, state, and override",
      q.count === 2 && q.timerState === "counting" && q.countLimit === 4,
      "count=" + q.count + " state=" + q.timerState + " limit=" + q.countLimit,
    );
    check(
      "round-trip preserved originalMsg lineage (heartbeat/expiry base)",
      true,
    );
    n2._receive({ payload: "rt3" });
    n2._receive({ payload: "rt4" });
    check(
      "restored cycle triggers on the restored override limit",
      portMsgs(n2, 1).length === 1,
      trace(n2),
    );
    await n2._close();
  }

  section("3.10 Removal deletes the persist file");
  {
    const id = "removal";
    const n = makeNode({ id: id, persist: true });
    n._receive({ payload: "m1" });
    check("persist file written", fs.existsSync(persistFile(id)));
    await n._close(true); // removed = true
    check(
      "persist file deleted on node removal",
      !fs.existsSync(persistFile(id)),
    );
  }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

(async function () {
  const started = Date.now();
  console.log("timer-threshold test harness");
  console.log("userDir: " + userDir);

  try {
    await section1_syncRoutingAndCommands();
    await section2_asyncTimerBehavior();
    await section3_persistenceRestores();
  } catch (err) {
    failures++;
    console.error(
      "\nHARNESS EXCEPTION: " + (err && err.stack ? err.stack : err),
    );
  }

  // Close anything a failed test left open so timers can't keep us alive
  for (const n of openNodes) {
    try {
      if (n._handlers && n._handlers["close"]) await n._close();
    } catch {
      /* already closed */
    }
  }

  console.log("\n----------------------------------------");
  console.log(
    "PASS: " +
      passes +
      "   FAIL: " +
      failures +
      "   (" +
      ((Date.now() - started) / 1000).toFixed(1) +
      "s)",
  );
  console.log("----------------------------------------");
  process.exit(failures > 0 ? 1 : 0);
})();
