# timer-threshold — Design Document

## Overview

`timer-threshold` is a Node-RED node that triggers an output after a
configured number of messages arrive within a configured time window. It
exists to suppress false positives from sensor noise: rather than acting
on a single threshold breach, it requires the breach to repeat N times
within Y time before firing.

It is the sibling of `timer-events` and deliberately replicates its
design language: purpose-built outputs, a consistent event envelope
(`ignored`, `source`), case-insensitive control commands, an
Events output that captures everything (including blocked attempts),
Heartbeat, status-label-only Reporting, and the same persistence model.
Where `timer-events` debounces by *ignoring* repeat messages, this node
requires *accumulation* of repeat messages.

**Use case example:** A humidity sensor fluctuates around 55%. Rather
than alerting on the first exceedance, require 3 exceedances within a
15-minute window before triggering.

The node has **1 input** and **3 outputs**:

| # | Label   | Fires on |
|---|---------|----------|
| 1 | Trigger | The count reaching the limit within the window. Nothing else. |
| 2 | Query   | An incoming `query` message, or a Heartbeat tick. Nothing else. |
| 3 | Events  | Every other event, plus a duplicate copy of every Trigger event. The only output where `msg.ignored` can be `true`. |

Output 1 **never** carries a blocked/ignored message — anything that
didn't truly happen only appears on output 3.

---

## Window Modes

A dropdown selects one of two window behaviors. This is the node's most
consequential configuration choice.

### Fixed Window
- The window is anchored to the **first counted message** of a cycle.
- One shared clock: message 1 starts a Y-duration countdown.
- If the count reaches the limit before the clock expires → Trigger.
- If the clock expires first → the entire count resets at once
  (`windowexpired` event), and the node returns to idle.

### Sliding Window
- Each counted message is individually relevant for Y after **its own**
  arrival, then ages out.
- On every arrival, timestamps older than `now - Y` are pruned, the new
  arrival is appended, and the count is checked against the limit.
- The count can therefore **decay stepwise** as individual messages age
  out. Example (limit 3, window 3 min): messages at 0:00 and 2:00 → at
  3:00 the first ages out and the count drops from 2 to 1; a message at
  4:00 brings it back to 2 (the 2:00 and 4:00 messages), not 3.
- Intermediate decay does **not** emit an event — it is observable via
  status, `query`, and Heartbeat. Only decay **to zero** (the last live
  message aging out) emits an event, since that is the counting → idle
  transition.

Both modes emit the same `windowexpired` event for their counting → idle
transition (fixed: the shared window expired; sliding: the last message
aged out). The envelope's `windowMode` field distinguishes them if a
consumer cares.

---

## States (`msg.timerState`)

```
                       stop / reset
        ┌──────────────────────────────────────┐
        │                                      │
      idle ──first counted message──► counting─┤
        ▲                                │     │
        │                          count = limit
        │                                │
        ├──── windowexpired ─────────────┤
        │     (fixed: window closed;     │
        │      sliding: decayed to zero) ▼
        │                            TRIGGER (output 1 + 3)
        │                                │
        ├──── no cooldown configured ────┤
        │                                │ cooldown configured
        │                                ▼
        └──── cooldownended ───────── cooldown
              (stop also exits early)
```

- `idle` — no live counted messages; ready for a new cycle
- `counting` — at least one counted message is live; count < limit
- `cooldown` — a timed block on counting following a Trigger

There is no `paused` state — pause/resume was deliberately not carried
over (see "What Was Deliberately Dropped").

---

## Message Envelope

Every output message is a **clone of the message that triggered it** (see
"originalMsg lineage" below), with these properties layered on top. The
property names deliberately match `timer-events` so downstream
function/switch nodes can process both nodes' output with the same code.

| Property | Description |
|---|---|
| `msg.timerEvent` | The event type (see table below) |
| `msg.timerState` | `idle`, `counting`, or `cooldown` |
| `msg.count` | Current live count |
| `msg.countLimit` | The configured (or runtime-overridden) count limit |
| `msg.windowMode` | `fixed` or `sliding` |
| `msg.windowDuration` | The window length in ms |
| `msg.windowRemaining` | Fixed: ms until the window expires. Sliding: ms until the **oldest** live message ages out. `0` when idle. Cooldown time remaining while in cooldown. |
| `msg.cooldownRemaining` | ms left in cooldown, `0` otherwise |
| `msg.ignoredCount` | Messages ignored (not counted) during the current cooldown/disabled period |
| `msg.lastIgnoredTime` | ISO 8601 timestamp of the last ignored message, or `null` |
| `msg.disabled` | Current disabled state (boolean) |
| `msg.ignored` | `true` if this message was received but did not take effect. Always `false` on outputs 1 and 2. |
| `msg.source` | `"external"` (a live incoming message) or `"internal"` (window expiry, decay-to-zero, cooldown transitions, heartbeat tick, persisted restore) |

Event-specific extras (`countLimitSet`, `windowSet`) are added only to
the relevant event types.

---

## Event Type Taxonomy (`msg.timerEvent`)

| Event | Output(s) | Can be `ignored:true`? | `source` values |
|---|---|---|---|
| `counted` | 3 only | Yes (message arrived during cooldown or while disabled — a blocked count attempt) | external |
| `triggered` | 1 + 3 | No | external |
| `windowexpired` | 3 only | No | internal only |
| `stopped` | 3 only | Yes (`stop` while already idle) | external |
| `reset` | 3 only | Yes (`reset` while already idle) | external |
| `disabled` | 3 only | Yes (redundant disable) | external |
| `enabled` | 3 only | Yes (redundant enable) | external |
| `countlimitset` | 3 only | Yes (invalid value) | external |
| `windowset` | 3 only | Yes (invalid value) | external |
| `query` | 2 only | No | external, internal |
| `cooldownstarted` | 3 only | No | internal only |
| `cooldownended` | 3 only | No | internal only |

**Key design decision — `ignored` as a modifier, not a category:** same
philosophy as `timer-events`. A message discarded during cooldown is
still labeled `counted` (with `ignored:true`) — the event type says what
was *attempted*, `ignored` says whether it *took effect*. The first
counted message of a cycle is distinguishable by `count === 1` and
`timerState` transitioning to `counting`; there is no separate
"cycle started" event.

**`triggered` fires exactly once per cycle**, on the message that pushes
the count to the limit, immediately — the node never waits for the window
to close. The Trigger output message is a clone of that final counted
message (not a synthetic message), so its payload/topic carry through to
downstream nodes, with the envelope layered on top. The envelope on a
`triggered` message reports the **settled post-trigger state** — count
`0`, `windowRemaining` `0`, and `timerState` of `idle` (a subsequent
`cooldownstarted` reports the cooldown, if one begins) — not a
half-torn-down mid-transition snapshot. A `setcountlimit`-induced Trigger
(lowering the limit to at or below the live count) has no completing
message of its own, so it clones `originalMsg` — the last counted
message — instead.

**Note:** the message that causes a Trigger emits only `triggered` — it
does not additionally emit a `counted` event for itself.

---

## Control Commands (`msg.payload`, case-insensitive)

Any message whose payload is **not** a recognized command is a count
candidate. Recognized commands are never counted.

| Command | Effect |
|---|---|
| `stop` | The universal escape hatch. While counting: wipes the count, returns to idle. While in cooldown: cancels the cooldown immediately, returns to idle. While idle: `ignored:true`. |
| `reset` | Wipes the count back to 0 but stays armed — the next message becomes count 1. Unlike `stop`, it does not cancel a cooldown (`ignored:true` during cooldown; use `stop` for that). While idle: `ignored:true`. |
| `query` | Returns a full snapshot on output 2, no side effects. Works in every state. |
| `disable` | Blocks messages from being counted. An in-flight window is left to expire/decay naturally — `disable` does not wipe it. All commands still work while disabled. Redundant disable: `ignored:true`. |
| `enable` | Re-allows counting. Has no effect on an active cooldown. Redundant enable: `ignored:true`. |
| `setcountlimit` | Sets the count limit for the current and future cycles. Requires `msg.setcountlimit`, must be a positive integer; invalid: `ignored:true` with the attempted value included. If the live count already meets the new lower limit, the Trigger fires immediately. |
| `setwindow` | Sets the window duration. Requires `msg.setwindow` in ms; `msg.setwindowunits` optionally overrides units (same unit strings as `timer-events`). Must be positive; invalid: `ignored:true`. Applies immediately: fixed mode re-anchors the remaining time from the original window start; sliding mode re-prunes against the new duration on the next evaluation. |

Blocked/invalid attempts include the attempted value on the event message
(`countLimitSet`, `windowSet`) so downstream consumers can see what was
rejected. No `node.warn()` calls — the `ignored:true` output-3 event is
the sole surfacing mechanism.

---

## Blocking / Gating Rules

Two independent conditions block a count candidate. Each produces a
`counted` / `ignored:true` event on output 3 (never output 1):

1. **Cooldown gate** — while in `cooldown`, count candidates are
   discarded (not counted, not queued). This replaces the original
   spec's optional "ignored message notification" output and its
   `outputIgnoredMessages` flag — the Events output now carries this
   unconditionally.
2. **Disabled gate** — while `disabled`, count candidates are discarded
   the same way, in any state.

`ignoredCount` / `lastIgnoredTime` track these discards and reset to 0
when a new counting cycle begins.

---

## Feature: Cooldown (Recovery)

The original spec's `recoveryMode: reset | cooldown` enum is **dropped**
in favor of the `timer-events` convention: a *Cooldown Duration* of `0`
(default) disables cooldown entirely, which **is** simple-reset behavior.
A non-zero duration enables cooldown.

- **Sequence:** `counting` → `triggered` (output 1 + 3, fires exactly
  once) → `cooldown` (output 3: `cooldownstarted`) → cooldown ends →
  `idle` (output 3: `cooldownended`).
- Cooldown begins **only** after a genuine Trigger.
- Count candidates during cooldown are blocked (see gating above).
- `stop` during cooldown cancels it immediately (`stopped`, output 3) —
  the only way to cut a cooldown short. `enable` has no effect on it.
- Heartbeat keeps ticking through cooldown.
- `query` during cooldown reports `timerState: "cooldown"` with
  `windowRemaining` / `cooldownRemaining` reflecting cooldown time left.
- Runs on its own dedicated timer handles, fully separate from the
  window timer handles, mirroring the `timer-events` isolation rule.

## Feature: Heartbeat

- Configurable fixed-interval tick (`heartbeatinterval` + units, `0`
  disables) firing a Query-output message (output 2,
  `source: "internal"`) on a fixed schedule.
- Starts when a counting cycle begins (first counted message); keeps
  ticking through `counting` and `cooldown`; stops whenever the node
  returns to `idle` — windowexpired, `stop`, `reset`, or cooldownended.
  (A Trigger with no cooldown configured returns to idle too, so it also
  stops the heartbeat.)
- Independent `setInterval`, unaffected by setwindow, setcountlimit, or
  gating — its schedule never shifts while a cycle is live.
- After a persisted restore, restarts fresh rather than recalculating
  the original schedule.

## Feature: Status Reporting (node status label only)

Same model as `timer-events`: purely cosmetic, drives the on-canvas
status text, produces **no output message**. Same two dropdowns:

- *Status Reporting*: Never (default) / Every Second / Every Minute,
  Last minute by seconds (same adaptive cadence).
- *Reporting Format*: HH:MM:SS (default) / seconds / minutes / hours.

Status text by state:

| State | Status |
|---|---|
| Idle | `Ready` (green dot) |
| Counting (fixed) | `Counting: 2/3 \| Window: 00:12:30` (yellow dot) |
| Counting (sliding) | `Counting: 2/3 \| Oldest expires: 00:01:42` (yellow dot) |
| Cooldown | `Cooldown: 00:02:30` (yellow dot) — no ignored-count detail, same brevity rule as `timer-events` |
| Just triggered | brief `Triggered` flash (blue dot), then cooldown or idle status |
| Disabled | grey ring, `Disabled \| ` prefixed to the normal status text; during cooldown just `Disabled` |

In sliding mode the countdown shown is the time until the **oldest** live
message ages out — the moment the displayed count will next change. When
it does, the label refreshes to the new count and the next-oldest
message's remaining time.

---

## `originalMsg` Lineage

Events with no live triggering message of their own clone the **last
counted message** as their payload base:

- **Set/overwritten on:** every genuinely counted message (including the
  one that fires the Trigger).
- **Read/cloned by:** `windowexpired`, `cooldownstarted`,
  `cooldownended`, Heartbeat ticks, and a persisted restore.
- **Untouched by:** stop, reset, disable, enable, setcountlimit,
  setwindow, query — these clone whatever message actually triggered
  them.

---

## Persistence (`Resume on deploy/restart`)

Disabled by default. When enabled, state is written to
`<userDir>/timerthreshold-timers/<node-id>` on every meaningful state
change and restored on node startup:

- **The full timestamp array is persisted** (not just a count), plus
  window mode, cooldown state, disabled state, runtime overrides
  (`setcountlimit` / `setwindow`), and `ignoredCount` /
  `lastIgnoredTime`.
- **Sliding restore:** prune the restored timestamps against the current
  wall clock — messages that aged out during downtime simply fall away.
  This restores more gracefully than a countdown: if live messages
  remain, the node resumes `counting` at the correct decayed count; if
  none remain, it settles to `idle`. No event is emitted either way.
- **Fixed restore:** recalculate the window's remaining time from the
  original anchor, same as a `timer-events` running restore. If the
  window already expired during downtime, settle to `idle` (no
  retroactive `windowexpired` event — the restore itself is not a live
  transition). If less than 3 seconds remain, randomize the remaining
  window to 3–8 seconds (same anti-flood rule as `timer-events`).
- **Cooldown restore:** restore directly into `cooldown` at the
  recalculated remaining time (randomized to 3–8s if negligible);
  Heartbeat restarts fresh.
- A restore never fires the Trigger output, even if the persisted count
  already met the limit — Trigger only ever fires on a live counted
  message.

Uses the same `JSON.decycle`/`retrocycle` file format conventions as
`timer-events`. Unrelated to Node-RED's built-in "Persistent Context."

**Deliberate deviation from `timer-events`:** the persist file is **not**
deleted when the node returns to idle — it is deleted only when the node
is removed (or persistence is turned off). An idle node can still carry
restorable context (the `disabled` flag, runtime overrides, ignored
tracking), and this also avoids a quirk in `timer-events` where a state
file written while idle can cause a phantom restore on restart. Safe
here because the restore logic branches on the saved `timerState`: an
idle restore reapplies context only and starts nothing.

---

## What Was Deliberately Dropped (from the original spec and from timer-events)

- **`recoveryMode` enum** — folded into "Cooldown Duration 0 = simple
  reset," matching the `timer-events` convention.
- **`outputIgnoredMessages` flag and dedicated ignored output** — folded
  into the Events output as `counted` / `ignored:true`, always on.
- **`originalMessages` array on the Trigger output** — dropped entirely;
  no benefit worth the memory risk with noisy sensors. Internal state
  keeps only timestamps.
- **`pause` / `resume`** — pausing a measurement window is semantically
  murky (freeze the clock? shift timestamps?) with no identified use
  case. Not carried over from `timer-events`.
- **`lock` / `unlock`** — no analog; there is no reset-on-message
  behavior to lock.
- **Threshold actions** — no analog; this node's entire purpose *is* the
  threshold.
- **"Message at the last millisecond" special handling** — the event
  loop serializes timer callbacks and message handling; a simple
  expiry-check-before-count is sufficient, no extra machinery.

---

## Configuration Reference

| Field | Default | Notes |
|---|---|---|
| Count Limit | 3 | Messages required to Trigger; runtime-overridable via `setcountlimit` |
| Window / Units | 15 Minutes | Runtime-overridable via `setwindow` |
| Window Mode | Fixed | Fixed / Sliding dropdown |
| Status Reporting | Never | Drives status label only, no output |
| Reporting Format | HH:MM:SS | Also used for cooldown status |
| Resume on deploy/restart | Off | Persistence |
| Cooldown Duration / Units | 0 / Second | `0` disables (= simple reset); blocks counting for a fixed period after a Trigger |
| Heartbeat Interval / Units | 0 / Second | `0` disables; fires Query output on a fixed schedule |
| Name | "" | Standard node name |

---

## Repository Structure

```
node-red-contrib-timer-threshold/     (repo root = npm package root)
├── package.json                       # node-red registration, test scripts
├── README.md                          # overview; details live in the wiki
├── timer-threshold/
│   ├── timer-threshold.js             # Runtime logic
│   ├── timer-threshold.html           # Editor UI + embedded help
│   └── cycle.js                       # Persistence serialization (shared
│                                      #   with timer-events; must sit next
│                                      #   to the .js for require('./cycle.js'))
└── test-scripts/
    └── test-harness.js                # Standalone assertion harness
```

`package.json` registers the node as
`"timer-threshold": "timer-threshold/timer-threshold.js"`. Because the
repo root is the package root, everything above (including the harness)
ships in the published npm package. Same file conventions as
`timer-events`: embedded help in the .html, constants blocks at module
level, `cycle.js` for persistence serialization.

---

## Testing

A standalone test harness (`test-scripts/test-harness.js`) stubs enough
of the Node-RED runtime to instantiate the node for real and asserts
90 checks across three sections:

1. **Synchronous routing & commands** — output exclusivity, envelope
   shape, `ignored` tagging, gating, both set commands (including
   lower-limit-fires-Trigger and `setwindow` re-evaluation in both
   window modes).
2. **Asynchronous timer behavior** — fixed expiry, sliding
   intermediate-decay-is-silent vs. decay-to-zero, the full cooldown
   lifecycle, `stop` cancelling a cooldown, heartbeat start/tick/stop
   and heartbeat-through-cooldown. Uses real timers with short
   durations; a full run takes ~11 seconds.
3. **Persistence restores** — every restore path, including crafted
   state files proving a restore never fires the Trigger even when the
   persisted count already meets the limit, and the 3–8 second
   randomization band.

The harness exits `0`/`1` for CI use, writes all persistence files to a
throwaway temp directory, and falls back to an identity `cycle.js` stub
(with a printed NOTE) if the real one is absent. It is wired to both
`npm test` (manual runs) and `prepublishOnly` — `npm publish` runs it
automatically and aborts the publish on any failure.