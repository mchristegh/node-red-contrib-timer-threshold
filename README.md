# timer-threshold

A threshold-counting node for [Node-RED](https://nodered.org/) that
triggers an output after a configured number of messages arrive within a
fixed or sliding time window. Designed to suppress false positives from
sensor noise: rather than acting on a single threshold breach, it requires
the breach to repeat N times within Y time before firing.

Sibling of [`node-red-contrib-timer-events`](https://github.com/mchristegh/node-red-contrib-timer-events) —
the two nodes share the same event envelope, command conventions, and
output philosophy, so downstream flows can process both with the same code.

## Why this node

A humidity sensor fluctuating around its alert threshold will breach it
briefly and repeatedly without anything actually being wrong. Wiring the
sensor through this node — for example, "3 exceedances within 15
minutes" — means a single blip never triggers, while a genuinely
persistent condition still alerts quickly: the Trigger fires the instant
the count is met, never waiting for the window to close. And like its
sibling, everything is observable: every counted message, blocked
message, expiry, and cooldown transition is a tagged event with a
consistent envelope describing the node's full state at that moment.

## Outputs

| #   | Output      | Fires on                                                                          |
| --- | ----------- | --------------------------------------------------------------------------------- |
| 1   | **Trigger** | The count reaching the limit within the window. Nothing else.                     |
| 2   | **Query**   | An incoming `query` message, or a Heartbeat tick.                                 |
| 3   | **Events**  | Every event, including a copy of every Trigger and every ignored/blocked message. |

Output 1 never carries a blocked or redundant message — anything that
didn't truly happen appears only on output 3, tagged `msg.ignored: true`.
The Trigger message is a clone of the final counted message, so its
payload and topic carry through downstream.

## Features at a glance

- **Fixed or sliding window** — fixed anchors one shared clock to the
  first message of a cycle; sliding lets each message age out
  individually, so the count decays naturally as messages fall out of the
  window
- **Control commands** — `stop`, `reset`, `query`, `disable`, `enable`,
  `setcountlimit`, `setwindow` (all case-insensitive); anything else is
  counted
- **Event envelope** — every output message carries `timerEvent`,
  `timerState`, `count`, `countLimit`, `windowMode`, `windowRemaining`,
  `ignored`, `source`, and more
- **Cooldown** — block counting for a fixed period after each Trigger to
  prevent rapid re-triggering from a noisy source
- **Heartbeat** — periodic status snapshots on the Query output for
  monitoring the decaying count or watchdogging long windows
- **Persistence** — resume a counting cycle or cooldown across deploys
  and Node-RED restarts, with sliding-window timestamps pruned against
  the wall clock

## Install

From your Node-RED user directory (typically `~/.node-red`):

```bash
npm install node-red-contrib-timer-threshold
```

Or via the Node-RED palette manager. The node appears in the **function**
category as **Timer Threshold**.

## Documentation

Full documentation — configuration reference, event taxonomy, command
details, window-mode behavior, persistence, example flows, and
troubleshooting — lives in the [project wiki](../../wiki). The node's
built-in help panel in the Node-RED editor also covers everyday usage.

## Testing

The repository includes a self-contained test harness
(`test-scripts/test-harness.js`) covering routing, gating, both window
modes, cooldown, heartbeat, and every persistence restore path. Run it
from the repo root with `npm test`; it also runs automatically before
every `npm publish`.

## License

Licensed under the [Apache License 2.0](LICENSE).

Copyright (C) 2026 mchristegh.# node-red-contrib-timer-threshold
