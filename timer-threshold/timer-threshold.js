/**
 * timer-threshold
 * A Node-RED node that triggers an output after a configured number of
 * messages arrive within a configured time window (fixed or sliding).
 * Designed to suppress false positives from sensor noise by requiring
 * repeated threshold breaches before acting.
 *
 * Sibling of timer-events - deliberately replicates its design language:
 * purpose-built outputs, a consistent event envelope (ignored, source),
 * case-insensitive control commands, and a 3-output event model:
 *   1. Trigger - fires only when the count genuinely reaches the limit
 *   2. Query   - fires on an incoming query message, or a heartbeat tick
 *   3. Events  - fires for every other event, including tagged copies of
 *                ignored/blocked attempts and a duplicate of every Trigger
 *
 * Copyright (C) 2026 mchristegh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
  "use strict";

  // ---------------------------------------------------------------------------
  // Module-level constants
  // ---------------------------------------------------------------------------

  const TIMER_STATE = {
    IDLE:     "idle",
    COUNTING: "counting",
    COOLDOWN: "cooldown"
  };

  // Canonical event-type list for output 3 (and output 2, for QUERY).
  // Note: there is no separate "cycle started" event - the first counted
  // message of a cycle is a normal COUNTED event, distinguishable by
  // msg.count === 1 and timerState transitioning to COUNTING. Both window
  // modes emit the same WINDOWEXPIRED for their counting -> idle
  // transition (fixed: the shared window expired; sliding: the last live
  // message aged out); msg.windowMode distinguishes them if needed.
  const TIMER_EVENT = {
    COUNTED:         "counted",
    TRIGGERED:       "triggered",
    WINDOWEXPIRED:   "windowexpired",
    STOPPED:         "stopped",
    RESET:           "reset",
    DISABLED:        "disabled",
    ENABLED:         "enabled",
    COUNTLIMITSET:   "countlimitset",
    WINDOWSET:       "windowset",
    QUERY:           "query",
    COOLDOWNSTARTED: "cooldownstarted",
    COOLDOWNENDED:   "cooldownended"
  };

  // Identifies whether an event was triggered by a live incoming message
  // ("external") or by the node itself ("internal") - e.g. a window
  // expiry, a sliding decay-to-zero, a cooldown transition, or a
  // heartbeat tick.
  const EVENT_SOURCE = {
    EXTERNAL: "external",
    INTERNAL: "internal"
  };

  const WINDOW_MODE = {
    FIXED:   "fixed",
    SLIDING: "sliding"
  };

  const UNITS = {
    MILLISECOND: "Millisecond",
    SECOND:      "Second",
    MINUTE:      "Minute",
    HOUR:        "Hour"
  };

  const UNITS_INPUT = {
    MILLISECOND: "millisecond",
    SECOND:      "second",
    MINUTE:      "minute",
    HOUR:        "hour"
  };

  const PAYLOAD = {
    STOP:          "stop",
    RESET:         "reset",
    QUERY:         "query",
    DISABLE:       "disable",
    ENABLE:        "enable",
    SETCOUNTLIMIT: "setcountlimit",
    SETWINDOW:     "setwindow"
  };

  const REPORTING_FORMAT = {
    HUMAN:   "human",
    SECONDS: "seconds",
    MINUTES: "minutes",
    HOURS:   "hours"
  };

  // Reporting only drives the node's status label (see startWindowReporting).
  // It never produces an output message - that role is served by the query
  // output (manual query or heartbeat tick).
  const REPORTING = {
    NONE:                "none",
    EVERY_SECOND:        "every_second",
    LAST_MINUTE_SECONDS: "last_minute_seconds"
  };

  // How long the "Triggered" status flash stays on the canvas before the
  // normal state status (cooldown or Ready) replaces it.
  const TRIGGER_FLASH_MS = 1500;

  // ---------------------------------------------------------------------------
  // Node definition
  // ---------------------------------------------------------------------------

  function TimerThreshold(n) {
    RED.nodes.createNode(this, n);
    let fs   = require('fs');
    let path = require('path');
    let nodefile = n.id.toString();
    let nodepath = "";
    require('./cycle.js');

    if (n._alias != null) {
      nodepath = n._flow.path.replace(/\//g, "-") + "-";
      nodefile = n._alias;
    }

    const thresholdFile = path.join(RED.settings.userDir, "timerthreshold-timers", nodepath + nodefile);

    // -------------------------------------------------------------------------
    // Node property initialization
    // -------------------------------------------------------------------------

    this.windowunits     = n.windowunits || UNITS.MINUTE;
    this.windowType      = n.windowType;
    this.window          = isNaN(Number(RED.util.evaluateNodeProperty(n.window, this.windowType, this, null))) ? 15 : Number(RED.util.evaluateNodeProperty(n.window, this.windowType, this, null));
    this.windowmode      = n.windowmode || WINDOW_MODE.FIXED;
    this.countlimit      = (!isNaN(Number(n.countlimit)) && Number(n.countlimit) >= 1) ? Math.floor(Number(n.countlimit)) : 3;
    this.reporting       = n.reporting       || REPORTING.NONE;
    this.reportingformat = n.reportingformat || REPORTING_FORMAT.HUMAN;
    this.persist         = n.persist         || false;
    this.cooldownduration       = isNaN(Number(n.cooldownduration))  ? 0 : Number(n.cooldownduration);
    this.cooldownunits          = n.cooldownunits || UNITS.SECOND;
    this.heartbeatinterval      = isNaN(Number(n.heartbeatinterval)) ? 0 : Number(n.heartbeatinterval);
    this.heartbeatintervalunits = n.heartbeatintervalunits || UNITS.SECOND;

    if (this.window <= 0) {
      this.window = 15;
      this.windowunits = UNITS.MINUTE;
    }
    if (this.windowunits === UNITS.SECOND) this.window = this.window * 1000;
    if (this.windowunits === UNITS.MINUTE) this.window = this.window * 1000 * 60;
    if (this.windowunits === UNITS.HOUR)   this.window = this.window * 1000 * 60 * 60;

    let node = this;

    // -------------------------------------------------------------------------
    // Runtime state variables
    // -------------------------------------------------------------------------

    const maxTimeout = 2147483647;

    // The heart of the node: epoch-ms arrival times of every currently-live
    // counted message. In fixed mode the first entry anchors the shared
    // window; in sliding mode each entry ages out individually,
    // effectiveWindowMS after its own arrival.
    let timestamps            = [];

    // Effective (runtime-overridable) copies of the configured limit and
    // window - the setcountlimit/setwindow commands write here, never to
    // the node config itself.
    let effectiveCountLimit   = this.countlimit;
    let effectiveWindowMS     = this.window;

    let timerState            = TIMER_STATE.IDLE;
    let disabled              = false;
    let ignoredCount          = 0;
    let lastIgnoredTime       = null;
    let originalMsg           = null;   // last counted message; reused as the payload base
                                         // for events with no live triggering msg of their
                                         // own (window expiry, cooldown transitions,
                                         // heartbeat ticks, persisted restores)

    // Window timer - fires at the "next transition moment": the window's
    // end in fixed mode, or the oldest live message's age-out in sliding
    // mode (where it is then re-armed after each decay).
    let windowTimeout              = null;
    let actualWindowDelayInUse     = 0;
    let actualWindowDelayRemaining = 0;

    // Reporting (status label only)
    let reportInterval        = null;
    let reportMiniTimeout     = null;
    let windowRemainingDisplay = 0;
    let reporting             = this.reporting;
    let reportingformat       = this.reportingformat;

    // Heartbeat - fixed schedule, independent of everything else
    let heartbeatTimer        = null;

    // Trigger status flash
    let statusFlashTimeout    = null;

    // Cooldown - a self-expiring, timed block on counting that begins
    // automatically after a genuine Trigger. Deliberately kept on its own
    // timer handles, fully independent of the window timers, so
    // clearWindowTimers() (used freely elsewhere) can never accidentally
    // interrupt an in-progress cooldown.
    let cooldownActive               = false;
    let cooldownRemainingDisplay     = 0;
    let cooldownTimeout              = null;
    let cooldownReportInterval       = null;
    let cooldownReportMiniTimeout    = null;
    let actualCooldownDelayInUse     = 0;
    let actualCooldownDelayRemaining = 0;

    // -------------------------------------------------------------------------
    // Persist restore
    // -------------------------------------------------------------------------

    if (this.persist === true) {
      try {
        if (fs.existsSync(thresholdFile)) {
          let savedState = JSON.retrocycle(JSON.parse(readState()));
          let nowMS      = (new Date()).getTime();

          this.reporting       = typeof savedState.reporting !== 'undefined' ? savedState.reporting.toString() : this.reporting;
          this.reportingformat = typeof savedState.reportingformat !== 'undefined' ? savedState.reportingformat.toString() : this.reportingformat;
          reporting            = this.reporting;
          reportingformat      = this.reportingformat;

          if (typeof savedState.disabled            !== 'undefined') disabled            = savedState.disabled;
          if (typeof savedState.ignoredCount        !== 'undefined') ignoredCount        = savedState.ignoredCount;
          if (typeof savedState.lastIgnoredTime     !== 'undefined' && savedState.lastIgnoredTime !== null) {
            lastIgnoredTime = new Date(savedState.lastIgnoredTime);
          }
          if (typeof savedState.effectiveCountLimit !== 'undefined') effectiveCountLimit = savedState.effectiveCountLimit;
          if (typeof savedState.effectiveWindowMS   !== 'undefined') effectiveWindowMS   = savedState.effectiveWindowMS;
          if (typeof savedState.origmsg             !== 'undefined') originalMsg         = savedState.origmsg;

          if (savedState.cooldownActive === true && typeof savedState.cooldownTarget !== 'undefined') {
            // -- Cooldown restore --------------------------------------------
            let remainingMS = (new Date(savedState.cooldownTarget.toString())).getTime() - nowMS;
            if (remainingMS <= 0) remainingMS = (Math.floor((Math.random() * 5) + 3) * 1000);
            cooldownActive           = true;
            cooldownRemainingDisplay = remainingMS;
            timerState               = TIMER_STATE.COOLDOWN;
            refreshStatus();
            startCooldownTimeout();
            startCooldownReporting();
            // Heartbeat restarts fresh after a restore - does not recalculate original schedule
            startHeartbeat();
          } else if (savedState.timerState === TIMER_STATE.COUNTING && Array.isArray(savedState.timestamps)) {
            // -- Counting restore --------------------------------------------
            // A restore never fires the Trigger output, even if the restored
            // count already meets the limit - Trigger only ever fires on a
            // live counted message. No events are emitted here at all: the
            // restore itself is not a live transition.
            timestamps = savedState.timestamps.slice();

            if (node.windowmode === WINDOW_MODE.SLIDING) {
              // Messages that aged out during downtime simply fall away.
              timestamps = timestamps.filter(function(t) { return (t + effectiveWindowMS) > nowMS; });
              if (timestamps.length === 0) {
                timerState = TIMER_STATE.IDLE;
                refreshStatus();
              } else {
                timerState = TIMER_STATE.COUNTING;
                armWindowTimeout();
                startWindowReporting();
                startHeartbeat();
              }
            } else {
              let remainingMS = (timestamps[0] + effectiveWindowMS) - nowMS;
              if (remainingMS <= 0) {
                // Window already expired during downtime - settle to idle
                // silently (no retroactive WINDOWEXPIRED).
                timestamps = [];
                timerState = TIMER_STATE.IDLE;
                refreshStatus();
              } else {
                if (remainingMS < 3000) {
                  // Same anti-flood rule as timer-events: randomize a
                  // nearly-expired window to 3-8 seconds by shifting the
                  // anchor so dependent nodes have time to initialize.
                  let randMS = (Math.floor((Math.random() * 5) + 3) * 1000);
                  timestamps[0] = nowMS + randMS - effectiveWindowMS;
                }
                timerState = TIMER_STATE.COUNTING;
                armWindowTimeout();
                startWindowReporting();
                startHeartbeat();
              }
            }
          } else {
            // -- Idle restore ------------------------------------------------
            // Nothing live to resume, but disabled / overrides / ignored
            // context carried over above still applies.
            timerState = TIMER_STATE.IDLE;
            refreshStatus();
          }
        } else {
          refreshStatus();
        }
      } catch (error) {
        this.error("Error processing persistent file data for timer-threshold node " + n.id.toString() + "\n\n" + error.toString());
      }
    } else {
      deleteState();
      refreshStatus();
    }

    // -------------------------------------------------------------------------
    // Event listeners
    // -------------------------------------------------------------------------

    this.on("input", function(msg) {
      handleInputEvent(msg);
    });

    this.on("close", function(removed, done) {
      clearWindowTimers();
      clearCooldownTimers();
      stopHeartbeat();
      if (statusFlashTimeout) clearTimeout(statusFlashTimeout);
      statusFlashTimeout = null;
      node.status({});
      if (removed) deleteState();
      done();
    });

    // -------------------------------------------------------------------------
    // Status helpers
    // -------------------------------------------------------------------------

    function buildStatus(state) {
      let baseText = "";
      let fill     = "green";
      let shape    = "dot";

      if (state === TIMER_STATE.COUNTING) {
        fill  = "yellow";
        shape = "dot";
        let label = node.windowmode === WINDOW_MODE.SLIDING ? "Oldest expires: " : "Window: ";
        baseText = "Counting: " + liveCount() + "/" + effectiveCountLimit + " | " +
                   label + displayTime(windowRemainingDisplay, reportingformat);
      } else if (state === TIMER_STATE.COOLDOWN) {
        // Deliberately short, no ignored-count detail - same brevity rule
        // as timer-events' cooldown status.
        fill     = "yellow";
        shape    = "dot";
        baseText = "Cooldown: " + displayTime(cooldownRemainingDisplay, reportingformat);
      } else {
        fill     = "green";
        shape    = "dot";
        baseText = "Ready";
      }

      if (disabled) {
        if (state === TIMER_STATE.COOLDOWN) {
          return { fill: "grey", shape: "ring", text: "Disabled" };
        }
        return { fill: "grey", shape: "ring", text: "Disabled | " + baseText };
      }

      return { fill: fill, shape: shape, text: baseText };
    }

    /**
     * Re-renders the status label from the current state. Used anywhere
     * the state may have changed without going through a reporting tick -
     * including the end of the Trigger status flash.
     */
    function refreshStatus() {
      if (timerState === TIMER_STATE.COUNTING) {
        windowRemainingDisplay = currentWindowRemaining();
      }
      node.status(buildStatus(timerState));
    }

    /**
     * Shows a brief "Triggered" flash, then restores the normal status for
     * whatever state the node has settled into (cooldown or idle). If
     * reporting is active, the per-second ticks will overwrite the flash
     * on their own - the flash timeout is the guarantee for the
     * reporting=none case.
     */
    function flashTriggered() {
      if (statusFlashTimeout) clearTimeout(statusFlashTimeout);
      node.status({ fill: "blue", shape: "dot", text: "Triggered" });
      statusFlashTimeout = setTimeout(function() {
        statusFlashTimeout = null;
        refreshStatus();
      }, TRIGGER_FLASH_MS);
    }

    // -------------------------------------------------------------------------
    // Utility helpers
    // -------------------------------------------------------------------------

    function convertToMilliseconds(value, units) {
      switch (units) {
        case UNITS.SECOND:      return value * 1000;
        case UNITS.MINUTE:      return value * 1000 * 60;
        case UNITS.HOUR:        return value * 1000 * 60 * 60;
        case UNITS.MILLISECOND: return value;
        default:                return value;
      }
    }

    function normalizeUnits(units) {
      return typeof units === 'string' ? units.toLowerCase().replace(/s$/, '') : null;
    }

    function msgValueToMs(value, units) {
      switch (units) {
        case UNITS_INPUT.SECOND: return value * 1000;
        case UNITS_INPUT.MINUTE: return value * 1000 * 60;
        case UNITS_INPUT.HOUR:   return value * 1000 * 60 * 60;
        default:                 return value;
      }
    }

    /**
     * Non-mutating live count. In fixed mode every stored timestamp is
     * live until the shared window expires. In sliding mode a timestamp is
     * live only while it is younger than the window - computing this
     * without mutating lets status/query/envelope reads happen at any
     * moment without racing the decay timer (which does the actual
     * pruning).
     */
    function liveCount() {
      if (timestamps.length === 0) return 0;
      if (node.windowmode === WINDOW_MODE.FIXED) return timestamps.length;
      let nowMS = (new Date()).getTime();
      let count = 0;
      for (let i = 0; i < timestamps.length; i++) {
        if ((timestamps[i] + effectiveWindowMS) > nowMS) count++;
      }
      return count;
    }

    /**
     * Mutating prune - sliding mode only. Drops every timestamp that has
     * aged out. Called on each arrival, decay tick, and setwindow
     * re-evaluation.
     */
    function pruneTimestamps() {
      if (node.windowmode !== WINDOW_MODE.SLIDING) return;
      let nowMS = (new Date()).getTime();
      timestamps = timestamps.filter(function(t) { return (t + effectiveWindowMS) > nowMS; });
    }

    /**
     * Returns the "window remaining" that should be reported right now:
     *   cooldown  - time left in the cooldown period
     *   counting  - fixed: ms until the shared window expires
     *               sliding: ms until the OLDEST live message ages out
     *               (the moment the displayed count will next change)
     *   idle      - 0
     */
    function currentWindowRemaining() {
      if (timerState === TIMER_STATE.COOLDOWN) return cooldownRemainingDisplay;
      if (timestamps.length === 0) return 0;
      let nowMS = (new Date()).getTime();
      if (node.windowmode === WINDOW_MODE.FIXED) {
        return Math.max(0, (timestamps[0] + effectiveWindowMS) - nowMS);
      }
      for (let i = 0; i < timestamps.length; i++) {
        let remaining = (timestamps[i] + effectiveWindowMS) - nowMS;
        if (remaining > 0) return remaining;
      }
      return 0;
    }

    // -------------------------------------------------------------------------
    // Event message construction + output dispatch
    // -------------------------------------------------------------------------

    /**
     * Builds the standard event message envelope by cloning a base message
     * (either the live triggering msg, or originalMsg when there is no
     * live trigger - e.g. window expiry, cooldown transitions, heartbeat)
     * and layering the standard state/metadata fields on top. Property
     * names deliberately match timer-events (timerEvent, timerState,
     * ignored, source) so downstream nodes can process both nodes' output
     * with the same code.
     */
    function buildEventMessage(timerEvent, baseMsg, ignored, source) {
      let evtMsg = RED.util.cloneMessage(baseMsg || {});
      evtMsg.timerEvent        = timerEvent;
      evtMsg.timerState        = timerState;
      evtMsg.count             = liveCount();
      evtMsg.countLimit        = effectiveCountLimit;
      evtMsg.windowMode        = node.windowmode;
      evtMsg.windowDuration    = effectiveWindowMS;
      evtMsg.windowRemaining   = currentWindowRemaining();
      evtMsg.cooldownRemaining = timerState === TIMER_STATE.COOLDOWN ? cooldownRemainingDisplay : 0;
      evtMsg.ignoredCount      = ignoredCount;
      evtMsg.lastIgnoredTime   = lastIgnoredTime ? lastIgnoredTime.toISOString() : null;
      evtMsg.disabled          = disabled;
      evtMsg.ignored           = ignored;
      evtMsg.source            = source;
      return evtMsg;
    }

    /**
     * Central output router for every event. Applies the fixed
     * output-exclusivity rules:
     *   - Output 1 (Trigger): TIMER_EVENT.TRIGGERED only, and only when
     *                         ignored is false. A genuine Trigger always
     *                         also fires on output 3.
     *   - Output 2 (Query):   TIMER_EVENT.QUERY only. Never fires on
     *                         output 3.
     *   - Output 3 (Events):  every event except QUERY, including ignored
     *                         copies of blocked attempts.
     *
     * extraProps allows event-specific fields (countLimitSet, windowSet)
     * to be layered onto the built message.
     */
    function dispatchEvent(timerEvent, baseMsg, ignored, source, extraProps) {
      let evtMsg = buildEventMessage(timerEvent, baseMsg, ignored, source);
      if (extraProps) {
        for (let key in extraProps) {
          if (Object.prototype.hasOwnProperty.call(extraProps, key)) evtMsg[key] = extraProps[key];
        }
      }

      if (timerEvent === TIMER_EVENT.QUERY) {
        node.send([null, evtMsg, null]);
        return;
      }

      let out1 = null;
      let out3 = evtMsg;

      if (!ignored && timerEvent === TIMER_EVENT.TRIGGERED) {
        out1 = RED.util.cloneMessage(evtMsg);
      }

      node.send([out1, null, out3]);
    }

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    /**
     * Starts the heartbeat interval if heartbeatinterval is configured
     * (> 0). Clears any existing heartbeat interval first to avoid
     * duplicates. Runs on a fixed wall-clock schedule, unaffected by
     * reset, setwindow, setcountlimit, or gating. Starts when a counting
     * cycle begins; keeps ticking through COUNTING and COOLDOWN; stopped
     * only when the node returns to IDLE. Each tick triggers a QUERY event
     * (output 2) with source "internal", carrying a full status snapshot.
     */
    function startHeartbeat() {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
      if (node.heartbeatinterval > 0) {
        let intervalMS = convertToMilliseconds(node.heartbeatinterval, node.heartbeatintervalunits);
        if (intervalMS > 0) {
          heartbeatTimer = setInterval(function() {
            dispatchEvent(TIMER_EVENT.QUERY, originalMsg, false, EVENT_SOURCE.INTERNAL);
          }, intervalMS);
        }
      }
    }

    /**
     * Stops the heartbeat interval. Called whenever the node returns to
     * IDLE (windowexpired, stop, reset, cooldownended).
     */
    function stopHeartbeat() {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
    }

    // -------------------------------------------------------------------------
    // Window timer management
    // -------------------------------------------------------------------------

    /**
     * Clears the window timeout and the reporting interval/miniTimeout.
     * Does NOT clear the heartbeat or the cooldown handles - those live
     * on their own independent schedules.
     */
    function clearWindowTimers() {
      clearTimeout(windowTimeout);
      clearInterval(reportInterval);
      clearTimeout(reportMiniTimeout);
      windowTimeout     = null;
      reportInterval    = null;
      reportMiniTimeout = null;
    }

    /**
     * Arms (or re-arms) the window timeout to fire at the next transition
     * moment: the window's end in fixed mode, or the oldest live message's
     * age-out in sliding mode. Uses the same maxTimeout chunking pattern as
     * timer-events to support windows longer than ~24.8 days.
     */
    function armWindowTimeout() {
      clearTimeout(windowTimeout);
      windowTimeout = null;

      actualWindowDelayRemaining = currentWindowRemaining();
      if (actualWindowDelayRemaining <= 0) actualWindowDelayRemaining = 1;

      if (actualWindowDelayRemaining > maxTimeout) {
        actualWindowDelayInUse     = maxTimeout;
        actualWindowDelayRemaining = actualWindowDelayRemaining - maxTimeout;
      } else {
        actualWindowDelayInUse     = actualWindowDelayRemaining;
        actualWindowDelayRemaining = 0;
      }
      windowTimeout = setTimeout(windowElapsed, actualWindowDelayInUse);
    }

    /**
     * Fires at the next transition moment.
     *   Fixed mode:   the shared window has closed without the count being
     *                 reached - the whole cycle expires at once.
     *   Sliding mode: the oldest live message has aged out. Prune; if
     *                 messages remain this is an intermediate decay (no
     *                 event - observable via status/query/heartbeat only)
     *                 and the timeout re-arms for the next-oldest. Only
     *                 decay to ZERO emits WINDOWEXPIRED, since that is the
     *                 counting -> idle transition.
     */
    function windowElapsed() {
      if (actualWindowDelayRemaining > 0) {
        if (actualWindowDelayRemaining > maxTimeout) {
          actualWindowDelayInUse      = maxTimeout;
          actualWindowDelayRemaining -= maxTimeout;
        } else {
          actualWindowDelayInUse     = actualWindowDelayRemaining;
          actualWindowDelayRemaining = 0;
        }
        windowTimeout = setTimeout(windowElapsed, actualWindowDelayInUse);
        return;
      }

      if (node.windowmode === WINDOW_MODE.SLIDING) {
        pruneTimestamps();
        if (timestamps.length > 0) {
          // Intermediate decay - count dropped but the cycle is still live.
          clearWindowTimers();
          armWindowTimeout();
          startWindowReporting();
          writeState();
          return;
        }
      }

      expireWindow();
    }

    /**
     * The counting -> idle transition without a Trigger: the fixed window
     * closed, or the sliding count decayed to zero.
     */
    function expireWindow() {
      clearWindowTimers();
      timestamps = [];
      timerState = TIMER_STATE.IDLE;
      stopHeartbeat();
      writeState();
      refreshStatus();
      // Expiry is never externally triggered - it is always the node's own
      // clock reaching the transition moment.
      dispatchEvent(TIMER_EVENT.WINDOWEXPIRED, originalMsg, false, EVENT_SOURCE.INTERNAL);
    }

    /**
     * Drives the node's status label only, using the same adaptive
     * every-minute-then-every-second cadence as timer-events. Produces no
     * output message. In sliding mode the countdown shown is the time
     * until the oldest live message ages out - when it does, windowElapsed
     * recomputes and restarts this reporting to stay in sync.
     */
    function startWindowReporting() {
      clearInterval(reportInterval);
      clearTimeout(reportMiniTimeout);
      reportInterval    = null;
      reportMiniTimeout = null;

      windowRemainingDisplay = currentWindowRemaining();
      node.status(buildStatus(TIMER_STATE.COUNTING));

      if (reporting === REPORTING.NONE) return;

      if ((windowRemainingDisplay > 60000) && (reporting === REPORTING.LAST_MINUTE_SECONDS)) {
        reportMiniTimeout = setTimeout(function() {
          if ((windowRemainingDisplay % 60000) !== 0) {
            windowRemainingDisplay -= (windowRemainingDisplay % 60000);
            node.status(buildStatus(TIMER_STATE.COUNTING));
          }

          if (windowRemainingDisplay <= 60000) {
            reportInterval = setInterval(function() {
              windowRemainingDisplay -= 1000;
              node.status(buildStatus(TIMER_STATE.COUNTING));
            }, 1000);
          } else {
            reportInterval = setInterval(function() {
              if (windowRemainingDisplay > 60000) {
                windowRemainingDisplay -= 60000;
                node.status(buildStatus(TIMER_STATE.COUNTING));
              }
              if (windowRemainingDisplay <= 60000) {
                clearInterval(reportInterval);
                reportInterval = null;
                reportInterval = setInterval(function() {
                  windowRemainingDisplay -= 1000;
                  node.status(buildStatus(TIMER_STATE.COUNTING));
                }, 1000);
              }
            }, 60000);
          }
          reportMiniTimeout = null;
        }, windowRemainingDisplay % 60000);

      } else {
        reportInterval = setInterval(function() {
          windowRemainingDisplay -= 1000;
          node.status(buildStatus(TIMER_STATE.COUNTING));
        }, 1000);
      }
    }

    // -------------------------------------------------------------------------
    // Cooldown management
    // -------------------------------------------------------------------------

    /**
     * Clears every cooldown-specific timer handle. Deliberately separate
     * from clearWindowTimers() - a cooldown in progress must never be
     * interrupted by the window's own timer handling.
     */
    function clearCooldownTimers() {
      clearTimeout(cooldownTimeout);
      clearInterval(cooldownReportInterval);
      clearTimeout(cooldownReportMiniTimeout);
      cooldownTimeout           = null;
      cooldownReportInterval    = null;
      cooldownReportMiniTimeout = null;
    }

    /**
     * Begins a cooldown period following a genuine Trigger. Only ever
     * called right after TIMER_EVENT.TRIGGERED has been dispatched. A
     * cooldownduration of 0 disables the feature entirely, which IS
     * simple-reset behavior. Heartbeat is left running uninterrupted (it
     * ticks regardless of counting/cooldown state).
     */
    function startCooldown(baseMsg) {
      let cooldownMS = convertToMilliseconds(node.cooldownduration, node.cooldownunits);
      if (cooldownMS <= 0) return; // cooldown disabled - caller handles the return to idle

      cooldownActive           = true;
      cooldownRemainingDisplay = cooldownMS;
      timerState               = TIMER_STATE.COOLDOWN;
      writeState();
      dispatchEvent(TIMER_EVENT.COOLDOWNSTARTED, baseMsg, false, EVENT_SOURCE.INTERNAL);
      startCooldownTimeout();
      startCooldownReporting();
    }

    function startCooldownTimeout() {
      actualCooldownDelayRemaining = cooldownRemainingDisplay;
      if (actualCooldownDelayRemaining > maxTimeout) {
        actualCooldownDelayInUse     = maxTimeout;
        actualCooldownDelayRemaining = actualCooldownDelayRemaining - maxTimeout;
      } else {
        actualCooldownDelayInUse     = actualCooldownDelayRemaining;
        actualCooldownDelayRemaining = 0;
      }
      cooldownTimeout = setTimeout(cooldownElapsed, actualCooldownDelayInUse);
    }

    /**
     * Drives the cooldown status label only, same adaptive cadence as
     * startWindowReporting(), kept as a separate implementation so it
     * never shares timer handles with the window reporting. Note: unlike
     * the window reporting, this does NOT set an immediate initial status -
     * the Trigger flash owns the label for the first TRIGGER_FLASH_MS, and
     * either the first reporting tick or the flash timeout takes over.
     */
    function startCooldownReporting() {
      if (reporting === REPORTING.NONE) return;

      if ((cooldownRemainingDisplay > 60000) && (reporting === REPORTING.LAST_MINUTE_SECONDS)) {
        cooldownReportMiniTimeout = setTimeout(function() {
          if ((cooldownRemainingDisplay % 60000) !== 0) {
            cooldownRemainingDisplay -= (cooldownRemainingDisplay % 60000);
            node.status(buildStatus(TIMER_STATE.COOLDOWN));
          }

          if (cooldownRemainingDisplay <= 60000) {
            cooldownReportInterval = setInterval(function() {
              cooldownRemainingDisplay -= 1000;
              node.status(buildStatus(TIMER_STATE.COOLDOWN));
            }, 1000);
          } else {
            cooldownReportInterval = setInterval(function() {
              if (cooldownRemainingDisplay > 60000) {
                cooldownRemainingDisplay -= 60000;
                node.status(buildStatus(TIMER_STATE.COOLDOWN));
              }
              if (cooldownRemainingDisplay <= 60000) {
                clearInterval(cooldownReportInterval);
                cooldownReportInterval = null;
                cooldownReportInterval = setInterval(function() {
                  cooldownRemainingDisplay -= 1000;
                  node.status(buildStatus(TIMER_STATE.COOLDOWN));
                }, 1000);
              }
            }, 60000);
          }
          cooldownReportMiniTimeout = null;
        }, cooldownRemainingDisplay % 60000);

      } else {
        cooldownReportInterval = setInterval(function() {
          cooldownRemainingDisplay -= 1000;
          node.status(buildStatus(TIMER_STATE.COOLDOWN));
        }, 1000);
      }
    }

    /**
     * Fires when the cooldown period completes naturally. Settles back to
     * IDLE and dispatches COOLDOWNENDED - the Trigger is NOT re-fired,
     * since it was already reported once when the count originally hit
     * the limit.
     */
    function cooldownElapsed() {
      if (actualCooldownDelayRemaining === 0) {
        clearCooldownTimers();
        cooldownActive           = false;
        cooldownRemainingDisplay = 0;
        timerState               = TIMER_STATE.IDLE;
        stopHeartbeat();
        writeState();
        refreshStatus();
        dispatchEvent(TIMER_EVENT.COOLDOWNENDED, originalMsg, false, EVENT_SOURCE.INTERNAL);
        return;
      } else if (actualCooldownDelayRemaining > maxTimeout) {
        actualCooldownDelayInUse      = maxTimeout;
        actualCooldownDelayRemaining -= maxTimeout;
      } else {
        actualCooldownDelayInUse     = actualCooldownDelayRemaining;
        actualCooldownDelayRemaining = 0;
      }
      cooldownTimeout = setTimeout(cooldownElapsed, actualCooldownDelayInUse);
    }

    // -------------------------------------------------------------------------
    // Trigger
    // -------------------------------------------------------------------------

    /**
     * The count has genuinely reached the limit. Fires exactly once per
     * cycle, immediately - the node never waits for the window to close.
     * The Trigger message is a clone of the final counted message (or, for
     * a setcountlimit-induced trigger, of originalMsg - the last counted
     * message), so its payload/topic carry through downstream.
     *
     * The remaining window time is deliberately wiped BEFORE dispatch so
     * the trigger message reports the settled post-trigger state (count 0,
     * window 0) rather than a half-torn-down snapshot.
     */
    function fireTrigger(baseMsg, source) {
      clearWindowTimers();
      timestamps = [];
      timerState = TIMER_STATE.IDLE;
      writeState();

      dispatchEvent(TIMER_EVENT.TRIGGERED, baseMsg, false, source);
      flashTriggered();

      startCooldown(baseMsg);
      if (!cooldownActive) {
        stopHeartbeat();
        writeState();
      }
    }

    // -------------------------------------------------------------------------
    // Input event handler
    // -------------------------------------------------------------------------

    function handleInputEvent(msg) {
      const msgPayload = typeof msg.payload === 'string' ? msg.payload.toLowerCase() : msg.payload;
      const msgSource  = EVENT_SOURCE.EXTERNAL;

      reporting       = node.reporting;
      reportingformat = node.reportingformat;

      // -- Query -----------------------------------------------------------
      if (msgPayload === PAYLOAD.QUERY) {
        refreshStatus();
        dispatchEvent(TIMER_EVENT.QUERY, msg, false, msgSource);
        return;
      }

      // -- Stop --------------------------------------------------------------
      // The universal escape hatch: aborts a counting cycle, or cancels an
      // in-progress cooldown - the only way to cut a cooldown short.
      if (msgPayload === PAYLOAD.STOP) {
        if (timerState === TIMER_STATE.COOLDOWN) {
          clearCooldownTimers();
          cooldownActive           = false;
          cooldownRemainingDisplay = 0;
          timerState               = TIMER_STATE.IDLE;
          stopHeartbeat();
          ignoredCount    = 0;
          lastIgnoredTime = null;
          writeState();
          refreshStatus();
          dispatchEvent(TIMER_EVENT.STOPPED, msg, false, msgSource);
        } else if (timerState === TIMER_STATE.COUNTING) {
          clearWindowTimers();
          timestamps      = [];
          timerState      = TIMER_STATE.IDLE;
          stopHeartbeat();
          ignoredCount    = 0;
          lastIgnoredTime = null;
          writeState();
          refreshStatus();
          dispatchEvent(TIMER_EVENT.STOPPED, msg, false, msgSource);
        } else {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.STOPPED, msg, true, msgSource);
        }
        return;
      }

      // -- Reset -------------------------------------------------------------
      // Wipes the count back to zero but stays armed - the next message
      // becomes count 1. Unlike stop, reset does NOT cancel a cooldown
      // (ignored:true during cooldown; use stop for that).
      if (msgPayload === PAYLOAD.RESET) {
        if (timerState === TIMER_STATE.COUNTING) {
          clearWindowTimers();
          timestamps      = [];
          timerState      = TIMER_STATE.IDLE;
          stopHeartbeat();
          ignoredCount    = 0;
          lastIgnoredTime = null;
          writeState();
          refreshStatus();
          dispatchEvent(TIMER_EVENT.RESET, msg, false, msgSource);
        } else {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.RESET, msg, true, msgSource);
        }
        return;
      }

      // -- Disable -----------------------------------------------------------
      // Blocks messages from being counted. An in-flight window is left to
      // expire/decay naturally - disable does not wipe it.
      if (msgPayload === PAYLOAD.DISABLE) {
        if (disabled) {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.DISABLED, msg, true, msgSource);
          return;
        }
        disabled = true;
        writeState();
        refreshStatus();
        dispatchEvent(TIMER_EVENT.DISABLED, msg, false, msgSource);
        return;
      }

      // -- Enable ------------------------------------------------------------
      // Re-allows counting. Has no effect on an active cooldown.
      if (msgPayload === PAYLOAD.ENABLE) {
        if (!disabled) {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.ENABLED, msg, true, msgSource);
          return;
        }
        disabled = false;
        writeState();
        refreshStatus();
        dispatchEvent(TIMER_EVENT.ENABLED, msg, false, msgSource);
        return;
      }

      // -- Set Count Limit -----------------------------------------------------
      if (msgPayload === PAYLOAD.SETCOUNTLIMIT) {
        let newLimit = Number(msg.setcountlimit);
        if (!Number.isInteger(newLimit) || newLimit < 1) {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.COUNTLIMITSET, msg, true, msgSource, { countLimitSet: msg.setcountlimit });
          return;
        }
        effectiveCountLimit = newLimit;
        writeState();
        refreshStatus();
        dispatchEvent(TIMER_EVENT.COUNTLIMITSET, msg, false, msgSource, { countLimitSet: newLimit });
        // Lowering the limit below (or to) the live count means the
        // requirement is already met - the Trigger fires immediately,
        // based on originalMsg (the last counted message).
        if (timerState === TIMER_STATE.COUNTING && liveCount() >= effectiveCountLimit) {
          fireTrigger(originalMsg, msgSource);
        }
        return;
      }

      // -- Set Window ----------------------------------------------------------
      if (msgPayload === PAYLOAD.SETWINDOW) {
        let winUnits = normalizeUnits(msg.setwindowunits);
        let winMS    = msgValueToMs(Number(msg.setwindow), winUnits);
        if (isNaN(winMS) || winMS <= 0) {
          refreshStatus();
          dispatchEvent(TIMER_EVENT.WINDOWSET, msg, true, msgSource, { windowSet: msg.setwindow });
          return;
        }
        effectiveWindowMS = winMS;
        writeState();
        dispatchEvent(TIMER_EVENT.WINDOWSET, msg, false, msgSource, { windowSet: winMS });

        // Re-evaluate a live cycle against the new window immediately.
        // Fixed: the window re-anchors from the original start - if that
        // now places the expiry in the past, the cycle expires right away.
        // Sliding: re-prune - if everything has aged out under the new
        // window, that is a decay to zero.
        if (timerState === TIMER_STATE.COUNTING) {
          if (node.windowmode === WINDOW_MODE.SLIDING) {
            pruneTimestamps();
            if (timestamps.length === 0) {
              expireWindow();
              return;
            }
          } else {
            if (currentWindowRemaining() <= 0) {
              expireWindow();
              return;
            }
          }
          armWindowTimeout();
          startWindowReporting();
        } else {
          refreshStatus();
        }
        return;
      }

      // -- Count candidate -----------------------------------------------------
      // Any message whose payload is not a recognized command. Two
      // independent gates block a candidate; each produces a COUNTED /
      // ignored:true event on output 3 (never output 1):
      //   1. Cooldown gate - candidates during cooldown are discarded
      //      (not counted, not queued).
      //   2. Disabled gate - candidates while disabled are discarded the
      //      same way, in any state.
      if (cooldownActive || disabled) {
        ignoredCount++;
        lastIgnoredTime = new Date();
        writeState();
        refreshStatus();
        dispatchEvent(TIMER_EVENT.COUNTED, msg, true, msgSource);
        return;
      }

      let nowMS = (new Date()).getTime();
      pruneTimestamps();

      let firstOfCycle = (timestamps.length === 0);
      timestamps.push(nowMS);
      originalMsg = msg;

      if (firstOfCycle) {
        ignoredCount    = 0;
        lastIgnoredTime = null;
        timerState      = TIMER_STATE.COUNTING;
        startHeartbeat();
      }

      if (timestamps.length >= effectiveCountLimit) {
        fireTrigger(msg, msgSource);
        return;
      }

      writeState();
      armWindowTimeout();
      startWindowReporting();
      dispatchEvent(TIMER_EVENT.COUNTED, msg, false, msgSource);
    }

    // -------------------------------------------------------------------------
    // Display time formatter
    // -------------------------------------------------------------------------

    function displayTime(delayToDisplay, reportingformat) {
      delayToDisplay = delayToDisplay / 1000;
      switch (reportingformat) {
        case REPORTING_FORMAT.SECONDS: return delayToDisplay;
        case REPORTING_FORMAT.MINUTES: return delayToDisplay / 60;
        case REPORTING_FORMAT.HOURS:   return delayToDisplay / 3600;
        default:
          let hours   = String(Math.floor(delayToDisplay / 3600)).padStart(2, "0");
          delayToDisplay %= 3600;
          let minutes = String(Math.floor(delayToDisplay / 60)).padStart(2, "0");
          let seconds = String(Math.floor(delayToDisplay % 60)).padStart(2, "0");
          return hours + ":" + minutes + ":" + seconds;
      }
    }

    // -------------------------------------------------------------------------
    // Persist helpers
    // -------------------------------------------------------------------------

    /**
     * Written on every meaningful state change. Unlike timer-events, the
     * file is NOT deleted when returning to idle - an idle node can still
     * carry state worth restoring (disabled, runtime overrides, ignored
     * context). The file is only deleted when the node is removed or
     * persistence is turned off.
     */
    function writeState() {
      if (node.persist !== true) return;
      try {
        if (!fs.existsSync(path.dirname(thresholdFile))) {
          fs.mkdirSync(path.dirname(thresholdFile), { recursive: true });
        }
        let cooldownTarget = cooldownActive
          ? (new Date((new Date()).getTime() + cooldownRemainingDisplay)).toISOString()
          : null;
        fs.writeFileSync(thresholdFile, JSON.stringify(JSON.decycle({
          reporting:           node.reporting,
          reportingformat:     node.reportingformat,
          timerState:          timerState,
          timestamps:          timestamps,
          cooldownActive:      cooldownActive,
          cooldownTarget:      cooldownTarget,
          origmsg:             originalMsg !== null ? originalMsg : {},
          disabled:            disabled,
          ignoredCount:        ignoredCount,
          lastIgnoredTime:     lastIgnoredTime ? lastIgnoredTime.toISOString() : null,
          effectiveCountLimit: effectiveCountLimit,
          effectiveWindowMS:   effectiveWindowMS
        })));
      } catch (error) {
        node.error("Error writing persistent file for timer-threshold node " + node.id.toString() + "\n\n" + error.toString());
      }
    }

    function readState() {
      try {
        let contents = fs.readFileSync(thresholdFile).toString();
        if (typeof contents !== 'undefined') return contents;
      } catch (error) {
        node.error("Error reading persistent file for timer-threshold node " + node.id.toString() + "\n\n" + error.toString());
      }
      return -1;
    }

    function deleteState() {
      try {
        if (fs.existsSync(thresholdFile)) fs.unlinkSync(thresholdFile);
      } catch (error) {
        node.error("Error deleting persistent file for timer-threshold node " + node.id.toString() + "\n\n" + error.toString());
      }
    }
  }

  RED.nodes.registerType("timer-threshold", TimerThreshold);
}
