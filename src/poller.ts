/**
 * The poll loop: read new contract events, notify, persist the cursor.
 *
 * ── Failure policy ───────────────────────────────────────────────────────────
 *
 * This process is meant to stay up for weeks. Nothing in one cycle may end it:
 *
 *  - A failed RPC call fails ONE contract's scan for ONE cycle. Its cursor is
 *    left untouched, so the next cycle picks up exactly where it stopped.
 *  - A scan cursor is committed after its returned page has been processed,
 *    even when delivery was partial. Unknown events, the per-cycle cap, and
 *    exhausted Telegram retries are deliberate drops. Holding the cursor back
 *    would turn a broken token or chat into an infinite replay, and recovery
 *    would flood the channel. Notifications are lossy by design; the chain
 *    remains the record.
 *  - A cursor file that cannot be read is treated as a cold start; one that
 *    cannot be written is logged, and the in-memory cursor keeps working until
 *    the next restart.
 */

import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";

import type { rpc } from "@stellar/stellar-sdk";

import type { BotConfig } from "./config.js";
import { formatEvent, safeErrorMessage } from "./notifications/format.js";
import { readContractEvents, type WatchTarget } from "./stellar/events.js";
import type { ContractSource, DecodedEvent } from "./stellar/decode.js";

export interface TargetState {
  source: ContractSource;
  contractId: string;
  cursor: string | null;
  /** Highest ledger an event was seen in, from this run or the cursor file. */
  lastEventLedger: number | null;
  lastError: string | null;
}

export type PollerPauseResult = "paused" | "already-paused" | "stopped";
export type PollerResumeResult = "resumed" | "already-running" | "stopped";

export interface PollerStatus {
  running: boolean;
  /** Operator pause only prevents new cycles; an in-flight cycle may finish. */
  paused: boolean;
  /**
   * Chain clock: unix ms close time of the newest chain event this poller has
   * observed. `null` before the first scan returns one.
   *
   * The chain is the source of truth, so this only ever advances from a
   * `ledgerClosedAt` the RPC actually reported. A quiet page, a failed scan, a
   * Telegram outage or an open circuit breaker leaves the last observed value
   * in place: the skew against the local clock then grows on its own, which is
   * exactly what an operator needs to see during a long outage. It is saved
   * with the cursors so a restart resumes the same clock instead of going back
   * to `unknown`.
   */
  chainClockAt: number | null;
  startedAt: number;
  cycles: number;
  lastPollAt: number | null;
  lastSuccessAt: number | null;
  latestLedger: number | null;
  oldestLedger: number | null;
  notificationsSent: number;
  notificationsFailed: number;
  eventsSkipped: number;
  consecutiveFailures: number;
  lastError: { at: number; message: string } | null;
  targets: TargetState[];
  /** RPC circuit breaker state */
  circuitBreaker: {
    open: boolean;
    openedAt: number | null;
    failureCount: number;
    lastFailureAt: number | null;
  };
}

interface CursorFile {
  version: 1;
  updatedAt: string;
  /**
   * Newest observed chain close time (unix ms). Optional and additive: files
   * written before this field existed load as `null`, and older builds ignore
   * it, so the on-disk format stays version 1 either way.
   */
  chainClockAt?: number | null;
  targets: Record<string, { cursor: string | null; lastEventLedger: number | null }>;
}

interface CursorTarget {
  cursor: string | null;
  lastEventLedger: number | null;
}

/**
 * Bounds for a chain close time. Stellar launched in 2015 and the year 2100 is
 * far past this network's horizon, so anything outside that window is a
 * malformed `ledgerClosedAt` rather than chain data. The bound matters because
 * the chain clock is monotonic and persisted: one bogus future value would
 * otherwise be reported for the rest of the process's life, and then survive a
 * restart through the cursor file.
 */
const CHAIN_CLOCK_MIN_MS = Date.UTC(2015, 0, 1);
const CHAIN_CLOCK_MAX_MS = Date.UTC(2100, 0, 1);

function isPlausibleChainClock(ms: number): boolean {
  return Number.isFinite(ms) && ms >= CHAIN_CLOCK_MIN_MS && ms <= CHAIN_CLOCK_MAX_MS;
}

/**
 * Shape check for a saved chain clock: a positive, safe, unix-ms timestamp
 * inside the plausible window above.
 *
 * Anything else (a hand-edited file, an older writer, a truncated write) is
 * dropped and the cursors are kept — a cosmetic field must never cost an
 * operator their resume position. Files written before this field existed
 * are `undefined` and load as `null`, i.e. "no chain clock observed yet".
 */
function parseChainClock(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) return null;
  return isPlausibleChainClock(value) ? value : null;
}

function parseCursorFile(raw: string): CursorFile {
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("cursor root must be an object");
  }

  const candidate = parsed as Partial<CursorFile>;
  if (candidate.version !== 1 || typeof candidate.targets !== "object" || candidate.targets === null) {
    throw new Error("unsupported cursor format; expected version 1");
  }

  const targets: Record<string, CursorTarget> = {};
  for (const [source, value] of Object.entries(candidate.targets)) {
    if (typeof value !== "object" || value === null) {
      throw new Error(`invalid cursor target ${source}`);
    }
    const target = value as Partial<CursorTarget>;
    if (
      target.cursor !== null &&
      (typeof target.cursor !== "string" || target.cursor.length === 0 || target.cursor.length > 256)
    ) {
      throw new Error(`invalid cursor value for ${source}`);
    }
    if (
      target.lastEventLedger !== null &&
      (typeof target.lastEventLedger !== "number" ||
        !Number.isSafeInteger(target.lastEventLedger) ||
        target.lastEventLedger < 0)
    ) {
      throw new Error(`invalid last event ledger for ${source}`);
    }
    targets[source] = {
      cursor: target.cursor ?? null,
      lastEventLedger: target.lastEventLedger ?? null,
    };
  }

  return {
    version: 1,
    updatedAt: String(candidate.updatedAt ?? ""),
    chainClockAt: parseChainClock(candidate.chainClockAt),
    targets,
  };
}

/** Tuning knobs for Telegram delivery; defaults suit production, tests shrink them. */
export interface SendOptions {
  sendSpacingMs?: number;
  maxSendRetries?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
}

export interface PollerDeps {
  config: BotConfig;
  server: rpc.Server;
  /** Sends one already-formatted MarkdownV2 message. May reject. */
  send: (text: string) => Promise<void>;
  sendOptions?: SendOptions;
  /** Circuit breaker configuration */
  circuitBreakerOptions?: CircuitBreakerOptions;
}

export interface CircuitBreakerOptions {
  /** Number of consecutive failures before opening the circuit */
  failureThreshold?: number;
  /** Milliseconds to wait before attempting to close the circuit */
  cooldownMs?: number;
}

/** Default number of consecutive RPC failures before opening the circuit. */
const DEFAULT_CIRCUIT_FAILURE_THRESHOLD = 5;

/** Default cooldown period in milliseconds before attempting to close the circuit. */
const DEFAULT_CIRCUIT_COOLDOWN_MS = 60_000;

/** Telegram tolerates ~20 messages/minute to one chat; stay under it. */
const DEFAULT_SEND_SPACING_MS = 1_500;

/** Maximum number of retry attempts for a single Telegram send. */
const DEFAULT_MAX_SEND_RETRIES = 3;

/** Initial backoff in milliseconds for Telegram send retries. */
const DEFAULT_INITIAL_BACKOFF_MS = 1_000;

/** Maximum backoff in milliseconds for Telegram send retries. */
const DEFAULT_MAX_BACKOFF_MS = 10_000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));


/**
 * Sends a message with bounded exponential backoff.
 *
 * If the Telegram API is temporarily unavailable (rate limit, network error,
 * or bad token), we retry a few times with increasing delays. This prevents
 * transient failures from dropping notifications while avoiding infinite
 * retries that would block the poller loop.
 */
async function sendWithRetry(
  send: (text: string) => Promise<void>,
  text: string,
  botToken: string,
  opts?: SendOptions,
): Promise<void> {
  let attempt = 0;
  const maxRetries = opts?.maxSendRetries ?? DEFAULT_MAX_SEND_RETRIES;
  let backoff = opts?.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS;
  const maxBackoff = opts?.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;

  while (true) {
    try {
      await send(text);
      return;
    } catch (err) {
      attempt++;
      if (attempt >= maxRetries) {
        throw err; // Exhausted retries
      }
      console.warn(
        `[poller] send attempt ${attempt} failed, retrying in ${backoff}ms: ` +
          safeErrorMessage(err, [botToken]),
      );
      await sleep(backoff);
      // Exponential backoff with cap
      backoff = Math.min(backoff * 2, maxBackoff);
    }
  }
}

export function createPoller(deps: PollerDeps) {
  const { config, server, send } = deps;
  const sendSpacing = deps.sendOptions?.sendSpacingMs ?? DEFAULT_SEND_SPACING_MS;
  const circuitThreshold = deps.circuitBreakerOptions?.failureThreshold ?? DEFAULT_CIRCUIT_FAILURE_THRESHOLD;
  const circuitCooldown = deps.circuitBreakerOptions?.cooldownMs ?? DEFAULT_CIRCUIT_COOLDOWN_MS;
  const errorMessage = (err: unknown): string => safeErrorMessage(err, [config.botToken]);
  const boundedLabel = (value: unknown, max = 120): string => {
    const compact = String(value).replace(/\s+/g, " ").trim() || "unknown";
    return compact.length <= max ? compact : `${compact.slice(0, max - 1)}…`;
  };
  const cursorPreview = (cursor: string | null): string => {
    if (cursor === null) return "none";
    return boundedLabel(cursor, 24);
  };

  const targets: WatchTarget[] = [
    { source: "market", contractId: config.marketContractId },
    { source: "squad", contractId: config.squadContractId },
  ];

  const state = new Map<ContractSource, TargetState>(
    targets.map((t) => [
      t.source,
      { source: t.source, contractId: t.contractId, cursor: null, lastEventLedger: null, lastError: null },
    ]),
  );

  const status: PollerStatus = {
    running: false,
    paused: false,
    chainClockAt: null,
    startedAt: 0,
    cycles: 0,
    lastPollAt: null,
    lastSuccessAt: null,
    latestLedger: null,
    oldestLedger: null,
    notificationsSent: 0,
    notificationsFailed: 0,
    eventsSkipped: 0,
    consecutiveFailures: 0,
    lastError: null,
    targets: [],
    circuitBreaker: {
      open: false,
      openedAt: null,
      failureCount: 0,
      lastFailureAt: null,
    },
  };

  let timer: NodeJS.Timeout | null = null;
  let stopped = false;
  let paused = false;
  let inFlight = false;
  let resumePending = false;

  // ── Cursor persistence ─────────────────────────────────────────────────────

  async function loadCursors(): Promise<void> {
    let raw: string;
    try {
      raw = await readFile(config.cursorFile, "utf8");
    } catch {
      console.log(
        `[poller] no cursor file at ${config.cursorFile}; cold start ` +
          `${config.startLookbackLedgers} ledgers behind the tip`,
      );
      return;
    }

    try {
      const parsed = parseCursorFile(raw);
      for (const [source, saved] of Object.entries(parsed.targets ?? {})) {
        const target = state.get(source as ContractSource);
        if (!target) continue;
        target.cursor = saved.cursor ?? null;
        target.lastEventLedger = saved.lastEventLedger ?? null;
      }
      // Resume the chain clock alongside the cursors. Without this a restart
      // between two quiet scans would report `unknown` until the next event
      // happened to land, hiding a perfectly healthy (or long-stalled) chain.
      status.chainClockAt = parsed.chainClockAt ?? null;
      console.log(
        `[poller] resumed from ${config.cursorFile}: ` +
          [...state.values()]
            .map((t) => `${t.source}@${cursorPreview(t.cursor)}`)
            .join(" "),
      );
    } catch (err) {
      // A corrupt state file must not wedge the bot; a cold start is recoverable.
      console.warn(`[poller] cursor file unreadable, starting cold: ${errorMessage(err)}`);
    }
  }

  async function saveCursors(): Promise<void> {
    const payload: CursorFile = {
      version: 1,
      updatedAt: new Date().toISOString(),
      chainClockAt: status.chainClockAt,
      targets: Object.fromEntries(
        [...state.values()].map((t) => [
          t.source,
          { cursor: t.cursor, lastEventLedger: t.lastEventLedger },
        ]),
      ),
    };

    try {
      await mkdir(path.dirname(config.cursorFile), { recursive: true });
      // Write-then-rename: a crash mid-write must not leave a truncated file
      // that sends the next start back to the beginning of the retained window.
      const tmp = `${config.cursorFile}.tmp`;
      await writeFile(tmp, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      await rename(tmp, config.cursorFile);
    } catch (err) {
      console.error(`[poller] could not persist cursor: ${errorMessage(err)}`);
    }
  }

  // ── One cycle ──────────────────────────────────────────────────────────────

  interface NotificationResult {
    sent: number;
    failed: number;
    skipped: number;
  }

  async function notify(events: DecodedEvent[]): Promise<NotificationResult> {
    let sentThisCycle = 0;
    let failed = 0;
    let skipped = 0;

    for (const event of events) {
      if (event.payload.name === "unknown") {
        status.eventsSkipped += 1;
        skipped += 1;
        console.log(
          `[poller] skipped ${event.source} event "${boundedLabel(event.payload.eventName, 80)}" ` +
            `at ledger ${event.ledger}` +
            (event.payload.reason
              ? ` (${boundedLabel(event.payload.reason, 160)})`
              : ""),
        );
        continue;
      }

      const text = formatEvent(config, event);
      if (text === null) {
        status.eventsSkipped += 1;
        skipped += 1;
        continue;
      }

      if (sentThisCycle >= config.maxNotificationsPerCycle) {
        status.eventsSkipped += 1;
        skipped += 1;
        console.warn(
          `[poller] cycle notification cap (${config.maxNotificationsPerCycle}) reached; ` +
            `dropping ${event.payload.name} at ledger ${event.ledger}`,
        );
        continue;
      }

      try {
        // Use bounded retry for Telegram sends to handle transient failures
        await sendWithRetry(send, text, config.botToken, deps.sendOptions);
        status.notificationsSent += 1;
        sentThisCycle += 1;
      } catch (err) {
        // All retries exhausted; drop the message but continue processing others.
        status.notificationsFailed += 1;
        failed += 1;
        console.error(
          `[poller] send failed for ${event.payload.name} at ledger ${event.ledger} after retries: ` +
            errorMessage(err),
        );
      }

      if (sentThisCycle < config.maxNotificationsPerCycle && sendSpacing > 0) {
        await sleep(sendSpacing);
      }
    }

    return { sent: sentThisCycle, failed, skipped };
  }

  async function cycle(): Promise<void> {
    if (inFlight) return;
    inFlight = true;
    status.cycles += 1;
    status.lastPollAt = Date.now();

    // ── Circuit breaker check ─────────────────────────────────────────────────────
    if (status.circuitBreaker.open) {
      const now = Date.now();
      const timeSinceOpen = status.circuitBreaker.openedAt ? now - status.circuitBreaker.openedAt : Infinity;
      
      if (timeSinceOpen >= circuitCooldown) {
        // Cooldown elapsed, attempt to close the circuit
        console.log(
          `[poller] circuit breaker cooldown elapsed (${timeSinceOpen}ms >= ${circuitCooldown}ms), attempting recovery`,
        );
        status.circuitBreaker.open = false;
        status.circuitBreaker.openedAt = null;
        status.circuitBreaker.failureCount = 0;
      } else {
        // Still in cooldown, skip RPC calls
        console.log(
          `[poller] circuit breaker open, skipping RPC calls (${Math.round(timeSinceOpen / 1000)}s/${Math.round(circuitCooldown / 1000)}s elapsed)`,
        );
        status.targets = [...state.values()].map((t) => ({ ...t }));
        inFlight = false;
        return;
      }
    }

    let anyOk = false;
    let cycleFailures = 0;

    for (const target of targets) {
      const current = state.get(target.source);
      if (!current) continue;

      try {
        const scan = await readContractEvents(server, target, {
          cursor: current.cursor ?? undefined,
          lookbackLedgers: current.cursor ? undefined : config.startLookbackLedgers,
        });

        status.latestLedger = scan.latestLedger;
        status.oldestLedger = scan.oldestLedger;
        current.lastError = null;
        anyOk = true;

        // Advance the chain clock from close times the RPC actually reported.
        // `at` is 0 when the RPC omitted `ledgerClosedAt`, which is not an
        // error, and a value outside the plausible window is malformed and must
        // never be adopted. The update is monotonic: the two contracts are
        // scanned in turn, so a rescan or reordering must never move the clock
        // backwards. A quiet or failed scan keeps the last observed value, which
        // is what makes a long outage show up as a growing skew rather than as
        // a clock that keeps time on its own.
        let implausibleCloseTime = false;
        for (const event of scan.events) {
          if (event.at === 0) continue;
          const closedAtMs = event.at * 1_000;
          if (!isPlausibleChainClock(closedAtMs)) {
            implausibleCloseTime = true;
            continue;
          }
          if (status.chainClockAt === null || closedAtMs > status.chainClockAt) {
            status.chainClockAt = closedAtMs;
          }
        }
        if (implausibleCloseTime) {
          // One bounded line per target per cycle: no payload, no remote text.
          console.warn(
            `[poller] ${target.source}: ignored an implausible chain close time; chain clock unchanged`,
          );
        }

        // Reset circuit breaker on success
        if (status.circuitBreaker.failureCount > 0) {
          console.log(
            `[poller] RPC succeeded, resetting circuit breaker (was at ${status.circuitBreaker.failureCount} failures)`,
          );
          status.circuitBreaker.failureCount = 0;
          status.circuitBreaker.lastFailureAt = null;
        }

        let delivery: NotificationResult = { sent: 0, failed: 0, skipped: 0 };
        if (scan.events.length > 0) {
          console.log(
            `[poller] ${target.source}: ${scan.events.length} event(s) ` +
              `up to ledger ${scan.lastEventLedger} in ${scan.pages} page(s)`,
          );
          delivery = await notify(scan.events);
        }

        if (scan.lastEventLedger !== null) current.lastEventLedger = scan.lastEventLedger;
        // The opaque cursor covers the whole returned page, so it cannot be
        // committed per event. Commit after processing the page, including
        // deliberate drops, to avoid replaying a permanent Telegram failure.
        if (scan.cursor) {
          current.cursor = scan.cursor;
          if (delivery.failed > 0 || delivery.skipped > 0) {
            console.warn(
              `[poller] ${target.source}: committed cursor after partial delivery ` +
                `(sent=${delivery.sent}, failed=${delivery.failed}, skipped=${delivery.skipped})`,
            );
          }
        }
      } catch (err) {
        cycleFailures++;
        const message = errorMessage(err);
        current.lastError = message;
        status.lastError = { at: Date.now(), message: `${target.source}: ${message}` };
        console.error(`[poller] ${target.source} scan failed: ${message}`);
      }
    }

    // ── Circuit breaker state update ───────────────────────────────────────────────
    if (cycleFailures > 0) {
      status.circuitBreaker.failureCount += cycleFailures;
      status.circuitBreaker.lastFailureAt = Date.now();
      
      if (status.circuitBreaker.failureCount >= circuitThreshold && !status.circuitBreaker.open) {
        status.circuitBreaker.open = true;
        status.circuitBreaker.openedAt = Date.now();
        console.error(
          `[poller] circuit breaker opened after ${status.circuitBreaker.failureCount} failures (threshold: ${circuitThreshold})`,
        );
      }
    }

    if (anyOk) {
      status.lastSuccessAt = Date.now();
      status.consecutiveFailures = 0;
    } else {
      status.consecutiveFailures += 1;
    }

    status.targets = [...state.values()].map((t) => ({ ...t }));
    await saveCursors();
    inFlight = false;
  }

  function schedule(delayMs: number): void {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      void loop();
    }, delayMs);
  }

  async function loop(): Promise<void> {
    if (stopped || paused || inFlight) return;
    try {
      await cycle();
    } catch (err) {
      // Belt and braces: `cycle` already swallows per-target failures, so this
      // only fires on a bug. Either way the loop survives it.
      status.consecutiveFailures += 1;
      status.lastError = { at: Date.now(), message: errorMessage(err) };
      console.error(`[poller] cycle threw: ${errorMessage(err)}`);
      inFlight = false;
    }
    if (stopped || paused) return;
    if (resumePending) {
      resumePending = false;
      schedule(0);
    } else {
      schedule(config.pollIntervalMs);
    }
  }

  return {
    async start(): Promise<void> {
      await loadCursors();
      stopped = false;
      paused = false;
      status.paused = false;
      status.running = true;
      status.startedAt = Date.now();
      status.targets = [...state.values()].map((t) => ({ ...t }));
      console.log(
        `[poller] watching market=${config.marketContractId} squad=${config.squadContractId} ` +
          `every ${config.pollIntervalMs}ms`,
      );
      void loop();
    },

    pause(): PollerPauseResult {
      if (stopped) return "stopped";
      if (paused) return "already-paused";
      paused = true;
      status.paused = true;
      resumePending = false;
      if (timer) clearTimeout(timer);
      timer = null;
      console.log("[poller] paused by operator; an in-flight cycle may finish");
      return "paused";
    },

    resume(): PollerResumeResult {
      if (stopped) return "stopped";
      if (!paused) return "already-running";
      paused = false;
      status.paused = false;
      console.log("[poller] resumed by operator; next cycle starts now");
      if (inFlight) {
        resumePending = true;
      } else {
        schedule(0);
      }
      return "resumed";
    },

    stop(): void {
      stopped = true;
      paused = false;
      status.paused = false;
      status.running = false;
      resumePending = false;
      if (timer) clearTimeout(timer);
      timer = null;
    },

    status(): PollerStatus {
      return { ...status, targets: [...state.values()].map((t) => ({ ...t })) };
    },
  };
}

export type Poller = ReturnType<typeof createPoller>;