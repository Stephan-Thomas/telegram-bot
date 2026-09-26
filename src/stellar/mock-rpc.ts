/**
 * A local mock Soroban RPC for the Mimir notifier's `mock` profile.
 *
 * ── Why this exists ──────────────────────────────────────────────────────────
 *
 * The bot's hard parts — cursor pagination, empty pages, retention floors,
 * stale cursors, long-running RPC and Telegram failures — are painful to
 * rehearse against live Testnet, and impossible to rehearse in CI. This mock
 * serves the exact JSON-RPC subset the bot uses (`getHealth`, `getEvents`,
 * `getNetwork`) on loopback so the scanner (`npm run scan -- --mock`), the
 * dry-run runner (`npm run mock:poll`), and the test suite can drive the REAL
 * client → poller → health path deterministically, with no network, no
 * credentials, and no signing keys anywhere in the process.
 *
 * ── What it emulates faithfully ──────────────────────────────────────────────
 *
 *  - Opaque `<TOID>-<index>` cursors (ledger in the high 32 bits), so
 *    `eventCursorLedger` in `events.ts` reads them correctly.
 *  - A bounded ledger window per request plus `limit`, so one page covers a
 *    slice of history: **empty pages advance the cursor** and the walk only
 *    terminates when the cursor reaches the tip or stops moving.
 *  - Request validation the real RPC enforces: `cursor` and `startLedger` are
 *    mutually exclusive; `startLedger` below the retained floor is an ERROR,
 *    not an empty result; a cursor outside the retained window is stale.
 *  - Retention: events before `oldestLedger` simply do not exist.
 *
 * ── Bounds and safety ────────────────────────────────────────────────────────
 *
 *  - Binds loopback only (default `127.0.0.1:8420`); never a public surface.
 *  - Logs lifecycle, injected failures and counters — never raw event
 *    payloads, XDR, or request bodies, and never more than a bounded line.
 *  - Request bodies are size-capped; unknown methods get a JSON-RPC
 *    "method not found" instead of a crash.
 *  - Read-only by construction: it answers reads, has no keys, and cannot
 *    submit transactions. It is dev tooling; it must never face production.
 */

import { createHash } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { pathToFileURL } from "node:url";

import { nativeToScVal, xdr } from "@stellar/stellar-sdk";

import {
  MOCK_MARKET_CONTRACT_ID,
  MOCK_NETWORK_PASSPHRASE,
  MOCK_RPC_DEFAULT_PORT,
  MOCK_SQUAD_CONTRACT_ID,
} from "./mock-constants.js";

/** Loopback only. The mock is dev tooling and must never be exposed. */
const DEFAULT_HOST = "127.0.0.1";

/** Events per page when the client omits `limit`. The real RPC also defaults. */
const DEFAULT_PAGE_LIMIT = 100;

/** Hard ceiling on `limit`, mirroring the real RPC's cap. */
const MAX_PAGE_LIMIT = 1_000;

/** Refuse oversized request bodies instead of buffering unbounded input. */
const MAX_BODY_BYTES = 64 * 1024;

/** Rough close-time per ledger, for plausible `ledgerClosedAt` values. */
const SECONDS_PER_LEDGER = 5;
const BASE_CLOSE_TIME_SECONDS = 1_790_000_000;

/** Events the scenario may use; anything else is a programming error. */
export type MockValue =
  | string
  | number
  | bigint
  | { address: string }
  | { bytes: string }
  | { symbol: string };

export interface MockEventInput {
  /** Which mock contract emitted it. Default: `market`. */
  source?: "market" | "squad";
  /** Explicit contract id; overrides `source` (used by tests for odd ids). */
  contractId?: string;
  ledger: number;
  /** Topic 0, encoded as a symbol — the decoder's event name. */
  eventName: string;
  /** Topics after the name, in declaration order. Plain strings are strings. */
  topics?: MockValue[];
  /** Non-topic fields, encoded as the event's value map. */
  fields?: Record<string, MockValue>;
}

export interface MockScenario {
  /** Chain tip: windows clamp here and cursors may not exceed it. */
  latestLedger: number;
  /** Retained floor: `startLedger`/cursors below this are errors, not gaps. */
  oldestLedger: number;
  /** Ledgers one `getEvents` request may walk (the bounded slice). */
  ledgersPerPage: number;
  events: MockEventInput[];
}

export type MockFailureKind = "error" | "http-500" | "rate-limit" | "stale-cursor";

export interface MockFailure {
  kind: MockFailureKind;
  /** Apply to the next N matching requests, then auto-clear. Omitted: until cleared. */
  times?: number;
}

export type MockMethod = "getHealth" | "getEvents";

export interface MockRpcOptions {
  /** `0` (default in tests) picks an ephemeral port. */
  port?: number;
  host?: string;
  scenario?: MockScenario;
  failures?: Partial<Record<MockMethod, MockFailure>>;
}

export interface MockRpcStats {
  requests: number;
  byMethod: Record<string, number>;
}

export interface MockRpc {
  readonly url: string;
  readonly port: number;
  stats(): MockRpcStats;
  /** Inject a failure for the next N (or forever) matching requests. `null` clears. */
  setFailure(method: MockMethod, failure: MockFailure | null): void;
  /** Replace the scenario entirely (ledger window + events). */
  setScenario(scenario: MockScenario): void;
  /** Append one event (assigns its per-ledger sequence) without touching the window. */
  addEvent(event: MockEventInput): void;
  close(): Promise<void>;
}

// ── Encoding ────────────────────────────────────────────────────────────────

function encodeValue(value: MockValue): xdr.ScVal {
  if (typeof value === "string") return nativeToScVal(value, { type: "string" });
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new Error(`mock event numbers must be safe integers; got ${value}`);
    }
    return nativeToScVal(BigInt(value), { type: "u64" });
  }
  if (typeof value === "bigint") return nativeToScVal(value, { type: "i128" });
  if ("address" in value) return nativeToScVal(value.address, { type: "address" });
  if ("bytes" in value) return nativeToScVal(Buffer.from(value.bytes, "hex"), { type: "bytes" });
  if ("symbol" in value) return nativeToScVal(value.symbol, { type: "symbol" });
  throw new Error("mock event value must be string|number|bigint|{address}|{bytes}|{symbol}");
}

function encodeValueMap(fields: Record<string, MockValue>): xdr.ScVal {
  const entries = Object.entries(fields).map(
    ([key, value]) =>
      new xdr.ScMapEntry({
        key: nativeToScVal(key, { type: "symbol" }),
        val: encodeValue(value),
      }),
  );
  return xdr.ScVal.scvMap(entries);
}

// ── Cursors ─────────────────────────────────────────────────────────────────

/** `<TOID>-<index>` with the ledger in the TOID high 32 bits (see events.ts). */
function positionCursor(ledger: number, seq: number): string {
  const toid = (BigInt(ledger) << 32n) | BigInt(seq);
  return `${toid.toString()}-${seq}`;
}

function parseCursor(cursor: string): { ledger: number; seq: number } | null {
  const match = /^(\d{1,20})-(\d{1,10})$/.exec(cursor);
  if (!match || !match[1] || !match[2]) return null;
  const toid = BigInt(match[1]);
  const ledger = Number(toid >> 32n);
  const seq = Number(match[2]);
  if (!Number.isSafeInteger(ledger)) return null;
  return { ledger, seq };
}

// ── Scenario ────────────────────────────────────────────────────────────────

interface NormalizedEvent {
  contractId: string;
  ledger: number;
  seq: number;
  txHash: string;
  closedAt: string;
  /** `type: "contract"` — the only kind the bot reads. */
  type: "contract";
  topicB64: string[];
  valueB64: string;
}

function contractFor(input: MockEventInput): string {
  if (input.contractId !== undefined) return input.contractId;
  return input.source === "squad" ? MOCK_SQUAD_CONTRACT_ID : MOCK_MARKET_CONTRACT_ID;
}

function normalizeEvents(inputs: MockEventInput[]): NormalizedEvent[] {
  const sorted = [...inputs].sort((a, b) => a.ledger - b.ledger);
  const perLedger = new Map<number, number>();

  return sorted.map((input) => {
    if (!Number.isSafeInteger(input.ledger) || input.ledger < 1) {
      throw new Error(`mock event ledger must be a positive integer; got ${input.ledger}`);
    }
    const seq = perLedger.get(input.ledger) ?? 0;
    perLedger.set(input.ledger, seq + 1);

    const contractId = contractFor(input);
    const topics = [
      nativeToScVal(input.eventName, { type: "symbol" }),
      ...(input.topics ?? []).map(encodeValue),
    ];

    return {
      contractId,
      ledger: input.ledger,
      seq,
      type: "contract",
      txHash: createHash("sha256")
        .update(`${contractId}:${input.ledger}:${seq}:${input.eventName}`)
        .digest("hex"),
      closedAt: new Date((BASE_CLOSE_TIME_SECONDS + input.ledger * SECONDS_PER_LEDGER) * 1000)
        .toISOString(),
      topicB64: topics.map((topic) => topic.toXDR("base64")),
      valueB64: encodeValueMap(input.fields ?? {}).toXDR("base64"),
    };
  });
}

/**
 * The default scenario: a compact, coherent slice of Mimir history near the
 * tip — claims created → challenged → resolved → settled → paid on the market
 * contract, a squad market created → deposited → resolved → claimed, plus one
 * admin-shaped event (`oracle_changed`, no decoder) so the skip path is
 * exercised on every cold start. The first window after a default cold-start
 * lookback is intentionally empty, reproducing the empty-page trap.
 */
export function defaultMockScenario(): MockScenario {
  const latest = 1000;
  return {
    latestLedger: latest,
    oldestLedger: 900,
    ledgersPerPage: 50,
    events: [
      {
        source: "market",
        ledger: 990,
        eventName: "claim_created",
        topics: [7, { address: "GCLJONJVCLJGE6CSMCHCGYA563ADTCK5YGF3KERDMSNS3MNNDAIXHFA5" }],
        fields: { category: "crypto" },
      },
      {
        source: "market",
        ledger: 992,
        eventName: "claim_challenged",
        topics: [7, { address: "GC22MRUQSG6TWXMKANC7MDKBDOVZXB27774NYOINQKOCFUWIUBRTVNTV" }],
        fields: { stake: 20_000_000n },
      },
      {
        source: "market",
        ledger: 993,
        eventName: "oracle_changed",
        topics: [{ address: "GCUNJUGICQAHQR3QQMVQSKU2GWC4LXHEHQGRXU3GWWVN42R2OH3ALMPP" }],
        fields: {},
      },
      {
        source: "squad",
        ledger: 994,
        eventName: "market_created",
        topics: [5, { address: "GCLJONJVCLJGE6CSMCHCGYA563ADTCK5YGF3KERDMSNS3MNNDAIXHFA5" }],
        fields: {
          deadline: 1_893_456_000,
          fee_bps: 100,
          question: "Will Mimir settle its first claim on-chain?",
        },
      },
      {
        source: "squad",
        ledger: 995,
        eventName: "deposited",
        topics: [5, 1, { address: "GCLJONJVCLJGE6CSMCHCGYA563ADTCK5YGF3KERDMSNS3MNNDAIXHFA5" }],
        fields: { amount: 50_000_000n, shares: 50_000_000n },
      },
      {
        source: "market",
        ledger: 996,
        eventName: "claim_resolved",
        topics: [7],
        fields: {
          winner_side: 2,
          summary: "On-chain smoke: challengers awarded so the payout pull can be exercised",
          confidence: 100,
          evidence_hash: { bytes: "deadbeef".repeat(8) },
        },
      },
      {
        source: "market",
        ledger: 997,
        eventName: "market_settled",
        topics: [7],
        fields: {
          total_paid: 18_000_000n,
          total_fees: 1_000_000n,
          owed_to_challengers: 17_000_000n,
          dust: 0n,
        },
      },
      {
        source: "market",
        ledger: 998,
        eventName: "challenger_paid",
        topics: [7, { address: "GC22MRUQSG6TWXMKANC7MDKBDOVZXB27774NYOINQKOCFUWIUBRTVNTV" }],
        fields: {
          stake: 20_000_000n,
          gross: 17_000_000n,
          fee: 850_000n,
          net: 16_150_000n,
        },
      },
      {
        source: "squad",
        ledger: 999,
        eventName: "resolved",
        topics: [5],
        fields: { result: 1, pool_a: 60_000_000n, pool_b: 40_000_000n },
      },
      {
        source: "squad",
        ledger: 1000,
        eventName: "claimed",
        topics: [5, { address: "GCLJONJVCLJGE6CSMCHCGYA563ADTCK5YGF3KERDMSNS3MNNDAIXHFA5" }],
        fields: { gross: 55_000_000n, fee: 2_750_000n, net: 52_250_000n },
      },
    ],
  };
}

/** A malformed event the decoder must survive: address topic is a plain string. */
export function malformedMockEvent(ledger: number): MockEventInput {
  return {
    source: "market",
    ledger,
    eventName: "claim_challenged",
    topics: [7, "NOT-A-STELLAR-ADDRESS"],
    fields: { stake: 1n },
  };
}

// ── JSON-RPC plumbing ───────────────────────────────────────────────────────

interface RpcSuccess {
  result: unknown;
}
interface RpcFailure {
  error: { code: number; message: string };
  /** HTTP status; 200 = in-band JSON-RPC error, 500/429 = transport-shaped. */
  http: number;
}
type RpcOutcome = RpcSuccess | RpcFailure;

function isFailure(outcome: RpcOutcome): outcome is RpcFailure {
  return "error" in outcome;
}

function rpcError(code: number, message: string, http = 200): RpcFailure {
  return { error: { code, message }, http };
}

/** Longest failure/validation message the mock will ever emit. */
const MOCK_ERROR_BUDGET = 200;

function bounded(message: string): string {
  return message.length <= MOCK_ERROR_BUDGET
    ? message
    : `${message.slice(0, MOCK_ERROR_BUDGET - 1)}…`;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

// ── Server ──────────────────────────────────────────────────────────────────

export async function startMockRpc(options: MockRpcOptions = {}): Promise<MockRpc> {
  const host = options.host ?? DEFAULT_HOST;
  const scenarioState = { current: options.scenario ?? defaultMockScenario() };
  let normalized = normalizeEvents(scenarioState.current.events);
  const failures: Partial<Record<MockMethod, MockFailure>> = { ...options.failures };

  const stats: MockRpcStats = { requests: 0, byMethod: {} };

  const recompute = (): void => {
    normalized = normalizeEvents(scenarioState.current.events);
  };

  /** Consumes one "times" budget; returns true while the failure still applies. */
  const failureApplies = (method: MockMethod): MockFailure | null => {
    const failure = failures[method];
    if (!failure) return null;
    if (failure.times !== undefined) {
      if (failure.times <= 0) return null;
      failure.times -= 1;
      if (failure.times === 0) {
        delete failures[method];
        console.log(`[mock-rpc] ${method} injected failure cleared (budget spent)`);
      }
    }
    return failure;
  };

  const transportFailure = (method: string, failure: MockFailure): RpcFailure => {
    const note = `injected mock ${method} failure: ${failure.kind}`;
    console.log(`[mock-rpc] responding ${failure.kind} to ${method} (bounded drill)`);
    switch (failure.kind) {
      case "http-500":
        return rpcError(-32603, bounded(note), 500);
      case "rate-limit":
        return rpcError(-32005, bounded(note), 429);
      default:
        return rpcError(-32603, bounded(note), 200);
    }
  };

  const handleGetHealth = (): RpcOutcome => {
    const failure = failureApplies("getHealth");
    // `stale-cursor` is a getEvents concern; it cannot apply here.
    if (failure && failure.kind !== "stale-cursor") {
      return transportFailure("getHealth", failure);
    }
    const { latestLedger, oldestLedger } = scenarioState.current;
    return {
      result: {
        status: "healthy",
        latestLedger,
        oldestLedger,
        ledgerRetentionWindow: latestLedger - oldestLedger + 1,
      },
    };
  };

  const handleGetEvents = (rawParams: unknown): RpcOutcome => {
    const params = asRecord(rawParams);
    const pagination = asRecord(params.pagination);
    const cursor =
      typeof pagination.cursor === "string" && pagination.cursor !== ""
        ? pagination.cursor
        : undefined;
    const startLedgerRaw = params.startLedger;
    const hasStartLedger = startLedgerRaw !== undefined && startLedgerRaw !== null;

    const failure = failureApplies("getEvents");
    if (failure) {
      if (failure.kind === "stale-cursor") {
        if (cursor !== undefined) {
          console.log("[mock-rpc] rejecting cursor as stale (bounded drill)");
          return rpcError(
            -32000,
            bounded(`cursor ${cursor.slice(0, 40)} is stale: outside the retained window`),
          );
        }
        // No cursor to reject; the drill starts once the poller has one.
      } else {
        return transportFailure("getEvents", failure);
      }
    }

    // The real RPC rejects the two pagination modes being combined.
    if (cursor !== undefined && hasStartLedger) {
      return rpcError(-32600, "startLedger and cursor are mutually exclusive in one request");
    }

    const limitRaw = pagination.limit === undefined ? DEFAULT_PAGE_LIMIT : Number(pagination.limit);
    if (!Number.isFinite(limitRaw)) {
      return rpcError(-32602, `pagination.limit must be a number; got ${String(pagination.limit).slice(0, 40)}`);
    }
    const limit = Math.min(Math.max(Math.trunc(limitRaw), 1), MAX_PAGE_LIMIT);

    const { latestLedger, oldestLedger, ledgersPerPage } = scenarioState.current;
    const window = Math.max(1, Math.trunc(ledgersPerPage));

    let startFrom: number;
    let afterPosition: { ledger: number; seq: number } | null;
    let inclusiveLedger = false;

    if (cursor !== undefined) {
      const position = parseCursor(cursor);
      if (!position) {
        return rpcError(-32602, bounded(`cursor ${cursor.slice(0, 40)} is not a valid resume token`));
      }
      if (position.ledger < oldestLedger) {
        return rpcError(
          -32000,
          bounded(
            `cursor is stale: ledger ${position.ledger} precedes the retained floor ${oldestLedger}`,
          ),
        );
      }
      if (position.ledger > latestLedger) {
        return rpcError(
          -32602,
          bounded(`cursor ledger ${position.ledger} is ahead of the chain tip ${latestLedger}`),
        );
      }
      startFrom = position.ledger;
      afterPosition = position;
    } else {
      const requested = hasStartLedger ? Number(startLedgerRaw) : latestLedger;
      if (!Number.isSafeInteger(requested) || requested < 1) {
        return rpcError(-32602, `startLedger must be a positive integer; got ${String(startLedgerRaw).slice(0, 40)}`);
      }
      // Below the floor is an error, not an empty page — same as the real RPC.
      if (requested < oldestLedger) {
        return rpcError(
          -32603,
          bounded(`startLedger ${requested} is before the retained floor ${oldestLedger}`),
        );
      }
      if (requested > latestLedger) {
        return rpcError(
          -32602,
          bounded(`startLedger ${requested} is ahead of the chain tip ${latestLedger}`),
        );
      }
      startFrom = requested;
      afterPosition = null;
      inclusiveLedger = true;
    }

    const wanted = new Set<string>();
    const filters = Array.isArray(params.filters) ? params.filters : [];
    for (const raw of filters) {
      const filter = asRecord(raw);
      const ids = Array.isArray(filter.contractIds) ? filter.contractIds : [];
      for (const id of ids) if (typeof id === "string") wanted.add(id);
    }

    const windowEnd = Math.min(startFrom + window - 1, latestLedger);

    const candidates = normalized.filter((event) => {
      if (wanted.size > 0 && !wanted.has(event.contractId)) return false;
      if (event.ledger > windowEnd) return false;
      if (afterPosition === null) {
        return inclusiveLedger ? event.ledger >= startFrom : event.ledger > startFrom;
      }
      return (
        event.ledger > afterPosition.ledger ||
        (event.ledger === afterPosition.ledger && event.seq > afterPosition.seq)
      );
    });

    const taken = candidates.slice(0, limit);
    const moreRemain = candidates.length > taken.length;
    const last = taken[taken.length - 1];
    const nextCursor =
      moreRemain && last !== undefined
        ? positionCursor(last.ledger, last.seq)
        : positionCursor(windowEnd, 0xffffffff);

    return {
      result: {
        events: taken.map((event) => ({
          id: `${((BigInt(event.ledger) << 32n) | BigInt(event.seq)).toString()}-${event.seq}`,
          type: event.type,
          ledger: event.ledger,
          ledgerClosedAt: event.closedAt,
          transactionIndex: event.seq,
          operationIndex: 0,
          inSuccessfulContractCall: true,
          txHash: event.txHash,
          contractId: event.contractId,
          topic: event.topicB64,
          value: event.valueB64,
        })),
        cursor: nextCursor,
        latestLedger,
        oldestLedger,
        latestLedgerCloseTime: String(BASE_CLOSE_TIME_SECONDS + latestLedger * SECONDS_PER_LEDGER),
        oldestLedgerCloseTime: String(BASE_CLOSE_TIME_SECONDS + oldestLedger * SECONDS_PER_LEDGER),
      },
    };
  };

  const handleGetNetwork = (): RpcOutcome => ({
    result: {
      passphrase: MOCK_NETWORK_PASSPHRASE,
      protocolVersion: "22.1.0",
    },
  });

  const route = (method: string, params: unknown): RpcOutcome => {
    switch (method) {
      case "getHealth":
        return handleGetHealth();
      case "getEvents":
        return handleGetEvents(params);
      case "getNetwork":
        return handleGetNetwork();
      default:
        return rpcError(-32601, bounded(`method ${method.slice(0, 60)} not found`));
    }
  };

  const server = http.createServer((req, res) => {
    const respond = (status: number, body: unknown): void => {
      const payload = `${JSON.stringify(body)}\n`;
      res.writeHead(status, {
        "content-type": "application/json; charset=utf-8",
        "content-length": Buffer.byteLength(payload),
      });
      res.end(payload);
    };

    if (req.method !== "POST") {
      respond(405, { jsonrpc: "2.0", id: null, error: { code: -32600, message: "only POST is supported" } });
      return;
    }

    const chunks: Buffer[] = [];
    let size = 0;
    let aborted = false;
    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        aborted = true;
        respond(413, {
          jsonrpc: "2.0",
          id: null,
          error: { code: -32600, message: `request body exceeds ${MAX_BODY_BYTES} bytes` },
        });
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (aborted) return;

      stats.requests += 1;

      let parsed: unknown;
      try {
        parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      } catch {
        respond(400, { jsonrpc: "2.0", id: null, error: { code: -32700, message: "parse error" } });
        return;
      }

      const body = asRecord(parsed);
      const id = body.id ?? null;
      const method = typeof body.method === "string" ? body.method : "";
      stats.byMethod[method] = (stats.byMethod[method] ?? 0) + 1;

      const outcome = route(method, body.params);
      if (isFailure(outcome)) {
        respond(outcome.http, { jsonrpc: "2.0", id, error: outcome.error });
      } else {
        respond(200, { jsonrpc: "2.0", id, result: outcome.result });
      }
    });
  });

  // A body that dies mid-request must not take the server with it.
  server.on("clientError", (_err, socket) => {
    socket.destroy();
  });

  const port = options.port ?? MOCK_RPC_DEFAULT_PORT;
  await new Promise<void>((resolve, reject) => {
    const onError = (err: Error): void => reject(err);
    server.once("error", onError);
    server.listen(port, host, () => {
      server.removeListener("error", onError);
      resolve();
    });
  });
  server.on("error", (err) => {
    console.error(`[mock-rpc] server error: ${err.message}`);
  });

  const boundPort = (server.address() as AddressInfo).port;
  const url = `http://${host}:${boundPort}`;
  const scenario = scenarioState.current;
  console.log(
    `[mock-rpc] listening on ${url} · ledgers ${scenario.oldestLedger}..${scenario.latestLedger} · ` +
      `events ${normalized.length} · ${scenario.ledgersPerPage} ledger(s) per page`,
  );

  return {
    url,
    port: boundPort,
    stats: () => ({
      requests: stats.requests,
      byMethod: { ...stats.byMethod },
    }),
    setFailure(method, failure) {
      if (failure === null) {
        delete failures[method];
        console.log(`[mock-rpc] ${method} failure cleared by operator`);
      } else {
        failures[method] = { ...failure };
        console.log(
          `[mock-rpc] ${method} failure armed: ${failure.kind}` +
            (failure.times === undefined ? "" : ` for ${failure.times} request(s)`),
        );
      }
    },
    setScenario(next) {
      scenarioState.current = next;
      recompute();
      console.log(
        `[mock-rpc] scenario replaced: ledgers ${next.oldestLedger}..${next.latestLedger} · ` +
          `events ${normalized.length}`,
      );
    },
    addEvent(event) {
      scenarioState.current.events.push(event);
      // An appended event is the chain advancing; keep the tip honest so the
      // event is reachable behind an existing tip-position cursor.
      scenarioState.current.latestLedger = Math.max(
        scenarioState.current.latestLedger,
        event.ledger,
      );
      recompute();
      console.log(
        `[mock-rpc] event appended at ledger ${event.ledger} · total ${normalized.length}`,
      );
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
        // Keep-alive sockets would otherwise hold `close` open until timeout.
        server.closeAllConnections?.();
      }),
  };
}

// ── Standalone CLI ──────────────────────────────────────────────────────────
//
//   npm run mock:rpc                          # healthy mock on 127.0.0.1:8420
//   npm run mock:rpc -- --fail-events error   # getEvents fails until cleared
//   npm run mock:rpc -- --stale-cursor        # shorthand: reject every cursor
//   npm run mock:rpc -- --rate-limit          # shorthand: 429 every request
//   npm run mock:rpc -- --malformed           # append an undecodable event
//   npm run mock:rpc -- --port 0              # ephemeral port
//
// Pair it with `MIMIR_PROFILE=mock npm run scan` or `npm run mock:poll`.

const FAILURE_KINDS: readonly MockFailureKind[] = [
  "error",
  "http-500",
  "rate-limit",
  "stale-cursor",
];

export interface MockCliOptions {
  port: number;
  getEvents?: MockFailure;
  getHealth?: MockFailure;
  malformed: boolean;
}

export type MockCliResult = { ok: true; options: MockCliOptions } | { ok: false; error: string };

/**
 * Shared CLI parsing for the mock's entry points (`mock:rpc`, `mock:poll`).
 * Shorthands (`--stale-cursor`, `--rate-limit`, `--fail-rpc`) set the
 * `getEvents` failure; `--fail-events <kind>` / `--fail-health <kind>` are the
 * general form. Anything unrecognized is ignored, matching the scanner CLI.
 */
export function parseMockCli(argv: readonly string[]): MockCliResult {
  const argValue = (name: string): string | undefined => {
    const index = argv.indexOf(`--${name}`);
    if (index === -1) return undefined;
    const value = argv[index + 1];
    return value === undefined || value.startsWith("--") ? undefined : value;
  };
  const hasFlag = (name: string): boolean => argv.includes(`--${name}`);

  const portRaw = argValue("port");
  const port = portRaw === undefined ? MOCK_RPC_DEFAULT_PORT : Number(portRaw);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    return {
      ok: false,
      error: `--port must be 0-65535; got ${portRaw === undefined ? "(missing value)" : portRaw.slice(0, 20)}`,
    };
  }

  const parseKind = (flag: string): { ok: true; kind?: MockFailureKind } | { ok: false; error: string } => {
    if (!hasFlag(flag)) return { ok: true };
    const value = argValue(flag);
    if (value === undefined) {
      return { ok: false, error: `--${flag} requires a kind (${FAILURE_KINDS.join(", ")})` };
    }
    if (!(FAILURE_KINDS as readonly string[]).includes(value)) {
      return {
        ok: false,
        error: `unknown failure kind "${value.slice(0, 40)}" for --${flag}; expected one of ${FAILURE_KINDS.join(", ")}`,
      };
    }
    return { ok: true, kind: value as MockFailureKind };
  };

  const events = parseKind("fail-events");
  if (!events.ok) return { ok: false, error: events.error };
  const health = parseKind("fail-health");
  if (!health.ok) return { ok: false, error: health.error };

  // Shorthands only apply when the general form did not already set a kind.
  const shorthand: MockFailureKind | undefined = hasFlag("stale-cursor")
    ? "stale-cursor"
    : hasFlag("rate-limit")
      ? "rate-limit"
      : hasFlag("fail-rpc")
        ? "error"
        : undefined;

  return {
    ok: true,
    options: {
      port,
      ...(events.kind ? { getEvents: { kind: events.kind } } : {}),
      ...(events.kind === undefined && shorthand
        ? { getEvents: { kind: shorthand } }
        : {}),
      ...(health.kind ? { getHealth: { kind: health.kind } } : {}),
      malformed: hasFlag("malformed"),
    },
  };
}

async function main(): Promise<void> {
  const parsed = parseMockCli(process.argv);
  if (!parsed.ok) {
    console.error(`[mock-rpc] ${parsed.error}`);
    process.exit(2);
  }
  const { port, malformed, ...failures } = parsed.options;

  const scenario = defaultMockScenario();
  if (malformed) {
    scenario.events.push(malformedMockEvent(scenario.latestLedger));
    console.log("[mock-rpc] appending one malformed event (decoder must skip it, not crash)");
  }

  const mock = await startMockRpc({ port, scenario, failures });

  console.log(`[mock-rpc] ready — point a client at it with: npm run scan -- --mock`);
  console.log("[mock-rpc] press Ctrl+C to stop");

  const shutdown = (signal: string): void => {
    console.log(`[mock-rpc] ${signal} received, closing`);
    void mock.close().then(() => process.exit(0));
  };
  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
}

// Only when executed directly, not when imported by tests or the runner.
const invokedPath = process.argv[1];
if (invokedPath && import.meta.url === pathToFileURL(invokedPath).href) {
  main().catch((err: unknown) => {
    console.error(`[mock-rpc] failed to start: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
}
