/**
 * Tests for src/stellar/events.ts
 *
 * All fakes are pure in-process objects — no network, no Testnet credentials.
 *
 * Key invariants verified:
 *   1. Empty pages never terminate the walk early (the "EVM trap").
 *   2. eventCursorLedger parses and rejects correctly.
 *   3. The walk terminates when the cursor's ledger meets or exceeds the tip.
 *   4. maxPages cap sets truncated=true and stops the walk.
 *   5. A stalled cursor (server echoes the same cursor twice) terminates safely.
 *   6. getHealth failure propagates instead of silently returning empty.
 */

import assert from "node:assert/strict";
import test from "node:test";

import {
  eventCursorLedger,
  paginatedGetEvents,
  EVENT_MAX_PAGES,
  EVENT_PAGE_LIMIT,
} from "../dist/stellar/events.js";

// ── Cursor helpers ────────────────────────────────────────────────────────────

/**
 * Build a cursor string that encodes a given ledger sequence the same way
 * Soroban does: TOID = (ledger << 32) | txIndex, cursor = `${TOID}-${opIndex}`.
 */
function makeCursor(ledger, txIndex = 1, opIndex = 0) {
  const toid = (BigInt(ledger) << 32n) | BigInt(txIndex);
  return `${toid}-${opIndex}`;
}

// ── Server fake ───────────────────────────────────────────────────────────────

/**
 * Build a minimal fake rpc.Server from a list of page descriptors.
 *
 * Each call to fakeServer.getEvents() pops the next page off the queue.
 * A page descriptor is:
 *   { events: [...], cursor: string, latestLedger: number }
 *
 * fakeServer.getHealth() always returns the supplied health values.
 */
function makeServer(health, pages) {
  const queue = [...pages];
  let callIndex = 0;
  return {
    async getHealth() {
      return health;
    },
    async getEvents(_req) {
      const page = queue[callIndex] ?? queue[queue.length - 1];
      callIndex += 1;
      return {
        events: page.events ?? [],
        cursor: page.cursor ?? "",
        latestLedger: page.latestLedger ?? health.latestLedger,
      };
    },
  };
}

// ── eventCursorLedger ─────────────────────────────────────────────────────────

test("eventCursorLedger: valid cursor returns correct ledger", () => {
  // ledger 1000, txIndex 1 → TOID = (1000 << 32) | 1 = 4294968297
  const cursor = makeCursor(1000, 1, 0);
  assert.equal(eventCursorLedger(cursor), 1000);
});

test("eventCursorLedger: ledger 0 is representable", () => {
  const cursor = makeCursor(0, 0, 0);
  assert.equal(eventCursorLedger(cursor), 0);
});

test("eventCursorLedger: large ledger round-trips", () => {
  const ledger = 9_999_999;
  const cursor = makeCursor(ledger, 4294967295, 4294967295);
  assert.equal(eventCursorLedger(cursor), ledger);
});

test("eventCursorLedger: missing TOID part returns null", () => {
  assert.equal(eventCursorLedger(""), null);
  assert.equal(eventCursorLedger("-1"), null);
});

test("eventCursorLedger: non-numeric TOID returns null", () => {
  assert.equal(eventCursorLedger("abc-0"), null);
  assert.equal(eventCursorLedger("0x1A-0"), null);
});

test("eventCursorLedger: bare number with no dash parses TOID as-is (ledger 0 for small values)", () => {
  // "12345" has no dash, so split("-")[0] = "12345" which is numeric.
  // BigInt("12345") >> 32n = 0 because 12345 < 2^32.
  // The function returns a number (0), not null.
  const result = eventCursorLedger("12345");
  assert.ok(typeof result === "number", "should return a number for a numeric-only token");
});

// ── Empty-page walk ───────────────────────────────────────────────────────────

test("paginatedGetEvents: 12 empty pages then 1 events page — all events collected (the EVM trap)", async () => {
  // This is the documented Testnet reality: 12 empty pages before the one
  // that holds all 11 events.  Stopping on an empty page would yield zero.
  const tip = 5_000;
  const eventLedger = 4_950;
  const finalCursor = makeCursor(tip, 1, 0);

  const pages = [];
  // 12 empty pages, each advancing the cursor by ~4 ledgers
  for (let i = 0; i < 12; i++) {
    const ledger = 4_500 + i * 4;
    pages.push({ events: [], cursor: makeCursor(ledger), latestLedger: tip });
  }
  // Page 13: holds the events, cursor at or past the tip
  pages.push({
    events: [
      { id: "e1", contractId: "C1", ledger: eventLedger, txHash: "abc", ledgerClosedAt: "2026-01-01T00:00:00Z", topic: [], value: null },
      { id: "e2", contractId: "C1", ledger: eventLedger, txHash: "abc", ledgerClosedAt: "2026-01-01T00:00:00Z", topic: [], value: null },
    ],
    cursor: finalCursor,
    latestLedger: tip,
  });

  const server = makeServer(
    { status: "healthy", oldestLedger: 4_000, latestLedger: tip },
    pages,
  );

  const result = await paginatedGetEvents(server, [], {
    startLedger: 4_500,
    maxPages: 20,
  });

  assert.equal(result.events.length, 2, "all events on page 13 must be returned");
  assert.equal(result.pages, 13, "must have walked all 13 pages");
  assert.equal(result.truncated, false, "walk ended at cursor tip, not page cap");
  assert.equal(result.cursor, finalCursor);
});

test("paginatedGetEvents: single empty page with cursor at tip terminates and returns no events", async () => {
  const tip = 4_000;
  const cursor = makeCursor(tip, 1, 0);

  const server = makeServer(
    { status: "healthy", oldestLedger: 3_900, latestLedger: tip },
    [{ events: [], cursor, latestLedger: tip }],
  );

  const result = await paginatedGetEvents(server, [], { startLedger: 3_950 });

  assert.equal(result.events.length, 0);
  assert.equal(result.pages, 1);
  assert.equal(result.truncated, false);
  assert.equal(result.cursor, cursor);
});

test("paginatedGetEvents: cold start uses lookbackLedgers to compute startLedger", async () => {
  const tip = 5_000;
  const cursor = makeCursor(tip, 1, 0);

  const requests = [];
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(req) {
      requests.push(req);
      return { events: [], cursor, latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { lookbackLedgers: 100 });

  // The request should have used startLedger = tip - 100 = 4900
  const req = requests[0];
  assert.ok(!req.cursor, "cursor request must not be sent on a cold start");
  assert.ok(req.startLedger !== undefined, "startLedger should be set");
  assert.ok(req.startLedger >= 4_900, `startLedger ${req.startLedger} should be >= 4900`);
});

test("paginatedGetEvents: startLedger is clamped up to oldestLedger", async () => {
  // If the requested startLedger is below the floor, it is clamped up.
  const tip = 10_000;
  const oldest = 9_000;
  const cursor = makeCursor(tip, 1, 0);

  const requests = [];
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: oldest, latestLedger: tip };
    },
    async getEvents(req) {
      requests.push(req);
      return { events: [], cursor, latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { startLedger: 100 }); // far below floor

  assert.ok(requests[0].startLedger >= oldest, "must not request below retained floor");
});

test("paginatedGetEvents: resume cursor request omits startLedger", async () => {
  const tip = 5_000;
  const resumeCursor = makeCursor(4_900, 1, 0);
  const endCursor = makeCursor(tip, 1, 0);

  const requests = [];
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(req) {
      requests.push(req);
      return { events: [], cursor: endCursor, latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { cursor: resumeCursor });

  // First request must use cursor, not startLedger
  assert.equal(requests[0].cursor, resumeCursor, "first request must use cursor");
  assert.ok(!("startLedger" in requests[0]), "cursor request must not include startLedger");
});

// ── Cursor stall detection ────────────────────────────────────────────────────

test("paginatedGetEvents: stalled cursor (server echoes same cursor) terminates without looping", async () => {
  const tip = 5_000;
  const stalledCursor = makeCursor(4_800, 1, 0); // well behind tip

  let calls = 0;
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(_req) {
      calls += 1;
      // Always return the same cursor — simulates a stuck server.
      return { events: [], cursor: stalledCursor, latestLedger: tip };
    },
  };

  const result = await paginatedGetEvents(server, [], { startLedger: 4_800, maxPages: 10 });

  // The first call gets the stalled cursor. The second call sends that cursor back
  // and gets the same cursor again — previousCursor === nextCursor, so it stops.
  assert.ok(calls <= 3, `stall must terminate in a small number of calls, got ${calls}`);
  assert.equal(result.truncated, false, "stall is not a truncation");
});

test("paginatedGetEvents: empty cursor string from server terminates walk", async () => {
  const tip = 5_000;
  let calls = 0;
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(_req) {
      calls += 1;
      return { events: [], cursor: "", latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { startLedger: 4_900, maxPages: 10 });
  assert.equal(calls, 1, "empty cursor on first page must stop immediately");
});

// ── Page-limit truncation ─────────────────────────────────────────────────────

test("paginatedGetEvents: maxPages cap sets truncated=true and stops the walk", async () => {
  const tip = 9_999;
  // Provide 10 pages, each advancing the cursor but never reaching the tip.
  const pages = Array.from({ length: 10 }, (_, i) => ({
    events: [],
    cursor: makeCursor(1_000 + i * 10, 1, 0),
    latestLedger: tip,
  }));

  const server = makeServer(
    { status: "healthy", oldestLedger: 900, latestLedger: tip },
    pages,
  );

  const result = await paginatedGetEvents(server, [], { startLedger: 1_000, maxPages: 5 });

  assert.equal(result.truncated, true, "must be marked truncated when capped");
  assert.equal(result.pages, 5, "must stop at maxPages");
});

test("paginatedGetEvents: default maxPages is EVENT_MAX_PAGES", async () => {
  const tip = 99_999;
  // Provide a large number of advancing pages, each short of the tip.
  const pages = Array.from({ length: EVENT_MAX_PAGES + 5 }, (_, i) => ({
    events: [],
    cursor: makeCursor(1_000 + i, 1, 0),
    latestLedger: tip,
  }));

  const server = makeServer(
    { status: "healthy", oldestLedger: 900, latestLedger: tip },
    pages,
  );

  const result = await paginatedGetEvents(server, [], { startLedger: 1_000 });
  assert.equal(result.pages, EVENT_MAX_PAGES);
  assert.equal(result.truncated, true);
});

test("paginatedGetEvents: limit defaults to EVENT_PAGE_LIMIT passed to server", async () => {
  const tip = 5_000;
  const cursor = makeCursor(tip, 1, 0);

  const requests = [];
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(req) {
      requests.push(req);
      return { events: [], cursor, latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { startLedger: 4_900 });
  assert.equal(requests[0].limit, EVENT_PAGE_LIMIT);
});

test("paginatedGetEvents: custom limit is forwarded to the server", async () => {
  const tip = 5_000;
  const cursor = makeCursor(tip, 1, 0);

  const requests = [];
  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(req) {
      requests.push(req);
      return { events: [], cursor, latestLedger: tip };
    },
  };

  await paginatedGetEvents(server, [], { startLedger: 4_900, limit: 42 });
  assert.equal(requests[0].limit, 42);
});

// ── Termination on cursor ledger >= tip ───────────────────────────────────────

test("paginatedGetEvents: walk stops as soon as cursor ledger meets the chain tip", async () => {
  const tip = 5_000;
  let pageCount = 0;

  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(_req) {
      pageCount += 1;
      // On the first page the cursor ledger equals the tip — should terminate.
      return {
        events: [],
        cursor: makeCursor(tip, 1, 0),
        latestLedger: tip,
      };
    },
  };

  const result = await paginatedGetEvents(server, [], { startLedger: 4_900 });
  assert.equal(pageCount, 1);
  assert.equal(result.truncated, false);
});

test("paginatedGetEvents: cursor past tip terminates without fetching another page", async () => {
  const tip = 4_999;
  let pageCount = 0;

  const server = {
    async getHealth() {
      return { status: "healthy", oldestLedger: 4_000, latestLedger: tip };
    },
    async getEvents(_req) {
      pageCount += 1;
      return {
        events: [],
        cursor: makeCursor(tip + 10, 1, 0), // past tip
        latestLedger: tip,
      };
    },
  };

  await paginatedGetEvents(server, [], { startLedger: 4_900 });
  assert.equal(pageCount, 1);
});

// ── getHealth failure ─────────────────────────────────────────────────────────

test("paginatedGetEvents: getHealth failure propagates as a rejection", async () => {
  const server = {
    async getHealth() {
      throw new Error("RPC getHealth timeout");
    },
    async getEvents(_req) {
      return { events: [], cursor: "", latestLedger: 0 };
    },
  };

  await assert.rejects(
    () => paginatedGetEvents(server, [], { startLedger: 100 }),
    /getHealth timeout/,
  );
});

// ── Event accumulation across pages ──────────────────────────────────────────

test("paginatedGetEvents: events from multiple pages are concatenated", async () => {
  const tip = 6_000;

  const makeEvent = (id) => ({
    id,
    contractId: "C1",
    ledger: 5_000,
    txHash: "abc",
    ledgerClosedAt: "2026-01-01T00:00:00Z",
    topic: [],
    value: null,
  });

  const pages = [
    { events: [makeEvent("e1"), makeEvent("e2")], cursor: makeCursor(5_100), latestLedger: tip },
    { events: [makeEvent("e3")], cursor: makeCursor(tip), latestLedger: tip },
  ];

  const server = makeServer({ status: "healthy", oldestLedger: 4_000, latestLedger: tip }, pages);
  const result = await paginatedGetEvents(server, [], { startLedger: 4_900 });

  assert.equal(result.events.length, 3);
  assert.equal(result.pages, 2);
  assert.deepEqual(
    result.events.map((e) => e.id),
    ["e1", "e2", "e3"],
  );
});

test("paginatedGetEvents: latestLedger is taken from the last response", async () => {
  const tip1 = 5_000;
  const tip2 = 5_005; // tip advances between requests

  const pages = [
    { events: [], cursor: makeCursor(4_990), latestLedger: tip1 },
    { events: [], cursor: makeCursor(tip2), latestLedger: tip2 },
  ];

  const server = makeServer({ status: "healthy", oldestLedger: 4_000, latestLedger: tip1 }, pages);
  const result = await paginatedGetEvents(server, [], { startLedger: 4_900 });

  assert.equal(result.latestLedger, tip2);
});
