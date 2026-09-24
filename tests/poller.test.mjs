import assert from "node:assert/strict";
import test from "node:test";
import { rm, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Since it's mjs in tests/ we import the compiled ts
import { extractRetryAfterMs, createPoller } from "../dist/poller.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function baseConfig(overrides = {}) {
  return {
    marketContractId: "CDV6JXIJCALSXQELCS6YUEWJWG5DFXQK5PJ5I7MWI6KVMQJBC5DLPKZI",
    squadContractId: "CBPGVXHXLULUBVZ24D6XNSUX7NH45HYXGWHAJFWTBHXYNO72KDRKCDFY",
    rpcUrl: "https://soroban-testnet.stellar.org",
    horizonUrl: "https://horizon-testnet.stellar.org",
    networkPassphrase: "Test SDF Network ; September 2015",
    botToken: "0000000000:SECRET-TOKEN-DO-NOT-LEAK",
    chatId: "-1001234567890",
    pollIntervalMs: 10_000,
    startLookbackLedgers: 60,
    cursorFile: path.join(__dirname, "temp-cursor.json"),
    maxNotificationsPerCycle: 20,
    ...overrides,
  };
}

test("extractRetryAfterMs parses Grammy-style parameters", () => {
  const err = { parameters: { retry_after: 42 } };
  assert.equal(extractRetryAfterMs(err), 42_000);
});

test("extractRetryAfterMs parses fetch-style Headers", () => {
  const headers = new Headers();
  headers.set("Retry-After", "15");
  const err = { response: { headers } };
  assert.equal(extractRetryAfterMs(err), 15_000);
});

test("extractRetryAfterMs parses node fetch / axios object headers", () => {
  const err = { response: { headers: { "retry-after": "5" } } };
  assert.equal(extractRetryAfterMs(err), 5_000);
});

test("extractRetryAfterMs clamps to MAX_RETRY_AFTER_MS", () => {
  const err = { parameters: { retry_after: 100_000 } };
  assert.equal(extractRetryAfterMs(err), 3_600_000);
});

test("extractRetryAfterMs returns null for missing or unparseable headers", () => {
  assert.equal(extractRetryAfterMs(null), null);
  assert.equal(extractRetryAfterMs({}), null);
  assert.equal(extractRetryAfterMs({ response: { headers: { "retry-after": "nope" } } }), null);
});

test("createPoller stale cursor resets state", async () => {
  const cursorFile = path.join(__dirname, "temp-cursor-stale.json");
  const config = baseConfig({ cursorFile, pollIntervalMs: 200 });
  
  await writeFile(
    config.cursorFile,
    JSON.stringify({
      version: 1,
      targets: { market: { cursor: "12345-0" } }
    })
  );

  let eventsCalls = 0;
  const server = {
    getHealth: async () => ({ latestLedger: 100, oldestLedger: 50 }),
    getEvents: async (req) => {
      eventsCalls++;
      if (req.cursor === "12345-0") {
        throw new Error("Cursor is before the oldest ledger (retention)");
      }
      return { events: [], latestLedger: 100 };
    },
  };

  const poller = createPoller({
    config,
    server,
    send: async () => {},
  });

  try {
    await poller.start();
    // wait for cycle to run
    await new Promise(r => setTimeout(r, 50));
    const status = poller.status();
    const marketTarget = status.targets.find(t => t.source === "market");
    assert.equal(marketTarget.cursor, null, "Cursor should be reset on stale cursor error");
    assert.match(marketTarget.lastError, /Stale cursor reset/);
  } finally {
    poller.stop();
    await rm(config.cursorFile, { force: true });
  }
});

test("createPoller honors RPC Retry-After and pauses other targets", async () => {
  const cursorFile = path.join(__dirname, "temp-cursor-rpc.json");
  const config = baseConfig({ cursorFile, pollIntervalMs: 200 });
  await rm(config.cursorFile, { force: true });

  let marketCalls = 0;
  let squadCalls = 0;
  
  const server = {
    getHealth: async () => ({ latestLedger: 100, oldestLedger: 50 }),
    getEvents: async (req) => {
      if (req.filters[0].contractIds[0] === config.marketContractId) {
        marketCalls++;
        const err = new Error("Rate limited");
        err.response = { headers: { "retry-after": "5" } };
        throw err;
      } else {
        squadCalls++;
        return { events: [], latestLedger: 100 };
      }
    },
  };

  const poller = createPoller({
    config,
    server,
    send: async () => {},
  });

  try {
    await poller.start();
    await new Promise(r => setTimeout(r, 50));
    
    // Market should have been called and failed with Retry-After
    assert.equal(marketCalls, 1);
    
    // Squad should have been skipped because cycle broke early
    assert.equal(squadCalls, 0);
    
    const status = poller.status();
    assert.equal(status.consecutiveFailures, 1);
  } finally {
    poller.stop();
    await rm(config.cursorFile, { force: true });
  }
});

test("createPoller normal run persists cursor", async () => {
  const cursorFile = path.join(__dirname, "temp-cursor-normal.json");
  const config = baseConfig({ cursorFile, pollIntervalMs: 200 });
  await rm(config.cursorFile, { force: true });

  const server = {
    getHealth: async () => ({ latestLedger: 100, oldestLedger: 50 }),
    getEvents: async (req) => {
      return { events: [], latestLedger: 100, cursor: "999-0" };
    },
  };

  const poller = createPoller({
    config,
    server,
    send: async () => {},
  });

  try {
    await poller.start();
    await new Promise(r => setTimeout(r, 50));
    
    const status = poller.status();
    const marketTarget = status.targets.find(t => t.source === "market");
    assert.equal(marketTarget.cursor, "999-0");
  } finally {
    poller.stop();
    await rm(config.cursorFile, { force: true });
  }
});
