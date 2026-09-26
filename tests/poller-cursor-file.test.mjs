import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { createPoller } from "../dist/poller.js";

const CURSOR_FILE = JSON.stringify({
  version: 1,
  updatedAt: "2026-09-24T00:00:00.000Z",
  targets: {
    market: { cursor: "123-0", lastEventLedger: 40 },
    squad: { cursor: "456-0", lastEventLedger: 41 },
  },
});

function baseConfig(cursorFile) {
  return {
    marketContractId: "C" + "A".repeat(55),
    squadContractId: "C" + "B".repeat(55),
    rpcUrl: "https://example.invalid/rpc",
    horizonUrl: "https://example.invalid/horizon",
    networkPassphrase: "Test SDF Network ; September 2015",
    explorerBaseUrl: "https://example.invalid/explorer",
    botToken: "123456789:TEST-ONLY-TOKEN-NEVER-USE",
    chatId: "-1001234567890",
    operatorTelegramUserId: "42",
    pollIntervalMs: 5_000,
    startLookbackLedgers: 60,
    cursorFile,
    maxNotificationsPerCycle: 20,
    healthHost: "127.0.0.1",
    healthPort: 0,
    healthStaleMs: 90_000,
  };
}

function stuckServer() {
  return {
    getHealth: async () => new Promise(() => undefined),
  };
}

async function waitForFailedCycle(poller) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (poller.status().consecutiveFailures > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("poller failure cycle did not finish");
}

/**
 * `stop()` lets an in-flight cycle finish, and that cycle rewrites the cursor
 * file (write-then-rename). Deleting the directory while it lands races
 * ENOTEMPTY, so wait for the rewrite the cycle already started.
 */
async function waitForCursorRewrite(cursorFile, originalUpdatedAt) {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    try {
      const current = JSON.parse(await readFile(cursorFile, "utf8")).updatedAt;
      if (current !== originalUpdatedAt) return;
    } catch {
      // Mid-rename; the next attempt will see the new file.
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

test("pause/resume is bounded during an in-flight scan and restart reloads version-1 cursors", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "mimir-resume-"));
  const cursorFile = path.join(directory, "cursor.json");
  await writeFile(cursorFile, CURSOR_FILE, "utf8");

  const first = createPoller({
    config: baseConfig(cursorFile),
    server: stuckServer(),
    send: async () => undefined,
  });

  try {
    await first.start();
    assert.equal(first.status().paused, false);
    assert.equal(first.status().running, true);
    assert.equal(first.status().targets[0].cursor, "123-0");

    assert.equal(first.pause(), "paused");
    assert.equal(first.status().paused, true);
    assert.equal(first.pause(), "already-paused");
    assert.equal(first.resume(), "resumed");
    assert.equal(first.status().paused, false);
    assert.equal(first.resume(), "already-running");

    // Operator control never rewrites the version-1 cursor compatibility shape.
    assert.equal(JSON.parse(await readFile(cursorFile, "utf8")).version, 1);
  } finally {
    first.stop();
  }

  const second = createPoller({
    config: baseConfig(cursorFile),
    server: stuckServer(),
    send: async () => undefined,
  });
  try {
    await second.start();
    assert.equal(second.status().paused, false, "pause must not survive a process restart");
    assert.equal(second.status().targets[1].cursor, "456-0");
  } finally {
    second.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("stopped poller rejects both operator controls", () => {
  const poller = createPoller({
    config: baseConfig("/tmp/unused-mimir-cursor.json"),
    server: stuckServer(),
    send: async () => undefined,
  });

  poller.stop();
  assert.equal(poller.pause(), "stopped");
  assert.equal(poller.resume(), "stopped");
  assert.equal(poller.status().running, false);
  assert.equal(poller.status().paused, false);
});

test("unsupported cursor versions fall back to a cold start", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "mimir-cursor-format-"));
  const cursorFile = path.join(directory, "cursor.json");
  await writeFile(
    cursorFile,
    JSON.stringify({ version: 2, targets: { market: { cursor: "123-0", lastEventLedger: 40 } } }),
    "utf8",
  );

  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: stuckServer(),
    send: async () => undefined,
  });

  try {
    await poller.start();
    assert.equal(poller.status().targets[0].cursor, null);
    assert.equal(poller.status().targets[1].cursor, null);
  } finally {
    poller.stop();
    await rm(directory, { recursive: true, force: true });
  }
});

test("RPC failures are bounded and redact the configured bot token in status", async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), "mimir-rpc-failure-"));
  const cursorFile = path.join(directory, "cursor.json");
  await writeFile(cursorFile, CURSOR_FILE, "utf8");
  const originalUpdatedAt = JSON.parse(await readFile(cursorFile, "utf8")).updatedAt;
  const secret = "123456789:TEST-ONLY-TOKEN-NEVER-USE";
  const longPayload = "remote-payload".repeat(100);
  const poller = createPoller({
    config: baseConfig(cursorFile),
    server: {
      getHealth: async () => {
        throw new Error(`${secret} ${longPayload}`);
      },
    },
    send: async () => undefined,
  });
  const originalError = console.error;
  const logs = [];
  console.error = (...args) => logs.push(args.join(" "));

  try {
    await poller.start();
    await waitForFailedCycle(poller);
    const status = poller.status();
    assert.equal(status.consecutiveFailures, 1);
    assert.equal(status.targets.find((target) => target.source === "market").cursor, "123-0");
    assert.match(status.lastError.message, /^(market|squad): /);
    assert.equal(status.lastError.message.includes(secret), false);
    assert.ok(status.lastError.message.length <= 250);
    assert.equal(logs.join("\n").includes(secret), false);
  } finally {
    console.error = originalError;
    poller.stop();
    await waitForCursorRewrite(cursorFile, originalUpdatedAt);
    await rm(directory, { recursive: true, force: true });
  }
});
