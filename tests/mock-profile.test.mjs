import assert from "node:assert/strict";
import path from "node:path";
import test from "node:test";

import {
  ConfigError,
  activeProfileName,
  loadConfig,
  loadStellarConfig,
  networkLabel,
} from "../dist/config.js";
import {
  MOCK_MARKET_CONTRACT_ID,
  MOCK_NETWORK_PASSPHRASE,
  MOCK_SQUAD_CONTRACT_ID,
} from "../dist/stellar/mock-constants.js";

/**
 * Config-profile tests: the mock profile must supply defaults for unset
 * values only, never override explicit environment configuration, and never
 * appear when it was not asked for. A developer's local .env is neutralised
 * by deleting and restoring every key these tests depend on.
 */

const KEYS = [
  "MIMIR_PROFILE",
  "MARKET_CONTRACT_ID",
  "SQUAD_CONTRACT_ID",
  "STELLAR_RPC_URL",
  "STELLAR_HORIZON_URL",
  "STELLAR_NETWORK_PASSPHRASE",
  "STELLAR_EXPLORER_BASE_URL",
  "CURSOR_FILE",
  "BOT_TOKEN",
  "TELEGRAM_CHAT_ID",
  "OPERATOR_TELEGRAM_USER_ID",
  "POLL_INTERVAL_MS",
  "START_LOOKBACK_LEDGERS",
  "MAX_NOTIFICATIONS_PER_CYCLE",
  "HEALTH_HOST",
  "HEALTH_PORT",
  "HEALTH_STALE_MS",
];

function withEnv(overrides, fn) {
  const saved = new Map();
  for (const key of KEYS) {
    saved.set(key, process.env[key]);
    delete process.env[key];
  }
  Object.assign(process.env, overrides);
  try {
    return fn();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("without a profile, defaults are exactly the pre-profile behaviour", () => {
  withEnv(
    {
      MARKET_CONTRACT_ID: "CDV6JXIJCALSXQELCS6YUEWJWG5DFXQK5PJ5I7MWI6KVMQJBC5DLPKZI",
      SQUAD_CONTRACT_ID: "CBPGVXHXLULUBVZ24D6XNSUX7NH45HYXGWHAJFWTBHXYNO72KDRKCDFY",
      BOT_TOKEN: "123456789:TEST-ONLY-TOKEN-NEVER-USE",
      TELEGRAM_CHAT_ID: "-1001234567890",
    },
    () => {
      assert.equal(activeProfileName(), null);

      const stellar = loadStellarConfig();
      assert.equal(stellar.rpcUrl, "https://soroban-testnet.stellar.org");
      assert.equal(stellar.networkPassphrase, "Test SDF Network ; September 2015");
      assert.equal(networkLabel(stellar), "testnet");

      const config = loadConfig();
      assert.equal(config.cursorFile, path.resolve("./data/cursor.json"));
      assert.equal(config.botToken, "123456789:TEST-ONLY-TOKEN-NEVER-USE");
      assert.equal(config.marketContractId, "CDV6JXIJCALSXQELCS6YUEWJWG5DFXQK5PJ5I7MWI6KVMQJBC5DLPKZI");
    },
  );
});

test("the mock profile fills unset values with loopback and isolated-cursor defaults", () => {
  withEnv({ MIMIR_PROFILE: "mock" }, () => {
    assert.equal(activeProfileName(), "mock");

    // No Telegram credentials, no contract ids, no RPC url — it must still load.
    const config = loadConfig();

    assert.equal(config.rpcUrl, "http://127.0.0.1:8420");
    assert.equal(config.marketContractId, MOCK_MARKET_CONTRACT_ID);
    assert.equal(config.squadContractId, MOCK_SQUAD_CONTRACT_ID);
    // Profile ids are real strkey shapes so they pass the same validation.
    assert.match(config.marketContractId, /^C[A-Z2-7]{55}$/);
    assert.match(config.squadContractId, /^C[A-Z2-7]{55}$/);
    // The drill's cursor must never be the real bot's cursor file.
    assert.equal(config.cursorFile, path.resolve("./data/cursor.mock.json"));
    assert.equal(config.networkPassphrase, MOCK_NETWORK_PASSPHRASE);
    assert.equal(networkLabel(config), "mock");
    // Placeholder, not credential-shaped, and never delivered anywhere.
    assert.equal(config.botToken, "MOCK-PROFILE-NOT-A-BOT-TOKEN");
    assert.doesNotMatch(config.botToken, /^\d{6,12}:/);
    assert.equal(config.chatId, "@mock_profile");
    assert.equal(config.operatorTelegramUserId, null);
  });
});

test("explicit environment values win over mock profile defaults", () => {
  withEnv(
    {
      MIMIR_PROFILE: "mock",
      STELLAR_RPC_URL: "https://example.invalid/custom-rpc",
      CURSOR_FILE: "./data/cursor.explicit.json",
      BOT_TOKEN: "123456789:TEST-ONLY-TOKEN-NEVER-USE",
      TELEGRAM_CHAT_ID: "-1001234567890",
    },
    () => {
      const config = loadConfig();
      assert.equal(config.rpcUrl, "https://example.invalid/custom-rpc");
      assert.equal(config.cursorFile, path.resolve("./data/cursor.explicit.json"));
      assert.equal(config.botToken, "123456789:TEST-ONLY-TOKEN-NEVER-USE");
      // Values with no explicit override still come from the profile.
      assert.equal(config.marketContractId, MOCK_MARKET_CONTRACT_ID);
    },
  );
});

test("an unknown profile fails fast with an actionable hint", () => {
  withEnv({ MIMIR_PROFILE: "production" }, () => {
    assert.throws(
      () => loadStellarConfig(),
      (err) => {
        assert.ok(err instanceof ConfigError, "expected a ConfigError");
        assert.match(err.message, /MIMIR_PROFILE must be "mock" when set; got "production"/);
        assert.match(err.message, /MIMIR_PROFILE=mock/);
        assert.equal(err.problems.length, 1);
        return true;
      },
    );
  });
});

test("a blank MIMIR_PROFILE is treated as unset", () => {
  withEnv(
    {
      MIMIR_PROFILE: "   ",
      MARKET_CONTRACT_ID: "CDV6JXIJCALSXQELCS6YUEWJWG5DFXQK5PJ5I7MWI6KVMQJBC5DLPKZI",
      SQUAD_CONTRACT_ID: "CBPGVXHXLULUBVZ24D6XNSUX7NH45HYXGWHAJFWTBHXYNO72KDRKCDFY",
    },
    () => {
      assert.equal(activeProfileName(), null);
      // Normal validation applies — the mock profile must not be silently active.
      const config = loadStellarConfig();
      assert.equal(config.rpcUrl, "https://soroban-testnet.stellar.org");
      assert.equal(networkLabel(config), "testnet");
    },
  );
});
