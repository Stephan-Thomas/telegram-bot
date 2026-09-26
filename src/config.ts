/**
 * Environment loading and validation.
 *
 * Fails fast and LOUDLY: a notifier that boots with a missing chat id or a
 * typo'd contract id looks healthy while silently notifying nobody, which is
 * worse than not starting. Every problem found is collected and reported in one
 * error rather than one-at-a-time across restarts.
 *
 * Split into two loaders on purpose:
 *   - {@link loadStellarConfig} needs no Telegram credentials, so the chain
 *     reader (`src/stellar/events.ts`) can be run standalone against Testnet.
 *   - {@link loadConfig} is the full bot config.
 *
 * ── Profiles ─────────────────────────────────────────────────────────────────
 *
 * `MIMIR_PROFILE=mock` selects the local Soroban mock profile: it supplies
 * defaults for values the environment does NOT set (loopback RPC, fixture
 * contract ids, an isolated cursor file, placeholder credentials). Explicit
 * environment variables always win, so a profile can never change an existing
 * deployment's configuration. Any other profile name fails fast.
 */

import path from "node:path";

import "dotenv/config";

import {
  MOCK_BOT_TOKEN,
  MOCK_CHAT_ID,
  MOCK_CURSOR_FILE,
  MOCK_MARKET_CONTRACT_ID,
  MOCK_NETWORK_PASSPHRASE,
  MOCK_PROFILE_NAME,
  MOCK_RPC_DEFAULT_PORT,
  MOCK_SQUAD_CONTRACT_ID,
} from "./stellar/mock-constants.js";

export interface StellarConfig {
  marketContractId: string;
  squadContractId: string;
  rpcUrl: string;
  horizonUrl: string;
  networkPassphrase: string;
  /** Optional override for stellar.expert (or compatible) explorer origin. */
  explorerBaseUrl: string;
}

export interface BotConfig extends StellarConfig {
  botToken: string;
  chatId: string;
  /** Optional per-contract destinations; absent values use `chatId`. */
  marketChatId?: string;
  squadChatId?: string;
  /** Chats allowed to use /status. Empty array means no restriction. */
  allowedChatIds: string[];
  /** Telegram user id allowed to run operator-only commands. Null disables them. */
  operatorTelegramUserId: string | null;
  pollIntervalMs: number;
  startLookbackLedgers: number;
  cursorFile: string;
  statusFile: string;
  maxNotificationsPerCycle: number;
  /** Loopback host for the local HTTP health endpoint. */
  healthHost: string;
  /** TCP port for the health endpoint. `0` disables the listener. */
  healthPort: number;
  /**
   * After the first successful poll, treat the process as degraded if no
   * successful cycle lands within this window. `0` disables the stale check.
   */
  healthStaleMs: number;
  /**
   * How long a graceful shutdown waits for an in-flight cycle before flushing
   * cursor state and giving up on it. `0` skips the wait entirely.
   */
  shutdownTimeoutMs: number;
  /** When true, notifications sent to Telegram are formatted in preview mode. */
  channelPreviewMode: boolean;
}

/** Fallback drain budget when a config object predates `SHUTDOWN_TIMEOUT_MS`. */
export const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;

export class ConfigError extends Error {
  readonly problems: string[];

  constructor(problems: string[], hint?: string) {
    super(
      `Invalid configuration (${problems.length} problem${problems.length === 1 ? "" : "s"}):\n` +
        problems.map((p) => `  - ${p}`).join("\n") +
        `\n\n${hint ?? "Copy .env.example to .env and fill in the missing values."}`,
    );
    this.name = "ConfigError";
    this.problems = problems;
  }
}

const DEFAULTS = {
  rpcUrl: "https://soroban-testnet.stellar.org",
  horizonUrl: "https://horizon-testnet.stellar.org",
  networkPassphrase: "Test SDF Network ; September 2015",
  pollIntervalMs: 30_000,
  minPollIntervalMs: 5_000,
  startLookbackLedgers: 60,
  cursorFile: "./data/cursor.json",
  statusFile: "./data/status.json",
  maxNotificationsPerCycle: 20,
  healthHost: "127.0.0.1",
  healthPort: 8787,
  // 3× default poll interval — one missed cycle is fine; three is not.
  healthStaleMs: 90_000,
  // Long enough for an in-flight read to finish and its cursors to land, short
  // enough that a deploy is never held open by a wedged RPC.
  shutdownTimeoutMs: DEFAULT_SHUTDOWN_TIMEOUT_MS,
  channelPreviewMode: false,
} as const;

/**
 * Platform deployers (Railway among them) inject a `PORT` variable and probe it
 * for the deploy healthcheck. When `HEALTH_PORT` is unset we fall back to it,
 * so the `/health` listener is reachable without a manual override. `PORT` is
 * not a default local dev value, so the loopback port still wins on a desktop.
 */
function defaultHealthPort(): number {
  const port = Number(process.env.PORT);
  if (Number.isInteger(port) && port > 0) return port;
  return DEFAULTS.healthPort;
}

/** Strkey for a contract: `C` + 55 base32 characters. */
const CONTRACT_ID_RE = /^C[A-Z2-7]{55}$/;

/**
 * Defaults each profile supplies for values the environment leaves unset.
 * Profile values never override explicit environment variables.
 */
const PROFILE_DEFAULTS: Readonly<Record<string, Readonly<Record<string, string>>>> = {
  [MOCK_PROFILE_NAME]: {
    MARKET_CONTRACT_ID: MOCK_MARKET_CONTRACT_ID,
    SQUAD_CONTRACT_ID: MOCK_SQUAD_CONTRACT_ID,
    STELLAR_RPC_URL: `http://127.0.0.1:${MOCK_RPC_DEFAULT_PORT}`,
    STELLAR_HORIZON_URL: `http://127.0.0.1:${MOCK_RPC_DEFAULT_PORT}/horizon`,
    STELLAR_NETWORK_PASSPHRASE: MOCK_NETWORK_PASSPHRASE,
    CURSOR_FILE: MOCK_CURSOR_FILE,
    BOT_TOKEN: MOCK_BOT_TOKEN,
    TELEGRAM_CHAT_ID: MOCK_CHAT_ID,
  },
};

/** The profile selected by `MIMIR_PROFILE`, or null when unset. Display/boot use. */
export function activeProfileName(): string | null {
  return read("MIMIR_PROFILE") ?? null;
}

/** Profile defaults for the active profile. Unknown names fail fast. */
function resolveProfileDefaults(): Record<string, string> {
  const name = read("MIMIR_PROFILE");
  if (name === undefined) return {};
  const defaults = PROFILE_DEFAULTS[name];
  if (!defaults) {
    throw new ConfigError(
      [`MIMIR_PROFILE must be "${MOCK_PROFILE_NAME}" when set; got "${name}"`],
      `Unset MIMIR_PROFILE, or set MIMIR_PROFILE=${MOCK_PROFILE_NAME} for the local mock.`,
    );
  }
  return { ...defaults };
}

function read(name: string): string | undefined {
  const raw = process.env[name];
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();
  return trimmed === "" ? undefined : trimmed;
}

function collector(profile: Record<string, string>) {
  const problems: string[] = [];
  /** Environment first, then the active profile's defaults. */
  const get = (name: string): string | undefined => read(name) ?? profile[name];

  return {
    problems,

    get,

    required(name: string): string {
      const value = get(name);
      if (value === undefined) {
        problems.push(`${name} is required but not set`);
        return "";
      }
      return value;
    },

    contractId(name: string): string {
      const value = this.required(name);
      if (value !== "" && !CONTRACT_ID_RE.test(value)) {
        problems.push(
          `${name} is not a Soroban contract id (expected C… strkey, 56 chars); got "${value}"`,
        );
      }
      return value;
    },

    url(name: string, fallback: string): string {
      const value = get(name) ?? fallback;
      try {
        const parsed = new URL(value);
        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
          problems.push(`${name} must be an http(s) URL; got "${value}"`);
        }
      } catch {
        problems.push(`${name} is not a valid URL; got "${value}"`);
      }
      return value;
    },

    int(name: string, fallback: number, min: number): number {
      const raw = read(name);
      if (raw === undefined) return fallback;
      const parsed = Number(raw);
      if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
        problems.push(`${name} must be an integer; got "${raw}"`);
        return fallback;
      }
      if (parsed < min) {
        problems.push(`${name} must be >= ${min}; got ${parsed}`);
        return fallback;
      }
      return parsed;
    },

    bool(name: string, fallback: boolean): boolean {
      const raw = read(name);
      if (raw === undefined) return fallback;
      const lower = raw.toLowerCase();
      if (lower === "true" || lower === "1" || lower === "yes") return true;
      if (lower === "false" || lower === "0" || lower === "no") return false;
      problems.push(`${name} must be a boolean (true/false); got "${raw}"`);
      return fallback;
    },

    chatId(name: string): string {
      const value = this.required(name);
      // Telegram chat ids are integers (channels/supergroups are negative).
      // A @channelusername also works for public channels, so both are allowed.
      if (value !== "" && !/^-?\d+$/.test(value) && !/^@[A-Za-z0-9_]{4,}$/.test(value)) {
        problems.push(
          `${name} must be a numeric chat id (e.g. -1001234567890) or a @channelusername; got "${value}"`,
        );
      }
      return value;
    },

    optionalChatId(name: string, fallback: string): string {
      const value = read(name) ?? fallback;
      if (value === "") return value;
      if (!/^-?\d+$/.test(value) && !/^@[A-Za-z0-9_]{4,}$/.test(value)) {
        problems.push(
          `${name} must be a numeric chat id (e.g. -1001234567890) or a @channelusername; got "${value}"`,
        );
      }
      return value;
    },

    /**
     * Parses an optional comma-separated list of chat ids / @usernames.
     * Returns an empty array when the variable is absent or empty (= no
     * restriction). Each entry is validated with the same rules as chatId.
     */
    allowedChatIds(name: string): string[] {
      const raw = read(name);
      if (raw === undefined) return [];

      const entries = raw
        .split(",")
        .map((s) => s.trim())
        .filter((s) => s.length > 0);

      for (const entry of entries) {
        if (!/^-?\d+$/.test(entry) && !/^@[A-Za-z0-9_]{4,}$/.test(entry)) {
          problems.push(
            `${name} contains an invalid entry "${entry}" — ` +
              `each value must be a numeric chat id or a @channelusername`,
          );
        }
      }

      return entries;
    },

    optionalUserId(name: string): string | null {
      const value = read(name);
      if (value === undefined) return null;
      if (!/^[1-9]\d*$/.test(value) || BigInt(value) > BigInt(Number.MAX_SAFE_INTEGER)) {
        problems.push(`${name} must be a positive Telegram user id; got "${value}"`);
        return null;
      }
      return value;
    },

    host(name: string, fallback: string): string {
      const value = read(name) ?? fallback;
      // Keep this a host, not a URL — the health server binds a TCP listener.
      if (/[\s/]/.test(value) || value.includes("://")) {
        problems.push(
          `${name} must be a hostname or IP (e.g. 127.0.0.1); got "${value}"`,
        );
      }
      return value;
    },
  };
}

function stellarFrom(c: ReturnType<typeof collector>): StellarConfig {
  return {
    marketContractId: c.contractId("MARKET_CONTRACT_ID"),
    squadContractId: c.contractId("SQUAD_CONTRACT_ID"),
    rpcUrl: c.url("STELLAR_RPC_URL", DEFAULTS.rpcUrl),
    horizonUrl: c.url("STELLAR_HORIZON_URL", DEFAULTS.horizonUrl),
    networkPassphrase: c.get("STELLAR_NETWORK_PASSPHRASE") ?? DEFAULTS.networkPassphrase,
    explorerBaseUrl: c.url(
      "STELLAR_EXPLORER_BASE_URL",
      "https://stellar.expert/explorer",
    ),
  };
}

/** Chain-only config. No Telegram credentials required. */
export function loadStellarConfig(): StellarConfig {
  const c = collector(resolveProfileDefaults());
  const config = stellarFrom(c);
  if (c.problems.length > 0) throw new ConfigError(c.problems);
  return config;
}

/**
 * Resolve just the status snapshot path. Used by `--status`, which must work
 * without BOT_TOKEN: reading a status file is a read-only operation and should
 * not require the credentials of the process that wrote it.
 */
export function resolveStatusFile(): string {
  return path.resolve(process.cwd(), read("STATUS_FILE") ?? DEFAULTS.statusFile);
}

/** Full bot config: chain + Telegram + poller tuning. */
export function loadConfig(): BotConfig {
  const c = collector(resolveProfileDefaults());
  const stellar = stellarFrom(c);

  const config: BotConfig = {
    ...stellar,
    botToken: c.required("BOT_TOKEN"),
    chatId: c.chatId("TELEGRAM_CHAT_ID"),
    marketChatId: c.optionalChatId("TELEGRAM_MARKET_CHAT_ID", c.get("TELEGRAM_CHAT_ID") ?? ""),
    squadChatId: c.optionalChatId("TELEGRAM_SQUAD_CHAT_ID", c.get("TELEGRAM_CHAT_ID") ?? ""),
    allowedChatIds: c.allowedChatIds("ALLOWED_CHAT_IDS"),
    operatorTelegramUserId: c.optionalUserId("OPERATOR_TELEGRAM_USER_ID"),
    pollIntervalMs: c.int("POLL_INTERVAL_MS", DEFAULTS.pollIntervalMs, DEFAULTS.minPollIntervalMs),
    startLookbackLedgers: c.int("START_LOOKBACK_LEDGERS", DEFAULTS.startLookbackLedgers, 0),
    cursorFile: path.resolve(process.cwd(), c.get("CURSOR_FILE") ?? DEFAULTS.cursorFile),
    statusFile: path.resolve(process.cwd(), c.get("STATUS_FILE") ?? DEFAULTS.statusFile),
    maxNotificationsPerCycle: c.int(
      "MAX_NOTIFICATIONS_PER_CYCLE",
      DEFAULTS.maxNotificationsPerCycle,
      1,
    ),
    healthHost: c.host("HEALTH_HOST", DEFAULTS.healthHost),
    // Port 0 is the explicit disable switch (min 0).
    healthPort: c.int("HEALTH_PORT", defaultHealthPort(), 0),
    healthStaleMs: c.int("HEALTH_STALE_MS", DEFAULTS.healthStaleMs, 0),
    shutdownTimeoutMs: c.int("SHUTDOWN_TIMEOUT_MS", DEFAULTS.shutdownTimeoutMs, 0),
    channelPreviewMode: c.bool("CHANNEL_PREVIEW_MODE", DEFAULTS.channelPreviewMode),
  };

  if (c.problems.length > 0) throw new ConfigError(c.problems);
  return config;
}

/** `mock` / `testnet` / `public` / `unknown`, derived from the passphrase. Display only. */
export function networkLabel(config: StellarConfig): string {
  if (config.networkPassphrase === MOCK_NETWORK_PASSPHRASE) return "mock";
  if (config.networkPassphrase === "Test SDF Network ; September 2015") return "testnet";
  if (config.networkPassphrase === "Public Global Stellar Network ; September 2015") return "public";
  return "custom";
}
