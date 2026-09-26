/**
 * Shared constants for the local Soroban mock profile (`MIMIR_PROFILE=mock`).
 *
 * Kept in their own module so both `src/config.ts` (profile defaults) and
 * `src/stellar/mock-rpc.ts` (the mock's own chain identity) can share one
 * source of truth without an import cycle.
 *
 * Everything here is a stable, obviously-fake local value: contract ids and
 * addresses are real strkey *shapes* (so config validation passes) derived
 * deterministically from fixed seeds, never from a live network, and the bot
 * token placeholder is not a credential — the mock profile never talks to
 * Telegram.
 */

/** The value of `MIMIR_PROFILE` that selects the local mock profile. */
export const MOCK_PROFILE_NAME = "mock";

/** Loopback port `npm run mock:rpc` / `npm run mock:poll` bind by default. */
export const MOCK_RPC_DEFAULT_PORT = 8420;

/** Network passphrase reported by the mock's `getNetwork`. Display/explorer only. */
export const MOCK_NETWORK_PASSPHRASE = "Local Mimir Mock ; Mimir Notifier";

/**
 * Deterministic strkey-shaped contract ids (sha256 of a fixed seed, encoded
 * as contract strkeys). They satisfy `^C[A-Z2-7]{55}$` so profile config
 * passes the same validation as production ids.
 */
export const MOCK_MARKET_CONTRACT_ID = "CAP3GIE7KQDU4NOEF2UPNP7YRH2ARA6KBXXFV73RNBVEGMRCIKT4BBNF";
export const MOCK_SQUAD_CONTRACT_ID = "CADTGYRIPLZRIZBQVYT7ZD6K5LG5CSLLEX53WIG5NXJLTUSEYN7ATYAQ";

/**
 * Cursor file used when the mock profile is active and `CURSOR_FILE` is not
 * set: isolated from the real `./data/cursor.json` so a local drill can never
 * overwrite a running bot's resume position.
 */
export const MOCK_CURSOR_FILE = "./data/cursor.mock.json";

/**
 * Placeholder credentials so `loadConfig()` succeeds with zero secrets. The
 * dry-run entry (`npm run mock:poll`) never constructs a Telegram client, and
 * the values are deliberately non-credential-shaped.
 */
export const MOCK_BOT_TOKEN = "MOCK-PROFILE-NOT-A-BOT-TOKEN";
export const MOCK_CHAT_ID = "@mock_profile";
