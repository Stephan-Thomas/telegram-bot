/**
 * The grammy bot: commands, and the one send path the poller uses.
 *
 * The bot half is deliberately thin. It answers public status and contract
 * commands plus operator pause/resume controls. Operator controls only change
 * when the next polling cycle starts; they never edit cursors or touch chain
 * state. All chain logic lives in `src/poller.ts` and `src/stellar/`.
 */

import { Bot, type Context } from "grammy";
import type { UserFromGetMe } from "grammy/types";

import { escapeMd, previewMessage, safeErrorMessage, type ExplorerKeyboard } from "./notifications/format.js";
export { previewMessage } from "./notifications/format.js";
import { networkLabel, type BotConfig } from "./config.js";
import { contractExplorerUrl } from "./stellar/client.js";
import type { ContractSource } from "./stellar/decode.js";
import { buildHealthReport, chainClockLabel } from "./health.js";
import type { PollerPauseResult, PollerResumeResult, PollerStatus } from "./poller.js";

const HELP_BASE = [
  "*Mimir notifier*",
  "",
  "I watch Mimir's two Soroban contracts on Stellar and post every new on-chain event here: claims opened, challenges staked, oracle resolutions, settlements and payouts\\.",
  "",
  "/status — what I am watching and how far I have read",
  "/contracts — the contract ids I watch and where to look them up",
  "/health — health assessment and operational readiness",
  "/preview — preview channel notification formatting",
  "/help — this message",
];

function helpMessage(config: BotConfig): string {
  if (config.operatorTelegramUserId === null) return HELP_BASE.join("\n");
  return [
    ...HELP_BASE.slice(0, -1),
    "/pause — operator only: pause scheduling new scans",
    "/resume — operator only: resume polling now",
    HELP_BASE.at(-1) as string,
  ].join("\n");
}

const TELEGRAM_OPTIONS = {
  parse_mode: "MarkdownV2" as const,
  link_preview_options: { is_disabled: true },
};

function ago(timestamp: number | null, nowMs: number = Date.now()): string {
  if (timestamp === null) return "never";
  const seconds = Math.max(0, Math.round((nowMs - timestamp) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

function cursorPreview(cursor: string | null): string {
  if (cursor === null) return "none (cold start)";
  const compact = cursor.replace(/\s+/g, " ").replace(/[`\\]/g, "?").trim() || "empty";
  return compact.length <= 24 ? compact : `${compact.slice(0, 23)}…`;
}

function statusMessage(config: BotConfig, status: PollerStatus, nowMs: number = Date.now()): string {
  const lifecycle = status.stopping
    ? "stopping"
    : status.paused
      ? "paused"
      : status.running
        ? "running"
        : "stopped";

  const lines: string[] = [
    `*Status* — ${lifecycle} on Stellar ${networkLabel(config)}`,
    `Channel preview: ${config.channelPreviewMode ? "enabled" : "disabled"}`,
    "",
    `Chain tip: ${status.latestLedger ?? "unknown"}`,
    `RPC retains from ledger: ${status.oldestLedger ?? "unknown"}`,
    `Chain clock skew: ${escapeMd(chainClockLabel(status.chainClockAt, nowMs))}`,
    `Poll interval: ${Math.round(config.pollIntervalMs / 1000)}s · last poll ${ago(status.lastPollAt, nowMs)}`,
    `Cycles: ${status.cycles} · sent ${status.notificationsSent} · failed sends ${status.notificationsFailed} · skipped ${status.eventsSkipped}` +
      (status.notificationsDropped
        ? ` · dropped during shutdown ${status.notificationsDropped}`
        : ""),
    "",
    "*Watching*",
  ];

  for (const target of status.targets) {
    lines.push(
      `· mimir\\-${target.source} \`${target.contractId}\``,
      `  last event ledger: ${target.lastEventLedger ?? "none seen"}`,
      `  cursor: \`${cursorPreview(target.cursor)}\``,
    );
    if (target.lastError) lines.push(`  last error: ${escapeMd(target.lastError)}`);
  }

  if (status.lastError) {
    lines.push(
      "",
      `Last error \\(${ago(status.lastError.at, nowMs)}\\): ${escapeMd(status.lastError.message)}`,
    );
  }
  if (status.consecutiveFailures > 0) {
    lines.push(`Consecutive failed cycles: ${status.consecutiveFailures}`);
  }

  if (status.stopping) {
    lines.push(
      "",
      "Graceful shutdown in progress: no new cycles, unsent notifications dropped" +
        (status.pendingFlush ? ", cursor flush still pending" : ", cursor flushed") +
        "\\.",
    );
  }

  return lines.join("\n");
}

/**
 * The `/contracts` message: which two contracts this bot watches, and where to
 * look each one up independently — deliberately static (config only, no
 * poller state), so it answers the same whether the poller is mid-cycle,
 * between restarts, or wedged on a run of RPC failures. `/status` is for
 * "is it working"; this is for "what is it even watching".
 */
export function healthMessage(
  config: BotConfig,
  status: PollerStatus,
  nowMs: number = Date.now(),
): string {
  const report = buildHealthReport(config, status, nowMs);
  const statusLabel = report.status.toUpperCase();

  const lines: string[] = [
    `*Health* — ${escapeMd(statusLabel)} on Stellar ${networkLabel(config)}`,
    "",
    `Status: \`${report.status}\` \\(${report.ok ? "ok" : "action required"}\\)`,
    `Poller: ${report.poller.running ? "running" : "stopped"}`,
    `Uptime: ${report.uptimeMs > 0 ? ago(nowMs - report.uptimeMs, nowMs) : "0s"}`,
    `Poll interval: ${Math.round(config.pollIntervalMs / 1000)}s · last poll ${ago(status.lastPollAt, nowMs)}`,
    `Last successful poll: ${ago(status.lastSuccessAt, nowMs)}`,
    `Chain tip: ${report.poller.latestLedger ?? "unknown"}`,
    `Chain clock skew: ${escapeMd(chainClockLabel(status.chainClockAt, nowMs))}`,
    `Cycles: ${report.poller.cycles} · consecutive failures: ${report.poller.consecutiveFailures}`,
    `Notifications: sent ${report.poller.notificationsSent} · failed ${report.poller.notificationsFailed} · skipped ${report.poller.eventsSkipped}`,
    "",
    "*Watched Contracts*",
  ];

  for (const target of report.poller.targets) {
    lines.push(
      `· mimir\\-${target.source} \`${target.contractId}\``,
      `  last event ledger: ${target.lastEventLedger ?? "none seen"}`,
      `  cursor: \`${target.cursorPreview ?? "none (cold start)"}\``,
    );
    if (target.hasError) {
      const targetState = status.targets.find((t) => t.source === target.source);
      if (targetState?.lastError) {
        lines.push(`  last error: ${escapeMd(targetState.lastError)}`);
      }
    }
  }

  if (report.poller.lastError) {
    lines.push(
      "",
      `Last error \\(${ago(status.lastError?.at ?? null, nowMs)}\\): ${escapeMd(report.poller.lastError.message)}`,
    );
  }

  return lines.join("\n");
}

export function contractsMessage(config: BotConfig): string {
  const targets: Array<{ label: string; contractId: string }> = [
    { label: "mimir\\-market", contractId: config.marketContractId },
    { label: "mimir\\-squad", contractId: config.squadContractId },
  ];

  const lines: string[] = [
    `*Contracts* — Mimir on Stellar ${escapeMd(networkLabel(config))}`,
    "",
    "Read\\-only: this bot holds no signing keys and cannot submit transactions\\.",
  ];

  for (const target of targets) {
    lines.push(
      "",
      `*${target.label}*`,
      `\`${escapeMd(target.contractId)}\``,
      `[View on stellar\\.expert](${contractExplorerUrl(config, target.contractId)})`,
    );
  }

  return lines.join("\n");
}

/** Exact operator replies, exported for deterministic Telegram payload tests. */
export function pauseMessage(result: PollerPauseResult): string {
  switch (result) {
    case "paused":
      return "*Polling paused*\nThe current scan may finish, but no new cycle will start\\. Cursors were not changed\\.";
    case "already-paused":
      return "*Polling is already paused*";
    case "stopped":
      return "*Polling cannot pause* — the process is stopping\\.";
  }
}

export function resumeMessage(result: PollerResumeResult): string {
  switch (result) {
    case "resumed":
      return "*Polling resumed*\nThe next scan starts now\\. Cursors were not changed\\.";
    case "already-running":
      return "*Polling is already running*";
    case "stopped":
      return "*Polling cannot resume* — the process is stopping\\.";
  }
}

export interface BotDeps {
  config: BotConfig;
  status: () => PollerStatus;
  /**
   * Pre-populated bot info. When provided (e.g. in tests) grammy skips the
   * getMe() call so `bot.handleUpdate()` works without a real Telegram token.
   */
  botInfo?: UserFromGetMe;
  pause: () => PollerPauseResult;
  resume: () => PollerResumeResult;
}

/**
 * Returns true when the chat is permitted to use restricted commands.
 *
 * Rules:
 * - If `allowedChatIds` is empty the list is open (any chat may use /status).
 * - Otherwise the incoming chat id must appear in the list. Both the numeric
 *   id (stored as a number in grammy's ctx.chat.id) and its string form are
 *   compared so that negative group ids such as -1001234567890 match correctly.
 */
function isChatAllowed(allowedChatIds: string[], chatId: number): boolean {
  if (allowedChatIds.length === 0) return true;
  const asString = String(chatId);
  return allowedChatIds.some((allowed) => allowed === asString);
}

function isOperator(ctx: Context, config: BotConfig): boolean {
  const operatorId = config.operatorTelegramUserId;
  return operatorId !== null && ctx.from?.id.toString() === operatorId;
}

/** Register command handlers on a grammy-compatible bot (also useful in tests). */
export function registerCommandHandlers(bot: Bot, deps: BotDeps): void {
  const { config, status, pause, resume } = deps;

  bot.command("start", async (ctx) => {
    await ctx.reply(helpMessage(config), TELEGRAM_OPTIONS);
  });

  bot.command("help", async (ctx) => {
    await ctx.reply(helpMessage(config), TELEGRAM_OPTIONS);
  });

  bot.command("status", async (ctx) => {
    if (!isChatAllowed(config.allowedChatIds, ctx.chat.id)) {
      // Silently ignore requests from unapproved chats. Responding with an
      // error would leak the existence of the restriction; not responding at
      // all is consistent with privacy-mode bots that simply never see most
      // messages. Log so operators can diagnose misconfigured chat ids.
      console.warn(
        `[bot] /status denied for chat ${ctx.chat.id} (not in ALLOWED_CHAT_IDS)`,
      );
      return;
    }
    await ctx.reply(statusMessage(config, status()), TELEGRAM_OPTIONS);
  });

  // Config-only, so this never fails on account of poller or RPC state —
  // unlike /status, it has nothing to report failure on.
  bot.command("health", async (ctx) => {
    await ctx.reply(healthMessage(config, status()), TELEGRAM_OPTIONS);
  });

  bot.command("contracts", async (ctx) => {
    await ctx.reply(contractsMessage(config), TELEGRAM_OPTIONS);
  });

  bot.command("preview", async (ctx) => {
    const text = ctx.message?.text ?? "";
    const spaceIndex = text.indexOf(" ");
    const arg = spaceIndex !== -1 ? text.slice(spaceIndex + 1).trim() : "";
    await ctx.reply(previewMessage(config, arg || "market"), TELEGRAM_OPTIONS);
  });

  bot.command("pause", async (ctx) => {
    if (!isOperator(ctx, config)) {
      console.warn(`[bot] ignored unauthorized /pause on update ${ctx.update.update_id}`);
      return;
    }
    await ctx.reply(pauseMessage(pause()), TELEGRAM_OPTIONS);
  });

  bot.command("resume", async (ctx) => {
    if (!isOperator(ctx, config)) {
      console.warn(`[bot] ignored unauthorized /resume on update ${ctx.update.update_id}`);
      return;
    }
    await ctx.reply(resumeMessage(resume()), TELEGRAM_OPTIONS);
  });
}

export function createBot(deps: BotDeps): Bot {
  const bot = new Bot(deps.config.botToken, deps.botInfo !== undefined ? { botInfo: deps.botInfo } : undefined);
  registerCommandHandlers(bot, deps);

  // grammy rethrows handler errors by default, which would take the process
  // with it. Keep Telegram/RPC error text bounded and redact known secrets.
  bot.catch((err) => {
    console.error(
      `[bot] handler error on update ${err.ctx.update.update_id}: ` +
        safeErrorMessage(err.error, [deps.config.botToken]),
    );
  });

  return bot;
}

/** Extra Telegram send options the poller may attach to a notification. */
export interface SendExtra {
  reply_markup?: ExplorerKeyboard | undefined;
}

/**
 * The poller's send path: route each contract's messages to its named chat,
 * with the event's explorer button when `extra.reply_markup` is set.
 */
export function createNotifier(bot: Bot, config: BotConfig) {
  return async (text: string, source?: ContractSource, extra?: SendExtra): Promise<void> => {
    const chatId = source === "market"
      ? config.marketChatId ?? config.chatId
      : source === "squad"
        ? config.squadChatId ?? config.chatId
        : config.chatId;
    await bot.api.sendMessage(chatId, text, {
      ...TELEGRAM_OPTIONS,
      ...(extra?.reply_markup ? { reply_markup: extra.reply_markup } : {}),
    });
  };
}

/** Registers the command list so Telegram's UI offers autocompletion. */
export async function registerCommands(bot: Bot): Promise<void> {
  try {
    await bot.api.setMyCommands([
      { command: "start", description: "What this bot does" },
      { command: "help", description: "Show help" },
      { command: "status", description: "Last-seen ledger and watched contracts" },
      { command: "contracts", description: "Contract ids and explorer links" },
      { command: "health", description: "Health assessment and operational readiness" },
      { command: "preview", description: "Preview channel notification formatting" },
      { command: "pause", description: "Operator only: pause new scans" },
      { command: "resume", description: "Operator only: resume polling now" },
    ]);
  } catch (err) {
    // Cosmetic. Never worth failing a boot over, and never log an unbounded API error.
    console.warn(`[bot] setMyCommands failed: ${safeErrorMessage(err)}`);
  }
}

