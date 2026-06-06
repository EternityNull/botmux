/**
 * v3 runtime — the scheduling main loop.
 *
 * Ties the pure pieces together against the SHARED contract:
 *   load dag → freeze bot snapshots → init runDir →
 *   { materialize journal → decideNext → dispatch ready work under caps →
 *     await a settle → repeat } until terminal.
 *
 * Every side effect lives here (journal append, STATE checkpoint, dir layout,
 * goal/inputs/env materialization).  The actual worker spawn (`runNode`) and
 * manifest validation (`validateManifest`) are INJECTED — codex's
 * `ephemeral-pool.ts` / `manifest.ts` provide them, but the runtime compiles
 * against the contract types alone so the two halves build independently.
 *
 * MVP scope: static DAG, fail-fast, no retry (always `attempts/001`).  Retry
 * (`attempts/NNN`) and richer cancel semantics are deferred — see
 * `docs/design/2026-06-01-v3-mvp-engine-split.md`.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';

import {
  DEFAULT_NODE_TIMEOUT_SEC,
  isGoalNode,
  isLoopNode,
  loopInstanceId,
  type V3Dag,
  type V3LoopExitWhen,
  type V3LoopNode,
  type V3Node,
  type V3ResultSchema,
} from './dag.js';
import { decideNext, type V3Action } from './orchestrator.js';
import { appendEvent, readJournal, type StoredEvent, type V3ErrorClass, type V3LoopRef } from './journal.js';
import { materialize, writeState } from './state.js';
import { writePendingWait } from './human-gate.js';
import {
  GOAL_ENV,
  MANIFEST_FILE_KINDS,
  MANIFEST_SCHEMA_VERSION,
  MANIFEST_STATUSES,
  V3_SUPPORTED_CLIS,
  isV3SupportedCli,
  type BotSnapshot,
  type GoalInputs,
  type Manifest,
  type RunNode,
  type RunNodeRequest,
  type ValidateManifest,
} from './contract.js';

// ─── goal.txt rendering ─────────────────────────────────────────────────────

/**
 * Render the self-contained instruction file the goal-mode agent reads via
 * `$BOTMUX_GOAL_PATH`.  The execution contract (read inputs / write products /
 * write the manifest) lives HERE — in a file — rather than inside the `/goal`
 * command text, because a long multi-line `/goal` argument trips Claude Code's
 * paste-detection (the TUI folds it into a "[Pasted text]" blob and the
 * slash-command parser never fires).  The pool's `buildGoalCommand` therefore
 * sends only a short single-line `/goal` that points the agent at this file.
 *
 * Rendered from `contract.ts` constants so the manifest shape stays a single
 * source of truth shared with codex's validator.
 */
export function renderGoalFile(
  goal: string,
  resultSchema?: V3ResultSchema,
  loopCtx?: { loopId: string; iteration: number; maxIterations: number },
): string {
  const E = GOAL_ENV;
  const kinds = MANIFEST_FILE_KINDS.join(' | ');
  const [okStatus, failStatus] = MANIFEST_STATUSES;
  const hasEnum = resultSchema && Object.values(resultSchema.properties).some((p) => p.enum);
  const resultSection = resultSchema
    ? [
        '## Structured result (REQUIRED for this node)',
        `This node declares a structured output contract. Write a JSON file \`result.json\` directly under $${E.OUTPUT_DIR} matching this schema (declared property types are enforced at the top level; every \`required\` field must be present):`,
        '',
        '  ' + JSON.stringify(resultSchema),
        '',
        ...(hasEnum
          ? [
              'Fields declaring an `enum` MUST use one of the listed values EXACTLY (case-sensitive) — downstream routing decisions read these values, and anything outside the vocabulary blocks this node.',
              '',
            ]
          : []),
        `List \`result.json\` in the manifest \`files\` array like any other product (its \`path\` is exactly "result.json"). A missing or schema-violating result.json blocks this node.`,
        '',
      ]
    : [];
  const loopSection = loopCtx
    ? [
        '## Loop context',
        `This node runs inside loop "${loopCtx.loopId}", iteration ${loopCtx.iteration} of at most ${loopCtx.maxIterations}.`,
        ...(loopCtx.iteration > 1
          ? [
              'Inputs labeled `previous.<node>` are products of the PREVIOUS iteration (e.g. the last test report). Read them FIRST and fix what they describe — do not redo work that already passed, and do not guess what happened last round.',
            ]
          : []),
        'Report results honestly — a truthful "not passed" routes the rework correctly; a wishful "passed" ships a broken result.',
        '',
      ]
    : [];
  return [
    '# botmux v3 节点任务 / botmux v3 node task',
    '',
    '## Goal',
    goal,
    '',
    ...loopSection,
    '## How to complete this node',
    'You are an autonomous agent completing exactly ONE botmux v3 workflow node.',
    'Work toward the goal above until it is done, then stop. Do not ask the user any questions.',
    '',
    `- Upstream inputs: the file at $${E.INPUTS_PATH} is a JSON object \`{ "inputs": [...] }\` listing upstream products, each with an absolute \`path\`. Read only the ones the goal needs (it may be empty). If an \`omitted\` array is present, those declared inputs were intentionally not produced (their workflow branch was not taken) — treat their absence as by-design, do NOT invent their content.`,
    `- Output: write ALL products under the directory at $${E.OUTPUT_DIR}. Do NOT write anything outside that directory.`,
    `- Manifest (required): before you finish, write a JSON manifest to $${E.MANIFEST_PATH} with exactly this shape:`,
    '',
    '  {',
    `    "schemaVersion": ${MANIFEST_SCHEMA_VERSION},`,
    `    "status": "${okStatus}" | "${failStatus}",`,
    '    "summary": "<one short line>",',
    '    "files": [',
    `      { "name": "<logical name>", "path": "<RELATIVE to the output dir>", "kind": "<${kinds}>", "bytes": <int>, "sha256": "<hex sha256 of the file; empty string \\"\\" for a directory>", "mime": "<mime type>", "preview": "<optional short excerpt>" }`,
    '    ],',
    `    "error": { "code": "...", "message": "...", "retryable": false }`,
    '  }',
    '',
    `  - On success: status "${okStatus}", at least one file entry, and NO \`error\` field.`,
    `  - On failure: status "${failStatus}", \`error\` required, \`files\` may be empty. Set \`error.retryable\` honestly: \`true\` when a human can unblock you and a fresh attempt could then succeed; \`false\` when retrying cannot help.`,
    `  - Every file \`path\` is relative to $${E.OUTPUT_DIR} ITSELF. A file you wrote directly into that directory has a path that is JUST its filename, e.g. \`"path": "report.md"\`. Do NOT prepend the directory or its folder name (NOT \`"work/report.md"\`) and do NOT use an absolute path — both are rejected.`,
    '',
    ...resultSection,
    `You are DONE only after the manifest at $${E.MANIFEST_PATH} exists and every file it references exists.`,
    'If you cannot complete the goal, write a failure manifest and stop.',
    'If you hit an authentication / authorization / interactive-confirmation wall (a login prompt, an expired token, a permission you cannot grant yourself): do NOT wait for a human and do NOT keep retrying. Immediately write a failure manifest with an \`error.code\` like "AUTH_REQUIRED" and \`error.retryable: true\`, then stop — a human will unblock and retry this node.',
    '',
  ].join('\n');
}

// ─── Terminal classification + structured-result validation (pure) ──────────

/**
 * Map a node's failure to its terminal kind (the blocked/failed split):
 *   - `blocked`  = semantic/contract failure — retryable via a new attempt
 *   - `failed`   = infrastructure / human-veto / budget — needs intervention
 *
 * `selfReportedFail` marks the special case where the manifest is structurally
 * VALID but declares `status:'fail'` — then the node's own `error.retryable`
 * decides (`false` → failed; `true`/absent → blocked, the agent presumably
 * knows a human can unblock it).
 */
export function classifyTerminal(
  errorClass: V3ErrorClass,
  opts?: { selfReportedFail?: boolean; retryable?: boolean },
): 'blocked' | 'failed' {
  if (opts?.selfReportedFail) return opts.retryable === false ? 'failed' : 'blocked';
  switch (errorClass) {
    case 'manifestInvalid': // agent wrote a bad manifest — a retry may fix it
    case 'resultInvalid':   // result.json missing/violating — same
      return 'blocked';
    case 'workerError':     // process crash = infrastructure
    case 'timeout':         // budget exceeded = infrastructure (for now)
    case 'gateRejected':    // a human said no — retrying won't change that
    case 'cancelled':
      return 'failed';
  }
}

/** Validation outcome for an opt-in `result.json` against its node schema. */
export interface ResultValidation {
  ok: boolean;
  problems?: string[];
}

/**
 * Validate a `result.json` against the node's (already dag-validated) result
 * schema subset.  Top-level types only — see `V3ResultSchema`.  Undeclared
 * extra properties are allowed (JSON-Schema default).
 */
export function validateResult(filePath: string, schema: V3ResultSchema): ResultValidation {
  if (!existsSync(filePath)) return { ok: false, problems: [`result.json not found at ${filePath}`] };
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(filePath, 'utf-8'));
  } catch (err) {
    return { ok: false, problems: [`result.json is not valid JSON: ${err instanceof Error ? err.message : String(err)}`] };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, problems: ['result.json root must be a JSON object'] };
  }
  const obj = parsed as Record<string, unknown>;
  const problems: string[] = [];
  for (const field of schema.required ?? []) {
    if (!(field in obj)) problems.push(`missing required field "${field}"`);
  }
  for (const [name, spec] of Object.entries(schema.properties)) {
    if (!(name in obj)) continue; // absence is only a problem when required
    const v = obj[name];
    const okType =
      spec.type === 'string' ? typeof v === 'string'
      : spec.type === 'number' ? typeof v === 'number' && Number.isFinite(v)
      : spec.type === 'boolean' ? typeof v === 'boolean'
      : spec.type === 'array' ? Array.isArray(v)
      : typeof v === 'object' && v !== null && !Array.isArray(v);
    if (!okType) {
      problems.push(`field "${name}" must be of type ${spec.type}`);
      continue;
    }
    // Enum enforcement (edge-activation design §1.3): a declared vocabulary
    // is part of the contract — an out-of-vocabulary value is `resultInvalid`
    // (blocked, retryable), same as a type violation.
    if (spec.type === 'string' && spec.enum && !spec.enum.includes(v as string)) {
      problems.push(`field "${name}" must be one of [${spec.enum.join(', ')}] (got ${JSON.stringify(v)})`);
    }
  }
  return problems.length > 0 ? { ok: false, problems } : { ok: true };
}

/**
 * Evaluate a (dag-validated) loop exit predicate against the observed result
 * field.  Pure + exported for tests.  Type mismatches simply don't match —
 * validateDag already guarantees the field is declared/required with a
 * compatible type, so a mismatch here means the result was tampered with
 * post-validation; not-matching (→ continue/exhausted) is the safe answer.
 */
export function matchLoopExitWhen(when: V3LoopExitWhen, value: unknown): boolean {
  if (when.equals !== undefined) return value === when.equals;
  if (when.notEquals !== undefined) return value !== when.notEquals;
  if (typeof value !== 'number') return false;
  if (when.gt !== undefined) return value > when.gt;
  if (when.gte !== undefined) return value >= when.gte;
  if (when.lt !== undefined) return value < when.lt;
  if (when.lte !== undefined) return value <= when.lte;
  return false;
}

// ─── Attempt numbering (journal-derived — no hardcoded 001) ──────────────────

const ATTEMPT_NNN_RE = /\/attempts\/(\d{3})$/;

function attemptNumber(attemptId: string): number | undefined {
  const m = ATTEMPT_NNN_RE.exec(attemptId);
  return m ? parseInt(m[1]!, 10) : undefined;
}

/**
 * Compute the attemptId the NEXT dispatch of `nodeId` must use, from the
 * journal: an unconsumed `nodeRetryRequested` reservation wins (retry intent
 * is authoritative for the redrive); otherwise max(seen)+1 — which is 001 for
 * a first dispatch.  Dispatch events are the authority for "seen"; a
 * reservation is consumed by a later `nodeDispatched` with the same number.
 */
export function nextAttemptIdFor(events: StoredEvent[], nodeId: string): string {
  let maxSeen = 0;
  let reserved: number | undefined;
  for (const e of events) {
    if (e.type === 'nodeDispatched' && e.nodeId === nodeId) {
      const n = attemptNumber(e.attemptId);
      if (n === undefined) continue;
      maxSeen = Math.max(maxSeen, n);
      if (reserved === n) reserved = undefined; // reservation consumed
    } else if (e.type === 'nodeRetryRequested' && e.nodeId === nodeId) {
      const n = attemptNumber(e.nextAttemptId);
      if (n === undefined) continue;
      reserved = n;
      maxSeen = Math.max(maxSeen, n);
    }
  }
  const n = reserved ?? maxSeen + 1;
  return `${nodeId}/attempts/${String(n).padStart(3, '0')}`;
}

/** Latest dispatched attemptId for a node (the `previousAttemptId` a retry
 *  entrypoint must reference).  Undefined when the node never dispatched. */
export function latestAttemptIdFor(events: StoredEvent[], nodeId: string): string | undefined {
  let latest: string | undefined;
  for (const e of events) {
    if (e.type === 'nodeDispatched' && e.nodeId === nodeId) latest = e.attemptId;
  }
  return latest;
}

// ─── Injected dependencies + options ────────────────────────────────────────

export interface V3RuntimeDeps {
  /** Spawn an ephemeral worker for one goal node (codex's pool). */
  runNode: RunNode;
  /** Validate a node's manifest after the worker exits (codex's manifest.ts). */
  validateManifest: ValidateManifest;
  /** Freeze a node's bot spawn config at run start.  Given `node.bot` (may be
   *  undefined → the run's default bot), returns the snapshot persisted in the
   *  runDir and threaded through `runNode` (never re-resolved mid-run). */
  resolveBotSnapshot: (botId: string | undefined) => BotSnapshot;
  /** Resolve a humanGate.  Required only if the DAG declares any gate; the
   *  runtime throws if a gate is hit without a handler.  (Wired by
   *  `human-gate.ts` post-milestone.) */
  resolveGate?: (req: {
    nodeId: string;
    prompt: string;
    waitId: string;
    runDir: string;
  }) => Promise<'approved' | 'rejected'>;
}

export interface V3RuntimeOptions {
  /** The run lives in `${baseDir}/${dag.runId}`. */
  baseDir: string;
  /** Gate handling model. `blocking` keeps the CLI/dev y/N path; `suspend`
   *  writes the pending wait and returns `awaitingGate` for a daemon/card layer
   *  to resolve and re-drive from disk. */
  gateMode?: 'blocking' | 'suspend'; // default blocking
  /** Concurrency caps (codex's three-layer cap; conservative defaults). */
  globalConcurrency?: number; // default 4
  perBotConcurrency?: number; // default 1
  perCliConcurrency?: number; // default 2
  cancelSignal?: AbortSignal;
}

export interface V3PendingGate {
  nodeId: string;
  waitId: string;
  prompt: string;
}

export type V3RunOutcome =
  | {
      reason: 'terminal';
      // `blocked` is its OWN status — never collapse it into failed (it is the
      // retryable half of the blocked/failed split).
      runStatus: 'succeeded' | 'failed' | 'blocked';
      failedNodeId?: string;
      blockedNodeId?: string;
      failureReason?: 'allSinksSkipped';
      runDir: string;
    }
  | { reason: 'awaitingGate'; pendingWaits: V3PendingGate[]; runDir: string };

// ─── Main loop ───────────────────────────────────────────────────────────

/**
 * Run a validated DAG to terminal.  Resumable: if `journal.ndjson` already has
 * events (daemon restart), the loop picks up from the materialized state
 * instead of re-running completed nodes.
 */
export async function runWorkflow(
  dag: V3Dag,
  deps: V3RuntimeDeps,
  opts: V3RuntimeOptions,
): Promise<V3RunOutcome> {
  const runDir = join(opts.baseDir, dag.runId);
  mkdirSync(runDir, { recursive: true });
  const journalPath = join(runDir, 'journal.ndjson');
  const statePath = join(runDir, 'STATE');

  const globalCap = opts.globalConcurrency ?? 4;
  const perBotCap = opts.perBotConcurrency ?? 1;
  const perCliCap = opts.perCliConcurrency ?? 2;
  const gateMode = opts.gateMode ?? 'blocking';

  const nodesById = new Map(dag.nodes.map((n) => [n.id, n]));

  // Freeze bot snapshots once, keyed by the node's `bot` field (''=default),
  // and persist for audit / resume.  Re-resolving mid-run would let a drifted
  // bots.json change cliId/model/workingDir under a retry (codex point 1).
  // Loop body nodes are frozen too (a body node inherits the loop's bot when
  // it has none of its own — mirror instanceNodeFor's resolution).
  const botSnapshots = new Map<string, BotSnapshot>();
  const freezeBot = (bot: string | undefined): void => {
    const key = bot ?? '';
    if (!botSnapshots.has(key)) botSnapshots.set(key, deps.resolveBotSnapshot(bot));
  };
  for (const node of dag.nodes) {
    freezeBot(node.bot);
    if (isLoopNode(node)) {
      for (const b of node.body.nodes) freezeBot(b.bot ?? node.bot);
    }
  }

  // CLI-scope guard (老滕 directive): goal-mode rides the native `/goal`
  // command, which only Claude Code / Codex support.  Fail the whole run up
  // front — clearly — rather than spawning a worker on an unsupported CLI that
  // would never understand `/goal`.
  for (const [key, snap] of botSnapshots) {
    if (!isV3SupportedCli(snap.cliId)) {
      throw new Error(
        `v3 runtime: bot "${key || '<default>'}" resolves to CLI "${snap.cliId}", ` +
        `which is not supported by v3 goal-mode (supported: ${V3_SUPPORTED_CLIS.join(', ')})`,
      );
    }
  }

  writeFileSync(
    join(runDir, 'bots.snapshot.json'),
    JSON.stringify(Object.fromEntries(botSnapshots), null, 2),
  );

  // Persist the dag into the runDir so the dashboard projection can read the
  // node graph (depends → edges) and a resume is self-describing.  Deterministic
  // (same runId ⇒ same dag), so re-writing on resume is harmless.
  writeFileSync(join(runDir, 'dag.json'), JSON.stringify(dag, null, 2));

  // First run only: stamp runStarted (idempotent on resume).
  if (readJournal(journalPath).length === 0) {
    appendEvent(journalPath, { type: 'runStarted', runId: dag.runId });
  }

  // In-flight bookkeeping.  Work uses the nodeId as the key; gates use
  // `${nodeId}::gate` so a gated node's work + gate never collide.
  const inFlight = new Map<string, Promise<void>>();
  const botInFlight = new Map<string, number>();
  const cliInFlight = new Map<string, number>();

  while (true) {
    const events = readJournal(journalPath);
    const snap = materialize(events);
    writeState(statePath, snap);
    if (snap.runStatus !== 'running') break;

    const actions = decideNext(dag, snap.nodes, snap.loops, snap.edges);

    // Terminal sweep: write the run terminal event, then re-tick so the top of
    // the loop observes it and breaks (single exit path).
    const terminal = actions.find(
      (a) =>
        a.kind === 'completeRunSucceeded' ||
        a.kind === 'completeRunFailed' ||
        a.kind === 'completeRunBlocked',
    );
    if (terminal) {
      if (terminal.kind === 'completeRunSucceeded') {
        appendEvent(journalPath, { type: 'runSucceeded' });
      } else if (terminal.kind === 'completeRunFailed') {
        appendEvent(journalPath, {
          type: 'runFailed',
          failedNodeId: terminal.failedNodeId,
          reason: terminal.reason,
        });
      } else {
        appendEvent(journalPath, { type: 'runBlocked', blockedNodeId: terminal.blockedNodeId });
      }
      continue;
    }

    // Control sweep: each action is one cheap journal append (no worker
    // involved), applied together and re-ticked — same single-exit shape as
    // the terminal sweep.  Work dispatches in the same action list simply
    // re-emerge next tick.  Edge resolution is deliberately serial/control
    // phase (H8): no inFlight, no concurrency slot, no AbortController.
    const controls = actions.filter(
      (a): a is
        | Extract<V3Action, { loopId: string }>
        | Extract<V3Action, { kind: 'resolveEdge' | 'skipNode' }> =>
        a.kind === 'startLoop' ||
        a.kind === 'startLoopIteration' ||
        a.kind === 'evaluateLoopIteration' ||
        a.kind === 'completeLoop' ||
        a.kind === 'resolveEdge' ||
        a.kind === 'skipNode',
    );
    if (controls.length > 0) {
      const eventsForControl = readJournal(journalPath);
      for (const a of controls) {
        if (a.kind === 'resolveEdge') applyResolveEdge(a, eventsForControl);
        else if (a.kind === 'skipNode') {
          appendEvent(journalPath, {
            type: 'nodeSkipped',
            nodeId: a.nodeId,
            reason: 'triggerRuleUnsatisfied',
            detail: a.detail,
          });
        } else applyLoopControl(a);
      }
      continue;
    }

    // Dispatch the ready set under the three-layer cap.  Anything not started
    // this tick (cap hit) is retried next tick.
    let startedThisTick = 0;
    const aborted = opts.cancelSignal?.aborted === true;
    if (!aborted) {
      for (const a of actions) {
        if (inFlight.size >= globalCap) break;
        if (a.kind === 'dispatchWork') {
          // Loop body instances are synthesized from the body definition; the
          // instance id is theirs alone (attempt dirs, journal events, retry).
          const node = a.loop ? instanceNodeFor(a.loop) : nodesById.get(a.nodeId)!;
          const botKey = node.bot ?? '';
          const botSnap = botSnapshots.get(botKey)!;
          if ((botInFlight.get(botKey) ?? 0) >= perBotCap) continue;
          if ((cliInFlight.get(botSnap.cliId) ?? 0) >= perCliCap) continue;
          startWork(node, botSnap, botKey, events, a.loop, a.omitted);
          startedThisTick++;
        } else if (a.kind === 'dispatchGate') {
          startGate(nodesById.get(a.nodeId)!);
          startedThisTick++;
        }
      }
    }

    if (inFlight.size === 0) {
      if (aborted) break; // cancelled with nothing running → stop
      if (startedThisTick === 0) {
        const pendingWaits = gateMode === 'suspend' ? pendingGateWaits(snap.nodes) : [];
        if (pendingWaits.length > 0) {
          return { reason: 'awaitingGate', pendingWaits, runDir };
        }
        // Not terminal, nothing running, nothing dispatchable — a correct
        // decideNext never gets here; guard against an infinite spin.
        throw new Error('v3 runtime: no progress possible and run is not terminal');
      }
    }

    // Wait for at least one in-flight unit to settle before re-evaluating.
    if (inFlight.size > 0) await Promise.race(inFlight.values());
  }

  const finalSnap = materialize(readJournal(journalPath));
  return {
    reason: 'terminal',
    // Map 1:1 — blocked must NOT collapse into failed.  A 'running' snapshot
    // here means the loop exited via cancel-abort with nothing in flight;
    // report that as failed (the run did not complete).
    runStatus:
      finalSnap.runStatus === 'succeeded' ? 'succeeded'
      : finalSnap.runStatus === 'blocked' ? 'blocked'
      : 'failed',
    failedNodeId: finalSnap.failedNodeId,
    failureReason: finalSnap.failureReason,
    blockedNodeId: finalSnap.blockedNodeId,
    runDir,
  };

  // ─── closures over runDir / journalPath / caps ──────────────────────────

  function startWork(
    node: V3Node,
    botSnap: BotSnapshot,
    botKey: string,
    events: StoredEvent[],
    loopRef?: V3LoopRef,
    omitted?: GoalInputs['omitted'],
  ): void {
    // Attempt number derived from the journal: 001 on first dispatch, the
    // reserved nextAttemptId after a blocked retry (no hardcoded 001 — a retry
    // must not overwrite the previous attempt's logs/manifest/pty).
    const attemptId = nextAttemptIdFor(events, node.id);
    const attemptNNN = attemptId.slice(attemptId.lastIndexOf('/') + 1);
    const attemptDir = join(runDir, node.id, 'attempts', attemptNNN);
    const outputDir = join(attemptDir, 'work');
    mkdirSync(outputDir, { recursive: true });

    const goalPath = join(attemptDir, 'goal.txt');
    const loopCtx = loopRef
      ? {
          loopId: loopRef.loopId,
          iteration: loopRef.iteration,
          maxIterations: (nodesById.get(loopRef.loopId) as V3LoopNode).maxIterations,
        }
      : undefined;
    writeFileSync(goalPath, renderGoalFile(node.goal ?? '', node.resultSchema, loopCtx));

    const inputsPath = join(attemptDir, 'inputs.json');
    writeFileSync(inputsPath, JSON.stringify(buildInputs(node, events, loopRef, omitted), null, 2));

    const manifestPath = join(attemptDir, 'manifest.json');
    const env: Record<string, string> = {
      [GOAL_ENV.GOAL_PATH]: goalPath,
      [GOAL_ENV.INPUTS_PATH]: inputsPath,
      [GOAL_ENV.OUTPUT_DIR]: outputDir,
      [GOAL_ENV.MANIFEST_PATH]: manifestPath,
      [GOAL_ENV.ATTEMPT_DIR]: attemptDir,
      [GOAL_ENV.V3_MARKER]: '1',
    };

    appendEvent(journalPath, { type: 'nodeDispatched', nodeId: node.id, attemptId, loop: loopRef });
    botInFlight.set(botKey, (botInFlight.get(botKey) ?? 0) + 1);
    cliInFlight.set(botSnap.cliId, (cliInFlight.get(botSnap.cliId) ?? 0) + 1);

    // `isGoalNode` is guaranteed by validateDag (host is rejected), but the
    // contract types `runNode` to V3GoalNode, so narrow explicitly.
    if (!isGoalNode(node)) {
      appendEvent(journalPath, {
        type: 'nodeFailed', nodeId: node.id, attemptId,
        errorClass: 'workerError', message: `node "${node.id}" is not a goal node`,
      });
      releaseSlots(botKey, botSnap.cliId);
      return;
    }

    const req: RunNodeRequest = {
      runId: dag.runId,
      attemptId,
      node,
      botSnapshot: botSnap,
      runDir,
      attemptDir,
      inputsPath,
      outputDir,
      env,
      timeoutMs: (node.timeoutSec ?? DEFAULT_NODE_TIMEOUT_SEC) * 1000,
      cancelSignal: opts.cancelSignal,
      // Worker terminal is ready mid-run → stamp nodeSessionReady so the
      // dashboard can attach to the LIVE terminal.  Sync appendEvent (no await
      // on the pool's fire-and-forget ready path — codex note).
      onSessionReady: (info) => {
        // Drop the write `token` — never persist it (codex security review):
        // the dashboard view is read-only and doesn't need write access.
        appendEvent(journalPath, {
          type: 'nodeSessionReady',
          nodeId: node.id,
          attemptId,
          sessionInfo: { sessionId: info.sessionId, webPort: info.webPort },
          ptyLogPath: info.ptyLogPath,
        });
      },
    };

    const p = deps
      .runNode(req)
      .then(async (result) => {
        // Final verdict = process outcome AND manifest validation (codex
        // point 4 — NOT v0.2 final_output semantics).  Always validate the
        // manifest so a clean `status:'fail'` manifest yields a precise
        // root cause instead of an opaque process error (codex's advice).
        const verdict = await deps.validateManifest(result.manifestPath, outputDir);
        const manifestSaysOk = verdict.ok && verdict.manifest?.status === 'ok';

        if (result.status === 'ok' && manifestSaysOk) {
          // Opt-in structured-result contract: the manifest MUST list a
          // `result.json` entry (so it went through the manifest validator's
          // path/hash checks like every other product), and the file must
          // match the node's schema.  A violation BLOCKS (retryable), it does
          // not fail.
          if (node.resultSchema) {
            const entry = verdict.manifest!.files.find((f) => f.path === 'result.json');
            if (!entry) {
              appendEvent(journalPath, {
                type: 'nodeBlocked', nodeId: node.id, attemptId,
                errorClass: 'resultInvalid',
                message: 'node declares resultSchema but its manifest lists no "result.json" file',
              });
              return;
            }
            const res = validateResult(join(outputDir, entry.path), node.resultSchema);
            if (!res.ok) {
              appendEvent(journalPath, {
                type: 'nodeBlocked', nodeId: node.id, attemptId,
                errorClass: 'resultInvalid',
                message: (res.problems ?? ['result.json failed schema validation']).join('; '),
              });
              return;
            }
          }
          appendEvent(journalPath, {
            type: 'nodeSucceeded', nodeId: node.id, attemptId, manifestPath: result.manifestPath,
          });
          return;
        }

        let errorClass: V3ErrorClass;
        let message: string;
        let errorCode: string | undefined;
        let selfReportedFail = false;
        let retryable: boolean | undefined;
        if (!verdict.ok) {
          // Manifest missing / malformed.  If the process itself also failed,
          // the worker crash is the root cause; otherwise it's a bad manifest.
          errorClass = result.status === 'ok' ? 'manifestInvalid' : 'workerError';
          message = (verdict.problems ?? ['manifest missing or invalid']).join('; ');
        } else {
          // Manifest is structurally valid but declares failure (or the
          // process failed despite an ok manifest) — surface the node's own
          // error when present.  A self-reported fail is the agent's "I am
          // blocked, a human can fix this" channel (e.g. AUTH_REQUIRED with
          // retryable:true), so it feeds the blocked/failed split below.
          const m = verdict.manifest!;
          errorClass = 'workerError';
          if (m.status === 'fail' && m.error) {
            message = `${m.error.code}: ${m.error.message}`;
            errorCode = m.error.code;
            if (result.status === 'ok') {
              // Only an intact worker's self-report counts; a crashed process
              // with a leftover fail manifest is still an infrastructure error.
              selfReportedFail = true;
              retryable = m.error.retryable;
            }
          } else {
            message = 'runNode reported process failure';
          }
        }
        const kind = classifyTerminal(errorClass, { selfReportedFail, retryable });
        appendEvent(journalPath, {
          type: kind === 'blocked' ? 'nodeBlocked' : 'nodeFailed',
          nodeId: node.id, attemptId, errorClass, errorCode, message,
        });
      })
      .catch((err: unknown) => {
        appendEvent(journalPath, {
          type: 'nodeFailed', nodeId: node.id, attemptId,
          errorClass: 'workerError', message: err instanceof Error ? err.message : String(err),
        });
      })
      .finally(() => {
        inFlight.delete(node.id);
        releaseSlots(botKey, botSnap.cliId);
      });
    inFlight.set(node.id, p);
  }

  function startGate(node: V3Node): void {
    const waitId = `${node.id}-gate`; // MVP: one gate per node
    const prompt = node.humanGate!.prompt;
    appendEvent(journalPath, { type: 'gateDispatched', nodeId: node.id, waitId });

    if (gateMode === 'suspend') {
      writePendingWait(runDir, { waitId, nodeId: node.id, prompt });
      return;
    }

    if (!deps.resolveGate) {
      throw new Error(
        `v3 runtime: node "${node.id}" has a humanGate but no resolveGate handler was injected`,
      );
    }
    const key = `${node.id}::gate`;
    const p = deps
      .resolveGate({ nodeId: node.id, prompt, waitId, runDir })
      .then((resolution) => {
        appendEvent(journalPath, { type: 'gateResolved', nodeId: node.id, waitId, resolution, by: 'human' });
      })
      .catch(() => {
        // A gate that errors out is treated as rejected (fail-fast); the
        // run-failure root cause is the rejection, recorded on the journal.
        appendEvent(journalPath, { type: 'gateResolved', nodeId: node.id, waitId, resolution: 'rejected', by: 'system' });
      })
      .finally(() => {
        inFlight.delete(key);
      });
    inFlight.set(key, p);
  }

  /** Synthesize the effective node a body instance runs as: the body
   *  definition re-id'd into the iteration namespace, with internal deps /
   *  inputs mapped to instance ids and the bot inherited from the loop. */
  function instanceNodeFor(ref: V3LoopRef): V3Node {
    const loopNode = nodesById.get(ref.loopId) as V3LoopNode;
    const bodyDef = loopNode.body.nodes.find((b) => b.id === ref.bodyNodeId)!;
    return {
      ...bodyDef,
      id: loopInstanceId(ref.loopId, ref.iteration, ref.bodyNodeId),
      bot: bodyDef.bot ?? loopNode.bot,
      depends: bodyDef.depends.map((d) => ({ from: loopInstanceId(ref.loopId, ref.iteration, d.from) })),
      inputs: bodyDef.inputs.map((r) => ({ from: loopInstanceId(ref.loopId, ref.iteration, r.from) })),
    };
  }

  /** Translate one loop-control action into its single journal append. */
  function applyLoopControl(a: Extract<V3Action, { loopId: string }>): void {
    if (a.kind === 'startLoop') {
      appendEvent(journalPath, { type: 'loopStarted', loopId: a.loopId });
      return;
    }
    if (a.kind === 'startLoopIteration') {
      appendEvent(journalPath, { type: 'loopIterationStarted', loopId: a.loopId, iteration: a.iteration });
      return;
    }
    const loopNode = nodesById.get(a.loopId) as V3LoopNode;
    if (a.kind === 'completeLoop') {
      // Seal the loop with a nodeSucceeded on the LOOP id carrying the output
      // projection's manifest — downstream deps gating + buildInputs then
      // treat the loop exactly like any done node.
      const events = readJournal(journalPath);
      const outInstId = loopInstanceId(a.loopId, a.iteration, loopNode.output.from);
      const succ = [...events]
        .reverse()
        .find((e): e is StoredEvent & { type: 'nodeSucceeded' } =>
          e.type === 'nodeSucceeded' && e.nodeId === outInstId);
      if (!succ) {
        // Engine anomaly — the decision said exit but the output instance has
        // no success record.  Fail loudly rather than fabricate a product.
        appendEvent(journalPath, {
          type: 'nodeFailed', nodeId: a.loopId, attemptId: `${a.loopId}/iterations/${String(a.iteration).padStart(3, '0')}`,
          errorClass: 'workerError',
          message: `loop "${a.loopId}" decided exit but output node "${outInstId}" has no nodeSucceeded`,
        });
        return;
      }
      appendEvent(journalPath, {
        type: 'nodeSucceeded', nodeId: a.loopId, attemptId: succ.attemptId, manifestPath: succ.manifestPath,
      });
      return;
    }
    // evaluateLoopIteration: read the exit instance's structured result and
    // record the decision.  Every input here was already validated when the
    // exit node succeeded (resultSchema is mandatory on the exit node), so an
    // unreadable result is an engine anomaly → 'exhausted' (blocks for a
    // human) rather than a silent extra round.
    const events = readJournal(journalPath);
    const exitInstId = loopInstanceId(a.loopId, a.iteration, loopNode.exit.node);
    const succ = [...events]
      .reverse()
      .find((e): e is StoredEvent & { type: 'nodeSucceeded' } =>
        e.type === 'nodeSucceeded' && e.nodeId === exitInstId);
    const key = loopNode.exit.when.path.slice('result.'.length);
    let matched = false;
    let observed = `${loopNode.exit.when.path}=<unreadable>`;
    if (succ) {
      try {
        const manifest = JSON.parse(readFileSync(succ.manifestPath, 'utf-8')) as Manifest;
        const entry = manifest.files.find((f) => f.path === 'result.json');
        if (entry) {
          const result = JSON.parse(
            readFileSync(join(dirname(succ.manifestPath), 'work', entry.path), 'utf-8'),
          ) as Record<string, unknown>;
          observed = `${loopNode.exit.when.path}=${JSON.stringify(result[key])}`;
          matched = matchLoopExitWhen(loopNode.exit.when, result[key]);
        }
      } catch {
        // fall through with matched=false, observed=<unreadable>
      }
    }
    const granted = events.filter(
      (e) => e.type === 'loopIterationGranted' && e.loopId === a.loopId,
    ).length;
    const effectiveMax = loopNode.maxIterations + granted;
    const anomalous = observed.endsWith('<unreadable>');
    const decision = matched ? 'exit' : !anomalous && a.iteration < effectiveMax ? 'continue' : 'exhausted';
    appendEvent(journalPath, {
      type: 'loopIterationDecision', loopId: a.loopId, iteration: a.iteration, decision,
      detail: `${observed} (iteration ${a.iteration}/${effectiveMax})`,
    });
  }

  /** Resolve one conditional edge by reading the source's latest successful
   *  result.json exactly once, then journaling the boolean verdict. */
  function applyResolveEdge(
    a: Extract<V3Action, { kind: 'resolveEdge' }>,
    events: StoredEvent[],
  ): void {
    const target = nodesById.get(a.to);
    const dep = target?.depends.find((d) => d.from === a.from);
    if (!target || !dep?.when) {
      appendEvent(journalPath, {
        type: 'edgeResolved',
        from: a.from,
        to: a.to,
        sourceAttemptId: latestAttemptIdFor(events, a.from) ?? `${a.from}/attempts/unknown`,
        active: false,
        detail: 'edge predicate missing at resolution time',
      });
      return;
    }

    const succ = [...events]
      .reverse()
      .find((e): e is StoredEvent & { type: 'nodeSucceeded' } =>
        e.type === 'nodeSucceeded' && e.nodeId === a.from);
    const sourceAttemptId = succ?.attemptId ?? `${a.from}/attempts/unknown`;
    const key = dep.when.path.slice('result.'.length);
    let active = false;
    let detail = `${dep.when.path}=<unreadable>`;
    if (succ) {
      try {
        const manifest = JSON.parse(readFileSync(succ.manifestPath, 'utf-8')) as Manifest;
        const entry = manifest.files.find((f) => f.path === 'result.json');
        if (entry) {
          const result = JSON.parse(
            readFileSync(join(dirname(succ.manifestPath), 'work', entry.path), 'utf-8'),
          ) as Record<string, unknown>;
          detail = `${dep.when.path}=${JSON.stringify(result[key])}`;
          active = matchLoopExitWhen(dep.when, result[key]);
        }
      } catch {
        // The source's resultSchema should make this unreachable; keep the run
        // progressing deterministically and surface the anomaly in detail.
      }
    }
    appendEvent(journalPath, {
      type: 'edgeResolved',
      from: a.from,
      to: a.to,
      sourceAttemptId,
      active,
      detail,
    });
  }

  function pendingGateWaits(state: Map<string, { status: string }>): V3PendingGate[] {
    const waits: V3PendingGate[] = [];
    for (const node of dag.nodes) {
      if (state.get(node.id)?.status !== 'gateWaiting') continue;
      const prompt = node.humanGate?.prompt;
      if (!prompt) continue;
      waits.push({ nodeId: node.id, waitId: `${node.id}-gate`, prompt });
    }
    return waits;
  }

  function releaseSlots(botKey: string, cliId: string): void {
    botInFlight.set(botKey, Math.max(0, (botInFlight.get(botKey) ?? 1) - 1));
    cliInFlight.set(cliId, Math.max(0, (cliInFlight.get(cliId) ?? 1) - 1));
  }

  /** Resolve a node's upstream products into its `GoalInputs` (absolute paths).
   *  Reads each upstream's already-validated manifest from the latest
   *  `nodeSucceeded` event; the manifest's relative `path` is joined onto the
   *  upstream outputDir (`<manifestDir>/work`) to produce an absolute path the
   *  downstream agent can Read directly.
   *
   *  Loop body instances additionally receive (a) the LOOP's outer inputs —
   *  every body node may read what the loop consumes — and (b) from iteration
   *  2 on, the previous iteration's `feedback` products, labeled
   *  `previous.<bodyId>` so the agent can tell rework context from fresh
   *  upstream input.
   *
   *  `omitted` (edge-activation design §6): declared inputs the engine layer
   *  determined must NOT be injected (edge inactive / source skipped) — they
   *  are excluded from resolution AND surfaced to the agent so the absence
   *  reads as by-design.  Empty/absent → exactly today's behavior. */
  function buildInputs(
    node: V3Node,
    events: StoredEvent[],
    loopRef?: V3LoopRef,
    omitted?: GoalInputs['omitted'],
  ): GoalInputs {
    const inputs: GoalInputs['inputs'] = [];
    const omittedFrom = new Set((omitted ?? []).map((o) => o.from));

    const latestSuccess = (nodeId: string) =>
      [...events]
        .reverse()
        .find((e): e is StoredEvent & { type: 'nodeSucceeded' } =>
          e.type === 'nodeSucceeded' && e.nodeId === nodeId);

    const pushFrom = (
      label: string,
      nodeId: string,
      filter?: (f: Manifest['files'][number]) => boolean,
    ): void => {
      const succ = latestSuccess(nodeId);
      if (!succ) return; // deps are gated upstream — defensive skip
      const upstreamOutputDir = join(dirname(succ.manifestPath), 'work');
      const manifest = JSON.parse(readFileSync(succ.manifestPath, 'utf-8')) as Manifest;
      for (const f of manifest.files) {
        if (filter && !filter(f)) continue;
        inputs.push({
          from: label,
          name: f.name,
          path: join(upstreamOutputDir, f.path),
          kind: f.kind,
          preview: f.preview,
        });
      }
    };

    for (const ref of node.inputs) {
      if (omittedFrom.has(ref.from)) continue; // branch not taken — surfaced via `omitted`
      pushFrom(ref.from, ref.from);
    }

    if (loopRef) {
      const loopNode = nodesById.get(loopRef.loopId) as V3LoopNode;
      // (a) The loop's outer inputs (e.g. `prepare`'s products) flow into
      // every body instance of every iteration.
      for (const ref of loopNode.inputs) pushFrom(ref.from, ref.from);
      // (b) Declared previous-iteration feedback.
      if (loopRef.iteration > 1) {
        for (const fb of loopNode.feedback) {
          const dot = fb.lastIndexOf('.');
          const bodyId = fb.slice(0, dot);
          const kind = fb.slice(dot + 1);
          const prevInstId = loopInstanceId(loopRef.loopId, loopRef.iteration - 1, bodyId);
          const label = `previous.${bodyId}`;
          if (kind === 'manifest') {
            const succ = latestSuccess(prevInstId);
            if (succ) {
              inputs.push({ from: label, name: 'manifest', path: succ.manifestPath, kind: 'json' });
            }
          } else {
            // 'result' → just result.json; 'files' → the whole product set.
            pushFrom(label, prevInstId, kind === 'result' ? (f) => f.path === 'result.json' : undefined);
          }
        }
      }
    }
    // Dedupe by (label, path) — `feedback: ["test.result", "test.files"]`
    // legitimately overlaps on result.json; one entry is enough.
    const seen = new Set<string>();
    return {
      inputs: inputs.filter((i) => {
        const key = JSON.stringify([i.from, i.path]);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      }),
      ...(omitted && omitted.length > 0 ? { omitted } : {}),
    };
  }
}
