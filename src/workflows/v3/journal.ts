/**
 * v3 journal — append-only event stream (the run's audit truth).
 *
 * Codex's v1-review blocker #1: v3 is NOT a "only-mutable-STATE" recoverable
 * system.  `journal.ndjson` is the append-only source of audit truth (one JSON
 * object per line, `ts`-stamped) from which `state.ts` materializes the STATE
 * checkpoint.  Concurrency / retry / gate / cancel / failure-root-cause all
 * leave an ordered trail here.
 *
 * Append-only + line-oriented = crash-tolerant by construction: a torn final
 * line (process died mid-write) is skipped on read; everything before it is
 * intact.  No locking needed for the journal itself — appends are serialized
 * by the single runtime loop (the per-node LOCK guards worker dispatch, not
 * the journal).
 */

import { appendFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

// ─── Event taxonomy ─────────────────────────────────────────────────────────

/** Why a node failed — drives fail-fast root-cause reporting + retry policy
 *  (`classifyTerminal` maps each class to `blocked` or `failed`).
 *  `gateRejected` / `cancelled` are user-driven; the rest are faults. */
export type V3ErrorClass =
  | 'workerError'      // ephemeral worker crashed / non-zero exit
  | 'manifestInvalid'  // worker exited ok but manifest failed validation
  | 'resultInvalid'    // result.json missing / unparsable / fails the node's resultSchema
  | 'timeout'          // node exceeded its wall-clock budget
  | 'gateRejected'     // human rejected the approval gate
  | 'cancelled';       // run cancelled out from under the node

export type V3RunFailureReason = 'allSinksSkipped';

/**
 * The MVP event union.  Static DAG + fail-fast, so the lifecycle is small:
 * run boundaries, per-node dispatch/settle, and gate dispatch/resolve.  Retry
 * (`attempts/NNN`) is modeled by `attemptId` on dispatch/settle events — a new
 * attempt is just another `nodeDispatched` with a fresh `attemptId`.
 */
/**
 * Structured loop reference carried on body-instance dispatches.  The expanded
 * nodeId (`repairLoop.i001.code`) is an OPAQUE key — projections/dashboards
 * group by THIS field and never parse the id string (body ids may themselves
 * contain dots).
 */
export interface V3LoopRef {
  loopId: string;
  iteration: number;
  bodyNodeId: string;
}

export type V3Event =
  | { type: 'runStarted'; runId: string }
  | { type: 'nodeDispatched'; nodeId: string; attemptId: string; loop?: V3LoopRef }
  // Written when the node's worker web terminal is ready (mid-run) so the
  // dashboard can attach to an in-flight node's LIVE terminal instead of waiting
  // for completion.  Kept even if the node later fails (terminal info survives).
  // NOTE: deliberately NO web-terminal `token` here — it's a WRITE token, and
  // read-only "watch the subagent" doesn't need it; persisting it would turn
  // write access into a durable artifact (codex security review 2026-06-02).
  | { type: 'nodeSessionReady'; nodeId: string; attemptId: string; sessionInfo: { sessionId: string; webPort?: number }; ptyLogPath?: string }
  | { type: 'nodeSucceeded'; nodeId: string; attemptId: string; manifestPath: string }
  // `errorCode` carries the node's self-reported `manifest.error.code` (e.g.
  // AUTH_REQUIRED) so dashboards see the real cause, not just the coarse class.
  | { type: 'nodeFailed'; nodeId: string; attemptId: string; errorClass: V3ErrorClass; errorCode?: string; message?: string }
  // Semantic/contract failure — recoverable.  classifyTerminal(errorClass,
  // retryable) decides blocked-vs-failed; blocked halts the run like failed
  // but is retryable via `nodeRetryRequested` (journal event, NOT in-memory).
  | { type: 'nodeBlocked'; nodeId: string; attemptId: string; errorClass: V3ErrorClass; errorCode?: string; message?: string }
  // Retry intent for a blocked node.  Appended by the retry entrypoint (CLI /
  // daemon card click).  materialize() resets the node to pending and records
  // `nextAttemptId` as the attempt reservation; the orchestrator then re-
  // dispatches naturally.  `previousErrorClass`/`previousErrorCode` are copied
  // from the blocked event purely for audit (grep the journal and see WHY this
  // retry happened) — they do not participate in the state machine.
  | {
      type: 'nodeRetryRequested';
      nodeId: string;
      previousAttemptId: string;
      nextAttemptId: string;
      reason: 'blockedRetry';
      previousErrorClass?: V3ErrorClass;
      previousErrorCode?: string;
    }
  | { type: 'gateDispatched'; nodeId: string; waitId: string }
  | { type: 'gateResolved'; nodeId: string; waitId: string; resolution: 'approved' | 'rejected'; by: string }
  // ── edge activation lifecycle ──
  // Conditional edge predicates read a source node's validated result.json
  // exactly once, then persist the verdict here.  Replay / dashboards /
  // decideNext consume this event and never re-read result.json.
  | {
      type: 'edgeResolved';
      from: string;
      to: string;
      sourceAttemptId: string;
      active: boolean;
      detail?: string;
    }
  // A node whose triggerRule cannot be satisfied is a terminal skipped node.
  // It is not a failure and does not trigger fail-fast; its outgoing edges
  // become inactive by pure derivation from node state + static DAG.
  | { type: 'nodeSkipped'; nodeId: string; reason: 'triggerRuleUnsatisfied'; detail?: string }
  // ── structured loop lifecycle ──
  // A loop is a composite node: its body expands into REAL journal-level nodes
  // per iteration (instance ids via loopInstanceId), so dispatch/settle/retry
  // reuse the node events above.  These events only mark the loop's own state
  // machine: started → (iterationStarted → [body runs] → iterationDecision)* →
  // done (decision 'exit', sealed by a nodeSucceeded on the LOOP id carrying
  // the output projection's manifest) | blocked (decision 'exhausted').
  | { type: 'loopStarted'; loopId: string }
  | { type: 'loopIterationStarted'; loopId: string; iteration: number }
  // The runtime's verdict after an iteration's body completed: evaluate
  // exit.when on the exit node's result.json.  `exit` ends the loop, `continue`
  // schedules the next iteration, `exhausted` (predicate unmatched at the
  // iteration budget) blocks the loop — recoverable via loopIterationGranted.
  | {
      type: 'loopIterationDecision';
      loopId: string;
      iteration: number;
      decision: 'exit' | 'continue' | 'exhausted';
      /** Human-readable why (e.g. `result.passed=false (2/3 iterations used)`). */
      detail?: string;
    }
  // A human granted ONE extra iteration to an exhausted-blocked loop (the
  // loop analogue of nodeRetryRequested — journal event, never an in-memory
  // patch).  Consumed by the next loopIterationStarted.
  | { type: 'loopIterationGranted'; loopId: string; fromIteration: number; by?: string }
  | { type: 'runSucceeded' }
  | { type: 'runFailed'; failedNodeId?: string; reason?: V3RunFailureReason }
  // Terminal-for-now: every non-done path is blocked (recoverable).  A retry
  // clears it back to running on replay (see state.ts materialize).
  | { type: 'runBlocked'; blockedNodeId: string };

/** A journal line: the event flattened with its append timestamp (flattened —
 *  not `{ts, event}` — so `grep nodeFailed journal.ndjson` just works). */
export type StoredEvent = V3Event & { ts: number };

// ─── Append ─────────────────────────────────────────────────────────────────

/**
 * Append one event as a single NDJSON line.  Stamps `ts` (epoch ms) at write
 * time.  Creates the parent directory if missing so the very first
 * `runStarted` doesn't require the caller to pre-create the runDir.
 *
 * Synchronous on purpose: the runtime loop must observe its own writes in
 * order, and the journal is the linearization point — an async append would
 * open a window where `decideNext` runs against stale state.
 */
export function appendEvent(journalPath: string, event: V3Event): StoredEvent {
  const stored: StoredEvent = { ts: Date.now(), ...event };
  const dir = dirname(journalPath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  appendFileSync(journalPath, JSON.stringify(stored) + '\n');
  return stored;
}

// ─── Read / replay ────────────────────────────────────────────────────────

/**
 * Read every event in append order.  Tolerates a torn final line (crash
 * mid-append) by skipping any line that fails to parse — the journal stays
 * usable for replay after an unclean shutdown.  Returns `[]` if the file does
 * not exist yet (a run that never started).
 */
export function readJournal(journalPath: string): StoredEvent[] {
  if (!existsSync(journalPath)) return [];
  const raw = readFileSync(journalPath, 'utf-8');
  const out: StoredEvent[] = [];
  for (const line of raw.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      out.push(JSON.parse(trimmed) as StoredEvent);
    } catch {
      // Torn / partial final line from an interrupted append — skip it.
      // (Only the last line can legitimately be partial; a mid-file parse
      //  failure would indicate real corruption, but skipping is still the
      //  safe choice — replay proceeds with the events it can read.)
    }
  }
  return out;
}
