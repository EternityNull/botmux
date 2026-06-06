/**
 * v3 state — materialize the journal into a run snapshot + STATE checkpoint.
 *
 * Two-layer truth (codex v1-review blocker #1):
 *   - `journal.ndjson` (journal.ts) = append-only audit truth
 *   - `STATE` (this file) = materialized checkpoint, derived by replaying the
 *     journal.  Cheap to grep, fast for the dashboard / a human to read, and
 *     re-derivable at any time so it never becomes an independent source of
 *     truth that can drift from the journal.
 *
 * Resume after a daemon restart replays the journal through `materialize`
 * (correctness first); the on-disk STATE file is an observability artifact and
 * a fast-path the runtime refreshes after every event.
 */

import { writeFileSync, renameSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import type { StoredEvent, V3RunFailureReason } from './journal.js';
import type {
  V3EdgeRunState,
  V3LoopRunState,
  V3LoopState,
  V3NodeState,
  V3NodeStatus,
  V3RunState,
} from './orchestrator.js';

export type V3RunStatus = 'running' | 'succeeded' | 'failed' | 'blocked';

export interface V3RunSnapshot {
  runStatus: V3RunStatus;
  /** Set once `runFailed` is observed — the node that triggered fail-fast. */
  failedNodeId?: string;
  /** Workflow-level failure reason; ordinary node failures keep using
   *  `failedNodeId`. */
  failureReason?: V3RunFailureReason;
  /** Set once `runBlocked` is observed — the blocked node (cleared back to
   *  running by a subsequent `nodeRetryRequested` on replay). */
  blockedNodeId?: string;
  /** nodeId → current node state (the input `decideNext` consumes). */
  nodes: V3RunState;
  /** nodeId → the attemptId of its latest dispatch — or, after a
   *  `nodeRetryRequested`, the reserved `nextAttemptId` the retry will use. */
  attempts: Map<string, string>;
  /** loopId → composite loop state (iteration cursor / decision / grants). */
  loops: V3LoopRunState;
  /** `${from}->${to}` → conditional edge verdicts folded from edgeResolved. */
  edges: V3EdgeRunState;
}

// ─── Materialize (replay) ────────────────────────────────────────────────

/**
 * Fold the journal into a snapshot.  Pure — same events always yield the same
 * snapshot, which is what makes STATE safe to throw away and re-derive.
 *
 * Node status transitions:
 *   nodeDispatched     → running   (preserve gateCleared across the transition)
 *   nodeSucceeded      → done
 *   nodeFailed         → failed
 *   nodeBlocked        → blocked
 *   nodeRetryRequested → pending  (+ attempt reservation; run blocked→running)
 *   gateDispatched     → gateWaiting
 *   gateResolved/ok    → pending + gateCleared (next tick dispatches work)
 *   gateResolved/no    → failed
 *   edgeResolved       → edges[first `${from}->${to}`] (first-wins)
 *   nodeSkipped        → skipped
 */
export function materialize(events: StoredEvent[]): V3RunSnapshot {
  const nodes: V3RunState = new Map();
  const attempts = new Map<string, string>();
  const loops: V3LoopRunState = new Map();
  const edges: V3EdgeRunState = new Map();
  let runStatus: V3RunStatus = 'running';
  let failedNodeId: string | undefined;
  let failureReason: V3RunFailureReason | undefined;
  let blockedNodeId: string | undefined;

  const set = (id: string, status: V3NodeStatus, gateCleared?: boolean): void => {
    const prev = nodes.get(id);
    const next: V3NodeState = { status };
    // Carry an approved-gate flag forward unless this transition sets it.
    const carried = gateCleared ?? prev?.gateCleared;
    if (carried) next.gateCleared = true;
    nodes.set(id, next);
  };

  const loop = (id: string): V3LoopState => {
    let ls = loops.get(id);
    if (!ls) {
      ls = { iteration: 0, decided: false, granted: 0, pendingGrant: false };
      loops.set(id, ls);
    }
    return ls;
  };

  for (const e of events) {
    switch (e.type) {
      case 'runStarted':
        runStatus = 'running';
        break;
      case 'nodeDispatched':
        set(e.nodeId, 'running');
        attempts.set(e.nodeId, e.attemptId);
        break;
      case 'nodeSucceeded':
        set(e.nodeId, 'done');
        break;
      case 'nodeFailed':
        set(e.nodeId, 'failed');
        break;
      case 'nodeBlocked':
        set(e.nodeId, 'blocked');
        break;
      case 'nodeRetryRequested':
        // Replay-correct unblock (codex blocker #1 of the blocked design):
        // the retry is a journal event, so a fresh materialize() yields
        // pending — NOT a memory-only patch that evaporates on next replay.
        set(e.nodeId, 'pending');
        attempts.set(e.nodeId, e.nextAttemptId);
        if (runStatus === 'blocked') {
          runStatus = 'running';
          blockedNodeId = undefined;
        }
        break;
      case 'gateDispatched':
        set(e.nodeId, 'gateWaiting');
        break;
      case 'gateResolved':
        if (e.resolution === 'approved') set(e.nodeId, 'pending', true);
        else set(e.nodeId, 'failed');
        break;
      case 'edgeResolved': {
        const key = `${e.from}->${e.to}`;
        if (!edges.has(key)) {
          edges.set(key, { active: e.active, sourceAttemptId: e.sourceAttemptId });
        }
        break;
      }
      case 'nodeSkipped':
        set(e.nodeId, 'skipped');
        break;
      case 'runSucceeded':
        runStatus = 'succeeded';
        break;
      case 'runFailed':
        runStatus = 'failed';
        failedNodeId = e.failedNodeId;
        failureReason = e.reason;
        break;
      case 'runBlocked':
        runStatus = 'blocked';
        blockedNodeId = e.blockedNodeId;
        break;
      // ── structured loop lifecycle ──
      case 'loopStarted':
        loop(e.loopId);
        set(e.loopId, 'running');
        break;
      case 'loopIterationStarted': {
        const ls = loop(e.loopId);
        ls.iteration = e.iteration;
        ls.decided = false;
        ls.pendingGrant = false; // an unconsumed grant is consumed here
        set(e.loopId, 'running');
        break;
      }
      case 'loopIterationDecision': {
        const ls = loop(e.loopId);
        ls.decided = true;
        ls.lastDecision = e.decision;
        // 'exhausted' blocks the LOOP node (the run-level runBlocked is
        // appended by the orchestrator's blocked sweep on the next tick).
        // 'exit' keeps the node 'running' until the orchestrator seals it
        // with a nodeSucceeded on the loop id (output projection manifest).
        if (e.decision === 'exhausted') set(e.loopId, 'blocked');
        break;
      }
      case 'loopIterationGranted': {
        // Replay-correct unblock — the loop analogue of nodeRetryRequested:
        // a journal event, so a fresh materialize() yields a running loop.
        const ls = loop(e.loopId);
        ls.granted += 1;
        ls.pendingGrant = true;
        set(e.loopId, 'running');
        if (runStatus === 'blocked') {
          runStatus = 'running';
          blockedNodeId = undefined;
        }
        break;
      }
    }
  }

  return { runStatus, failedNodeId, failureReason, blockedNodeId, nodes, attempts, loops, edges };
}

// ─── STATE checkpoint (atomic write / read) ────────────────────────────────

/** On-disk shape of the STATE file — Maps flattened to plain objects so it's
 *  human-readable and `jq`-able. */
interface StateFile {
  runStatus: V3RunStatus;
  failedNodeId?: string;
  failureReason?: V3RunFailureReason;
  blockedNodeId?: string;
  nodes: Record<string, V3NodeState>;
  attempts: Record<string, string>;
  loops?: Record<string, V3LoopState>;
  edges?: Record<string, { active: boolean; sourceAttemptId: string }>;
  updatedAt: number;
}

/**
 * Write the snapshot to `statePath` atomically: write a sibling `.tmp` then
 * `rename` over the target (rename is atomic on the same filesystem), so a
 * crash mid-write never leaves a half-written STATE — readers see either the
 * old or the new file, never a torn one.
 */
export function writeState(statePath: string, snap: V3RunSnapshot): void {
  const dir = dirname(statePath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const file: StateFile = {
    runStatus: snap.runStatus,
    failedNodeId: snap.failedNodeId,
    failureReason: snap.failureReason,
    blockedNodeId: snap.blockedNodeId,
    nodes: Object.fromEntries(snap.nodes),
    attempts: Object.fromEntries(snap.attempts),
    loops: snap.loops.size > 0 ? Object.fromEntries(snap.loops) : undefined,
    edges: snap.edges.size > 0 ? Object.fromEntries(snap.edges) : undefined,
    updatedAt: Date.now(),
  };
  const tmp = `${statePath}.tmp`;
  writeFileSync(tmp, JSON.stringify(file, null, 2));
  renameSync(tmp, statePath);
}

/** Read a previously-written STATE checkpoint back into a snapshot.  Returns
 *  `undefined` when absent.  Resume normally prefers `materialize(readJournal)`
 *  (the journal is authoritative); this is for fast reads / observability. */
export function readState(statePath: string): V3RunSnapshot | undefined {
  if (!existsSync(statePath)) return undefined;
  const file = JSON.parse(readFileSync(statePath, 'utf-8')) as StateFile;
  return {
    runStatus: file.runStatus,
    failedNodeId: file.failedNodeId,
    failureReason: file.failureReason,
    blockedNodeId: file.blockedNodeId,
    nodes: new Map(Object.entries(file.nodes)),
    attempts: new Map(Object.entries(file.attempts)),
    loops: new Map(Object.entries(file.loops ?? {})),
    edges: new Map(Object.entries(file.edges ?? {})),
  };
}
