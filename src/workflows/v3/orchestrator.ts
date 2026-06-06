/**
 * v3 orchestrator — pure decision layer.
 *
 * Mirrors v0.2's `orchestrator.ts` pattern: a pure function maps the current
 * run state + DAG to a list of action descriptors.  The runtime (`runtime.ts`)
 * owns every side effect — journal/STATE writes, ephemeral-worker dispatch via
 * `runNode`, humanGate card posting — and the per-bot/per-CLI/global
 * concurrency caps.  Keeping the decision pure makes the critical-path
 * semantics testable without spawning workers or touching the filesystem.
 *
 * MVP scope: static DAG, fail-fast.  No loops / decisions / dynamic expand
 * (those are deferred — design Q3/§7).  Gate set is frozen at authoring time;
 * the orchestrator never invents or skips a gate (design Q10).
 */

import { isLoopNode, loopInstanceId, topologicalOrder, type V3Dag, type V3Node } from './dag.js';
import type { V3RunFailureReason } from './journal.js';
import type { V3LoopRef } from './journal.js';

// ─── Run state (materialized from the journal by state.ts) ──────────────────

export type V3NodeStatus =
  | 'pending'      // not yet dispatched (deps may or may not be ready)
  | 'gateWaiting'  // humanGate dispatched, awaiting human resolution
  | 'running'      // worker dispatched, in flight
  | 'done'         // succeeded (work + manifest validated)
  | 'skipped'      // triggerRule unsatisfied; acceptable terminal if a sink still reaches done
  | 'blocked'      // semantic/contract failure — recoverable via retry (new attempt)
  | 'failed';      // infrastructure failure / gate rejected / timed out — needs intervention

export interface V3NodeState {
  status: V3NodeStatus;
  /** True once an approved humanGate cleared this node — so after approval the
   *  next tick dispatches work instead of re-dispatching the gate.  A rejected
   *  gate transitions the node straight to `failed` (set by the runtime), so
   *  this flag only ever records the approved case. */
  gateCleared?: boolean;
}

/** nodeId → state.  A node absent from the map is treated as `pending`. */
export type V3RunState = Map<string, V3NodeState>;

/**
 * Per-loop composite state, folded from the loop lifecycle events.  A loop
 * absent from the map has not started.  The loop's coarse status (running /
 * blocked / done) lives in the regular node-state map under the loop's id;
 * this struct carries what that one enum can't: where the iteration cursor is
 * and what the last decision said.
 */
export interface V3LoopState {
  /** Current iteration, 1-based; 0 between loopStarted and the first
   *  loopIterationStarted. */
  iteration: number;
  /** True once the CURRENT iteration's decision event is recorded (reset by
   *  the next loopIterationStarted). */
  decided: boolean;
  /** The latest decision — drives what the orchestrator does next. */
  lastDecision?: 'exit' | 'continue' | 'exhausted';
  /** Extra iterations granted (each loopIterationGranted adds one); the
   *  effective budget is maxIterations + granted. */
  granted: number;
  /** An appended-but-unconsumed grant (cleared by the next
   *  loopIterationStarted) — the idempotency key for "already granted". */
  pendingGrant: boolean;
}

/** loopId → loop state. */
export type V3LoopRunState = Map<string, V3LoopState>;

export interface V3EdgeState {
  active: boolean;
  sourceAttemptId: string;
}

/** `${from}->${to}` → conditional edge state. */
export type V3EdgeRunState = Map<string, V3EdgeState>;

export interface V3OmittedInput {
  from: string;
  reason: 'edgeInactive' | 'sourceSkipped';
}

// ─── Actions (runtime translates each into journal writes + side effects) ───

export type V3Action =
  /** Read one source result.json once and append edgeResolved. */
  | { kind: 'resolveEdge'; from: string; to: string }
  /** Mark a node skipped because its triggerRule cannot be satisfied. */
  | { kind: 'skipNode'; nodeId: string; detail?: string }
  /** Post the humanGate approval card + persist a `waits/<id>.json` (Q10). */
  | { kind: 'dispatchGate'; nodeId: string }
  /** Spawn an ephemeral worker via `runNode` for this node's goal.  `loop` is
   *  set for body-instance dispatches (the runtime synthesizes the instance
   *  node from the loop's body definition). */
  | { kind: 'dispatchWork'; nodeId: string; loop?: V3LoopRef; omitted?: V3OmittedInput[] }
  // ── loop control (the runtime translates each into ONE journal append) ──
  /** Outer deps of a loop are done → append loopStarted. */
  | { kind: 'startLoop'; loopId: string }
  /** Begin iteration N (first, after a continue-decision, or after a grant). */
  | { kind: 'startLoopIteration'; loopId: string; iteration: number }
  /** Current iteration's body is fully done and undecided → the runtime reads
   *  the exit node's result.json, evaluates exit.when, appends the decision. */
  | { kind: 'evaluateLoopIteration'; loopId: string; iteration: number }
  /** Decision was 'exit' → seal the loop with a nodeSucceeded on the LOOP id
   *  carrying the output projection's manifest (downstream inputs/deps then
   *  treat the loop like any done node — zero special-casing). */
  | { kind: 'completeLoop'; loopId: string; iteration: number }
  /** Terminal: every node done; the run's product is the sink set. */
  | { kind: 'completeRunSucceeded' }
  /** Terminal (fail-fast): a node failed, so the run cannot proceed. */
  | { kind: 'completeRunFailed'; failedNodeId?: string; reason?: V3RunFailureReason }
  /** Terminal-for-now: a node is blocked (contract failure, recoverable).
   *  Halts dispatch like failed, but the run can resume via a retry event. */
  | { kind: 'completeRunBlocked'; blockedNodeId: string };

// ─── Decision function ──────────────────────────────────────────────────────

/**
 * Pure decision: given the current `state`, return every action that can be
 * taken *now*.  The runtime applies concurrency caps by acting on a prefix of
 * the returned dispatch actions and re-invoking on the next tick — this
 * function intentionally returns ALL ready dispatches (it does not throttle).
 *
 * Ordering follows topological order so callers see deps-ready nodes first.
 * Fail-fast: the moment any node is `failed`, the only action is
 * `completeRunFailed` (in-flight peers are torn down by the runtime / cancel).
 * When no dispatch is possible and nothing is pending, the run is complete.
 */
export function decideNext(
  dag: V3Dag,
  state: V3RunState,
  loops: V3LoopRunState = new Map(),
  edges: V3EdgeRunState = new Map(),
): V3Action[] {
  const order = topologicalOrder(dag);
  const nodes = new Map(dag.nodes.map((n) => [n.id, n]));

  // Fail-fast sweep first: a single failed node ends the run.  Pick the
  // earliest in topo order for a deterministic `failedNodeId`.  `failed`
  // (infrastructure, needs intervention) takes priority over `blocked`
  // (contract failure, retryable) when both exist.  Loop body instances are
  // swept too — only the CURRENT iteration can be non-done (a decision is
  // only ever recorded once the whole iteration completed).
  for (const id of order) {
    const node = nodes.get(id)!;
    for (const sweepId of [id, ...currentInstanceIds(node, loops)]) {
      if (st(state, sweepId).status === 'failed') {
        return [{ kind: 'completeRunFailed', failedNodeId: sweepId }];
      }
    }
  }
  for (const id of order) {
    const node = nodes.get(id)!;
    for (const sweepId of [id, ...currentInstanceIds(node, loops)]) {
      if (st(state, sweepId).status === 'blocked') {
        return [{ kind: 'completeRunBlocked', blockedNodeId: sweepId }];
      }
    }
  }

  const actions: V3Action[] = [];
  let pending = 0; // nodes not yet terminal — gates the success sweep

  for (const id of order) {
    const node = nodes.get(id)!;
    const s = st(state, id);

    if (s.status === 'done' || s.status === 'skipped') continue;

    if (isLoopNode(node)) {
      pending++;
      const ls = loops.get(id);
      if (!ls) {
        const readiness = readinessFor(node, state, edges);
        if (readiness.kind === 'wait') continue;
        if (readiness.kind === 'resolveEdge') {
          actions.push({ kind: 'resolveEdge', from: readiness.from, to: id });
          continue;
        }
        if (readiness.kind === 'skip') {
          actions.push({ kind: 'skipNode', nodeId: id, detail: readiness.detail });
          continue;
        }
        // Not started: deps/trigger satisfied, start the composite node.
        actions.push({ kind: 'startLoop', loopId: id });
        continue;
      }
      if (ls.iteration === 0) {
        actions.push({ kind: 'startLoopIteration', loopId: id, iteration: 1 });
        continue;
      }
      if (ls.decided) {
        if (ls.lastDecision === 'exit') {
          actions.push({ kind: 'completeLoop', loopId: id, iteration: ls.iteration });
        } else if (
          ls.lastDecision === 'continue' ||
          // After an exhausted-block, a grant re-opens exactly one round.  An
          // exhausted loop WITHOUT a pending grant never reaches here — its
          // node status is 'blocked' and the sweep above already returned.
          (ls.lastDecision === 'exhausted' && ls.pendingGrant)
        ) {
          actions.push({ kind: 'startLoopIteration', loopId: id, iteration: ls.iteration + 1 });
        }
        continue;
      }
      // Undecided current iteration: schedule the body like a mini-DAG over
      // instance ids; once every body instance is done, ask for the decision.
      const bodyOrder = topologicalOrder({ runId: id, nodes: node.body.nodes });
      const bodyById = new Map(node.body.nodes.map((b) => [b.id, b]));
      let allDone = true;
      for (const bodyId of bodyOrder) {
        const instId = loopInstanceId(id, ls.iteration, bodyId);
        const bs = st(state, instId);
        if (bs.status === 'done') continue;
        allDone = false;
        if (bs.status === 'running' || bs.status === 'gateWaiting') continue;
        const bodyDef = bodyById.get(bodyId)!;
        const depsOk = bodyDef.depends.every(
          (dep) => st(state, loopInstanceId(id, ls.iteration, dep.from)).status === 'done',
        );
        if (!depsOk) continue;
        actions.push({
          kind: 'dispatchWork',
          nodeId: instId,
          loop: { loopId: id, iteration: ls.iteration, bodyNodeId: bodyId },
        });
      }
      if (allDone) {
        actions.push({ kind: 'evaluateLoopIteration', loopId: id, iteration: ls.iteration });
      }
      continue;
    }

    // In-flight: a dispatched worker or an open gate. Nothing to emit; the
    // node is still pending completion.
    if (s.status === 'running' || s.status === 'gateWaiting') {
      pending++;
      continue;
    }

    // s.status === 'pending'
    pending++;
    const readiness = readinessFor(node, state, edges);
    if (readiness.kind === 'wait') continue;
    if (readiness.kind === 'resolveEdge') {
      actions.push({ kind: 'resolveEdge', from: readiness.from, to: id });
      continue;
    }
    if (readiness.kind === 'skip') {
      actions.push({ kind: 'skipNode', nodeId: id, detail: readiness.detail });
      continue;
    }

    if (node.humanGate && !s.gateCleared) {
      actions.push({ kind: 'dispatchGate', nodeId: id });
      continue;
    }
    actions.push(
      readiness.omitted
        ? { kind: 'dispatchWork', nodeId: id, omitted: readiness.omitted }
        : { kind: 'dispatchWork', nodeId: id },
    );
  }

  if (actions.length === 0 && pending === 0) {
    const sinks = findSinks(dag);
    if (sinks.some((id) => st(state, id).status === 'done')) {
      return [{ kind: 'completeRunSucceeded' }];
    }
    return [{ kind: 'completeRunFailed', reason: 'allSinksSkipped' }];
  }
  return actions;
}

type EdgeActivity =
  | { kind: 'active'; from: string }
  | { kind: 'inactive'; from: string; reason: 'edgeInactive' | 'sourceSkipped' }
  | { kind: 'unresolved'; from: string }
  | { kind: 'unsettled' };

type NodeReadiness =
  | { kind: 'ready'; omitted?: V3OmittedInput[] }
  | { kind: 'skip'; detail: string }
  | { kind: 'resolveEdge'; from: string }
  | { kind: 'wait' };

function readinessFor(node: V3Node, state: V3RunState, edges: V3EdgeRunState): NodeReadiness {
  if (node.depends.length === 0) return { kind: 'ready' };
  const activities = node.depends.map((dep): EdgeActivity => {
    const source = st(state, dep.from);
    if (source.status === 'done') {
      if (!dep.when) return { kind: 'active', from: dep.from };
      const edge = edges.get(edgeKey(dep.from, node.id));
      if (!edge) return { kind: 'unresolved', from: dep.from };
      return edge.active
        ? { kind: 'active', from: dep.from }
        : { kind: 'inactive', from: dep.from, reason: 'edgeInactive' };
    }
    if (source.status === 'skipped') return { kind: 'inactive', from: dep.from, reason: 'sourceSkipped' };
    return { kind: 'unsettled' };
  });

  if (activities.some((a) => a.kind === 'unsettled')) return { kind: 'wait' };
  const unresolved = activities.find((a): a is Extract<EdgeActivity, { kind: 'unresolved' }> =>
    a.kind === 'unresolved');
  if (unresolved) return { kind: 'resolveEdge', from: unresolved.from };

  const active = activities.filter((a) => a.kind === 'active').length;
  const triggerRule = node.triggerRule ?? 'all_success';
  const required =
    triggerRule === 'all_success' ? node.depends.length
    : triggerRule === 'one_success' ? 1
    : triggerRule.quorum;

  if (active >= required) {
    const omitted = activities
      .filter((a): a is Extract<EdgeActivity, { kind: 'inactive' }> => a.kind === 'inactive')
      .map((a) => ({ from: a.from, reason: a.reason }));
    return omitted.length > 0 ? { kind: 'ready', omitted } : { kind: 'ready' };
  }

  const detail = activities
    .map((a, idx) => {
      const dep = node.depends[idx]!;
      if (a.kind === 'active') return `${dep.from}:active`;
      if (a.kind === 'inactive') return `${dep.from}:inactive(${a.reason})`;
      return `${dep.from}:${a.kind}`;
    })
    .join(', ');
  return { kind: 'skip', detail: `triggerRule=${JSON.stringify(triggerRule)} unsatisfied; ${detail}` };
}

function edgeKey(from: string, to: string): string {
  return `${from}->${to}`;
}

/** The CURRENT iteration's expanded instance ids of a loop node (empty for
 *  plain nodes / unstarted loops) — the sweep surface for failed/blocked. */
function currentInstanceIds(node: V3Node, loops: V3LoopRunState): string[] {
  if (!isLoopNode(node)) return [];
  const ls = loops.get(node.id);
  if (!ls || ls.iteration === 0) return [];
  return node.body.nodes.map((b) => loopInstanceId(node.id, ls.iteration, b.id));
}

/** Sink nodes — no other node depends on them.  Their products are the run's
 *  output.  Pure helper for the runtime's success path. */
export function findSinks(dag: V3Dag): string[] {
  const referenced = new Set<string>();
  for (const node of dag.nodes) for (const dep of node.depends) referenced.add(dep.from);
  return dag.nodes.map((n) => n.id).filter((id) => !referenced.has(id));
}

function st(state: V3RunState, id: string): V3NodeState {
  return state.get(id) ?? { status: 'pending' };
}
