/**
 * Federation spoke endpoints (join-remote / remote-roster / leave-remote) with a
 * mock fetcher standing in for the hub.
 * Run: pnpm vitest run test/federation-spoke-api.test.ts
 */
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it, expect, beforeEach, vi } from 'vitest';

const state = vi.hoisted(() => ({ dataDir: '' }));
vi.mock('../src/config.js', () => ({ config: {
  session: { get dataDir() { return state.dataDir; } },
  dashboard: { externalHost: 'localhost', port: 7891 },
} }));

import { handleFederationSpokeApi } from '../src/dashboard/federation-spoke-api.js';
import { listMemberships } from '../src/services/federation-membership-store.js';
import { getDeploymentIdentity } from '../src/services/deployment-identity.js';
import { consumeInvite } from '../src/services/invite-store.js';
import { DEFAULT_TEAM_ID } from '../src/services/team-store.js';
import { registerDeployment } from '../src/services/federation-store.js';

let dataDir: string;
beforeEach(() => { dataDir = mkdtempSync(join(tmpdir(), 'botmux-spoke-')); state.dataDir = dataDir; });

function writeBots(entries: any[]) { writeFileSync(join(dataDir, 'bots-info.json'), JSON.stringify(entries)); }
function makeReq(method: string, path: string, body?: unknown): any {
  const req: any = { method, url: path, headers: {} };
  req[Symbol.asyncIterator] = async function* () { if (body !== undefined) yield Buffer.from(JSON.stringify(body)); };
  return req;
}
function makeRes(): any {
  const res: any = { statusCode: 0, _headers: {}, _body: '' };
  res.setHeader = (k: string, v: any) => { res._headers[k.toLowerCase()] = v; };
  res.writeHead = (s: number, h?: any) => { res.statusCode = s; if (h) Object.assign(res._headers, h); };
  res.end = (b?: string) => { res._body = b ?? ''; };
  return res;
}
const json = (res: any) => JSON.parse(res._body);
const jsonResp = (status: number, body: any) => ({ ok: status >= 200 && status < 300, status, json: async () => body } as any);

describe('handleFederationSpokeApi', () => {
  it('local: GET /api/team/local returns this deployment + own roster + suggested hub url', async () => {
    writeBots([{ larkAppId: 'cli_me1', botOpenId: null, botName: '我的Bot', cliId: 'claude' }]);
    const res = makeRes();
    const handled = await handleFederationSpokeApi(makeReq('GET', '/api/team/local'), res, new URL('http://x/api/team/local'), { dataDir });
    expect(handled).toBe(true);
    expect(res.statusCode).toBe(200);
    const b = json(res);
    expect(b.deployment.deploymentId).toMatch(/^dep_/);
    expect(b.suggestedHubUrl).toBe('http://localhost:7891');
    expect(b.bots.map((x: any) => x.larkAppId)).toEqual(['cli_me1']);
    expect(b.bots[0].deployment.local).toBe(true);
  });

  it('local: POST /api/team/local-invite mints a usable invite for my default team', async () => {
    writeBots([]);
    const res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/local-invite'), res, new URL('http://x/api/team/local-invite'), { dataDir });
    expect(res.statusCode).toBe(200);
    const code = json(res).code;
    expect(code).toBeTruthy();
    // the minted code admits to the default team
    expect(consumeInvite(dataDir, code)).toEqual({ ok: true, teamId: DEFAULT_TEAM_ID });
  });

  it('local: POST /api/team/rename-deployment changes the name (id stable)', async () => {
    writeBots([]);
    const before = getDeploymentIdentity(dataDir);
    const res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/rename-deployment', { name: '申晗的部署' }), res, new URL('http://x/api/team/rename-deployment'), { dataDir });
    expect(res.statusCode).toBe(200);
    expect(json(res).deployment).toMatchObject({ deploymentId: before.deploymentId, name: '申晗的部署' });
  });

  it('federated-group: validates against aggregated roster, delegates local+federated app_ids to createTeamGroup', async () => {
    writeBots([{ larkAppId: 'cli_local', botOpenId: null, botName: '本地Bot', cliId: 'claude' }]);
    registerDeployment(dataDir, DEFAULT_TEAM_ID, { deploymentId: 'dep_r', name: '远端', bots: [{ larkAppId: 'cli_remote', botName: '远端Bot', cliId: 'codex' }] });
    let captured: any = null;
    const createTeamGroup = vi.fn(async (args: any) => { captured = args; return { ok: true, chatId: 'oc_x', shareLink: 'https://x/join', invalidBotIds: [] }; });
    const url = new URL('http://x/api/team/federated-group');
    // valid local + federated selection → delegated
    let res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/federated-group', { name: '排障', larkAppIds: ['cli_local', 'cli_remote'] }), res, url, { dataDir, createTeamGroup: createTeamGroup as any });
    expect(res.statusCode).toBe(200);
    expect(json(res).chatId).toBe('oc_x');
    expect(captured.larkAppIds.sort()).toEqual(['cli_local', 'cli_remote']);
    // unknown bot (not on aggregated roster) → 400, never delegated
    res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/federated-group', { larkAppIds: ['cli_ghost'] }), res, url, { dataDir, createTeamGroup: createTeamGroup as any });
    expect(res.statusCode).toBe(400);
    expect(json(res).error).toBe('unknown_bot');
    // empty selection → 400
    res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/federated-group', { larkAppIds: [] }), res, url, { dataDir, createTeamGroup: createTeamGroup as any });
    expect(json(res).error).toBe('no_bots_selected');
    // no creator dep → 501
    res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/federated-group', { larkAppIds: ['cli_local'] }), res, url, { dataDir });
    expect(res.statusCode).toBe(501);
  });

  it('join-remote: posts local bots to the hub and stores the membership', async () => {
    writeBots([{ larkAppId: 'cli_me1', botOpenId: null, botName: '我的Bot', cliId: 'claude' }]);
    let captured: any = null;
    const fetcher = vi.fn(async (u: any, init: any) => {
      captured = { url: String(u), body: JSON.parse(init.body) };
      return jsonResp(200, { ok: true, teamId: 'default', teamName: '研发团队', syncToken: 'TOK123' });
    });
    const res = makeRes();
    const handled = await handleFederationSpokeApi(
      makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://hub:7891/', inviteCode: 'INV' }),
      res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: fetcher as any },
    );
    expect(handled).toBe(true);
    expect(res.statusCode).toBe(200);
    // called the hub join endpoint with our deployment + bots
    expect(captured.url).toBe('http://hub:7891/api/federation/join'); // trailing slash normalized
    expect(captured.body.inviteCode).toBe('INV');
    expect(captured.body.deployment.bots.map((b: any) => b.larkAppId)).toEqual(['cli_me1']);
    expect(captured.body.deployment.deploymentId).toMatch(/^dep_/);
    // membership stored
    const ms = listMemberships(dataDir);
    expect(ms.length).toBe(1);
    expect(ms[0]).toMatchObject({ hubUrl: 'http://hub:7891', teamId: 'default', teamName: '研发团队', syncToken: 'TOK123' });
  });

  it('join-remote: surfaces hub rejection (403 invite) without storing membership', async () => {
    writeBots([]);
    const fetcher = vi.fn(async () => jsonResp(403, { ok: false, error: 'invite_used' }));
    const res = makeRes();
    await handleFederationSpokeApi(
      makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://hub:7891', inviteCode: 'INV' }),
      res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: fetcher as any },
    );
    expect(res.statusCode).toBe(403);
    expect(json(res).error).toBe('invite_used');
    expect(listMemberships(dataDir).length).toBe(0);
  });

  it('join-remote: hub unreachable → 502 hub_unreachable', async () => {
    writeBots([]);
    const fetcher = vi.fn(async () => { throw new Error('ECONNREFUSED'); });
    const res = makeRes();
    await handleFederationSpokeApi(
      makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://hub:7891', inviteCode: 'INV' }),
      res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: fetcher as any },
    );
    expect(res.statusCode).toBe(502);
    expect(json(res).error).toBe('hub_unreachable');
  });

  it('join-remote: rejects bad hub url and missing code', async () => {
    let res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/join-remote', { hubUrl: 'ftp://x', inviteCode: 'a' }), res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: (async () => jsonResp(200, {})) as any });
    expect(json(res).error).toBe('bad_hub_url');
    res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://h:1' }), res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: (async () => jsonResp(200, {})) as any });
    expect(json(res).error).toBe('code_required');
  });

  it('join-remote: hub timeout → 504 hub_timeout', async () => {
    writeBots([]);
    const timeoutFetcher = vi.fn(async () => { const e: any = new Error('aborted'); e.name = 'AbortError'; throw e; });
    const res = makeRes();
    await handleFederationSpokeApi(
      makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://hub:7891', inviteCode: 'INV' }),
      res, new URL('http://x/api/team/join-remote'), { dataDir, fetcher: timeoutFetcher as any },
    );
    expect(res.statusCode).toBe(504);
    expect(json(res).error).toBe('hub_timeout');
  });

  it('remote-roster: sends token in header (not URL); leave-remote revokes at hub + forgets locally', async () => {
    writeBots([]);
    // join one hub
    const joinFetcher = vi.fn(async () => jsonResp(200, { ok: true, teamId: 'default', teamName: 'T', syncToken: 'TOK' }));
    await handleFederationSpokeApi(makeReq('POST', '/api/team/join-remote', { hubUrl: 'http://hub:7891', inviteCode: 'INV' }), makeRes(), new URL('http://x/api/team/join-remote'), { dataDir, fetcher: joinFetcher as any });

    // remote-roster pulls the hub roster — token in Authorization header, NOT the URL
    const rosterFetcher = vi.fn(async (u: any, init: any) => {
      expect(String(u)).toBe('http://hub:7891/api/federation/roster'); // no ?syncToken=
      expect(init.headers.authorization).toBe('Bearer TOK');
      return jsonResp(200, { ok: true, team: { id: 'default', name: 'T', memberCount: 1 }, deployments: [], bots: [{ larkAppId: 'cli_x', name: 'X' }] });
    });
    let res = makeRes();
    await handleFederationSpokeApi(makeReq('GET', '/api/team/remote-roster'), res, new URL('http://x/api/team/remote-roster'), { dataDir, fetcher: rosterFetcher as any });
    expect(res.statusCode).toBe(200);
    expect(json(res).memberships[0].roster.bots[0].larkAppId).toBe('cli_x');

    // leave-remote calls the hub's /leave (with the token) then forgets locally
    const leaveFetcher = vi.fn(async (u: any, init: any) => {
      expect(String(u)).toBe('http://hub:7891/api/federation/leave');
      expect(init.headers.authorization).toBe('Bearer TOK');
      return jsonResp(200, { ok: true });
    });
    res = makeRes();
    await handleFederationSpokeApi(makeReq('POST', '/api/team/leave-remote', { hubUrl: 'http://hub:7891', teamId: 'default' }), res, new URL('http://x/api/team/leave-remote'), { dataDir, fetcher: leaveFetcher as any });
    expect(res.statusCode).toBe(200);
    expect(json(res).hubRevoked).toBe(true);
    expect(leaveFetcher).toHaveBeenCalled();
    expect(listMemberships(dataDir).length).toBe(0);
  });
});
