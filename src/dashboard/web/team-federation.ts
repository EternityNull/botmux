// Team (federation) page: manage this deployment's team membership across
// deployments — show my bots + federated bots, mint invite codes (Hub), and
// join other deployments' teams by invite (Spoke). All dashboard-token authed.
// See docs/federation-design.md.
import { escapeHtml } from './ui.js';

interface RosterBot {
  larkAppId: string; name: string; cliId: string; capability: string | null;
  hasTeamRole: boolean; deployment: { id: string; name: string; local: boolean; stale: boolean };
}
interface RosterDeployment { id: string; name: string; local: boolean; botCount: number; stale: boolean; }
interface LocalResp {
  ok: boolean; deployment: { deploymentId: string; name: string };
  suggestedHubUrl: string; deployments: RosterDeployment[]; bots: RosterBot[];
}

async function jget(u: string) { const r = await fetch(u); return { status: r.status, body: await r.json().catch(() => ({})) }; }
async function jpost(u: string, b?: unknown) {
  const r = await fetch(u, { method: 'POST', headers: { 'content-type': 'application/json' }, body: b ? JSON.stringify(b) : undefined });
  return { status: r.status, body: await r.json().catch(() => ({})) };
}

function pageHtml(): string {
  return `<section class="page">
<div class="page-heading">
  <div>
    <p class="eyebrow">团队</p>
    <h1>团队协作（跨部署）</h1>
    <p>把别的部署（同事自己跑的 botmux）邀请进同一个团队，互相发现机器人、协作。</p>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <h2 style="margin-top:0">本部署</h2>
  <p>名称：<b id="tf-dep-name">…</b>
    <button id="tf-rename" class="ghost" style="margin-left:8px">重命名</button></p>
  <p class="muted" style="font-size:13px">别人加入你的团队时，需要你的 Hub 地址 + 邀请码。</p>
  <p><button id="tf-invite" class="primary">生成邀请码</button></p>
  <div id="tf-invite-out" style="display:none;margin-top:8px"></div>
</div>

<div class="card" style="margin-bottom:16px">
  <h2 style="margin-top:0">加入别人的团队</h2>
  <p style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <input id="tf-hub" placeholder="Hub 地址，如 http://10.0.0.5:7891" style="flex:1;min-width:240px">
    <input id="tf-code" placeholder="邀请码" style="min-width:160px">
    <button id="tf-join" class="primary">加入</button>
  </p>
  <div id="tf-join-out" style="display:none;margin-top:6px"></div>
</div>

<div class="card" style="margin-bottom:16px">
  <h2 style="margin-top:0">团队花名册 <span class="muted" id="tf-roster-meta" style="font-size:13px"></span></h2>
  <div id="tf-roster">加载中…</div>
  <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <input id="tf-grp-name" placeholder="群名（如：跨团队排障）" style="min-width:200px">
    <button id="tf-grp" class="primary">把勾选的机器人拉一个群</button>
    <span class="muted" style="font-size:13px">勾选机器人（可跨部署）→ 拉到一个飞书群协作</span>
  </div>
  <div id="tf-grp-out" style="display:none;margin-top:8px"></div>
</div>

<div class="card">
  <h2 style="margin-top:0">我加入的远端团队 <button id="tf-sync" class="ghost" style="float:right;font-size:13px">同步</button></h2>
  <div id="tf-remote">加载中…</div>
</div>
</section>`;
}

function renderRoster(el: HTMLElement, deployments: RosterDeployment[], bots: RosterBot[]): void {
  if (!bots.length) { el.innerHTML = '<p class="muted">还没有机器人。</p>'; return; }
  // group by deployment, local first
  const ordered = [...deployments].sort((a, b) => (a.local === b.local ? 0 : a.local ? -1 : 1));
  let html = '';
  for (const dep of ordered) {
    const tag = dep.local ? '（本部署）' : (dep.stale ? '（远端 · 离线？）' : '（远端）');
    html += `<div style="margin:10px 0 4px"><b>${escapeHtml(dep.name)}</b> <span class="muted" style="font-size:12px">${tag} · ${dep.botCount} 个</span></div>`;
    html += '<table style="width:100%;border-collapse:collapse;font-size:14px"><tbody>';
    for (const b of bots.filter(x => x.deployment.id === dep.id)) {
      const cap = b.capability ? escapeHtml(b.capability) : '<span class="muted">—</span>';
      const role = b.hasTeamRole ? '有角色' : '<span class="muted">—</span>';
      const dim = b.deployment.stale ? 'opacity:.55' : '';
      html += `<tr style="${dim}"><td style="padding:4px 8px"><input type="checkbox" class="tf-pick" data-app="${escapeHtml(b.larkAppId)}"></td><td style="padding:4px 8px">${escapeHtml(b.name)}</td><td style="padding:4px 8px" class="muted">${escapeHtml(b.cliId)}</td><td style="padding:4px 8px">${cap}</td><td style="padding:4px 8px">${role}</td></tr>`;
    }
    html += '</tbody></table>';
  }
  el.innerHTML = html;
}

async function loadLocal(): Promise<void> {
  const r = await jget('/api/team/local');
  const b = r.body as LocalResp;
  if (!b?.ok) { document.getElementById('tf-roster')!.innerHTML = '<p class="muted">加载失败。</p>'; return; }
  document.getElementById('tf-dep-name')!.textContent = b.deployment.name;
  (document.getElementById('tf-roster') as HTMLElement).dataset.hub = b.suggestedHubUrl;
  const remoteCount = b.deployments.filter(d => !d.local).length;
  document.getElementById('tf-roster-meta')!.textContent = `· ${b.bots.length} 个机器人 / ${b.deployments.length} 个部署${remoteCount ? `（含 ${remoteCount} 个远端）` : ''}`;
  renderRoster(document.getElementById('tf-roster')!, b.deployments, b.bots);
}

async function loadRemote(): Promise<void> {
  const r = await jget('/api/team/remote-roster');
  const list = (r.body as any)?.memberships || [];
  const el = document.getElementById('tf-remote')!;
  if (!list.length) { el.innerHTML = '<p class="muted">还没加入任何远端团队。用上方「加入别人的团队」粘对方的 Hub 地址 + 邀请码。</p>'; return; }
  el.innerHTML = list.map((m: any) => {
    const status = m.ok ? `<span class="ok">已连接</span>` : `<span class="err">连接失败：${escapeHtml(m.error || '')}</span>`;
    const bots = m.ok && m.roster ? `${m.roster.bots.length} 个机器人 / ${m.roster.deployments.length} 个部署` : '';
    return `<div style="padding:8px 0;border-bottom:1px solid var(--border,#eee)">
      <b>${escapeHtml(m.teamName || m.teamId)}</b> <span class="muted" style="font-size:12px">${escapeHtml(m.hubUrl)}</span> — ${status} <span class="muted">${bots}</span>
      <button class="ghost tf-leave" data-hub="${escapeHtml(m.hubUrl)}" data-team="${escapeHtml(m.teamId)}" style="float:right;font-size:12px">退出</button>
    </div>`;
  }).join('');
  el.querySelectorAll<HTMLButtonElement>('.tf-leave').forEach(btn => {
    btn.onclick = async () => {
      if (!confirm('退出该远端团队？将通知对方 Hub 移除你的部署。')) return;
      await jpost('/api/team/leave-remote', { hubUrl: btn.dataset.hub, teamId: btn.dataset.team });
      loadRemote();
    };
  });
}

export function renderTeamFederationPage(root: HTMLElement): void {
  root.innerHTML = pageHtml();

  document.getElementById('tf-rename')!.onclick = async () => {
    const name = prompt('部署名称：', document.getElementById('tf-dep-name')!.textContent || '');
    if (!name || !name.trim()) return;
    await jpost('/api/team/rename-deployment', { name: name.trim() });
    loadLocal();
  };

  document.getElementById('tf-invite')!.onclick = async () => {
    const r = await jpost('/api/team/local-invite');
    const out = document.getElementById('tf-invite-out')!;
    out.style.display = '';
    if ((r.body as any)?.code) {
      const hub = (document.getElementById('tf-roster') as HTMLElement).dataset.hub || '';
      const code = (r.body as any).code;
      out.innerHTML = `<p class="muted" style="font-size:13px">把下面两项发给对方，让 ta 在自己的 dashboard「团队」页里填（24 小时内、单次有效）：</p>
        <p>Hub 地址：<code>${escapeHtml(hub)}</code></p>
        <p>邀请码：<code style="font-size:16px">${escapeHtml(code)}</code></p>`;
    } else {
      out.innerHTML = '<span class="err">生成失败。</span>';
    }
  };

  document.getElementById('tf-join')!.onclick = async () => {
    const hubUrl = (document.getElementById('tf-hub') as HTMLInputElement).value.trim();
    const inviteCode = (document.getElementById('tf-code') as HTMLInputElement).value.trim();
    const out = document.getElementById('tf-join-out')!;
    out.style.display = '';
    if (!hubUrl || !inviteCode) { out.innerHTML = '<span class="err">请填 Hub 地址和邀请码。</span>'; return; }
    out.innerHTML = '<span class="muted">加入中…</span>';
    const r = await jpost('/api/team/join-remote', { hubUrl, inviteCode });
    if ((r.body as any)?.ok) {
      out.innerHTML = `<span class="ok">已加入「${escapeHtml((r.body as any).teamName || '')}」</span>`;
      (document.getElementById('tf-code') as HTMLInputElement).value = '';
      loadRemote();
    } else {
      const e = (r.body as any)?.error || r.status;
      const msg = e === 'deployment_already_joined' ? '你的部署已经加入过这个团队了' : e === 'hub_unreachable' ? '连不上对方 Hub（检查地址/网络）' : e === 'hub_timeout' ? '对方 Hub 响应超时' : `加入失败：${e}`;
      out.innerHTML = `<span class="err">${escapeHtml(String(msg))}</span>`;
    }
  };

  document.getElementById('tf-sync')!.onclick = async () => {
    await jpost('/api/team/sync-remote');
    loadRemote();
  };

  document.getElementById('tf-grp')!.onclick = async () => {
    const apps = Array.from(document.querySelectorAll<HTMLInputElement>('.tf-pick:checked')).map(c => c.dataset.app!);
    const out = document.getElementById('tf-grp-out')!;
    out.style.display = '';
    if (!apps.length) { out.innerHTML = '<span class="err">请先勾选至少一个机器人。</span>'; return; }
    const name = (document.getElementById('tf-grp-name') as HTMLInputElement).value.trim() || '协作群';
    out.innerHTML = '<span class="muted">建群中…</span>';
    const r = await jpost('/api/team/federated-group', { name, larkAppIds: apps });
    const b: any = r.body;
    if (b?.ok && b.chatId) {
      const link = b.shareLink || ('https://applink.feishu.cn/client/chat/open?openChatId=' + encodeURIComponent(b.chatId));
      const invalid = (b.invalidBotIds || []).length ? `<p class="hint err">未能加入的机器人：${escapeHtml((b.invalidBotIds || []).join(', '))}</p>` : '';
      out.innerHTML = `<span class="ok">群已创建</span> · <a href="${escapeHtml(link)}" target="_blank">在飞书打开</a>${invalid}`;
    } else {
      const e = b?.error || r.status;
      const msg = e === 'no_online_daemon' ? '你这边没有在线的本地机器人来建群（建群必须由本部署的机器人发起，请至少勾选一个本部署的在线机器人）' : `建群失败：${e}`;
      out.innerHTML = `<span class="err">${escapeHtml(String(msg))}</span>`;
    }
  };

  void loadLocal();
  void loadRemote();
}
