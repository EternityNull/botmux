# 跨部署联邦协作设计（Federation）

> 目标：让**独立的多套 botmux 部署**（同一飞书租户、各自单 owner）组成一个共享协作团队——
> 互相发现对方的 bot + 能力，并最终把跨部署的 bot 拉进同一个飞书群协作。
>
> 三方共识（申晗 / Claude / Codex）。本文是 [[platform-design]] 的跨部署扩展。

## 前提（已确认）

1. **同一飞书租户**：`union_id` 跨部署稳定、不同部署的 bot 能进同一个飞书群、能互相 @。
2. **网络可达**：spoke 能通过 HTTP 访问 hub（公司内网）。方向以 **spoke → hub** 为主，spoke 不需要对外暴露端口。
3. **一套部署 = 一个 owner = 一个团队成员**。单 daemon 单 owner，不存在多人共操作同一 dashboard，因此**跨部署不需要 /pair**——每个人在自己的 dashboard 里本就是已认证的 owner（dashboard token 即代表他）。

## 核心洞察

真正的协作发生在**飞书群**里：不同部署的 bot 本就能共处一个飞书群，botmux 已支持多 bot @（observed-bots 交叉引用）。
所以「跨部署」要解决的只有两件事：

- **发现**：看到彼此的 bot + 能力（聚合花名册）
- **建群**：把跨部署选中的 bot 拉进同一个飞书群（唯一的跨部署「写」）

## 拓扑：Hub + Spoke，各用各的 dashboard

- 每个人的 dashboard 仍然**只管自己本机的 daemon/bot**（localhost IPC 代理不变）——不把任何人的 daemon 暴露给别人。
- **Hub** = 建队的那套部署，持有团队 / 成员 / 聚合花名册。
- **Spoke** = 用邀请码加入某个 hub 团队的部署。
- 团队相关的数据走 hub 的联邦 API；各方在**自己的 dashboard**「团队」板块里读写。

```
  申晗 dashboard ──(本机IPC)── 申晗的 daemon/bot
       │
       │  /api/federation/*  (注册/同步/拉聚合花名册)
       ▼
  ┌─────────── Hub（申晗这套）持有 team + 联邦 bot ───────────┐
       ▲
       │  /api/federation/*  (spoke 主动 → hub)
       │
  别人 dashboard ──(本机IPC)── 别人的 daemon/bot
```

## 身份与信任

- **部署身份**：每套部署生成一次性 `deploymentId`（`{dataDir}/deployment-identity.json`，uuid）+ 一个可读 `name`（owner 自己填，默认机器名/owner 名）。
- **加入凭证**：复用现有**邀请码**（[[invite-store]]，单次/24h）。spoke 用邀请码向 hub 注册一次，hub 换发一个**长期 `syncToken`**（每 spoke 一个）用于后续同步/拉花名册。
- **信任边界仍是「团队」**：团队内互信，不做逐操作鉴权。邀请码=部署级准入；syncToken=该 spoke 的持续凭证。

## 数据模型

### Hub 侧：`federation-store.ts` → `{dataDir}/federations.json`
按 teamId 存已加入的远端部署：
```ts
interface FederatedDeployment {
  deploymentId: string;
  name: string;            // 展示用（owner/部署名）
  syncToken: string;       // 该 spoke 的持续凭证（高熵，不外泄）
  bots: FederatedBot[];    // 最近一次同步推上来的 bot
  joinedAt: number;
  lastSeenAt: number;      // 心跳
}
interface FederatedBot {
  larkAppId: string; botName: string; cliId: string;
  botUnionId?: string;     // 租户稳定，P2 拉群按此加 bot
  capability?: string; hasTeamRole?: boolean;
}
```

### Spoke 侧：`federation-membership-store.ts` → `{dataDir}/federation-memberships.json`
本部署加入了哪些远端团队：
```ts
interface RemoteMembership {
  hubUrl: string; teamId: string; teamName: string;
  syncToken: string; deploymentId: string; joinedAt: number;
}
```

## API

### Hub 侧（挂在 dashboard，**在 token 网关之前**，跨部署可达；用邀请码/syncToken 自鉴权）
- `POST /api/federation/join` — `{ inviteCode, deployment:{deploymentId,name,bots[]} }`
  → 校验并消费邀请码（→teamId）；**新**部署 → 换发 `syncToken`，回 `{ ok, teamId, teamName, syncToken }`。
  ⚠️ `deploymentId` 是公开的（进 roster），所以**重复 deploymentId 不回吐已有 token、也不覆盖记录**，回 `409 deployment_already_joined`；重绑/轮转留待后续显式 reset（凭旧 syncToken）。
- `POST /api/federation/sync` — `{ syncToken, bots[] }` → 刷新该部署 bot + `lastSeenAt`；回 `{ ok }`。
- `GET /api/federation/roster` — **syncToken 走 `Authorization: Bearer <token>` 头**（不进 URL，避免落 access/proxy log）；Hub 短期兼容 `?syncToken=` 查询作 fallback。回聚合花名册（hub 本地 bot + 各 spoke 的 bot，按部署分组）。
- `POST /api/federation/leave` — `{ syncToken }`（或 Bearer 头）→ 删除该部署（`removeDeploymentByToken`），spoke 主动退出/撤销时调用。幂等。

所有出站调用（spoke→hub）统一 `fetchWithTimeout`（AbortController，8s），错误面稳定区分 `hub_timeout`(504) / `hub_unreachable`(502)。

### Spoke 侧（挂在 dashboard，**dashboard token 鉴权**，owner 操作）
- `POST /api/team/join-remote` — `{ hubUrl, inviteCode }`
  → 收集本机 bot（bots-info + 能力 + 尽力解析 botUnionId）→ 调 `hubUrl/api/federation/join` → 存 `RemoteMembership` → 回结果（hub 拒绝时透出 409 `deployment_already_joined` / 403 invite 错 / 504 `hub_timeout` / 502 `hub_unreachable`）。
- `GET /api/team/remote-roster` → 对每个已加入的 hub 拉 `/api/federation/roster`（Bearer 头带 syncToken），汇总展示。
- `POST /api/team/sync-remote` → 手动触发向各 hub 推 bot+心跳。
- `POST /api/team/leave-remote` — `{ hubUrl, teamId }` → 先 best-effort 调 hub `/api/federation/leave` 撤销（回 `hubRevoked`），再忘掉本地 `RemoteMembership`。
- 周期同步：dashboard 进程内 timer（2min）向各 hub `POST /api/federation/sync` 推 bot + 心跳。

## 聚合花名册

Hub 的团队花名册 = 本地 bot（[[team-roster]]，按 bots.json 顺序）+ 各 spoke 的 `FederatedBot`，
每条带 `deployment: { id, name, local, stale }`，按部署分组展示（本地置顶，远端按 name）。
远端部署超过 `FEDERATION_STALE_MS`（5min）未同步即标 `stale`（疑似离线）——不硬隐藏，留 UI 降级展示。
`AggregatedRosterBot` 保留 `botUnionId?`（P2 拉群按 union_id 加 bot 时免改接口）。

## 拉群（跨部署，P2 — 已实现）

**关键事实**：飞书加 bot 进群用的是 **app_id**（`im/v1/chats` 的 `bot_id_list`、`im/v1/chats/:id/members` 的 `member_id_type=app_id`），**不是 union_id**。
而 botmux 每个 bot 本就是独立飞书 app，现有 `/group` 早已在「同一个群里加多个不同 app 的 bot」——「跨 app 加 bot」在本部署已验证可用。**联邦 bot 也是同租户的另一个 app，加法完全相同。**（早先 union_id 的顾虑是误解，已纠正。）

所以 P2 直接**复用现有建群链路**，无需新机制、无需 spike：
- Hub 的建群由它**自己某个在线本地 bot** 当 creator（创建 chat 必须本机 daemon 发起）。
- 选中的 bot（本地 + 联邦）全部按 **larkAppId** 进 `bot_id_list`；联邦 bot 由 Feishu 按 app_id 加入，加不上的进 `invalid_bot_id_list`。
- 联邦 bot 被加进群后，它**自己部署的 daemon**（订阅了该 app 事件）自动感知该群并参与——Hub 不碰对方 daemon。
- 校验：选中 app_id 必须都在**聚合花名册**里；至少一个本地在线 bot 作 creator（否则 `no_online_daemon`）。

实现：`POST /api/team/federated-group {name, larkAppIds}`（dashboard token），校验聚合花名册后复用 `createTeamGroup`（pickCreator 只从本地在线 bot 里选，联邦 bot 自然只作被加成员）。operator 不自动入群（用 share link 进），避开 open_id app-scope。

## 分期

- **P1**：联邦基础——部署身份 + 邀请注册 + 同步/心跳 + 聚合花名册 + spoke 加入/拉花名册 API。让大家「看到彼此的 bot」。
- **P2**：跨部署拉群——复用现有 createChat 按 app_id 加联邦 bot（见上，已实现）。
- **P3**：跨部署共享 connector / 团队角色（按需）。

## 与现有代码的关系

- 复用：`invite-store`（邀请码）、`team-store`（团队/teamId）、`team-roster`（本地花名册）、`bot-profile-store`（能力）。
- 新增：`deployment-identity`、`federation-store`（hub）、`federation-membership-store`（spoke）、`dashboard/federation-api`（hub 端点）、`dashboard/team-routes` 增 spoke 端点。
- `/pair` + 单部署多用户那条线在联邦模型下非主路径；本次不删除，后续按需退役。
