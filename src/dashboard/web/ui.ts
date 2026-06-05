import {
  DASHBOARD_LOCALE_STORAGE_KEY,
  createDashboardTranslator,
  readStoredDashboardLocale,
  type DashboardLocale,
} from './i18n.js';
import {
  THEME_STORAGE_KEY,
  SKIN_STORAGE_KEY,
  readStoredThemeMode,
  readStoredSkin,
  resolveThemeMode,
  type ResolvedTheme,
  type ThemeMode,
  type SkinId,
} from './preferences.js';
import { applyCyberFx } from './cyber-fx.js';
import { playSkinIntro } from './skin-intro.js';

type UiListener = () => void;

class DashboardUiState {
  locale: DashboardLocale = 'zh';
  themeMode: ThemeMode = 'system';
  resolvedTheme: ResolvedTheme = 'light';
  skin: SkinId = 'default';
  private listeners = new Set<UiListener>();
  private translate = createDashboardTranslator(this.locale);
  private mediaQuery: MediaQueryList | null = null;

  init(): void {
    const w = typeof window !== 'undefined' ? window : undefined;
    this.locale = readStoredDashboardLocale(w?.localStorage, navigatorLanguages());
    this.translate = createDashboardTranslator(this.locale);
    this.themeMode = readStoredThemeMode(w?.localStorage);
    this.skin = readStoredSkin(w?.localStorage);
    this.mediaQuery = w?.matchMedia?.('(prefers-color-scheme: dark)') ?? null;
    this.mediaQuery?.addEventListener('change', () => {
      this.applyTheme();
      this.emit();
    });
    this.applyTheme();
    this.applySkin();
    this.applyLocale();
  }

  t(key: string, params?: Record<string, string | number>): string {
    return this.translate(key, params);
  }

  setLocale(locale: DashboardLocale): void {
    if (this.locale === locale) return;
    this.locale = locale;
    this.translate = createDashboardTranslator(locale);
    window.localStorage.setItem(DASHBOARD_LOCALE_STORAGE_KEY, locale);
    this.applyLocale();
    this.emit();
  }

  // The topbar exposes a single "Theme" dropdown whose value is either a base
  // colour mode (system/light/dark → the `default` skin) or a named skin id.
  get theme(): string {
    return this.skin === 'default' ? this.themeMode : this.skin;
  }

  setTheme(value: string): void {
    const isMode = value === 'system' || value === 'light' || value === 'dark';
    const nextSkin: SkinId = isMode ? 'default' : (value as SkinId);
    const skinChanged = nextSkin !== this.skin;
    if (isMode && this.themeMode !== value) {
      this.themeMode = value as ThemeMode;
      window.localStorage.setItem(THEME_STORAGE_KEY, this.themeMode);
    }
    if (skinChanged) {
      this.skin = nextSkin;
      window.localStorage.setItem(SKIN_STORAGE_KEY, this.skin);
    }
    this.applyTheme();
    this.applySkin(skinChanged);
    this.emit();
  }

  on(fn: UiListener): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  private emit(): void {
    for (const fn of this.listeners) fn();
  }

  private applyTheme(): void {
    this.resolvedTheme = resolveThemeMode(this.themeMode, !!this.mediaQuery?.matches);
    // A named skin ships its own light/dark palette, so drive data-theme from the
    // skin's intrinsic mode — that way the base theme's light/dark component rules
    // (incl. PR #123's dark-only overrides) match the skin instead of fighting it.
    // The default skin follows the user's system/light/dark choice.
    const themeAttr = this.skin === 'default' ? this.resolvedTheme : SKIN_THEME[this.skin];
    document.documentElement.dataset.theme = themeAttr;
    document.documentElement.dataset.themeMode = this.themeMode;
  }

  // `animate` plays the boot loader — true when the user actively switches in,
  // false on initial load so a refresh doesn't replay the 3s decrypt overlay.
  private applySkin(animate = false): void {
    document.documentElement.dataset.skin = this.skin;
    applyCyberFx(this.skin === 'cyber', animate);
    // 2077 plays its own boot loader; the other skins get a themed switch-in intro.
    if (animate && this.skin !== 'cyber' && this.skin !== 'default') {
      playSkinIntro(this.skin);
    }
  }

  private applyLocale(): void {
    document.documentElement.lang = this.locale === 'zh' ? 'zh-CN' : 'en';
  }
}

// Each named skin's intrinsic light/dark mode (drives the data-theme attribute).
const SKIN_THEME: Record<SkinId, ResolvedTheme> = {
  default: 'light',
  cyber: 'dark',
  genshin: 'light',
  fallout: 'dark',
  prts: 'dark',
  bluearchive: 'dark',
  zzz: 'dark',
  dragonball: 'light',
  ikun: 'dark',
};

function navigatorLanguages(): readonly string[] {
  if (typeof navigator === 'undefined') return [];
  return navigator.languages?.length ? navigator.languages : [navigator.language].filter(Boolean);
}

export const ui = new DashboardUiState();

export function t(key: string, params?: Record<string, string | number>): string {
  return ui.t(key, params);
}

export function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]!));
}

export function relTime(ms: number): string {
  if (!ms) return '-';
  const diff = Date.now() - ms;
  if (diff < 60_000) return t('common.now');
  if (diff < 3_600_000) return Math.floor(diff / 60_000) + 'm';
  if (diff < 86_400_000) return Math.floor(diff / 3_600_000) + 'h';
  return Math.floor(diff / 86_400_000) + 'd';
}

// ── 数字员工视觉：每个 bot 一颗专属色相的"数字生命球" ─────────────────────
// 按名字 hash 从固定色板取渐变对，同名永远同色，跨页面一致。
const ORB_PALETTE: Array<{ c1: string; c2: string }> = [
  { c1: '#5be3ff', c2: '#4f8bff' },
  { c1: '#b89bff', c2: '#6b4df0' },
  { c1: '#7ce0c3', c2: '#2e9e8f' },
  { c1: '#8fb4ff', c2: '#3b62d8' },
  { c1: '#ffd28f', c2: '#d8783b' },
  { c1: '#7df0a8', c2: '#1f9e63' },
  { c1: '#9fd0ff', c2: '#4878c8' },
  { c1: '#ff9fb8', c2: '#d84a78' },
];

export function botOrbStyle(name: string): string {
  let h = 0;
  const key = String(name ?? '');
  for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
  const { c1, c2 } = ORB_PALETTE[h % ORB_PALETTE.length];
  return `--c1:${c1};--c2:${c2}`;
}

// ── 跨页共享的展示名解析（bot 友好名 / 群聊标题）────────────────────────────
// daemon IPC 上报的 SessionRow.botName 历史上填的是 larkAppId（friendly name
// probe 回来只回写了注册表 descriptor，没回填 IPC 的 cachedBotName），这里用
// /api/groups 的注册表 + 群列表把 id 解析成人话。加载失败静默降级显示原值——
// 纯展示增强，不挡核心功能。
const botNameByAppId = new Map<string, string>();
const chatNameById = new Map<string, string>();
let nameMapsPromise: Promise<void> | null = null;

export function loadNameMaps(): Promise<void> {
  nameMapsPromise ??= (async () => {
    try {
      const r = await fetch('/api/groups');
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      for (const b of data.bots ?? []) {
        if (b.larkAppId && b.botName && b.botName !== b.larkAppId) {
          botNameByAppId.set(b.larkAppId, String(b.botName));
        }
      }
      for (const c of data.chats ?? []) {
        if (c.chatId && c.name) chatNameById.set(c.chatId, String(c.name));
      }
    } catch {
      // 失败不缓存（dashboard 刚启动 /api/groups 可能短暂 503）——
      // 清掉 memo，下一个页面 mount / strip 重绘再重试；期间显示原始 id。
      nameMapsPromise = null;
    }
  })();
  return nameMapsPromise;
}

/** 会话所属 bot 的显示名：注册表友好名 → 会话自带 botName（非 id 时）→ id。 */
export function botDisplayName(s: Record<string, any>): string {
  const mapped = s.larkAppId ? botNameByAppId.get(s.larkAppId) : undefined;
  if (mapped) return mapped;
  if (s.botName && s.botName !== s.larkAppId) return String(s.botName);
  return String(s.botName ?? s.larkAppId ?? '-');
}

/** 会话所在群聊的标题；单聊或群列表里查不到时返回 null（由调用方回退）。 */
export function chatDisplayTitle(s: Record<string, any>): string | null {
  return (s.chatId && chatNameById.get(s.chatId)) || null;
}

/** 话题首条消息常以 "@bot " 开头（群里要 @ 才能触发）——展示时剥掉开头的
 *  连续 mention，只留真正的消息内容；剥空了（纯 @ 消息）就保留原文。 */
export function stripMentionPrefix(title: unknown): string {
  const raw = String(title ?? '');
  const out = raw.replace(/^(?:@\S+\s*)+/, '').trim();
  return out || raw;
}

/** 会话当前是否卡在等人，以及等什么（全局 strip 和工作台共用同一判定）。 */
export function attentionReason(s: Record<string, any>): string | null {
  if (s.status === 'closed') return null;
  if (s.pendingRepo) return t('sessions.board.signalRepo');
  if (s.tuiPromptActive) return t('sessions.board.signalPrompt');
  if (s.status === 'limited') return t('sessions.board.signalLimited');
  return null;
}
