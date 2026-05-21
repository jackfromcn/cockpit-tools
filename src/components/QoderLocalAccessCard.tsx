import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Activity,
  Check,
  CircleAlert,
  Copy,
  Database,
  Eye,
  EyeOff,
  FolderPlus,
  Gauge,
  KeyRound,
  Power,
  RefreshCw,
  Server,
  Trash2,
  X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { buildQoderAccountPresentation } from '../presentation/platformAccountPresentation';
import { useEscClose } from '../hooks/useEscClose';
import { SingleSelectDropdown } from './SingleSelectDropdown';
import * as qoderLocalAccessService from '../services/qoderLocalAccessService';
import type {
  QoderLocalAccessRoutingStrategy,
  QoderLocalAccessScope,
  QoderLocalAccessState,
} from '../services/qoderLocalAccessService';
import type { QoderAccount } from '../types/qoder';
import './CodexLocalAccessModal.css';

interface Props {
  accounts: QoderAccount[];
  currentAccountId?: string | null;
  maskAccountText: (value?: string | null) => string;
  layoutMode: 'grid' | 'list';
}

type CopyableField = 'baseUrl' | 'apiKey';

const ROUTING_STRATEGY_OPTIONS: Array<{ value: QoderLocalAccessRoutingStrategy; label: string }> = [
  { value: 'auto', label: '自动路由' },
  { value: 'quota_high_first', label: '优先高配额' },
  { value: 'quota_low_first', label: '优先低配额' },
];

const ACCESS_SCOPE_OPTIONS: Array<{ value: QoderLocalAccessScope; label: string }> = [
  { value: 'localhost', label: '仅本机' },
  { value: 'lan', label: '局域网' },
];

function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat('en', {
    notation: value >= 1000 ? 'compact' : 'standard',
    maximumFractionDigits: value >= 1000 ? 1 : 0,
  }).format(value || 0);
}

function formatLatencyMs(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return '--';
  if (value >= 1000) return `${(value / 1000).toFixed(2)}s`;
  return `${Math.round(value)}ms`;
}

function maskApiKey(value?: string | null): string {
  if (!value) return '未初始化';
  if (value.length <= 10) return value;
  return `${value.slice(0, 10)}••••••••••••`;
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function resolveRoutingStrategyLabel(value?: string | null): string {
  switch (value) {
    case 'quota_high_first':
      return '优先高配额';
    case 'quota_low_first':
      return '优先低配额';
    case 'auto':
    default:
      return '自动路由';
  }
}

function resolveAccessScopeLabel(value?: string | null): string {
  switch (value) {
    case 'lan':
      return '局域网';
    case 'localhost':
    default:
      return '仅本机';
  }
}

export function QoderLocalAccessCard({
  accounts,
  currentAccountId,
  maskAccountText,
  layoutMode,
}: Props) {
  const { t } = useTranslation();
  const [state, setState] = useState<QoderLocalAccessState | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [copiedField, setCopiedField] = useState<CopyableField | null>(null);
  const [keyVisible, setKeyVisible] = useState(false);
  const [showPanel, setShowPanel] = useState(false);
  const [portInput, setPortInput] = useState('');

  useEscClose(showPanel, () => {
    if (!loading) {
      setShowPanel(false);
    }
  });

  const refresh = useCallback(async (opts?: { silent?: boolean }) => {
    try {
      const nextState = await qoderLocalAccessService.getState();
      setState(nextState);
      setError('');
      if (!opts?.silent) {
        setNotice('');
      }
      return nextState;
    } catch (err) {
      const message = formatError(err);
      setError(message);
      throw err;
    }
  }, []);

  useEffect(() => {
    void refresh({ silent: true });
  }, [refresh]);

  useEffect(() => {
    setPortInput(state?.collection?.port ? String(state.collection.port) : '');
  }, [state?.collection?.port]);

  const accountMap = useMemo(() => {
    const map = new Map<string, QoderAccount>();
    accounts.forEach((account) => {
      map.set(account.id, account);
    });
    return map;
  }, [accounts]);

  const collection = state?.collection ?? null;
  const enabled = collection?.enabled ?? false;
  const running = state?.running ?? false;
  const baseUrl = state?.baseUrl ?? '';
  const apiKey = collection?.apiKey ?? '';
  const stats = state?.stats?.totals;
  const actionBusy = loading;
  const statusTone = !collection ? 'disabled' : running ? 'running' : enabled ? 'stopped' : 'disabled';
  const statusText = !collection
    ? '已停用'
    : running
      ? '运行中'
      : enabled
        ? '未运行'
        : '已停用';

  const memberAccounts = useMemo(() => {
    const ids = collection?.accountIds ?? [];
    return ids
      .map((accountId) => accountMap.get(accountId))
      .filter((account): account is QoderAccount => Boolean(account));
  }, [accountMap, collection?.accountIds]);

  const missingMemberCount = Math.max(0, (collection?.accountIds?.length ?? 0) - memberAccounts.length);
  const previewAccounts = memberAccounts.slice(0, 2);
  const hiddenCount = Math.max(0, memberAccounts.length - previewAccounts.length);
  const modelId = state?.modelIds?.[0] ?? 'auto';
  const summaryStats = useMemo(
    () => [
      {
        key: 'requests',
        label: '总请求数',
        value: formatCompactNumber(stats?.requestCount ?? 0),
        detail: `成功 ${formatCompactNumber(stats?.successCount ?? 0)} / 失败 ${formatCompactNumber(stats?.failureCount ?? 0)}`,
      },
      {
        key: 'success',
        label: '成功率',
        value:
          stats && stats.requestCount > 0
            ? `${Math.round((stats.successCount / stats.requestCount) * 100)}%`
            : '--',
        detail: `最近更新 ${state?.stats?.updatedAt ? new Date(state.stats.updatedAt).toLocaleString() : '--'}`,
      },
      {
        key: 'members',
        label: '账号池',
        value: formatCompactNumber(state?.memberCount ?? 0),
        detail: missingMemberCount > 0 ? `另有 ${missingMemberCount} 个账号未在当前列表中` : '账号池与当前列表一致',
      },
      {
        key: 'latency',
        label: '平均延迟',
        value:
          stats && stats.requestCount > 0
            ? formatLatencyMs(stats.totalLatencyMs / stats.requestCount)
            : '--',
        detail: `模型 ${modelId}`,
      },
    ],
    [missingMemberCount, modelId, state?.memberCount, state?.stats?.updatedAt, stats],
  );

  const handleCopy = useCallback(async (field: CopyableField, value: string) => {
    if (!value) return;
    try {
      await navigator.clipboard.writeText(value);
      setCopiedField(field);
      window.setTimeout(() => {
        setCopiedField((current) => (current === field ? null : current));
      }, 1800);
    } catch (err) {
      setError(`复制失败: ${formatError(err)}`);
    }
  }, []);

  const syncAllAccounts = useCallback(async () => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.saveAccounts(accounts.map((account) => account.id));
      setState(nextState);
      setNotice(`账号池已同步，共 ${accounts.length} 个账号`);
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, [accounts]);

  const handleToggle = useCallback(async () => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextEnabled = !enabled;
      let nextState = await qoderLocalAccessService.setEnabled(nextEnabled);
      if (nextEnabled && accounts.length > 0) {
        nextState = await qoderLocalAccessService.saveAccounts(accounts.map((account) => account.id));
      }
      setState(nextState);
      setNotice(
        nextEnabled
          ? accounts.length > 0
            ? `API 服务已启用，并同步 ${accounts.length} 个账号`
            : 'API 服务已启用'
          : 'API 服务已停用',
      );
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, [accounts, enabled]);

  const handleRotateKey = useCallback(async () => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.rotateApiKey();
      setState(nextState);
      setNotice('API Key 已轮换');
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const handleClearStats = useCallback(async () => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.clearStats();
      setState(nextState);
      setNotice('服务统计已清空');
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const handleRefreshState = useCallback(async () => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      await refresh({ silent: true });
      setNotice('状态已刷新');
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, [refresh]);

  const handleUpdatePort = useCallback(async () => {
    const nextPort = Number(portInput);
    if (!Number.isInteger(nextPort) || nextPort <= 0 || nextPort > 65535) {
      setError('请输入 1-65535 之间的端口');
      return;
    }

    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.updatePort(nextPort);
      setState(nextState);
      setNotice(`监听端口已更新为 ${nextPort}`);
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, [portInput]);

  const handleUpdateRoutingStrategy = useCallback(async (strategy: QoderLocalAccessRoutingStrategy) => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.updateRoutingStrategy(strategy);
      setState(nextState);
      setNotice(`调度策略已更新为 ${resolveRoutingStrategyLabel(strategy)}`);
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const handleUpdateAccessScope = useCallback(async (accessScope: QoderLocalAccessScope) => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.updateAccessScope(accessScope);
      setState(nextState);
      setNotice(`监听范围已更新为 ${resolveAccessScopeLabel(accessScope)}`);
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const handleRemoveAccount = useCallback(async (accountId: string) => {
    setLoading(true);
    setError('');
    setNotice('');
    try {
      const nextState = await qoderLocalAccessService.removeAccount(accountId);
      setState(nextState);
      setNotice('账号已从 API 服务移除');
    } catch (err) {
      setError(formatError(err));
    } finally {
      setLoading(false);
    }
  }, []);

  const openPanel = useCallback(() => {
    setShowPanel(true);
  }, []);

  const closePanel = useCallback(() => {
    if (actionBusy) return;
    setShowPanel(false);
  }, [actionBusy]);

  const apiKeyDisplay = keyVisible ? apiKey || '未初始化' : maskApiKey(apiKey);
  const summaryText = `${state?.memberCount ?? 0} 个账号 · OpenAI 兼容`;
  const footerHint = running
    ? `监听 ${collection?.accessScope === 'lan' ? '0.0.0.0' : '127.0.0.1'}，本地 OpenAI 兼容网关已就绪`
    : '启用后提供 /v1/models 与 /v1/chat/completions';
  const serviceModeLine = collection
    ? `账号池：${state?.memberCount ?? 0} 个账号 · 调度：${resolveRoutingStrategyLabel(collection.routingStrategy)}`
    : '启用后可同步当前 Qoder 账号池';
  const cardLayoutClass = layoutMode === 'grid' ? 'grid' : 'compact';
  const shouldMarkCurrent = enabled || running;
  const summaryAccountPresentation = previewAccounts[0]
    ? buildQoderAccountPresentation(previewAccounts[0], t)
    : null;
  const summaryPrimaryQuota = summaryAccountPresentation?.quotaItems[0] ?? null;
  const summarySecondaryQuota = summaryAccountPresentation?.quotaItems[1] ?? null;

  return (
    <>
      <div
        className={`ghcp-account-card codex-account-card folder-inline-card codex-local-access-card codex-local-access-card--${cardLayoutClass} ${
          shouldMarkCurrent ? 'current' : ''
        } is-expanded`}
      >
        <div className="folder-inline-header codex-local-access-header">
          <div className="codex-local-access-summary-trigger" style={{ cursor: 'default' }}>
            <div className="folder-inline-icon codex-local-access-icon">
              <Database size={24} />
            </div>
            <div className="folder-inline-info">
              <div className="codex-local-access-title-row">
                <span className="folder-inline-name">API 服务</span>
                <span className="codex-local-access-summary-text">{summaryText}</span>
              </div>
              <span className="folder-inline-count">Qoder 本地 OpenAI 兼容入口</span>
            </div>
          </div>
          <div className="codex-local-access-header-actions">
            {shouldMarkCurrent && <span className="current-tag">当前</span>}
            <span className={`codex-local-access-status ${statusTone}`}>{statusText}</span>
          </div>
        </div>

        <div className="codex-local-access-meta">
          <div className="codex-local-access-row">
            <span className="codex-local-access-label">本机</span>
            <code className="codex-local-access-code" title={baseUrl || '未启用'}>
              {baseUrl || '未启用'}
            </code>
            <div className="codex-local-access-row-actions">
              <button
                type="button"
                className="folder-icon-btn"
                onClick={() => void handleCopy('baseUrl', baseUrl)}
                title={t('common.copy', '复制')}
                disabled={!baseUrl}
              >
                {copiedField === 'baseUrl' ? <Check size={14} /> : <Copy size={14} />}
              </button>
            </div>
          </div>
          <div className="codex-local-access-row">
            <span className="codex-local-access-label">密钥</span>
            <code className="codex-local-access-code" title={apiKey || '未初始化'}>
              {apiKeyDisplay}
            </code>
            <div className="codex-local-access-row-actions">
              <button
                type="button"
                className="folder-icon-btn"
                onClick={() => setKeyVisible((current) => !current)}
                title={keyVisible ? '隐藏密钥' : '显示密钥'}
                disabled={!apiKey}
              >
                {keyVisible ? <EyeOff size={14} /> : <Eye size={14} />}
              </button>
              <button
                type="button"
                className="folder-icon-btn"
                onClick={() => void handleCopy('apiKey', apiKey)}
                title={t('common.copy', '复制')}
                disabled={!apiKey}
              >
                {copiedField === 'apiKey' ? <Check size={14} /> : <Copy size={14} />}
              </button>
            </div>
          </div>
          <div className="account-sub-line codex-provider-inline-line codex-oauth-binding-line codex-local-access-oauth-line">
            <span className="codex-login-subline codex-provider-inline-text" title={serviceModeLine}>
              {serviceModeLine}
            </span>
            <button
              type="button"
              className="codex-provider-inline-switch codex-oauth-binding-action"
              onClick={openPanel}
              title="服务面板"
              disabled={!collection}
            >
              <Database size={11} />
              面板
            </button>
          </div>
        </div>

        <div className="folder-inline-preview codex-local-access-preview">
          {previewAccounts.length === 0 ? (
            <div className="codex-local-access-empty-state">
              <span className="codex-local-access-empty-text">当前账号池为空</span>
              <button
                type="button"
                className="codex-local-access-empty-action"
                onClick={() => void syncAllAccounts()}
                disabled={actionBusy || accounts.length === 0 || !collection}
              >
                <FolderPlus size={14} />
                <span>同步当前账号</span>
              </button>
            </div>
          ) : (
            <>
              {previewAccounts.map((account) => {
                const presentation = buildQoderAccountPresentation(account, t);
                const primaryQuota = presentation.quotaItems[0];
                return (
                  <div
                    key={account.id}
                    className="folder-preview-item codex-local-access-member codex-local-access-member--single-quota"
                  >
                    <span
                      className="folder-preview-email codex-local-access-member-email"
                      title={maskAccountText(presentation.displayName)}
                    >
                      {maskAccountText(presentation.displayName)}
                    </span>
                    <span
                      className={`codex-local-access-member-text codex-local-access-member-quota ${primaryQuota?.quotaClass || 'unknown'}`}
                      title={primaryQuota?.label || '额度'}
                    >
                      {primaryQuota?.valueText || '--'}
                    </span>
                    <span className={`codex-local-access-member-plan tier-badge ${presentation.planClass || 'unknown'}`}>
                      {presentation.planLabel}
                    </span>
                    <button
                      type="button"
                      className="folder-preview-remove-btn"
                      onClick={() => void handleRemoveAccount(account.id)}
                      title="从 API 服务移除"
                      disabled={actionBusy}
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                );
              })}
              {hiddenCount > 0 && (
                <button type="button" className="folder-preview-item more" onClick={openPanel} title="查看全部成员">
                  +{hiddenCount}
                </button>
              )}
            </>
          )}
        </div>

        {summaryAccountPresentation && (
          <div className="codex-local-access-pool-row" aria-label="账号池摘要">
            <div className="codex-local-access-pool-pill">
              <strong>
                {summaryAccountPresentation.planLabel} ({state?.memberCount ?? 0})
              </strong>
              <span>
                {summaryPrimaryQuota?.label ?? '额度'} {summaryPrimaryQuota?.valueText ?? '--'}
              </span>
              {summarySecondaryQuota && (
                <span>
                  {summarySecondaryQuota.label} {summarySecondaryQuota.valueText ?? '--'}
                </span>
              )}
            </div>
          </div>
        )}

        {(error || state?.lastError) && (
          <div className="quota-error-inline">
            <CircleAlert size={14} />
            <span>{error || state?.lastError}</span>
          </div>
        )}

        <div className="codex-card-bottom codex-local-access-card-bottom">
          <span className="card-date">{footerHint}</span>
          <div className="card-footer codex-local-access-footer">
            <div className="card-actions">
              <button
                className="card-action-btn"
                onClick={() => void syncAllAccounts()}
                title="同步账号池"
                disabled={actionBusy || accounts.length === 0 || !collection}
              >
                {loading ? <RefreshCw size={14} className="loading-spinner" /> : <FolderPlus size={14} />}
              </button>
              <button
                className="card-action-btn"
                onClick={() => void handleRotateKey()}
                title="轮换 API Key"
                disabled={actionBusy || !collection}
              >
                <KeyRound size={14} />
              </button>
              <button
                className="card-action-btn"
                onClick={openPanel}
                title="服务面板"
                disabled={!collection}
              >
                <Database size={14} />
              </button>
              <button
                className="card-action-btn"
                onClick={() => void handleRefreshState()}
                title="刷新状态"
                disabled={actionBusy}
              >
                <RefreshCw size={14} className={loading ? 'loading-spinner' : ''} />
              </button>
              <button
                className={`card-action-btn ${enabled ? '' : 'success'}`}
                onClick={() => void handleToggle()}
                title={enabled ? '停用服务' : '启用服务'}
                disabled={actionBusy}
              >
                <Power size={14} />
              </button>
            </div>
          </div>
        </div>
      </div>

      {showPanel && (
        <div
          className="modal-overlay codex-local-access-modal-overlay codex-local-access-modal-overlay-panel"
          onClick={closePanel}
        >
          <div
            className="modal codex-local-access-modal codex-local-access-modal-panel"
            role="dialog"
            aria-modal="true"
            aria-labelledby="qoder-local-access-panel-title"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="modal-header codex-local-access-modal-header">
              <div className="codex-local-access-header-main">
                <div className="group-account-picker-title">
                  <Server size={20} />
                  <span id="qoder-local-access-panel-title">Qoder API 服务面板</span>
                </div>
                <div className="codex-local-access-header-meta">
                  <div className="codex-local-access-header-badges">
                    <span className={`codex-local-access-status ${statusTone}`}>{statusText}</span>
                    <span className="codex-local-access-subtle-badge">{state?.memberCount ?? 0} 个账号</span>
                    <span className="codex-local-access-subtle-badge">
                      {resolveAccessScopeLabel(collection?.accessScope)}
                    </span>
                  </div>
                </div>
              </div>
              <button
                className="modal-close codex-local-access-close"
                onClick={closePanel}
                disabled={actionBusy}
                aria-label={t('common.close', '关闭')}
              >
                <X size={18} />
              </button>
            </div>

            <div className="modal-body codex-local-access-modal-body">
              {notice && (
                <div className="codex-local-access-inline-success">
                  <Check size={14} />
                  <span>{notice}</span>
                </div>
              )}
              {error && (
                <div className="codex-local-access-inline-error">
                  <CircleAlert size={14} />
                  <span>{error}</span>
                </div>
              )}
              {state?.lastError && !error && (
                <div className="codex-local-access-inline-error">
                  <CircleAlert size={14} />
                  <span>{state.lastError}</span>
                </div>
              )}
              <div className="codex-local-access-inline-info">
                <CircleAlert size={14} />
                <span>当前实现会把 OpenAI 兼容请求转换成 Qoder 上游请求，并按所选调度策略在账号池中转发。</span>
              </div>

              <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-summary-block">
                <div className="codex-local-access-summary-head">
                  <div className="codex-local-access-section-title">
                    <Activity size={16} />
                    <span>服务概览</span>
                  </div>
                  <div className="codex-local-access-summary-actions">
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => void handleClearStats()}
                      disabled={actionBusy || !collection}
                    >
                      清空统计
                    </button>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => void handleRefreshState()}
                      disabled={actionBusy}
                    >
                      刷新状态
                    </button>
                  </div>
                </div>
                <div className="codex-local-access-stats-grid">
                  {summaryStats.map((item) => (
                    <div key={item.key} className={`codex-local-access-stat-card codex-local-access-stat-card-${item.key}`}>
                      <span className="codex-local-access-stat-label">{item.label}</span>
                      <strong>{item.value}</strong>
                      <span className="codex-local-access-stat-sub">{item.detail}</span>
                    </div>
                  ))}
                </div>
              </section>

              <div className="codex-local-access-panel-grid">
                <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-config-section">
                  <div className="codex-local-access-section-title">
                    <Gauge size={16} />
                    <span>服务配置</span>
                  </div>
                  {collection ? (
                    <>
                      <div className="codex-local-access-config-grid">
                        <div className="codex-local-access-config-card codex-local-access-config-card-base">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">Base URL</span>
                            <div className="codex-local-access-config-actions">
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleCopy('baseUrl', baseUrl)}
                                title={t('common.copy', '复制')}
                                disabled={!baseUrl}
                              >
                                {copiedField === 'baseUrl' ? <Check size={14} /> : <Copy size={14} />}
                              </button>
                            </div>
                          </div>
                          <code className="codex-local-access-code" title={baseUrl || '未启用'}>
                            {baseUrl || '未启用'}
                          </code>
                        </div>

                        <div className="codex-local-access-config-card codex-local-access-config-card-key">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">API Key</span>
                            <div className="codex-local-access-config-actions">
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => setKeyVisible((current) => !current)}
                                title={keyVisible ? '隐藏密钥' : '显示密钥'}
                              >
                                {keyVisible ? <EyeOff size={14} /> : <Eye size={14} />}
                              </button>
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleCopy('apiKey', apiKey)}
                                title={t('common.copy', '复制')}
                              >
                                {copiedField === 'apiKey' ? <Check size={14} /> : <Copy size={14} />}
                              </button>
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleRotateKey()}
                                title="轮换密钥"
                                disabled={actionBusy}
                              >
                                <KeyRound size={14} />
                              </button>
                            </div>
                          </div>
                          <code className="codex-local-access-code" title={apiKey || '未初始化'}>
                            {apiKeyDisplay}
                          </code>
                        </div>

                        <div className="codex-local-access-config-card codex-local-access-config-card-port codex-local-access-port-card">
                          <div className="codex-local-access-config-head">
                            <label className="codex-local-access-config-label" htmlFor="qoder-local-access-port">
                              监听端口
                            </label>
                          </div>
                          <div className="codex-local-access-port-row">
                            <input
                              id="qoder-local-access-port"
                              type="number"
                              min={1}
                              max={65535}
                              value={portInput}
                              onChange={(event) => setPortInput(event.target.value)}
                              disabled={actionBusy}
                            />
                            <button
                              type="button"
                              className="btn btn-primary btn-sm"
                              onClick={() => void handleUpdatePort()}
                              disabled={actionBusy || !portInput}
                            >
                              保存端口
                            </button>
                          </div>
                        </div>

                        <div className="codex-local-access-config-card codex-local-access-config-card-model">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">默认模型</span>
                            <span className="codex-local-access-view-only-badge">只读</span>
                          </div>
                          <div className="codex-local-access-model-row">
                            <code className="codex-local-access-code" title={modelId}>{modelId}</code>
                          </div>
                        </div>
                      </div>

                      <div className="codex-local-access-config-extra-grid">
                        <div className="codex-local-access-config-card codex-local-access-config-card-root">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">监听范围</span>
                          </div>
                          <SingleSelectDropdown
                            value={collection.accessScope}
                            options={ACCESS_SCOPE_OPTIONS}
                            onChange={(value) => void handleUpdateAccessScope(value as QoderLocalAccessScope)}
                            disabled={actionBusy}
                            ariaLabel="监听范围"
                            className="codex-local-access-address-select"
                            menuClassName="codex-local-access-address-menu"
                          />
                        </div>
                        <div className="codex-local-access-config-card codex-local-access-config-card-root">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">调度策略</span>
                          </div>
                          <div className="codex-local-access-routing-main">
                            <SingleSelectDropdown
                              value={collection.routingStrategy}
                              options={ROUTING_STRATEGY_OPTIONS}
                              onChange={(value) => void handleUpdateRoutingStrategy(value as QoderLocalAccessRoutingStrategy)}
                              disabled={actionBusy}
                              ariaLabel="调度策略"
                            />
                          </div>
                        </div>
                        <div className="codex-local-access-config-card codex-local-access-config-card-root">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">支持端点</span>
                          </div>
                          <code className="codex-local-access-code" title="/v1/models · /v1/chat/completions">
                            /v1/models · /v1/chat/completions
                          </code>
                        </div>
                        <div className="codex-local-access-config-card codex-local-access-config-card-root">
                          <div className="codex-local-access-config-head">
                            <span className="codex-local-access-config-label">转发方式</span>
                          </div>
                          <code className="codex-local-access-code" title={resolveRoutingStrategyLabel(collection.routingStrategy)}>
                            {resolveRoutingStrategyLabel(collection.routingStrategy)}
                          </code>
                        </div>
                      </div>
                    </>
                  ) : (
                    <div className="group-account-empty">启用服务后才会生成配置。</div>
                  )}
                </section>

                <section className="codex-local-access-section codex-local-access-section-surface codex-local-access-account-stats-section">
                  <div className="codex-local-access-section-head">
                    <div className="codex-local-access-section-title">
                      <Server size={16} />
                      <span>账号池</span>
                    </div>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => void syncAllAccounts()}
                      disabled={actionBusy || accounts.length === 0 || !collection}
                    >
                      同步当前账号池
                    </button>
                  </div>
                  <div className="codex-local-access-account-stats">
                    {memberAccounts.length === 0 ? (
                      <div className="group-account-empty">当前账号池为空，请先同步账号。</div>
                    ) : (
                      memberAccounts.map((account) => {
                        const presentation = buildQoderAccountPresentation(account, t);
                        return (
                          <div key={account.id} className="codex-local-access-account-stat-row">
                            <div className="codex-local-access-account-stat-top">
                              <div className="codex-local-access-account-stat-main">
                                <span className="group-account-email" title={maskAccountText(presentation.displayName)}>
                                  {maskAccountText(presentation.displayName)}
                                </span>
                                <span className={`tier-badge ${presentation.planClass || 'unknown'}`}>
                                  {presentation.planLabel}
                                </span>
                                {currentAccountId === account.id && <span className="current-tag">当前</span>}
                              </div>
                              <button
                                type="button"
                                className="folder-icon-btn"
                                onClick={() => void handleRemoveAccount(account.id)}
                                title="从 API 服务移除"
                                disabled={actionBusy}
                              >
                                <Trash2 size={14} />
                              </button>
                            </div>
                            <div className="codex-local-access-account-stat-block codex-local-access-account-stat-block-metrics">
                              <div className="codex-local-access-account-stat-metrics">
                                {presentation.quotaItems.map((item) => (
                                  <span key={item.key} className="codex-local-access-account-stat-pill">
                                    {item.label}: {item.valueText || '--'}
                                  </span>
                                ))}
                                {presentation.cycleText && (
                                  <span className="codex-local-access-account-stat-pill">周期: {presentation.cycleText}</span>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </section>
              </div>
            </div>

            <div className="modal-footer codex-local-access-modal-footer">
              <button className="btn btn-secondary" onClick={closePanel} disabled={actionBusy}>
                {t('common.close', '关闭')}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
