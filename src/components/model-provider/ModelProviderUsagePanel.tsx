import { useTranslation } from 'react-i18next';
import {
  formatModelProviderUsageInteger,
  formatModelProviderUsageMoney,
  formatModelProviderUsageTokenCount,
  resolveModelProviderUsageMode,
  type ModelProviderUsageSummary,
} from '../../services/modelProviderUsageService';

interface ModelProviderUsagePanelProps {
  summary?: ModelProviderUsageSummary | null;
  loading?: boolean;
  error?: string;
  unavailable?: boolean;
  className?: string;
  variant?: 'card' | 'table';
}

export function ModelProviderUsagePanel({
  summary,
  loading = false,
  error,
  unavailable = false,
  className,
  variant,
}: ModelProviderUsagePanelProps) {
  const { t } = useTranslation();
  const usageMode = resolveModelProviderUsageMode(summary ?? undefined);
  const isSupportedUsage = usageMode === 'sub2api' || usageMode === 'new_api';
  const classNames = [
    'codex-api-key-usage-panel',
    usageMode ?? 'sub2api',
    variant,
    className,
  ].filter(Boolean).join(' ');

  if (!summary) {
    const emptyText = loading
      ? t('codex.modelProviders.usage.loading', '正在查询额度...')
      : error
        ? error
        : unavailable
          ? t('codex.modelProviders.usage.noKey', '暂无可查询额度')
          : t('codex.modelProviders.usage.pending', '等待查询额度');
    return (
      <div className={`${classNames} empty`}>
        <div className="codex-api-key-usage-empty" title={emptyText}>
          {emptyText}
        </div>
      </div>
    );
  }

  if (!isSupportedUsage) {
    return null;
  }

  const balanceText = formatModelProviderUsageMoney(
    summary.remaining ?? summary.balance ?? summary.quotaRemaining,
    summary.unit,
  );
  const todayRequests = formatModelProviderUsageInteger(summary.todayRequests);
  const todayTokens = formatModelProviderUsageTokenCount(summary.todayTotalTokens);

  return (
    <div className={classNames}>
      <div className="codex-api-key-usage-grid">
        <div>
          <span>{t('codex.modelProviders.usage.accountBalance', '账户余额')}</span>
          <strong>{balanceText}</strong>
        </div>
        <div>
          <span>{t('codex.modelProviders.usage.fields.todayRequests', '今日请求')}</span>
          <strong>{todayRequests}</strong>
        </div>
        <div>
          <span>{t('codex.modelProviders.usage.fields.todayTokens', '今日 Token')}</span>
          <strong>{todayTokens}</strong>
        </div>
      </div>
    </div>
  );
}
