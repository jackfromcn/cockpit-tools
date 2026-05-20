import { useCallback, useEffect, useState } from 'react';
import { Copy, Check, RefreshCw, Power, PowerOff } from 'lucide-react';
import * as qoderLocalAccessService from '../services/qoderLocalAccessService';
import type { QoderLocalAccessState } from '../services/qoderLocalAccessService';

interface Props {
  accountIds: string[];
}

export function QoderLocalAccessCard({ accountIds }: Props) {
  const [state, setState] = useState<QoderLocalAccessState | null>(null);
  const [copied, setCopied] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const s = await qoderLocalAccessService.getState();
      setState(s);
    } catch (e) {
      console.error('[QoderLocalAccess] getState failed:', e);
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);

  const handleToggle = async () => {
    setLoading(true);
    try {
      const enabled = !state?.collection?.enabled;
      const s = await qoderLocalAccessService.setEnabled(enabled);
      setState(s);
      if (enabled && accountIds.length > 0) {
        const s2 = await qoderLocalAccessService.saveAccounts(accountIds);
        setState(s2);
      }
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = (text: string, key: string) => {
    navigator.clipboard.writeText(text);
    setCopied(key);
    setTimeout(() => setCopied(null), 2000);
  };

  const handleRotateKey = async () => {
    const s = await qoderLocalAccessService.rotateApiKey();
    setState(s);
  };

  const handleSyncAccounts = async () => {
    if (accountIds.length > 0) {
      const s = await qoderLocalAccessService.saveAccounts(accountIds);
      setState(s);
    }
  };

  const enabled = state?.collection?.enabled ?? false;
  const running = state?.running ?? false;
  const baseUrl = state?.baseUrl || '';
  const apiKey = state?.collection?.apiKey || '';
  const stats = state?.stats?.totals;

  return (
    <div className="ghcp-flow-notice" style={{ marginBottom: 12 }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          {running ? <Power size={16} color="var(--color-success)" /> : <PowerOff size={16} color="var(--color-text-tertiary)" />}
          <strong>API 服务 (Local Access)</strong>
          {running && <span style={{ fontSize: 11, color: 'var(--color-success)', fontWeight: 500 }}>运行中</span>}
        </div>
        <button
          type="button"
          className={`ghcp-btn ghcp-btn-sm ${enabled ? 'ghcp-btn-danger' : 'ghcp-btn-primary'}`}
          onClick={handleToggle}
          disabled={loading}
        >
          {enabled ? '停用' : '启用'}
        </button>
      </div>

      {enabled && state?.collection && (
        <div style={{ fontSize: 12, color: 'var(--color-text-secondary)', display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ minWidth: 60 }}>Base URL:</span>
            <code style={{ flex: 1, fontSize: 11 }}>{baseUrl}</code>
            <button type="button" className="ghcp-btn-icon" onClick={() => handleCopy(baseUrl, 'url')} title="复制">
              {copied === 'url' ? <Check size={12} /> : <Copy size={12} />}
            </button>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ minWidth: 60 }}>API Key:</span>
            <code style={{ flex: 1, fontSize: 11 }}>{apiKey.slice(0, 20)}...</code>
            <button type="button" className="ghcp-btn-icon" onClick={() => handleCopy(apiKey, 'key')} title="复制">
              {copied === 'key' ? <Check size={12} /> : <Copy size={12} />}
            </button>
            <button type="button" className="ghcp-btn-icon" onClick={handleRotateKey} title="轮换 Key">
              <RefreshCw size={12} />
            </button>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ minWidth: 60 }}>模型:</span>
            <span>{state.modelIds.join(', ')}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ minWidth: 60 }}>账号池:</span>
            <span>{state.memberCount} 个账号</span>
            <button type="button" className="ghcp-btn-icon" onClick={handleSyncAccounts} title="同步当前账号列表">
              <RefreshCw size={12} />
            </button>
          </div>
          {stats && stats.requestCount > 0 && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, opacity: 0.7 }}>
              <span style={{ minWidth: 60 }}>统计:</span>
              <span>请求 {stats.requestCount} | 成功 {stats.successCount} | 失败 {stats.failureCount}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
