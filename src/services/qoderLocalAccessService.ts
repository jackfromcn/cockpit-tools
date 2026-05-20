import { invoke } from '@tauri-apps/api/core';

export interface QoderLocalAccessState {
  collection: {
    enabled: boolean;
    port: number;
    apiKey: string;
    accessScope: string;
    routingStrategy: string;
    accountIds: string[];
    createdAt: number;
    updatedAt: number;
  } | null;
  running: boolean;
  baseUrl: string | null;
  modelIds: string[];
  lastError: string | null;
  memberCount: number;
  stats: {
    since: number;
    updatedAt: number;
    totals: {
      requestCount: number;
      successCount: number;
      failureCount: number;
      totalLatencyMs: number;
    };
  };
}

export async function getState(): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_get_state');
}

export async function setEnabled(enabled: boolean): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_set_enabled', { enabled });
}

export async function saveAccounts(accountIds: string[]): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_save_accounts', { accountIds });
}

export async function removeAccount(accountId: string): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_remove_account', { accountId });
}

export async function rotateApiKey(): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_rotate_api_key');
}

export async function updatePort(port: number): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_update_port', { port });
}

export async function clearStats(): Promise<QoderLocalAccessState> {
  return invoke('qoder_local_access_clear_stats');
}
