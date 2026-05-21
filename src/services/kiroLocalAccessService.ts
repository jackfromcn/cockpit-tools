import { invoke } from '@tauri-apps/api/core';

export interface KiroLocalAccessState {
  collection: {
    enabled: boolean;
    port: number;
    apiKey: string;
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

export interface KiroLocalAccessTestFailure {
  title: string;
  stage: string;
  cause: string;
  suggestion: string;
  status: number | null;
  modelId: string | null;
  detail: string | null;
}

export interface KiroLocalAccessTestResult {
  modelId: string | null;
  latencyMs: number | null;
  output: string | null;
  failure: KiroLocalAccessTestFailure | null;
}

export async function getState(): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_get_state');
}

export async function setEnabled(enabled: boolean): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_set_enabled', { enabled });
}

export async function saveAccounts(accountIds: string[]): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_save_accounts', { accountIds });
}

export async function removeAccount(accountId: string): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_remove_account', { accountId });
}

export async function rotateApiKey(): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_rotate_api_key');
}

export async function updatePort(port: number): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_update_port', { port });
}

export async function clearStats(): Promise<KiroLocalAccessState> {
  return invoke('kiro_local_access_clear_stats');
}

export async function test(): Promise<KiroLocalAccessTestResult> {
  return invoke('kiro_local_access_test');
}
