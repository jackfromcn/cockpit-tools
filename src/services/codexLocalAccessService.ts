import { invoke } from '@tauri-apps/api/core';
import type {
  CodexLocalAccessApiKeyInput,
  CodexLocalAccessPortCleanupResult,
  CodexLocalAccessRoutingStrategy,
  CodexLocalAccessState,
} from '../types/codexLocalAccess';

export async function getCodexLocalAccessState(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_get_state');
}

export async function saveCodexLocalAccessAccounts(
  accountIds: string[],
  restrictFreeAccounts: boolean,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_save_accounts', {
    accountIds,
    restrictFreeAccounts,
  });
}

export async function removeCodexLocalAccessAccount(
  accountId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_remove_account', { accountId });
}

export async function createCodexLocalAccessApiKey(
  input: CodexLocalAccessApiKeyInput,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_create_api_key', {
    name: input.name,
    monthlyTokenLimit: input.monthlyTokenLimit,
  });
}

export async function updateCodexLocalAccessApiKey(
  apiKeyId: string,
  input: CodexLocalAccessApiKeyInput & { enabled: boolean },
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_api_key', {
    apiKeyId,
    name: input.name,
    enabled: input.enabled,
    monthlyTokenLimit: input.monthlyTokenLimit,
  });
}

export async function rotateCodexLocalAccessApiKey(
  apiKeyId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_rotate_api_key', { apiKeyId });
}

export async function deleteCodexLocalAccessApiKey(
  apiKeyId: string,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_delete_api_key', { apiKeyId });
}

export async function clearCodexLocalAccessStats(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_clear_stats');
}

export async function prepareCodexLocalAccessForRestart(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_prepare_restart');
}

export async function killCodexLocalAccessPort(): Promise<CodexLocalAccessPortCleanupResult> {
  return await invoke('codex_local_access_kill_port');
}

export async function updateCodexLocalAccessPort(
  port: number,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_port', { port });
}

export async function updateCodexLocalAccessRoutingStrategy(
  strategy: CodexLocalAccessRoutingStrategy,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_update_routing_strategy', { strategy });
}

export async function setCodexLocalAccessEnabled(
  enabled: boolean,
): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_set_enabled', { enabled });
}

export async function activateCodexLocalAccess(): Promise<CodexLocalAccessState> {
  return await invoke('codex_local_access_activate');
}
