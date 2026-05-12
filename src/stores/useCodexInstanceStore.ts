import * as codexInstanceService from '../services/codexInstanceService';
import type {
  CodexSessionVisibilityRepairSummary,
  CodexInstanceThreadSyncSummary,
  CodexInstanceTargetThreadSyncSummary,
  CodexSharedChatCatalogRecord,
  CodexSharedChatVisibilitySummary,
  CodexSessionRecord,
  CodexSessionTokenStats,
  CodexSessionTrashSummary,
  CodexTrashedSessionRecord,
  CodexSessionRestoreSummary,
} from '../types/codex';
import { createInstanceStore, type InstanceStoreState } from './createInstanceStore';

type CodexInstanceStoreState = InstanceStoreState & {
  syncThreadsAcrossInstances: () => Promise<CodexInstanceThreadSyncSummary>;
  syncSessionsToInstance: (
    sessionIds: string[],
    targetInstanceId: string,
  ) => Promise<CodexInstanceTargetThreadSyncSummary>;
  repairSessionVisibilityAcrossInstances: () => Promise<CodexSessionVisibilityRepairSummary>;
  listSessionsAcrossInstances: () => Promise<CodexSessionRecord[]>;
  listSharedChatCatalog: (instanceId: string) => Promise<CodexSharedChatCatalogRecord[]>;
  ensureSharedChatVisibility: (instanceId: string) => Promise<CodexSharedChatVisibilitySummary>;
  getSessionTokenStatsAcrossInstances: (sessionIds: string[]) => Promise<CodexSessionTokenStats[]>;
  moveSessionsToTrashAcrossInstances: (sessionIds: string[]) => Promise<CodexSessionTrashSummary>;
  listTrashedSessionsAcrossInstances: () => Promise<CodexTrashedSessionRecord[]>;
  restoreSessionsFromTrashAcrossInstances: (sessionIds: string[]) => Promise<CodexSessionRestoreSummary>;
};

type CodexInstanceStoreHook = {
  (): CodexInstanceStoreState;
  <T>(selector: (state: CodexInstanceStoreState) => T): T;
  getState: () => CodexInstanceStoreState;
  setState: (partial: Partial<CodexInstanceStoreState>) => void;
};

const baseStore = createInstanceStore(codexInstanceService, 'agtools.codex.instances.cache');
const typedBaseStore = baseStore as unknown as CodexInstanceStoreHook;

const syncThreadsAcrossInstances = async (): Promise<CodexInstanceThreadSyncSummary> => {
  const summary = await codexInstanceService.syncThreadsAcrossInstances();
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

const syncSessionsToInstance = async (
  sessionIds: string[],
  targetInstanceId: string,
): Promise<CodexInstanceTargetThreadSyncSummary> => {
  const summary = await codexInstanceService.syncSessionsToInstance(sessionIds, targetInstanceId);
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

const repairSessionVisibilityAcrossInstances = async (): Promise<CodexSessionVisibilityRepairSummary> => {
  const summary = await codexInstanceService.repairSessionVisibilityAcrossInstances();
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

const listSessionsAcrossInstances = async (): Promise<CodexSessionRecord[]> => {
  return await codexInstanceService.listSessionsAcrossInstances();
};

const listSharedChatCatalog = async (
  instanceId: string,
): Promise<CodexSharedChatCatalogRecord[]> => {
  return await codexInstanceService.listSharedChatCatalog(instanceId);
};

const ensureSharedChatVisibility = async (
  instanceId: string,
): Promise<CodexSharedChatVisibilitySummary> => {
  const summary = await codexInstanceService.ensureSharedChatVisibility(instanceId);
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

const getSessionTokenStatsAcrossInstances = async (
  sessionIds: string[],
): Promise<CodexSessionTokenStats[]> => {
  return await codexInstanceService.getSessionTokenStatsAcrossInstances(sessionIds);
};

const moveSessionsToTrashAcrossInstances = async (
  sessionIds: string[],
): Promise<CodexSessionTrashSummary> => {
  const summary = await codexInstanceService.moveSessionsToTrashAcrossInstances(sessionIds);
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

const listTrashedSessionsAcrossInstances = async (): Promise<CodexTrashedSessionRecord[]> => {
  return await codexInstanceService.listTrashedSessionsAcrossInstances();
};

const restoreSessionsFromTrashAcrossInstances = async (
  sessionIds: string[],
): Promise<CodexSessionRestoreSummary> => {
  const summary = await codexInstanceService.restoreSessionsFromTrashAcrossInstances(sessionIds);
  await typedBaseStore.getState().fetchInstances();
  return summary;
};

typedBaseStore.setState({
  syncThreadsAcrossInstances,
  syncSessionsToInstance,
  repairSessionVisibilityAcrossInstances,
  listSessionsAcrossInstances,
  listSharedChatCatalog,
  ensureSharedChatVisibility,
  getSessionTokenStatsAcrossInstances,
  moveSessionsToTrashAcrossInstances,
  listTrashedSessionsAcrossInstances,
  restoreSessionsFromTrashAcrossInstances,
});

export const useCodexInstanceStore = typedBaseStore;
