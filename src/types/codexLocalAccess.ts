export type CodexLocalAccessRoutingStrategy =
  | 'auto'
  | 'quota_high_first'
  | 'quota_low_first'
  | 'plan_high_first'
  | 'plan_low_first'
  | 'expiry_soon_first';

export interface CodexLocalAccessApiKey {
  id: string;
  name: string;
  key: string;
  enabled: boolean;
  monthlyTokenLimit: number | null;
  createdAt: number;
  updatedAt: number;
  lastUsedAt: number | null;
}

export interface CodexLocalAccessCollection {
  enabled: boolean;
  port: number;
  apiKeys: CodexLocalAccessApiKey[];
  routingStrategy: CodexLocalAccessRoutingStrategy;
  restrictFreeAccounts: boolean;
  accountIds: string[];
  createdAt: number;
  updatedAt: number;
}

export interface CodexLocalAccessApiKeyInput {
  name: string;
  monthlyTokenLimit: number | null;
}

export interface CodexLocalAccessUsageStats {
  requestCount: number;
  successCount: number;
  failureCount: number;
  totalLatencyMs: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  cachedTokens: number;
  reasoningTokens: number;
}

export interface CodexLocalAccessAccountStats {
  accountId: string;
  email: string;
  usage: CodexLocalAccessUsageStats;
  updatedAt: number;
}

export interface CodexLocalAccessApiKeyStats {
  apiKeyId: string;
  apiKeyName: string;
  usage: CodexLocalAccessUsageStats;
  updatedAt: number;
}

export interface CodexLocalAccessStatsWindow {
  since: number;
  updatedAt: number;
  totals: CodexLocalAccessUsageStats;
  accounts: CodexLocalAccessAccountStats[];
  apiKeys: CodexLocalAccessApiKeyStats[];
}

export interface CodexLocalAccessStats {
  since: number;
  updatedAt: number;
  totals: CodexLocalAccessUsageStats;
  accounts: CodexLocalAccessAccountStats[];
  apiKeys: CodexLocalAccessApiKeyStats[];
  daily: CodexLocalAccessStatsWindow;
  weekly: CodexLocalAccessStatsWindow;
  monthly: CodexLocalAccessStatsWindow;
}

export interface CodexLocalAccessState {
  collection: CodexLocalAccessCollection | null;
  running: boolean;
  apiPortUrl: string | null;
  baseUrl: string | null;
  modelIds: string[];
  lastError: string | null;
  memberCount: number;
  stats: CodexLocalAccessStats;
}

export interface CodexLocalAccessPortCleanupResult {
  killedCount: number;
  state: CodexLocalAccessState;
}
