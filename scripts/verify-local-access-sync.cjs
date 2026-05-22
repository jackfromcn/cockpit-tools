#!/usr/bin/env node
/*
 * Guard the fork-only Kiro/Qoder local API service integrations after upstream syncs.
 * This is intentionally dependency-free so it can run before install/build steps.
 */
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');

const checks = [
  {
    file: 'src/pages/KiroAccountsPage.tsx',
    patterns: [
      "import { KiroLocalAccessCard } from '../components/KiroLocalAccessCard'",
      '<KiroLocalAccessCard',
      'layoutMode={viewMode}',
      'maskAccountText={maskAccountText}',
    ],
  },
  {
    file: 'src/pages/QoderAccountsPage.tsx',
    patterns: [
      "import { QoderLocalAccessCard } from '../components/QoderLocalAccessCard'",
      '<QoderLocalAccessCard',
      'layoutMode={viewMode}',
      'maskAccountText={maskAccountText}',
    ],
  },
  {
    file: 'src/components/KiroLocalAccessCard.tsx',
    patterns: [
      "import './CodexLocalAccessModal.css'",
      'kiroLocalAccessService.setEnabled',
      'kiroLocalAccessService.saveAccounts',
      'kiroLocalAccessService.test',
      'codex-local-access-card',
      'codex-local-access-header',
      'codex-local-access-header-actions',
      'codex-local-access-card-bottom',
      'API 服务',
      '/v1/models 与 /v1/chat/completions',
    ],
  },
  {
    file: 'src/components/QoderLocalAccessCard.tsx',
    patterns: [
      "import './CodexLocalAccessModal.css'",
      'qoderLocalAccessService.setEnabled',
      'qoderLocalAccessService.saveAccounts',
      'qoderLocalAccessService.updateRoutingStrategy',
      'qoderLocalAccessService.updateAccessScope',
      'codex-local-access-card',
      'codex-local-access-header',
      'codex-local-access-header-actions',
      'codex-local-access-card-bottom',
      'API 服务',
      '/v1/models 与 /v1/chat/completions',
    ],
  },
  {
    file: 'src/services/kiroLocalAccessService.ts',
    patterns: [
      "invoke('kiro_local_access_get_state'",
      "invoke('kiro_local_access_set_enabled'",
      "invoke('kiro_local_access_save_accounts'",
      "invoke('kiro_local_access_update_port'",
      "invoke('kiro_local_access_test'",
    ],
  },
  {
    file: 'src/services/qoderLocalAccessService.ts',
    patterns: [
      "invoke('qoder_local_access_get_state'",
      "invoke('qoder_local_access_set_enabled'",
      "invoke('qoder_local_access_save_accounts'",
      "invoke('qoder_local_access_update_port'",
      "invoke('qoder_local_access_update_routing_strategy'",
      "invoke('qoder_local_access_update_access_scope'",
    ],
  },
  {
    file: 'src-tauri/src/lib.rs',
    patterns: [
      'modules::qoder_local_access::restore_local_access_gateway().await',
      'modules::kiro_local_access::restore_local_access_gateway().await',
      'commands::kiro::kiro_local_access_get_state',
      'commands::kiro::kiro_local_access_set_enabled',
      'commands::kiro::kiro_local_access_test',
      'commands::qoder::qoder_local_access_get_state',
      'commands::qoder::qoder_local_access_set_enabled',
      'commands::qoder::qoder_local_access_update_access_scope',
    ],
  },
  {
    file: 'src-tauri/src/modules/mod.rs',
    patterns: ['pub mod kiro_local_access;', 'pub mod qoder_local_access;'],
  },
  {
    file: 'src-tauri/src/models/mod.rs',
    patterns: ['pub mod kiro_local_access;', 'pub mod qoder_local_access;'],
  },
  {
    file: 'src-tauri/src/commands/kiro.rs',
    patterns: [
      'pub async fn kiro_local_access_get_state',
      'pub async fn kiro_local_access_set_enabled',
      'pub async fn kiro_local_access_save_accounts',
      'pub async fn kiro_local_access_update_port',
      'pub async fn kiro_local_access_test',
    ],
  },
  {
    file: 'src-tauri/src/commands/qoder.rs',
    patterns: [
      'pub async fn qoder_local_access_get_state',
      'pub async fn qoder_local_access_set_enabled',
      'pub async fn qoder_local_access_save_accounts',
      'pub async fn qoder_local_access_update_port',
      'pub async fn qoder_local_access_update_routing_strategy',
      'pub async fn qoder_local_access_update_access_scope',
    ],
  },
  {
    file: 'src-tauri/src/modules/kiro_local_access.rs',
    patterns: [
      'pub async fn restore_local_access_gateway()',
      'pub async fn test_local_access()',
      'parsed.target.starts_with("/v1/chat/completions")',
      'format!("{}/chat/completions", base_url)',
      'kiro-cli',
      'Authorization',
    ],
  },
  {
    file: 'src-tauri/src/modules/qoder_local_access.rs',
    patterns: [
      'pub async fn restore_local_access_gateway()',
      'OpenAI → Qoder 请求格式转换',
      'Qoder SSE → OpenAI 响应格式转换',
      'parsed.target.starts_with("/v1/chat/completions")',
      'QoderLocalAccessScope',
      'QoderLocalAccessRoutingStrategy',
      'Authorization',
    ],
  },
  {
    file: 'src/styles/pages/codex.css',
    patterns: [
      '.codex-local-access-card',
      '.codex-local-access-header',
      '.codex-local-access-header-actions',
      '.codex-local-access-summary-trigger',
      '.codex-local-access-card-bottom',
      '.codex-local-access-footer',
    ],
  },
];

let failures = 0;

function readFile(relativePath) {
  const absolutePath = path.join(root, relativePath);
  try {
    return fs.readFileSync(absolutePath, 'utf8');
  } catch (error) {
    failures += 1;
    console.error(`[missing] ${relativePath}: ${error.message}`);
    return null;
  }
}

for (const check of checks) {
  const content = readFile(check.file);
  if (content == null) continue;

  for (const pattern of check.patterns) {
    if (!content.includes(pattern)) {
      failures += 1;
      console.error(`[missing-pattern] ${check.file}: ${pattern}`);
    }
  }
}

if (failures > 0) {
  console.error(`\nLocal access sync verification failed: ${failures} issue(s).`);
  console.error('Review docs/fork-sync-local-access.md before merging/pushing upstream sync changes.');
  process.exit(1);
}

console.log('Local access sync verification passed.');
