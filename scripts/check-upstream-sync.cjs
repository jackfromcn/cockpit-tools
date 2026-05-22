#!/usr/bin/env node
/*
 * Read-only preflight for syncing the fork from its source repository.
 * It reports upstream commits since the merge-base and highlights files that
 * can affect the fork-only Kiro/Qoder API service requirements.
 */
const { execFileSync } = require('node:child_process');

const upstreamRef = process.argv[2] || process.env.SYNC_REF || 'upstream/main';
const highRiskPaths = [
  'src-tauri/src/**/*local_access*',
  'src/**/*LocalAccess*',
  'src/pages/KiroAccountsPage.tsx',
  'src/pages/QoderAccountsPage.tsx',
  'src/styles/pages/codex.css',
  'src-tauri/src/lib.rs',
  'src-tauri/src/modules/mod.rs',
  'src-tauri/src/models/mod.rs',
];

function git(args, options = {}) {
  return execFileSync('git', args, {
    encoding: 'utf8',
    stdio: options.stdio || ['ignore', 'pipe', 'pipe'],
  }).trim();
}

function printSection(title, body) {
  console.log(`\n## ${title}`);
  console.log(body || '(none)');
}

try {
  git(['rev-parse', '--is-inside-work-tree']);
} catch {
  console.error('This script must be run inside the cockpit-tools git repository.');
  process.exit(1);
}

let mergeBase;
try {
  mergeBase = git(['merge-base', 'HEAD', upstreamRef]);
} catch (error) {
  console.error(`Unable to resolve merge-base with ${upstreamRef}.`);
  console.error('Run `git fetch upstream main` first, or pass another ref:');
  console.error('  npm run sync:upstream:check -- origin/main');
  process.exit(1);
}

const head = git(['rev-parse', '--short', 'HEAD']);
const upstream = git(['rev-parse', '--short', upstreamRef]);
const base = git(['rev-parse', '--short', mergeBase]);
const upstreamCommits = git(['log', '--oneline', `${mergeBase}..${upstreamRef}`]);
const upstreamChangedFiles = git(['diff', '--name-status', `${mergeBase}..${upstreamRef}`]);
const highRiskChanges = git([
  'diff',
  '--name-status',
  `${mergeBase}..${upstreamRef}`,
  '--',
  ...highRiskPaths,
]);
const localForkChanges = git([
  'diff',
  '--name-status',
  `${mergeBase}..HEAD`,
  '--',
  ...highRiskPaths,
]);

console.log(`Fork sync preflight: HEAD ${head}, ${upstreamRef} ${upstream}, merge-base ${base}`);
printSection(`Upstream commits not yet merged from ${upstreamRef}`, upstreamCommits);
printSection('Upstream changed files since merge-base', upstreamChangedFiles);
printSection('High-risk upstream changes for Kiro/Qoder API service', highRiskChanges);
printSection('Fork-side protected local-access changes since merge-base', localForkChanges);

if (upstreamCommits) {
  console.log('\nNext sync command:');
  console.log(`  git merge ${upstreamRef}`);
  console.log('\nThen run:');
  console.log('  npm run verify:local-access-sync');
  console.log('  npm run typecheck');
  console.log('  npm run build');
  console.log('  cd src-tauri && cargo check && cd ..');
} else {
  console.log(`\n${upstreamRef} is already merged into HEAD.`);
}
