# Fork Sync Verification: Kiro/Qoder API Service

This fork carries local API service features for Kiro and Qoder. Upstream regularly changes the Codex local-access UI, page layout, Tauri command registration, and shared CSS. After every upstream sync, verify that Kiro/Qoder still follow the Codex API service behavior and that the reverse proxy chain still works.

## Sync Procedure

1. Fetch the branch you plan to sync from.

```bash
# Use upstream/main if a dedicated upstream remote is configured.
# Use origin/main when the fork remote already contains the upstream merge.
SYNC_REF=origin/main
git fetch origin main
```

2. Review upstream changes that can affect local API service behavior.

```bash
git diff --name-status HEAD..$SYNC_REF -- \
  'src-tauri/src/**/*local_access*' \
  'src/**/*LocalAccess*' \
  'src/pages/KiroAccountsPage.tsx' \
  'src/pages/QoderAccountsPage.tsx' \
  'src/styles/pages/codex.css' \
  'src-tauri/src/lib.rs' \
  'src-tauri/src/modules/mod.rs' \
  'src-tauri/src/models/mod.rs'
```

3. Rebase local fork commits onto the updated remote branch.

```bash
git rebase $SYNC_REF
```

4. Run the fork-specific static guard.

```bash
npm run verify:local-access-sync
```

5. Run normal compile checks.

```bash
npm run typecheck
npm run build
cd src-tauri && cargo check && cd ..
```

6. For release validation, build the Tauri app.

```bash
npm run tauri build
```

If updater signing is not configured locally, the Tauri build may finish app/DMG generation and then fail at updater signing. Treat generated app bundle success plus the signing error as sufficient for local validation, but do not publish unsigned release artifacts.

The static guard verifies that the fork-only UI, Tauri command registration, and OpenAI-compatible `/v1` gateway paths are still present. It is not a substitute for the runtime chain check below.

## Required Manual UI Checks

After starting the app with `npm run tauri dev` or the installed app:

1. Kiro Accounts overview shows an API service card matching the Codex card layout.
2. Qoder Accounts overview shows an API service card matching the Codex card layout.
3. Kiro and Qoder cards expose enable/disable controls and the controls update state.
4. Qoder card still exposes routing strategy and access scope controls.
5. The card panel can copy base URL/API key, rotate key, update port, clear stats, and sync account pool.
6. Layout remains correct in both grid and list modes.

## Required Runtime Chain Checks

Qoder must remain directly usable by Claude/free-code through claudex:

```bash
NO_PROXY=127.0.0.1,localhost,::1 no_proxy=127.0.0.1,localhost,::1 claudex proxy start
```

In another terminal:

```bash
cd ~/Desktop/idea/github/free-code
env \
  http_proxy=http://127.0.0.1:7897 \
  https_proxy=http://127.0.0.1:7897 \
  all_proxy=socks5://127.0.0.1:7897 \
  NO_PROXY=127.0.0.1,localhost,::1 \
  no_proxy=127.0.0.1,localhost,::1 \
  ANTHROPIC_BASE_URL=http://127.0.0.1:13456/proxy/qoder \
  ANTHROPIC_AUTH_TOKEN=claudex-passthrough \
  ~/.local/bin/free-code --dangerously-skip-permissions --bare \
    --model performance \
    --output-format json \
    --no-session-persistence \
    -p "Reply with exactly FINAL_CHAIN_OK and nothing else."
```

Expected result:

```json
{"result":"FINAL_CHAIN_OK","is_error":false}
```

Claudex logs should show forwarding to Cockpit Tools Qoder:

```text
url=http://127.0.0.1:8963/v1/chat/completions
status=200 OK
```

## High-Risk Upstream Files

Treat changes in these files as requiring extra review:

- `src/components/CodexLocalAccessModal.tsx`
- `src/components/CodexLocalAccessModal.css`
- `src/services/codexLocalAccessService.ts`
- `src-tauri/src/modules/codex_local_access.rs`
- `src-tauri/src/models/codex_local_access.rs`
- `src-tauri/src/lib.rs`
- `src/styles/pages/codex.css`
- `src/pages/KiroAccountsPage.tsx`
- `src/pages/QoderAccountsPage.tsx`
- `src-tauri/src/modules/qoder_local_access.rs`
- `src-tauri/src/modules/kiro_local_access.rs`

## Current Sync Assessment

The upstream update rebased under commit `2956ca2` touched Codex local access, shared CSS, Kiro/Qoder pages, and Tauri command registration. It did not remove the fork-specific Kiro/Qoder local-access wiring after rebase. The required static guard should pass before future sync pushes.
