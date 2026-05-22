# Fork Sync Verification: Kiro/Qoder API Service

This fork carries local API service features for Kiro and Qoder. Upstream regularly changes the Codex local-access UI, page layout, Tauri command registration, and shared CSS. After every upstream sync, verify that Kiro/Qoder still follow the Codex API service behavior and that the reverse proxy chain still works.

Repository roles:

- Fork repository: `jackfromcn/cockpit-tools` (`origin`)
- Fork source repository: `jlcodes99/cockpit-tools` (`upstream`)
- Protected fork-only requirements: Kiro/Qoder API service cards, local OpenAI-compatible `/v1` gateways, and the Qoder chain used by `free-code -> claudex -> Cockpit Tools(qoder)`.

## Sync Procedure

1. Make sure the source repository remote exists and cannot be pushed to accidentally.

```bash
git remote add upstream https://github.com/jlcodes99/cockpit-tools.git 2>/dev/null || \
  git remote set-url upstream https://github.com/jlcodes99/cockpit-tools.git
git remote set-url --push upstream DISABLED
```

2. Fetch the fork and source repository.

```bash
git fetch origin main
git fetch upstream main
```

3. Run the read-only upstream sync preflight.

```bash
npm run sync:upstream:check
```

The preflight compares `merge-base(HEAD, upstream/main)..upstream/main`, not `HEAD..upstream/main`. This avoids incorrectly treating fork-only Kiro/Qoder files as upstream deletions.

4. Merge the source repository into the fork.

```bash
git merge upstream/main
```

Prefer merge for routine fork syncs so the fork-only feature history remains visible and reversible. Resolve conflicts by preserving the protected Kiro/Qoder API service behavior unless upstream has intentionally replaced that capability.

5. Run the fork-specific static guard.

```bash
npm run verify:local-access-sync
```

6. Run normal compile checks.

```bash
npm run typecheck
npm run build
cd src-tauri && cargo check && cd ..
```

7. For release validation, build the Tauri app.

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

As of the sync preflight after commit `325efd2`, `upstream/main` was at `9e7bac8` and had two source commits not merged into the fork. Since the merge-base `78850e0`, upstream changed only `announcements.json`; it did not touch the high-risk Kiro/Qoder local-access paths. The protected fork-side changes remain local to the fork and should be preserved by a normal merge.
