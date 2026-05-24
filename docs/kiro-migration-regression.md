# Kiro Migration Regression Constraints

This note captures a concrete session-bloat failure mode observed while investigating Kiro reverse-proxy migration work.

## Reference Session

- Thread id: `019e5842-9cb5-7641-8383-9e4bcf4fa7a9`
- Session file: `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl`
- Size: about `548 KB`
- Lines: `152`

## Observed Bloat Sources

1. Large `function_call_output` entries dominate the file.
   - Roughly `316 KB / 561 KB` came from raw tool outputs written inline.
   - Representative lines in the session file:
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:39`
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:55`
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:128`
   - These lines contain multi-KB source dumps, search output, and even nested serialized conversation fragments.

2. `session_meta` was re-appended multiple times.
   - Three separate `session_meta` records each carried the full `base_instructions` payload at about `15 KB` each.
   - Representative lines:
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:1`
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:112`
     - `/Users/xuwencheng/.antigravity_cockpit/instances/codex/3219b6d2cafb6fe7/sessions/2026/05/24/rollout-2026-05-24T12-33-41-019e5842-9cb5-7641-8383-9e4bcf4fa7a9.jsonl:122`

3. Startup prompt payload is large, but not the primary growth vector.
   - Developer message at line `3`: about `31 KB`
   - User message containing full `AGENTS.md` at line `4`: about `38 KB`
   - This is a one-time cost, unlike the repeated tool-output inflation.

## What This Means For Kiro Migration

When aligning Cockpit Tools Kiro proxy behavior with KiroAccountManager, we must not introduce the same session-growth pattern through proxy debugging or upstream replay behavior.

## Regression Constraints

1. Do not persist full upstream request or response bodies into user-visible conversation history.
   - Large Kiro payloads and eventstream aggregates may be logged to disk for debugging, but they must not be injected back into conversational `tool_result` content.

2. Do not inline multi-KB raw tool output when a summarized result plus file reference is enough.
   - If output exceeds a safe threshold, store it externally and keep only a compact preview in the session stream.

3. Do not re-append full `session_meta` or equivalent base instructions on every restore, compact, or replay step.
   - Reuse metadata by reference where possible.
   - If metadata must be rewritten, dedupe identical blocks.

4. Do not embed nested serialized session JSON into proxy/tool output.
   - This creates multiplicative growth and makes later compaction much less effective.

5. Keep Kiro proxy diagnostics out of the model-visible transcript by default.
   - Upstream payload logging should stay in app logs, not in replayed assistant/tool content.

## Validation Checklist

Before considering the Kiro migration stable, verify:

- A failing Kiro request does not write the full upstream payload into chat history.
- A retry or replay does not duplicate base instructions or session metadata blocks.
- Large tool outputs are truncated or externalized instead of copied verbatim into session content.
- Session size growth remains roughly proportional to the number of user/assistant turns, not to the size of local diagnostics.
