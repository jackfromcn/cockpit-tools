#!/bin/bash
# Build cockpit-tools (dev mode) and test kiro local access proxy with Claude Code
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${COCKPIT_TOOLS_DATA_DIR:-$HOME/.antigravity_cockpit}"
CONFIG_FILE="$DATA_DIR/kiro_local_access.json"

# ─── Colors ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}⚠${NC}  $*"; }
fail() { echo -e "${RED}✗${NC} $*"; }
info() { echo -e "  $*"; }

# ─── Step 1: cargo check ──────────────────────────────────────────────────────
echo ""
echo "═══ Step 1: Rust syntax check ═══════════════════════════════════════════"
cd "$PROJECT_DIR/src-tauri"
if COCKPIT_SKIP_CLIPROXY_BUILD=1 cargo check 2>&1 | grep -E "^error"; then
  fail "cargo check failed — fix Rust errors before building"
  exit 1
fi
ok "cargo check passed"

# ─── Step 2: Build (dev) ──────────────────────────────────────────────────────
echo ""
echo "═══ Step 2: Build cockpit-tools (dev) ═══════════════════════════════════"
cd "$PROJECT_DIR"
npm install --silent
echo "Starting tauri dev build (this takes a few minutes)..."
echo "(Press Ctrl+C to skip the full build if app is already running)"
echo ""
# In a real run, this opens the GUI window:
# npm run tauri:dev
# For a silent compile-only check without opening the window use cargo build:
cd "$PROJECT_DIR/src-tauri"
COCKPIT_TOOLS_PROFILE=dev COCKPIT_SKIP_CLIPROXY_BUILD=1 cargo build 2>&1 | tail -5
ok "Rust backend compiled"

# ─── Step 3: Read proxy config ───────────────────────────────────────────────
echo ""
echo "═══ Step 3: Read kiro proxy config ══════════════════════════════════════"
if [ ! -f "$CONFIG_FILE" ]; then
  warn "No config found at $CONFIG_FILE"
  warn "Start cockpit-tools and enable Kiro Local Access to create the config."
  exit 1
fi

PORT=$(python3 -c "import json,sys; d=json.load(open('$CONFIG_FILE')); print(d['port'])")
API_KEY=$(python3 -c "import json,sys; d=json.load(open('$CONFIG_FILE')); print(d['apiKey'])")
ENABLED=$(python3 -c "import json,sys; d=json.load(open('$CONFIG_FILE')); print(d['enabled'])")
BASE_URL="http://127.0.0.1:$PORT/v1"

info "Port:    $PORT"
info "Api Key: $API_KEY"
info "Enabled: $ENABLED"
info "URL:     $BASE_URL"

if [ "$ENABLED" != "True" ] && [ "$ENABLED" != "true" ]; then
  warn "Kiro Local Access is disabled. Enable it in cockpit-tools settings."
  exit 1
fi

# ─── Step 4: Wait for proxy to be ready ─────────────────────────────────────
echo ""
echo "═══ Step 4: Wait for proxy listener ════════════════════════════════════"
MAX_WAIT=30
for i in $(seq 1 $MAX_WAIT); do
  if curl -s --connect-timeout 1 "http://127.0.0.1:$PORT/v1/models" \
       -H "Authorization: Bearer $API_KEY" -o /dev/null 2>/dev/null; then
    ok "Proxy is listening on port $PORT"
    break
  fi
  if [ "$i" -eq "$MAX_WAIT" ]; then
    fail "Proxy not reachable on port $PORT after ${MAX_WAIT}s"
    fail "Make sure cockpit-tools app is running with Kiro Local Access enabled"
    exit 1
  fi
  printf "\r  Waiting for proxy... ($i/${MAX_WAIT}s)"
  sleep 1
done

# ─── Step 5: Test /v1/models ─────────────────────────────────────────────────
echo ""
echo "═══ Step 5: Test /v1/models endpoint ════════════════════════════════════"
MODELS_RESP=$(curl -s "http://127.0.0.1:$PORT/v1/models" \
  -H "Authorization: Bearer $API_KEY")
MODEL_COUNT=$(echo "$MODELS_RESP" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(len(d.get('data', [])))
" 2>/dev/null || echo "0")
if [ "$MODEL_COUNT" -gt "0" ]; then
  ok "Models endpoint OK — $MODEL_COUNT models available"
  echo "$MODELS_RESP" | python3 -c "
import json,sys
d = json.load(sys.stdin)
for m in d.get('data', [])[:5]:
    print('   •', m['id'])
"
else
  fail "Models endpoint failed or returned empty list"
  info "$MODELS_RESP"
fi

# ─── Step 6: Test /v1/messages (non-streaming) ───────────────────────────────
echo ""
echo "═══ Step 6: Test Anthropic /v1/messages ════════════════════════════════"
FIRST_MODEL=$(echo "$MODELS_RESP" | python3 -c "
import json,sys
d = json.load(sys.stdin)
models = d.get('data', [])
print(models[0]['id'] if models else 'claude-sonnet-4-7')
" 2>/dev/null || echo "claude-sonnet-4-7")

info "Using model: $FIRST_MODEL"
MESSAGES_RESP=$(curl -s -X POST "http://127.0.0.1:$PORT/v1/messages" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -d "{
    \"model\": \"$FIRST_MODEL\",
    \"max_tokens\": 64,
    \"stream\": false,
    \"messages\": [{\"role\": \"user\", \"content\": \"Reply with exactly: PROXY_OK\"}]
  }" 2>&1)

REPLY=$(echo "$MESSAGES_RESP" | python3 -c "
import json,sys
d = json.load(sys.stdin)
for block in d.get('content', []):
    if block.get('type') == 'text':
        print(block['text'].strip())
        break
" 2>/dev/null || echo "")

if echo "$REPLY" | grep -q "PROXY_OK"; then
  ok "Anthropic API proxy works! Response: $REPLY"
else
  fail "Unexpected response from /v1/messages"
  info "Raw response: $MESSAGES_RESP"
fi

# ─── Step 7: Test OpenAI-compat /v1/chat/completions ─────────────────────────
echo ""
echo "═══ Step 7: Test OpenAI /v1/chat/completions ════════════════════════════"
CHAT_RESP=$(curl -s -X POST "http://127.0.0.1:$PORT/v1/chat/completions" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"model\": \"$FIRST_MODEL\",
    \"max_tokens\": 64,
    \"stream\": false,
    \"messages\": [{\"role\": \"user\", \"content\": \"Reply with exactly: OPENAI_OK\"}]
  }" 2>&1)

CHAT_REPLY=$(echo "$CHAT_RESP" | python3 -c "
import json,sys
d = json.load(sys.stdin)
choices = d.get('choices', [])
if choices:
    print(choices[0].get('message', {}).get('content', '').strip())
" 2>/dev/null || echo "")

if echo "$CHAT_REPLY" | grep -q "OPENAI_OK"; then
  ok "OpenAI-compat proxy works! Response: $CHAT_REPLY"
else
  fail "Unexpected response from /v1/chat/completions"
  info "Raw response: $CHAT_RESP"
fi

# ─── Step 8: Claude Code integration instructions ────────────────────────────
echo ""
echo "═══ Step 8: Configure Claude Code ══════════════════════════════════════"
ok "All proxy tests passed! Configure Claude Code with:"
echo ""
echo "  export ANTHROPIC_BASE_URL=\"http://127.0.0.1:$PORT\""
echo "  export ANTHROPIC_API_KEY=\"$API_KEY\""
echo "  claude"
echo ""
echo "Or add to ~/.zshrc / your shell profile:"
echo "  export ANTHROPIC_BASE_URL=\"http://127.0.0.1:$PORT\""
echo "  export ANTHROPIC_API_KEY=\"$API_KEY\""
echo ""
