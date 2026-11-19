#!/bin/sh
# One pass of the inbox agent with Claude Code in headless mode.
# Needs: the bridge running, `claude` CLI signed in, B24_API_TOKEN if API_TOKEN is set.
cd "$(dirname "$0")" || exit 1
exec claude -p "$(cat prompt.md)" \
  --mcp-config mcp.json --strict-mcp-config \
  --allowedTools "mcp__b24__inbox_list,mcp__b24__inbox_done,mcp__b24__chat_history,mcp__b24__task_get,mcp__b24__propose_reply,mcp__b24__drafts_list"
