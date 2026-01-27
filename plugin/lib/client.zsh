# nicehist CLI client
# All daemon communication goes through the Rust CLI binary

# Cached CLI binary path (zero forks on hot path)
typeset -g _NICEHIST_CLI_PATH=""

# Resolve CLI binary path, caching the result
_nicehist_ensure_cli() {
    [[ -x "$_NICEHIST_CLI_PATH" ]] && return 0
    local p="${NICEHIST[DAEMON_PATH]%-daemon}"
    if [[ -x "$p" ]]; then _NICEHIST_CLI_PATH="$p"; return 0; fi
    for p in "${commands[nicehist]:-}" "/usr/local/bin/nicehist" "$HOME/.cargo/bin/nicehist"; do
        if [[ -x "$p" ]]; then _NICEHIST_CLI_PATH="$p"; return 0; fi
    done
    return 1
}

# Store command via CLI (non-blocking)
function _nicehist_store_async() {
    local cmd="$1"
    local cwd="$2"
    local exit_status="$3"
    local duration_ms="$4"
    local start_time="$5"
    local prev_cmd="$6"
    local prev2_cmd="$7"

    _nicehist_ensure_cli || return 1

    local -a argv=("$_NICEHIST_CLI_PATH" store --cmd "$cmd" --cwd "$cwd")
    [[ -n "$exit_status" ]] && argv+=(--exit-status "$exit_status")
    [[ -n "$duration_ms" ]] && argv+=(--duration-ms "$duration_ms")
    [[ -n "$start_time" ]] && argv+=(--start-time "$start_time")
    [[ -n "$_NICEHIST_SESSION_ID" ]] && argv+=(--session-id "$_NICEHIST_SESSION_ID")
    [[ -n "$prev_cmd" ]] && argv+=(--prev-cmd "$prev_cmd")
    [[ -n "$prev2_cmd" ]] && argv+=(--prev2-cmd "$prev2_cmd")

    { "${argv[@]}" &>/dev/null } &!
}

# Get predictions via CLI (returns plain text lines)
function _nicehist_predict() {
    local prefix="$1"
    local cwd="${2:-$PWD}"
    local limit="${3:-${NICEHIST[MAX_SUGGESTIONS]:-5}}"

    _nicehist_ensure_cli || return 1

    local -a argv=("$_NICEHIST_CLI_PATH" predict --prefix "$prefix" --cwd "$cwd" --limit "$limit" --plain)
    [[ -n "$_NICEHIST_LAST_CMD" ]] && argv+=(--last-cmd "$_NICEHIST_LAST_CMD")
    [[ -n "$_NICEHIST_PREV_CMD" ]] && argv+=(--prev-cmd "$_NICEHIST_PREV_CMD")

    "${argv[@]}" 2>/dev/null
}

# Search history via CLI (returns plain text lines)
function _nicehist_search() {
    local pattern="$1"
    local limit="${2:-20}"
    local dir="$3"

    _nicehist_ensure_cli || return 1

    local -a argv=("$_NICEHIST_CLI_PATH" search "$pattern" --limit "$limit" --plain)
    [[ -n "$dir" ]] && argv+=(--dir "$dir")

    "${argv[@]}" 2>/dev/null
}

# vim: ft=zsh
