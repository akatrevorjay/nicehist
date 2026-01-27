# nicehist core functionality
# Daemon lifecycle management and utilities

# Debug logging
function _nicehist_debug() {
    (( NICEHIST[DEBUG] )) && print -P "%F{cyan}[nicehist]%f $*" >&2
}

# Check if daemon is running
function _nicehist_daemon_running() {
    [[ -S "${NICEHIST[SOCKET_PATH]}" ]] && _nicehist_ping
}

# Ping the daemon to check if it's responsive
function _nicehist_ping() {
    _nicehist_ensure_cli && "$_NICEHIST_CLI_PATH" ping &>/dev/null
}

# Start the daemon if not running
function _nicehist_start_daemon() {
    _nicehist_daemon_running && return 0

    local daemon_path="${NICEHIST[DAEMON_PATH]}"

    # Try alternative paths if default doesn't exist
    if [[ ! -x "$daemon_path" ]]; then
        for path in \
            "${NICEHIST_PLUGIN_DIR:h}/target/debug/nicehist-daemon" \
            "${commands[nicehist-daemon]:-}" \
            "/usr/local/bin/nicehist-daemon" \
            "$HOME/.cargo/bin/nicehist-daemon"
        do
            if [[ -x "$path" ]]; then
                daemon_path="$path"
                break
            fi
        done
    fi

    if [[ ! -x "$daemon_path" ]]; then
        _nicehist_debug "Daemon not found at $daemon_path"
        return 1
    fi

    _nicehist_debug "Starting daemon: $daemon_path"

    # Start daemon in background
    "$daemon_path" &>/dev/null &!

    # Wait for daemon to start (max 500ms)
    local i
    for i in {1..10}; do
        if _nicehist_daemon_running; then
            _nicehist_debug "Daemon started"
            return 0
        fi
        sleep 0.05
    done

    _nicehist_debug "Daemon failed to start"
    return 1
}

# Stop the daemon
function _nicehist_stop_daemon() {
    if [[ -S "${NICEHIST[SOCKET_PATH]}" ]]; then
        # Try graceful shutdown first
        _nicehist_ensure_cli && "$_NICEHIST_CLI_PATH" shutdown &>/dev/null
        sleep 0.1

        # Force remove socket if still exists
        [[ -S "${NICEHIST[SOCKET_PATH]}" ]] && rm -f "${NICEHIST[SOCKET_PATH]}"
    fi
}

# Track initialization state
typeset -g _NICEHIST_INITIALIZED=0

# Initialize nicehist (idempotent)
function _nicehist_init() {
    # Load add-zsh-hook if available
    autoload -Uz add-zsh-hook 2>/dev/null || return 1

    # Auto-start daemon if configured
    if (( NICEHIST[AUTO_START_DAEMON] )); then
        _nicehist_start_daemon
    fi

    # Only register hooks once
    if (( ! _NICEHIST_INITIALIZED )); then
        _nicehist_register_hooks
        _NICEHIST_INITIALIZED=1
    fi

    # Initialize widget if suggestions enabled
    if (( NICEHIST[SUGGESTIONS_ENABLED] )); then
        _nicehist_init_widget
    fi

    _nicehist_debug "Initialized"
}

# Session tracking
typeset -g _NICEHIST_SESSION_ID="$$"
typeset -g _NICEHIST_LAST_CMD=""
typeset -g _NICEHIST_PREV_CMD=""
typeset -g _NICEHIST_CMD_START_TIME=0

# Get current context (cached)
typeset -gA _NICEHIST_CONTEXT_CACHE
typeset -g _NICEHIST_CONTEXT_DIR=""

function _nicehist_get_context() {
    local cwd="$PWD"

    # Return cached context if same directory
    if [[ "$cwd" == "$_NICEHIST_CONTEXT_DIR" && -n "${_NICEHIST_CONTEXT_CACHE[vcs]:-}" ]]; then
        return 0
    fi

    _nicehist_ensure_cli || return 1

    # Fetch context via CLI (key=value lines)
    local line key value
    _NICEHIST_CONTEXT_CACHE=()
    while IFS='=' read -r key value; do
        [[ -n "$key" ]] && _NICEHIST_CONTEXT_CACHE[$key]="$value"
    done < <("$_NICEHIST_CLI_PATH" context --cwd "$cwd" 2>/dev/null)

    [[ ${#_NICEHIST_CONTEXT_CACHE} -gt 0 ]] && _NICEHIST_CONTEXT_DIR="$cwd"
}

# Invalidate context cache (called on chpwd)
function _nicehist_invalidate_context() {
    _NICEHIST_CONTEXT_DIR=""
    _NICEHIST_CONTEXT_CACHE=()
}

# vim: ft=zsh
