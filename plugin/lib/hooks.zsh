# nicehist ZSH hooks
# preexec, precmd, zshaddhistory, chpwd

# Register all hooks
function _nicehist_register_hooks() {
    autoload -Uz add-zsh-hook

    add-zsh-hook preexec _nicehist_preexec
    add-zsh-hook precmd _nicehist_precmd
    add-zsh-hook chpwd _nicehist_chpwd

    # Note: zshaddhistory is handled differently as it's not in add-zsh-hook
    # We hook into it via the function
}

# preexec: Called just before command execution
# Capture start time and command
function _nicehist_preexec() {
    local cmd="$1"

    # Skip if command matches ignore patterns
    if [[ -n "${NICEHIST[IGNORE_PATTERNS]}" && "$cmd" =~ ${NICEHIST[IGNORE_PATTERNS]} ]]; then
        _NICEHIST_CMD_START_TIME=0
        return
    fi

    _NICEHIST_CMD_START_TIME=$EPOCHREALTIME
    _nicehist_debug "preexec: $cmd"
}

# precmd: Called just before prompt is displayed
# Capture exit status and duration, store command
function _nicehist_precmd() {
    local exit_status=$?

    # Skip if no command was timed
    (( _NICEHIST_CMD_START_TIME == 0 )) && return

    local end_time=$EPOCHREALTIME
    local duration_ms=$(( (end_time - _NICEHIST_CMD_START_TIME) * 1000 ))
    duration_ms=${duration_ms%.*}  # Truncate to integer

    # Get the last command from history (HISTCMD points to next, so use -1)
    local cmd
    cmd=$(fc -ln -1 2>/dev/null)
    # Trim leading whitespace
    cmd="${cmd#"${cmd%%[![:space:]]*}"}"

    # Skip empty commands
    [[ -z "$cmd" ]] && return

    _nicehist_debug "precmd: exit=$exit_status duration=${duration_ms}ms cmd=$cmd"

    # Store command asynchronously
    _nicehist_store_async \
        "$cmd" \
        "$PWD" \
        "$exit_status" \
        "$duration_ms" \
        "${_NICEHIST_CMD_START_TIME%.*}" \
        "$_NICEHIST_LAST_CMD" \
        "$_NICEHIST_PREV_CMD"

    # Update command history for n-grams
    _NICEHIST_PREV_CMD="$_NICEHIST_LAST_CMD"
    _NICEHIST_LAST_CMD="$cmd"

    # Reset timer
    _NICEHIST_CMD_START_TIME=0
}

# chpwd: Called when directory changes
# Invalidate context cache
function _nicehist_chpwd() {
    _nicehist_invalidate_context
    _nicehist_debug "chpwd: $PWD"
}

# zshaddhistory hook (optional - can be used for filtering)
# Return 1 to prevent adding to history, 0 to allow
# Note: This is called before preexec
function _nicehist_zshaddhistory() {
    local cmd="$1"

    # Allow all commands by default
    # Could add filtering logic here if needed

    return 0
}

# Hook into zshaddhistory if user wants custom filtering
# Usage: NICEHIST[FILTER_HOOK]=1 to enable
if (( ${NICEHIST[FILTER_HOOK]:-0} )); then
    # Save original hook if exists
    if (( $+functions[zshaddhistory] )); then
        functions[_nicehist_orig_zshaddhistory]="$functions[zshaddhistory]"
    fi

    function zshaddhistory() {
        # Call original hook first
        if (( $+functions[_nicehist_orig_zshaddhistory] )); then
            _nicehist_orig_zshaddhistory "$@" || return 1
        fi

        # Call our hook
        _nicehist_zshaddhistory "$@"
    }
fi

# vim: ft=zsh
