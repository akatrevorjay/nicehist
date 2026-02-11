# nicehist ZLE widget
# Async ghost text suggestions

# State variables
typeset -g _NICEHIST_SUGGESTION=""
typeset -g _NICEHIST_SUGGESTION_PREFIX=""
typeset -g _NICEHIST_LAST_BUFFER=""  # Buffer when suggestion was shown
typeset -g _NICEHIST_ASYNC_FD=""
typeset -g _NICEHIST_WIDGET_INITIALIZED=0

# Initialize widget and keybindings (idempotent)
function _nicehist_init_widget() {
    # Skip if already initialized
    (( _NICEHIST_WIDGET_INITIALIZED )) && return 0

    # Create the suggestion widgets
    zle -N nicehist-suggest _nicehist_suggest_widget
    zle -N nicehist-accept _nicehist_accept_widget
    zle -N nicehist-accept-word _nicehist_accept_word_widget
    zle -N nicehist-clear _nicehist_clear_widget

    # Hook into self-insert to trigger suggestions
    zle -N self-insert _nicehist_self_insert

    # Override widgets that should clear suggestions (editing + navigation)
    local widget
    for widget in backward-delete-char delete-char kill-line kill-word backward-kill-word \
                  up-line-or-history down-line-or-history \
                  up-line-or-search down-line-or-search \
                  up-line-or-beginning-search down-line-or-beginning-search \
                  beginning-of-line end-of-line \
                  history-beginning-search-backward history-beginning-search-forward \
                  history-search-backward history-search-forward; do
        eval "function _nicehist_$widget() { _nicehist_clear_suggestion; zle .$widget }"
        zle -N $widget _nicehist_$widget
    done

    # Wrap accept-line to clear ghost text before Enter commits the line
    function _nicehist_accept_line() {
        _nicehist_clear_suggestion
        zle .accept-line
    }
    zle -N accept-line _nicehist_accept_line

    # Use zle-line-pre-redraw to catch ALL buffer changes (including tab completion)
    # This is more robust than trying to wrap every possible completion widget
    # Must register as widget first, then add to hook
    zle -N _nicehist_check_buffer_widget _nicehist_check_buffer
    autoload -Uz add-zle-hook-widget
    add-zle-hook-widget line-pre-redraw _nicehist_check_buffer_widget

    # Wrap TAB to clear ghost text before completion
    function _nicehist_complete() {
        _nicehist_clear_suggestion
        zle expand-or-complete
    }
    zle -N _nicehist_complete
    bindkey '^I' _nicehist_complete  # TAB

    # Bind keys
    bindkey '^[f' nicehist-accept-word  # Alt+F accepts word
    bindkey '^[F' nicehist-accept-word  # Alt+Shift+F accepts word

    # Ctrl+E accepts full suggestion (gated by config)
    if (( NICEHIST[BIND_CTRL_E] )); then
        bindkey '^E' nicehist-accept
    fi

    # Right arrow accepts full suggestion (gated by config, off by default)
    if (( NICEHIST[BIND_RIGHT_ARROW] )); then
        bindkey '^[[C' nicehist-accept
    fi

    # FZF history search widget (gated by config)
    if (( NICEHIST[FZF_BIND_CTRL_R] )); then
        zle -N nicehist-fzf
        bindkey '^R' nicehist-fzf
    fi

    _NICEHIST_WIDGET_INITIALIZED=1
    _nicehist_debug "Widget initialized"
}

# Called before every line redraw - checks if buffer changed unexpectedly
function _nicehist_check_buffer() {
    # If we have a suggestion displayed, check if buffer changed unexpectedly
    if [[ -n "$POSTDISPLAY" && -n "$_NICEHIST_LAST_BUFFER" ]]; then
        # If buffer changed from what we showed the suggestion for, clear it
        # This catches tab completion and any other buffer modifications
        if [[ "$BUFFER" != "$_NICEHIST_LAST_BUFFER" ]]; then
            _nicehist_clear_suggestion
        fi
    fi
}

# Self-insert wrapper to trigger suggestions
function _nicehist_self_insert() {
    # Call original self-insert
    zle .self-insert

    # Trigger suggestion update
    _nicehist_update_suggestion
}

# Update suggestion based on current buffer
function _nicehist_update_suggestion() {
    local prefix="$BUFFER"

    # Clear suggestion if buffer is too short
    if (( ${#prefix} < ${NICEHIST[MIN_PREFIX_LENGTH]:-2} )); then
        _nicehist_clear_suggestion
        return
    fi

    # Skip if prefix unchanged
    [[ "$prefix" == "$_NICEHIST_SUGGESTION_PREFIX" ]] && return

    # Request new suggestion asynchronously
    _nicehist_request_suggestion "$prefix"
}

# Request suggestion from daemon
function _nicehist_request_suggestion() {
    local prefix="$1"
    _NICEHIST_SUGGESTION_PREFIX="$prefix"

    # Get prediction from CLI (plain text, one command per line)
    local suggestion
    suggestion=$(_nicehist_predict "$prefix")

    if [[ -n "$suggestion" ]]; then
        # Take first line only
        suggestion="${suggestion%%$'\n'*}"

        # Only show if suggestion starts with prefix
        if [[ "$suggestion" == "$prefix"* ]]; then
            _NICEHIST_SUGGESTION="$suggestion"
            _nicehist_show_suggestion
            return
        fi
    fi

    # No valid suggestion
    _nicehist_clear_suggestion
}

# Show suggestion as ghost text
function _nicehist_show_suggestion() {
    local suggestion="$_NICEHIST_SUGGESTION"
    local prefix="$BUFFER"

    if [[ -z "$suggestion" || "$suggestion" == "$prefix" ]]; then
        _nicehist_clear_suggestion
        return
    fi

    # Show the part after the prefix as ghost text
    local suffix="${suggestion#$prefix}"

    if [[ -n "$suffix" ]]; then
        # Track the buffer state when showing suggestion
        _NICEHIST_LAST_BUFFER="$BUFFER"

        # Use POSTDISPLAY for ghost text (dimmed)
        POSTDISPLAY="${suffix}"

        # Style the ghost text (gray)
        region_highlight=("${#BUFFER} $((${#BUFFER} + ${#suffix})) fg=8")
    fi
}

# Clear suggestion
function _nicehist_clear_suggestion() {
    _NICEHIST_SUGGESTION=""
    _NICEHIST_SUGGESTION_PREFIX=""
    _NICEHIST_LAST_BUFFER=""
    POSTDISPLAY=""
    region_highlight=()
}

# Widget: Show suggestion
function _nicehist_suggest_widget() {
    _nicehist_update_suggestion
}

# Widget: Accept full suggestion
function _nicehist_accept_widget() {
    if [[ -n "$_NICEHIST_SUGGESTION" && "$_NICEHIST_SUGGESTION" != "$BUFFER" ]]; then
        BUFFER="$_NICEHIST_SUGGESTION"
        CURSOR=${#BUFFER}
        _nicehist_clear_suggestion
    else
        # No suggestion, do normal end-of-line behavior
        zle .end-of-line
    fi
}

# Widget: Accept next word from suggestion
function _nicehist_accept_word_widget() {
    if [[ -n "$_NICEHIST_SUGGESTION" && "$_NICEHIST_SUGGESTION" != "$BUFFER" ]]; then
        local suffix="${_NICEHIST_SUGGESTION#$BUFFER}"
        local word

        # Extract next word (up to space or end)
        if [[ "$suffix" == *" "* ]]; then
            word="${suffix%% *}"
        else
            word="$suffix"
        fi

        if [[ -n "$word" ]]; then
            BUFFER+="$word"
            CURSOR=${#BUFFER}
            _nicehist_update_suggestion
        fi
    else
        # No suggestion, do normal forward-word
        zle .forward-word
    fi
}

# Widget: Clear suggestion
function _nicehist_clear_widget() {
    _nicehist_clear_suggestion
    zle redisplay
}

# vim: ft=zsh
