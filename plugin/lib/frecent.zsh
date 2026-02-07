# nicehist frecent (fasd-like frecency tracking)
# Provides: z, zz, d, f functions

# z -- jump to best matching frecent directory
z() {
    if [[ $# -eq 0 ]]; then
        # List top frecent directories
        "$_NICEHIST_CLI_PATH" frecent -d --plain
        return
    fi
    local result
    result=$("$_NICEHIST_CLI_PATH" frecent "$@" -d --plain --limit 1)
    if [[ -n "$result" && -d "$result" ]]; then
        cd "$result"
    else
        print "z: no match" >&2
        return 1
    fi
}

# zz -- interactive fzf directory picker
zz() {
    if ! command -v fzf &>/dev/null; then
        print "zz: fzf not found" >&2
        return 1
    fi
    local selected
    selected=$("$_NICEHIST_CLI_PATH" frecent "$@" -d --plain --limit 200 | \
        fzf --height=40% --layout=reverse --prompt="z> " \
            --preview='ls -la {}' --preview-window=right:40%:wrap)
    [[ -n "$selected" && -d "$selected" ]] && cd "$selected"
}

# d -- list frecent directories (display, no cd)
d() {
    "$_NICEHIST_CLI_PATH" frecent "$@" -d
}

# f -- frecent file lookup (prints path)
f() {
    if [[ $# -eq 0 ]]; then
        "$_NICEHIST_CLI_PATH" frecent -f --plain
        return
    fi
    local result
    result=$("$_NICEHIST_CLI_PATH" frecent "$@" -f --plain --limit 1)
    if [[ -n "$result" ]]; then
        print "$result"
    else
        print "f: no match" >&2
        return 1
    fi
}

# chpwd hook -- bump directory frecency on every cd
_nicehist_frecent_chpwd() {
    _nicehist_ensure_cli || return 0
    { "$_NICEHIST_CLI_PATH" frecent-add "$PWD" -t d &>/dev/null } &!
}

# vim: ft=zsh
