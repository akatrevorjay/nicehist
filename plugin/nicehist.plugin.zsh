# nicehist - ZSH history with ML-based prediction
# Main plugin entry point

# Get plugin directory
0="${ZERO:-${${0:#$ZSH_ARGZERO}:-${(%):-%N}}}"
0="${${(M)0:#/*}:-$PWD/$0}"
NICEHIST_PLUGIN_DIR="${0:h}"

# Default configuration (use := to allow override before sourcing)
typeset -gA NICEHIST
: ${NICEHIST[DB_PATH]:="${XDG_DATA_HOME:-$HOME/.local/share}/nicehist/history.db"}
if [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
    : ${NICEHIST[SOCKET_PATH]:="${XDG_RUNTIME_DIR}/nicehist.sock"}
else
    : ${NICEHIST[SOCKET_PATH]:="/tmp/nicehist-${UID}.sock"}
fi
: ${NICEHIST[DAEMON_PATH]:="${NICEHIST_PLUGIN_DIR:h}/target/release/nicehist-daemon"}
: ${NICEHIST[SUGGESTIONS_ENABLED]:=1}
: ${NICEHIST[MAX_SUGGESTIONS]:=5}
: ${NICEHIST[PREDICTION_TIMEOUT_MS]:=100}
: ${NICEHIST[MIN_PREFIX_LENGTH]:=2}
: ${NICEHIST[IGNORE_PATTERNS]:="^(ls|cd|pwd|exit|clear|history)$"}
: ${NICEHIST[AUTO_START_DAEMON]:=1}
: ${NICEHIST[BIND_CTRL_E]:=1}
: ${NICEHIST[BIND_RIGHT_ARROW]:=0}
: ${NICEHIST[FZF_BIND_CTRL_R]:=1}
: ${NICEHIST[DEBUG]:=0}

# Load library files
source "${NICEHIST_PLUGIN_DIR}/lib/core.zsh"
source "${NICEHIST_PLUGIN_DIR}/lib/client.zsh"
source "${NICEHIST_PLUGIN_DIR}/lib/hooks.zsh"
source "${NICEHIST_PLUGIN_DIR}/lib/widget.zsh"

# Add functions directory to fpath (avoid duplicates)
if [[ -z "${fpath[(r)${NICEHIST_PLUGIN_DIR}/functions]}" ]]; then
    fpath=("${NICEHIST_PLUGIN_DIR}/functions" $fpath)
fi

# Autoload functions
autoload -Uz nicehist nicehist-fzf

# Initialize (idempotent)
_nicehist_init

# vim: ft=zsh
