# nicehist

Smarter shell history for ZSH. nicehist replaces your default history with an ML-powered prediction engine that learns your command patterns and suggests what you'll type next -- as ghost text, right in your terminal.

## Why nicehist?

Your shell history is one of your most valuable productivity tools, but the default ZSH history is a flat file with no context. nicehist stores every command with **where** you ran it, **what project** you were in, **whether it succeeded**, and **what you ran before it** -- then uses all of that to predict what you'll type next.

- **Ghost text suggestions** appear inline as you type. Hit Ctrl+E to accept.
- **Learns command sequences**: after `git add`, it knows you usually `git commit`. After `cargo build`, it suggests `cargo test`.
- **Directory-aware**: suggests `npm start` in your Node project, `cargo run` in your Rust project.
- **Built-in fasd replacement**: `z` to jump to frecent directories, `zz` for interactive fzf picker, `f` for frecent files. Frecency is bootstrapped from your existing history automatically.
- **Interactive search** with Ctrl+R via fzf integration.
- **Fast**: <10ms predictions with sub-millisecond cache hits. A Rust daemon handles all the heavy lifting over a Unix socket so your shell never blocks.

## Architecture

```
ZSH Plugin ──── Unix Socket ──── Rust Daemon
  (hooks,                          (SQLite, n-gram
   widgets,                         prediction,
   ghost text)                      context detection)
```

The plugin registers ZSH hooks to capture commands as you run them, and ZLE widgets to display ghost text suggestions. All storage, prediction, and search happens in the daemon -- the plugin never touches disk or blocks your shell.

## Installation

### Build from source

```bash
git clone https://github.com/akatrevorjay/nicehist
cd nicehist
cargo build --release
```

### ZSH Plugin Setup

Add to your `.zshrc`:

```zsh
# Option 1: Direct source
source /path/to/nicehist/plugin/nicehist.plugin.zsh

# Option 2: With zinit
zinit light akatrevorjay/nicehist

# Option 3: With oh-my-zsh
# Clone to ~/.oh-my-zsh/custom/plugins/nicehist
# Add "nicehist" to plugins array
```

### Import your existing history

nicehist starts learning immediately, but you can bootstrap it with your existing history:

```zsh
# Import from $HISTFILE (default: ~/.zsh_history)
nicehist import

# Or specify a file
nicehist import /path/to/history
```

### Migrating from fasd

If you're coming from fasd, nicehist can import your frecency data:

```zsh
# Import from ~/.fasd (default)
nicehist import-fasd

# Or specify a file
nicehist import-fasd /path/to/.fasd
```

Your `z`, `zz`, `d`, and `f` commands will work immediately -- no re-learning required.

## Configuration

All settings are optional with sensible defaults. Set them in `.zshrc` **before** sourcing the plugin:

```zsh
typeset -gA NICEHIST
NICEHIST[SUGGESTIONS_ENABLED]=1          # Enable ghost text suggestions
NICEHIST[MAX_SUGGESTIONS]=5              # Max suggestions to fetch
NICEHIST[PREDICTION_TIMEOUT_MS]=10       # Timeout for predictions (ms)
NICEHIST[MIN_PREFIX_LENGTH]=2            # Min chars before suggesting
NICEHIST[AUTO_START_DAEMON]=1            # Auto-start daemon on shell init
NICEHIST[BIND_CTRL_E]=1                  # Bind Ctrl+E to accept suggestion
NICEHIST[BIND_RIGHT_ARROW]=0            # Bind Right Arrow to accept suggestion
NICEHIST[FZF_BIND_CTRL_R]=1             # Bind Ctrl+R to nicehist-fzf
NICEHIST[FZF_NGRAM_BOOST]=1            # Use n-gram context in Ctrl+R search
NICEHIST[FRECENT_ENABLED]=1             # Enable fasd-like z/zz/d/f functions
NICEHIST[FRECENT_BOOST]=1               # Boost predictions in frecent directories
NICEHIST[DEBUG]=0                        # Enable debug logging
```

### Ranking Weights

Fine-tune prediction scoring by setting `NICEHIST[RANK_WEIGHTS]` to a JSON object. Only include the weights you want to override -- unset keys use defaults:

```zsh
NICEHIST[RANK_WEIGHTS]='{"frequency":0.35,"recency":0.30,"ngram":0.40,"dir_exact":0.35,"dir_hierarchy":0.15,"failure_penalty":0.5,"frecent_boost_max":0.1}'
```

| Weight | Default | Description |
|--------|---------|-------------|
| `frequency` | 0.35 | How much command frequency matters |
| `recency` | 0.30 | How much recent usage matters |
| `ngram` | 0.40 | Weight for command sequence patterns (bigram/trigram) |
| `dir_exact` | 0.35 | Bonus for commands used in the exact same directory |
| `dir_hierarchy` | 0.15 | Bonus for commands used in parent directories |
| `failure_penalty` | 0.50 | How much to penalize frequently-failing commands (0 = no penalty, 1 = full) |
| `frecent_boost_max` | 0.10 | Maximum boost for predictions in frecent directories |

## Usage

### Keybindings

| Key | Action |
|-----|--------|
| **Ctrl+E** | Accept full suggestion |
| **Alt+F** | Accept next word from suggestion |
| **Ctrl+R** | Interactive FZF history search (requires fzf) |

### Commands

```zsh
nicehist search <pattern> [--ngram-boost] # Search history (--ngram-boost for context-aware ranking)
nicehist predict <prefix>                # Get predictions
nicehist import [file]                   # Import zsh_history ($HISTFILE by default)
nicehist context                         # Show current context
nicehist stats                           # Show statistics
nicehist start / stop / restart          # Manage daemon
nicehist ping                            # Check daemon status
nicehist debug                           # Toggle debug mode

# Frecent (fasd replacement)
z <terms>                                # Jump to best matching directory
zz [terms]                               # Interactive fzf directory picker
d [terms]                                # List frecent directories with scores
f [terms]                                # Find best matching frecent file
nicehist frecent [terms] [-d] [-f]       # Query frecent paths
nicehist import-fasd [file]              # Import fasd data (~/.fasd)
nicehist export-fasd [file]              # Export frecent data in fasd format
```

## How It Works

### Prediction Engine

nicehist builds a model of your command patterns using multiple signals:

1. **N-gram sequences** (bigram + trigram): tracks which commands follow which. If you always run `git commit` after `git add`, that pattern gets reinforced with each use.
2. **Directory affinity**: commands are scored higher when you've used them before in the same directory or project.
3. **Recency decay**: recent commands are weighted more heavily, with exponential decay over 30 days.
4. **Argument patterns**: learns which arguments you use with each command, per-directory. Knows that `git checkout main` happens in one repo and `git checkout develop` in another.

### Performance

The n-gram tables store **unique sequences**, not individual executions. Running `git add` then `git commit` 10,000 times is still one bigram row with an incremented counter. Table size is bounded by your vocabulary of distinct command sequences, not your total history length.

All prediction queries hit indexed columns, and an LRU cache (1,000 entries) means repeated keystrokes for the same prefix never touch SQLite. Typical prediction latency is under 1ms for cache hits, under 10ms for cold lookups.

### Storage

Commands are stored in SQLite (WAL mode) with rich metadata:

- Command text (deduplicated)
- Working directory and hostname
- Exit status and duration
- VCS state (repo root, branch)
- Project type detection
- Timestamp and session ID

Default database location:
- **macOS**: `~/Library/Application Support/nicehist/history.db`
- **Linux**: `~/.local/share/nicehist/history.db`

### Schema

```sql
commands        -- Deduplicated command strings
places          -- Directory + host combinations
contexts        -- VCS and project metadata
history         -- Command executions with FK references
ngrams_2        -- Bigram frequencies (prev_cmd -> cmd)
ngrams_3        -- Trigram frequencies (prev2_cmd -> prev_cmd -> cmd)
dir_command_freq -- Per-directory command frequencies
arg_patterns    -- Argument patterns per program/subcommand
frecent_paths   -- Frecent path tracking (fasd replacement)
```

## Development

```bash
# Run tests
cargo test

# Run daemon in foreground with debug logging
RUST_LOG=nicehist_daemon=debug ./target/release/nicehist-daemon

# Test RPC manually
echo '{"method":"ping"}' | socat - UNIX-CONNECT:/tmp/nicehist-$(id -u).sock
```

## License

MIT
