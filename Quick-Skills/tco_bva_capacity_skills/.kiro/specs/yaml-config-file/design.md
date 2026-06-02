# Technical Design Document

## Overview

Add a YAML configuration system to `bedrock_pricing.py` that loads user-level and project-level config files, merges them with a defined precedence chain, and exposes resolved settings to all calculation functions. The implementation is self-contained within the existing monolithic script. PyYAML is required for parsing; template generation uses string formatting only (no PyYAML needed for `--init-config`).

## Architecture

### Data Flow

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│ ~/.bedrock_skills/  │     │ ./.bedrock_skills.yaml│     │ Environment Vars    │
│ config.yaml         │     │ (project-level)      │     │ BEDROCK_*           │
│ (user-level)        │     │                      │     │                     │
└────────┬────────────┘     └──────────┬───────────┘     └──────────┬──────────┘
         │                             │                            │
         ▼                             ▼                            │
   ┌─────────────────────────────────────────┐                     │
   │         load_config()                    │                     │
   │  1. Read user config (if exists)         │                     │
   │  2. Read project config (if exists)      │                     │
   │  3. Validate both                        │                     │
   │  4. Deep merge (project > user)          │                     │
   │  → Returns: Merged_Config dict           │                     │
   └────────────────────┬────────────────────┘                     │
                        │                                           │
                        ▼                                           ▼
              ┌──────────────────────────────────────────────────────────┐
              │              resolve_setting(section, key, explicit_val)  │
              │  Precedence: explicit_val > env_var > config > default    │
              └──────────────────────────┬───────────────────────────────┘
                                         │
                                         ▼
                              ┌───────────────────────┐
                              │  Calculation Functions │
                              │  (use resolved values) │
                              └───────────────────────┘
```

### Module Placement

All config code lives in `bedrock_pricing.py` as top-level functions, placed after the imports/constants block (line ~80) and before the first existing function (`classify_provider`). This keeps the config system available to all downstream functions when the script is `exec()`'d by skills.

## Components and Interfaces

### Components

1. **Config_Schema** — Static `CONFIG_SCHEMA` dict defining all sections, keys, types, defaults, and validation constraints. Single source of truth.
2. **Config_Loader** — `load_config()` + `_read_yaml_file()` + `_validate_config()` + `_deep_merge()`. Discovers, reads, validates, and merges config files.
3. **Config_Resolver** — `resolve_setting()`. Applies the full precedence chain (explicit > env > config > default) for any individual setting.
4. **Config_Generator** — `generate_config_template()`. Produces the commented YAML template from `CONFIG_SCHEMA`.
5. **CLI Handler** — `--init-config` argument in `main()`. Entry point for template generation.

### Interfaces

```
Config_Loader.load_config(user_path?, project_path?) → Merged_Config dict
Config_Resolver.resolve_setting(section, key, explicit_value?, env_var?) → resolved value
Config_Generator.generate_config_template(output_path?) → YAML string
Config_Loader.get_config(section?, key?) → dict | value | None
```

### Integration Interface

Existing calculation functions call `resolve_setting()` for each parameter that has a `None` default, which transparently resolves through the precedence chain without changing the function's external API.

## Data Models

### CONFIG_SCHEMA Structure

```python
CONFIG_SCHEMA: dict[str, dict[str, FieldSpec]]

FieldSpec = {
    "type": type,           # int, float, str, bool
    "default": Any,         # hardcoded default value
    "min": number,          # optional: minimum value (inclusive)
    "max": number,          # optional: maximum value (inclusive)
    "choices": list[str],   # optional: valid string values
    "max_len": int,         # optional: max string length
}
```

### Merged_Config Structure

```python
Merged_Config = {
    "reports": {"output_dir": str, "format": str, ...},
    "defaults": {"region": str|None, "tier_preference": str|None, "history_mode": str},
    "agent_defaults": {"questions_per_session": int, "input_tokens": int, ...},
    "rag_defaults": {"system_prompt_tokens": int, "rag_n_chunks": int, "rag_chunk_size": int, ...},
    "research_defaults": {"system_prompt_tokens": int, "n_research_iterations": int, "fetch_probability": float, ...},
    "agentcore_defaults": {"num_vcpus": int, "peak_memory_gb": float, ...},
    "business_value_defaults": {"time_without_ai_min": float, ...},
    "capacity": {"peak_to_avg_ratio": float, ...},
    "pricing_cache": {"dir": str, "max_age_days": int, "auto_refresh": bool},
    "behavior": {"skip_confirmation": bool, "auto_capacity_check": bool, "detail_level": str},
    "model_preferences": {"router": str, "general": str, "rag": str, "research": str, "version": str},
}
```

### Environment Variable Mapping

Pattern: `BEDROCK_{SECTION}_{KEY}` (all uppercase, underscores for separators)

Example: `agent_defaults.input_tokens` → `BEDROCK_AGENT_DEFAULTS_INPUT_TOKENS`

## Detailed Design

### 1. Configuration Schema Definition

A `CONFIG_SCHEMA` dict defines all recognized sections, keys, types, defaults, and validation rules. This is the single source of truth for both validation and template generation.

```python
CONFIG_SCHEMA = {
    "reports": {
        "output_dir": {"type": str, "default": "~/bedrock_reports"},
        "format": {"type": str, "default": "markdown", "choices": ["markdown", "json", "csv"]},
        "retention_days": {"type": int, "default": 30, "min": 1, "max": 3650},
        "naming_template": {"type": str, "default": "{model}_{volume}_{timestamp}.md", "max_len": 128},
        "include_metadata": {"type": bool, "default": True},
        "auto_cleanup": {"type": bool, "default": False},
    },
    "defaults": {
        "region": {"type": str, "default": None},
        "tier_preference": {"type": str, "default": None},
        "history_mode": {"type": str, "default": "full", "choices": ["full", "condensed"]},
    },
    "agent_defaults": {
        "questions_per_session": {"type": int, "default": 5, "min": 1},
        "input_tokens": {"type": int, "default": 100, "min": 1},
        "output_tokens": {"type": int, "default": 150, "min": 1},
        "system_prompt_tokens": {"type": int, "default": 2000, "min": 1},
        "tools_passed": {"type": int, "default": 10, "min": 0},
        "tool_spec_tokens": {"type": int, "default": 100, "min": 1},
        "tools_invoked": {"type": int, "default": 5, "min": 0},
        "tool_call_tokens": {"type": int, "default": 100, "min": 1},
        "tool_result_tokens": {"type": int, "default": 100, "min": 1},
    },
    "agentcore_defaults": {
        "num_vcpus": {"type": int, "default": 2, "min": 1},
        "peak_memory_gb": {"type": float, "default": 4.0, "min": 0.5},
        "io_wait_pct": {"type": float, "default": 0.70, "min": 0.0, "max": 1.0},
        "idle_time_between_questions_s": {"type": int, "default": 30, "min": 0},
        "stm_events_per_question": {"type": int, "default": 2, "min": 0},
        "ltm_records_per_session": {"type": int, "default": 3, "min": 0},
        "ltm_retrievals_per_question": {"type": int, "default": 1, "min": 0},
        "tools_indexed": {"type": int, "default": 50, "min": 0},
        "eval_sampling_rate": {"type": float, "default": 0.10, "min": 0.0, "max": 1.0},
        "eval_builtin_evaluators": {"type": int, "default": 3, "min": 0},
    },
    "rag_defaults": {
        "system_prompt_tokens": {"type": int, "default": 500, "min": 1},
        "n_tools": {"type": int, "default": 2, "min": 0},
        "tool_spec_tokens": {"type": int, "default": 100, "min": 1},
        "input_query_tokens": {"type": int, "default": 100, "min": 1},
        "tool_call_tokens": {"type": int, "default": 50, "min": 1},
        "rag_n_retrieval_calls": {"type": int, "default": 2, "min": 1},
        "rag_n_chunks": {"type": int, "default": 10, "min": 1},
        "rag_chunk_size": {"type": int, "default": 300, "min": 1},
        "n_other_tool_calls": {"type": int, "default": 1, "min": 0},
        "other_tool_result_tokens": {"type": int, "default": 200, "min": 1},
        "output_tokens": {"type": int, "default": 300, "min": 1},
    },
    "research_defaults": {
        "system_prompt_tokens": {"type": int, "default": 500, "min": 1},
        "n_tools": {"type": int, "default": 2, "min": 0},
        "tool_spec_tokens": {"type": int, "default": 50, "min": 1},
        "input_query_tokens": {"type": int, "default": 100, "min": 1},
        "tool_call_tokens": {"type": int, "default": 50, "min": 1},
        "n_research_iterations": {"type": int, "default": 4, "min": 1},
        "fetch_probability": {"type": float, "default": 0.5, "min": 0.0, "max": 1.0},
        "search_result_tokens": {"type": int, "default": 100, "min": 1},
        "fetch_result_tokens": {"type": int, "default": 2000, "min": 1},
        "output_tokens": {"type": int, "default": 1000, "min": 1},
    },
    "business_value_defaults": {
        "time_without_ai_min": {"type": float, "default": 20.0, "min": 0.1},
        "time_with_ai_min": {"type": float, "default": 10.0, "min": 0.1},
        "human_cost_per_hour": {"type": float, "default": 75.0, "min": 0.0},
        "revenue_per_hour": {"type": float, "default": 300.0, "min": 0.0},
        "agent_effectiveness_pct": {"type": float, "default": 0.65, "min": 0.0, "max": 1.0},
        "efficiency_factor_pct": {"type": float, "default": 0.60, "min": 0.0, "max": 1.0},
        "churn_without_ai_pct": {"type": float, "default": 2.0, "min": 0.0},
        "churn_with_ai_pct": {"type": float, "default": 1.0, "min": 0.0},
        "sales_increase_pct": {"type": float, "default": 10.0, "min": 0.0},
    },
    "capacity": {
        "peak_to_avg_ratio": {"type": float, "default": 3.0, "min": 1.0, "max": 100.0},
        "active_hours_per_day": {"type": int, "default": 12, "min": 1, "max": 24},
        "active_days_per_month": {"type": int, "default": 22, "min": 1, "max": 31},
        "max_tokens_setting": {"type": int, "default": 4096, "min": 1, "max": 65536},
    },
    "pricing_cache": {
        "dir": {"type": str, "default": "~/bedrock_cache"},
        "max_age_days": {"type": int, "default": 7, "min": 1, "max": 365},
        "auto_refresh": {"type": bool, "default": False},
    },
    "behavior": {
        "skip_confirmation": {"type": bool, "default": False},
        "auto_capacity_check": {"type": bool, "default": False},
        "detail_level": {"type": str, "default": "summary", "choices": ["summary", "full"]},
    },
    "model_preferences": {
        "router": {"type": str, "default": "Claude Opus"},
        "general": {"type": str, "default": "Claude Sonnet"},
        "rag": {"type": str, "default": "Claude Haiku"},
        "research": {"type": str, "default": "Nova Lite"},
        "version": {"type": str, "default": "latest"},
    },
}
```

### 2. Core Functions

#### `load_config(user_path=None, project_path=None) -> dict`

```python
def load_config(user_path=None, project_path=None):
    """Load and merge YAML configuration files.

    Args:
        user_path: Override user config path (default: ~/.bedrock_skills/config.yaml)
        project_path: Override project config path (default: ./config.yaml)

    Returns:
        dict: Merged configuration. Empty dict if no config files found.
        Emits warnings to stderr for validation issues.
    """
```

**Algorithm:**
1. Resolve paths: `user_path` defaults to `~/.bedrock_skills/config.yaml`, `project_path` to `./.bedrock_skills.yaml`
2. Read user config (if exists) → `user_dict`
3. Read project config (if exists) → `project_dict`
4. Validate each against `CONFIG_SCHEMA` (collect warnings/errors, don't abort)
5. Deep merge: `_deep_merge(user_dict, project_dict)` → project wins at every level
6. Store result in module-level `_LOADED_CONFIG` for access by other functions
7. Return merged dict

#### `_read_yaml_file(path) -> dict | None`

```python
def _read_yaml_file(path):
    """Read and parse a YAML file using PyYAML. Returns None on error (with stderr warning)."""
```

**Parsing strategy:**
- Uses `yaml.safe_load()` for all parsing
- On parse error: emit warning with file path + line number (from yaml.YAMLError), return None
- If PyYAML is not available: emit install instructions to stderr, return None

#### `_deep_merge(base, override) -> dict`

```python
def _deep_merge(base, override):
    """Recursively merge override into base. Override wins for leaf values."""
```

#### `_validate_config(config, source_path) -> tuple[dict, list]`

```python
def _validate_config(config, source_path):
    """Validate config dict against CONFIG_SCHEMA.

    Returns:
        (validated_config, warnings): validated_config has invalid values removed,
        warnings is a list of human-readable warning strings.
    """
```

**Validation rules:**
- Unknown top-level sections → warning, skip
- Type mismatch → warning, discard value
- Out-of-range → warning, discard value
- Invalid choice → warning, discard value
- Zero/negative where positive required → warning, discard value

#### `get_config(section=None, key=None) -> dict | Any`

```python
def get_config(section=None, key=None):
    """Access the loaded configuration.

    get_config() → full merged config dict
    get_config("agent_defaults") → that section's dict
    get_config("agent_defaults", "input_tokens") → that specific value or None
    """
```

#### `resolve_setting(section, key, explicit_value=None, env_var=None) -> Any`

```python
def resolve_setting(section, key, explicit_value=None, env_var=None):
    """Resolve a setting through the full precedence chain.

    Precedence: explicit_value > env_var > config > schema default

    Args:
        section: Config section name (e.g., "agent_defaults")
        key: Setting key within section (e.g., "input_tokens")
        explicit_value: Function parameter value (None = not provided)
        env_var: Environment variable name to check (e.g., "BEDROCK_INPUT_TOKENS")

    Returns:
        Resolved value with correct type.
    """
```

#### `generate_config_template(output_path=None) -> str`

```python
def generate_config_template(output_path=None):
    """Generate a commented YAML config template from CONFIG_SCHEMA.

    Args:
        output_path: Where to write. Default: ~/.bedrock_skills/config.yaml

    Returns:
        str: The generated YAML content.
    """
```

**Template generation** iterates `CONFIG_SCHEMA` and produces commented-out entries:

```yaml
# ─── Reports ───────────────────────────────────────────────────────────────
# Configure report output preferences (used by file-based report output feature)
reports:
  # output_dir (string): Directory for report files
  # output_dir: ~/bedrock_reports

  # format (string): Report format. Options: markdown, json, csv
  # format: markdown

  # retention_days (integer, 1-3650): Days to keep old reports
  # retention_days: 30
  ...

# ─── Model Preferences ────────────────────────────────────────────────────
# Default model selections by agent role. Used when the user does not specify
# a model in their prompt. Values are search hints passed to query_model_pricing().
model_preferences:
  # router (string): Model for orchestrator/router agents
  # router: Claude Opus

  # general (string): Model for main inference agents
  # general: Claude Sonnet

  # rag (string): Model for RAG sub-agents (cost-efficient retrieval)
  # rag: Claude Haiku

  # research (string): Model for research sub-agents
  # research: Nova Lite

  # version (string): Model version. "latest" or pin to specific (e.g., "4.6")
  # version: latest
```

### 3. Environment Variable Naming Convention

Pattern: `BEDROCK_{SECTION}_{KEY}` (uppercase, underscores)

| Config Key | Env Var |
|-----------|---------|
| `defaults.region` | `BEDROCK_DEFAULTS_REGION` |
| `agent_defaults.input_tokens` | `BEDROCK_AGENT_DEFAULTS_INPUT_TOKENS` |
| `pricing_cache.dir` | `BEDROCK_PRICING_CACHE_DIR` |
| `capacity.peak_to_avg_ratio` | `BEDROCK_CAPACITY_PEAK_TO_AVG_RATIO` |
| `behavior.detail_level` | `BEDROCK_BEHAVIOR_DETAIL_LEVEL` |

Type conversion from env var string:
- `int`: `int(value)`
- `float`: `float(value)`
- `bool`: `value.lower() in ("true", "1", "yes")`
- `str`: as-is

### 4. Integration Points

#### 4a. Script Initialization

Config is loaded once when the script is `exec()`'d by skills. Add near the top (after imports, before first function):

```python
# Module-level config (loaded lazily on first access)
_LOADED_CONFIG = None

def _ensure_config_loaded():
    """Load config on first access (lazy initialization)."""
    global _LOADED_CONFIG
    if _LOADED_CONFIG is None:
        _LOADED_CONFIG = load_config()
```

#### 4b. Integration with `calculate_agent_session_compounded_cost()`

The function already uses `main_agent_config.get("key", default)` pattern. Integration approach:

```python
# Inside calculate_agent_session_compounded_cost():
questions_per_session = main_agent_config.get(
    "questions_per_agent_session",
    resolve_setting("agent_defaults", "questions_per_session")
)
```

This preserves backward compatibility: if the caller passes the value in `main_agent_config`, it wins. If not, config/default is used.

#### 4c. Integration with `check_capacity_fit()`

```python
def check_capacity_fit(
    capacity_profile,
    questions_per_month,
    output_burndown_rate=1,
    max_tokens_setting=None,  # Changed from 4096 to None
    peak_to_avg_ratio=None,   # Changed from 3.0 to None
    active_hours_per_day=None,  # Changed from 12 to None
    active_days_per_month=None,  # Changed from 22 to None
    tier_limits=None,
):
    # Resolve from config if not explicitly provided
    max_tokens_setting = resolve_setting("capacity", "max_tokens_setting", max_tokens_setting)
    peak_to_avg_ratio = resolve_setting("capacity", "peak_to_avg_ratio", peak_to_avg_ratio)
    active_hours_per_day = resolve_setting("capacity", "active_hours_per_day", active_hours_per_day)
    active_days_per_month = resolve_setting("capacity", "active_days_per_month", active_days_per_month)
    ...
```

#### 4d. Integration with `calculate_agentcore_cost()`

Same pattern — change hardcoded defaults to `None`, resolve via config:

```python
def calculate_agentcore_cost(
    ...,
    num_vcpus=None,
    peak_memory_gb=None,
    io_wait_pct=None,
    idle_time_between_questions_s=None,
    ...
):
    num_vcpus = resolve_setting("agentcore_defaults", "num_vcpus", num_vcpus)
    peak_memory_gb = resolve_setting("agentcore_defaults", "peak_memory_gb", peak_memory_gb)
    ...
```

#### 4e. Integration with `calculate_business_value()`

```python
def calculate_business_value(
    ...,
    time_without_ai_min=None,
    time_with_ai_min=None,
    human_cost_per_hour=None,
    ...
):
    time_without_ai_min = resolve_setting("business_value_defaults", "time_without_ai_min", time_without_ai_min)
    ...
```

#### 4e2. Integration with `calculate_rag_subagent_tokens()`

```python
def calculate_rag_subagent_tokens(
    system_prompt_tokens=None,
    n_tools=None,
    rag_n_chunks=None,
    rag_chunk_size=None,
    output_tokens=None,
    ...
):
    system_prompt_tokens = resolve_setting("rag_defaults", "system_prompt_tokens", system_prompt_tokens)
    n_tools = resolve_setting("rag_defaults", "n_tools", n_tools)
    rag_n_chunks = resolve_setting("rag_defaults", "rag_n_chunks", rag_n_chunks)
    rag_chunk_size = resolve_setting("rag_defaults", "rag_chunk_size", rag_chunk_size)
    output_tokens = resolve_setting("rag_defaults", "output_tokens", output_tokens)
    ...
```

#### 4e3. Integration with `calculate_research_subagent_tokens()`

```python
def calculate_research_subagent_tokens(
    system_prompt_tokens=None,
    n_research_iterations=None,
    fetch_probability=None,
    output_tokens=None,
    ...
):
    system_prompt_tokens = resolve_setting("research_defaults", "system_prompt_tokens", system_prompt_tokens)
    n_research_iterations = resolve_setting("research_defaults", "n_research_iterations", n_research_iterations)
    fetch_probability = resolve_setting("research_defaults", "fetch_probability", fetch_probability)
    output_tokens = resolve_setting("research_defaults", "output_tokens", output_tokens)
    ...
```

#### 4f. Integration with `check_pricing_data_status()`

```python
def check_pricing_data_status(cache_dir=None):
    cache_dir = resolve_setting("pricing_cache", "dir", cache_dir)
    cache_dir = os.path.expanduser(cache_dir)
    max_age = resolve_setting("pricing_cache", "max_age_days")
    # Use max_age instead of hardcoded 7
    ...
```

#### 4g. CLI `--init-config` Integration

Add to `main()`:

```python
parser.add_argument("--init-config", action="store_true",
                    help="Generate commented config template at ~/.bedrock_skills/config.yaml")
parser.add_argument("--force", action="store_true",
                    help="Overwrite existing config file without prompting (use with --init-config)")

# In the handler:
if args.init_config:
    generate_config_template(force=args.force)
    return
```

### 5. YAML Dependency Strategy

The script requires PyYAML for config file parsing:

1. **`import yaml`** at the top of the config section (guarded with try/except)
2. **If PyYAML is not installed**: emit a clear error message to stderr ("PyYAML is required for config file support. Install with: pip install pyyaml"), set `_LOADED_CONFIG = {}`, and continue — all functions use hardcoded defaults
3. **For template generation** (`--init-config`): uses string formatting only, no PyYAML needed. Users can generate the template even without PyYAML installed.
4. **No custom YAML parser.** PyYAML's `yaml.safe_load()` handles all parsing. This avoids the bug surface of a hand-rolled parser.

PyYAML is available in all target environments (Kiro, Claude Code, Quick Desktop). For minimal environments, the script degrades gracefully — config is simply not loaded, and all defaults come from `CONFIG_SCHEMA`.

### 5b. exec() Loading Behavior

When the script is loaded via `exec(open(script).read())` by AI agent skills:
- The `CONFIG_SCHEMA` dict and all config functions are defined (available immediately)
- `_LOADED_CONFIG` is initialized to `None` (no file I/O at load time)
- Config is loaded **lazily** on the first call to `resolve_setting()` or `get_config()`
- This means config loading happens when the first calculation function is called, not at `exec()` time
- If `load_config()` needs to be called explicitly (e.g., to reload after changing config), it can be called directly and will update `_LOADED_CONFIG`

### 6. SKILL.md Updates

Each SKILL.md gets a new "Configuration" section replacing the defaults tables:

```markdown
## Configuration

Parameter defaults are defined in the Python function signatures in `bedrock_pricing.py`.
Override any default via `~/.bedrock_skills/config.yaml` (user-level) or `./config.yaml` (project-level).

Run `python3 bedrock_pricing.py --init-config` to generate a commented template showing all
available settings with their current defaults.

**Precedence:** function parameter > environment variable > project config > user config > hardcoded default

**Config values are defaults only.** If the user specifies a value in their prompt, always use
the user's value. Config defaults apply only to parameters the user has not mentioned.

See the config template for the full list of overridable settings in the `agent_defaults`,
`agentcore_defaults`, `business_value_defaults`, `capacity`, `pricing_cache`, and
`model_preferences` sections.
```

The existing parameter tables in SKILL.md files are simplified to show only:
- Parameter name
- Description/notes
- (Default value column removed — reference config template instead)

## File Changes

| File | Change |
|------|--------|
| `skills/bedrock-pricing/scripts/bedrock_pricing.py` | Add ~280 lines: CONFIG_SCHEMA (with model_preferences), load_config(), resolve_setting(), generate_config_template(), _deep_merge(), _validate_config(), _read_yaml_file(), --init-config CLI handler |
| `skills/bedrock-pricing/SKILL.md` | Replace "Parameter Defaults" section with "Configuration" section; remove default value columns from tables |
| `skills/bedrock-capacity/SKILL.md` | Replace "Defaults" section with "Configuration" section; remove default value columns |
| `skills/agentcore-pricing/SKILL.md` | Replace "Defaults" section with "Configuration" section; remove default value columns |
| `skills/agent-business-value/SKILL.md` | Replace "Defaults & Sources" section with "Configuration" section; keep source citations but remove default values |
| `skills/bedrock-tier-advisor/SKILL.md` | Add "Configuration" section referencing the config system |

## Backward Compatibility

- **No breaking changes.** All existing function signatures retain their current defaults via `resolve_setting()` which falls back to `CONFIG_SCHEMA` defaults when no config exists.
- **PyYAML is the only new dependency.** It's already available in all target environments (Kiro, Claude Code, Quick Desktop). If missing, the script degrades gracefully to hardcoded defaults.
- **No config file required.** Script works identically without any config file present.
- **Existing callers unaffected.** Functions that pass explicit values continue to work — explicit values always win in the precedence chain.
- **`reports` and `behavior` sections are schema-only in this PR.** They appear in the template and are validated, but no code integrates them yet. They're reserved for the file-based report output feature (Spec 2).

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Config file not found | Silent — return empty config, use defaults |
| Invalid YAML syntax | Warning to stderr with file path + line number; skip that file entirely |
| Permission denied reading file | Warning to stderr; skip that file, fall back to next source |
| Unknown top-level section | Warning to stderr naming the section; continue loading known sections |
| Type mismatch on a value | Warning to stderr with key name, expected type, actual value; discard that value, use default |
| Value out of range | Warning to stderr with key name, valid range, actual value; discard that value, use default |
| Env var type conversion failure | Warning to stderr; ignore env var, continue to next precedence source |
| `--init-config` target dir not writable | Error message to stderr; exit without writing |
| `--init-config` file already exists | Prompt user for confirmation (TTY) or abort with --force suggestion (non-TTY) |
| PyYAML not installed | Error to stderr with install instructions; config not loaded, all defaults used |

All warnings/errors go to `stderr` so they don't pollute `stdout` output (which may be piped or captured by skills).

## Correctness Properties

### Property 1: Idempotent Loading
Calling `load_config()` multiple times with the same files produces the same `Merged_Config`.

**Validates: Requirements 1.3, 1.5**

### Property 2: Precedence Invariant
For any setting, `explicit_value` always wins over env var, which always wins over project config, which always wins over user config, which always wins over schema default.

**Validates: Requirements 2.1, 2.2, 2.3**

### Property 3: Independence
Resolving one setting never affects the resolution of another setting.

**Validates: Requirements 2.4**

### Property 4: Backward Compatibility
With no config file present and no env vars set, every `resolve_setting()` call returns exactly the same value as the current hardcoded default in the function signature.

**Validates: Requirements 1.4, 6.4, 7.4, 8.4, 9.4**

### Property 5: Deep Merge Correctness
Keys present in user config but absent in project config are preserved (not deleted by the merge).

**Validates: Requirements 1.3**

### Property 6: Graceful Degradation Without PyYAML
When PyYAML is not installed, all `resolve_setting()` calls return schema defaults. No exceptions are raised, no config files are read, and the script behaves identically to pre-config behavior.

**Validates: Requirements 15.1, 15.2**

## Testing Strategy

Since this is a skill script (not a traditional Python package with pytest), testing is done via:

1. **Manual verification**: Run `--init-config`, inspect the generated template, uncomment values, verify they're picked up by calculation functions.
2. **Inline assertions**: Add a `--test-config` hidden flag that runs a self-test:
   - Creates a temp config file with known values
   - Calls `load_config()` with explicit paths
   - Verifies `resolve_setting()` returns expected values at each precedence level
   - Verifies validation catches bad types/ranges
   - Prints PASS/FAIL summary
3. **Integration test via test_cases.md**: Run existing use cases with a config file that overrides some defaults; verify output matches expectations.
4. **Edge cases to verify**:
   - Empty config file → no errors, all defaults
   - Config with only one section → other sections use defaults
   - Env var overrides config → env var wins
   - Explicit param overrides env var → param wins
   - Invalid YAML → warning, graceful fallback
   - Missing PyYAML → minimal parser works for simple configs
